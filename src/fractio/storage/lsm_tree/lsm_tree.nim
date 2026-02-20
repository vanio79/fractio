# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree Implementation
##
## A Log-Structured Merge tree for efficient key-value storage.

import fractio/storage/error
import ./types
import ./memtable
import ./sstable/writer
import ./sstable/reader
import std/[os, atomics, locks, options, tables, streams, strutils]

export types except ItemSizeResult

# Default configuration values
const
  DefaultLevelCount* = 7
  DefaultMaxMemtableSize* = 64 * 1024 * 1024 # 64 MiB
  DefaultBlockSize* = 4 * 1024               # 4 KiB

# Create default configuration
proc newConfig*(path: string): LsmTreeConfig =
  LsmTreeConfig(
    path: path,
    levelCount: DefaultLevelCount,
    maxMemtableSize: DefaultMaxMemtableSize,
    blockSize: DefaultBlockSize,
    cacheCapacity: 256 * 1024 * 1024 # 256 MiB
  )

# Create a new LSM tree
proc newLsmTree*(config: LsmTreeConfig,
                 seqnoCounter: SequenceNumberCounter,
                 snapshotTracker: SnapshotTracker): LsmTree =
  # Initialize tables as empty seq for each level
  var emptyTables: seq[seq[SsTable]] = @[]
  for i in 0 ..< config.levelCount:
    emptyTables.add(@[])

  var memtableIdCounter: Atomic[uint64]
  var tableIdCounter: Atomic[uint64]
  memtableIdCounter.store(1'u64, moRelaxed)
  tableIdCounter.store(1'u64, moRelaxed)

  result = LsmTree(
    config: config,
    activeMemtable: newMemtable(),
    sealedMemtables: @[],
    tables: emptyTables,
    memtableIdCounter: memtableIdCounter,
    tableIdCounter: tableIdCounter,
    seqnoCounter: seqnoCounter,
    snapshotTracker: snapshotTracker
  )
  initLock(result.versionLock)

  # Create directory if it doesn't exist
  if not dirExists(config.path):
    createDir(config.path)

# Create a new LSM tree with defaults
proc open*(config: LsmTreeConfig): LsmTree =
  let seqnoCounter = newSequenceNumberCounter()
  let snapshotTracker = newSnapshotTracker()
  result = newLsmTree(config, seqnoCounter, snapshotTracker)

# Insert a key-value pair
proc insert*(tree: LsmTree, key: string, value: string,
    seqno: uint64): ItemSizeResult =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  let itemSize = uint64(key.len + value.len)
  let memtableSize = tree.activeMemtable.insert(key, value, seqno, vtValue)

  result = (itemSize: itemSize, memtableSize: memtableSize)

# Remove a key (inserts a tombstone)
proc remove*(tree: LsmTree, key: string, seqno: uint64): ItemSizeResult =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  let itemSize = uint64(key.len)
  let memtableSize = tree.activeMemtable.remove(key, seqno, weak = false)

  result = (itemSize: itemSize, memtableSize: memtableSize)

# Remove a key with weak tombstone
proc removeWeak*(tree: LsmTree, key: string, seqno: uint64): ItemSizeResult =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  let itemSize = uint64(key.len)
  let memtableSize = tree.activeMemtable.remove(key, seqno, weak = true)

  result = (itemSize: itemSize, memtableSize: memtableSize)

# Get a value by key
proc get*(tree: LsmTree, key: string, seqno: uint64): Option[string] =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  # Check active memtable first
  let memtableEntry = tree.activeMemtable.get(key)
  if memtableEntry.isSome:
    let entry = memtableEntry.get
    if entry.seqno <= seqno:
      case entry.valueType
      of vtValue:
        return some(entry.value)
      of vtTombstone, vtWeakTombstone:
        return none(string)
      of vtIndirection:
        # In a full implementation, this would resolve the indirection
        return some(entry.value)

  # Check sealed memtables
  for memtable in tree.sealedMemtables:
    let entry = memtable.get(key)
    if entry.isSome:
      let e = entry.get
      if e.seqno <= seqno:
        case e.valueType
        of vtValue:
          return some(e.value)
        of vtTombstone, vtWeakTombstone:
          return none(string)
        of vtIndirection:
          return some(e.value)

  # Check SSTables (from newest to oldest, level 0 first)
  # Level 0 tables are not sorted, need to check all
  for table in tree.tables[0]:
    if table.path.len > 0:
      # Open and search the SSTable
      let readerResult = openSsTable(table.path)
      if readerResult.isOk:
        let reader = readerResult.value
        let value = reader.get(key)
        reader.close()
        if value.isSome:
          return value

  # Check other levels (sorted, can use binary search)
  for level in 1 ..< tree.tables.len:
    for table in tree.tables[level]:
      if table.path.len > 0:
        let readerResult = openSsTable(table.path)
        if readerResult.isOk:
          let reader = readerResult.value
          let value = reader.get(key)
          reader.close()
          if value.isSome:
            return value

  return none(string)

# Check if key exists
proc containsKey*(tree: LsmTree, key: string, seqno: uint64): bool =
  let value = tree.get(key, seqno)
  return value.isSome

# Get approximate length
proc approximateLen*(tree: LsmTree): int =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  result = tree.activeMemtable.len
  for memtable in tree.sealedMemtables:
    result += memtable.len

# Check if tree is empty
proc isEmpty*(tree: LsmTree, seqno: uint64, snapshot: Option[uint64]): bool =
  # Simplified check - just check if active memtable is empty
  return tree.activeMemtable.isEmpty()

# Get disk space usage
proc diskSpace*(tree: LsmTree): uint64 =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  result = 0
  for level in tree.tables:
    for table in level:
      result += table.size

# Rotate memtable (move active to sealed)
proc rotateMemtable*(tree: LsmTree): Option[Memtable] =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  if tree.activeMemtable.isEmpty:
    return none(Memtable)

  # Move active memtable to sealed
  let sealed = tree.activeMemtable
  tree.sealedMemtables.add(sealed)

  # Create new active memtable
  tree.activeMemtable = newMemtable()

  return some(sealed)

# Get active memtable
proc activeMemtable*(tree: LsmTree): Memtable =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()
  return tree.activeMemtable

# Get sealed memtable count
proc sealedMemtableCount*(tree: LsmTree): int =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()
  return tree.sealedMemtables.len

# Clear active memtable
proc clearActiveMemtable*(tree: LsmTree) =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()
  tree.activeMemtable.clear()

# Clear the entire tree
proc clear*(tree: LsmTree): StorageResult[void] =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  tree.activeMemtable.clear()
  tree.sealedMemtables = @[]

  # In a full implementation, this would also clear SSTables
  for level in tree.tables.mitems:
    level = @[]

  return okVoid

# Get highest sequence number in tree
proc getHighestSeqno*(tree: LsmTree): Option[uint64] =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  var highest: uint64 = 0
  let activeHighest = tree.activeMemtable.getHighestSeqno()
  if activeHighest > highest:
    highest = activeHighest

  for memtable in tree.sealedMemtables:
    let memHighest = memtable.getHighestSeqno()
    if memHighest > highest:
      highest = memHighest

  if highest > 0:
    return some(highest)
  return none(uint64)

# Get highest persisted sequence number
proc getHighestPersistedSeqno*(tree: LsmTree): Option[uint64] =
  # In a full implementation, this would check the SSTables
  # For now, return none
  return none(uint64)

# L0 run count (for compaction triggers)
proc l0RunCount*(tree: LsmTree): int =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()
  return tree.tables[0].len

# Table count
proc tableCount*(tree: LsmTree): int =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  result = 0
  for level in tree.tables:
    result += level.len

# Level table count
proc levelTableCount*(tree: LsmTree, level: int): Option[int] =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  if level >= 0 and level < tree.tables.len:
    return some(tree.tables[level].len)
  return none(int)

# Blob file count (for KV separation)
proc blobFileCount*(tree: LsmTree): int =
  # Not implemented yet
  return 0

# Stale blob bytes
proc staleBlobBytes*(tree: LsmTree): uint64 =
  # Not implemented yet
  return 0

# Major compaction
proc majorCompact*(tree: LsmTree, targetSize: uint64,
    gcWatermark: uint64): StorageResult[void] =
  # Not fully implemented yet
  # In a full implementation, this would:
  # 1. Take all SSTables
  # 2. Merge them into new SSTables
  # 3. Remove old SSTables
  return okVoid

# Tree config accessor
proc treeConfig*(tree: LsmTree): LsmTreeConfig =
  tree.config

# Get version history lock (for GC)
type VersionHistoryLock* = object
  dummy*: int

proc getVersionHistoryLock*(tree: LsmTree): VersionHistoryLock =
  # Placeholder - in a full implementation, this would return the actual lock
  return VersionHistoryLock(dummy: 0)

proc maintenance*(lock: var VersionHistoryLock, path: string,
    gcWatermark: uint64): StorageResult[void] =
  # Placeholder for version history GC
  return okVoid

# Flush oldest sealed memtable to SSTable
proc flushOldestSealed*(tree: LsmTree): StorageResult[uint64] =
  ## Flush the oldest sealed memtable to disk as an SSTable.
  ## Returns the number of bytes flushed, or 0 if nothing to flush.

  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  if tree.sealedMemtables.len == 0:
    return ok[uint64, StorageError](0'u64)

  # Get the oldest sealed memtable
  let memtable = tree.sealedMemtables[0]
  let flushedBytes = memtable.size

  # Generate SSTable ID
  let tableId = tree.tableIdCounter.fetchAdd(1, moRelaxed) + 1

  # Create SSTable file path
  let sstablePath = tree.config.path / "L0" / ($tableId & ".sst")

  # Ensure L0 directory exists
  let l0Path = tree.config.path / "L0"
  if not dirExists(l0Path):
    try:
      createDir(l0Path)
    except OSError:
      return err[uint64, StorageError](StorageError(kind: seIo,
          ioError: "Failed to create L0 directory"))

  # Write memtable to SSTable
  let writeResult = writeMemtable(sstablePath, memtable)
  if writeResult.isErr:
    return err[uint64, StorageError](writeResult.error)

  var sstable = writeResult.value
  sstable.id = tableId
  sstable.level = 0

  # Add SSTable to level 0
  tree.tables[0].add(sstable)

  # Remove flushed memtable from sealed list
  tree.sealedMemtables.delete(0)

  echo "[INFO] Flushed memtable to SSTable: " & sstablePath &
        " (id=" & $tableId & ", size=" & $flushedBytes & " bytes)"

  return ok[uint64, StorageError](flushedBytes)

# Check if tree has sealed memtables to flush
proc hasSealedMemtables*(tree: LsmTree): bool =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()
  tree.sealedMemtables.len > 0

# Get sealed memtable count
proc sealedCount*(tree: LsmTree): int =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()
  tree.sealedMemtables.len
