# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree v2 - Main Implementation with Version History
##
## Updated to use VersionHistory for proper MVCC snapshot isolation.
## Matches Rust implementation architecture.

import std/[atomics, os, locks, options]
import types
import error
import memtable
import table
import merge
import atomics_helpers
import config
import cache
import version_history
import rwlock
import wal
import manifest

# Re-export for compatibility
export types
export error
export memtable
export table
export merge
export config
export cache
export version_history

# ============================================================================
# TreeInner - Core structure for LSM tree (matches Rust implementation)
# ============================================================================

type
  TreeInner* = ref object
    id*: TreeId
    config*: Config
    versionHistory*: RwLock[VersionHistory] # Historical versions for MVCC
    tableIdCounter*: Atomic[uint64]
    flushLock*: Lock
    walManager*: Option[WALManager]         # None when walEnabled=false (in-memory only)
    manifestManager*: ManifestManager
    insertedKeys*: seq[string]              # For benchmark verification

# ============================================================================
# Tree - Public interface wrapper
# ============================================================================

type
  Tree* = ref object
    inner*: TreeInner
    memtableIdCounter*: Atomic[int64]
    blockCache*: Cache

proc getInsertedKeys*(t: Tree): seq[string] =
  ## Return the list of keys inserted during benchmark
  # This is only safe if no inserts are ongoing
  result = t.inner.insertedKeys

proc newTree*(config: Config, id: TreeId = TreeId(0)): Tree =
  ## Create a new LSM tree with version history support
  var tableIdCounter: Atomic[uint64]
  var memtableIdCounter: Atomic[int64]
  var flushLock: Lock
  initLock(flushLock)

  # Initialize block cache if configured
  let cacheSize = if config.cacheSize > 0: uint64(config.cacheSize) else: 64 *
      1024 * 1024
  var blockCache = newCache(cacheSize)

  # Create initial SuperVersion
  let initialVersion = newSuperVersion(0.SeqNo)

  # Create version history
  let versionHist = newVersionHistory(initialVersion)
  let versionHistoryLock = newRwLock[VersionHistory](versionHist)

  # Create WAL manager only if enabled
  var walManager: Option[WALManager] = none(WALManager)
  var memtable = newMemtable(MemtableId(0))

  if config.walEnabled:
    let walResult = newWALManager(config.path)
    if walResult.isErr:
      raise newIoError("Failed to create WAL manager: " & walResult.error.msg)
    walManager = some(walResult.get)

    # Replay WAL to reconstruct memtable state
    let replayResult = walManager.get.replay(memtable)
    if replayResult.isErr:
      raise newIoError("Failed to replay WAL: " & replayResult.error.msg)

  # Create manifest manager
  let manifestResult = newManifestManager(config.path)
  if manifestResult.isErr:
    raise newIoError("Failed to create manifest manager: " &
        manifestResult.error.msg)
  let manifestManager = manifestResult.get

  # Replay manifest to reconstruct SSTable state
  let replayManifestResult = manifestManager.replay()
  if replayManifestResult.isErr:
    raise newIoError("Failed to replay manifest: " &
        replayManifestResult.error.msg)

  let treeInner = TreeInner(
    id: id,
    config: config,
    versionHistory: versionHistoryLock,
    tableIdCounter: tableIdCounter,
    flushLock: flushLock,
    walManager: walManager,
    manifestManager: manifestManager
  )

  result = Tree(
    inner: treeInner,
    memtableIdCounter: memtableIdCounter,
    blockCache: blockCache
  )

# ============================================================================
# ID Management
# ============================================================================

# Forward declarations
proc rotateMemtable*(t: Tree): Option[Memtable]
proc flush*(t: Tree, memtable: Memtable): LsmResult[SsTable]

proc nextTableId*(t: Tree): TableId =
  TableId(fetchAdd(t.inner.tableIdCounter, 1, moRelaxed))

proc nextMemtableId*(t: Tree): MemtableId =
  MemtableId(fetchAdd(t.memtableIdCounter, 1, moRelaxed))

proc currentSnapshotSeqno*(t: Tree): SeqNo =
  ## Return current snapshot sequence number
  load(t.inner.versionHistory.value.latestSeqno, moRelaxed)

# ============================================================================
# Core Operations with Version History
# ============================================================================

proc insert*(t: Tree, key: string, value: string, seqno: SeqNo): (uint64, uint64) =
  ## Insert a key-value pair into the tree

  # Write to WAL first for durability (if enabled)
  if t.inner.config.walEnabled and t.inner.walManager.isSome:
    let walResult = t.inner.walManager.get.appendWrite(seqno, key, value)
    if walResult.isErr:
      discard # WAL write failed, continue anyway

  # Get current version from history (thread-safe read)
  t.inner.versionHistory.acquireRead()
  let currentVersion = t.inner.versionHistory.value.latestVersion()
  t.inner.versionHistory.releaseRead()

  # Use insertFromString which stores a copy of the key
  let (size, totalSize) = currentVersion.activeMemtable.insertFromString(key,
      value, seqno)

  # CRITICAL: Update VersionHistory.latestSeqno to make key visible in snapshot
  t.inner.versionHistory.acquireWrite()
  let currentLatest = load(t.inner.versionHistory.value.latestSeqno, moRelaxed)
  if seqno > currentLatest:
    store(t.inner.versionHistory.value.latestSeqno, seqno, moRelaxed)
  t.inner.versionHistory.releaseWrite()

  return (size, totalSize)

proc remove*(t: Tree, key: string, seqno: SeqNo): (uint64, uint64) =
  ## Remove a key from the tree (insert tombstone)

  # Write to WAL first for durability (if enabled)
  if t.inner.config.walEnabled and t.inner.walManager.isSome:
    let walResult = t.inner.walManager.get.appendRemove(seqno, key)
    if walResult.isErr:
      discard # WAL write failed, continue anyway

  # Get current version from history (thread-safe read)
  t.inner.versionHistory.acquireRead()
  let currentVersion = t.inner.versionHistory.value.latestVersion()
  t.inner.versionHistory.releaseRead()

  # Insert tombstone
  let (size, totalSize) = currentVersion.activeMemtable.insertFromString(key,
      "", seqno)
  return (size, totalSize)

proc getInternalEntry*(t: Tree, key: string, seqno: Option[SeqNo] = none(
    SeqNo)): Option[string] =
  ## Get value for key with snapshot isolation
  ## Simplified to just return the value (key is known by caller)
  let snapshotSeqno = if seqno.isSome: seqno.get else: t.currentSnapshotSeqno()

  # Get appropriate version for this snapshot (thread-safe)
  t.inner.versionHistory.acquireRead()
  let version = t.inner.versionHistory.value.getVersionForSnapshot(snapshotSeqno)
  t.inner.versionHistory.releaseRead()

  # Check active memtable
  let activeResult = version.activeMemtable.get(key, snapshotSeqno)
  if activeResult.isSome:
    return activeResult

  # Check sealed memtables
  if version.sealedMemtables.len > 0:
    for i in countdown(version.sealedMemtables.len - 1, 0):
      let result = version.sealedMemtables[i].get(key, snapshotSeqno)
      if result.isSome:
        return result

  # Check SSTables
  for table in version.tables:
    if table.meta.minKey.len > 0 and key < table.meta.minKey:
      continue
    if table.meta.maxKey.len > 0 and key > table.meta.maxKey:
      continue

    let tableResult = table.lookup(key, snapshotSeqno)
    if tableResult.isSome:
      let entry = tableResult.get
      if not entry.key.isTombstone():
        return some(entry.value)

  none(string)

proc lookup*(t: Tree, key: string, seqno: Option[SeqNo] = none(SeqNo)): Option[string] =
  ## Lookup with snapshot isolation
  let snapshotSeqno = if seqno.isSome: seqno.get else: t.currentSnapshotSeqno()

  # Get appropriate version for this snapshot (thread-safe)
  t.inner.versionHistory.acquireRead()
  let version = t.inner.versionHistory.value.getVersionForSnapshot(snapshotSeqno)
  t.inner.versionHistory.releaseRead()

  # Check active memtable
  let activeResult = version.activeMemtable.get(key, snapshotSeqno)
  if activeResult.isSome:
    return activeResult

  # Check sealed memtables
  if version.sealedMemtables.len > 0:
    for i in countdown(version.sealedMemtables.len - 1, 0):
      let result = version.sealedMemtables[i].get(key, snapshotSeqno)
      if result.isSome:
        return result

  # Check SSTables
  for table in version.tables:
    if table.meta.minKey.len > 0 and key < table.meta.minKey:
      continue
    if table.meta.maxKey.len > 0 and key > table.meta.maxKey:
      continue

    let tableResult = table.lookup(key, snapshotSeqno)
    if tableResult.isSome:
      let entry = tableResult.get
      if not entry.key.isTombstone():
        return some(entry.value)

  none(string)

# Backward compatibility
proc get*(t: Tree, key: string, seqno: Option[SeqNo] = none(
    SeqNo)): Option[string] =
  t.lookup(key, seqno)

# ============================================================================
# Flush and Compaction Support
# ============================================================================

proc rotateMemtable*(t: Tree): Option[Memtable] =
  ## Rotate active memtable to sealed (requires write lock)
  t.inner.versionHistory.acquireWrite()

  if t.inner.versionHistory.value.latestVersion().activeMemtable.isEmpty():
    t.inner.versionHistory.releaseWrite()
    return none(Memtable)

  let yanked = t.inner.versionHistory.value.latestVersion().activeMemtable
  let newMemtable = newMemtable(t.nextMemtableId())

  # Create new SuperVersion with rotated memtable
  var newVersion = newSuperVersion(t.currentSnapshotSeqno())
  newVersion.activeMemtable = newMemtable
  newVersion.sealedMemtables = t.inner.versionHistory.value.latestVersion().sealedMemtables & yanked
  newVersion.tables = t.inner.versionHistory.value.latestVersion().tables

  t.inner.versionHistory.value.appendVersion(newVersion)

  t.inner.versionHistory.releaseWrite()
  return some(yanked)

proc flush*(t: Tree, memtable: Memtable): LsmResult[SsTable] =
  ## Flush a memtable to disk as an SSTable
  ## Returns the created SSTable
  try:
    let tableId = t.nextTableId()
    let path = t.inner.config.path & "/" & $tableId & ".sst"

    # Create table writer
    let writer = newTableWriter(path)
    if writer.isErr:
      return err[SsTable](writer.error)

    # Write all entries from memtable
    var entries = memtable.iter()
    for (key, value) in entries:
      let addResult = writer.get.addEntrySimple(key.userKey, key.seqno,
          key.valueType, value)
      if addResult.isErr:
        writer.get.close()
        return err[SsTable](addResult.error)

    # Finish table
    let finishResult = writer.get.finish()
    if finishResult.isErr:
      writer.get.close()
      return err[SsTable](finishResult.error)

    # Copy metadata from writer BEFORE closing
    let metaMinKey = writer.get.getMinKey()
    let metaMaxKey = writer.get.getMaxKey()
    let metaEntryCount = writer.get.getEntryCount()
    let metaSmallestSeqno = writer.get.getSmallestSeqno()
    let metaLargestSeqno = writer.get.getLargestSeqno()

    # Close writer
    writer.get.close()

    # Create SsTable from the written file
    let ssTable = newSsTable(path)
    ssTable.meta.id = tableId
    # CRITICAL: Copy metadata from writer to SsTable
    ssTable.meta.minKey = metaMinKey
    ssTable.meta.maxKey = metaMaxKey
    ssTable.meta.entryCount = metaEntryCount
    ssTable.meta.smallestSeqno = metaSmallestSeqno
    ssTable.meta.largestSeqno = metaLargestSeqno
    # Get file size
    ssTable.fileSize = uint64(getFileSize(path))

    # Add to manifest
    let manifestResult = t.inner.manifestManager.addTable(ssTable)
    if manifestResult.isErr:
      return err[SsTable](manifestResult.error)

    # Add to version history
    t.inner.versionHistory.acquireWrite()
    let currentVersion = t.inner.versionHistory.value.latestVersion()
    var newVersion = newSuperVersion(t.currentSnapshotSeqno())
    newVersion.activeMemtable = currentVersion.activeMemtable
    newVersion.sealedMemtables = currentVersion.sealedMemtables
    newVersion.tables = currentVersion.tables & ssTable
    t.inner.versionHistory.value.appendVersion(newVersion)
    t.inner.versionHistory.releaseWrite()

    ok(ssTable)
  except:
    err[SsTable](newIoError("Failed to flush memtable: " &
        getCurrentExceptionMsg()))

proc advanceSnapshotSeqno*(t: Tree): SeqNo =
  ## Advance snapshot sequence number (called during flush)
  let oldSeqno = fetchAddSeqNo(t.inner.versionHistory.value.latestSeqno, 1, moRelaxed)
  SeqNo(int64(oldSeqno) + 1)

# ============================================================================
# Tests
# ============================================================================

proc createNewTree*(config: Config, id: TreeId = TreeId(0'u64)): LsmResult[Tree] =
  if dirExists(config.path):
    removeDir(config.path)
  createDir(config.path)
  let tree = newTree(config, id)
  ok(tree)

proc contains*(t: Tree, key: string, seqno: Option[SeqNo] = none(SeqNo)): bool =
  ## Check if key exists in tree
  let result = t.lookup(key, seqno)
  result.isSome()
