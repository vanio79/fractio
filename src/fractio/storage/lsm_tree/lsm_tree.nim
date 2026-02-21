# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree Implementation
##
## A Log-Structured Merge tree for efficient key-value storage.

import fractio/storage/error
import fractio/storage/blob/types as blob_types
import fractio/storage/blob/writer as blob_writer_module
import fractio/storage/blob/reader as blob_reader_module
import fractio/storage/types as storage_types
import fractio/storage/keyspace/options
import ./types
import ./memtable
import ./sstable/writer
import ./sstable/reader
import ./sstable/blob_integration
import ./compaction
import ./block_cache
import fractio/storage/snapshot_tracker
import std/[os, atomics, locks, options, tables, streams, strutils, algorithm]

export types except ItemSizeResult
export storage_types.CompressionType

# Default configuration values
const
  DefaultLevelCount* = 7
  DefaultMaxMemtableSize* = 64 * 1024 * 1024 # 64 MiB
  DefaultBlockSize* = 4 * 1024               # 4 KiB
  DefaultRestartInterval* = 16
  DefaultBloomFpr* = 0.01                    # 1% false positive rate

# Get block size for a level
proc getBlockSize*(config: LsmTreeConfig, level: int): uint32 =
  ## Returns the block size for a specific level.
  if config.blockSizes.len > level:
    return config.blockSizes[level]
  elif config.blockSizes.len > 0:
    # Fall back to last configured level
    return config.blockSizes[^1]
  else:
    # Fall back to default (backward compatibility)
    return uint32(config.blockSize)

# Get restart interval for a level
proc getRestartInterval*(config: LsmTreeConfig, level: int): int =
  ## Returns the restart interval for a specific level.
  if config.restartIntervals.len > level:
    return config.restartIntervals[level]
  elif config.restartIntervals.len > 0:
    return config.restartIntervals[^1]
  else:
    return DefaultRestartInterval

# Get compression type for a level
proc getCompressionType*(config: LsmTreeConfig,
    level: int): storage_types.CompressionType =
  ## Returns the compression type for a specific level.
  if config.compressionTypes.len > level:
    return config.compressionTypes[level]
  elif config.compressionTypes.len > 0:
    return config.compressionTypes[^1]
  else:
    return storage_types.ctNone

# Get bloom filter FPR for a level
proc getBloomFpr*(config: LsmTreeConfig, level: int): float64 =
  ## Returns the bloom filter false positive rate for a specific level.
  ## Returns 0.0 if bloom filter is disabled for this level.
  if config.bloomFpr.len > level:
    return config.bloomFpr[level]
  elif config.bloomFpr.len > 0:
    return config.bloomFpr[^1]
  else:
    return DefaultBloomFpr

# Get bloom filter bits per key for a level
proc getBloomBitsPerKey*(config: LsmTreeConfig, level: int): float64 =
  ## Returns bloom filter bits per key for a specific level.
  ## Returns 0.0 if not configured (will use FPR instead).
  if config.bloomBitsPerKey.len > level:
    return config.bloomBitsPerKey[level]
  elif config.bloomBitsPerKey.len > 0:
    return config.bloomBitsPerKey[^1]
  else:
    return 0.0

# Check if bloom filter is enabled for a level
proc isBloomFilterEnabled*(config: LsmTreeConfig, level: int): bool =
  ## Returns true if bloom filter is enabled for this level.
  let fpr = config.getBloomFpr(level)
  let bitsPerKey = config.getBloomBitsPerKey(level)
  return fpr > 0.0 or bitsPerKey > 0.0

# Blob resolution helper - resolves vtIndirection values
proc resolveBlobValue*(serializedHandle: string,
                       blobPath: string,
                       cache: var blob_reader_module.BlobReaderCache): Option[string] =
  ## Resolves a blob indirection by reading from the blob file.
  ## Returns the actual value, or none if resolution fails.
  try:
    let handle = blob_writer_module.deserializeHandle(serializedHandle)
    let result = blob_reader_module.readValue(cache, handle, blobPath)
    if result.isOk:
      return some(result.value)
  except:
    discard
  return none(string)

# Create default configuration
proc newConfig*(path: string): LsmTreeConfig =
  # Initialize per-level defaults for 7 levels
  var blockSizes: seq[uint32] = @[]
  var restartIntervals: seq[int] = @[]
  var compressionTypes: seq[CompressionType] = @[]
  var bloomFpr: seq[float64] = @[]
  var bloomBitsPerKey: seq[float64] = @[]

  for i in 0 ..< DefaultLevelCount:
    blockSizes.add(uint32(DefaultBlockSize))
    restartIntervals.add(DefaultRestartInterval)
    compressionTypes.add(ctNone)
    bloomFpr.add(DefaultBloomFpr)
    bloomBitsPerKey.add(0.0)

  # Disable bloom filter for last level (optimization)
  bloomFpr[^1] = 0.0

  LsmTreeConfig(
    path: path,
    levelCount: DefaultLevelCount,
    maxMemtableSize: DefaultMaxMemtableSize,
    blockSize: DefaultBlockSize,
    blockSizes: blockSizes,
    restartIntervals: restartIntervals,
    cacheCapacity: 256 * 1024 * 1024, # 256 MiB
    compactionStrategy: defaultLeveled(),
    compressionTypes: compressionTypes,
    bloomFpr: bloomFpr,
    bloomBitsPerKey: bloomBitsPerKey
  )

# Create configuration with custom compaction strategy
proc newConfigWithStrategy*(path: string,
                            strategy: CompactionStrategy): LsmTreeConfig =
  result = newConfig(path)
  result.compactionStrategy = strategy

# Create configuration from CreateOptions
proc newConfigFromOptions*(path: string, opts: CreateOptions): LsmTreeConfig =
  ## Creates an LsmTreeConfig from CreateOptions with per-level settings.

  var blockSizes: seq[uint32] = @[]
  var restartIntervals: seq[int] = @[]
  var compressionTypes: seq[storage_types.CompressionType] = @[]
  var bloomFpr: seq[float64] = @[]
  var bloomBitsPerKey: seq[float64] = @[]

  let levelCount = int(opts.levelCount)

  for level in 0 ..< levelCount:
    # Block size
    if level < opts.dataBlockSizePolicy.sizes.len:
      blockSizes.add(opts.dataBlockSizePolicy.sizes[level])
    elif blockSizes.len > 0:
      blockSizes.add(blockSizes[^1])
    else:
      blockSizes.add(uint32(DefaultBlockSize))

    # Restart interval
    if level < opts.dataBlockRestartIntervalPolicy.intervals.len:
      restartIntervals.add(opts.dataBlockRestartIntervalPolicy.intervals[level])
    elif restartIntervals.len > 0:
      restartIntervals.add(restartIntervals[^1])
    else:
      restartIntervals.add(DefaultRestartInterval)

    # Compression
    if level < opts.dataBlockCompressionPolicy.compressionTypes.len:
      compressionTypes.add(opts.dataBlockCompressionPolicy.compressionTypes[level])
    elif compressionTypes.len > 0:
      compressionTypes.add(compressionTypes[^1])
    else:
      compressionTypes.add(storage_types.ctNone)

    # Bloom filter
    if level < opts.filterPolicy.entries.len:
      let entry = opts.filterPolicy.entries[level]
      if entry.kind == fpeBloom:
        case entry.bloomPolicy.kind
        of bcpFalsePositiveRate:
          bloomFpr.add(entry.bloomPolicy.falsePositiveRate)
          bloomBitsPerKey.add(0.0)
        of bcpBitsPerKey:
          bloomFpr.add(0.0)
          bloomBitsPerKey.add(entry.bloomPolicy.bitsPerKey)
    else:
      bloomFpr.add(DefaultBloomFpr)
      bloomBitsPerKey.add(0.0)

  # Disable bloom filter for last level if expectPointReadHits is true
  if opts.expectPointReadHits and bloomFpr.len > 0:
    bloomFpr[^1] = 0.0
    bloomBitsPerKey[^1] = 0.0

  # Get default block size from first level or default
  let defaultBlockSize = if blockSizes.len > 0: int(blockSizes[
      0]) else: DefaultBlockSize

  LsmTreeConfig(
    path: path,
    levelCount: levelCount,
    maxMemtableSize: opts.maxMemtableSize,
    blockSize: defaultBlockSize,
    blockSizes: blockSizes,
    restartIntervals: restartIntervals,
    cacheCapacity: 256 * 1024 * 1024,
    compactionStrategy: opts.compactionStrategy,
    compressionTypes: compressionTypes,
    bloomFpr: bloomFpr,
    bloomBitsPerKey: bloomBitsPerKey,
    kvSeparationOpts: opts.kvSeparationOpts
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
    snapshotTracker: snapshotTracker,
    blockCache: newBlockCache(config.cacheCapacity),
    counters: newMetricCounters()
  )
  initLock(result.versionLock)

  # Create directory if it doesn't exist
  if not dirExists(config.path):
    createDir(config.path)

# Load SSTables from disk for recovery
proc loadSsTables*(tree: LsmTree) =
  ## Load existing SSTables from disk into the tree.
  ## This is called during recovery to restore persisted data.

  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  var highestSeqno: uint64 = 0

  # Load SSTables for each level
  for level in 0 ..< tree.config.levelCount:
    let levelPath = if level == 0: tree.config.path / "L0"
                    else: tree.config.path / ("L" & $level)

    if not dirExists(levelPath):
      continue

    for kind, filePath in walkDir(levelPath):
      if kind == pcFile and filePath.endsWith(".sst"):
        # Get file info
        let fileSize = getFileSize(filePath)

        # Create SsTable entry
        let tableId = tree.tableIdCounter.fetchAdd(1, moRelaxed) + 1
        var sstable = SsTable(
          id: tableId,
          path: filePath,
          size: uint64(fileSize),
          level: level
        )

        # Try to open SSTable to get key range and highest seqno
        let readerResult = openSsTable(filePath, tableId, tree.blockCache)
        if readerResult.isOk:
          let reader = readerResult.value
          sstable.smallestKey = reader.smallestKey
          sstable.largestKey = reader.largestKey

          # Read all entries to find highest seqno
          for idxEntry in reader.indexBlock.entries:
            let blockResult = readDataBlock(reader.stream, idxEntry.handle)
            if blockResult.isOk:
              let dataBlk = blockResult.value
              for entry in dataBlk.entries:
                if entry.seqno > highestSeqno:
                  highestSeqno = entry.seqno

          reader.close()

        # Add to tree
        tree.tables[level].add(sstable)

  # Update seqno counter to at least the highest seqno found
  if highestSeqno > 0:
    tree.seqnoCounter.fetchMax(highestSeqno + 1)
    snapshot_tracker.set(tree.snapshotTracker, highestSeqno + 1)

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

  # Update metrics (atomic, no lock needed)
  incWrites(tree.counters, itemSize)

  result = (itemSize: itemSize, memtableSize: memtableSize)

# Remove a key (inserts a tombstone)
proc remove*(tree: LsmTree, key: string, seqno: uint64): ItemSizeResult =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  let itemSize = uint64(key.len)
  let memtableSize = tree.activeMemtable.remove(key, seqno, weak = false)

  # Update metrics
  incWrites(tree.counters, itemSize)

  result = (itemSize: itemSize, memtableSize: memtableSize)

# Remove a key with weak tombstone
proc removeWeak*(tree: LsmTree, key: string, seqno: uint64): ItemSizeResult =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  let itemSize = uint64(key.len)
  let memtableSize = tree.activeMemtable.remove(key, seqno, weak = true)

  # Update metrics
  incWrites(tree.counters, itemSize)

  result = (itemSize: itemSize, memtableSize: memtableSize)

# Get a value by key
proc get*(tree: LsmTree, key: string, seqno: uint64): Option[string] =
  ## Get a value by key, resolving blob indirections if KV separation is enabled.

  # Update metrics (atomic, before lock)
  incReads(tree.counters)

  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  # Check if blob resolution is needed
  let needsBlobResolution = tree.config.kvSeparationOpts.isSome
  var blobCache: blob_reader_module.BlobReaderCache = nil
  let blobPath = tree.config.path / "blobs"

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
        if needsBlobResolution:
          if blobCache == nil:
            blobCache = blob_reader_module.newBlobReaderCache()
          return resolveBlobValue(entry.value, blobPath, blobCache)
        else:
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
          if needsBlobResolution:
            if blobCache == nil:
              blobCache = blob_reader_module.newBlobReaderCache()
            return resolveBlobValue(e.value, blobPath, blobCache)
          else:
            return some(e.value)

  # Check SSTables (from newest to oldest, level 0 first)
  for table in tree.tables[0]:
    if table.path.len > 0:
      let readerResult = openSsTable(table.path, table.id, tree.blockCache)
      if readerResult.isOk:
        let reader = readerResult.value

        if needsBlobResolution:
          let entry = reader.getEntry(key)
          reader.close()
          if entry.found:
            let vt = cast[ValueType](entry.valueType)
            case vt
            of vtValue:
              return some(entry.value)
            of vtTombstone, vtWeakTombstone:
              return none(string)
            of vtIndirection:
              if blobCache == nil:
                blobCache = blob_reader_module.newBlobReaderCache()
              return resolveBlobValue(entry.value, blobPath, blobCache)
        else:
          let value = reader.get(key)
          reader.close()
          if value.isSome:
            return value

  # Check other levels (sorted, can use binary search)
  for level in 1 ..< tree.tables.len:
    for table in tree.tables[level]:
      if table.path.len > 0:
        let readerResult = openSsTable(table.path, table.id, tree.blockCache)
        if readerResult.isOk:
          let reader = readerResult.value

          if needsBlobResolution:
            let entry = reader.getEntry(key)
            reader.close()
            if entry.found:
              let vt = cast[ValueType](entry.valueType)
              case vt
              of vtValue:
                return some(entry.value)
              of vtTombstone, vtWeakTombstone:
                return none(string)
              of vtIndirection:
                if blobCache == nil:
                  blobCache = blob_reader_module.newBlobReaderCache()
                return resolveBlobValue(entry.value, blobPath, blobCache)
          else:
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

# Get highest sequence number in memtables (not persisted)
proc getHighestMemtableSeqno*(tree: LsmTree): Option[uint64] =
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

# Get highest sequence number in tree (alias for compatibility)
proc getHighestSeqno*(tree: LsmTree): Option[uint64] =
  tree.getHighestMemtableSeqno()

# Get highest persisted sequence number (from SSTables)
proc getHighestPersistedSeqno*(tree: LsmTree): Option[uint64] =
  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  var highest: uint64 = 0

  # Check all levels for highest seqno in SSTables
  for level in tree.tables:
    for table in level:
      if table.seqnoRange[1] > highest:
        highest = table.seqnoRange[1]

  if highest > 0:
    return some(highest)
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
  ## Performs compaction on the LSM tree.
  ##
  ## This implementation does leveled compaction:
  ## 1. Compacts L0 tables into L1
  ## 2. Merges overlapping tables
  ## 3. Removes tombstones older than gcWatermark

  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  # Find levels with tables to compact
  var sourceLevel = -1
  for level in 0 ..< tree.config.levelCount - 1:
    if tree.tables[level].len > 0:
      sourceLevel = level
      break

  if sourceLevel < 0:
    # No tables to compact
    return okVoid

  let targetLevel = sourceLevel + 1

  # Collect tables to compact from source level
  var tablesToCompact: seq[SsTable] = @[]

  if sourceLevel == 0:
    # For L0, compact all tables
    tablesToCompact = tree.tables[0]
  else:
    # For other levels, pick tables that overlap with next level
    for srcTable in tree.tables[sourceLevel]:
      var hasOverlap = false
      for tgtTable in tree.tables[targetLevel]:
        let srcRange = (srcTable.smallestKey, srcTable.largestKey)
        let tgtRange = (tgtTable.smallestKey, tgtTable.largestKey)
        if keyRangesOverlap(srcRange, tgtRange):
          hasOverlap = true
          break
      if hasOverlap or tree.tables[targetLevel].len == 0:
        tablesToCompact.add(srcTable)

    # Also include overlapping tables from target level
    for tgtTable in tree.tables[targetLevel]:
      for srcTable in tablesToCompact:
        let srcRange = (srcTable.smallestKey, srcTable.largestKey)
        let tgtRange = (tgtTable.smallestKey, tgtTable.largestKey)
        if keyRangesOverlap(srcRange, tgtRange):
          if tgtTable notin tablesToCompact:
            tablesToCompact.add(tgtTable)
          break

  if tablesToCompact.len == 0:
    return okVoid

  # Read all entries from tables to compact
  var allEntries: seq[seq[MergeEntry]] = @[]
  var loadedPaths: seq[string] = @[]

  for table in tablesToCompact:
    if table.path.len > 0 and fileExists(table.path):
      let entriesResult = readSsTableEntries(table.path)
      if entriesResult.isErr:
        return err[void, StorageError](entriesResult.error)
      allEntries.add(entriesResult.value)
      loadedPaths.add(table.path)

  if allEntries.len == 0:
    return okVoid

  # Merge entries with tombstone GC
  # Use provided targetSize, or get from strategy, or use default
  let usedTargetSize = if targetSize > 0:
                         targetSize
                       elif tree.config.compactionStrategy.getTargetTableSize() > 0:
                         tree.config.compactionStrategy.getTargetTableSize()
                       else:
                         DEFAULT_TARGET_TABLE_SIZE
  let mergeResult = mergeEntries(allEntries, gcWatermark)
  if mergeResult.isErr:
    return err[void, StorageError](mergeResult.error)

  let mergedEntries = mergeResult.value

  if mergedEntries.len == 0:
    # All entries were tombstones and got GC'd
    # Just delete the old tables

    # Invalidate block cache for old tables
    for table in tablesToCompact:
      tree.blockCache.invalidateSsTable(table.id)

    let deleteResult = deleteOldTables(tablesToCompact)
    if deleteResult.isErr:
      return err[void, StorageError](deleteResult.error)

    # Remove tables from tree
    if sourceLevel == 0:
      tree.tables[0] = @[]
    else:
      var remainingSource: seq[SsTable] = @[]
      for t in tree.tables[sourceLevel]:
        if t notin tablesToCompact:
          remainingSource.add(t)
      tree.tables[sourceLevel] = remainingSource

    if sourceLevel > 0:
      var remainingTarget: seq[SsTable] = @[]
      for t in tree.tables[targetLevel]:
        if t notin tablesToCompact:
          remainingTarget.add(t)
      tree.tables[targetLevel] = remainingTarget

    return okVoid

  # Write new compacted tables
  var tableIdCounter = tree.tableIdCounter.load(moRelaxed)
  let writeResult = writeCompactedTables(
    mergedEntries,
    tree.config.path,
    targetLevel,
    usedTargetSize,
    tableIdCounter
  )
  if writeResult.isErr:
    return err[void, StorageError](writeResult.error)

  let newTables = writeResult.value
  tree.tableIdCounter.store(tableIdCounter, moRelaxed)

  # Invalidate block cache for old tables before deletion
  for table in tablesToCompact:
    tree.blockCache.invalidateSsTable(table.id)

  # Delete old tables
  let deleteResult = deleteOldTables(tablesToCompact)
  if deleteResult.isErr:
    # Rollback: delete the new tables we just created
    for newTable in newTables:
      if newTable.path.len > 0 and fileExists(newTable.path):
        try:
          removeFile(newTable.path)
        except OSError:
          discard
    return err[void, StorageError](deleteResult.error)

  # Update tree: remove old tables, add new ones
  if sourceLevel == 0:
    tree.tables[0] = @[]
  else:
    var remainingSource: seq[SsTable] = @[]
    for t in tree.tables[sourceLevel]:
      if t notin tablesToCompact:
        remainingSource.add(t)
    tree.tables[sourceLevel] = remainingSource

  if sourceLevel > 0:
    # Remove compacted tables from target level too
    var remainingTarget: seq[SsTable] = @[]
    for t in tree.tables[targetLevel]:
      if t notin tablesToCompact:
        remainingTarget.add(t)
    tree.tables[targetLevel] = remainingTarget

  # Add new compacted tables to target level
  for newTable in newTables:
    tree.tables[targetLevel].add(newTable)

  echo "[INFO] Compaction complete: ", tablesToCompact.len, " tables -> ",
        newTables.len, " tables at level ", targetLevel

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
  ##
  ## If KV separation is enabled, large values are stored in blob files
  ## and the SSTable stores BlobHandles as vtIndirection values.

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

  # Check if KV separation is enabled
  if tree.config.kvSeparationOpts.isSome:
    # Use blob separation during flush
    var blobCtx = newBlobSeparationContext(tree.config.path,
        tree.config.kvSeparationOpts)

    let writerResult = newSsTableWriter(sstablePath, memtable.len)
    if writerResult.isErr:
      return err[uint64, StorageError](writerResult.error)

    let writer = writerResult.value

    # Process each entry, separating large values to blob files
    for entry in memtable.getSortedEntries():
      let processResult = blobCtx.processEntry(entry)
      if processResult.isErr:
        return err[uint64, StorageError](processResult.error)

      let (key, value, valueType, seqno) = processResult.value
      let addResult = writer.add(key, value, seqno, valueType)
      if addResult.isErr:
        return err[uint64, StorageError](addResult.error)

    # Finalize blob files
    let blobFinalizeResult = blobCtx.finalize()
    if blobFinalizeResult.isErr:
      return err[uint64, StorageError](blobFinalizeResult.error)

    let writeResult = writer.finish()
    if writeResult.isErr:
      return err[uint64, StorageError](writeResult.error)

    var sstable = writeResult.value
    sstable.id = tableId
    sstable.level = 0

    # Add SSTable to level 0
    tree.tables[0].add(sstable)
  else:
    # Standard flush without blob separation
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

# Get active memtable size
proc activeMemtableSize*(tree: LsmTree): uint64 =
  ## Returns the size of the active memtable in bytes.
  tree.versionLock.acquire()
  defer: tree.versionLock.release()
  tree.activeMemtable.size

# Get block cache statistics
proc blockCacheStats*(tree: LsmTree): tuple[hits, misses: uint64, hitRate: float,
                                            size: uint64, count: int] =
  ## Get statistics from the block cache.
  ## Returns (hits, misses, hitRate, size, count).
  if tree.blockCache != nil:
    return tree.blockCache.stats()
  return (0'u64, 0'u64, 0.0, 0'u64, 0)

# Clear block cache
proc clearBlockCache*(tree: LsmTree) =
  ## Clear all entries from the block cache.
  if tree.blockCache != nil:
    tree.blockCache.clear()

# Get comprehensive LSM tree metrics
proc getMetrics*(tree: LsmTree): LsmTreeMetrics =
  ## Returns comprehensive metrics for this LSM tree.
  ##
  ## Includes:
  ## - Per-level table counts and sizes
  ## - Memtable statistics
  ## - Cache hit rates
  ## - Read/write counters
  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  result = LsmTreeMetrics()
  result.levelCount = tree.config.levelCount

  # Level metrics
  result.levelMetrics = newSeq[LevelMetrics](tree.config.levelCount)
  for level in 0 ..< tree.config.levelCount:
    var levelMetrics = LevelMetrics()
    levelMetrics.tableCount = tree.tables[level].len
    for table in tree.tables[level]:
      levelMetrics.sizeBytes += table.size
    result.levelMetrics[level] = levelMetrics
    result.diskSpaceBytes += levelMetrics.sizeBytes

  # Memtable metrics
  result.activeMemtableSize = tree.activeMemtable.size
  result.sealedMemtableCount = tree.sealedMemtables.len
  for memtable in tree.sealedMemtables:
    result.sealedMemtableSize += memtable.size

  # Block cache metrics
  if tree.blockCache != nil:
    let cacheStats = tree.blockCache.stats()
    result.cacheHits = cacheStats.hits
    result.cacheMisses = cacheStats.misses
    result.blockCacheMemoryBytes = cacheStats.size

  # Counter metrics
  result.reads = tree.counters.getReads()
  result.writes = tree.counters.getWrites()
  result.writeBytes = tree.counters.getWriteBytes()
  result.batches = tree.counters.getBatches()


