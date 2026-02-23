# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Compaction Helper
##
## Provides utilities for merging SSTables during compaction.

import fractio/storage/lsm_tree/types
import fractio/storage/lsm_tree/sstable/reader
import fractio/storage/lsm_tree/sstable/writer
import fractio/storage/lsm_tree/sstable/types
import fractio/storage/error
import std/[os, heapqueue]

# Default target SSTable size for compaction
const DEFAULT_TARGET_TABLE_SIZE* = 64 * 1024 * 1024     # 64 MiB

# Entry with source tracking for merging
type
  MergeEntry* = object
    key*: string
    value*: string
    seqno*: uint64
    valueType*: ValueType
    sourceIdx*: int # Which SSTable this came from

# Heap entry for k-way merge
type
  HeapMergeEntry* = object
    entry*: MergeEntry
    readerIdx*: int

# Compare for heap (min-heap by key, then max seqno for same key)
proc `<`(a, b: HeapMergeEntry): bool =
  if a.entry.key != b.entry.key:
    return a.entry.key < b.entry.key
  # For same key, higher seqno = newer = should come first
  return a.entry.seqno > b.entry.seqno

# Read all entries from a single SSTable
proc readSsTableEntries*(path: string): StorageResult[seq[MergeEntry]] =
  let readerResult = openSsTable(path)
  if readerResult.isErr:
    return err[seq[MergeEntry], StorageError](readerResult.error)

  let reader = readerResult.value
  var entries: seq[MergeEntry] = @[]

  # Handle both partitioned and full index modes
  if reader.indexMode == imPartitioned:
    # Partitioned index: iterate through top level index and load each index block
    for tliEntry in reader.topLevelIndex.entries:
      let idxBlockResult = readIndexBlock(reader.stream, tliEntry.handle)
      if idxBlockResult.isErr:
        reader.close()
        return err[seq[MergeEntry], StorageError](idxBlockResult.error)

      let idxBlock = idxBlockResult.value
      for idxEntry in idxBlock.entries:
        let blockResult = readDataBlock(reader.stream, idxEntry.handle)
        if blockResult.isErr:
          reader.close()
          return err[seq[MergeEntry], StorageError](blockResult.error)

        let dataBlk = blockResult.value
        for blkEntry in dataBlk.entries:
          entries.add(MergeEntry(
            key: blkEntry.key,
            value: blkEntry.value,
            seqno: blkEntry.seqno,
            valueType: ValueType(blkEntry.valueType),
            sourceIdx: 0
          ))
  else:
    # Full index: use the single index block
    for idxEntry in reader.indexBlock.entries:
      let blockResult = readDataBlock(reader.stream, idxEntry.handle)
      if blockResult.isErr:
        reader.close()
        return err[seq[MergeEntry], StorageError](blockResult.error)

      let dataBlk = blockResult.value
      for blkEntry in dataBlk.entries:
        entries.add(MergeEntry(
          key: blkEntry.key,
          value: blkEntry.value,
          seqno: blkEntry.seqno,
          valueType: ValueType(blkEntry.valueType),
          sourceIdx: 0
        ))

  reader.close()
  return ok[seq[MergeEntry], StorageError](entries)

# Merge multiple sorted entry sequences into one
proc mergeEntries*(allEntries: seq[seq[MergeEntry]],
                   gcWatermark: uint64): StorageResult[seq[MergeEntry]] =
  ## Merge multiple sorted entry sequences, applying:
  ## 1. Keep only the newest version of each key
  ## 2. Remove tombstones older than gcWatermark

  var result: seq[MergeEntry] = @[]

  if allEntries.len == 0:
    return ok[seq[MergeEntry], StorageError](result)

  # Count total entries for progress logging
  var totalEntries = 0
  for entries in allEntries:
    totalEntries += entries.len

  # Create heap with first entry from each source
  var heap: HeapQueue[HeapMergeEntry] = initHeapQueue[HeapMergeEntry]()
  var indices = newSeq[int](allEntries.len)

  for i, entries in allEntries:
    if entries.len > 0:
      var he = HeapMergeEntry(
        entry: entries[0],
        readerIdx: i
      )
      he.entry.sourceIdx = i
      heap.push(he)
      indices[i] = 1

  var lastKey = ""
  var lastAdded = false
  var processedCount = 0

  while heap.len > 0:
    let top = heap.pop()
    let entry = top.entry
    let readerIdx = top.readerIdx
    inc(processedCount)

    # Check if this is a new key or same key with newer seqno
    if entry.key != lastKey:
      # Different key - output the previous key if it wasn't a deleted tombstone
      lastKey = entry.key
      lastAdded = false

      # Decide whether to include this entry
      # Skip tombstones that are old enough to GC
      if entry.valueType == vtTombstone or entry.valueType == vtWeakTombstone:
        # Only keep tombstone if it's recent enough (above watermark)
        if entry.seqno >= gcWatermark:
          result.add(entry)
          lastAdded = true
        # Otherwise, discard the tombstone (GC'd)
      else:
        result.add(entry)
        lastAdded = true
    else:
      # Same key - only add if we haven't added anything for this key
      # and the previous entry wasn't newer
      if not lastAdded and entry.seqno >= gcWatermark:
        if entry.valueType != vtTombstone and entry.valueType != vtWeakTombstone:
          result.add(entry)
          lastAdded = true

    # Push next entry from the same source
    let srcEntries = allEntries[readerIdx]
    if indices[readerIdx] < srcEntries.len:
      var he = HeapMergeEntry(
        entry: srcEntries[indices[readerIdx]],
        readerIdx: readerIdx
      )
      he.entry.sourceIdx = readerIdx
      heap.push(he)
      indices[readerIdx] += 1

  return ok[seq[MergeEntry], StorageError](result)

# Write merged entries to new SSTables
proc writeCompactedTables*(entries: seq[MergeEntry],
                           outputPath: string,
                           targetLevel: int,
                           targetSize: uint64,
                           tableIdCounter: var uint64): StorageResult[seq[SsTable]] =
  var tables: seq[SsTable] = @[]

  if entries.len == 0:
    return ok[seq[SsTable], StorageError](tables)

  # Create level directory if needed
  let levelPath = outputPath / ("L" & $targetLevel)
  if not dirExists(levelPath):
    try:
      createDir(levelPath)
    except OSError:
      return err[seq[SsTable], StorageError](StorageError(
        kind: seIo, ioError: "Failed to create level directory: " & levelPath))

  var currentWriter: SsTableWriter = nil
  var currentSize: uint64 = 0
  var currentEntries: seq[MergeEntry] = @[]

  for entry in entries:
    let entrySize = uint64(entry.key.len + entry.value.len)

    # Start new table if current one is full
    if currentWriter == nil or currentSize + entrySize > targetSize:
      # Flush current table first
      if currentWriter != nil and currentEntries.len > 0:
        for ce in currentEntries:
          let addResult = currentWriter.add(ce.key, ce.value, ce.seqno, ce.valueType)
          if addResult.isErr:
            return err[seq[SsTable], StorageError](addResult.error)

        let finishResult = currentWriter.finish()
        if finishResult.isErr:
          return err[seq[SsTable], StorageError](finishResult.error)

        var sstable = finishResult.value
        sstable.id = tableIdCounter
        sstable.level = targetLevel
        tables.add(sstable)

      # Start new table
      inc tableIdCounter
      let tablePath = levelPath / ($tableIdCounter & ".sst")
      let writerResult = newSsTableWriter(tablePath)
      if writerResult.isErr:
        return err[seq[SsTable], StorageError](writerResult.error)

      currentWriter = writerResult.value
      currentSize = 0
      currentEntries = @[]

    currentEntries.add(entry)
    currentSize += entrySize

  # Flush final table
  if currentWriter != nil and currentEntries.len > 0:
    for ce in currentEntries:
      let addResult = currentWriter.add(ce.key, ce.value, ce.seqno, ce.valueType)
      if addResult.isErr:
        return err[seq[SsTable], StorageError](addResult.error)

    let finishResult = currentWriter.finish()
    if finishResult.isErr:
      return err[seq[SsTable], StorageError](finishResult.error)

    var sstable = finishResult.value
    sstable.id = tableIdCounter
    sstable.level = targetLevel
    tables.add(sstable)

  return ok[seq[SsTable], StorageError](tables)

# Delete old SSTable files
proc deleteOldTables*(tables: seq[SsTable]): StorageResult[void] =
  for table in tables:
    if table.path.len > 0 and fileExists(table.path):
      try:
        removeFile(table.path)
      except OSError:
        return err[void, StorageError](StorageError(
          kind: seIo, ioError: "Failed to delete SSTable: " & table.path))
  return okVoid

# Check if two key ranges overlap
proc keyRangesOverlap*(a: (string, string), b: (string, string)): bool =
  if a[0].len == 0 or b[0].len == 0:
    return false
  # a.smallest <= b.largest AND a.largest >= b.smallest
  return a[0] <= b[1] and a[1] >= b[0]
