# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Ingestion API for Bulk Loading
##
## Provides fast bulk loading of sorted key-value pairs by writing
## directly to SSTables, bypassing the journal and memtable.
##
## Usage:
##   var ingestion = newIngestion(keyspace)?
##   for kv in sortedData:
##     ingestion.write(kv.key, kv.value)?
##   ingestion.finish()
##
## Requirements for optimal performance:
##   - Keys should be in ascending order (or will be sorted automatically)
##   - Tree should be empty or have minimal existing data
##
## Use cases:
##   - Restoring from backup
##   - Migrating from a different database
##   - Schema migrations

import fractio/storage/[error, types, keyspace]
import fractio/storage/types as storage_types
import fractio/storage/snapshot_tracker
import fractio/storage/lsm_tree/[types as lsm_types, lsm_tree, sstable/writer]
import fractio/storage/lsm_tree/compression as lsm_compression
import std/[os, algorithm, sequtils, locks, atomics]

const
  DefaultTableSize* = 64 * 1024 * 1024  ## 64 MiB target SSTable size
  MaxEntriesPerIngestion* = 100_000_000 ## Maximum entries per ingestion

type
  IngestionEntry* = object
    ## A single entry in the ingestion buffer
    key*: string
    value*: string
    valueType*: ValueType
    seqno*: uint64

  IngestionInner* = ref object
    ## Internal state for ingestion
    keyspace: Keyspace
    entries: seq[IngestionEntry]
    currentSize: uint64  ## Approximate size of buffered entries
    maxTableSize: uint64 ## Target SSTable size
    nextSeqno: uint64    ## Next sequence number to use
    isFinished: bool     ## True if finish() was called
    strictOrder: bool    ## If true, require strict ascending key order
    lastKey: string      ## Last key added (for order validation)

  Ingestion* = object
    ## Handle for bulk loading data into a keyspace
    inner*: IngestionInner

  AnyIngestion* = object
    ## Placeholder for generic ingestion over different tree types

# Constructor
proc newIngestion*(keyspace: Keyspace,
                   maxTableSize: uint64 = DefaultTableSize,
                   strictOrder: bool = false): StorageResult[Ingestion] =
  ## Creates a new ingestion handle for bulk loading.
  ##
  ## Parameters:
  ##   keyspace: The target keyspace
  ##   maxTableSize: Target SSTable size (default 64 MiB)
  ##   strictOrder: If true, validate that keys are in ascending order
  ##
  ## Returns an ingestion handle that can be used to write entries.

  if keyspace.inner.isNil:
    return err[Ingestion, StorageError](StorageError(
      kind: seInvalidArgument,
      message: "Keyspace is nil"
    ))

  let seqno = keyspace.inner.tree.seqnoCounter.next()

  let inner = IngestionInner(
    keyspace: keyspace,
    entries: @[],
    currentSize: 0,
    maxTableSize: maxTableSize,
    nextSeqno: seqno,
    isFinished: false,
    strictOrder: strictOrder,
    lastKey: ""
  )

  return ok[Ingestion, StorageError](Ingestion(inner: inner))

# Write key-value pair
proc write*(ingestion: var Ingestion, key: UserKey,
    value: UserValue): StorageResult[void] =
  ## Writes a key-value pair to the ingestion buffer.
  ##
  ## For best performance, keys should be in ascending order.
  ## If strictOrder was enabled, out-of-order keys will cause an error.

  if ingestion.inner.isNil:
    return errVoid(StorageError(
      kind: seInvalidArgument,
      message: "Ingestion is nil"
    ))

  if ingestion.inner.isFinished:
    return errVoid(StorageError(
      kind: seInvalidState,
      message: "Ingestion already finished"
    ))

  if key.len > 65536:
    return errVoid(StorageError(
      kind: seInvalidArgument,
      message: "Key too large (max 65536 bytes)"
    ))

  # Validate order if strict mode
  if ingestion.inner.strictOrder and ingestion.inner.lastKey.len > 0:
    if key <= ingestion.inner.lastKey:
      return errVoid(StorageError(
        kind: seInvalidArgument,
        message: "Keys must be in strictly ascending order"
      ))

  let seqno = ingestion.inner.nextSeqno
  inc ingestion.inner.nextSeqno

  let entry = IngestionEntry(
    key: key,
    value: value,
    valueType: vtValue,
    seqno: seqno
  )

  ingestion.inner.entries.add(entry)
  ingestion.inner.currentSize += uint64(key.len + value.len)
  ingestion.inner.lastKey = key

  return okVoid

# Write tombstone
proc writeTombstone*(ingestion: var Ingestion, key: UserKey): StorageResult[void] =
  ## Writes a tombstone (deletion marker) to the ingestion buffer.

  if ingestion.inner.isNil:
    return errVoid(StorageError(
      kind: seInvalidArgument,
      message: "Ingestion is nil"
    ))

  if ingestion.inner.isFinished:
    return errVoid(StorageError(
      kind: seInvalidState,
      message: "Ingestion already finished"
    ))

  let seqno = ingestion.inner.nextSeqno
  inc ingestion.inner.nextSeqno

  let entry = IngestionEntry(
    key: key,
    value: "",
    valueType: vtTombstone,
    seqno: seqno
  )

  ingestion.inner.entries.add(entry)
  ingestion.inner.currentSize += uint64(key.len)
  ingestion.inner.lastKey = key

  return okVoid

# Write weak tombstone
proc writeWeakTombstone*(ingestion: var Ingestion, key: UserKey): StorageResult[void] =
  ## Writes a weak tombstone to the ingestion buffer.
  ## Weak tombstones only delete if the key exists.

  if ingestion.inner.isNil:
    return errVoid(StorageError(
      kind: seInvalidArgument,
      message: "Ingestion is nil"
    ))

  if ingestion.inner.isFinished:
    return errVoid(StorageError(
      kind: seInvalidState,
      message: "Ingestion already finished"
    ))

  let seqno = ingestion.inner.nextSeqno
  inc ingestion.inner.nextSeqno

  let entry = IngestionEntry(
    key: key,
    value: "",
    valueType: vtWeakTombstone,
    seqno: seqno
  )

  ingestion.inner.entries.add(entry)
  ingestion.inner.currentSize += uint64(key.len)
  ingestion.inner.lastKey = key

  return okVoid

# Sort entries by key (and seqno descending for same keys)
proc cmpEntries(a, b: IngestionEntry): int =
  result = cmp(a.key, b.key)
  if result == 0:
    # Same key: higher seqno first (newer first)
    result = cmp(b.seqno, a.seqno)

# Write a batch of entries to an SSTable
proc writeSsTable(tree: lsm_types.LsmTree,
                  entries: openArray[IngestionEntry],
                  level: int): StorageResult[lsm_types.SsTable] =
  ## Writes entries to a new SSTable file.

  let levelPath = if level == 0: tree.config.path / "L0"
                  else: tree.config.path / ("L" & $level)

  if not dirExists(levelPath):
    createDir(levelPath)

  let tableId = tree.tableIdCounter.load(moRelaxed) + 1
  let path = levelPath / ($tableId & ".sst")

  # Convert compression type from storage to lsm_tree compression
  let storageCompression = tree.config.getCompressionType(level)
  let compression = case storageCompression
    of storage_types.ctNone: lsm_compression.ctNone
    of storage_types.ctLz4: lsm_compression.ctLz4
    of storage_types.ctSnappy: lsm_compression.ctZlib # Fallback to zlib

  let writerResult = newSsTableWriter(path, entries.len, compression)
  if writerResult.isErr:
    return err[lsm_types.SsTable, StorageError](writerResult.error)

  let writer = writerResult.value

  for entry in entries:
    let addResult = writer.add(entry.key, entry.value, entry.seqno,
        entry.valueType)
    if addResult.isErr:
      return err[lsm_types.SsTable, StorageError](addResult.error)

  return writer.finish()

# Finish ingestion
proc finish*(ingestion: var Ingestion): StorageResult[uint64] =
  ## Finishes the ingestion by writing all entries to SSTables
  ## and registering them atomically into the tree.
  ##
  ## Returns the number of entries ingested.

  if ingestion.inner.isNil:
    return err[uint64, StorageError](StorageError(
      kind: seInvalidArgument,
      message: "Ingestion is nil"
    ))

  if ingestion.inner.isFinished:
    return err[uint64, StorageError](StorageError(
      kind: seInvalidState,
      message: "Ingestion already finished"
    ))

  let tree = ingestion.inner.keyspace.inner.tree
  let totalEntries = uint64(ingestion.inner.entries.len)

  if totalEntries == 0:
    ingestion.inner.isFinished = true
    return ok[uint64, StorageError](0'u64)

  # Sort entries if not already sorted
  # (Always sort to ensure correctness; caller can optimize by pre-sorting)
  sort(ingestion.inner.entries, cmpEntries)

  # Acquire tree lock for atomic registration
  tree.versionLock.acquire()
  defer: tree.versionLock.release()

  # Split entries into SSTables based on size
  var currentBatch: seq[IngestionEntry] = @[]
  var currentBatchSize: uint64 = 0
  var tablesCreated: seq[lsm_types.SsTable] = @[]
  var highestSeqno: uint64 = 0

  for entry in ingestion.inner.entries:
    let entrySize = uint64(entry.key.len + entry.value.len)

    # Check if we need to flush current batch
    if currentBatchSize + entrySize > ingestion.inner.maxTableSize and
        currentBatch.len > 0:
      # Write current batch to SSTable
      let tableResult = writeSsTable(tree, currentBatch, 0)
      if tableResult.isErr:
        return err[uint64, StorageError](tableResult.error)

      var sstable = tableResult.value
      sstable.id = tree.tableIdCounter.fetchAdd(1, moRelaxed) + 1
      tablesCreated.add(sstable)

      # Update highest seqno
      for e in currentBatch:
        if e.seqno > highestSeqno:
          highestSeqno = e.seqno

      currentBatch = @[]
      currentBatchSize = 0

    currentBatch.add(entry)
    currentBatchSize += entrySize

  # Write remaining entries
  if currentBatch.len > 0:
    let tableResult = writeSsTable(tree, currentBatch, 0)
    if tableResult.isErr:
      return err[uint64, StorageError](tableResult.error)

    var sstable = tableResult.value
    sstable.id = tree.tableIdCounter.fetchAdd(1, moRelaxed) + 1
    tablesCreated.add(sstable)

    # Update highest seqno
    for e in currentBatch:
      if e.seqno > highestSeqno:
        highestSeqno = e.seqno

  # Register all tables atomically
  for sstable in tablesCreated:
    tree.tables[0].add(sstable)

  # Update seqno counter
  if highestSeqno > 0:
    tree.seqnoCounter.fetchMax(highestSeqno + 1)

  ingestion.inner.isFinished = true
  ingestion.inner.entries = @[] # Free memory

  return ok[uint64, StorageError](totalEntries)

# Get entry count
proc len*(ingestion: Ingestion): int =
  ## Returns the number of entries buffered in the ingestion.
  if ingestion.inner.isNil:
    return 0
  return ingestion.inner.entries.len

# Get approximate size
proc size*(ingestion: Ingestion): uint64 =
  ## Returns the approximate size of buffered data in bytes.
  if ingestion.inner.isNil:
    return 0
  return ingestion.inner.currentSize

# Check if finished
proc isFinished*(ingestion: Ingestion): bool =
  ## Returns true if finish() has been called.
  if ingestion.inner.isNil:
    return true
  return ingestion.inner.isFinished
