# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Lazy Iterator Implementation
##
## Provides memory-efficient iterators that stream data from SSTables
## instead of loading everything into memory.

import ./types
import ./memtable
import ./sstable/reader
import ./sstable/types
import fractio/storage/error
import fractio/storage/snapshot_nonce
import std/[options, algorithm, streams, os, heapqueue, strutils]

# ============================================================================
# Iterator Entry
# ============================================================================

type
  LazyIterEntry* = object
    ## Single entry returned by lazy iterator
    key*: string
    value*: string
    seqno*: uint64
    valueType*: ValueType

# ============================================================================
# Memtable Iterator
# ============================================================================

type
  MemtableIter* = ref object
    ## Iterator over memtable entries (in-memory)
    entries*: seq[(string, MemtableEntry)] # (key, entry) pairs
    currentIndex*: int

proc newMemtableIter*(memtable: Memtable): MemtableIter =
  ## Create iterator over memtable
  result = MemtableIter(
    entries: @[],
    currentIndex: 0
  )
  # Get sorted entries from memtable
  let sortedEntries = memtable.getSortedEntries()
  for e in sortedEntries:
    result.entries.add((e.key, e))
  if result.entries.len > 0:
    result.currentIndex = 0

proc isValid*(iter: MemtableIter): bool =
  iter.currentIndex >= 0 and iter.currentIndex < iter.entries.len

proc current*(iter: MemtableIter): Option[LazyIterEntry] =
  if iter.isValid:
    let (key, entry) = iter.entries[iter.currentIndex]
    return some(LazyIterEntry(
      key: key,
      value: entry.value,
      seqno: entry.seqno,
      valueType: entry.valueType
    ))
  return none(LazyIterEntry)

proc next*(iter: MemtableIter): bool =
  ## Advance to next entry. Returns false if exhausted.
  if iter.currentIndex < iter.entries.len - 1:
    iter.currentIndex += 1
    return true
  # Exhausted - set to invalid state
  iter.currentIndex = -1
  return false

proc key*(iter: MemtableIter): string =
  if iter.isValid:
    return iter.entries[iter.currentIndex][0]
  return ""

# ============================================================================
# SSTable Block Iterator (Lazy)
# ============================================================================

type
  SsTableBlockIter* = ref object
    ## Iterator over a single SSTable, loading blocks lazily
    reader*: SsTableReader
    currentBlockIdx*: int
    currentBlockEntries*: seq[BlockEntry]
    currentEntryIdx*: int
    path*: string
    isValid*: bool

proc newSsTableIter*(path: string): StorageResult[SsTableBlockIter] =
  ## Create lazy iterator over SSTable
  let readerResult = openSsTable(path)
  if readerResult.isErr:
    return err[SsTableBlockIter, StorageError](readerResult.error)

  let reader = readerResult.value
  var iter = SsTableBlockIter(
    reader: reader,
    currentBlockIdx: -1,
    currentBlockEntries: @[],
    currentEntryIdx: 0,
    path: path,
    isValid: false
  )

  # Load first block
  if iter.reader.indexBlock.entries.len > 0:
    iter.currentBlockIdx = 0
    let blockResult = readDataBlock(iter.reader.stream,
                                     iter.reader.indexBlock.entries[0].handle)
    if blockResult.isOk:
      iter.currentBlockEntries = blockResult.value.entries
      iter.currentEntryIdx = 0
      iter.isValid = iter.currentBlockEntries.len > 0

  return ok[SsTableBlockIter, StorageError](iter)

proc loadNextBlock*(iter: SsTableBlockIter): bool =
  ## Load the next block from disk
  iter.currentBlockIdx += 1
  if iter.currentBlockIdx >= iter.reader.indexBlock.entries.len:
    iter.isValid = false
    return false

  let blockResult = readDataBlock(iter.reader.stream,
                                   iter.reader.indexBlock.entries[
                                       iter.currentBlockIdx].handle)
  if blockResult.isErr:
    iter.isValid = false
    return false

  iter.currentBlockEntries = blockResult.value.entries
  iter.currentEntryIdx = 0
  iter.isValid = iter.currentBlockEntries.len > 0
  return iter.isValid

proc current*(iter: SsTableBlockIter): Option[LazyIterEntry] =
  if iter.isValid and iter.currentEntryIdx < iter.currentBlockEntries.len:
    let entry = iter.currentBlockEntries[iter.currentEntryIdx]
    return some(LazyIterEntry(
      key: entry.key,
      value: entry.value,
      seqno: entry.seqno,
      valueType: ValueType(entry.valueType)
    ))
  return none(LazyIterEntry)

proc next*(iter: SsTableBlockIter): bool =
  ## Move to next entry, loading next block if needed
  if not iter.isValid:
    return false

  iter.currentEntryIdx += 1
  if iter.currentEntryIdx >= iter.currentBlockEntries.len:
    # Need to load next block
    return iter.loadNextBlock()

  return true

proc key*(iter: SsTableBlockIter): string =
  if iter.isValid and iter.currentEntryIdx < iter.currentBlockEntries.len:
    return iter.currentBlockEntries[iter.currentEntryIdx].key
  return ""

proc close*(iter: SsTableBlockIter) =
  if iter.reader != nil:
    iter.reader.close()

# ============================================================================
# Merge Iterator (combines multiple sources)
# ============================================================================

type
  IterSourceKind* = enum
    iskMemtable
    iskSsTable

  IterSource* = object
    ## Reference to an iterator source
    case kind*: IterSourceKind
    of iskMemtable:
      memIter*: MemtableIter
    of iskSsTable:
      ssIter*: SsTableBlockIter

  MergeHeapEntry* = object
    ## Entry in merge heap
    entry*: LazyIterEntry
    sourceIdx*: int

proc `<`(a, b: MergeHeapEntry): bool =
  ## Compare for heap (min-heap by key, then max seqno for same key)
  if a.entry.key != b.entry.key:
    return a.entry.key < b.entry.key
  return a.entry.seqno > b.entry.seqno # Higher seqno = newer

type
  MergeIterator* = ref object
    ## Merges multiple sorted iterators into one
    sources*: seq[IterSource]
    heap*: HeapQueue[MergeHeapEntry]
    lastKey*: string
    initialized*: bool
    exhausted*: bool
    snapshotSeqno*: uint64 # Only return entries with seqno <= this

proc initHeap*(iter: MergeIterator) =
  ## Initialize heap with first entry from each source
  iter.heap.clear()
  for i, src in iter.sources:
    var entry: Option[LazyIterEntry] = none(LazyIterEntry)
    case src.kind:
    of iskMemtable:
      entry = src.memIter.current()
    of iskSsTable:
      entry = src.ssIter.current()

    if entry.isSome and entry.get.seqno <= iter.snapshotSeqno:
      iter.heap.push(MergeHeapEntry(entry: entry.get, sourceIdx: i))

proc newMergeIterator*(snapshotSeqno: uint64): MergeIterator =
  result = MergeIterator(
    sources: @[],
    heap: initHeapQueue[MergeHeapEntry](),
    lastKey: "",
    initialized: false,
    exhausted: false,
    snapshotSeqno: snapshotSeqno
  )

proc addMemtable*(iter: MergeIterator, memtable: Memtable) =
  ## Add memtable to merge iterator
  let memIter = newMemtableIter(memtable)
  iter.sources.add(IterSource(kind: iskMemtable, memIter: memIter))

proc addSsTable*(iter: MergeIterator, path: string): StorageResult[void] =
  ## Add SSTable to merge iterator
  let ssIterResult = newSsTableIter(path)
  if ssIterResult.isErr:
    return err[void, StorageError](ssIterResult.error)

  iter.sources.add(IterSource(kind: iskSsTable, ssIter: ssIterResult.value))
  return okVoid

proc isValid*(iter: MergeIterator): bool =
  not iter.exhausted and iter.heap.len > 0

proc current*(iter: MergeIterator): Option[LazyIterEntry] =
  if iter.isValid:
    return some(iter.heap[0].entry)
  return none(LazyIterEntry)

proc advanceSource*(iter: MergeIterator, sourceIdx: int) =
  ## Advance a source and add its next entry to heap
  if sourceIdx >= iter.sources.len:
    return

  let src = iter.sources[sourceIdx]
  var entry: Option[LazyIterEntry] = none(LazyIterEntry)

  case src.kind:
  of iskMemtable:
    if src.memIter.next():
      entry = src.memIter.current()
  of iskSsTable:
    if src.ssIter.next():
      entry = src.ssIter.current()

  if entry.isSome and entry.get.seqno <= iter.snapshotSeqno:
    iter.heap.push(MergeHeapEntry(entry: entry.get, sourceIdx: sourceIdx))

proc next*(iter: MergeIterator): bool =
  ## Move to next unique key
  ## Returns true if there's an entry available, false if exhausted

  # First call - initialize heap
  if not iter.initialized:
    iter.initHeap()
    iter.initialized = true
    if iter.heap.len == 0:
      iter.exhausted = true
      return false
    iter.lastKey = iter.heap[0].entry.key
    return true

  # Already exhausted
  if iter.exhausted:
    return false

  # Keep advancing until we find a new key or exhaust
  while iter.heap.len > 0:
    let top = iter.heap.pop()

    # Advance the source we just consumed
    iter.advanceSource(top.sourceIdx)

    # Skip entries with same key (already returned the newest)
    if iter.heap.len > 0 and iter.heap[0].entry.key == iter.lastKey:
      continue

    # Found a new key
    if iter.heap.len > 0:
      iter.lastKey = iter.heap[0].entry.key
      return true
    else:
      iter.exhausted = true
      return false

  iter.exhausted = true
  return false

proc close*(iter: MergeIterator) =
  ## Clean up resources
  for src in iter.sources:
    if src.kind == iskSsTable and src.ssIter != nil:
      src.ssIter.close()

# ============================================================================
# Range Iterator
# ============================================================================

type
  RangeIterator* = ref object
    ## Iterator over a range of keys
    mergeIter*: MergeIterator
    startKey*: Option[string]
    endKey*: Option[string]
    isValid*: bool

proc newRangeIterator*(mergeIter: MergeIterator,
                       startKey: Option[string] = none(string),
                       endKey: Option[string] = none(string)): RangeIterator =
  result = RangeIterator(
    mergeIter: mergeIter,
    startKey: startKey,
    endKey: endKey,
    isValid: false
  )

  # Initialize and seek to start key
  mergeIter.initHeap()
  mergeIter.initialized = true

  # Skip to start key if specified
  if startKey.isSome:
    while mergeIter.isValid and mergeIter.current().get.key < startKey.get:
      if not mergeIter.next():
        break

  # Check if we're in range
  if mergeIter.isValid:
    let current = mergeIter.current()
    if current.isSome:
      let key = current.get.key
      let inRange = (startKey.isNone or key >= startKey.get) and
                    (endKey.isNone or key <= endKey.get)
      result.isValid = inRange
  else:
    result.isValid = false

proc current*(iter: RangeIterator): Option[LazyIterEntry] =
  if iter.isValid:
    return iter.mergeIter.current()
  return none(LazyIterEntry)

proc next*(iter: RangeIterator): bool =
  if not iter.isValid:
    return false

  if not iter.mergeIter.next():
    iter.isValid = false
    return false

  # Check if still in range
  let current = iter.mergeIter.current()
  if current.isSome:
    let key = current.get.key
    if iter.endKey.isSome and key > iter.endKey.get:
      iter.isValid = false
      return false
    return true

  iter.isValid = false
  return false

proc close*(iter: RangeIterator) =
  if iter.mergeIter != nil:
    iter.mergeIter.close()

# ============================================================================
# Prefix Iterator
# ============================================================================

type
  PrefixIterator* = ref object
    ## Iterator over keys with a specific prefix
    mergeIter*: MergeIterator
    prefix*: string
    isValid*: bool

proc newPrefixIterator*(mergeIter: MergeIterator,
    prefix: string): PrefixIterator =
  result = PrefixIterator(
    mergeIter: mergeIter,
    prefix: prefix,
    isValid: false
  )

  # Initialize and seek to prefix
  mergeIter.initHeap()
  mergeIter.initialized = true

  # Skip to first key with prefix
  while mergeIter.isValid:
    let current = mergeIter.current()
    if current.isSome:
      let key = current.get.key
      if key.startsWith(prefix):
        result.isValid = true
        break
      elif key > prefix and not key.startsWith(prefix):
        # Past the prefix range
        break
    if not mergeIter.next():
      break

proc current*(iter: PrefixIterator): Option[LazyIterEntry] =
  if iter.isValid:
    return iter.mergeIter.current()
  return none(LazyIterEntry)

proc next*(iter: PrefixIterator): bool =
  if not iter.isValid:
    return false

  if not iter.mergeIter.next():
    iter.isValid = false
    return false

  # Check if still has prefix
  let current = iter.mergeIter.current()
  if current.isSome:
    if current.get.key.startsWith(iter.prefix):
      return true

  iter.isValid = false
  return false

proc close*(iter: PrefixIterator) =
  if iter.mergeIter != nil:
    iter.mergeIter.close()
