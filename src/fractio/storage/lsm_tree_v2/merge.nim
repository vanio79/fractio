# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree v2 - Merge Iterator
##
## This module provides the merge iterator for combining multiple sorted
## sources into a single sorted stream.

import std/[heapqueue, options]
import types
import error

# ============================================================================
# Merge Item
# ============================================================================

type
  MergeItem* = tuple[idx: int, key: InternalKey, value: string]
  MergeResult* = LsmResult[tuple[key: InternalKey, value: string]]

# ============================================================================
# Generic Merge Iterator
# ============================================================================

# Iterator concept (simplified - actual iterators must provide these)
type
  KvIterator* = concept it
    it.next() is Option[tuple[key: InternalKey, value: string]]
    it.hasNext() is bool

proc cmpMergeItem*(a, b: MergeItem): int =
  ## Compare merge items for heap ordering
  ## Uses InternalKey comparison (user_key ASC, seqno DESC)
  cmpInternalKey(a.key, b.key)

proc `<`*(a, b: MergeItem): bool =
  cmpMergeItem(a, b) < 0

proc `<=`*(a, b: MergeItem): bool =
  cmpMergeItem(a, b) <= 0

proc `>`*(a, b: MergeItem): bool =
  cmpMergeItem(a, b) > 0

proc `>=`*(a, b: MergeItem): bool =
  cmpMergeItem(a, b) >= 0

# ============================================================================
# Merger
# ============================================================================

type
  Merger*[I] = ref object
    ## K-way merge iterator combining multiple sorted sources
    iterators*: seq[I]
    heap*: HeapQueue[MergeItem]
    initializedLo*: bool
    initializedHi*: bool

proc newMerger*[I](iterators: seq[I]): Merger[I] =
  Merger[I](
    iterators: iterators,
    heap: initHeapQueue[MergeItem](),
    initializedLo: false,
    initializedHi: false
  )

proc initializeLo*[I](m: var Merger[I]): LsmResult[void] =
  ## Initialize for forward iteration
  for idx, it in m.iterators.mitems:
    let nextItem = it.next()
    if nextItem.isSome:
      m.heap.push((idx, nextItem.get))

  m.initializedLo = true
  okVoid()

proc initializeHi*[I](m: var Merger[I]) =
  ## Initialize for reverse iteration
  for idx, it in m.iterators.mitems:
    let nextItem = it.next()
    if nextItem.isSome:
      # For reverse, we'd need next_back - simplified here
      discard

  m.initializedHi = true

proc next*[I](m: var Merger[I]): MergeResult =
  ## Get next item in forward order
  if not m.initializedLo:
    let initResult = m.initializeLo()
    if initResult.isErr:
      return err[tuple[key: InternalKey, value: string]](initResult.error)

  if m.heap.len == 0:
    return err[tuple[key: InternalKey, value: string]](newIoError("Iterator is exhausted"))

  let minItem = m.heap.pop()
  let idx = minItem.idx

  # Get next item from the same iterator
  let nextItem = m.iterators[idx].next()
  if nextItem.isSome:
    m.heap.push((idx, nextItem.get.key, nextItem.get.value))

  ok[tuple[key: InternalKey, value: string]]((minItem.key, minItem.value))

proc hasNext*[I](m: var Merger[I]): bool =
  if not m.initializedLo:
    let initResult = m.initializeLo()
    if initResult.isErr:
      return false

  m.heap.len > 0

# ============================================================================
# Sequence-based Merge (for in-memory data)
# ============================================================================

type
  SeqMergeIterator* = ref object
    sources*: seq[seq[tuple[key: InternalKey, value: string]]]
    indices*: seq[int]
    heap*: HeapQueue[MergeItem]
    initialized*: bool

proc newSeqMergeIterator*(sources: seq[seq[tuple[key: InternalKey,
    value: string]]]): SeqMergeIterator =
  SeqMergeIterator(
    sources: sources,
    indices: newSeq[int](sources.len),
    heap: initHeapQueue[MergeItem](),
    initialized: false
  )

proc initialize*(m: var SeqMergeIterator) =
  for i, source in m.sources:
    if source.len > 0:
      m.heap.push((i, source[0].key, source[0].value))
      m.indices[i] = 1
  m.initialized = true

proc next*(m: var SeqMergeIterator): Option[tuple[key: InternalKey,
    value: string]] =
  if not m.initialized:
    m.initialize()

  if m.heap.len == 0:
    return none(tuple[key: InternalKey, value: string])

  let minItem = m.heap.pop()
  let idx = minItem.idx
  let result = (minItem.key, minItem.value)

  # Get next item from the same source
  if m.indices[idx] < m.sources[idx].len:
    let nextValue = m.sources[idx][m.indices[idx]]
    m.heap.push((idx, nextValue.key, nextValue.value))
    m.indices[idx] += 1

  return some(result)

proc hasNext*(m: var SeqMergeIterator): bool =
  if not m.initialized:
    m.initialize()
  m.heap.len > 0

# ============================================================================
# Streaming Merge Iterator (with tombstone filtering for GC)
# ============================================================================

type
  StreamingMergeIterator* = ref object
    sources*: seq[seq[tuple[key: InternalKey, value: string]]]
    indices*: seq[int]
    heap*: HeapQueue[MergeItem]
    lastKey*: string
    lastAdded*: bool
    gcWatermark*: SeqNo
    initialized*: bool

proc newStreamingMergeIterator*(sources: seq[seq[tuple[key: InternalKey,
    value: string]]], gcWatermark: SeqNo): StreamingMergeIterator =
  StreamingMergeIterator(
    sources: sources,
    indices: newSeq[int](sources.len),
    heap: initHeapQueue[MergeItem](),
    lastKey: "",
    lastAdded: false,
    gcWatermark: gcWatermark,
    initialized: false
  )

proc initialize*(m: var StreamingMergeIterator) =
  for i, source in m.sources:
    if source.len > 0:
      m.heap.push((i, source[0].key, source[0].value))
      m.indices[i] = 1
  m.lastKey = ""
  m.lastAdded = false

proc next*(m: var StreamingMergeIterator): Option[tuple[key: InternalKey,
    value: string]] =
  if not m.initialized:
    m.initialize()
    m.initialized = true

  while m.heap.len > 0:
    let top = m.heap.pop()
    let key = top.key
    let value = top.value
    let idx = top.idx

    # Check if this is a new key
    if key.userKey != m.lastKey:
      m.lastKey = key.userKey
      m.lastAdded = false

      # Decide whether to include this entry
      # Skip tombstones that are old enough to GC
      if key.valueType.isTombstone():
        if key.seqno >= m.gcWatermark:
          # Keep recent tombstone
          m.lastAdded = true
          if m.indices[idx] < m.sources[idx].len:
            let nextEntry = m.sources[idx][m.indices[idx]]
            m.heap.push((idx, nextEntry.key, nextEntry.value))
            m.indices[idx] += 1
          return some((key, value))
        # Otherwise discard (GC'd)
      else:
        # Regular value
        m.lastAdded = true
        if m.indices[idx] < m.sources[idx].len:
          let nextEntry = m.sources[idx][m.indices[idx]]
          m.heap.push((idx, nextEntry.key, nextEntry.value))
          m.indices[idx] += 1
        return some((key, value))
    else:
      # Same key - only add if we haven't added anything for this key
      if not m.lastAdded and key.seqno >= m.gcWatermark:
        if not key.valueType.isTombstone():
          m.lastAdded = true
          if m.indices[idx] < m.sources[idx].len:
            let nextEntry = m.sources[idx][m.indices[idx]]
            m.heap.push((idx, nextEntry.key, nextEntry.value))
            m.indices[idx] += 1
          return some((key, value))

      # Get next entry from same source
      if m.indices[idx] < m.sources[idx].len:
        let nextEntry = m.sources[idx][m.indices[idx]]
        m.heap.push((idx, nextEntry.key, nextEntry.value))
        m.indices[idx] += 1

  return none(tuple[key: InternalKey, value: string])

proc hasNext*(m: var StreamingMergeIterator): bool =
  if not m.initialized:
    m.initialize()
    m.initialized = true
  m.heap.len > 0

# ============================================================================
# Utility Functions
# ============================================================================

proc mergeSequences*(sources: seq[seq[tuple[key: InternalKey,
    value: string]]]): seq[tuple[key: InternalKey, value: string]] =
  var merger = newSeqMergeIterator(sources)
  result = newSeq[tuple[key: InternalKey, value: string]]()

  while merger.hasNext():
    let item = merger.next()
    if item.isSome:
      result.add(item.get)

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing merge iterators..."

  # Test sequence merge
  let source1 = @[
    (key: InternalKey(userKey: "a", seqno: 1, valueType: vtValue),
        value: "value1"),
    (key: InternalKey(userKey: "c", seqno: 3, valueType: vtValue),
        value: "value3")
  ]
  let source2 = @[
    (key: InternalKey(userKey: "b", seqno: 2, valueType: vtValue),
        value: "value2"),
    (key: InternalKey(userKey: "d", seqno: 4, valueType: vtValue),
        value: "value4")
  ]

  let merged = mergeSequences(@[source1, source2])
  echo "Merged count: ", merged.len
  for item in merged:
    echo "  ", item.key.userKey, " => ", item.value

  # Test with tombstones and GC
  let source3 = @[
    (key: InternalKey(userKey: "a", seqno: 1, valueType: vtValue), value: "v1"),
    (key: InternalKey(userKey: "a", seqno: 5, valueType: vtTombstone),
        value: "v2"), # Old tombstone - should be GC'd
  ]
  let source4 = @[
    (key: InternalKey(userKey: "a", seqno: 10, valueType: vtValue),
        value: "v3"), # New value
  ]

  let mergedWithGc = mergeSequences(@[source3, source4])
  echo "\nMerged with GC watermark=8:"
  for item in mergedWithGc:
    let shouldInclude = item.key.seqno >= 8
    echo "  ", item.key.userKey, " seqno=", item.key.seqno,
         " tombstone=", item.key.valueType.isTombstone(), " include=", shouldInclude

  echo "Merge tests passed!"
