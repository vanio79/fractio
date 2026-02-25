# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree v2 - Memtable
##
## This module provides the Memtable implementation - an in-memory write buffer
## for the LSM tree using a skip list for efficient range iteration.

import std/[atomics, options]
import types
import skiplist

# ============================================================================
# Memtable Entry
# ============================================================================

type
  MemtableEntry* = object
    ## Single version of a key-value pair
    seqno*: SeqNo
    valueType*: ValueType
    value*: types.Slice

# ============================================================================
# Memtable - Uses skip list for O(log n) range iteration
# ============================================================================

type
  Memtable* = ref object
    ## In-memory write buffer for the LSM tree
    id*: MemtableId
    items*: SkipList[InternalKey, types.Slice]
    approximateSize*: Atomic[uint64]
    highestSeqno*: Atomic[SeqNo]
    requestedRotation*: Atomic[bool]

proc newMemtable*(id: MemtableId): Memtable =
  var approxSize: Atomic[uint64]
  var highSeqno: Atomic[SeqNo]
  var reqRotation: Atomic[bool]

  Memtable(
    id: id,
    items: newSkipList[InternalKey, types.Slice](),
    approximateSize: approxSize,
    highestSeqno: highSeqno,
    requestedRotation: reqRotation
  )

proc id*(m: Memtable): MemtableId {.inline.} =
  m.id

proc isFlaggedForRotation*(m: Memtable): bool {.inline.} =
  load(m.requestedRotation)

proc flagRotated*(m: Memtable) {.inline.} =
  store(m.requestedRotation, true)

proc len*(m: Memtable): int {.inline.} =
  m.items.len

proc isEmpty*(m: Memtable): bool {.inline.} =
  m.items.isEmpty

proc size*(m: Memtable): uint64 {.inline.} =
  load(m.approximateSize)

proc getHighestSeqno*(m: Memtable): Option[SeqNo] =
  if m.isEmpty:
    none(SeqNo)
  else:
    some(load(m.highestSeqno))

# ============================================================================
# Insert
# ============================================================================

proc insert*(m: Memtable, item: InternalValue): (uint64, uint64) =
  let itemSize = uint64(item.key.userKey.len + item.value.len + 16)
  let sizeBefore = fetchAdd(m.approximateSize, itemSize, moRelaxed)
  discard m.items.insert(item.key, item.value)

  var currentSeqno = load(m.highestSeqno, moRelaxed)
  while item.key.seqno > currentSeqno:
    if compareExchange(m.highestSeqno, currentSeqno, item.key.seqno, moRelaxed, moRelaxed):
      break
    currentSeqno = load(m.highestSeqno, moRelaxed)

  (itemSize, sizeBefore + itemSize)

# ============================================================================
# Point Lookup with MVCC
# ============================================================================

proc get*(m: Memtable, key: types.Slice, seqno: SeqNo): Option[InternalValue] =
  if seqno == 0:
    return none(InternalValue)

  let userKey = key.data
  let searchKey = newInternalKey(userKey, seqno - 1, vtValue)

  # Custom search for the rightmost entry with the same userKey
  # This handles InternalKey's descending seqno ordering correctly
  var update: seq[SkipListNode[InternalKey, types.Slice]] = newSeq[SkipListNode[
      InternalKey, types.Slice]](MAX_LEVEL)
  var x = m.items.header
  var lastSameKeyNode: SkipListNode[InternalKey, types.Slice] = nil

  for i in countdown(m.items.level - 1, 0):
    while x.forward[i] != nil:
      let fwd = x.forward[i]
      if fwd.key <= searchKey:
        # Check if this forward node has the same userKey
        if fwd.key.userKey == userKey:
          lastSameKeyNode = fwd
        x = fwd
      else:
        break
    update[i] = x

  let node = if lastSameKeyNode != nil: lastSameKeyNode else: x.forward[0]

  if node != nil and node.key.userKey == userKey:
    if node.key.isTombstone():
      return none(InternalValue)
    return some(InternalValue(key: node.key, value: node.value))

  none(InternalValue)

# ============================================================================
# Range Iterator
# ============================================================================

type
  MemtableRangeIter* = ref object
    memtable*: Memtable
    startKey*: InternalKey
    endKey*: InternalKey
    targetSeqno*: SeqNo
    iter*: SkipListRangeIter[InternalKey, types.Slice]
    initialized*: bool

proc newMemtableRangeIter*(m: Memtable, startKey, endKey: InternalKey,
    targetSeqno: SeqNo): MemtableRangeIter =
  MemtableRangeIter(
    memtable: m,
    startKey: startKey,
    endKey: endKey,
    targetSeqno: targetSeqno,
    iter: m.items.newRangeIter(startKey, endKey, false),
    initialized: true
  )

proc hasNext*(m: MemtableRangeIter): bool =
  while m.iter.hasNext():
    let (key, value) = m.iter.next()
    if key.seqno <= m.targetSeqno:
      # Found a valid entry
      m.iter = m.memtable.items.newRangeIter(key, m.endKey, false)
      return true
  return false

proc next*(m: MemtableRangeIter): Option[InternalValue] =
  if not m.hasNext():
    return none(InternalValue)

  # Get the current entry
  let (key, value) = m.iter.next()
  some(InternalValue(key: key, value: value))

# ============================================================================
# Full iteration
# ============================================================================

proc iter*(m: Memtable): seq[InternalValue] =
  result = newSeq[InternalValue]()
  let iter = m.items.all()
  while iter.hasNext():
    let (key, value) = iter.next()
    if key.valueType == vtValue:
      result.add(InternalValue(key: key, value: value))

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing Memtable..."

  let memtable = newMemtable(0)

  discard memtable.insert(newInternalValue("key1", "value1", 1, vtValue))
  discard memtable.insert(newInternalValue("key2", "value2", 2, vtValue))
  discard memtable.insert(newInternalValue("key3", "value3", 3, vtValue))

  echo "Memtable size: ", memtable.size()
  echo "Memtable len: ", memtable.len()

  # Test get
  let val1 = memtable.get(newSlice("key1"), 1.SeqNo)
  if val1.isSome:
    echo "Got key1: ", val1.get.value.asString()

  # Test range iteration
  let startKey = newInternalKey("key1", 3.SeqNo, vtValue)
  let endKey = newInternalKey("key3", 0.SeqNo, vtValue)
  let rangeIter = memtable.newMemtableRangeIter(startKey, endKey, 3.SeqNo)

  echo "Range results:"
  var count = 0
  while rangeIter.hasNext() and count < 10:
    let item = rangeIter.next()
    if item.isSome:
      echo "  ", item.get.key.userKey, ":", item.get.value.asString()
      count += 1

  if count >= 10:
    echo "  ... (truncated)"

  echo "Memtable tests passed!"
