# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree v2 - Memtable
##
## This module provides the Memtable implementation - an in-memory write buffer
## for the LSM tree using a skip list for efficient range iteration.

import std/[atomics, options]
import types
import crossbeam_skiplist

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

  # Rust's approach: Search from (seqno - 1) downward and take the FIRST match.
  # Since InternalKey stores seqno in reverse order, the first entry with the
  # matching userKey will have the highest seqno <= target.
  #
  # We search starting at (seqno - 1) because the skiplist orders by:
  # 1. user_key (ascending)
  # 2. seqno (descending, via Reverse)
  # 3. valueType
  #
  # So searching from seqno-1 gives us the highest seqno <= target as the first result.
  let searchSeqno = if seqno > 0: seqno - 1 else: 0.SeqNo
  let startKey = newInternalKey(userKey, searchSeqno, vtValue)

  # Range from startKey to infinity - first match will have highest seqno
  let rangeIter = m.items.rangeFrom(startKey)

  # Get the first entry - that's the one with highest seqno <= target
  if rangeIter.hasNext():
    let (k, v) = rangeIter.next()
    # Verify the key matches
    if k.userKey == userKey:
      # Check if it's a tombstone - if so, we need to find older version
      if k.isTombstone():
        # Tombstone found - need to continue searching for older version
        # Keep iterating until we find a non-tombstone or key changes
        var iter = m.items.rangeFrom(startKey)
        var result: Option[InternalValue] = none(InternalValue)
        while iter.hasNext():
          let (tk, tv) = iter.next()
          if tk.userKey != userKey:
            break
          if not tk.isTombstone():
            # Found a valid value
            result = some(InternalValue(key: tk, value: tv))
        return result
      else:
        # Found a valid value
        return some(InternalValue(key: k, value: v))

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
    iter*: RangeIter[InternalKey, types.Slice]
    initialized*: bool
    currentKey*: InternalKey
    currentValue*: types.Slice

proc newMemtableRangeIter*(m: Memtable, startKey, endKey: InternalKey,
    targetSeqno: SeqNo): MemtableRangeIter =
  MemtableRangeIter(
    memtable: m,
    startKey: startKey,
    endKey: endKey,
    targetSeqno: targetSeqno,
    iter: m.items.range(startKey, endKey, false),
    initialized: false,
    currentKey: nil,
    currentValue: nil
  )

proc hasNext*(m: MemtableRangeIter): bool =
  while m.iter.hasNext():
    let (key, value) = m.iter.next()
    if key.seqno <= m.targetSeqno:
      # Found a valid entry - store it and return true
      m.currentKey = key
      m.currentValue = value
      return true
    # Skip entries with seqno > targetSeqno (they're newer)
  return false

proc next*(m: MemtableRangeIter): Option[InternalValue] =
  if m.currentKey.isNil:
    return none(InternalValue)
  let result = some(InternalValue(key: m.currentKey, value: m.currentValue))
  m.currentKey = nil
  m.currentValue = nil
  return result

# ============================================================================
# Full iteration
# ============================================================================

proc iter*(m: Memtable): seq[InternalValue] =
  result = newSeq[InternalValue]()
  let iter = m.items.iter()
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
