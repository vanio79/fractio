# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree v2 - Memtable
##
## This module provides the Memtable implementation - an in-memory write buffer
## for the LSM tree using a sorted structure.

import std/[tables, sequtils, algorithm, atomics, options]
import types
import error

# ============================================================================
# Memtable
# ============================================================================

type
  Memtable* = ref object
    ## In-memory write buffer for the LSM tree
    ## Uses a table for simple key-value storage (would be skip list in production)
    id*: MemtableId
    items*: Table[InternalKey, types.Slice] # InternalKey -> value
    approximateSize*: Atomic[uint64]
    highestSeqno*: Atomic[SeqNo]
    requestedRotation*: Atomic[bool]

proc newMemtable*(id: MemtableId): Memtable =
  var approxSize: Atomic[uint64]
  var highSeqno: Atomic[SeqNo]
  var reqRotation: Atomic[bool]

  Memtable(
    id: id,
    items: initTable[InternalKey, types.Slice](),
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
  m.items.len == 0

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
  ## Insert an item into the memtable
  ## Returns (item_size, total_size_after)
  let itemSize = uint64(item.key.userKey.len + item.value.len + sizeof(
      InternalKey) + sizeof(types.Slice))

  let sizeBefore = fetchAdd(m.approximateSize, itemSize, moRelaxed)

  # Insert into table
  m.items[item.key] = item.value

  # Update highest seqno using compareExchange
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
  ## Get value for key at given sequence number
  ## Implements MVCC by returning highest seqno <= given seqno
  if seqno == 0:
    return none(InternalValue)

  var bestMatch: Option[InternalValue] = none(InternalValue)

  for internalKey, value in m.items:
    if internalKey.userKey == key and internalKey.seqno <= seqno:
      if bestMatch.isNone or internalKey.seqno > bestMatch.get.key.seqno:
        bestMatch = some(InternalValue(key: internalKey, value: value))

  bestMatch

# ============================================================================
# Iteration
# ============================================================================

proc iter*(m: Memtable): seq[InternalValue] =
  ## Full range iterator
  result = newSeq[InternalValue]()
  for key, value in m.items:
    result.add(InternalValue(key: key, value: value))
  # Sort by key (user key ascending, seqno descending)
  result.sort(proc(a, b: InternalValue): int = cmpInternalKey(a.key, b.key))

proc range*(m: Memtable, startKey: InternalKey, endKey: InternalKey): seq[
    InternalValue] =
  ## Range iterator
  result = newSeq[InternalValue]()
  for key, value in m.items:
    if key >= startKey and key < endKey:
      result.add(InternalValue(key: key, value: value))
  result.sort(proc(a, b: InternalValue): int = cmpInternalKey(a.key, b.key))

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing memtable..."

  let memtable = newMemtable(0)

  # Insert some values
  discard memtable.insert(newInternalValue("key1", "value1", 1, vtValue))
  discard memtable.insert(newInternalValue("key1", "value2", 2, vtValue))
  discard memtable.insert(newInternalValue("key2", "value3", 3, vtValue))

  echo "Memtable size: ", memtable.size()
  echo "Memtable len: ", memtable.len()

  # Test get with latest seqno
  let key1 = newSlice("key1")
  let val = memtable.get(key1, 2)
  if val.isSome:
    echo "Got key1 (seqno 2): ", val.get.value.asString()

  # Test get with older seqno
  let val1 = memtable.get(key1, 1)
  if val1.isSome:
    echo "Got key1 (seqno 1): ", val1.get.value.asString()

  # Test tombstone
  discard memtable.insert(newTombstone("key2", 4))
  let val2 = memtable.get(key1, 4)
  echo "After tombstone, key2 exists: ", val2.isSome

  # Test iter
  echo "\nAll items:"
  for item in memtable.iter():
    echo "  ", $item

  echo "Memtable tests passed!"
