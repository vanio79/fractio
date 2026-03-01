# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree v2 - Memtable with Arena allocation
##
## Uses a shared arena for all keys - eliminates per-key GC overhead

import std/[atomics, options]
import fractio/storage/lsm_tree_v2/types
import crossbeam_skiplist
import skiplist_search
export crossbeam_skiplist
import fractio/storage/lsm_tree_v2/atomics_helpers

type
  Memtable* = ref object
    ## In-memory write buffer for the LSM tree
    id*: MemtableId
    items*: SkipList[InternalKey, string]
    arena*: KeyArena # Shared arena for all keys in this memtable
    approximateSize*: Atomic[uint64]
    highestSeqno*: Atomic[SeqNo]
    requestedRotation*: Atomic[bool]

proc newMemtable*(id: MemtableId, arena: KeyArena = nil): Memtable =
  var approxSize: Atomic[uint64]
  approxSize.store(0, moRelaxed)
  var highestSeqno: Atomic[SeqNo]
  highestSeqno.store(0.SeqNo, moRelaxed)
  var requestedRotation: Atomic[bool]
  requestedRotation.store(false, moRelaxed)

  # Create arena if not provided (256MB default - large enough for most workloads)
  # WARNING: Arena growing invalidates existing KeySlice pointers, so we need
  # a large initial size to avoid growth during normal operation
  let theArena = if arena != nil: arena else: newKeyArena(256 * 1024 * 1024)

  result = Memtable(
    id: id,
    items: newSkipList[InternalKey, string](),
    arena: theArena,
    approximateSize: approxSize,
    highestSeqno: highestSeqno,
    requestedRotation: requestedRotation
  )

proc insert*(m: Memtable, key: InternalKey, value: string): (uint64, uint64) =
  let itemSize = uint64(key.userKey.len + value.len + 16)
  let sizeBefore = fetchAdd(m.approximateSize, itemSize, moRelaxed)
  discard m.items.insert(key, value)
  discard atomicMaxSeqNo(m.highestSeqno, key.seqno)
  (itemSize, sizeBefore + itemSize)

proc insertFromString*(m: Memtable, key: string, value: string, seqno: SeqNo): (
    uint64, uint64) =
  ## Insert from string key - copies key into arena first
  let keySlice = m.arena.copyKey(key)
  let internalKey = newInternalKey(keySlice, seqno, vtValue)
  result = m.insert(internalKey, value)

proc isEmpty*(m: Memtable): bool =
  m.items.isEmpty()

proc iter*(m: Memtable): auto =
  m.items.iter()

proc get*(m: Memtable, key: string, seqno: SeqNo): Option[string] =
  ## Get value for key at given snapshot - ZERO-COPY lookup
  if seqno == 0.SeqNo:
    return none(string)

  # Create SearchKey borrowing from caller's string - NO allocation
  let searchKey = newSearchKey(key, seqno, vtValue)

  # Use zero-copy range search
  let rangeIter = m.items.rangeFromWithSearchKey(searchKey)

  if rangeIter.hasNext():
    let (k, v) = rangeIter.next()
    # Compare using KeySlice == string
    if k.userKey == key and not isTombstone(k.valueType):
      return some(v)
  result = none(string)

# ===========================================================================
# MVCC Snapshot Support
# ===========================================================================

proc getAt*(m: Memtable, key: string, seqno: SeqNo): Option[string] =
  get(m, key, seqno)

# ===========================================================================
# Iteration
# ===========================================================================

type
  MemtableIter* = object
    iter*: RangeIter[InternalKey, string]
    currentKey: string
    currentValue: string
    hasNext: bool

proc newIter*(m: Memtable, startKey: string): MemtableIter =
  # Create temporary search key
  let searchKey = newSearchKey(startKey, 0.SeqNo, vtValue)
  result.iter = m.items.rangeFromWithSearchKey(searchKey)
  result.hasNext = result.iter.hasNext()
  if result.hasNext:
    let (k, v) = result.iter.next()
    result.currentKey = k.userKey.toString()
    result.currentValue = v

proc hasNext*(iter: var MemtableIter): bool =
  result = iter.hasNext

proc next*(iter: var MemtableIter): tuple[key: string, value: string] =
  if not iter.hasNext:
    raise newException(ValueError, "no more items")
  let result = (iter.currentKey, iter.currentValue)
  iter.hasNext = iter.iter.hasNext()
  if iter.hasNext:
    let (k, v) = iter.iter.next()
    iter.currentKey = k.userKey.toString()
    iter.currentValue = v
  result

proc close*(iter: var MemtableIter) =
  iter.hasNext = false
