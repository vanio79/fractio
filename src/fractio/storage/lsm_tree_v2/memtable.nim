# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree v2 - Memtable (string-based implementation)

import std/[atomics, options]
import fractio/storage/lsm_tree_v2/types
import crossbeam_skiplist
export crossbeam_skiplist
import fractio/storage/lsm_tree_v2/atomics_helpers

type
  Memtable* = ref object
    ## In-memory write buffer for the LSM tree
    id*: MemtableId
    items*: SkipList[InternalKey, string]
    approximateSize*: Atomic[uint64]
    highestSeqno*: Atomic[SeqNo]
    requestedRotation*: Atomic[bool]

proc newMemtable*(id: MemtableId): Memtable =
  var approxSize: Atomic[uint64]
  approxSize.store(0, moRelaxed)
  var highestSeqno: Atomic[SeqNo]
  highestSeqno.store(0.SeqNo, moRelaxed)
  var requestedRotation: Atomic[bool]
  requestedRotation.store(false, moRelaxed)

  result = Memtable(
    id: id,
    items: newSkipList[InternalKey, string](),
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
  ## Insert from string key - stores a copy of the key
  let internalKey = newInternalKey(key, seqno, vtValue)
  result = m.insert(internalKey, value)

proc insertTombstone*(m: Memtable, key: string, seqno: SeqNo): (uint64, uint64) =
  ## Insert a tombstone (deletion marker)
  let internalKey = newInternalKey(key, seqno, vtTombstone)
  result = m.insert(internalKey, "")

proc isEmpty*(m: Memtable): bool =
  m.items.isEmpty()

proc iter*(m: Memtable): auto =
  m.items.iter()

proc get*(m: Memtable, key: string, seqno: SeqNo): Option[string] =
  ## Get value for key at given snapshot
  if seqno == 0.SeqNo:
    return none(string)

  # Create a search key with max seqno to find all versions of this key
  let searchKey = newInternalKey(key, SeqNo(high(int64)), vtValue)
  var rangeIter = m.items.rangeFrom(searchKey)

  # Look for a matching entry visible to our snapshot
  while rangeIter.hasNext():
    let (k, v) = rangeIter.next()
    # Check if we've moved past the target key
    if k.userKey > key:
      break
    # Check if this is our key
    if k.userKey == key:
      # Check if this version is visible to our snapshot
      if int(k.seqno) <= int(seqno):
        if not k.isTombstone():
          return some(v)
        else:
          return none(string) # Tombstone

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
    iter*: Iter[InternalKey, string]
    currentKey: string
    currentValue: string
    hasNext: bool

proc newIter*(m: Memtable, startKey: string): MemtableIter =
  result.iter = m.items.iter()
  result.hasNext = result.iter.hasNext()
  # Skip to startKey
  while result.hasNext:
    let (k, v) = result.iter.next()
    if k.userKey >= startKey:
      result.currentKey = k.userKey
      result.currentValue = v
      return
  result.hasNext = false

proc hasNext*(iter: var MemtableIter): bool =
  result = iter.hasNext

proc next*(iter: var MemtableIter): tuple[key: string, value: string] =
  if not iter.hasNext:
    raise newException(ValueError, "no more items")
  let ret = (iter.currentKey, iter.currentValue)
  iter.hasNext = iter.iter.hasNext()
  if iter.hasNext:
    let (k, v) = iter.iter.next()
    iter.currentKey = k.userKey
    iter.currentValue = v
  ret

proc close*(iter: var MemtableIter) =
  iter.hasNext = false
