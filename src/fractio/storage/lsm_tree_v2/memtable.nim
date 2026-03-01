# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree v2 - Memtable (string-based implementation)
##
## OPTIMIZED: Hot paths use templates for true inlining

import std/[atomics, options]
import types
import crossbeam_skiplist
export crossbeam_skiplist
import atomics_helpers

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

# OPTIMIZED: Template for true inlining
template insert*(m: Memtable, key: InternalKey, value: string): (uint64, uint64) =
  let itemSize = uint64(key.userKey.len + value.len + 16)
  let sizeBefore = fetchAdd(m.approximateSize, itemSize, moRelaxed)
  discard m.items.insert(key, value)
  discard atomicMaxSeqNo(m.highestSeqno, key.seqno)
  (itemSize, sizeBefore + itemSize)

# OPTIMIZED: Template for true inlining
template insertFromString*(m: Memtable, key: string, value: string,
    seqno: SeqNo): (uint64, uint64) =
  let internalKey = InternalKey(userKey: key, seqno: seqno, valueType: vtValue)
  m.insert(internalKey, value)

# OPTIMIZED: Template for true inlining
template insertTombstone*(m: Memtable, key: string, seqno: SeqNo): (uint64, uint64) =
  let internalKey = InternalKey(userKey: key, seqno: seqno,
      valueType: vtTombstone)
  m.insert(internalKey, "")

template isEmpty*(m: Memtable): bool =
  m.items.isEmpty()

template iter*(m: Memtable): auto =
  m.items.iter()

# OPTIMIZED: Direct lookup - use proc with inline pragma
proc getImpl*(m: Memtable, key: string, seqno: SeqNo): Option[
    string] {.inline.} =
  if seqno == 0.SeqNo:
    return none(string)
  let searchKey = InternalKey(userKey: key, seqno: SeqNo(high(int64)),
      valueType: vtValue)
  var rangeIter = m.items.rangeFrom(searchKey)
  while rangeIter.hasNext():
    let (k, v) = rangeIter.next()
    if k.userKey > key:
      break
    if k.userKey == key:
      if int(k.seqno) <= int(seqno):
        if not (k.valueType == vtTombstone or k.valueType == vtWeakTombstone):
          return some(v)
        break
  none(string)

template get*(m: Memtable, key: string, seqno: SeqNo): Option[string] =
  getImpl(m, key, seqno)

# ===========================================================================
# MVCC Snapshot Support
# ===========================================================================

template getAt*(m: Memtable, key: string, seqno: SeqNo): Option[string] =
  m.get(key, seqno)

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
  while result.hasNext:
    let (k, v) = result.iter.next()
    if k.userKey >= startKey:
      result.currentKey = k.userKey
      result.currentValue = v
      return
  result.hasNext = false

template hasNext*(iter: var MemtableIter): bool =
  iter.hasNext

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
