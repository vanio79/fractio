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
# Memtable Entry - sorted by seqno (descending) for fast MVCC lookup
# ============================================================================

type
  MemtableEntry* = object
    ## Single version of a key-value pair
    seqno*: SeqNo
    valueType*: ValueType
    value*: types.Slice

  MemtableKeyVersions* = ref object
    ## All versions of a single user key, sorted by seqno descending
    versions*: seq[MemtableEntry]

# Compare for binary search - by seqno descending
proc cmpSeqnoDesc(a, b: MemtableEntry): int =
  # Descending order (higher seqno first)
  if a.seqno < b.seqno: 1
  elif a.seqno > b.seqno: -1
  else: 0

# ============================================================================
# Memtable
# ============================================================================

type
  Memtable* = ref object
    ## In-memory write buffer for the LSM tree
    ## Uses a table indexed by user key for O(log n) lookups
    id*: MemtableId
    ## Main storage: userKey -> all versions of that key, sorted by seqno desc
    items*: Table[string, MemtableKeyVersions]
    approximateSize*: Atomic[uint64]
    highestSeqno*: Atomic[SeqNo]
    requestedRotation*: Atomic[bool]

proc newMemtable*(id: MemtableId): Memtable =
  var approxSize: Atomic[uint64]
  var highSeqno: Atomic[SeqNo]
  var reqRotation: Atomic[bool]

  Memtable(
    id: id,
    items: initTable[string, MemtableKeyVersions](),
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
  let userKeyStr = item.key.userKey # Already a string now
  let itemSize = uint64(userKeyStr.len + item.value.len + 16) # Approximate

  let sizeBefore = fetchAdd(m.approximateSize, itemSize, moRelaxed)

  # Check if key exists and get or create
  let isNew = not m.items.hasKey(userKeyStr)
  if isNew:
    m.items[userKeyStr] = MemtableKeyVersions(versions: @[])

  # Add new version at end - seqnos are increasing so entries are naturally sorted (ascending)
  let entry = MemtableEntry(
    seqno: item.key.seqno,
    valueType: item.key.valueType,
    value: item.value
  )
  m.items[userKeyStr].versions.add(entry)

  # Update highest seqno using compareExchange
  var currentSeqno = load(m.highestSeqno, moRelaxed)
  while item.key.seqno > currentSeqno:
    if compareExchange(m.highestSeqno, currentSeqno, item.key.seqno, moRelaxed, moRelaxed):
      break
    currentSeqno = load(m.highestSeqno, moRelaxed)

  (itemSize, sizeBefore + itemSize)

# ============================================================================
# Point Lookup with MVCC - O(log n)
# ============================================================================

proc get*(m: Memtable, key: types.Slice, seqno: SeqNo): Option[InternalValue] =
  ## Get value for key at given sequence number
  ## Implements MVCC by returning highest seqno <= given seqno
  if seqno == 0:
    return none(InternalValue)

  let userKey = key.data # Get string data from Slice directly
  if not m.items.hasKey(userKey):
    return none(InternalValue)

  let keyVersions = m.items[userKey]

  # Binary search for highest seqno <= target seqno
  # Versions are sorted by seqno ascending
  var lo = 0
  var hi = keyVersions.versions.len - 1
  var bestIdx = -1

  while lo <= hi:
    let mid = (lo + hi) div 2
    let midSeqno = keyVersions.versions[mid].seqno
    if midSeqno <= seqno:
      bestIdx = mid
      lo = mid + 1
    else:
      hi = mid - 1

  if bestIdx < 0:
    return none(InternalValue)

  let entry = keyVersions.versions[bestIdx]

  # Check if it's a value or tombstone
  case entry.valueType
  of vtValue:
    let internalKey = newInternalKey(
      userKey, # Already a string
      entry.seqno,
      entry.valueType
    )
    return some(InternalValue(key: internalKey, value: entry.value))
  of vtTombstone, vtWeakTombstone:
    return none(InternalValue)
  of vtIndirection:
    # TODO: Handle blob indirection
    return none(InternalValue)

# ============================================================================
# Iteration
# ============================================================================

proc iter*(m: Memtable): seq[InternalValue] =
  ## Full range iterator - returns latest version of each key
  result = newSeq[InternalValue]()
  for userKey, keyVersions in m.items:
    if keyVersions.versions.len > 0:
      let entry = keyVersions.versions[0] # Highest seqno
      if entry.valueType == vtValue:
        let internalKey = newInternalKey(
          userKey, # Already a string
          entry.seqno,
          entry.valueType
        )
        result.add(InternalValue(key: internalKey, value: entry.value))

proc range*(m: Memtable, startKey: InternalKey, endKey: InternalKey): seq[
    InternalValue] =
  ## Range iterator
  result = newSeq[InternalValue]()
  let startStr = startKey.userKey # Already a string
  let endStr = endKey.userKey # Already a string

  for userKey, keyVersions in m.items:
    if userKey >= startStr and userKey < endStr:
      if keyVersions.versions.len > 0:
        let entry = keyVersions.versions[0]
        if entry.valueType == vtValue:
          let internalKey = newInternalKey(
            userKey, # Already a string
            entry.seqno,
            entry.valueType
          )
          result.add(InternalValue(key: internalKey, value: entry.value))

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
