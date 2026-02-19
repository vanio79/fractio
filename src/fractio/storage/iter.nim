# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Storage Iterator Implementation
##
## Provides iterators for scanning key-value pairs in a keyspace.

import fractio/storage/[error, snapshot_nonce, guard]
import fractio/storage/lsm_tree/[types, memtable, lsm_tree]
import std/[options, algorithm]

type
  # Entry returned by iterator
  IteratorEntry* = object
    key*: string
    value*: string
    seqno*: uint64
    valueType*: ValueType

  # Iterator state
  IterState* = enum
    isReady
    isValid
    isExhausted
    isError

  # Keyspace iterator that holds a snapshot
  KeyspaceIter* = ref object
    nonce*: SnapshotNonce
    entries*: seq[IteratorEntry]
    currentIndex*: int
    state*: IterState

  # Range iterator options
  RangeOptions* = object
    startKey*: Option[string]
    endKey*: Option[string]
    limit*: Option[int]
    reverse*: bool

  # Prefix iterator options
  PrefixOptions* = object
    prefix*: string
    limit*: Option[int]

# Create a new keyspace iterator
proc newKeyspaceIter*(nonce: SnapshotNonce): KeyspaceIter =
  result = KeyspaceIter(
    nonce: nonce,
    entries: @[],
    currentIndex: 0,
    state: isReady
  )

# Add entry to iterator buffer
proc add*(iter: KeyspaceIter, key: string, value: string,
          seqno: uint64, valueType: ValueType) =
  iter.entries.add(IteratorEntry(
    key: key,
    value: value,
    seqno: seqno,
    valueType: valueType
  ))
  iter.state = isValid

# Check if iterator is valid
proc isValid*(iter: KeyspaceIter): bool =
  iter.state == isValid and iter.currentIndex < iter.entries.len

# Check if iterator is exhausted
proc isExhausted*(iter: KeyspaceIter): bool =
  iter.state == isExhausted or (iter.state == isValid and iter.currentIndex >=
      iter.entries.len)

# Move to next entry
proc next*(iter: KeyspaceIter): bool =
  if iter.currentIndex < iter.entries.len - 1:
    iter.currentIndex += 1
    return true
  iter.state = isExhausted
  return false

# Get current key
proc key*(iter: KeyspaceIter): Option[string] =
  if iter.isValid:
    return some(iter.entries[iter.currentIndex].key)
  return none(string)

# Get current value
proc value*(iter: KeyspaceIter): Option[string] =
  if iter.isValid:
    return some(iter.entries[iter.currentIndex].value)
  return none(string)

# Get current entry
proc entry*(iter: KeyspaceIter): Option[IteratorEntry] =
  if iter.isValid:
    return some(iter.entries[iter.currentIndex])
  return none(IteratorEntry)

# Reset iterator to beginning
proc reset*(iter: KeyspaceIter) =
  iter.currentIndex = 0
  if iter.entries.len > 0:
    iter.state = isValid
  else:
    iter.state = isReady

# Sort entries by key
proc sortEntries*(iter: KeyspaceIter, reverse: bool = false) =
  if reverse:
    iter.entries.sort(proc(a, b: IteratorEntry): int = cmp(b.key, a.key))
  else:
    iter.entries.sort(proc(a, b: IteratorEntry): int = cmp(a.key, b.key))

# Create iterator from memtable entries
proc fromMemtable*(iter: KeyspaceIter, memtable: Memtable) =
  let entries = memtable.getSortedEntries()
  for e in entries:
    iter.add(e.key, e.value, e.seqno, ValueType(e.valueType))

# Iterator for all entries in a keyspace
iterator items*(iter: KeyspaceIter): IteratorEntry =
  iter.reset()
  while iter.isValid:
    yield iter.entries[iter.currentIndex]
    if not iter.next():
      break

# Iterator for range of keys
iterator range*(iter: KeyspaceIter, startKey: string,
    endKey: string): IteratorEntry =
  iter.reset()
  while iter.isValid:
    let entry = iter.entries[iter.currentIndex]
    if entry.key >= startKey and entry.key <= endKey:
      yield entry
    if not iter.next():
      break

# Iterator for prefix
iterator prefix*(iter: KeyspaceIter, prefixStr: string): IteratorEntry =
  iter.reset()
  while iter.isValid:
    let entry = iter.entries[iter.currentIndex]
    if entry.key.startsWith(prefixStr):
      yield entry
    elif entry.key > prefixStr and not entry.key.startsWith(prefixStr):
      break
    if not iter.next():
      break

# Create range options with defaults
proc defaultRangeOptions*(): RangeOptions =
  RangeOptions(
    startKey: none(string),
    endKey: none(string),
    limit: none(int),
    reverse: false
  )

# Create prefix options
proc prefixOptions*(prefixStr: string): PrefixOptions =
  PrefixOptions(
    prefix: prefixStr,
    limit: none(int)
  )
