# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Memtable Implementation
##
## An in-memory sorted table that buffers writes before they are flushed to disk.

import ./types
import std/[tables, locks, atomics, options, algorithm]

# Global memtable ID counter
var globalMemtableIdCounter: Atomic[uint64]
globalMemtableIdCounter.store(1'u64, moRelaxed)

# Create a new memtable
proc newMemtable*(): Memtable =
  result = Memtable(
    id: globalMemtableIdCounter.fetchAdd(1, moRelaxed),
    entries: initTable[string, MemtableEntry](),
    size: 0'u64,
    highestSeqno: 0'u64
  )
  initLock(result.lock)

# Insert an entry into the memtable
proc insert*(memtable: Memtable, key: string, value: string,
             seqno: uint64, valueType: ValueType = vtValue): uint64 =
  memtable.lock.acquire()
  defer: memtable.lock.release()

  let entrySize = uint64(key.len + value.len)

  # Check if key already exists
  if key in memtable.entries:
    let oldEntry = memtable.entries[key]
    let oldSize = uint64(key.len + oldEntry.value.len)
    memtable.size = memtable.size - oldSize + entrySize
  else:
    memtable.size += entrySize

  memtable.entries[key] = MemtableEntry(
    key: key,
    value: value,
    seqno: seqno,
    valueType: valueType
  )

  if seqno > memtable.highestSeqno:
    memtable.highestSeqno = seqno

  return memtable.size

# Remove an entry (inserts a tombstone)
proc remove*(memtable: Memtable, key: string, seqno: uint64,
             weak: bool = false): uint64 =
  let valueType = if weak: vtWeakTombstone else: vtTombstone
  return memtable.insert(key, "", seqno, valueType)

# Get an entry from the memtable
proc get*(memtable: Memtable, key: string): Option[MemtableEntry] =
  memtable.lock.acquire()
  defer: memtable.lock.release()

  if key in memtable.entries:
    return some(memtable.entries[key])
  return none(MemtableEntry)

# Check if key exists (and is not a tombstone)
proc containsKey*(memtable: Memtable, key: string): bool =
  memtable.lock.acquire()
  defer: memtable.lock.release()

  if key in memtable.entries:
    let entry = memtable.entries[key]
    return entry.valueType != vtTombstone and entry.valueType != vtWeakTombstone
  return false

# Get approximate size
proc approximateSize*(memtable: Memtable): uint64 =
  memtable.lock.acquire()
  defer: memtable.lock.release()
  return memtable.size

# Get entry count
proc len*(memtable: Memtable): int =
  memtable.lock.acquire()
  defer: memtable.lock.release()
  return memtable.entries.len

# Get highest sequence number
proc getHighestSeqno*(memtable: Memtable): uint64 =
  memtable.lock.acquire()
  defer: memtable.lock.release()
  return memtable.highestSeqno

# Get all entries sorted by key
proc getSortedEntries*(memtable: Memtable): seq[MemtableEntry] =
  memtable.lock.acquire()
  defer: memtable.lock.release()

  result = newSeq[MemtableEntry]()
  for entry in memtable.entries.values:
    result.add(entry)

  # Sort by internal key (key ascending, seqno descending)
  result.sort(proc(a, b: MemtableEntry): int =
    if a.key != b.key:
      return cmp(a.key, b.key)
    return cmp(b.seqno, a.seqno) # Higher seqno first
  )

# Clear the memtable
proc clear*(memtable: Memtable) =
  memtable.lock.acquire()
  defer: memtable.lock.release()

  memtable.entries.clear()
  memtable.size = 0
  memtable.highestSeqno = 0

# Check if memtable is empty
proc isEmpty*(memtable: Memtable): bool =
  memtable.lock.acquire()
  defer: memtable.lock.release()
  return memtable.entries.len == 0
