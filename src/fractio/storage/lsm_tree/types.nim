# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree Types
##
## Core types for the LSM (Log-Structured Merge) tree implementation.

import std/[tables, atomics, locks, options, times]

# Forward declarations
type
  LsmTreeConfig* = ref object
    path*: string
    levelCount*: int
    maxMemtableSize*: uint64
    blockSize*: int
    cacheCapacity*: uint64

  SequenceNumberCounter* = ref object
    value*: Atomic[uint64]

  SnapshotTracker* = ref object
    lowestFreedInstant*: Atomic[uint64]

# Sequence number operations
proc newSequenceNumberCounter*(): SequenceNumberCounter =
  result = SequenceNumberCounter()
  result.value.store(0'u64, moRelaxed)

proc get*(counter: SequenceNumberCounter): uint64 =
  counter.value.load(moAcquire)

proc next*(counter: SequenceNumberCounter): uint64 =
  counter.value.fetchAdd(1, moRelaxed) + 1

proc fetchMax*(counter: SequenceNumberCounter, value: uint64) =
  var current = counter.value.load(moAcquire)
  while value > current:
    if counter.value.compareExchange(current, value, moAcquire, moAcquire):
      break
    current = counter.value.load(moAcquire)

# Snapshot tracker operations
proc newSnapshotTracker*(): SnapshotTracker =
  result = SnapshotTracker()
  result.lowestFreedInstant.store(0'u64, moRelaxed)

proc getRef*(tracker: SnapshotTracker): SequenceNumberCounter =
  # Return a reference to a shared counter
  # In a full implementation, this would return the actual counter
  result = newSequenceNumberCounter()

# Value types for LSM tree entries
type
  ValueType* = enum
    vtValue         ## Regular value
    vtTombstone     ## Tombstone (deleted key)
    vtWeakTombstone ## Weak tombstone (for merge operations)
    vtIndirection   ## Blob value indirection (for KV separation)

# Internal key representation (key + seqno + type)
type
  InternalKey* = object
    key*: string
    seqno*: uint64
    valueType*: ValueType

# Compare internal keys (for sorting)
proc `<`*(a, b: InternalKey): bool =
  # Sort by key first, then by seqno descending (newer entries first)
  if a.key != b.key:
    return a.key < b.key
  return a.seqno > b.seqno # Higher seqno = newer = comes first

# Entry in the memtable
type
  MemtableEntry* = object
    key*: string
    value*: string
    seqno*: uint64
    valueType*: ValueType

# Memtable ID type
type
  MemtableId* = uint64

# Memtable - in-memory sorted table
type
  Memtable* = ref object
    id*: MemtableId
    entries*: Table[string, MemtableEntry] # Key -> Entry
    size*: uint64                          # Approximate size in bytes
    highestSeqno*: uint64
    lock*: Lock

# SSTable - on-disk sorted string table
type
  SsTableId* = uint64

  SsTable* = ref object
    id*: SsTableId
    path*: string
    size*: uint64
    level*: int
    smallestKey*: string
    largestKey*: string
    seqnoRange*: (uint64, uint64) # (min, max)

# Version - represents a consistent snapshot of the tree
type
  Version* = ref object
    id*: uint64
    activeMemtable*: Memtable
    sealedMemtables*: seq[Memtable]
    tables*: seq[seq[SsTable]] # Tables per level
    lock*: Lock

# LSM Tree - the main tree structure
type
  LsmTree* = ref object
    config*: LsmTreeConfig
    activeMemtable*: Memtable
    sealedMemtables*: seq[Memtable]
    tables*: seq[seq[SsTable]] # Tables per level
    memtableIdCounter*: Atomic[uint64]
    tableIdCounter*: Atomic[uint64]
    versionLock*: Lock
    seqnoCounter*: SequenceNumberCounter
    snapshotTracker*: SnapshotTracker

# Abstract tree interface
type
  AbstractTree* = concept T
    insert(T, string, string, uint64)
    remove(T, string, uint64)
    get(T, string, uint64): Option[string]
    containsKey(T, string, uint64): bool
    approximateLen(T): int
    diskSpace(T): uint64

# AnyTree - can hold any tree implementation
type
  AnyTree* = ref object
    case kind*: bool
    of true:
      tree*: LsmTree
    of false:
      data*: Table[string, string] # Fallback for testing

# Result of insert/remove operations
type
  ItemSizeResult* = tuple[itemSize: uint64, memtableSize: uint64]
