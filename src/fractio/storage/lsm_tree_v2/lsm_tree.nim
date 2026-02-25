# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree v2 - Main Implementation
##
## This module provides the main Tree implementation for the LSM tree.

import std/[atomics, os, tables, options, algorithm, heapqueue]
import types
import error
import memtable
import table
import merge
import config
import range_iter

# ============================================================================
# Super Version
# ============================================================================

type
  SuperVersion* = ref object
    ## Contains current state of the tree
    activeMemtable*: Memtable
    sealedMemtables*: seq[Memtable]
    tables*: seq[SsTable] # SSTables organized by level
    snapshotSeqno*: SeqNo
    refCount*: Atomic[int32]
    versionId*: int64

proc newSuperVersion*(): SuperVersion =
  var refCount: Atomic[int32]
  SuperVersion(
    activeMemtable: newMemtable(0),
    sealedMemtables: newSeq[Memtable](),
    tables: newSeq[SsTable](),
    snapshotSeqno: 0,
    refCount: refCount,
    versionId: 0
  )

proc acquire*(sv: SuperVersion): int32 =
  atomicInc(sv.refCount, 1)
  return sv.refCount.load()

proc release*(sv: SuperVersion): int32 =
  atomicDec(sv.refCount, 1)
  return sv.refCount.load()

# ============================================================================
# Tree
# ============================================================================

type
  Tree* = ref object
    config*: Config
    id*: TreeId
    tableIdCounter*: Atomic[uint64]
    memtableIdCounter*: Atomic[int64]
    snapshotSeqnoCounter*: Atomic[uint64]
    superVersion*: SuperVersion

proc newTree*(config: Config, id: TreeId = 0): Tree =
  var tableIdCounter: Atomic[uint64]
  var memtableIdCounter: Atomic[int64]
  var snapshotSeqnoCounter: Atomic[uint64]

  Tree(
    config: config,
    id: id,
    tableIdCounter: tableIdCounter,
    memtableIdCounter: memtableIdCounter,
    snapshotSeqnoCounter: snapshotSeqnoCounter,
    superVersion: newSuperVersion()
  )

# ============================================================================
# ID Management
# ============================================================================

proc nextTableId*(t: Tree): TableId =
  TableId(fetchAdd(t.tableIdCounter, 1, moRelaxed))

proc nextMemtableId*(t: Tree): MemtableId =
  MemtableId(fetchAdd(t.memtableIdCounter, 1, moRelaxed))

proc currentSnapshotSeqno*(t: Tree): SeqNo =
  load(t.snapshotSeqnoCounter)

proc advanceSnapshotSeqno*(t: Tree): SeqNo =
  let newSeqno = fetchAdd(t.snapshotSeqnoCounter, 1, moRelaxed) + 1
  return newSeqno

# ============================================================================
# Core Operations
# ============================================================================

proc insert*(t: Tree, key: string, value: string, seqno: SeqNo): (
    uint64, uint64) =
  let internalKey = newInternalKey(key, seqno, vtValue)
  let internalValue = InternalValue(key: internalKey, value: newSlice(value))
  t.superVersion.activeMemtable.insert(internalValue)

proc remove*(t: Tree, key: string, seqno: SeqNo): (uint64, uint64) =
  let internalKey = newInternalKey(key, seqno, vtTombstone)
  let internalValue = InternalValue(key: internalKey, value: newSlice(""))
  t.superVersion.activeMemtable.insert(internalValue)

proc flushMemtable*(t: Tree, memtable: Memtable): LsmResult[SsTable] =
  let tableId = t.nextTableId()
  let level = 0
  let tablePath = t.config.path / ("L" & $level & "/" & $tableId & ".sst")

  createDir(t.config.path / ("L" & $level))

  let builderResult = newTableWriter(tablePath,
      t.config.dataBlockRestartInterval.interval)
  if builderResult.isErr:
    return err[SsTable](builderResult.error)

  let builder = builderResult.value

  for entry in memtable.iter():
    let addResult = builder.addEntry(entry)
    if addResult.isErr:
      builder.close()
      return err[SsTable](addResult.error)

  let tableResult = builder.finish()
  if tableResult.isErr:
    return err[SsTable](tableResult.error)

  # Open the table for reading
  let openResult = openSsTable(tablePath)
  if openResult.isErr:
    return err[SsTable](openResult.error)

  # Copy metadata from the created table
  let openedTable = openResult.value
  openedTable.meta = tableResult.value.meta

  ok(openedTable)

proc rotateMemtable*(t: Tree): Option[Memtable]

proc flushActiveMemtable*(t: Tree): LsmResult[Option[SsTable]] =
  if t.superVersion.activeMemtable.isEmpty():
    return ok(none(SsTable))

  let rotated = t.rotateMemtable()
  if rotated.isNone:
    return ok(none(SsTable))

  let flushResult = t.flushMemtable(rotated.get)
  if flushResult.isErr:
    return err[Option[SsTable]](flushResult.error)

  # Add the flushed table to tables
  let flushedTable = flushResult.value
  t.superVersion.tables.add(flushedTable)

  ok(some(flushedTable))

proc rotateMemtable*(t: Tree): Option[Memtable] =
  if t.superVersion.activeMemtable.isEmpty():
    return none(Memtable)

  let yanked = t.superVersion.activeMemtable

  let newMemtable = newMemtable(t.nextMemtableId())

  var newSv = newSuperVersion()
  newSv.activeMemtable = newMemtable
  newSv.sealedMemtables = t.superVersion.sealedMemtables & yanked
  newSv.tables = t.superVersion.tables
  newSv.snapshotSeqno = t.superVersion.snapshotSeqno

  t.superVersion = newSv

  return some(yanked)

# ============================================================================
# Iteration
# ============================================================================

type
  TreeIterator* = ref object
    tree*: Tree
    snapshotSeqno*: SeqNo

proc newTreeIterator*(tree: Tree, snapshotSeqno: Option[SeqNo] = none(
    SeqNo)): TreeIterator =
  let seqno = if snapshotSeqno.isSome: snapshotSeqno.get else: tree.currentSnapshotSeqno()

  TreeIterator(
    tree: tree,
    snapshotSeqno: seqno
  )

proc getInternalEntry*(t: Tree, key: types.Slice, seqno: Option[SeqNo] = none(
    SeqNo)): Option[InternalValue] =
  let snapshotSeqno = if seqno.isSome: seqno.get else: t.currentSnapshotSeqno()

  # Check active memtable
  let activeResult = t.superVersion.activeMemtable.get(key, snapshotSeqno)
  if activeResult.isSome:
    return activeResult

  # Check sealed memtables
  for i in countdown(t.superVersion.sealedMemtables.len - 1, 0):
    let result = t.superVersion.sealedMemtables[i].get(key, snapshotSeqno)
    if result.isSome:
      return result

  # Check SSTables - iterate from newest to oldest (L0 to higher levels)
  # Convert key to string for SSTable lookup
  let keyStr = key.data

  for table in t.superVersion.tables:
    # Skip if table is not open
    if not table.fileOpened:
      continue

    # Check if key is in range
    if table.meta.minKey.len > 0 and keyStr < table.meta.minKey:
      continue
    if table.meta.maxKey.len > 0 and keyStr > table.meta.maxKey:
      continue

    # Look up in SSTable
    let tableResult = table.lookup(keyStr, snapshotSeqno)
    if tableResult.isSome:
      let entry = tableResult.get
      # Check if this is a tombstone
      if entry.key.isTombstone():
        # Return tombstone to indicate deletion
        return some(entry)
      return some(entry)

  none(InternalValue)

proc lookup*(t: Tree, key: types.Slice, seqno: Option[SeqNo] = none(
    SeqNo)): Option[types.Slice] =
  let snapshotSeqno = if seqno.isSome: seqno.get else: t.currentSnapshotSeqno()

  # Check active memtable
  let activeResult = t.superVersion.activeMemtable.get(key, snapshotSeqno)
  if activeResult.isSome:
    let value = activeResult.get
    if value.key.isTombstone():
      return none(types.Slice)
    return some(value.value)

  # Check sealed memtables (newest first)
  for i in countdown(t.superVersion.sealedMemtables.len - 1, 0):
    let memtable = t.superVersion.sealedMemtables[i]
    let result = memtable.get(key, snapshotSeqno)
    if result.isSome:
      let value = result.get
      if value.key.isTombstone():
        return none(types.Slice)
      return some(value.value)

  # Check SSTables
  let keyStr = key.data
  for table in t.superVersion.tables:
    # Skip if table is not open
    if not table.fileOpened:
      continue

    # Check if key is in range
    if table.meta.minKey.len > 0 and keyStr < table.meta.minKey:
      continue
    if table.meta.maxKey.len > 0 and keyStr > table.meta.maxKey:
      continue

    # Look up in SSTable
    let tableResult = table.lookup(keyStr, snapshotSeqno)
    if tableResult.isSome:
      let entry = tableResult.get
      if entry.key.isTombstone():
        return none(types.Slice)
      return some(entry.value)

  none(types.Slice)

# Backward compatibility alias
proc get*(t: Tree, key: types.Slice, seqno: Option[SeqNo] = none(
    SeqNo)): Option[types.Slice] =
  t.lookup(key, seqno)

proc contains*(t: Tree, key: types.Slice, seqno: Option[SeqNo] = none(SeqNo)): bool =
  t.lookup(key, seqno).isSome

# ============================================================================
# Range Iterator - Lazy, streaming iterator like Rust's TreeIter
# ============================================================================

type
  RangeIterator* = ref object
    ## Lazy range iterator that yields items one at a time
    ## Mirrors Rust's TreeIter pattern
    tree*: Tree
    startKey*: string
    endKey*: string
    targetSeqno*: SeqNo
    ## Iterator state
    initialized*: bool
    heap*: HeapQueue[MergeItem]
    iterators*: seq[proc(): Option[InternalValue]]

proc newRangeIterator*(tree: Tree, startKey, endKey: string,
    targetSeqno: SeqNo): RangeIterator =
  RangeIterator(
    tree: tree,
    startKey: startKey,
    endKey: endKey,
    targetSeqno: targetSeqno,
    initialized: false,
    heap: initHeapQueue[MergeItem](),
    iterators: newSeq[proc(): Option[InternalValue]]()
  )

proc initRangeIterator*(r: RangeIterator) =
  ## Initialize the iterator by creating sub-iterators for each data source
  if r.initialized:
    return

  let startKeyStr = r.startKey
  let endKeyStr = r.endKey
  let targetSeqno = r.targetSeqno

  # Helper to get next from memtable range - uses skiplist's native range
  proc makeMemtableIterator(memtable: Memtable, startKey, endKey: string,
      targetSeqno: SeqNo): proc(): Option[InternalValue] =
    let startIKey = newInternalKey(startKey, targetSeqno, vtValue)
    let endIKey = newInternalKey(endKey, 0, vtValue)
    var memtableIter = memtable.newMemtableRangeIter(startIKey, endIKey, targetSeqno)

    result = proc(): Option[InternalValue] =
      if memtableIter.hasNext():
        return memtableIter.next()
      none(InternalValue)

  # Helper to get next from table range
  proc makeTableIterator(table: SsTable, startKey, endKey: string,
      targetSeqno: SeqNo): proc(): Option[InternalValue] =
    var scanner = newTableRangeIterator(table, startKey, endKey, targetSeqno)

    result = proc(): Option[InternalValue] =
      if scanner.isNil:
        return none(InternalValue)
      scanner.next()

  # Add active memtable
  r.iterators.add(makeMemtableIterator(r.tree.superVersion.activeMemtable,
      startKeyStr, endKeyStr, targetSeqno))

  # Add sealed memtables
  for memtable in r.tree.superVersion.sealedMemtables:
    r.iterators.add(makeMemtableIterator(memtable, startKeyStr, endKeyStr, targetSeqno))

  # Add SSTables
  for table in r.tree.superVersion.tables:
    if table.fileOpened:
      if not (table.meta.maxKey.len > 0 and startKeyStr > table.meta.maxKey):
        if not (table.meta.minKey.len > 0 and endKeyStr < table.meta.minKey):
          r.iterators.add(makeTableIterator(table, startKeyStr, endKeyStr, targetSeqno))

  r.initialized = true

proc hasNext*(r: RangeIterator): bool =
  ## Check if there are more items (without advancing)
  if not r.initialized:
    r.initRangeIterator()

  # Initialize heap with first item from each iterator if not done
  if r.heap.len == 0:
    for i in 0 ..< r.iterators.len:
      let item = r.iterators[i]()
      if item.isSome:
        r.heap.push((i, item.get))

  r.heap.len > 0

proc next*(r: RangeIterator): Option[InternalValue] =
  ## Get the next item in the range
  if not r.initialized:
    r.initRangeIterator()

  # Initialize heap with first item from each iterator if not done
  if r.heap.len == 0:
    for i in 0 ..< r.iterators.len:
      let item = r.iterators[i]()
      if item.isSome:
        r.heap.push((i, item.get))

  if r.heap.len == 0:
    return none(InternalValue)

  let minItem = r.heap.pop()
  if minItem.value.key.userKey.len == 0:
    return none(InternalValue)

  let idx = minItem.idx
  if idx < r.iterators.len:
    let nextItem = r.iterators[idx]()
    if nextItem.isSome:
      r.heap.push((idx, nextItem.get))

  some(minItem.value)

proc collect*(r: RangeIterator): seq[InternalValue] =
  ## Collect all items from the iterator into a seq
  result = newSeq[InternalValue]()
  while r.hasNext():
    let item = r.next()
    if item.isSome:
      result.add(item.get)

# ============================================================================
# Range Query - Returns lazy iterator (like Rust's TreeIter)
# ============================================================================

proc range*(t: Tree, startKey: types.Slice, endKey: types.Slice,
    seqno: Option[SeqNo] = none(SeqNo)): RangeIterator =
  ## Returns a lazy range iterator that yields items one at a time.
  ## This is the primary range scan API - use hasNext()/next() to iterate.
  let targetSeqno = if seqno.isSome: seqno.get else: t.currentSnapshotSeqno()
  let startKeyStr = startKey.data
  let endKeyStr = endKey.data

  newRangeIterator(t, startKeyStr, endKeyStr, targetSeqno)

# ============================================================================
# Prefix Iterator - Lazy, streaming iterator like Rust's TreeIter
# ============================================================================

type
  PrefixIterator* = ref object
    ## Lazy prefix iterator that yields items one at a time
    tree*: Tree
    prefix*: string
    endPrefix*: string
    targetSeqno*: SeqNo
    initialized*: bool
    heap*: HeapQueue[MergeItem]
    iterators*: seq[proc(): Option[InternalValue]]

proc newPrefixIterator*(tree: Tree, prefix: string,
    targetSeqno: SeqNo): PrefixIterator =
  # Calculate end prefix
  var endPrefix = ""
  var found = false
  for i in countdown(prefix.len - 1, 0):
    if prefix[i] != '\255':
      endPrefix = prefix[0 ..< i] & chr(ord(prefix[i]) + 1)
      found = true
      break
  if not found:
    endPrefix = "" # All 255s means unbounded

  PrefixIterator(
    tree: tree,
    prefix: prefix,
    endPrefix: endPrefix,
    targetSeqno: targetSeqno,
    initialized: false,
    heap: initHeapQueue[MergeItem](),
    iterators: newSeq[proc(): Option[InternalValue]]()
  )

proc initPrefixIterator*(p: PrefixIterator) =
  if p.initialized:
    return

  let prefix = p.prefix
  let endPrefix = p.endPrefix
  let targetSeqno = p.targetSeqno

  # Helper for memtable prefix - uses skiplist's native range
  proc makeMemtablePrefixIterator(memtable: Memtable, prefix, endPrefix: string,
      targetSeqno: SeqNo): proc(): Option[InternalValue] =
    # For prefix scan, we use the range iterator with the prefix range
    let startKey = newInternalKey(prefix, targetSeqno, vtValue)
    # Calculate end key for the prefix
    var prefixEnd = ""
    if endPrefix.len > 0:
      prefixEnd = endPrefix
    else:
      # Create a key that's just after all keys with this prefix
      prefixEnd = prefix
      if prefixEnd.len > 0:
        prefixEnd[^1] = chr(ord(prefixEnd[^1]) + 1)
    let endKey = newInternalKey(prefixEnd, 0, vtValue)
    var memtableIter = memtable.newMemtableRangeIter(startKey, endKey, targetSeqno)

    result = proc(): Option[InternalValue] =
      if memtableIter.hasNext():
        return memtableIter.next()
      none(InternalValue)

  # Helper for table prefix
  proc makeTablePrefixIterator(table: SsTable, prefix, endPrefix: string,
      targetSeqno: SeqNo): proc(): Option[InternalValue] =
    var scanner = newTablePrefixScannerForIter(table, prefix, targetSeqno)

    result = proc(): Option[InternalValue] =
      if scanner.isNil:
        return none(InternalValue)
      let entryOpt = scanner.nextPrefix()
      if entryOpt.isSome:
        let entry = entryOpt.get
        if endPrefix.len > 0 and entry.key.userKey >= endPrefix:
          return none(InternalValue)
        if entry.key.seqno <= targetSeqno:
          return some(entry)
      none(InternalValue)

  # Add active memtable
  p.iterators.add(makeMemtablePrefixIterator(p.tree.superVersion.activeMemtable,
      prefix, endPrefix, targetSeqno))

  # Add sealed memtables
  for memtable in p.tree.superVersion.sealedMemtables:
    p.iterators.add(makeMemtablePrefixIterator(memtable, prefix, endPrefix, targetSeqno))

  # Add SSTables
  for table in p.tree.superVersion.tables:
    if table.fileOpened:
      if not (table.meta.maxKey.len > 0 and prefix > table.meta.maxKey):
        p.iterators.add(makeTablePrefixIterator(table, prefix, endPrefix, targetSeqno))

  p.initialized = true

proc hasNext*(p: PrefixIterator): bool =
  if not p.initialized:
    p.initPrefixIterator()

  if p.heap.len == 0:
    for i in 0 ..< p.iterators.len:
      let item = p.iterators[i]()
      if item.isSome:
        p.heap.push((i, item.get))

  p.heap.len > 0

proc next*(p: PrefixIterator): Option[InternalValue] =
  if not p.initialized:
    p.initPrefixIterator()

  if p.heap.len == 0:
    for i in 0 ..< p.iterators.len:
      let item = p.iterators[i]()
      if item.isSome:
        p.heap.push((i, item.get))

  if p.heap.len == 0:
    return none(InternalValue)

  let minItem = p.heap.pop()
  if minItem.value.key.userKey.len == 0:
    return none(InternalValue)

  let idx = minItem.idx
  if idx < p.iterators.len:
    let nextItem = p.iterators[idx]()
    if nextItem.isSome:
      p.heap.push((idx, nextItem.get))

  some(minItem.value)

proc collectPrefix*(p: PrefixIterator): seq[InternalValue] =
  result = newSeq[InternalValue]()
  while p.hasNext():
    let item = p.next()
    if item.isSome:
      result.add(item.get)

# ============================================================================
# Prefix Scan - Returns lazy iterator (like Rust's TreeIter)
# ============================================================================

proc prefixScan*(t: Tree, prefix: types.Slice,
    seqno: Option[SeqNo] = none(SeqNo)): PrefixIterator =
  ## Returns a lazy prefix iterator that yields items one at a time.
  ## This is the primary prefix scan API - use hasNext()/next() to iterate.
  let targetSeqno = if seqno.isSome: seqno.get else: t.currentSnapshotSeqno()
  let prefixStr = prefix.data

  newPrefixIterator(t, prefixStr, targetSeqno)

# ============================================================================
# Statistics
# ============================================================================

proc tableCount*(t: Tree): int =
  t.superVersion.tables.len

proc approximateLen*(t: Tree): int =
  result = t.superVersion.activeMemtable.len()
  for memtable in t.superVersion.sealedMemtables:
    result += memtable.len()
  for table in t.superVersion.tables:
    result += int(table.meta.entryCount)

proc diskUsage*(t: Tree): uint64 =
  result = 0
  for table in t.superVersion.tables:
    result += table.meta.size

proc getHighestMemtableSeqno*(t: Tree): Option[SeqNo] =
  var result: Option[SeqNo] = t.superVersion.activeMemtable.getHighestSeqno()

  for memtable in t.superVersion.sealedMemtables:
    let mtSeqno = memtable.getHighestSeqno()
    if mtSeqno.isSome:
      if result.isNone or mtSeqno.get > result.get:
        result = mtSeqno

  result

proc getHighestPersistedSeqno*(t: Tree): Option[SeqNo] =
  var result: Option[SeqNo] = none(SeqNo)

  for table in t.superVersion.tables:
    if result.isNone or table.meta.largestSeqno > result.get:
      result = some(table.meta.largestSeqno)

  result

# ============================================================================
# Tree Lifecycle
# ============================================================================

proc openTree*(config: Config, id: TreeId = 0): LsmResult[Tree] =
  if not dirExists(config.path):
    return err[Tree](newUnrecoverableError("Database directory does not exist"))

  let tree = newTree(config, id)
  ok(tree)

proc createNewTree*(config: Config, id: TreeId = 0): LsmResult[Tree] =
  if dirExists(config.path):
    removeDir(config.path)

  createDir(config.path)

  let tree = newTree(config, id)
  ok(tree)

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing Tree..."

  let tmpDir = "/tmp/test_lsm_tree"
  if dirExists(tmpDir):
    removeDir(tmpDir)

  let cfg = newDefaultConfig(tmpDir)
  let treeResult = createNewTree(cfg, 0)
  if treeResult.isErr:
    echo "Error creating tree: ", treeResult.error
    quit(1)

  let tree = treeResult.value

  # Insert some data
  discard tree.insert("key1", "value1", 1)
  discard tree.insert("key2", "value2", 2)
  discard tree.insert("key3", "value3", 3)

  # Test get
  let val1 = tree.get(newSlice("key1"), some(1.SeqNo))
  if val1.isSome:
    echo "Got key1: ", val1.get.asString()

  # Test range
  let rangeResult = tree.range(newSlice("key1"), newSlice("key3"), some(3.SeqNo))
  echo "Range results: ", rangeResult.len

  # Test prefix scan
  let prefixResult = tree.prefixScan(newSlice("key"), some(3.SeqNo))
  echo "Prefix results: ", prefixResult.len

  # Cleanup
  if dirExists(tmpDir):
    removeDir(tmpDir)

  echo "Tree tests passed!"
