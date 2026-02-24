# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree v2 - Main Implementation
##
## This module provides the main Tree implementation for the LSM tree.

import std/[sequtils, strformat, atomics, os, tables, options, algorithm]
import types
import error
import memtable
import table
import merge
import config

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
  ## Optimized insert that accepts strings directly to avoid Slice allocations
  let internalValue = newInternalValue(key, value, seqno, vtValue)
  let (itemSize, sizeAfter) = t.superVersion.activeMemtable.insert(internalValue)

  # Check if we need to rotate the memtable
  if t.superVersion.activeMemtable.size() >= t.config.maxMemtableSize:
    t.superVersion.activeMemtable.flagRotated()

    # Create new memtable
    let newMemtable = newMemtable(t.nextMemtableId())
    t.superVersion.sealedMemtables.add(t.superVersion.activeMemtable)
    t.superVersion.activeMemtable = newMemtable

  (itemSize, sizeAfter)

proc insertSlice*(t: Tree, key: types.Slice, value: types.Slice,
    seqno: SeqNo): (uint64, uint64) =
  ## Insert with Slice arguments (for compatibility)
  let internalValue = newInternalValue(key, value, seqno, vtValue)
  let (itemSize, sizeAfter) = t.superVersion.activeMemtable.insert(internalValue)

  # Check if we need to rotate the memtable
  if t.superVersion.activeMemtable.size() >= t.config.maxMemtableSize:
    t.superVersion.activeMemtable.flagRotated()

    # Create new memtable
    let newMemtable = newMemtable(t.nextMemtableId())
    t.superVersion.sealedMemtables.add(t.superVersion.activeMemtable)
    t.superVersion.activeMemtable = newMemtable

  (itemSize, sizeAfter)

proc remove*(t: Tree, key: string, seqno: SeqNo): (uint64, uint64) =
  let tombstone = newTombstone(key, seqno)
  t.superVersion.activeMemtable.insert(tombstone)

proc removeWeak*(t: Tree, key: types.Slice, seqno: SeqNo): (uint64, uint64) =
  let weakTombstone = newWeakTombstone(key, seqno)
  t.superVersion.activeMemtable.insert(weakTombstone)

proc get*(t: Tree, key: types.Slice, seqno: Option[SeqNo] = none(
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

proc contains*(t: Tree, key: types.Slice, seqno: Option[SeqNo] = none(SeqNo)): bool =
  t.get(key, seqno).isSome

proc isEmpty*(t: Tree): bool =
  t.superVersion.activeMemtable.isEmpty() and
  t.superVersion.sealedMemtables.len == 0 and
  t.superVersion.tables.len == 0

# ============================================================================
# Flush Operations
# ============================================================================

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
      if entry.key.valueType.isTombstone:
        # Return tombstone to indicate deletion
        return some(entry)
      return some(entry)

  none(InternalValue)

# ============================================================================
# Range Query
# ============================================================================

proc range*(t: Tree, startKey: types.Slice, endKey: types.Slice): seq[
    InternalValue] =
  result = newSeq[InternalValue]()

  let seqno = t.currentSnapshotSeqno()

  # Active memtable
  let activeStart = newInternalKey(startKey, seqno, vtValue)
  let activeEnd = newInternalKey(endKey, seqno, vtValue)
  result.add(t.superVersion.activeMemtable.range(activeStart, activeEnd))

  # Sealed memtables
  for memtable in t.superVersion.sealedMemtables:
    result.add(memtable.range(activeStart, activeEnd))

  # Sort combined results
  result.sort(proc(a, b: InternalValue): int = cmpInternalKey(a.key, b.key))

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
  try:
    if not dirExists(config.path):
      createDir(config.path)

    let tree = newTree(config, id)
    ok(tree)
  except:
    err[Tree](newIoError("Failed to open tree: " & getCurrentExceptionMsg()))

proc createNewTree*(config: Config, id: TreeId = 0): LsmResult[Tree] =
  try:
    if dirExists(config.path):
      return err[Tree](newIoError("Tree already exists at path"))

    createDir(config.path)

    for level in 0 ..< config.levelCount:
      createDir(config.path / ("L" & $level))

    let tree = newTree(config, id)
    ok(tree)
  except:
    err[Tree](newIoError("Failed to create tree: " & getCurrentExceptionMsg()))

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing LSM tree v2..."

  let treeConfig = newDefaultConfig("/tmp/lsm_tree_v2_test")

  let treeResult = createNewTree(treeConfig, 1)
  if treeResult.isErr:
    echo "Failed to create tree: ", treeResult.error
    quit(1)

  let tree = treeResult.value

  # Insert values
  let seqno1 = tree.advanceSnapshotSeqno()
  discard tree.insert(newSlice("key1"), newSlice("value1"), seqno1)

  let seqno2 = tree.advanceSnapshotSeqno()
  discard tree.insert(newSlice("key2"), newSlice("value2"), seqno2)

  let seqno3 = tree.advanceSnapshotSeqno()
  discard tree.insert(newSlice("key3"), newSlice("value3"), seqno3)

  echo "Tree approximate len: ", tree.approximateLen()
  echo "Tree is empty: ", tree.isEmpty()

  # Test get
  let val1 = tree.get(newSlice("key1"))
  if val1.isSome:
    echo "Got key1: ", val1.get.asString()
  else:
    echo "key1 not found"

  # Test remove
  let seqno4 = tree.advanceSnapshotSeqno()
  discard tree.remove(newSlice("key2"), seqno4)
  let val2 = tree.get(newSlice("key2"))
  if val2.isSome:
    echo "key2 still exists: ", val2.get.asString()
  else:
    echo "key2 removed"

  # Test contains
  echo "Contains key1: ", tree.contains(newSlice("key1"))
  echo "Contains key2: ", tree.contains(newSlice("key2"))
  echo "Contains key4: ", tree.contains(newSlice("key4"))

  # Test flush
  echo "\nTesting flush..."
  let flushResult = tree.flushActiveMemtable()
  if flushResult.isOk:
    if flushResult.value.isSome:
      echo "Flushed to table: ", flushResult.value.get.path
    else:
      echo "Nothing to flush"
  else:
    echo "Flush failed: ", flushResult.error

  echo "Table count: ", tree.tableCount()
  echo "Disk usage: ", tree.diskUsage()

  echo "\nLSM tree v2 tests passed!"
