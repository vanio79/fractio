# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for LSM Tree v2 - Tree Operations

import std/unittest
import std/os
import std/options
import fractio/storage/lsm_tree_v2/lsm_tree
import fractio/storage/lsm_tree_v2/config
import fractio/storage/lsm_tree_v2/types
import fractio/storage/lsm_tree_v2/seqno

suite "tree":
  test "newTree":
    let cfg = newDefaultConfig("/tmp/test_tree")
    let tree = newTree(cfg, 1)

    check tree.id == 1
    check tree.config == cfg
    check tree.superVersion != nil
    check tree.superVersion.activeMemtable != nil

  test "tree next table id":
    let cfg = newDefaultConfig("/tmp/test_tree")
    let tree = newTree(cfg, 0)

    let id1 = tree.nextTableId()
    let id2 = tree.nextTableId()
    check id1 != id2

  test "tree next memtable id":
    let cfg = newDefaultConfig("/tmp/test_tree")
    let tree = newTree(cfg, 0)

    let id1 = tree.nextMemtableId()
    let id2 = tree.nextMemtableId()
    check id1 != id2

  test "tree snapshot seqno":
    let cfg = newDefaultConfig("/tmp/test_tree")
    let tree = newTree(cfg, 0)

    check tree.currentSnapshotSeqno() == 0

    let seqno1 = tree.advanceSnapshotSeqno()
    check seqno1 == 1
    check tree.currentSnapshotSeqno() == 1

    let seqno2 = tree.advanceSnapshotSeqno()
    check seqno2 == 2

  test "tree insert and get":
    let cfg = newDefaultConfig("/tmp/test_tree_insert")
    let tree = newTree(cfg, 0)

    let seqno = tree.advanceSnapshotSeqno()
    discard tree.insert(newSlice("key1"), newSlice("value1"), seqno)

    let result = tree.get(newSlice("key1"))
    check result.isSome()
    if result.isSome():
      check result.get.asString() == "value1"

  test "tree get nonexistent":
    let cfg = newDefaultConfig("/tmp/test_tree_none")
    let tree = newTree(cfg, 0)

    let result = tree.get(newSlice("nonexistent"))
    check result.isNone()

  test "tree remove":
    let cfg = newDefaultConfig("/tmp/test_tree_remove")
    let tree = newTree(cfg, 0)

    # Insert
    let seqno1 = tree.advanceSnapshotSeqno()
    discard tree.insert(newSlice("key1"), newSlice("value1"), seqno1)

    # Verify exists
    let before = tree.get(newSlice("key1"))
    check before.isSome()

    # Remove
    let seqno2 = tree.advanceSnapshotSeqno()
    discard tree.remove(newSlice("key1"), seqno2)

    # Verify removed (returns value but marked as tombstone)
    let after = tree.get(newSlice("key1"))
    check after.isNone() # Tombstones result in none for value retrieval

  test "tree contains":
    let cfg = newDefaultConfig("/tmp/test_tree_contains")
    let tree = newTree(cfg, 0)

    let seqno = tree.advanceSnapshotSeqno()
    discard tree.insert(newSlice("key1"), newSlice("value1"), seqno)

    check tree.contains(newSlice("key1")) == true
    check tree.contains(newSlice("key2")) == false

  test "tree isEmpty":
    let cfg = newDefaultConfig("/tmp/test_tree_empty")
    let tree = newTree(cfg, 0)

    check tree.isEmpty() == true

    let seqno = tree.advanceSnapshotSeqno()
    discard tree.insert(newSlice("key1"), newSlice("value1"), seqno)

    check tree.isEmpty() == false

  test "tree approximate len":
    let cfg = newDefaultConfig("/tmp/test_tree_len")
    let tree = newTree(cfg, 0)

    check tree.approximateLen() == 0

    for i in 0 ..< 10:
      let seqno = tree.advanceSnapshotSeqno()
      discard tree.insert(newSlice("key" & $i), newSlice("value" & $i), seqno)

    check tree.approximateLen() == 10

  test "tree disk usage":
    let cfg = newDefaultConfig("/tmp/test_tree_disk")
    let tree = newTree(cfg, 0)

    check tree.diskUsage() == 0

    let seqno = tree.advanceSnapshotSeqno()
    discard tree.insert(newSlice("key1"), newSlice("value1"), seqno)

    check tree.diskUsage() == 0 # No tables yet

  test "tree table count":
    let cfg = newDefaultConfig("/tmp/test_tree_count")
    let tree = newTree(cfg, 0)

    check tree.tableCount() == 0

  test "tree highest seqno":
    let cfg = newDefaultConfig("/tmp/test_tree_high")
    let tree = newTree(cfg, 0)

    check tree.getHighestMemtableSeqno().isNone()
    check tree.getHighestPersistedSeqno().isNone()

    let seqno1 = tree.advanceSnapshotSeqno()
    discard tree.insert(newSlice("key1"), newSlice("value1"), seqno1)

    let highest = tree.getHighestMemtableSeqno()
    check highest.isSome()
    if highest.isSome():
      check highest.get == seqno1
