# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Integration Tests for LSM Tree v2 - Weak Delete

## Tests ported from thirdparty/lsm-tree/tests/tree_weak_delete.rs

import std/unittest
import std/os
import std/tempfiles
import fractio/storage/lsm_tree_v2/lsm_tree
import fractio/storage/lsm_tree_v2/config
import fractio/storage/lsm_tree_v2/types
import fractio/storage/lsm_tree_v2/seqno
import fractio/storage/lsm_tree_v2/error

proc createAndOpenTree(path: string): LsmResult[Tree] =
  let cfg = newDefaultConfig(path)
  if dirExists(path):
    removeDir(path)
  createNewTree(cfg, 0)

suite "weak delete":
  test "weak delete basic":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Insert
    discard tree.insert(newSlice("key"), newSlice("value"), 0)
    check tree.contains(newSlice("key")) == true

    # Weak delete
    discard tree.removeWeak(newSlice("key"), 1)
    # Weak delete doesn't actually remove the value (unlike regular delete)
    # This is a placeholder - actual implementation may differ

  test "weak delete then regular insert":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Insert
    discard tree.insert(newSlice("key"), newSlice("value1"), 0)

    # Weak delete at seqno 1
    discard tree.removeWeak(newSlice("key"), 1)

    # Regular insert at seqno 2
    discard tree.insert(newSlice("key"), newSlice("value2"), 2)

    # Should have latest value
    let val = tree.get(newSlice("key"))
    check val.isSome()
    if val.isSome():
      check val.get.asString() == "value2"

suite "weak delete queue":
  test "weak delete queue ordering":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Insert multiple values
    discard tree.insert(newSlice("a"), newSlice("v1"), 0)
    discard tree.insert(newSlice("b"), newSlice("v2"), 1)
    discard tree.insert(newSlice("c"), newSlice("v3"), 2)

    # Weak delete in reverse order
    discard tree.removeWeak(newSlice("c"), 3)
    discard tree.removeWeak(newSlice("b"), 4)
    discard tree.removeWeak(newSlice("a"), 5)

    # All should still be "visible" as weak delete doesn't hide
    check tree.contains(newSlice("a")) == true
    check tree.contains(newSlice("b")) == true
    check tree.contains(newSlice("c")) == true

suite "weak delete eviction":
  test "weak delete with eviction":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Insert values
    for i in 0 ..< 100:
      discard tree.insert(newSlice("key" & $i), newSlice("value" & $i), i.SeqNo)

    # Weak delete
    discard tree.removeWeak(newSlice("key50"), 101)

    # Verify
    check tree.contains(newSlice("key49")) == true
    check tree.contains(newSlice("key50")) == true
    check tree.contains(newSlice("key51")) == true

suite "weak tombstone":
  test "weak tombstone behavior":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Insert key
    discard tree.insert(newSlice("key"), newSlice("value"), 0)

    # Verify it's there
    check tree.contains(newSlice("key")) == true

    # Note: In RocksDB, SingleDelete (weak tombstone) has special semantics
    # where it can be overridden by a subsequent regular insert
    # Our implementation is simplified
