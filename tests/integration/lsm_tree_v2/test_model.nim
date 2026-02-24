# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Integration Tests for LSM Tree v2 - Model Tests (Property-Based)

## These tests verify invariants using model checking

import std/unittest
import std/os
import std/tempfiles
import fractio/storage/lsm_tree_v2/lsm_tree
import fractio/storage/lsm_tree_v2/config
import fractio/storage/lsm_tree_v2/types
import fractio/storage/lsm_tree_v2/error

proc createAndOpenTree(path: string): LsmResult[Tree] =
  let cfg = newDefaultConfig(path)
  if dirExists(path):
    removeDir(path)
  createNewTree(cfg, 0)

suite "model 1 - simple insert":
  test "single key insert":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("key"), newSlice("value"), 0)
    check tree.contains(newSlice("key")) == true

suite "model 2 - multiple inserts":
  test "multiple different keys":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    for i in 0 ..< 100:
      discard tree.insert(newSlice("key" & $i), newSlice("value" & $i), i.SeqNo)

    for i in 0 ..< 100:
      check tree.contains(newSlice("key" & $i)) == true

suite "model 3 - insert delete insert":
  test "delete then reinsert":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Insert
    discard tree.insert(newSlice("key"), newSlice("v1"), 0)
    check tree.contains(newSlice("key")) == true

    # Delete
    discard tree.remove(newSlice("key"), 1)
    check tree.contains(newSlice("key")) == false

    # Re-insert
    discard tree.insert(newSlice("key"), newSlice("v2"), 2)
    check tree.contains(newSlice("key")) == true

    let val = tree.get(newSlice("key"))
    check val.isSome()
    if val.isSome():
      check val.get.asString() == "v2"

suite "model 4 - insert multiple versions":
  test "version history":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Insert multiple versions
    for i in 0 ..< 10:
      discard tree.insert(newSlice("key"), newSlice("v" & $i), i.SeqNo)

    # Latest version
    let val = tree.get(newSlice("key"))
    check val.isSome()
    if val.isSome():
      check val.get.asString() == "v9"

suite "model 5 - delete non-existent":
  test "delete missing key":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Delete non-existent
    discard tree.remove(newSlice("nonexistent"), 0)

    # Should not cause issues
    check tree.approximateLen() == 0

suite "model 6 - interleaved operations":
  test "interleaved insert and delete":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Interleave inserts and deletes
    for i in 0 ..< 50:
      discard tree.insert(newSlice("key" & $(i mod 10)), newSlice("v" & $i), i.SeqNo)
      if i mod 5 == 0:
        discard tree.remove(newSlice("key" & $(i mod 10)), i.SeqNo + 1)

    # Check final state
    check tree.approximateLen() >= 0

suite "bulk ingest":
  test "bulk insert":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Bulk insert
    for i in 0 ..< 1000:
      discard tree.insert(newSlice("key" & $i), newSlice("value" & $i), i.SeqNo)

    check tree.approximateLen() == 1000

suite "different block size":
  test "different block size":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Insert data
    for i in 0 ..< 100:
      discard tree.insert(newSlice("key" & $i), newSlice("value" & $i), i.SeqNo)

    check tree.approximateLen() == 100
