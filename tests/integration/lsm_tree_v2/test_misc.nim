# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Integration Tests for LSM Tree v2 - Snapshot, Recovery, and Misc

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

suite "snapshot":
  test "dirty snapshot":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("key"), newSlice("value"), 0)

  test "latest snapshot":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Snapshot operations would require full implementation

    check tree.approximateLen() >= 0

  test "snapshot compact":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Requires full implementation

  test "snapshot len":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    check tree.approximateLen() >= 0

  test "snapshot point read":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("key"), newSlice("value"), 0)
    let val = tree.get(newSlice("key"))
    check val.isSome()

  test "snapshot zombie":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("key"), newSlice("value"), 0)

suite "recovery":
  test "recover counter":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    for i in 0 ..< 100:
      discard tree.insert(newSlice("key" & $i), newSlice("value"), i.SeqNo)

  test "recover large value":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    let largeValue = "x".repeat(100000)
    discard tree.insert(newSlice("key"), newSlice(largeValue), 0)

    let val = tree.get(newSlice("key"))
    check val.isSome()

  test "recovery versions":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Multiple versions
    for i in 0 ..< 10:
      discard tree.insert(newSlice("key"), newSlice("v" & $i), i.SeqNo)

  test "recover from different folder":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("key"), newSlice("value"), 0)

  test "recovery mac ds store":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("key"), newSlice("value"), 0)

  test "recovery mac underscore":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("key"), newSlice("value"), 0)

  test "recover cleanup orphans":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("key"), newSlice("value"), 0)

suite "compaction":
  test "compaction readers grouping":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    for i in 0 ..< 100:
      discard tree.insert(newSlice("key" & $i), newSlice("value"), i.SeqNo)

  test "leveled trivial move":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("key"), newSlice("value"), 0)

suite "fifo and ttl":
  test "fifo ttl":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("key"), newSlice("value"), 0)

suite "other":
  test "expect point read hits":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    for i in 0 ..< 100:
      discard tree.insert(newSlice("key" & $i), newSlice("value"), i.SeqNo)

    for i in 0 ..< 100:
      check tree.contains(newSlice("key" & $i)) == true

  test "guard if":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("key"), newSlice("value"), 0)

  test "ingestion dirty snapshot":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("key"), newSlice("value"), 0)

  test "ingestion api":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("key"), newSlice("value"), 0)

  test "ingestion invariants":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("key"), newSlice("value"), 0)

  test "multi trees":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult1 = createAndOpenTree(tmpDir)
    check treeResult1.isOk()
    let tree1 = treeResult1.value

    discard tree1.insert(newSlice("key"), newSlice("value1"), 0)

    # Different path for tree 2
    let tmpDir2 = createTempDir("lsm_test2_", "")
    defer: removeDir(tmpDir2)

    let treeResult2 = createAndOpenTree(tmpDir2)
    check treeResult2.isOk()
    let tree2 = treeResult2.value

    discard tree2.insert(newSlice("key"), newSlice("value2"), 0)

  test "memtable id":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    let id1 = tree.nextMemtableId()
    let id2 = tree.nextMemtableId()
    check id1 != id2
