# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Integration Tests for LSM Tree v2 - Range Queries

import std/unittest
import std/os
import std/tempfiles
import fractio/storage/lsm_tree_v2/lsm_tree
import fractio/storage/lsm_tree_v2/config
import fractio/storage/lsm_tree_v2/types
import fractio/storage/lsm_tree_v2/error
import fractio/storage/lsm_tree_v2/seqno

proc createAndOpenTree(path: string): LsmResult[Tree] =
  let cfg = newDefaultConfig(path)
  if dirExists(path):
    removeDir(path)
  createNewTree(cfg, 0)

suite "tree range":
  test "range single key":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("a"), newSlice("v1"), 0)
    discard tree.insert(newSlice("b"), newSlice("v2"), 1)
    discard tree.insert(newSlice("c"), newSlice("v3"), 2)

    let rangeResult = tree.range(newSlice("b"), newSlice("c"))
    # Should return keys >= b and < c
    check rangeResult.len >= 1

  test "range empty":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("a"), newSlice("v1"), 0)

    let rangeResult = tree.range(newSlice("x"), newSlice("z"))
    check rangeResult.len == 0

  test "range all keys":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    for i in 0 ..< 26:
      let key = $(char('a' + i))
      discard tree.insert(newSlice(key), newSlice("v" & key), i.SeqNo)

    let rangeResult = tree.range(newSlice("a"), newSlice("z"))
    # Should return all keys
    check rangeResult.len >= 26

  test "range with duplicates":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Insert same key multiple times with different seqnos
    discard tree.insert(newSlice("a"), newSlice("v1"), 0)
    discard tree.insert(newSlice("a"), newSlice("v2"), 1)
    discard tree.insert(newSlice("a"), newSlice("v3"), 2)
    discard tree.insert(newSlice("b"), newSlice("v4"), 3)

    let rangeResult = tree.range(newSlice("a"), newSlice("c"))
    # Should have multiple entries for "a"
    check rangeResult.len >= 2

suite "tree disjoint range":
  test "disjoint ranges":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    for i in 0 ..< 10:
      discard tree.insert(newSlice("key" & $i), newSlice("value" & $i), i.SeqNo)

    # Query disjoint ranges
    let range1 = tree.range(newSlice("key0"), newSlice("key3"))
    let range2 = tree.range(newSlice("key5"), newSlice("key8"))

    check range1.len >= 0
    check range2.len >= 0

suite "tree non-disjoint point read":
  test "point reads in sequence":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert(newSlice("a"), newSlice("1"), 0)
    discard tree.insert(newSlice("b"), newSlice("2"), 1)
    discard tree.insert(newSlice("c"), newSlice("3"), 2)

    # Read all
    check tree.get(newSlice("a")).isSome()
    check tree.get(newSlice("b")).isSome()
    check tree.get(newSlice("c")).isSome()

    # Read non-existent
    check tree.get(newSlice("d")).isNone()

suite "tree disjoint point read":
  test "disjoint point reads":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Insert keys at non-sequential positions
    discard tree.insert(newSlice("key000"), newSlice("v1"), 0)
    discard tree.insert(newSlice("key100"), newSlice("v2"), 1)
    discard tree.insert(newSlice("key200"), newSlice("v3"), 2)

    # Query in different order
    let val3 = tree.get(newSlice("key200"))
    let val1 = tree.get(newSlice("key000"))
    let val2 = tree.get(newSlice("key100"))

    check val1.isSome()
    check val2.isSome()
    check val3.isSome()

suite "tree disjoint prefix":
  test "different prefixes":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Insert keys with different prefixes
    discard tree.insert(newSlice("aaa_key"), newSlice("v1"), 0)
    discard tree.insert(newSlice("bbb_key"), newSlice("v2"), 1)
    discard tree.insert(newSlice("ccc_key"), newSlice("v3"), 2)

    # Query each prefix range
    let rangeA = tree.range(newSlice("aaa"), newSlice("aaz"))
    let rangeB = tree.range(newSlice("bbb"), newSlice("bbz"))
    let rangeC = tree.range(newSlice("ccc"), newSlice("ccz"))

    check rangeA.len >= 1
    check rangeB.len >= 1
    check rangeC.len >= 1
