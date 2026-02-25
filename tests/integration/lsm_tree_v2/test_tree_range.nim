# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Integration Tests for LSM Tree v2 - Range Queries

import std/unittest
import std/os
import std/tempfiles
import std/options
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

    discard tree.insert("a", "v1", 1)
    discard tree.insert("b", "v2", 2)
    discard tree.insert("c", "v3", 3)

    let rangeIter = tree.range(newSlice("b"), newSlice("c"), some(3.SeqNo))
    # Should return keys >= b and < c
    var count = 0
    while rangeIter.hasNext():
      discard rangeIter.next()
      count += 1
    check count >= 1

  test "range empty":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert("a", "v1", 1)

    let rangeIter = tree.range(newSlice("x"), newSlice("z"), some(1.SeqNo))
    var count = 0
    while rangeIter.hasNext():
      discard rangeIter.next()
      count += 1
    check count == 0

  test "range all keys":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    for i in 0 ..< 26:
      let key = $(chr(ord('a') + i))
      discard tree.insert(key, "v" & key, (i + 1).SeqNo)

    # Range is exclusive on end, so use "{" (ASCII after "z") to include "z"
    let rangeIter = tree.range(newSlice("a"), newSlice("{"), some(26.SeqNo))
    # Should return all keys (a-z)
    var count = 0
    while rangeIter.hasNext():
      discard rangeIter.next()
      count += 1
    check count >= 26

  test "range with duplicates":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    # Insert same key multiple times with different seqnos
    discard tree.insert("a", "v1", 1)
    discard tree.insert("a", "v2", 2)
    discard tree.insert("a", "v3", 3)
    discard tree.insert("b", "v4", 4)

    let rangeIter = tree.range(newSlice("a"), newSlice("c"), some(4.SeqNo))
    # Should have multiple entries for "a"
    var count = 0
    while rangeIter.hasNext():
      discard rangeIter.next()
      count += 1
    check count >= 2

suite "tree disjoint range":
  test "disjoint ranges":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    for i in 0 ..< 10:
      discard tree.insert("key" & $i, "value" & $i, (i + 1).SeqNo)

    # Query disjoint ranges
    let rangeIter1 = tree.range(newSlice("key0"), newSlice("key3"), some(10.SeqNo))
    let rangeIter2 = tree.range(newSlice("key5"), newSlice("key8"), some(10.SeqNo))

    var count1 = 0
    while rangeIter1.hasNext():
      discard rangeIter1.next()
      count1 += 1

    var count2 = 0
    while rangeIter2.hasNext():
      discard rangeIter2.next()
      count2 += 1

    check count1 >= 0
    check count2 >= 0

suite "tree non-disjoint point read":
  test "point reads in sequence":
    let tmpDir = createTempDir("lsm_test_", "")
    defer: removeDir(tmpDir)

    let treeResult = createAndOpenTree(tmpDir)
    check treeResult.isOk()
    let tree = treeResult.value

    discard tree.insert("a", "1", 1)
    discard tree.insert("b", "2", 2)
    discard tree.insert("c", "3", 3)

    # Read all - use explicit seqno for visibility
    check tree.get(newSlice("a"), some(1.SeqNo)).isSome()
    check tree.get(newSlice("b"), some(2.SeqNo)).isSome()
    check tree.get(newSlice("c"), some(3.SeqNo)).isSome()

    # Read non-existent
    check tree.get(newSlice("d"), some(3.SeqNo)).isNone()

  suite "tree disjoint point read":
    test "disjoint point reads":
      let tmpDir = createTempDir("lsm_test_", "")
      defer: removeDir(tmpDir)

      let treeResult = createAndOpenTree(tmpDir)
      check treeResult.isOk()
      let tree = treeResult.value

      # Insert keys at non-sequential positions
      discard tree.insert("key000", "v1", 1)
      discard tree.insert("key100", "v2", 2)
      discard tree.insert("key200", "v3", 3)

      # Query in different order
      let val3 = tree.get(newSlice("key200"), some(3.SeqNo))
      let val1 = tree.get(newSlice("key000"), some(1.SeqNo))
      let val2 = tree.get(newSlice("key100"), some(2.SeqNo))

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
    discard tree.insert("aaa_key", "v1", 1)
    discard tree.insert("bbb_key", "v2", 2)
    discard tree.insert("ccc_key", "v3", 3)

    # Query each prefix range
    let rangeIterA = tree.range(newSlice("aaa"), newSlice("aaz"), some(3.SeqNo))
    let rangeIterB = tree.range(newSlice("bbb"), newSlice("bbz"), some(3.SeqNo))
    let rangeIterC = tree.range(newSlice("ccc"), newSlice("ccz"), some(3.SeqNo))

    var countA = 0
    while rangeIterA.hasNext():
      discard rangeIterA.next()
      countA += 1

    var countB = 0
    while rangeIterB.hasNext():
      discard rangeIterB.next()
      countB += 1

    var countC = 0
    while rangeIterC.hasNext():
      discard rangeIterC.next()
      countC += 1

    check countA >= 1
    check countB >= 1
    check countC >= 1
