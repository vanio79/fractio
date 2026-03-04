# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Concurrent tests for concurrent-skiplist

import std/unittest
import std/options
import std/atomics
import std/typedthreads
import fractio/storage/lsm_tree_v2/crossbeam_skiplist

suite "concurrent-skiplist":
  test "concurrent inserts":
    let s = newSkipList[string, string]()
    var barrier: Atomic[int]

    proc writer(id: int) {.thread.} =
      discard barrier.fetchAdd(1, moRelaxed)
      while barrier.load(moRelaxed) < 3:
        discard

      for i in 0 ..< 100:
        let key = "key_" & $id & "_" & $i
        discard s.insert(key, "value_" & $id & "_" & $i)

    var t1, t2, t3: Thread[int]
    createThread(t1, writer, 1)
    createThread(t2, writer, 2)
    createThread(t3, writer, 3)

    joinThread(t1)
    joinThread(t2)
    joinThread(t3)

    # Check all inserts succeeded
    doAssert s.len() == 300

    # Verify some values
    let v1 = s.get("key_1_0")
    doAssert v1.isSome
    if v1.isSome:
      doAssert v1.get() == "value_1_0"
    let v2 = s.get("key_2_99")
    doAssert v2.isSome
    if v2.isSome:
      doAssert v2.get() == "value_2_99"
    let v3 = s.get("key_3_50")
    doAssert v3.isSome
    if v3.isSome:
      doAssert v3.get() == "value_3_50"

  test "concurrent inserts and reads":
    let s = newSkipList[int, string]()
    var barrier: Atomic[int]

    proc writer(id: int) {.thread.} =
      discard barrier.fetchAdd(1, moRelaxed)
      while barrier.load(moRelaxed) < 2:
        discard

      for i in 0 ..< 500:
        let key = id * 1000 + i
        discard s.insert(key, "value_" & $id & "_" & $i)

    proc reader(id: int) {.thread.} =
      discard barrier.fetchAdd(1, moRelaxed)
      while barrier.load(moRelaxed) < 2:
        discard

      for i in 0 ..< 100:
        discard s.get(id * 1000 + i)
        discard s.len()

    var t1, t2: Thread[int]
    createThread(t1, writer, 1)
    createThread(t2, reader, 2)

    joinThread(t1)
    joinThread(t2)

    doAssert s.len() == 500

  test "concurrent insert and iterate":
    let s = newSkipList[int, string]()
    var barrier: Atomic[int]

    proc writer(id: int) {.thread.} =
      discard barrier.fetchAdd(1, moRelaxed)
      while barrier.load(moRelaxed) < 2:
        discard

      for i in 0 ..< 100:
        discard s.insert(i, "value_" & $i)

    proc iter(id: int) {.thread.} =
      discard barrier.fetchAdd(1, moRelaxed)
      while barrier.load(moRelaxed) < 2:
        discard

      var count = 0
      let iter = s.iter()
      while iter.hasNext() and count < 100:
        discard iter.next()
        count += 1

    var t1, t2: Thread[int]
    createThread(t1, writer, 1)
    createThread(t2, iter, 2)

    joinThread(t1)
    joinThread(t2)

    doAssert s.len() == 100
