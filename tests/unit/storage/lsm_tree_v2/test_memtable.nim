# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for LSM Tree v2 - Memtable

import std/unittest
import std/options
import fractio/storage/lsm_tree_v2/memtable
import fractio/storage/lsm_tree_v2/types

suite "memtable":
  test "memtable new":
    let mt = newMemtable(0)
    check mt.id() == 0
    check mt.isEmpty()
    check mt.len() == 0

  test "memtable insert and get":
    let mt = newMemtable(0)
    let iv = newInternalValue("key", "value", 0, vtValue)
    discard mt.insert(iv)

    check mt.len() == 1
    check mt.size() > 0

    # Get with MAX seqno should return the value
    let result = mt.get(newSlice("key"), high(SeqNo))
    check result.isSome()
    if result.isSome():
      check result.get.value.asString() == "value"

  test "memtable get with different seqnos":
    let mt = newMemtable(0)

    # Insert version 0
    let iv1 = newInternalValue("key", "value0", 0, vtValue)
    discard mt.insert(iv1)

    # Insert version 1
    let iv2 = newInternalValue("key", "value1", 1, vtValue)
    discard mt.insert(iv2)

    # Get with MAX seqno should return latest (seqno 1)
    let result = mt.get(newSlice("key"), high(SeqNo))
    check result.isSome()
    if result.isSome():
      check result.get.key.seqno == 1
      check result.get.value.asString() == "value1"

    # Get with seqno 0 should return nothing (seqno 0 is reserved)
    let resultAt0 = mt.get(newSlice("key"), 0)
    check resultAt0.isNone()

  test "memtable get nonexistent key":
    let mt = newMemtable(0)
    let iv = newInternalValue("key", "value", 0, vtValue)
    discard mt.insert(iv)

    let result = mt.get(newSlice("nonexistent"), high(SeqNo))
    check result.isNone()

  test "memtable get prefix":
    let mt = newMemtable(0)

    # Insert abc0
    let iv1 = newInternalValue("abc0", "value0", 0, vtValue)
    discard mt.insert(iv1)

    # Insert abc (higher seqno)
    let iv2 = newInternalValue("abc", "value_abc", 255, vtValue)
    discard mt.insert(iv2)

    # Get "abc" should return version with seqno 255
    let result = mt.get(newSlice("abc"), high(SeqNo))
    check result.isSome()
    if result.isSome():
      check result.get.key.seqno == 255

    # Get "abc0" should return version with seqno 0
    let result0 = mt.get(newSlice("abc0"), high(SeqNo))
    check result0.isSome()
    if result0.isSome():
      check result0.get.key.seqno == 0

  test "memtable get highest seqno":
    let mt = newMemtable(0)

    check mt.getHighestSeqno().isNone()

    let iv1 = newInternalValue("a", "v", 10, vtValue)
    discard mt.insert(iv1)
    check mt.getHighestSeqno() == some(10.SeqNo)

    let iv2 = newInternalValue("b", "v", 5, vtValue)
    discard mt.insert(iv2)
    check mt.getHighestSeqno() == some(10.SeqNo) # Still 10 (max)

    let iv3 = newInternalValue("c", "v", 20, vtValue)
    discard mt.insert(iv3)
    check mt.getHighestSeqno() == some(20.SeqNo)

  test "memtable rotation flag":
    let mt = newMemtable(0)

    check mt.isFlaggedForRotation() == false

    mt.flagRotated()

    check mt.isFlaggedForRotation() == true

  test "memtable iter":
    let mt = newMemtable(0)

    let iv1 = newInternalValue("a", "value1", 0, vtValue)
    discard mt.insert(iv1)

    let iv2 = newInternalValue("b", "value2", 1, vtValue)
    discard mt.insert(iv2)

    let iv3 = newInternalValue("c", "value3", 2, vtValue)
    discard mt.insert(iv3)

    var count = 0
    for item in mt.iter():
      count += 1

    check count == 3
