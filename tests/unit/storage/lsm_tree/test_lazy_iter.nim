# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for lazy iterator implementation

import unittest
import fractio/storage/lsm_tree/[types, memtable, lazy_iter]
import fractio/storage/lsm_tree/sstable/writer
import std/[os, tempfiles, options, strutils]

suite "Memtable Iterator Tests":
  test "Iterate empty memtable":
    let memtable = newMemtable()
    let iter = newMemtableIter(memtable)
    check not iter.isValid()

  test "Iterate single entry":
    let memtable = newMemtable()
    discard memtable.insert("key1", "value1", 1, vtValue)
    let iter = newMemtableIter(memtable)
    check iter.isValid()
    let current = iter.current()
    check current.isSome()
    check current.get().key == "key1"
    check current.get().value == "value1"
    check current.get().seqno == 1

  test "Iterate multiple entries in order":
    let memtable = newMemtable()
    discard memtable.insert("key3", "value3", 3, vtValue)
    discard memtable.insert("key1", "value1", 1, vtValue)
    discard memtable.insert("key2", "value2", 2, vtValue)
    let iter = newMemtableIter(memtable)

    var keys: seq[string] = @[]
    while iter.isValid():
      let current = iter.current()
      if current.isSome():
        keys.add(current.get().key)
      discard iter.next()

    check keys == @["key1", "key2", "key3"]

suite "SSTable Block Iterator Tests":
  setup:
    let tempDir = createTempDir("lazy_iter_test_", "")

  teardown:
    try:
      removeDir(tempDir)
    except OSError:
      discard

  test "Iterate single block":
    let memtable = newMemtable()
    for i in 0 ..< 10:
      discard memtable.insert("key" & $i, "value" & $i, uint64(i), vtValue)

    let writeResult = writeMemtable(tempDir / "test.sst", memtable)
    check writeResult.isOk

    let iterResult = newSsTableIter(tempDir / "test.sst")
    check iterResult.isOk
    let iter = iterResult.value

    var count = 0
    while iter.isValid:
      let current = iter.current()
      check current.isSome
      inc count
      discard iter.next()

    check count == 10
    iter.close()

suite "Merge Iterator Tests":
  setup:
    let tempDir = createTempDir("lazy_merge_test_", "")

  teardown:
    try:
      removeDir(tempDir)
    except OSError:
      discard

  test "Merge single memtable":
    let memtable = newMemtable()
    discard memtable.insert("key1", "value1", 1, vtValue)
    discard memtable.insert("key2", "value2", 2, vtValue)

    let mergeIter = newMergeIterator(100)
    mergeIter.addMemtable(memtable)

    # Must call next() first to initialize
    var count = 0
    if mergeIter.next():
      while mergeIter.isValid:
        let current = mergeIter.current()
        check current.isSome
        inc count
        if not mergeIter.next():
          break

    check count == 2
    mergeIter.close()

  test "Merge respects snapshot seqno":
    let memtable = newMemtable()
    discard memtable.insert("key1", "value1", 1, vtValue)
    discard memtable.insert("key2", "value2", 10, vtValue)
    discard memtable.insert("key3", "value3", 20, vtValue)

    # Only see entries with seqno <= 5
    let mergeIter = newMergeIterator(5)
    mergeIter.addMemtable(memtable)

    var count = 0
    if mergeIter.next():
      while mergeIter.isValid:
        let current = mergeIter.current()
        check current.isSome
        check current.get().seqno <= 5
        inc count
        if not mergeIter.next():
          break

    check count == 1 # Only key1
    mergeIter.close()

suite "Range Iterator Tests":
  setup:
    let tempDir = createTempDir("lazy_range_test_", "")

  teardown:
    try:
      removeDir(tempDir)
    except OSError:
      discard

  test "Range iteration":
    let memtable = newMemtable()
    for i in 0 ..< 50:
      discard memtable.insert("key" & intToStr(i, 4), "value" & $i, uint64(i), vtValue)

    let mergeIter = newMergeIterator(1000)
    mergeIter.addMemtable(memtable)

    let rangeIter = newRangeIterator(mergeIter, some("key0010"), some("key0020"))

    var keys: seq[string] = @[]
    while rangeIter.isValid:
      let current = rangeIter.current()
      check current.isSome
      keys.add(current.get().key)
      discard rangeIter.next()

    check keys.len == 11 # key0010 to key0020 inclusive
    check keys[0] == "key0010"
    check keys[^1] == "key0020"
    rangeIter.close()

suite "Prefix Iterator Tests":
  setup:
    let tempDir = createTempDir("lazy_prefix_test_", "")

  teardown:
    try:
      removeDir(tempDir)
    except OSError:
      discard

  test "Prefix iteration":
    let memtable = newMemtable()
    discard memtable.insert("prefix1_a", "value1", 1, vtValue)
    discard memtable.insert("prefix1_b", "value2", 2, vtValue)
    discard memtable.insert("prefix2_a", "value3", 3, vtValue)
    discard memtable.insert("prefix1_c", "value4", 4, vtValue)
    discard memtable.insert("other", "value5", 5, vtValue)

    let mergeIter = newMergeIterator(1000)
    mergeIter.addMemtable(memtable)

    let prefixIter = newPrefixIterator(mergeIter, "prefix1_")

    var keys: seq[string] = @[]
    while prefixIter.isValid:
      let current = prefixIter.current()
      check current.isSome
      keys.add(current.get().key)
      discard prefixIter.next()

    check keys.len == 3
    for k in keys:
      check k.startsWith("prefix1_")
    prefixIter.close()
