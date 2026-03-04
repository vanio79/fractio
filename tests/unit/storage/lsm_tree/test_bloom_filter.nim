# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for bloom filter implementation

import unittest
import fractio/storage/lsm_tree/bloom_filter
import fractio/storage/lsm_tree/types
import fractio/storage/lsm_tree/memtable
import fractio/storage/lsm_tree/sstable/writer
import fractio/storage/lsm_tree/sstable/reader
import std/[os, tempfiles, options, streams]

suite "Bloom Filter Basic Tests":
  test "Create bloom filter":
    let bf = newBloomFilter(100, 0.01)
    check bf != nil
    check bf.numBits > 0
    check bf.numHashes > 0

  test "Add and check membership":
    let bf = newBloomFilter(100, 0.01)
    bf.add("key1")
    bf.add("key2")
    bf.add("key3")

    check bf.mayContain("key1")
    check bf.mayContain("key2")
    check bf.mayContain("key3")

  test "Absent keys usually not present":
    let bf = newBloomFilter(100, 0.01)

    # Add some keys
    for i in 0 ..< 50:
      bf.add("key" & $i)

    # Check that most non-existent keys are rejected
    var rejections = 0
    for i in 50 ..< 150:
      if not bf.mayContain("nonexistent" & $i):
        inc rejections

    # Should reject most (bloom filters have false positives but no false negatives)
    check rejections > 80

  test "Clear filter":
    let bf = newBloomFilter(100, 0.01)
    bf.add("key1")
    check bf.mayContain("key1")

    bf.clear()
    check bf.numKeys == 0

  test "Fill ratio":
    let bf = newBloomFilter(100, 0.01)
    check bf.fillRatio() == 0.0

    for i in 0 ..< 50:
      bf.add("key" & $i)

    check bf.fillRatio() > 0.0

suite "Bloom Filter Serialization Tests":
  test "Serialize and deserialize":
    let bf1 = newBloomFilter(100, 0.01)
    bf1.add("key1")
    bf1.add("key2")
    bf1.add("key3")

    # Serialize
    let strm = newStringStream()
    bf1.serialize(strm)

    # Deserialize
    strm.setPosition(0)
    let bf2 = deserializeBloomFilter(strm)

    check bf2.numBits == bf1.numBits
    check bf2.numHashes == bf1.numHashes
    check bf2.numKeys == bf1.numKeys

    # Check that keys are still recognized
    check bf2.mayContain("key1")
    check bf2.mayContain("key2")
    check bf2.mayContain("key3")

    strm.close()

suite "Bloom Filter with SSTable Tests":
  setup:
    let tempDir = createTempDir("bloom_test_", "")

  teardown:
    try:
      removeDir(tempDir)
    except OSError:
      discard

  test "SSTable without bloom filter (v1 format)":
    # Create v1 format SSTable (without bloom filter)
    # This would require a separate writer function
    # For now, skip this test
    skip()

  test "SSTable with bloom filter (v2 format)":
    let mt = newMemtable()
    for i in 0 ..< 100:
      discard mt.insert("key" & $i, "value" & $i, uint64(i), vtValue)

    let sstablePath = tempDir / "test.sst"
    let writeResult = writeMemtable(sstablePath, mt, 100)
    check writeResult.isOk

    let readerResult = openSsTable(sstablePath, 1, nil)
    check readerResult.isOk
    let reader = readerResult.value

    # Check bloom filter exists
    check reader.bloomFilter != nil

    # Check key range
    check reader.smallestKey == "key0"
    check reader.largestKey == "key99"

    # Check mightContain
    check reader.mightContain("key0")
    check reader.mightContain("key50")
    check reader.mightContain("key99")
    check not reader.mightContain("zzz") # Should be rejected by key range

    reader.close()

  test "Bloom filter rejects nonexistent keys":
    let mt = newMemtable()
    for i in 0 ..< 100:
      discard mt.insert("key" & $i, "value" & $i, uint64(i), vtValue)

    let sstablePath = tempDir / "test.sst"
    let writeResult = writeMemtable(sstablePath, mt, 100)
    check writeResult.isOk

    let readerResult = openSsTable(sstablePath, 1, nil)
    check readerResult.isOk
    let reader = readerResult.value

    # Test that bloom filter rejects most nonexistent keys
    var rejections = 0
    for i in 0 ..< 100:
      if not reader.mightContain("nonexistent" & $i):
        inc rejections

    # Most should be rejected
    check rejections > 50

    reader.close()

  test "Bloom filter with key lookups":
    let mt = newMemtable()
    for i in 0 ..< 100:
      discard mt.insert("key" & $i, "value" & $i, uint64(i), vtValue)

    let sstablePath = tempDir / "test.sst"
    let writeResult = writeMemtable(sstablePath, mt, 100)
    check writeResult.isOk

    let readerResult = openSsTable(sstablePath, 1, nil)
    check readerResult.isOk
    let reader = readerResult.value

    # Existing keys should be found
    for i in [0, 25, 50, 75, 99]:
      let val = reader.get("key" & $i)
      check val.isSome
      check val.get == "value" & $i

    # Nonexistent keys should not be found
    let val = reader.get("nonexistent")
    check val.isNone

    reader.close()
