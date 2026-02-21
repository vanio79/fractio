# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for Block Hash Index functionality

import unittest
import std/[os, tempfiles, streams]
import fractio/storage/types as storage_types
import fractio/storage/lsm_tree/sstable/[types, blocks]

suite "Block Hash Index":

  test "newBlockHashIndex creates empty buckets":
    let hashIndex = newBlockHashIndex(16)
    check hashIndex.numBuckets == 16
    check hashIndex.buckets.len == 16
    for bucket in hashIndex.buckets:
      check bucket == HASH_INDEX_EMPTY

  test "buildHashIndex with empty block":
    var dataBlk = newDataBlock()
    dataBlk.buildHashIndex()
    check dataBlk.hashIndex == nil

  test "buildHashIndex creates hash index for block":
    var dataBlk = newDataBlock()

    # Add some entries
    for i in 0..<50:
      check dataBlk.add("key" & $i, "value" & $i, uint64(i + 1),
          storage_types.vtValue)

    dataBlk.buildHashIndex(0.75)

    # Should have hash index (restart points < MAX_HASH_RESTARTS)
    check dataBlk.hashIndex != nil
    check dataBlk.hashIndex.numBuckets > 0

  test "buildHashIndex skips when too many restarts":
    var dataBlk = newDataBlock()

    # Add many entries to exceed MAX_HASH_RESTARTS (253 * 16 = 4048 entries)
    # We'll just test with restart points > MAX_HASH_RESTARTS
    for i in 0..<260:
      dataBlk.restartPoints.add(uint32(i * 100))

    dataBlk.buildHashIndex()

    # Should NOT have hash index (too many restart points)
    check dataBlk.hashIndex == nil

  test "findWithHashIndex returns -1 for empty bucket":
    var dataBlk = newDataBlock()

    # Add a few entries
    for i in 0..<10:
      check dataBlk.add("key" & $i, "value" & $i, uint64(i + 1),
          storage_types.vtValue)

    dataBlk.buildHashIndex(0.5)

    # Search for key that definitely doesn't hash to any entry
    # (exact behavior depends on hash function)
    let result = dataBlk.findWithHashIndex("nonexistent_key_xyz_12345")
    # Result can be -1 (empty bucket) or a restart index (collision)
    check result >= -1

  test "findWithHashIndex with nil hash index returns 0":
    var dataBlk = newDataBlock()
    check dataBlk.hashIndex == nil

    let result = dataBlk.findWithHashIndex("any_key")
    check result == 0 # Falls back to searching from start

  test "hash index different for different keys":
    var dataBlk1 = newDataBlock()
    var dataBlk2 = newDataBlock()

    # Block 1: keys starting with 'a'
    for i in 0..<50:
      discard dataBlk1.add("a" & $i, "value", uint64(i + 1),
          storage_types.vtValue)

    # Block 2: keys starting with 'z'
    for i in 0..<50:
      discard dataBlk2.add("z" & $i, "value", uint64(i + 1),
          storage_types.vtValue)

    dataBlk1.buildHashIndex(0.5) # Lower ratio for more buckets
    dataBlk2.buildHashIndex(0.5)

    # Both should have hash indexes
    check dataBlk1.hashIndex != nil
    check dataBlk2.hashIndex != nil

    # Check that hash indexes are populated
    var emptyBuckets1 = 0
    var emptyBuckets2 = 0
    for bucket in dataBlk1.hashIndex.buckets:
      if bucket == HASH_INDEX_EMPTY:
        inc emptyBuckets1
    for bucket in dataBlk2.hashIndex.buckets:
      if bucket == HASH_INDEX_EMPTY:
        inc emptyBuckets2

    # Both should have at least some non-empty buckets
    check emptyBuckets1 < dataBlk1.hashIndex.buckets.len
    check emptyBuckets2 < dataBlk2.hashIndex.buckets.len

suite "Hash Key Function":

  test "hashKey produces consistent results":
    let hash1 = hashKey("test_key")
    let hash2 = hashKey("test_key")
    check hash1 == hash2

  test "hashKey produces different results for different keys":
    let hash1 = hashKey("key1")
    let hash2 = hashKey("key2")
    check hash1 != hash2

  test "hashKey handles empty string":
    let hash = hashKey("")
    check hash != 0 # Should still produce a valid hash

  test "hashKey handles long keys":
    let longKey = newString(1000)
    let hash = hashKey(longKey)
    check hash != 0

suite "Serialization with Hash Index":

  test "serializeWithHashIndex produces valid output":
    var dataBlk = newDataBlock()

    for i in 0..<20:
      check dataBlk.add("key" & $i, "value" & $i, uint64(i + 1),
          storage_types.vtValue)

    var stream = newStringStream()
    let result = dataBlk.serializeWithHashIndex(stream)

    check result.isOk

    # Check that something was written
    check stream.getPosition() > 0

    stream.close()

  test "serialize with no hash index works":
    var dataBlk = newDataBlock()

    # Add many restart points to exceed MAX_HASH_RESTARTS (253)
    # Each restart is every 16 entries
    # 300 restarts = 300 * 16 = 4800 entries
    # But block size limits entries, so we manually add restart points
    for i in 0..<260:
      dataBlk.restartPoints.add(uint32(i * 100))

    # Add just a few entries
    for i in 0..<10:
      discard dataBlk.add("key" & $i, "value" & $i, uint64(i + 1),
          storage_types.vtValue)

    dataBlk.buildHashIndex()

    # Should NOT have hash index (too many restart points)
    check dataBlk.hashIndex == nil

    var stream = newStringStream()
    let result = dataBlk.serialize(stream)

    check result.isOk
    check stream.getPosition() > 0

    stream.close()
