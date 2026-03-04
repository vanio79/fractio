# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for block cache implementation

import unittest
import fractio/storage/lsm_tree/block_cache
import fractio/storage/lsm_tree/types
import fractio/storage/lsm_tree/memtable
import fractio/storage/lsm_tree/sstable/writer
import fractio/storage/lsm_tree/sstable/reader
import fractio/storage/error
import std/[os, tempfiles, options]

suite "Block Cache Basic Tests":
  test "Create block cache":
    let cache = newBlockCache(1024)
    check cache != nil

  test "Get from empty cache returns none":
    let cache = newBlockCache(1024)
    let key = BlockKey(sstableId: 1, blockOffset: 0)
    let result = cache.get(key)
    check result.isNone

  test "Put and get from cache":
    let cache = newBlockCache(1024)
    let key = BlockKey(sstableId: 1, blockOffset: 0)
    let data = "test data"

    cache.put(key, data)
    let result = cache.get(key)
    check result.isSome
    check result.get == data

  test "Put replaces existing entry":
    let cache = newBlockCache(1024)
    let key = BlockKey(sstableId: 1, blockOffset: 0)

    cache.put(key, "old data")
    cache.put(key, "new data")

    let result = cache.get(key)
    check result.isSome
    check result.get == "new data"

  test "Contains returns true for existing entry":
    let cache = newBlockCache(1024)
    let key = BlockKey(sstableId: 1, blockOffset: 0)

    check not cache.contains(key)
    cache.put(key, "data")
    check cache.contains(key)

  test "Remove entry from cache":
    let cache = newBlockCache(1024)
    let key = BlockKey(sstableId: 1, blockOffset: 0)

    cache.put(key, "data")
    check cache.contains(key)

    let removed = cache.remove(key)
    check removed
    check not cache.contains(key)

  test "Remove non-existent entry returns false":
    let cache = newBlockCache(1024)
    let key = BlockKey(sstableId: 1, blockOffset: 0)
    check not cache.remove(key)

suite "Block Cache LRU Tests":
  test "LRU eviction removes oldest entry":
    # Create a small cache that can only hold 10 bytes
    let cache = newBlockCache(10)

    let key1 = BlockKey(sstableId: 1, blockOffset: 0)
    let key2 = BlockKey(sstableId: 1, blockOffset: 1)

    # Put key1 (5 bytes)
    cache.put(key1, "12345")
    check cache.contains(key1)

    # Put key2 (5 bytes) - cache is now full (10 bytes)
    cache.put(key2, "67890")
    check cache.contains(key1)
    check cache.contains(key2)

    # Put key3 (5 bytes) - should evict key1 (oldest)
    let key3 = BlockKey(sstableId: 1, blockOffset: 2)
    cache.put(key3, "abcde")

    check not cache.contains(key1) # Evicted
    check cache.contains(key2)
    check cache.contains(key3)

  test "LRU updates on get":
    let cache = newBlockCache(10)

    let key1 = BlockKey(sstableId: 1, blockOffset: 0)
    let key2 = BlockKey(sstableId: 1, blockOffset: 1)

    cache.put(key1, "12345")
    cache.put(key2, "67890")

    # Access key1 to make it more recent
    discard cache.get(key1)

    # Put key3 - should evict key2 (now oldest)
    let key3 = BlockKey(sstableId: 1, blockOffset: 2)
    cache.put(key3, "abcde")

    check cache.contains(key1) # Still there (accessed recently)
    check not cache.contains(key2) # Evicted
    check cache.contains(key3)

suite "Block Cache Statistics Tests":
  test "Hit and miss counts":
    let cache = newBlockCache(1024)
    let key = BlockKey(sstableId: 1, blockOffset: 0)

    # Miss
    discard cache.get(key)
    check cache.stats().misses == 1
    check cache.stats().hits == 0

    # Put and hit
    cache.put(key, "data")
    discard cache.get(key)
    check cache.stats().misses == 1
    check cache.stats().hits == 1

  test "Hit rate calculation":
    let cache = newBlockCache(1024)
    let key1 = BlockKey(sstableId: 1, blockOffset: 0)
    let key2 = BlockKey(sstableId: 1, blockOffset: 1)

    # 2 misses
    discard cache.get(key1)
    discard cache.get(key2)
    check cache.hitRate() == 0.0

    # 1 hit
    cache.put(key1, "data")
    discard cache.get(key1)
    check cache.hitRate() == 1.0 / 3.0

  test "Size and count":
    let cache = newBlockCache(1024)

    check cache.count() == 0
    check cache.size() == 0

    cache.put(BlockKey(sstableId: 1, blockOffset: 0), "12345")
    check cache.count() == 1
    check cache.size() == 5

    cache.put(BlockKey(sstableId: 1, blockOffset: 1), "67890")
    check cache.count() == 2
    check cache.size() == 10

suite "Block Cache Invalidate SSTable Tests":
  test "Invalidate all blocks for an SSTable":
    let cache = newBlockCache(1024)

    # Add blocks from two SSTables
    cache.put(BlockKey(sstableId: 1, blockOffset: 0), "data1")
    cache.put(BlockKey(sstableId: 1, blockOffset: 1), "data2")
    cache.put(BlockKey(sstableId: 2, blockOffset: 0), "data3")

    check cache.count() == 3

    # Invalidate SSTable 1
    cache.invalidateSsTable(1)

    check cache.count() == 1
    check not cache.contains(BlockKey(sstableId: 1, blockOffset: 0))
    check not cache.contains(BlockKey(sstableId: 1, blockOffset: 1))
    check cache.contains(BlockKey(sstableId: 2, blockOffset: 0))

suite "Block Cache Clear Tests":
  test "Clear removes all entries":
    let cache = newBlockCache(1024)

    cache.put(BlockKey(sstableId: 1, blockOffset: 0), "data1")
    cache.put(BlockKey(sstableId: 2, blockOffset: 0), "data2")

    check cache.count() == 2

    cache.clear()

    check cache.count() == 0
    check cache.size() == 0
    check cache.stats().hits == 0
    check cache.stats().misses == 0

suite "Block Cache with SSTable Reader Tests":
  setup:
    let tempDir = createTempDir("block_cache_test_", "")

  teardown:
    try:
      removeDir(tempDir)
    except OSError:
      discard

  test "SSTable reader uses block cache":
    let memtable = newMemtable()
    for i in 0 ..< 100:
      discard memtable.insert("key" & $i, "value" & $i, uint64(i), vtValue)

    let sstablePath = tempDir / "test.sst"
    let writeResult = writeMemtable(sstablePath, memtable)
    check writeResult.isOk

    # Create block cache
    let cache = newBlockCache(1024 * 1024)

    # Open SSTable with cache
    let readerResult = openSsTable(sstablePath, 1, cache)
    check readerResult.isOk
    let reader = readerResult.value

    # First read - cache miss
    let value1 = reader.get("key50")
    check value1.isSome
    check cache.stats().misses >= 1

    # Second read of same key - should hit cache
    # (Note: This actually opens a new reader each time in the current impl)
    let reader2Result = openSsTable(sstablePath, 1, cache)
    check reader2Result.isOk
    let reader2 = reader2Result.value

    let value2 = reader2.get("key50")
    check value2.isSome
    check cache.stats().hits >= 1

    reader.close()
    reader2.close()

  test "Block cache survives between readers":
    let memtable = newMemtable()
    for i in 0 ..< 50:
      discard memtable.insert("key" & $i, "value" & $i, uint64(i), vtValue)

    let sstablePath = tempDir / "test.sst"
    let writeResult = writeMemtable(sstablePath, memtable)
    check writeResult.isOk

    let cache = newBlockCache(1024 * 1024)

    # First reader - populates cache
    let reader1Result = openSsTable(sstablePath, 1, cache)
    check reader1Result.isOk
    let reader1 = reader1Result.value
    discard reader1.get("key25")
    reader1.close()

    let missesAfterFirst = cache.stats().misses

    # Second reader - should hit cache
    let reader2Result = openSsTable(sstablePath, 1, cache)
    check reader2Result.isOk
    let reader2 = reader2Result.value
    discard reader2.get("key25")
    reader2.close()

    # Should have more hits now
    check cache.stats().hits > 0
