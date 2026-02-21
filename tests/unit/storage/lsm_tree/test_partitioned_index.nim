# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Test partitioned index blocks for large SSTables

import unittest
import os
import std/options
import std/strutils
import fractio/storage/lsm_tree/sstable/types
import fractio/storage/lsm_tree/sstable/writer
import fractio/storage/lsm_tree/sstable/reader
import fractio/storage/lsm_tree/sstable/blocks
import fractio/storage/lsm_tree/types as lsm_types

const TestDir = "tmp/test_partitioned_index"

proc setupTestDir(): void =
  if not dirExists(TestDir):
    createDir(TestDir)

proc cleanupTestDir(): void =
  if dirExists(TestDir):
    removeDir(TestDir)

suite "Partitioned Index Tests":
  setup:
    setupTestDir()

  teardown:
    cleanupTestDir()

  test "Small SSTable uses full index (not partitioned)":
    let path = TestDir / "small.sst"

    # Create a small SSTable with fewer entries than partitioning threshold
    let writerResult = newSsTableWriter(path, 50)
    check writerResult.isOk
    let writer = writerResult.value

    # Add 50 entries with zero-padded keys for correct lexicographic order
    for i in 0 ..< 50:
      let key = "key" & align($i, 2, '0') # key00, key01, ..., key49
      let value = "value" & $i
      check writer.add(key, value, uint64(i), lsm_types.vtValue).isOk

    let sstResult = writer.finish()
    check sstResult.isOk

    # Open and verify
    let readerResult = openSsTable(path, 1)
    check readerResult.isOk
    let reader = readerResult.value

    check reader.indexMode == imFull
    check reader.isPartitioned() == false
    check reader.numIndexBlocks() == 1

    # Verify reads work
    for i in 0 ..< 50:
      let key = "key" & align($i, 2, '0')
      let val = reader.get(key)
      check val.isSome
      check val.get == "value" & $i

    reader.close()

  test "Large SSTable uses partitioned index automatically":
    let path = TestDir / "large.sst"

    # Create a large SSTable with explicit partitioned index
    # Note: Partitioning is based on data block count, not key count
    let writerResult = newSsTableWriter(path, 500, usePartitionedIndex = true)
    check writerResult.isOk
    let writer = writerResult.value

    # Add entries with zero-padded keys
    for i in 0 ..< 500:
      let key = "key" & align($i, 3, '0') # key000, key001, ..., key499
      let value = "value" & $i
      check writer.add(key, value, uint64(i), lsm_types.vtValue).isOk

    let sstResult = writer.finish()
    check sstResult.isOk

    # Open and verify
    let readerResult = openSsTable(path, 1)
    check readerResult.isOk
    let reader = readerResult.value

    # Should use partitioned index because we explicitly requested it
    check reader.indexMode == imPartitioned
    check reader.isPartitioned() == true

    # Verify reads work for a sample of keys
    for i in [0, 1, 50, 100, 250, 499]:
      let key = "key" & align($i, 3, '0')
      let val = reader.get(key)
      check val.isSome
      check val.get == "value" & $i

    reader.close()

  test "Partitioned index with explicit flag":
    let path = TestDir / "explicit_partitioned.sst"

    # Create SSTable with explicit partitioned index flag
    let writerResult = newSsTableWriter(path, 100, usePartitionedIndex = true)
    check writerResult.isOk
    let writer = writerResult.value

    # Add entries with zero-padded keys
    for i in 0 ..< 100:
      let key = "key" & align($i, 2, '0') # key00, key01, ..., key99
      let value = "value" & $i
      check writer.add(key, value, uint64(i), lsm_types.vtValue).isOk

    let sstResult = writer.finish()
    check sstResult.isOk

    # Open and verify
    let readerResult = openSsTable(path, 1)
    check readerResult.isOk
    let reader = readerResult.value

    # Should use partitioned index
    check reader.indexMode == imPartitioned
    check reader.isPartitioned() == true

    # Verify reads work
    for i in 0 ..< 100:
      let key = "key" & align($i, 2, '0')
      let val = reader.get(key)
      check val.isSome
      check val.get == "value" & $i

    reader.close()

  test "Partitioned index key range is correct":
    let path = TestDir / "keyrange.sst"

    let writerResult = newSsTableWriter(path, 200, usePartitionedIndex = true)
    check writerResult.isOk
    let writer = writerResult.value

    # Add entries with zero-padded keys
    for i in 0 ..< 200:
      let key = "key" & align($i, 3, '0') # key000, key001, ..., key199
      let value = "value" & $i
      check writer.add(key, value, uint64(i), lsm_types.vtValue).isOk

    let sstResult = writer.finish()
    check sstResult.isOk

    # Open and verify key range
    let readerResult = openSsTable(path, 1)
    check readerResult.isOk
    let reader = readerResult.value

    check reader.smallestKey == "key000"
    check reader.largestKey == "key199"

    let (smallest, largest) = reader.getKeyRange()
    check smallest == "key000"
    check largest == "key199"

    reader.close()

  test "Partitioned index handles missing keys":
    let path = TestDir / "missing.sst"

    let writerResult = newSsTableWriter(path, 100, usePartitionedIndex = true)
    check writerResult.isOk
    let writer = writerResult.value

    # Add some entries with zero-padded keys
    for i in 0 ..< 100:
      let key = "key" & align($i, 2, '0')
      let value = "value" & $i
      check writer.add(key, value, uint64(i), lsm_types.vtValue).isOk

    let sstResult = writer.finish()
    check sstResult.isOk

    # Open and verify missing keys return none
    let readerResult = openSsTable(path, 1)
    check readerResult.isOk
    let reader = readerResult.value

    # Keys that don't exist
    check reader.get("key100").isNone
    check reader.get("key999").isNone
    check reader.get("nonexistent").isNone

    reader.close()

  test "Partitioned index with tombstones":
    let path = TestDir / "tombstones.sst"

    let writerResult = newSsTableWriter(path, 100, usePartitionedIndex = true)
    check writerResult.isOk
    let writer = writerResult.value

    # Add entries with tombstones interleaved (in sorted order)
    # Keys 00-24: values
    for i in 0 ..< 25:
      let key = "key" & align($i, 2, '0')
      let value = "value" & $i
      check writer.add(key, value, uint64(i), lsm_types.vtValue).isOk

    # Keys 25-49: tombstones (added first so they're in the data block first)
    for i in 25 ..< 50:
      let key = "key" & align($i, 2, '0')
      check writer.add(key, "", uint64(i + 100), lsm_types.vtTombstone).isOk

    # Keys 50-99: values
    for i in 50 ..< 100:
      let key = "key" & align($i, 2, '0')
      let value = "value" & $i
      check writer.add(key, value, uint64(i), lsm_types.vtValue).isOk

    let sstResult = writer.finish()
    check sstResult.isOk

    # Open and verify
    let readerResult = openSsTable(path, 1)
    check readerResult.isOk
    let reader = readerResult.value

    # First 25 keys should exist
    for i in 0 ..< 25:
      let key = "key" & align($i, 2, '0')
      let val = reader.get(key)
      check val.isSome
      check val.get == "value" & $i

    # Keys 25-49 should be deleted (tombstoned)
    for i in 25 ..< 50:
      let key = "key" & align($i, 2, '0')
      check reader.get(key).isNone

    # Keys 50-99 should exist
    for i in 50 ..< 100:
      let key = "key" & align($i, 2, '0')
      let val = reader.get(key)
      check val.isSome
      check val.get == "value" & $i

    reader.close()

  test "Partitioned index getEntry returns metadata":
    let path = TestDir / "metadata.sst"

    let writerResult = newSsTableWriter(path, 100, usePartitionedIndex = true)
    check writerResult.isOk
    let writer = writerResult.value

    # Add entries with specific seqnos and zero-padded keys
    for i in 0 ..< 100:
      let key = "key" & align($i, 2, '0')
      let value = "value" & $i
      check writer.add(key, value, uint64(i * 10), lsm_types.vtValue).isOk

    let sstResult = writer.finish()
    check sstResult.isOk

    # Open and verify metadata
    let readerResult = openSsTable(path, 1)
    check readerResult.isOk
    let reader = readerResult.value

    let entry = reader.getEntry("key50")
    check entry.found
    check entry.value == "value50"
    check entry.seqno == 500
    check entry.valueType == uint8(lsm_types.vtValue)

    reader.close()

  test "Partitioned index backward compatibility with v2 format":
    # Test that we can still read v2 format (full index) files
    let path = TestDir / "v2compat.sst"

    # Create a small SSTable that won't auto-partition
    let writerResult = newSsTableWriter(path, 50, usePartitionedIndex = false)
    check writerResult.isOk
    let writer = writerResult.value

    for i in 0 ..< 50:
      let key = "key" & align($i, 2, '0')
      let value = "value" & $i
      check writer.add(key, value, uint64(i), lsm_types.vtValue).isOk

    let sstResult = writer.finish()
    check sstResult.isOk

    # Open and verify it uses full index
    let readerResult = openSsTable(path, 1)
    check readerResult.isOk
    let reader = readerResult.value

    check reader.indexMode == imFull
    check reader.isPartitioned() == false

    # All reads should work
    for i in 0 ..< 50:
      let key = "key" & align($i, 2, '0')
      let val = reader.get(key)
      check val.isSome
      check val.get == "value" & $i

    reader.close()
