# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for the Descriptor Table (file handle cache)

import unittest
import std/[os, tempfiles]
import fractio/storage/[descriptor_table, error]
import fractio/storage/types as storage_types
import fractio/storage/lsm_tree/[types as lsm_types, sstable/writer]

suite "Descriptor Table":

  test "newDescriptorTable creates empty table":
    let table = newDescriptorTable(10)
    check table.len == 0
    check table.maxFiles == 10

  test "getStream opens and caches file":
    let tempDir = createTempDir("desc_test_", "")
    defer: removeDir(tempDir)

    # Create a test file
    let testPath = tempDir / "test.sst"
    let writerResult = newSsTableWriter(testPath, 10)
    check writerResult.isOk
    let writer = writerResult.value
    check writer.add("key1", "value1", 1, storage_types.vtValue).isOk
    check writer.finish().isOk

    let table = newDescriptorTable(10)
    check table.len == 0

    let streamResult = table.getStream(testPath)
    check streamResult.isOk
    check table.len == 1

    table.releaseStream(testPath)

    table.close()

  test "getStream returns cached stream on second call":
    let tempDir = createTempDir("desc_test_", "")
    defer: removeDir(tempDir)

    # Create a test file
    let testPath = tempDir / "test.sst"
    let writerResult = newSsTableWriter(testPath, 10)
    check writerResult.isOk
    let writer = writerResult.value
    check writer.add("key1", "value1", 1, storage_types.vtValue).isOk
    check writer.finish().isOk

    let table = newDescriptorTable(10)

    # First access - miss
    let stream1Result = table.getStream(testPath)
    check stream1Result.isOk
    let stream1 = stream1Result.value
    table.releaseStream(testPath)

    # Second access - hit
    let stream2Result = table.getStream(testPath)
    check stream2Result.isOk
    let stream2 = stream2Result.value
    check stream1 == stream2 # Same stream object

    table.releaseStream(testPath)

    let stats = table.stats()
    check stats.hits == 1
    check stats.misses == 1
    check stats.size == 1

    table.close()

  test "getStream fails for non-existent file":
    let table = newDescriptorTable(10)

    let result = table.getStream("/nonexistent/path/file.sst")
    check result.isErr
    check result.error.kind == seIo

    table.close()

  test "LRU eviction works":
    let tempDir = createTempDir("desc_test_", "")
    defer: removeDir(tempDir)

    let table = newDescriptorTable(3) # Small limit

    # Create test files
    var paths: seq[string] = @[]
    for i in 0..<5:
      let path = tempDir / ("test" & $i & ".sst")
      let writerResult = newSsTableWriter(path, 10)
      check writerResult.isOk
      let writer = writerResult.value
      check writer.add("key" & $i, "value" & $i, uint64(i + 1), vtValue).isOk
      check writer.finish().isOk
      paths.add(path)

    # Open first 3 files
    for i in 0..<3:
      let result = table.getStream(paths[i])
      check result.isOk
      table.releaseStream(paths[i])

    check table.len == 3

    # Open 4th file - should evict oldest (paths[0])
    let result4 = table.getStream(paths[3])
    check result4.isOk
    table.releaseStream(paths[3])

    check table.len == 3
    check not table.contains(paths[0]) # Evicted

    let stats = table.stats()
    check stats.evictions == 1

    table.close()

  test "releaseStream allows eviction":
    let tempDir = createTempDir("desc_test_", "")
    defer: removeDir(tempDir)

    let table = newDescriptorTable(2)

    # Create test files
    var paths: seq[string] = @[]
    for i in 0..<3:
      let path = tempDir / ("test" & $i & ".sst")
      let writerResult = newSsTableWriter(path, 10)
      check writerResult.isOk
      let writer = writerResult.value
      check writer.add("key" & $i, "value" & $i, uint64(i + 1), vtValue).isOk
      check writer.finish().isOk
      paths.add(path)

    # Open first file but don't release
    let result1 = table.getStream(paths[0])
    check result1.isOk
    # NOT released - in use

    # Open second file and release
    let result2 = table.getStream(paths[1])
    check result2.isOk
    table.releaseStream(paths[1])

    # Try to open third - should evict paths[1] not paths[0] (in use)
    let result3 = table.getStream(paths[2])
    check result3.isOk
    table.releaseStream(paths[2])

    check table.contains(paths[0]) # Still in cache (in use)
    check not table.contains(paths[1]) # Evicted

    table.releaseStream(paths[0])
    table.close()

  test "remove deletes from cache":
    let tempDir = createTempDir("desc_test_", "")
    defer: removeDir(tempDir)

    let testPath = tempDir / "test.sst"
    let writerResult = newSsTableWriter(testPath, 10)
    check writerResult.isOk
    let writer = writerResult.value
    check writer.add("key1", "value1", 1, storage_types.vtValue).isOk
    check writer.finish().isOk

    let table = newDescriptorTable(10)

    let streamResult = table.getStream(testPath)
    check streamResult.isOk
    table.releaseStream(testPath)

    check table.contains(testPath)

    table.remove(testPath)

    check not table.contains(testPath)
    check table.len == 0

    table.close()

  test "clear closes all files":
    let tempDir = createTempDir("desc_test_", "")
    defer: removeDir(tempDir)

    let table = newDescriptorTable(10)

    # Create and open multiple files
    for i in 0..<5:
      let path = tempDir / ("test" & $i & ".sst")
      let writerResult = newSsTableWriter(path, 10)
      check writerResult.isOk
      let writer = writerResult.value
      check writer.add("key" & $i, "value" & $i, uint64(i + 1), vtValue).isOk
      check writer.finish().isOk

      let result = table.getStream(path)
      check result.isOk
      table.releaseStream(path)

    check table.len == 5

    table.clear()

    check table.len == 0

    table.close()

  test "prefetch loads file into cache":
    let tempDir = createTempDir("desc_test_", "")
    defer: removeDir(tempDir)

    let testPath = tempDir / "test.sst"
    let writerResult = newSsTableWriter(testPath, 10)
    check writerResult.isOk
    let writer = writerResult.value
    check writer.add("key1", "value1", 1, storage_types.vtValue).isOk
    check writer.finish().isOk

    let table = newDescriptorTable(10)

    check not table.contains(testPath)

    check table.prefetch(testPath)

    check table.contains(testPath)

    table.close()

  test "hitRate calculates correctly":
    let tempDir = createTempDir("desc_test_", "")
    defer: removeDir(tempDir)

    let testPath = tempDir / "test.sst"
    let writerResult = newSsTableWriter(testPath, 10)
    check writerResult.isOk
    let writer = writerResult.value
    check writer.add("key1", "value1", 1, storage_types.vtValue).isOk
    check writer.finish().isOk

    let table = newDescriptorTable(10)

    # Empty cache - 0% hit rate
    check table.hitRate() == 0.0

    # First access - miss
    let r1 = table.getStream(testPath)
    check r1.isOk
    table.releaseStream(testPath)
    check table.hitRate() == 0.0 # 0 hits, 1 miss
    
    # Second access - hit
    let r2 = table.getStream(testPath)
    check r2.isOk
    table.releaseStream(testPath)
    check table.hitRate() == 0.5 # 1 hit, 1 miss
    
    # Third access - hit
    let r3 = table.getStream(testPath)
    check r3.isOk
    table.releaseStream(testPath)
    check abs(table.hitRate() - 0.666) < 0.01 # 2 hits, 1 miss

    table.close()
