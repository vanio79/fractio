# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Integration test for blob read path through LSM tree
## This tests the complete flow: insert -> memtable -> SSTable flush -> blob write -> read

import unittest
import fractio/storage/lsm_tree/lsm_tree
import fractio/storage/lsm_tree/types
import fractio/storage/blob/types
import fractio/storage/blob/writer
import fractio/storage/blob/reader
import fractio/storage/keyspace/options
import fractio/storage/types as storage_types
import std/[os, tempfiles, strutils, options, streams]

suite "Blob Read Path Integration Tests":
  setup:
    let tempDir = createTempDir("blob_read_test_", "")
    let dbPath = tempDir / "db"

  teardown:
    try:
      removeDir(tempDir)
    except OSError:
      discard

  test "Read regular value (non-blob) with KV separation enabled":
    var config = newConfig(dbPath)
    config.kvSeparationOpts = some(KvSeparationOptions(
      separationThreshold: 1000'u32, # High threshold
      fileTargetSize: 1024 * 1024'u64,
      compression: storage_types.ctNone,
      ageCutoff: 0.5,
      stalenessThreshold: 0.3
    ))
    config.maxMemtableSize = 1024 * 1024 # 1 MB

    let tree = open(config)
    defer:
      try:
        removeDir(dbPath)
      except CatchableError:
        discard

    # Insert a small value that won't go to blob
    let smallKey = "small_key"
    let smallValue = "small_value"

    let result = tree.insert(smallKey, smallValue, 1'u64)
    check result.itemSize > 0

    # Read back
    let readValue = tree.get(smallKey, 1'u64)

    check readValue.isSome
    check readValue.get() == smallValue

  test "Read non-existent key with blob enabled":
    var config = newConfig(dbPath)
    config.kvSeparationOpts = some(KvSeparationOptions(
      separationThreshold: 100'u32,
      fileTargetSize: 1024 * 1024'u64,
      compression: storage_types.ctNone,
      ageCutoff: 0.5,
      stalenessThreshold: 0.3
    ))

    let tree = open(config)
    defer:
      try:
        removeDir(dbPath)
      except CatchableError:
        discard

    # Try to read non-existent key
    let readValue = tree.get("nonexistent", 1'u64)

    check readValue.isNone

  test "Blob handle serialization roundtrip":
    let handle = BlobHandle(
      fileId: 12345'u64,
      offset: 67890'u64,
      size: 1000'u32,
      compressedSize: 800'u32
    )

    let serialized = serializeHandle(handle)
    check serialized.len == 24

    let deserialized = deserializeHandle(serialized)
    check deserialized.fileId == handle.fileId
    check deserialized.offset == handle.offset
    check deserialized.size == handle.size
    check deserialized.compressedSize == handle.compressedSize

  test "Resolve blob value helper function":
    let tempDir2 = createTempDir("blob_resolve_test_", "")
    defer:
      try:
        removeDir(tempDir2)
      except:
        discard

    let blobPath = tempDir2 / "blobs"
    createDir(blobPath)

    # Create a blob file with a value
    let blobFileId = 1'u64
    let filePath = blobFilePath(tempDir2, blobFileId)
    var stream = newFileStream(filePath, fmWrite)
    let writer = newBlobWriter(tempDir2, blobFileId)
    discard writer.writeHeader(stream)

    let testKey = "test_key"
    let testValue = "test_blob_value_12345"
    let entryResult = writer.writeEntry(stream, testKey, testValue, 1'u64)
    check entryResult.isOk
    let handle = entryResult.value

    discard writer.finalize(stream)
    stream.close()

    # Create blob reader cache and resolve the value
    var blobCache = newBlobReaderCache()
    let serializedHandle = serializeHandle(handle)

    let resolvedValue = resolveBlobValue(serializedHandle, tempDir2, blobCache)

    check resolvedValue.isSome
    check resolvedValue.get() == testValue
