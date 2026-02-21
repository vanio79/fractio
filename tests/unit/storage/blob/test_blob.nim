# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for blob storage functionality

import unittest
import fractio/storage/blob/types
import fractio/storage/blob/writer
import fractio/storage/blob/reader
import std/[os, tempfiles, streams, strutils]

suite "Blob Storage Tests":
  setup:
    let tempDir = createTempDir("blob_test_", "")
    let blobPath = tempDir / "blobs"

  teardown:
    try:
      removeDir(tempDir)
    except OSError:
      discard

  test "Create blob manager":
    let manager = newBlobManager(blobPath)
    check manager.path == blobPath
    check manager.separationThreshold == DEFAULT_SEPARATION_THRESHOLD
    check manager.targetFileSize == DEFAULT_BLOB_FILE_TARGET_SIZE
    check manager.fileCount == 0

  test "Blob manager with custom threshold":
    let manager = newBlobManager(blobPath,
      separationThreshold = 1024'u32,
      targetFileSize = 32 * 1024 * 1024'u64)
    check manager.separationThreshold == 1024'u32
    check manager.targetFileSize == 32 * 1024 * 1024'u64

  test "Should separate - small value":
    let manager = newBlobManager(blobPath, separationThreshold = 1024'u32)
    let smallValue = "x".repeat(512)
    check not manager.shouldSeparate(smallValue)

  test "Should separate - large value":
    let manager = newBlobManager(blobPath, separationThreshold = 1024'u32)
    let largeValue = "x".repeat(2048)
    check manager.shouldSeparate(largeValue)

  test "Should separate - exactly at threshold":
    let manager = newBlobManager(blobPath, separationThreshold = 1024'u32)
    let exactValue = "x".repeat(1024)
    check manager.shouldSeparate(exactValue)

  test "Blob handle serialization/deserialization":
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

  test "Blob file path generation":
    let path = blobFilePath("/data/db", 42'u64)
    check path == "/data/db/blobs/42.blob"

  test "Blob writer creation":
    let writer = newBlobWriter(blobPath, 1'u64)
    check writer.path == blobPath
    check writer.fileId == 1'u64
    check writer.itemCount == 0
    check writer.currentSize == 0'u64

  test "Blob writer should rotate - under limit":
    let writer = newBlobWriter(blobPath, 1'u64, targetSize = 1024'u64)
    writer.currentSize = 512'u64
    check not writer.shouldRotate()

  test "Blob writer should rotate - at limit":
    let writer = newBlobWriter(blobPath, 1'u64, targetSize = 1024'u64)
    writer.currentSize = 1024'u64
    check writer.shouldRotate()

  test "Write and read blob entry":
    createDir(blobPath / "blobs")
    let filePath = blobFilePath(blobPath, 1'u64)

    var stream = newFileStream(filePath, fmWrite)
    check stream != nil

    let writer = newBlobWriter(blobPath, 1'u64)

    # Write header
    let headerResult = writer.writeHeader(stream)
    check headerResult.isOk

    # Write entry
    let key = "testkey"
    let value = "x".repeat(5000)
    let entryResult = writer.writeEntry(stream, key, value, 1'u64)
    check entryResult.isOk

    let handle = entryResult.value
    check handle.fileId == 1'u64
    check handle.size == 5000'u32
    check handle.compressedSize == 0'u32 # No compression in this test
    
    # Finalize
    let finalizeResult = writer.finalize(stream)
    check finalizeResult.isOk

    stream.close()

    # Read back
    let readResult = readEntry(newFileStream(filePath, fmRead), handle)
    check readResult.isOk
    check readResult.value == value

  test "Write and read blob with compression":
    createDir(blobPath / "blobs")
    let filePath = blobFilePath(blobPath, 1'u64)

    var stream = newFileStream(filePath, fmWrite)
    check stream != nil

    let writer = newBlobWriter(blobPath, 1'u64)

    # Write header
    discard writer.writeHeader(stream)

    # Write entry with compression
    let key = "testkey"
    let value = "x".repeat(10000) # Highly compressible
    let entryResult = writer.writeEntry(stream, key, value, 1'u64,
        compress = true)
    check entryResult.isOk

    let handle = entryResult.value
    check handle.fileId == 1'u64
    check handle.size == 10000'u32
    # Compressed size should be smaller
    check handle.compressedSize > 0'u32
    check handle.compressedSize < handle.size

    stream.close()

    # Read back
    let readResult = readEntry(newFileStream(filePath, fmRead), handle)
    check readResult.isOk
    check readResult.value == value

  test "Blob reader cache":
    createDir(blobPath / "blobs")
    let filePath = blobFilePath(blobPath, 1'u64)

    # First create a blob file
    var stream = newFileStream(filePath, fmWrite)
    let writer = newBlobWriter(blobPath, 1'u64)
    discard writer.writeHeader(stream)

    let key = "testkey"
    let value = "testvalue"
    let entryResult = writer.writeEntry(stream, key, value, 1'u64)
    let handle = entryResult.value

    discard writer.finalize(stream)
    stream.close()

    # Now test reading through cache
    let cache = newBlobReaderCache(maxOpenFiles = 4)

    let readResult = cache.readValue(handle, blobPath)
    check readResult.isOk
    check readResult.value == value

    cache.closeAll()

  test "Scan blob file":
    createDir(blobPath / "blobs")
    let filePath = blobFilePath(blobPath, 1'u64)

    var stream = newFileStream(filePath, fmWrite)
    let writer = newBlobWriter(blobPath, 1'u64)
    discard writer.writeHeader(stream)

    # Write multiple entries
    let entries = @[
      ("key1", "value1", 1'u64),
      ("key2", "value2", 2'u64),
      ("key3", "value3", 3'u64)
    ]

    for (key, value, seqno) in entries:
      discard writer.writeEntry(stream, key, value, seqno)

    discard writer.finalize(stream)
    stream.close()

    # Scan the file
    let scanResult = scanBlobFile(filePath)
    check scanResult.isOk

    let scanned = scanResult.value
    check scanned.len == 3

    # Verify entries (order may vary)
    var foundKeys = newSeq[string]()
    for entry in scanned:
      foundKeys.add(entry.key)

    check "key1" in foundKeys
    check "key2" in foundKeys
    check "key3" in foundKeys

  test "Blob file next ID":
    let manager = newBlobManager(blobPath)

    let id1 = manager.nextFileId()
    let id2 = manager.nextFileId()
    let id3 = manager.nextFileId()

    check id1 == 1'u64
    check id2 == 2'u64
    check id3 == 3'u64

  test "Blob file creation":
    let file = newBlobFile(1'u64, "/path/to/blob/1.blob")
    check file.id == 1'u64
    check file.path == "/path/to/blob/1.blob"
    check file.size == 0'u64
    check file.itemCount == 0
    check file.staleBytes == 0'u64

  test "Blob GC stats default":
    let stats = defaultBlobGCStats()
    check stats.totalFiles == 0
    check stats.totalBytes == 0
    check stats.staleBytes == 0
    check stats.liveBytes == 0
    check stats.fragmentationRatio == 0.0

  test "Read blob file header - valid":
    createDir(blobPath / "blobs")
    let filePath = blobFilePath(blobPath, 1'u64)

    var stream = newFileStream(filePath, fmWrite)
    stream.write(BLOB_MAGIC)
    stream.write(uint64(5)) # 5 items
    stream.close()

    var readStream = newFileStream(filePath, fmRead)
    let headerResult = readHeader(readStream)
    check headerResult.isOk
    check headerResult.value.valid
    check headerResult.value.itemCount == 5'u64
    readStream.close()

  test "Read blob file header - invalid magic":
    createDir(blobPath / "blobs")
    let filePath = blobFilePath(blobPath, 1'u64)

    var stream = newFileStream(filePath, fmWrite)
    stream.write("INVALID")
    stream.close()

    var readStream = newFileStream(filePath, fmRead)
    let headerResult = readHeader(readStream)
    check headerResult.isOk
    check not headerResult.value.valid
    readStream.close()
