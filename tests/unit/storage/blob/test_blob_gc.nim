# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for blob garbage collection functionality

import unittest
import fractio/storage/blob/types
import fractio/storage/blob/writer
import fractio/storage/blob/reader
import fractio/storage/blob/gc
import std/[os, tempfiles, streams, strutils, times, tables]

suite "Blob GC Tests":
  setup:
    let tempDir = createTempDir("blob_gc_test_", "")
    let blobPath = tempDir / "blobs"

  teardown:
    try:
      removeDir(tempDir)
    except OSError:
      discard

  test "Default blob GC result":
    let result = defaultBlobGCResult()
    check result.filesProcessed == 0
    check result.bytesReclaimed == 0
    check result.bytesRewritten == 0
    check result.duration == 0.0

  test "Default blob GC metrics":
    let metrics = defaultBlobGCMetrics()
    check metrics.totalRuns == 0
    check metrics.totalBytesReclaimed == 0
    check metrics.totalFilesRewritten == 0

  test "Should GC file - no stale bytes":
    let file = newBlobFile(1'u64, "/path/to/blob/1.blob")
    file.size = 1024
    file.staleBytes = 0

    check not shouldGCFile(file, 0.3)

  test "Should GC file - below threshold":
    let file = newBlobFile(1'u64, "/path/to/blob/1.blob")
    file.size = 1000
    file.staleBytes = 100 # 10% stale

    check not shouldGCFile(file, 0.3) # Threshold 30%

  test "Should GC file - above threshold":
    let file = newBlobFile(1'u64, "/path/to/blob/1.blob")
    file.size = 1000
    file.staleBytes = 500 # 50% stale

    check shouldGCFile(file, 0.3) # Threshold 30%

  test "Should GC file - at threshold":
    let file = newBlobFile(1'u64, "/path/to/blob/1.blob")
    file.size = 1000
    file.staleBytes = 300 # 30% stale

    check shouldGCFile(file, 0.3) # Threshold 30%

  test "Should GC file - zero size":
    let file = newBlobFile(1'u64, "/path/to/blob/1.blob")
    file.size = 0
    file.staleBytes = 0

    check not shouldGCFile(file, 0.3)

  test "Live blob refs":
    var refs: LiveBlobRefs = initTable[BlobFileId, seq[uint64]]()

    check refs.len == 0

    gc.addRef(refs, 1'u64, 100'u64)
    gc.addRef(refs, 1'u64, 200'u64)
    gc.addRef(refs, 2'u64, 300'u64)

    check refs.len == 2
    check 1'u64 in refs
    check 2'u64 in refs
    check refs[1'u64].len == 2
    check refs[2'u64].len == 1
    check 100'u64 in refs[1'u64]
    check 200'u64 in refs[1'u64]
    check 300'u64 in refs[2'u64]

  test "Get blob GC stats":
    let manager = newBlobManager(blobPath)

    # Add some files
    let file1 = newBlobFile(1'u64, blobPath / "1.blob")
    file1.size = 1000
    file1.staleBytes = 200

    let file2 = newBlobFile(2'u64, blobPath / "2.blob")
    file2.size = 2000
    file2.staleBytes = 400

    manager.files[1'u64] = file1
    manager.files[2'u64] = file2

    let stats = manager.getGCStats()

    check stats.totalFiles == 2
    check stats.totalBytes == 3000
    check stats.staleBytes == 600
    check stats.liveBytes == 2400
    check abs(stats.fragmentationRatio - 0.2) < 0.01 # 20%

  test "Get blob GC stats - empty manager":
    let manager = newBlobManager(blobPath)
    let stats = manager.getGCStats()

    check stats.totalFiles == 0
    check stats.totalBytes == 0
    check stats.staleBytes == 0
    check stats.liveBytes == 0
    check stats.fragmentationRatio == 0.0

  test "Rewrite blob file - keeps live entries":
    createDir(blobPath / "blobs")

    # Create original blob file
    let filePath = blobFilePath(blobPath, 1'u64)
    var stream = newFileStream(filePath, fmWrite)
    let writer = newBlobWriter(blobPath, 1'u64)
    discard writer.writeHeader(stream)

    let entries = @[
      ("key1", "value1", 1'u64),
      ("key2", "value2", 2'u64),
      ("key3", "value3", 3'u64)
    ]

    var offsets: seq[uint64] = @[]
    for (key, value, seqno) in entries:
      let offset = stream.getPosition()
      offsets.add(uint64(offset))
      discard writer.writeEntry(stream, key, value, seqno)

    discard writer.finalize(stream)
    stream.close()

    # Create manager and add file
    let manager = newBlobManager(blobPath)
    let file1 = newBlobFile(1'u64, filePath)
    file1.size = uint64(getFileSize(filePath))
    file1.itemCount = 3
    file1.staleBytes = file1.size div 3 # Simulate 1/3 stale
    manager.files[1'u64] = file1

    # Rewrite keeping only key2 (offset[1])
    let liveOffsets = @[offsets[1]]
    let rewriteResult = rewriteBlobFile(manager, 1'u64, liveOffsets, 2'u64)

    check rewriteResult.isOk

    # Verify new file has only the live entry
    let newFilePath = blobFilePath(blobPath, 2'u64)
    check fileExists(newFilePath)

    let scanResult = scanBlobFile(newFilePath)
    check scanResult.isOk
    check scanResult.value.len == 1
    check scanResult.value[0].key == "key2"

  test "Run blob GC - no files need GC":
    let manager = newBlobManager(blobPath)
    manager.stalenessThreshold = 0.3

    # Add file with no stale bytes
    let file = newBlobFile(1'u64, blobPath / "1.blob")
    file.size = 1000
    file.staleBytes = 0
    manager.files[1'u64] = file

    let refs: LiveBlobRefs = initTable[BlobFileId, seq[uint64]]()
    let result = runBlobGC(manager, refs)

    check result.filesProcessed == 0
    check result.bytesReclaimed == 0

  test "GC blob file - file not in manager":
    let manager = newBlobManager(blobPath)

    let liveOffsets = @[100'u64, 200'u64]
    let result = gcBlobFile(manager, 999'u64, liveOffsets) # Non-existent file

    check result.isOk
    check result.value == 0
