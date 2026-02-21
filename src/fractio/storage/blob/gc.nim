# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Blob Garbage Collection
##
## Reclaims space from blob files by removing stale (overwritten/deleted) entries.

import ./types
import ./writer
import ./reader
import ../error
import std/[os, streams, tables, locks, times, algorithm]

type
  BlobGCResult* = object
    ## Result of a blob GC operation.
    filesProcessed*: int
    bytesReclaimed*: uint64
    bytesRewritten*: uint64
    duration*: float64 # seconds

  BlobGCMetrics* = object
    ## Metrics for blob GC activity.
    totalRuns*: uint64
    totalBytesReclaimed*: uint64
    totalFilesRewritten*: uint64
    lastRunTime*: Time

# Create default blob GC result
proc defaultBlobGCResult*(): BlobGCResult =
  BlobGCResult(
    filesProcessed: 0,
    bytesReclaimed: 0,
    bytesRewritten: 0,
    duration: 0.0
  )

# Create default blob GC metrics
proc defaultBlobGCMetrics*(): BlobGCMetrics =
  BlobGCMetrics(
    totalRuns: 0,
    totalBytesReclaimed: 0,
    totalFilesRewritten: 0,
    lastRunTime: getTime()
  )

# Check if a blob file should be garbage collected
proc shouldGCFile*(file: BlobFile, stalenessThreshold: float64): bool =
  ## Returns true if the blob file should be garbage collected.
  ## A file is a GC candidate if:
  ## 1. It has stale bytes above the threshold
  ## 2. It's not the currently active blob file

  if file.size == 0:
    return false

  # Calculate staleness ratio
  let stalenessRatio = float64(file.staleBytes) / float64(file.size)
  return stalenessRatio >= stalenessThreshold

# Collect live blob references from SSTables
# This is a placeholder - the actual implementation would scan SSTables
# for vtIndirection entries and collect the blob handles
type
  LiveBlobRefs* = Table[BlobFileId, seq[uint64]] # fileId -> seq[offset]

proc newLiveBlobRefs*(): LiveBlobRefs =
  initTable[BlobFileId, seq[uint64]]()

proc addRef*(refs: var LiveBlobRefs, fileId: BlobFileId, offset: uint64) =
  ## Adds a live blob reference.
  if fileId notin refs:
    refs[fileId] = @[]
  refs[fileId].add(offset)

# Rewrite a blob file, keeping only live entries
proc rewriteBlobFile*(manager: BlobManager,
                      fileId: BlobFileId,
                      liveOffsets: seq[uint64],
                      newFileId: BlobFileId): StorageResult[uint64] =
  ## Rewrites a blob file, keeping only entries at the specified offsets.
  ## Returns the number of bytes reclaimed.

  let oldPath = blobFilePath(manager.path, fileId)
  let newPath = blobFilePath(manager.path, newFileId)

  if not fileExists(oldPath):
    return err[uint64, StorageError](StorageError(
      kind: seIo,
      ioError: "Blob file not found: " & oldPath
    ))

  # Open old file for reading
  var oldStream = newFileStream(oldPath, fmRead)
  if oldStream == nil:
    return err[uint64, StorageError](StorageError(
      kind: seIo,
      ioError: "Failed to open blob file: " & oldPath
    ))

  # Read header
  let headerResult = readHeader(oldStream)
  if not headerResult.isOk or not headerResult.value.valid:
    oldStream.close()
    return err[uint64, StorageError](StorageError(
      kind: seIo,
      ioError: "Invalid blob file header: " & oldPath
    ))

  # Create new file
  createDir(parentDir(newPath))
  var newStream = newFileStream(newPath, fmWrite)
  if newStream == nil:
    oldStream.close()
    return err[uint64, StorageError](StorageError(
      kind: seIo,
      ioError: "Failed to create new blob file: " & newPath
    ))

  let writer = newBlobWriter(manager.path, newFileId)
  discard writer.writeHeader(newStream)

  var oldSize = 0'u64
  var newSize = 0'u64
  var itemsWritten = 0'u32

  # Sort live offsets for sequential reading
  var sortedOffsets = liveOffsets
  sort(sortedOffsets)

  # Copy live entries
  for offset in sortedOffsets:
    oldStream.setPosition(int(offset))

    # Read entry header
    let keyLen = oldStream.readUInt32()
    let valueLen = oldStream.readUInt32()
    let seqno = oldStream.readUInt64()
    let compressed = oldStream.readUInt8() == 1
    discard oldStream.readUInt8() # Reserved
    discard oldStream.readUInt8() # Reserved
    discard oldStream.readUInt8() # Reserved
    
    # Read key
    let key = oldStream.readStr(int(keyLen))

    # Read value
    let actualValueLen = if compressed: valueLen else: valueLen
    let value = oldStream.readStr(int(actualValueLen))

    # Write to new file
    let entryResult = writer.writeEntry(newStream, key, value, seqno,
        compress = compressed)
    if entryResult.isOk:
      newSize += uint64(keyLen) + uint64(actualValueLen) + 16 # header + data
      itemsWritten.inc()

    oldSize += uint64(keyLen) + uint64(actualValueLen) + 16

  # Finalize new file
  discard writer.finalize(newStream)

  newStream.close()
  oldStream.close()

  # Update manager
  manager.lock.acquire()
  defer: manager.lock.release()

  # Get old file size
  let oldFileSize = if fileId in manager.files: manager.files[
      fileId].size else: 0'u64

  # Remove old file entry
  manager.files.del(fileId)

  # Add new file entry
  var newFile = newBlobFile(newFileId, newPath)
  newFile.size = newSize
  newFile.itemCount = itemsWritten
  manager.files[newFileId] = newFile

  # Delete old file
  try:
    removeFile(oldPath)
  except OSError:
    discard # Ignore deletion errors
  
  # Calculate reclaimed bytes
  let reclaimed = if oldFileSize > newSize: oldFileSize - newSize else: 0'u64

  return ok[uint64, StorageError](reclaimed)

# Run garbage collection on a single blob file
proc gcBlobFile*(manager: BlobManager,
                 fileId: BlobFileId,
                 liveOffsets: seq[uint64]): StorageResult[uint64] =
  ## Runs garbage collection on a single blob file.
  ## Returns the number of bytes reclaimed.

  manager.lock.acquire()
  let shouldGC = fileId in manager.files and
                 shouldGCFile(manager.files[fileId], manager.stalenessThreshold)
  manager.lock.release()

  if not shouldGC:
    return ok[uint64, StorageError](0'u64)

  # Allocate new file ID
  let newFileId = manager.nextFileId()

  # Rewrite the file
  result = rewriteBlobFile(manager, fileId, liveOffsets, newFileId)

# Run full blob garbage collection
proc runBlobGC*(manager: BlobManager,
                liveRefs: LiveBlobRefs): BlobGCResult =
  ## Runs garbage collection on all eligible blob files.
  ## liveRefs contains all live blob references from SSTables.

  let startTime = cpuTime()
  result = defaultBlobGCResult()

  # Find files to GC
  var filesToGC: seq[BlobFileId] = @[]

  manager.lock.acquire()
  for fileId, file in manager.files:
    if shouldGCFile(file, manager.stalenessThreshold):
      filesToGC.add(fileId)
  manager.lock.release()

  # Process each file
  for fileId in filesToGC:
    # Get live offsets for this file
    let liveOffsets = if fileId in liveRefs: liveRefs[fileId] else: @[]

    # Run GC on this file
    let gcResult = gcBlobFile(manager, fileId, liveOffsets)
    if gcResult.isOk:
      result.filesProcessed.inc()
      result.bytesReclaimed += gcResult.value

  result.duration = cpuTime() - startTime

# Get blob GC statistics
proc getGCStats*(manager: BlobManager): BlobGCStats =
  ## Returns statistics about blob files for GC decisions.

  manager.lock.acquire()
  defer: manager.lock.release()

  result = defaultBlobGCStats()
  result.totalFiles = manager.files.len

  for file in manager.files.values:
    result.totalBytes += file.size
    result.staleBytes += file.staleBytes

  result.liveBytes = result.totalBytes - result.staleBytes

  if result.totalBytes > 0:
    result.fragmentationRatio = float64(result.staleBytes) / float64(
        result.totalBytes)
