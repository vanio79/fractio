# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Blob File Reader
##
## Reads blob files containing large values that are separated from SSTables.

import ./types
import ./writer
import ../error
import ../lsm_tree/compression as lsm_compression
import std/[os, streams, tables, locks]

# Blob file reader cache (simple LRU-like cache)
type
  BlobReaderCache* = ref object
    ## A simple cache for open blob file streams.
    maxOpenFiles*: int
    openFiles*: Table[BlobFileId, FileStream]
    paths*: Table[BlobFileId, string]
    lock*: Lock

# Create a new blob reader cache
proc newBlobReaderCache*(maxOpenFiles: int = 16): BlobReaderCache =
  result = BlobReaderCache(
    maxOpenFiles: maxOpenFiles,
    openFiles: initTable[BlobFileId, FileStream](),
    paths: initTable[BlobFileId, string]()
  )
  initLock(result.lock)

# Get or open a blob file stream
proc getStream*(cache: BlobReaderCache, fileId: BlobFileId,
    basePath: string): StorageResult[FileStream] =
  ## Gets or opens a blob file stream.
  cache.lock.acquire()
  defer: cache.lock.release()

  # Check if already open
  if fileId in cache.openFiles:
    return ok[FileStream, StorageError](cache.openFiles[fileId])

  # Close oldest file if at capacity
  if cache.openFiles.len >= cache.maxOpenFiles:
    # Simple LRU: close first file
    var oldestId: BlobFileId = 0
    for id in cache.openFiles.keys:
      oldestId = id
      break
    if oldestId > 0:
      cache.openFiles[oldestId].close()
      cache.openFiles.del(oldestId)

  # Open new file
  let path = blobFilePath(basePath, fileId)
  if not fileExists(path):
    return err[FileStream, StorageError](StorageError(
      kind: seIo,
      ioError: "Blob file not found: " & path
    ))

  var stream = newFileStream(path, fmRead)
  if stream == nil:
    return err[FileStream, StorageError](StorageError(
      kind: seIo,
      ioError: "Failed to open blob file: " & path
    ))

  cache.openFiles[fileId] = stream
  return ok[FileStream, StorageError](stream)

# Read a blob value using a handle
proc readValue*(cache: BlobReaderCache, handle: BlobHandle,
    basePath: string): StorageResult[string] =
  ## Reads a blob value using the handle.
  let streamResult = cache.getStream(handle.fileId, basePath)
  if not streamResult.isOk:
    return err[string, StorageError](streamResult.err[])

  let stream = streamResult.value

  stream.setPosition(int(handle.offset))

  # Read entry header
  let keyLen = stream.readUInt32()
  let valueLen = stream.readUInt32()
  let seqno = stream.readUInt64()
  let compressed = stream.readUInt8() == 1
  discard stream.readUInt8() # Reserved
  discard stream.readUInt8() # Reserved
  discard stream.readUInt8() # Reserved
  
  # Skip key
  stream.setPosition(stream.getPosition() + int(keyLen))

  # Read value
  let actualValueLen = if compressed: handle.compressedSize else: handle.size
  var value = stream.readStr(int(actualValueLen))

  # Decompress if needed
  if compressed:
    value = lsm_compression.decompress(value, lsm_compression.ctZlib)

  return ok[string, StorageError](value)

# Close all open files
proc closeAll*(cache: BlobReaderCache) =
  ## Closes all open blob files.
  cache.lock.acquire()
  defer: cache.lock.release()

  for stream in cache.openFiles.values:
    stream.close()
  cache.openFiles.clear()

# Read blob file header
proc readHeader*(stream: FileStream): StorageResult[tuple[itemCount: uint64,
    valid: bool]] =
  ## Reads and validates a blob file header.
  let magic = stream.readStr(BLOB_MAGIC.len)
  if magic != BLOB_MAGIC:
    return ok[tuple[itemCount: uint64, valid: bool], StorageError]((
        itemCount: 0'u64, valid: false))

  let itemCount = stream.readUInt64()
  return ok[tuple[itemCount: uint64, valid: bool], StorageError]((
      itemCount: itemCount, valid: true))

# Scan blob file and return all entries (for recovery)
type
  BlobScanEntry* = tuple[key: string, handle: BlobHandle, seqno: uint64]

proc scanBlobFile*(path: string): StorageResult[seq[BlobScanEntry]] =
  ## Scans a blob file and returns all entries.
  var entries: seq[BlobScanEntry] = @[]

  var stream = newFileStream(path, fmRead)
  if stream == nil:
    return err[seq[BlobScanEntry], StorageError](StorageError(
      kind: seIo,
      ioError: "Failed to open blob file: " & path
    ))

  defer: stream.close()

  # Read header
  let headerResult = readHeader(stream)
  if not headerResult.isOk:
    return err[seq[BlobScanEntry], StorageError](headerResult.err[])

  if not headerResult.value.valid:
    return err[seq[BlobScanEntry], StorageError](StorageError(
      kind: seIo,
      ioError: "Invalid blob file header: " & path
    ))

  # Read entries
  while not stream.atEnd():
    let offset = stream.getPosition()

    # Read entry header
    let keyLen = stream.readUInt32()
    let valueLen = stream.readUInt32()
    let seqno = stream.readUInt64()
    let compressed = stream.readUInt8() == 1
    discard stream.readUInt8() # Reserved
    discard stream.readUInt8() # Reserved
    discard stream.readUInt8() # Reserved

    if keyLen == 0 or keyLen > 65535:
      break # Invalid entry
    
    # Read key
    let key = stream.readStr(int(keyLen))

    # Skip value
    let actualValueLen = if compressed: valueLen else: valueLen
    stream.setPosition(stream.getPosition() + int(actualValueLen))

    # Create handle
    let handle = BlobHandle(
      fileId: 0, # Will be set by caller
      offset: uint64(offset),
      size: if compressed: 0 else: valueLen,
      compressedSize: if compressed: valueLen else: 0
    )

    entries.add((key: key, handle: handle, seqno: seqno))

  return ok[seq[BlobScanEntry], StorageError](entries)
