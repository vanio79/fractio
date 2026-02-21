# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Blob File Writer
##
## Writes blob files containing large values that are separated from SSTables.

import ./types
import ../error
import ../lsm_tree/compression as lsm_compression
import std/[os, streams]

# Create a new blob writer
proc newBlobWriter*(path: string, fileId: BlobFileId,
                    targetSize: uint64 = DEFAULT_BLOB_FILE_TARGET_SIZE): BlobWriter =
  result = BlobWriter(
    path: path,
    fileId: fileId,
    itemCount: 0,
    currentSize: 0,
    targetSize: targetSize,
    compression: false
  )

# Write blob file header
proc writeHeader*(writer: BlobWriter, stream: FileStream): StorageResult[void] =
  ## Writes the blob file header.
  stream.write(BLOB_MAGIC)
  stream.write(uint64(0)) # Placeholder for item count (will update at close)
  return okVoid

# Write a blob entry and return its handle
proc writeEntry*(writer: BlobWriter, stream: FileStream, key: string,
                 value: string, seqno: uint64,
                 compress: bool = false): StorageResult[BlobHandle] =
  ## Writes a blob entry to the stream.
  ## Returns a BlobHandle that can be used to read the value later.

  let offset = stream.getPosition()

  # Compress if requested
  var valueToWrite = value
  var compressed = false
  var compressedSize = 0'u32

  if compress and value.len > 64: # Only compress if value is reasonably large
    let compressedData = lsm_compression.compress(value, lsm_compression.ctZlib)
    if compressedData.len < value.len:
      valueToWrite = compressedData
      compressed = true
      compressedSize = uint32(valueToWrite.len)

  # Write entry header
  stream.write(uint32(key.len))
  stream.write(uint32(valueToWrite.len))
  stream.write(seqno)
  stream.write(uint8(if compressed: 1 else: 0))
  stream.write(uint8(0)) # Reserved
  stream.write(uint8(0)) # Reserved
  stream.write(uint8(0)) # Reserved
  
  # Write key
  stream.write(key)

  # Write value
  stream.write(valueToWrite)

  writer.itemCount += 1
  writer.currentSize = uint64(stream.getPosition())

  let handle = BlobHandle(
    fileId: writer.fileId,
    offset: uint64(offset),
    size: uint32(value.len),
    compressedSize: if compressed: compressedSize else: 0
  )

  return ok[BlobHandle, StorageError](handle)

# Finalize blob file (update header with item count)
proc finalize*(writer: BlobWriter, stream: FileStream): StorageResult[void] =
  ## Finalizes the blob file by updating the header with the item count.
  let currentPos = stream.getPosition()

  # Seek to item count position (after magic)
  stream.setPosition(BLOB_MAGIC.len)
  stream.write(uint64(writer.itemCount))

  # Seek back to end
  stream.setPosition(currentPos)

  return okVoid

# Check if writer should rotate to new file
proc shouldRotate*(writer: BlobWriter): bool =
  ## Returns true if the blob file should be rotated.
  writer.currentSize >= writer.targetSize

# Read a blob entry
proc readEntry*(stream: FileStream, handle: BlobHandle): StorageResult[string] =
  ## Reads a blob entry from the stream using the handle.
  stream.setPosition(int(handle.offset))

  # Read entry header
  let keyLen = stream.readUInt32()
  let valueLen = stream.readUInt32()
  let seqno = stream.readUInt64()
  let isCompressed = stream.readUInt8() == 1
  discard stream.readUInt8() # Reserved
  discard stream.readUInt8() # Reserved
  discard stream.readUInt8() # Reserved
  
  # Skip key
  stream.setPosition(stream.getPosition() + int(keyLen))

  # Read value
  let actualValueLen = if isCompressed: handle.compressedSize else: handle.size
  var value = stream.readStr(int(actualValueLen))

  # Decompress if needed
  if isCompressed:
    value = lsm_compression.decompress(value, lsm_compression.ctZlib)

  return ok[string, StorageError](value)

# Create a blob file path
proc blobFilePath*(basePath: string, fileId: BlobFileId): string =
  ## Returns the path for a blob file.
  ## The path includes the "blobs" subdirectory.
  basePath / "blobs" / ($fileId & ".blob")

# Serialize blob handle for storage in SSTable
proc serializeHandle*(handle: BlobHandle): string =
  ## Serializes a blob handle to a string for storage.
  result = newString(24)
  var offset = 0

  # File ID (8 bytes)
  copyMem(addr result[offset], unsafeAddr handle.fileId, 8)
  offset += 8

  # Offset (8 bytes)
  copyMem(addr result[offset], unsafeAddr handle.offset, 8)
  offset += 8

  # Size (4 bytes)
  copyMem(addr result[offset], unsafeAddr handle.size, 4)
  offset += 4

  # Compressed size (4 bytes)
  copyMem(addr result[offset], unsafeAddr handle.compressedSize, 4)

# Deserialize blob handle from SSTable
proc deserializeHandle*(data: string): BlobHandle =
  ## Deserializes a blob handle from a string.
  var offset = 0

  # File ID (8 bytes)
  copyMem(addr result.fileId, unsafeAddr data[offset], 8)
  offset += 8

  # Offset (8 bytes)
  copyMem(addr result.offset, unsafeAddr data[offset], 8)
  offset += 8

  # Size (4 bytes)
  copyMem(addr result.size, unsafeAddr data[offset], 4)
  offset += 4

  # Compressed size (4 bytes)
  copyMem(addr result.compressedSize, unsafeAddr data[offset], 4)
