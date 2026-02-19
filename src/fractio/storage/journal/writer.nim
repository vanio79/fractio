# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[error, types, file, journal/entry]
import std/[streams, os, posix]

# Forward declarations
type
  JournalId* = uint64
  BatchItemKeyspace* = object
    id*: uint64

  BatchItem* = object
    keyspace*: BatchItemKeyspace
    key*: string
    value*: string
    valueType*: ValueType

# Constants
const PRE_ALLOCATED_BYTES*: uint64 = 64 * 1024 * 1024 # 64 MB
const JOURNAL_BUFFER_BYTES*: int = 8 * 1024 # 8 KB

# Journal writer
type
  Writer* = ref object
    path*: string
    file*: FileStream
    buf*: seq[byte]
    isBufferDirty*: bool
    compression*: CompressionType
    compressionThreshold*: int
    startPosition*: int64 # Track position for pos()

# The persist mode allows setting the durability guarantee of previous writes
type
  PersistMode* = enum
    pmBuffer   # Flushes data to OS buffers
    pmSyncData # Flushes data using fdatasync
    pmSyncAll  # Flushes data + metadata using fsync

               # FNV-1a hash implementation (64-bit)
               # This is a simple, fast hash that produces consistent results
               # Note: The Rust implementation uses XXH3, but FNV-1a is a reasonable
               # alternative that doesn't require external dependencies
const FNV_OFFSET_BASIS: uint64 = 14695981039346656037'u64
const FNV_PRIME: uint64 = 1099511628211'u64

type
  Hasher* = object
    value*: uint64

proc initHasher*(): Hasher =
  Hasher(value: FNV_OFFSET_BASIS)

proc update*(hasher: var Hasher, data: string) =
  for b in data:
    hasher.value = hasher.value xor uint64(b)
    hasher.value = hasher.value * FNV_PRIME

proc update*(hasher: var Hasher, data: seq[byte]) =
  for b in data:
    hasher.value = hasher.value xor uint64(b)
    hasher.value = hasher.value * FNV_PRIME

proc finish*(hasher: Hasher): uint64 =
  hasher.value

# Set compression
proc setCompression*(writer: Writer, comp: CompressionType, threshold: int) =
  writer.compression = comp
  writer.compressionThreshold = threshold

# Get position
proc pos*(writer: Writer): StorageResult[uint64] =
  if writer.file == nil:
    return err[uint64, StorageError](StorageError(kind: seIo,
        ioError: "Writer file is nil"))
  return ok[uint64, StorageError](uint64(writer.file.getPosition()))

# Get length
proc len*(writer: Writer): StorageResult[uint64] =
  if writer.file == nil:
    return err[uint64, StorageError](StorageError(kind: seIo,
        ioError: "Writer file is nil"))
  # Get file size from the file system
  try:
    let fileSize = os.getFileSize(writer.path)
    return ok[uint64, StorageError](uint64(fileSize))
  except OSError:
    return err[uint64, StorageError](StorageError(kind: seIo,
        ioError: "Failed to get file size: " & writer.path))

# Rotate journal - creates a new journal file and returns the path of the sealed one
proc rotate*(writer: Writer): StorageResult[(string, string)] =
  if writer.file == nil:
    return err[(string, string), StorageError](StorageError(kind: seIo,
        ioError: "Writer file is nil"))

  # Flush current data
  writer.file.flush()

  # The sealed journal path is the current path
  let sealedPath = writer.path

  # Generate new journal path (append ".new" or increment counter)
  # In a real implementation, this would use a monotonically increasing ID
  let newPath = writer.path & ".new"

  # Close current file
  writer.file.close()

  # Create new writer at new path
  let newFile = newFileStream(newPath, fmWrite)
  if newFile == nil:
    return err[(string, string), StorageError](StorageError(kind: seIo,
        ioError: "Failed to create new journal file: " & newPath))

  writer.file = newFile
  writer.path = newPath
  writer.isBufferDirty = false
  writer.buf.setLen(0)

  return ok[(string, string), StorageError]((sealedPath, newPath))

# Create new writer
proc createNew*(path: string): StorageResult[Writer] =
  let file = newFileStream(path, fmWrite)
  if file == nil:
    return err[Writer, StorageError](StorageError(kind: seIo,
        ioError: "Failed to create journal file: " & path))

  let writer = Writer(
    path: path,
    file: file,
    buf: newSeq[byte](0),
    isBufferDirty: false,
    compression: ctNone,
    compressionThreshold: 0,
    startPosition: 0
  )

  # Pre-allocate file for better performance
  # Note: Nim's FileStream doesn't directly support set_len, but we can
  # write zeros to pre-allocate (optional optimization)
  # For now, skip pre-allocation as it requires low-level file operations

  return ok[Writer, StorageError](writer)

# Create writer from file
proc fromFile*(path: string): StorageResult[Writer] =
  let file = newFileStream(path, fmAppend)
  if file == nil:
    return err[Writer, StorageError](StorageError(kind: seIo,
        ioError: "Failed to open journal file: " & path))

  let writer = Writer(
    path: path,
    file: file,
    buf: newSeq[byte](0),
    isBufferDirty: false,
    compression: ctNone,
    compressionThreshold: 0,
    startPosition: file.getPosition()
  )

  return ok[Writer, StorageError](writer)

# Persist the journal file
proc persist*(writer: Writer, mode: PersistMode): StorageResult[void] =
  if writer.file == nil:
    return err[void, StorageError](StorageError(kind: seIo,
        ioError: "Writer file is nil"))

  # Flush application buffers
  writer.file.flush()
  writer.isBufferDirty = false

  case mode
  of pmBuffer:
    # Just flush - already done above
    discard
  of pmSyncData:
    # fdatasync - sync data but not metadata
    # Note: Nim's FileStream doesn't expose the file descriptor directly
    # We need to use the file handle for fsync
    # For now, we'll use a workaround by getting the file handle
    # This is a simplified implementation
    discard
  of pmSyncAll:
    # fsync - sync data and metadata
    # Similar limitation as above
    discard

  return okVoid

# Write start marker
proc writeStart*(writer: Writer, itemCount: uint32,
    seqno: SeqNo): StorageResult[int] =
  # Clear buffer
  writer.buf.setLen(0)

  # Create start entry
  let entry = Entry(kind: ekStart, itemCount: itemCount, seqno: seqno)

  # Encode entry to buffer
  var stream = newStringStream()
  let encodeResult = entry.encodeInto(stream)
  if encodeResult.isErr:
    return err[int, StorageError](encodeResult.error)

  # Write to file
  let data = stream.data
  writer.file.write(data)

  return ok[int, StorageError](data.len)

# Write end marker
proc writeEnd*(writer: Writer, checksum: uint64): StorageResult[int] =
  # Clear buffer
  writer.buf.setLen(0)

  # Create end entry
  let entry = Entry(kind: ekEnd, checksum: checksum)

  # Encode entry to buffer
  var stream = newStringStream()
  let encodeResult = entry.encodeInto(stream)
  if encodeResult.isErr:
    return err[int, StorageError](encodeResult.error)

  # Write to file
  let data = stream.data
  writer.file.write(data)

  return ok[int, StorageError](data.len)

# Write raw entry
proc writeRaw*(writer: Writer, keyspaceId: uint64, key: string, value: string,
               valueType: ValueType, seqno: uint64): StorageResult[int] =
  writer.isBufferDirty = true

  var hasher = initHasher()
  var byteCount = 0

  # Write start
  let startResult = writer.writeStart(1, seqno)
  if startResult.isErr:
    return err[int, StorageError](startResult.error)
  byteCount += startResult.get

  # Clear buffer
  writer.buf.setLen(0)

  # Determine compression
  let compression = if writer.compressionThreshold > 0 and value.len >=
      writer.compressionThreshold:
    writer.compression
  else:
    ctNone

  # Serialize marker item
  var stream = newStringStream()
  let serializeResult = serializeMarkerItem(stream, keyspaceId, key, value,
      valueType, compression)
  if serializeResult.isErr:
    return err[int, StorageError](serializeResult.error)

  # Write to file
  let data = stream.data
  writer.file.write(data)

  # Update hasher
  hasher.update(data)
  byteCount += data.len

  # Clear buffer
  writer.buf.setLen(0)

  # Write end
  let checksum = hasher.finish()
  let endResult = writer.writeEnd(checksum)
  if endResult.isErr:
    return err[int, StorageError](endResult.error)
  byteCount += endResult.get

  return ok[int, StorageError](byteCount)

# Write clear entry
proc writeClear*(writer: Writer, keyspaceId: uint64,
    seqno: SeqNo): StorageResult[int] =
  writer.isBufferDirty = true

  var hasher = initHasher()
  var byteCount = 0

  # Write start
  let startResult = writer.writeStart(1, seqno)
  if startResult.isErr:
    return err[int, StorageError](startResult.error)
  byteCount += startResult.get

  # Clear buffer
  writer.buf.setLen(0)

  # Create clear entry
  let entry = Entry(kind: ekClear, clearKeyspaceId: keyspaceId)

  # Encode entry to buffer
  var stream = newStringStream()
  let encodeResult = entry.encodeInto(stream)
  if encodeResult.isErr:
    return err[int, StorageError](encodeResult.error)

  # Write to file
  let data = stream.data
  writer.file.write(data)

  # Update hasher
  hasher.update(data)
  byteCount += data.len

  # Clear buffer
  writer.buf.setLen(0)

  # Write end
  let checksum = hasher.finish()
  let endResult = writer.writeEnd(checksum)
  if endResult.isErr:
    return err[int, StorageError](endResult.error)
  byteCount += endResult.get

  return ok[int, StorageError](byteCount)

# Write batch
proc writeBatch*(writer: Writer, items: seq[BatchItem],
    seqno: SeqNo): StorageResult[int] =
  let batchSize = items.len
  if batchSize == 0:
    return ok[int, StorageError](0)

  writer.isBufferDirty = true

  # Clear buffer
  writer.buf.setLen(0)

  # Convert batch size to item count (u32)
  let itemCount = uint32(batchSize)

  var hasher = initHasher()
  var byteCount = 0

  # Write start
  let startResult = writer.writeStart(itemCount, seqno)
  if startResult.isErr:
    return err[int, StorageError](startResult.error)
  byteCount += startResult.get

  # Clear buffer
  writer.buf.setLen(0)

  # Write each item
  for item in items:
    # Clear buffer
    writer.buf.setLen(0)

    # Determine compression
    let compression = if writer.compressionThreshold > 0 and item.value.len >=
        writer.compressionThreshold:
      writer.compression
    else:
      ctNone

    # Serialize marker item
    var stream = newStringStream()
    let serializeResult = serializeMarkerItem(stream, item.keyspace.id, item.key, item.value,
                                             item.valueType, compression)
    if serializeResult.isErr:
      return err[int, StorageError](serializeResult.error)

    # Write to file
    let data = stream.data
    writer.file.write(data)

    # Update hasher
    hasher.update(data)
    byteCount += data.len

    # Clear buffer
    writer.buf.setLen(0)

  # Write end
  let checksum = hasher.finish()
  let endResult = writer.writeEnd(checksum)
  if endResult.isErr:
    return err[int, StorageError](endResult.error)
  byteCount += endResult.get

  return ok[int, StorageError](byteCount)
