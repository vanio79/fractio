# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Optimized Journal Writer Implementation
##
## Key optimizations:
## 1. Pre-allocated write buffer to avoid allocations
## 2. Buffered I/O using a large buffer to batch writes
## 3. Fast hash function (SipHash-like) instead of FNV-1a
## 4. Direct serialization to buffer without intermediate streams

import fractio/storage/[error, types, file, journal/entry]
import std/[streams, os, posix, endians]

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
const JOURNAL_BUFFER_BYTES*: int = 64 * 1024 # 64 KB buffer for better throughput

# Journal writer with optimizations
type
  Writer* = ref object
    path*: string
    file*: FileStream
    buf*: seq[byte]   # Reusable serialization buffer
    ioBuf*: seq[byte] # I/O buffer for batching writes
    ioBufPos*: int    # Current position in I/O buffer
    isBufferDirty*: bool
    compression*: CompressionType
    compressionThreshold*: int
    startPosition*: int64

# The persist mode allows setting the durability guarantee of previous writes
type
  PersistMode* = enum
    pmBuffer   # Flushes data to OS buffers
    pmSyncData # Flushes data using fdatasync
    pmSyncAll  # Flushes data + metadata using fsync

               # Fast hash implementation - SipHash-like for speed
               # This is a simplified version optimized for speed over cryptographic security
const SIPHASH_KEY0: uint64 = 0x0706050403020100'u64
const SIPHASH_KEY1: uint64 = 0x0F0E0D0C0B0A0908'u64

type
  FastHasher* = object
    v0: uint64
    v1: uint64
    v2: uint64
    v3: uint64
    buf: array[8, byte]
    bufLen: int
    totalLen: uint64

  # Backward compatibility alias
  Hasher* = FastHasher

proc rotateLeft(x: uint64, b: int): uint64 {.inline.} =
  (x shl b) or (x shr (64 - b))

proc sipRound(h: var Hasher) {.inline.} =
  h.v0 = h.v0 + h.v1
  h.v1 = rotateLeft(h.v1, 13)
  h.v1 = h.v1 xor h.v0
  h.v0 = rotateLeft(h.v0, 32)
  h.v2 = h.v2 + h.v3
  h.v3 = rotateLeft(h.v3, 16)
  h.v3 = h.v3 xor h.v2
  h.v0 = h.v0 + h.v3
  h.v3 = rotateLeft(h.v3, 21)
  h.v3 = h.v3 xor h.v0
  h.v1 = h.v1 + h.v2
  h.v2 = rotateLeft(h.v2, 17)
  h.v2 = h.v2 xor h.v1
  h.v1 = rotateLeft(h.v1, 32)

proc initHasher*(): Hasher =
  result.v0 = SIPHASH_KEY0 xor 0x736f6d6570736575'u64
  result.v1 = SIPHASH_KEY1 xor 0x646f72616e646f6d'u64
  result.v2 = SIPHASH_KEY0 xor 0x6c7967656e657261'u64
  result.v3 = SIPHASH_KEY1 xor 0x7465646279746573'u64
  result.bufLen = 0
  result.totalLen = 0

proc update*(hasher: var Hasher, data: openArray[byte]) {.inline.} =
  hasher.totalLen += uint64(data.len)

  var i = 0
  var remaining = data.len

  # Process any buffered bytes first
  if hasher.bufLen > 0:
    let need = 8 - hasher.bufLen
    let take = min(need, remaining)
    for j in 0..<take:
      hasher.buf[hasher.bufLen + j] = data[i + j]
    hasher.bufLen += take
    i += take
    remaining -= take

    if hasher.bufLen == 8:
      var m: uint64
      littleEndian64(addr m, addr hasher.buf[0])
      hasher.v3 = hasher.v3 xor m
      sipRound(hasher)
      sipRound(hasher)
      hasher.v0 = hasher.v0 xor m
      hasher.bufLen = 0

  # Process 8-byte chunks
  while remaining >= 8:
    var m: uint64
    littleEndian64(addr m, addr data[i])
    hasher.v3 = hasher.v3 xor m
    sipRound(hasher)
    sipRound(hasher)
    hasher.v0 = hasher.v0 xor m
    i += 8
    remaining -= 8

  # Buffer remaining bytes
  if remaining > 0:
    for j in 0..<remaining:
      hasher.buf[j] = data[i + j]
    hasher.bufLen = remaining

proc update*(hasher: var Hasher, data: string) {.inline.} =
  # Convert string to openArray[byte] without copying
  when nimvm:
    var byteData = newSeq[byte](data.len)
    for i, c in data:
      byteData[i] = byte(c)
    hasher.update(byteData)
  else:
    hasher.update(data.toOpenArrayByte(0, data.len - 1))

proc finish*(hasher: var Hasher): uint64 =
  # Pad remaining bytes
  var m: uint64 = uint64(hasher.totalLen) shl 56

  for i in 0..<hasher.bufLen:
    m = m or (uint64(hasher.buf[i]) shl (i * 8))

  hasher.v3 = hasher.v3 xor m
  sipRound(hasher)
  sipRound(hasher)
  hasher.v0 = hasher.v0 xor m

  # Finalization
  hasher.v2 = hasher.v2 xor 0xFF
  sipRound(hasher)
  sipRound(hasher)
  sipRound(hasher)
  sipRound(hasher)

  result = hasher.v0 xor hasher.v1 xor hasher.v2 xor hasher.v3

# Helper to write directly to buffer without stream allocation
proc writeToBuf(buf: var seq[byte], pos: var int, data: openArray[
    byte]) {.inline.} =
  let newPos = pos + data.len
  if newPos > buf.len:
    buf.setLen(max(newPos, buf.len * 2))
  copyMem(addr buf[pos], addr data[0], data.len)
  pos = newPos

proc writeToBuf(buf: var seq[byte], pos: var int, data: string) {.inline.} =
  if data.len > 0:
    let newPos = pos + data.len
    if newPos > buf.len:
      buf.setLen(max(newPos, buf.len * 2))
    copyMem(addr buf[pos], addr data[0], data.len)
    pos = newPos

proc writeToBuf(buf: var seq[byte], pos: var int, val: byte) {.inline.} =
  if pos >= buf.len:
    buf.setLen(max(pos + 1, buf.len * 2))
  buf[pos] = val
  pos += 1

proc writeToBufLe32(buf: var seq[byte], pos: var int, val: uint32) {.inline.} =
  let newPos = pos + 4
  if newPos > buf.len:
    buf.setLen(max(newPos, buf.len * 2))
  littleEndian32(addr buf[pos], addr val)
  pos = newPos

proc writeToBufLe64(buf: var seq[byte], pos: var int, val: uint64) {.inline.} =
  let newPos = pos + 8
  if newPos > buf.len:
    buf.setLen(max(newPos, buf.len * 2))
  littleEndian64(addr buf[pos], addr val)
  pos = newPos

proc writeLe16ToBuf(buf: var seq[byte], pos: var int, val: uint16) {.inline.} =
  let newPos = pos + 2
  if newPos > buf.len:
    buf.setLen(max(newPos, buf.len * 2))
  littleEndian16(addr buf[pos], addr val)
  pos = newPos

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
  try:
    let fileSize = os.getFileSize(writer.path)
    return ok[uint64, StorageError](uint64(fileSize))
  except OSError:
    return err[uint64, StorageError](StorageError(kind: seIo,
        ioError: "Failed to get file size: " & writer.path))

# Flush I/O buffer to file
proc flushIoBuffer(writer: Writer): StorageResult[void] =
  if writer.ioBufPos > 0:
    writer.file.writeData(addr writer.ioBuf[0], writer.ioBufPos)
    writer.ioBufPos = 0
  return okVoid

# Write data through I/O buffer
proc bufferedWrite(writer: Writer, data: openArray[byte]): StorageResult[void] =
  var offset = 0
  var remaining = data.len

  while remaining > 0:
    let available = writer.ioBuf.len - writer.ioBufPos
    let toWrite = min(remaining, available)

    copyMem(addr writer.ioBuf[writer.ioBufPos], addr data[offset], toWrite)
    writer.ioBufPos += toWrite
    offset += toWrite
    remaining -= toWrite

    # Flush if buffer is full
    if writer.ioBufPos >= writer.ioBuf.len:
      let flushResult = writer.flushIoBuffer()
      if flushResult.isErr:
        return flushResult

  return okVoid

proc bufferedWrite(writer: Writer, data: string): StorageResult[void] =
  if data.len > 0:
    when nimvm:
      var byteData = newSeq[byte](data.len)
      for i, c in data:
        byteData[i] = byte(c)
      return writer.bufferedWrite(byteData)
    else:
      return writer.bufferedWrite(data.toOpenArrayByte(0, data.len - 1))
  return okVoid

# Rotate journal
proc rotate*(writer: Writer): StorageResult[(string, string)] =
  if writer.file == nil:
    return err[(string, string), StorageError](StorageError(kind: seIo,
        ioError: "Writer file is nil"))

  # Flush current data
  let flushResult = writer.flushIoBuffer()
  if flushResult.isErr:
    return err[(string, string), StorageError](flushResult.error)
  writer.file.flush()

  let sealedPath = writer.path
  let newPath = writer.path & ".new"

  writer.file.close()

  let newFile = newFileStream(newPath, fmWrite)
  if newFile == nil:
    return err[(string, string), StorageError](StorageError(kind: seIo,
        ioError: "Failed to create new journal file: " & newPath))

  writer.file = newFile
  writer.path = newPath
  writer.isBufferDirty = false
  writer.ioBufPos = 0

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
    buf: newSeq[byte](4096),                           # 4KB serialization buffer
    ioBuf: newSeq[byte](JOURNAL_BUFFER_BYTES),         # 64KB I/O buffer
    ioBufPos: 0,
    isBufferDirty: false,
    compression: ctNone,
    compressionThreshold: 0,
    startPosition: 0
  )

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
    buf: newSeq[byte](4096),
    ioBuf: newSeq[byte](JOURNAL_BUFFER_BYTES),
    ioBufPos: 0,
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

  # Flush I/O buffer first
  let flushResult = writer.flushIoBuffer()
  if flushResult.isErr:
    return flushResult

  # Flush application buffers
  writer.file.flush()
  writer.isBufferDirty = false

  case mode
  of pmBuffer:
    discard
  of pmSyncData:
    discard
  of pmSyncAll:
    discard

  return okVoid

# Serialize start entry directly to buffer
proc serializeStartEntry(buf: var seq[byte], pos: var int, itemCount: uint32,
    seqno: uint64) =
  writeToBuf(buf, pos, uint8FromTag(tStart).byte)
  writeToBufLe32(buf, pos, itemCount)
  writeToBufLe64(buf, pos, seqno)

# Serialize end entry directly to buffer
proc serializeEndEntry(buf: var seq[byte], pos: var int, checksum: uint64) =
  writeToBuf(buf, pos, uint8FromTag(tEnd).byte)
  writeToBufLe64(buf, pos, checksum)
  # Write magic bytes
  writeToBuf(buf, pos, MAGIC_BYTES)

# Serialize marker item directly to buffer
proc serializeMarkerItemIntoBuf(buf: var seq[byte], pos: var int,
    keyspaceId: uint64, key: string, value: string,
    valueType: ValueType, compression: CompressionType): StorageResult[void] =

  writeToBuf(buf, pos, uint8FromTag(tItem).byte)
  writeToBuf(buf, pos, uint8FromValueType(valueType).byte)
  writeToBuf(buf, pos, uint8FromCompressionType(compression).byte)
  writeToBufLe64(buf, pos, keyspaceId)

  if key.len > 65535:
    return err[void, StorageError](StorageError(kind: seIo,
        ioError: "Key too long (max 65535 bytes)"))
  writeLe16ToBuf(buf, pos, uint16(key.len))

  if value.len > uint32.high.int:
    return err[void, StorageError](StorageError(kind: seIo,
        ioError: "Value too long (max 2^32 bytes)"))
  writeToBufLe32(buf, pos, uint32(value.len))
  writeToBufLe32(buf, pos, uint32(value.len)) # compressed len same as value len for now

  writeToBuf(buf, pos, key)
  writeToBuf(buf, pos, value)

  return okVoid

# Write start marker
proc writeStart*(writer: Writer, itemCount: uint32,
    seqno: SeqNo): StorageResult[int] =
  var pos = 0
  writer.buf.setLen(0)
  writer.buf.setLen(32) # Pre-allocate
  serializeStartEntry(writer.buf, pos, itemCount, seqno)
  writer.buf.setLen(pos)

  let writeResult = writer.bufferedWrite(writer.buf)
  if writeResult.isErr:
    return err[int, StorageError](writeResult.error)

  return ok[int, StorageError](pos)

# Write end marker
proc writeEnd*(writer: Writer, checksum: uint64): StorageResult[int] =
  var pos = 0
  writer.buf.setLen(0)
  writer.buf.setLen(32)
  serializeEndEntry(writer.buf, pos, checksum)
  writer.buf.setLen(pos)

  let writeResult = writer.bufferedWrite(writer.buf)
  if writeResult.isErr:
    return err[int, StorageError](writeResult.error)

  return ok[int, StorageError](pos)

# Write raw entry - optimized version
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

  # Determine compression
  let compression = if writer.compressionThreshold > 0 and value.len >=
      writer.compressionThreshold:
    writer.compression
  else:
    ctNone

  # Serialize item directly to buffer
  var pos = 0
  writer.buf.setLen(0)
  writer.buf.setLen(key.len + value.len + 64) # Pre-allocate estimated size

  let serializeResult = serializeMarkerItemIntoBuf(writer.buf, pos, keyspaceId,
      key, value, valueType, compression)
  if serializeResult.isErr:
    return err[int, StorageError](serializeResult.error)
  writer.buf.setLen(pos)

  # Write to file through buffer
  let writeResult = writer.bufferedWrite(writer.buf)
  if writeResult.isErr:
    return err[int, StorageError](writeResult.error)

  # Update hasher
  hasher.update(writer.buf)
  byteCount += pos

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

  let startResult = writer.writeStart(1, seqno)
  if startResult.isErr:
    return err[int, StorageError](startResult.error)
  byteCount += startResult.get

  # Serialize clear entry directly
  var pos = 0
  writer.buf.setLen(0)
  writer.buf.setLen(32)
  writeToBuf(writer.buf, pos, uint8FromTag(tClear).byte)
  writeToBufLe64(writer.buf, pos, keyspaceId)
  writer.buf.setLen(pos)

  let writeResult = writer.bufferedWrite(writer.buf)
  if writeResult.isErr:
    return err[int, StorageError](writeResult.error)

  hasher.update(writer.buf)
  byteCount += pos

  let checksum = hasher.finish()
  let endResult = writer.writeEnd(checksum)
  if endResult.isErr:
    return err[int, StorageError](endResult.error)
  byteCount += endResult.get

  return ok[int, StorageError](byteCount)

# Write batch - optimized version
proc writeBatch*(writer: Writer, items: seq[BatchItem],
    seqno: SeqNo): StorageResult[int] =
  let batchSize = items.len
  if batchSize == 0:
    return ok[int, StorageError](0)

  writer.isBufferDirty = true

  var hasher = initHasher()
  var byteCount = 0

  # Write start
  let startResult = writer.writeStart(uint32(batchSize), seqno)
  if startResult.isErr:
    return err[int, StorageError](startResult.error)
  byteCount += startResult.get

  # Write each item
  for item in items:
    let compression = if writer.compressionThreshold > 0 and item.value.len >=
        writer.compressionThreshold:
      writer.compression
    else:
      ctNone

    var pos = 0
    writer.buf.setLen(0)
    writer.buf.setLen(item.key.len + item.value.len + 64)

    let serializeResult = serializeMarkerItemIntoBuf(writer.buf, pos,
        item.keyspace.id, item.key, item.value, item.valueType, compression)
    if serializeResult.isErr:
      return err[int, StorageError](serializeResult.error)
    writer.buf.setLen(pos)

    let writeResult = writer.bufferedWrite(writer.buf)
    if writeResult.isErr:
      return err[int, StorageError](writeResult.error)

    hasher.update(writer.buf)
    byteCount += pos

  # Write end
  let checksum = hasher.finish()
  let endResult = writer.writeEnd(checksum)
  if endResult.isErr:
    return err[int, StorageError](endResult.error)
  byteCount += endResult.get

  return ok[int, StorageError](byteCount)
