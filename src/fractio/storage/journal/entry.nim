# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[error, types, file]
import std/[streams, endians]

# Forward declarations
type
  InternalKeyspaceId* = uint64
  Slice* = string # Simplified for now

# Journal entry. Every batch is composed as a Start, followed by N items, followed by an End.
#
# - The start entry contains the numbers of items. If the numbers of items following doesn't match, the batch is broken.
# - The end entry contains a checksum value. If the checksum of the items doesn't match that, the batch is broken.
# - The end entry terminates each batch with the magic string: MAGIC_BYTES.
# - If a start entry is detected, while inside a batch, the batch is broken.

type
  EntryKind* = enum
    ekStart
    ekItem
    ekEnd
    ekClear

  Entry* = object
    case kind*: EntryKind
    of ekStart:
      itemCount*: uint32
      seqno*: SeqNo
    of ekItem:
      keyspaceId*: InternalKeyspaceId
      key*: UserKey
      value*: UserValue
      valueType*: ValueType
      compression*: CompressionType
    of ekEnd:
      checksum*: uint64
    of ekClear:
      clearKeyspaceId*: InternalKeyspaceId

# Tags for journal entries
type
  Tag* = enum
    tStart = 1
    tItem = 2
    tEnd = 3
    tClear = 4

# Convert uint8 to Tag
proc tagFromUint8*(value: uint8): StorageResult[Tag] =
  case value
  of 1: ok[Tag, StorageError](tStart)
  of 2: ok[Tag, StorageError](tItem)
  of 3: ok[Tag, StorageError](tEnd)
  of 4: ok[Tag, StorageError](tClear)
  else: err[Tag, StorageError](StorageError(kind: seInvalidTag,
      tagName: "JournalMarkerTag", tagValue: value))

# Convert Tag to uint8
proc uint8FromTag*(tag: Tag): uint8 =
  case tag
  of tStart: 1
  of tItem: 2
  of tEnd: 3
  of tClear: 4

# Convert uint8 to ValueType
proc valueTypeFromUint8*(value: uint8): StorageResult[ValueType] =
  case value
  of 0: ok[ValueType, StorageError](vtValue)
  of 1: ok[ValueType, StorageError](vtTombstone)
  of 2: ok[ValueType, StorageError](vtWeakTombstone)
  of 3: ok[ValueType, StorageError](vtIndirection)
  else: err[ValueType, StorageError](StorageError(kind: seInvalidTag,
      tagName: "ValueType", tagValue: value))

# Convert ValueType to uint8
proc uint8FromValueType*(vt: ValueType): uint8 =
  case vt
  of vtValue: 0
  of vtTombstone: 1
  of vtWeakTombstone: 2
  of vtIndirection: 3

# Convert uint8 to CompressionType
proc compressionTypeFromUint8*(value: uint8): StorageResult[CompressionType] =
  case value
  of 0: ok[CompressionType, StorageError](ctNone)
  of 1: ok[CompressionType, StorageError](ctSnappy)
  of 2: ok[CompressionType, StorageError](ctLz4)
  else: err[CompressionType, StorageError](StorageError(kind: seInvalidTag,
      tagName: "CompressionType", tagValue: value))

# Convert CompressionType to uint8
proc uint8FromCompressionType*(ct: CompressionType): uint8 =
  case ct
  of ctNone: 0
  of ctSnappy: 1
  of ctLz4: 2

# Helper proc to write uint64 in little-endian format
proc writeLe64*(writer: Stream, value: uint64) =
  var leValue: uint64
  # cpuToLE64 is not available in all Nim versions, use littleEndian64
  # littleEndian64(dst, src) - converts from native (src) to little-endian (dst)
  littleEndian64(addr leValue, addr value)
  writer.write(leValue)

# Helper proc to write uint32 in little-endian format
proc writeLe32*(writer: Stream, value: uint32) =
  var leValue: uint32
  littleEndian32(addr leValue, addr value)
  writer.write(leValue)

# Helper proc to write uint16 in little-endian format
proc writeLe16*(writer: Stream, value: uint16) =
  var leValue: uint16
  littleEndian16(addr leValue, addr value)
  writer.write(leValue)

# Helper proc to read uint64 in little-endian format
proc readLe64*(reader: Stream): uint64 =
  var rawValue = reader.readInt64()
  var nativeValue: uint64
  littleEndian64(addr nativeValue, addr rawValue)
  return nativeValue

# Helper proc to read uint32 in little-endian format
proc readLe32*(reader: Stream): uint32 =
  var rawValue = reader.readInt32()
  var nativeValue: uint32
  littleEndian32(addr nativeValue, addr rawValue)
  return nativeValue

# Helper proc to read uint16 in little-endian format
proc readLe16*(reader: Stream): uint16 =
  var rawValue = reader.readInt16()
  var nativeValue: uint16
  littleEndian16(addr nativeValue, addr rawValue)
  return nativeValue

# Serialize marker item
proc serializeMarkerItem*(writer: Stream, keyspaceId: InternalKeyspaceId,
                         key: string, value: string, valueType: ValueType,
                         compression: CompressionType): StorageResult[void] =
  # Write tag
  writer.write(uint8FromTag(tItem))

  # Write value type
  writer.write(uint8FromValueType(valueType))

  # Write compression type
  writer.write(uint8FromCompressionType(compression))

  # For now, we'll simplify compression handling
  # In a full implementation, this would compress the value
  let compressedValue = case compression
    of ctNone: value
    of ctLz4, ctSnappy: value # TODO: Implement actual compression

  # Write keyspace ID (little endian)
  writeLe64(writer, keyspaceId)

  # Write key length (16-bit, little endian)
  if key.len > 65535:
    return err[void, StorageError](StorageError(kind: seIo,
        ioError: "Key too long (max 65535 bytes)"))
  writeLe16(writer, uint16(key.len))

  # Write value length (32-bit, little endian)
  if value.len > uint32.high.int:
    return err[void, StorageError](StorageError(kind: seIo,
        ioError: "Value too long (max 2^32 bytes)"))
  writeLe32(writer, uint32(value.len))

  # Write compressed value length (32-bit, little endian)
  writeLe32(writer, uint32(compressedValue.len))

  # Write key
  if key.len > 0:
    writer.write(key)

  # Write compressed value
  if compressedValue.len > 0:
    writer.write(compressedValue)

  return okVoid

# Encode entry into writer
proc encodeInto*(entry: Entry, writer: Stream): StorageResult[void] =
  case entry.kind
  of ekStart:
    # Write tag
    writer.write(uint8FromTag(tStart))
    # Write item count (little endian)
    writeLe32(writer, entry.itemCount)
    # Write seqno (little endian)
    writeLe64(writer, entry.seqno)

  of ekItem:
    # Serialize item
    return serializeMarkerItem(writer, entry.keyspaceId, entry.key, entry.value,
                              entry.valueType, entry.compression)

  of ekEnd:
    # Write tag
    writer.write(uint8FromTag(tEnd))
    # Write checksum (little endian)
    writeLe64(writer, entry.checksum)
    # Write magic bytes trailer
    writer.writeData(addr MAGIC_BYTES[0], MAGIC_BYTES.len)

  of ekClear:
    # Write tag
    writer.write(uint8FromTag(tClear))
    # Write keyspace ID (little endian)
    writeLe64(writer, entry.clearKeyspaceId)

  return okVoid

# Decode entry from reader
proc decodeFrom*(reader: Stream): StorageResult[Entry] =
  # Read tag
  let tagByte = reader.readInt8()
  let tagResult = tagFromUint8(uint8(tagByte))
  if tagResult.isErr:
    return err[Entry, StorageError](tagResult.error)
  let tag = tagResult.get

  case tag
  of tStart:
    # Read item count (little endian)
    let itemCount = readLe32(reader)

    # Read seqno (little endian)
    let seqno = readLe64(reader)

    return ok[Entry, StorageError](Entry(kind: ekStart, itemCount: itemCount, seqno: seqno))

  of tItem:
    # Read value type
    let valueTypeByte = reader.readInt8()
    let valueTypeResult = valueTypeFromUint8(uint8(valueTypeByte))
    if valueTypeResult.isErr:
      return err[Entry, StorageError](valueTypeResult.error)
    let valueType = valueTypeResult.get

    # Read compression type
    let compressionByte = reader.readInt8()
    let compressionResult = compressionTypeFromUint8(uint8(compressionByte))
    if compressionResult.isErr:
      return err[Entry, StorageError](compressionResult.error)
    let compression = compressionResult.get

    # Read keyspace ID (little endian)
    let keyspaceId = readLe64(reader)

    # Read key length (little endian)
    let keyLen = readLe16(reader)

    # Read value length (little endian)
    let valueLen = readLe32(reader)

    # Read compressed value length (little endian)
    let compressedValueLen = readLe32(reader)

    # Read key
    var key = newString(keyLen)
    if keyLen > 0:
      if reader.readData(addr key[0], keyLen.int) != keyLen.int:
        return err[Entry, StorageError](StorageError(kind: seIo,
            ioError: "Failed to read key"))

    # Read compressed value
    var compressedValue = newString(compressedValueLen)
    if compressedValueLen > 0:
      if reader.readData(addr compressedValue[0], compressedValueLen.int) !=
          compressedValueLen.int:
        return err[Entry, StorageError](StorageError(kind: seIo,
            ioError: "Failed to read value"))

    # Decompress if needed
    var value: string
    case compression
    of ctNone:
      # Value should match compressed value length when no compression
      value = compressedValue
    of ctLz4, ctSnappy:
      # TODO: Implement actual decompression
      # For now, just use the compressed value (this is a bug, but allows basic operation)
      value = compressedValue

    return ok[Entry, StorageError](Entry(
      kind: ekItem,
      keyspaceId: keyspaceId,
      key: key,
      value: value,
      valueType: valueType,
      compression: compression
    ))

  of tEnd:
    # Read checksum (little endian)
    let checksum = readLe64(reader)

    # Read magic bytes trailer
    var magic: array[4, byte]
    if reader.readData(addr magic[0], 4) != 4:
      return err[Entry, StorageError](StorageError(kind: seIo,
          ioError: "Failed to read trailer"))

    # Check magic bytes
    if magic != MAGIC_BYTES:
      return err[Entry, StorageError](StorageError(kind: seInvalidTrailer))

    return ok[Entry, StorageError](Entry(kind: ekEnd, checksum: checksum))

  of tClear:
    # Read keyspace ID (little endian)
    let keyspaceId = readLe64(reader)

    return ok[Entry, StorageError](Entry(kind: ekClear,
        clearKeyspaceId: keyspaceId))
