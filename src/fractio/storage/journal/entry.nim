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

# Serialize marker item
proc serializeMarkerItem*(writer: Stream, keyspaceId: InternalKeyspaceId,
                         key: string, value: string, valueType: ValueType,
                         compression: CompressionType): StorageResult[void] =
  # Write tag
  writer.write(uint8FromTag(tItem))

  # Write value type
  writer.write(uint8(valueType))

  # Write compression type
  # In a full implementation, this would encode the compression type

  # For now, we'll simplify compression handling
  let compressedValue = case compression
    of ctNone: value
    of ctLz4, ctSnappy: value # Simplified - no actual compression in this translation

  # Write keyspace ID (little endian)
  var keyspaceIdLe = keyspaceId
  littleEndian64(addr keyspaceIdLe, addr keyspaceId)
  writer.write(keyspaceIdLe)

  # Write key length (16-bit, little endian)
  var keyLenLe: uint16 = uint16(key.len)
  littleEndian16(addr keyLenLe, addr keyLenLe)
  writer.write(keyLenLe)

  # Write value length (32-bit, little endian)
  var valueLenLe: uint32 = uint32(value.len)
  littleEndian32(addr valueLenLe, addr valueLenLe)
  writer.write(valueLenLe)

  # Write compressed value length (32-bit, little endian)
  var compressedValueLenLe: uint32 = uint32(compressedValue.len)
  littleEndian32(addr compressedValueLenLe, addr compressedValueLenLe)
  writer.write(compressedValueLenLe)

  # Write key
  writer.write(key)

  # Write compressed value
  writer.write(compressedValue)

  return okVoid()

# Encode entry into writer
proc encodeInto*(entry: Entry, writer: Stream): StorageResult[void] =
  case entry.kind
  of ekStart:
    # Write tag
    writer.write(uint8FromTag(tStart))
    # Write item count (little endian)
    var itemCountLe = entry.itemCount
    littleEndian32(addr itemCountLe, addr itemCountLe)
    writer.write(itemCountLe)
    # Write seqno (little endian)
    var seqnoLe = entry.seqno
    littleEndian64(addr seqnoLe, addr seqnoLe)
    writer.write(seqnoLe)

  of ekItem:
    # Serialize item
    return serializeMarkerItem(writer, entry.keyspaceId, entry.key, entry.value,
                              entry.valueType, entry.compression)

  of ekEnd:
    # Write tag
    writer.write(uint8FromTag(tEnd))
    # Write checksum (little endian)
    var checksumLe = entry.checksum
    littleEndian64(addr checksumLe, addr checksumLe)
    writer.write(checksumLe)
    # Write magic bytes trailer
    writer.writeData(addr MAGIC_BYTES[0], MAGIC_BYTES.len)

  of ekClear:
    # Write tag
    writer.write(uint8FromTag(tClear))
    # Write keyspace ID (little endian)
    var keyspaceIdLe = entry.clearKeyspaceId
    littleEndian64(addr keyspaceIdLe, addr keyspaceIdLe)
    writer.write(keyspaceIdLe)

  return okVoid()

# Decode entry from reader
proc decodeFrom*(reader: Stream): StorageResult[Entry] =
  # Read tag
  let tagByte = reader.readInt8()
  let tagResult = tagFromUint8(uint8(tagByte))
  if tagResult.isErr():
    return err[Entry, StorageError](tagResult.error())
  let tag = tagResult.get()

  case tag
  of tStart:
    # Read item count (little endian)
    var itemCountLe = reader.readInt32()
    var itemCount: uint32
    littleEndian32(addr itemCount, addr itemCountLe)

    # Read seqno (little endian)
    var seqnoLe = reader.readInt64()
    var seqno: SeqNo
    littleEndian64(addr seqno, addr seqnoLe)

    return ok[Entry, StorageError](Entry(kind: ekStart, itemCount: itemCount, seqno: seqno))

  of tItem:
    # Read value type
    let valueTypeByte = reader.readInt8()
    # In a full implementation, this would convert the byte to ValueType

    # Read compression type
    # In a full implementation, this would decode the compression type

    # Read keyspace ID (little endian)
    var keyspaceIdLe = reader.readInt64()
    var keyspaceId: InternalKeyspaceId
    littleEndian64(addr keyspaceId, addr keyspaceIdLe)

    # Read key length (little endian)
    var keyLenLe = reader.readInt16()
    var keyLen: uint16
    littleEndian16(addr keyLen, addr keyLenLe)

    # Read value length (little endian)
    var valueLenLe = reader.readInt32()
    var valueLen: uint32
    littleEndian32(addr valueLen, addr valueLenLe)

    # Read compressed value length (little endian)
    var compressedValueLenLe = reader.readInt32()
    var compressedValueLen: uint32
    littleEndian32(addr compressedValueLen, addr compressedValueLenLe)

    # Read key
    var key = newString(keyLen)
    if reader.readData(addr key[0], keyLen.int) != keyLen.int:
      return err[Entry, StorageError](StorageError(kind: seIo,
          ioError: "Failed to read key"))

    # Read value
    var value = newString(compressedValueLen)
    if reader.readData(addr value[0], compressedValueLen.int) !=
        compressedValueLen.int:
      return err[Entry, StorageError](StorageError(kind: seIo,
          ioError: "Failed to read value"))

    # Simplified - no decompression in this translation
    return ok[Entry, StorageError](Entry(kind: ekItem, keyspaceId: keyspaceId, key: key, value: value,
                   valueType: vtValue, compression: ctNone))

  of tEnd:
    # Read checksum (little endian)
    var checksumLe = reader.readInt64()
    var checksum: uint64
    littleEndian64(addr checksum, addr checksumLe)

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
    var keyspaceIdLe = reader.readInt64()
    var keyspaceId: InternalKeyspaceId
    littleEndian64(addr keyspaceId, addr keyspaceIdLe)

    return ok[Entry, StorageError](Entry(kind: ekClear,
        clearKeyspaceId: keyspaceId))
