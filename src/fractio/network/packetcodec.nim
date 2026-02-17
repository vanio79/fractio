## PacketCodec - Binary serialization/deserialization for time synchronization protocol
##
## Handles byte order detection (BOM) and encoding/decoding of sync request/response packets.
## Designed to be stateless and thread-safe.

import ../core/types

type
  Request* = object
    t1*: Timestamp
  Response* = object
    t2*: Timestamp
    t3*: Timestamp

const
  BOM_BIG_ENDIAN* = 0xFEFF'u16
  BOM_LITTLE_ENDIAN* = 0xFFFE'u16
  MSG_SYNC_REQUEST* = 1'u8
  MSG_SYNC_RESPONSE* = 2'u8
  REQUEST_PACKET_SIZE* = 11
  RESPONSE_PACKET_SIZE* = 19
  MAX_PACKET_SIZE* = 65507

type
  PacketCodec* = ref object of RootObj
    ## Stateless codec - methods can be called on a shared instance.

proc newPacketCodec*(): PacketCodec =
  ## Create a new PacketCodec instance.
  result = PacketCodec()

proc readUint16BE*(self: PacketCodec, src: openArray[uint8],
    offset: int): uint16 {.gcsafe, raises: [ValueError].} =
  ## Read a 16-bit big-endian unsigned integer from src at offset.
  ## Raises ValueError if offset is out of bounds.
  if offset < 0 or offset + 1 >= src.len:
    raise newException(ValueError, "readUint16BE: offset " & $offset &
        " out of bounds for length " & $src.len)
  result = (uint16(src[offset]) shl 8) or uint16(src[offset+1])

proc readUint16LE*(self: PacketCodec, src: openArray[uint8],
    offset: int): uint16 {.gcsafe, raises: [ValueError].} =
  ## Read a 16-bit little-endian unsigned integer from src at offset.
  ## Raises ValueError if offset is out of bounds.
  if offset < 0 or offset + 1 >= src.len:
    raise newException(ValueError, "readUint16LE: offset " & $offset &
        " out of bounds for length " & $src.len)
  result = uint16(src[offset]) or (uint16(src[offset+1]) shl 8)

proc readUint64BE*(self: PacketCodec, src: openArray[uint8],
    offset: int): uint64 {.gcsafe, raises: [ValueError].} =
  ## Read a 64-bit big-endian unsigned integer from src at offset.
  ## Raises ValueError if offset is out of bounds.
  if offset < 0 or offset + 7 >= src.len:
    raise newException(ValueError, "readUint64BE: offset " & $offset &
        " out of bounds for length " & $src.len)
  result = (src[offset].uint64 shl 56) or
           (src[offset+1].uint64 shl 48) or
           (src[offset+2].uint64 shl 40) or
           (src[offset+3].uint64 shl 32) or
           (src[offset+4].uint64 shl 24) or
           (src[offset+5].uint64 shl 16) or
           (src[offset+6].uint64 shl 8) or
           src[offset+7].uint64

proc readUint64LE*(self: PacketCodec, src: openArray[uint8],
    offset: int): uint64 {.gcsafe, raises: [ValueError].} =
  ## Read a 64-bit little-endian unsigned integer from src at offset.
  ## Raises ValueError if offset is out of bounds.
  if offset < 0 or offset + 7 >= src.len:
    raise newException(ValueError, "readUint64LE: offset " & $offset &
        " out of bounds for length " & $src.len)
  result = src[offset].uint64 or
           (src[offset+1].uint64 shl 8) or
           (src[offset+2].uint64 shl 16) or
           (src[offset+3].uint64 shl 24) or
           (src[offset+4].uint64 shl 32) or
           (src[offset+5].uint64 shl 40) or
           (src[offset+6].uint64 shl 48) or
           (src[offset+7].uint64 shl 56)

proc writeUint16BE*(self: PacketCodec, value: uint16, dest: var seq[uint8],
    offset: int) {.gcsafe, raises: [ValueError].} =
  ## Write a 16-bit big-endian unsigned integer to dest at offset.
  ## Raises ValueError if offset is out of bounds.
  if offset < 0 or offset + 1 >= dest.len:
    raise newException(ValueError, "writeUint16BE: offset " & $offset &
        " out of bounds for length " & $dest.len)
  dest[offset] = uint8((value shr 8) and 0xFF)
  dest[offset+1] = uint8(value and 0xFF)

proc writeUint64BE*(self: PacketCodec, value: uint64, dest: var seq[uint8],
    offset: int) {.gcsafe, raises: [ValueError].} =
  ## Write a 64-bit big-endian unsigned integer to dest at offset.
  ## Raises ValueError if offset is out of bounds.
  if offset < 0 or offset + 7 >= dest.len:
    raise newException(ValueError, "writeUint64BE: offset " & $offset &
        " out of bounds for length " & $dest.len)
  dest[offset] = uint8((value shr 56) and 0xFF)
  dest[offset+1] = uint8((value shr 48) and 0xFF)
  dest[offset+2] = uint8((value shr 40) and 0xFF)
  dest[offset+3] = uint8((value shr 32) and 0xFF)
  dest[offset+4] = uint8((value shr 24) and 0xFF)
  dest[offset+5] = uint8((value shr 16) and 0xFF)
  dest[offset+6] = uint8((value shr 8) and 0xFF)
  dest[offset+7] = uint8(value and 0xFF)

proc writeUint64LE*(self: PacketCodec, value: uint64, dest: var seq[uint8],
    offset: int) {.gcsafe, raises: [ValueError].} =
  ## Write a 64-bit little-endian unsigned integer to dest at offset.
  ## Raises ValueError if offset is out of bounds.
  if offset < 0 or offset + 7 >= dest.len:
    raise newException(ValueError, "writeUint64LE: offset " & $offset &
        " out of bounds for length " & $dest.len)
  dest[offset] = uint8(value and 0xFF)
  dest[offset+1] = uint8((value shr 8) and 0xFF)
  dest[offset+2] = uint8((value shr 16) and 0xFF)
  dest[offset+3] = uint8((value shr 24) and 0xFF)
  dest[offset+4] = uint8((value shr 32) and 0xFF)
  dest[offset+5] = uint8((value shr 40) and 0xFF)
  dest[offset+6] = uint8((value shr 48) and 0xFF)
  dest[offset+7] = uint8((value shr 56) and 0xFF)

proc timestampToBytes*(self: PacketCodec, ts: Timestamp, bom: uint16,
    dest: var seq[uint8], offset: int) {.gcsafe, raises: [ValueError].} =
  ## Serialize a Timestamp into dest at offset according to BOM.
  ## Timestamps are serialized as 64-bit unsigned using two's complement reinterpretation.
  ## Raises ValueError if offset is out of bounds.
  let uval = cast[uint64](ts)
  if bom == BOM_BIG_ENDIAN:
    self.writeUint64BE(uval, dest, offset)
  else:
    self.writeUint64LE(uval, dest, offset)

proc bytesToTimestamp*(self: PacketCodec, src: openArray[uint8], offset: int,
    bom: uint16): Timestamp {.gcsafe, raises: [ValueError].} =
  ## Deserialize a Timestamp from src at offset according to BOM.
  ## The 64-bit unsigned value is reinterpreted as signed int64.
  ## Raises ValueError if offset is out of bounds.
  let uval = if bom == BOM_BIG_ENDIAN:
               self.readUint64BE(src, offset)
             else:
               self.readUint64LE(src, offset)
  result = cast[Timestamp](uval)

proc decodeRequest*(self: PacketCodec, buffer: openArray[uint8]): tuple[
    bom: uint16, request: Request] {.gcsafe, raises: [ValueError].} =
  ## Decode a SyncRequest packet.
  if buffer.len < REQUEST_PACKET_SIZE:
    raise newException(ValueError, "Packet too small: " & $buffer.len)
  let bom = self.readUint16BE(buffer, 0)
  if buffer[2] != MSG_SYNC_REQUEST:
    raise newException(ValueError, "Invalid message type: " & $buffer[2])
  let t1 = self.bytesToTimestamp(buffer, 3, bom)
  result = (bom, Request(t1: t1))

proc encodeRequest*(self: PacketCodec, t1: Timestamp,
    bom: uint16 = BOM_BIG_ENDIAN): seq[uint8] {.gcsafe, raises: [ValueError].} =
  ## Encode a SyncRequest packet.
  result = newSeq[uint8](REQUEST_PACKET_SIZE)
  # BOM (always in big-endian on the wire for consistency)
  self.writeUint16BE(bom, result, 0)
  # Message type
  result[2] = MSG_SYNC_REQUEST
  # Timestamp t1 (serialized per BOM)
  self.timestampToBytes(t1, bom, result, 3)

proc decodeResponse*(self: PacketCodec, buffer: openArray[uint8]): tuple[
    bom: uint16, response: Response] {.gcsafe, raises: [ValueError].} =
  ## Decode a SyncResponse packet.
  if buffer.len < RESPONSE_PACKET_SIZE:
    raise newException(ValueError, "Packet too small: " & $buffer.len)
  let bom = self.readUint16BE(buffer, 0)
  if buffer[2] != MSG_SYNC_RESPONSE:
    raise newException(ValueError, "Invalid message type: " & $buffer[2])
  let t2 = self.bytesToTimestamp(buffer, 3, bom)
  let t3 = self.bytesToTimestamp(buffer, 11, bom)
  result = (bom, Response(t2: t2, t3: t3))

proc encodeResponse*(self: PacketCodec, t2, t3: Timestamp,
    requestBom: uint16): seq[uint8] {.gcsafe, raises: [ValueError].} =
  ## Encode a SyncResponse packet using the same BOM as the request.
  result = newSeq[uint8](RESPONSE_PACKET_SIZE)
  self.writeUint16BE(requestBom, result, 0)
  result[2] = MSG_SYNC_RESPONSE
  self.timestampToBytes(t2, requestBom, result, 3)
  self.timestampToBytes(t3, requestBom, result, 11)
