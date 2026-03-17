# Binary Serialization for Fractio
#
# High-performance binary serialization primitives.
# Uses length-prefixed encoding for variable-length fields.
# Little-endian byte order for consistency across platforms.
#
# Design goals:
# - Zero-allocation decode for fixed-size types
# - Length-prefixed strings/sequences for variable-length data
# - No type tags (caller knows the schema)
# - No alignment requirements (packed)

# =============================================================================
# Byte Order Helpers
# =============================================================================

proc toBytesLE*(value: uint16): array[2, byte] {.inline.} =
  ## Convert uint16 to little-endian bytes
  result[0] = byte(value and 0xFF'u16)
  result[1] = byte((value shr 8) and 0xFF'u16)

proc toBytesLE*(value: uint32): array[4, byte] {.inline.} =
  ## Convert uint32 to little-endian bytes
  result[0] = byte(value and 0xFF'u32)
  result[1] = byte((value shr 8) and 0xFF'u32)
  result[2] = byte((value shr 16) and 0xFF'u32)
  result[3] = byte((value shr 24) and 0xFF'u32)

proc toBytesLE*(value: uint64): array[8, byte] {.inline.} =
  ## Convert uint64 to little-endian bytes
  result[0] = byte(value and 0xFF'u64)
  result[1] = byte((value shr 8) and 0xFF'u64)
  result[2] = byte((value shr 16) and 0xFF'u64)
  result[3] = byte((value shr 24) and 0xFF'u64)
  result[4] = byte((value shr 32) and 0xFF'u64)
  result[5] = byte((value shr 40) and 0xFF'u64)
  result[6] = byte((value shr 48) and 0xFF'u64)
  result[7] = byte((value shr 56) and 0xFF'u64)

proc toBytesLE*(value: int32): array[4, byte] {.inline.} =
  toBytesLE(cast[uint32](value))

proc toBytesLE*(value: int64): array[8, byte] {.inline.} =
  toBytesLE(cast[uint64](value))

proc fromBytesU16*(bytes: openArray[byte]): uint16 {.inline.} =
  ## Convert little-endian bytes to uint16
  result = uint16(bytes[0]) or (uint16(bytes[1]) shl 8)

proc fromBytesU32*(bytes: openArray[byte]): uint32 {.inline.} =
  ## Convert little-endian bytes to uint32
  result = uint32(bytes[0]) or
           (uint32(bytes[1]) shl 8) or
           (uint32(bytes[2]) shl 16) or
           (uint32(bytes[3]) shl 24)

proc fromBytesU64*(bytes: openArray[byte]): uint64 {.inline.} =
  ## Convert little-endian bytes to uint64
  result = uint64(bytes[0]) or
           (uint64(bytes[1]) shl 8) or
           (uint64(bytes[2]) shl 16) or
           (uint64(bytes[3]) shl 24) or
           (uint64(bytes[4]) shl 32) or
           (uint64(bytes[5]) shl 40) or
           (uint64(bytes[6]) shl 48) or
           (uint64(bytes[7]) shl 56)

proc fromBytesI32*(bytes: openArray[byte]): int32 {.inline.} =
  cast[int32](fromBytesU32(bytes))

proc fromBytesI64*(bytes: openArray[byte]): int64 {.inline.} =
  cast[int64](fromBytesU64(bytes))

# =============================================================================
# Binary Writer
# =============================================================================

type
  BinaryWriter* = object
    ## Incremental binary buffer writer
    data*: seq[byte]
    pos*: int

proc initBinaryWriter*(initialSize: int = 256): BinaryWriter =
  ## Create a new binary writer with optional initial capacity
  result.data = newSeq[byte](initialSize)
  result.pos = 0

proc ensureCapacity*(w: var BinaryWriter, needed: int) {.inline.} =
  ## Ensure buffer has space for 'needed' more bytes
  let totalNeeded = w.pos + needed
  if totalNeeded > w.data.len:
    var newSize = w.data.len
    while newSize < totalNeeded:
      newSize = newSize * 2
    var newData = newSeq[byte](newSize)
    copyMem(addr newData[0], addr w.data[0], w.pos)
    w.data = newData

proc writeU8*(w: var BinaryWriter, value: uint8) {.inline.} =
  ## Write a single byte
  w.ensureCapacity(1)
  w.data[w.pos] = value
  inc w.pos

proc writeU16*(w: var BinaryWriter, value: uint16) {.inline.} =
  ## Write a uint16 (little-endian)
  w.ensureCapacity(2)
  let bytes = toBytesLE(value)
  w.data[w.pos] = bytes[0]
  w.data[w.pos + 1] = bytes[1]
  inc w.pos, 2

proc writeU32*(w: var BinaryWriter, value: uint32) {.inline.} =
  ## Write a uint32 (little-endian)
  w.ensureCapacity(4)
  let bytes = toBytesLE(value)
  for i in 0..<4:
    w.data[w.pos + i] = bytes[i]
  inc w.pos, 4

proc writeU64*(w: var BinaryWriter, value: uint64) {.inline.} =
  ## Write a uint64 (little-endian)
  w.ensureCapacity(8)
  let bytes = toBytesLE(value)
  for i in 0..<8:
    w.data[w.pos + i] = bytes[i]
  inc w.pos, 8

proc writeI32*(w: var BinaryWriter, value: int32) {.inline.} =
  ## Write an int32 (little-endian)
  writeU32(w, cast[uint32](value))

proc writeI64*(w: var BinaryWriter, value: int64) {.inline.} =
  ## Write an int64 (little-endian)
  writeU64(w, cast[uint64](value))

proc writeFloat64*(w: var BinaryWriter, value: float64) {.inline.} =
  ## Write a float64 (IEEE 754, little-endian)
  writeU64(w, cast[uint64](value))

proc writeString*(w: var BinaryWriter, value: string) {.inline.} =
  ## Write a length-prefixed string (u32 length + bytes)
  let len = value.len
  writeU32(w, uint32(len))
  if len > 0:
    w.ensureCapacity(len)
    copyMem(addr w.data[w.pos], unsafeAddr value[0], len)
    inc w.pos, len

proc writeBytes*(w: var BinaryWriter, value: openArray[byte]) {.inline.} =
  ## Write raw bytes (no length prefix)
  let len = value.len
  if len > 0:
    w.ensureCapacity(len)
    copyMem(addr w.data[w.pos], unsafeAddr value[0], len)
    inc w.pos, len

proc writeSeqU32*(w: var BinaryWriter, values: seq[uint32]) {.inline.} =
  ## Write a length-prefixed sequence of uint32
  writeU32(w, uint32(values.len))
  for v in values:
    writeU32(w, v)

proc writeSeqU64*(w: var BinaryWriter, values: seq[uint64]) {.inline.} =
  ## Write a length-prefixed sequence of uint64
  writeU32(w, uint32(values.len))
  for v in values:
    writeU64(w, v)

proc writeSeqI32*(w: var BinaryWriter, values: seq[int32]) {.inline.} =
  ## Write a length-prefixed sequence of int32
  writeU32(w, uint32(values.len))
  for v in values:
    writeI32(w, v)

proc finish*(w: var BinaryWriter): string =
  ## Finalize and return the written data as a string
  result = newString(w.pos)
  if w.pos > 0:
    copyMem(addr result[0], addr w.data[0], w.pos)

# =============================================================================
# Binary Reader
# =============================================================================

type
  BinaryReader* = object
    ## Binary buffer reader
    data*: string
    pos*: int

proc initBinaryReader*(data: string): BinaryReader {.inline.} =
  ## Create a reader for the given binary data
  result.data = data
  result.pos = 0

proc remaining*(r: BinaryReader): int {.inline.} =
  ## Bytes remaining to read
  r.data.len - r.pos

proc readU8*(r: var BinaryReader): uint8 {.inline.} =
  ## Read a single byte
  if r.pos >= r.data.len:
    raise newException(ValueError, "BinaryReader: unexpected end of data")
  result = cast[uint8](r.data[r.pos])
  inc r.pos

proc readU16*(r: var BinaryReader): uint16 {.inline.} =
  ## Read a uint16 (little-endian)
  if r.pos + 2 > r.data.len:
    raise newException(ValueError, "BinaryReader: unexpected end of data")
  var bytes: array[2, byte]
  bytes[0] = cast[uint8](r.data[r.pos])
  bytes[1] = cast[uint8](r.data[r.pos + 1])
  result = fromBytesU16(bytes)
  inc r.pos, 2

proc readU32*(r: var BinaryReader): uint32 {.inline.} =
  ## Read a uint32 (little-endian)
  if r.pos + 4 > r.data.len:
    raise newException(ValueError, "BinaryReader: unexpected end of data")
  var bytes: array[4, byte]
  for i in 0..<4:
    bytes[i] = cast[uint8](r.data[r.pos + i])
  result = fromBytesU32(bytes)
  inc r.pos, 4

proc readU64*(r: var BinaryReader): uint64 {.inline.} =
  ## Read a uint64 (little-endian)
  if r.pos + 8 > r.data.len:
    raise newException(ValueError, "BinaryReader: unexpected end of data")
  var bytes: array[8, byte]
  for i in 0..<8:
    bytes[i] = cast[uint8](r.data[r.pos + i])
  result = fromBytesU64(bytes)
  inc r.pos, 8

proc readI32*(r: var BinaryReader): int32 {.inline.} =
  ## Read an int32 (little-endian)
  cast[int32](readU32(r))

proc readI64*(r: var BinaryReader): int64 {.inline.} =
  ## Read an int64 (little-endian)
  cast[int64](readU64(r))

proc readFloat64*(r: var BinaryReader): float64 {.inline.} =
  ## Read a float64 (IEEE 754, little-endian)
  cast[float64](readU64(r))

proc readString*(r: var BinaryReader): string {.inline.} =
  ## Read a length-prefixed string
  let len = int(readU32(r))
  if len == 0:
    return ""
  if r.pos + len > r.data.len:
    raise newException(ValueError, "BinaryReader: unexpected end of data in string")
  result = newString(len)
  copyMem(addr result[0], addr r.data[r.pos], len)
  inc r.pos, len

proc readBytes*(r: var BinaryReader, len: int): seq[byte] {.inline.} =
  ## Read 'len' raw bytes
  if r.pos + len > r.data.len:
    raise newException(ValueError, "BinaryReader: unexpected end of data")
  result = newSeq[byte](len)
  if len > 0:
    copyMem(addr result[0], addr r.data[r.pos], len)
    inc r.pos, len

proc readSeqU32*(r: var BinaryReader): seq[uint32] {.inline.} =
  ## Read a length-prefixed sequence of uint32
  let len = int(readU32(r))
  result = newSeq[uint32](len)
  for i in 0..<len:
    result[i] = readU32(r)

proc readSeqU64*(r: var BinaryReader): seq[uint64] {.inline.} =
  ## Read a length-prefixed sequence of uint64
  let len = int(readU32(r))
  result = newSeq[uint64](len)
  for i in 0..<len:
    result[i] = readU64(r)

proc readSeqI32*(r: var BinaryReader): seq[int32] {.inline.} =
  ## Read a length-prefixed sequence of int32
  let len = int(readU32(r))
  result = newSeq[int32](len)
  for i in 0..<len:
    result[i] = readI32(r)

# =============================================================================
# Fixed-size record encode/decode (zero-copy for simple types)
# =============================================================================

proc encodeRecord*[T: object](value: T): string =
  ## Encode a simple object to binary (direct memory copy)
  ## Only works for objects without GC-managed fields
  result = newString(sizeof(value))
  copyMem(addr result[0], unsafeAddr value, sizeof(value))

proc decodeRecord*[T: object](data: string): T =
  ## Decode binary data to a simple object (direct memory copy)
  ## Only works for objects without GC-managed fields
  if data.len < sizeof(result):
    raise newException(ValueError, "BinaryReader: data too small for record")
  copyMem(addr result, unsafeAddr data[0], sizeof(T))
