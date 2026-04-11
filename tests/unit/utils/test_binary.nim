# Unit tests for fractio/utils/binary.nim
# Tests binary serialization primitives with comprehensive coverage

import std/[unittest, strutils]
import fractio/utils/binary

suite "Byte Order Helpers - toBytesLE":

  test "uint16 to bytes little-endian":
    let value: uint16 = 0x1234
    let bytes = toBytesLE(value)
    check bytes[0] == 0x34'u8 # Low byte first (LE)
    check bytes[1] == 0x12'u8 # High byte

  test "uint16 zero":
    let bytes = toBytesLE(0'u16)
    check bytes[0] == 0'u8
    check bytes[1] == 0'u8

  test "uint16 max value":
    let bytes = toBytesLE(0xFFFF'u16)
    check bytes[0] == 0xFF'u8
    check bytes[1] == 0xFF'u8

  test "uint32 to bytes little-endian":
    let value: uint32 = 0x12345678
    let bytes = toBytesLE(value)
    check bytes[0] == 0x78'u8
    check bytes[1] == 0x56'u8
    check bytes[2] == 0x34'u8
    check bytes[3] == 0x12'u8

  test "uint32 zero":
    let bytes = toBytesLE(0'u32)
    for b in bytes:
      check b == 0'u8

  test "uint32 max value":
    let bytes = toBytesLE(0xFFFFFFFF'u32)
    for b in bytes:
      check b == 0xFF'u8

  test "uint64 to bytes little-endian":
    let value: uint64 = 0x0123456789ABCDEF'u64
    let bytes = toBytesLE(value)
    check bytes[0] == 0xEF'u8
    check bytes[1] == 0xCD'u8
    check bytes[2] == 0xAB'u8
    check bytes[3] == 0x89'u8
    check bytes[4] == 0x67'u8
    check bytes[5] == 0x45'u8
    check bytes[6] == 0x23'u8
    check bytes[7] == 0x01'u8
    check bytes[1] == 0xCD'u8
    check bytes[2] == 0xAB'u8
    check bytes[3] == 0x89'u8
    check bytes[4] == 0x67'u8
    check bytes[5] == 0x45'u8
    check bytes[6] == 0x23'u8
    check bytes[7] == 0x01'u8

  test "uint64 zero":
    let bytes = toBytesLE(0'u64)
    for b in bytes:
      check b == 0'u8

  test "uint64 max value":
    let bytes = toBytesLE(0xFFFFFFFFFFFFFFFF'u64)
    for b in bytes:
      check b == 0xFF'u8

  test "int32 to bytes":
    let value: int32 = -1
    let bytes = toBytesLE(value)
    for b in bytes:
      check b == 0xFF'u8 # -1 in two's complement

  test "int32 positive":
    let value: int32 = 0x12345678
    let bytes = toBytesLE(value)
    check bytes[0] == 0x78'u8

  test "int64 to bytes":
    let value: int64 = -1
    let bytes = toBytesLE(value)
    for b in bytes:
      check b == 0xFF'u8

  test "int64 positive":
    let value: int64 = cast[int64](0x0123456789ABCDEF'u64)
    let bytes = toBytesLE(value)
    check bytes[0] == 0xEF'u8

suite "Byte Order Helpers - fromBytes":

  test "uint16 from bytes":
    let bytes: array[2, byte] = [0x34'u8, 0x12'u8]
    let value = fromBytesU16(bytes)
    check value == 0x1234'u16

  test "uint16 from zero bytes":
    let bytes: array[2, byte] = [0'u8, 0'u8]
    check fromBytesU16(bytes) == 0'u16

  test "uint16 from max bytes":
    let bytes: array[2, byte] = [0xFF'u8, 0xFF'u8]
    check fromBytesU16(bytes) == 0xFFFF'u16

  test "uint32 from bytes":
    let bytes: array[4, byte] = [0x78'u8, 0x56'u8, 0x34'u8, 0x12'u8]
    let value = fromBytesU32(bytes)
    check value == 0x12345678'u32

  test "uint32 from zero bytes":
    let bytes: array[4, byte] = [0'u8, 0'u8, 0'u8, 0'u8]
    check fromBytesU32(bytes) == 0'u32

  test "uint32 from max bytes":
    let bytes: array[4, byte] = [0xFF'u8, 0xFF'u8, 0xFF'u8, 0xFF'u8]
    check fromBytesU32(bytes) == 0xFFFFFFFF'u32

  test "uint64 from bytes":
    let bytes: array[8, byte] = [0xEF'u8, 0xCD'u8, 0xAB'u8, 0x89'u8,
                                  0x67'u8, 0x45'u8, 0x23'u8, 0x01'u8]
    let value = fromBytesU64(bytes)
    check value == 0x0123456789ABCDEF'u64

  test "uint64 from zero bytes":
    let bytes: array[8, byte] = [0'u8, 0'u8, 0'u8, 0'u8,
                                  0'u8, 0'u8, 0'u8, 0'u8]
    check fromBytesU64(bytes) == 0'u64

  test "uint64 from max bytes":
    let bytes: array[8, byte] = [0xFF'u8, 0xFF'u8, 0xFF'u8, 0xFF'u8,
                                  0xFF'u8, 0xFF'u8, 0xFF'u8, 0xFF'u8]
    check fromBytesU64(bytes) == 0xFFFFFFFFFFFFFFFF'u64

  test "int32 from bytes negative":
    let bytes: array[4, byte] = [0xFF'u8, 0xFF'u8, 0xFF'u8, 0xFF'u8]
    let value = fromBytesI32(bytes)
    check value == -1'i32

  test "int32 from bytes positive":
    let bytes: array[4, byte] = [0x78'u8, 0x56'u8, 0x34'u8, 0x12'u8]
    let value = fromBytesI32(bytes)
    check value == 0x12345678'i32

  test "int64 from bytes negative":
    let bytes: array[8, byte] = [0xFF'u8, 0xFF'u8, 0xFF'u8, 0xFF'u8,
                                  0xFF'u8, 0xFF'u8, 0xFF'u8, 0xFF'u8]
    let value = fromBytesI64(bytes)
    check value == -1'i64

  test "int64 from bytes positive":
    let bytes: array[8, byte] = [0xEF'u8, 0xCD'u8, 0xAB'u8, 0x89'u8,
                                  0x67'u8, 0x45'u8, 0x23'u8, 0x01'u8]
    let value = fromBytesI64(bytes)
    check value == cast[int64](0x0123456789ABCDEF'u64)

suite "BinaryWriter":

  test "initBinaryWriter creates empty writer":
    var w = initBinaryWriter()
    check w.pos == 0
    check w.data.len == 256 # Default size

  test "initBinaryWriter with custom size":
    var w = initBinaryWriter(512)
    check w.pos == 0
    check w.data.len == 512

  test "writeU8 single byte":
    var w = initBinaryWriter()
    w.writeU8(0xAB'u8)
    check w.pos == 1
    check w.data[0] == 0xAB'u8

  test "writeU8 multiple bytes":
    var w = initBinaryWriter()
    w.writeU8(0x01'u8)
    w.writeU8(0x02'u8)
    w.writeU8(0x03'u8)
    check w.pos == 3
    check w.data[0] == 0x01'u8
    check w.data[1] == 0x02'u8
    check w.data[2] == 0x03'u8

  test "writeU16":
    var w = initBinaryWriter()
    w.writeU16(0x1234'u16)
    check w.pos == 2
    check w.data[0] == 0x34'u8
    check w.data[1] == 0x12'u8

  test "writeU32":
    var w = initBinaryWriter()
    w.writeU32(0x12345678'u32)
    check w.pos == 4
    check w.data[0] == 0x78'u8
    check w.data[1] == 0x56'u8
    check w.data[2] == 0x34'u8
    check w.data[3] == 0x12'u8

  test "writeU64":
    var w = initBinaryWriter()
    w.writeU64(0x0123456789ABCDEF'u64)
    check w.pos == 8
    check w.data[0] == 0xEF'u8

  test "writeI32":
    var w = initBinaryWriter()
    w.writeI32(-1'i32)
    check w.pos == 4
    for i in 0..<4:
      check w.data[i] == 0xFF'u8

  test "writeI64":
    var w = initBinaryWriter()
    w.writeI64(-1'i64)
    check w.pos == 8
    for i in 0..<8:
      check w.data[i] == 0xFF'u8

  test "writeFloat64":
    var w = initBinaryWriter()
    let value: float64 = 1.5
    w.writeFloat64(value)
    check w.pos == 8

  test "writeString empty":
    var w = initBinaryWriter()
    w.writeString("")
    check w.pos == 4 # Just the length prefix (0)

  test "writeString simple":
    var w = initBinaryWriter()
    w.writeString("hello")
    check w.pos == 4 + 5 # Length prefix + content
    # Read back length
    var r = initBinaryReader(w.finish())
    let len = r.readU32()
    check len == 5'u32

  test "writeString with special chars":
    var w = initBinaryWriter()
    w.writeString("hello\x00world")
    check w.pos == 4 + 11 # Including embedded null

  test "writeBytes from seq":
    var w = initBinaryWriter()
    w.writeBytes(@[1'u8, 2'u8, 3'u8])
    check w.pos == 3

  test "writeBytes from string":
    var w = initBinaryWriter()
    w.writeBytes("abc")
    check w.pos == 3

  test "writeSeqU32 empty":
    var w = initBinaryWriter()
    w.writeSeqU32(@[])
    check w.pos == 4 # Just length prefix

  test "writeSeqU32 with values":
    var w = initBinaryWriter()
    w.writeSeqU32(@[1'u32, 2'u32, 3'u32])
    check w.pos == 4 + 3 * 4 # Length + 3 elements

  test "writeSeqU64 empty":
    var w = initBinaryWriter()
    w.writeSeqU64(@[])
    check w.pos == 4

  test "writeSeqU64 with values":
    var w = initBinaryWriter()
    w.writeSeqU64(@[1'u64, 2'u64])
    check w.pos == 4 + 2 * 8

  test "writeSeqI32 empty":
    var w = initBinaryWriter()
    w.writeSeqI32(@[])
    check w.pos == 4

  test "writeSeqI32 with values":
    var w = initBinaryWriter()
    w.writeSeqI32(@[-1'i32, 0'i32, 1'i32])
    check w.pos == 4 + 3 * 4

  test "ensureCapacity grows buffer":
    var w = initBinaryWriter(8) # Small initial
    w.ensureCapacity(100)
    check w.data.len >= 100

  test "ensureCapacity multiple grows":
    var w = initBinaryWriter(4)
    # Write more than initial capacity
    for i in 0..<100:
      w.writeU8(uint8(i))
    check w.pos == 100
    check w.data.len >= 100

  test "finish returns exact data":
    var w = initBinaryWriter()
    w.writeU8(0x01'u8)
    w.writeU8(0x02'u8)
    let data = w.finish()
    check data.len == 2
    check data[0] == '\x01'
    check data[1] == '\x02'

  test "finish empty":
    var w = initBinaryWriter()
    let data = w.finish()
    check data.len == 0

suite "BinaryReader":

  test "initBinaryReader":
    let data = "hello"
    var r = initBinaryReader(data)
    check r.data == data
    check r.pos == 0

  test "remaining":
    var r = initBinaryReader("abc")
    check r.remaining() == 3
    discard r.readU8()
    check r.remaining() == 2
    discard r.readU8()
    check r.remaining() == 1

  test "readU8":
    var r = initBinaryReader("\xAB")
    let value = r.readU8()
    check value == 0xAB'u8
    check r.pos == 1

  test "readU8 raises on empty":
    var r = initBinaryReader("")
    var raised = false
    try:
      discard r.readU8()
    except ValueError:
      raised = true
    check raised

  test "readU16":
    var w = initBinaryWriter()
    w.writeU16(0x1234'u16)
    var r = initBinaryReader(w.finish())
    let value = r.readU16()
    check value == 0x1234'u16

  test "readU32":
    var w = initBinaryWriter()
    w.writeU32(0x12345678'u32)
    var r = initBinaryReader(w.finish())
    let value = r.readU32()
    check value == 0x12345678'u32

  test "readU64":
    var w = initBinaryWriter()
    w.writeU64(0x0123456789ABCDEF'u64)
    var r = initBinaryReader(w.finish())
    let value = r.readU64()
    check value == 0x0123456789ABCDEF'u64

  test "readI32":
    var w = initBinaryWriter()
    w.writeI32(-1'i32)
    var r = initBinaryReader(w.finish())
    let value = r.readI32()
    check value == -1'i32

  test "readI64":
    var w = initBinaryWriter()
    w.writeI64(-1'i64)
    var r = initBinaryReader(w.finish())
    let value = r.readI64()
    check value == -1'i64

  test "readFloat64":
    var w = initBinaryWriter()
    let original: float64 = 1.5
    w.writeFloat64(original)
    var r = initBinaryReader(w.finish())
    let value = r.readFloat64()
    check value == original

  test "readString empty":
    var w = initBinaryWriter()
    w.writeString("")
    var r = initBinaryReader(w.finish())
    let value = r.readString()
    check value == ""

  test "readString simple":
    var w = initBinaryWriter()
    w.writeString("hello world")
    var r = initBinaryReader(w.finish())
    let value = r.readString()
    check value == "hello world"

  test "readString with special chars":
    var w = initBinaryWriter()
    w.writeString("hello\x00world")
    var r = initBinaryReader(w.finish())
    let value = r.readString()
    check value == "hello\x00world"

  test "readBytes":
    var w = initBinaryWriter()
    w.writeBytes(@[1'u8, 2'u8, 3'u8, 4'u8])
    var r = initBinaryReader(w.finish())
    let value = r.readBytes(4)
    check value == @[1'u8, 2'u8, 3'u8, 4'u8]

  test "readBytes zero length":
    var w = initBinaryWriter()
    var r = initBinaryReader(w.finish())
    let value = r.readBytes(0)
    check value.len == 0

  test "readFixedString":
    var w = initBinaryWriter()
    w.writeBytes("12345")
    var r = initBinaryReader(w.finish())
    let value = r.readFixedString(5)
    check value == "12345"

  test "readSeqU32 empty":
    var w = initBinaryWriter()
    w.writeSeqU32(@[])
    var r = initBinaryReader(w.finish())
    let value = r.readSeqU32()
    check value.len == 0

  test "readSeqU32 with values":
    var w = initBinaryWriter()
    w.writeSeqU32(@[1'u32, 2'u32, 3'u32])
    var r = initBinaryReader(w.finish())
    let value = r.readSeqU32()
    check value == @[1'u32, 2'u32, 3'u32]

  test "readSeqU64 empty":
    var w = initBinaryWriter()
    w.writeSeqU64(@[])
    var r = initBinaryReader(w.finish())
    let value = r.readSeqU64()
    check value.len == 0

  test "readSeqU64 with values":
    var w = initBinaryWriter()
    w.writeSeqU64(@[100'u64, 200'u64])
    var r = initBinaryReader(w.finish())
    let value = r.readSeqU64()
    check value == @[100'u64, 200'u64]

  test "readSeqI32 empty":
    var w = initBinaryWriter()
    w.writeSeqI32(@[])
    var r = initBinaryReader(w.finish())
    let value = r.readSeqI32()
    check value.len == 0

  test "readSeqI32 with values":
    var w = initBinaryWriter()
    w.writeSeqI32(@[-10'i32, 0'i32, 10'i32])
    var r = initBinaryReader(w.finish())
    let value = r.readSeqI32()
    check value == @[-10'i32, 0'i32, 10'i32]

  test "read past end raises":
    var r = initBinaryReader("\x01")
    discard r.readU8()
    var raised = false
    try:
      discard r.readU8()
    except ValueError:
      raised = true
    check raised

  test "readString past end raises":
    var w = initBinaryWriter()
    w.writeU32(100'u32) # Claim 100 chars but no data
    var r = initBinaryReader(w.finish())
    var raised = false
    try:
      discard r.readString()
    except ValueError:
      raised = true
    check raised

suite "encodeRecord / decodeRecord":

  test "encode simple object":
    type SimpleObj = object
      a: int32
      b: uint32
    let obj = SimpleObj(a: -1'i32, b: 0xFFFFFFFF'u32)
    let data = encodeRecord(obj)
    check data.len == sizeof(SimpleObj)

  test "decode simple object":
    type SimpleObj = object
      a: int32
      b: uint32
    let original = SimpleObj(a: 123, b: 456)
    let data = encodeRecord(original)
    let decoded = decodeRecord[SimpleObj](data)
    check decoded.a == original.a
    check decoded.b == original.b

  test "encode/decode larger object":
    type LargerObj = object
      x: int64
      y: int64
      z: uint32
      w: uint16
    let original = LargerObj(x: -123456789, y: 987654321, z: 100, w: 200)
    let data = encodeRecord(original)
    let decoded = decodeRecord[LargerObj](data)
    check decoded.x == original.x
    check decoded.y == original.y
    check decoded.z == original.z
    check decoded.w == original.w

  test "decodeRecord raises on small data":
    type SimpleObj = object
      a: int64
    var raised = false
    try:
      discard decodeRecord[SimpleObj]("abc") # Too small
    except ValueError:
      raised = true
    check raised

suite "Roundtrip Tests":

  test "full roundtrip uint16":
    for v in [0'u16, 1'u16, 0xFF'u16, 0xFFFF'u16, 0x1234'u16]:
      let bytes = toBytesLE(v)
      let restored = fromBytesU16(bytes)
      check restored == v

  test "full roundtrip uint32":
    for v in [0'u32, 1'u32, 0xFF'u32, 0xFFFF'u32, 0xFFFFFFFF'u32,
        0x12345678'u32]:
      let bytes = toBytesLE(v)
      let restored = fromBytesU32(bytes)
      check restored == v

  test "full roundtrip uint64":
    for v in [0'u64, 1'u64, 0xFFFFFFFF'u64, 0xFFFFFFFFFFFFFFFF'u64,
        0x0123456789ABCDEF'u64]:
      let bytes = toBytesLE(v)
      let restored = fromBytesU64(bytes)
      check restored == v

  test "writer-reader roundtrip complex":
    var w = initBinaryWriter()
    w.writeU8(0xAB'u8)
    w.writeU16(0x1234'u16)
    w.writeU32(0x12345678'u32)
    w.writeU64(0x0123456789ABCDEF'u64)
    w.writeI32(-1'i32)
    w.writeI64(-2'i64)
    w.writeString("test string")
    w.writeSeqU32(@[1'u32, 2'u32, 3'u32])
    w.writeSeqI32(@[-1'i32, 0'i32, 1'i32])

    var r = initBinaryReader(w.finish())
    check r.readU8() == 0xAB'u8
    check r.readU16() == 0x1234'u16
    check r.readU32() == 0x12345678'u32
    check r.readU64() == 0x0123456789ABCDEF'u64
    check r.readI32() == -1'i32
    check r.readI64() == cast[int64](-2'i64)
    check r.readString() == "test string"
    check r.readSeqU32() == @[1'u32, 2'u32, 3'u32]
    check r.readSeqI32() == @[-1'i32, 0'i32, 1'i32]
