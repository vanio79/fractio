# Unit tests for Protocol Codec
# Tests binary encoding/decoding primitives with edge cases and error handling

import unittest
import std/strutils
import fractio/protocol/codec
import fractio/protocol/types

suite "Protocol Codec - Write Operations":
  test "write uint8":
    var buf = ""
    buf.writeUint8(0x42)
    check buf.len == 1
    check buf[0] == char(0x42)

  test "write uint8 zero":
    var buf = ""
    buf.writeUint8(0)
    check buf.len == 1
    check buf[0] == char(0)

  test "write uint8 max":
    var buf = ""
    buf.writeUint8(255)
    check buf.len == 1
    check buf[0] == char(255)

  test "write uint16 BE":
    var buf = ""
    buf.writeUint16BE(0x1234)
    check buf.len == 2
    check buf[0] == char(0x12)
    check buf[1] == char(0x34)

  test "write uint16 BE zero":
    var buf = ""
    buf.writeUint16BE(0)
    check buf.len == 2
    check buf == "\x00\x00"

  test "write uint16 BE max":
    var buf = ""
    buf.writeUint16BE(65535)
    check buf.len == 2
    check buf[0] == char(0xFF)
    check buf[1] == char(0xFF)

  test "write uint32 BE":
    var buf = ""
    buf.writeUint32BE(0x12345678)
    check buf.len == 4
    check buf[0] == char(0x12)
    check buf[1] == char(0x34)
    check buf[2] == char(0x56)
    check buf[3] == char(0x78)

  test "write uint32 BE zero":
    var buf = ""
    buf.writeUint32BE(0)
    check buf.len == 4
    check buf == "\x00\x00\x00\x00"

  test "write uint32 BE max":
    var buf = ""
    buf.writeUint32BE(uint32(0xFFFFFFFF))
    check buf.len == 4
    check buf == "\xFF\xFF\xFF\xFF"

  test "write uint64 BE":
    var buf = ""
    buf.writeUint64BE(0x0102030405060708'u64)
    check buf.len == 8
    check buf[0] == char(0x01)
    check buf[1] == char(0x02)
    check buf[2] == char(0x03)
    check buf[3] == char(0x04)
    check buf[4] == char(0x05)
    check buf[5] == char(0x06)
    check buf[6] == char(0x07)
    check buf[7] == char(0x08)

  test "write uint64 BE zero":
    var buf = ""
    buf.writeUint64BE(0'u64)
    check buf.len == 8
    check buf == "\x00\x00\x00\x00\x00\x00\x00\x00"

  test "write uint64 BE max":
    var buf = ""
    buf.writeUint64BE(0xFFFFFFFFFFFFFFFF'u64)
    check buf.len == 8
    for i in 0..<8:
      check buf[i] == char(0xFF)

  test "write bytes with length prefix":
    var buf = ""
    buf.writeBytes("Hello")
    check buf.len == 9 # 4 bytes length + 5 bytes data
    # Check length prefix
    check buf[0] == char(0x00)
    check buf[1] == char(0x00)
    check buf[2] == char(0x00)
    check buf[3] == char(0x05)
    # Check data
    check buf[4] == 'H'
    check buf[5] == 'e'
    check buf[6] == 'l'
    check buf[7] == 'l'
    check buf[8] == 'o'

  test "write bytes empty":
    var buf = ""
    buf.writeBytes("")
    check buf.len == 4 # Just length prefix (0)
    check buf == "\x00\x00\x00\x00"

  test "write bytes large":
    var buf = ""
    let largeData = "x".repeat(1000)
    buf.writeBytes(largeData)
    check buf.len == 1004
    # Check length prefix (1000 = 0x000003E8)
    check buf[0] == char(0x00)
    check buf[1] == char(0x00)
    check buf[2] == char(0x03)
    check buf[3] == char(0xE8)

  test "write bytes8":
    var buf = ""
    buf.writeBytes8("Hello")
    check buf.len == 6 # 1 byte length + 5 bytes data
    check buf[0] == char(5)
    check buf[1] == 'H'

  test "write bytes8 empty":
    var buf = ""
    buf.writeBytes8("")
    check buf.len == 1
    check buf[0] == char(0)

  test "write bytes8 max length":
    var buf = ""
    let data = "x".repeat(255)
    buf.writeBytes8(data)
    check buf.len == 256
    check buf[0] == char(255)

  test "write bytes16":
    var buf = ""
    buf.writeBytes16("Hello")
    check buf.len == 7 # 2 bytes length + 5 bytes data
    check buf[0] == char(0x00)
    check buf[1] == char(0x05)

  test "write bytes16 empty":
    var buf = ""
    buf.writeBytes16("")
    check buf.len == 2
    check buf == "\x00\x00"

  test "write bytes16 max length":
    var buf = ""
    let data = "x".repeat(65535)
    buf.writeBytes16(data)
    check buf.len == 65537
    check buf[0] == char(0xFF)
    check buf[1] == char(0xFF)

  test "write bytes32":
    var buf = ""
    buf.writeBytes32("Hello")
    check buf.len == 9 # 4 bytes length + 5 bytes data

  test "write int32 BE":
    var buf = ""
    buf.writeInt32BE(0x12345678)
    check buf.len == 4
    check buf[0] == char(0x12)
    check buf[1] == char(0x34)

  test "write int32 BE negative":
    var buf = ""
    buf.writeInt32BE(-1)
    check buf.len == 4
    check buf == "\xFF\xFF\xFF\xFF"

  test "write int32 BE min":
    var buf = ""
    buf.writeInt32BE(-2147483648) # INT32_MIN
    check buf.len == 4
    check buf[0] == char(0x80)

  test "write int64 BE":
    var buf = ""
    buf.writeInt64BE(0x0102030405060708)
    check buf.len == 8

  test "write int64 BE negative":
    var buf = ""
    buf.writeInt64BE(-1)
    check buf.len == 8
    for i in 0..<8:
      check buf[i] == char(0xFF)

suite "Protocol Codec - Read Operations":
  test "read uint8":
    var buf = "\x42"
    var pos = 0
    let result = readUint8(buf, pos)
    check result.isOk
    check result.value == 0x42
    check pos == 1

  test "read uint8 zero":
    var buf = "\x00"
    var pos = 0
    let result = readUint8(buf, pos)
    check result.isOk
    check result.value == 0

  test "read uint8 max":
    var buf = "\xFF"
    var pos = 0
    let result = readUint8(buf, pos)
    check result.isOk
    check result.value == 255

  test "read uint16 BE":
    var buf = "\x12\x34"
    var pos = 0
    let result = readUint16BE(buf, pos)
    check result.isOk
    check result.value == 0x1234
    check pos == 2

  test "read uint16 BE zero":
    var buf = "\x00\x00"
    var pos = 0
    let result = readUint16BE(buf, pos)
    check result.isOk
    check result.value == 0

  test "read uint16 BE max":
    var buf = "\xFF\xFF"
    var pos = 0
    let result = readUint16BE(buf, pos)
    check result.isOk
    check result.value == 65535

  test "read uint32 BE":
    var buf = "\x12\x34\x56\x78"
    var pos = 0
    let result = readUint32BE(buf, pos)
    check result.isOk
    check result.value == 0x12345678'u32
    check pos == 4

  test "read uint32 BE zero":
    var buf = "\x00\x00\x00\x00"
    var pos = 0
    let result = readUint32BE(buf, pos)
    check result.isOk
    check result.value == 0

  test "read uint32 BE max":
    var buf = "\xFF\xFF\xFF\xFF"
    var pos = 0
    let result = readUint32BE(buf, pos)
    check result.isOk
    check result.value == 0xFFFFFFFF'u32

  test "read uint64 BE":
    var buf = "\x01\x02\x03\x04\x05\x06\x07\x08"
    var pos = 0
    let result = readUint64BE(buf, pos)
    check result.isOk
    check result.value == 0x0102030405060708'u64
    check pos == 8

  test "read uint64 BE zero":
    var buf = "\x00\x00\x00\x00\x00\x00\x00\x00"
    var pos = 0
    let result = readUint64BE(buf, pos)
    check result.isOk
    check result.value == 0'u64

  test "read uint64 BE max":
    var buf = "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"
    var pos = 0
    let result = readUint64BE(buf, pos)
    check result.isOk
    check result.value == 0xFFFFFFFFFFFFFFFF'u64

  test "read bytes":
    var buf = "\x00\x00\x00\x05Hello"
    var pos = 0
    let result = readBytes(buf, pos)
    check result.isOk
    check result.value == "Hello"
    check pos == 9

  test "read bytes empty":
    var buf = "\x00\x00\x00\x00"
    var pos = 0
    let result = readBytes(buf, pos)
    check result.isOk
    check result.value == ""
    check pos == 4

  test "read bytes8":
    var buf = "\x05Hello"
    var pos = 0
    let result = readBytes8(buf, pos)
    check result.isOk
    check result.value == "Hello"
    check pos == 6

  test "read bytes8 empty":
    var buf = "\x00"
    var pos = 0
    let result = readBytes8(buf, pos)
    check result.isOk
    check result.value == ""
    check pos == 1

  test "read bytes16":
    var buf = "\x00\x05Hello"
    var pos = 0
    let result = readBytes16(buf, pos)
    check result.isOk
    check result.value == "Hello"
    check pos == 7

  test "read bytes16 empty":
    var buf = "\x00\x00"
    var pos = 0
    let result = readBytes16(buf, pos)
    check result.isOk
    check result.value == ""
    check pos == 2

  test "read bytes32":
    var buf = "\x00\x00\x00\x05Hello"
    var pos = 0
    let result = readBytes32(buf, pos)
    check result.isOk
    check result.value == "Hello"
    check pos == 9

  test "read int32 BE":
    var buf = "\x12\x34\x56\x78"
    var pos = 0
    let result = readInt32BE(buf, pos)
    check result.isOk
    check result.value == 0x12345678
    check pos == 4

  test "read int32 BE negative":
    var buf = "\xFF\xFF\xFF\xFF"
    var pos = 0
    let result = readInt32BE(buf, pos)
    check result.isOk
    check result.value == -1

  test "read int32 BE min":
    var buf = "\x80\x00\x00\x00"
    var pos = 0
    let result = readInt32BE(buf, pos)
    check result.isOk
    check result.value == -2147483648

  test "read int64 BE":
    var buf = "\x01\x02\x03\x04\x05\x06\x07\x08"
    var pos = 0
    let result = readInt64BE(buf, pos)
    check result.isOk
    check result.value == 0x0102030405060708
    check pos == 8

  test "read int64 BE negative":
    var buf = "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"
    var pos = 0
    let result = readInt64BE(buf, pos)
    check result.isOk
    check result.value == -1

suite "Protocol Codec - Bounds Checking":
  test "read uint8 insufficient bytes":
    var buf = ""
    var pos = 0
    let result = readUint8(buf, pos)
    check result.isErr
    check result.error.kind == peBoundsOverflow

  test "read uint16 BE insufficient bytes - 1 byte":
    var buf = "\x12"
    var pos = 0
    let result = readUint16BE(buf, pos)
    check result.isErr
    check result.error.kind == peBoundsOverflow

  test "read uint32 BE insufficient bytes - 2 bytes":
    var buf = "\x12\x34"
    var pos = 0
    let result = readUint32BE(buf, pos)
    check result.isErr
    check result.error.kind == peBoundsOverflow

  test "read uint64 BE insufficient bytes - 4 bytes":
    var buf = "\x12\x34\x56\x78"
    var pos = 0
    let result = readUint64BE(buf, pos)
    check result.isErr
    check result.error.kind == peBoundsOverflow

  test "read bytes insufficient bytes for length":
    var buf = "\x00\x00"
    var pos = 0
    let result = readBytes(buf, pos)
    check result.isErr
    check result.error.kind == peBoundsOverflow

  test "read bytes insufficient bytes for data":
    var buf = "\x00\x00\x00\x05Hel"
    var pos = 0
    let result = readBytes(buf, pos)
    check result.isErr
    check result.error.kind == peBoundsOverflow

  test "read bytes8 insufficient bytes":
    var buf = "\x05Hel"
    var pos = 0
    let result = readBytes8(buf, pos)
    check result.isErr
    check result.error.kind == peBoundsOverflow

  test "check bounds success":
    let buf = "\x01\x02\x03\x04\x05"
    let result = checkBounds(buf, 0, 5)
    check result.isOk

  test "check bounds failure":
    let buf = "\x01\x02\x03"
    let result = checkBounds(buf, 0, 5)
    check result.isErr
    check result.error.kind == peBoundsOverflow

  test "check bounds at edge":
    let buf = "\x01\x02\x03\x04\x05"
    let result = checkBounds(buf, 3, 2)
    check result.isOk

  test "check bounds past end":
    let buf = "\x01\x02\x03\x04\x05"
    let result = checkBounds(buf, 3, 3)
    check result.isErr

suite "Protocol Codec - Round-trip Tests":
  test "uint8 round-trip":
    var buf = ""
    let original = 0x42'u8
    buf.writeUint8(original)
    var pos = 0
    let decoded = readUint8(buf, pos)
    check decoded.isOk
    check decoded.value == original

  test "uint16 BE round-trip":
    var buf = ""
    let original = 0x1234'u16
    buf.writeUint16BE(original)
    var pos = 0
    let decoded = readUint16BE(buf, pos)
    check decoded.isOk
    check decoded.value == original

  test "uint32 BE round-trip":
    var buf = ""
    let original = 0x12345678'u32
    buf.writeUint32BE(original)
    var pos = 0
    let decoded = readUint32BE(buf, pos)
    check decoded.isOk
    check decoded.value == original

  test "uint64 BE round-trip":
    var buf = ""
    let original = 0x0102030405060708'u64
    buf.writeUint64BE(original)
    var pos = 0
    let decoded = readUint64BE(buf, pos)
    check decoded.isOk
    check decoded.value == original

  test "bytes round-trip":
    var buf = ""
    let original = "Hello, World!"
    buf.writeBytes(original)
    var pos = 0
    let decoded = readBytes(buf, pos)
    check decoded.isOk
    check decoded.value == original

  test "bytes8 round-trip":
    var buf = ""
    let original = "Test"
    buf.writeBytes8(original)
    var pos = 0
    let decoded = readBytes8(buf, pos)
    check decoded.isOk
    check decoded.value == original

  test "bytes16 round-trip":
    var buf = ""
    let original = "Test data"
    buf.writeBytes16(original)
    var pos = 0
    let decoded = readBytes16(buf, pos)
    check decoded.isOk
    check decoded.value == original

  test "bytes32 round-trip":
    var buf = ""
    let original = "Test data for 32"
    buf.writeBytes32(original)
    var pos = 0
    let decoded = readBytes32(buf, pos)
    check decoded.isOk
    check decoded.value == original

  test "int32 BE round-trip positive":
    var buf = ""
    let original = int32(12345678)
    buf.writeInt32BE(original)
    var pos = 0
    let decoded = readInt32BE(buf, pos)
    check decoded.isOk
    check decoded.value == original

  test "int32 BE round-trip negative":
    var buf = ""
    let original = int32(-98765432)
    buf.writeInt32BE(original)
    var pos = 0
    let decoded = readInt32BE(buf, pos)
    check decoded.isOk
    check decoded.value == original

  test "int64 BE round-trip positive":
    var buf = ""
    let original = 9876543210'i64
    buf.writeInt64BE(original)
    var pos = 0
    let decoded = readInt64BE(buf, pos)
    check decoded.isOk
    check decoded.value == original

  test "int64 BE round-trip negative":
    var buf = ""
    let original = -9876543210'i64
    buf.writeInt64BE(original)
    var pos = 0
    let decoded = readInt64BE(buf, pos)
    check decoded.isOk
    check decoded.value == original

suite "Protocol Codec - Complex Sequences":
  test "write multiple values":
    var buf = ""
    buf.writeUint8(0x01)
    buf.writeUint16BE(0x0203)
    buf.writeUint32BE(0x04050607'u32)
    buf.writeUint64BE(0x08090A0B0C0D0E0F'u64)
    check buf.len == 15
    check buf == "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F"

  test "read multiple values":
    var buf = "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F"
    var pos = 0
    let u8 = readUint8(buf, pos)
    check u8.isOk and u8.value == 0x01
    let u16 = readUint16BE(buf, pos)
    check u16.isOk and u16.value == 0x0203
    let u32 = readUint32BE(buf, pos)
    check u32.isOk and u32.value == 0x04050607'u32
    let u64 = readUint64BE(buf, pos)
    check u64.isOk and u64.value == 0x08090A0B0C0D0E0F'u64
    check pos == 15

  test "mixed types sequence":
    var buf = ""
    buf.writeBytes("Hello")
    buf.writeUint32BE(42'u32)
    buf.writeInt32BE(-100)
    buf.writeBytes("World")
    check buf.len == 26

  test "read mixed types sequence":
    var buf = ""
    buf.writeBytes("Hello")
    buf.writeUint32BE(42'u32)
    buf.writeInt32BE(-100)
    buf.writeBytes("World")

    var pos = 0
    let s1 = readBytes(buf, pos)
    check s1.isOk and s1.value == "Hello"
    let u32 = readUint32BE(buf, pos)
    check u32.isOk and u32.value == 42
    let i32 = readInt32BE(buf, pos)
    check i32.isOk and i32.value == -100
    let s2 = readBytes(buf, pos)
    check s2.isOk and s2.value == "World"

suite "Protocol Codec - Edge Cases":
  test "empty buffer":
    var buf = ""
    var pos = 0
    let result = readUint8(buf, pos)
    check result.isErr

  test "read past buffer end":
    var buf = "\x01"
    var pos = 1
    let result = readUint8(buf, pos)
    check result.isErr

  test "large bytes value":
    var buf = ""
    let largeData = "x".repeat(10000)
    buf.writeBytes(largeData)
    check buf.len == 10004

    var pos = 0
    let decoded = readBytes(buf, pos)
    check decoded.isOk
    check decoded.value.len == 10000

  test "consecutive reads same buffer":
    var buf = "\x01\x02\x03\x04\x05\x06\x07\x08"
    var pos = 0
    for i in 0..<8:
      let result = readUint8(buf, pos)
      check result.isOk
      check result.value == uint8(i + 1)
    check pos == 8

  test "partial read then error":
    var buf = "\x01\x02\x03"
    var pos = 0
    let u8 = readUint8(buf, pos)
    check u8.isOk
    let u16 = readUint16BE(buf, pos)
    check u16.isOk
    let next = readUint8(buf, pos)
    check next.isErr

suite "Protocol Codec - Error Messages":
  test "bounds overflow error message":
    var buf = ""
    var pos = 0
    let result = readUint8(buf, pos)
    check result.isErr
    check "need" in result.error.msg
    check "bytes" in result.error.msg
    check "buffer" in result.error.msg

  test "bytes length overflow error":
    var buf = "\xFF\xFF\xFF\xFF" # Length = max uint32
    var pos = 0
    let result = readBytes(buf, pos)
    check result.isErr

suite "Protocol Codec - Result Types":
  test "PResult success":
    let result = pOk()
    check result.isOk

  test "PResult error":
    let err = newProtocolError(peBoundsOverflow, "Test error")
    let result = pErr(err)
    check result.isErr
    check result.error.kind == peBoundsOverflow

  test "ProtocolError construction":
    let err = newProtocolError(peInvalidFrame, "Invalid frame format")
    check err.kind == peInvalidFrame
    check err.msg == "Invalid frame format"
