# Unit tests for protocol/codec.nim
# Tests every write/read primitive, bounds checking, and round-trip integrity.

import std/unittest
import std/strutils
import fractio/protocol/types
import fractio/protocol/codec

suite "codec - write/read uint8":
  test "round-trip uint8":
    var buf = ""
    buf.writeUint8(0x00'u8)
    buf.writeUint8(0xAB'u8)
    buf.writeUint8(0xFF'u8)
    check buf.len == 3
    var pos = 0
    let r0 = readUint8(buf, pos)
    check r0.isOk
    check r0.value == 0x00'u8
    let r1 = readUint8(buf, pos)
    check r1.isOk
    check r1.value == 0xAB'u8
    let r2 = readUint8(buf, pos)
    check r2.isOk
    check r2.value == 0xFF'u8
    check pos == 3

  test "readUint8 bounds overflow":
    let buf = ""
    var pos = 0
    let r = readUint8(buf, pos)
    check r.isErr
    check r.err.kind == peBoundsOverflow

suite "codec - write/read uint16BE":
  test "round-trip uint16 zero":
    var buf = ""
    buf.writeUint16BE(0x0000'u16)
    var pos = 0
    let r = readUint16BE(buf, pos)
    check r.isOk
    check r.value == 0x0000'u16
    check pos == 2

  test "round-trip uint16 max":
    var buf = ""
    buf.writeUint16BE(0xFFFF'u16)
    var pos = 0
    let r = readUint16BE(buf, pos)
    check r.isOk
    check r.value == 0xFFFF'u16

  test "big-endian byte order uint16":
    var buf = ""
    buf.writeUint16BE(0x1234'u16)
    check buf[0] == '\x12'
    check buf[1] == '\x34'

  test "readUint16BE bounds overflow":
    let buf = "\x00" # only 1 byte, need 2
    var pos = 0
    let r = readUint16BE(buf, pos)
    check r.isErr
    check r.err.kind == peBoundsOverflow

suite "codec - write/read uint32BE":
  test "round-trip uint32 known value":
    var buf = ""
    buf.writeUint32BE(0xDEADBEEF'u32)
    check buf[0] == '\xDE'
    check buf[1] == '\xAD'
    check buf[2] == '\xBE'
    check buf[3] == '\xEF'
    var pos = 0
    let r = readUint32BE(buf, pos)
    check r.isOk
    check r.value == 0xDEADBEEF'u32
    check pos == 4

  test "round-trip uint32 zero":
    var buf = ""
    buf.writeUint32BE(0'u32)
    var pos = 0
    let r = readUint32BE(buf, pos)
    check r.isOk
    check r.value == 0'u32

  test "round-trip uint32 max":
    var buf = ""
    buf.writeUint32BE(0xFFFFFFFF'u32)
    var pos = 0
    let r = readUint32BE(buf, pos)
    check r.isOk
    check r.value == 0xFFFFFFFF'u32

  test "readUint32BE bounds overflow — empty":
    let buf = ""
    var pos = 0
    let r = readUint32BE(buf, pos)
    check r.isErr
    check r.err.kind == peBoundsOverflow

  test "readUint32BE bounds overflow — partial":
    var buf = ""
    buf.writeUint8(0xAA'u8)
    buf.writeUint8(0xBB'u8)
    var pos = 0
    let r = readUint32BE(buf, pos)
    check r.isErr
    check r.err.kind == peBoundsOverflow

suite "codec - write/read uint64BE":
  test "round-trip uint64 known value":
    var buf = ""
    buf.writeUint64BE(0x0102030405060708'u64)
    check buf[0] == '\x01'
    check buf[1] == '\x02'
    check buf[7] == '\x08'
    var pos = 0
    let r = readUint64BE(buf, pos)
    check r.isOk
    check r.value == 0x0102030405060708'u64
    check pos == 8

  test "round-trip uint64 zero":
    var buf = ""
    buf.writeUint64BE(0'u64)
    var pos = 0
    let r = readUint64BE(buf, pos)
    check r.isOk
    check r.value == 0'u64

  test "round-trip uint64 max":
    var buf = ""
    buf.writeUint64BE(0xFFFFFFFFFFFFFFFF'u64)
    var pos = 0
    let r = readUint64BE(buf, pos)
    check r.isOk
    check r.value == 0xFFFFFFFFFFFFFFFF'u64

  test "readUint64BE bounds overflow":
    let buf = "\x00\x01\x02" # only 3 bytes, need 8
    var pos = 0
    let r = readUint64BE(buf, pos)
    check r.isErr
    check r.err.kind == peBoundsOverflow

suite "codec - writeBytes / readBytes (uint32-prefixed)":
  test "round-trip empty string":
    var buf = ""
    buf.writeBytes("")
    check buf.len == 4 # just the length prefix
    var pos = 0
    let r = readBytes(buf, pos)
    check r.isOk
    check r.value == ""
    check pos == 4

  test "round-trip non-empty string":
    var buf = ""
    let data = "hello, world"
    buf.writeBytes(data)
    check buf.len == 4 + data.len
    var pos = 0
    let r = readBytes(buf, pos)
    check r.isOk
    check r.value == data
    check pos == 4 + data.len

  test "round-trip binary data with null bytes":
    var buf = ""
    let data = "\x00\xFF\x00\xAB\xCD"
    buf.writeBytes(data)
    var pos = 0
    let r = readBytes(buf, pos)
    check r.isOk
    check r.value == data

  test "readBytes truncated payload":
    var buf = ""
    buf.writeUint32BE(100'u32) # claims 100 bytes, but buffer is empty after header
    var pos = 0
    let r = readBytes(buf, pos)
    check r.isErr
    check r.err.kind == peBoundsOverflow

  test "readBytes truncated header":
    let buf = "\x00\x00" # incomplete uint32 length
    var pos = 0
    let r = readBytes(buf, pos)
    check r.isErr
    check r.err.kind == peBoundsOverflow

suite "codec - writeBytes8 / readBytes8 (uint8-prefixed)":
  test "round-trip empty":
    var buf = ""
    buf.writeBytes8("")
    check buf.len == 1
    var pos = 0
    let r = readBytes8(buf, pos)
    check r.isOk
    check r.value == ""

  test "round-trip max length (255 bytes)":
    var buf = ""
    let data = repeat('X', 255)
    buf.writeBytes8(data)
    check buf.len == 256
    var pos = 0
    let r = readBytes8(buf, pos)
    check r.isOk
    check r.value == data

  test "round-trip short string":
    var buf = ""
    buf.writeBytes8("fractio")
    var pos = 0
    let r = readBytes8(buf, pos)
    check r.isOk
    check r.value == "fractio"

  test "readBytes8 truncated payload":
    var buf = ""
    buf.writeUint8(10'u8) # claims 10 bytes, buffer empty
    var pos = 0
    let r = readBytes8(buf, pos)
    check r.isErr
    check r.err.kind == peBoundsOverflow

suite "codec - checkBounds":
  test "ok when exactly enough bytes":
    let buf = "1234"
    let r = checkBounds(buf, 0, 4)
    check r.isOk

  test "ok when plenty of bytes":
    let buf = "1234567890"
    let r = checkBounds(buf, 2, 5)
    check r.isOk

  test "err when buffer too short":
    let buf = "12"
    let r = checkBounds(buf, 0, 5)
    check r.isErr
    check r.err.kind == peBoundsOverflow

  test "err when pos is at end":
    let buf = "1234"
    let r = checkBounds(buf, 4, 1)
    check r.isErr
    check r.err.kind == peBoundsOverflow

suite "codec - sequential multi-value round-trip":
  test "write multiple values sequentially and read back":
    var buf = ""
    buf.writeUint8(0x01'u8)
    buf.writeUint16BE(0x0203'u16)
    buf.writeUint32BE(0x04050607'u32)
    buf.writeUint64BE(0x08090A0B0C0D0E0F'u64)
    buf.writeBytes("hello")
    buf.writeBytes8("nim")

    var pos = 0
    let r8 = readUint8(buf, pos); check r8.isOk; check r8.value == 0x01'u8
    let r16 = readUint16BE(buf, pos); check r16.isOk; check r16.value == 0x0203'u16
    let r32 = readUint32BE(buf, pos); check r32.isOk; check r32.value == 0x04050607'u32
    let r64 = readUint64BE(buf, pos); check r64.isOk; check r64.value == 0x08090A0B0C0D0E0F'u64
    let rB = readBytes(buf, pos); check rB.isOk; check rB.value == "hello"
    let rB8 = readBytes8(buf, pos); check rB8.isOk; check rB8.value == "nim"
    check pos == buf.len
