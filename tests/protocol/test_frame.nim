# Unit tests for protocol/frame.nim
# Tests CRC-16, frame encoding, header decoding, full frame decoding,
# error payload encoding, and all error paths.

import std/unittest
import fractio/protocol/types
import fractio/protocol/codec
import fractio/protocol/frame

# ---------------------------------------------------------------------------
# Helper: build a raw frame string manually for reference comparisons
# ---------------------------------------------------------------------------

proc buildRawFrame(payload: string, requestId: uint32, flags: uint16): string =
  var buf = ""
  buf.writeUint32BE(uint32(payload.len))
  buf.writeUint32BE(requestId)
  buf.writeUint16BE(flags)
  buf.writeUint16BE(computeCRC16(payload))
  buf.add(payload)
  buf

# ---------------------------------------------------------------------------
# CRC-16/CCITT-FALSE
# ---------------------------------------------------------------------------

suite "frame - CRC16":
  test "empty string has known CRC":
    # CRC-16/CCITT-FALSE of empty input = 0xFFFF (initial value, no data)
    let crc = computeCRC16("")
    check crc == 0xFFFF'u16

  test "single byte 0x00":
    # Known reference value for CRC-16/CCITT-FALSE over [0x00]
    let crc = computeCRC16("\x00")
    check crc != 0xFFFF'u16 # must differ from empty

  test "same data always yields same CRC":
    let data = "hello, fractio protocol"
    check computeCRC16(data) == computeCRC16(data)

  test "different data yields different CRC (high confidence)":
    let crc1 = computeCRC16("aaaa")
    let crc2 = computeCRC16("aaab")
    check crc1 != crc2

  test "known vector: '123456789' = 0x29B1":
    # CRC-16/CCITT-FALSE standard test vector
    let crc = computeCRC16("123456789")
    check crc == 0x29B1'u16

  test "binary data round-trip checksum integrity":
    var data = newString(256)
    for i in 0 ..< 256:
      data[i] = char(i)
    let crc1 = computeCRC16(data)
    let crc2 = computeCRC16(data)
    check crc1 == crc2

# ---------------------------------------------------------------------------
# encodeFrame
# ---------------------------------------------------------------------------

suite "frame - encodeFrame":
  test "empty payload produces 12-byte frame":
    let f = encodeFrame("", 1'u32)
    check f.len == FRAME_HEADER_SIZE

  test "header fields are correct for empty payload":
    let f = encodeFrame("", 42'u32, FlagIsResponse)
    var pos = 0
    let lenR = readUint32BE(f, pos); check lenR.isOk; check lenR.value == 0'u32
    let idR = readUint32BE(f, pos); check idR.isOk; check idR.value == 42'u32
    let flR = readUint16BE(f, pos); check flR.isOk; check flR.value == FlagIsResponse
    let crcR = readUint16BE(f, pos); check crcR.isOk; check crcR.value ==
        computeCRC16("")

  test "payload is appended verbatim after header":
    let payload = "test-payload"
    let f = encodeFrame(payload, 1'u32)
    check f.len == FRAME_HEADER_SIZE + payload.len
    check f[FRAME_HEADER_SIZE ..< f.len] == payload

  test "checksum in header matches computeCRC16 of payload":
    let payload = "some data bytes"
    let f = encodeFrame(payload, 7'u32)
    var pos = 8 # skip payloadLen + requestId
    let flR = readUint16BE(f, pos); check flR.isOk
    let crcR = readUint16BE(f, pos); check crcR.isOk
    check crcR.value == computeCRC16(payload)

  test "matches manually built raw frame":
    let payload = "ping"
    let requestId = 100'u32
    let flags = FlagIsResponse
    let encoded = encodeFrame(payload, requestId, flags)
    let expected = buildRawFrame(payload, requestId, flags)
    check encoded == expected

  test "non-zero flags are preserved":
    let flags = FlagCompressed or FlagRequiresAck or FlagIsResponse or
        FlagIsError or FlagEndOfStream
    let f = encodeFrame("x", 1'u32, flags)
    var pos = 8
    let flR = readUint16BE(f, pos); check flR.isOk
    check flR.value == flags

# ---------------------------------------------------------------------------
# decodeFrameHeader
# ---------------------------------------------------------------------------

suite "frame - decodeFrameHeader":
  test "decode header of encoded frame":
    let payload = "hello"
    let f = encodeFrame(payload, 5'u32, FlagIsResponse)
    var pos = 0
    let r = decodeFrameHeader(f, pos)
    check r.isOk
    let hdr = r.value
    check hdr.payloadLen == uint32(payload.len)
    check hdr.requestId == 5'u32
    check hdr.flags == FlagIsResponse
    check hdr.checksum == computeCRC16(payload)
    check pos == FRAME_HEADER_SIZE

  test "decode advances pos by exactly FRAME_HEADER_SIZE":
    let f = encodeFrame("abc", 1'u32)
    var pos = 0
    let r = decodeFrameHeader(f, pos)
    check r.isOk
    check pos == FRAME_HEADER_SIZE

  test "error on buffer shorter than header":
    let buf = "\x00\x00\x00" # 3 bytes, need 12
    var pos = 0
    let r = decodeFrameHeader(buf, pos)
    check r.isErr
    check r.err.kind == peBoundsOverflow

  test "decode from non-zero pos":
    var buf = "GARBAGE" # 7 bytes of garbage prefix
    buf.add(encodeFrame("payload", 9'u32))
    var pos = 7
    let r = decodeFrameHeader(buf, pos)
    check r.isOk
    check r.value.requestId == 9'u32
    check pos == 7 + FRAME_HEADER_SIZE

# ---------------------------------------------------------------------------
# decodeFrame
# ---------------------------------------------------------------------------

suite "frame - decodeFrame":
  test "round-trip encode then decode":
    let payload = "hello, world!"
    let f = encodeFrame(payload, 42'u32, FlagIsResponse)
    let r = decodeFrame(f)
    check r.isOk
    let frame = r.value
    check frame.header.payloadLen == uint32(payload.len)
    check frame.header.requestId == 42'u32
    check frame.header.flags == FlagIsResponse
    check frame.payload == payload

  test "round-trip empty payload":
    let f = encodeFrame("", 1'u32)
    let r = decodeFrame(f)
    check r.isOk
    check r.value.payload == ""
    check r.value.header.payloadLen == 0'u32

  test "round-trip binary payload":
    var payload = newString(256)
    for i in 0 ..< 256:
      payload[i] = char(i)
    let f = encodeFrame(payload, 3'u32)
    let r = decodeFrame(f)
    check r.isOk
    check r.value.payload == payload

  test "error on buffer too small (< FRAME_HEADER_SIZE)":
    let r = decodeFrame("\x00\x00\x00")
    check r.isErr
    check r.err.kind == peInvalidFrame

  test "error on CRC mismatch — tampered payload":
    var f = encodeFrame("original", 1'u32)
    # Flip a byte in the payload region (after header)
    f[FRAME_HEADER_SIZE] = char(ord(f[FRAME_HEADER_SIZE]) xor 0xFF)
    let r = decodeFrame(f)
    check r.isErr
    check r.err.kind == peChecksumMismatch

  test "error on CRC mismatch — tampered checksum field":
    var f = encodeFrame("data", 1'u32)
    # Flip checksum bytes (bytes 10 and 11)
    f[10] = char(ord(f[10]) xor 0xFF)
    let r = decodeFrame(f)
    check r.isErr
    check r.err.kind == peChecksumMismatch

  test "error on truncated payload":
    let payload = "hello, world"
    var f = encodeFrame(payload, 1'u32)
    # Remove last 4 bytes to truncate payload
    f = f[0 ..< f.len - 4]
    let r = decodeFrame(f)
    check r.isErr
    # either bounds overflow or checksum mismatch depending on truncation
    check r.err.kind in {peBoundsOverflow, peChecksumMismatch}

  test "error on frame too large":
    # Build a header that claims MAX_FRAME_SIZE + 1 bytes
    var f = ""
    f.writeUint32BE(uint32(MAX_FRAME_SIZE) + 1'u32)
    f.writeUint32BE(1'u32) # requestId
    f.writeUint16BE(0'u16) # flags
    f.writeUint16BE(0'u16) # checksum
    let r = decodeFrame(f)
    check r.isErr
    check r.err.kind == peFrameTooLarge

# ---------------------------------------------------------------------------
# encodeErrorPayload / encodeErrorFrame
# ---------------------------------------------------------------------------

suite "frame - error encoding":
  test "encodeErrorPayload has correct structure":
    let p = encodeErrorPayload(ErrNotFound, ErrCatKV, "key not found", "detail")
    var pos = 0
    let codeR = readUint32BE(p, pos); check codeR.isOk; check codeR.value == ErrNotFound
    let catR = readUint8(p, pos); check catR.isOk; check catR.value == ErrCatKV
    let mlenR = readUint16BE(p, pos); check mlenR.isOk
    let mlen = int(mlenR.value)
    check p[pos ..< pos + mlen] == "key not found"
    pos += mlen
    let dlenR = readUint16BE(p, pos); check dlenR.isOk
    let dlen = int(dlenR.value)
    check p[pos ..< pos + dlen] == "detail"

  test "encodeErrorPayload empty strings":
    let p = encodeErrorPayload(ErrProtocol, ErrCatProtocol, "", "")
    check p.len == 4 + 1 + 2 + 0 + 2 + 0 # code + cat + msgLen + msg + detLen + det

  test "encodeErrorFrame produces decodable frame with FlagIsError set":
    let f = encodeErrorFrame(99'u32, ErrAuthFailed, ErrCatAuth, "bad credentials")
    let r = decodeFrame(f)
    check r.isOk
    let frm = r.value
    check frm.header.requestId == 99'u32
    check (frm.header.flags and FlagIsError) != 0
    check (frm.header.flags and FlagIsResponse) != 0

  test "encodeErrorFrame — message type prefix is 0x0000":
    let f = encodeErrorFrame(1'u32, ErrProtocol, ErrCatProtocol, "test")
    let r = decodeFrame(f)
    check r.isOk
    var pos = 0
    let mtR = readUint16BE(r.value.payload, pos)
    check mtR.isOk
    check mtR.value == 0x0000'u16

suite "frame - constants":
  test "FRAME_HEADER_SIZE is 12":
    check FRAME_HEADER_SIZE == 12

  test "MAX_FRAME_SIZE is 16 MB":
    check MAX_FRAME_SIZE == 16 * 1024 * 1024
