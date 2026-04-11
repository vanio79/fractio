# Unit tests for fractio/protocol/frame.nim
# Tests frame encoding/decoding, CRC16, error frames, and leader redirect

import unittest
import std/[strformat, strutils]
import fractio/protocol/frame
import fractio/protocol/types
import fractio/protocol/codec

suite "CRC16 Computation":
  test "computeCRC16 empty string":
    check computeCRC16("") == 0xFFFF'u16

  test "computeCRC16 single byte":
    check computeCRC16("\x00") == 0xE1F0'u16

  test "computeCRC16 known value":
    let data = "123456789"
    check computeCRC16(data) == 0x29B1'u16

  test "computeCRC16 consistency":
    let data = "test data"
    check computeCRC16(data) == computeCRC16(data)

  test "computeCRC16 different values":
    check computeCRC16("a") != computeCRC16("b")

suite "Frame Constants":
  test "FRAME_HEADER_SIZE":
    check FRAME_HEADER_SIZE == 12

  test "MAX_FRAME_SIZE":
    check MAX_FRAME_SIZE == 16 * 1024 * 1024

suite "FrameHeader Types":
  test "FrameHeader default":
    let hdr = FrameHeader()
    check hdr.payloadLen == 0
    check hdr.requestId == 0
    check hdr.flags == 0
    check hdr.checksum == 0

  test "FrameHeader construction":
    let hdr = FrameHeader(
      payloadLen: 100,
      requestId: 42,
      flags: FlagIsResponse,
      checksum: 0x1234
    )
    check hdr.payloadLen == 100
    check hdr.requestId == 42
    check hdr.flags == FlagIsResponse
    check hdr.checksum == 0x1234

suite "Frame Types":
  test "Frame default":
    let f = Frame()
    check f.header.payloadLen == 0
    check f.payload == ""

  test "Frame construction":
    let hdr = FrameHeader(payloadLen: 5, requestId: 1, flags: 0, checksum: 0)
    let f = Frame(header: hdr, payload: "hello")
    check f.payload == "hello"

suite "encodeFrame":
  test "encodeFrame empty payload":
    let encoded = encodeFrame("", 1, 0)
    check encoded.len == FRAME_HEADER_SIZE
    var pos = 0
    let len = readUint32BE(encoded, pos)
    check len.isOk
    check len.value == 0

  test "encodeFrame with payload":
    let payload = "test payload"
    let encoded = encodeFrame(payload, 42, FlagIsResponse)
    check encoded.len == FRAME_HEADER_SIZE + payload.len

    var pos = 0
    let len = readUint32BE(encoded, pos)
    check len.isOk
    check len.value == uint32(payload.len)

    let reqId = readUint32BE(encoded, pos)
    check reqId.isOk
    check reqId.value == 42

    let flags = readUint16BE(encoded, pos)
    check flags.isOk
    check flags.value == FlagIsResponse

  test "encodeFrame includes checksum":
    let payload = "data"
    let encoded = encodeFrame(payload, 0, 0)
    var pos = 10
    let crc = readUint16BE(encoded, pos)
    check crc.isOk
    check crc.value == computeCRC16(payload)

  test "encodeFrame preserves payload":
    let payload = "\x00\x01\x02\x03binary"
    let encoded = encodeFrame(payload, 0, 0)
    let payloadStart = FRAME_HEADER_SIZE
    check encoded[payloadStart ..< encoded.len] == payload

suite "decodeFrameHeader":
  test "decodeFrameHeader valid":
    let encoded = encodeFrame("test", 123, FlagCompressed)
    var pos = 0
    let hdr = decodeFrameHeader(encoded, pos)
    check hdr.isOk
    check hdr.value.payloadLen == 4
    check hdr.value.requestId == 123
    check hdr.value.flags == FlagCompressed
    check pos == FRAME_HEADER_SIZE

  test "decodeFrameHeader insufficient bytes":
    let buf = "\x00\x00\x00\x01"
    var pos = 0
    let hdr = decodeFrameHeader(buf, pos)
    check hdr.isErr

suite "decodeFrame":
  test "decodeFrame valid":
    let payload = "hello world"
    let encoded = encodeFrame(payload, 55, FlagIsResponse or FlagIsError)
    let decoded = decodeFrame(encoded)
    check decoded.isOk
    check decoded.value.header.payloadLen == uint32(payload.len)
    check decoded.value.header.requestId == 55
    check decoded.value.header.flags == (FlagIsResponse or FlagIsError)
    check decoded.value.payload == payload

  test "decodeFrame empty payload":
    let encoded = encodeFrame("", 0, 0)
    let decoded = decodeFrame(encoded)
    check decoded.isOk
    check decoded.value.payload == ""

  test "decodeFrame buffer too small":
    let buf = "\x00\x00\x00\x0C\x00\x00\x00\x01\x00\x00\x00\x00"
    let decoded = decodeFrame(buf)
    check decoded.isErr
    check decoded.error.kind == peBoundsOverflow

  test "decodeFrame payload exceeds max":
    var buf = ""
    buf.writeUint32BE(MAX_FRAME_SIZE.uint32 + 1)
    buf.writeUint32BE(0)
    buf.writeUint16BE(0)
    buf.writeUint16BE(0)

    let decoded = decodeFrame(buf)
    check decoded.isErr
    check decoded.error.kind == peFrameTooLarge

  test "decodeFrame checksum mismatch":
    var buf = ""
    buf.writeUint32BE(4)
    buf.writeUint32BE(1)
    buf.writeUint16BE(0)
    buf.writeUint16BE(0x0000)
    buf.add("test")

    let decoded = decodeFrame(buf)
    check decoded.isErr
    check decoded.error.kind == peChecksumMismatch

suite "Frame Roundtrip":
  test "simple roundtrip":
    let payload = "simple"
    let encoded = encodeFrame(payload, 1, 0)
    let decoded = decodeFrame(encoded)
    check decoded.isOk
    check decoded.value.payload == payload

  test "binary payload roundtrip":
    let payload = "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F"
    let encoded = encodeFrame(payload, 100, FlagCompressed)
    let decoded = decodeFrame(encoded)
    check decoded.isOk
    check decoded.value.payload == payload

  test "large payload roundtrip":
    let payload = "x".repeat(1000)
    let encoded = encodeFrame(payload, 0, 0)
    let decoded = decodeFrame(encoded)
    check decoded.isOk
    check decoded.value.payload.len == 1000

  test "flags preserved roundtrip":
    for flags in [0'u16, FlagCompressed, FlagIsResponse, FlagIsError,
                  FlagCompressed or FlagIsResponse, FlagEndOfStream]:
      let encoded = encodeFrame("data", 0, flags)
      let decoded = decodeFrame(encoded)
      check decoded.isOk
      check decoded.value.header.flags == flags

suite "encodeErrorPayload":
  test "encodeErrorPayload basic":
    let payload = encodeErrorPayload(ErrNotFound, ErrCatKV, "key not found")
    var pos = 0
    let code = readUint32BE(payload, pos)
    check code.isOk
    check code.value == ErrNotFound

    let cat = readUint8(payload, pos)
    check cat.isOk
    check cat.value == ErrCatKV

  test "encodeErrorPayload with details":
    let payload = encodeErrorPayload(ErrInternal, ErrCatSystem,
        "internal error", "stack trace")
    check payload.len > 0

  test "encodeErrorPayload empty message":
    let payload = encodeErrorPayload(ErrOK, ErrCatProtocol, "")
    check payload.len > 0

suite "encodeErrorFrame":
  test "encodeErrorFrame basic":
    let frame = encodeErrorFrame(42, ErrNotFound, ErrCatKV, "key missing")
    check frame.len > FRAME_HEADER_SIZE

    let decoded = decodeFrame(frame)
    check decoded.isOk
    check decoded.value.header.requestId == 42
    check (decoded.value.header.flags and FlagIsError) != 0
    check (decoded.value.header.flags and FlagIsResponse) != 0

  test "encodeErrorFrame payload structure":
    let errCode = ErrNotFound
    let category = ErrCatKV
    let msg = "key missing"
    let frame = encodeErrorFrame(1, errCode, category, msg)
    let decoded = decodeFrame(frame)
    check decoded.isOk

    let payload = decoded.value.payload
    check payload.len >= 2 + 4 + 1 + 2

    var pos = 0
    let mt = readUint16BE(payload, pos)
    check mt.isOk
    check mt.value == 0x0000'u16

    let code = readUint32BE(payload, pos)
    check code.isOk
    check code.value == errCode

    let cat = readUint8(payload, pos)
    check cat.isOk
    check cat.value == category

    let msgLen = readUint16BE(payload, pos)
    check msgLen.isOk
    check msgLen.value == uint16(msg.len)

suite "encodeNotLeaderErrorFrame":
  test "encodeNotLeaderErrorFrame basic":
    let redirect = LeaderRedirect(
      leaderId: 5,
      leaderHost: "192.168.1.5",
      leaderClientPort: 8080
    )
    let frame = encodeNotLeaderErrorFrame(100, "not leader", redirect)

    let decoded = decodeFrame(frame)
    check decoded.isOk
    check decoded.value.header.requestId == 100
    check (decoded.value.header.flags and FlagIsError) != 0

  test "encodeNotLeaderErrorFrame includes redirect":
    let redirect = LeaderRedirect(
      leaderId: 10,
      leaderHost: "leader.host.com",
      leaderClientPort: 9000
    )
    let frame = encodeNotLeaderErrorFrame(1, "redirect", redirect)
    let decoded = decodeFrame(frame)
    check decoded.isOk

    var details = ""
    details.writeUint32BE(redirect.leaderId)
    details.writeBytes16(redirect.leaderHost)
    details.writeUint16BE(redirect.leaderClientPort)

    let decodedRedirect = decodeLeaderRedirect(details)
    check decodedRedirect.leaderId == 10
    check decodedRedirect.leaderHost == "leader.host.com"
    check decodedRedirect.leaderClientPort == 9000

suite "decodeLeaderRedirect":
  test "decodeLeaderRedirect valid":
    var details = ""
    details.writeUint32BE(42)
    details.writeBytes16("myhost")
    details.writeUint16BE(7000)

    let redirect = decodeLeaderRedirect(details)
    check redirect.leaderId == 42
    check redirect.leaderHost == "myhost"
    check redirect.leaderClientPort == 7000

  test "decodeLeaderRedirect empty":
    let redirect = decodeLeaderRedirect("")
    check redirect.leaderId == 0

  test "decodeLeaderRedirect truncated":
    let redirect = decodeLeaderRedirect("\x00\x01")
    check redirect.leaderId == 0

  test "decodeLeaderRedirect malformed host":
    var details = ""
    details.writeUint32BE(5)
    details.writeUint16BE(100)
    let redirect = decodeLeaderRedirect(details)
    check redirect.leaderId == 0

suite "LeaderRedirect Roundtrip":
  test "full redirect roundtrip":
    let original = LeaderRedirect(
      leaderId: 123,
      leaderHost: "cluster-node-1.internal",
      leaderClientPort: 9200
    )

    var details = ""
    details.writeUint32BE(original.leaderId)
    details.writeBytes16(original.leaderHost)
    details.writeUint16BE(original.leaderClientPort)

    let decoded = decodeLeaderRedirect(details)
    check decoded.leaderId == original.leaderId
    check decoded.leaderHost == original.leaderHost
    check decoded.leaderClientPort == original.leaderClientPort

suite "Error Frame Categories":
  test "protocol error frame payload structure":
    let frame = encodeErrorFrame(1, ErrProtocol, ErrCatProtocol, "bad frame")
    let decoded = decodeFrame(frame)
    check decoded.isOk

    let payload = decoded.value.payload
    var pos = 6
    let category = readUint8(payload, pos)
    check category.isOk
    check category.value == ErrCatProtocol

  test "transaction error frame payload structure":
    let frame = encodeErrorFrame(1, ErrTxnConflict, ErrCatTransaction, "conflict")
    let decoded = decodeFrame(frame)
    check decoded.isOk

    let payload = decoded.value.payload
    var pos = 6
    let category = readUint8(payload, pos)
    check category.isOk
    check category.value == ErrCatTransaction

suite "Edge Cases":
  test "frame with zero requestId":
    let encoded = encodeFrame("data", 0, 0)
    let decoded = decodeFrame(encoded)
    check decoded.isOk
    check decoded.value.header.requestId == 0

  test "frame with max requestId":
    let encoded = encodeFrame("data", 0xFFFFFFFF'u32, 0)
    let decoded = decodeFrame(encoded)
    check decoded.isOk
    check decoded.value.header.requestId == 0xFFFFFFFF'u32

  test "frame with all flags":
    let allFlags = FlagCompressed or FlagRequiresAck or FlagIsResponse or
                   FlagIsError or FlagEndOfStream
    let encoded = encodeFrame("x", 1, allFlags)
    let decoded = decodeFrame(encoded)
    check decoded.isOk
    check decoded.value.header.flags == allFlags

suite "Message Type in Payload":
  test "encodeErrorFrame message type prefix is zero":
    let frame = encodeErrorFrame(1, ErrNotFound, ErrCatKV, "error")
    let decoded = decodeFrame(frame)
    check decoded.isOk

    let payload = decoded.value.payload
    var pos = 0
    let mt = readUint16BE(payload, pos)
    check mt.isOk
    check mt.value == 0x0000'u16
