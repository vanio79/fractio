# Unit tests for fractio/protocol/messages/core.nim
# Tests Ping, Echo, Close, CancelStream encoding/decoding

import std/[unittest, strutils]
import fractio/protocol/messages/core
import fractio/protocol/types
import fractio/protocol/codec

suite "Ping Messages":

  test "encodePingRequest":
    let req = encodePingRequest()
    check req.len == 2 # Just message type
    var pos = 0
    let mt = readUint16BE(req, pos)
    check mt.isOk
    check mt.value == uint16(mtPing)

  test "encodePingResponse":
    let timestamp = 123456789'u64
    let resp = encodePingResponse(timestamp)
    check resp.len == 10 # 2 byte type + 8 byte timestamp

    var pos = 0
    let mt = readUint16BE(resp, pos)
    check mt.isOk
    check mt.value == uint16(mtPing)

    let ts = readUint64BE(resp, pos)
    check ts.isOk
    check ts.value == timestamp

  test "decodePingResponse":
    let timestamp = 987654321'u64
    let resp = encodePingResponse(timestamp)
    let decoded = decodePingResponse(resp)
    check decoded.isOk
    check decoded.value == timestamp

  test "decodePingResponse invalid payload":
    let invalid = "\x00\x01" # Just message type, no timestamp
    let decoded = decodePingResponse(invalid)
    check decoded.isErr

suite "Echo Messages":

  test "encodeEchoRequest empty":
    let req = encodeEchoRequest("")
    check req.len == 6 # 2 byte type + 4 byte length prefix
    var pos = 0
    let mt = readUint16BE(req, pos)
    check mt.isOk
    check mt.value == uint16(mtEcho)
    let len = readUint32BE(req, pos)
    check len.isOk
    check len.value == 0

  test "encodeEchoRequest with data":
    let data = "hello world"
    let req = encodeEchoRequest(data)
    check req.len == 2 + 4 + data.len
    var pos = 0
    let mt = readUint16BE(req, pos)
    check mt.value == uint16(mtEcho)
    let decodedData = readBytes(req, pos)
    check decodedData.isOk
    check decodedData.value == data

  test "encodeEchoResponse":
    let data = "echoed"
    let resp = encodeEchoResponse(data)
    check resp.len == 2 + 4 + data.len
    var pos = 0
    let mt = readUint16BE(resp, pos)
    check mt.value == uint16(mtEcho)
    let decoded = readBytes(resp, pos)
    check decoded.value == data

  test "decodeEchoData":
    let data = "test echo data"
    let req = encodeEchoRequest(data)
    let decoded = decodeEchoData(req)
    check decoded.isOk
    check decoded.value == data

  test "decodeEchoData binary":
    let data = "\x00\x01\x02\x03"
    let req = encodeEchoRequest(data)
    let decoded = decodeEchoData(req)
    check decoded.isOk
    check decoded.value == data

  test "decodeEchoData empty":
    let req = encodeEchoRequest("")
    let decoded = decodeEchoData(req)
    check decoded.isOk
    check decoded.value == ""

suite "Close Messages":

  test "encodeCloseRequest no reason":
    let req = encodeCloseRequest()
    check req.len == 3 # 2 byte type + 1 byte length prefix
    var pos = 0
    let mt = readUint16BE(req, pos)
    check mt.value == uint16(mtClose)
    let reason = readBytes8(req, pos)
    check reason.isOk
    check reason.value == ""

  test "encodeCloseRequest with reason":
    let reason = "server shutdown"
    let req = encodeCloseRequest(reason)
    check req.len == 2 + 1 + reason.len
    var pos = 0
    let mt = readUint16BE(req, pos)
    check mt.value == uint16(mtClose)
    let decoded = readBytes8(req, pos)
    check decoded.value == reason

  test "encodeCloseRequest max reason length":
    # uint8 max length is 255
    let reason = "x".repeat(255)
    let req = encodeCloseRequest(reason)
    var pos = 0
    discard readUint16BE(req, pos)
    let len = readUint8(req, pos)
    check len.value == 255

  test "decodeCloseReason empty":
    let req = encodeCloseRequest()
    let decoded = decodeCloseReason(req)
    check decoded.isOk
    check decoded.value == ""

  test "decodeCloseReason with data":
    let reason = "maintenance"
    let req = encodeCloseRequest(reason)
    let decoded = decodeCloseReason(req)
    check decoded.isOk
    check decoded.value == reason

suite "CancelStream Messages":

  test "encodeCancelStreamRequest":
    let requestId = 42'u32
    let req = encodeCancelStreamRequest(requestId)
    check req.len == 6 # 2 byte type + 4 byte request ID
    var pos = 0
    let mt = readUint16BE(req, pos)
    check mt.value == uint16(mtCancelStream)
    let id = readUint32BE(req, pos)
    check id.isOk
    check id.value == requestId

  test "decodeCancelStreamRequest":
    let requestId = 123'u32
    let req = encodeCancelStreamRequest(requestId)
    let decoded = decodeCancelStreamRequest(req)
    check decoded.isOk
    check decoded.value == requestId

  test "encodeCancelStreamResponse cancelled":
    let resp = encodeCancelStreamResponse(false)
    check resp.len == 3 # 2 byte type + 1 byte status
    var pos = 0
    let mt = readUint16BE(resp, pos)
    check mt.value == uint16(mtCancelStream)
    let status = readUint8(resp, pos)
    check status.value == 0x00

  test "encodeCancelStreamResponse already complete":
    let resp = encodeCancelStreamResponse(true)
    check resp.len == 3
    var pos = 0
    let mt = readUint16BE(resp, pos)
    check mt.value == uint16(mtCancelStream)
    let status = readUint8(resp, pos)
    check status.value == 0x01

  test "decodeCancelStreamResponse cancelled":
    let resp = encodeCancelStreamResponse(false)
    let decoded = decodeCancelStreamResponse(resp)
    check decoded.isOk
    check decoded.value == false

  test "decodeCancelStreamResponse already complete":
    let resp = encodeCancelStreamResponse(true)
    let decoded = decodeCancelStreamResponse(resp)
    check decoded.isOk
    check decoded.value == true

suite "Roundtrip Tests":

  test "Ping full roundtrip":
    let ts = 123456'u64
    let encoded = encodePingResponse(ts)
    let decoded = decodePingResponse(encoded)
    check decoded.value == ts

  test "Echo full roundtrip":
    for data in ["", "a", "hello", "binary\x00data"]:
      let encoded = encodeEchoRequest(data)
      let decoded = decodeEchoData(encoded)
      check decoded.value == data

  test "Close full roundtrip":
    for reason in ["", "shutdown", "error"]:
      let encoded = encodeCloseRequest(reason)
      let decoded = decodeCloseReason(encoded)
      check decoded.value == reason

  test "CancelStream request roundtrip":
    for requestId in [0'u32, 1'u32, 100'u32, 0xFFFFFFFF'u32]:
      let encoded = encodeCancelStreamRequest(requestId)
      let decoded = decodeCancelStreamRequest(encoded)
      check decoded.value == requestId

  test "CancelStream response roundtrip":
    for alreadyComplete in [false, true]:
      let encoded = encodeCancelStreamResponse(alreadyComplete)
      let decoded = decodeCancelStreamResponse(encoded)
      check decoded.value == alreadyComplete
