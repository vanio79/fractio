# Unit tests for fractio/protocol/handshake.nim
# Tests connection handshake encoding/decoding roundtrips

import unittest
import std/strutils
import fractio/protocol/handshake
import fractio/protocol/types
import fractio/protocol/codec

suite "ServerGreeting Encoding":
  test "encodeGreeting basic":
    let g = ServerGreeting(
      magic: PROTOCOL_MAGIC,
      version: PROTOCOL_VERSION_1,
      features: 0x00000001'u32,
      authMethods: @[uint8(amNone), uint8(amPassword)],
      serverId: 1,
      clusterId: 12345'u64
    )
    let encoded = encodeGreeting(g)
    check encoded.len >= 4

  test "encodeGreeting magic bytes":
    let g = ServerGreeting(
      magic: PROTOCOL_MAGIC,
      version: PROTOCOL_VERSION_1,
      features: 0'u32,
      authMethods: @[],
      serverId: 0,
      clusterId: 0'u64
    )
    let encoded = encodeGreeting(g)
    check encoded[0..3] == "FRC1"

  test "encodeGreeting empty auth methods":
    let g = ServerGreeting(
      magic: PROTOCOL_MAGIC,
      version: PROTOCOL_VERSION_1,
      features: 0'u32,
      authMethods: @[],
      serverId: 0,
      clusterId: 0'u64
    )
    let encoded = encodeGreeting(g)
    var pos = 4
    discard readUint16BE(encoded, pos)
    discard readUint32BE(encoded, pos)
    let count = readUint8(encoded, pos)
    check count.isOk
    check count.value == 0

  test "encodeGreeting multiple auth methods":
    let g = ServerGreeting(
      magic: PROTOCOL_MAGIC,
      version: PROTOCOL_VERSION_1,
      features: 0'u32,
      authMethods: @[0x00, 0x01, 0x02, 0x03],
      serverId: 0,
      clusterId: 0'u64
    )
    let encoded = encodeGreeting(g)
    var pos = 4
    discard readUint16BE(encoded, pos)
    discard readUint32BE(encoded, pos)
    let count = readUint8(encoded, pos)
    check count.isOk
    check count.value == 4

suite "ServerGreeting Decoding":
  test "decodeGreeting valid":
    let g = ServerGreeting(
      magic: PROTOCOL_MAGIC,
      version: PROTOCOL_VERSION_1,
      features: FeatTLS or FeatCompression,
      authMethods: @[uint8(amNone), uint8(amToken)],
      serverId: 42,
      clusterId: 99999'u64
    )
    let encoded = encodeGreeting(g)
    let decoded = decodeGreeting(encoded)
    check decoded.isOk
    check decoded.value.magic == PROTOCOL_MAGIC
    check decoded.value.version == PROTOCOL_VERSION_1
    check decoded.value.features == (FeatTLS or FeatCompression)
    check decoded.value.authMethods.len == 2
    check decoded.value.authMethods[0] == uint8(amNone)
    check decoded.value.authMethods[1] == uint8(amToken)
    check decoded.value.serverId == 42
    check decoded.value.clusterId == 99999'u64

  test "decodeGreeting invalid magic":
    var buf = ""
    buf.add("BAD1")
    buf.writeUint16BE(1)
    buf.writeUint32BE(0)
    buf.writeUint8(0)
    buf.writeUint16BE(0)
    buf.writeUint64BE(0)

    let decoded = decodeGreeting(buf)
    check decoded.isErr
    check decoded.error.kind == peInvalidFrame
    check "invalid magic" in decoded.error.msg

  test "decodeGreeting truncated":
    let buf = "FRC"
    let decoded = decodeGreeting(buf)
    check decoded.isErr

  test "decodeGreeting missing auth method bytes":
    var buf = ""
    buf.add("FRC1")
    buf.writeUint16BE(1)
    buf.writeUint32BE(0)
    buf.writeUint8(3)
    let decoded = decodeGreeting(buf)
    check decoded.isErr

suite "ServerGreeting Roundtrip":
  test "minimal roundtrip":
    let g = ServerGreeting(
      magic: PROTOCOL_MAGIC,
      version: PROTOCOL_VERSION_1,
      features: 0'u32,
      authMethods: @[],
      serverId: 0,
      clusterId: 0'u64
    )
    let encoded = encodeGreeting(g)
    let decoded = decodeGreeting(encoded)
    check decoded.isOk
    check decoded.value.magic == g.magic
    check decoded.value.version == g.version
    check decoded.value.features == g.features
    check decoded.value.authMethods == g.authMethods
    check decoded.value.serverId == g.serverId
    check decoded.value.clusterId == g.clusterId

  test "full roundtrip":
    let g = ServerGreeting(
      magic: PROTOCOL_MAGIC,
      version: PROTOCOL_VERSION_1,
      features: 0xFFFFFFFF'u32,
      authMethods: @[0x00, 0x01, 0x02, 0x03],
      serverId: 65535,
      clusterId: 0xFFFFFFFFFFFFFFFF'u64
    )
    let encoded = encodeGreeting(g)
    let decoded = decodeGreeting(encoded)
    check decoded.isOk
    check decoded.value == g

suite "ClientHandshake Encoding":
  test "encodeClientHandshake basic":
    let h = ClientHandshake(
      version: PROTOCOL_VERSION_1,
      features: FeatTLS,
      authType: uint8(amNone),
      authData: "",
      clientId: "test-client"
    )
    let encoded = encodeClientHandshake(h)
    check encoded.len > 0

  test "encodeClientHandshake with auth data":
    let h = ClientHandshake(
      version: PROTOCOL_VERSION_1,
      features: 0'u32,
      authType: uint8(amPassword),
      authData: "user:pass",
      clientId: ""
    )
    let encoded = encodeClientHandshake(h)
    var pos = 0
    discard readUint16BE(encoded, pos)
    discard readUint32BE(encoded, pos)
    discard readUint8(encoded, pos)
    let data = readBytes(encoded, pos)
    check data.isOk
    check data.value == "user:pass"

  test "encodeClientHandshake empty fields":
    let h = ClientHandshake(
      version: PROTOCOL_VERSION_1,
      features: 0'u32,
      authType: 0,
      authData: "",
      clientId: ""
    )
    let encoded = encodeClientHandshake(h)
    check encoded.len > 0

suite "ClientHandshake Decoding":
  test "decodeClientHandshake valid":
    let h = ClientHandshake(
      version: PROTOCOL_VERSION_1,
      features: FeatTransactions or FeatSQL,
      authType: uint8(amToken),
      authData: "mytoken123",
      clientId: "client-abc"
    )
    let encoded = encodeClientHandshake(h)
    let decoded = decodeClientHandshake(encoded)
    check decoded.isOk
    check decoded.value.version == PROTOCOL_VERSION_1
    check decoded.value.features == (FeatTransactions or FeatSQL)
    check decoded.value.authType == uint8(amToken)
    check decoded.value.authData == "mytoken123"
    check decoded.value.clientId == "client-abc"

  test "decodeClientHandshake truncated":
    var buf = ""
    buf.writeUint16BE(1)
    let decoded = decodeClientHandshake(buf)
    check decoded.isErr

suite "ClientHandshake Roundtrip":
  test "empty roundtrip":
    let h = ClientHandshake(
      version: PROTOCOL_VERSION_1,
      features: 0'u32,
      authType: 0,
      authData: "",
      clientId: ""
    )
    let encoded = encodeClientHandshake(h)
    let decoded = decodeClientHandshake(encoded)
    check decoded.isOk
    check decoded.value.version == h.version
    check decoded.value.features == h.features
    check decoded.value.authType == h.authType
    check decoded.value.authData == h.authData
    check decoded.value.clientId == h.clientId

  test "full roundtrip":
    let h = ClientHandshake(
      version: PROTOCOL_VERSION_1,
      features: 0xFFFFFFFF'u32,
      authType: 0xFF,
      authData: "long-auth-data-string",
      clientId: "long-client-id-string"
    )
    let encoded = encodeClientHandshake(h)
    let decoded = decodeClientHandshake(encoded)
    check decoded.isOk
    check decoded.value.version == h.version
    check decoded.value.features == h.features
    check decoded.value.authType == h.authType
    check decoded.value.authData == h.authData
    check decoded.value.clientId == h.clientId

suite "HandshakeResponse Encoding":
  test "encodeHandshakeResponse success":
    let r = HandshakeResponse(
      status: HandshakeOK,
      features: FeatCompression,
      serverName: "fractio-server",
      errorMessage: ""
    )
    let encoded = encodeHandshakeResponse(r)
    check encoded.len > 0

  test "encodeHandshakeResponse error":
    let r = HandshakeResponse(
      status: HandshakeError,
      features: 0'u32,
      serverName: "fractio",
      errorMessage: "version mismatch"
    )
    let encoded = encodeHandshakeResponse(r)
    var pos = 0
    let status = readUint8(encoded, pos)
    check status.isOk
    check status.value == HandshakeError

  test "encodeHandshakeResponse no error message on OK":
    let r = HandshakeResponse(
      status: HandshakeOK,
      features: 0'u32,
      serverName: "",
      errorMessage: ""
    )
    let encoded = encodeHandshakeResponse(r)
    check "error" notin encoded.toLowerAscii()

suite "HandshakeResponse Decoding":
  test "decodeHandshakeResponse success":
    let r = HandshakeResponse(
      status: HandshakeOK,
      features: FeatTLS or FeatPipelining,
      serverName: "test-server",
      errorMessage: ""
    )
    let encoded = encodeHandshakeResponse(r)
    let decoded = decodeHandshakeResponse(encoded)
    check decoded.isOk
    check decoded.value.status == HandshakeOK
    check decoded.value.features == (FeatTLS or FeatPipelining)
    check decoded.value.serverName == "test-server"
    check decoded.value.errorMessage == ""

  test "decodeHandshakeResponse error":
    let r = HandshakeResponse(
      status: HandshakeError,
      features: 0'u32,
      serverName: "server",
      errorMessage: "authentication failed"
    )
    let encoded = encodeHandshakeResponse(r)
    let decoded = decodeHandshakeResponse(encoded)
    check decoded.isOk
    check decoded.value.status == HandshakeError
    check decoded.value.errorMessage == "authentication failed"

  test "decodeHandshakeResponse truncated":
    var buf = ""
    buf.writeUint8(HandshakeOK)
    let decoded = decodeHandshakeResponse(buf)
    check decoded.isErr

suite "HandshakeResponse Roundtrip":
  test "success roundtrip":
    let r = HandshakeResponse(
      status: HandshakeOK,
      features: 0x1234'u32,
      serverName: "fractio",
      errorMessage: ""
    )
    let encoded = encodeHandshakeResponse(r)
    let decoded = decodeHandshakeResponse(encoded)
    check decoded.isOk
    check decoded.value.status == r.status
    check decoded.value.features == r.features
    check decoded.value.serverName == r.serverName
    check decoded.value.errorMessage == r.errorMessage

  test "error roundtrip":
    let r = HandshakeResponse(
      status: HandshakeError,
      features: 0'u32,
      serverName: "srv",
      errorMessage: "test error message"
    )
    let encoded = encodeHandshakeResponse(r)
    let decoded = decodeHandshakeResponse(encoded)
    check decoded.isOk
    check decoded.value.status == r.status
    check decoded.value.errorMessage == r.errorMessage

suite "Feature Negotiation":
  test "negotiateFeatures empty":
    check negotiateFeatures(0'u32, 0'u32) == 0'u32

  test "negotiateFeatures full match":
    check negotiateFeatures(0xFFFFFFFF'u32, 0xFFFFFFFF'u32) == 0xFFFFFFFF'u32

  test "negotiateFeatures partial match":
    let server = FeatTLS or FeatCompression or FeatSQL
    let client = FeatTLS or FeatTransactions
    let negotiated = negotiateFeatures(server, client)
    check negotiated == FeatTLS

  test "negotiateFeatures no overlap":
    let server = FeatTLS
    let client = FeatGraph
    check negotiateFeatures(server, client) == 0'u32

  test "negotiateFeatures all flags":
    let allFeatures = FeatTLS or FeatCompression or FeatPipelining or
                      FeatAsync or FeatTransactions or FeatSQL or
                      FeatGraph or FeatVector or FeatRedirect or FeatProxy
    check negotiateFeatures(allFeatures, allFeatures) == allFeatures
