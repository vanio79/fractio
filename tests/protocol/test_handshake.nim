# Unit tests for protocol/handshake.nim
# Tests ServerGreeting, ClientHandshake, HandshakeResponse encode/decode
# and feature negotiation.

import std/unittest
import fractio/protocol/types
import fractio/protocol/codec
import fractio/protocol/handshake

# ---------------------------------------------------------------------------
# ServerGreeting
# ---------------------------------------------------------------------------

suite "handshake - ServerGreeting encode/decode":
  test "round-trip minimal greeting (no auth methods)":
    let g = ServerGreeting(
      magic: PROTOCOL_MAGIC,
      version: PROTOCOL_VERSION_1,
      features: FeatPipelining or FeatTransactions,
      authMethods: @[],
      serverId: 1'u16,
      clusterId: 0xDEADBEEFCAFEBABE'u64,
    )
    let encoded = encodeGreeting(g)
    let r = decodeGreeting(encoded)
    check r.isOk
    let g2 = r.value
    check g2.magic == PROTOCOL_MAGIC
    check g2.version == PROTOCOL_VERSION_1
    check g2.features == (FeatPipelining or FeatTransactions)
    check g2.authMethods.len == 0
    check g2.serverId == 1'u16
    check g2.clusterId == 0xDEADBEEFCAFEBABE'u64

  test "round-trip greeting with multiple auth methods":
    let g = ServerGreeting(
      magic: PROTOCOL_MAGIC,
      version: PROTOCOL_VERSION_1,
      features: 0'u32,
      authMethods: @[uint8(amNone), uint8(amPassword), uint8(amToken), uint8(
          amTLS)],
      serverId: 7'u16,
      clusterId: 1'u64,
    )
    let encoded = encodeGreeting(g)
    let r = decodeGreeting(encoded)
    check r.isOk
    check r.value.authMethods == @[uint8(amNone), uint8(amPassword), uint8(
        amToken), uint8(amTLS)]

  test "round-trip all feature flags set":
    let allFeats = FeatTLS or FeatCompression or FeatPipelining or FeatAsync or
                   FeatTransactions or FeatSQL or FeatGraph or FeatVector or
                   FeatRedirect or FeatProxy
    let g = ServerGreeting(
      magic: PROTOCOL_MAGIC,
      version: PROTOCOL_VERSION_1,
      features: allFeats,
      authMethods: @[uint8(amPassword)],
      serverId: 42'u16,
      clusterId: 999'u64,
    )
    let encoded = encodeGreeting(g)
    let r = decodeGreeting(encoded)
    check r.isOk
    check r.value.features == allFeats

  test "decode error on bad magic bytes":
    let g = ServerGreeting(
      magic: "XXXX", # wrong magic
      version: PROTOCOL_VERSION_1,
      features: 0'u32,
      authMethods: @[],
      serverId: 1'u16,
      clusterId: 0'u64,
    )
    let encoded = encodeGreeting(g)
    let r = decodeGreeting(encoded)
    check r.isErr
    check r.err.kind == peInvalidFrame

  test "decode error on truncated buffer":
    let encoded = "FRC" # only 3 bytes — need 4 for magic
    let r = decodeGreeting(encoded)
    check r.isErr
    check r.err.kind == peBoundsOverflow

  test "decode error on buffer missing version":
    let encoded = PROTOCOL_MAGIC # magic only, no version field
    let r = decodeGreeting(encoded)
    check r.isErr
    check r.err.kind == peBoundsOverflow

  test "serverId zero and clusterId zero":
    let g = ServerGreeting(
      magic: PROTOCOL_MAGIC,
      version: PROTOCOL_VERSION_1,
      features: 0'u32,
      authMethods: @[],
      serverId: 0'u16,
      clusterId: 0'u64,
    )
    let r = decodeGreeting(encodeGreeting(g))
    check r.isOk
    check r.value.serverId == 0'u16
    check r.value.clusterId == 0'u64

# ---------------------------------------------------------------------------
# ClientHandshake
# ---------------------------------------------------------------------------

suite "handshake - ClientHandshake encode/decode":
  test "round-trip minimal (no auth, no clientId)":
    let h = ClientHandshake(
      version: PROTOCOL_VERSION_1,
      features: FeatPipelining,
      authType: uint8(amNone),
      authData: "",
      clientId: "",
    )
    let encoded = encodeClientHandshake(h)
    let r = decodeClientHandshake(encoded)
    check r.isOk
    let h2 = r.value
    check h2.version == PROTOCOL_VERSION_1
    check h2.features == FeatPipelining
    check h2.authType == uint8(amNone)
    check h2.authData == ""
    check h2.clientId == ""

  test "round-trip with password auth":
    let h = ClientHandshake(
      version: PROTOCOL_VERSION_1,
      features: FeatPipelining or FeatTransactions,
      authType: uint8(amPassword),
      authData: "user:secretpassword",
      clientId: "my-app-v1",
    )
    let encoded = encodeClientHandshake(h)
    let r = decodeClientHandshake(encoded)
    check r.isOk
    let h2 = r.value
    check h2.authData == "user:secretpassword"
    check h2.clientId == "my-app-v1"
    check h2.authType == uint8(amPassword)

  test "round-trip with binary auth data":
    var authData = newString(16)
    for i in 0 ..< 16:
      authData[i] = char(i)
    let h = ClientHandshake(
      version: PROTOCOL_VERSION_1,
      features: FeatTLS,
      authType: uint8(amToken),
      authData: authData,
      clientId: "client",
    )
    let r = decodeClientHandshake(encodeClientHandshake(h))
    check r.isOk
    check r.value.authData == authData

  test "round-trip with max-length clientId (255 bytes)":
    var clientId = newString(255)
    for i in 0 ..< 255:
      clientId[i] = char(ord('a') + (i mod 26))
    let h = ClientHandshake(
      version: PROTOCOL_VERSION_1,
      features: 0'u32,
      authType: uint8(amNone),
      authData: "",
      clientId: clientId,
    )
    let r = decodeClientHandshake(encodeClientHandshake(h))
    check r.isOk
    check r.value.clientId == clientId

  test "decode error on truncated buffer":
    let r = decodeClientHandshake("")
    check r.isErr
    check r.err.kind == peBoundsOverflow

  test "decode error on buffer with only version field":
    var buf = ""
    buf.writeUint16BE(uint16(PROTOCOL_VERSION_1)) # version only, no features
    let r = decodeClientHandshake(buf)
    check r.isErr
    check r.err.kind == peBoundsOverflow

# ---------------------------------------------------------------------------
# HandshakeResponse
# ---------------------------------------------------------------------------

suite "handshake - HandshakeResponse encode/decode":
  test "round-trip OK response":
    let resp = HandshakeResponse(
      status: HandshakeOK,
      features: FeatPipelining or FeatTransactions,
      serverName: "fractio-node-1",
      errorMessage: "",
    )
    let encoded = encodeHandshakeResponse(resp)
    let r = decodeHandshakeResponse(encoded)
    check r.isOk
    let resp2 = r.value
    check resp2.status == HandshakeOK
    check resp2.features == (FeatPipelining or FeatTransactions)
    check resp2.serverName == "fractio-node-1"
    check resp2.errorMessage == ""

  test "round-trip error response":
    let resp = HandshakeResponse(
      status: HandshakeError,
      features: 0'u32,
      serverName: "fractio-node-2",
      errorMessage: "authentication failed: bad credentials",
    )
    let encoded = encodeHandshakeResponse(resp)
    let r = decodeHandshakeResponse(encoded)
    check r.isOk
    let resp2 = r.value
    check resp2.status == HandshakeError
    check resp2.errorMessage == "authentication failed: bad credentials"

  test "OK response does NOT encode errorMessage":
    let resp = HandshakeResponse(
      status: HandshakeOK,
      features: 0'u32,
      serverName: "srv",
      errorMessage: "should be ignored",
    )
    let encoded = encodeHandshakeResponse(resp)
    let r = decodeHandshakeResponse(encoded)
    check r.isOk
    check r.value.errorMessage == "" # OK responses never carry errorMessage

  test "round-trip empty serverName":
    let resp = HandshakeResponse(
      status: HandshakeOK,
      features: 0'u32,
      serverName: "",
      errorMessage: "",
    )
    let r = decodeHandshakeResponse(encodeHandshakeResponse(resp))
    check r.isOk
    check r.value.serverName == ""

  test "round-trip max-length serverName (255 bytes)":
    var name = newString(255)
    for i in 0 ..< 255:
      name[i] = char(ord('A') + (i mod 26))
    let resp = HandshakeResponse(
      status: HandshakeOK,
      features: 0'u32,
      serverName: name,
      errorMessage: "",
    )
    let r = decodeHandshakeResponse(encodeHandshakeResponse(resp))
    check r.isOk
    check r.value.serverName == name

  test "decode error on empty buffer":
    let r = decodeHandshakeResponse("")
    check r.isErr
    check r.err.kind == peBoundsOverflow

  test "decode error response with empty errorMessage":
    let resp = HandshakeResponse(
      status: HandshakeError,
      features: 0'u32,
      serverName: "srv",
      errorMessage: "",
    )
    let r = decodeHandshakeResponse(encodeHandshakeResponse(resp))
    check r.isOk
    check r.value.status == HandshakeError
    check r.value.errorMessage == ""

# ---------------------------------------------------------------------------
# Feature negotiation
# ---------------------------------------------------------------------------

suite "handshake - negotiateFeatures":
  test "intersection of identical sets":
    let feats = FeatPipelining or FeatTransactions or FeatTLS
    check negotiateFeatures(feats, feats) == feats

  test "intersection is subset of both":
    let server = FeatPipelining or FeatTransactions or FeatTLS or FeatSQL
    let client = FeatPipelining or FeatTransactions or FeatGraph
    let negotiated = negotiateFeatures(server, client)
    check negotiated == (FeatPipelining or FeatTransactions)
    check (negotiated and FeatTLS) == 0'u32
    check (negotiated and FeatSQL) == 0'u32
    check (negotiated and FeatGraph) == 0'u32

  test "no common features → zero":
    check negotiateFeatures(FeatTLS, FeatCompression) == 0'u32

  test "either side has zero features → zero":
    check negotiateFeatures(0'u32, FeatPipelining) == 0'u32
    check negotiateFeatures(FeatPipelining, 0'u32) == 0'u32

  test "both zero → zero":
    check negotiateFeatures(0'u32, 0'u32) == 0'u32

  test "all features on both sides → all features":
    let all: uint32 = 0xFFFFFFFF'u32
    check negotiateFeatures(all, all) == all

# ---------------------------------------------------------------------------
# HandshakeOK / HandshakeError constants
# ---------------------------------------------------------------------------

suite "handshake - constants":
  test "HandshakeOK is 0x00":
    check HandshakeOK == 0x00'u8

  test "HandshakeError is 0x01":
    check HandshakeError == 0x01'u8
