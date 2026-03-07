# Connection handshake encoding/decoding for the Fractio protocol.
#
# Handshake sequence (not framed — raw TCP bytes, no CRC):
#
#   Server → Client : ServerGreeting
#   Client → Server : ClientHandshake
#   Server → Client : HandshakeResponse
#
# After HandshakeResponse with status 0x00, both sides switch to the
# standard framed message protocol.

import ./types
import ./codec

# ---------------------------------------------------------------------------
# ServerGreeting
# ---------------------------------------------------------------------------

type
  ServerGreeting* = object
    magic*: string           # Must equal PROTOCOL_MAGIC ("FRC1")
    version*: ProtocolVersion
    features*: uint32        # Server feature flags
    authMethods*: seq[uint8] # List of supported AuthMethod codes
    serverId*: uint16
    clusterId*: uint64

proc encodeGreeting*(g: ServerGreeting): string =
  var buf = ""
  # magic: 4 raw bytes (no length prefix)
  buf.add(g.magic)
  buf.writeUint16BE(uint16(g.version))
  buf.writeUint32BE(g.features)
  # auth methods: 1-byte count + each method as 1 byte
  buf.writeUint8(uint8(g.authMethods.len))
  for m in g.authMethods:
    buf.writeUint8(m)
  buf.writeUint16BE(g.serverId)
  buf.writeUint64BE(g.clusterId)
  buf

proc decodeGreeting*(data: string): Result[ServerGreeting, ProtocolError] =
  var pos = 0
  let r = checkBounds(data, pos, 4)
  if r.isErr: return peErr(r.error)
  let magic = data[0 ..< 4]
  pos += 4
  if magic != PROTOCOL_MAGIC:
    return peErr(newProtocolError(peInvalidFrame,
      "invalid magic bytes: expected FRC1, got " & magic))

  let verR = readUint16BE(data, pos)
  if verR.isErr: return peErr(verR.error)

  let featR = readUint32BE(data, pos)
  if featR.isErr: return peErr(featR.error)

  let countR = readUint8(data, pos)
  if countR.isErr: return peErr(countR.error)
  var methods: seq[uint8]
  for _ in 0 ..< int(countR.value):
    let mR = readUint8(data, pos)
    if mR.isErr: return peErr(mR.error)
    methods.add(mR.value)

  let srvR = readUint16BE(data, pos)
  if srvR.isErr: return peErr(srvR.error)

  let clR = readUint64BE(data, pos)
  if clR.isErr: return peErr(clR.error)

  peOk(ServerGreeting(
    magic: magic,
    version: ProtocolVersion(verR.value),
    features: featR.value,
    authMethods: methods,
    serverId: srvR.value,
    clusterId: clR.value,
  ))

# ---------------------------------------------------------------------------
# ClientHandshake
# ---------------------------------------------------------------------------

type
  ClientHandshake* = object
    version*: ProtocolVersion
    features*: uint32 # Requested feature flags
    authType*: uint8  # AuthMethod code
    authData*: string # Encoded auth credentials
    clientId*: string # Optional client identifier

proc encodeClientHandshake*(h: ClientHandshake): string =
  var buf = ""
  buf.writeUint16BE(uint16(h.version))
  buf.writeUint32BE(h.features)
  buf.writeUint8(h.authType)
  buf.writeBytes(h.authData) # uint32-length-prefixed
  buf.writeBytes8(h.clientId) # uint8-length-prefixed (max 255 chars)
  buf

proc decodeClientHandshake*(data: string): Result[ClientHandshake,
    ProtocolError] =
  var pos = 0

  let verR = readUint16BE(data, pos)
  if verR.isErr: return peErr(verR.error)

  let featR = readUint32BE(data, pos)
  if featR.isErr: return peErr(featR.error)

  let authTypeR = readUint8(data, pos)
  if authTypeR.isErr: return peErr(authTypeR.error)

  let authDataR = readBytes(data, pos)
  if authDataR.isErr: return peErr(authDataR.error)

  let clientIdR = readBytes8(data, pos)
  if clientIdR.isErr: return peErr(clientIdR.error)

  peOk(ClientHandshake(
    version: ProtocolVersion(verR.value),
    features: featR.value,
    authType: authTypeR.value,
    authData: authDataR.value,
    clientId: clientIdR.value,
  ))

# ---------------------------------------------------------------------------
# HandshakeResponse
# ---------------------------------------------------------------------------

const
  HandshakeOK* = 0x00'u8
  HandshakeError* = 0x01'u8

type
  HandshakeResponse* = object
    status*: uint8        # HandshakeOK or HandshakeError
    features*: uint32     # Negotiated features (intersection of server+client)
    serverName*: string   # Server identifier string
    errorMessage*: string # Only meaningful when status != HandshakeOK

proc encodeHandshakeResponse*(r: HandshakeResponse): string =
  var buf = ""
  buf.writeUint8(r.status)
  buf.writeUint32BE(r.features)
  buf.writeBytes8(r.serverName)
  if r.status != HandshakeOK:
    buf.writeUint16BE(uint16(r.errorMessage.len))
    buf.add(r.errorMessage)
  buf

proc decodeHandshakeResponse*(data: string): Result[HandshakeResponse,
    ProtocolError] =
  var pos = 0

  let statusR = readUint8(data, pos)
  if statusR.isErr: return peErr(statusR.error)

  let featR = readUint32BE(data, pos)
  if featR.isErr: return peErr(featR.error)

  let nameR = readBytes8(data, pos)
  if nameR.isErr: return peErr(nameR.error)

  var errMsg = ""
  if statusR.value != HandshakeOK:
    let msgLenR = readUint16BE(data, pos)
    if msgLenR.isErr: return peErr(msgLenR.error)
    let msgLen = int(msgLenR.value)
    let rb = checkBounds(data, pos, msgLen)
    if rb.isErr: return peErr(rb.error)
    errMsg = data[pos ..< pos + msgLen]
    pos += msgLen

  peOk(HandshakeResponse(
    status: statusR.value,
    features: featR.value,
    serverName: nameR.value,
    errorMessage: errMsg,
  ))

# ---------------------------------------------------------------------------
# Feature negotiation helper
# ---------------------------------------------------------------------------

proc negotiateFeatures*(serverFeatures, clientFeatures: uint32): uint32 =
  ## Return the intersection of features both sides support.
  serverFeatures and clientFeatures
