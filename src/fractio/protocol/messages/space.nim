# Space management message encoding/decoding for the Fractio wire protocol.
#
# Implements:
#   CreateSpace (0x0708) — create a new space with specified replication factor
#   DropSpace   (0x0709) — drop an existing space
#
# Wire formats:
#   All encode procs prepend a 2-byte MessageType prefix.
#   Integers are big-endian.
#   Strings are uint8-length-prefixed (max 255 bytes each).
#
# CreateSpace Request:
#   [MessageType:2][name:1+N][replicas:4]
# CreateSpace Response:
#   [MessageType:2][success:1][spaceId:4][groupCount:4][message:1+N]
#
# DropSpace Request:
#   [MessageType:2][name:1+N]
# DropSpace Response:
#   [MessageType:2][success:1][message:1+N]

import ../types
import ../codec

# ---------------------------------------------------------------------------
# CreateSpace (0x0708)
# ---------------------------------------------------------------------------

type
  CreateSpaceRequest* = object
    name*: string    ## Space name (max 255 bytes)
    replicas*: int32 ## Replication factor (0 = ALL nodes)

  CreateSpaceResponse* = object
    success*: bool
    spaceId*: int32    ## Assigned space ID (0 on failure)
    groupCount*: int32 ## Number of Raft groups created
    message*: string   ## Human-readable result or error detail

proc encodeCreateSpaceRequest*(req: CreateSpaceRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtCreateSpace))
  buf.writeBytes8(req.name)
  buf.writeInt32BE(req.replicas)
  buf

proc decodeCreateSpaceRequest*(payload: string): Result[CreateSpaceRequest,
    ProtocolError] =
  var pos = 2 # skip MessageType
  var req: CreateSpaceRequest

  let nameR = readBytes8(payload, pos)
  if nameR.isErr: return peErr(nameR.error)
  req.name = nameR.value

  let replicasR = readInt32BE(payload, pos)
  if replicasR.isErr: return peErr(replicasR.error)
  req.replicas = replicasR.value

  peOk(req)

proc encodeCreateSpaceResponse*(resp: CreateSpaceResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtCreateSpace))
  buf.writeUint8(if resp.success: 0x01'u8 else: 0x00'u8)
  buf.writeInt32BE(resp.spaceId)
  buf.writeInt32BE(resp.groupCount)
  buf.writeBytes8(resp.message)
  buf

proc decodeCreateSpaceResponse*(payload: string): Result[CreateSpaceResponse,
    ProtocolError] =
  var pos = 2
  var resp: CreateSpaceResponse

  let successR = readUint8(payload, pos)
  if successR.isErr: return peErr(successR.error)
  resp.success = successR.value != 0

  let spaceIdR = readInt32BE(payload, pos)
  if spaceIdR.isErr: return peErr(spaceIdR.error)
  resp.spaceId = spaceIdR.value

  let groupCountR = readInt32BE(payload, pos)
  if groupCountR.isErr: return peErr(groupCountR.error)
  resp.groupCount = groupCountR.value

  let msgR = readBytes8(payload, pos)
  if msgR.isErr: return peErr(msgR.error)
  resp.message = msgR.value

  peOk(resp)

# ---------------------------------------------------------------------------
# DropSpace (0x0709)
# ---------------------------------------------------------------------------

type
  DropSpaceRequest* = object
    name*: string ## Space name to drop (max 255 bytes)

  DropSpaceResponse* = object
    success*: bool
    message*: string ## Human-readable result or error detail

proc encodeDropSpaceRequest*(req: DropSpaceRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtDropSpace))
  buf.writeBytes8(req.name)
  buf

proc decodeDropSpaceRequest*(payload: string): Result[DropSpaceRequest,
    ProtocolError] =
  var pos = 2 # skip MessageType
  var req: DropSpaceRequest

  let nameR = readBytes8(payload, pos)
  if nameR.isErr: return peErr(nameR.error)
  req.name = nameR.value

  peOk(req)

proc encodeDropSpaceResponse*(resp: DropSpaceResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtDropSpace))
  buf.writeUint8(if resp.success: 0x01'u8 else: 0x00'u8)
  buf.writeBytes8(resp.message)
  buf

proc decodeDropSpaceResponse*(payload: string): Result[DropSpaceResponse,
    ProtocolError] =
  var pos = 2
  var resp: DropSpaceResponse

  let successR = readUint8(payload, pos)
  if successR.isErr: return peErr(successR.error)
  resp.success = successR.value != 0

  let msgR = readBytes8(payload, pos)
  if msgR.isErr: return peErr(msgR.error)
  resp.message = msgR.value

  peOk(resp)
