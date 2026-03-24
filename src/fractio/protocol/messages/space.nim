# Space management message encoding/decoding for the Fractio wire protocol.
#
# Implements:
#   CreateSpace (0x0708) — create a new space with specified replication factor
#   DropSpace   (0x0709) — drop an existing space
#
# Wire formats:
#   All encode procs prepend a 2-byte MessageType prefix.
#   Integers are big-endian.
#   Strings use uint16 length prefix for longer names.
#   Binary records use uint32 length prefix.
#   ULIDs are 16 bytes binary.
#
# CreateSpace Request:
#   [MessageType:2][nameLen:2][name:N][replicas:4]
# CreateSpace Response:
#   [MessageType:2][success:1]
#   On success:
#     [spaceId:16][groupCount:4]
#     [spaceRecordLen:4][spaceRecord:N]
#     [groupCount groups, each:]
#       [groupId:16][groupRecordLen:4][groupRecord:N]
#   On failure:
#     [errorLen:2][error:N]
#
# DropSpace Request:
#   [MessageType:2][nameLen:2][name:N]
# DropSpace Response:
#   [MessageType:2][success:1]
#   On success:
#     [spaceId:16]
#     [deletedGroupCount:4]
#     [deletedGroupCount deleted groupIds, each:]
#       [groupId:16]
#   On failure:
#     [errorLen:2][error:N]

import ../types
import ../codec
import ../../core/types

# ---------------------------------------------------------------------------
# CreateSpace (0x0708)
# ---------------------------------------------------------------------------

type
  CreateSpaceRequest* = object
    name*: string    ## Space name (max 65535 bytes)
    replicas*: int32 ## Replication factor (0 = ALL nodes)

  GroupRecordData* = object
    ## A single group record returned in CreateSpaceResponse
    groupId*: ULID
    record*: string ## Binary-encoded GroupRecord

  CreateSpaceResponse* = object
    success*: bool
    ## On success:
    spaceId*: ULID                      ## Assigned space ID
    groupCount*: int32                  ## Number of Raft groups created
    spaceRecord*: string                ## Binary-encoded SpaceRecord for client cache
    groupRecords*: seq[GroupRecordData] ## All created group records
                                        ## On failure:
    error*: string                      ## Error message

proc encodeCreateSpaceRequest*(req: CreateSpaceRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtCreateSpace))
  buf.writeBytes16(req.name)
  buf.writeInt32BE(req.replicas)
  buf

proc decodeCreateSpaceRequest*(payload: string): Result[CreateSpaceRequest,
    ProtocolError] =
  var pos = 2 # skip MessageType
  var req: CreateSpaceRequest

  let nameR = readBytes16(payload, pos)
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

  if resp.success:
    # Write 16-byte ULID
    buf.writeBytes(ulidToBytes(resp.spaceId))
    buf.writeInt32BE(resp.groupCount)
    buf.writeBytes32(resp.spaceRecord)
    buf.writeInt32BE(resp.groupRecords.len.int32)
    for gr in resp.groupRecords:
      # Write 16-byte ULID
      buf.writeBytes(ulidToBytes(gr.groupId))
      buf.writeBytes32(gr.record)
  else:
    buf.writeBytes16(resp.error)
  buf

proc decodeCreateSpaceResponse*(payload: string): Result[CreateSpaceResponse,
    ProtocolError] =
  var pos = 2
  var resp: CreateSpaceResponse

  let successR = readUint8(payload, pos)
  if successR.isErr: return peErr(successR.error)
  resp.success = successR.value != 0

  if resp.success:
    # Read 16-byte ULID
    if pos + 16 > payload.len:
      return peErr(ProtocolError(kind: peBoundsOverflow,
          msg: "payload too short for spaceId ULID"))
    var ulidBytes: string
    for i in 0..<16:
      ulidBytes.add(payload[pos])
      inc pos
    resp.spaceId = ulidFromBytes(ulidBytes)

    let groupCountR = readInt32BE(payload, pos)
    if groupCountR.isErr: return peErr(groupCountR.error)
    resp.groupCount = groupCountR.value

    let spaceRecordR = readBytes32(payload, pos)
    if spaceRecordR.isErr: return peErr(spaceRecordR.error)
    resp.spaceRecord = spaceRecordR.value

    let numGroupsR = readInt32BE(payload, pos)
    if numGroupsR.isErr: return peErr(numGroupsR.error)
    let numGroups = numGroupsR.value

    resp.groupRecords = newSeqOfCap[GroupRecordData](numGroups.int)
    for i in 0 ..< numGroups.int:
      # Read 16-byte ULID
      if pos + 16 > payload.len:
        return peErr(ProtocolError(kind: peBoundsOverflow,
            msg: "payload too short for groupId ULID"))
      var gidBytes: string
      for j in 0..<16:
        gidBytes.add(payload[pos])
        inc pos
      let gid = ulidFromBytes(gidBytes)

      let recR = readBytes32(payload, pos)
      if recR.isErr: return peErr(recR.error)

      resp.groupRecords.add(GroupRecordData(
        groupId: gid,
        record: recR.value
      ))
  else:
    let errR = readBytes16(payload, pos)
    if errR.isErr: return peErr(errR.error)
    resp.error = errR.value

  peOk(resp)

# ---------------------------------------------------------------------------
# DropSpace (0x0709)
# ---------------------------------------------------------------------------

type
  DropSpaceRequest* = object
    name*: string ## Space name to drop (max 65535 bytes)

  DropSpaceResponse* = object
    success*: bool
    ## On success:
    spaceId*: ULID              ## ID of deleted space (for client cache cleanup)
    deletedGroupIds*: seq[ULID] ## GroupIds that were deleted
                                ## On failure:
    error*: string              ## Error message

proc encodeDropSpaceRequest*(req: DropSpaceRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtDropSpace))
  buf.writeBytes16(req.name)
  buf

proc decodeDropSpaceRequest*(payload: string): Result[DropSpaceRequest,
    ProtocolError] =
  var pos = 2 # skip MessageType
  var req: DropSpaceRequest

  let nameR = readBytes16(payload, pos)
  if nameR.isErr: return peErr(nameR.error)
  req.name = nameR.value

  peOk(req)

proc encodeDropSpaceResponse*(resp: DropSpaceResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtDropSpace))
  buf.writeUint8(if resp.success: 0x01'u8 else: 0x00'u8)

  if resp.success:
    # Write 16-byte ULID
    buf.writeBytes(ulidToBytes(resp.spaceId))
    buf.writeInt32BE(resp.deletedGroupIds.len.int32)
    for gid in resp.deletedGroupIds:
      # Write 16-byte ULID
      buf.writeBytes(ulidToBytes(gid))
  else:
    buf.writeBytes16(resp.error)
  buf

proc decodeDropSpaceResponse*(payload: string): Result[DropSpaceResponse,
    ProtocolError] =
  var pos = 2
  var resp: DropSpaceResponse

  let successR = readUint8(payload, pos)
  if successR.isErr: return peErr(successR.error)
  resp.success = successR.value != 0

  if resp.success:
    # Read 16-byte ULID
    if pos + 16 > payload.len:
      return peErr(ProtocolError(kind: peBoundsOverflow,
          msg: "payload too short for spaceId ULID"))
    var ulidBytes: string
    for i in 0..<16:
      ulidBytes.add(payload[pos])
      inc pos
    resp.spaceId = ulidFromBytes(ulidBytes)

    let numGroupsR = readInt32BE(payload, pos)
    if numGroupsR.isErr: return peErr(numGroupsR.error)
    let numGroups = numGroupsR.value

    resp.deletedGroupIds = newSeqOfCap[ULID](numGroups.int)
    for i in 0 ..< numGroups.int:
      # Read 16-byte ULID
      if pos + 16 > payload.len:
        return peErr(ProtocolError(kind: peBoundsOverflow,
            msg: "payload too short for groupId ULID"))
      var gidBytes: string
      for j in 0..<16:
        gidBytes.add(payload[pos])
        inc pos
      resp.deletedGroupIds.add(ulidFromBytes(gidBytes))
  else:
    let errR = readBytes16(payload, pos)
    if errR.isErr: return peErr(errR.error)
    resp.error = errR.value

  peOk(resp)
