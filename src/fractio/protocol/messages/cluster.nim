# Cluster admin message encoding/decoding for the Fractio wire protocol.
#
# Implements:
#   JoinNode        (0x0703) — add a node to the cluster
#   RemoveNode      (0x0704) — remove a node from the cluster
#   ListNodes       (0x0705) — list all known cluster nodes
#   RebalanceStatus (0x0706) — query pending/in-progress/completed rebalance ops
#
# Wire formats:
#   All encode procs prepend a 2-byte MessageType prefix.
#   Integers are big-endian.
#   Strings are uint8-length-prefixed (max 255 bytes each).
#
# JoinNode Request:
#   [MessageType:2][nodeId:2][host:1+N][raftPort:2][clientPort:2]
# JoinNode Response:
#   [MessageType:2][success:1][message:1+N]
#
# RemoveNode Request:
#   [MessageType:2][nodeId:2]
# RemoveNode Response:
#   [MessageType:2][success:1][message:1+N]
#
# ListNodes Request:
#   [MessageType:2]
# ListNodes Response:
#   [MessageType:2][count:2][nodes: count × (nodeId:2 host:1+N raftPort:2 clientPort:2 status:1)]
#
# RebalanceStatus Request:
#   [MessageType:2]
# RebalanceStatus Response:
#   [MessageType:2][pending:4][inProgress:4][completed:4][failed:4]

import ../types
import ../codec

# ---------------------------------------------------------------------------
# Node status constants (used in ListNodes response)
# ---------------------------------------------------------------------------

const
  NodeStatusUnknown* = 0x00'u8
  NodeStatusActive* = 0x01'u8   ## Node is reachable and serving traffic
  NodeStatusDraining* = 0x02'u8 ## Node is being gracefully removed
  NodeStatusDown* = 0x03'u8     ## Node is unreachable

# ---------------------------------------------------------------------------
# JoinNode (0x0703)
# ---------------------------------------------------------------------------

type
  JoinNodeRequest* = object
    nodeId*: uint16     ## Numeric ID for the joining node (1-based, operator-assigned)
    host*: string       ## Reachable hostname or IP
    raftPort*: uint16   ## Port the node listens on for Raft RPCs
    clientPort*: uint16 ## Port the node listens on for client connections

  JoinNodeResponse* = object
    success*: bool
    message*: string ## Human-readable result or error detail

proc encodeJoinNodeRequest*(req: JoinNodeRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtJoinNode))
  buf.writeUint16BE(req.nodeId)
  buf.writeBytes8(req.host)
  buf.writeUint16BE(req.raftPort)
  buf.writeUint16BE(req.clientPort)
  buf

proc decodeJoinNodeRequest*(payload: string): Result[JoinNodeRequest,
    ProtocolError] =
  var pos = 2 # skip MessageType
  var req: JoinNodeRequest

  let nodeIdR = readUint16BE(payload, pos)
  if nodeIdR.isErr: return peErr(nodeIdR.error)
  req.nodeId = nodeIdR.value

  let hostR = readBytes8(payload, pos)
  if hostR.isErr: return peErr(hostR.error)
  req.host = hostR.value

  let raftPortR = readUint16BE(payload, pos)
  if raftPortR.isErr: return peErr(raftPortR.error)
  req.raftPort = raftPortR.value

  let clientPortR = readUint16BE(payload, pos)
  if clientPortR.isErr: return peErr(clientPortR.error)
  req.clientPort = clientPortR.value

  peOk(req)

proc encodeJoinNodeResponse*(resp: JoinNodeResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtJoinNode))
  buf.writeUint8(if resp.success: 0x01'u8 else: 0x00'u8)
  buf.writeBytes8(resp.message)
  buf

proc decodeJoinNodeResponse*(payload: string): Result[JoinNodeResponse,
    ProtocolError] =
  var pos = 2
  var resp: JoinNodeResponse

  let successR = readUint8(payload, pos)
  if successR.isErr: return peErr(successR.error)
  resp.success = successR.value != 0

  let msgR = readBytes8(payload, pos)
  if msgR.isErr: return peErr(msgR.error)
  resp.message = msgR.value

  peOk(resp)

# ---------------------------------------------------------------------------
# RemoveNode (0x0704)
# ---------------------------------------------------------------------------

type
  RemoveNodeRequest* = object
    nodeId*: uint16 ## Numeric ID of the node to remove

  RemoveNodeResponse* = object
    success*: bool
    message*: string

proc encodeRemoveNodeRequest*(req: RemoveNodeRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtRemoveNode))
  buf.writeUint16BE(req.nodeId)
  buf

proc decodeRemoveNodeRequest*(payload: string): Result[RemoveNodeRequest,
    ProtocolError] =
  var pos = 2
  let nodeIdR = readUint16BE(payload, pos)
  if nodeIdR.isErr: return peErr(nodeIdR.error)
  peOk(RemoveNodeRequest(nodeId: nodeIdR.value))

proc encodeRemoveNodeResponse*(resp: RemoveNodeResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtRemoveNode))
  buf.writeUint8(if resp.success: 0x01'u8 else: 0x00'u8)
  buf.writeBytes8(resp.message)
  buf

proc decodeRemoveNodeResponse*(payload: string): Result[RemoveNodeResponse,
    ProtocolError] =
  var pos = 2
  var resp: RemoveNodeResponse

  let successR = readUint8(payload, pos)
  if successR.isErr: return peErr(successR.error)
  resp.success = successR.value != 0

  let msgR = readBytes8(payload, pos)
  if msgR.isErr: return peErr(msgR.error)
  resp.message = msgR.value

  peOk(resp)

# ---------------------------------------------------------------------------
# ListNodes (0x0705)
# ---------------------------------------------------------------------------

type
  NodeInfo* = object
    nodeId*: uint16
    host*: string
    raftPort*: uint16
    clientPort*: uint16
    status*: uint8 ## NodeStatus* constant

  ListNodesRequest* = object
    discard

  ListNodesResponse* = object
    nodes*: seq[NodeInfo]

proc encodeListNodesRequest*(): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtListNodes))
  buf

proc decodeListNodesRequest*(payload: string): Result[ListNodesRequest,
    ProtocolError] =
  let rb = checkBounds(payload, 0, 2)
  if rb.isErr: return peErr(rb.error)
  peOk(ListNodesRequest())

proc encodeListNodesResponse*(resp: ListNodesResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtListNodes))
  buf.writeUint16BE(uint16(resp.nodes.len))
  for node in resp.nodes:
    buf.writeUint16BE(node.nodeId)
    buf.writeBytes8(node.host)
    buf.writeUint16BE(node.raftPort)
    buf.writeUint16BE(node.clientPort)
    buf.writeUint8(node.status)
  buf

proc decodeListNodesResponse*(payload: string): Result[ListNodesResponse,
    ProtocolError] =
  var pos = 2
  var resp: ListNodesResponse

  let countR = readUint16BE(payload, pos)
  if countR.isErr: return peErr(countR.error)
  let count = int(countR.value)

  resp.nodes = newSeq[NodeInfo](count)
  for i in 0..<count:
    let nodeIdR = readUint16BE(payload, pos)
    if nodeIdR.isErr: return peErr(nodeIdR.error)
    resp.nodes[i].nodeId = nodeIdR.value

    let hostR = readBytes8(payload, pos)
    if hostR.isErr: return peErr(hostR.error)
    resp.nodes[i].host = hostR.value

    let raftPortR = readUint16BE(payload, pos)
    if raftPortR.isErr: return peErr(raftPortR.error)
    resp.nodes[i].raftPort = raftPortR.value

    let clientPortR = readUint16BE(payload, pos)
    if clientPortR.isErr: return peErr(clientPortR.error)
    resp.nodes[i].clientPort = clientPortR.value

    let statusR = readUint8(payload, pos)
    if statusR.isErr: return peErr(statusR.error)
    resp.nodes[i].status = statusR.value

  peOk(resp)

# ---------------------------------------------------------------------------
# RebalanceStatus (0x0706)
# ---------------------------------------------------------------------------

type
  RebalanceStatusRequest* = object
    discard

  RebalanceStatusResponse* = object
    pending*: uint32
    inProgress*: uint32
    completed*: uint32
    failed*: uint32

proc encodeRebalanceStatusRequest*(): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtRebalanceStatus))
  buf

proc decodeRebalanceStatusRequest*(payload: string): Result[
    RebalanceStatusRequest, ProtocolError] =
  let rb = checkBounds(payload, 0, 2)
  if rb.isErr: return peErr(rb.error)
  peOk(RebalanceStatusRequest())

proc encodeRebalanceStatusResponse*(resp: RebalanceStatusResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtRebalanceStatus))
  buf.writeUint32BE(resp.pending)
  buf.writeUint32BE(resp.inProgress)
  buf.writeUint32BE(resp.completed)
  buf.writeUint32BE(resp.failed)
  buf

proc decodeRebalanceStatusResponse*(payload: string): Result[
    RebalanceStatusResponse, ProtocolError] =
  var pos = 2
  var resp: RebalanceStatusResponse

  let pendR = readUint32BE(payload, pos)
  if pendR.isErr: return peErr(pendR.error)
  resp.pending = pendR.value

  let inpR = readUint32BE(payload, pos)
  if inpR.isErr: return peErr(inpR.error)
  resp.inProgress = inpR.value

  let compR = readUint32BE(payload, pos)
  if compR.isErr: return peErr(compR.error)
  resp.completed = compR.value

  let failR = readUint32BE(payload, pos)
  if failR.isErr: return peErr(failR.error)
  resp.failed = failR.value

  peOk(resp)

# ---------------------------------------------------------------------------
# DrainNode (0x0707)
# ---------------------------------------------------------------------------

type
  DrainNodeRequest* = object
    nodeId*: uint16 ## ID of the node to mark as draining

  DrainNodeResponse* = object
    success*: bool
    message*: string

proc encodeDrainNodeRequest*(req: DrainNodeRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtDrainNode))
  buf.writeUint16BE(req.nodeId)
  buf

proc decodeDrainNodeRequest*(payload: string): Result[DrainNodeRequest,
    ProtocolError] =
  var pos = 2
  let nodeIdR = readUint16BE(payload, pos)
  if nodeIdR.isErr: return peErr(nodeIdR.error)
  peOk(DrainNodeRequest(nodeId: nodeIdR.value))

proc encodeDrainNodeResponse*(resp: DrainNodeResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtDrainNode))
  buf.writeUint8(if resp.success: 0x01'u8 else: 0x00'u8)
  buf.writeBytes8(resp.message)
  buf

proc decodeDrainNodeResponse*(payload: string): Result[DrainNodeResponse,
    ProtocolError] =
  var pos = 2
  var resp: DrainNodeResponse

  let successR = readUint8(payload, pos)
  if successR.isErr: return peErr(successR.error)
  resp.success = successR.value != 0

  let msgR = readBytes8(payload, pos)
  if msgR.isErr: return peErr(msgR.error)
  resp.message = msgR.value

  peOk(resp)

# ---------------------------------------------------------------------------
# CreateGroup (0x070A) — Directed group creation
# ---------------------------------------------------------------------------
# Meta leader sends CreateGroupRequest to the preferred leader node.
# The preferred leader creates the group and wins election unopposed.
# Then the preferred leader (or meta leader) sends JoinGroupRequest to other nodes.
#
# Wire format:
# CreateGroup Request:
#   [MessageType:2][groupId:16][preferredLeaderId:2][memberCount:2]
#   [memberCount members, each:]
#     [nodeId:2][host:1+N][raftPort:2][clientPort:2]
# CreateGroup Response:
#   [MessageType:2][success:1]
#   On success: [groupId:16]
#   On failure: [errorLen:2][error:N]
#
# JoinGroup Request:
#   [MessageType:2][groupId:16][creatorNodeId:2][creatorHost:1+N][creatorPort:2]
# JoinGroup Response:
#   [MessageType:2][success:1]
#   On success: [groupId:16]
#   On failure: [errorLen:2][error:N]

type
  CreateGroupMember* = object
    ## Member info for CreateGroupRequest
    nodeId*: uint16
    host*: string
    raftPort*: uint16
    clientPort*: uint16

  CreateGroupRequest* = object
    ## Request for a specific node to create a Raft group and become leader.
    groupId*: string                 ## 16-byte ULID as binary string
    preferredLeaderId*: uint16       ## Node that should become leader (usually the recipient)
    members*: seq[CreateGroupMember] ## All group members

  CreateGroupResponse* = object
    success*: bool
    groupId*: string ## 16-byte ULID as binary string (on success)
    error*: string   ## Error message (on failure)

  JoinGroupRequest* = object
    ## Request for a node to join an existing Raft group.
    groupId*: string       ## 16-byte ULID as binary string
    creatorNodeId*: uint16 ## Node that created the group (to connect to)
    creatorHost*: string   ## Host address of creator
    creatorPort*: uint16   ## Raft port of creator

  JoinGroupResponse* = object
    success*: bool
    groupId*: string ## 16-byte ULID as binary string (on success)
    error*: string   ## Error message (on failure)

proc encodeCreateGroupRequest*(req: CreateGroupRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtCreateGroup))
  # GroupId is 16 bytes binary (no length prefix)
  buf.add(req.groupId)
  buf.writeUint16BE(req.preferredLeaderId)
  buf.writeUint16BE(uint16(req.members.len))
  for m in req.members:
    buf.writeUint16BE(m.nodeId)
    buf.writeBytes8(m.host)
    buf.writeUint16BE(m.raftPort)
    buf.writeUint16BE(m.clientPort)
  buf

proc decodeCreateGroupRequest*(payload: string): Result[CreateGroupRequest,
    ProtocolError] =
  var pos = 2 # skip MessageType
  var req: CreateGroupRequest

  # GroupId is 16 bytes binary
  if pos + 16 > payload.len:
    return peErr(ProtocolError(kind: peBoundsOverflow,
        msg: "payload too short for groupId"))
  req.groupId = payload[pos..pos+15]
  pos += 16

  let prefLeaderR = readUint16BE(payload, pos)
  if prefLeaderR.isErr: return peErr(prefLeaderR.error)
  req.preferredLeaderId = prefLeaderR.value

  let memberCountR = readUint16BE(payload, pos)
  if memberCountR.isErr: return peErr(memberCountR.error)
  let memberCount = int(memberCountR.value)

  req.members = newSeqOfCap[CreateGroupMember](memberCount)
  for i in 0..<memberCount:
    var m: CreateGroupMember

    let nodeIdR = readUint16BE(payload, pos)
    if nodeIdR.isErr: return peErr(nodeIdR.error)
    m.nodeId = nodeIdR.value

    let hostR = readBytes8(payload, pos)
    if hostR.isErr: return peErr(hostR.error)
    m.host = hostR.value

    let raftPortR = readUint16BE(payload, pos)
    if raftPortR.isErr: return peErr(raftPortR.error)
    m.raftPort = raftPortR.value

    let clientPortR = readUint16BE(payload, pos)
    if clientPortR.isErr: return peErr(clientPortR.error)
    m.clientPort = clientPortR.value

    req.members.add(m)

  peOk(req)

proc encodeCreateGroupResponse*(resp: CreateGroupResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtCreateGroup))
  buf.writeUint8(if resp.success: 0x01'u8 else: 0x00'u8)
  if resp.success:
    buf.add(resp.groupId)
  else:
    buf.writeBytes16(resp.error)
  buf

proc decodeCreateGroupResponse*(payload: string): Result[CreateGroupResponse,
    ProtocolError] =
  var pos = 2
  var resp: CreateGroupResponse

  let successR = readUint8(payload, pos)
  if successR.isErr: return peErr(successR.error)
  resp.success = successR.value != 0

  if resp.success:
    if pos + 16 > payload.len:
      return peErr(ProtocolError(kind: peBoundsOverflow,
          msg: "payload too short for groupId"))
    resp.groupId = payload[pos..pos+15]
  else:
    let errR = readBytes16(payload, pos)
    if errR.isErr: return peErr(errR.error)
    resp.error = errR.value

  peOk(resp)

proc encodeJoinGroupRequest*(req: JoinGroupRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtJoinGroup))
  # GroupId is 16 bytes binary (no length prefix)
  buf.add(req.groupId)
  buf.writeUint16BE(req.creatorNodeId)
  buf.writeBytes8(req.creatorHost)
  buf.writeUint16BE(req.creatorPort)
  buf

proc decodeJoinGroupRequest*(payload: string): Result[JoinGroupRequest,
    ProtocolError] =
  var pos = 2 # skip MessageType
  var req: JoinGroupRequest

  # GroupId is 16 bytes binary
  if pos + 16 > payload.len:
    return peErr(ProtocolError(kind: peBoundsOverflow,
        msg: "payload too short for groupId"))
  req.groupId = payload[pos..pos+15]
  pos += 16

  let creatorNodeIdR = readUint16BE(payload, pos)
  if creatorNodeIdR.isErr: return peErr(creatorNodeIdR.error)
  req.creatorNodeId = creatorNodeIdR.value

  let creatorHostR = readBytes8(payload, pos)
  if creatorHostR.isErr: return peErr(creatorHostR.error)
  req.creatorHost = creatorHostR.value

  let creatorPortR = readUint16BE(payload, pos)
  if creatorPortR.isErr: return peErr(creatorPortR.error)
  req.creatorPort = creatorPortR.value

  peOk(req)

proc encodeJoinGroupResponse*(resp: JoinGroupResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtJoinGroup))
  buf.writeUint8(if resp.success: 0x01'u8 else: 0x00'u8)
  if resp.success:
    buf.add(resp.groupId)
  else:
    buf.writeBytes16(resp.error)
  buf

proc decodeJoinGroupResponse*(payload: string): Result[JoinGroupResponse,
    ProtocolError] =
  var pos = 2
  var resp: JoinGroupResponse

  let successR = readUint8(payload, pos)
  if successR.isErr: return peErr(successR.error)
  resp.success = successR.value != 0

  if resp.success:
    if pos + 16 > payload.len:
      return peErr(ProtocolError(kind: peBoundsOverflow,
          msg: "payload too short for groupId"))
    resp.groupId = payload[pos..pos+15]
  else:
    let errR = readBytes16(payload, pos)
    if errR.isErr: return peErr(errR.error)
    resp.error = errR.value

  peOk(resp)
