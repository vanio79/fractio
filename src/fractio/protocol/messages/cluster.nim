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
