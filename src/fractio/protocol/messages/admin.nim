# Admin message encoding/decoding for the Fractio wire protocol.
#
# Implements:
#   ServerInfo (0x0700) — version, uptime, role, shard count, client count
#   Metrics    (0x0701) — request counters, latency stats, KV stats
#   Health     (0x0702) — cluster/leader/replication health status
#
# Wire formats:
#   All encode procs prepend a 2-byte MessageType prefix.
#   Integers are big-endian.
#   Strings are uint8-length-prefixed (max 255 bytes each).
#
# ServerInfo Request:   [MessageType:2]
# ServerInfo Response:  [MessageType:2][nodeId:2][version:1+N][uptime:8]
#                       [role:1][groupCount:4][clientCount:4]
#
# Metrics Request:      [MessageType:2][flags:1]
# Metrics Response:     [MessageType:2][requestsTotal:8][requestsOK:8]
#                       [requestsErr:8][bytesIn:8][bytesOut:8]
#                       [kvGets:8][kvPuts:8][kvDeletes:8]
#                       [activeTxns:4][committedTxns:8][abortedTxns:8]
#
# Health Request:       [MessageType:2]
# Health Response:      [MessageType:2][status:1][leaderOK:1][replicaCount:2]
#                       [healthyReplicas:2][clusterName:1+N]

import ../types
import ../codec

# ---------------------------------------------------------------------------
# Server role constants
# ---------------------------------------------------------------------------

const
  RoleLeader* = 0x01'u8
  RoleFollower* = 0x02'u8
  RoleCandidate* = 0x03'u8
  RoleUnknown* = 0x00'u8

# ---------------------------------------------------------------------------
# Health status constants
# ---------------------------------------------------------------------------

const
  HealthOK* = 0x00'u8       ## All systems healthy
  HealthDegraded* = 0x01'u8 ## Cluster has minority of healthy replicas
  HealthCritical* = 0x02'u8 ## No quorum / leader unavailable

# ---------------------------------------------------------------------------
# Metrics request flags
# ---------------------------------------------------------------------------

const
  MetricsFlagReset* = 0x01'u8 ## Reset counters after reading

# ---------------------------------------------------------------------------
# ServerInfo (0x0700)
#
# Request:  no fields beyond MessageType
# Response: nodeId (2), version string (uint8-len prefixed), uptimeSecs (8),
#           role (1), groupCount (4), clientCount (4)
# ---------------------------------------------------------------------------

type
  ServerInfoRequest* = object
    discard

  ServerInfoResponse* = object
    nodeId*: uint16
    version*: string ## e.g. "1.0.0"
    uptimeSecs*: uint64
    role*: uint8     ## Role* constant
    groupCount*: uint32
    clientCount*: uint32

proc encodeServerInfoRequest*(): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtServerInfo))
  buf

proc decodeServerInfoRequest*(payload: string): Result[ServerInfoRequest,
    ProtocolError] =
  # Only the 2-byte MessageType prefix; nothing else to decode.
  let rb = checkBounds(payload, 0, 2)
  if rb.isErr: return peErr(rb.error)
  peOk(ServerInfoRequest())

proc encodeServerInfoResponse*(resp: ServerInfoResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtServerInfo))
  buf.writeUint16BE(resp.nodeId)
  buf.writeBytes8(resp.version)
  buf.writeUint64BE(resp.uptimeSecs)
  buf.writeUint8(resp.role)
  buf.writeUint32BE(resp.groupCount)
  buf.writeUint32BE(resp.clientCount)
  buf

proc decodeServerInfoResponse*(payload: string): Result[ServerInfoResponse,
    ProtocolError] =
  var pos = 2 # skip MessageType
  var resp: ServerInfoResponse

  let nodeIdR = readUint16BE(payload, pos)
  if nodeIdR.isErr: return peErr(nodeIdR.error)
  resp.nodeId = nodeIdR.value

  let verR = readBytes8(payload, pos)
  if verR.isErr: return peErr(verR.error)
  resp.version = verR.value

  let upR = readUint64BE(payload, pos)
  if upR.isErr: return peErr(upR.error)
  resp.uptimeSecs = upR.value

  let roleR = readUint8(payload, pos)
  if roleR.isErr: return peErr(roleR.error)
  resp.role = roleR.value

  let scR = readUint32BE(payload, pos)
  if scR.isErr: return peErr(scR.error)
  resp.groupCount = scR.value

  let ccR = readUint32BE(payload, pos)
  if ccR.isErr: return peErr(ccR.error)
  resp.clientCount = ccR.value

  peOk(resp)

# ---------------------------------------------------------------------------
# Metrics (0x0701)
#
# Request:  flags (1)
# Response: requestsTotal (8), requestsOK (8), requestsErr (8),
#           bytesIn (8), bytesOut (8),
#           kvGets (8), kvPuts (8), kvDeletes (8),
#           activeTxns (4), committedTxns (8), abortedTxns (8)
# ---------------------------------------------------------------------------

type
  MetricsRequest* = object
    flags*: uint8

  MetricsResponse* = object
    requestsTotal*: uint64
    requestsOK*: uint64
    requestsErr*: uint64
    bytesIn*: uint64
    bytesOut*: uint64
    kvGets*: uint64
    kvPuts*: uint64
    kvDeletes*: uint64
    activeTxns*: uint32
    committedTxns*: uint64
    abortedTxns*: uint64

proc encodeMetricsRequest*(req: MetricsRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtMetrics))
  buf.writeUint8(req.flags)
  buf

proc decodeMetricsRequest*(payload: string): Result[MetricsRequest,
    ProtocolError] =
  var pos = 2
  let flagsR = readUint8(payload, pos)
  if flagsR.isErr: return peErr(flagsR.error)
  peOk(MetricsRequest(flags: flagsR.value))

proc encodeMetricsResponse*(resp: MetricsResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtMetrics))
  buf.writeUint64BE(resp.requestsTotal)
  buf.writeUint64BE(resp.requestsOK)
  buf.writeUint64BE(resp.requestsErr)
  buf.writeUint64BE(resp.bytesIn)
  buf.writeUint64BE(resp.bytesOut)
  buf.writeUint64BE(resp.kvGets)
  buf.writeUint64BE(resp.kvPuts)
  buf.writeUint64BE(resp.kvDeletes)
  buf.writeUint32BE(resp.activeTxns)
  buf.writeUint64BE(resp.committedTxns)
  buf.writeUint64BE(resp.abortedTxns)
  buf

proc decodeMetricsResponse*(payload: string): Result[MetricsResponse,
    ProtocolError] =
  var pos = 2
  var resp: MetricsResponse

  template r64(field: untyped) =
    let r = readUint64BE(payload, pos)
    if r.isErr: return peErr(r.error)
    resp.field = r.value

  r64(requestsTotal)
  r64(requestsOK)
  r64(requestsErr)
  r64(bytesIn)
  r64(bytesOut)
  r64(kvGets)
  r64(kvPuts)
  r64(kvDeletes)

  let actR = readUint32BE(payload, pos)
  if actR.isErr: return peErr(actR.error)
  resp.activeTxns = actR.value

  r64(committedTxns)
  r64(abortedTxns)

  peOk(resp)

# ---------------------------------------------------------------------------
# Health (0x0702)
#
# Request:  no fields beyond MessageType
# Response: status (1), leaderOK (1), replicaCount (2), healthyReplicas (2),
#           clusterName (uint8-len prefixed string)
# ---------------------------------------------------------------------------

type
  HealthRequest* = object
    discard

  HealthResponse* = object
    status*: uint8 ## Health* constant
    leaderOK*: bool
    replicaCount*: uint16
    healthyReplicas*: uint16
    clusterName*: string

proc encodeHealthRequest*(): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtHealth))
  buf

proc decodeHealthRequest*(payload: string): Result[HealthRequest,
    ProtocolError] =
  let rb = checkBounds(payload, 0, 2)
  if rb.isErr: return peErr(rb.error)
  peOk(HealthRequest())

proc encodeHealthResponse*(resp: HealthResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtHealth))
  buf.writeUint8(resp.status)
  buf.writeUint8(if resp.leaderOK: 0x01'u8 else: 0x00'u8)
  buf.writeUint16BE(resp.replicaCount)
  buf.writeUint16BE(resp.healthyReplicas)
  buf.writeBytes8(resp.clusterName)
  buf

proc decodeHealthResponse*(payload: string): Result[HealthResponse,
    ProtocolError] =
  var pos = 2
  var resp: HealthResponse

  let statusR = readUint8(payload, pos)
  if statusR.isErr: return peErr(statusR.error)
  resp.status = statusR.value

  let leaderR = readUint8(payload, pos)
  if leaderR.isErr: return peErr(leaderR.error)
  resp.leaderOK = leaderR.value != 0

  let rcR = readUint16BE(payload, pos)
  if rcR.isErr: return peErr(rcR.error)
  resp.replicaCount = rcR.value

  let hrR = readUint16BE(payload, pos)
  if hrR.isErr: return peErr(hrR.error)
  resp.healthyReplicas = hrR.value

  let cnR = readBytes8(payload, pos)
  if cnR.isErr: return peErr(cnR.error)
  resp.clusterName = cnR.value

  peOk(resp)
