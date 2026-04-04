# Integration tests for Phase 4 Admin/Metrics Protocol.
#
# Covers:
#   - messages/admin: codec round-trips for ServerInfo, Metrics, Health
#   - server/client: end-to-end ServerInfo, Metrics, Health over TCP
#   - Metrics counters: KV ops increment request counters
#   - Metrics reset flag zeroes counters
#   - Health response reflects cluster name and healthy status
#   - ServerInfo reports version, role, and uptime
#
# Port allocation: 20000-20049

import std/[unittest, os]
import fractio/protocol/types
import fractio/protocol/server
import fractio/protocol/client
import fractio/protocol/messages/admin as adminMsgs
import fractio/protocol/mvcc_store
import fractio/protocol/txn_manager
import fractio/protocol/raft_store
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/raft/multigroup_types
import fractio/distributed/meta/system_tables
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

var testBasePort {.global.} = 20050

proc nextRaftPort(): int =
  result = testBasePort
  testBasePort += 10

proc startAdminServer(port: int, clusterName: string = "test-cluster",
    version: string = "1.0.0"): ProtocolServer =
  var cfg = defaultServerConfig()
  cfg.host = "127.0.0.1"
  cfg.port = port
  cfg.idleTimeoutSecs = 120
  cfg.clusterName = clusterName
  cfg.serverVersion = version
  cfg.serverName = "fractio-test"
  result = newProtocolServer(cfg)

  # Set up MVCC store for KV operations (requires single-node Raft)
  let storagePath = "/tmp/fractio_admin_test_" & $port
  try: removeDir(storagePath) except CatchableError: discard
  createDir(storagePath)

  let nodeId = rangeTypes.NodeID(1)
  let raftPort = nextRaftPort()
  let members = @[(nodeId: 1'u32, host: "127.0.0.1", port: raftPort)]

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    port: raftPort,
    host: "127.0.0.1",
    dataDir: storagePath,
    electionTimeoutLowerMs: 200,
    electionTimeoutUpperMs: 400,
    heartbeatIntervalMs: 100,
  ))
  coord.start()

  # Create meta + data groups
  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    discard coord.createAndStartGroup(gid, members)

  # Wait for leader election on both groups
  for attempt in 0 ..< 30:
    if coord.isLeader(META_GROUP_ID) and coord.isLeader(DATA_GROUP_START_ID):
      break
    sleep(100)

  let raftStore = newRaftKVStoreExt(coord, proposeTimeoutMs = 2000)
  raftStore.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  let txnMgr = newTransactionManager()
  let mockTimer = MockTimeProvider(currentTime: timerTypes.Timestamp(1_000_000_000))
  let tsProvider = newTimestampProvider(mockTimer, nodeId.uint16)
  let mvccStore = newMvccTransactionStore(raftStore, txnMgr, tsProvider)

  result.raftStore = raftStore
  result.raftCoord = coord
  result.mvccStore = mvccStore
  result.txnMgr = txnMgr

  result.start()
  sleep(100)

proc connectAdmin(port: int): ProtocolClient =
  var cfg = defaultClientConfig("127.0.0.1", port)
  cfg.timeoutMs = 10_000
  result = newProtocolClient(cfg)
  let r = result.connect()
  doAssert r.isOk, "connect failed: " & $r.err

proc withAdminServer(port: int, body: proc(srv: ProtocolServer,
    cli: ProtocolClient)) =
  let srv = startAdminServer(port)
  let cli = connectAdmin(port)
  try:
    body(srv, cli)
  finally:
    cli.disconnect()
    srv.stop()
    sleep(50)

# ---------------------------------------------------------------------------
# Suite: admin codec — ServerInfo round-trips
# ---------------------------------------------------------------------------

suite "admin codec - ServerInfo":
  test "encode and decode ServerInfoRequest":
    let payload = adminMsgs.encodeServerInfoRequest()
    let r = adminMsgs.decodeServerInfoRequest(payload)
    check r.isOk

  test "encode and decode ServerInfoResponse":
    let resp = adminMsgs.ServerInfoResponse(
      nodeId: 42,
      version: "1.2.3",
      uptimeSecs: 3600,
      role: adminMsgs.RoleLeader,
      shardCount: 4,
      clientCount: 7,
    )
    let payload = adminMsgs.encodeServerInfoResponse(resp)
    let r = adminMsgs.decodeServerInfoResponse(payload)
    check r.isOk
    check r.value.nodeId == 42
    check r.value.version == "1.2.3"
    check r.value.uptimeSecs == 3600
    check r.value.role == adminMsgs.RoleLeader
    check r.value.shardCount == 4
    check r.value.clientCount == 7

  test "ServerInfoResponse RoleFollower":
    let resp = adminMsgs.ServerInfoResponse(
      nodeId: 1,
      version: "2.0.0",
      uptimeSecs: 0,
      role: adminMsgs.RoleFollower,
      shardCount: 1,
      clientCount: 0,
    )
    let payload = adminMsgs.encodeServerInfoResponse(resp)
    let r = adminMsgs.decodeServerInfoResponse(payload)
    check r.isOk
    check r.value.role == adminMsgs.RoleFollower

  test "ServerInfoResponse empty version string":
    let resp = adminMsgs.ServerInfoResponse(
      nodeId: 0,
      version: "",
      uptimeSecs: 1,
      role: adminMsgs.RoleUnknown,
      shardCount: 0,
      clientCount: 0,
    )
    let payload = adminMsgs.encodeServerInfoResponse(resp)
    let r = adminMsgs.decodeServerInfoResponse(payload)
    check r.isOk
    check r.value.version == ""

  test "ServerInfoResponse truncated payload returns error":
    let payload = adminMsgs.encodeServerInfoRequest() # too short for a response
    let r = adminMsgs.decodeServerInfoResponse(payload)
    check r.isErr

# ---------------------------------------------------------------------------
# Suite: admin codec — Metrics round-trips
# ---------------------------------------------------------------------------

suite "admin codec - Metrics":
  test "encode and decode MetricsRequest no flags":
    let req = adminMsgs.MetricsRequest(flags: 0)
    let payload = adminMsgs.encodeMetricsRequest(req)
    let r = adminMsgs.decodeMetricsRequest(payload)
    check r.isOk
    check r.value.flags == 0

  test "encode and decode MetricsRequest reset flag":
    let req = adminMsgs.MetricsRequest(flags: adminMsgs.MetricsFlagReset)
    let payload = adminMsgs.encodeMetricsRequest(req)
    let r = adminMsgs.decodeMetricsRequest(payload)
    check r.isOk
    check (r.value.flags and adminMsgs.MetricsFlagReset) != 0

  test "encode and decode MetricsResponse all fields":
    let resp = adminMsgs.MetricsResponse(
      requestsTotal: 1000,
      requestsOK: 990,
      requestsErr: 10,
      bytesIn: 512000,
      bytesOut: 256000,
      kvGets: 500,
      kvPuts: 300,
      kvDeletes: 100,
      activeTxns: 5,
      committedTxns: 80,
      abortedTxns: 20,
    )
    let payload = adminMsgs.encodeMetricsResponse(resp)
    let r = adminMsgs.decodeMetricsResponse(payload)
    check r.isOk
    check r.value.requestsTotal == 1000
    check r.value.requestsOK == 990
    check r.value.requestsErr == 10
    check r.value.bytesIn == 512000
    check r.value.bytesOut == 256000
    check r.value.kvGets == 500
    check r.value.kvPuts == 300
    check r.value.kvDeletes == 100
    check r.value.activeTxns == 5
    check r.value.committedTxns == 80
    check r.value.abortedTxns == 20

  test "MetricsResponse zero values":
    let resp = adminMsgs.MetricsResponse()
    let payload = adminMsgs.encodeMetricsResponse(resp)
    let r = adminMsgs.decodeMetricsResponse(payload)
    check r.isOk
    check r.value.requestsTotal == 0
    check r.value.kvGets == 0

  test "MetricsResponse max uint64 values":
    let resp = adminMsgs.MetricsResponse(
      requestsTotal: high(uint64),
      requestsOK: high(uint64),
      requestsErr: 0,
      bytesIn: high(uint64),
      bytesOut: 0,
      kvGets: high(uint64),
      kvPuts: 0,
      kvDeletes: 0,
      activeTxns: 0,
      committedTxns: 0,
      abortedTxns: 0,
    )
    let payload = adminMsgs.encodeMetricsResponse(resp)
    let r = adminMsgs.decodeMetricsResponse(payload)
    check r.isOk
    check r.value.requestsTotal == high(uint64)
    check r.value.kvGets == high(uint64)

# ---------------------------------------------------------------------------
# Suite: admin codec — Health round-trips
# ---------------------------------------------------------------------------

suite "admin codec - Health":
  test "encode and decode HealthRequest":
    let payload = adminMsgs.encodeHealthRequest()
    let r = adminMsgs.decodeHealthRequest(payload)
    check r.isOk

  test "encode and decode HealthResponse OK":
    let resp = adminMsgs.HealthResponse(
      status: adminMsgs.HealthOK,
      leaderOK: true,
      replicaCount: 3,
      healthyReplicas: 3,
      clusterName: "prod-cluster",
    )
    let payload = adminMsgs.encodeHealthResponse(resp)
    let r = adminMsgs.decodeHealthResponse(payload)
    check r.isOk
    check r.value.status == adminMsgs.HealthOK
    check r.value.leaderOK == true
    check r.value.replicaCount == 3
    check r.value.healthyReplicas == 3
    check r.value.clusterName == "prod-cluster"

  test "HealthResponse Degraded leaderOK false":
    let resp = adminMsgs.HealthResponse(
      status: adminMsgs.HealthDegraded,
      leaderOK: false,
      replicaCount: 3,
      healthyReplicas: 1,
      clusterName: "test",
    )
    let payload = adminMsgs.encodeHealthResponse(resp)
    let r = adminMsgs.decodeHealthResponse(payload)
    check r.isOk
    check r.value.status == adminMsgs.HealthDegraded
    check r.value.leaderOK == false
    check r.value.healthyReplicas == 1

  test "HealthResponse Critical status":
    let resp = adminMsgs.HealthResponse(
      status: adminMsgs.HealthCritical,
      leaderOK: false,
      replicaCount: 3,
      healthyReplicas: 0,
      clusterName: "down",
    )
    let payload = adminMsgs.encodeHealthResponse(resp)
    let r = adminMsgs.decodeHealthResponse(payload)
    check r.isOk
    check r.value.status == adminMsgs.HealthCritical
    check r.value.healthyReplicas == 0

  test "HealthResponse empty cluster name":
    let resp = adminMsgs.HealthResponse(
      status: adminMsgs.HealthOK,
      leaderOK: true,
      replicaCount: 1,
      healthyReplicas: 1,
      clusterName: "",
    )
    let payload = adminMsgs.encodeHealthResponse(resp)
    let r = adminMsgs.decodeHealthResponse(payload)
    check r.isOk
    check r.value.clusterName == ""

# ---------------------------------------------------------------------------
# Suite: end-to-end — ServerInfo over TCP
# ---------------------------------------------------------------------------

suite "admin e2e - ServerInfo":
  test "serverInfo returns correct version and role":
    withAdminServer(20000) do (srv: ProtocolServer, cli: ProtocolClient):
      let r = cli.serverInfo()
      check r.isOk
      check r.value.version == "1.0.0"
      check r.value.role == adminMsgs.RoleLeader
      check r.value.nodeId == srv.config.serverId

  test "serverInfo uptimeSecs is non-negative":
    withAdminServer(20001) do (srv: ProtocolServer, cli: ProtocolClient):
      sleep(10) # ensure at least a few ms
      let r = cli.serverInfo()
      check r.isOk
      # uptimeSecs is uint64; just check it doesn't overflow/go huge
      check r.value.uptimeSecs < 86400 # less than 1 day (test just started)

  test "serverInfo clientCount is at least 1":
    withAdminServer(20002) do (srv: ProtocolServer, cli: ProtocolClient):
      let r = cli.serverInfo()
      check r.isOk
      check r.value.clientCount >= 1

  test "serverInfo shardCount is 2 with meta and data groups":
    withAdminServer(20003) do (srv: ProtocolServer, cli: ProtocolClient):
      let r = cli.serverInfo()
      check r.isOk
      check r.value.shardCount == 2 # META_GROUP_ID + DATA_GROUP_START_ID

  test "serverInfo custom version string":
    var cfg = defaultServerConfig()
    cfg.host = "127.0.0.1"
    cfg.port = 20004
    cfg.idleTimeoutSecs = 120
    cfg.serverVersion = "3.7.1-rc2"
    let srv = newProtocolServer(cfg)
    srv.start()
    sleep(60)
    let cli = connectAdmin(20004)
    try:
      let r = cli.serverInfo()
      check r.isOk
      check r.value.version == "3.7.1-rc2"
    finally:
      cli.disconnect()
      srv.stop()
      sleep(50)

# ---------------------------------------------------------------------------
# Suite: end-to-end — Metrics over TCP
# ---------------------------------------------------------------------------

suite "admin e2e - Metrics":
  test "metrics returns zero counters on fresh server":
    withAdminServer(20010) do (srv: ProtocolServer, cli: ProtocolClient):
      # The first request (metrics itself) will have incremented requestsTotal by 1
      # at the time we read, but counters were 0 before any ops
      let r = cli.metrics()
      check r.isOk
      # At least no error
      check r.value.requestsTotal >= 1

  test "kvPuts increments kvPuts counter":
    withAdminServer(20011) do (srv: ProtocolServer, cli: ProtocolClient):
      # Do 3 puts
      for i in 0..2:
        let pr = cli.kvPut("key" & $i, "val" & $i)
        check pr.isOk
      let r = cli.metrics()
      check r.isOk
      check r.value.kvPuts >= 3

  test "kvGets increments kvGets counter":
    withAdminServer(20012) do (srv: ProtocolServer, cli: ProtocolClient):
      discard cli.kvPut("kk", "vv")
      for i in 0..4:
        let gr = cli.kvGet("kk")
        check gr.isOk
      let r = cli.metrics()
      check r.isOk
      check r.value.kvGets >= 5

  test "kvDeletes increments kvDeletes counter":
    withAdminServer(20013) do (srv: ProtocolServer, cli: ProtocolClient):
      discard cli.kvPut("dk", "dv")
      discard cli.kvDelete("dk")
      let r = cli.metrics()
      check r.isOk
      check r.value.kvDeletes >= 1

  test "requestsOK increments with each successful request":
    withAdminServer(20014) do (srv: ProtocolServer, cli: ProtocolClient):
      discard cli.ping()
      discard cli.ping()
      let r = cli.metrics()
      check r.isOk
      # 2 pings are already counted; the metrics request itself may or may
      # not be counted depending on ordering, so just require >= 2.
      check r.value.requestsOK >= 2

  test "metrics reset flag zeroes counters":
    withAdminServer(20015) do (srv: ProtocolServer, cli: ProtocolClient):
      # Generate some traffic
      discard cli.kvPut("reset-key", "value")
      discard cli.kvGet("reset-key")
      # Read with reset
      let r1 = cli.metrics(adminMsgs.MetricsFlagReset)
      check r1.isOk
      check r1.value.kvPuts >= 1
      check r1.value.kvGets >= 1
      # Now counters should be reset (next call sees near-zero)
      let r2 = cli.metrics()
      check r2.isOk
      check r2.value.kvPuts == 0
      check r2.value.kvGets == 0

  test "metrics activeTxns reflects open transactions":
    withAdminServer(20016) do (srv: ProtocolServer, cli: ProtocolClient):
      let txR = cli.beginTxn()
      check txR.isOk
      let r = cli.metrics()
      check r.isOk
      check r.value.activeTxns >= 1
      discard cli.rollbackTxn(txR.value.txnId)

  test "committedTxns increments on commit":
    withAdminServer(20017) do (srv: ProtocolServer, cli: ProtocolClient):
      let txR = cli.beginTxn()
      check txR.isOk
      discard cli.kvPut("tk", "tv", txnId = txR.value.txnId)
      let cR = cli.commitTxn(txR.value.txnId)
      check cR.isOk
      let r = cli.metrics()
      check r.isOk
      check r.value.committedTxns >= 1

# ---------------------------------------------------------------------------
# Suite: end-to-end — Health over TCP
# ---------------------------------------------------------------------------

suite "admin e2e - Health":
  test "health returns HealthOK on fresh server":
    withAdminServer(20020) do (srv: ProtocolServer, cli: ProtocolClient):
      let r = cli.health()
      check r.isOk
      check r.value.status == adminMsgs.HealthOK

  test "health leaderOK is true":
    withAdminServer(20021) do (srv: ProtocolServer, cli: ProtocolClient):
      let r = cli.health()
      check r.isOk
      check r.value.leaderOK == true

  test "health replicaCount is 1 in Phase 4":
    withAdminServer(20022) do (srv: ProtocolServer, cli: ProtocolClient):
      let r = cli.health()
      check r.isOk
      check r.value.replicaCount == 1
      check r.value.healthyReplicas == 1

  test "health returns configured cluster name":
    var cfg = defaultServerConfig()
    cfg.host = "127.0.0.1"
    cfg.port = 20023
    cfg.idleTimeoutSecs = 120
    cfg.clusterName = "my-special-cluster"
    let srv = newProtocolServer(cfg)
    srv.start()
    sleep(60)
    let cli = connectAdmin(20023)
    try:
      let r = cli.health()
      check r.isOk
      check r.value.clusterName == "my-special-cluster"
    finally:
      cli.disconnect()
      srv.stop()
      sleep(50)

  test "repeated health calls all return HealthOK":
    withAdminServer(20024) do (srv: ProtocolServer, cli: ProtocolClient):
      for _ in 0..4:
        let r = cli.health()
        check r.isOk
        check r.value.status == adminMsgs.HealthOK
