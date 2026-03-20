# Integration test — spaces on a 5-node cluster.
#
# Verifies that CREATE SPACE creates real Raft groups that can store data,
# and that the space continues to function after adding and killing nodes.
#
# Cluster topology:
#   Nodes 1–5, each with its own NuRaftCoordinator + ASIO networking.
#   NuRaft handles leader election automatically.
#   A space with REPLICAS 3 is created via executeSQL on the leader.
#
# After CREATE SPACE, the onGroupMetadataApplied callback automatically
# creates space groups on peer nodes as sys.groups entries replicate via
# Raft. Tests wait for replication and then use transferLeadership for
# determinism.
#
# Port allocation: 27000–27499 (NuRaft ASIO, basePort per node spaced by 100)
# Temp storage: /tmp/fractio_space_mn_<nodeId>/ (cleaned up per test)

import std/[unittest, os, options, json, strutils, tables]
import fractio/client/fractio_client
import fractio/client/sql_client

import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/protocol/raft_store
import fractio/protocol/mvcc_store
import fractio/protocol/txn_manager
import fractio/protocol/server
import fractio/protocol/types
import fractio/storage/wisckey_backend
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider
import fractio/sql/executor
import fractio/storage/mvcc/types as mvccTypes

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  TMP_DIR = "/tmp/fractio_space_mn_"


var testBasePort {.global.} = 28000

proc nextBasePort(): int =
  result = testBasePort
  testBasePort += 100

var nextClientPort = 19200 ## incremented per node to avoid port conflicts between tests

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

type
  TestNode = object
    id*: int ## 1-based node number
    basePort*: int
    clientPort*: int
    server*: ProtocolServer
    coord*: NURAFT_COORDINATOR
    store*: RaftKVStoreExt
    mvccStore*: MvccTransactionStore
    storagePath*: string
    client*: FractioClient

proc cleanDir(p: string) =
  try: removeDir(p) except CatchableError: discard

proc makeNode(nodeNum: int, basePort: int,
    members: seq[tuple[nodeId: uint32, host: string,
        basePort: int]]): TestNode =
  let nodeId = rangeTypes.NodeID(uint32(nodeNum))
  let cPort = nextClientPort
  nextClientPort += 1

  # Isolate LevelDB storage per instance to avoid LOCK contention
  let storagePath = TMP_DIR & $nodeNum & "_" & $cPort
  cleanDir(storagePath)
  createDir(storagePath)

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    basePort: basePort,
    host: "127.0.0.1",
    dataDir: storagePath,
    electionTimeoutLowerMs: 200,
    electionTimeoutUpperMs: 400,
    heartbeatIntervalMs: 100,
  ))

  for m in members:
    coord.peerInfo[m.nodeId] = (host: m.host, basePort: m.basePort)

  coord.start()

  # Create meta + data groups
  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    var success = false
    for attempt in 0 ..< 5:
      if coord.createAndStartGroup(gid, members):
        success = true
        break
      sleep(200)
    doAssert success, "failed to create group " & $gid.uint64

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 6000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  # Create MVCC store for DDL operations
  let txnMgr = newTransactionManager()
  let mockTimer = MockTimeProvider(currentTime: timerTypes.Timestamp(1_000_000_000))
  let tsProvider = newTimestampProvider(mockTimer, nodeId.uint16)
  let mvccStore = newMvccTransactionStore(store, txnMgr, tsProvider)

  var cfg = defaultServerConfig()
  cfg.host = "127.0.0.1"
  cfg.port = cPort
  cfg.serverId = uint16(nodeNum)
  cfg.dataDir = storagePath
  let srv = newProtocolServer(cfg)
  srv.raftStore = store
  srv.raftCoord = coord
  srv.mvccStore = mvccStore
  srv.txnMgr = txnMgr

  TestNode(
    id: nodeNum, basePort: basePort, clientPort: cPort, server: srv,
    coord: coord, store: store, mvccStore: mvccStore, storagePath: storagePath,
  )

proc initClient(n: var TestNode, leaderPort: int) =
  n.client = newFractioClient("127.0.0.1", leaderPort)
  doAssert n.client.initialize()

proc startNode(n: var TestNode) =
  n.server.start()

proc stopNode(n: TestNode) =
  if not n.client.isNil: n.client.close()
  n.server.stop()
  n.coord.stop()
  cleanDir(n.storagePath)

proc waitForLeaderOnGroup(nodes: seq[TestNode], gid: GroupID,
    maxAttempts: int = 50): int =
  ## Wait for a leader to be elected for a group. Returns leader node index or -1.
  for attempt in 0 ..< maxAttempts:
    for i, n in nodes:
      if n.coord.isLeader(gid):
        return i
    sleep(100)
  -1

proc seedSysNodes(leaderStore: RaftKVStoreExt, nodes: seq[TestNode]) =
  for n in nodes:
    let key = encodeTableKey(SYS_NODES_TABLE_ID, $n.id)
    let nodeRec = NodeRecord(
      nodeId: uint32(n.id),
      host: "127.0.0.1",
      raftPort: uint16(n.basePort),
      clientPort: uint16(n.clientPort),
      status: nsAlive,
    )
    let r = leaderStore.sysTablePut(key, nodeRec.encode())
    doAssert r, "failed to seed sys.nodes for node " & $n.id

proc seedSysGroups(leaderStore: RaftKVStoreExt, nodeNums: seq[int]) =
  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid.uint64)
    var replicasSeq: seq[GroupReplicaBin] = @[]
    for num in nodeNums:
      replicasSeq.add(GroupReplicaBin(nodeId: uint32(num),
          replicaType: rtVoter))
    let groupRec = GroupRecord(
      groupId: gid.uint64,
      spaceId: if gid == META_GROUP_ID: 0 else: 1,
      leader: uint32(nodeNums[0]),
      replicas: replicasSeq,
    )
    discard leaderStore.sysTablePut(key, groupRec.encode())

proc seedDefaults(leaderStore: RaftKVStoreExt) =
  let dbKey = encodeTableKey(SYS_DATABASES_TABLE_ID, "default")
  discard leaderStore.sysTablePut(dbKey, DatabaseRecord(
    name: "default",
    createdAtNs: system_schemas.nowNs()
  ).encode())
  let scKey = encodeTableKey(SYS_SCHEMAS_TABLE_ID, "default.public")
  discard leaderStore.sysTablePut(scKey, SchemaRecord(
    name: "public",
    database: "default",
    createdAtNs: system_schemas.nowNs()
  ).encode())

proc waitForAutoDistribution(nodes: seq[TestNode], expectedGroupIds: seq[
    uint64], replicaCount: int, maxWaitMs: int = 5000) =
  ## Wait for the onGroupMetadataApplied callback to create space groups on
  ## all peer nodes. Polls until the expected total membership count is reached
  ## or the timeout expires.
  let expectedTotal = expectedGroupIds.len * replicaCount
  let stepMs = 50
  var waited = 0
  while waited < maxWaitMs:
    var totalMemberships = 0
    for node in nodes:
      for gid in expectedGroupIds:
        if node.coord.hasGroup(GroupID(gid)):
          inc totalMemberships
    if totalMemberships >= expectedTotal:
      break
    sleep(stepMs)
    waited += stepMs

proc waitForSpaceLeaders(nodes: seq[TestNode]) =
  ## Wait for all space groups to have elected leaders.
  let store = nodes[0].store
  let grpStart = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
  let grpEnd = encodeTableKey(SYS_GROUPS_TABLE_ID + 1, "")
  let grpScan = store.raftScan(grpStart, grpEnd, 0, includeSystemKeys = true)
  if grpScan.isOk:
    for (key, entry) in grpScan.value:
      try:
        var data = entry.value
        # Check if MVCC-encoded
        if mvccTypes.isLikelyMVCCValue(data):
          try:
            let mvccVal = mvccTypes.decodeMVCCValue(data)
            if not mvccVal.isDeleted:
              data = mvccVal.data
            else:
              continue
          except CatchableError:
            discard
        let gid = if data.len > 0 and data[0] != '{':
          # Binary format
          let rec = decodeGroupRecord(data)
          GroupID(rec.groupId)
        else:
          # Legacy JSON format
          let j = parseJson(data)
          GroupID(uint64(j["groupId"].getInt()))
        if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID: continue
        # Wait for this group to have a leader
        for attempt in 0 ..< 50:
          var hasLeader = false
          for node in nodes:
            if node.coord.isLeader(gid):
              hasLeader = true
              break
          if hasLeader: break
          sleep(100)
      except: discard

proc distributeSpaceGroups(nodes: seq[TestNode], replicaCount: int = 3) =
  ## After CREATE SPACE on the leader: wait for the onGroupMetadataApplied
  ## callback to create space groups on peer nodes, then wait for leaders.
  waitForAutoDistribution(nodes, @[3'u64, 4, 5, 6, 7], replicaCount)
  waitForSpaceLeaders(nodes)

proc reelectLeaders(nodes: seq[TestNode], deadNodeIds: seq[int]) =
  ## After killing nodes, wait for NuRaft to re-elect leaders on surviving nodes.
  ## NuRaft handles this automatically, but we may need to wait for timeouts.
  sleep(1000) # Allow NuRaft election timeouts to fire

proc exec(node: TestNode, sql: string): ExecResult =
  node.client.query(sql)

proc loadMetadataOnAllNodes(nodes: seq[TestNode]) =
  ## Load space, table, and group membership metadata on all nodes.
  let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
  if leaderIdx < 0: return
  let leader = nodes[leaderIdx].store
  let leaderBackend = leader.getBackend()
  for sysTableId in [SYS_TABLES_TABLE_ID, SYS_SPACES_TABLE_ID,
                      SYS_GROUPS_TABLE_ID, SYS_NODES_TABLE_ID]:
    let startKey = encodeTableKey(sysTableId, "")
    let endKey = encodeTableKey(sysTableId + 1, "")
    let pairs = leaderBackend.scan(startKey, endKey)
    for (k, v) in pairs:
      for i in 0 ..< nodes.len:
        if i == leaderIdx: continue
        let peerBackend = nodes[i].store.getBackend()
        if peerBackend != nil and peerBackend.isOpen:
          discard peerBackend.put(k, v)
  for node in nodes:
    node.store.loadSpaces()
    node.store.loadGroupMembers()
    node.store.loadTableSpaces()

proc execOnLeader(nodes: seq[TestNode], sql: string): ExecResult =
  ## Try executing SQL on each node until one succeeds.
  for node in nodes:
    let r = exec(node, sql)
    if r.kind != erkError:
      return r
    if isNotLeaderError(r.error):
      continue
    return r
  exec(nodes[^1], sql)

# ---------------------------------------------------------------------------
# Cluster fixture: 5 nodes
# ---------------------------------------------------------------------------

proc makeCluster5(): seq[TestNode] =
  let p1 = nextBasePort()
  let p2 = nextBasePort()
  let p3 = nextBasePort()
  let p4 = nextBasePort()
  let p5 = nextBasePort()
  let members = @[
    (nodeId: 1'u32, host: "127.0.0.1", basePort: p1),
    (nodeId: 2'u32, host: "127.0.0.1", basePort: p2),
    (nodeId: 3'u32, host: "127.0.0.1", basePort: p3),
    (nodeId: 4'u32, host: "127.0.0.1", basePort: p4),
    (nodeId: 5'u32, host: "127.0.0.1", basePort: p5),
  ]

  var nodes: seq[TestNode]
  for i, m in members:
    nodes.add(makeNode(int(m.nodeId), m.basePort, members))

  for i in 0 ..< nodes.len: startNode(nodes[i])

  # Wait for leader election on meta + data groups
  discard waitForLeaderOnGroup(nodes, META_GROUP_ID)
  discard waitForLeaderOnGroup(nodes, DATA_GROUP_START_ID)

  # Find the meta leader and seed system tables
  let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
  doAssert leaderIdx >= 0

  let allNums = @[1, 2, 3, 4, 5]
  seedSysNodes(nodes[leaderIdx].store, nodes)
  seedSysGroups(nodes[leaderIdx].store, allNums)
  seedDefaults(nodes[leaderIdx].store)
  sleep(400)
  for i in 0 ..< nodes.len: initClient(nodes[i], nodes[leaderIdx].clientPort)

  nodes

proc stopCluster(nodes: seq[TestNode]) =
  for i in countdown(nodes.high, 0): stopNode(nodes[i])

# ---------------------------------------------------------------------------
# Test suites
# ---------------------------------------------------------------------------

suite "Space multinode — CREATE SPACE creates real Raft groups":

  test "CREATE SPACE succeeds and groups exist in coordinator":
    var nodes = makeCluster5()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let res = exec(nodes[leaderIdx],
        "CREATE SPACE testspace WITH REPLICAS = 3")
    if res.kind == erkError:
      echo "  CREATE SPACE error: " & res.error
    check res.kind == erkOk
    if res.kind == erkOk:
      check "5 groups" in res.okMessage

    # With RF=3, the leader (node 1) has groups it's a member of.
    var leaderGroupCount = 0
    for gid in 3'u64 .. 7'u64:
      if nodes[leaderIdx].coord.hasGroup(GroupID(gid)):
        inc leaderGroupCount
    check leaderGroupCount >= 3

  test "onGroupMetadataApplied creates groups on all member nodes":
    var nodes = makeCluster5()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    discard exec(nodes[leaderIdx],
        "CREATE SPACE testspace WITH REPLICAS = 3")

    waitForAutoDistribution(nodes, @[3'u64, 4, 5, 6, 7], 3)

    var totalMemberships = 0
    for i in 0 ..< 5:
      for gid in 3'u64 .. 7'u64:
        if nodes[i].coord.hasGroup(GroupID(gid)):
          inc totalMemberships
    check totalMemberships == 15

  test "CREATE TABLE IN SPACE succeeds":
    var nodes = makeCluster5()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    discard exec(nodes[leaderIdx],
        "CREATE SPACE testspace WITH REPLICAS = 3")
    distributeSpaceGroups(nodes)
    nodes[leaderIdx].store.loadSpaces()
    nodes[leaderIdx].store.loadGroupMembers()

    let ctRes = exec(nodes[leaderIdx],
        "CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) IN SPACE testspace")
    check ctRes.kind == erkOk

suite "Space multinode — data operations through space groups":

  test "INSERT and SELECT in space-bound table":
    var nodes = makeCluster5()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    discard exec(nodes[leaderIdx],
        "CREATE SPACE testspace WITH REPLICAS = 3")
    distributeSpaceGroups(nodes)

    discard exec(nodes[leaderIdx],
        "CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) IN SPACE testspace")
    loadMetadataOnAllNodes(nodes)

    let ins1 = execOnLeader(nodes, "INSERT INTO t1 VALUES (1, 'alice')")
    if ins1.kind == erkError:
      echo "  INSERT 1 error: " & ins1.error
    check ins1.kind == erkModified
    if ins1.kind == erkModified:
      check ins1.count == 1

    let ins2 = execOnLeader(nodes, "INSERT INTO t1 VALUES (2, 'bob')")
    if ins2.kind == erkError:
      echo "  INSERT 2 error: " & ins2.error
    check ins2.kind == erkModified

    let ins3 = execOnLeader(nodes, "INSERT INTO t1 VALUES (3, 'carol')")
    if ins3.kind == erkError:
      echo "  INSERT 3 error: " & ins3.error
    check ins3.kind == erkModified

    let sel = exec(nodes[leaderIdx],
        "SELECT * FROM t1")
    check sel.kind == erkRows
    if sel.kind == erkRows:
      check sel.rows.len == 3

  test "multiple inserts and point lookups":
    var nodes = makeCluster5()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    discard exec(nodes[leaderIdx],
        "CREATE SPACE myspace WITH REPLICAS = 3")
    distributeSpaceGroups(nodes)

    discard exec(nodes[leaderIdx],
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT) IN SPACE myspace")
    loadMetadataOnAllNodes(nodes)

    for i in 1 .. 10:
      let r = execOnLeader(nodes,
          "INSERT INTO users VALUES (" & $i & ", 'user" & $i & "@test.com')")
      if r.kind == erkError:
        echo "  INSERT " & $i & " error: " & r.error
      check r.kind == erkModified

    let sel = execOnLeader(nodes, "SELECT * FROM users WHERE id = 5")
    check sel.kind == erkRows
    if sel.kind == erkRows:
      check sel.rows.len == 1
      if sel.rows.len > 0:
        check sel.rows[0][1] == "user5@test.com"

    let all = exec(nodes[leaderIdx],
        "SELECT * FROM users")
    check all.kind == erkRows
    if all.kind == erkRows:
      check all.rows.len == 10

suite "Space multinode — resilience after adding a node":

  test "space works after adding a 6th node":
    var nodes = makeCluster5()
    defer:
      for n in nodes: stopNode(n)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    discard exec(nodes[leaderIdx],
        "CREATE SPACE testspace WITH REPLICAS = 3")
    distributeSpaceGroups(nodes)

    discard exec(nodes[leaderIdx],
        "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT) IN SPACE testspace")
    loadMetadataOnAllNodes(nodes)

    let ins1 = execOnLeader(nodes, "INSERT INTO t1 VALUES (1, 'before-add')")
    check ins1.kind == erkModified

    # Add node 6
    let p6 = nextBasePort()
    let node6Members = @[
      (nodeId: 1'u32, host: "127.0.0.1", basePort: nodes[0].basePort),
      (nodeId: 2'u32, host: "127.0.0.1", basePort: nodes[1].basePort),
      (nodeId: 3'u32, host: "127.0.0.1", basePort: nodes[2].basePort),
      (nodeId: 4'u32, host: "127.0.0.1", basePort: nodes[3].basePort),
      (nodeId: 5'u32, host: "127.0.0.1", basePort: nodes[4].basePort),
      (nodeId: 6'u32, host: "127.0.0.1", basePort: p6),
    ]
    var node6 = makeNode(6, p6, node6Members)
    startNode(node6)
    nodes.add(node6)
    let metaLeader = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    initClient(nodes[^1], nodes[metaLeader].clientPort)

    # Register node 6 with existing nodes' NuRaft groups
    for i in 0 ..< 5:
      nodes[i].server.addPeerToRaft(6, "127.0.0.1", p6)

    # Seed node 6 into sys.nodes
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, "6")
    let nodeRec = NodeRecord(
      nodeId: 6'u32,
      host: "127.0.0.1",
      raftPort: 32000'u16,
      clientPort: uint16(node6.clientPort),
      status: nsAlive,
    )
    discard nodes[leaderIdx].store.sysTablePut(nodeKey, nodeRec.encode())
    sleep(500)

    # Verify space still works — insert via client-side retry
    let ins2 = execOnLeader(nodes, "INSERT INTO t1 VALUES (2, 'after-add')")
    if ins2.kind == erkError:
      echo "  INSERT after add error: " & ins2.error
    check ins2.kind == erkModified

    let sel = exec(nodes[leaderIdx],
        "SELECT * FROM t1")
    check sel.kind == erkRows
    if sel.kind == erkRows:
      check sel.rows.len == 2

suite "Space multinode — resilience after killing a node":

  test "space works after killing a non-leader node":
    var nodes = makeCluster5()
    defer:
      for n in nodes:
        try: stopNode(n)
        except: discard

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    discard exec(nodes[leaderIdx],
        "CREATE SPACE testspace WITH REPLICAS = 3")
    distributeSpaceGroups(nodes)

    discard exec(nodes[leaderIdx],
        "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT) IN SPACE testspace")
    loadMetadataOnAllNodes(nodes)

    let ins1 = execOnLeader(nodes, "INSERT INTO t1 VALUES (1, 'before-kill')")
    check ins1.kind == erkModified

    sleep(400)

    # Kill node 5 (a non-leader follower)
    nodes[4].coord.stop()
    sleep(300)

    # NuRaft handles re-election automatically
    reelectLeaders(nodes, @[5])

    let aliveNodes = nodes[0 ..< 4]
    var postKillSuccess = 0
    let ins2 = execOnLeader(aliveNodes, "INSERT INTO t1 VALUES (2, 'after-kill')")
    if ins2.kind == erkModified: inc postKillSuccess
    let ins3 = execOnLeader(aliveNodes, "INSERT INTO t1 VALUES (3, 'also-after-kill')")
    if ins3.kind == erkModified: inc postKillSuccess

    check postKillSuccess >= 1

    let sel = exec(nodes[leaderIdx],
        "SELECT * FROM t1")
    check sel.kind == erkRows
    if sel.kind == erkRows:
      check sel.rows.len >= postKillSuccess

  test "space works after killing two non-leader nodes (minority failure)":
    var nodes = makeCluster5()
    defer:
      for n in nodes:
        try: stopNode(n)
        except: discard

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    discard exec(nodes[leaderIdx],
        "CREATE SPACE testspace WITH REPLICAS = 3")
    distributeSpaceGroups(nodes)

    discard exec(nodes[leaderIdx],
        "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT) IN SPACE testspace")
    loadMetadataOnAllNodes(nodes)

    let ins1 = execOnLeader(nodes, "INSERT INTO t1 VALUES (1, 'initial')")
    check ins1.kind == erkModified

    sleep(400)

    # Kill nodes 4 and 5
    nodes[3].coord.stop()
    nodes[4].coord.stop()
    sleep(300)

    # NuRaft handles re-election automatically
    reelectLeaders(nodes, @[4, 5])

    let aliveNodes = nodes[0 ..< 3]
    var successCount = 0
    for i in 2 .. 6:
      let r = execOnLeader(aliveNodes,
          "INSERT INTO t1 VALUES (" & $i & ", 'post-kill-" & $i & "')")
      if r.kind == erkModified:
        inc successCount

    check successCount > 0

    let sel = exec(nodes[leaderIdx],
        "SELECT * FROM t1")
    check sel.kind == erkRows
    if sel.kind == erkRows:
      check sel.rows.len >= successCount
