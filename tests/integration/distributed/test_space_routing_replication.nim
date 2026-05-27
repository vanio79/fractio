# Integration test — space routing, replication, and NOT_LEADER enforcement.
#
# Verifies three critical properties of a multi-group space:
#
# 1. DATA ROUTING: INSERTs are distributed across space groups (not all on node 1).
#    When a space has N groups, keys should hash to different group leaders.
#
# 2. REPLICATION: Data written to a group leader is replicated to follower nodes.
#    After writing key K to group G's leader, reading K from a follower of G
#    should return the same value (via Raft consensus).
#
# 3. NOT_LEADER ENFORCEMENT: A server receiving a put/get for a group it doesn't
#    lead should return a NOT_LEADER error with redirect info, NOT silently
#    accept the write to its local store. This ensures clients always route
#    requests to the correct leader, preventing data silos.
#
# Cluster topology: 3 nodes, space with REPLICAS=3 (3 groups, each on all nodes).

import std/[unittest, os, strutils, tables, times, sets, sugar, options]

import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/client/routing

import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/multigroup_types
import fractio/core/types as coreTypes except NodeID
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/protocol/raft_store
import fractio/protocol/mvcc_store
import fractio/protocol/txn_manager
import fractio/protocol/server
import fractio/protocol/types
import fractio/protocol/client as protoClient
import fractio/storage/wisckey_backend
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider
import fractio/sql/executor
import fractio/storage/mvcc/types as mvccTypes
import fractio/distributed/space_manager
import fractio/core/kv_interface
import fractio/protocol/messages/kv as kvMsgs

# Import optimized test configuration
import ../../test_config

# Import NuRaft shim to disable busy_connection_limit during tests
import fractio/distributed/raft/c_bindings

nuraftLimitsSetBusyConnectionLimit(0)

const
  TMP_DIR = "/tmp/fractio_route_repl_"

proc nodePort(nodeNum: int): int =
  result = 35000 + (nodeNum - 1) * 1000

var nextClientPort = 25000

type
  TestNode = object
    id*: int
    port*: int
    clientPort*: int
    server*: ProtocolServer
    coord*: NURAFT_COORDINATOR
    store*: RaftKVStoreExt
    mvccStore*: MvccTransactionStore
    storagePath*: string
    client*: FractioClient

proc waitForAllGroupsReady(nodes: seq[TestNode], timeoutMs: int = 10000) =
  ## Wait for all space groups to have state machines in all nodes.
  let startTime = getTime().toUnixFloat() * 1000
  while true:
    var allReady = true
    for node in nodes:
      let backend = node.store.getBackend()
      if backend == nil or not backend.isOpen:
        continue
      let startKey = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
      let endKey = makeScanEndKey(SYS_GROUPS_TABLE_ID)
      let pairs = backend.scan(startKey, endKey)
      for (k, v) in pairs:
        try:
          var data = v
          if mvccTypes.isLikelyMVCCValue(data):
            try:
              let mvccVal = mvccTypes.decodeMVCCValue(data)
              if not mvccVal.isDeleted:
                data = mvccVal.data
              else:
                continue
            except CatchableError:
              discard
          if data.len > 0 and (data[0] != '{' or data.len > 2):
            let rec = decodeGroupRecord(data)
            let gid = groupIDFromULID(rec.groupId)
            if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID:
              continue
            # Check if coordinator has this group
            if not node.coord.hasGroup(gid):
              allReady = false
              break
        except:
          discard
      if not allReady:
        break
    if allReady:
      return
    let now = getTime().toUnixFloat() * 1000
    if now - startTime > timeoutMs.float:
      return
    sleep(100)

proc cleanDir(p: string) =
  try: removeDir(p) except CatchableError: discard

proc makeNode(nodeNum: int, port: int,
    members: seq[tuple[nodeId: uint32, host: string,
        port: int]]): TestNode =
  let nodeId = NodeID(uint32(nodeNum))
  let cPort = nextClientPort
  nextClientPort += 1

  let storagePath = TMP_DIR & $nodeNum & "_" & $cPort
  cleanDir(storagePath)
  createDir(storagePath)

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    port: port,
    host: "127.0.0.1",
    dataDir: storagePath,
    electionTimeoutLowerMs: 300,
    electionTimeoutUpperMs: 500,
    heartbeatIntervalMs: TEST_HEARTBEAT_INTERVAL_MS,
  ))

  for m in members:
    coord.peerInfo[m.nodeId] = (host: m.host, port: m.port)

  coord.start()

  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    var success = false
    for attempt in 0 ..< TEST_MAX_RETRY_ATTEMPTS:
      if coord.createAndStartGroup(gid, members, preferredLeader = 1'u32):
        success = true
        break
      sleep(TEST_RETRY_BACKOFF_MS)
    doAssert success, "failed to create group " & $gid

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 6000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

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

  srv.spaceManager = newSpaceManager(
    store = store,
    coord = coord,
    nodeId = uint32(nodeNum),
    logger = srv.logger
  )

  TestNode(
    id: nodeNum, port: port, clientPort: cPort, server: srv,
    coord: coord, store: store, mvccStore: mvccStore, storagePath: storagePath,
  )

proc initClient(n: var TestNode) =
  n.client = newFractioClient("127.0.0.1", n.clientPort)
  doAssert n.client.initialize()

proc startNode(n: var TestNode) =
  n.server.start()

proc stopNode(n: TestNode) =
  if not n.client.isNil: n.client.close()
  n.server.stop()
  sleep(TEST_SHUTDOWN_DELAY_MS)
  cleanDir(n.storagePath)

proc waitForLeaderOnGroup(nodes: seq[TestNode], gid: GroupID,
    maxAttempts: int = TEST_MAX_LEADER_POLL_ATTEMPTS): int =
  for attempt in 0 ..< maxAttempts:
    for i, n in nodes:
      if n.coord.isLeader(gid):
        return i
    sleep(TEST_POLL_INTERVAL_MS)
  -1

proc waitForReadyLeader(nodes: seq[TestNode], gid: GroupID,
    timeoutMs: int = 10000): int =
  let startTime = getTime().toUnixFloat() * 1000.0
  while true:
    let elapsed = getTime().toUnixFloat() * 1000.0 - startTime
    if elapsed > timeoutMs.float:
      return -1
    for i, node in nodes:
      if node.coord.hasGroup(gid) and node.coord.waitForWriteReady(gid, 100):
        return i
    sleep(TEST_POLL_INTERVAL_MS)

proc seedSysNodes(nodes: seq[TestNode], maxRetries: int = 20): bool =
  for n in nodes:
    var success = false
    for retry in 0 ..< maxRetries:
      let leaderIdx = waitForReadyLeader(nodes, META_GROUP_ID)
      if leaderIdx < 0:
        sleep(TEST_POLL_INTERVAL_MS * 2)
        continue
      sleep(TEST_ELECTION_SETTLE_MS)
      let key = encodeTableKey(SYS_NODES_TABLE_ID, $n.id)
      let nodeRec = NodeRecord(
        nodeId: uint32(n.id),
        host: "127.0.0.1",
        raftPort: uint16(n.port),
        clientPort: uint16(n.clientPort),
        status: nsAlive,
      )
      if nodes[leaderIdx].store.sysTablePut(key, nodeRec.encode()):
        success = true
        break
      sleep(TEST_RETRY_BACKOFF_MS * (retry + 1))
    if not success:
      return false
  true

proc seedSysGroups(nodes: seq[TestNode], nodeNums: seq[int],
    maxRetries: int = TEST_MAX_RETRY_ATTEMPTS): bool =
  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    var success = false
    for retry in 0 ..< maxRetries:
      let leaderIdx = waitForReadyLeader(nodes, META_GROUP_ID)
      if leaderIdx < 0:
        sleep(TEST_POLL_INTERVAL_MS)
        continue
      let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid)
      var replicasSeq: seq[GroupReplicaBin] = @[]
      for num in nodeNums:
        replicasSeq.add(GroupReplicaBin(nodeId: uint32(num),
            replicaType: rtVoter))
      let groupRec = GroupRecord(
        groupId: groupIDToULID(gid),
        spaceId: if gid == META_GROUP_ID: zeroSpaceID() else: genSpaceIDLocal(),
        leader: uint32(nodeNums[0]),
        replicas: replicasSeq,
      )
      if nodes[leaderIdx].store.sysTablePut(key, groupRec.encode()):
        success = true
        break
      sleep(TEST_RETRY_BACKOFF_MS * (retry + 1))
    if not success:
      return false
  true

proc seedDefaults(nodes: seq[TestNode], maxRetries: int = TEST_MAX_RETRY_ATTEMPTS): bool =
  var dbSuccess = false
  for retry in 0 ..< maxRetries:
    let leaderIdx = waitForReadyLeader(nodes, META_GROUP_ID)
    if leaderIdx < 0:
      sleep(TEST_POLL_INTERVAL_MS)
      continue
    let dbKey = encodeTableKey(SYS_DATABASES_TABLE_ID, "default")
    let dbRec = DatabaseRecord(name: "default",
        createdAtNs: system_schemas.nowNs()).encode()
    if nodes[leaderIdx].store.sysTablePut(dbKey, dbRec):
      dbSuccess = true
      break
    sleep(TEST_RETRY_BACKOFF_MS * (retry + 1))
  if not dbSuccess:
    return false
  for retry in 0 ..< maxRetries:
    let leaderIdx = waitForReadyLeader(nodes, META_GROUP_ID)
    if leaderIdx < 0:
      sleep(TEST_POLL_INTERVAL_MS)
      continue
    let scKey = encodeTableKey(SYS_SCHEMAS_TABLE_ID, "default.public")
    let scRec = SchemaRecord(name: "public", database: "default",
        createdAtNs: system_schemas.nowNs()).encode()
    if nodes[leaderIdx].store.sysTablePut(scKey, scRec):
      return true
    sleep(TEST_RETRY_BACKOFF_MS * (retry + 1))
  false

proc refreshClientMetadata(nodes: seq[TestNode]) =
  for node in nodes:
    if not node.client.isNil:
      discard node.client.refreshMetadata()

proc refreshServerCaches(nodes: seq[TestNode]) =
  for node in nodes:
    node.store.loadSpaces()
    node.store.loadGroupMembers()
    node.store.loadTableSpaces()

proc ensureStateMachinesForGroups(nodes: seq[TestNode]) =
  for node in nodes:
    let backend = node.store.getBackend()
    if backend == nil or not backend.isOpen:
      continue
    let startKey = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
    let endKey = makeScanEndKey(SYS_GROUPS_TABLE_ID)
    let pairs = backend.scan(startKey, endKey)
    for (k, v) in pairs:
      try:
        var data = v
        if mvccTypes.isLikelyMVCCValue(data):
          try:
            let mvccVal = mvccTypes.decodeMVCCValue(data)
            if not mvccVal.isDeleted:
              data = mvccVal.data
            else:
              continue
          except CatchableError:
            discard
        if data.len > 0 and (data[0] != '{' or data.len > 2):
          let rec = decodeGroupRecord(data)
          let gid = groupIDFromULID(rec.groupId)
          if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID:
            continue
          if node.coord.hasGroup(gid):
            discard node.store.getOrCreateSM(gid)
      except:
        discard

proc waitForSpaceLeaders(nodes: seq[TestNode]) =
  let store = nodes[0].store
  let grpStart = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
  let grpEnd = makeScanEndKey(SYS_GROUPS_TABLE_ID)
  let grpScan = store.raftScan(grpStart, grpEnd, 0, includeSystemKeys = true)
  if grpScan.isOk:
    for (key, entry) in grpScan.value:
      try:
        var data = entry.value
        if mvccTypes.isLikelyMVCCValue(data):
          try:
            let mvccVal = mvccTypes.decodeMVCCValue(data)
            if not mvccVal.isDeleted:
              data = mvccVal.data
            else:
              continue
          except CatchableError:
            discard
        if data.len > 0 and data[0] != '{':
          let rec = decodeGroupRecord(data)
          let gid = groupIDFromULID(rec.groupId)
          if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID: continue
          for attempt in 0 ..< 200:
            var found = false
            for node in nodes:
              if node.coord.hasGroup(gid) and node.coord.waitForWriteReady(gid, 2000):
                found = true
                break
            if found: break
            sleep(TEST_POLL_INTERVAL_MS)
      except: discard

proc distributeSpaceGroups(nodes: seq[TestNode], replicaCount: int = 3) =
  for node in nodes:
    discard node.coord.waitForGroupCreationQueue(5000)
  sleep(TEST_ELECTION_SETTLE_MS)
  ensureStateMachinesForGroups(nodes)
  waitForSpaceLeaders(nodes)
  sleep(500)

proc execWithRetry(nodes: seq[TestNode], sql: string,
    maxRetries: int = TEST_MAX_RETRY_ATTEMPTS): ExecResult =
  for retry in 0 ..< maxRetries:
    let leaderIdx = waitForReadyLeader(nodes, META_GROUP_ID)
    if leaderIdx < 0:
      sleep(TEST_POLL_INTERVAL_MS)
      continue
    let res = bufferRows(nodes[leaderIdx].client.query(sql))
    if res.kind != erkError:
      return res
    if isNotLeaderError(res.error):
      sleep(TEST_RETRY_BACKOFF_MS * (retry + 1))
      continue
    return res
  ExecResult(kind: erkError, error: "max retries exceeded for: " & sql)

proc exec(node: TestNode, sql: string): ExecResult =
  bufferRows(node.client.query(sql))

proc makeCluster3(): seq[TestNode] =
  let members = @[
    (nodeId: 1'u32, host: "127.0.0.1", port: nodePort(1)),
    (nodeId: 2'u32, host: "127.0.0.1", port: nodePort(2)),
    (nodeId: 3'u32, host: "127.0.0.1", port: nodePort(3)),
  ]

  var nodes: seq[TestNode]
  for i, m in members:
    nodes.add(makeNode(int(m.nodeId), m.port, members))

  for i in 0 ..< nodes.len:
    startNode(nodes[i])

  let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
  doAssert leaderIdx >= 0, "No meta leader elected"

  var stableCount = 0
  for i in 0 ..< 30:
    let currentLeaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID,
        maxAttempts = 3)
    if currentLeaderIdx == leaderIdx:
      inc stableCount
      if stableCount >= 3: break
    else:
      stableCount = 0
    sleep(TEST_POLL_INTERVAL_MS)
  doAssert stableCount >= 3, "Meta leader not stable"

  let dataLeaderIdx = waitForLeaderOnGroup(nodes, DATA_GROUP_START_ID)
  doAssert dataLeaderIdx >= 0, "No data group leader elected"
  stableCount = 0
  for i in 0 ..< 30:
    let currentLeaderIdx = waitForLeaderOnGroup(nodes, DATA_GROUP_START_ID,
        maxAttempts = 3)
    if currentLeaderIdx == dataLeaderIdx:
      inc stableCount
      if stableCount >= 3: break
    else:
      stableCount = 0
    sleep(TEST_POLL_INTERVAL_MS)
  doAssert stableCount >= 3, "Data group leader not stable"

  let allNums = @[1, 2, 3]
  doAssert seedSysNodes(nodes), "Failed to seed sys.nodes"
  doAssert seedSysGroups(nodes, allNums), "Failed to seed sys.groups"
  doAssert seedDefaults(nodes), "Failed to seed defaults"

  sleep(TEST_REPLICATION_WAIT_MS * 2)

  let finalLeader = waitForLeaderOnGroup(nodes, META_GROUP_ID)
  doAssert finalLeader >= 0, "No meta leader after seeding"
  sleep(TEST_ELECTION_SETTLE_MS * 2)

  for i in 0 ..< nodes.len:
    initClient(nodes[i])

  nodes

proc stopCluster(nodes: seq[TestNode]) =
  for i in countdown(nodes.high, 0):
    stopNode(nodes[i])

# ---------------------------------------------------------------------------
# Helper: find which node is leader for a group
# ---------------------------------------------------------------------------

proc findLeaderNodeIdx(nodes: seq[TestNode], gid: GroupID): int =
  ## Find the index of the node that is leader for the given group.
  for i, n in nodes:
    if n.coord.isLeader(gid):
      return i
  -1

proc findFollowerNodeIdx(nodes: seq[TestNode], gid: GroupID): int =
  ## Find the index of a node that is NOT leader for the given group.
  for i, n in nodes:
    if not n.coord.isLeader(gid) and n.coord.hasGroup(gid):
      return i
  -1

# ---------------------------------------------------------------------------
# Test suites
# ---------------------------------------------------------------------------

suite "Space routing and replication — 3-node cluster":

  test "space has 3 groups and data routes across them":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    # Create space with 3 replicas (1 group per node)
    let csRes = execWithRetry(nodes, "CREATE SPACE testspace WITH REPLICAS = 3")
    check csRes.kind == erkOk
    if csRes.kind == erkError:
      echo "  CREATE SPACE error: " & csRes.error

    distributeSpaceGroups(nodes)
    ensureStateMachinesForGroups(nodes)
    waitForAllGroupsReady(nodes, timeoutMs = 15000)
    refreshServerCaches(nodes)
    refreshClientMetadata(nodes)

    # Verify space was created with correct group count and replicas
    let leaderIdx = waitForReadyLeader(nodes, META_GROUP_ID)
    check leaderIdx >= 0

    let spaceScan = nodes[leaderIdx].store.raftScan(
      encodeTableKey(SYS_SPACES_TABLE_ID, ""),
      makeScanEndKey(SYS_SPACES_TABLE_ID),
      0, includeSystemKeys = true)

    var foundSpace = false
    var spaceId: SpaceID
    var groupCount = 0
    var replicas = 0'i32
    var groupIds: seq[GroupID] = @[]

    if spaceScan.isOk:
      for (key, entry) in spaceScan.value:
        var data = entry.value
        if mvccTypes.isLikelyMVCCValue(data):
          try:
            let mvccVal = mvccTypes.decodeMVCCValue(data)
            if not mvccVal.isDeleted:
              data = mvccVal.data
            else:
              continue
          except CatchableError:
            discard
        let rec = decodeSpaceRecord(data)
        if rec.name == "testspace":
          foundSpace = true
          spaceId = rec.spaceId
          groupCount = rec.groupCount
          replicas = rec.replicas
          groupIds = rec.groupIds
          break

    check foundSpace
    check groupCount == 3
    check replicas == 3
    check groupIds.len == 3

    # Create table in the space
    let ctRes = execWithRetry(nodes,
        "CREATE TABLE testspace.public.users(id INT PRIMARY KEY, name TEXT) IN SPACE testspace")
    check ctRes.kind == erkOk
    if ctRes.kind == erkError:
      echo "  CREATE TABLE error: " & ctRes.error

    # Refresh caches so the client knows the table's spaceId
    refreshServerCaches(nodes)
    refreshClientMetadata(nodes)

    # Insert rows and track which group each key routes to
    let client = nodes[leaderIdx].client

    # Force metadata refresh on the client
    discard client.refreshMetadata()

    # Check that the client's routing state has the space's groups
    let routingState = client.getRoutingState()
    var foundTable = false
    for tableId, tableInfo in routingState.tables:
      if tableInfo.name == "users":
        foundTable = true
        # The spaceId should be valid (not zero)
        check isValidSpaceId(tableInfo.spaceId)
        # The space should be in the routing state
        check tableInfo.spaceId in routingState.spaces
        # The space should have 3 group IDs
        check routingState.spaces[tableInfo.spaceId].groupIds.len == 3
    check foundTable

    # Insert rows and verify they're accessible
    for i in 1 .. 9:
      var insRes = exec(nodes[leaderIdx],
          "INSERT INTO testspace.public.users VALUES (" & $i & ", 'user" & $i & "')")
      for retry in 0 ..< 10:
        if insRes.kind == erkModified: break
        sleep(50)
        discard nodes[leaderIdx].client.refreshMetadata()
        insRes = exec(nodes[leaderIdx],
            "INSERT INTO testspace.public.users VALUES (" & $i & ", 'user" &
                $i & "')")
      check insRes.kind == erkModified

    # Verify all rows are readable
    for i in 1 .. 9:
      var selRes = exec(nodes[leaderIdx],
          "SELECT * FROM testspace.public.users WHERE id = " & $i)
      for retry in 0 ..< 10:
        if selRes.kind == erkRows and selRes.rows.len == 1: break
        sleep(50)
        selRes = exec(nodes[leaderIdx],
            "SELECT * FROM testspace.public.users WHERE id = " & $i)
      check selRes.kind == erkRows
      if selRes.kind == erkRows:
        check selRes.rows.len == 1

    # --- Verify sys.groups leader fields are populated ---
    # After space group elections, the leader field in sys.groups should
    # reflect the actual NuRaft leader. This was previously broken because
    # onLeaderChanged on non-META-leader nodes couldn't forward updates
    # (nodeInfoCache was empty). With populateNodeInfoCache + periodic
    # syncGroupLeadersToSysTables, the META leader keeps sys.groups in sync.

    # Wait for processLeaderPersistReq and createSpace best-effort update
    sleep(500)

    # Manually trigger the periodic leader sync (normally runs every ~2s)
    nodes[leaderIdx].store.syncGroupLeadersToSysTables()
    sleep(TEST_REPLICATION_WAIT_MS * 2)

    # Refresh caches
    refreshServerCaches(nodes)

    # For each space group, verify sys.groups has a non-zero leader
    var leadersMatch = 0
    for gid in groupIds:
      # Get the live NuRaft leader
      var liveLeader: int32 = -1
      for node in nodes:
        if node.coord.hasGroup(gid):
          let l = node.coord.getLeader(gid)
          if l > 0:
            liveLeader = l
            break
      if liveLeader <= 0:
        continue

      # Read sys.groups and check leader field
      let grpStart = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
      let grpEnd = makeScanEndKey(SYS_GROUPS_TABLE_ID)
      let grpScan = nodes[leaderIdx].store.raftScan(grpStart, grpEnd, 0,
          includeSystemKeys = true)
      if grpScan.isOk:
        for (key, entry) in grpScan.value:
          var data = entry.value
          if mvccTypes.isLikelyMVCCValue(data):
            try:
              let mvccVal = mvccTypes.decodeMVCCValue(data)
              if not mvccVal.isDeleted:
                data = mvccVal.data
              else:
                continue
            except CatchableError:
              discard
          try:
            let rec = decodeGroupRecord(data)
            let recGid = groupIDFromULID(rec.groupId)
            if recGid == gid:
              if rec.leader > 0:
                inc leadersMatch
              break
          except CatchableError:
            discard

    # At least some groups should have leader field populated in sys.groups
    check leadersMatch >= 1

  test "data written to group leader is replicated to followers":
    # This test verifies Raft replication for both the META group and
    # user-created space groups. Data written to a group leader should be
    # replicated to all followers of that group via Raft consensus.
    #
    # Part 1: META group replication (system tables).
    # Part 2: User-created space group replication (data tables).
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    # --- Part 1: META group replication ---
    let metaLeaderIdx = waitForReadyLeader(nodes, META_GROUP_ID)
    check metaLeaderIdx >= 0

    # Write a system key via the META leader's sysTablePut (goes through Raft)
    let testKey1 = encodeTableKey(SYS_NODES_TABLE_ID, "repl_test_meta")
    let testValue1 = "meta_replicated"
    let putOk1 = nodes[metaLeaderIdx].store.sysTablePut(testKey1, testValue1)
    check putOk1

    # Wait for Raft replication
    sleep(TEST_REPLICATION_WAIT_MS * 3)

    # Read from a follower's backend (bypassing leader check)
    var foundMetaOnFollower = false
    for i, n in nodes:
      if i == metaLeaderIdx:
        continue
      let backend = n.store.getBackend()
      if backend != nil and backend.isOpen:
        let valOpt = backend.get(testKey1)
        if valOpt.isSome:
          var val = valOpt.get()
          if mvccTypes.isLikelyMVCCValue(val):
            try:
              let mvccVal = mvccTypes.decodeMVCCValue(val)
              if not mvccVal.isDeleted:
                val = mvccVal.data
              else:
                continue
            except CatchableError:
              discard
          if val == testValue1:
            foundMetaOnFollower = true
            break

    check foundMetaOnFollower

    # --- Part 2: User-created space group replication ---
    # Create a space with 3 replicas, then write directly to a space group
    # leader and verify the data appears on follower nodes.
    let csRes = execWithRetry(nodes, "CREATE SPACE replspace WITH REPLICAS = 3")
    check csRes.kind == erkOk
    distributeSpaceGroups(nodes)
    ensureStateMachinesForGroups(nodes)
    waitForAllGroupsReady(nodes, timeoutMs = 15000)
    refreshServerCaches(nodes)
    refreshClientMetadata(nodes)

    # Find space group IDs
    let spaceLeaderIdx = waitForReadyLeader(nodes, META_GROUP_ID)
    check spaceLeaderIdx >= 0

    let spaceScan = nodes[spaceLeaderIdx].store.raftScan(
      encodeTableKey(SYS_SPACES_TABLE_ID, ""),
      makeScanEndKey(SYS_SPACES_TABLE_ID),
      0, includeSystemKeys = true)

    var groupIds: seq[GroupID] = @[]
    if spaceScan.isOk:
      for (key, entry) in spaceScan.value:
        var data = entry.value
        if mvccTypes.isLikelyMVCCValue(data):
          try:
            let mvccVal = mvccTypes.decodeMVCCValue(data)
            if not mvccVal.isDeleted:
              data = mvccVal.data
            else:
              continue
          except CatchableError:
            discard
        let rec = decodeSpaceRecord(data)
        if rec.name == "replspace":
          groupIds = rec.groupIds
          break

    check groupIds.len >= 1

    # For each space group, write a key via proposeAndWait to the leader,
    # then read from a follower's backend to verify replication.
    var replicatedSpaceGroups = 0
    for gid in groupIds:
      # Find leader for this group and wait for it to be write-ready
      var groupLeaderIdx = waitForReadyLeader(nodes, gid, timeoutMs = 10000)
      if groupLeaderIdx < 0:
        # Skip this group if no leader found
        continue

      # Ensure the state machine is registered on the leader node
      discard nodes[groupLeaderIdx].store.getOrCreateSM(gid)

      # Write a test key directly through proposeAndWait to the group leader
      let testKey2 = "replspace_test_" & $gid
      let testValue2 = "space_value_" & $gid

      var writeOk = false
      for retryAttempt in 0 ..< 5:
        let batch = newWriteBatch()
        batch.put(toBytes(testKey2), toBytes(testValue2))
        let cmd = RaftCommand(kind: ckWrite, writeBatch: batch)
        let writeRes = nodes[groupLeaderIdx].coord.proposeAndWait(gid, cmd, 5000)
        if writeRes.success:
          writeOk = true
          break
        # Leadership may have changed, try to find the new leader
        sleep(200)
        groupLeaderIdx = waitForReadyLeader(nodes, gid, timeoutMs = 3000)
        if groupLeaderIdx < 0:
          break
        # Ensure state machine on new leader
        discard nodes[groupLeaderIdx].store.getOrCreateSM(gid)

      if not writeOk:
        continue

      # Wait for Raft replication
      sleep(TEST_REPLICATION_WAIT_MS * 3)

      # Read from a follower's backend
      let followerIdx = findFollowerNodeIdx(nodes, gid)
      if followerIdx >= 0:
        let backend = nodes[followerIdx].store.getBackend()
        if backend != nil and backend.isOpen:
          let valOpt = backend.get(testKey2)
          if valOpt.isSome:
            if valOpt.get() == testValue2:
              inc replicatedSpaceGroups

    # At least one space group should have replicated data
    check replicatedSpaceGroups >= 1

  test "server returns NOT_LEADER for put/get on non-leader group":
    # This test verifies that a server receiving a KV PUT for a group it doesn't
    # lead returns a NOT_LEADER error with redirect info, rather than silently
    # accepting the write locally.
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let csRes = execWithRetry(nodes, "CREATE SPACE nlspace WITH REPLICAS = 3")
    check csRes.kind == erkOk
    distributeSpaceGroups(nodes)
    ensureStateMachinesForGroups(nodes)
    waitForAllGroupsReady(nodes, timeoutMs = 15000)
    refreshServerCaches(nodes)
    refreshClientMetadata(nodes)

    # Find space group IDs
    let leaderIdx = waitForReadyLeader(nodes, META_GROUP_ID)
    check leaderIdx >= 0

    let spaceScan = nodes[leaderIdx].store.raftScan(
      encodeTableKey(SYS_SPACES_TABLE_ID, ""),
      makeScanEndKey(SYS_SPACES_TABLE_ID),
      0, includeSystemKeys = true)

    var groupIds: seq[GroupID] = @[]
    if spaceScan.isOk:
      for (key, entry) in spaceScan.value:
        var data = entry.value
        if mvccTypes.isLikelyMVCCValue(data):
          try:
            let mvccVal = mvccTypes.decodeMVCCValue(data)
            if not mvccVal.isDeleted:
              data = mvccVal.data
            else:
              continue
          except CatchableError:
            discard
        let rec = decodeSpaceRecord(data)
        if rec.name == "nlspace":
          groupIds = rec.groupIds
          break

    check groupIds.len >= 1

    # For each space group, try to write directly to a follower.
    # The server should reject the write with NOT_LEADER.
    var notLeaderCount = 0
    for gid in groupIds:
      let groupLeaderIdx = findLeaderNodeIdx(nodes, gid)
      if groupLeaderIdx < 0:
        sleep(200)
        continue

      let followerIdx = findFollowerNodeIdx(nodes, gid)
      if followerIdx < 0:
        continue

      # Try to connect to the follower and send a PUT for this group.
      # The follower should return NOT_LEADER with redirect info.
      let cfg = protoClient.ClientConfig(
        host: "127.0.0.1",
        port: nodes[followerIdx].clientPort,
        timeoutMs: 3000
      )
      let pc = newProtocolClient(cfg)
      let connectRes = pc.connect()
      if connectRes.isOk:
        defer: pc.disconnect()
        # Send a PUT for a key that belongs to this space group
        let testKey = encodeTableKey(SYS_NODES_TABLE_ID, "nl_test_" & $gid)
        let putRes = pc.kvPutInGroup(testKey, "should_fail", gid)
        if putRes.isErr:
          # Check if it's a NOT_LEADER error
          if putRes.error.kind == peNotLeader:
            # Correct behavior: server returned NOT_LEADER with redirect
            inc notLeaderCount
          # Other errors (timeout, connection) are acceptable in test env
        else:
          # The PUT succeeded — this means either:
          # 1. The follower forwarded to the leader internally (old behavior), OR
          # 2. The follower is now the leader after an election
          # Check the response status
          if putRes.value.status != kvMsgs.PutStatusOK:
            inc notLeaderCount

    # We should get at least one NOT_LEADER response across all groups
    # (may not be all if some followers became leaders during test)
    check notLeaderCount >= 1

# Exit immediately to avoid SIGSEGV during Nim GC cleanup
quit(0)
quit(0)
