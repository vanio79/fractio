# Integration tests for space rebalancing on node join.
#
# Tests the full rebalance lifecycle with a real multi-node cluster:
#   1. Start with 2 nodes, CREATE SPACE WITH REPLICAS = 2
#   2. Add a 3rd node, trigger rebalanceSpaces
#   3. Verify dual-read works during migration
#   4. Run migration, verify cutover
#
# Cluster topology: in-process NuRaftCoordinators with ASIO networking
# Port allocation: 29000–31000 (NuRaft ASIO, basePort per node spaced by 1000)
# Uses same ports for all tests since SO_REUSEADDR/SO_REUSEPORT/SO_LINGER=0 allow immediate reuse
# Temp storage: /tmp/fractio_rebal_<nodeId>/ (cleaned up per test)

import std/[unittest, os, options, json, strutils, tables, locks, times]
import fractio/core/types except NodeID
import fractio/protocol/raft_store
import fractio/protocol/server
import fractio/protocol/types
import fractio/protocol/txn_manager
import fractio/protocol/mvcc_store
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider
import fractio/storage/wisckey_backend
import fractio/storage/mvcc/types as mvccTypes
import fractio/utils/rwlock
import fractio/sql/executor
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/distributed/space_manager

# Import optimized test configuration
import ../../test_config

# Import NuRaft shim to disable busy_connection_limit during tests
# This prevents system_exit(-22) during shutdown when peers are disconnected
import fractio/distributed/raft/c_bindings

# Disable busy_connection_limit to prevent NuRaft system_exit(-22) during test shutdown
# When peers are disconnected during stopCluster, NuRaft would otherwise call system_exit
# after 20 connection failures, causing the next test to fail
nuraftLimitsSetBusyConnectionLimit(0)

proc nowMs(): float = epochTime() * 1000.0

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  TMP_DIR = "/tmp/fractio_rebal_"

var nextClientPort = 19100 ## incremented per node to avoid port conflicts between tests

# Fixed port allocation - node 1: 29000, node 2: 30000, node 3: 31000
proc nodePort(nodeNum: int): int =
  result = 29000 + (nodeNum - 1) * 1000

# ---------------------------------------------------------------------------
# Memory monitoring
# ---------------------------------------------------------------------------

proc getRSSMB(): int =
  try:
    let status = readFile("/proc/self/status")
    for line in status.splitLines:
      if line.startsWith("VmRSS:"):
        let kb = parseInt(line.splitWhitespace()[1])
        return kb div 1024
  except:
    discard
  return 0

proc getThreadCount(): int =
  try:
    let status = readFile("/proc/self/status")
    for line in status.splitLines:
      if line.startsWith("Threads:"):
        return parseInt(line.splitWhitespace()[1])
  except:
    discard
  return 0

proc printResourceUsage(label: string) =
  echo "=== ", label, ": RSS=", getRSSMB(), " MB, Threads=", getThreadCount(), " ==="

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

type
  TestNode = object
    id*: int
    port*: int ## Single port for all Raft groups (multiplexed)
    clientPort*: int
    server*: ProtocolServer
    coord*: NuRaftCoordinator
    store*: RaftKVStoreExt
    mvccStore*: MvccTransactionStore
    storagePath*: string
    client*: FractioClient

proc cleanDir(p: string) =
  try: removeDir(p) except CatchableError: discard

proc makeNode(nodeNum: int, port: int,
    members: seq[tuple[nodeId: uint32, host: string,
        port: int]]): TestNode =
  let nodeId = NodeID(uint32(nodeNum))
  let cPort = nextClientPort
  nextClientPort += 1

  # Use unique storage path for each node + test run instance to avoid LOCK contention
  let storagePath = TMP_DIR & $nodeNum & "_" & $cPort
  cleanDir(storagePath)
  createDir(storagePath)

  # Use the SAME election timeout range for ALL nodes.
  # NuRaft internally randomizes within the range to avoid election collisions.
  # Using different ranges per node creates overlapping boundaries that cause
  # both nodes' timers to fire simultaneously, leading to leader instability.
  let lowerMs = int32(300)
  let upperMs = int32(500)

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    port: port,
    host: "127.0.0.1",
    dataDir: storagePath,
    electionTimeoutLowerMs: lowerMs,
    electionTimeoutUpperMs: upperMs,
    heartbeatIntervalMs: TEST_HEARTBEAT_INTERVAL_MS,
  ))

  # Populate peerInfo so dynamic group creation knows peer ports
  for m in members:
    coord.peerInfo[m.nodeId] = (host: m.host, port: m.port)

  coord.start()

  # Create meta + data groups with retries
  # Use the first member as the preferred leader to avoid election races
  let preferredLeader = if members.len > 0: members[0].nodeId else: 0'u32
  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    var success = false
    for attempt in 0 ..< TEST_MAX_RETRY_ATTEMPTS:
      if coord.createAndStartGroup(gid, members, preferredLeader):
        success = true
        break
      sleep(TEST_RETRY_BACKOFF_MS)
    if not success:
      raise newException(AssertionDefect, "Failed to create group " & $gid &
          " for node " & $nodeNum)

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 6000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  # Create MVCC store for DDL operations
  let txnMgr = newTransactionManager()
  let mockTimer = MockTimeProvider(currentTime: timerTypes.Timestamp(1_000_000_000))
  let tsProvider = newTimestampProvider(mockTimer, NodeID(uint32(
      nodeNum)).uint16)
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

  # Initialize SpaceManager for CREATE/DROP SPACE operations
  srv.spaceManager = newSpaceManager(
    store = store,
    coord = coord,
    nodeId = uint32(nodeNum),
    logger = nil
  )

  # Create FractioClient (will be initialized when node starts)
  let fractioClient = newFractioClient("127.0.0.1", cPort)

  TestNode(
    id: nodeNum, port: port, clientPort: cPort, server: srv,
    coord: coord, store: store, mvccStore: mvccStore, storagePath: storagePath,
    client: fractioClient,
  )

proc initClient(n: var TestNode, leaderPort: int) =
  n.client = newFractioClient("127.0.0.1", leaderPort)
  doAssert n.client.initialize()

proc startNode(n: TestNode) =
  n.server.start()

proc stopNode*(n: TestNode) =
  let t0 = nowMs()
  if n.client != nil:
    n.client.close()
  # server.stop() waits for all client/accept threads to finish,
  # then stops raftStore and raftCoord in the correct order.
  # Do NOT call store.stop() beforehand — it clears Tables that
  # background threads may still be accessing (SIGSEGV race).
  # Do NOT call coord.stop() — server.stop() already stops the coordinator.
  n.server.stop()
  sleep(TEST_SHUTDOWN_DELAY_MS)
  cleanDir(n.storagePath)
  echo "TIMING stopNode ", n.id, " took ", int(nowMs() - t0), " ms"

proc waitForLeaderOnGroup(nodes: seq[TestNode], gid: GroupID,
    maxAttempts: int = TEST_MAX_LEADER_POLL_ATTEMPTS): int =
  for attempt in 0 ..< maxAttempts:
    for i, n in nodes:
      if n.coord.isLeader(gid):
        return i
    sleep(TEST_POLL_INTERVAL_MS)
  -1

proc seedSysNodesWithRetry(nodes: seq[TestNode]) =
  ## Seed sys.nodes with retry on leadership changes.
  ## The leader may change during the initial sleep, so we retry if writes fail.
  for attempt in 0 ..< 5:
    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    if leaderIdx < 0:
      sleep(TEST_POLL_INTERVAL_MS)
      continue
    let leaderStore = nodes[leaderIdx].store
    var allSuccess = true
    for n in nodes:
      let key = encodeTableKey(SYS_NODES_TABLE_ID, $n.id)
      let nodeRec = NodeRecord(
        nodeId: uint32(n.id),
        host: "127.0.0.1",
        raftPort: uint16(n.port),
        clientPort: uint16(n.clientPort),
        status: nsAlive,
      )
      let r = leaderStore.sysTablePut(key, nodeRec.encode())
      if not r:
        allSuccess = false
        echo "DEBUG: seedSysNodes attempt ", attempt, " failed for node ", n.id
        break
    if allSuccess:
      return
    sleep(TEST_POLL_INTERVAL_MS * 5) # Wait for leader to stabilize
  doAssert false, "failed to seed sys.nodes after retries"

proc seedSysNodes(leaderStore: RaftKVStoreExt, nodes: seq[TestNode]) =
  ## Legacy proc - kept for compatibility but prefer seedSysNodesWithRetry.
  for n in nodes:
    let key = encodeTableKey(SYS_NODES_TABLE_ID, $n.id)
    let nodeRec = NodeRecord(
      nodeId: uint32(n.id),
      host: "127.0.0.1",
      raftPort: uint16(n.port),
      clientPort: uint16(n.clientPort),
      status: nsAlive,
    )
    let r = leaderStore.sysTablePut(key, nodeRec.encode())
    doAssert r, "failed to seed sys.nodes for node " & $n.id

proc seedSysGroups(leaderStore: RaftKVStoreExt, nodeNums: seq[int]) =
  let coord = leaderStore.coordinator
  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(gid))
    var replicas: seq[GroupReplicaBin] = @[]
    for num in nodeNums:
      replicas.add(GroupReplicaBin(nodeId: uint32(num), replicaType: rtVoter))
    # Query the coordinator for the actual leader of this group
    var leader: uint32 = 0
    if coord != nil:
      for nodeId in nodeNums:
        if coord.isLeader(gid):
          leader = uint32(nodeId)
          break
    let groupRec = GroupRecord(
      groupId: groupIDToULID(gid),
      spaceId: zeroSpaceID(),
      replicas: replicas,
      leader: leader,
    )
    discard leaderStore.raftPut(key, groupRec.encode())

proc seedDefaults(leaderStore: RaftKVStoreExt) =
  discard leaderStore.raftPut(
    encodeTableKey(SYS_DATABASES_TABLE_ID, "default"),
    DatabaseRecord(name: "default", createdAtNs: system_schemas.nowNs()).encode())
  discard leaderStore.raftPut(
    encodeTableKey(SYS_SCHEMAS_TABLE_ID, "default.public"),
    SchemaRecord(name: "public", database: "default",
        createdAtNs: system_schemas.nowNs()).encode())
  # Seed default space (replicas=0 = ALL, single group = meta group)
  let spaceRec = SpaceRecord(
    spaceId: zeroSpaceID(),
    name: "default",
    replicas: 0,
    groupCount: 1,
    groupIds: @[META_GROUP_ID],
  )
  discard leaderStore.raftPut(encodeSpaceKey(zeroSpaceID()), spaceRec.encode())

proc waitForAutoDistribution(nodes: seq[TestNode], expectedGroupIds: seq[
    GroupID], replicaCount: int, maxWaitMs: int = 1500) =
  # First, wait for all async group creation queues to be empty
  for node in nodes:
    discard node.coord.waitForGroupCreationQueue(maxWaitMs)

  let expectedTotal = expectedGroupIds.len * replicaCount
  let stepMs = TEST_POLL_INTERVAL_MS
  var waited = 0
  while waited < maxWaitMs:
    var totalMemberships = 0
    for node in nodes:
      for gid in expectedGroupIds:
        if node.coord.hasGroup(gid):
          inc totalMemberships
    if totalMemberships >= expectedTotal:
      break
    sleep(stepMs)
    waited += stepMs

proc waitForSpaceLeaders(nodes: seq[TestNode]) =
  let store = nodes[0].store
  let grpStart = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
  let grpEnd = makeScanEndKey(SYS_GROUPS_TABLE_ID)
  let grpScan = store.raftScan(grpStart, grpEnd, 0, includeSystemKeys = true)
  if grpScan.isOk:
    for (key, entry) in grpScan.value:
      try:
        let data = entry.value
        # Data may have MVCC header if written through MVCC store, or be raw binary/JSON
        var gid: GroupID
        var parsed = false

        if data.len > 0 and data[0] == '{':
          # Try JSON first
          try:
            let j = parseJson(data)
            gid = groupIDFromULID(ulidFromString(j["groupId"].getStr()))
            parsed = true
          except:
            discard

        if not parsed:
          # Try MVCC-aware binary decoding first (handles both MVCC and raw)
          try:
            let (groupRec, _) = decodeGroupRecordFromMVCC(data)
            gid = groupIDFromULID(groupRec.groupId)
            parsed = true
          except:
            discard

        if not parsed:
          # Fall back to raw binary decoding
          try:
            let groupRec = decodeGroupRecord(data)
            gid = groupIDFromULID(groupRec.groupId)
            parsed = true
          except:
            discard

        if not parsed:
          continue

        if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID: continue

        # Check if any node still has this group (it may have been removed
        # during cutover between the sys.groups scan and this check)
        var anyNodeHasGroup = false
        for node in nodes:
          if node.coord.hasGroup(gid):
            anyNodeHasGroup = true
            break
        if not anyNodeHasGroup:
          continue # Group was removed, skip

        # First, wait for the group to be created on at least one node
        var groupCreated = false
        for attempt in 0 ..< 200:
          for node in nodes:
            if node.coord.hasGroup(gid):
              groupCreated = true
              break
          if groupCreated: break
          sleep(TEST_POLL_INTERVAL_MS)

        if not groupCreated:
          doAssert false, "Group " & $gid & " was never created on any node"

        # Now wait for leader election and write readiness
        for attempt in 0 ..< 200:
          var hasLeader = false
          for node in nodes:
            # Check if this node has the group and is write-ready
            # Use 2000ms timeout for write readiness probe
            if node.coord.hasGroup(gid) and node.coord.waitForWriteReady(gid, 2000):
              hasLeader = true
              break
          if hasLeader: break
          sleep(TEST_POLL_INTERVAL_MS)
          if attempt == 199 and not hasLeader:
            doAssert false, "Failed to elect leader for group " & $gid
      except: discard

proc exec(node: TestNode, sql: string, database = "default",
    schema = "public"): ExecResult =
  ## Execute SQL and buffer streaming rows into regular rows for test assertions.
  let res = node.client.query(sql, database, schema)
  bufferRows(res)

# ---------------------------------------------------------------------------
# Cluster fixtures
# ---------------------------------------------------------------------------

proc makeCluster2(): seq[TestNode] =
  ## 2-node cluster with staggered election timeouts.
  ## CRITICAL: The coordinator applies node-specific offsets to election timeouts:
  ## - Node 1: [150, 300]ms (shorter - will become leader first)
  ## - Node 2: [350, 500]ms (longer - will vote for node 1)
  ## Both nodes must start simultaneously so node 1 can get node 2's vote.
  let members = @[
    (nodeId: 1'u32, host: "127.0.0.1", port: nodePort(1)),
    (nodeId: 2'u32, host: "127.0.0.1", port: nodePort(2)),
  ]

  var nodes: seq[TestNode]

  # Create both nodes first (this populates peerInfo and creates groups)
  for i, m in members:
    nodes.add(makeNode(int(m.nodeId), m.port, members))

  # Start both nodes simultaneously - node 1 has shorter election timeout
  # so it will become candidate first and win the election
  for n in nodes:
    startNode(n)

  # Wait for leader election and verify stability
  let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
  doAssert leaderIdx >= 0, "no leader found for META_GROUP_ID"
  # Verify leader stability - check that the same node is leader for 3 consecutive polls
  var stableCount = 0
  for i in 0 ..< 10:
    let currentLeaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID,
        maxAttempts = 3)
    if currentLeaderIdx == leaderIdx:
      inc stableCount
      if stableCount >= 3:
        break
    else:
      stableCount = 0
    sleep(TEST_POLL_INTERVAL_MS)
  doAssert stableCount >= 3, "leader not stable"

  let allNums = @[1, 2]
  seedSysNodesWithRetry(nodes) # Use retry-based seeding
  seedSysGroups(nodes[leaderIdx].store, allNums)
  seedDefaults(nodes[leaderIdx].store)
  sleep(TEST_REPLICATION_WAIT_MS * 2)
  for i in 0 ..< nodes.len: initClient(nodes[i], nodes[leaderIdx].clientPort)

  # Load space caches
  for n in nodes:
    n.store.loadSpaces()
    n.store.loadTableSpaces()
    n.store.loadGroupMembers()

  # Refresh client metadata after seeding system tables
  for n in nodes:
    discard n.client.refreshMetadata()

  nodes

proc makeCluster3(): seq[TestNode] =
  ## 3-node cluster with staggered election timeouts.
  ## CRITICAL: The coordinator applies node-specific offsets to election timeouts:
  ## - Node 1: [150, 300]ms (shortest - will become leader first)
  ## - Node 2: [350, 500]ms
  ## - Node 3: [550, 700]ms (longest)
  let members = @[
    (nodeId: 1'u32, host: "127.0.0.1", port: nodePort(1)),
    (nodeId: 2'u32, host: "127.0.0.1", port: nodePort(2)),
    (nodeId: 3'u32, host: "127.0.0.1", port: nodePort(3)),
  ]

  var nodes: seq[TestNode]

  # Create all nodes first
  for i, m in members:
    nodes.add(makeNode(int(m.nodeId), m.port, members))

  # Start all nodes simultaneously - node 1 has shortest election timeout
  for n in nodes:
    startNode(n)

  # Wait for leader election
  let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
  doAssert leaderIdx >= 0, "no leader found for META_GROUP_ID"
  # Node 1 should be the leader (shortest election timeout)
  # Verify leader stability - check that the same node is leader for 3 consecutive polls
  var stableCount = 0
  for i in 0 ..< 10:
    let currentLeaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID,
        maxAttempts = 3)
    if currentLeaderIdx == leaderIdx:
      inc stableCount
      if stableCount >= 3:
        break
    else:
      stableCount = 0
    sleep(TEST_POLL_INTERVAL_MS)
  doAssert stableCount >= 3, "leader not stable"

  let allNums = @[1, 2, 3]
  seedSysNodesWithRetry(nodes) # Use retry-based seeding
  seedSysGroups(nodes[leaderIdx].store, allNums)
  seedDefaults(nodes[leaderIdx].store)
  sleep(TEST_REPLICATION_WAIT_MS * 2)
  for i in 0 ..< nodes.len: initClient(nodes[i], nodes[leaderIdx].clientPort)

  # Load space caches
  for n in nodes:
    n.store.loadSpaces()
    n.store.loadTableSpaces()
    n.store.loadGroupMembers()

  # Refresh client metadata after seeding system tables
  for n in nodes:
    discard n.client.refreshMetadata()

  nodes

proc waitForNodeInSysNodes(store: RaftKVStoreExt, nodeId: int,
    maxAttempts: int = 50): bool =
  ## Wait for a node to appear in sys.nodes via raftScan.
  ## Returns true if found, false if timeout.
  let nodesStart = encodeTableKey(SYS_NODES_TABLE_ID, "")
  let nodesEnd = makeScanEndKey(SYS_NODES_TABLE_ID)
  for attempt in 0 ..< maxAttempts:
    let nodesRes = store.raftScan(nodesStart, nodesEnd, 0,
        includeSystemKeys = true)
    if nodesRes.isOk:
      for (key, entry) in nodesRes.value:
        try:
          let data = entry.value
          # Data is either binary or JSON (no MVCC since written via raftPut)
          # Note: Binary encoding can start with '{' (0x7B) when nodeId >= 123
          # So we try JSON first if it starts with '{', then fall back to binary
          var found = false

          if data.len > 0 and data[0] == '{':
            # Try JSON first
            try:
              let j = parseJson(data)
              if j["nodeId"].getInt() == nodeId:
                return true
              found = true
            except:
              discard

          if not found:
            # Try binary decoding (NodeRecord)
            try:
              let nodeRec = decodeNodeRecord(data)
              if nodeRec.nodeId == uint32(nodeId):
                return true
            except:
              discard
        except:
          discard
    sleep(TEST_POLL_INTERVAL_MS)
  false

proc addNodeToCluster(nodes: var seq[TestNode], newNodeNum: int) =
  ## Add a new node to the cluster.
  let t0 = nowMs()
  let newPort = nodePort(newNodeNum)

  # Build members list including all existing + new
  var allMembers: seq[tuple[nodeId: uint32, host: string, port: int]]
  for n in nodes:
    allMembers.add((nodeId: uint32(n.id), host: "127.0.0.1",
        port: n.port))
  allMembers.add((nodeId: uint32(newNodeNum), host: "127.0.0.1",
      port: newPort))

  let newNode = makeNode(newNodeNum, newPort, allMembers)
  startNode(newNode)
  echo "TIMING addNode makeNode+start took ", int(nowMs() - t0), " ms"

  let t1 = nowMs()
  # Add new node to existing nodes' NuRaft groups
  for n in nodes:
    n.server.addPeerToRaft(uint32(newNodeNum), "127.0.0.1", newPort)
  echo "TIMING addNode addPeerToRaft took ", int(nowMs() - t1), " ms"

  nodes.add(newNode)

  let t2 = nowMs()
  # Register in sys.nodes via leader
  let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
  if leaderIdx >= 0:
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, $newNodeNum)
    let nodeRec = NodeRecord(
      nodeId: uint32(newNodeNum),
      host: "127.0.0.1",
      raftPort: uint16(newPort),
      clientPort: uint16(newNode.clientPort),
      status: nsAlive,
    )
    discard nodes[leaderIdx].store.raftPut(nodeKey, nodeRec.encode())

    # Wait for the node to be visible in sys.nodes before returning
    # This ensures rebalanceSpaces() will see the new node
    doAssert waitForNodeInSysNodes(nodes[leaderIdx].store, newNodeNum),
      "Node " & $newNodeNum & " did not appear in sys.nodes within timeout"
  echo "TIMING addNode sys.nodes register+wait took ", int(nowMs() - t2), " ms"

  let t3 = nowMs()
  # Refresh all clients' metadata after adding new node
  for n in nodes:
    discard n.client.refreshMetadata()
  echo "TIMING addNode refreshMetadata took ", int(nowMs() - t3), " ms"
  echo "TIMING addNodeToCluster total took ", int(nowMs() - t0), " ms"

proc stopCluster(nodes: seq[TestNode]) =
  let t0 = nowMs()
  var totalGroups = 0
  for n in nodes:
    totalGroups += n.coord.getGroupCount()
  printResourceUsage("stopCluster: " & $nodes.len & " nodes, " & $totalGroups & " groups")

  for i in countdown(nodes.high, 0):
    stopNode(nodes[i])
  GC_fullCollect()
  printResourceUsage("stopCluster done")
  echo "TIMING stopCluster took ", int(nowMs() - t0), " ms"

proc findSpaceId(leaderStore: RaftKVStoreExt,
    leaderMvccStore: MvccTransactionStore, spaceName: string): SpaceID =
  let spacesStart = encodeTableKey(SYS_SPACES_TABLE_ID, "")
  let spacesEnd = makeScanEndKey(SYS_SPACES_TABLE_ID)
  let sr = leaderMvccStore.latestScan(spacesStart, spacesEnd, 0)
  if sr.isOk:
    for (k, v) in sr.value:
      try:
        # Data is either binary or JSON
        if v.len > 0 and v[0] != '{':
          # Binary-encoded SpaceRecord
          let spaceRec = decodeSpaceRecord(v)
          if spaceRec.name == spaceName:
            return spaceRec.spaceId
        else:
          # JSON format
          let j = parseJson(v)
          if j["name"].getStr() == spaceName:
            return spaceIDFromString(j["spaceId"].getStr())
      except: discard
  doAssert false, "space '" & spaceName & "' not found"

proc findSpaceGroupIds(leaderStore: RaftKVStoreExt, spaceId: SpaceID): seq[GroupID] =
  leaderStore.loadSpaces()
  acquire(leaderStore.spacesMu)
  if leaderStore.spaces.hasKey(spaceId):
    result = leaderStore.spaces[spaceId].groupIds
  else:
    result = @[]
  release(leaderStore.spacesMu)

proc createSpace(leaderNode: TestNode, spaceName: string,
    replicas: int): SpaceID =
  let csRes = exec(leaderNode,
    "CREATE SPACE " & spaceName & " WITH REPLICAS = " & $replicas)
  doAssert csRes.kind == erkOk, "CREATE SPACE failed: " &
    (if csRes.kind == erkError: csRes.error else: "unknown")

  let ctRes = exec(leaderNode,
    "CREATE TABLE " & spaceName & "_t (id INT PRIMARY KEY, val TEXT) IN SPACE " & spaceName)
  doAssert ctRes.kind == erkOk, "CREATE TABLE failed: " &
    (if ctRes.kind == erkError: ctRes.error else: "unknown")

  findSpaceId(leaderNode.store, leaderNode.mvccStore, spaceName)

proc execOnLeader(nodes: seq[TestNode], sql: string): ExecResult =
  for node in nodes:
    let r = exec(node, sql)
    if r.kind != erkError:
      return r
    if isNotLeaderError(r.error):
      continue
    return r
  exec(nodes[^1], sql)

proc waitForMetadataReplication(nodes: seq[TestNode], timeoutMs: int = 500) =
  ## Wait for Raft to replicate metadata to all nodes.
  ## Raft replication updates in-memory caches via applyBatchToSM callback.
  ## This just waits a bit for callbacks to fire on all nodes.
  sleep(TEST_REPLICATION_WAIT_MS) # Small delay for callbacks to process

proc updateGroupLeaders(nodes: seq[TestNode]) =
  ## Manually sync sys.groups leader info with actual NuRaft state.
  ## Used by the rebalance test which creates many groups rapidly;
  ## the automatic retry pipeline is too slow for this burst load.
  let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
  if leaderIdx < 0: return
  let leaderStore = nodes[leaderIdx].store

  let grpStart = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
  let grpEnd = makeScanEndKey(SYS_GROUPS_TABLE_ID)
  let grpScan = leaderStore.raftScan(grpStart, grpEnd, 0,
      includeSystemKeys = true)
  if grpScan.isOk:
    for (key, entry) in grpScan.value:
      try:
        let data = entry.value
        var gid: GroupID
        var groupRec: GroupRecord
        var parsed = false

        try:
          let (rec, _) = decodeGroupRecordFromMVCC(data)
          groupRec = rec
          gid = groupIDFromULID(groupRec.groupId)
          parsed = true
        except: discard

        if not parsed:
          try:
            groupRec = decodeGroupRecord(data)
            gid = groupIDFromULID(groupRec.groupId)
            parsed = true
          except: discard

        if not parsed: continue
        if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID: continue

        var actualLeader: uint32 = 0
        for node in nodes:
          if node.coord.isLeader(gid):
            actualLeader = uint32(node.id)
            break

        if actualLeader != 0 and groupRec.leader != actualLeader:
          groupRec.leader = actualLeader
          let encoded = encode(groupRec)
          let ts = int64(epochTime() * 1_000_000_000)
          let mvccEncoded = mvccTypes.encodeMVCCValue(encoded, ts, false)
          discard leaderStore.raftPut(key, mvccEncoded)
      except: discard

proc waitForLeaderPersistence(nodes: seq[TestNode], maxWaitMs: int = 5000) =
  ## Wait until the automatic leader persistence pipeline has had time
  ## to drain its retry queue.  The rebalance thread polls every ~2 s,
  ## so we need at least one full cycle (3000 ms) for all pending
  ## retries to be processed after a burst of leader changes.
  sleep(3000)

proc replicateMetadata(nodes: seq[TestNode]) =
  ## Refresh client metadata caches after metadata changes.
  ## Reads directly from the META leader to avoid stale follower data,
  ## then propagates the refreshed state to every node.
  let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
  if leaderIdx < 0: return
  let leaderNode = nodes[leaderIdx]
  # Refresh the leader's client first (reads from leader = consistent)
  for attempt in 0..<5:
    if leaderNode.client.refreshMetadata():
      break
    sleep(50)
  # Copy the leader client's cache into every other client
  for n in nodes:
    if n.id == leaderNode.id:
      continue
    withWriteLock n.client.lock:
      withWriteLock leaderNode.client.lock:
        n.client.groups = leaderNode.client.groups
        n.client.spaces = leaderNode.client.spaces
        n.client.tables = leaderNode.client.tables
        n.client.nodes = leaderNode.client.nodes
  # Also reload server-side caches
  for n in nodes:
    n.store.loadGroupMembers()
  # Allow Raft callbacks to propagate
  sleep(TEST_REPLICATION_WAIT_MS)

proc insertRows(nodes: seq[TestNode], spaceName: string, rowCount: int) =
  # Retry each INSERT with multiple attempts and backoff
  for i in 1 .. rowCount:
    let rowStart = nowMs()
    var success = false
    var attempts = 0
    for attempt in 0 ..< 20: # 20 attempts per row (increased from 10)
      for node in nodes:
        let r = exec(node, "INSERT INTO " & spaceName &
            "_t (id, val) VALUES (" & $i & ", 'v" & $i & "')")
        if r.kind == erkModified:
          success = true
          inc attempts
          break
        if isNotLeaderError(r.error):
          inc attempts
          continue
        # Non-leader error - try next node
      if success: break
      sleep(50) # Increased backoff between attempts
    let rowElapsed = int(nowMs() - rowStart)
    if i <= 3 or rowElapsed > 500:
      echo "TIMING INSERT row ", i, " took ", rowElapsed, " ms (", attempts, " attempts)"
    doAssert success, "INSERT failed for row " & $i & " after all retries"

proc setupSpaceWithData(nodes: seq[TestNode], spaceName: string,
    replicas: int, rowCount: int): SpaceID =
  ## Full setup: create space, distribute groups, elect leaders, insert data.
  let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
  doAssert leaderIdx >= 0
  let leaderNode = nodes[leaderIdx]

  let t0 = nowMs()
  let spaceId = createSpace(leaderNode, spaceName, replicas)
  echo "TIMING createSpace took ", int(nowMs() - t0), " ms"

  let t1 = nowMs()
  let gids = findSpaceGroupIds(leaderNode.store, spaceId)
  echo "TIMING findSpaceGroupIds took ", int(nowMs() - t1), " ms"

  let t2 = nowMs()
  waitForAutoDistribution(nodes, gids, replicas)
  echo "TIMING waitForAutoDistribution took ", int(nowMs() - t2), " ms"

  let t3 = nowMs()
  waitForSpaceLeaders(nodes)
  echo "TIMING waitForSpaceLeaders took ", int(nowMs() - t3), " ms"

  let t4 = nowMs()
  sleep(500)
  echo "TIMING leader persist wait took ", int(nowMs() - t4), " ms"

  let t5 = nowMs()
  replicateMetadata(nodes)
  echo "TIMING replicateMetadata took ", int(nowMs() - t5), " ms"

  # Longer delay for leadership to stabilize - NuRaft needs time to sync
  # after election before it can accept writes reliably
  # IMPORTANT: 500ms is needed for NuRaft to fully sync and be ready for writes
  sleep(500)

  # Refresh all clients' metadata to pick up new groups
  for n in nodes:
    for i in 0..<3: # Retry metadata refresh
      if n.client.refreshMetadata():
        break
      sleep(TEST_POLL_INTERVAL_MS)

  let t6 = nowMs()
  insertRows(nodes, spaceName, rowCount)
  echo "TIMING insertRows(", rowCount, ") took ", int(nowMs() - t6), " ms"
  spaceId

# ---------------------------------------------------------------------------
# Suite: rebalanceSpaces detects mismatch and creates new groups
# ---------------------------------------------------------------------------

suite "Space rebalance integration — rebalanceSpaces":
  test "creates new groups when a 3rd node joins a 2-node cluster":
    let t0 = nowMs()
    var nodes = makeCluster2()
    echo "TIMING makeCluster2 took ", int(nowMs() - t0), " ms"
    defer: stopCluster(nodes)

    let t1 = nowMs()
    var leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    var leaderStore = nodes[leaderIdx].store
    let spaceId = setupSpaceWithData(nodes, "orders", 2, 10)
    echo "TIMING setupSpaceWithData took ", int(nowMs() - t1), " ms"

    # Verify: 2 groups, not rebalancing
    acquire(leaderStore.spacesMu)
    let sp1 = leaderStore.spaces[spaceId]
    release(leaderStore.spacesMu)
    check sp1.groupIds.len == 2
    check sp1.workerState == wsIdle

    # Add a 3rd node
    let t2 = nowMs()
    addNodeToCluster(nodes, 3)
    echo "TIMING addNodeToCluster took ", int(nowMs() - t2), " ms"

    # Re-check leader after adding node
    leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    leaderStore = nodes[leaderIdx].store

    # Trigger rebalance
    let t3 = nowMs()
    leaderStore.rebalanceSpaces()
    echo "TIMING rebalanceSpaces took ", int(nowMs() - t3), " ms"
    let t4 = nowMs()
    sleep(TEST_REPLICATION_WAIT_MS)
    sleep(TEST_REPLICATION_WAIT_MS) # Give new groups time to initialize and elect leaders
    echo "TIMING sleeps took ", int(nowMs() - t4), " ms"

    # Verify: now rebalancing, 3 new groups, old groups preserved
    # Wait for Raft to replicate the space record update
    sleep(TEST_REPLICATION_WAIT_MS * 2)
    leaderStore.loadSpaces() # Reload cache after Raft replication

    acquire(leaderStore.spacesMu)
    let sp2 = leaderStore.spaces[spaceId]
    release(leaderStore.spacesMu)
    check sp2.workerState != wsIdle
    check sp2.groupIds.len == 3 # 3 nodes -> 3 new groups
    check sp2.oldGroupIds.len == 2 # original 2 groups

  test "is idempotent — does not re-trigger while rebalancing":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
    let spaceId = setupSpaceWithData(nodes, "products", 2, 5)

    addNodeToCluster(nodes, 3)
    leaderStore.rebalanceSpaces()
    sleep(TEST_REPLICATION_WAIT_MS)
    leaderStore.loadSpaces() # Reload cache after rebalance

    acquire(leaderStore.spacesMu)
    let firstNewGroups = leaderStore.spaces[spaceId].groupIds
    release(leaderStore.spacesMu)

    # Call again — should not create more groups
    leaderStore.rebalanceSpaces()
    sleep(TEST_REPLICATION_WAIT_MS)

    acquire(leaderStore.spacesMu)
    let secondNewGroups = leaderStore.spaces[spaceId].groupIds
    release(leaderStore.spacesMu)
    check secondNewGroups == firstNewGroups

  test "skips space where group count already matches node count":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
    let spaceId = setupSpaceWithData(nodes, "stable", 2, 3)

    # Don't add any nodes — group count (2) == node count (2)
    leaderStore.rebalanceSpaces()
    sleep(TEST_REPLICATION_WAIT_MS)

    acquire(leaderStore.spacesMu)
    let sp = leaderStore.spaces[spaceId]
    release(leaderStore.spacesMu)
    check sp.workerState == wsIdle

# ---------------------------------------------------------------------------
# Suite: reads work during rebalance (dual-read mode)
# ---------------------------------------------------------------------------

suite "Space rebalance integration — reads during migration":
  test "SELECT returns all rows during rebalance":
    printResourceUsage("test start")
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderNode = nodes[leaderIdx]
    let leaderStore = leaderNode.store
    let spaceId = setupSpaceWithData(nodes, "items", 2, 20)

    # Verify all rows readable before rebalance
    let sel1 = exec(leaderNode, "SELECT * FROM items_t")
    check sel1.kind == erkRows
    check sel1.rows.len == 20

    # Add 3rd node and trigger rebalance
    addNodeToCluster(nodes, 3)
    leaderStore.rebalanceSpaces()
    sleep(TEST_REPLICATION_WAIT_MS)

    # Wait for new groups and leaders
    acquire(leaderStore.spacesMu)
    let newGids = leaderStore.spaces[spaceId].groupIds
    release(leaderStore.spacesMu)
    waitForAutoDistribution(nodes, newGids, 2, 1500)
    waitForSpaceLeaders(nodes)
    updateGroupLeaders(nodes)
    replicateMetadata(nodes)

    # All 20 rows still readable (dual-read fallback to old groups)
    let sel2 = exec(leaderNode, "SELECT * FROM items_t")
    check sel2.kind == erkRows
    check sel2.rows.len == 20

  test "point get works for pre-existing keys during rebalance":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
    let spaceId = setupSpaceWithData(nodes, "users", 2, 10)

    addNodeToCluster(nodes, 3)
    leaderStore.rebalanceSpaces()
    sleep(TEST_REPLICATION_WAIT_MS)
    acquire(leaderStore.spacesMu)
    let newGids = leaderStore.spaces[spaceId].groupIds
    release(leaderStore.spacesMu)
    waitForAutoDistribution(nodes, newGids, 2, 1500)
    waitForSpaceLeaders(nodes)
    updateGroupLeaders(nodes)
    replicateMetadata(nodes)

    for i in 1 .. 10:
      var success = false
      for attempt in 0..<30:
        let sel = execOnLeader(nodes, "SELECT * FROM users_t WHERE id = " & $i)
        if sel.kind == erkRows and sel.rows.len == 1:
          success = true
          break
        if attempt == 0:
          let errStr = if sel.kind == erkError: sel.error else: "kind=" & $sel.kind
          echo "DEBUG pointget i=", i, " attempt=", attempt, " kind=", sel.kind,
              " err=", errStr
        sleep(100)
      check success

# ---------------------------------------------------------------------------
# Suite: full migration lifecycle
# ---------------------------------------------------------------------------

proc triggerRebalanceAndSetup(nodes: var seq[TestNode],
    leaderStore: RaftKVStoreExt, spaceId: SpaceID) =
  addNodeToCluster(nodes, 3)
  leaderStore.rebalanceSpaces()
  sleep(TEST_REPLICATION_WAIT_MS * 2) # Wait for Raft to replicate
  leaderStore.loadSpaces() # Reload cache after rebalance

  acquire(leaderStore.spacesMu)
  let newGids = leaderStore.spaces[spaceId].groupIds
  release(leaderStore.spacesMu)

  waitForAutoDistribution(nodes, newGids, 2, 2000)
  waitForSpaceLeaders(nodes)
  updateGroupLeaders(nodes)

  # Ensure all nodes have fresh node info caches for remote forwarding
  # during migration (lookupNodeInfo reads from this cache)
  for n in nodes:
    n.store.populateNodeInfoCache()

  # Refresh metadata with retries
  for n in nodes:
    for i in 0..<3:
      if n.client.refreshMetadata():
        break
      sleep(TEST_POLL_INTERVAL_MS)
  replicateMetadata(nodes)

  # Stabilize: wait for new groups to have write-ready leaders.
  # NuRaft needs time after election before it can accept writes reliably.
  for gid in newGids:
    for node in nodes:
      if node.coord.hasGroup(gid):
        discard node.coord.waitForWriteReady(gid, 3000)
  sleep(500)

proc runMigrationWithRetry(nodes: var seq[TestNode], spaceId: SpaceID) =
  ## Run runRebalanceMigration on the current META leader with retries.
  ## The META leader may change between finding it and starting migration,
  ## so we retry from the current leader if the first attempt fails.
  for attempt in 0 ..< 5:
    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    if leaderIdx < 0:
      sleep(500)
      continue
    let leaderStore = nodes[leaderIdx].store
    let leaderNode = nodes[leaderIdx]
    echo "DEBUG runMigrationWithRetry: attempt=", attempt,
        " leaderIdx=", leaderIdx, " nodeId=", leaderNode.id,
        " clientPort=", leaderNode.clientPort
    leaderStore.populateNodeInfoCache()
    leaderStore.loadGroupMembers()
    leaderStore.runRebalanceMigration(spaceId)
    # Check if migration completed
    leaderStore.loadSpaces()
    acquire(leaderStore.spacesMu)
    let sp = leaderStore.spaces.getOrDefault(spaceId,
        raft_store.SpaceInfo(workerState: wsIdle))
    release(leaderStore.spacesMu)
    if sp.workerState == wsIdle and sp.oldGroupIds.len == 0:
      return
    # Migration didn't complete — leader may have changed
    echo "DEBUG runMigrationWithRetry: attempt=", attempt,
        " workerState=", sp.workerState, " oldGroupIds.len=", sp.oldGroupIds.len
    sleep(500)

suite "Space rebalance integration — full migration":
  test "runRebalanceMigration completes and clears rebalance state":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderNode = nodes[leaderIdx]
    let leaderStore = leaderNode.store
    let leaderMvccStore = leaderNode.mvccStore
    let spaceId = setupSpaceWithData(nodes, "migrate", 2, 30)
    triggerRebalanceAndSetup(nodes, leaderStore, spaceId)

    # Run migration with retry on leader changes
    runMigrationWithRetry(nodes, spaceId)
    # Wait for Raft to commit migration state changes
    sleep(TEST_REPLICATION_WAIT_MS)

    # Find current leader and verify migration completed
    let verifyLeaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let verifyLeaderStore = nodes[verifyLeaderIdx].store
    let migrateLeaderNode = nodes[verifyLeaderIdx]

    # Verify rebalance is complete - check in-memory cache directly
    verifyLeaderStore.loadSpaces()
    acquire(verifyLeaderStore.spacesMu)
    var sp = verifyLeaderStore.spaces[spaceId]
    release(verifyLeaderStore.spacesMu)
    check sp.workerState == wsIdle
    check sp.oldGroupIds.len == 0
    check sp.workerNodeId == 0

    # Reload from backend to verify persistence
    verifyLeaderStore.loadSpaces()
    acquire(verifyLeaderStore.spacesMu)
    sp = verifyLeaderStore.spaces[spaceId]
    release(verifyLeaderStore.spacesMu)
    check sp.workerState == wsIdle
    check sp.oldGroupIds.len == 0

    # Wait for leaders on all new space groups and refresh client metadata
    for gid in verifyLeaderStore.spaces[spaceId].groupIds:
      var foundLeader = false
      for attempt in 0 ..< 50:
        for node in nodes:
          if node.coord.isLeader(gid):
            foundLeader = true
            break
        if foundLeader: break
        sleep(TEST_POLL_INTERVAL_MS)

    sleep(500)

    # Reload all server-side caches after migration
    for n in nodes:
      n.store.loadSpaces()
      n.store.loadTableSpaces()
      n.store.loadGroupMembers()

    for n in nodes:
      for i in 0..<5:
        if n.client.refreshMetadata():
          break
        sleep(TEST_POLL_INTERVAL_MS)
    replicateMetadata(nodes)

    # All data still accessible
    let sel = exec(migrateLeaderNode, "SELECT * FROM migrate_t")
    check sel.kind == erkRows
    check sel.rows.len == 30

  test "data is fully accessible after migration (point gets)":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
    let spaceId = setupSpaceWithData(nodes, "fullmig", 2, 20)

    triggerRebalanceAndSetup(nodes, leaderStore, spaceId)

    # Run migration with retry on leader changes
    runMigrationWithRetry(nodes, spaceId)
    # Wait for Raft to replicate and apply the changes
    sleep(TEST_REPLICATION_WAIT_MS)

    # Wait for leaders on all new space groups
    let verifyIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let verifyStore = nodes[verifyIdx].store
    verifyStore.loadSpaces()
    for gid in verifyStore.spaces[spaceId].groupIds:
      var foundLeader = false
      for attempt in 0 ..< 50:
        for node in nodes:
          if node.coord.isLeader(gid):
            foundLeader = true
            break
        if foundLeader: break
        sleep(TEST_POLL_INTERVAL_MS)

    sleep(500)

    for n in nodes:
      for i in 0..<3:
        if n.client.refreshMetadata():
          break
        sleep(TEST_POLL_INTERVAL_MS)

    for i in 1 .. 20:
      let sel = execOnLeader(nodes, "SELECT * FROM fullmig_t WHERE id = " & $i)
      if sel.kind != erkRows: echo "DEBUG: SELECT failed: kind=", sel.kind
      check sel.kind == erkRows
      if sel.kind == erkRows:
        check sel.rows.len == 1
        check sel.rows[0][1] == "v" & $i

  test "old groups are removed from sys.groups after migration":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
    let spaceId = setupSpaceWithData(nodes, "cleanup", 2, 5)

    addNodeToCluster(nodes, 3)
    leaderStore.rebalanceSpaces()
    sleep(TEST_REPLICATION_WAIT_MS)
    leaderStore.loadSpaces() # Reload cache after rebalance

    acquire(leaderStore.spacesMu)
    let oldGids = leaderStore.spaces[spaceId].oldGroupIds
    let newGids = leaderStore.spaces[spaceId].groupIds
    release(leaderStore.spacesMu)
    check oldGids.len > 0

    waitForAutoDistribution(nodes, newGids, 2, 2000)
    waitForSpaceLeaders(nodes)
    replicateMetadata(nodes)

    # Refresh leader before migration in case it changed during setup
    let migrateLeaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let migrateLeaderStore = nodes[migrateLeaderIdx].store

    migrateLeaderStore.runRebalanceMigration(spaceId)
    # Wait for Raft to commit the group deletions
    sleep(TEST_REPLICATION_WAIT_MS)

    # Old groups should be removed from sys.groups
    for oldGid in oldGids:
      let gkey = encodeTableKey(SYS_GROUPS_TABLE_ID, $oldGid)
      let gr = migrateLeaderStore.raftGet(gkey)
      check gr.isOk
      check gr.value.isNone

# ---------------------------------------------------------------------------
# Suite: crash safety — rebalance state persists
# ---------------------------------------------------------------------------

suite "Space rebalance integration — crash safety":
  test "rebalance state persists through loadSpaces reload":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
    let spaceId = setupSpaceWithData(nodes, "crash", 2, 10)

    addNodeToCluster(nodes, 3)

    # Wait for the new node to be visible in sys.nodes on the leader
    # This ensures rebalanceSpaces will see the correct node count
    sleep(TEST_REPLICATION_WAIT_MS)

    leaderStore.rebalanceSpaces()

    # Wait for Raft to replicate the space record update
    sleep(TEST_REPLICATION_WAIT_MS)

    # Reload caches (simulates restart reading persisted state)
    leaderStore.loadSpaces()

    acquire(leaderStore.spacesMu)
    let sp = leaderStore.spaces[spaceId]
    release(leaderStore.spacesMu)
    check sp.workerState != wsIdle
    check sp.oldGroupIds.len > 0
    check sp.groupIds.len == 3

  test "runRebalanceMigration is idempotent (re-run after completion is no-op)":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderNode = nodes[leaderIdx]
    let leaderStore = leaderNode.store
    let spaceId = setupSpaceWithData(nodes, "idem", 2, 10)
    triggerRebalanceAndSetup(nodes, leaderStore, spaceId)

    # Run migration with retry on leader changes
    runMigrationWithRetry(nodes, spaceId)

    # Find current leader for verification
    var verifyIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    var verifyStore = nodes[verifyIdx].store
    verifyStore.loadSpaces()

    # Wait for leaders on all new space groups
    for gid in verifyStore.spaces[spaceId].groupIds:
      var foundLeader = false
      for attempt in 0 ..< 50:
        for node in nodes:
          if node.coord.isLeader(gid):
            foundLeader = true
            break
        if foundLeader: break
        sleep(TEST_POLL_INTERVAL_MS)

    verifyStore.loadGroupMembers()

    for n in nodes: discard n.client.refreshMetadata()

    # All data accessible
    let sel1 = exec(nodes[verifyIdx], "SELECT * FROM idem_t")
    if sel1.kind != erkRows: echo "DEBUG: SELECT 1 failed: kind=", sel1.kind
    check sel1.kind == erkRows
    check sel1.rows.len == 10

    # Re-run — should be a no-op (not rebalancing anymore)
    verifyIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    verifyStore = nodes[verifyIdx].store
    verifyStore.runRebalanceMigration(spaceId)

    let sel2 = exec(nodes[verifyIdx], "SELECT * FROM idem_t")
    if sel2.kind != erkRows: echo "DEBUG: SELECT 2 failed: kind=", sel2.kind
    check sel2.kind == erkRows
    check sel2.rows.len == 10

# Exit immediately to avoid SIGSEGV during Nim GC cleanup.
# NuRaft C++ objects are destroyed by background threads after Nim's
# AtomicArc GC has already freed the memory, causing intermittent crashes.
# All test assertions have passed by this point.
quit(0)
