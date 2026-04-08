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
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider
import fractio/storage/wisckey_backend
import fractio/storage/mvcc/types as mvccTypes
import fractio/sql/executor
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/distributed/space_manager

# Import optimized test configuration
import ../../test_config



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
  let nodeId = rangeTypes.NodeID(uint32(nodeNum))
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
  let tsProvider = newTimestampProvider(mockTimer, rangeTypes.NodeID(uint32(
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
  echo "=== stopNode ", n.id, " starting ==="
  if n.client != nil:
    n.client.close()
  n.store.stop()
  echo "=== stopNode ", n.id, " store stopped ==="
  n.server.stop()
  echo "=== stopNode ", n.id, " server stopped ==="
  n.coord.stop()
  echo "=== stopNode ", n.id, " coord stopped ==="
  sleep(TEST_SHUTDOWN_DELAY_MS) # Give LevelDB a moment to release its lock
  cleanDir(n.storagePath)
  echo "=== stopNode ", n.id, " done ==="

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
      spaceId: ZeroULID(),
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
    spaceId: ULID(zeroSpaceID()),
    name: "default",
    replicas: 0,
    groupCount: 1,
    groupIds: @[groupIDToULID(META_GROUP_ID)],
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

proc updateGroupLeaders(nodes: seq[TestNode]) =
  ## Update sys.groups with actual leader info from the coordinator.
  ## This is needed because onLeaderChanged only persists if the node is meta leader.
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

        # Try MVCC-aware binary decoding first
        try:
          let (rec, _) = decodeGroupRecordFromMVCC(data)
          groupRec = rec
          gid = groupIDFromULID(groupRec.groupId)
          parsed = true
        except:
          discard

        if not parsed:
          # Try raw binary decoding
          try:
            groupRec = decodeGroupRecord(data)
            gid = groupIDFromULID(groupRec.groupId)
            parsed = true
          except:
            discard

        if not parsed:
          continue

        # Skip meta and default data groups
        if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID:
          continue

        # Find the actual leader from the coordinator
        var actualLeader: uint32 = 0
        for node in nodes:
          if node.coord.isLeader(gid):
            actualLeader = uint32(node.id)
            break

        # Update the group record if leader differs
        if actualLeader != 0 and groupRec.leader != actualLeader:
          groupRec.leader = actualLeader
          let encoded = encode(groupRec)
          let ts = int64(epochTime() * 1_000_000_000)
          let mvccEncoded = mvccTypes.encodeMVCCValue(encoded, ts, false)
          discard leaderStore.raftPut(key, mvccEncoded)
      except:
        discard

proc exec(node: TestNode, sql: string, database = "default",
    schema = "public"): ExecResult =
  node.client.query(sql, database, schema)

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

  # Add new node to existing nodes' NuRaft groups
  for n in nodes:
    n.server.addPeerToRaft(uint32(newNodeNum), "127.0.0.1", newPort)

  nodes.add(newNode)

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

  # Refresh all clients' metadata after adding new node
  for n in nodes:
    discard n.client.refreshMetadata()

proc stopCluster(nodes: seq[TestNode]) =
  # Log group counts before shutdown
  var totalGroups = 0
  for n in nodes:
    totalGroups += n.coord.getGroupCount()
  printResourceUsage("stopCluster: " & $nodes.len & " nodes, " & $totalGroups & " groups")

  for i in countdown(nodes.high, 0):
    stopNode(nodes[i])
  # Force garbage collection to release memory from previous test
  # before starting the next one. Without this, sequential tests
  # accumulate memory until the process is OOM killed.
  GC_fullCollect()
  printResourceUsage("stopCluster done")

proc findSpaceId(leaderStore: RaftKVStoreExt,
    leaderMvccStore: MvccTransactionStore, spaceName: string): ULID =
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
            return ulidFromString(j["spaceId"].getStr())
      except: discard
  doAssert false, "space '" & spaceName & "' not found"

proc findSpaceGroupIds(leaderStore: RaftKVStoreExt, spaceId: ULID): seq[GroupID] =
  leaderStore.loadSpaces()
  let sid = SpaceID(spaceId) # Convert ULID to SpaceID for table lookup
  acquire(leaderStore.spacesMu)
  if leaderStore.spaces.hasKey(sid):
    result = leaderStore.spaces[sid].groupIds
  else:
    result = @[]
  release(leaderStore.spacesMu)

proc createSpace(leaderNode: TestNode, spaceName: string, replicas: int): ULID =
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

proc replicateMetadata(nodes: seq[TestNode]) =
  ## Deprecated: Use waitForMetadataReplication instead.
  ## This is now a no-op since Raft handles replication and applyBatchToSM
  ## updates in-memory caches automatically.
  sleep(TEST_REPLICATION_WAIT_MS) # Small delay for Raft callbacks to process

proc insertRows(nodes: seq[TestNode], spaceName: string, rowCount: int) =
  # Retry each INSERT with multiple attempts and backoff
  for i in 1 .. rowCount:
    var success = false
    for attempt in 0 ..< 20: # 20 attempts per row (increased from 10)
      for node in nodes:
        let r = exec(node, "INSERT INTO " & spaceName &
            "_t (id, val) VALUES (" & $i & ", 'v" & $i & "')")
        if r.kind == erkModified:
          success = true
          break
        if isNotLeaderError(r.error):
          continue
        # Non-leader error - try next node
      if success: break
      sleep(50) # Increased backoff between attempts
    doAssert success, "INSERT failed for row " & $i & " after all retries"

proc setupSpaceWithData(nodes: seq[TestNode], spaceName: string,
    replicas: int, rowCount: int): ULID =
  ## Full setup: create space, distribute groups, elect leaders, insert data.
  let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
  doAssert leaderIdx >= 0
  let leaderNode = nodes[leaderIdx]

  let spaceId = createSpace(leaderNode, spaceName, replicas)
  let gids = findSpaceGroupIds(leaderNode.store, spaceId)

  waitForAutoDistribution(nodes, gids, replicas)
  waitForSpaceLeaders(nodes)
  updateGroupLeaders(nodes) # Update sys.groups with actual leaders
  replicateMetadata(nodes)

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

  insertRows(nodes, spaceName, rowCount)
  spaceId

# ---------------------------------------------------------------------------
# Suite: rebalanceSpaces detects mismatch and creates new groups
# ---------------------------------------------------------------------------

suite "Space rebalance integration — rebalanceSpaces":
  test "creates new groups when a 3rd node joins a 2-node cluster":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    var leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    var leaderStore = nodes[leaderIdx].store
    let spaceId = setupSpaceWithData(nodes, "orders", 2, 10)

    # Verify: 2 groups, not rebalancing
    acquire(leaderStore.spacesMu)
    let sp1 = leaderStore.spaces[SpaceID(spaceId)]
    release(leaderStore.spacesMu)
    check sp1.groupIds.len == 2
    check sp1.rebalancing == false

    # Add a 3rd node
    addNodeToCluster(nodes, 3)

    # Re-check leader after adding node
    leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    leaderStore = nodes[leaderIdx].store

    # Trigger rebalance
    leaderStore.rebalanceSpaces()
    sleep(TEST_REPLICATION_WAIT_MS)
    sleep(TEST_REPLICATION_WAIT_MS) # Give new groups time to initialize and elect leaders

    # Verify: now rebalancing, 3 new groups, old groups preserved
    # Wait for Raft to replicate the space record update
    sleep(TEST_REPLICATION_WAIT_MS * 2)
    leaderStore.loadSpaces() # Reload cache after Raft replication

    acquire(leaderStore.spacesMu)
    let sp2 = leaderStore.spaces[SpaceID(spaceId)]
    release(leaderStore.spacesMu)
    check sp2.rebalancing == true
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
    let firstNewGroups = leaderStore.spaces[SpaceID(spaceId)].groupIds
    release(leaderStore.spacesMu)

    # Call again — should not create more groups
    leaderStore.rebalanceSpaces()
    sleep(TEST_REPLICATION_WAIT_MS)

    acquire(leaderStore.spacesMu)
    let secondNewGroups = leaderStore.spaces[SpaceID(spaceId)].groupIds
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
    let sp = leaderStore.spaces[SpaceID(spaceId)]
    release(leaderStore.spacesMu)
    check sp.rebalancing == false

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
    let newGids = leaderStore.spaces[SpaceID(spaceId)].groupIds
    release(leaderStore.spacesMu)
    waitForAutoDistribution(nodes, newGids, 2, 1500)
    waitForSpaceLeaders(nodes)
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
    let newGids = leaderStore.spaces[SpaceID(spaceId)].groupIds
    release(leaderStore.spacesMu)
    waitForAutoDistribution(nodes, newGids, 2, 1500)
    waitForSpaceLeaders(nodes)
    replicateMetadata(nodes)

    for i in 1 .. 10:
      let sel = execOnLeader(nodes, "SELECT * FROM users_t WHERE id = " & $i)
      if sel.kind != erkRows: echo "DEBUG: SELECT failed: ", sel.error
      check sel.kind == erkRows
      if sel.kind == erkRows:
        check sel.rows.len == 1
        check sel.rows[0][0] == $i

# ---------------------------------------------------------------------------
# Suite: full migration lifecycle
# ---------------------------------------------------------------------------

proc triggerRebalanceAndSetup(nodes: var seq[TestNode],
    leaderStore: RaftKVStoreExt, spaceId: ULID) =
  addNodeToCluster(nodes, 3)
  leaderStore.rebalanceSpaces()
  sleep(TEST_REPLICATION_WAIT_MS * 2) # Wait for Raft to replicate
  leaderStore.loadSpaces() # Reload cache after rebalance

  acquire(leaderStore.spacesMu)
  let newGids = leaderStore.spaces[SpaceID(spaceId)].groupIds
  release(leaderStore.spacesMu)

  waitForAutoDistribution(nodes, newGids, 2, 2000)
  waitForSpaceLeaders(nodes)
  # Refresh metadata with retries
  for n in nodes:
    for i in 0..<3:
      if n.client.refreshMetadata():
        break
      sleep(TEST_POLL_INTERVAL_MS)
  replicateMetadata(nodes)

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

    # Verify rebalancing is active
    acquire(leaderStore.spacesMu)
    var sp = leaderStore.spaces[SpaceID(spaceId)]
    release(leaderStore.spacesMu)
    check sp.rebalancing == true

    # Run migration
    leaderStore.runRebalanceMigration(spaceId)
    # Wait for Raft to commit migration state changes
    sleep(TEST_REPLICATION_WAIT_MS)

    # Verify rebalance is complete - check in-memory cache directly
    acquire(leaderStore.spacesMu)
    sp = leaderStore.spaces[SpaceID(spaceId)]
    release(leaderStore.spacesMu)
    check sp.rebalancing == false
    check sp.oldGroupIds.len == 0
    check sp.rebalanceWorker == 0

    # Reload from backend to verify persistence
    leaderStore.loadSpaces()
    acquire(leaderStore.spacesMu)
    sp = leaderStore.spaces[SpaceID(spaceId)]
    release(leaderStore.spacesMu)
    check sp.rebalancing == false
    check sp.oldGroupIds.len == 0

    # All data still accessible
    let sel = exec(leaderNode, "SELECT * FROM migrate_t")
    check sel.kind == erkRows
    check sel.rows.len == 30

  test "data is fully accessible after migration (point gets)":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
    let spaceId = setupSpaceWithData(nodes, "fullmig", 2, 20)

    triggerRebalanceAndSetup(nodes, leaderStore, spaceId)

    leaderStore.runRebalanceMigration(spaceId)
    # Wait for Raft to replicate and apply the changes
    sleep(TEST_REPLICATION_WAIT_MS)

    # Wait for leaders on all new space groups
    for gid in leaderStore.spaces[SpaceID(spaceId)].groupIds:
      var foundLeader = false
      for attempt in 0 ..< 50:
        for node in nodes:
          if node.coord.isLeader(gid):
            foundLeader = true
            break
        if foundLeader: break
        sleep(TEST_POLL_INTERVAL_MS)

    for n in nodes: discard n.client.refreshMetadata()
    for i in 1 .. 20:
      let sel = execOnLeader(nodes, "SELECT * FROM fullmig_t WHERE id = " & $i)
      if sel.kind != erkRows: echo "DEBUG: SELECT failed: ", sel.error
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
    let oldGids = leaderStore.spaces[SpaceID(spaceId)].oldGroupIds
    let newGids = leaderStore.spaces[SpaceID(spaceId)].groupIds
    release(leaderStore.spacesMu)
    check oldGids.len > 0

    waitForAutoDistribution(nodes, newGids, 2, 2000)
    waitForSpaceLeaders(nodes)
    replicateMetadata(nodes)

    leaderStore.runRebalanceMigration(spaceId)
    # Wait for Raft to commit the group deletions
    sleep(TEST_REPLICATION_WAIT_MS)

    # Old groups should be removed from sys.groups
    for oldGid in oldGids:
      let gkey = encodeTableKey(SYS_GROUPS_TABLE_ID, $oldGid)
      let gr = leaderStore.raftGet(gkey)
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
    let sp = leaderStore.spaces[SpaceID(spaceId)]
    release(leaderStore.spacesMu)
    check sp.rebalancing == true
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

    leaderStore.runRebalanceMigration(spaceId)
    leaderStore.loadSpaces()

    # Wait for leaders on all new space groups
    for gid in leaderStore.spaces[SpaceID(spaceId)].groupIds:
      var foundLeader = false
      for attempt in 0 ..< 50:
        for node in nodes:
          if node.coord.isLeader(gid):
            foundLeader = true
            break
        if foundLeader: break
        sleep(TEST_POLL_INTERVAL_MS)

    leaderStore.loadGroupMembers()

    for n in nodes: discard n.client.refreshMetadata()

    # All data accessible
    let sel1 = exec(leaderNode, "SELECT * FROM idem_t")
    if sel1.kind != erkRows: echo "DEBUG: SELECT 1 failed: ", sel1.error
    check sel1.kind == erkRows
    check sel1.rows.len == 10

    # Re-run — should be a no-op (not rebalancing anymore)
    leaderStore.runRebalanceMigration(spaceId)

    let sel2 = exec(leaderNode, "SELECT * FROM idem_t")
    if sel2.kind != erkRows: echo "DEBUG: SELECT 2 failed: ", sel2.error
    check sel2.kind == erkRows
    check sel2.rows.len == 10
