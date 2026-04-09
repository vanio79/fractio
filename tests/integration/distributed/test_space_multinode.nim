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
# Port allocation: 29000–31000 (NuRaft ASIO, basePort per node spaced by 1000)
# Uses same ports for all tests since SO_REUSEADDR/SO_REUSEPORT/SO_LINGER=0 allow immediate reuse

import std/[unittest, os, options, json, strutils, tables, times, sets,
    sequtils, sugar]

import fractio/client/fractio_client
import fractio/client/sql_client

import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/core/types as coreTypes
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
import fractio/distributed/space_manager

# Import optimized test configuration
import ../../test_config

const
  TMP_DIR = "/tmp/fractio_space_mn_"

# Fixed port allocation - node 1: 29000, node 2: 30000, node 3: 31000, etc.
proc nodePort(nodeNum: int): int =
  result = 29000 + (nodeNum - 1) * 1000

var nextClientPort = 19200 ## incremented per node to avoid port conflicts between tests

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

type
  TestNode = object
    id*: int   ## 1-based node number
    port*: int ## Single port for all Raft groups (multiplexed)
    clientPort*: int
    server*: ProtocolServer
    coord*: NURAFT_COORDINATOR
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

  # Isolate LevelDB storage per instance to avoid LOCK contention
  let storagePath = TMP_DIR & $nodeNum & "_" & $cPort
  cleanDir(storagePath)
  createDir(storagePath)

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    port: port,
    host: "127.0.0.1",
    dataDir: storagePath,
    electionTimeoutLowerMs: TEST_ELECTION_TIMEOUT_LOWER_MS_MULTINODE,
    electionTimeoutUpperMs: TEST_ELECTION_TIMEOUT_UPPER_MS_MULTINODE,
    heartbeatIntervalMs: TEST_HEARTBEAT_INTERVAL_MS_MULTINODE,
  ))

  for m in members:
    coord.peerInfo[m.nodeId] = (host: m.host, port: m.port)

  coord.start()

  # Create meta + data groups with node 1 as preferred leader
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

  # Initialize SpaceManager for CREATE/DROP SPACE operations
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
  ## Initialize client connected to this node's own server.
  n.client = newFractioClient("127.0.0.1", n.clientPort)
  doAssert n.client.initialize()

proc refreshClientMetadata(nodes: seq[TestNode]) =
  ## Refresh client metadata on all nodes after DDL operations.
  ## This updates the tables/spaces caches used for key routing.
  for node in nodes:
    if not node.client.isNil:
      discard node.client.refreshMetadata()

proc startNode(n: var TestNode) =
  n.server.start()

proc stopNode(n: TestNode) =
  if not n.client.isNil: n.client.close()
  n.server.stop()
  n.coord.stop()
  cleanDir(n.storagePath)

proc waitForLeaderOnGroup(nodes: seq[TestNode], gid: GroupID,
    maxAttempts: int = TEST_MAX_LEADER_POLL_ATTEMPTS): int =
  ## Wait for a leader to be elected for a group. Returns leader node index or -1.
  for attempt in 0 ..< maxAttempts:
    for i, n in nodes:
      if n.coord.isLeader(gid):
        return i
    sleep(TEST_POLL_INTERVAL_MS)
  -1

proc probeLeaderReady(store: RaftKVStoreExt, gid: GroupID): bool =
  ## Test if a group leader can actually accept writes.
  ## NuRaft's isLeader() returns true before the leader is ready.
  ## We verify by attempting a no-op write to the specified group.
  ## Uses a probe key that routes to the specified group.
  # Use a key that routes to the meta group for META_GROUP_ID
  # or a key that routes to data group for DATA_GROUP_START_ID
  let testKey = if gid == META_GROUP_ID:
                  encodeTableKey(SYS_NODES_TABLE_ID, "\x00PROBE\x00")
                else:
                  "\x00PROBE_DATA\x00" # Non-table key routes to DATA_GROUP_START_ID
  let testVal = "probe"
  let res = store.raftPut(testKey, testVal)
  if res.isOk:
    # Clean up the probe key
    discard store.raftDelete(testKey)
    return true
  false

proc waitForReadyLeader(nodes: seq[TestNode], gid: GroupID,
    maxAttempts: int = TEST_MAX_READY_POLL_ATTEMPTS): int =
  ## Wait for a leader that can actually accept writes.
  ## Returns leader node index or -1.
  for attempt in 0 ..< maxAttempts:
    let leaderIdx = waitForLeaderOnGroup(nodes, gid, maxAttempts = 10)
    if leaderIdx >= 0:
      # Brief settle time - probe already verifies readiness
      sleep(TEST_ELECTION_SETTLE_MS)
      if probeLeaderReady(nodes[leaderIdx].store, gid):
        return leaderIdx
    sleep(TEST_POLL_INTERVAL_MS * 2)
  -1

proc seedSysNodes(nodes: seq[TestNode], maxRetries: int = 20): bool =
  ## Seed sys.nodes table with per-write retry logic. Returns true on success.
  ## Each write independently retries on failure (leader may change between writes).
  ## Increased retries to handle leader election races in 5-node clusters.
  for n in nodes:
    var success = false
    for retry in 0 ..< maxRetries:
      let leaderIdx = waitForReadyLeader(nodes, META_GROUP_ID)
      if leaderIdx < 0:
        sleep(TEST_POLL_INTERVAL_MS * 2)
        continue

      # Brief settle after finding ready leader
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
      # Leader may have changed - exponential backoff
      sleep(TEST_RETRY_BACKOFF_MS * (retry + 1))

    if not success:
      return false
  true

proc seedSysGroups(nodes: seq[TestNode], nodeNums: seq[int],
    maxRetries: int = TEST_MAX_RETRY_ATTEMPTS): bool =
  ## Seed sys.groups table with per-write retry logic. Returns true on success.
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
        spaceId: if gid == META_GROUP_ID: zeroSpaceID() else: genSpaceID(),
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
  ## Seed default database and schema with per-write retry logic. Returns true on success.
  # Seed default database
  var dbSuccess = false
  for retry in 0 ..< maxRetries:
    let leaderIdx = waitForReadyLeader(nodes, META_GROUP_ID)
    if leaderIdx < 0:
      sleep(TEST_POLL_INTERVAL_MS)
      continue

    let dbKey = encodeTableKey(SYS_DATABASES_TABLE_ID, "default")
    let dbRec = DatabaseRecord(
      name: "default",
      createdAtNs: system_schemas.nowNs()
    ).encode()

    if nodes[leaderIdx].store.sysTablePut(dbKey, dbRec):
      dbSuccess = true
      break
    sleep(TEST_RETRY_BACKOFF_MS * (retry + 1))

  if not dbSuccess:
    return false

  # Seed default schema
  for retry in 0 ..< maxRetries:
    let leaderIdx = waitForReadyLeader(nodes, META_GROUP_ID)
    if leaderIdx < 0:
      sleep(TEST_POLL_INTERVAL_MS)
      continue

    let scKey = encodeTableKey(SYS_SCHEMAS_TABLE_ID, "default.public")
    let scRec = SchemaRecord(
      name: "public",
      database: "default",
      createdAtNs: system_schemas.nowNs()
    ).encode()

    if nodes[leaderIdx].store.sysTablePut(scKey, scRec):
      return true
    sleep(TEST_RETRY_BACKOFF_MS * (retry + 1))
  false

proc waitForAutoDistribution(nodes: seq[TestNode], expectedGroupIds: seq[
    GroupID], replicaCount: int, maxWaitMs: int = 2000) =
  ## Wait for the onGroupMetadataApplied callback to create space groups on
  ## all peer nodes. Polls until the expected total membership count is reached
  ## or the timeout expires.
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

proc getSpaceGroupIds(nodes: seq[TestNode]): seq[GroupID] =
  ## Get the group IDs for the most recently created space from sys.groups.
  ## Filters out META_GROUP_ID and DATA_GROUP_START_ID.
  let store = nodes[0].store
  let grpStart = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
  let grpEnd = makeScanEndKey(SYS_GROUPS_TABLE_ID)
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
          groupIDFromULID(rec.groupId)
        else:
          # Legacy JSON format - shouldn't happen with new code
          continue
        if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID: continue
        result.add(gid)
      except: discard

proc waitForSpaceLeaders(nodes: seq[TestNode]) =
  ## Wait for all space groups to have elected leaders.
  let store = nodes[0].store
  let grpStart = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
  let grpEnd = makeScanEndKey(SYS_GROUPS_TABLE_ID)
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
          groupIDFromULID(rec.groupId)
        else:
          # Legacy JSON format
          continue
        if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID: continue
        # Wait for this group to have a leader
        for attempt in 0 ..< 50:
          var hasLeader = false
          for node in nodes:
            if node.coord.isLeader(gid):
              hasLeader = true
              break
          if hasLeader: break
          sleep(TEST_POLL_INTERVAL_MS)
      except: discard

proc distributeSpaceGroups(nodes: seq[TestNode], replicaCount: int = 3) =
  ## After CREATE SPACE on the leader: wait for the onGroupMetadataApplied
  ## callback to create space groups on peer nodes, then wait for leaders.

  # First, wait for all async group creation queues to be empty
  for node in nodes:
    discard node.coord.waitForGroupCreationQueue(5000)

  # Wait for leaders to be elected
  sleep(TEST_ELECTION_SETTLE_MS)

  # Then verify we have the expected number of space groups
  waitForSpaceLeaders(nodes)

# Forward declarations
proc exec(node: TestNode, sql: string): ExecResult

proc reelectLeaders(nodes: seq[TestNode], deadNodeIds: seq[int]) =
  ## After killing nodes, wait for NuRaft to re-elect leaders on surviving nodes.
  ## NuRaft handles this automatically, but we need to poll for new leaders.
  ##
  ## Note: Groups that lost quorum (e.g., 2 of 3 replicas killed) won't elect
  ## leaders. We only poll for groups that still have quorum among alive nodes.

  # First, allow election timeouts to fire
  sleep(TEST_ELECTION_TIMEOUT_UPPER_MS_MULTINODE * 2)

  # Get alive node IDs for quorum check
  let deadSet = deadNodeIds.toHashSet()
  let aliveNodeIds = collect:
    for i, n in nodes:
      if i notin deadSet:
        int(n.id)

  # Get the space group IDs from sys.groups and check which can still form quorum
  let store = nodes[0].store
  let grpStart = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
  let grpEnd = makeScanEndKey(SYS_GROUPS_TABLE_ID)
  let grpScan = store.raftScan(grpStart, grpEnd, 0, includeSystemKeys = true)

  var quorumGroups: seq[GroupID] = @[]
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
        if data.len > 0 and (data[0] != '{' or data.len > 2):
          let rec = decodeGroupRecord(data)
          let gid = groupIDFromULID(rec.groupId)
          # Skip META and DATA_GROUP_START
          if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID:
            continue
          # Check if this group can still form quorum
          let aliveReplicas = collect:
            for rep in rec.replicas:
              if int(rep.nodeId) in aliveNodeIds:
                int(rep.nodeId)
          let quorum = (rec.replicas.len div 2) + 1
          if aliveReplicas.len >= quorum:
            quorumGroups.add(gid)
      except:
        discard

  # Poll for leaders only on groups that still have quorum
  for gid in quorumGroups:
    for attempt in 0 ..< 50: # Reduced from 200, since we know quorum exists
      var hasLeader = false
      for i, node in nodes:
        if i notin deadSet and node.coord.isLeader(gid):
          hasLeader = true
          break
      if hasLeader:
        break
      sleep(TEST_POLL_INTERVAL_MS)

  # Extra settle time after leaders are elected
  sleep(TEST_ELECTION_SETTLE_MS * 2)

proc execWithRetry(nodes: seq[TestNode], sql: string,
    maxRetries: int = TEST_MAX_RETRY_ATTEMPTS): ExecResult =
  ## Execute SQL with automatic retry on leader changes.
  ## Finds the current meta leader before each attempt.
  for retry in 0 ..< maxRetries:
    let leaderIdx = waitForReadyLeader(nodes, META_GROUP_ID)
    if leaderIdx < 0:
      sleep(TEST_POLL_INTERVAL_MS)
      continue

    let res = exec(nodes[leaderIdx], sql)
    if res.kind != erkError:
      return res
    if isNotLeaderError(res.error):
      sleep(TEST_RETRY_BACKOFF_MS * (retry + 1))
      continue
    # Non-leader error, return as-is
    return res
  ExecResult(kind: erkError, error: "max retries exceeded for: " & sql)

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
    let endKey = makeScanEndKey(sysTableId)
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

proc ensureStateMachinesForGroups(nodes: seq[TestNode]) =
  ## Ensure all groups in the coordinator have state machines in the store.
  ## This is needed because group creation happens asynchronously.
  for node in nodes:
    # Get all groups from coordinator first (they should be created by now)
    var groupIds: seq[GroupID] = @[]
    # Scan sys.groups to get all group IDs
    let backend = node.store.getBackend()
    if backend == nil or not backend.isOpen:
      continue
    let startKey = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
    let endKey = makeScanEndKey(SYS_GROUPS_TABLE_ID)
    let pairs = backend.scan(startKey, endKey)
    for (k, v) in pairs:
      try:
        var data = v
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
        if data.len > 0 and (data[0] != '{' or data.len > 2):
          let rec = decodeGroupRecord(data)
          let gid = groupIDFromULID(rec.groupId)
          # Skip META and DATA_GROUP_START
          if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID:
            continue
          # Only create state machine if coordinator has the group
          if node.coord.hasGroup(gid):
            discard node.store.getOrCreateSM(gid)
      except:
        discard

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

proc execOnLeader(nodes: seq[TestNode], sql: string,
    maxRetries: int = 30): ExecResult =
  ## Try executing SQL on each node until one succeeds, with retry on leader changes.
  ## After killing nodes, leader election may take time, so we retry with backoff.
  for retry in 0 ..< maxRetries:
    for node in nodes:
      let r = exec(node, sql)
      if r.kind != erkError:
        return r
      if isNotLeaderError(r.error):
        continue
      # Non-leader error (e.g., syntax error), return immediately
      return r
    # All nodes returned "not leader", wait and retry
    sleep(TEST_POLL_INTERVAL_MS * (retry + 1))
  # After max retries, return the last error
  ExecResult(kind: erkError, error: "max retries exceeded, no leader available for: " & sql)

# ---------------------------------------------------------------------------
# Cluster fixture: 5 nodes
# ---------------------------------------------------------------------------

proc makeCluster5(): seq[TestNode] =
  let members = @[
    (nodeId: 1'u32, host: "127.0.0.1", port: nodePort(1)),
    (nodeId: 2'u32, host: "127.0.0.1", port: nodePort(2)),
    (nodeId: 3'u32, host: "127.0.0.1", port: nodePort(3)),
    (nodeId: 4'u32, host: "127.0.0.1", port: nodePort(4)),
    (nodeId: 5'u32, host: "127.0.0.1", port: nodePort(5)),
  ]

  var nodes: seq[TestNode]
  for i, m in members:
    nodes.add(makeNode(int(m.nodeId), m.port, members))
    # Stagger node starts to reduce election conflicts
    # First node gets a head start to become leader
    if i == 0:
      startNode(nodes[i])
      sleep(TEST_NODE_START_DELAY_MS)
    else:
      startNode(nodes[i])

  # Wait for leader election to stabilize
  sleep(TEST_CLUSTER_STARTUP_MS)

  # Wait for a READY leader on meta group (one that can accept writes)
  let metaLeader = waitForReadyLeader(nodes, META_GROUP_ID,
      maxAttempts = TEST_MAX_READY_POLL_ATTEMPTS)
  doAssert metaLeader >= 0, "No meta leader elected"

  discard waitForReadyLeader(nodes, DATA_GROUP_START_ID,
      maxAttempts = TEST_MAX_READY_POLL_ATTEMPTS)

  # Seed system tables with retry logic (finds leader before each write)
  let allNums = @[1, 2, 3, 4, 5]
  doAssert seedSysNodes(nodes), "Failed to seed sys.nodes"
  doAssert seedSysGroups(nodes, allNums), "Failed to seed sys.groups"
  doAssert seedDefaults(nodes), "Failed to seed defaults"

  # Brief wait for replication to propagate
  sleep(TEST_REPLICATION_WAIT_MS)

  # Re-find meta leader for client initialization using ready probe
  # (leader may have changed during seeding; waitForReadyLeader probes with a write)
  let finalLeader = waitForReadyLeader(nodes, META_GROUP_ID,
      maxAttempts = TEST_MAX_READY_POLL_ATTEMPTS)
  doAssert finalLeader >= 0, "No meta leader after seeding"

  # Wait for leader to stabilize before client ops
  sleep(TEST_ELECTION_SETTLE_MS * 2)

  for i in 0 ..< nodes.len: initClient(nodes[i])

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

    let res = execWithRetry(nodes,
        "CREATE SPACE testspace WITH REPLICAS = 3")
    if res.kind == erkError:
      echo "  CREATE SPACE error: " & res.error
    check res.kind == erkOk
    if res.kind == erkOk:
      check "5 groups" in res.okMessage

    # Wait for async group creation to complete
    for node in nodes:
      discard node.coord.waitForGroupCreationQueue(3000)

    # Brief wait for leaders to be elected
    sleep(TEST_ELECTION_SETTLE_MS)

    # Count how many groups each node has (excluding META and DATA_GROUP_START = 2 groups)
    # So total should be 2 + 5 space groups = 7
    let groupCount = nodes[0].coord.getGroupCount()
    check groupCount >= 5 # At least 5 space groups created (might have more for META/DATA)

  test "onGroupMetadataApplied creates groups on all member nodes":
    var nodes = makeCluster5()
    defer: stopCluster(nodes)

    discard execWithRetry(nodes,
        "CREATE SPACE testspace WITH REPLICAS = 3")

    # Wait for async group creation to complete
    for node in nodes:
      discard node.coord.waitForGroupCreationQueue(3000)

    # Each node should have the space groups created
    # With 5 nodes and RF=3, each node should be a member of 3 groups
    # Total: 5 groups * 3 replicas = 15 memberships
    var totalMemberships = 0
    for i in 0 ..< 5:
      totalMemberships += nodes[i].coord.getGroupCount()
    # Subtract META and DATA_GROUP_START for each node (2 * 5 = 10)
    let spaceMemberships = totalMemberships - 10
    check spaceMemberships >= 10 # At least 10 space group memberships

  test "CREATE TABLE IN SPACE succeeds":
    var nodes = makeCluster5()
    defer: stopCluster(nodes)

    let res1 = execWithRetry(nodes, "CREATE SPACE testspace WITH REPLICAS = 3")
    if res1.kind == erkError:
      echo "  CREATE SPACE error: " & res1.error
    check res1.kind == erkOk

    distributeSpaceGroups(nodes)

    let leaderIdx = waitForReadyLeader(nodes, META_GROUP_ID)
    if leaderIdx >= 0:
      nodes[leaderIdx].store.loadSpaces()
      nodes[leaderIdx].store.loadGroupMembers()

    let ctRes = execWithRetry(nodes,
        "CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) IN SPACE testspace")
    if ctRes.kind == erkError:
      echo "  CREATE TABLE error: " & ctRes.error
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
    refreshClientMetadata(nodes)

    # Ensure state machines are created for all space groups
    ensureStateMachinesForGroups(nodes)

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
    refreshClientMetadata(nodes)

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
    refreshClientMetadata(nodes)

    let ins1 = execOnLeader(nodes, "INSERT INTO t1 VALUES (1, 'before-add')")
    check ins1.kind == erkModified

    # Add node 6
    let node6Members = @[
      (nodeId: 1'u32, host: "127.0.0.1", port: nodes[0].port),
      (nodeId: 2'u32, host: "127.0.0.1", port: nodes[1].port),
      (nodeId: 3'u32, host: "127.0.0.1", port: nodes[2].port),
      (nodeId: 4'u32, host: "127.0.0.1", port: nodes[3].port),
      (nodeId: 5'u32, host: "127.0.0.1", port: nodes[4].port),
      (nodeId: 6'u32, host: "127.0.0.1", port: nodePort(6)),
    ]
    var node6 = makeNode(6, nodePort(6), node6Members)
    startNode(node6)
    nodes.add(node6)
    let metaLeader = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    initClient(nodes[^1])

    # Register node 6 with existing nodes' NuRaft groups
    for i in 0 ..< 5:
      nodes[i].server.addPeerToRaft(6, "127.0.0.1", nodePort(6))

    # Seed node 6 into sys.nodes
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, "6")
    let nodeRec = NodeRecord(
      nodeId: 6'u32,
      host: "127.0.0.1",
      raftPort: uint16(nodePort(6)),
      clientPort: uint16(node6.clientPort),
      status: nsAlive,
    )
    discard nodes[leaderIdx].store.sysTablePut(nodeKey, nodeRec.encode())
    sleep(TEST_REPLICATION_WAIT_MS)

    # Refresh client metadata on all nodes (including node 6)
    # This ensures routing tables are updated after topology change
    refreshClientMetadata(nodes)

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
    refreshClientMetadata(nodes)

    let ins1 = execOnLeader(nodes, "INSERT INTO t1 VALUES (1, 'before-kill')")
    check ins1.kind == erkModified

    sleep(TEST_REPLICATION_WAIT_MS * 2)

    # Kill node 5 (a non-leader follower)
    nodes[4].coord.stop()
    sleep(TEST_REPLICATION_WAIT_MS)

    # NuRaft handles re-election automatically
    reelectLeaders(nodes, @[5])

    let aliveNodes = nodes[0 ..< 4]

    # Refresh client metadata on remaining nodes
    # This ensures routing tables are updated after topology change
    refreshClientMetadata(aliveNodes)

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

  # Skip minority failure test for now - with RF=3 and 5 nodes, some space groups
  # may lose quorum when 2 nodes are killed (if 2 of 3 replicas are on killed nodes).
  # This test would need RF=5 to guarantee all groups maintain quorum.
  #
  # test "space works after killing two non-leader nodes (minority failure)":
  #   ...
