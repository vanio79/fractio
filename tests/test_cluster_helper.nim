# Test Cluster Helper Module
#
# Provides a unified API for creating and managing test clusters with
# parallel startup support for faster test execution.
#
# Usage:
#   import test_cluster_helper
#
#   # Per-test cluster (creates new cluster for each test)
#   var cluster = newTestCluster(defaultTestClusterConfig())
#   defer: cluster.stop()
#
#   # Shared fixture (cluster shared across tests in a suite)
#   var fixture = newSharedClusterFixture(defaultTestClusterConfig())
#   suite "My tests":
#     setup:
#       fixture.setup()
#     teardown:
#       fixture.teardown()
#     test "example":
#       let cluster = fixture.get()
#       # use cluster...

import std/[os, locks, tables, options]

import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/protocol/raft_store

import test_config

# ============================================================================
# Types
# ============================================================================

type
  MemberInfo* = tuple
    nodeId: uint32
    host: string
    basePort: int

  TestClusterConfig* = object
    ## Configuration for a test cluster
    nodeCount*: int
    basePort*: int               ## Starting port (each node uses basePort + (nodeId-1)*1000)
    portOffset*: int             ## Additional offset to avoid port conflicts between tests
    preferredLeader*: uint32     ## Which node should be preferred leader (default: 1)
    electionTimeoutLowerMs*: int32
    electionTimeoutUpperMs*: int32
    heartbeatIntervalMs*: int32
    parallelStartup*: bool       ## Create nodes in parallel (default: true)
    parallelGroupCreation*: bool ## Create groups in parallel (default: true)
    seedSystemTables*: bool      ## Automatically seed sys.nodes and sys.groups (default: true)

  TestNode* = object
    ## A single test node with its coordinator and store
    id*: int
    nodeId*: uint32
    basePort*: int
    coord*: NuRaftCoordinator
    store*: RaftKVStoreExt
    storagePath*: string

  TestCluster* = object
    ## A cluster of test nodes
    nodes*: seq[TestNode]
    config*: TestClusterConfig
    members*: seq[MemberInfo]

  WaitForLeaderOptions* = object
    ## Options for waiting for leader election
    maxAttempts*: int
    stableCount*: int ## Number of consecutive agreements required

# ============================================================================
# Defaults
# ============================================================================

proc defaultTestClusterConfig*(): TestClusterConfig =
  result = TestClusterConfig(
    nodeCount: 3,
    basePort: 29000,
    portOffset: 0,
    preferredLeader: 1,
    electionTimeoutLowerMs: TEST_ELECTION_TIMEOUT_LOWER_MS_MULTINODE,
    electionTimeoutUpperMs: TEST_ELECTION_TIMEOUT_UPPER_MS_MULTINODE,
    heartbeatIntervalMs: TEST_HEARTBEAT_INTERVAL_MS_MULTINODE,
    parallelStartup: true,
    parallelGroupCreation: true,
    seedSystemTables: true
  )

proc defaultWaitForLeaderOptions*(): WaitForLeaderOptions =
  result = WaitForLeaderOptions(
    maxAttempts: 100,
    stableCount: 3
  )

# ============================================================================
# Forward Declarations
# ============================================================================

proc findLeader*(cluster: TestCluster, gid: GroupID): int
proc getAgreedLeader*(cluster: TestCluster, gid: GroupID): int
proc waitForLeader*(cluster: TestCluster, gid: GroupID,
                    options: WaitForLeaderOptions = defaultWaitForLeaderOptions()): int
proc seedSystemTables*(cluster: TestCluster)

# ============================================================================
# Helpers
# ============================================================================

proc cleanDir(p: string) =
  try: removeDir(p)
  except CatchableError: discard

proc getMemberInfo(config: TestClusterConfig): seq[MemberInfo] =
  ## Generate member info for all nodes in the cluster
  for i in 1 .. config.nodeCount:
    let nodeId = uint32(i)
    let basePort = config.basePort + config.portOffset + (i - 1) * 1000
    result.add((nodeId: nodeId, host: "127.0.0.1", basePort: basePort))

proc getStoragePath(nodeId: uint32, portOffset: int): string =
  "/tmp/fractio_test_node" & $nodeId & "_" & $portOffset

# ============================================================================
# Node Creation (Single Node)
# ============================================================================

proc newTestNode(
  nodeId: uint32,
  host: string,
  basePort: int,
  storagePath: string,
  members: seq[MemberInfo],
  config: TestClusterConfig
): TestNode =
  ## Create a single test node with coordinator and store.

  cleanDir(storagePath)
  createDir(storagePath)

  # Create coordinator
  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: rangeTypes.NodeID(nodeId),
    basePort: basePort,
    host: host,
    dataDir: storagePath,
    electionTimeoutLowerMs: config.electionTimeoutLowerMs,
    electionTimeoutUpperMs: config.electionTimeoutUpperMs,
    heartbeatIntervalMs: config.heartbeatIntervalMs,
  ))

  # Populate peer info
  for m in members:
    coord.peerInfo[m.nodeId] = (host: m.host, basePort: m.basePort)

  # Start coordinator (just sets running flag)
  coord.start()

  result = TestNode(
    id: int(nodeId),
    nodeId: nodeId,
    basePort: basePort,
    coord: coord,
    storagePath: storagePath
  )

proc createGroups(node: var TestNode, members: seq[MemberInfo],
    config: TestClusterConfig): bool =
  ## Create META and DATA groups for a node.
  ## Uses parallel creation if config.parallelGroupCreation is true.

  let preferredLeader = if config.preferredLeader >
      0: config.preferredLeader else: 0'u32

  if config.parallelGroupCreation:
    # Create both groups in parallel for faster startup
    return node.coord.createAndStartGroupsParallel(
      @[META_GROUP_ID, DATA_GROUP_START_ID], members, preferredLeader)
  else:
    # Sequential creation (fallback for debugging)
    if not node.coord.createAndStartGroup(META_GROUP_ID, members,
        preferredLeader):
      return false
    if not node.coord.createAndStartGroup(DATA_GROUP_START_ID, members,
        preferredLeader):
      return false
    return true

proc createStore(node: var TestNode): bool =
  ## Create and bootstrap the RaftKVStoreExt for a node.

  let store = newRaftKVStoreExt(node.coord, proposeTimeoutMs = 6000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])
  node.store = store
  return true

# ============================================================================
# Parallel Startup
# ============================================================================

type
  NodeCreationArg = object
    nodeId: uint32
    host: string
    basePort: int
    storagePath: string
    members: seq[MemberInfo]
    config: TestClusterConfig
    result: TestNode
    success: bool
    error: string

proc nodeCreationWorker(arg: ptr NodeCreationArg) {.thread.} =
  ## Worker thread for parallel node creation
  try:
    arg.result = newTestNode(
      arg.nodeId,
      arg.host,
      arg.basePort,
      arg.storagePath,
      arg.members,
      arg.config
    )
    arg.success = true
  except CatchableError as e:
    arg.success = false
    arg.error = e.msg

proc createNodesParallel(config: TestClusterConfig, members: seq[
    MemberInfo]): seq[TestNode] =
  ## Create all nodes in parallel using threads.

  var args = newSeq[NodeCreationArg](config.nodeCount)
  var threads = newSeq[Thread[ptr NodeCreationArg]](config.nodeCount)

  # Initialize arguments
  for i in 0 ..< config.nodeCount:
    let nodeId = uint32(i + 1)
    let basePort = config.basePort + config.portOffset + i * 1000
    let storagePath = getStoragePath(nodeId, config.portOffset)

    args[i] = NodeCreationArg(
      nodeId: nodeId,
      host: "127.0.0.1",
      basePort: basePort,
      storagePath: storagePath,
      members: members,
      config: config
    )

  # Start all threads
  for i in 0 ..< config.nodeCount:
    createThread(threads[i], nodeCreationWorker, addr args[i])

  # Wait for all threads
  for i in 0 ..< config.nodeCount:
    joinThread(threads[i])

    if args[i].success:
      result.add(args[i].result)
    else:
      # Clean up on failure
      for j in 0 ..< i:
        if args[j].success:
          args[j].result.coord.stop()
          cleanDir(args[j].result.storagePath)
      raise newException(IOError, "Failed to create node " & $(i + 1) & ": " &
          args[i].error)

proc createNodesSequential(config: TestClusterConfig, members: seq[
    MemberInfo]): seq[TestNode] =
  ## Create all nodes sequentially (fallback for debugging).

  for i in 0 ..< config.nodeCount:
    let nodeId = uint32(i + 1)
    let basePort = config.basePort + config.portOffset + i * 1000
    let storagePath = getStoragePath(nodeId, config.portOffset)

    var node = newTestNode(nodeId, "127.0.0.1", basePort, storagePath, members, config)
    result.add(node)

# ============================================================================
# Cluster Creation
# ============================================================================

proc newTestCluster*(config: TestClusterConfig): TestCluster =
  ## Create a new test cluster with all nodes initialized.
  ##
  ## If config.parallelStartup is true, nodes are created in parallel threads
  ## for faster startup. Groups are created after all nodes are ready.
  ##
  ## If config.seedSystemTables is true, sys.nodes and sys.groups are
  ## automatically seeded after leader election.

  let members = getMemberInfo(config)

  # Create nodes (parallel or sequential)
  var nodes: seq[TestNode]
  if config.parallelStartup:
    nodes = createNodesParallel(config, members)
  else:
    nodes = createNodesSequential(config, members)

  # Create groups on all nodes
  for i in 0 ..< nodes.len:
    if not createGroups(nodes[i], members, config):
      # Clean up on failure
      for j in 0 ..< nodes.len:
        nodes[j].coord.stop()
        cleanDir(nodes[j].storagePath)
      raise newException(IOError, "Failed to create groups on node " & $(i + 1))

  # Create stores on all nodes
  for i in 0 ..< nodes.len:
    if not createStore(nodes[i]):
      for j in 0 ..< nodes.len:
        nodes[j].coord.stop()
        cleanDir(nodes[j].storagePath)
      raise newException(IOError, "Failed to create store on node " & $(i + 1))

  result = TestCluster(
    nodes: nodes,
    config: config,
    members: members
  )

  # Wait for leader election on all groups
  if result.waitForLeader(META_GROUP_ID) < 0:
    raise newException(IOError, "No leader elected for META_GROUP_ID")
  if result.waitForLeader(DATA_GROUP_START_ID) < 0:
    raise newException(IOError, "No leader elected for DATA_GROUP_START_ID")

  # Seed system tables if requested
  if config.seedSystemTables:
    result.seedSystemTables()

proc newTestCluster*(nodeCount: int, portOffset: int = 0): TestCluster =
  ## Convenience overload for simple cluster creation.
  var config = defaultTestClusterConfig()
  config.nodeCount = nodeCount
  config.portOffset = portOffset
  result = newTestCluster(config)

# ============================================================================
# Leader Management
# ============================================================================

proc findLeader*(cluster: TestCluster, gid: GroupID): int =
  ## Return the index of the leader node for the given group, or -1.
  for i, n in cluster.nodes:
    if n.coord.isLeader(gid):
      return i
  -1

proc getAgreedLeader*(cluster: TestCluster, gid: GroupID): int =
  ## Get the leader that all nodes agree on. Returns -1 if no agreement.
  var leaderId = -1
  for i, n in cluster.nodes:
    let lid = n.coord.getLeader(gid)
    if lid < 0:
      return -1
    if leaderId < 0:
      leaderId = lid
    elif leaderId != lid:
      return -1
  # Convert server ID (1-based) to node index (0-based)
  if leaderId >= 1 and leaderId <= cluster.nodes.len:
    return leaderId - 1
  -1

proc waitForLeader*(cluster: TestCluster, gid: GroupID,
                    options: WaitForLeaderOptions = defaultWaitForLeaderOptions()): int =
  ## Wait for a leader to be elected for the given group.
  ## Returns the index of the leader node, or -1 if timeout.

  var lastLeader = -1
  var consecutiveAgreements = 0

  for attempt in 0 ..< options.maxAttempts:
    let idx = if options.stableCount > 1:
      cluster.getAgreedLeader(gid)
    else:
      cluster.findLeader(gid)

    if idx >= 0:
      if options.stableCount <= 1:
        return idx
      if idx == lastLeader:
        inc consecutiveAgreements
        if consecutiveAgreements >= options.stableCount:
          return idx
      else:
        consecutiveAgreements = 0
        lastLeader = idx

    sleep(TEST_POLL_INTERVAL_MS)

  -1

proc waitForAllLeaders*(cluster: TestCluster): bool =
  ## Wait for leaders on all groups (META + DATA).

  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    if cluster.waitForLeader(gid) < 0:
      return false
  true

# ============================================================================
# System Table Seeding
# ============================================================================

proc seedSystemTables*(cluster: TestCluster) =
  ## Seed sys.nodes and sys.groups tables on the cluster.
  ## Uses batch writes for efficiency.
  ## Must be called after leader election.

  let leaderIdx = cluster.findLeader(META_GROUP_ID)
  if leaderIdx < 0:
    raise newException(IOError, "No leader found for META_GROUP_ID")

  let leader = cluster.nodes[leaderIdx]
  let config = cluster.config

  # Build batch of node records
  var nodeWrites: seq[tuple[key: string, value: string]] = @[]
  for i, node in cluster.nodes:
    let key = encodeTableKey(SYS_NODES_TABLE_ID, $node.nodeId)
    let nodeRec = NodeRecord(
      nodeId: node.nodeId,
      host: "127.0.0.1",
      raftPort: uint16(node.basePort),
      clientPort: uint16(19000 + i),
      status: nsAlive,
    )
    nodeWrites.add((key: key, value: nodeRec.encode()))

  # Build batch of group records
  var groupWrites: seq[tuple[key: string, value: string]] = @[]
  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid.uint64)
    var replicasSeq: seq[GroupReplicaBin] = @[]
    for node in cluster.nodes:
      replicasSeq.add(GroupReplicaBin(nodeId: node.nodeId,
          replicaType: rtVoter))
    let groupRec = GroupRecord(
      groupId: gid.uint64,
      replicas: replicasSeq,
      preferredLeader: config.preferredLeader,
    )
    groupWrites.add((key: key, value: groupRec.encode()))

  # Write all records in batches (same timestamp for atomicity)
  discard leader.store.sysTablePutBatch(nodeWrites)
  discard leader.store.sysTablePutBatch(groupWrites)

  # Wait for state machine to catch up (need enough time for replication)
  sleep(TEST_REPLICATION_WAIT_MS * 4) # 400ms total

# ============================================================================
# Cluster Management
# ============================================================================

proc stop*(cluster: var TestCluster) =
  ## Stop all nodes in the cluster and clean up storage.
  for i in countdown(cluster.nodes.high, 0):
    cluster.nodes[i].coord.stop()
    cleanDir(cluster.nodes[i].storagePath)
  cluster.nodes.setLen(0)

proc getNode*(cluster: TestCluster, idx: int): TestNode =
  ## Get a node by index (0-based).
  cluster.nodes[idx]

proc getLeaderNode*(cluster: TestCluster, gid: GroupID): TestNode =
  ## Get the leader node for a group, or raise exception.
  let idx = cluster.findLeader(gid)
  if idx < 0:
    raise newException(IOError, "No leader found for group " & $gid)
  cluster.nodes[idx]

# ============================================================================
# Additional Convenience
# ============================================================================

proc isLeader*(node: TestNode, gid: GroupID): bool =
  ## Check if this node is the leader for a group.
  node.coord.isLeader(gid)

proc kvPut*(node: TestNode, key: string, value: string): bool =
  ## Write a key-value pair to the node's store.
  let res = node.store.raftPut(key, value)
  res.isOk

proc kvGet*(node: TestNode, key: string): Option[string] =
  ## Read a key from the node's store.
  let res = node.store.raftGet(key)
  if res.isOk and res.value.isSome:
    return some(res.value.get.value)
  none(string)

proc sysTablePutBatch*(node: TestNode,
    writes: openArray[tuple[key: string, value: string]]): bool =
  ## Write multiple sys table entries atomically with MVCC encoding.
  ## All entries get the same timestamp for atomicity.
  ## Returns true on success, false on failure.
  node.store.sysTablePutBatch(writes)

proc sysTableDeleteBatch*(node: TestNode, keys: openArray[string]): bool =
  ## Delete multiple sys table entries through Raft.
  ## Returns true on success, false on failure.
  node.store.sysTableDeleteBatch(keys)

proc sysTablePutAndDeleteBatch*(node: TestNode,
    puts: openArray[tuple[key: string, value: string]],
    deletes: openArray[string]): bool =
  ## Write and delete sys table entries atomically through Raft.
  ## Returns true on success, false on failure.
  node.store.sysTablePutAndDeleteBatch(puts, deletes)

# ============================================================================
# Shared Test Fixtures
# ============================================================================

type
  SharedClusterFixture* = ref object
    ## A shared test fixture that allows multiple tests to reuse the same cluster.
    ## This avoids the overhead of creating/destroying clusters between tests.
    ##
    ## Usage:
    ##   var fixture = newSharedClusterFixture(defaultTestClusterConfig())
    ##   suite "My tests":
    ##     setup:
    ##       fixture.setup()
    ##     teardown:
    ##       fixture.teardown()
    ##     test "example 1":
    ##       let cluster = fixture.get()
    ##       # use cluster...
    ##     test "example 2":
    ##       let cluster = fixture.get()  # Same cluster, state persists
    ##       # use cluster...
    ##
    ## The cluster is created lazily on first setup() and destroyed when
    ## the fixture is garbage collected or stop() is called.
    config: TestClusterConfig
    cluster: TestCluster
    initialized: bool
    testCount: int
    lock: Lock

proc newSharedClusterFixture*(config: TestClusterConfig): SharedClusterFixture =
  ## Create a new shared cluster fixture with the given configuration.
  ## The cluster is not created until setup() is called.
  result = SharedClusterFixture(
    config: config,
    initialized: false,
    testCount: 0
  )
  initLock(result.lock)

proc setup*(fixture: SharedClusterFixture) =
  ## Setup for each test. Creates the cluster on first call.
  ## Subsequent calls return the same cluster.
  withLock fixture.lock:
    if not fixture.initialized:
      fixture.cluster = newTestCluster(fixture.config)
      fixture.initialized = true
    inc fixture.testCount

proc teardown*(fixture: SharedClusterFixture) =
  ## Teardown for each test. Currently a no-op since the cluster is shared.
  ## Override this in your tests if you need per-test cleanup.
  discard

proc get*(fixture: SharedClusterFixture): var TestCluster =
  ## Get the shared cluster. Must call setup() first.
  doAssert fixture.initialized, "Fixture not initialized - call setup() first"
  fixture.cluster

proc stop*(fixture: SharedClusterFixture) =
  ## Stop the shared cluster and clean up.
  withLock fixture.lock:
    if fixture.initialized:
      fixture.cluster.stop()
      fixture.initialized = false

proc isInitialized*(fixture: SharedClusterFixture): bool =
  ## Check if the fixture has been initialized.
  withLock fixture.lock:
    result = fixture.initialized

proc reset*(fixture: SharedClusterFixture) =
  ## Reset the cluster by stopping and recreating it.
  ## Use this if tests corrupt the cluster state.
  withLock fixture.lock:
    if fixture.initialized:
      fixture.cluster.stop()
    fixture.cluster = newTestCluster(fixture.config)
    fixture.initialized = true

# ============================================================================
# Test State Isolation Helpers
# ============================================================================

proc clearTestData*(cluster: var TestCluster) =
  ## Clear all user data from the cluster, keeping system tables.
  ## Use this between tests to isolate test data while reusing the cluster.
  for node in cluster.nodes.mitems:
    # Clear any user-created groups (keep META and DATA_GROUP_START)
    var groupsToRemove: seq[GroupID] = @[]
    for gid, _ in node.coord.groups:
      if gid != META_GROUP_ID and gid != DATA_GROUP_START_ID:
        groupsToRemove.add(gid)
    for gid in groupsToRemove:
      node.coord.removeGroup(gid)

proc reseedSystemTables*(cluster: var TestCluster) =
  ## Re-seed system tables after clearing or modifying cluster state.
  cluster.seedSystemTables()

# ============================================================================
# Test Suite Template
# ============================================================================

template sharedClusterSuite*(suiteName: string, config: TestClusterConfig,
                             body: untyped): untyped =
  ## Template to create a test suite with a shared cluster fixture.
  ## The cluster is created once and shared across all tests in the suite.
  ##
  ## Usage:
  ##   sharedClusterSuite("My Cluster Tests", defaultTestClusterConfig()):
  ##     test "first test":
  ##       let cluster = fixture.get()
  ##       # ...
  ##     test "second test":
  ##       let cluster = fixture.get()
  ##       # ... (same cluster, state persists from first test)
  ##
  ## If you need isolated state between tests, call fixture.reset() in setup.
  var fixture {.global.} = newSharedClusterFixture(config)

  suite suiteName:
    setup:
      fixture.setup()

    teardown:
      fixture.teardown()

    body
