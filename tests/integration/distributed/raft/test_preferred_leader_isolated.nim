# Isolated test for preferred leader rebalancing.
import std/[unittest, os, options, json, strutils, tables, atomics]
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/protocol/raft_store

const TMP_DIR = "/tmp/fractio_pref_leader_iso_"

type
  TestNode = object
    id*: int
    basePort*: int
    coord*: NuRaftCoordinator
    store*: RaftKVStoreExt
    storagePath*: string

proc cleanDir(p: string) =
  try: removeDir(p) except CatchableError: discard

proc makeCluster3(portOffset: int = 0): seq[TestNode] =
  let members = @[
    (nodeId: 1'u32, host: "127.0.0.1", basePort: 29000 + portOffset),
    (nodeId: 2'u32, host: "127.0.0.1", basePort: 30000 + portOffset),
    (nodeId: 3'u32, host: "127.0.0.1", basePort: 31000 + portOffset),
  ]

  var nodes: seq[TestNode]
  for nodeNum in 1 .. 3:
    let nodeId = rangeTypes.NodeID(uint32(nodeNum))
    let basePort = (29000 + portOffset) + (nodeNum - 1) * 1000
    let storagePath = TMP_DIR & $nodeNum & "_" & $portOffset
    cleanDir(storagePath)
    createDir(storagePath)

    let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
      nodeId: nodeId,
      basePort: basePort,
      host: "127.0.0.1",
      dataDir: storagePath,
      electionTimeoutLowerMs: 1000,
      electionTimeoutUpperMs: 2000,
      heartbeatIntervalMs: 500,
    ))

    # Populate peerInfo so dynamic group creation knows peer ports
    for m in members:
      coord.peerInfo[m.nodeId] = (host: m.host, basePort: m.basePort)

    coord.start()

    doAssert coord.createAndStartGroup(META_GROUP_ID, members,
        preferredLeader = 1)
    doAssert coord.createAndStartGroup(DATA_GROUP_START_ID, members,
        preferredLeader = 1)

    let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 6000)
    store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

    nodes.add(TestNode(
      id: nodeNum, basePort: basePort, coord: coord, store: store,
      storagePath: storagePath,
    ))

  # Wait for leader election on meta + data groups
  for attempt in 0 ..< 100:
    var allLeaders = true
    for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
      var hasLeader = false
      for n in nodes:
        if n.coord.isLeader(gid): hasLeader = true; break
      if not hasLeader: allLeaders = false; break
    if allLeaders: break
    sleep(100)

  var leaderIdx = 0
  for i, n in nodes:
    if n.coord.isLeader(META_GROUP_ID): leaderIdx = i; break

  for num in [1, 2, 3]:
    let key = encodeTableKey(SYS_NODES_TABLE_ID, $num)
    let nodeRec = NodeRecord(
      nodeId: uint32(num),
      host: "127.0.0.1",
      raftPort: uint16(29000 + portOffset + (num - 1) * 1000),
      clientPort: uint16(19000 + num),
      status: nsAlive,
    )
    discard nodes[leaderIdx].store.raftPut(key, nodeRec.encode())

  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid.uint64)
    var replicasSeq: seq[GroupReplicaBin] = @[]
    for num in [1, 2, 3]:
      replicasSeq.add(GroupReplicaBin(nodeId: uint32(num),
          replicaType: rtVoter))
    let groupRec = GroupRecord(
      groupId: gid.uint64,
      replicas: replicasSeq,
      preferredLeader: 1,
    )
    discard nodes[leaderIdx].store.raftPut(key, groupRec.encode())

  sleep(500)
  nodes

proc stopCluster(nodes: seq[TestNode]) =
  for i in countdown(nodes.high, 0):
    nodes[i].coord.stop()
    cleanDir(nodes[i].storagePath)

proc getAgreedLeader(nodes: seq[TestNode], gid: GroupID): int =
  ## Get the leader that all nodes agree on. Returns -1 if no agreement.
  ## Uses getLeader() which returns the leader ID from each node's perspective,
  ## rather than isLeader() which can show transient "multiple leaders" during
  ## leadership transfers.
  var leaderId = -1
  for i, n in nodes:
    let lid = n.coord.getLeader(gid)
    if lid < 0:
      return -1 # Unknown leader
    if leaderId < 0:
      leaderId = lid
    elif leaderId != lid:
      # Nodes disagree on leader - this is normal during transitions
      return -1
  # Convert server ID (1-based) to node index (0-based)
  if leaderId >= 1 and leaderId <= nodes.len:
    return leaderId - 1
  -1

proc waitForStableLeader(nodes: seq[TestNode], gid: GroupID,
    maxAttempts: int = 100, stableCount: int = 3): int =
  ## Wait for leader to stabilize - same leader reported for `stableCount` consecutive checks.
  var lastLeader = -1
  var consecutiveAgreements = 0
  for attempt in 0 ..< maxAttempts:
    let idx = getAgreedLeader(nodes, gid)
    if idx >= 0 and idx == lastLeader:
      inc consecutiveAgreements
      if consecutiveAgreements >= stableCount:
        return idx
    else:
      consecutiveAgreements = 0
    lastLeader = idx
    sleep(100)
  -1

proc waitForLeader(nodes: seq[TestNode], gid: GroupID,
    maxAttempts: int = 100): int =
  for attempt in 0 ..< maxAttempts:
    let idx = getAgreedLeader(nodes, gid)
    if idx >= 0: return idx
    sleep(100)
  -1

suite "Preferred leader isolated test":
  test "non-preferred leader is replaced exactly once":
    let offset = 10000
    var nodes = makeCluster3(offset)
    defer: stopCluster(nodes)

    for node in nodes: node.store.loadGroupMembers()

    let testGid = GroupID(101)
    let metaLeader = waitForLeader(nodes, META_GROUP_ID)
    doAssert metaLeader >= 0

    let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID, $testGid.uint64)
    var replicasSeq: seq[GroupReplicaBin] = @[]
    for n in 1..3:
      replicasSeq.add(GroupReplicaBin(nodeId: uint32(n), replicaType: rtVoter))
    let groupRec = GroupRecord(
      groupId: testGid.uint64,
      replicas: replicasSeq,
      preferredLeader: 3,
    )
    discard nodes[metaLeader].store.raftPut(groupKey, groupRec.encode())

    sleep(1000)
    for node in nodes: node.store.loadGroupMembers()

    # Wait for dynamic group creation
    var groupCreated = false
    for attempt in 0 ..< 100:
      sleep(100)
      groupCreated = true
      for node in nodes:
        if not node.coord.hasGroup(testGid):
          groupCreated = false
          break
      if groupCreated: break

    check groupCreated

    # Wait for initial election - use stable leader to avoid transient states
    let initialLeader = waitForStableLeader(nodes, testGid, maxAttempts = 200,
        stableCount = 5)
    check initialLeader >= 0

    # Reload explicitly
    for node in nodes: node.store.loadGroupMembers()
    check nodes[2].store.preferredLeaders.hasKey(testGid)
    check nodes[2].store.preferredLeaders[testGid] == 3'u32

    # Trigger rebalance
    for node in nodes: node.store.triggerRebal.store(true)
    sleep(5000)

    # Wait for leadership to stabilize on preferred leader (node 3, index 2)
    let finalLeader = waitForStableLeader(nodes, testGid, maxAttempts = 300,
        stableCount = 10)
    check finalLeader == 2 # Node 3 (index 2) should be leader
