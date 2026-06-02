# Integration tests for space-routed operations on a multi-node cluster.
#
# Verifies that raftPut/raftDelete via resolveGroupId route to the correct
# leader node directly (no forwarding needed) and that non-leader nodes
# return NOT_LEADER for space-routed keys (client must retry on leader).
#
# 3-node cluster with NuRaft ASIO networking. Space with 3 groups, each
# potentially led by a different node. Tests exercise writes from the node
# that owns the target group (deterministic routing).
#
# Port allocation: 26000–26299 (NuRaft ASIO, basePort per node spaced by 100).
# Temp storage: /tmp/fractio_net_fwd_<nodeId>/

import std/[unittest, os, options, tables]

import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types
import fractio/core/types as coreTypes except NodeID
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/protocol/raft_store
import fractio/protocol/server
import fractio/protocol/mvcc_store
import fractio/protocol/txn_manager
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  TMP_DIR = "/tmp/fractio_net_fwd_"
  NODE_COUNT = 3
  SPACE_GROUP_START = 10'u64 # space groups 10, 11, 12

var
  nextClientPort = 9100   ## incremented per node to avoid port conflicts between tests
  seededSpaceUid: SpaceID ## the spaceUid that was seeded in makeCluster3
  seededTableId: TableId  ## the tableId seeded in makeCluster3

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

type
  TestNode = ref object
    id*: int
    port*: int ## Single port for all Raft groups (multiplexed)
    clientPort*: int
    server*: ProtocolServer
    coord*: NuRaftCoordinator
    store*: RaftKVStoreExt
    storagePath*: string

proc cleanDir(p: string) =
  try: removeDir(p) except CatchableError: discard

proc makeCluster3(): seq[TestNode] =
  let members = @[
    (nodeId: 1'u32, host: "127.0.0.1", port: 26000),
    (nodeId: 2'u32, host: "127.0.0.1", port: 26100),
    (nodeId: 3'u32, host: "127.0.0.1", port: 26200),
  ]

  var nodes: seq[TestNode]

  # Phase 1: Create all coordinators first (without starting)
  for nodeNum in 1 .. NODE_COUNT:
    let nodeId = NodeID(uint32(nodeNum))
    let port = 26000 + (nodeNum - 1) * 100
    let storagePath = TMP_DIR & $nodeNum
    cleanDir(storagePath)
    createDir(storagePath)

    let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
      nodeId: nodeId,
      port: port,
      host: "127.0.0.1",
      dataDir: storagePath,
      electionTimeoutLowerMs: 200,
      electionTimeoutUpperMs: 400,
      heartbeatIntervalMs: 100,
    ))

    nodes.add(TestNode(
      id: nodeNum, port: port, clientPort: 0, server: nil,
      coord: coord, store: nil, storagePath: storagePath,
    ))

  # Phase 2: Start all coordinators simultaneously
  for n in nodes:
    n.coord.start()

  # Phase 3: Create groups on all nodes
  for n in nodes:
    # Create meta + data groups
    doAssert n.coord.createAndStartGroup(META_GROUP_ID, members)
    doAssert n.coord.createAndStartGroup(DATA_GROUP_START_ID, members)

    # Create space groups
    for i in 0 ..< NODE_COUNT:
      let gid = groupIDFromInt(SPACE_GROUP_START + uint64(i))
      doAssert n.coord.createAndStartGroup(gid, members)

  # Phase 4: Create stores and bootstrap
  for idx, n in nodes:
    let nodeNum = idx + 1
    let store = newRaftKVStoreExt(n.coord, proposeTimeoutMs = 6000)
    store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

    for i in 0 ..< NODE_COUNT:
      discard store.getOrCreateSM(groupIDFromInt(SPACE_GROUP_START + uint64(i)))

    let cPort = nextClientPort
    nextClientPort += 1

    var cfg = defaultServerConfig()
    cfg.host = "127.0.0.1"
    cfg.port = cPort
    cfg.serverId = uint16(nodeNum)
    cfg.dataDir = n.storagePath
    let srv = newProtocolServer(cfg)
    srv.raftStore = store
    srv.raftCoord = n.coord

    # Set up MVCC store for transaction support
    let txnMgr = newTransactionManager()
    let mockTimer = MockTimeProvider(currentTime: timerTypes.Timestamp(1_000_000_000))
    let tsProvider = newTimestampProvider(mockTimer, uint16(nodeNum))
    let mvccStore = newMvccTransactionStore(store, txnMgr, tsProvider)
    srv.mvccStore = mvccStore

    n.clientPort = cPort
    n.server = srv
    n.store = store

  for n in nodes:
    n.server.start()

  # Wait for leader election on meta + data groups
  for attempt in 0 ..< 50:
    var allLeaders = true
    for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
      var hasLeader = false
      for n in nodes:
        if n.coord.isLeader(gid):
          hasLeader = true
          break
      if not hasLeader:
        allLeaders = false
        break
    if allLeaders: break
    sleep(100)

  # Wait for space group leaders
  for attempt in 0 ..< 50:
    var allLeaders = true
    for i in 0 ..< NODE_COUNT:
      let gid = groupIDFromInt(SPACE_GROUP_START + uint64(i))
      var hasLeader = false
      for n in nodes:
        if n.coord.isLeader(gid):
          hasLeader = true
          break
      if not hasLeader:
        allLeaders = false
        break
    if allLeaders: break
    sleep(100)

  # Find the meta leader and seed system tables
  var leaderIdx = 0
  for i, n in nodes:
    if n.coord.isLeader(META_GROUP_ID):
      leaderIdx = i
      break

# Seed sys.nodes
  for n in nodes:
    let key = encodeTableKey(SYS_NODES_TABLE_ID, $n.id)
    let nodeRec = NodeRecord(
      nodeId: uint32(n.id),
      host: "127.0.0.1",
      raftPort: uint16(n.port),
      clientPort: uint16(n.clientPort),
      status: nsAlive,
    )
    let r = nodes[leaderIdx].store.raftPut(key, nodeRec.encode())
    doAssert r.isOk, "failed to seed sys.nodes for node " & $n.id

  # Seed sys.groups
  let allNums = @[1, 2, 3]
  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid)
    var replicas: seq[GroupReplicaBin] = @[]
    for num in allNums:
      replicas.add(GroupReplicaBin(nodeId: uint32(num), replicaType: rtVoter))
    let groupRec = GroupRecord(
      groupId: groupIDToULID(gid),
      spaceId: coreTypes.zeroSpaceID(),
      replicas: replicas,
    )
    discard nodes[leaderIdx].store.raftPut(key, groupRec.encode())

  for i in 0 ..< NODE_COUNT:
    let gid = groupIDFromInt(SPACE_GROUP_START + uint64(i))
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid)
    var replicas: seq[GroupReplicaBin] = @[]
    for num in allNums:
      replicas.add(GroupReplicaBin(nodeId: uint32(num), replicaType: rtVoter))
    # Find who is leader for this group
    var leaderNodeId = 1'u32
    for n in nodes:
      if n.coord.isLeader(gid):
        leaderNodeId = uint32(n.id)
        break
    let groupRec = GroupRecord(
      groupId: groupIDToULID(gid),
      spaceId: coreTypes.zeroSpaceID(),
      replicas: replicas,
      leader: leaderNodeId,
    )
    discard nodes[leaderIdx].store.raftPut(key, groupRec.encode())

  for i in 0 ..< NODE_COUNT:
    let gid = groupIDFromInt(SPACE_GROUP_START + uint64(i))
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid)
    var replicasSeq: seq[GroupReplicaBin] = @[]
    for num in allNums:
      replicasSeq.add(GroupReplicaBin(nodeId: uint32(num),
          replicaType: rtVoter))
    # Find who is leader for this group
    var leaderNodeId = 1
    for n in nodes:
      if n.coord.isLeader(gid):
        leaderNodeId = n.id
        break
    let groupRec = GroupRecord(
      groupId: groupIDToULID(gid),
      spaceId: coreTypes.zeroSpaceID(),
      replicas: replicasSeq,
      leader: uint32(leaderNodeId),
    )
    discard nodes[leaderIdx].store.raftPut(key, groupRec.encode())

  # Seed space and table
  var spaceGroupIds: seq[GroupID] = @[]
  for i in 0 ..< NODE_COUNT:
    spaceGroupIds.add(groupIDFromInt(SPACE_GROUP_START + uint64(i)))
  seededSpaceUid = coreTypes.genSpaceIDLocal()
  seededTableId = coreTypes.genTableIdLocal()
  let spaceKey = encodeSpaceKey(seededSpaceUid)
  let spaceRec = SpaceRecord(
    spaceId: seededSpaceUid,
    name: "space_2",
    replicas: int32(NODE_COUNT),
    groupCount: int32(NODE_COUNT),
    groupIds: spaceGroupIds,
  )
  discard nodes[leaderIdx].store.raftPut(spaceKey, spaceRec.encode())

  let tableKey = encodeTableKey(SYS_TABLES_TABLE_ID, "default.public.t100")
  let tableRec = TableRecord(
    tableId: seededTableId,
    name: "t100",
    schema: "public",
    database: "default",
    spaceId: seededSpaceUid, # TableRecord.spaceId is SpaceID
    keyEncoding: tkeDataRow,
  )
  discard nodes[leaderIdx].store.raftPut(tableKey, tableRec.encode())

  # Wait for replication and state machine application
  # The Raft log entries need to be applied to the state machine before
  # loadSpaces/loadTableSpaces can see them via backend.scan()
  sleep(1000)

  for n in nodes:
    n.store.loadGroupMembers()
    n.store.loadSpaces()
    n.store.loadTableSpaces()

  nodes

proc stopCluster(nodes: seq[TestNode]) =
  # Stop in reverse order to minimize heartbeat timeouts to dead peers.
  for i in countdown(nodes.high, 0):
    nodes[i].server.stop()
    nodes[i].coord.stop()
    cleanDir(nodes[i].storagePath)

proc spaceInfo(): SpaceInfo =
  let groupIds = @[
    groupIDFromInt(SPACE_GROUP_START),
    groupIDFromInt(SPACE_GROUP_START + 1),
    groupIDFromInt(SPACE_GROUP_START + 2)
  ]
  SpaceInfo(
    spaceId: seededSpaceUid, # SpaceInfo.spaceId is SpaceID
    name: "space_2",
    replicas: NODE_COUNT,
    groupIds: groupIds,
  )

proc findKeyForNode(nodes: seq[TestNode], targetGroupIdx: int,
    space: SpaceInfo): string =
  ## Find a bare PK that routes to the group at targetGroupIdx in the space.
  let targetGid = groupIDFromInt(SPACE_GROUP_START + uint64(targetGroupIdx))
  for i in 0 ..< 1000:
    let pk = "k" & $i
    if routeToGroup(pk, space.groupIds) == targetGid:
      return pk
  doAssert false, "could not find key routing to group " & $targetGid
  ""

proc findLeaderNodeIdx(nodes: seq[TestNode], gid: GroupID): int =
  ## Find which node is leader for a group. Returns -1 if none.
  for i, n in nodes:
    if n.coord.isLeader(gid):
      return i
  -1

# ---------------------------------------------------------------------------
# Suite: resolveGroupId routes consistently with raftPutInSpace
# ---------------------------------------------------------------------------

suite "Multi-node — resolveGroupId consistency with raftPutInSpace":

  test "resolveGroupId and routeToGroup agree on the same group":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()

    # For 50 keys, verify resolveGroupId matches routeToGroup(barePK)
    for i in 0 ..< 50:
      let pk = "key_" & $i
      let key = encodeDataRowScanBound(seededTableId, pk)
      let resolved = nodes[0].store.resolveGroupId(key)
      let expected = routeToGroup(pk, space.groupIds)
      check resolved.isSome
      check resolved.get() == expected

  test "raftPut via resolveGroupId succeeds on leader node":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()

    # Find keys for each group and write from the owning leader
    for groupIdx in 0 ..< NODE_COUNT:
      let gid = groupIDFromInt(SPACE_GROUP_START + uint64(groupIdx))
      let leaderIdx = findLeaderNodeIdx(nodes, gid)
      if leaderIdx < 0: continue

      let pk = findKeyForNode(nodes, groupIdx, space)
      let key = encodeDataRowScanBound(seededTableId, pk)
      let val = """{"groupIdx":""" & $groupIdx & "}"

      # raftPut on the leader node for this group should succeed locally
      let wr = nodes[leaderIdx].store.raftPut(key, val)
      check wr.isOk
      if wr.isOk:
        check wr.value.value == val

  test "raftDelete via resolveGroupId succeeds on leader node":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()

    for groupIdx in 0 ..< NODE_COUNT:
      let gid = groupIDFromInt(SPACE_GROUP_START + uint64(groupIdx))
      let leaderIdx = findLeaderNodeIdx(nodes, gid)
      if leaderIdx < 0: continue

      let pk = findKeyForNode(nodes, groupIdx, space)
      let key = encodeDataRowScanBound(seededTableId, pk)
      discard nodes[leaderIdx].store.raftPut(key, "to_delete")

      let dr = nodes[leaderIdx].store.raftDelete(key)
      check dr.isOk

      let gr = nodes[leaderIdx].store.raftGet(key)
      check gr.isOk
      check gr.value.isNone

# ---------------------------------------------------------------------------
# Suite: cross-node forwarding via peer stores
# ---------------------------------------------------------------------------

suite "Multi-node — NOT_LEADER enforcement for space-routed keys":

  test "raftPut on wrong node returns not-leader":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()

    # Find a key owned by a specific group and try writing from a non-leader
    let pk = findKeyForNode(nodes, 1, space)
    let gid = groupIDFromInt(SPACE_GROUP_START + 1)
    let leaderIdx = findLeaderNodeIdx(nodes, gid)
    let nonLeaderIdx = if leaderIdx == 0: 1 else: 0
    # Only test if leaderIdx != nonLeaderIdx (non-leader tries to write)
    if leaderIdx >= 0 and leaderIdx != nonLeaderIdx:
      let key = encodeDataRowScanBound(seededTableId, pk)
      let val = """{"wrong_node":true}"""

      # raftPut on non-leader for this group — should fail (not leader)
      let wr = nodes[nonLeaderIdx].store.raftPut(key, val)
      check not wr.isOk
      check wr.error.kind == rseNotLeader

  test "raftDelete on wrong node returns not-leader":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()

    let pk = findKeyForNode(nodes, 1, space)
    let gid = groupIDFromInt(SPACE_GROUP_START + 1)
    let leaderIdx = findLeaderNodeIdx(nodes, gid)
    if leaderIdx >= 0:
      let key = encodeDataRowScanBound(seededTableId, pk)
      # Insert on leader
      discard nodes[leaderIdx].store.raftPut(key, "will_delete")

      # raftDelete on a non-leader — should fail
      let nonLeaderIdx = if leaderIdx == 0: 1 else: 0
      let dr = nodes[nonLeaderIdx].store.raftDelete(key)
      check not dr.isOk
      check dr.error.kind == rseNotLeader

  test "raftPutInSpace from non-leader returns NOT_LEADER":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()
    let pk = findKeyForNode(nodes, 2, space)
    let gid = groupIDFromInt(SPACE_GROUP_START + 2)
    let leaderIdx = findLeaderNodeIdx(nodes, gid)
    if leaderIdx >= 0:
      let nonLeaderIdx = if leaderIdx == 0: 1 else: 0
      let key = encodeDataRowScanBound(seededTableId, pk)
      let val = """{"space_forward":1}"""

      # Write from non-leader — should fail with NOT_LEADER (no forwarding)
      let wr = nodes[nonLeaderIdx].store.raftPutInSpace(key, val, space, pk)
      check not wr.isOk
      check wr.error.kind == rseNotLeader

  test "raftDeleteInSpace from non-leader returns NOT_LEADER":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()
    let pk = findKeyForNode(nodes, 2, space)
    let gid = groupIDFromInt(SPACE_GROUP_START + 2)
    let leaderIdx = findLeaderNodeIdx(nodes, gid)
    if leaderIdx >= 0:
      let nonLeaderIdx = if leaderIdx == 0: 1 else: 0
      let key = encodeDataRowScanBound(seededTableId, pk)

      # Delete from non-leader — should fail with NOT_LEADER (no forwarding)
      let dr = nodes[nonLeaderIdx].store.raftDeleteInSpace(key, space, pk)
      check not dr.isOk
      check dr.error.kind == rseNotLeader

  test "raftGetInSpace from non-leader returns NOT_LEADER":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()
    let pk = findKeyForNode(nodes, 1, space)
    let gid = groupIDFromInt(SPACE_GROUP_START + 1)
    let leaderIdx = findLeaderNodeIdx(nodes, gid)
    if leaderIdx >= 0:
      let nonLeaderIdx = if leaderIdx == 0: 1 else: 0
      let key = encodeDataRowScanBound(seededTableId, pk)

      # Read from non-leader — should fail with NOT_LEADER (no forwarding)
      let gr = nodes[nonLeaderIdx].store.raftGetInSpace(key, space, pk)
      check not gr.isOk
      check gr.error.kind == rseNotLeader

# ---------------------------------------------------------------------------
# Suite: routing validation
# ---------------------------------------------------------------------------

suite "Multi-node — routing validation for group-routed requests":

  test "raftPutInGroup rejects key routed to wrong group":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()
    # Find a key that routes to group 10 (first space group)
    let pk = findKeyForNode(nodes, 0, space)
    let key = encodeDataRowScanBound(seededTableId, pk)

    # Find leader for group 11 (wrong group for this key)
    let wrongGid = groupIDFromInt(SPACE_GROUP_START + 1)
    let leaderIdx = findLeaderNodeIdx(nodes, wrongGid)
    if leaderIdx >= 0:
      # Try to put it in group 11 (wrong group) — should fail with rseBadRouting
      let wr = nodes[leaderIdx].store.raftPutInGroup(key, "bad", wrongGid)
      check not wr.isOk
      if not wr.isOk:
        check wr.error.kind == rseBadRouting

  test "raftDeleteInGroupExplicit rejects key routed to wrong group":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()
    let pk = findKeyForNode(nodes, 0, space)
    let key = encodeDataRowScanBound(seededTableId, pk)

    let wrongGid = groupIDFromInt(SPACE_GROUP_START + 1)
    let leaderIdx = findLeaderNodeIdx(nodes, wrongGid)
    if leaderIdx >= 0:
      # Try to delete from group 11 (wrong group) — should fail with rseBadRouting
      let dr = nodes[leaderIdx].store.raftDeleteInGroupExplicit(key, wrongGid)
      check not dr.isOk
      check dr.error.kind == rseBadRouting

  test "raftGetInGroup rejects key routed to wrong group":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()
    let pk = findKeyForNode(nodes, 0, space)
    let key = encodeDataRowScanBound(seededTableId, pk)

    let wrongGid = groupIDFromInt(SPACE_GROUP_START + 1)
    let leaderIdx = findLeaderNodeIdx(nodes, wrongGid)
    if leaderIdx >= 0:
      # Try to get from group 11 (wrong group) — should fail with rseBadRouting
      let gr = nodes[leaderIdx].store.raftGetInGroup(key, wrongGid)
      check not gr.isOk
      check gr.error.kind == rseBadRouting

# ---------------------------------------------------------------------------
# Suite: nodeInfoCache
# ---------------------------------------------------------------------------

suite "Multi-node — nodeInfoCache population":

  test "lookupNodeInfo populates cache from sys.nodes":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    # Cache starts empty
    nodes[0].store.nodeInfoCache.clear()
    check not nodes[0].store.nodeInfoCache.hasKey(2)

    let info = nodes[0].store.lookupNodeInfo(2)
    check info.isSome
    if info.isSome:
      check info.get().host == "127.0.0.1"

    # Now cached
    check nodes[0].store.nodeInfoCache.hasKey(2)
