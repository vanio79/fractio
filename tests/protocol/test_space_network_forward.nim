# Integration tests for space-routed writes on a multi-node cluster.
#
# Verifies that raftPut/raftDelete via resolveGroupId route to the correct
# leader node directly (no forwarding needed) and that the routing is
# consistent with raftPutInSpace/raftGetInSpace which use the bare PK.
#
# 3-node cluster with NuRaft ASIO networking. Space with 3 groups, each
# potentially led by a different node. Tests exercise writes from the node
# that owns the target group (deterministic routing).
#
# Port allocation: 26000–26299 (NuRaft ASIO, basePort per node spaced by 100).
# Temp storage: /tmp/fractio_net_fwd_<nodeId>/

import std/[unittest, os, options, json, strutils, tables]

import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/core/types as coreTypes
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/protocol/raft_store
import fractio/protocol/server

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  TMP_DIR = "/tmp/fractio_net_fwd_"
  NODE_COUNT = 3
  SPACE_GROUP_START = 10'u64 # space groups 10, 11, 12

var nextClientPort = 9100 ## incremented per node to avoid port conflicts between tests

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

type
  TestNode = object
    id*: int
    basePort*: int
    clientPort*: int
    server*: ProtocolServer
    coord*: NuRaftCoordinator
    store*: RaftKVStoreExt
    storagePath*: string

proc cleanDir(p: string) =
  try: removeDir(p) except CatchableError: discard

proc makeCluster3(): seq[TestNode] =
  let members = @[
    (nodeId: 1'u32, host: "127.0.0.1", basePort: 26000),
    (nodeId: 2'u32, host: "127.0.0.1", basePort: 26100),
    (nodeId: 3'u32, host: "127.0.0.1", basePort: 26200),
  ]

  var nodes: seq[TestNode]
  for nodeNum in 1 .. NODE_COUNT:
    let nodeId = rangeTypes.NodeID(uint32(nodeNum))
    let basePort = 26000 + (nodeNum - 1) * 100
    let storagePath = TMP_DIR & $nodeNum
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
    coord.start()

    # Create meta + data groups
    doAssert coord.createAndStartGroup(META_GROUP_ID, members)
    doAssert coord.createAndStartGroup(DATA_GROUP_START_ID, members)

    # Create space groups
    for i in 0 ..< NODE_COUNT:
      let gid = groupIDFromInt(SPACE_GROUP_START + uint64(i))
      doAssert coord.createAndStartGroup(gid, members)

    let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 6000)
    store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

    for i in 0 ..< NODE_COUNT:
      discard store.getOrCreateSM(groupIDFromInt(SPACE_GROUP_START + uint64(i)))

    let cPort = nextClientPort
    nextClientPort += 1

    var cfg = defaultServerConfig()
    cfg.host = "127.0.0.1"
    cfg.port = cPort
    cfg.serverId = uint16(nodeNum)
    cfg.dataDir = storagePath
    let srv = newProtocolServer(cfg)
    srv.raftStore = store
    srv.raftCoord = coord

    nodes.add(TestNode(
      id: nodeNum, basePort: basePort, clientPort: cPort, server: srv,
      coord: coord, store: store, storagePath: storagePath,
    ))

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
      raftPort: uint16(n.basePort),
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
      spaceId: coreTypes.ZeroULID(),
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
      spaceId: coreTypes.ZeroULID(),
      replicas: replicasSeq,
      leader: uint32(leaderNodeId),
    )
    discard nodes[leaderIdx].store.raftPut(key, groupRec.encode())

  # Seed space and table
  var spaceGroupIds: seq[ULID] = @[]
  for i in 0 ..< NODE_COUNT:
    spaceGroupIds.add(groupIDToULID(groupIDFromInt(SPACE_GROUP_START + uint64(i))))
  let spaceKey = encodeSpaceKey(coreTypes.genULID())
  let spaceRec = SpaceRecord(
    spaceId: coreTypes.ZeroULID(),
    name: "space_2",
    replicas: int32(NODE_COUNT),
    groupCount: int32(NODE_COUNT),
    groupIds: spaceGroupIds,
  )
  discard nodes[leaderIdx].store.raftPut(spaceKey, spaceRec.encode())

  let tableKey = encodeTableKey(SYS_TABLES_TABLE_ID, "default.public.t100")
  let tableRec = TableRecord(
    tableId: 100'u32,
    name: "t100",
    schema: "public",
    database: "default",
    spaceId: coreTypes.ZeroULID(),
  )
  discard nodes[leaderIdx].store.raftPut(tableKey, tableRec.encode())

  sleep(200)

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
    spaceId: coreTypes.ZeroULID(),
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
      let key = encodeDataRowKey(100, pk)
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
      let key = encodeDataRowKey(100, pk)
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
      let key = encodeDataRowKey(100, pk)
      discard nodes[leaderIdx].store.raftPut(key, "to_delete")

      let dr = nodes[leaderIdx].store.raftDelete(key)
      check dr.isOk

      let gr = nodes[leaderIdx].store.raftGet(key)
      check gr.isOk
      check gr.value.isNone

# ---------------------------------------------------------------------------
# Suite: cross-node forwarding via peer stores
# ---------------------------------------------------------------------------

suite "Multi-node — peer store forwarding for space-routed keys":

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
      let key = encodeDataRowKey(100, pk)
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
      let key = encodeDataRowKey(100, pk)
      # Insert on leader
      discard nodes[leaderIdx].store.raftPut(key, "will_delete")

      # raftDelete on a non-leader — should fail
      let nonLeaderIdx = if leaderIdx == 0: 1 else: 0
      let dr = nodes[nonLeaderIdx].store.raftDelete(key)
      check not dr.isOk
      check dr.error.kind == rseNotLeader

  test "raftPutInSpace from non-leader forwards to leader":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()
    let pk = findKeyForNode(nodes, 2, space)
    let gid = groupIDFromInt(SPACE_GROUP_START + 2)
    let leaderIdx = findLeaderNodeIdx(nodes, gid)
    if leaderIdx >= 0:
      let nonLeaderIdx = if leaderIdx == 0: 1 else: 0
      let key = encodeDataRowKey(100, pk)
      let val = """{"space_forward":1}"""

      # Write from non-leader — should succeed via forwarding
      let wr = nodes[nonLeaderIdx].store.raftPutInSpace(key, val, space, pk)
      check wr.isOk
      if wr.isOk:
        check wr.value.value == val

      # Verify the data is readable from the owning leader
      let gr = nodes[leaderIdx].store.raftGetInSpaceFromGroup(key, gid)
      check gr.isOk
      if gr.isOk:
        check gr.value.isSome
        if gr.value.isSome:
          check gr.value.get().value == val

  test "raftDeleteInSpace from non-leader forwards to leader":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()
    let pk = findKeyForNode(nodes, 2, space)
    let gid = groupIDFromInt(SPACE_GROUP_START + 2)
    let leaderIdx = findLeaderNodeIdx(nodes, gid)
    if leaderIdx >= 0:
      let nonLeaderIdx = if leaderIdx == 0: 1 else: 0
      let key = encodeDataRowKey(100, pk)
      discard nodes[leaderIdx].store.raftPutInSpace(key, "to_del", space, pk)

      # Delete from non-leader — should succeed via forwarding
      let dr = nodes[nonLeaderIdx].store.raftDeleteInSpace(key, space, pk)
      check dr.isOk

      # Verify the key is gone from the owning leader
      let gr = nodes[leaderIdx].store.raftGetInSpaceFromGroup(key, gid)
      check gr.isOk
      if gr.isOk:
        check gr.value.isNone

  test "raftGetInSpace from non-leader forwards to leader":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()
    let pk = findKeyForNode(nodes, 1, space)
    let gid = groupIDFromInt(SPACE_GROUP_START + 1)
    let leaderIdx = findLeaderNodeIdx(nodes, gid)
    if leaderIdx >= 0:
      let nonLeaderIdx = if leaderIdx == 0: 1 else: 0
      let key = encodeDataRowKey(100, pk)
      let val = """{"get_forward":true}"""

      # Write on the owning leader
      let wr = nodes[leaderIdx].store.raftPutInSpace(key, val, space, pk)
      check wr.isOk

      # Read from non-leader — should forward to leader
      let gr = nodes[nonLeaderIdx].store.raftGetInSpace(key, space, pk)
      check gr.isOk
      if gr.isOk:
        check gr.value.isSome
        if gr.value.isSome:
          check gr.value.get().value == val

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
    let key = encodeDataRowKey(100, pk)

    # Find leader for group 11 (wrong group for this key)
    let wrongGid = groupIDFromInt(SPACE_GROUP_START + 1)
    let leaderIdx = findLeaderNodeIdx(nodes, wrongGid)
    if leaderIdx >= 0:
      # Try to put it in group 11 (wrong group) — should fail with rseBadRouting
      let wr = nodes[leaderIdx].store.raftPutInGroup(key, "bad", wrongGid)
      check not wr.isOk
      check wr.error.kind == rseBadRouting

  test "raftDeleteInGroupExplicit rejects key routed to wrong group":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()
    let pk = findKeyForNode(nodes, 0, space)
    let key = encodeDataRowKey(100, pk)

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
    let key = encodeDataRowKey(100, pk)

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
