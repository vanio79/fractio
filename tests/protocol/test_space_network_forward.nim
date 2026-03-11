# Integration tests for space-routed writes on a multi-node cluster.
#
# Verifies that raftPut/raftDelete via resolveGroupId route to the correct
# leader node directly (no forwarding needed) and that the routing is
# consistent with raftPutInSpace/raftGetInSpace which use the bare PK.
#
# 3-node cluster with peer stores wired. Space with 3 groups, each led by
# a different node. Tests exercise writes from the node that owns the
# target group (deterministic routing).
#
# Port allocation: 21800–21829 (Raft).
# Temp storage: /tmp/fractio_net_fwd_<nodeId>/

import std/[unittest, os, options, json, strutils, tables]

import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_transport
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/protocol/raft_store
import fractio/protocol/server

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  BASE_RAFT_PORT = 21800
  TMP_DIR = "/tmp/fractio_net_fwd_"
  NODE_COUNT = 3
  SPACE_GROUP_START = 10'u64  # space groups 10, 11, 12

var nextClientPort = 9100  ## incremented per node to avoid port conflicts between tests

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

type
  TestNode = object
    id*: int
    clientPort*: int
    server*: ProtocolServer
    coord*: MultiRaftCoordinator
    store*: RaftKVStoreExt
    rgt*: RaftGroupTransport
    storagePath*: string

proc cleanDir(p: string) =
  try: removeDir(p) except CatchableError: discard

proc raftPort(nodeNum: int): int = BASE_RAFT_PORT + (nodeNum - 1) * 10

proc makeNode(nodeNum: int, peerNums: seq[int]): TestNode =
  let nodeId = rangeTypes.NodeID(uint32(nodeNum))
  let rPort = raftPort(nodeNum)

  var peers: seq[PeerAddr]
  for pn in peerNums:
    peers.add(PeerAddr(
      nodeId: rangeTypes.NodeID(uint32(pn)),
      host: "127.0.0.1",
      raftPort: raftPort(pn),
    ))

  let rgt = newRaftGroupTransport(nodeId, "127.0.0.1", rPort, peers)
  let transport = newMultiRaftTransport(rgt)

  let storagePath = TMP_DIR & $nodeNum
  cleanDir(storagePath)
  createDir(storagePath)

  let coord = newMultiRaftCoordinator(CoordinatorConfig(
    nodeId: nodeId,
    numWorkers: 1,
    electionTimeoutNs: 800_000_000'i64,
    heartbeatIntervalNs: 50_000_000'i64,
    storagePath: storagePath / "raft",
    proposeTimeoutMs: 6000,
    transport: transport,
  ))

  # Create meta + data groups with all peers as voters
  for groupId in [META_GROUP_ID, DATA_GROUP_START_ID]:
    var desc = rangeTypes.newGroupDescriptor(groupId)
    discard desc.addReplica(nodeId, rangeTypes.rtVoter)
    for pn in peerNums:
      discard desc.addReplica(rangeTypes.NodeID(uint32(pn)), rangeTypes.rtVoter)
    let rep = desc.getReplica(nodeId)
    doAssert rep.isSome
    discard coord.createGroup(desc, rep.get.replicaId)

  # Create space groups — each group has all 3 nodes as voters
  for i in 0 ..< NODE_COUNT:
    let gid = GroupID(SPACE_GROUP_START + uint64(i))
    var desc = rangeTypes.newGroupDescriptor(gid)
    discard desc.addReplica(nodeId, rangeTypes.rtVoter)
    for pn in peerNums:
      discard desc.addReplica(rangeTypes.NodeID(uint32(pn)), rangeTypes.rtVoter)
    let rep = desc.getReplica(nodeId)
    doAssert rep.isSome
    discard coord.createGroup(desc, rep.get.replicaId)

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 6000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  for i in 0 ..< NODE_COUNT:
    discard store.getOrCreateSM(GroupID(SPACE_GROUP_START + uint64(i)))

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
  srv.raftTransport = rgt

  TestNode(
    id: nodeNum, clientPort: cPort, server: srv, coord: coord, store: store,
    rgt: rgt, storagePath: storagePath,
  )

proc seedSysNodes(leaderStore: RaftKVStoreExt, nodes: seq[TestNode]) =
  for n in nodes:
    let key = encodeTableKey(SYS_NODES_TABLE_ID, $n.id)
    let val = $ %*{
      "nodeId": n.id,
      "host": "127.0.0.1",
      "raftPort": raftPort(n.id),
      "clientPort": n.clientPort,
      "status": 1,
    }
    let r = leaderStore.raftPut(key, val)
    doAssert r.isOk, "failed to seed sys.nodes for node " & $n.id

proc seedSysGroups(leaderStore: RaftKVStoreExt, nodeNums: seq[int]) =
  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid.uint64)
    var replicas = newJArray()
    for num in nodeNums:
      replicas.add(%*{"nodeId": num, "type": "voter"})
    let val = $ %*{"groupId": gid.uint64.int, "replicas": replicas}
    discard leaderStore.raftPut(key, val)

  for i in 0 ..< NODE_COUNT:
    let gid = SPACE_GROUP_START + uint64(i)
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid)
    var replicas = newJArray()
    for num in nodeNums:
      replicas.add(%*{"nodeId": num, "type": "voter"})
    # Leader for group 10+i is node i+1
    let val = $ %*{"groupId": int(gid), "replicas": replicas, "leader": i + 1}
    discard leaderStore.raftPut(key, val)

proc seedSpaceAndTable(store: RaftKVStoreExt, spaceId: int, tableId: uint32) =
  var gids = newJArray()
  for i in 0 ..< NODE_COUNT:
    gids.add(newJInt(int(SPACE_GROUP_START + uint64(i))))
  let spaceKey = encodeSpaceKey(spaceId)
  let spaceVal = $ %*{
    "spaceId": spaceId,
    "name": "space_" & $spaceId,
    "replicas": NODE_COUNT,
    "groupIds": gids,
  }
  discard store.raftPut(spaceKey, spaceVal)

  let tableKey = encodeTableKey(SYS_TABLES_TABLE_ID, "default.public.t" & $tableId)
  let tableVal = $ %*{
    "tableId": int(tableId),
    "name": "t" & $tableId,
    "spaceId": spaceId,
  }
  discard store.raftPut(tableKey, tableVal)

  store.loadSpaces()
  store.loadTableSpaces()

proc makeCluster3(): seq[TestNode] =
  let allNums = @[1, 2, 3]
  var nodes: seq[TestNode]
  for n in allNums:
    var peers: seq[int]
    for p in allNums:
      if p != n: peers.add(p)
    nodes.add(makeNode(n, peers))

  for n in nodes:
    n.coord.start()
    n.server.start()

  # Force node 1 as leader for meta + data groups
  for groupId in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let g = nodes[0].coord.getGroup(groupId)
    doAssert g.isSome
    g.get.becomeLeader()

  # Force different leaders for space groups:
  #   group 10 → node 1, group 11 → node 2, group 12 → node 3
  for i in 0 ..< NODE_COUNT:
    let gid = GroupID(SPACE_GROUP_START + uint64(i))
    let g = nodes[i].coord.getGroup(gid)
    doAssert g.isSome
    g.get.becomeLeader()

  sleep(400)

  seedSysNodes(nodes[0].store, nodes)
  seedSysGroups(nodes[0].store, allNums)
  seedSpaceAndTable(nodes[0].store, 2, 100)
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
  let groupIds = @[SPACE_GROUP_START, SPACE_GROUP_START + 1, SPACE_GROUP_START + 2]
  SpaceInfo(
    spaceId: 2,
    name: "space_2",
    replicas: NODE_COUNT,
    groupIds: groupIds,
  )

proc findKeyForNode(targetNodeIdx: int, space: SpaceInfo): string =
  ## Find a bare PK that routes to the group led by nodes[targetNodeIdx].
  let targetGid = GroupID(SPACE_GROUP_START + uint64(targetNodeIdx))
  for i in 0 ..< 1000:
    let pk = "k" & $i
    if routeToGroup(pk, space.groupIds) == targetGid:
      return pk
  doAssert false, "could not find key routing to node " & $(targetNodeIdx + 1)
  ""

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

    # Find keys for each node and write from the owning leader
    for nodeIdx in 0 ..< NODE_COUNT:
      let pk = findKeyForNode(nodeIdx, space)
      let key = encodeDataRowKey(100, pk)
      let val = """{"node":""" & $nodeIdx & "}"

      # raftPut on the leader node for this group should succeed locally
      let wr = nodes[nodeIdx].store.raftPut(key, val)
      check wr.isOk
      if wr.isOk:
        check wr.value.value == val

  test "raftDelete via resolveGroupId succeeds on leader node":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()

    for nodeIdx in 0 ..< NODE_COUNT:
      let pk = findKeyForNode(nodeIdx, space)
      let key = encodeDataRowKey(100, pk)
      discard nodes[nodeIdx].store.raftPut(key, "to_delete")

      let dr = nodes[nodeIdx].store.raftDelete(key)
      check dr.isOk

      let gr = nodes[nodeIdx].store.raftGet(key)
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

    # Find a key owned by node 2 (index 1)
    let pk = findKeyForNode(1, space)
    let key = encodeDataRowKey(100, pk)
    let val = """{"wrong_node":true}"""

    # raftPut on node 1 for a key owned by node 2 — should fail (not leader)
    let wr = nodes[0].store.raftPut(key, val)
    check not wr.isOk
    check wr.error.kind == rseNotLeader

  test "raftDelete on wrong node returns not-leader":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()

    # Insert on owner (node 2)
    let pk = findKeyForNode(1, space)
    let key = encodeDataRowKey(100, pk)
    discard nodes[1].store.raftPut(key, "will_delete")

    # raftDelete on node 1 for a key owned by node 2 — should fail
    let dr = nodes[0].store.raftDelete(key)
    check not dr.isOk
    check dr.error.kind == rseNotLeader

  test "raftPutInSpace from non-leader forwards to leader":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()
    let pk = findKeyForNode(2, space)  # owned by node 3
    let key = encodeDataRowKey(100, pk)
    let val = """{"space_forward":1}"""

    # Write from node 1 for a key owned by node 3 — should succeed via forwarding
    let wr = nodes[0].store.raftPutInSpace(key, val, space, pk)
    check wr.isOk
    if wr.isOk:
      check wr.value.value == val

    # Verify the data is readable from the owning leader (node 3)
    let gr = nodes[2].store.raftGetInSpaceFromGroup(key, GroupID(SPACE_GROUP_START + 2))
    check gr.isOk
    if gr.isOk:
      check gr.value.isSome
      if gr.value.isSome:
        check gr.value.get().value == val

  test "raftDeleteInSpace from non-leader forwards to leader":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()
    let pk = findKeyForNode(2, space)
    let key = encodeDataRowKey(100, pk)
    discard nodes[2].store.raftPutInSpace(key, "to_del", space, pk)

    # Delete from node 1 for a key owned by node 3 — should succeed via forwarding
    let dr = nodes[0].store.raftDeleteInSpace(key, space, pk)
    check dr.isOk

    # Verify the key is gone from the owning leader (node 3)
    let gr = nodes[2].store.raftGetInSpaceFromGroup(key, GroupID(SPACE_GROUP_START + 2))
    check gr.isOk
    if gr.isOk:
      check gr.value.isNone

  test "raftGetInSpace from non-leader forwards to leader":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()
    let pk = findKeyForNode(1, space)  # owned by node 2
    let key = encodeDataRowKey(100, pk)
    let val = """{"get_forward":true}"""

    # Write on the owning leader (node 2)
    let wr = nodes[1].store.raftPutInSpace(key, val, space, pk)
    check wr.isOk

    # Read from node 1 (not the leader) — should forward to node 2
    let gr = nodes[0].store.raftGetInSpace(key, space, pk)
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
    # Find a key that routes to group 10 (node 1's group)
    let pk = findKeyForNode(0, space)
    let key = encodeDataRowKey(100, pk)

    # Try to put it in group 11 (wrong group) — should fail with rseBadRouting
    let wr = nodes[1].store.raftPutInGroup(key, "bad", GroupID(SPACE_GROUP_START + 1))
    check not wr.isOk
    check wr.error.kind == rseBadRouting

  test "raftDeleteInGroupExplicit rejects key routed to wrong group":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()
    let pk = findKeyForNode(0, space)
    let key = encodeDataRowKey(100, pk)

    # Try to delete from group 11 (wrong group) — should fail with rseBadRouting
    let dr = nodes[1].store.raftDeleteInGroupExplicit(key, GroupID(SPACE_GROUP_START + 1))
    check not dr.isOk
    check dr.error.kind == rseBadRouting

  test "raftGetInGroup rejects key routed to wrong group":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    let space = spaceInfo()
    let pk = findKeyForNode(0, space)
    let key = encodeDataRowKey(100, pk)

    # Try to get from group 11 (wrong group) — should fail with rseBadRouting
    let gr = nodes[1].store.raftGetInGroup(key, GroupID(SPACE_GROUP_START + 1))
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
