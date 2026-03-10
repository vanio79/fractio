# Integration test — preferred leader rebalancing on a 3-node cluster.
#
# Verifies that when a non-preferred leader is elected, the timerProc
# eventually steps it down and the preferred leader wins the next election.
#
# Cluster topology:
#   Nodes 1–3, fully connected via RaftGroupTransport.
#   A group is created with preferredLeader = node 1.
#   Node 2 is forced to become leader (simulating a failover).
#   The test verifies that timerProc steps node 2 down and node 1
#   eventually wins the election.
#
# Port allocation: 22600–22699 (Raft TCP ports)
# Temp storage: /tmp/fractio_pref_leader_<nodeId>/ (cleaned up per test)

import std/[unittest, os, options, json, strutils, tables, atomics]

import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_transport
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/protocol/raft_store

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  BASE_PORT = 22600
  TMP_DIR = "/tmp/fractio_pref_leader_"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

type
  TestNode = object
    id*: int
    coord*: MultiRaftCoordinator
    store*: RaftKVStoreExt
    rgt*: RaftGroupTransport
    storagePath*: string

proc cleanDir(p: string) =
  try: removeDir(p) except CatchableError: discard

proc raftPort(nodeNum: int): int = BASE_PORT + (nodeNum - 1) * 10

proc makeNode(nodeNum: int, peerNums: seq[int]): TestNode =
  let nodeId = rangeTypes.NodeID(uint32(nodeNum))
  let port = raftPort(nodeNum)

  var peers: seq[PeerAddr]
  for pn in peerNums:
    peers.add(PeerAddr(
      nodeId: rangeTypes.NodeID(uint32(pn)),
      host: "127.0.0.1",
      raftPort: raftPort(pn),
    ))

  let rgt = newRaftGroupTransport(nodeId, "127.0.0.1", port, peers)
  let transport = newMultiRaftTransport(rgt)

  let storagePath = TMP_DIR & $nodeNum
  cleanDir(storagePath)
  createDir(storagePath)

  let coord = newMultiRaftCoordinator(CoordinatorConfig(
    nodeId: nodeId,
    numWorkers: 1,
    electionTimeoutNs: 800_000_000'i64,    # 800 ms
    heartbeatIntervalNs: 50_000_000'i64,   # 50 ms
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
    doAssert rep.isSome, "replica not found for node " & $nodeNum
    discard coord.createGroup(desc, rep.get.replicaId)

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 6000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  TestNode(
    id: nodeNum, coord: coord, store: store,
    rgt: rgt, storagePath: storagePath,
  )

proc startNode(n: TestNode) =
  n.coord.start()

proc stopNode(n: TestNode) =
  n.coord.stop()
  cleanDir(n.storagePath)

proc seedSysNodes(leaderStore: RaftKVStoreExt, nodeNums: seq[int]) =
  for num in nodeNums:
    let key = encodeTableKey(SYS_NODES_TABLE_ID, $num)
    let val = $ %*{
      "nodeId": num,
      "host": "127.0.0.1",
      "raftPort": raftPort(num),
      "clientPort": 19000 + num,
      "status": 1,
    }
    discard leaderStore.raftPut(key, val)

proc seedSysGroups(leaderStore: RaftKVStoreExt, nodeNums: seq[int]) =
  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid.uint64)
    var replicas = newJArray()
    for num in nodeNums:
      replicas.add(%*{"nodeId": num, "type": "voter"})
    let val = $ %*{"groupId": gid.uint64.int, "replicas": replicas,
                    "preferredLeader": nodeNums[0]}
    discard leaderStore.raftPut(key, val)

proc wirePeerStores(nodes: seq[TestNode]) =
  for i in 0 ..< nodes.len:
    for j in 0 ..< nodes.len:
      if i != j:
        nodes[i].store.addPeerStore(uint32(nodes[j].id), nodes[j].store)

proc makeCluster3(): seq[TestNode] =
  let allNums = @[1, 2, 3]
  var nodes: seq[TestNode]
  for n in allNums:
    var peers: seq[int]
    for p in allNums:
      if p != n: peers.add(p)
    nodes.add(makeNode(n, peers))

  for n in nodes: startNode(n)

  # Force node 1 to be leader for meta + data groups
  for groupId in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let g = nodes[0].coord.getGroup(groupId)
    doAssert g.isSome
    g.get.becomeLeader()

  sleep(400)

  # Seed system tables on the leader
  seedSysNodes(nodes[0].store, allNums)
  seedSysGroups(nodes[0].store, allNums)
  sleep(400)

  wirePeerStores(nodes)
  nodes

proc stopCluster(nodes: seq[TestNode]) =
  for n in nodes: stopNode(n)

proc findLeader(nodes: seq[TestNode], gid: GroupID): int =
  ## Return the index of the leader node for the given group, or -1.
  for i, n in nodes:
    if n.coord.hasGroup(gid):
      let g = n.coord.getGroup(gid)
      if g.isSome and g.get.isLeader():
        return i
  -1

# ---------------------------------------------------------------------------
# Test suites
# ---------------------------------------------------------------------------

suite "Preferred leader rebalancing — 3-node cluster":

  test "loadGroupMembers reads preferredLeader from sys.groups":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    # loadGroupMembers on the leader node
    nodes[0].store.loadGroupMembers()

    # Meta and data groups should have preferredLeader = 1
    check nodes[0].store.preferredLeaders.hasKey(META_GROUP_ID)
    check nodes[0].store.preferredLeaders[META_GROUP_ID] == 1'u32
    check nodes[0].store.preferredLeaders.hasKey(DATA_GROUP_START_ID)
    check nodes[0].store.preferredLeaders[DATA_GROUP_START_ID] == 1'u32

  test "non-preferred leader steps down and preferred leader wins election":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    # Load preferred leaders on all nodes so the callback can find them.
    # In a real cluster this happens during recovery.
    for node in nodes:
      node.store.loadGroupMembers()

    # Verify node 1 is currently the leader for DATA_GROUP_START_ID
    let g1 = nodes[0].coord.getGroup(DATA_GROUP_START_ID)
    check g1.isSome
    check g1.get.isLeader()

    # Simulate failover: force node 2 to become leader
    let g2 = nodes[1].coord.getGroup(DATA_GROUP_START_ID)
    check g2.isSome
    g2.get.becomeCandidate()
    g2.get.becomeLeader()
    # Make node 1 a follower (simulating it lost leadership)
    g1.get.becomeFollower(g2.get.getTerm() + 1)

    # Verify node 2 is now leader
    check g2.get.isLeader()
    check not g1.get.isLeader()

    # Wait for the timerProc to detect the non-preferred leader (node 2)
    # and step it down. The preferred leader (node 1) should then win
    # the election because it has a shorter election timeout.
    # Max wait: cooldown is 10s, but since node 2 has never stepped down
    # before, the cooldown check passes immediately. We need to wait for:
    # - timerProc tick (10ms) to detect the mismatch and step down node 2
    # - election timeout (~800ms + jitter/2 for preferred leader) for node 1
    # Total: ~2-3 seconds should be sufficient with some margin.
    var preferredWon = false
    for attempt in 0 ..< 60:  # 60 × 100ms = 6s max wait
      sleep(100)
      let leaderIdx = findLeader(nodes, DATA_GROUP_START_ID)
      if leaderIdx == 0:
        # Node 1 (preferred) is leader again
        preferredWon = true
        break

    check preferredWon

  test "transferLeadership sets cooldown timestamp":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    # Create a test group where node 2 is leader
    let testGid = GroupID(100)
    for i, node in nodes:
      var desc = rangeTypes.newGroupDescriptor(testGid)
      for n in 1..3:
        discard desc.addReplica(rangeTypes.NodeID(uint32(n)), rangeTypes.rtVoter)
      let rep = desc.getReplica(rangeTypes.NodeID(uint32(node.id)))
      doAssert rep.isSome
      discard node.coord.createAndStartGroup(desc, rep.get.replicaId)

    sleep(200)

    # Make node 2 the leader for testGid
    let g = nodes[1].coord.getGroup(testGid)
    check g.isSome
    g.get.becomeLeader()

    # Transfer leadership from node 2 to node 1
    let before = g.get.lastPreferredLeaderStepdownNs.load()
    let ok = nodes[1].coord.transferLeadership(testGid, rangeTypes.NodeID(1))
    check ok
    check not g.get.isLeader()

    let after = g.get.lastPreferredLeaderStepdownNs.load()
    check after > before
    check after > 0
