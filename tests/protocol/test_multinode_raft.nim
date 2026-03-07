# Phase 6 — Multi-node Raft integration tests.
#
# Starts a real 3-node Raft cluster using MultiRaftCoordinator wired to
# RaftGroupTransport (TCP-based, NetworkRaftNode). Verifies:
#   1. Leader election — exactly one leader after startup.
#   2. Log replication — write on leader is visible on followers.
#   3. Follower apply — applyBatchCallback drives KV state machine on followers.
#   4. Quorum write — proposeAndWait returns success on leader with 3 nodes.
#
# Port allocation: 20200–20299 (Raft TCP ports only; no protocol server here).
# Temp storage: /tmp/fractio_mn_<nodeId>/ (cleaned up per test).
#
# Design notes:
#   - Each node gets a unique RaftPort in 20200..20299.
#   - Single range r1 with 3 voter replicas.
#   - Leader forced via becomeLeader() on node 1 for determinism.
#   - Uses newMultiRaftTransport() from multigroup_transport to wrap the
#     RaftGroupTransport into the MultiRaftTransport vtable.

import std/[unittest, os, times, options, tables]

import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_transport
import fractio/distributed/raft/multigroup_types
import fractio/distributed/range/types as rangeTypes
import fractio/protocol/raft_store

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

const
  BASE_PORT = 20200
  TMP_DIR = "/tmp/fractio_mn_"

type
  NodeSetup = object
    coord*: MultiRaftCoordinator
    store*: RaftKVStoreExt
    storagePath*: string
    rgt*: RaftGroupTransport ## keeps the transport alive (GC root)

proc cleanDir(p: string) =
  try: removeDir(p) except CatchableError: discard
  try: createDir(p) except CatchableError: discard

proc makeNode(nodeNum: int, ## 1, 2, or 3
              peerNums: seq[int], ## other node numbers
              rid: RangeID,
              desc: RangeDescriptor): NodeSetup =
  let nodeId = rangeTypes.RangeNodeID(uint32(nodeNum))
  let port = BASE_PORT + (nodeNum - 1) * 10       # 20200 / 20210 / 20220

  # Build peer list (all nodes except self)
  var peers: seq[PeerAddr]
  for pn in peerNums:
    peers.add(PeerAddr(
      nodeId: rangeTypes.RangeNodeID(uint32(pn)),
      host: "127.0.0.1",
      raftPort: BASE_PORT + (pn - 1) * 10,
    ))

  let rgt = newRaftGroupTransport(nodeId, "127.0.0.1", port, peers)
  let transport = newMultiRaftTransport(rgt)

  let storagePath = TMP_DIR & $nodeNum
  cleanDir(storagePath)

  let cfg = CoordinatorConfig(
    nodeId: nodeId,
    numWorkers: 1,
    # Use shorter intervals for tests so heartbeats fire within the observation
      # window (sleep(300) below) and election timeouts don't fire during teardown.
    electionTimeoutNs: 800_000_000'i64, # 800 ms
    heartbeatIntervalNs: 50_000_000'i64, # 50 ms
    storagePath: storagePath,
    proposeTimeoutMs: 6000,
    transport: transport,
  )
  let coord = newMultiRaftCoordinator(cfg)

  # Add the replica for this node to the descriptor
  let rep = desc.getReplica(nodeId)
  doAssert rep.isSome, "replica not found for node " & $nodeNum
  discard coord.createGroup(desc, rep.get.replicaId)

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 6000)
  store.bootstrapSingleShardExt(rid)
  store.wireApplyCallback()

  NodeSetup(coord: coord, store: store, storagePath: storagePath, rgt: rgt)

proc startNode(ns: NodeSetup) =
  ns.coord.start()

proc stopNode(ns: NodeSetup) =
  ns.coord.stop()
  cleanDir(ns.storagePath)

proc findLeader(nodes: seq[NodeSetup]): int =
  ## Return index of the node that believes itself to be leader, or -1.
  for i, ns in nodes:
    let grpOpt = ns.coord.getGroup(RangeID(1))
    if grpOpt.isSome and grpOpt.get.isLeader():
      return i
  -1

# ---------------------------------------------------------------------------
# Shared cluster fixture
# ---------------------------------------------------------------------------

proc makeCluster(): (seq[NodeSetup], RangeID, RangeDescriptor) =
  let rid = RangeID(1)
  let desc = newRangeDescriptor(rid, @[], @[])
  # Pre-add all 3 replicas so every node knows the quorum configuration
  discard desc.addReplica(rangeTypes.RangeNodeID(1))
  discard desc.addReplica(rangeTypes.RangeNodeID(2))
  discard desc.addReplica(rangeTypes.RangeNodeID(3))

  let nodes = @[
    makeNode(1, @[2, 3], rid, desc),
    makeNode(2, @[1, 3], rid, desc),
    makeNode(3, @[1, 2], rid, desc),
  ]
  (nodes, rid, desc)

# ---------------------------------------------------------------------------
# Test suites
# ---------------------------------------------------------------------------

suite "MultiNode Raft — leader election":

  test "exactly one leader after startup with forced leader":
    let (nodes, rid, _) = makeCluster()
    for ns in nodes: startNode(ns)

    # Force node 0 (nodeId=1) to be leader for deterministic tests
    let grp0 = nodes[0].coord.getGroup(rid)
    doAssert grp0.isSome
    grp0.get.becomeLeader()

    # Wait long enough for leader to send heartbeats (heartbeatIntervalNs=50ms)
    # and followers to receive them, resetting their election timers.
    sleep(300)

    let leaderIdx = findLeader(nodes)
    check leaderIdx == 0

    for ns in nodes: stopNode(ns)

  test "only one node is leader at any time":
    let (nodes, rid, _) = makeCluster()
    for ns in nodes: startNode(ns)

    let grp0 = nodes[0].coord.getGroup(rid)
    doAssert grp0.isSome
    grp0.get.becomeLeader()

    sleep(300)

    var leaderCount = 0
    for ns in nodes:
      let g = ns.coord.getGroup(rid)
      if g.isSome and g.get.isLeader(): inc leaderCount
    check leaderCount == 1

    for ns in nodes: stopNode(ns)

suite "MultiNode Raft — quorum write":

  test "proposeAndWait succeeds on leader with 3 nodes":
    let (nodes, rid, _) = makeCluster()
    for ns in nodes: startNode(ns)

    let grp0 = nodes[0].coord.getGroup(rid)
    doAssert grp0.isSome
    grp0.get.becomeLeader()
    sleep(300)

    # Write via leader (node 0)
    let putRes = nodes[0].store.raftPut("hello", "world")
    check putRes.isOk
    if putRes.isOk:
      check putRes.value.value == "world"

    for ns in nodes: stopNode(ns)

  test "proposeAndWait on follower returns Not the leader":
    let (nodes, rid, _) = makeCluster()
    for ns in nodes: startNode(ns)

    let grp0 = nodes[0].coord.getGroup(rid)
    doAssert grp0.isSome
    grp0.get.becomeLeader()
    sleep(300)

    # Write via follower (node 1) — should fail with not-leader
    let putRes = nodes[1].store.raftPut("key", "val")
    check not putRes.isOk
    if not putRes.isOk:
      check putRes.error.kind == rseNotLeader

    for ns in nodes: stopNode(ns)

suite "MultiNode Raft — log replication":

  test "leader write is replicated to followers (applyBatchCallback)":
    let (nodes, rid, _) = makeCluster()
    for ns in nodes: startNode(ns)

    let grp0 = nodes[0].coord.getGroup(rid)
    doAssert grp0.isSome
    grp0.get.becomeLeader()
    sleep(300)

    # Write on leader
    let putRes = nodes[0].store.raftPut("replkey", "replval")
    check putRes.isOk

    # Give replication time to propagate
    sleep(300)

    # Read on followers — should see the value if applyBatchCallback fired
    for i in 1..2:
      let getRes = nodes[i].store.raftGet("replkey")
      check getRes.isOk
      if getRes.isOk:
        check getRes.value.isSome
        if getRes.value.isSome:
          check getRes.value.get.value == "replval"

    for ns in nodes: stopNode(ns)

  test "multiple writes are replicated in order":
    let (nodes, rid, _) = makeCluster()
    for ns in nodes: startNode(ns)

    let grp0 = nodes[0].coord.getGroup(rid)
    doAssert grp0.isSome
    grp0.get.becomeLeader()
    sleep(300)

    # Write several keys
    for i in 1..5:
      let k = "k" & $i
      let v = "v" & $i
      let res = nodes[0].store.raftPut(k, v)
      check res.isOk

    sleep(400)

    # Verify all replicated to node 2
    for i in 1..5:
      let k = "k" & $i
      let expected = "v" & $i
      let res = nodes[2].store.raftGet(k)
      check res.isOk
      if res.isOk and res.value.isSome:
        check res.value.get.value == expected

    for ns in nodes: stopNode(ns)

suite "MultiNode Raft — scan":

  test "leader scan returns all written keys":
    let (nodes, rid, _) = makeCluster()
    for ns in nodes: startNode(ns)

    let grp0 = nodes[0].coord.getGroup(rid)
    doAssert grp0.isSome
    grp0.get.becomeLeader()
    sleep(300)

    discard nodes[0].store.raftPut("apple", "1")
    discard nodes[0].store.raftPut("banana", "2")
    discard nodes[0].store.raftPut("cherry", "3")

    let scanRes = nodes[0].store.raftScan("", "", 100)
    check scanRes.isOk
    if scanRes.isOk:
      check scanRes.value.len == 3

    for ns in nodes: stopNode(ns)
