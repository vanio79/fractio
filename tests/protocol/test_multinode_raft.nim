# Phase 6 — Multi-node Raft integration tests.
#
# Starts a real 3-node Raft cluster using NuRaftCoordinator with ASIO
# networking. Verifies:
#   1. Leader election — exactly one leader after startup.
#   2. Log replication — write on leader is visible on followers.
#   3. Follower apply — applyBatchCallback drives KV state machine on followers.
#   4. Quorum write — proposeAndWait returns success on leader with 3 nodes.
#
# Port allocation: 24000–24299 (NuRaft ASIO ports, basePort per node spaced by 100).
# Temp storage: /tmp/fractio_mn_<nodeId>/ (cleaned up per test).

import std/[unittest, os, times, options, tables]

import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/protocol/raft_store

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

const
  TMP_DIR = "/tmp/fractio_mn_"

type
  NodeSetup = object
    coord*: NuRaftCoordinator
    store*: RaftKVStoreExt
    storagePath*: string

proc cleanDir(p: string) =
  try: removeDir(p) except CatchableError: discard
  try: createDir(p) except CatchableError: discard

proc stopNode(ns: NodeSetup) =
  # Stop the store's rebalance thread BEFORE stopping the coordinator
  ns.store.stop()
  ns.coord.stop()
  cleanDir(ns.storagePath)

proc stopAllNodes(nodes: seq[NodeSetup]) =
  ## Stop all nodes and wait for cleanup
  for ns in nodes:
    ns.store.stop()
    ns.coord.stop()
  # Wait for sockets to fully close with SO_LINGER zero
  # and any background threads to terminate
  # Need sufficient time for TimerThread cleanup and socket release
  sleep(300)
  for ns in nodes:
    cleanDir(ns.storagePath)

proc waitForLeader(nodes: seq[NodeSetup], groupId: GroupID,
    maxAttempts: int = 200): int =
  ## Wait for a leader to be elected. Returns leader index or -1.
  ## Uses 200 attempts * 100ms = 20s max wait time for reliability.
  for attempt in 0 ..< maxAttempts:
    for i, ns in nodes:
      if ns.coord.isLeader(groupId):
        return i
    sleep(100)
  -1

# ---------------------------------------------------------------------------
# Shared cluster fixture — creates all nodes before waiting for init
# ---------------------------------------------------------------------------
# Use fixed ports: SO_REUSE and SO_LINGER zero allow reuse between tests

const
  BASE_PORT = 24000
  PORT_SPACING = 100

proc makeCluster(): (seq[NodeSetup], GroupID) =
  let rid = DATA_GROUP_START_ID
  let members = @[
    (nodeId: 1'u32, host: "127.0.0.1", port: BASE_PORT),
    (nodeId: 2'u32, host: "127.0.0.1", port: BASE_PORT + PORT_SPACING),
    (nodeId: 3'u32, host: "127.0.0.1", port: BASE_PORT + 2 * PORT_SPACING),
  ]

  # Use node 1 as preferred leader to avoid election races
  let preferredLeader = members[0].nodeId

  # Create all coordinators first (don't wait for init between nodes)
  var nodes: seq[NodeSetup] = @[]
  for i in 0 ..< 3:
    let nodeNum = i + 1
    let nodeId = rangeTypes.NodeID(uint32(nodeNum))
    let storagePath = TMP_DIR & $nodeNum
    cleanDir(storagePath)

    let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
      nodeId: nodeId,
      port: members[i].port,
      host: "127.0.0.1",
      dataDir: storagePath,
      electionTimeoutLowerMs: 200,
      electionTimeoutUpperMs: 400,
      heartbeatIntervalMs: 100,
    ))
    coord.start()

    doAssert coord.createAndStartGroup(rid, members, preferredLeader),
        "Failed to create group on node " & $nodeNum

    let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 6000)
    store.bootstrapStore(@[rid])

    nodes.add(NodeSetup(coord: coord, store: store, storagePath: storagePath))

  (nodes, rid)

# ---------------------------------------------------------------------------
# Test suites
# ---------------------------------------------------------------------------

suite "MultiNode Raft — leader election":

  test "exactly one leader after startup":
    # First test needs extra time for system initialization
    sleep(100)

    let (nodes, rid) = makeCluster()

    # Wait for leader election
    let leaderIdx = waitForLeader(nodes, rid)
    check leaderIdx >= 0

    stopAllNodes(nodes)

  test "only one node is leader at any time":
    let (nodes, rid) = makeCluster()

    discard waitForLeader(nodes, rid)

    var leaderCount = 0
    for ns in nodes:
      if ns.coord.isLeader(rid): inc leaderCount
    check leaderCount == 1

    stopAllNodes(nodes)

suite "MultiNode Raft — quorum write":

  test "proposeAndWait succeeds on leader with 3 nodes":
    let (nodes, rid) = makeCluster()

    let leaderIdx = waitForLeader(nodes, rid)
    check leaderIdx >= 0

    # Write via leader
    let putRes = nodes[leaderIdx].store.raftPut("hello", "world")
    check putRes.isOk
    if putRes.isOk:
      check putRes.value.value == "world"

    stopAllNodes(nodes)

  test "proposeAndWait on follower returns Not the leader":
    let (nodes, rid) = makeCluster()

    let leaderIdx = waitForLeader(nodes, rid)
    check leaderIdx >= 0

    # Write via a follower — should fail with not-leader
    let followerIdx = (leaderIdx + 1) mod 3
    let putRes = nodes[followerIdx].store.raftPut("key", "val")
    check not putRes.isOk
    if not putRes.isOk:
      check putRes.error.kind == rseNotLeader

    stopAllNodes(nodes)

suite "MultiNode Raft — log replication":

  test "leader write is replicated to followers (applyBatchCallback)":
    let (nodes, rid) = makeCluster()

    let leaderIdx = waitForLeader(nodes, rid)
    check leaderIdx >= 0
    echo "DEBUG: Leader is node ", leaderIdx + 1

    # Write on leader
    let putRes = nodes[leaderIdx].store.raftPut("replkey", "replval")
    check putRes.isOk
    echo "DEBUG: Put succeeded on leader"

    # Give replication time to propagate - need more time for first replication
    # Also poll for the value to appear on followers
    var allReplicated = false
    for attempt in 0 ..< 20: # 10 seconds max
      sleep(500)
      allReplicated = true
      for i in 0 ..< 3:
        if i == leaderIdx: continue
        let getRes = nodes[i].store.raftGet("replkey")
        echo "DEBUG: Attempt ", attempt, " node ", i + 1, " getRes.isOk=",
            getRes.isOk, " isNone=", getRes.isOk and getRes.value.isNone
        if not getRes.isOk or getRes.value.isNone:
          allReplicated = false
          break
      if allReplicated:
        echo "DEBUG: All replicated at attempt ", attempt
        break

    # Read on followers — should see the value if applyBatchCallback fired
    for i in 0 ..< 3:
      if i == leaderIdx: continue
      let getRes = nodes[i].store.raftGet("replkey")
      check getRes.isOk
      if getRes.isOk:
        check getRes.value.isSome
        if getRes.value.isSome:
          check getRes.value.get.value == "replval"

    stopAllNodes(nodes)

  test "multiple writes are replicated in order":
    let (nodes, rid) = makeCluster()

    let leaderIdx = waitForLeader(nodes, rid)
    check leaderIdx >= 0

    # Write several keys
    for i in 1..5:
      let k = "k" & $i
      let v = "v" & $i
      let res = nodes[leaderIdx].store.raftPut(k, v)
      check res.isOk

    sleep(400)

    # Verify all replicated to a follower
    let followerIdx = (leaderIdx + 1) mod 3
    for i in 1..5:
      let k = "k" & $i
      let expected = "v" & $i
      let res = nodes[followerIdx].store.raftGet(k)
      check res.isOk
      if res.isOk and res.value.isSome:
        check res.value.get.value == expected

    stopAllNodes(nodes)

suite "MultiNode Raft — scan":

  test "leader scan returns all written keys":
    let (nodes, rid) = makeCluster()

    let leaderIdx = waitForLeader(nodes, rid)
    check leaderIdx >= 0

    discard nodes[leaderIdx].store.raftPut("apple", "1")
    discard nodes[leaderIdx].store.raftPut("banana", "2")
    discard nodes[leaderIdx].store.raftPut("cherry", "3")

    let scanRes = nodes[leaderIdx].store.raftScan("", "", 100)
    check scanRes.isOk
    if scanRes.isOk:
      check scanRes.value.len == 3

    stopAllNodes(nodes)
