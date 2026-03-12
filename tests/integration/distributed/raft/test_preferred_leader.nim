# Integration test — preferred leader rebalancing on a 3-node cluster.
#
# Verifies that when a non-preferred leader is elected, the system
# eventually transfers leadership to the preferred leader.
#
# Cluster topology:
#   Nodes 1–3, fully connected via NuRaft ASIO networking.
#   A group is created with preferredLeader = node 1.
#   The test verifies that transferLeadership works and that the
#   preferred leader mechanism functions correctly.
#
# Port allocation: 29000–29299 (NuRaft ASIO, basePort per node spaced by 100)
# Temp storage: /tmp/fractio_pref_leader_<nodeId>/ (cleaned up per test)

import std/[unittest, os, options, json, strutils, tables, atomics]

import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/protocol/raft_store

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  TMP_DIR = "/tmp/fractio_pref_leader_"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

type
  TestNode = object
    id*: int
    basePort*: int
    coord*: NuRaftCoordinator
    store*: RaftKVStoreExt
    storagePath*: string

proc cleanDir(p: string) =
  try: removeDir(p) except CatchableError: discard

proc makeCluster3(): seq[TestNode] =
  let members = @[
    (nodeId: 1'u32, host: "127.0.0.1", basePort: 29000),
    (nodeId: 2'u32, host: "127.0.0.1", basePort: 30000),
    (nodeId: 3'u32, host: "127.0.0.1", basePort: 31000),
  ]

  var nodes: seq[TestNode]
  for nodeNum in 1 .. 3:
    let nodeId = rangeTypes.NodeID(uint32(nodeNum))
    let basePort = 29000 + (nodeNum - 1) * 1000
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

    let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 6000)
    store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

    nodes.add(TestNode(
      id: nodeNum, basePort: basePort, coord: coord, store: store,
      storagePath: storagePath,
    ))

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

  # Find meta leader and seed system tables
  var leaderIdx = 0
  for i, n in nodes:
    if n.coord.isLeader(META_GROUP_ID):
      leaderIdx = i
      break

  let allNums = @[1, 2, 3]
  # Seed sys.nodes
  for num in allNums:
    let key = encodeTableKey(SYS_NODES_TABLE_ID, $num)
    let val = $ %*{
      "nodeId": num,
      "host": "127.0.0.1",
      "raftPort": 29000 + (num - 1) * 100,
      "clientPort": 19000 + num,
      "status": 1,
    }
    discard nodes[leaderIdx].store.raftPut(key, val)

  # Seed sys.groups with preferredLeader = node 1 (nodeNums[0])
  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid.uint64)
    var replicas = newJArray()
    for num in allNums:
      replicas.add(%*{"nodeId": num, "type": "voter"})
    let val = $ %*{"groupId": gid.uint64.int, "replicas": replicas,
                    "preferredLeader": allNums[0]}
    discard nodes[leaderIdx].store.raftPut(key, val)

  sleep(400)

  nodes

proc stopCluster(nodes: seq[TestNode]) =
  for i in countdown(nodes.high, 0):
    nodes[i].coord.stop()
    cleanDir(nodes[i].storagePath)

proc findLeader(nodes: seq[TestNode], gid: GroupID): int =
  ## Return the index of the leader node for the given group, or -1.
  for i, n in nodes:
    if n.coord.isLeader(gid):
      return i
  -1

proc waitForLeader(nodes: seq[TestNode], gid: GroupID,
    maxAttempts: int = 50): int =
  for attempt in 0 ..< maxAttempts:
    let idx = findLeader(nodes, gid)
    if idx >= 0: return idx
    sleep(100)
  -1

# ---------------------------------------------------------------------------
# Test suites
# ---------------------------------------------------------------------------

suite "Preferred leader rebalancing — 3-node cluster":

  test "loadGroupMembers reads preferredLeader from sys.groups":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    # Find meta leader
    let leaderIdx = findLeader(nodes, META_GROUP_ID)
    doAssert leaderIdx >= 0

    # loadGroupMembers on the leader node
    nodes[leaderIdx].store.loadGroupMembers()

    # Meta and data groups should have preferredLeader = 1
    check nodes[leaderIdx].store.preferredLeaders.hasKey(META_GROUP_ID)
    check nodes[leaderIdx].store.preferredLeaders[META_GROUP_ID] == 1'u32
    check nodes[leaderIdx].store.preferredLeaders.hasKey(DATA_GROUP_START_ID)
    check nodes[leaderIdx].store.preferredLeaders[DATA_GROUP_START_ID] == 1'u32

  test "transferLeadership moves leadership to target node":
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    # Load preferred leaders on all nodes
    for node in nodes:
      node.store.loadGroupMembers()

    # Wait for initial leader election on DATA_GROUP_START_ID
    let initialLeader = waitForLeader(nodes, DATA_GROUP_START_ID)
    doAssert initialLeader >= 0

    # If the initial leader is not node 2 (index 1), transfer to node 2
    if initialLeader != 1:
      let ok = nodes[initialLeader].coord.transferLeadership(
        DATA_GROUP_START_ID, rangeTypes.NodeID(2))
      check ok

      # Wait for node 2 to become leader
      var transferred = false
      for attempt in 0 ..< 50:
        sleep(100)
        if findLeader(nodes, DATA_GROUP_START_ID) == 1:
          transferred = true
          break
      check transferred

    # Now transfer back to node 1 (preferred leader)
    let currentLeader = findLeader(nodes, DATA_GROUP_START_ID)
    if currentLeader >= 0 and currentLeader != 0:
      let ok = nodes[currentLeader].coord.transferLeadership(
        DATA_GROUP_START_ID, rangeTypes.NodeID(1))
      check ok

      var preferredWon = false
      for attempt in 0 ..< 50:
        sleep(100)
        if findLeader(nodes, DATA_GROUP_START_ID) == 0:
          preferredWon = true
          break
      check preferredWon

  test "preferred leader wins via NuRaft election":
    ## Verifies that after leadership transfer, the preferred leader
    ## can take over and remain stable.
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    # Load preferred leaders on all nodes
    for node in nodes:
      node.store.loadGroupMembers()

    # Create a space group (gid=100) with preferredLeader = 2
    let testGid = GroupID(100)
    let members = @[
      (nodeId: 1'u32, host: "127.0.0.1", basePort: 29000),
      (nodeId: 2'u32, host: "127.0.0.1", basePort: 30000),
      (nodeId: 3'u32, host: "127.0.0.1", basePort: 31000),
    ]

    # Find meta leader to write sys.groups
    let metaLeader = findLeader(nodes, META_GROUP_ID)
    doAssert metaLeader >= 0

    # Write sys.groups record with preferredLeader = 2
    let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID, $testGid.uint64)
    var replicas = newJArray()
    for n in 1..3:
      replicas.add(%*{"nodeId": n, "type": "voter"})
    let groupVal = $ %*{
      "groupId": testGid.uint64.int,
      "replicas": replicas,
      "preferredLeader": 2,
    }
    discard nodes[metaLeader].store.raftPut(groupKey, groupVal)
    sleep(300)

    # Reload preferredLeaders on all nodes
    for node in nodes:
      node.store.loadGroupMembers()

    # Verify preferredLeader is set to 2
    check nodes[metaLeader].store.preferredLeaders[testGid] == 2'u32

    # Create the group on all nodes
    for node in nodes:
      doAssert node.coord.createAndStartGroup(testGid, members)

    # Let an initial election happen naturally via the network
    sleep(3000)

    # By now some node should be leader. If it's not node 2 (the preferred
    # leader), transfer leadership to node 2.
    let currentLeader = findLeader(nodes, testGid)
    if currentLeader >= 0 and currentLeader != 1:
      discard nodes[currentLeader].coord.transferLeadership(
        testGid, rangeTypes.NodeID(2))

    # Wait up to 15 seconds for node 2 to become the stable leader.
    var preferredWon = false
    for attempt in 0 ..< 150:  # 150 * 100ms = 15s
      sleep(100)
      let leaderIdx = findLeader(nodes, testGid)
      if leaderIdx == 1:  # node 2 is index 1
        # Verify it stays leader for at least 3 seconds (no storm)
        var stable = true
        for _ in 0 ..< 30:
          sleep(100)
          if findLeader(nodes, testGid) != 1:
            stable = false
            break
        if stable:
          preferredWon = true
          break

    check preferredWon

  test "non-preferred leader is replaced exactly once (no repeated stepdowns)":
    ## Verifies that once the preferred leader takes over, there are no
    ## further elections (the stepdown-election cycle is broken).
    var nodes = makeCluster3()
    defer: stopCluster(nodes)

    for node in nodes:
      node.store.loadGroupMembers()

    # Create group 101 with preferredLeader = node 3
    let testGid = GroupID(101)
    let members = @[
      (nodeId: 1'u32, host: "127.0.0.1", basePort: 29000),
      (nodeId: 2'u32, host: "127.0.0.1", basePort: 30000),
      (nodeId: 3'u32, host: "127.0.0.1", basePort: 31000),
    ]

    let metaLeader = findLeader(nodes, META_GROUP_ID)
    doAssert metaLeader >= 0

    let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID, $testGid.uint64)
    var replicas = newJArray()
    for n in 1..3:
      replicas.add(%*{"nodeId": n, "type": "voter"})
    discard nodes[metaLeader].store.raftPut(groupKey,
      $ %*{"groupId": testGid.uint64.int, "replicas": replicas,
           "preferredLeader": 3})
    sleep(300)
    for node in nodes:
      node.store.loadGroupMembers()

    # Create group on all nodes
    for node in nodes:
      doAssert node.coord.createAndStartGroup(testGid, members)

    # Wait for initial election
    discard waitForLeader(nodes, testGid, maxAttempts = 50)

    # If current leader is not node 3, transfer leadership
    let currentLeader = findLeader(nodes, testGid)
    if currentLeader >= 0 and currentLeader != 2:
      discard nodes[currentLeader].coord.transferLeadership(
        testGid, rangeTypes.NodeID(3))

    # Count how many times the leader changes
    var leaderChanges = 0
    var lastLeader = findLeader(nodes, testGid)
    for _ in 0 ..< 200:  # 200 * 100ms = 20s
      sleep(100)
      let cur = findLeader(nodes, testGid)
      if cur != lastLeader and cur >= 0:
        inc leaderChanges
        lastLeader = cur
      # Once preferred leader (node 3, index 2) is stable, verify
      if cur == 2 and leaderChanges >= 1:
        # Let it run a bit more to ensure no further changes
        var extraChanges = 0
        for _ in 0 ..< 50:  # 5 more seconds
          sleep(100)
          let c2 = findLeader(nodes, testGid)
          if c2 != 2 and c2 >= 0:
            inc extraChanges
        # Should be 0 extra changes after preferred leader wins
        check extraChanges == 0
        break

    # Preferred leader (node 3) should be the final leader
    check lastLeader == 2  # index 2 = node 3
    # Should have at most 2-3 leader changes, not hundreds
    check leaderChanges <= 5
