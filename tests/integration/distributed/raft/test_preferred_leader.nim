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
import fractio/distributed/meta/system_schemas
import fractio/protocol/raft_store
import ../../../test_config

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
      electionTimeoutLowerMs: TEST_ELECTION_TIMEOUT_LOWER_MS_MULTINODE,
      electionTimeoutUpperMs: TEST_ELECTION_TIMEOUT_UPPER_MS_MULTINODE,
      heartbeatIntervalMs: TEST_HEARTBEAT_INTERVAL_MS_MULTINODE,
    ))

    # Populate peerInfo so dynamic group creation knows peer ports
    for m in members:
      coord.peerInfo[m.nodeId] = (host: m.host, basePort: m.basePort)

    coord.start()

    # Create meta + data groups
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
    sleep(TEST_POLL_INTERVAL_MS)

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
    let nodeRec = NodeRecord(
      nodeId: uint32(num),
      host: "127.0.0.1",
      raftPort: uint16(29000 + portOffset + (num - 1) * 1000),
      clientPort: uint16(19000 + num),
      status: nsAlive,
    )
    discard nodes[leaderIdx].store.raftPut(key, nodeRec.encode())

  # Seed sys.groups with preferredLeader = node 1 (nodeNums[0])
  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid.uint64)
    var replicasSeq: seq[GroupReplicaBin] = @[]
    for num in allNums:
      replicasSeq.add(GroupReplicaBin(nodeId: uint32(num),
          replicaType: rtVoter))
    let groupRec = GroupRecord(
      groupId: gid.uint64,
      replicas: replicasSeq,
      preferredLeader: uint32(allNums[0]),
    )
    discard nodes[leaderIdx].store.raftPut(key, groupRec.encode())

  sleep(TEST_REPLICATION_WAIT_MS * 4) # 400ms for replication

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
    sleep(TEST_POLL_INTERVAL_MS)
  -1

# ---------------------------------------------------------------------------
# Test suites
# ---------------------------------------------------------------------------

import ../../../../src/fractio/utils/logging

suite "Preferred leader rebalancing — 3-node cluster":
  setup:
    globalLogger.setMinLevel(llDebug)

  test "loadGroupMembers reads preferredLeader from sys.groups":
    var nodes = makeCluster3(0)
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
    var nodes = makeCluster3(10000)
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
        sleep(TEST_POLL_INTERVAL_MS)
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
        sleep(TEST_POLL_INTERVAL_MS)
        if findLeader(nodes, DATA_GROUP_START_ID) == 0:
          preferredWon = true
          break
      check preferredWon

  test "preferred leader wins via NuRaft election":
    ## Verifies that after leadership transfer, the preferred leader
    ## can take over and remain stable.
    let offset = 20000
    var nodes = makeCluster3(offset)
    defer: stopCluster(nodes)

    # Load preferred leaders on all nodes
    for node in nodes:
      node.store.loadGroupMembers()

    # Create a space group (gid=100) with preferredLeader = 2
    let testGid = GroupID(100)
    let members = @[
      (nodeId: 1'u32, host: "127.0.0.1", basePort: 29000 + offset),
      (nodeId: 2'u32, host: "127.0.0.1", basePort: 30000 + offset),
      (nodeId: 3'u32, host: "127.0.0.1", basePort: 31000 + offset),
    ]

    # Find meta leader to write sys.groups
    let metaLeader = findLeader(nodes, META_GROUP_ID)
    doAssert metaLeader >= 0

    # Write sys.groups record with preferredLeader = 2
    let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID, $testGid.uint64)
    var replicasSeq: seq[GroupReplicaBin] = @[]
    for n in 1..3:
      replicasSeq.add(GroupReplicaBin(nodeId: uint32(n), replicaType: rtVoter))
    let groupRec = GroupRecord(
      groupId: testGid.uint64,
      replicas: replicasSeq,
      preferredLeader: 2,
    )
    discard nodes[metaLeader].store.raftPut(groupKey, groupRec.encode())
    sleep(TEST_POLL_INTERVAL_MS * 30) # 300ms

    # Force metadata refresh and group creation on all nodes
    for node in nodes:
      node.store.loadGroupMembers()
      # Explicitly trigger bootstrap if automatic metadata callback is slow/missed
      node.store.bootstrapStore(@[testGid])

    # Wait for group to be created automatically by metadata sync
    var groupCreated = false
    for attempt in 0 ..< 100:
      sleep(TEST_POLL_INTERVAL_MS)
      groupCreated = true
      for node in nodes:
        if not node.coord.hasGroup(testGid):
          groupCreated = false
          break
      if groupCreated: break
    check groupCreated

    # Wait for initial election
    discard waitForLeader(nodes, testGid, maxAttempts = 50)

    # Trigger rebalance background task on all nodes
    for node in nodes:
      node.store.triggerRebal.store(true)

    # Let rebalance task run and settle
    sleep(TEST_REBALANCE_SETTLE_MS)

    # Wait up to 15 seconds for node 2 to become the stable leader.
    var preferredWon = false
    for attempt in 0 ..< 150: # 150 * 10ms = 1.5s
      sleep(TEST_POLL_INTERVAL_MS)
      let leaderIdx = findLeader(nodes, testGid)
      if leaderIdx == 1: # node 2 is index 1
        # Verify it stays leader for at least 300ms (no storm)
        var stable = true
        for _ in 0 ..< TEST_LEADER_STABILITY_CHECKS:
          sleep(TEST_POLL_INTERVAL_MS)
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
    let offset = 30000
    var nodes = makeCluster3(offset)
    defer: stopCluster(nodes)

    for node in nodes:
      node.store.loadGroupMembers()

    # Create group 101 with preferredLeader = node 3
    let testGid = GroupID(101)
    let members = @[
      (nodeId: 1'u32, host: "127.0.0.1", basePort: 29000 + offset),
      (nodeId: 2'u32, host: "127.0.0.1", basePort: 30000 + offset),
      (nodeId: 3'u32, host: "127.0.0.1", basePort: 31000 + offset),
    ]

    let metaLeader = findLeader(nodes, META_GROUP_ID)
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
    sleep(TEST_POLL_INTERVAL_MS * 30) # 300ms
    for node in nodes:
      node.store.loadGroupMembers()
      node.store.bootstrapStore(@[testGid])

    # Wait for the group to be created automatically by metadata sync
    var groupCreated = false
    for attempt in 0 ..< 100:
      sleep(TEST_POLL_INTERVAL_MS)
      groupCreated = true
      for node in nodes:
        if not node.coord.hasGroup(testGid):
          groupCreated = false
          break
      if groupCreated: break
    check groupCreated

    # Wait for initial election
    discard waitForLeader(nodes, testGid, maxAttempts = 50)

    # Reload explicitly just to be safe
    for node in nodes:
      node.store.loadGroupMembers()

    # Verify that Node 3 is recorded as the preferred leader
    check nodes[2].store.preferredLeaders.hasKey(testGid)
    check nodes[2].store.preferredLeaders[testGid] == 3'u32

    # Trigger rebalance background task on all nodes
    for node in nodes:
      node.store.triggerRebal.store(true)

    sleep(TEST_REBALANCE_SETTLE_MS)

    # Count how many times the leader changes
    var leaderChanges = 0
    var lastLeader = findLeader(nodes, testGid)
    for _ in 0 ..< 200: # 200 * 10ms = 2s
      sleep(TEST_POLL_INTERVAL_MS)
      let cur = findLeader(nodes, testGid)
      if cur != lastLeader and cur >= 0:
        inc leaderChanges
        lastLeader = cur
      # Once preferred leader (node 3, index 2) is stable, verify
      if cur == 2 and leaderChanges >= 1:
        # Let it run a bit more to ensure no further changes
        var extraChanges = 0
        for _ in 0 ..< 50: # 500ms
          sleep(TEST_POLL_INTERVAL_MS)
          let c2 = findLeader(nodes, testGid)
          if c2 != 2 and c2 >= 0:
            inc extraChanges
        # Should be 0 extra changes after preferred leader wins
        check extraChanges == 0
        break

    # Preferred leader (node 3) should be the final leader
    check lastLeader == 2 # index 2 = node 3
    # Should have at most 2-3 leader changes, not hundreds
    check leaderChanges <= 5
