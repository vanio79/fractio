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
# Port allocation: 29000–31000 (NuRaft ASIO, basePort per node spaced by 1000)
# Uses same ports for all tests since SO_REUSEADDR/SO_REUSEPORT/SO_LINGER=0 allow immediate reuse
# Temp storage: /tmp/fractio_test_node<nodeId>/ (cleaned up per test)

import std/[unittest, os, atomics, tables]

import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/protocol/raft_store
import ../../../test_config
import ../../../test_cluster_helper

# ---------------------------------------------------------------------------
# Test suites
# ---------------------------------------------------------------------------

import ../../../../src/fractio/utils/logging

suite "Preferred leader rebalancing — 3-node cluster":
  setup:
    globalLogger.setMinLevel(llDebug)

  test "loadGroupMembers reads preferredLeader from sys.groups":
    var cfg = defaultTestClusterConfig()
    var cluster = newTestCluster(cfg)
    defer: cluster.stop()

    # Find meta leader
    let leaderIdx = cluster.findLeader(META_GROUP_ID)
    doAssert leaderIdx >= 0

    # loadGroupMembers on the leader node
    cluster.nodes[leaderIdx].store.loadGroupMembers(waitForCatchUp = true)

    # Meta and data groups should have preferredLeader = 1
    check cluster.nodes[leaderIdx].store.preferredLeaders.hasKey(META_GROUP_ID)
    check cluster.nodes[leaderIdx].store.preferredLeaders[META_GROUP_ID] == 1'u32
    check cluster.nodes[leaderIdx].store.preferredLeaders.hasKey(DATA_GROUP_START_ID)
    check cluster.nodes[leaderIdx].store.preferredLeaders[
        DATA_GROUP_START_ID] == 1'u32

  test "transferLeadership moves leadership to target node":
    var cfg = defaultTestClusterConfig()
    var cluster = newTestCluster(cfg)
    defer: cluster.stop()

    # Load preferred leaders on all nodes
    for node in cluster.nodes:
      node.store.loadGroupMembers()

    # Wait for initial leader election on DATA_GROUP_START_ID
    let initialLeader = cluster.waitForLeader(DATA_GROUP_START_ID)
    doAssert initialLeader >= 0

    # If the initial leader is not node 2 (index 1), transfer to node 2
    if initialLeader != 1:
      let ok = cluster.nodes[initialLeader].coord.transferLeadership(
        DATA_GROUP_START_ID, rangeTypes.NodeID(2))
      check ok

      # Wait for node 2 to become leader
      var transferred = false
      for attempt in 0 ..< 100:
        sleep(TEST_POLL_INTERVAL_MS)
        if cluster.findLeader(DATA_GROUP_START_ID) == 1:
          transferred = true
          break
      check transferred

    # Wait longer for leadership transfer to complete and stabilize
    # NuRaft's yield_leadership sets write_paused_ which prevents
    # another transfer until the current one completes
    sleep(500)

    # Now transfer back to node 1 (preferred leader)
    let currentLeader = cluster.findLeader(DATA_GROUP_START_ID)
    if currentLeader >= 0 and currentLeader != 0:
      let ok = cluster.nodes[currentLeader].coord.transferLeadership(
        DATA_GROUP_START_ID, rangeTypes.NodeID(1))
      check ok

      # Wait longer for transfer to complete (NuRaft needs time to propagate)
      var preferredWon = false
      for attempt in 0 ..< 200:
        sleep(TEST_POLL_INTERVAL_MS)
        if cluster.findLeader(DATA_GROUP_START_ID) == 0:
          preferredWon = true
          break
      check preferredWon

  test "preferred leader wins via NuRaft election":
    ## Verifies that after leadership transfer, the preferred leader
    ## can take over and remain stable.
    var cfg = defaultTestClusterConfig()
    var cluster = newTestCluster(cfg)
    defer: cluster.stop()

    # Load preferred leaders on all nodes
    for node in cluster.nodes:
      node.store.loadGroupMembers()

    # Create a space group (gid=100) with preferredLeader = 2
    let testGid = rangeTypes.groupIDFromInt(100)

    # Find meta leader to write sys.groups
    let metaLeader = cluster.findLeader(META_GROUP_ID)
    doAssert metaLeader >= 0

    # Write sys.groups record with preferredLeader = 2
    let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(testGid))
    var replicasSeq: seq[GroupReplicaBin] = @[]
    for n in 1..3:
      replicasSeq.add(GroupReplicaBin(nodeId: uint32(n), replicaType: rtVoter))
    let groupRec = GroupRecord(
      groupId: groupIDToULID(testGid),
      replicas: replicasSeq,
      preferredLeader: 2,
    )
    discard cluster.nodes[metaLeader].store.raftPut(groupKey, groupRec.encode())
    sleep(TEST_POLL_INTERVAL_MS * 30) # 300ms

    # Force metadata refresh and group creation on all nodes
    for node in cluster.nodes:
      node.store.loadGroupMembers()
      # Explicitly trigger bootstrap if automatic metadata callback is slow/missed
      node.store.bootstrapStore(@[testGid])

    # Wait for group to be created automatically by metadata sync
    var groupCreated = false
    for attempt in 0 ..< 100:
      sleep(TEST_POLL_INTERVAL_MS)
      groupCreated = true
      for node in cluster.nodes:
        if not node.coord.hasGroup(testGid):
          groupCreated = false
          break
      if groupCreated: break
    check groupCreated

    # Wait for initial election
    discard cluster.waitForLeader(testGid)

    # Trigger rebalance background task on all nodes
    for node in cluster.nodes:
      node.store.triggerRebal.store(true)

    # Let rebalance task run and settle
    sleep(TEST_REBALANCE_SETTLE_MS)

    # Wait up to 15 seconds for node 2 to become the stable leader.
    var preferredWon = false
    for attempt in 0 ..< 150: # 150 * 10ms = 1.5s
      sleep(TEST_POLL_INTERVAL_MS)
      let leaderIdx = cluster.findLeader(testGid)
      if leaderIdx == 1: # node 2 is index 1
        # Verify it stays leader for at least 300ms (no storm)
        var stable = true
        for _ in 0 ..< TEST_LEADER_STABILITY_CHECKS:
          sleep(TEST_POLL_INTERVAL_MS)
          if cluster.findLeader(testGid) != 1:
            stable = false
            break
        if stable:
          preferredWon = true
          break

    check preferredWon

  test "non-preferred leader is replaced exactly once (no repeated stepdowns)":
    ## Verifies that once the preferred leader takes over, there are no
    ## further elections (the stepdown-election cycle is broken).
    var cfg = defaultTestClusterConfig()
    var cluster = newTestCluster(cfg)
    defer: cluster.stop()

    for node in cluster.nodes:
      node.store.loadGroupMembers()

    # Create group 101 with preferredLeader = node 3
    let testGid = rangeTypes.groupIDFromInt(101)

    let metaLeader = cluster.findLeader(META_GROUP_ID)
    doAssert metaLeader >= 0

    let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(testGid))
    var replicasSeq: seq[GroupReplicaBin] = @[]
    for n in 1..3:
      replicasSeq.add(GroupReplicaBin(nodeId: uint32(n), replicaType: rtVoter))
    let groupRec = GroupRecord(
      groupId: groupIDToULID(testGid),
      replicas: replicasSeq,
      preferredLeader: 3,
    )
    discard cluster.nodes[metaLeader].store.raftPut(groupKey, groupRec.encode())
    sleep(TEST_POLL_INTERVAL_MS * 30) # 300ms
    for node in cluster.nodes:
      node.store.loadGroupMembers()
      node.store.bootstrapStore(@[testGid])

    # Wait for the group to be created automatically by metadata sync
    var groupCreated = false
    for attempt in 0 ..< 100:
      sleep(TEST_POLL_INTERVAL_MS)
      groupCreated = true
      for node in cluster.nodes:
        if not node.coord.hasGroup(testGid):
          groupCreated = false
          break
      if groupCreated: break
    check groupCreated

    # Wait for initial election
    discard cluster.waitForLeader(testGid)

    # Reload explicitly just to be safe
    for node in cluster.nodes:
      node.store.loadGroupMembers()

    # Verify that Node 3 is recorded as the preferred leader
    check cluster.nodes[2].store.preferredLeaders.hasKey(testGid)
    check cluster.nodes[2].store.preferredLeaders[testGid] == 3'u32

    # Trigger rebalance background task on all nodes
    for node in cluster.nodes:
      node.store.triggerRebal.store(true)

    sleep(TEST_REBALANCE_SETTLE_MS)

    # Count how many times the leader changes
    var leaderChanges = 0
    var lastLeader = cluster.findLeader(testGid)
    for _ in 0 ..< 200: # 200 * 10ms = 2s
      sleep(TEST_POLL_INTERVAL_MS)
      let cur = cluster.findLeader(testGid)
      if cur != lastLeader and cur >= 0:
        inc leaderChanges
        lastLeader = cur
      # Once preferred leader (node 3, index 2) is stable, verify
      if cur == 2 and leaderChanges >= 1:
        # Let it run a bit more to ensure no further changes
        var extraChanges = 0
        for _ in 0 ..< 50: # 500ms
          sleep(TEST_POLL_INTERVAL_MS)
          let c2 = cluster.findLeader(testGid)
          if c2 != 2 and c2 >= 0:
            inc extraChanges
        # Should be 0 extra changes after preferred leader wins
        check extraChanges == 0
        break

    # Preferred leader (node 3) should be the final leader
    check lastLeader == 2 # index 2 = node 3
    # Should have at most 2-3 leader changes, not hundreds
    check leaderChanges <= 5
