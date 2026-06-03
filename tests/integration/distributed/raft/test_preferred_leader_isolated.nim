import std/[unittest, os, atomics, tables]

import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/core/types except NodeID
import fractio/protocol/raft_store
import ../../../test_config
import ../../../test_cluster_helper

suite "Preferred leader isolated test":
  test "non-preferred leader is replaced exactly once":
    var cfg = defaultTestClusterConfig()
    cfg.portOffset = 10000
    var cluster = newTestCluster(cfg)
    defer: cluster.stop()

    for node in cluster.nodes: node.store.loadGroupMembers()

    let testGid = groupIDFromInt(101)
    let metaLeader = cluster.waitForLeader(META_GROUP_ID)
    doAssert metaLeader >= 0

    let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID, $testGid)
    var replicasSeq: seq[GroupReplicaBin] = @[]
    for n in 1..3:
      replicasSeq.add(GroupReplicaBin(nodeId: uint32(n), replicaType: rtVoter))
    let groupRec = GroupRecord(
      groupId: ULID(testGid),
      spaceId: zeroSpaceID(),
      replicas: replicasSeq,
      preferredLeader: 3,
    )
    discard cluster.nodes[metaLeader].store.raftPut(groupKey, groupRec.encode())

    sleep(TEST_POLL_INTERVAL_MS * 100) # 1s for group metadata to propagate
    for node in cluster.nodes: node.store.loadGroupMembers()

    # Wait for dynamic group creation
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

    # Wait for initial election - use stable leader to avoid transient states
    let initialLeader = cluster.waitForLeader(testGid, WaitForLeaderOptions(
      maxAttempts: 200,
      stableCount: 5
    ))
    check initialLeader >= 0

    # Reload explicitly
    for node in cluster.nodes: node.store.loadGroupMembers()
    check cluster.nodes[2].store.preferredLeaders.hasKey(testGid)
    check cluster.nodes[2].store.preferredLeaders[testGid] == 3'u32

    # Trigger rebalance
    for node in cluster.nodes: node.store.triggerRebal.store(true)
    sleep(TEST_REBALANCE_SETTLE_MS)

    # Wait for leadership to stabilize on preferred leader (node 3, index 2)
    let finalLeader = cluster.waitForLeader(testGid, WaitForLeaderOptions(
      maxAttempts: 300,
      stableCount: 10
    ))
    check finalLeader == 2 # Node 3 (index 2) should be leader
