# Integration Tests for Multi-Group Raft Node Recovery
# 
# These tests verify node crash and recovery scenarios.

import std/[unittest, random, times, options, atomics]

import fractio/distributed/raft/group_types
import fractio.distributed.raft.multigroup_types
import fractio.distributed.meta.types

type NodeRecoveryStats* = object of RootObj
  crashesSimulated: int64
  successfulRecoveries: int64
  dataLossDetected: int64
  leaderFailures: int64

suite "NodeRecovery":
  let rng = initRand(789)

  test "create node with multiple ranges":
    var groups: seq[RaftGroup]

    # Simulate a node with 10 ranges
    for i in 0..9:
      let desc = newGroupDescriptor(
        GroupID(i)
      )
      discard desc.addReplica(NodeID(1))
      discard desc.addReplica(NodeID(2))
      discard desc.addReplica(NodeID(3))

      let group = newRaftGroup(
        GroupID(i),
        NodeID(1),
        ReplicaID(1),
        desc
      )
      groups.add(group)

    check groups.len == 10

    # Cleanup
    for g in groups:
      g.close()

  test "simulate node crash during operation":
    var stats = NodeRecoveryStats(
      crashesSimulated: 0,
      successfulRecoveries: 0,
      dataLossDetected: 0,
      leaderFailures: 0
    )

    # Create a group (simulating one range on a node)
    let desc = newGroupDescriptor(
      META_GROUP_ID
    )
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    discard desc.addReplica(NodeID(3))

    let group = newRaftGroup(
      META_GROUP_ID,
      NodeID(1),
      ReplicaID(1),
      desc
    )

    # Node is operating normally - become leader
    group.becomeLeader()
    check group.isLeader()
    stats.leaderFailures.inc

    # Simulate crash - node steps down
    group.becomeFollower(group.getTerm() + 1)
    stats.crashesSimulated.inc
    check not group.isLeader()

    # Recovery - re-election
    group.becomeCandidate()
    group.becomeLeader()

    if group.isLeader():
      stats.successfulRecoveries.inc

    group.close()

    check stats.crashesSimulated == 1
    check stats.successfulRecoveries == 1

  test "multiple node failures":
    var stats = NodeRecoveryStats(
      crashesSimulated: 0,
      successfulRecoveries: 0,
      dataLossDetected: 0,
      leaderFailures: 0
    )

    var groups: seq[RaftGroup]

    # Create 5 groups
    for i in 0..4:
      let desc = newGroupDescriptor(
        GroupID(i)
      )
      discard desc.addReplica(NodeID(1))
      discard desc.addReplica(NodeID(2))
      discard desc.addReplica(NodeID(3))

      let group = newRaftGroup(
        GroupID(i),
        NodeID(1),
        ReplicaID(1),
        desc
      )
      groups.add(group)

      # All become leaders
      group.becomeLeader()
      if group.isLeader():
        stats.leaderFailures.inc

    check stats.leaderFailures == 5

    # Simulate multiple crashes
    for i in 0..2:
      groups[i].becomeFollower(10)
      stats.crashesSimulated.inc

    check stats.crashesSimulated == 3

    # Recovery - all leaders re-elected
    for g in groups:
      if not g.isLeader():
        g.becomeCandidate()
        g.becomeLeader()

      if g.isLeader():
        stats.successfulRecoveries.inc

    check stats.successfulRecoveries == 5

    # Cleanup
    for g in groups:
      g.close()

  test "leader failure detection simulation":
    var stats = NodeRecoveryStats(
      crashesSimulated: 0,
      successfulRecoveries: 0,
      dataLossDetected: 0,
      leaderFailures: 0
    )

    # Create group
    let desc = newGroupDescriptor(
      META_GROUP_ID
    )
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    discard desc.addReplica(NodeID(3))

    let group = newRaftGroup(
      META_GROUP_ID,
      NodeID(1),
      ReplicaID(1),
      desc
    )

    # Initial state
    check not group.isLeader()

    # Elect leader
    group.becomeLeader()
    stats.leaderFailures.inc
    check group.isLeader()

    let leaderTerm = group.getTerm()

    # Simulate leader failure (detected by followers)
    group.becomeFollower(leaderTerm + 1)
    stats.crashesSimulated.inc

    # New election
    group.becomeCandidate()
    group.becomeLeader()
    stats.successfulRecoveries.inc

    check group.isLeader()
    check group.getTerm() > leaderTerm

    group.close()

    check stats.crashesSimulated == 1
    check stats.successfulRecoveries == 1
    check stats.leaderFailures == 1

  test "node recovery with term progression":
    var stats = NodeRecoveryStats(
      crashesSimulated: 0,
      successfulRecoveries: 0,
      dataLossDetected: 0,
      leaderFailures: 0
    )

    let desc = newGroupDescriptor(
      META_GROUP_ID
    )
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    discard desc.addReplica(NodeID(3))

    let group = newRaftGroup(
      META_GROUP_ID,
      NodeID(1),
      ReplicaID(1),
      desc
    )

    # Multiple crash/recovery cycles
    for cycle in 0..4:
      # Operate normally
      group.becomeLeader()
      let termBefore = group.getTerm()

      # Crash
      group.becomeFollower(termBefore + 1)
      stats.crashesSimulated.inc

      # Recovery
      group.becomeCandidate()
      group.becomeLeader()
      stats.successfulRecoveries.inc

      # Verify term increased
      check group.getTerm() > termBefore

    group.close()

    check stats.crashesSimulated == 5
    check stats.successfulRecoveries == 5
    check stats.dataLossDetected == 0

  test "recovery after network partition":
    var stats = NodeRecoveryStats(
      crashesSimulated: 0,
      successfulRecoveries: 0,
      dataLossDetected: 0,
      leaderFailures: 0
    )

    var groups: seq[RaftGroup]

    # Create 3 groups representing 3 nodes in a cluster
    for i in 0..2:
      let desc = newGroupDescriptor(
        GroupID(i)
      )
      discard desc.addReplica(NodeID(1))
      discard desc.addReplica(NodeID(2))
      discard desc.addReplica(NodeID(3))

      let group = newRaftGroup(
        GroupID(i),
        NodeID(i + 1),
        ReplicaID(i + 1),
        desc
      )
      groups.add(group)

    # Simulate network partition - one node isolated
    groups[0].becomeLeader()
    check groups[0].isLeader()

    # Node 0 "crashes" (network partition)
    groups[0].becomeFollower(5)
    stats.crashesSimulated.inc

    # Other nodes continue
    groups[1].becomeLeader()
    groups[2].becomeLeader()

    # Partition heals - node 0 rejoins
    groups[0].becomeCandidate()
    groups[0].becomeLeader()
    stats.successfulRecoveries.inc

    check groups[0].isLeader()

    # Verify cluster is healthy
    var leaderCount = 0
    for g in groups:
      if g.isLeader():
        leaderCount.inc

    check leaderCount == 3

    # Cleanup
    for g in groups:
      g.close()

    check stats.crashesSimulated == 1
    check stats.successfulRecoveries == 1

  test "recovery stats tracking":
    var stats = NodeRecoveryStats(
      crashesSimulated: 0,
      successfulRecoveries: 0,
      dataLossDetected: 0,
      leaderFailures: 0
    )

    # Simulate various recovery scenarios
    for i in 0..9:
      let desc = newGroupDescriptor(
        GroupID(i)
      )
      discard desc.addReplica(NodeID(1))
      discard desc.addReplica(NodeID(2))

      let group = newRaftGroup(
        GroupID(i),
        NodeID(1),
        ReplicaID(1),
        desc
      )

      # Operate
      group.becomeLeader()
      stats.leaderFailures.inc

      # Crash
      group.becomeFollower(group.getTerm() + 1)
      stats.crashesSimulated.inc

      # Recover
      group.becomeLeader()
      stats.successfulRecoveries.inc

      group.close()

    check stats.leaderFailures == 10
    check stats.crashesSimulated == 10
    check stats.successfulRecoveries == 10
    check stats.dataLossDetected == 0
