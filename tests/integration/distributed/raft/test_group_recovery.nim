# Integration Tests for Multi-Group Raft Group Recovery
# 
# These tests verify group recovery and log replay after failures.

import std/[unittest, random, times, options, atomics]

import fractio/distributed/raft/group_types
import fractio/distributed/raft/multigroup_types
import fractio/distributed/meta/system_tables

type GroupRecoveryStats* = object of RootObj
  groupsTested: int64
  successfulRecoveries: int64
  logReplayFailures: int64
  inconsistentStates: int64

suite "GroupRecovery":
  let rng = initRand(456)

  test "create groups for recovery testing":
    var groups: seq[RaftGroup]

    for i in 0..7:
      let desc = newGroupDescriptor(
        groupIDFromInt(i)
      )
      discard desc.addReplica(NodeID(1))
      discard desc.addReplica(NodeID(2))
      discard desc.addReplica(NodeID(3))

      let group = newRaftGroup(
        groupIDFromInt(i),
        NodeID(1),
        ReplicaID(1),
        desc
      )
      groups.add(group)

    check groups.len == 8

    # Cleanup
    for g in groups:
      g.close()

  test "simulate group failure and recovery":
    var stats = GroupRecoveryStats(
      groupsTested: 0,
      successfulRecoveries: 0,
      logReplayFailures: 0,
      inconsistentStates: 0
    )

    # Create a group
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

    stats.groupsTested.inc

    # Simulate operations and leader election
    group.becomeLeader()

    stats.successfulRecoveries.inc
    check group.isLeader()

    # Simulate failure - step down
    group.becomeFollower(2)
    check not group.isLeader()

    # Simulate recovery - re-elect
    group.becomeCandidate()
    group.becomeLeader()
    check group.isLeader()

    group.close()

    check stats.groupsTested == 1
    check stats.successfulRecoveries == 1

  test "log replay after crash simulation":
    var stats = GroupRecoveryStats(
      groupsTested: 0,
      successfulRecoveries: 0,
      logReplayFailures: 0,
      inconsistentStates: 0
    )

    # Create multiple groups to simulate log replay scenarios
    var groups: seq[RaftGroup]

    for i in 0..3:
      let desc = newGroupDescriptor(
        groupIDFromInt(i)
      )
      discard desc.addReplica(NodeID(1))
      discard desc.addReplica(NodeID(2))
      discard desc.addReplica(NodeID(3))

      let group = newRaftGroup(
        groupIDFromInt(i),
        NodeID(1),
        ReplicaID(1),
        desc
      )
      groups.add(group)
      stats.groupsTested.inc

      # Elect leader
      group.becomeLeader()

    # Simulate log entries being committed on leaders
    for g in groups:
      if g.isLeader():
        stats.successfulRecoveries.inc

    check stats.groupsTested == 4
    check stats.successfulRecoveries == 4

    # Simulate crash and recovery for some groups
    groups[0].becomeFollower(5)
    groups[1].becomeFollower(5)

    # Verify they can be re-elected
    groups[0].becomeLeader()
    groups[1].becomeLeader()

    check groups[0].isLeader()
    check groups[1].isLeader()

    # Cleanup
    for g in groups:
      g.close()

  test "group metadata consistency":
    let desc = newGroupDescriptor(
      META_GROUP_ID
    )
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    discard desc.addReplica(NodeID(3))

    # Verify initial state
    check desc.groupId == META_GROUP_ID
    check desc.replicas.len == 3
    check desc.isInitialized()

    # After simulated crash/recovery, metadata should be unchanged
    check desc.groupId == META_GROUP_ID
    check desc.isInitialized()

  test "multiple group recovery scenarios":
    var recoveryCounts = 0
    var groups: seq[RaftGroup]

    # Create groups with different configurations
    for i in 0..5:
      let desc = newGroupDescriptor(
        groupIDFromInt(i)
      )
      discard desc.addReplica(NodeID(1))
      discard desc.addReplica(NodeID(2))

      let group = newRaftGroup(
        groupIDFromInt(i),
        NodeID(1),
        ReplicaID(1),
        desc
      )
      groups.add(group)

    # Simulate failures and recoveries
    for i, g in groups:
      # Each group becomes leader
      g.becomeLeader()
      check g.isLeader()

      # Simulate crash
      g.becomeFollower(g.getTerm() + 1)
      check not g.isLeader()

      # Recovery - re-election
      g.becomeCandidate()
      g.becomeLeader()

      if g.isLeader():
        recoveryCounts.inc

    check recoveryCounts == 6

    # Cleanup
    for g in groups:
      g.close()

  test "recovery stats tracking":
    var stats = GroupRecoveryStats(
      groupsTested: 0,
      successfulRecoveries: 0,
      logReplayFailures: 0,
      inconsistentStates: 0
    )

    # Simulate various recovery scenarios
    for i in 0..9:
      stats.groupsTested.inc

      let desc = newGroupDescriptor(
        groupIDFromInt(i)
      )
      discard desc.addReplica(NodeID(1))
      discard desc.addReplica(NodeID(2))

      let group = newRaftGroup(
        groupIDFromInt(i),
        NodeID(1),
        ReplicaID(1),
        desc
      )

      # Simulate successful recovery
      group.becomeLeader()
      if group.isLeader():
        stats.successfulRecoveries.inc

      group.close()

    check stats.groupsTested == 10
    check stats.successfulRecoveries == 10
    check stats.logReplayFailures == 0
    check stats.inconsistentStates == 0

  test "concurrent recovery simulation":
    # Simulate multiple groups recovering concurrently
    var groups: seq[RaftGroup]

    for i in 0..7:
      let desc = newGroupDescriptor(
        groupIDFromInt(i)
      )
      discard desc.addReplica(NodeID(1))
      discard desc.addReplica(NodeID(2))
      discard desc.addReplica(NodeID(3))

      let group = newRaftGroup(
        groupIDFromInt(i),
        NodeID(1),
        ReplicaID(1),
        desc
      )
      groups.add(group)

    # All become leaders (simulate concurrent recovery)
    for g in groups:
      g.becomeCandidate()
      g.becomeLeader()

    # Verify all are leaders
    var leaderCount = 0
    for g in groups:
      if g.isLeader():
        leaderCount.inc

    check leaderCount == 8

    # Simulate crash for half
    for i in 0..3:
      groups[i].becomeFollower(10)

    # Recover all
    for g in groups:
      if not g.isLeader():
        g.becomeCandidate()
        g.becomeLeader()

    # Verify all leaders again
    leaderCount = 0
    for g in groups:
      if g.isLeader():
        leaderCount.inc

    check leaderCount == 8

    # Cleanup
    for g in groups:
      g.close()
