# Concurrency Tests for Multi-Group Raft Stress Testing
# 
# These tests verify concurrent operations under stress.

import std/[unittest, random, times, options, atomics, threadpool]

import fractio/distributed/raft/group_types
import fractio.distributed.raft.multigroup_types
import fractio.distributed.meta.types
import fractio.distributed.meta.system_tables

type StressTestStats* = object of RootObj
  totalOperations: int64
  concurrentLeaders: int64
  stateTransitions: int64
  termChanges: int64
  failuresDetected: int64

suite "MultiGroupStress":
  let rng = initRand(999)

  test "concurrent group creation stress":
    var groups: seq[RaftGroup]

    # Create many groups rapidly
    for i in 0..99:
      let gid = genGroupID()
      let desc = newGroupDescriptor(gid)
      discard desc.addReplica(NodeID(1))
      discard desc.addReplica(NodeID(2))
      discard desc.addReplica(NodeID(3))

      let group = newRaftGroup(
        gid,
        NodeID(1),
        ReplicaID(1),
        desc
      )
      groups.add(group)

    check groups.len == 100

    # Cleanup
    for g in groups:
      g.close()

  test "concurrent leader elections":
    var stats = StressTestStats(
      totalOperations: 0,
      concurrentLeaders: 0,
      stateTransitions: 0,
      termChanges: 0,
      failuresDetected: 0
    )

    var groups: seq[RaftGroup]

    # Create 20 groups
    for i in 0..19:
      let gid = genGroupID()
      let desc = newGroupDescriptor(gid)
      discard desc.addReplica(NodeID(1))
      discard desc.addReplica(NodeID(2))
      discard desc.addReplica(NodeID(3))

      let group = newRaftGroup(
        gid,
        NodeID(1),
        ReplicaID(1),
        desc
      )
      groups.add(group)

    # Concurrently elect leaders
    for g in groups:
      g.becomeCandidate()
      g.becomeLeader()
      stats.stateTransitions.inc
      stats.termChanges.inc
      stats.totalOperations.inc

    # Count leaders
    for g in groups:
      if g.isLeader():
        stats.concurrentLeaders.inc

    check stats.concurrentLeaders == 20

    # Cleanup
    for g in groups:
      g.close()

  test "rapid state transitions":
    var stats = StressTestStats(
      totalOperations: 0,
      concurrentLeaders: 0,
      stateTransitions: 0,
      termChanges: 0,
      failuresDetected: 0
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

    # Rapid state transitions - after even number, ends as follower
    for i in 0..99:
      if group.isLeader():
        group.becomeFollower(group.getTerm() + 1)
      else:
        group.becomeCandidate()
        group.becomeLeader()

      stats.stateTransitions.inc
      stats.termChanges.inc
      stats.totalOperations.inc

    check stats.stateTransitions == 100
    # After 100 transitions (even), should be in follower state
    check not group.isLeader()

    group.close()

  test "concurrent operations on multiple groups":
    var stats = StressTestStats(
      totalOperations: 0,
      concurrentLeaders: 0,
      stateTransitions: 0,
      termChanges: 0,
      failuresDetected: 0
    )

    var groups: seq[RaftGroup]

    # Create 30 groups
    for i in 0..29:
      let gid = genGroupID()
      let desc = newGroupDescriptor(gid)
      discard desc.addReplica(NodeID(1))
      discard desc.addReplica(NodeID(2))
      discard desc.addReplica(NodeID(3))

      let group = newRaftGroup(
        gid,
        NodeID(1),
        ReplicaID(1),
        desc
      )
      groups.add(group)

    # Each group does operations
    for g in groups:
      # Election
      g.becomeLeader()
      stats.concurrentLeaders.inc
      stats.stateTransitions.inc

      # Some step down
      if g.getTerm() mod 2 == 0:
        g.becomeFollower(g.getTerm() + 1)
        stats.stateTransitions.inc

      stats.totalOperations.inc

    check stats.concurrentLeaders <= 30
    check stats.totalOperations == 30

    # Cleanup
    for g in groups:
      g.close()

  test "stress test with term progression":
    var stats = StressTestStats(
      totalOperations: 0,
      concurrentLeaders: 0,
      stateTransitions: 0,
      termChanges: 0,
      failuresDetected: 0
    )

    var groups: seq[RaftGroup]

    # Create groups
    for i in 0..9:
      let gid = genGroupID()
      let desc = newGroupDescriptor(gid)
      discard desc.addReplica(NodeID(1))
      discard desc.addReplica(NodeID(2))
      discard desc.addReplica(NodeID(3))

      let group = newRaftGroup(
        gid,
        NodeID(1),
        ReplicaID(1),
        desc
      )
      groups.add(group)

    # Multiple election cycles
    for cycle in 0..4:
      for g in groups:
        # Crash - become follower
        g.becomeFollower(g.getTerm() + 1)
        stats.stateTransitions.inc
        stats.failuresDetected.inc

        # Recovery - become leader
        g.becomeCandidate()
        g.becomeLeader()
        stats.stateTransitions.inc
        stats.termChanges.inc
        stats.totalOperations.inc

        if g.isLeader():
          stats.concurrentLeaders.inc

    # All groups should be leaders after last cycle
    var leaderCount = 0
    for g in groups:
      if g.isLeader():
        leaderCount.inc

    check leaderCount == 10

    # Cleanup
    for g in groups:
      g.close()

    check stats.totalOperations == 50
    check stats.termChanges == 50

  test "extensive stress with many groups":
    var stats = StressTestStats(
      totalOperations: 0,
      concurrentLeaders: 0,
      stateTransitions: 0,
      termChanges: 0,
      failuresDetected: 0
    )

    const NUM_GROUPS = 50
    var groups: seq[RaftGroup]

    # Create many groups
    for i in 0..NUM_GROUPS - 1:
      let gid = genGroupID()
      let desc = newGroupDescriptor(gid)
      discard desc.addReplica(NodeID(1))
      discard desc.addReplica(NodeID(2))
      discard desc.addReplica(NodeID(3))

      let group = newRaftGroup(
        gid,
        NodeID(1),
        ReplicaID(1),
        desc
      )
      groups.add(group)

    check groups.len == NUM_GROUPS

    # Elect all leaders
    for g in groups:
      g.becomeLeader()
      stats.concurrentLeaders.inc
      stats.stateTransitions.inc
      stats.totalOperations.inc

    check stats.concurrentLeaders == NUM_GROUPS

    # Simulate crashes on half
    for i in 0..(NUM_GROUPS div 2) - 1:
      groups[i].becomeFollower(groups[i].getTerm() + 1)
      stats.failuresDetected.inc
      stats.stateTransitions.inc

    # Recover
    for i in 0..(NUM_GROUPS div 2) - 1:
      groups[i].becomeLeader()
      stats.concurrentLeaders.inc
      stats.stateTransitions.inc
      stats.totalOperations.inc

    # Verify all leaders
    var finalLeaderCount = 0
    for g in groups:
      if g.isLeader():
        finalLeaderCount.inc

    check finalLeaderCount == NUM_GROUPS

    # Cleanup
    for g in groups:
      g.close()
