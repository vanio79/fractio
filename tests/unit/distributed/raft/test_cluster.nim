# Unit Tests for Cluster Management

import unittest
import std/[tables, sets, times]

import fractio/distributed/raft/types
import fractio/distributed/raft/node

# Test State Machine
type
  TestStateMachine* = ref object of StateMachine
    ## Test state machine for cluster testing
    appliedLogs*: seq[(int64, string)]
    lastAppliedIndex*: int64

method commit*(sm: TestStateMachine, logIdx: int64, data: string): string =
  ## Test commit implementation
  sm.appliedLogs.add((logIdx, data))
  sm.lastAppliedIndex = logIdx
  return "OK"

method rollback*(sm: TestStateMachine, logIdx: int64, data: string) =
  ## Test rollback implementation
  sm.appliedLogs.keepItIf(it[0] != logIdx)
  if sm.appliedLogs.len > 0:
    sm.lastAppliedIndex = sm.appliedLogs[^1][0]
  else:
    sm.lastAppliedIndex = 0

method getLastAppliedIndex*(sm: TestStateMachine): int64 =
  ## Get last applied log index
  result = sm.lastAppliedIndex


# Test Fixtures
type
  TestSetup* = object
    node1*: RaftNodeImpl
    node2*: RaftNodeImpl
    node3*: RaftNodeImpl
    stateMachine1*: TestStateMachine
    stateMachine2*: TestStateMachine
    stateMachine3*: TestStateMachine
    config1*: RaftConfig
    config2*: RaftConfig
    config3*: RaftConfig

proc setupTest*(): TestSetup =
  ## Setup test environment with 3-node cluster
  result.config1 = RaftConfig(
    serverId: 1,
    endpoint: "127.0.0.1:9000",
    electionTimeout: 1000,
    heartbeatInterval: 100,
    logStoragePath: "tmp/raft_cluster_1/",
    snapshotEnabled: false,
    snapshotDistance: 1000,
    maxAppendSize: 100
  )

  result.config2 = RaftConfig(
    serverId: 2,
    endpoint: "127.0.0.1:9001",
    electionTimeout: 1000,
    heartbeatInterval: 100,
    logStoragePath: "tmp/raft_cluster_2/",
    snapshotEnabled: false,
    snapshotDistance: 1000,
    maxAppendSize: 100
  )

  result.config3 = RaftConfig(
    serverId: 3,
    endpoint: "127.0.0.1:9002",
    electionTimeout: 1000,
    heartbeatInterval: 100,
    logStoragePath: "tmp/raft_cluster_3/",
    snapshotEnabled: false,
    snapshotDistance: 1000,
    maxAppendSize: 100
  )

  result.stateMachine1 = TestStateMachine(
    appliedLogs: @[],
    lastAppliedIndex: 0
  )

  result.stateMachine2 = TestStateMachine(
    appliedLogs: @[],
    lastAppliedIndex: 0
  )

  result.stateMachine3 = TestStateMachine(
    appliedLogs: @[],
    lastAppliedIndex: 0
  )

  # Create nodes (simplified - in real implementation these would have transport)
  result.node1 = RaftNodeImpl(
    serverId: result.config1.serverId,
    endpoint: result.config1.endpoint,
    config: result.config1,
    nodeState: RaftNodeState(
      role: SR_FOLLOWER,
      currentTerm: 0,
      votedFor: -1,
      leaderId: -1,
      commitIndex: 0,
      lastApplied: 0
    ),
    logStore: nil, # Would be WiscKeyLogStore in real implementation
    stateMachine: result.stateMachine1,
    initialized: true,
    isLeader: false,
    leaderId: -1,
    commitIndex: 0,
    lastApplied: 0
  )

  result.node2 = RaftNodeImpl(
    serverId: result.config2.serverId,
    endpoint: result.config2.endpoint,
    config: result.config2,
    nodeState: RaftNodeState(
      role: SR_FOLLOWER,
      currentTerm: 0,
      votedFor: -1,
      leaderId: -1,
      commitIndex: 0,
      lastApplied: 0
    ),
    logStore: nil,
    stateMachine: result.stateMachine2,
    initialized: true,
    isLeader: false,
    leaderId: -1,
    commitIndex: 0,
    lastApplied: 0
  )

  result.node3 = RaftNodeImpl(
    serverId: result.config3.serverId,
    endpoint: result.config3.endpoint,
    config: result.config3,
    nodeState: RaftNodeState(
      role: SR_FOLLOWER,
      currentTerm: 0,
      votedFor: -1,
      leaderId: -1,
      commitIndex: 0,
      lastApplied: 0
    ),
    logStore: nil,
    stateMachine: result.stateMachine3,
    initialized: true,
    isLeader: false,
    leaderId: -1,
    commitIndex: 0,
    lastApplied: 0
  )

  # Initialize nodes
  discard result.node1.init(result.config1, result.stateMachine1)
  discard result.node2.init(result.config2, result.stateMachine2)
  discard result.node3.init(result.config3, result.stateMachine3)

proc teardownTest*(setup: var TestSetup) =
  ## Teardown test environment
  setup.node1.shutdown()
  setup.node2.shutdown()
  setup.node3.shutdown()

  # Clean up test directories
  removeDir(setup.config1.logStoragePath)
  removeDir(setup.config2.logStoragePath)
  removeDir(setup.config3.logStoragePath)


# Test Suite
suite "Cluster Management Tests":

  setup:
    var testSetup = setupTest()

  teardown:
    teardownTest(testSetup)

  test "Three-node cluster initialization":
    check testSetup.node1.initialized == true
    check testSetup.node2.initialized == true
    check testSetup.node3.initialized == true

    check testSetup.node1.serverId == 1
    check testSetup.node2.serverId == 2
    check testSetup.node3.serverId == 3

    check testSetup.node1.nodeState.role == SR_FOLLOWER
    check testSetup.node2.nodeState.role == SR_FOLLOWER
    check testSetup.node3.nodeState.role == SR_FOLLOWER

  test "Server role transitions in cluster":
    # Node 1 becomes candidate
    testSetup.node1.becomeCandidate()
    check testSetup.node1.nodeState.role == SR_CANDIDATE
    check testSetup.node1.nodeState.currentTerm == 1
    check testSetup.node1.nodeState.votedFor == 1

    # Node 1 becomes leader
    testSetup.node1.becomeLeader()
    check testSetup.node1.nodeState.role == SR_LEADER
    check testSetup.node1.isLeader == true
    check testSetup.node1.nodeState.leaderId == 1

    # Node 1 steps down
    testSetup.node1.stepDown(2)
    check testSetup.node1.nodeState.role == SR_FOLLOWER
    check testSetup.node1.nodeState.currentTerm == 2
    check testSetup.node1.nodeState.votedFor == -1

  test "Commit operation with multiple nodes":
    # Node 1 becomes leader
    testSetup.node1.becomeCandidate()
    testSetup.node1.becomeLeader()

    let result1 = testSetup.node1.commit("leader commit")
    check result1 > 0

    # Verify state machine applied
    check testSetup.stateMachine1.appliedLogs.len == 1
    check testSetup.stateMachine1.appliedLogs[0][1] == "leader commit"

    # In real implementation, this would replicate to other nodes
    # For testing, we simulate state machine application on other nodes
    discard testSetup.stateMachine2.commit(result1, "leader commit")
    discard testSetup.stateMachine3.commit(result1, "leader commit")

    check testSetup.stateMachine2.appliedLogs.len == 1
    check testSetup.stateMachine3.appliedLogs.len == 1

  test "Server state consistency":
    # All nodes should start as followers
    check testSetup.node1.nodeState.role == SR_FOLLOWER
    check testSetup.node2.nodeState.role == SR_FOLLOWER
    check testSetup.node3.nodeState.role == SR_FOLLOWER

    check testSetup.node1.nodeState.currentTerm == 0
    check testSetup.node2.nodeState.currentTerm == 0
    check testSetup.node3.nodeState.currentTerm == 0

    # After election, terms should be consistent
    testSetup.node1.becomeCandidate()
    testSetup.node1.becomeLeader()

    check testSetup.node1.nodeState.currentTerm == 1
    check testSetup.node2.nodeState.currentTerm == 0 # Still 0 (follower)
    check testSetup.node3.nodeState.currentTerm == 0 # Still 0 (follower)

  test "Log consistency across nodes":
    # Node 1 becomes leader and commits
    testSetup.node1.becomeCandidate()
    testSetup.node1.becomeLeader()

    let commitIndex = testSetup.node1.commit("consistent data")
    check commitIndex > 0

    # In real implementation, this would be replicated
    # For testing, we simulate consistent state
    testSetup.stateMachine2.commit(commitIndex, "consistent data")
    testSetup.stateMachine3.commit(commitIndex, "consistent data")

    # Verify all state machines have same data
    check testSetup.stateMachine1.appliedLogs[0][1] == "consistent data"
    check testSetup.stateMachine2.appliedLogs[0][1] == "consistent data"
    check testSetup.stateMachine3.appliedLogs[0][1] == "consistent data"

  test "Multiple commits in sequence":
    testSetup.node1.becomeCandidate()
    testSetup.node1.becomeLeader()

    let idx1 = testSetup.node1.commit("first")
    let idx2 = testSetup.node1.commit("second")
    let idx3 = testSetup.node1.commit("third")

    check idx1 < idx2
    check idx2 < idx3

    # Verify sequence
    check testSetup.stateMachine1.appliedLogs.len == 3
    check testSetup.stateMachine1.appliedLogs[0][1] == "first"
    check testSetup.stateMachine1.appliedLogs[1][1] == "second"
    check testSetup.stateMachine1.appliedLogs[2][1] == "third"

  test "State machine recovery":
    # Commit some data
    testSetup.node1.becomeCandidate()
    testSetup.node1.becomeLeader()

    let idx = testSetup.node1.commit("recoverable")
    check idx > 0

    # Verify committed
    check testSetup.stateMachine1.appliedLogs.len == 1

    # Simulate recovery - create new state machine
    var recoveredSM = TestStateMachine(
      appliedLogs: @[],
      lastAppliedIndex: 0
    )

    # In real recovery, this would load from log
    # For testing, we manually apply
    discard recoveredSM.commit(idx, "recoverable")

    check recoveredSM.appliedLogs.len == 1
    check recoveredSM.appliedLogs[0][1] == "recoverable"

  test "Concurrent commits simulation":
    # Simulate concurrent operations from multiple clients
    testSetup.node1.becomeCandidate()
    testSetup.node1.becomeLeader()

    var indices: seq[int64]

    # Simulate multiple concurrent commits
    indices.add(testSetup.node1.commit("op1"))
    indices.add(testSetup.node1.commit("op2"))
    indices.add(testSetup.node1.commit("op3"))

    check indices.len == 3
    check indices[0] < indices[1]
    check indices[1] < indices[2]

    # Verify all operations were committed
    check testSetup.stateMachine1.appliedLogs.len == 3
    let data = testSetup.stateMachine1.appliedLogs.mapIt(it[1])
    check "op1" in data
    check "op2" in data
    check "op3" in data

  test "Leadership transfer simulation":
    # Node 1 becomes leader
    testSetup.node1.becomeCandidate()
    testSetup.node1.becomeLeader()

    # Commit some data
    let idx1 = testSetup.node1.commit("leader1-data")
    check idx1 > 0

    # Simulate leader stepping down (higher term arrives)
    testSetup.node1.stepDown(2)

    # Node 2 becomes new leader
    testSetup.node2.becomeCandidate()
    testSetup.node2.becomeLeader()
    check testSetup.node2.nodeState.currentTerm == 2
    check testSetup.node2.isLeader == true

    # New leader commits data
    let idx2 = testSetup.node2.commit("leader2-data")
    check idx2 > idx1 # New term should have higher index
    
    # Verify new leader's data
    check testSetup.stateMachine2.appliedLogs.len == 1
    check testSetup.stateMachine2.appliedLogs[0][1] == "leader2-data"

  test "Cluster state after node failure":
    # Node 1 becomes leader
    testSetup.node1.becomeCandidate()
    testSetup.node1.becomeLeader()

    # Commit data
    let idx = testSetup.node1.commit("before-failure")
    check idx > 0

    # Simulate node 1 failure
    # In real implementation, this would trigger re-election

    # Node 2 becomes new leader
    testSetup.node2.becomeCandidate()
    testSetup.node2.becomeLeader()

    # New leader commits new data
    let newIdx = testSetup.node2.commit("after-recovery")
    check newIdx > idx

    # Verify new leader's data
    check testSetup.stateMachine2.appliedLogs.len == 1
    check testSetup.stateMachine2.appliedLogs[0][1] == "after-recovery"

  test "State machine interface compliance":
    # Verify state machine implements required interface
    check compiles(testSetup.stateMachine1.commit(1, "test"))
    check compiles(testSetup.stateMachine1.rollback(1, "test"))
    check compiles(testSetup.stateMachine1.getLastAppliedIndex())

  test "Invalid operations on follower":
    expect RaftError:
      discard testSetup.node2.commit("should-fail")

    check testSetup.node2.nodeState.role == SR_FOLLOWER

  test "Invalid operations on candidate":
    testSetup.node3.becomeCandidate()

    expect RaftError:
      discard testSetup.node3.commit("should-fail")

    check testSetup.node3.nodeState.role == SR_CANDIDATE
