# Integration Tests for Single Node Lifecycle and Recovery

import unittest
import std/[options, os, strutils, tables, posix]

import fractio/distributed/raft/types
import fractio/distributed/raft/node
import fractio/distributed/raft/state_machine

# Test State Machine
type
  TestStateMachine* = ref object of StateMachine
    ## Test state machine for integration testing
    commits*: seq[(int64, string)]
    rollbacks*: seq[(int64, string)]
    lastAppliedIndex*: int64

  TestStateMachineError* = object of CatchableError

method commit*(sm: TestStateMachine, logIdx: int64, data: string): string =
  ## Test commit implementation
  sm.commits.add((logIdx, data))
  sm.lastAppliedIndex = logIdx
  return "OK"

method rollback*(sm: TestStateMachine, logIdx: int64, data: string) =
  ## Test rollback implementation
  sm.rollbacks.add((logIdx, data))

method getLastAppliedIndex*(sm: TestStateMachine): int64 =
  ## Get last applied log index
  result = sm.lastAppliedIndex


# Test Fixtures
type
  LifecycleTestSetup* = object
    node*: RaftNodeImpl
    stateMachine*: TestStateMachine
    config*: RaftConfig
    testPath*: string

proc setupLifecycleTest*(path: string = "tmp/raft_lifecycle_test/"): LifecycleTestSetup =
  ## Setup lifecycle test environment
  result.testPath = path
  # Ensure tmp directory exists
  if not dirExists("tmp"):
    createDir("tmp")
  # Clean up any existing test directory
  if dirExists(path):
    removeDir(path)

  result.config = RaftConfig(
    serverId: 1,
    endpoint: "127.0.0.1:9000",
    electionTimeout: 1000,
    heartbeatInterval: 100,
    logStoragePath: path,
    snapshotEnabled: false,
    snapshotDistance: 1000,
    maxAppendSize: 100
  )

  result.stateMachine = TestStateMachine(
    commits: @[],
    rollbacks: @[],
    lastAppliedIndex: 0
  )

  result.node = RaftNodeImpl(
    serverId: result.config.serverId,
    endpoint: result.config.endpoint,
    config: result.config,
    nodeState: RaftNodeState(
      role: SR_FOLLOWER,
      currentTerm: 0,
      votedFor: -1,
      leaderId: -1,
      commitIndex: 0,
      lastApplied: 0
    ),
    logStore: nil,
    stateMachine: result.stateMachine,
    initialized: false,
    isLeader: false,
    leaderId: -1,
    commitIndex: 0,
    lastApplied: 0
  )

proc teardownLifecycleTest*(setup: var LifecycleTestSetup) =
  ## Teardown lifecycle test environment
  if setup.node.initialized:
    setup.node.shutdown()
  # Clean up test directory
  if dirExists(setup.testPath):
    # Remove all files first to ensure LevelDB locks are released
    try:
      removeDir(setup.testPath)
    except:
      discard


# Test Suite
suite "Single Node Lifecycle Tests":

  var testCounter = 0

  setup:
    inc testCounter
    var testSetup = setupLifecycleTest("tmp/raft_lifecycle_" & $testCounter & "/")

  teardown:
    teardownLifecycleTest(testSetup)

  test "Node initialization":
    let success = testSetup.node.init(testSetup.config, testSetup.stateMachine)
    check success == true
    check testSetup.node.initialized == true
    check testSetup.node.serverId == 1
    check testSetup.node.endpoint == "127.0.0.1:9000"

  test "Node initialization creates log store":
    let success = testSetup.node.init(testSetup.config, testSetup.stateMachine)
    check success == true
    check testSetup.node.logStore != nil

  test "Node shutdown":
    discard testSetup.node.init(testSetup.config, testSetup.stateMachine)
    testSetup.node.shutdown()
    check testSetup.node.initialized == false

  test "Node cannot commit when not leader":
    discard testSetup.node.init(testSetup.config, testSetup.stateMachine)
    var exceptionRaised = false
    try:
      discard testSetup.node.commit("should fail")
    except node.RaftError:
      exceptionRaised = true
    check exceptionRaised

  test "Node becomes leader and commits":
    discard testSetup.node.init(testSetup.config, testSetup.stateMachine)
    testSetup.node.becomeCandidate()
    testSetup.node.becomeLeader()

    let idx = testSetup.node.commit("leader commit")
    check idx > 0

  test "Node state persists across restart":
    # First instance: create and write data
    var setup1 = setupLifecycleTest("tmp/raft_persist_test/")
    discard setup1.node.init(setup1.config, setup1.stateMachine)
    setup1.node.becomeCandidate()
    setup1.node.becomeLeader()

    let idx = setup1.node.commit("persistent data")
    check idx == 1
    setup1.node.shutdown()

    # Second instance: recover and verify
    var stateMachine2 = TestStateMachine(
      commits: @[],
      rollbacks: @[],
      lastAppliedIndex: 0
    )
    var node2 = RaftNodeImpl(
      serverId: setup1.config.serverId,
      endpoint: setup1.config.endpoint,
      config: setup1.config,
      nodeState: RaftNodeState(
        role: SR_FOLLOWER,
        currentTerm: 0,
        votedFor: -1,
        leaderId: -1,
        commitIndex: 0,
        lastApplied: 0
      ),
      logStore: nil,
      stateMachine: stateMachine2,
      initialized: false,
      isLeader: false,
      leaderId: -1,
      commitIndex: 0,
      lastApplied: 0
    )

    let success = node2.init(setup1.config, stateMachine2)
    check success == true

    # Verify data persisted
    let entry = node2.wsLogStore.getEntry(1)
    check entry.isSome
    check entry.get.data == "persistent data"

    node2.shutdown()
    removeDir("tmp/raft_persist_test/")

  test "Multiple init calls are safe":
    let success1 = testSetup.node.init(testSetup.config, testSetup.stateMachine)
    check success1 == true

    # Second init should still succeed (idempotent)
    let success2 = testSetup.node.init(testSetup.config, testSetup.stateMachine)
    check success2 == true

  test "Shutdown without init is safe":
    # Should not crash
    testSetup.node.shutdown()
    check testSetup.node.initialized == false

  test "Node role transitions":
    discard testSetup.node.init(testSetup.config, testSetup.stateMachine)

    # Follower -> Candidate
    testSetup.node.becomeCandidate()
    check testSetup.node.nodeState.role == SR_CANDIDATE
    check testSetup.node.nodeState.currentTerm == 1

    # Candidate -> Leader
    testSetup.node.becomeLeader()
    check testSetup.node.nodeState.role == SR_LEADER
    check testSetup.node.isLeader == true

    # Leader -> Follower (step down)
    testSetup.node.stepDown(2)
    check testSetup.node.nodeState.role == SR_FOLLOWER
    check testSetup.node.nodeState.currentTerm == 2
    check testSetup.node.isLeader == false

  test "Node commit updates state machine":
    discard testSetup.node.init(testSetup.config, testSetup.stateMachine)
    testSetup.node.becomeCandidate()
    testSetup.node.becomeLeader()

    let idx = testSetup.node.commit("test data")
    check idx > 0

    # Verify log entry was appended
    let lastEntry = testSetup.node.wsLogStore.getLastEntry()
    check lastEntry.isSome
    check lastEntry.get.data == "test data"

  test "Node handles multiple commits":
    discard testSetup.node.init(testSetup.config, testSetup.stateMachine)
    testSetup.node.becomeCandidate()
    testSetup.node.becomeLeader()

    var indices: seq[int64] = @[]
    for i in 1..10:
      indices.add(testSetup.node.commit("commit" & $i))

    # Verify all indices are sequential
    for i in 1..<indices.len:
      check indices[i] > indices[i-1]

    # Verify last entry
    let lastEntry = testSetup.node.wsLogStore.getLastEntry()
    check lastEntry.isSome
    check lastEntry.get.data == "commit10"

  test "Node state after crash simulation":
    # Simulate crash: write data, shutdown, and recover
    var setup = setupLifecycleTest("tmp/raft_crash_sim/")
    discard setup.node.init(setup.config, setup.stateMachine)
    setup.node.becomeCandidate()
    setup.node.becomeLeader()

    let idx = setup.node.commit("crash-data")
    check idx == 1
    setup.node.shutdown()

    # Recover in a new node instance
    var stateMachine2 = TestStateMachine(
      commits: @[],
      rollbacks: @[],
      lastAppliedIndex: 0
    )
    var node2 = RaftNodeImpl(
      serverId: setup.config.serverId,
      endpoint: setup.config.endpoint,
      config: setup.config,
      nodeState: RaftNodeState(
        role: SR_FOLLOWER,
        currentTerm: 0,
        votedFor: -1,
        leaderId: -1,
        commitIndex: 0,
        lastApplied: 0
      ),
      logStore: nil,
      stateMachine: stateMachine2,
      initialized: false,
      isLeader: false,
      leaderId: -1,
      commitIndex: 0,
      lastApplied: 0
    )

    let success = node2.init(setup.config, stateMachine2)
    check success == true

    # Verify crash data was recovered
    let entry = node2.wsLogStore.getEntry(1)
    check entry.isSome
    check entry.get.data == "crash-data"

    node2.shutdown()
    removeDir("tmp/raft_crash_sim/")

  test "Node state after crash simulation (data persistence check)":
    # Alternative test that verifies data is written to disk
    discard testSetup.node.init(testSetup.config, testSetup.stateMachine)
    testSetup.node.becomeCandidate()
    testSetup.node.becomeLeader()

    let idx = testSetup.node.commit("pre-crash")
    check idx > 0

    # Verify data was written
    let entry = testSetup.node.wsLogStore.getEntry(idx)
    check entry.isSome
    check entry.get.data == "pre-crash"

    testSetup.node.shutdown()


suite "Crash Recovery Tests":
  test "Recovery with empty log":
    var setup = setupLifecycleTest("tmp/raft_recovery_empty/")

    # Initialize and immediately shutdown
    discard setup.node.init(setup.config, setup.stateMachine)
    setup.node.shutdown()

    # Reopen - should work with empty log
    var stateMachine2 = TestStateMachine(commits: @[], rollbacks: @[],
        lastAppliedIndex: 0)
    var node2 = RaftNodeImpl(
      serverId: setup.config.serverId,
      endpoint: setup.config.endpoint,
      config: setup.config,
      nodeState: RaftNodeState(role: SR_FOLLOWER, currentTerm: 0, votedFor: -1,
          leaderId: -1, commitIndex: 0, lastApplied: 0),
      logStore: nil, stateMachine: stateMachine2, initialized: false,
          isLeader: false, leaderId: -1, commitIndex: 0, lastApplied: 0
    )

    let success = node2.init(setup.config, stateMachine2)
    check success == true
    check node2.initialized == true
    check node2.wsLogStore.nextIndex == 1

    node2.shutdown()
    removeDir("tmp/raft_recovery_empty/")

  test "Recovery with multiple log entries":
    var setup = setupLifecycleTest("tmp/raft_recovery_multi/")

    # Initialize, become leader, and add entries
    discard setup.node.init(setup.config, setup.stateMachine)
    setup.node.becomeCandidate()
    setup.node.becomeLeader()

    let idx1 = setup.node.commit("entry1")
    let idx2 = setup.node.commit("entry2")
    let idx3 = setup.node.commit("entry3")
    check idx1 == 1
    check idx2 == 2
    check idx3 == 3

    setup.node.shutdown()

    # Reopen and verify entries
    var stateMachine2 = TestStateMachine(commits: @[], rollbacks: @[],
        lastAppliedIndex: 0)
    var node2 = RaftNodeImpl(
      serverId: setup.config.serverId,
      endpoint: setup.config.endpoint,
      config: setup.config,
      nodeState: RaftNodeState(role: SR_FOLLOWER, currentTerm: 0, votedFor: -1,
          leaderId: -1, commitIndex: 0, lastApplied: 0),
      logStore: nil, stateMachine: stateMachine2, initialized: false,
          isLeader: false, leaderId: -1, commitIndex: 0, lastApplied: 0
    )

    let success = node2.init(setup.config, stateMachine2)
    check success == true
    check node2.wsLogStore.nextIndex == 4

    # Verify entries were recovered
    let entry1 = node2.wsLogStore.getEntry(1)
    let entry2 = node2.wsLogStore.getEntry(2)
    let entry3 = node2.wsLogStore.getEntry(3)

    check entry1.isSome
    check entry1.get.data == "entry1"
    check entry2.isSome
    check entry2.get.data == "entry2"
    check entry3.isSome
    check entry3.get.data == "entry3"

    node2.shutdown()
    removeDir("tmp/raft_recovery_multi/")

  test "Recovery and continue operations":
    var setup = setupLifecycleTest("tmp/raft_recovery_continue/")

    # Initialize, become leader, and add entries
    discard setup.node.init(setup.config, setup.stateMachine)
    setup.node.becomeCandidate()
    setup.node.becomeLeader()

    discard setup.node.commit("pre-crash")
    setup.node.shutdown()

    # Reopen and continue operations
    var stateMachine2 = TestStateMachine(commits: @[], rollbacks: @[],
        lastAppliedIndex: 0)
    var node2 = RaftNodeImpl(
      serverId: setup.config.serverId,
      endpoint: setup.config.endpoint,
      config: setup.config,
      nodeState: RaftNodeState(role: SR_FOLLOWER, currentTerm: 0, votedFor: -1,
          leaderId: -1, commitIndex: 0, lastApplied: 0),
      logStore: nil, stateMachine: stateMachine2, initialized: false,
          isLeader: false, leaderId: -1, commitIndex: 0, lastApplied: 0
    )

    let success = node2.init(setup.config, stateMachine2)
    check success == true

    # Become leader again and continue
    node2.becomeCandidate()
    node2.becomeLeader()

    let idx = node2.commit("post-crash")
    check idx == 2 # Should continue from index 2
    
    # Verify both entries exist
    let preEntry = node2.wsLogStore.getEntry(1)
    let postEntry = node2.wsLogStore.getEntry(2)

    check preEntry.isSome
    check preEntry.get.data == "pre-crash"
    check postEntry.isSome
    check postEntry.get.data == "post-crash"

    node2.shutdown()
    removeDir("tmp/raft_recovery_continue/")


suite "KVStateMachine Integration Tests":

  test "KVStateMachine commit tracks last applied index":
    var kvSM = newKVStateMachine()

    # KVStateMachine is a lightweight index tracker — commit always returns "ok"
    let result1 = kvSM.commit(1, "put:key1:value1")
    check result1 == "ok"
    check kvSM.getLastAppliedIndex() == 1

    let result2 = kvSM.commit(5, "put:key2:value2")
    check result2 == "ok"
    check kvSM.getLastAppliedIndex() == 5

  test "KVStateMachine with RaftNode":
    var testSetup = setupLifecycleTest("tmp/raft_kv_test/")

    var kvSM = newKVStateMachine()

    var node = RaftNodeImpl(
      serverId: testSetup.config.serverId,
      endpoint: testSetup.config.endpoint,
      config: testSetup.config,
      nodeState: RaftNodeState(
        role: SR_FOLLOWER,
        currentTerm: 0,
        votedFor: -1,
        leaderId: -1,
        commitIndex: 0,
        lastApplied: 0
      ),
      logStore: nil,
      stateMachine: kvSM,
      initialized: false,
      isLeader: false,
      leaderId: -1,
      commitIndex: 0,
      lastApplied: 0
    )

    discard node.init(testSetup.config, kvSM)
    node.becomeCandidate()
    node.becomeLeader()

    # Put key-value pair
    let idx1 = node.commit("put:testkey:testvalue")
    check idx1 > 0

    node.shutdown()
    removeDir("tmp/raft_kv_test/")

  test "KVStateMachine multiple commits track index correctly":
    var kvSM = newKVStateMachine()

    for i in 1..10:
      let result = kvSM.commit(int64(i), "put:key" & $i & ":value" & $i)
      check result == "ok"

    check kvSM.getLastAppliedIndex() == 10
