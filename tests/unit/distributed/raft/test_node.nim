# Unit Tests for RaftNode Implementation

import unittest
import std/[tables, sets, times]

import fractio/distributed/raft/node
import fractio/distributed/raft/types
import fractio/storage/wisckey_backend

# Test State Machine
type
  TestStateMachine* = ref object of StateMachine
    ## Test state machine for unit testing
    appliedLogs*: seq[(int64, string)]
    lastAppliedIndex*: int64

  TestStateMachineError* = object of CatchableError

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
    node*: RaftNodeImpl
    stateMachine*: TestStateMachine
    config*: RaftConfig
    logStore*: WiscKeyLogStore

proc setupTest*(path: string = "tmp/raft_test/"): TestSetup =
  ## Setup test environment
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
    appliedLogs: @[],
    lastAppliedIndex: 0
  )

  result.logStore = newWiscKeyLogStore(path)

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
    logStore: result.logStore,
    stateMachine: result.stateMachine,
    initialized: true,
    isLeader: false,
    leaderId: -1,
    commitIndex: 0,
    lastApplied: 0
  )

  result.node.init(result.config, result.stateMachine)

proc teardownTest*(setup: var TestSetup) =
  ## Teardown test environment
  setup.node.shutdown()
  setup.logStore.close()
  # Clean up test directory
  removeDir(setup.config.logStoragePath)


# Test Suite
suite "RaftNode Implementation Tests":

  setup:
    var testSetup = setupTest()

  teardown:
    teardownTest(testSetup)

  test "Initialization and basic state":
    check testSetup.node.initialized == true
    check testSetup.node.serverId == 1
    check testSetup.node.endpoint == "127.0.0.1:9000"
    check testSetup.node.nodeState.role == SR_FOLLOWER
    check testSetup.node.nodeState.currentTerm == 0
    check testSetup.node.nodeState.votedFor == -1
    check testSetup.node.nodeState.leaderId == -1
    check testSetup.node.nodeState.commitIndex == 0
    check testSetup.node.nodeState.lastApplied == 0

  test "Role transitions - follower to candidate":
    testSetup.node.becomeCandidate()
    check testSetup.node.nodeState.role == SR_CANDIDATE
    check testSetup.node.nodeState.currentTerm == 1
    check testSetup.node.nodeState.votedFor == 1
    check testSetup.node.nodeState.leaderId == -1

  test "Role transitions - candidate to leader":
    testSetup.node.becomeCandidate()
    testSetup.node.becomeLeader()
    check testSetup.node.nodeState.role == SR_LEADER
    check testSetup.node.isLeader == true
    check testSetup.node.nodeState.leaderId == 1

  test "Role transitions - leader to follower":
    testSetup.node.becomeCandidate()
    testSetup.node.becomeLeader()
    testSetup.node.stepDown(2)
    check testSetup.node.nodeState.role == SR_FOLLOWER
    check testSetup.node.isLeader == false
    check testSetup.node.nodeState.currentTerm == 2
    check testSetup.node.nodeState.votedFor == -1

  test "Election timeout handling":
    testSetup.node.becomeCandidate()
    testSetup.node.startElection()
    # In real implementation, this would trigger timeout logic
    check testSetup.node.nodeState.role == SR_CANDIDATE
    check testSetup.node.nodeState.currentTerm == 1

  test "Log append and retrieval":
    let entry = LogEntry(
      term: 1,
      entryType: LET_NORMAL,
      data: "test data"
    )

    let index = testSetup.node.logStore.appendEntry(entry)
    check index == 1

    let retrieved = testSetup.node.logStore.getEntry(1)
    check retrieved.isSome
    check retrieved.get.term == 1
    check retrieved.get.data == "test data"
    check retrieved.get.entryType == LET_NORMAL

  test "Get last log entry":
    let entry1 = LogEntry(term: 1, entryType: LET_NORMAL, data: "first")
    let entry2 = LogEntry(term: 1, entryType: LET_NORMAL, data: "second")

    discard testSetup.node.logStore.appendEntry(entry1)
    discard testSetup.node.logStore.appendEntry(entry2)

    let last = testSetup.node.logStore.getLastEntry()
    check last.isSome
    check last.get.data == "second"

  test "Log entry range retrieval":
    let entries = [
      LogEntry(term: 1, entryType: LET_NORMAL, data: "1"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "2"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "3"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "4")
    ]

    for entry in entries:
      discard testSetup.node.logStore.appendEntry(entry)

    let range = testSetup.node.logStore.getEntries(2, 4)
    check range.len == 3
    check range[0].data == "2"
    check range[1].data == "3"
    check range[2].data == "4"

  test "RPC handling as follower":
    let rpc = RaftRPC(
      rpcType: RPC_APPEND_ENTRIES,
      term: 1,
      leaderId: 2,
      prevLogIndex: 0,
      prevLogTerm: 0,
      entries: @[
        LogEntry(term: 1, entryType: LET_NORMAL, data: "entry1")
      ],
      leaderCommit: 1,
      success: false
    )

    let response = testSetup.node.handleRPCAsFollower(testSetup.node, rpc)
    check response.success == true
    check testSetup.node.nodeState.currentTerm == 1
    check testSetup.node.nodeState.leaderId == 2
    check testSetup.node.nodeState.commitIndex == 1

  test "RPC handling as leader":
    testSetup.node.becomeCandidate()
    testSetup.node.becomeLeader()

    let rpc = RaftRPC(
      rpcType: RPC_CLIENT_REQUEST,
      term: 1,
      leaderId: 1,
      data: "client data"
    )

    let response = testSetup.node.handleRPCAsLeader(testSetup.node, rpc)
    check response.success == true
    check testSetup.node.nodeState.commitIndex > 0

  test "RPC handling as candidate":
    testSetup.node.becomeCandidate()

    let appendRpc = RaftRPC(
      rpcType: RPC_APPEND_ENTRIES,
      term: 2,
      leaderId: 3,
      entries: @[]
    )

    let voteRpc = RaftRPC(
      rpcType: RPC_REQUEST_VOTE,
      term: 1,
      leaderId: 4
    )

    # Should step down to follower
    let response1 = testSetup.node.handleRPCAsCandidate(testSetup.node, appendRpc)
    check testSetup.node.nodeState.role == SR_FOLLOWER
    check testSetup.node.nodeState.currentTerm == 2

    # Should deny vote
    let response2 = testSetup.node.handleRPCAsCandidate(testSetup.node, voteRpc)
    check response2.success == false

  test "State machine integration":
    let entry = LogEntry(
      term: 1,
      entryType: LET_NORMAL,
      data: "commit data"
    )

    let idx = testSetup.node.logStore.appendEntry(entry)
    testSetup.node.stateMachine.commit(idx, entry.data)

    check testSetup.stateMachine.appliedLogs.len == 1
    check testSetup.stateMachine.appliedLogs[0][0] == idx
    check testSetup.stateMachine.appliedLogs[0][1] == "commit data"
    check testSetup.stateMachine.lastAppliedIndex == idx

  test "Commit operation":
    testSetup.node.becomeCandidate()
    testSetup.node.becomeLeader()

    let result = testSetup.node.commit("test commit")
    check result > 0

    # Verify entry was added
    let lastEntry = testSetup.node.logStore.getLastEntry()
    check lastEntry.isSome
    check lastEntry.get.data == "test commit"

  test "Append entries operation":
    let entries = @[
      LogEntry(term: 1, entryType: LET_NORMAL, data: "e1"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "e2")
    ]

    let lastIndex = testSetup.node.appendEntries(entries)
    check lastIndex == 2

    # Verify both entries exist
    let e1 = testSetup.node.logStore.getEntry(1)
    let e2 = testSetup.node.logStore.getEntry(2)
    check e1.isSome and e2.isSome
    check e1.get.data == "e1"
    check e2.get.data == "e2"

  test "Invalid state machine commit":
    # Create node without state machine
    var node = RaftNodeImpl(
      serverId: 1,
      endpoint: "127.0.0.1:9000",
      config: testSetup.config,
      nodeState: RaftNodeState(
        role: SR_LEADER,
        currentTerm: 1,
        votedFor: -1,
        leaderId: 1,
        commitIndex: 0,
        lastApplied: 0
      ),
      logStore: testSetup.logStore,
      stateMachine: nil,
      initialized: true,
      isLeader: true,
      leaderId: 1,
      commitIndex: 0,
      lastApplied: 0
    )

    expect RaftError:
      discard node.commit("should fail")
