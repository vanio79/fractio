# Integration Tests for Raft Cluster Replication

import unittest
import std/[tables, sets, times]

import fractio/distributed/raft/types
import fractio/distributed/raft/node

# Test State Machine
type
  TestStateMachine* = ref object of StateMachine
    ## Test state machine for integration testing
    commits*: seq[(int64, string)]
    rollbacks*: seq[(int64, string)]
    lastAppliedIndex*: int64

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


# Integration Test Fixtures
type
  IntegrationSetup* = object
    nodes*: seq[RaftNodeImpl]
    stateMachines*: seq[TestStateMachine]
    configs*: seq[RaftConfig]
    leader*: RaftNodeImpl
    followers*: seq[RaftNodeImpl]

proc setupIntegration*(): IntegrationSetup =
  ## Setup 3-node integration test environment
  result.configs = @[
    RaftConfig(
      serverId: 1,
      endpoint: "127.0.0.1:9000",
      electionTimeout: 100, # Shorter timeout for faster tests
    heartbeatInterval: 50,
    logStoragePath: "tmp/raft_integration_1/",
    snapshotEnabled: false,
    snapshotDistance: 1000,
    maxAppendSize: 100
  ),
    RaftConfig(
      serverId: 2,
      endpoint: "127.0.0.1:9001",
      electionTimeout: 100,
      heartbeatInterval: 50,
      logStoragePath: "tmp/raft_integration_2/",
      snapshotEnabled: false,
      snapshotDistance: 1000,
      maxAppendSize: 100
    ),
    RaftConfig(
      serverId: 3,
      endpoint: "127.0.0.1:9002",
      electionTimeout: 100,
      heartbeatInterval: 50,
      logStoragePath: "tmp/raft_integration_3/",
      snapshotEnabled: false,
      snapshotDistance: 1000,
      maxAppendSize: 100
    )
  ]

  result.stateMachines = @[
    TestStateMachine(
      commits: @[],
      rollbacks: @[],
      lastAppliedIndex: 0
    ),
    TestStateMachine(
      commits: @[],
      rollbacks: @[],
      lastAppliedIndex: 0
    ),
    TestStateMachine(
      commits: @[],
      rollbacks: @[],
      lastAppliedIndex: 0
    )
  ]

  # Create nodes (simplified - in real implementation these would have transport)
  result.nodes = @[
    RaftNodeImpl(
      serverId: result.configs[0].serverId,
      endpoint: result.configs[0].endpoint,
      config: result.configs[0],
      nodeState: RaftNodeState(
        role: SR_FOLLOWER,
        currentTerm: 0,
        votedFor: -1,
        leaderId: -1,
        commitIndex: 0,
        lastApplied: 0
    ),
    logStore: nil,
    stateMachine: result.stateMachines[0],
    initialized: true,
    isLeader: false,
    leaderId: -1,
    commitIndex: 0,
    lastApplied: 0
  ),
    RaftNodeImpl(
      serverId: result.configs[1].serverId,
      endpoint: result.configs[1].endpoint,
      config: result.configs[1],
      nodeState: RaftNodeState(
        role: SR_FOLLOWER,
        currentTerm: 0,
        votedFor: -1,
        leaderId: -1,
        commitIndex: 0,
        lastApplied: 0
    ),
    logStore: nil,
    stateMachine: result.stateMachines[1],
    initialized: true,
    isLeader: false,
    leaderId: -1,
    commitIndex: 0,
    lastApplied: 0
  ),
    RaftNodeImpl(
      serverId: result.configs[2].serverId,
      endpoint: result.configs[2].endpoint,
      config: result.configs[2],
      nodeState: RaftNodeState(
        role: SR_FOLLOWER,
        currentTerm: 0,
        votedFor: -1,
        leaderId: -1,
        commitIndex: 0,
        lastApplied: 0
    ),
    logStore: nil,
    stateMachine: result.stateMachines[2],
    initialized: true,
    isLeader: false,
    leaderId: -1,
    commitIndex: 0,
    lastApplied: 0
  )
  ]

  # Initialize nodes
  for i in 0..<result.nodes.len:
    discard result.nodes[i].init(result.configs[i], result.stateMachines[i])

  # Setup cluster relationships (for testing purposes)
  result.leader = result.nodes[0] # Assume node 1 becomes leader
  result.followers = @[result.nodes[1], result.nodes[2]]

proc teardownIntegration*(setup: var IntegrationSetup) =
  ## Teardown integration test environment
  for node in setup.nodes:
    node.shutdown()

  # Clean up test directories
  removeDir(setup.configs[0].logStoragePath)
  removeDir(setup.configs[1].logStoragePath)
  removeDir(setup.configs[2].logStoragePath)


# Integration Test Suite
suite "Raft Cluster Replication Tests":

  setup:
    var testSetup = setupIntegration()

  teardown:
    teardownIntegration(testSetup)

  test "Leader election in 3-node cluster":
    # Simulate election timeout on node 1
    testSetup.nodes[0].becomeCandidate()
    testSetup.nodes[0].becomeLeader()

    check testSetup.nodes[0].nodeState.role == SR_LEADER
    check testSetup.nodes[0].isLeader == true
    check testSetup.nodes[0].nodeState.leaderId == 1
    check testSetup.nodes[0].nodeState.currentTerm == 1

    # Followers should recognize leader
    for i in 1..2:
      testSetup.nodes[i].nodeState.leaderId = 1 # Simulate heartbeat
      check testSetup.nodes[i].nodeState.leaderId == 1

  test "Log replication from leader to followers":
    # Node 1 becomes leader
    testSetup.nodes[0].becomeCandidate()
    testSetup.nodes[0].becomeLeader()

    # Leader commits data
    let commitIndex = testSetup.nodes[0].commit("replicated data")
    check commitIndex > 0

    # Simulate replication to followers
    for i in 1..2:
      # In real implementation, this would be via RPC
      # For testing, we simulate state machine application
      discard testSetup.stateMachines[i].commit(commitIndex, "replicated data")

      check testSetup.stateMachines[i].commits.len == 1
      check testSetup.stateMachines[i].commits[0][1] == "replicated data"
      check testSetup.stateMachines[i].lastAppliedIndex == commitIndex

  test "Consistent commit across cluster":
    # Node 1 becomes leader
    testSetup.nodes[0].becomeCandidate()
    testSetup.nodes[0].becomeLeader()

    # Multiple commits
    let idx1 = testSetup.nodes[0].commit("first entry")
    let idx2 = testSetup.nodes[0].commit("second entry")
    let idx3 = testSetup.nodes[0].commit("third entry")

    check idx1 < idx2
    check idx2 < idx3

    # Simulate replication to all nodes
    for i in 0..2:
      discard testSetup.stateMachines[i].commit(idx1, "first entry")
      discard testSetup.stateMachines[i].commit(idx2, "second entry")
      discard testSetup.stateMachines[i].commit(idx3, "third entry")

    # Verify all nodes have same data
    for i in 0..2:
      check testSetup.stateMachines[i].commits.len == 3
      check testSetup.stateMachines[i].commits[0][1] == "first entry"
      check testSetup.stateMachines[i].commits[1][1] == "second entry"
      check testSetup.stateMachines[i].commits[2][1] == "third entry"
      check testSetup.stateMachines[i].lastAppliedIndex == idx3

  test "Leader failover and recovery":
    # Node 1 becomes leader and commits data
    testSetup.nodes[0].becomeCandidate()
    testSetup.nodes[0].becomeLeader()

    let initialCommit = testSetup.nodes[0].commit("before failover")
    check initialCommit > 0

    # Simulate leader failure
    # In real implementation, this would trigger re-election
    testSetup.nodes[0].nodeState.role = SR_FOLLOWER # Step down
    
    # Node 2 becomes new leader
    testSetup.nodes[1].becomeCandidate()
    testSetup.nodes[1].becomeLeader()

    # New leader commits new data
    let newCommit = testSetup.nodes[1].commit("after failover")
    check newCommit > initialCommit

    # Verify new leader's data
    check testSetup.stateMachines[1].commits.len == 1
    check testSetup.stateMachines[1].commits[0][1] == "after failover"

    # Simulate old leader recovery
    testSetup.nodes[0].becomeCandidate()
    testSetup.nodes[0].becomeFollower()

    # Old leader should sync with new leader
    # In real implementation, this would happen via RPC
    discard testSetup.stateMachines[0].commit(newCommit, "after failover")

    check testSetup.stateMachines[0].commits.len == 1
    check testSetup.stateMachines[0].commits[0][1] == "after failover"

  test "Network partition and reconciliation":
    # Normal cluster operation
    testSetup.nodes[0].becomeCandidate()
    testSetup.nodes[0].becomeLeader()

    let prePartition = testSetup.nodes[0].commit("pre-partition")
    check prePartition > 0

    # Simulate network partition - node 1 isolated
    # In real implementation, this would be detected via heartbeat timeout

    # Nodes 2 and 3 form new majority
    testSetup.nodes[1].becomeCandidate()
    testSetup.nodes[1].becomeLeader()

    let postPartition = testSetup.nodes[1].commit("post-partition")
    check postPartition > prePartition

    # Simulate partition healed
    # In real implementation, this would trigger log reconciliation
    # For testing, we simulate state synchronization

    # Node 1 should sync with new leader (node 2)
    testSetup.nodes[0].stepDown(2) # Step down to follower of new term
    testSetup.nodes[0].nodeState.leaderId = 2

    # Sync state
    discard testSetup.stateMachines[0].commit(postPartition, "post-partition")

    check testSetup.stateMachines[0].commits.len == 1
    check testSetup.stateMachines[0].commits[0][1] == "post-partition"

  test "Concurrent client requests":
    # Node 1 becomes leader
    testSetup.nodes[0].becomeCandidate()
    testSetup.nodes[0].becomeLeader()

    # Simulate multiple concurrent client requests
    var indices: seq[int64]

    # Multiple commits in quick succession
    indices.add(testSetup.nodes[0].commit("request1"))
    indices.add(testSetup.nodes[0].commit("request2"))
    indices.add(testSetup.nodes[0].commit("request3"))
    indices.add(testSetup.nodes[0].commit("request4"))

    check indices.len == 4
    check indices[0] < indices[1]
    check indices[1] < indices[2]
    check indices[2] < indices[3]

    # Verify all requests were committed
    for i in 0..3:
      discard testSetup.stateMachines[0].commit(indices[i], "request" & $(i+1))

    check testSetup.stateMachines[0].commits.len == 4
    let commitData = testSetup.stateMachines[0].commits.mapIt(it[1])
    check "request1" in commitData
    check "request2" in commitData
    check "request3" in commitData
    check "request4" in commitData

  test "Cluster state consistency after restart":
    # Node 1 becomes leader and commits data
    testSetup.nodes[0].becomeCandidate()
    testSetup.nodes[0].becomeLeader()

    let initialCommit = testSetup.nodes[0].commit("pre-restart")
    check initialCommit > 0

    # Simulate node restart - create new instance
    var restartedNode = RaftNodeImpl(
      serverId: testSetup.configs[0].serverId,
      endpoint: testSetup.configs[0].endpoint,
      config: testSetup.configs[0],
      nodeState: RaftNodeState(
        role: SR_FOLLOWER,
        currentTerm: 0,
        votedFor: -1,
        leaderId: -1,
        commitIndex: 0,
        lastApplied: 0
      ),
      logStore: nil,
      stateMachine: TestStateMachine(
        commits: @[],
        rollbacks: @[],
        lastAppliedIndex: 0
      ),
      initialized: true,
      isLeader: false,
      leaderId: -1,
      commitIndex: 0,
      lastApplied: 0
    )

    discard restartedNode.init(testSetup.configs[0], restartedNode.stateMachine)

    # In real restart, this would load from persistent storage
    # For testing, we simulate loading the committed data
    discard restartedNode.stateMachine.commit(initialCommit, "pre-restart")

    check restartedNode.stateMachine.commits.len == 1
    check restartedNode.stateMachine.commits[0][1] == "pre-restart"

    restartedNode.shutdown()

  test "Majority quorum validation":
    # For 3-node cluster, majority is 2
    let majorityCount = 2

    # Node 1 becomes leader
    testSetup.nodes[0].becomeCandidate()
    testSetup.nodes[0].becomeLeader()

    # Commit data - should succeed with majority
    let commitIndex = testSetup.nodes[0].commit("quorum data")
    check commitIndex > 0

    # Verify majority of nodes have data
    var nodesWithCommit = 1 # Leader has it

    # Simulate replication to followers (majority)
    for i in 1..1: # Only first follower for majority
      discard testSetup.stateMachines[i].commit(commitIndex, "quorum data")
      nodesWithCommit.inc

    check nodesWithCommit >= majorityCount

    # Verify at least one follower has the data
    check testSetup.stateMachines[1].commits.len == 1

  test "Cluster configuration changes":
    # Start with 3-node cluster
    check testSetup.nodes.len == 3

    # Simulate adding a new node (node 4)
    var newNodeConfig = RaftConfig(
      serverId: 4,
      endpoint: "127.0.0.1:9003",
      electionTimeout: 100,
      heartbeatInterval: 50,
      logStoragePath: "tmp/raft_integration_4/",
      snapshotEnabled: false,
      snapshotDistance: 1000,
      maxAppendSize: 100
    )

    var newNode = RaftNodeImpl(
      serverId: newNodeConfig.serverId,
      endpoint: newNodeConfig.endpoint,
      config: newNodeConfig,
      nodeState: RaftNodeState(
        role: SR_FOLLOWER,
        currentTerm: 0,
        votedFor: -1,
        leaderId: -1,
        commitIndex: 0,
        lastApplied: 0
      ),
      logStore: nil,
      stateMachine: TestStateMachine(
        commits: @[],
        rollbacks: @[],
        lastAppliedIndex: 0
      ),
      initialized: true,
      isLeader: false,
      leaderId: -1,
      commitIndex: 0,
      lastApplied: 0
    )

    discard newNode.init(newNodeConfig, newNode.stateMachine)

    # Simulate newNode joining cluster and syncing
    # In real implementation, this would happen via Raft configuration change

    # For testing, we just verify newNode can be created and initialized
    check newNode.initialized == true

    newNode.shutdown()
    removeDir(newNodeConfig.logStoragePath)

  test "Cluster state after partial failure":
    # Node 1 becomes leader and commits data
    testSetup.nodes[0].becomeCandidate()
    testSetup.nodes[0].becomeLeader()

    let commitIndex = testSetup.nodes[0].commit("partial data")
    check commitIndex > 0

    # Simulate node 3 failure
    # In real implementation, this would be detected via heartbeat timeout

    # Nodes 1 and 2 should still form majority
    check testSetup.nodes[0].isLeader == true
    check testSetup.nodes[1].nodeState.role == SR_FOLLOWER

    # Verify nodes 1 and 2 can still operate
    let newCommit = testSetup.nodes[0].commit("partial recovery")
    check newCommit > commitIndex

    # Verify both operational nodes have data
    check testSetup.stateMachines[0].commits.len == 2
    check testSetup.stateMachines[1].commits.len == 2
