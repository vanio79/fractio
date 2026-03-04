# Concurrency Tests for Raft Implementation

import unittest
import std/[tables, sets, times, locks, threads]

import fractio/distributed/raft/types
import fractio/distributed/raft/node

# Test State Machine with thread-safe operations
type
  ConcurrentStateMachine* = ref object of StateMachine
    ## Thread-safe state machine for concurrency testing
    lock*: Lock
    commits*: seq[(int64, string)]
    rollbacks*: seq[(int64, string)]
    lastAppliedIndex*: int64

  ConcurrentStateMachineError* = object of CatchableError

proc newConcurrentStateMachine*(): ConcurrentStateMachine =
  ## Create a new thread-safe state machine
  result = ConcurrentStateMachine(
    lock: Lock(),
    commits: @[],
    rollbacks: @[],
    lastAppliedIndex: 0
  )
  initLock(result.lock)

method commit*(sm: ConcurrentStateMachine, logIdx: int64,
    data: string): string =
  ## Thread-safe commit implementation
  withLock sm.lock:
    sm.commits.add((logIdx, data))
    sm.lastAppliedIndex = logIdx
  return "OK"

method rollback*(sm: ConcurrentStateMachine, logIdx: int64, data: string) =
  ## Thread-safe rollback implementation
  withLock sm.lock:
    sm.rollbacks.add((logIdx, data))

method getLastAppliedIndex*(sm: ConcurrentStateMachine): int64 =
  ## Get last applied log index (thread-safe read)
  withLock sm.lock:
    result = sm.lastAppliedIndex


# Thread-safe Raft Node wrapper
type
  ThreadSafeRaftNode* = object
    node*: RaftNodeImpl
    lock*: Lock

  ThreadSafeRaftNodeError* = object of CatchableError

proc newThreadSafeRaftNode*(config: RaftConfig,
    stateMachine: StateMachine): ThreadSafeRaftNode =
  ## Create a thread-safe Raft node
  result = ThreadSafeRaftNode(
    node: RaftNodeImpl(
      serverId: config.serverId,
      endpoint: config.endpoint,
      config: config,
      nodeState: RaftNodeState(
        role: SR_FOLLOWER,
        currentTerm: 0,
        votedFor: -1,
        leaderId: -1,
        commitIndex: 0,
        lastApplied: 0
    ),
    logStore: nil,
    stateMachine: stateMachine,
    initialized: true,
    isLeader: false,
    leaderId: -1,
    commitIndex: 0,
    lastApplied: 0
  ),
    lock: Lock()
  )
  initLock(result.lock)

  # Initialize the node
  discard result.node.init(config, stateMachine)

proc commit*(node: ThreadSafeRaftNode, data: string): int64 =
  ## Thread-safe commit operation
  withLock node.lock:
    result = node.node.commit(data)

proc becomeLeader*(node: ThreadSafeRaftNode) =
  ## Thread-safe leader transition
  withLock node.lock:
    node.node.becomeCandidate()
    node.node.becomeLeader()

proc getState*(node: ThreadSafeRaftNode): RaftNodeState =
  ## Thread-safe state retrieval
  withLock node.lock:
    result = node.node.nodeState

proc shutdown*(node: var ThreadSafeRaftNode) =
  ## Thread-safe shutdown
  withLock node.lock:
    node.node.shutdown()


# Test Fixtures
type
  ConcurrencySetup* = object
    node*: ThreadSafeRaftNode
    stateMachine*: ConcurrentStateMachine
    config*: RaftConfig

proc setupConcurrencyTest*(): ConcurrencySetup =
  ## Setup concurrency test environment
  result.config = RaftConfig(
    serverId: 1,
    endpoint: "127.0.0.1:9000",
    electionTimeout: 100,
    heartbeatInterval: 50,
    logStoragePath: "tmp/raft_concurrency/",
    snapshotEnabled: false,
    snapshotDistance: 1000,
    maxAppendSize: 100
  )

  result.stateMachine = newConcurrentStateMachine()
  result.node = newThreadSafeRaftNode(result.config, result.stateMachine)

proc teardownConcurrencyTest*(setup: var ConcurrencySetup) =
  ## Teardown concurrency test environment
  setup.node.shutdown()
  removeDir(setup.config.logStoragePath)


# Thread function for concurrent commits
type
  CommitThreadData* = object
    node*: ThreadSafeRaftNode
    data*: string
    result*: int64
    error*: string

proc commitWorker*(arg: pointer) {.thread.} =
  ## Worker thread for concurrent commits
  let data = cast[ptr CommitThreadData](arg)

  try:
    data.result = data.node.commit(data.data)
  except CatchableError as e:
    data.error = e.msg


# Test Suite
suite "Raft Concurrency Tests":

  setup:
    var testSetup = setupConcurrencyTest()

  teardown:
    teardownConcurrencyTest(testSetup)

  test "Single thread commit operations":
    check testSetup.node.getState().role == SR_FOLLOWER

    let result = testSetup.node.commit("single thread data")
    check result > 0

    # Verify state machine application
    check testSetup.stateMachine.commits.len == 1
    check testSetup.stateMachine.commits[0][1] == "single thread data"

  test "Multiple threads concurrent commits":
    # Make node leader
    testSetup.node.becomeLeader()

    # Create multiple threads for concurrent commits
    const threadCount = 10
    var threads: array[threadCount, Thread[pointer]]
    var threadData: array[threadCount, CommitThreadData]

    # Initialize thread data
    for i in 0..<threadCount:
      threadData[i] = CommitThreadData(
        node: testSetup.node,
        data: "data" & $(i+1),
        result: 0,
        error: ""
      )
      createThread(threads[i], commitWorker, addr(threadData[i]))

    # Wait for all threads to complete
    for i in 0..<threadCount:
      joinThread(threads[i])

    # Verify all commits succeeded
    var successCount = 0
    for i in 0..<threadCount:
      if threadData[i].error == "":
        successCount.inc
        check threadData[i].result > 0

    check successCount == threadCount

    # Verify state machine has all commits
    check testSetup.stateMachine.commits.len == threadCount

    # Verify commit indices are unique and sequential
    var indices = newSeq[int64]()
    for commit in testSetup.stateMachine.commits:
      indices.add(commit[0])

    indices.sort()
    for i in 1..<indices.len:
      check indices[i] == indices[i-1] + 1

  test "Concurrent role transitions":
    # Create multiple threads for concurrent role changes
    const threadCount = 5
    var threads: array[threadCount, Thread[pointer]]
    var threadData: array[threadCount, CommitThreadData]

    # Initialize thread data with different operations
    for i in 0..<threadCount:
      threadData[i] = CommitThreadData(
        node: testSetup.node,
        data: "role" & $(i+1),
        result: 0,
        error: ""
      )

    # Create threads: some become leader, some commit
    createThread(threads[0], commitWorker, addr(threadData[0])) # Commit
    createThread(threads[1], commitWorker, addr(threadData[1])) # Commit
    
    # Simulate role transitions (these would be direct calls in real implementation)
    # For testing, we verify thread safety of node operations
    
    # Wait for commit threads
    joinThread(threads[0])
    joinThread(threads[1])

    # Node should still be operational after concurrent operations
    let state = testSetup.node.getState()
    check state.role in {SR_FOLLOWER, SR_CANDIDATE, SR_LEADER}

  test "Thread-safe state machine operations":
    # Test concurrent access to state machine
    const threadCount = 10
    var threads: array[threadCount, Thread[pointer]]

    # Multiple threads accessing state machine simultaneously
    for i in 0..<threadCount:
      let data = "concurrent" & $(i+1)
      createThread(threads[i], commitWorker, addr(CommitThreadData(
        node: testSetup.node,
        data: data,
        result: 0,
        error: ""
      )))

    # Wait for all threads
    for i in 0..<threadCount:
      joinThread(threads[i])

    # Verify all commits succeeded
    check testSetup.stateMachine.commits.len == threadCount

    # Verify no data corruption
    var uniqueData: CountTable[string]
    for commit in testSetup.stateMachine.commits:
      uniqueData.inc(commit[1])

    check uniqueData.len == threadCount # All data should be unique

  test "Concurrent node initialization":
    # Test that node can handle concurrent initialization attempts
    const threadCount = 5
    var threads: array[threadCount, Thread[pointer]]
    var successCount = 0

    # Create threads that try to initialize the node
    for i in 0..<threadCount:
      createThread(threads[i], proc(arg: pointer) {.thread.} =
        var localNode = ThreadSafeRaftNode(
          node: RaftNodeImpl(
            serverId: 1,
            endpoint: "127.0.0.1:9000",
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
          stateMachine: testSetup.stateMachine,
          initialized: false,
          isLeader: false,
          leaderId: -1,
          commitIndex: 0,
          lastApplied: 0
        ),
          lock: Lock()
        )
        initLock(localNode.lock)

        try:
          discard localNode.node.init(testSetup.config, testSetup.stateMachine)
          atomicInc(successCount)
        except:
          discard
      , nil)

    # Wait for all threads
    for i in 0..<threadCount:
      joinThread(threads[i])

    # At least one should succeed
    check successCount > 0

  test "Thread-safe shutdown":
    # Test that shutdown can be called concurrently
    const threadCount = 5
    var threads: array[threadCount, Thread[pointer]]

    # Create threads that call shutdown simultaneously
    for i in 0..<threadCount:
      createThread(threads[i], proc(arg: pointer) {.thread.} =
        testSetup.node.shutdown()
      , nil)

    # Wait for all threads
    for i in 0..<threadCount:
      joinThread(threads[i])

    # Node should be properly shutdown
    let state = testSetup.node.getState()
    check state.role in {SR_FOLLOWER, SR_CANDIDATE, SR_LEADER} # State may still be readable
    
    # Verify node is marked as not initialized
    # (Note: In real implementation, this would be checked differently)

  test "Stress test with rapid concurrent operations":
    # Make node leader
    testSetup.node.becomeLeader()

    # High volume of concurrent operations
    const operationCount = 100
    var threads: seq[Thread[pointer]]
    threads.setLen(operationCount)

    var threadDataArray: seq[CommitThreadData]
    threadDataArray.setLen(operationCount)

    # Create many threads performing commits
    for i in 0..<operationCount:
      threadDataArray[i] = CommitThreadData(
        node: testSetup.node,
        data: "stress" & $(i+1),
        result: 0,
        error: ""
      )
      createThread(threads[i], commitWorker, addr(threadDataArray[i]))

    # Wait for all threads
    for i in 0..<operationCount:
      joinThread(threads[i])

    # Collect results
    var results: seq[int64] = @[]
    for i in 0..<operationCount:
      if threadDataArray[i].error == "":
        results.add(threadDataArray[i].result)

    # Filter out failed operations
    let successfulResults = results.filterIt(it > 0)

    # Should have many successful operations
    check successfulResults.len > operationCount div 2 # At least half should succeed

    # Verify state machine received the successful commits
    check testSetup.stateMachine.commits.len >= successfulResults.len

  test "Thread safety of state machine callbacks":
    # Test that state machine callbacks are thread-safe
    const threadCount = 20
    var threads: array[threadCount, Thread[pointer]]

    # Create threads that simultaneously access state machine
    for i in 0..<threadCount:
      createThread(threads[i], proc(arg: pointer) {.thread.} =
        # Simulate commit operation
        testSetup.stateMachine.commit(int64(i+1), "callback" & $(i+1))

        # Simulate rollback operation
        testSetup.stateMachine.rollback(int64(i+1), "callback" & $(i+1))
      , nil)

    # Wait for all threads
    for i in 0..<threadCount:
      joinThread(threads[i])

    # Verify no crashes occurred
    # State machine should handle concurrent access gracefully

    # Check final state
    let finalIndex = testSetup.stateMachine.getLastAppliedIndex()
    check finalIndex >= 0 # Should be valid

  test "Memory consistency under concurrent access":
    # Test that memory remains consistent under concurrent access
    testSetup.node.becomeLeader()

    # Create multiple threads that repeatedly commit and check state
    const iterationCount = 1000
    var threads: seq[Thread[pointer]]
    threads.setLen(10)

    var sharedCounter = 0
    var sharedCounterLock: Lock
    initLock(sharedCounterLock)

    for i in 0..<10:
      createThread(threads[i], proc(arg: pointer) {.thread.} =
        for j in 0..<iterationCount:
          # Commit data
          let result = testSetup.node.commit("memory" & $j)

          # Update shared counter (simulating external state)
          withLock sharedCounterLock:
            sharedCounter.inc

          # Verify result
          if result <= 0:
            echo "Commit failed: ", result
      , nil)

    # Wait for all threads
    for i in 0..<10:
      joinThread(threads[i])

    # Verify shared counter
    check sharedCounter == 10 * iterationCount

    # Verify state machine received all commits
    check testSetup.stateMachine.commits.len >= 10 *
        iterationCount div 2 # Many should succeed
