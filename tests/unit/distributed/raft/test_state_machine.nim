# Unit Tests for State Machine Interface

import unittest

import fractio/distributed/raft/types

# Test State Machine
type
  TestStateMachine* = ref object of StateMachine
    ## Test state machine for unit testing
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
  TestSetup* = object
    stateMachine*: TestStateMachine

proc setupTest*(): TestSetup =
  ## Setup test environment
  result.stateMachine = TestStateMachine(
    commits: @[],
    rollbacks: @[],
    lastAppliedIndex: 0
  )

proc teardownTest*(setup: var TestSetup) =
  ## Teardown test environment (nothing to cleanup for state machine)
  discard


# Test Suite
suite "StateMachine Interface Tests":

  setup:
    var testSetup = setupTest()

  teardown:
    teardownTest(testSetup)

  test "Initialization and basic state":
    check testSetup.stateMachine.commits.len == 0
    check testSetup.stateMachine.rollbacks.len == 0
    check testSetup.stateMachine.lastAppliedIndex == 0

  test "Commit operation":
    let result = testSetup.stateMachine.commit(1, "test data")
    check result == "OK"
    check testSetup.stateMachine.commits.len == 1
    check testSetup.stateMachine.commits[0][0] == 1
    check testSetup.stateMachine.commits[0][1] == "test data"
    check testSetup.stateMachine.lastAppliedIndex == 1

  test "Multiple commits":
    discard testSetup.stateMachine.commit(1, "data1")
    discard testSetup.stateMachine.commit(2, "data2")
    discard testSetup.stateMachine.commit(3, "data3")

    check testSetup.stateMachine.commits.len == 3
    check testSetup.stateMachine.commits[0][0] == 1
    check testSetup.stateMachine.commits[1][0] == 2
    check testSetup.stateMachine.commits[2][0] == 3
    check testSetup.stateMachine.lastAppliedIndex == 3

  test "Rollback operation":
    discard testSetup.stateMachine.commit(1, "data1")
    discard testSetup.stateMachine.commit(2, "data2")

    testSetup.stateMachine.rollback(2, "data2")

    check testSetup.stateMachine.rollbacks.len == 1
    check testSetup.stateMachine.rollbacks[0][0] == 2
    check testSetup.stateMachine.rollbacks[0][1] == "data2"
    check testSetup.stateMachine.commits.len == 2 # Commits not removed

  test "Rollback without commits":
    testSetup.stateMachine.rollback(1, "data1")
    check testSetup.stateMachine.rollbacks.len == 1
    check testSetup.stateMachine.rollbacks[0][0] == 1
    check testSetup.stateMachine.rollbacks[0][1] == "data1"

  test "Get last applied index":
    discard testSetup.stateMachine.commit(1, "data1")
    discard testSetup.stateMachine.commit(2, "data2")

    let lastIndex = testSetup.stateMachine.getLastAppliedIndex()
    check lastIndex == 2

  test "Last applied index without commits":
    let lastIndex = testSetup.stateMachine.getLastAppliedIndex()
    check lastIndex == 0

  test "Mixed commits and rollbacks":
    discard testSetup.stateMachine.commit(1, "data1")
    discard testSetup.stateMachine.commit(2, "data2")
    testSetup.stateMachine.rollback(2, "data2")
    discard testSetup.stateMachine.commit(3, "data3")

    check testSetup.stateMachine.commits.len == 3
    check testSetup.stateMachine.rollbacks.len == 1
    check testSetup.stateMachine.lastAppliedIndex == 3

  test "Commit with empty data":
    let result = testSetup.stateMachine.commit(1, "")
    check result == "OK"
    check testSetup.stateMachine.commits.len == 1
    check testSetup.stateMachine.commits[0][1] == ""

  test "Rollback with empty data":
    discard testSetup.stateMachine.commit(1, "data1")
    testSetup.stateMachine.rollback(1, "")
    check testSetup.stateMachine.rollbacks.len == 1
    check testSetup.stateMachine.rollbacks[0][1] == ""

  test "Rollback non-existent log":
    # Should not crash or raise exceptions
    testSetup.stateMachine.rollback(999, "non-existent")
    check testSetup.stateMachine.rollbacks.len == 1
    check testSetup.stateMachine.rollbacks[0][0] == 999

  test "Rollback multiple times":
    discard testSetup.stateMachine.commit(1, "data1")
    testSetup.stateMachine.rollback(1, "data1")
    testSetup.stateMachine.rollback(1, "data1") # Second rollback

    check testSetup.stateMachine.rollbacks.len == 2
    check testSetup.stateMachine.rollbacks[0][0] == 1
    check testSetup.stateMachine.rollbacks[1][0] == 1

  test "State machine persistence":
    # Verify state machine maintains state across operations
    discard testSetup.stateMachine.commit(1, "persist")
    check testSetup.stateMachine.lastAppliedIndex == 1

    discard testSetup.stateMachine.commit(2, "persist2")
    check testSetup.stateMachine.lastAppliedIndex == 2

    check testSetup.stateMachine.commits.len == 2
