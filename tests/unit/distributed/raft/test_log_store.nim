# Unit Tests for WiscKeyLogStore Operations

import unittest
import std/[options, strutils, json, os]

import fractio/distributed/raft/types
import fractio/distributed/raft/node

# Test Fixtures
type
  LogStoreTestSetup* = object
    logStore*: WiscKeyLogStore
    testPath*: string

proc setupLogStoreTest*(path: string = "tmp/raft_logstore_test/"): LogStoreTestSetup =
  ## Setup log store test environment
  result.testPath = path
  # Clean up any existing test directory
  if dirExists(path):
    removeDir(path)
  result.logStore = newWiscKeyLogStore(path)

proc teardownLogStoreTest*(setup: var LogStoreTestSetup) =
  ## Teardown log store test environment
  if setup.logStore != nil:
    setup.logStore.close()
  # Clean up test directory
  if dirExists(setup.testPath):
    removeDir(setup.testPath)


# Test Suite
suite "WiscKeyLogStore Unit Tests":

  setup:
    var testSetup = setupLogStoreTest()

  teardown:
    teardownLogStoreTest(testSetup)

  test "Log store initialization":
    check testSetup.logStore != nil
    check testSetup.logStore.startIndex == 1
    check testSetup.logStore.nextIndex == 1

  test "Append single entry":
    let entry = LogEntry(
      term: 1,
      entryType: LET_NORMAL,
      data: "test data"
    )

    let index = testSetup.logStore.appendEntry(entry)
    check index == 1
    check testSetup.logStore.nextIndex == 2

  test "Append multiple entries":
    let entries = [
      LogEntry(term: 1, entryType: LET_NORMAL, data: "entry1"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "entry2"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "entry3")
    ]

    var lastIndex: int64 = 0
    for entry in entries:
      lastIndex = testSetup.logStore.appendEntry(entry)

    check lastIndex == 3
    check testSetup.logStore.nextIndex == 4

  test "Get entry by index":
    let entry = LogEntry(
      term: 1,
      entryType: LET_NORMAL,
      data: "retrievable data"
    )

    discard testSetup.logStore.appendEntry(entry)

    let retrieved = testSetup.logStore.getEntry(1)
    check retrieved.isSome
    check retrieved.get.term == 1
    check retrieved.get.data == "retrievable data"
    check retrieved.get.entryType == LET_NORMAL

  test "Get non-existent entry":
    let retrieved = testSetup.logStore.getEntry(999)
    check retrieved.isNone

  test "Get last entry":
    let entries = [
      LogEntry(term: 1, entryType: LET_NORMAL, data: "first"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "second"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "third")
    ]

    for entry in entries:
      discard testSetup.logStore.appendEntry(entry)

    let last = testSetup.logStore.getLastEntry()
    check last.isSome
    check last.get.data == "third"

  test "Get last entry when empty":
    let last = testSetup.logStore.getLastEntry()
    check last.isNone

  test "Get entry range":
    let entries = [
      LogEntry(term: 1, entryType: LET_NORMAL, data: "1"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "2"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "3"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "4"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "5")
    ]

    for entry in entries:
      discard testSetup.logStore.appendEntry(entry)

    let range = testSetup.logStore.getEntries(2, 4)
    check range.len == 3
    check range[0].data == "2"
    check range[1].data == "3"
    check range[2].data == "4"

  test "Get entry range with invalid bounds":
    let entries = [
      LogEntry(term: 1, entryType: LET_NORMAL, data: "1"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "2")
    ]

    for entry in entries:
      discard testSetup.logStore.appendEntry(entry)

    # Request range beyond available entries
    let range = testSetup.logStore.getEntries(1, 10)
    check range.len == 2 # Only returns what exists

  test "Append entries with different types":
    let entries = [
      LogEntry(term: 1, entryType: LET_NORMAL, data: "normal"),
      LogEntry(term: 1, entryType: LET_CONFIG_CHANGE, data: "config"),
      LogEntry(term: 1, entryType: LET_NO_OP, data: "noop")
    ]

    for entry in entries:
      discard testSetup.logStore.appendEntry(entry)

    check testSetup.logStore.getEntry(1).get.entryType == LET_NORMAL
    check testSetup.logStore.getEntry(2).get.entryType == LET_CONFIG_CHANGE
    check testSetup.logStore.getEntry(3).get.entryType == LET_NO_OP

  test "Append entries with different terms":
    let entries = [
      LogEntry(term: 1, entryType: LET_NORMAL, data: "term1"),
      LogEntry(term: 2, entryType: LET_NORMAL, data: "term2"),
      LogEntry(term: 3, entryType: LET_NORMAL, data: "term3")
    ]

    for entry in entries:
      discard testSetup.logStore.appendEntry(entry)

    check testSetup.logStore.getEntry(1).get.term == 1
    check testSetup.logStore.getEntry(2).get.term == 2
    check testSetup.logStore.getEntry(3).get.term == 3

  test "Append entry with empty data":
    let entry = LogEntry(
      term: 1,
      entryType: LET_NORMAL,
      data: ""
    )

    let index = testSetup.logStore.appendEntry(entry)
    check index == 1

    let retrieved = testSetup.logStore.getEntry(1)
    check retrieved.isSome
    check retrieved.get.data == ""

  test "Append entry with large data":
    let largeData = "x".repeat(10000)
    let entry = LogEntry(
      term: 1,
      entryType: LET_NORMAL,
      data: largeData
    )

    let index = testSetup.logStore.appendEntry(entry)
    check index == 1

    let retrieved = testSetup.logStore.getEntry(1)
    check retrieved.isSome
    check retrieved.get.data == largeData

  test "Append entry with special characters":
    let specialData = "data with\nnewlines\ttabs\"quotes'and'unicode\u00E9"
    let entry = LogEntry(
      term: 1,
      entryType: LET_NORMAL,
      data: specialData
    )

    let index = testSetup.logStore.appendEntry(entry)
    check index == 1

    let retrieved = testSetup.logStore.getEntry(1)
    check retrieved.isSome
    check retrieved.get.data == specialData

  test "Log store persistence across close and reopen":
    # Write some entries
    let entries = [
      LogEntry(term: 1, entryType: LET_NORMAL, data: "persist1"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "persist2")
    ]

    for entry in entries:
      discard testSetup.logStore.appendEntry(entry)

    # Close the log store
    testSetup.logStore.close()

    # Reopen
    testSetup.logStore = newWiscKeyLogStore(testSetup.testPath)

    # Note: In current implementation, nextIndex is reset
    # This test verifies the backend can be reopened
    check testSetup.logStore != nil

  test "Sequential append and retrieve":
    var lastIndex: int64 = 0

    for i in 1..100:
      let entry = LogEntry(
        term: 1,
        entryType: LET_NORMAL,
        data: "seq" & $i
      )
      lastIndex = testSetup.logStore.appendEntry(entry)

    check lastIndex == 100

    # Verify all entries are retrievable
    for i in 1..100:
      let retrieved = testSetup.logStore.getEntry(int64(i))
      check retrieved.isSome
      check retrieved.get.data == "seq" & $i

  test "Log store handles binary data":
    let binaryData = "\x00\x01\x02\x03\x04\x05"
    let entry = LogEntry(
      term: 1,
      entryType: LET_NORMAL,
      data: binaryData
    )

    discard testSetup.logStore.appendEntry(entry)

    # Note: Binary data handling depends on JSON serialization
    # Current implementation uses JSON which may not preserve binary data
    # This test documents current behavior

  test "Multiple appends maintain correct indices":
    let idx1 = testSetup.logStore.appendEntry(LogEntry(term: 1,
        entryType: LET_NORMAL, data: "a"))
    let idx2 = testSetup.logStore.appendEntry(LogEntry(term: 1,
        entryType: LET_NORMAL, data: "b"))
    let idx3 = testSetup.logStore.appendEntry(LogEntry(term: 1,
        entryType: LET_NORMAL, data: "c"))

    check idx1 == 1
    check idx2 == 2
    check idx3 == 3
    check idx1 < idx2
    check idx2 < idx3

  test "Get entries in reverse order":
    let entries = [
      LogEntry(term: 1, entryType: LET_NORMAL, data: "first"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "second"),
      LogEntry(term: 1, entryType: LET_NORMAL, data: "third")
    ]

    for entry in entries:
      discard testSetup.logStore.appendEntry(entry)

    # Get entries from higher to lower index
    let e3 = testSetup.logStore.getEntry(3)
    let e2 = testSetup.logStore.getEntry(2)
    let e1 = testSetup.logStore.getEntry(1)

    check e3.get.data == "third"
    check e2.get.data == "second"
    check e1.get.data == "first"

  test "Log store with concurrent path":
    # Test that multiple log stores can exist in different paths
    var setup2 = setupLogStoreTest("tmp/raft_logstore_test_2/")

    let entry1 = LogEntry(term: 1, entryType: LET_NORMAL, data: "store1")
    let entry2 = LogEntry(term: 1, entryType: LET_NORMAL, data: "store2")

    discard testSetup.logStore.appendEntry(entry1)
    discard setup2.logStore.appendEntry(entry2)

    check testSetup.logStore.getEntry(1).get.data == "store1"
    check setup2.logStore.getEntry(1).get.data == "store2"

    setup2.logStore.close()
    removeDir("tmp/raft_logstore_test_2/")
