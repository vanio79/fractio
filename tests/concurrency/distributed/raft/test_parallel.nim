# Concurrency Tests for Parallel Appends and Reads/Writes
#
# LevelDB is thread-safe for concurrent operations. We use a single global
# log store instance shared across threads. The appendEntry function has
# its own lock for index assignment, so no additional synchronization needed.

import unittest
import std/[typedthreads, sequtils, options, os, algorithm]

import fractio/distributed/raft/types
import fractio/distributed/raft/node

# Global state - single log store instance shared across threads
var gLogStore: WiscKeyLogStore
var gLogStorePath: string
var gTestCounter: int = 0

# Thread data structures - no GC-managed fields
type
  AppendWorkerData* = object
    threadId*: int
    result*: int64
    error*: string
    success*: bool

  ReadWorkerData* = object
    index*: int64
    hasResult*: bool
    resultTerm*: int64
    resultType*: LogEntryType
    resultData*: string
    error*: string
    success*: bool

# Worker procs - use {.cast(gcsafe).} since LevelDB is thread-safe
proc appendWorker*(arg: pointer) {.thread.} =
  let data = cast[ptr AppendWorkerData](arg)
  try:
    {.cast(gcsafe).}:
      data.result = gLogStore.appendEntry(LogEntry(
        term: 1,
        entryType: LET_NORMAL,
        data: "thread_" & $data.threadId
      ))
    data.success = true
  except CatchableError as e:
    data.error = e.msg
    data.success = false

proc readWorker*(arg: pointer) {.thread.} =
  let data = cast[ptr ReadWorkerData](arg)
  try:
    {.cast(gcsafe).}:
      let entry = gLogStore.getEntry(data.index)
      if entry.isSome:
        data.hasResult = true
        data.resultTerm = entry.get.term
        data.resultType = entry.get.entryType
        data.resultData = entry.get.data
      else:
        data.hasResult = false
    data.success = true
  except CatchableError as e:
    data.error = e.msg
    data.success = false

# Test Fixtures
type
  ConcurrencyTestSetup* = object
    testPath*: string

proc setupConcurrencyTest*(path: string): ConcurrencyTestSetup =
  result.testPath = path
  if dirExists(path):
    removeDir(path)
  createDir(path)
  gLogStorePath = path
  gLogStore = newWiscKeyLogStore(path)

proc teardownConcurrencyTest*(setup: var ConcurrencyTestSetup) =
  if gLogStore != nil:
    gLogStore.close()
    gLogStore = nil
  if dirExists(setup.testPath):
    try:
      removeDir(setup.testPath)
    except:
      discard


# Test Suite
suite "Parallel Append Tests":

  setup:
    inc gTestCounter
    var testSetup = setupConcurrencyTest("tmp/raft_parallel_" & $gTestCounter & "/")

  teardown:
    teardownConcurrencyTest(testSetup)

  test "Single thread append":
    var entry = LogEntry(term: 1, entryType: LET_NORMAL, data: "single")
    let idx = gLogStore.appendEntry(entry)
    check idx == 1

    let retrieved = gLogStore.getEntry(idx)
    check retrieved.isSome
    check retrieved.get.data == "single"

  test "Two threads concurrent append":
    const threadCount = 2
    var threads: array[threadCount, Thread[pointer]]
    var workerData: array[threadCount, AppendWorkerData]

    # Initialize worker data
    for i in 0..<threadCount:
      workerData[i] = AppendWorkerData(
        threadId: i,
        result: 0,
        error: "",
        success: false
      )
      createThread(threads[i], appendWorker, addr(workerData[i]))

    # Wait for all threads
    for i in 0..<threadCount:
      joinThread(threads[i])

    # Check results
    var successCount = 0
    var indices: seq[int64] = @[]
    for i in 0..<threadCount:
      if workerData[i].success:
        successCount.inc
        indices.add(workerData[i].result)

    check successCount == threadCount

    # Verify entries exist using the shared log store (no need to reopen)
    for idx in indices:
      let entry = gLogStore.getEntry(idx)
      check entry.isSome

  test "Multiple threads concurrent append":
    const threadCount = 10
    var threads: array[threadCount, Thread[pointer]]
    var workerData: array[threadCount, AppendWorkerData]

    for i in 0..<threadCount:
      workerData[i] = AppendWorkerData(
        threadId: i,
        result: 0,
        error: "",
        success: false
      )
      createThread(threads[i], appendWorker, addr(workerData[i]))

    for i in 0..<threadCount:
      joinThread(threads[i])

    var successCount = 0
    var indices: seq[int64] = @[]
    for i in 0..<threadCount:
      if workerData[i].success:
        successCount.inc
        indices.add(workerData[i].result)

    check successCount == threadCount

    # All indices should be unique
    let uniqueIndices = indices.deduplicate()
    check uniqueIndices.len == threadCount

  test "High contention append test":
    const threadCount = 50
    var threads: seq[Thread[pointer]] = newSeq[Thread[pointer]](threadCount)
    var workerData: seq[AppendWorkerData] = newSeq[AppendWorkerData](threadCount)

    for i in 0..<threadCount:
      workerData[i] = AppendWorkerData(
        threadId: i,
        result: 0,
        error: "",
        success: false
      )
      createThread(threads[i], appendWorker, addr(workerData[i]))

    for i in 0..<threadCount:
      joinThread(threads[i])

    var successCount = 0
    for i in 0..<threadCount:
      if workerData[i].success:
        successCount.inc

    check successCount == threadCount


suite "Concurrent Read Tests":

  setup:
    inc gTestCounter
    var testSetup = setupConcurrencyTest("tmp/raft_concurrent_read_" &
        $gTestCounter & "/")

  teardown:
    teardownConcurrencyTest(testSetup)

  test "Read after single write":
    let writeIdx = gLogStore.appendEntry(LogEntry(
      term: 1,
      entryType: LET_NORMAL,
      data: "read test"
    ))

    var data = ReadWorkerData(
      index: writeIdx,
      hasResult: false,
      success: false
    )

    readWorker(addr(data))
    check data.success == true
    check data.hasResult == true
    check data.resultData == "read test"

  test "Concurrent reads of same entry":
    let writeIdx = gLogStore.appendEntry(LogEntry(
      term: 1,
      entryType: LET_NORMAL,
      data: "shared read"
    ))

    const threadCount = 10
    var threads: array[threadCount, Thread[pointer]]
    var workerData: array[threadCount, ReadWorkerData]

    for i in 0..<threadCount:
      workerData[i] = ReadWorkerData(
        index: writeIdx,
        hasResult: false,
        success: false
      )
      createThread(threads[i], readWorker, addr(workerData[i]))

    for i in 0..<threadCount:
      joinThread(threads[i])

    for i in 0..<threadCount:
      check workerData[i].success == true
      check workerData[i].hasResult == true
      check workerData[i].resultData == "shared read"

  test "Concurrent reads of different entries":
    var indices: seq[int64] = @[]
    for i in 1..10:
      indices.add(gLogStore.appendEntry(LogEntry(
        term: 1,
        entryType: LET_NORMAL,
        data: "entry" & $i
      )))

    const threadCount = 10
    var threads: array[threadCount, Thread[pointer]]
    var workerData: array[threadCount, ReadWorkerData]

    for i in 0..<threadCount:
      workerData[i] = ReadWorkerData(
        index: indices[i],
        hasResult: false,
        success: false
      )
      createThread(threads[i], readWorker, addr(workerData[i]))

    for i in 0..<threadCount:
      joinThread(threads[i])

    for i in 0..<threadCount:
      check workerData[i].success == true
      check workerData[i].hasResult == true
      check workerData[i].resultData == "entry" & $(i + 1)

  test "Read non-existent entry":
    var data = ReadWorkerData(
      index: 99999,
      hasResult: false,
      success: false
    )

    readWorker(addr(data))
    check data.success == true
    check data.hasResult == false


suite "Mixed Read/Write Tests":

  setup:
    inc gTestCounter
    var testSetup = setupConcurrencyTest("tmp/raft_mixed_" & $gTestCounter & "/")

  teardown:
    teardownConcurrencyTest(testSetup)

  test "Concurrent reads and writes":
    # Pre-populate some entries
    for i in 1..5:
      discard gLogStore.appendEntry(LogEntry(
        term: 1,
        entryType: LET_NORMAL,
        data: "initial" & $i
      ))

    const threadCount = 10
    var threads: array[threadCount, Thread[pointer]]
    var appendData: array[threadCount, AppendWorkerData]
    var readData: array[threadCount, ReadWorkerData]

    # Create append threads
    for i in 0..<5:
      appendData[i] = AppendWorkerData(threadId: i, result: 0, success: false)
      createThread(threads[i], appendWorker, addr(appendData[i]))

    # Create read threads
    for i in 0..<5:
      readData[i] = ReadWorkerData(
        index: int64(i + 1),
        hasResult: false,
        success: false
      )
      createThread(threads[5 + i], readWorker, addr(readData[i]))

    for i in 0..<threadCount:
      joinThread(threads[i])

    # Check results
    var appendSuccess = 0
    var readSuccess = 0

    for i in 0..<5:
      if appendData[i].success:
        appendSuccess.inc
      if readData[i].success and readData[i].hasResult:
        readSuccess.inc

    check appendSuccess == 5
    check readSuccess == 5


suite "Thread Safety Verification":

  test "No data corruption under concurrent access":
    inc gTestCounter
    var testSetup = setupConcurrencyTest("tmp/raft_corruption_" &
        $gTestCounter & "/")

    const threadCount = 20
    var threads: seq[Thread[pointer]] = newSeq[Thread[pointer]](threadCount)
    var workerData: seq[AppendWorkerData] = newSeq[AppendWorkerData](threadCount)

    for i in 0..<threadCount:
      workerData[i] = AppendWorkerData(threadId: i, result: 0, success: false)
      createThread(threads[i], appendWorker, addr(workerData[i]))

    for i in 0..<threadCount:
      joinThread(threads[i])

    # Verify all entries
    var successCount = 0
    for i in 0..<threadCount:
      if workerData[i].success and workerData[i].result > 0:
        successCount.inc
        let entry = gLogStore.getEntry(workerData[i].result)
        check entry.isSome

    check successCount == threadCount

    teardownConcurrencyTest(testSetup)

  test "Atomic index assignment":
    inc gTestCounter
    var testSetup = setupConcurrencyTest("tmp/raft_atomic_" & $gTestCounter & "/")

    const threadCount = 100
    var threads: seq[Thread[pointer]] = newSeq[Thread[pointer]](threadCount)
    var workerData: seq[AppendWorkerData] = newSeq[AppendWorkerData](threadCount)
    var indices: seq[int64] = @[]

    for i in 0..<threadCount:
      workerData[i] = AppendWorkerData(threadId: i, result: 0, success: false)
      createThread(threads[i], appendWorker, addr(workerData[i]))

    for i in 0..<threadCount:
      joinThread(threads[i])

    for i in 0..<threadCount:
      if workerData[i].success:
        indices.add(workerData[i].result)

    # All indices should be unique
    let uniqueIndices = indices.deduplicate()
    check uniqueIndices.len == indices.len

    # Indices should be sequential
    indices.sort()
    for i in 1..<indices.len:
      check indices[i] == indices[i-1] + 1

    teardownConcurrencyTest(testSetup)
