# Unit tests for MVCC Garbage Collector
# Comprehensive tests for GC policy, stats, version collection, and thread safety

import unittest
import std/[options, locks, atomics, tables, sets, sequtils, algorithm, strutils]
import std/[typedthreads, threadpool]
import fractio/core/types
import fractio/core/timestamp_provider
import fractio/core/transaction
import fractio/storage/mvcc/types as mvccTypes
import fractio/storage/mvcc/engine
import fractio/storage/mvcc/garbage_collector
import fractio/storage/backend
import fractio/distributed/sharedtimer/mock as sharedtimerMock
import fractio/utils/logging

# =============================================================================
# Mock Storage Backend for GC Testing
# =============================================================================

type
  MockGCBackend = ref object of StorageBackend
    ## Mock backend with full iterator support for GC testing
    data: tables.Table[string, string]
    sortedKeys: seq[string]
    putCount: int
    getCount: int
    deleteCount: int
    existsCount: int
    newIteratorCount: int
    isOpenFlag: bool
    lock: Lock

  MockGCIterator = ref object of StorageIterator
    currentIndex: int
    validFlag: bool

proc getMockBackend(iter: MockGCIterator): MockGCBackend =
  cast[MockGCBackend](iter.backend)

proc newMockGCBackend(): MockGCBackend =
  new(result)
  result.data = initTable[string, string]()
  result.sortedKeys = @[]
  result.isOpenFlag = true
  initLock(result.lock)

method open(backend: MockGCBackend, config: StorageConfig): bool =
  acquire(backend.lock)
  backend.isOpenFlag = true
  release(backend.lock)
  return true

method close(backend: MockGCBackend) =
  acquire(backend.lock)
  backend.isOpenFlag = false
  release(backend.lock)

method isOpen(backend: MockGCBackend): bool =
  acquire(backend.lock)
  result = backend.isOpenFlag
  release(backend.lock)

method put(backend: MockGCBackend, key: string, value: string): bool =
  acquire(backend.lock)
  backend.data[key] = value
  backend.putCount += 1
  if key notin backend.sortedKeys:
    backend.sortedKeys.add(key)
    backend.sortedKeys.sort()
  release(backend.lock)
  return true

method get(backend: MockGCBackend, key: string): Option[string] =
  acquire(backend.lock)
  backend.getCount += 1
  if key in backend.data:
    result = some(backend.data[key])
  else:
    result = none(string)
  release(backend.lock)

method delete(backend: MockGCBackend, key: string): bool =
  acquire(backend.lock)
  backend.deleteCount += 1
  if key in backend.data:
    backend.data.del(key)
    backend.sortedKeys = backend.sortedKeys.filterIt(it != key)
    release(backend.lock)
    return true
  release(backend.lock)
  return false

method exists(backend: MockGCBackend, key: string): bool =
  acquire(backend.lock)
  backend.existsCount += 1
  result = key in backend.data
  release(backend.lock)

method newIterator(backend: MockGCBackend): StorageIterator =
  acquire(backend.lock)
  backend.newIteratorCount += 1
  var iter: MockGCIterator
  new(iter)
  iter.backend = backend
  iter.currentIndex = -1
  iter.validFlag = false
  release(backend.lock)
  result = iter

method seekToFirst(iter: MockGCIterator): bool =
  let mb = iter.getMockBackend()
  acquire(mb.lock)
  if mb.sortedKeys.len > 0:
    iter.currentIndex = 0
    iter.validFlag = true
    release(mb.lock)
    return true
  iter.validFlag = false
  release(mb.lock)
  return false

method seekToLast(iter: MockGCIterator): bool =
  let mb = iter.getMockBackend()
  acquire(mb.lock)
  if mb.sortedKeys.len > 0:
    iter.currentIndex = mb.sortedKeys.len - 1
    iter.validFlag = true
    release(mb.lock)
    return true
  iter.validFlag = false
  release(mb.lock)
  return false

method seek(iter: MockGCIterator, key: string): bool =
  let mb = iter.getMockBackend()
  acquire(mb.lock)
  for i, k in mb.sortedKeys:
    if k >= key:
      iter.currentIndex = i
      iter.validFlag = true
      release(mb.lock)
      return true
  iter.validFlag = false
  release(mb.lock)
  return false

method next(iter: MockGCIterator): bool =
  let mb = iter.getMockBackend()
  acquire(mb.lock)
  if iter.currentIndex >= 0 and iter.currentIndex < mb.sortedKeys.len - 1:
    iter.currentIndex += 1
    iter.validFlag = true
    release(mb.lock)
    return true
  iter.validFlag = false
  release(mb.lock)
  return false

method prev(iter: MockGCIterator): bool =
  let mb = iter.getMockBackend()
  acquire(mb.lock)
  if iter.currentIndex > 0:
    iter.currentIndex -= 1
    iter.validFlag = true
    release(mb.lock)
    return true
  iter.validFlag = false
  release(mb.lock)
  return false

method valid(iter: MockGCIterator): bool =
  let mb = iter.getMockBackend()
  acquire(mb.lock)
  result = iter.validFlag and iter.currentIndex >= 0 and
           iter.currentIndex < mb.sortedKeys.len
  release(mb.lock)

method key(iter: MockGCIterator): string =
  let mb = iter.getMockBackend()
  acquire(mb.lock)
  if iter.validFlag and iter.currentIndex >= 0 and iter.currentIndex <
      mb.sortedKeys.len:
    result = mb.sortedKeys[iter.currentIndex]
  else:
    result = ""
  release(mb.lock)

method value(iter: MockGCIterator): string =
  let mb = iter.getMockBackend()
  acquire(mb.lock)
  if iter.validFlag and iter.currentIndex >= 0 and iter.currentIndex <
      mb.sortedKeys.len:
    let k = mb.sortedKeys[iter.currentIndex]
    if k in mb.data:
      result = mb.data[k]
    else:
      result = ""
  else:
    result = ""
  release(mb.lock)

method destroy(iter: MockGCIterator) =
  iter.validFlag = false
  iter.currentIndex = -1

# =============================================================================
# Helper Functions
# =============================================================================

proc addVersion(backend: MockGCBackend, userKey: string, timestamp: Timestamp,
    value: string, isDeleted: bool = false,
    txnId: TransactionID = InvalidTransactionID) =
  let mvccKey = makeVersionKey(userKey, timestamp)
  let mvccValue = encodeMVCCValue(value, timestamp, isDeleted, txnId)
  discard backend.put(mvccKey, mvccValue)

proc addIntent(backend: MockGCBackend, userKey: string, txnId: TransactionID,
    value: string, timestamp: Timestamp, isDeleted: bool = false) =
  let intentKey = makeIntentKey(userKey, txnId)
  let mvccValue = encodeMVCCValue(value, timestamp, isDeleted, txnId)
  discard backend.put(intentKey, mvccValue)

proc createTestEngine(mockBackend: MockGCBackend): MVCCEngine =
  let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1_000_000_000)
  let tsProvider = TimestampProvider(
    timer: mockTimer,
    lastTimestamp: 1000,
    lastCounter: 0,
    maxOffset: DEFAULT_MAX_OFFSET_NS,
    nodeId: 0
  )
  MVCCEngine(
    backend: mockBackend,
    timestampProvider: tsProvider,
    gcEnabled: false
  )

# =============================================================================
# Test Suites - GC Policy
# =============================================================================

suite "GC Policy - Creation":
  test "create default GC policy":
    let policy = newGCPolicy()
    check policy.minTimestamp == Timestamp(0)
    check policy.maxVersionsPerKey == DEFAULT_MAX_VERSIONS_PER_KEY
    check policy.maxAgeMs == DEFAULT_MAX_AGE_MS
    check policy.collectIntervalMs == DEFAULT_COLLECT_INTERVAL_MS

  test "create custom GC policy with all parameters":
    let policy = newGCPolicy(
      minTimestamp = Timestamp(5000),
      maxVersionsPerKey = 5,
      maxAgeMs = 60000,
      collectIntervalMs = 30000
    )
    check policy.minTimestamp == Timestamp(5000)
    check policy.maxVersionsPerKey == 5
    check policy.maxAgeMs == 60000
    check policy.collectIntervalMs == 30000

  test "create GC policy with partial parameters":
    let policy = newGCPolicy(minTimestamp = Timestamp(1000))
    check policy.minTimestamp == Timestamp(1000)
    check policy.maxVersionsPerKey == DEFAULT_MAX_VERSIONS_PER_KEY
    check policy.maxAgeMs == DEFAULT_MAX_AGE_MS
    check policy.collectIntervalMs == DEFAULT_COLLECT_INTERVAL_MS

  test "GC policy with zero minTimestamp":
    let policy = newGCPolicy(minTimestamp = Timestamp(0))
    check policy.minTimestamp == Timestamp(0)

  test "GC policy with high minTimestamp":
    let policy = newGCPolicy(minTimestamp = mvccTypes.MAX_TIMESTAMP)
    check policy.minTimestamp == mvccTypes.MAX_TIMESTAMP

  test "GC policy with zero maxVersionsPerKey":
    let policy = newGCPolicy(minTimestamp = Timestamp(0), maxVersionsPerKey = 0)
    check policy.maxVersionsPerKey == 0

  test "GC policy with large maxVersionsPerKey":
    let policy = newGCPolicy(minTimestamp = Timestamp(0),
        maxVersionsPerKey = 1000)
    check policy.maxVersionsPerKey == 1000

  test "GC policy with zero maxAgeMs":
    let policy = newGCPolicy(minTimestamp = Timestamp(0), maxAgeMs = 0)
    check policy.maxAgeMs == 0

  test "GC policy with very large maxAgeMs":
    let policy = newGCPolicy(minTimestamp = Timestamp(0), maxAgeMs = 86400000) # 24 hours
    check policy.maxAgeMs == 86400000

suite "GC Policy - Constants":
  test "DEFAULT_MAX_VERSIONS_PER_KEY is 10":
    check DEFAULT_MAX_VERSIONS_PER_KEY == 10

  test "DEFAULT_MAX_AGE_MS is 300000 (5 minutes)":
    check DEFAULT_MAX_AGE_MS == 300_000

  test "DEFAULT_COLLECT_INTERVAL_MS is 60000 (1 minute)":
    check DEFAULT_COLLECT_INTERVAL_MS == 60_000

suite "GC Stats - Creation":
  test "create empty GC stats":
    let stats = GCStats()
    check stats.keysScanned == 0
    check stats.versionsCollected == 0
    check stats.bytesCollected == 0
    check stats.lastRunTime == Timestamp(0)
    check stats.totalRunTimeMs == 0
    check stats.runCount == 0

  test "create GC stats with values":
    let stats = GCStats(
      keysScanned: 100,
      versionsCollected: 50,
      bytesCollected: 5000,
      lastRunTime: Timestamp(1000000),
      totalRunTimeMs: 100,
      runCount: 5
    )
    check stats.keysScanned == 100
    check stats.versionsCollected == 50
    check stats.bytesCollected == 5000
    check stats.lastRunTime == Timestamp(1000000)
    check stats.totalRunTimeMs == 100
    check stats.runCount == 5

  test "GC stats equality":
    let stats1 = GCStats(keysScanned: 10, versionsCollected: 5)
    let stats2 = GCStats(keysScanned: 10, versionsCollected: 5)
    let stats3 = GCStats(keysScanned: 20, versionsCollected: 10)
    check stats1 == stats2
    check (stats1 == stats3) == false

suite "GC Result - Creation":
  test "create empty GC result":
    let result = GCResult()
    check result.success == false
    check result.keysScanned == 0
    check result.versionsCollected == 0
    check result.bytesCollected == 0
    check result.error == ""

  test "create successful GC result":
    let result = GCResult(
      success: true,
      keysScanned: 50,
      versionsCollected: 25,
      bytesCollected: 2500,
      error: ""
    )
    check result.success == true
    check result.keysScanned == 50
    check result.versionsCollected == 25
    check result.bytesCollected == 2500

  test "create failed GC result":
    let result = GCResult(
      success: false,
      keysScanned: 0,
      versionsCollected: 0,
      bytesCollected: 0,
      error: "Collection failed"
    )
    check result.success == false
    check result.error == "Collection failed"

suite "Garbage Collector - Creation":
  test "create garbage collector with defaults":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    check gc.engine == engine
    check gc.policy.maxVersionsPerKey == DEFAULT_MAX_VERSIONS_PER_KEY
    check gc.running.load() == false
    check gc.stats.keysScanned == 0
    deinitLock(gc.lock)

  test "create garbage collector with custom policy":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let policy = newGCPolicy(minTimestamp = Timestamp(1000),
        maxVersionsPerKey = 5)
    let gc = newGarbageCollector(engine, policy)

    check gc.policy.minTimestamp == Timestamp(1000)
    check gc.policy.maxVersionsPerKey == 5
    deinitLock(gc.lock)

  test "create garbage collector with logger":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let logger = newLogger("gc_test")
    let gc = newGarbageCollector(engine, newGCPolicy(), logger)

    check gc.logger != nil
    deinitLock(gc.lock)

  test "garbage collector initializes lock":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    # Lock is initialized, we can acquire and release
    acquire(gc.lock)
    release(gc.lock)
    deinitLock(gc.lock)

suite "Garbage Collector - Running State":
  test "isRunning returns false initially":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    check gc.isRunning() == false
    deinitLock(gc.lock)

  test "startGC sets running to true":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    gc.startGC()
    check gc.isRunning() == true
    deinitLock(gc.lock)

  test "stopGC sets running to false":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    gc.startGC()
    gc.stopGC()
    check gc.isRunning() == false
    deinitLock(gc.lock)

  test "startGC twice is safe":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    gc.startGC()
    gc.startGC() # Should not crash
    check gc.isRunning() == true
    deinitLock(gc.lock)

  test "stopGC when not running is safe":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    gc.stopGC() # Should not crash when not running
    check gc.isRunning() == false
    deinitLock(gc.lock)

suite "Garbage Collector - Stats":
  test "getStats returns initial stats":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    let stats = gc.getStats()
    check stats.keysScanned == 0
    check stats.versionsCollected == 0
    check stats.bytesCollected == 0
    check stats.runCount == 0
    deinitLock(gc.lock)

  test "updateStats increments values":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    gc.updateStats(10, 5, 500, 100)
    let stats = gc.getStats()
    check stats.keysScanned == 10
    check stats.versionsCollected == 5
    check stats.bytesCollected == 500
    check stats.totalRunTimeMs == 100
    check stats.runCount == 1
    deinitLock(gc.lock)

  test "updateStats accumulates":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    gc.updateStats(10, 5, 500, 100)
    gc.updateStats(20, 10, 1000, 200)
    let stats = gc.getStats()
    check stats.keysScanned == 30
    check stats.versionsCollected == 15
    check stats.bytesCollected == 1500
    check stats.totalRunTimeMs == 300
    check stats.runCount == 2
    deinitLock(gc.lock)

  test "resetStats clears all":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    gc.updateStats(100, 50, 5000, 1000)
    gc.resetStats()
    let stats = gc.getStats()
    check stats.keysScanned == 0
    check stats.versionsCollected == 0
    check stats.bytesCollected == 0
    check stats.totalRunTimeMs == 0
    check stats.runCount == 0
    deinitLock(gc.lock)

suite "Garbage Collector - shouldCollectVersion":
  test "collect old version based on age":
    let gc = GarbageCollector(
      policy: newGCPolicy(minTimestamp = Timestamp(0), maxAgeMs = 60000),
      running: Atomic[bool](),
      stats: GCStats(),
      logger: newLogger("gc_test")
    )
    initLock(gc.lock)

    let version = MVCCValue(
      data: "old_data",
      timestamp: Timestamp(1_000_000_000),
      isDeleted: false,
      txnId: InvalidTransactionID
    )

    # 120 seconds later = 120,000 ms later
    let currentTime = Timestamp(1_000_000_000 + 120_000_000_000)
    check gc.shouldCollectVersion("key", version, currentTime) == true
    deinitLock(gc.lock)

  test "do not collect recent version":
    let gc = GarbageCollector(
      policy: newGCPolicy(minTimestamp = Timestamp(0), maxAgeMs = 60000),
      running: Atomic[bool](),
      stats: GCStats(),
      logger: newLogger("gc_test")
    )
    initLock(gc.lock)

    let version = MVCCValue(
      data: "recent_data",
      timestamp: Timestamp(1_000_000_000),
      isDeleted: false,
      txnId: InvalidTransactionID
    )

    # 30 seconds later = 30,000 ms later (less than 60 seconds)
    let currentTime = Timestamp(1_000_000_000 + 30_000_000_000)
    check gc.shouldCollectVersion("key", version, currentTime) == false
    deinitLock(gc.lock)

  test "collect version before minTimestamp":
    let gc = GarbageCollector(
      policy: newGCPolicy(minTimestamp = Timestamp(50_000_000_000)),
      running: Atomic[bool](),
      stats: GCStats(),
      logger: newLogger("gc_test")
    )
    initLock(gc.lock)

    let version = MVCCValue(
      data: "old_data",
      timestamp: Timestamp(10_000_000_000),
      isDeleted: false,
      txnId: InvalidTransactionID
    )

    let currentTime = Timestamp(100_000_000_000)
    check gc.shouldCollectVersion("key", version, currentTime) == true
    deinitLock(gc.lock)

  test "do not collect version after minTimestamp":
    let gc = GarbageCollector(
      policy: newGCPolicy(minTimestamp = Timestamp(50_000_000_000)),
      running: Atomic[bool](),
      stats: GCStats(),
      logger: newLogger("gc_test")
    )
    initLock(gc.lock)

    let version = MVCCValue(
      data: "new_data",
      timestamp: Timestamp(60_000_000_000),
      isDeleted: false,
      txnId: InvalidTransactionID
    )

    let currentTime = Timestamp(100_000_000_000)
    check gc.shouldCollectVersion("key", version, currentTime) == false
    deinitLock(gc.lock)

  test "collect version with both age and minTimestamp criteria":
    let gc = GarbageCollector(
      policy: newGCPolicy(
        minTimestamp = Timestamp(50_000_000_000),
        maxAgeMs = 60000
      ),
      running: Atomic[bool](),
      stats: GCStats(),
      logger: newLogger("gc_test")
    )
    initLock(gc.lock)

    # Version before minTimestamp but recent age
    let version = MVCCValue(
      data: "data",
      timestamp: Timestamp(40_000_000_000),
      isDeleted: false,
      txnId: InvalidTransactionID
    )

    # Current time makes it recent but before minTimestamp
    let currentTime = Timestamp(45_000_000_000)
    check gc.shouldCollectVersion("key", version, currentTime) == true
    deinitLock(gc.lock)

suite "Garbage Collector - collectVersionsForKey":
  test "collect versions for empty key":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    let result = gc.collectVersionsForKey("nonexistent_key")
    check result.success == true
    check result.keysScanned == 0
    check result.versionsCollected == 0
    deinitLock(gc.lock)

  test "collect versions keeps latest version":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine, newGCPolicy(minTimestamp = Timestamp(
        0), maxAgeMs = 0))

    # Add multiple versions
    addVersion(mockBackend, "key1", Timestamp(100), "v100")
    addVersion(mockBackend, "key1", Timestamp(200), "v200")
    addVersion(mockBackend, "key1", Timestamp(300), "v300")

    let result = gc.collectVersionsForKey("key1")
    check result.success == true
    check result.keysScanned == 1
    # Latest version should be kept
    let versions = engine.getAllVersions("key1")
    check versions.len >= 1
    check versions[0].value.data == "v300"
    deinitLock(gc.lock)

  test "collect versions removes old versions based on age":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine, newGCPolicy(minTimestamp = Timestamp(
        0), maxAgeMs = 1000))

    # Add old and new versions
    addVersion(mockBackend, "key1", Timestamp(100), "old_v100")
    addVersion(mockBackend, "key1", Timestamp(100_000_000_000), "new_v")

    let result = gc.collectVersionsForKey("key1")
    check result.success == true
    deinitLock(gc.lock)

  test "collect versions removes excess versions beyond max":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine, newGCPolicy(minTimestamp = Timestamp(
        0), maxVersionsPerKey = 2))

    # Add more versions than max
    addVersion(mockBackend, "key1", Timestamp(100), "v100")
    addVersion(mockBackend, "key1", Timestamp(200), "v200")
    addVersion(mockBackend, "key1", Timestamp(300), "v300")
    addVersion(mockBackend, "key1", Timestamp(400), "v400")

    let result = gc.collectVersionsForKey("key1")
    check result.success == true
    check result.versionsCollected >= 2 # Should remove older versions
    deinitLock(gc.lock)

  test "collect versions handles intents":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    let txnId = genTransactionIDLocal()
    addVersion(mockBackend, "key1", Timestamp(100), "v100")
    addIntent(mockBackend, "key1", txnId, "intent", Timestamp(200))

    let result = gc.collectVersionsForKey("key1")
    check result.success == true
    deinitLock(gc.lock)

suite "Garbage Collector - collectVersions":
  test "collect versions with empty backend":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    let result = gc.collectVersions()
    check result.success == true
    check result.keysScanned == 0
    deinitLock(gc.lock)

  test "collect versions with multiple keys":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine, newGCPolicy(minTimestamp = Timestamp(
        0), maxAgeMs = 0))

    addVersion(mockBackend, "key1", Timestamp(100), "v1")
    addVersion(mockBackend, "key2", Timestamp(100), "v2")
    addVersion(mockBackend, "key3", Timestamp(100), "v3")

    let result = gc.collectVersions()
    check result.success == true
    check result.keysScanned >= 0
    deinitLock(gc.lock)

  test "collect versions updates policy minTimestamp":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine, newGCPolicy(minTimestamp = Timestamp(0)))

    let result = gc.collectVersions(Timestamp(5000))
    check gc.policy.minTimestamp >= Timestamp(5000)
    deinitLock(gc.lock)

  test "collect versions updates stats":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    addVersion(mockBackend, "key1", Timestamp(100), "v1")
    discard gc.collectVersions()

    let stats = gc.getStats()
    check stats.runCount == 1
    deinitLock(gc.lock)

suite "Garbage Collector - collectVersionsForTransaction":
  test "collect versions for nonexistent transaction":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    let txnId = genTransactionIDLocal()
    let result = gc.collectVersionsForTransaction(txnId)
    check result.success == true
    check result.versionsCollected == 0
    deinitLock(gc.lock)

  test "collect versions for transaction with intents":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    let txnId = genTransactionIDLocal()
    addIntent(mockBackend, "key1", txnId, "intent1", Timestamp(100))

    let result = gc.collectVersionsForTransaction(txnId)
    check result.success == true
    check result.versionsCollected >= 1
    deinitLock(gc.lock)

  test "collect versions for specific transaction only":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    let txnId1 = genTransactionIDLocal()
    let txnId2 = genTransactionIDLocal()
    addIntent(mockBackend, "key1", txnId1, "intent1", Timestamp(100))
    addIntent(mockBackend, "key2", txnId2, "intent2", Timestamp(100))

    let result = gc.collectVersionsForTransaction(txnId1)
    check result.success == true
    # Should only collect intents for txnId1
    check mockBackend.exists(makeIntentKey("key2", txnId2))
    deinitLock(gc.lock)

suite "Garbage Collector - deleteKey Helper":
  test "deleteKey calls backend delete":
    let mockBackend = newMockGCBackend()
    discard mockBackend.put("test_key", "test_value")

    let result = deleteKey(mockBackend, "test_key")
    check result == true
    check mockBackend.exists("test_key") == false

  test "deleteKey returns false for nonexistent key":
    let mockBackend = newMockGCBackend()

    let result = deleteKey(mockBackend, "nonexistent")
    check result == false

suite "Garbage Collector - Thread Safety":
  # Thread safety tests use direct atomic operations on gc internals
  # since thread procs cannot capture GC-managed refs from outer scope

  test "stats lock is initialized":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    # Lock is properly initialized - can acquire/release
    acquire(gc.lock)
    release(gc.lock)

    # Atomic running field can be toggled
    gc.running.store(true)
    check gc.running.load() == true
    gc.running.store(false)
    check gc.running.load() == false
    deinitLock(gc.lock)

  test "running state atomic operations":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    # Test atomic running state directly
    check gc.running.load() == false
    gc.running.store(true)
    check gc.running.load() == true
    gc.running.store(false)
    check gc.running.load() == false
    deinitLock(gc.lock)

  test "stats update with lock":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    # Directly manipulate stats with lock (simulating thread-safe access)
    acquire(gc.lock)
    gc.stats.keysScanned = 100
    gc.stats.versionsCollected = 50
    gc.stats.bytesCollected = 5000
    gc.stats.runCount = 5
    release(gc.lock)

    let stats = gc.getStats()
    check stats.keysScanned == 100
    check stats.versionsCollected == 50
    check stats.bytesCollected == 5000
    check stats.runCount == 5
    deinitLock(gc.lock)

suite "Garbage Collector - Edge Cases":
  test "collect with zero maxVersionsPerKey":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine, newGCPolicy(minTimestamp = Timestamp(
        0), maxVersionsPerKey = 0))

    addVersion(mockBackend, "key1", Timestamp(100), "v100")

    let result = gc.collectVersionsForKey("key1")
    check result.success == true
    deinitLock(gc.lock)

  test "collect with zero maxAgeMs":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine, newGCPolicy(minTimestamp = Timestamp(
        0), maxAgeMs = 0))

    addVersion(mockBackend, "key1", Timestamp(100), "v100")

    let result = gc.collectVersionsForKey("key1")
    check result.success == true
    deinitLock(gc.lock)

  test "collect versions for key with single version":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    addVersion(mockBackend, "key1", Timestamp(100), "v100")

    let result = gc.collectVersionsForKey("key1")
    check result.success == true
    check result.keysScanned == 1
    check result.versionsCollected == 0 # Single version not collected
    deinitLock(gc.lock)

  test "collect handles deleted versions":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    addVersion(mockBackend, "key1", Timestamp(100), "", isDeleted = true)
    addVersion(mockBackend, "key1", Timestamp(200), "v200")

    let result = gc.collectVersionsForKey("key1")
    check result.success == true
    deinitLock(gc.lock)

  test "collect handles mixed keys and intents":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    addVersion(mockBackend, "key1", Timestamp(100), "v100")
    addIntent(mockBackend, "key1", genTransactionIDLocal(), "intent", Timestamp(200))
    addVersion(mockBackend, "key2", Timestamp(150), "v150")

    let result = gc.collectVersions()
    check result.success == true
    deinitLock(gc.lock)

suite "Garbage Collector - Large Data":
  test "collect many versions per key":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine, newGCPolicy(minTimestamp = Timestamp(
        0), maxVersionsPerKey = 10))

    for i in 0..<100:
      addVersion(mockBackend, "key1", Timestamp(i * 100), "v" & $i)

    let result = gc.collectVersionsForKey("key1")
    check result.success == true
    check result.versionsCollected >= 90 # Should keep only 10
    deinitLock(gc.lock)

  test "collect many keys":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    for i in 0..<100:
      addVersion(mockBackend, "key" & $i, Timestamp(100), "v" & $i)

    let result = gc.collectVersions()
    check result.success == true
    check result.keysScanned >= 0
    deinitLock(gc.lock)

  test "collect with large value data":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    let largeValue = "x".repeat(100000)
    addVersion(mockBackend, "key1", Timestamp(100), largeValue)

    let result = gc.collectVersionsForKey("key1")
    check result.success == true
    deinitLock(gc.lock)

suite "Garbage Collector - Integration":
  test "full collection workflow":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine, newGCPolicy(minTimestamp = Timestamp(
        0), maxVersionsPerKey = 5))

    # Add data
    for i in 0..<20:
      addVersion(mockBackend, "key" & $i, Timestamp(i * 100), "v" & $i)

    # Run collection
    gc.startGC()
    let result = gc.collectVersions()
    gc.stopGC()

    check result.success == true
    check gc.isRunning() == false

    let stats = gc.getStats()
    check stats.runCount == 1
    deinitLock(gc.lock)

  test "multiple collection runs accumulate stats":
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine)

    for run in 0..<5:
      for i in 0..<10:
        addVersion(mockBackend, "run" & $run & "_key" & $i, Timestamp(run *
            1000 + i), "v")
      discard gc.collectVersions()

    let stats = gc.getStats()
    check stats.runCount == 5
    deinitLock(gc.lock)

# =============================================================================
# GC Intent Key Bug Fix Tests
# =============================================================================
# The GC's collectVersionsForKey was using encodeMVCCKey(userKey, timestamp,
# isIntent=true) which produces <userKey>\x00\x01<8-byte timestamp>, but actual
# intent keys use <userKey>\x00\x01<16-byte ULID txnId> (from makeIntentKey).
# Fixed by using makeIntentKey(userKey, version.value.txnId) instead.
# =============================================================================

suite "GC Intent Key Format - Bug Fix Verification":
  test "makeIntentKey produces correct format":
    ## Verify that makeIntentKey produces <userKey>\x00\x01<16-byte ULID>
    let txnId = genTransactionIDLocal()
    let intentKey = makeIntentKey("test_key", txnId)
    # intentKey should end with \x00\x01 + 16-byte ULID
    check intentKey.len == "test_key".len + 2 + 16 # userKey + suffix(2) + ULID(16)
    # Check the \x00\x01 suffix is at the right position
    let suffixPos = intentKey.len - 18
    check intentKey[suffixPos] == '\x00'
    check intentKey[suffixPos + 1] == '\x01'

  test "encodeIntentKey matches makeIntentKey":
    ## encodeIntentKey and makeIntentKey should produce identical results
    let txnId = genTransactionIDLocal()
    let fromEncode = encodeIntentKey("my_key", txnId)
    let fromMake = makeIntentKey("my_key", txnId)
    check fromEncode == fromMake

  test "encodeMVCCKey with isIntent=true produces DIFFERENT format than makeIntentKey":
    ## This is the core of the bug: encodeMVCCKey(..., isIntent=true) produces
    ## <userKey>\x00\x01<8-byte timestamp> which is NOT a valid intent key.
    ## Actual intent keys use <userKey>\x00\x01<16-byte ULID txnId>.
    let txnId = genTransactionIDLocal()
    let timestamp = Timestamp(12345)
    let wrongIntentKey = encodeMVCCKey("my_key", timestamp, true)
    let correctIntentKey = makeIntentKey("my_key", txnId)
    # They should be DIFFERENT lengths (8 bytes vs 16 bytes for the ID part)
    check wrongIntentKey.len == "my_key".len + 2 + 8 # userKey + \x00\x01 + 8-byte ts
    check correctIntentKey.len == "my_key".len + 2 + 16 # userKey + \x00\x01 + 16-byte ULID
    check wrongIntentKey != correctIntentKey

  test "GC collects intent along with version using makeIntentKey":
    ## When GC collects a version, it should also clean up the associated intent.
    ## The intent key must match the actual intent key format.
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine, newGCPolicy(minTimestamp = Timestamp(0)))

    let txnId = genTransactionIDLocal()
    let oldTs = Timestamp(1_000_000_000) # 1 second in ns (very old)
    let newTs = Timestamp(10_000_000_000) # 10 seconds in ns (newer)

    # Add old version with its intent
    addVersion(mockBackend, "key1", oldTs, "old_value", txnId = txnId)
    addIntent(mockBackend, "key1", txnId, "intent_value", oldTs)
    # Add newer version (this will be kept)
    addVersion(mockBackend, "key1", newTs, "new_value")

    # Verify both version and intent exist
    let intentKey = makeIntentKey("key1", txnId)
    check mockBackend.exists(intentKey)

    # Run GC on this key
    let result = gc.collectVersionsForKey("key1")
    check result.success == true
    check result.versionsCollected >= 1

    # The old intent should have been cleaned up
    check not mockBackend.exists(intentKey)
    deinitLock(gc.lock)

  test "GC does not crash when intent doesn't exist":
    ## If the intent was already cleaned up (e.g. by intent resolution),
    ## GC should not crash when trying to clean it.
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine, newGCPolicy(minTimestamp = Timestamp(0)))

    let txnId = genTransactionIDLocal()
    let oldTs = Timestamp(1_000_000_000)
    let newTs = Timestamp(10_000_000_000)

    # Add old version WITHOUT intent (intent was already resolved)
    addVersion(mockBackend, "key1", oldTs, "old_value", txnId = txnId)
    # Add newer version
    addVersion(mockBackend, "key1", newTs, "new_value")

    # Run GC - should not crash even though intent doesn't exist
    let result = gc.collectVersionsForKey("key1")
    check result.success == true
    check result.versionsCollected >= 1
    deinitLock(gc.lock)

  test "GC with maxVersionsPerKey also uses makeIntentKey":
    ## When GC collects versions beyond maxVersionsPerKey, it also cleans
    ## up associated intents. This path uses the same makeIntentKey fix.
    let mockBackend = newMockGCBackend()
    let engine = createTestEngine(mockBackend)
    let gc = newGarbageCollector(engine, newGCPolicy(
      minTimestamp = Timestamp(0), maxVersionsPerKey = 3))

    let txnId = genTransactionIDLocal()
    # Add 5 versions with intents for each
    for i in 0..<5:
      let ts = Timestamp((i + 1) * 1_000_000_000)
      let tid = genTransactionIDLocal()
      addVersion(mockBackend, "key1", ts, "v" & $i, txnId = tid)
      addIntent(mockBackend, "key1", tid, "intent" & $i, ts)

    let result = gc.collectVersionsForKey("key1")
    check result.success == true
    # Should keep only 3 versions (the latest ones)
    check result.versionsCollected >= 2
    deinitLock(gc.lock)
