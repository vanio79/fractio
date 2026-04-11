# Unit tests for MVCC Engine
# Comprehensive tests for MVCC storage operations with dependency injection

import unittest
import std/[tables, options, sets, sequtils, algorithm, strutils]
import fractio/core/types
import fractio/core/timestamp_provider
import fractio/core/transaction
import fractio/storage/mvcc/types as mvccTypes
import fractio/storage/mvcc/engine
import fractio/storage/backend
import fractio/distributed/sharedtimer/mock as sharedtimerMock
import fractio/di/mocks

# =============================================================================
# Mock Storage Backend for MVCC Engine Testing
# =============================================================================

type
  MockMVCCBackend = ref object of StorageBackend
    ## Mock backend with full iterator support for MVCC testing
    data: tables.Table[string, string]
    sortedKeys: seq[string]
    putCount: int
    getCount: int
    deleteCount: int
    existsCount: int
    writeBatchCount: int
    flushCount: int
    newIteratorCount: int
    isOpenFlag: bool

  MockMVCCIterator = ref object of StorageIterator
    ## Mock iterator for MVCC backend (inherits backend field from StorageIterator)
    currentIndex: int
    validFlag: bool
    seekKey: string

proc getMockBackend(iter: MockMVCCIterator): MockMVCCBackend =
  ## Get the mock backend from iterator
  cast[MockMVCCBackend](iter.backend)

proc newMockMVCCBackend(): MockMVCCBackend =
  new(result)
  result.data = initTable[string, string]()
  result.sortedKeys = @[]
  result.isOpenFlag = true

method open(backend: MockMVCCBackend, config: StorageConfig): bool =
  backend.isOpenFlag = true
  return true

method close(backend: MockMVCCBackend) =
  backend.isOpenFlag = false

method isOpen(backend: MockMVCCBackend): bool =
  return backend.isOpenFlag

method put(backend: MockMVCCBackend, key: string, value: string): bool =
  backend.data[key] = value
  backend.putCount += 1
  # Update sorted keys
  if key notin backend.sortedKeys:
    backend.sortedKeys.add(key)
    backend.sortedKeys.sort()
  return true

method get(backend: MockMVCCBackend, key: string): Option[string] =
  backend.getCount += 1
  if key in backend.data:
    return some(backend.data[key])
  return none(string)

method delete(backend: MockMVCCBackend, key: string): bool =
  backend.deleteCount += 1
  if key in backend.data:
    backend.data.del(key)
    # Remove from sorted keys
    backend.sortedKeys = backend.sortedKeys.filterIt(it != key)
    return true
  return false

method exists(backend: MockMVCCBackend, key: string): bool =
  backend.existsCount += 1
  return key in backend.data

method writeBatch(backend: MockMVCCBackend, pairs: seq[KeyValuePair],
    deletes: seq[string]): bool =
  backend.writeBatchCount += 1
  for pair in pairs:
    backend.data[pair.key] = pair.value
    if pair.key notin backend.sortedKeys:
      backend.sortedKeys.add(pair.key)
  for delKey in deletes:
    if delKey in backend.data:
      backend.data.del(delKey)
  backend.sortedKeys.sort()
  return true

method flush(backend: MockMVCCBackend): bool =
  backend.flushCount += 1
  return true

method newIterator(backend: MockMVCCBackend): StorageIterator =
  backend.newIteratorCount += 1
  var iter: MockMVCCIterator
  new(iter)
  iter.backend = backend
  iter.currentIndex = -1
  iter.validFlag = false
  result = iter

method seekToFirst(iter: MockMVCCIterator): bool =
  let mb = iter.getMockBackend()
  if mb.sortedKeys.len > 0:
    iter.currentIndex = 0
    iter.validFlag = true
    return true
  iter.validFlag = false
  return false

method seekToLast(iter: MockMVCCIterator): bool =
  let mb = iter.getMockBackend()
  if mb.sortedKeys.len > 0:
    iter.currentIndex = mb.sortedKeys.len - 1
    iter.validFlag = true
    return true
  iter.validFlag = false
  return false

method seek(iter: MockMVCCIterator, key: string): bool =
  let mb = iter.getMockBackend()
  # Find first key >= given key
  for i, k in mb.sortedKeys:
    if k >= key:
      iter.currentIndex = i
      iter.validFlag = true
      iter.seekKey = key
      return true
  iter.validFlag = false
  return false

method next(iter: MockMVCCIterator): bool =
  let mb = iter.getMockBackend()
  if iter.currentIndex >= 0 and iter.currentIndex < mb.sortedKeys.len - 1:
    iter.currentIndex += 1
    iter.validFlag = true
    return true
  iter.validFlag = false
  return false

method prev(iter: MockMVCCIterator): bool =
  if iter.currentIndex > 0:
    iter.currentIndex -= 1
    iter.validFlag = true
    return true
  iter.validFlag = false
  return false

method valid(iter: MockMVCCIterator): bool =
  let mb = iter.getMockBackend()
  return iter.validFlag and iter.currentIndex >= 0 and
         iter.currentIndex < mb.sortedKeys.len

method key(iter: MockMVCCIterator): string =
  if iter.valid():
    return iter.getMockBackend().sortedKeys[iter.currentIndex]
  return ""

method value(iter: MockMVCCIterator): string =
  if iter.valid():
    let mb = iter.getMockBackend()
    let k = mb.sortedKeys[iter.currentIndex]
    if k in mb.data:
      return mb.data[k]
  return ""

method destroy(iter: MockMVCCIterator) =
  iter.validFlag = false
  iter.currentIndex = -1

# =============================================================================
# Helper functions to populate MVCC keys
# =============================================================================

proc addVersion(backend: MockMVCCBackend, userKey: string, timestamp: Timestamp,
    value: string, isDeleted: bool = false,
        txnId: TransactionID = InvalidTransactionID) =
  let mvccKey = makeVersionKey(userKey, timestamp)
  let mvccValue = encodeMVCCValue(value, timestamp, isDeleted, txnId)
  discard backend.put(mvccKey, mvccValue)

proc addIntent(backend: MockMVCCBackend, userKey: string, txnId: TransactionID,
    value: string, timestamp: Timestamp, isDeleted: bool = false) =
  let intentKey = makeIntentKey(userKey, txnId)
  let mvccValue = encodeMVCCValue(value, timestamp, isDeleted, txnId)
  discard backend.put(intentKey, mvccValue)

# =============================================================================
# Test Suites
# =============================================================================

suite "MVCCEngine - Basic Operations":
  var mockTimer: sharedtimerMock.MockTimeProvider
  var mockBackend: MockMVCCBackend
  var tsProvider: TimestampProvider
  var mvccEngine: MVCCEngine

  setup:
    mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    mockBackend = newMockMVCCBackend()
    tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 1000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    mvccEngine = MVCCEngine(
      backend: mockBackend,
      timestampProvider: tsProvider,
      gcEnabled: false
    )

  teardown:
    mockBackend.close()

  test "create MVCC engine":
    check mvccEngine.backend != nil
    check mvccEngine.timestampProvider != nil
    check mvccEngine.gcEnabled == false

  test "get non-existent key":
    let result = mvccEngine.mvccGet("key1", Timestamp(1000))
    check result.success == true
    check result.value.isNone

  test "get existing version":
    addVersion(mockBackend, "key1", Timestamp(500), "value500")
    addVersion(mockBackend, "key1", Timestamp(1000), "value1000")
    let result = mvccEngine.mvccGet("key1", Timestamp(800))
    check result.success == true
    check result.value.isSome
    check result.value.get().data == "value500"

  test "get latest version":
    addVersion(mockBackend, "key1", Timestamp(500), "value500")
    addVersion(mockBackend, "key1", Timestamp(1000), "value1000")
    let result = mvccEngine.mvccGet("key1", Timestamp(2000))
    check result.success == true
    check result.value.isSome
    check result.value.get().data == "value1000"

  test "get deleted version":
    addVersion(mockBackend, "key1", Timestamp(500), "value500",
        isDeleted = true)
    let result = mvccEngine.mvccGet("key1", Timestamp(1000))
    check result.success == true
    check result.value.isNone

  test "get with own intent":
    let txnId = genTransactionID()
    addIntent(mockBackend, "key1", txnId, "intentValue", Timestamp(1000))
    let result = mvccEngine.mvccGet("key1", Timestamp(2000), txnId)
    check result.success == true
    check result.value.isSome
    check result.value.get().data == "intentValue"

  test "get with foreign intent returns conflict":
    let foreignTxnId = genTransactionID()
    addIntent(mockBackend, "key1", foreignTxnId, "intentValue", Timestamp(1000))
    let result = mvccEngine.mvccGet("key1", Timestamp(2000))
    check result.success == false
    check result.error.code == mvccIntentConflict

suite "MVCCEngine - Write Operations":
  var mockTimer: sharedtimerMock.MockTimeProvider
  var mockBackend: MockMVCCBackend
  var tsProvider: TimestampProvider
  var mvccEngine: MVCCEngine

  setup:
    mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    mockBackend = newMockMVCCBackend()
    tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 1000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    mvccEngine = MVCCEngine(
      backend: mockBackend,
      timestampProvider: tsProvider,
      gcEnabled: false
    )

  test "put creates intent":
    let txn = newMVCCTransaction(tsProvider)
    let result = mvccEngine.mvccPut(txn, "key1", "value1")
    check result.success == true
    check mockBackend.putCount == 1
    check mockBackend.exists(makeIntentKey("key1", txn.id))

  test "put fails for non-pending transaction":
    let txn = newMVCCTransaction(tsProvider)
    txn.status = TXN_COMMITTED
    let result = mvccEngine.mvccPut(txn, "key1", "value1")
    check result.success == false
    check result.error.code == mvccInvalidTransaction

  test "put fails with existing intent":
    let txn = newMVCCTransaction(tsProvider)
    addIntent(mockBackend, "key1", txn.id, "oldValue", Timestamp(500))
    let result = mvccEngine.mvccPut(txn, "key1", "newValue")
    check result.success == false
    check result.error.code == mvccIntentConflict

  test "delete creates delete intent":
    let txn = newMVCCTransaction(tsProvider)
    let result = mvccEngine.mvccDelete(txn, "key1")
    check result.success == true
    check mockBackend.putCount == 1
    # Check that intent exists and is marked as deleted
    let intentKey = makeIntentKey("key1", txn.id)
    let intentValue = mockBackend.get(intentKey)
    check intentValue.isSome
    let decoded = decodeMVCCValue(intentValue.get())
    check decoded.isDeleted == true

  test "delete fails for non-pending transaction":
    let txn = newMVCCTransaction(tsProvider)
    txn.status = TXN_ABORTED
    let result = mvccEngine.mvccDelete(txn, "key1")
    check result.success == false
    check result.error.code == mvccInvalidTransaction

suite "MVCCEngine - Scan Operations":
  var mockTimer: sharedtimerMock.MockTimeProvider
  var mockBackend: MockMVCCBackend
  var tsProvider: TimestampProvider
  var mvccEngine: MVCCEngine

  setup:
    mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    mockBackend = newMockMVCCBackend()
    tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 1000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    mvccEngine = MVCCEngine(
      backend: mockBackend,
      timestampProvider: tsProvider,
      gcEnabled: false
    )

  test "scan empty range":
    let result = mvccEngine.mvccScan("key1", "key2", Timestamp(1000))
    check result.success == true
    check result.kvs.len == 0

  test "scan single key":
    addVersion(mockBackend, "key1", Timestamp(500), "value500")
    let result = mvccEngine.mvccScan("key1", "key2", Timestamp(1000))
    check result.success == true
    check result.kvs.len == 1
    check result.kvs[0].key.userKey == "key1"
    check result.kvs[0].value.data == "value500"

  test "scan multiple keys":
    addVersion(mockBackend, "key_a", Timestamp(500), "value_a")
    addVersion(mockBackend, "key_b", Timestamp(600), "value_b")
    addVersion(mockBackend, "key_c", Timestamp(700), "value_c")
    let result = mvccEngine.mvccScan("key_", "key_d", Timestamp(1000))
    check result.success == true
    check result.kvs.len == 3

  test "scan respects timestamp":
    addVersion(mockBackend, "key1", Timestamp(500), "value500")
    addVersion(mockBackend, "key1", Timestamp(1500), "value1500")
    let result = mvccEngine.mvccScan("key1", "key2", Timestamp(1000))
    check result.success == true
    check result.kvs.len == 1
    check result.kvs[0].value.data == "value500"

  test "scan skips intents":
    addVersion(mockBackend, "key1", Timestamp(500), "value500")
    addIntent(mockBackend, "key1", genTransactionID(), "intentValue", Timestamp(600))
    let result = mvccEngine.mvccScan("key1", "key2", Timestamp(1000))
    check result.success == true
    check result.kvs.len == 1
    check result.kvs[0].value.data == "value500"

  test "scan skips deleted versions":
    addVersion(mockBackend, "key1", Timestamp(500), "value500",
        isDeleted = true)
    addVersion(mockBackend, "key2", Timestamp(600), "value600")
    let result = mvccEngine.mvccScan("key1", "key3", Timestamp(1000))
    check result.success == true
    check result.kvs.len == 1
    check result.kvs[0].key.userKey == "key2"

  test "scan returns newest version per key":
    addVersion(mockBackend, "key1", Timestamp(300), "value300")
    addVersion(mockBackend, "key1", Timestamp(500), "value500")
    addVersion(mockBackend, "key1", Timestamp(700), "value700")
    let result = mvccEngine.mvccScan("key1", "key2", Timestamp(1000))
    check result.success == true
    check result.kvs.len == 1
    check result.kvs[0].value.data == "value700"

suite "MVCCEngine - Intent Operations":
  var mockTimer: sharedtimerMock.MockTimeProvider
  var mockBackend: MockMVCCBackend
  var tsProvider: TimestampProvider
  var mvccEngine: MVCCEngine

  setup:
    mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    mockBackend = newMockMVCCBackend()
    tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 1000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    mvccEngine = MVCCEngine(
      backend: mockBackend,
      timestampProvider: tsProvider,
      gcEnabled: false
    )

  test "resolve intent - commit":
    let txnId = genTransactionID()
    addIntent(mockBackend, "key1", txnId, "intentValue", Timestamp(1000))
    let result = mvccEngine.resolveIntent("key1", txnId, commit = true,
        commitTimestamp = Timestamp(1500))
    check result.success == true
    # Intent should be removed
    check mockBackend.exists(makeIntentKey("key1", txnId)) == false
    # Committed version should exist
    check mockBackend.exists(makeVersionKey("key1", Timestamp(1500)))

  test "resolve intent - abort":
    let txnId = genTransactionID()
    addIntent(mockBackend, "key1", txnId, "intentValue", Timestamp(1000))
    let result = mvccEngine.resolveIntent("key1", txnId, commit = false)
    check result.success == true
    # Intent should be removed
    check mockBackend.exists(makeIntentKey("key1", txnId)) == false
    # No committed version should exist
    check mockBackend.exists(makeVersionKey("key1", Timestamp(1500))) == false

  test "resolve intent fails if not found":
    let txnId = genTransactionID()
    let result = mvccEngine.resolveIntent("key1", txnId, commit = true)
    check result.success == false
    check result.error.code == mvccIntentNotFound

  test "cleanup intent":
    let txnId = genTransactionID()
    addIntent(mockBackend, "key1", txnId, "intentValue", Timestamp(1000))
    check mvccEngine.cleanupIntent("key1", txnId) == true
    check mockBackend.exists(makeIntentKey("key1", txnId)) == false

  test "get intent":
    let txnId = genTransactionID()
    addIntent(mockBackend, "key1", txnId, "intentValue", Timestamp(1000))
    let intent = mvccEngine.getIntent("key1", txnId)
    check intent.isSome
    check intent.get().data == "intentValue"

  test "get intent returns none if not found":
    let txnId = genTransactionID()
    let intent = mvccEngine.getIntent("key1", txnId)
    check intent.isNone

  test "has intent - true":
    let txnId = genTransactionID()
    addIntent(mockBackend, "key1", txnId, "intentValue", Timestamp(1000))
    check mvccEngine.hasIntent("key1") == true

  test "has intent - false":
    check mvccEngine.hasIntent("key1") == false

  test "get intents for key":
    let txnId1 = genTransactionID()
    let txnId2 = genTransactionID()
    addIntent(mockBackend, "key1", txnId1, "intent1", Timestamp(1000))
    addIntent(mockBackend, "key1", txnId2, "intent2", Timestamp(1100))
    let intents = mvccEngine.getIntentsForKey("key1")
    check intents.len == 2

suite "MVCCEngine - Version Operations":
  var mockTimer: sharedtimerMock.MockTimeProvider
  var mockBackend: MockMVCCBackend
  var tsProvider: TimestampProvider
  var mvccEngine: MVCCEngine

  setup:
    mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    mockBackend = newMockMVCCBackend()
    tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 1000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    mvccEngine = MVCCEngine(
      backend: mockBackend,
      timestampProvider: tsProvider,
      gcEnabled: false
    )

  test "get latest version":
    addVersion(mockBackend, "key1", Timestamp(500), "value500")
    addVersion(mockBackend, "key1", Timestamp(1000), "value1000")
    addVersion(mockBackend, "key1", Timestamp(1500), "value1500")
    let version = mvccEngine.getLatestVersion("key1")
    check version.isSome
    check version.get().data == "value1500"

  test "get latest version skips intents":
    addVersion(mockBackend, "key1", Timestamp(500), "value500")
    addIntent(mockBackend, "key1", genTransactionID(), "intentValue", Timestamp(600))
    let version = mvccEngine.getLatestVersion("key1")
    check version.isSome
    check version.get().data == "value500"

  test "get latest version returns none if not found":
    let version = mvccEngine.getLatestVersion("key1")
    check version.isNone

  test "get all versions":
    addVersion(mockBackend, "key1", Timestamp(500), "value500")
    addVersion(mockBackend, "key1", Timestamp(1000), "value1000")
    addVersion(mockBackend, "key1", Timestamp(1500), "value1500")
    let versions = mvccEngine.getAllVersions("key1")
    check versions.len == 3
    check versions[0].isLatest == true
    check versions[0].value.data == "value1500"

  test "get all versions empty key":
    let versions = mvccEngine.getAllVersions("key_nonexistent")
    check versions.len == 0

suite "MVCCEngine - Error Handling":
  var mockTimer: sharedtimerMock.MockTimeProvider
  var mockBackend: MockMVCCBackend
  var tsProvider: TimestampProvider
  var mvccEngine: MVCCEngine

  setup:
    mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    mockBackend = newMockMVCCBackend()
    tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 1000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    mvccEngine = MVCCEngine(
      backend: mockBackend,
      timestampProvider: tsProvider,
      gcEnabled: false
    )

  test "ok helper creates success result":
    let value = MVCCValue(data: "test", timestamp: 1000, isDeleted: false,
        txnId: InvalidTransactionID)
    let result = ok(some(value))
    check result.success == true
    check result.value.isSome

  test "err helper creates error result":
    let result = err(mvccKeyNotFound, "Key not found")
    check result.success == false
    check result.error.code == mvccKeyNotFound
    check "Key not found" in result.error.msg

  test "okScan helper creates success scan result":
    let kv = (
      key: MVCCKey(userKey: "key1", timestamp: Timestamp(1000),
          isIntent: false),
      value: MVCCValue(data: "value1", timestamp: 1000, isDeleted: false,
          txnId: InvalidTransactionID)
    )
    let kvs: seq[MVCCKeyValue] = @[kv]
    let result = okScan(kvs)
    check result.success == true
    check result.kvs.len == 1

  test "errScan helper creates error scan result":
    let result = errScan(mvccStorageError, "Storage error")
    check result.success == false
    check result.error.code == mvccStorageError

suite "MVCCEngine - Edge Cases":
  var mockTimer: sharedtimerMock.MockTimeProvider
  var mockBackend: MockMVCCBackend
  var tsProvider: TimestampProvider
  var mvccEngine: MVCCEngine

  setup:
    mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    mockBackend = newMockMVCCBackend()
    tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 1000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    mvccEngine = MVCCEngine(
      backend: mockBackend,
      timestampProvider: tsProvider,
      gcEnabled: false
    )

  test "scan with empty end key":
    addVersion(mockBackend, "key1", Timestamp(500), "value1")
    addVersion(mockBackend, "key2", Timestamp(600), "value2")
    addVersion(mockBackend, "key3", Timestamp(700), "value3")
    let result = mvccEngine.mvccScan("key1", "", Timestamp(1000))
    check result.success == true
    check result.kvs.len >= 1

  test "get with zero timestamp":
    addVersion(mockBackend, "key1", Timestamp(500), "value500")
    let result = mvccEngine.mvccGet("key1", Timestamp(0))
    check result.success == true
    check result.value.isNone

  test "get with max timestamp":
    addVersion(mockBackend, "key1", Timestamp(500), "value500")
    let result = mvccEngine.mvccGet("key1", mvccTypes.MAX_TIMESTAMP)
    check result.success == true
    check result.value.isSome

  test "put with empty value":
    let txn = newMVCCTransaction(tsProvider)
    let result = mvccEngine.mvccPut(txn, "key1", "")
    check result.success == true
    let intent = mvccEngine.getIntent("key1", txn.id)
    check intent.isSome
    check intent.get().data == ""

  test "resolve intent with delete flag":
    let txnId = genTransactionID()
    addIntent(mockBackend, "key1", txnId, "", Timestamp(1000), isDeleted = true)
    let result = mvccEngine.resolveIntent("key1", txnId, commit = true,
        commitTimestamp = Timestamp(1500))
    check result.success == true
    # Check committed version is marked deleted
    let committedKey = makeVersionKey("key1", Timestamp(1500))
    let committedValue = mockBackend.get(committedKey)
    check committedValue.isSome
    let decoded = decodeMVCCValue(committedValue.get())
    check decoded.isDeleted == true

suite "MVCCEngine - Backend Tracking":
  var mockTimer: sharedtimerMock.MockTimeProvider
  var mockBackend: MockMVCCBackend
  var tsProvider: TimestampProvider
  var mvccEngine: MVCCEngine

  setup:
    mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    mockBackend = newMockMVCCBackend()
    tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 1000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    mvccEngine = MVCCEngine(
      backend: mockBackend,
      timestampProvider: tsProvider,
      gcEnabled: false
    )

  test "backend operation counts":
    addVersion(mockBackend, "key1", Timestamp(500), "value1")
    addVersion(mockBackend, "key2", Timestamp(600), "value2")
    check mockBackend.putCount == 2

    # Note: mvccGet uses iterators, not direct get() calls
    discard mvccEngine.mvccGet("key1", Timestamp(1000))
    discard mvccEngine.mvccGet("key2", Timestamp(1000))
    # Iterator operations are used instead of get()
    check mockBackend.newIteratorCount >= 2

  test "backend writeBatch tracking":
    discard mockBackend.writeBatch(
      @[(key: "key1", value: "value1"), (key: "key2", value: "value2")],
      @[]
    )
    check mockBackend.writeBatchCount == 1
    check mockBackend.data.len == 2

  test "backend flush tracking":
    discard mockBackend.flush()
    check mockBackend.flushCount == 1

  test "backend open/close state":
    check mockBackend.isOpen() == true
    mockBackend.close()
    check mockBackend.isOpen() == false
    discard mockBackend.open(StorageConfig(path: "/tmp/test"))
    check mockBackend.isOpen() == true

suite "MVCCEngine - Key Encoding":
  test "make version key":
    let key = makeVersionKey("userKey", Timestamp(1000))
    check key.contains("userKey")
    check key.len > "userKey".len

  test "make intent key":
    let txnId = genTransactionID()
    let key = makeIntentKey("userKey", txnId)
    check key.contains("userKey")
    check key.len > "userKey".len

  test "encode/decode MVCC value":
    let original = MVCCValue(
      data: "testData",
      timestamp: 1500,
      isDeleted: false,
      txnId: genTransactionID()
    )
    let encoded = encodeMVCCValue(original.data, original.timestamp,
        original.isDeleted, original.txnId)
    let decoded = decodeMVCCValue(encoded)
    check decoded.data == original.data
    check decoded.timestamp == original.timestamp
    check decoded.isDeleted == original.isDeleted

  test "decode MVCC key":
    let versionKey = makeVersionKey("userKey", Timestamp(1000))
    let decoded = decodeMVCCKey(versionKey)
    check decoded.userKey == "userKey"
    check decoded.timestamp == Timestamp(1000)
    check decoded.isIntent == false

  test "decode intent key":
    let txnId = genTransactionID()
    let intentKey = makeIntentKey("userKey", txnId)
    let decoded = decodeIntentKey(intentKey)
    check decoded.userKey == "userKey"
    check decoded.txnId == txnId
