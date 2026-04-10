# Unit tests for Transaction Manager
# Migrated to use DI infrastructure where applicable
# Note: Uses inline MockStorageBackend for MVCCEngine compatibility (inherits from StorageBackend)

import unittest
import tables
import options
import fractio/core/transaction
import fractio/core/transaction_manager
import fractio/core/timestamp_provider
import fractio/core/types
import fractio/storage/mvcc/engine
import fractio/storage/mvcc/types
import fractio/storage/backend
import fractio/distributed/sharedtimer/mock as sharedtimerMock
import fractio/di/mocks as diMocks
import fractio/di/container
import fractio/app/bootstrap

# Inline mock for MVCCEngine compatibility (must inherit from StorageBackend)
type
  InlineMockStorageBackend = ref object of StorageBackend
    data: tables.Table[string, string]
    putCount: int
    getCount: int

proc newInlineMockStorageBackend(): InlineMockStorageBackend =
  new(result)
  result.data = initTable[string, string]()

method put(backend: InlineMockStorageBackend, key: string,
    value: string): bool =
  backend.data[key] = value
  backend.putCount += 1
  return true

method get(backend: InlineMockStorageBackend, key: string): Option[string] =
  backend.getCount += 1
  if key in backend.data:
    return some(backend.data[key])
  return none(string)

method delete(backend: InlineMockStorageBackend, key: string): bool =
  if key in backend.data:
    backend.data.del(key)
    return true
  return false

method exists(backend: InlineMockStorageBackend, key: string): bool =
  return key in backend.data

method newIterator(backend: InlineMockStorageBackend): StorageIterator =
  result = nil

suite "TransactionManager with DI Patterns":
  var mockTimer: sharedtimerMock.MockTimeProvider
  var mockBackend: InlineMockStorageBackend
  var tsProvider: TimestampProvider
  var mvccEngine: MVCCEngine
  var tm: TransactionManager

  setup:
    mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    mockBackend = newInlineMockStorageBackend()
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
    tm = newTransactionManager(tsProvider, mvccEngine)

  test "transaction manager creation":
    check tm.timestampProvider != nil
    check tm.mvccEngine != nil
    check tm.activeTransactions == 0
    check tm.committedCount == 0
    check tm.abortedCount == 0
    check tm.retryCount == 0
    check tm.maxRetries == DEFAULT_MAX_RETRIES

  test "transaction manager with custom options":
    let tm2 = newTransactionManager(tsProvider, mvccEngine,
      maxRetries = 10, defaultTimeoutMs = 5000)
    check tm2.maxRetries == 10
    check tm2.defaultTimeoutMs == 5000

  test "begin transaction":
    let txn = tm.beginTransaction()
    check txn != nil
    check txn.status == TXN_PENDING
    check tm.activeTransactions == 1

  test "end transaction":
    var txn = tm.beginTransaction()
    check tm.activeTransactions == 1
    tm.endTransaction(txn)
    check tm.activeTransactions == 0

  test "begin transaction with options":
    let options = newTransactionOptions(priority = 800, name = "test_txn")
    let txn = tm.beginTransaction(options)
    check txn != nil
    check txn.priority == 800
    check txn.name == "test_txn"
    check tm.activeTransactions == 1

  test "commit result helpers":
    let success = commitSuccess(Timestamp(1000))
    check success.success == true
    check success.commitTimestamp == Timestamp(1000)
    let fail = commitError(ceWriteConflict, "conflict", true)
    check fail.success == false
    check fail.error.code == ceWriteConflict
    check fail.error.retryable == true

  test "validate transaction state":
    var txn = tm.beginTransaction()
    check tm.validateTransactionState(txn) == true
    txn.status = TXN_COMMITTED
    check tm.validateTransactionState(txn) == false

  test "calculate backoff":
    check calculateBackoff(0) == 10
    check calculateBackoff(1) == 20
    check calculateBackoff(2) == 40
    check calculateBackoff(10) == 1000

  test "transaction statistics":
    tm.committedCount = 100
    tm.abortedCount = 10
    tm.retryCount = 25
    tm.activeTransactions = 5
    let stats = tm.getStatistics()
    check stats.committed == 100
    check stats.aborted == 10
    check stats.retries == 25
    check stats.active == 5
    check stats.successRate > 0.9

  test "reset statistics":
    tm.committedCount = 100
    tm.abortedCount = 10
    tm.retryCount = 25
    tm.resetStatistics()
    check tm.committedCount == 0
    check tm.abortedCount == 0
    check tm.retryCount == 0

  test "increment retries":
    check tm.retryCount == 0
    tm.incRetries()
    check tm.retryCount == 1
    tm.incRetries()
    tm.incRetries()
    check tm.retryCount == 3

suite "Transaction Lifecycle":
  var mockTimer: sharedtimerMock.MockTimeProvider
  var mockBackend: InlineMockStorageBackend
  var tsProvider: TimestampProvider
  var mvccEngine: MVCCEngine
  var tm: TransactionManager

  setup:
    mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    mockBackend = newInlineMockStorageBackend()
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
    tm = newTransactionManager(tsProvider, mvccEngine)

  test "full transaction lifecycle":
    let txn = tm.beginTransaction()
    check txn.status == TXN_PENDING
    txn.addWrite("key1", "value1", false)
    txn.addWrite("key2", "value2", false)
    txn.addRead("key3", Timestamp(500))
    txn.addRead("key4", Timestamp(600))
    check txn.getWriteCount() == 2
    check txn.getReadCount() == 2
    tm.endTransaction(txn)
    check tm.activeTransactions == 0

  test "transaction with write and read":
    let txn = tm.beginTransaction()
    txn.addWrite("key1", "value1", false)
    txn.addRead("key1", Timestamp(500))
    check txn.hasWrite("key1") == true
    check txn.hasRead("key1") == true
    check txn.getReadTimestamp("key1") == Timestamp(500)
    tm.endTransaction(txn)

  test "transaction retry tracking":
    var txn = tm.beginTransaction()
    check txn.epoch == 0
    check txn.canRetry(DEFAULT_MAX_RETRIES) == true
    txn.incrementEpoch()
    check txn.epoch == 1
    txn.resetForRetry(Timestamp(2000))
    check txn.epoch == 2
    check txn.status == TXN_PENDING
    tm.endTransaction(txn)

  test "advance time affects timestamps":
    mockTimer.currentTime = 2000_000_000
    let txn = tm.beginTransaction()
    let ts = tsProvider.now()
    check ts == 2000_000_000
    tm.endTransaction(txn)

suite "Error Handling":
  var mockTimer: sharedtimerMock.MockTimeProvider
  var mockBackend: InlineMockStorageBackend
  var tsProvider: TimestampProvider
  var mvccEngine: MVCCEngine
  var tm: TransactionManager

  setup:
    mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    mockBackend = newInlineMockStorageBackend()
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
    tm = newTransactionManager(tsProvider, mvccEngine)

  test "commit error types":
    let writeConflict = commitError(ceWriteConflict, "Write conflict", true)
    check writeConflict.error.code == ceWriteConflict
    check writeConflict.error.retryable == true
    let serialization = commitError(ceSerializationFailure,
        "Serialization failed", true)
    check serialization.error.code == ceSerializationFailure
    check serialization.error.retryable == true
    let timeout = commitError(ceTimeout, "Timeout exceeded", false)
    check timeout.error.code == ceTimeout
    check timeout.error.retryable == false
    let aborted = commitError(ceAborted, "Transaction aborted", false)
    check aborted.error.code == ceAborted
    check aborted.error.retryable == false

  test "error constructors":
    let abortErr = transactionAbortedError("User aborted")
    check abortErr.code == ceAborted
    let conflictErr = writeConflictError("key1")
    check conflictErr.code == ceWriteConflict
    check conflictErr.retryable == true
    let serialErr = serializationError("Read after write")
    check serialErr.code == ceSerializationFailure
    check serialErr.retryable == true

suite "Backend Operation Tracking":
  var mockTimer: sharedtimerMock.MockTimeProvider
  var mockBackend: InlineMockStorageBackend
  var tsProvider: TimestampProvider
  var mvccEngine: MVCCEngine
  var tm: TransactionManager

  setup:
    mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    mockBackend = newInlineMockStorageBackend()
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
    tm = newTransactionManager(tsProvider, mvccEngine)

  test "backend operation tracking":
    discard mockBackend.put("key1", "value1")
    discard mockBackend.put("key2", "value2")
    discard mockBackend.get("key1")
    check mockBackend.putCount == 2
    check mockBackend.getCount == 1

  test "backend can store and retrieve data":
    discard mockBackend.put("stored_key", "stored_value")
    let result = mockBackend.get("stored_key")
    check result.isSome
    check result.get() == "stored_value"

suite "DI MockBackend Independent Tests":
  # These tests demonstrate the DI MockBackend functionality
  # independent of the TransactionManager
  var mockBackend: diMocks.MockBackend

  setup:
    let container = createTestContainer()
    mockBackend = bootstrap.getMockBackend(container)

  test "DI mock backend put and get":
    discard mockBackend.put("key1", "value1")
    let result = mockBackend.get("key1")
    check result.isSome
    check result.get() == "value1"
    check mockBackend.putCallCount == 1
    check mockBackend.getCallCount == 1

  test "DI mock backend delete":
    discard mockBackend.put("key1", "value1")
    check mockBackend.delete("key1") == true
    let result = mockBackend.get("key1")
    check result.isNone

  test "DI mock backend scan":
    discard mockBackend.put("key_a", "value_a")
    discard mockBackend.put("key_b", "value_b")
    discard mockBackend.put("key_c", "value_c")
    let results = mockBackend.scan("key_", 10)
    check results.len == 3

  test "DI mock backend flush and compact":
    discard mockBackend.put("key1", "value1")
    discard mockBackend.put("key2", "value2")
    check mockBackend.flush() == true
    check mockBackend.compact() == true

  test "DI mock backend reset":
    discard mockBackend.put("key1", "value1")
    discard mockBackend.put("key2", "value2")
    check mockBackend.putCallCount == 2
    mockBackend.reset()
    check mockBackend.putCallCount == 0
    check mockBackend.getCallCount == 0
