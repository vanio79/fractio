# Unit tests for Transaction Manager

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

# Mock StorageBackend for testing
type
  MockStorageBackend* = ref object of StorageBackend
    data*: tables.Table[string, string]

proc newMockStorageBackend*(): MockStorageBackend =
  new(result)
  result.data = initTable[string, string]()

method put*(backend: MockStorageBackend, key: string,
    value: string): bool =
  backend.data[key] = value
  return true

method get*(backend: MockStorageBackend, key: string): Option[string] =
  if key in backend.data:
    return some(backend.data[key])
  return none(string)

method delete*(backend: MockStorageBackend, key: string): bool =
  if key in backend.data:
    backend.data.del(key)
    return true
  return false

method exists*(backend: MockStorageBackend, key: string): bool =
  return key in backend.data

method newIterator*(backend: MockStorageBackend): StorageIterator =
  # Return a mock iterator
  result = nil

suite "TransactionManager":
  setup:
    let mockBackend = newMockStorageBackend()
    let tsProvider = TimestampProvider(
      timer: nil, # Would use real timer in production
      lastTimestamp: 1000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    let mvccEngine = MVCCEngine(
      backend: mockBackend,
      timestampProvider: tsProvider,
      gcEnabled: false
    )
    var tm = newTransactionManager(tsProvider, mvccEngine)

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
    let options = newTransactionOptions(
      priority = 800,
      name = "test_txn"
    )
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
    let backoff1 = calculateBackoff(0)
    let backoff2 = calculateBackoff(1)
    let backoff3 = calculateBackoff(2)
    let backoff10 = calculateBackoff(10)

    check backoff1 == 10
    check backoff2 == 20
    check backoff3 == 40
    check backoff10 == 1000 # Max cap

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
    check stats.successRate > 0.9 # 100/110 ≈ 0.909

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
  setup:
    let mockBackend = newMockStorageBackend()
    let tsProvider = TimestampProvider(
      timer: nil,
      lastTimestamp: 1000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    let mvccEngine = MVCCEngine(
      backend: mockBackend,
      timestampProvider: tsProvider,
      gcEnabled: false
    )
    var tm = newTransactionManager(tsProvider, mvccEngine)

  test "full transaction lifecycle":
    # Begin
    let txn = tm.beginTransaction()
    check txn.status == TXN_PENDING

    # Add writes
    txn.addWrite("key1", "value1", false)
    txn.addWrite("key2", "value2", false)

    # Add reads
    txn.addRead("key3", Timestamp(500))
    txn.addRead("key4", Timestamp(600))

    check txn.getWriteCount() == 2
    check txn.getReadCount() == 2

    # End
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
    check txn.epoch == 1 # Incremented again in resetForRetry
    check txn.status == TXN_PENDING

    tm.endTransaction(txn)

suite "Error Handling":
  setup:
    let mockBackend = newMockStorageBackend()
    let tsProvider = TimestampProvider(
      timer: nil,
      lastTimestamp: 1000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    let mvccEngine = MVCCEngine(
      backend: mockBackend,
      timestampProvider: tsProvider,
      gcEnabled: false
    )
    var tm = newTransactionManager(tsProvider, mvccEngine)

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
