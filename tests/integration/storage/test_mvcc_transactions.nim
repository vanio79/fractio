import unittest
import std/[options, os]
import fractio/core/types
import fractio/core/transaction
import fractio/core/timestamp_provider
import fractio/core/transaction_manager
import fractio/core/conflict_detection
import fractio/storage/backend
import fractio/storage/mvcc/types
import fractio/storage/mvcc/engine
import fractio/storage/wisckey_backend
import fractio/distributed/sharedtimer/mock

# Constants
const
  DEFAULT_MAX_OFFSET_NS* = 100_000_000

suite "MVCC Transactions - Basic":
  test "create and open storage backend":
    let testPath = "/tmp/test_mvcc_basic_backend"
    removeDir(testPath)
    createDir(testPath)

    try:
      let backend = newWiscKeyBackend(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      check backend.open(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      # Basic put/get
      check backend.put("key1", "value1")
      let value = backend.get("key1")
      check value.isSome
      check value.get() == "value1"

      backend.close()
      discard backend.destroy()

    finally:
      removeDir(testPath)

  test "create MVCC engine":
    let testPath = "/tmp/test_mvcc_engine"
    removeDir(testPath)
    createDir(testPath)

    try:
      let backend = newWiscKeyBackend(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      check backend.open(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      let mockTimer = MockTimeProvider(currentTime: Timestamp(1000))
      let tsProvider = newTimestampProvider(mockTimer, nodeId = 0)

      let engine = newMVCCEngine(backend, tsProvider)
      check engine != nil
      check engine.backend != nil
      check engine.timestampProvider != nil

      backend.close()
      discard backend.destroy()

    finally:
      removeDir(testPath)

  test "create transaction manager":
    let testPath = "/tmp/test_mvcc_tm"
    removeDir(testPath)
    createDir(testPath)

    try:
      let backend = newWiscKeyBackend(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      check backend.open(StorageConfig(
        path: testPath,
        createIfMissing: true,
        syncWrites: false
      ))

      let mockTimer = MockTimeProvider(currentTime: Timestamp(1000))
      let tsProvider = newTimestampProvider(mockTimer, nodeId = 0)

      let engine = newMVCCEngine(backend, tsProvider)
      let tm = newTransactionManager(tsProvider, engine)

      check tm != nil
      check tm.timestampProvider != nil
      check tm.mvccEngine != nil

      backend.close()
      discard backend.destroy()

    finally:
      removeDir(testPath)

  test "create MVCC transaction":
    let mockTimer = MockTimeProvider(currentTime: Timestamp(1000))
    let tsProvider = newTimestampProvider(mockTimer, nodeId = 0)

    let txn = newMVCCTransaction(tsProvider)
    check txn != nil
    check txn.status == TXN_PENDING
    check txn.startTimestamp > Timestamp(0)

  test "MVCC key encoding":
    let key = "test_key"
    let ts: Timestamp = 12345

    let encoded = makeVersionKey(key, ts)
    check encoded.len > key.len

    let intentKey = makeIntentKey(key, genTransactionIDLocal())
    check intentKey.len > key.len

  test "MVCC value encoding":
    let value = "test_value"
    # Use a realistic nanosecond timestamp (year 2024)
    let ts: Timestamp = 1_700_000_000_000_000_000
    let txnId = genTransactionIDLocal()

    let encoded = encodeMVCCValue(value, ts, false, txnId)
    check encoded.len > value.len

    let decoded = decodeMVCCValue(encoded)
    check decoded.data == value
    check decoded.timestamp == ts
    check decoded.txnId == txnId
    check decoded.isDeleted == false

  test "transaction write set":
    let mockTimer = MockTimeProvider(currentTime: Timestamp(1000))
    let tsProvider = newTimestampProvider(mockTimer, nodeId = 0)

    var txn = newMVCCTransaction(tsProvider)

    # Add write
    txn.writeSet.entries.add(WriteEntry(key: "key1", value: "value1",
        isDelete: false))
    check txn.writeSet.entries.len == 1
    check txn.hasWrite("key1")

    # Add delete
    txn.writeSet.entries.add(WriteEntry(key: "key2", value: "", isDelete: true))
    check txn.writeSet.entries.len == 2
    check txn.hasWrite("key2")

  test "transaction read set":
    let mockTimer = MockTimeProvider(currentTime: Timestamp(1000))
    let tsProvider = newTimestampProvider(mockTimer, nodeId = 0)

    var txn = newMVCCTransaction(tsProvider)

    # Add read
    txn.readSet.keys.add("key1")
    txn.readSet.timestamps.add(Timestamp(100))
    check txn.hasRead("key1")
    check txn.getReadTimestamp("key1") == Timestamp(100)

  test "transaction status transitions":
    let mockTimer = MockTimeProvider(currentTime: Timestamp(1000))
    let tsProvider = newTimestampProvider(mockTimer, nodeId = 0)

    var txn = newMVCCTransaction(tsProvider)

    check txn.isPending()
    check txn.isActive()

    txn.status = TXN_COMMITTED
    check txn.isCommitted()
    check not txn.isActive()

    # Reset to pending for abort test
    txn.status = TXN_PENDING
    txn.status = TXN_ABORTED
    check txn.isAborted()
    check not txn.isActive()

suite "MVCC Transactions - Commit Result":
  test "commit success":
    let result = commitSuccess(Timestamp(1000))
    check result.success
    check result.commitTimestamp == Timestamp(1000)

  test "commit error":
    let result = commitError(ceWriteConflict, "test conflict", true)
    check not result.success
    check result.error.code == ceWriteConflict
    check result.error.retryable

  test "commit error non-retryable":
    let result = commitError(ceAborted, "aborted", false)
    check not result.success
    check not result.error.retryable

suite "MVCC Transactions - Conflict Detection":
  test "conflict statistics":
    var stats = newConflictStatistics()
    check stats.totalConflicts == 0

    stats.recordConflict(ctWriteWrite)
    check stats.writeWriteConflicts == 1
    check stats.totalConflicts == 1

    stats.recordConflict(ctReadWrite)
    check stats.readWriteConflicts == 1
    check stats.totalConflicts == 2

  test "conflict resolution tracking":
    var stats = newConflictStatistics()

    stats.recordResolution(crRetry)
    check stats.resolvedByRetry == 1

    stats.recordResolution(crAbort)
    check stats.resolvedByAbort == 1

suite "MVCC Transactions - Timestamp Provider":
  test "timestamp provider creation":
    let mockTimer = MockTimeProvider(currentTime: Timestamp(1000))
    let tsProvider = newTimestampProvider(mockTimer, nodeId = 1)

    check tsProvider != nil
    check tsProvider.nodeId == 1
