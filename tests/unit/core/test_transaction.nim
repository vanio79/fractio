# Unit tests for MVCC Transactions

import unittest
import fractio/core/transaction
import fractio/core/timestamp_provider
import fractio/core/types
import fractio/storage/mvcc/types

# Use MVCCTransactionStatus from mvcc/types
const
  TXN_PENDING* = MVCCTransactionStatus.TXN_PENDING
  TXN_PREPARED* = MVCCTransactionStatus.TXN_PREPARED
  TXN_COMMITTED* = MVCCTransactionStatus.TXN_COMMITTED
  TXN_ABORTED* = MVCCTransactionStatus.TXN_ABORTED
  INVALID_TIMESTAMP* = Timestamp(0)
  MAX_TIMESTAMP* = high(Timestamp)
  DEFAULT_PRIORITY* = 500
  DEFAULT_MAX_RETRIES* = 15

suite "MVCCTransaction":
  setup:
    # Create a mock timestamp provider for testing
    var mockTime: int64 = 1000

  test "transaction creation":
    let txn = MVCCTransaction(
      id: TransactionID(1),
      status: TXN_PENDING,
      startTimestamp: Timestamp(1000),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(1000),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    check txn.id == TransactionID(1)
    check txn.status == TXN_PENDING
    check txn.startTimestamp == Timestamp(1000)
    check txn.priority == DEFAULT_PRIORITY
    check txn.epoch == 0

  test "add write entry":
    var txn = MVCCTransaction(
      id: TransactionID(1),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    txn.addWrite("key1", "value1", false)
    txn.addWrite("key2", "", true) # delete

    check txn.writeSet.entries.len == 2
    check txn.writeSet.entries[0].key == "key1"
    check txn.writeSet.entries[0].isDelete == false
    check txn.writeSet.entries[1].isDelete == true

  test "add read entry":
    var txn = MVCCTransaction(
      id: TransactionID(1),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    txn.addRead("key1", Timestamp(50))
    txn.addRead("key2", Timestamp(75))

    check txn.readSet.keys.len == 2
    check txn.getReadTimestamp("key1") == Timestamp(50)
    check txn.getReadTimestamp("key2") == Timestamp(75)
    check txn.getReadTimestamp("nonexistent") == INVALID_TIMESTAMP

  test "transaction state checks":
    var txn = MVCCTransaction(
      id: TransactionID(1),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    check txn.isPending() == true
    check txn.isCommitted() == false
    check txn.isAborted() == false
    check txn.isActive() == true

    txn.status = TXN_COMMITTED
    check txn.isPending() == false
    check txn.isCommitted() == true
    check txn.isActive() == false

    txn.status = TXN_ABORTED
    check txn.isAborted() == true
    check txn.isActive() == false

  test "has write and has read":
    var txn = MVCCTransaction(
      id: TransactionID(1),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    check txn.hasWrite("key1") == false
    check txn.hasRead("key1") == false

    txn.addWrite("key1", "value1", false)
    txn.addRead("key2", Timestamp(50))

    check txn.hasWrite("key1") == true
    check txn.hasRead("key2") == true
    check txn.hasWrite("key2") == false

  test "get write and read counts":
    var txn = MVCCTransaction(
      id: TransactionID(1),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    check txn.getWriteCount() == 0
    check txn.getReadCount() == 0

    txn.addWrite("key1", "value1", false)
    txn.addWrite("key2", "value2", false)
    txn.addRead("key3", Timestamp(50))
    txn.addRead("key4", Timestamp(75))

    check txn.getWriteCount() == 2
    check txn.getReadCount() == 2

  test "clear read set":
    var txn = MVCCTransaction(
      id: TransactionID(1),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    txn.addRead("key1", Timestamp(50))
    txn.addRead("key2", Timestamp(75))

    check txn.getReadCount() == 2

    txn.clearReadSet()

    check txn.getReadCount() == 0
    check txn.readSet.keys.len == 0

  test "transaction epoch and retry tracking":
    var txn = MVCCTransaction(
      id: TransactionID(1),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    check txn.epoch == 0
    check txn.canRetry(DEFAULT_MAX_RETRIES) == true

    txn.incrementEpoch()

    check txn.epoch == 1
    check txn.canRetry(DEFAULT_MAX_RETRIES) == true

    # Simulate max retries
    txn.epoch = DEFAULT_MAX_RETRIES

    check txn.canRetry(DEFAULT_MAX_RETRIES) == false

  test "reset for retry":
    var txn = MVCCTransaction(
      id: TransactionID(1),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    txn.addWrite("key1", "value1", false)
    txn.addRead("key2", Timestamp(50))
    txn.status = TXN_ABORTED

    check txn.getWriteCount() == 1
    check txn.getReadCount() == 1
    check txn.status == TXN_ABORTED

    txn.resetForRetry(Timestamp(200))

    check txn.status == TXN_PENDING
    check txn.startTimestamp == Timestamp(200)
    check txn.commitTimestamp == INVALID_TIMESTAMP
    check txn.getWriteCount() == 0
    check txn.getReadCount() == 0
    check txn.epoch == 1

  test "deadline checking":
    var txn = MVCCTransaction(
      id: TransactionID(1),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: Timestamp(1000),
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    # Before deadline
    check txn.checkDeadline(Timestamp(500)) == false
    check txn.isExpired(Timestamp(500)) == false

    # At deadline
    check txn.checkDeadline(Timestamp(1000)) == false

    # After deadline
    check txn.checkDeadline(Timestamp(1500)) == true
    check txn.isExpired(Timestamp(1500)) == true

    # No deadline set
    txn.deadline = MAX_TIMESTAMP
    check txn.checkDeadline(Timestamp(999999)) == false

suite "TransactionOptions":
  test "default options":
    let options = newTransactionOptions()

    check options.priority == DEFAULT_PRIORITY
    check options.name == ""
    check options.isolationLevel == ilSerializable
    check options.maxRetries == DEFAULT_MAX_RETRIES

  test "custom options":
    let options = newTransactionOptions(
      priority = 800,
      timeoutMs = 5000,
      name = "test_txn",
      maxRetries = 5
    )

    check options.priority == 800
    check options.name == "test_txn"
    check options.isolationLevel == ilSerializable
    check options.maxRetries == 5

suite "CommitResult":
  test "success result":
    let result = commitSuccess(Timestamp(1000))

    check result.success == true
    check result.commitTimestamp == Timestamp(1000)

  test "error result":
    let result = commitError(ceWriteConflict, "conflict", true)

    check result.success == false
    check result.error.code == ceWriteConflict
    check result.error.retryable == true
    check result.error.msg == "conflict"

  test "error constructors":
    let abortErr = transactionAbortedError("test")
    check abortErr.code == ceAborted
    check abortErr.retryable == false

    let conflictErr = writeConflictError("key1")
    check conflictErr.code == ceWriteConflict
    check conflictErr.retryable == true

    let serialErr = serializationError("test failure")
    check serialErr.code == ceSerializationFailure
    check serialErr.retryable == true
