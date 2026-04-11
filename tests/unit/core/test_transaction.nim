# Unit tests for MVCC Transactions

import unittest
import std/[sets, hashes]
import fractio/core/transaction
import fractio/core/timestamp_provider
import fractio/core/types
import fractio/storage/mvcc/types
import fractio/distributed/sharedtimer/mock as sharedtimerMock

const
  TXN_PENDING* = MVCCTransactionStatus.TXN_PENDING
  TXN_PREPARED* = MVCCTransactionStatus.TXN_PREPARED
  TXN_COMMITTED* = MVCCTransactionStatus.TXN_COMMITTED
  TXN_ABORTED* = MVCCTransactionStatus.TXN_ABORTED
  INVALID_TIMESTAMP* = Timestamp(0)
  MAX_TIMESTAMP* = high(Timestamp)
  DEFAULT_PRIORITY* = 500
  DEFAULT_MAX_RETRIES* = 15

suite "TransactionID Generation":
  test "genTransactionID creates unique IDs":
    var seen: HashSet[TransactionID] = initHashSet[TransactionID]()
    for i in 0 ..< 100:
      let id = genTransactionID()
      check id notin seen
      seen.incl(id)

  test "TransactionID equality":
    let a = genTransactionID()
    let b = genTransactionID()
    check a == a
    check a != b

  test "TransactionID ordering":
    let zero = zeroTransactionID()
    let id = genTransactionID()
    check zero < id

  test "TransactionID string representation":
    let id = genTransactionID()
    let s = $id
    check s.len == 26

  test "TransactionID hash consistency":
    let a = genTransactionID()
    check hash(a) == hash(a)

  test "TransactionID bytes roundtrip":
    let original = genTransactionID()
    let bytes = transactionIDToBytes(original)
    check bytes.len == 16
    let restored = transactionIDFromBytes(bytes)
    check restored == original

  test "TransactionID string roundtrip":
    let original = genTransactionID()
    let s = $original
    let restored = transactionIDFromString(s)
    check restored == original

  test "isZero check":
    check isZero(zeroTransactionID())
    check not isZero(genTransactionID())

suite "MVCCTransaction Creation and Initialization":
  test "basic transaction creation with timestamp provider":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 10000_000_000)
    let tsProvider = newTimestampProvider(mockTimer)

    let txn = newMVCCTransaction(tsProvider)

    check txn.id != zeroTransactionID()
    check txn.status == TXN_PENDING
    check txn.startTimestamp > Timestamp(0)
    check txn.commitTimestamp == INVALID_TIMESTAMP
    check txn.priority == DEFAULT_PRIORITY
    check txn.maxTimestamp == MAX_TIMESTAMP
    check txn.deadline == MAX_TIMESTAMP
    check txn.createdAt == txn.startTimestamp
    check txn.writeSet.entries.len == 0
    check txn.readSet.keys.len == 0
    check txn.readSet.timestamps.len == 0
    check txn.lockedKeys == 0
    check txn.epoch == 0

  test "transaction creation with custom priority":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 10000_000_000)
    let tsProvider = newTimestampProvider(mockTimer)

    let txn = newMVCCTransaction(tsProvider, priority = 800)

    check txn.priority == 800
    check txn.status == TXN_PENDING

  test "transaction creation with TransactionOptions":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 10000_000_000)
    let tsProvider = newTimestampProvider(mockTimer)

    let options = newTransactionOptions(
      priority = 750,
      timeoutMs = 5000,
      name = "test_transaction",
      maxRetries = 10
    )

    let txn = newMVCCTransaction(tsProvider, options)

    check txn.priority == 750
    check txn.name == "test_transaction"
    check txn.status == TXN_PENDING
    check txn.startTimestamp > Timestamp(0)

  test "transaction creation with minimal options":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 10000_000_000)
    let tsProvider = newTimestampProvider(mockTimer)

    let options = TransactionOptions(
      priority: 100,
      deadline: Timestamp(50000),
      name: "minimal_txn",
      isolationLevel: ilSerializable,
      maxRetries: 3
    )

    let txn = newMVCCTransaction(tsProvider, options)

    check txn.priority == 100
    check txn.deadline == Timestamp(50000)
    check txn.name == "minimal_txn"
    check txn.epoch == 0
    check txn.lockedKeys == 0

  test "manual transaction construction":
    let txnId = genTransactionID()
    let txn = MVCCTransaction(
      id: txnId,
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

    check txn.id == txnId
    check txn.status == TXN_PENDING
    check txn.startTimestamp == Timestamp(1000)
    check txn.priority == DEFAULT_PRIORITY
    check txn.epoch == 0

suite "MVCCTransaction Status Changes":
  test "pending to committed transition":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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
    txn.commitTimestamp = Timestamp(200)

    check txn.isPending() == false
    check txn.isCommitted() == true
    check txn.isAborted() == false
    check txn.isActive() == false
    check txn.commitTimestamp == Timestamp(200)

  test "pending to aborted transition":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    txn.status = TXN_ABORTED

    check txn.isPending() == false
    check txn.isCommitted() == false
    check txn.isAborted() == true
    check txn.isActive() == false

  test "pending to prepared transition":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    txn.status = TXN_PREPARED

    check txn.isPending() == false
    check txn.isCommitted() == false
    check txn.isAborted() == false
    check txn.isActive() == true

  test "all status state checks":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    txn.status = TXN_PREPARED
    check txn.isActive() == true
    check txn.isPending() == false
    check txn.isCommitted() == false
    check txn.isAborted() == false

suite "MVCCTransaction ReadSet Operations":
  test "add single read entry":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    check txn.readSet.keys.len == 1
    check txn.readSet.timestamps.len == 1
    check txn.readSet.keys[0] == "key1"
    check txn.readSet.timestamps[0] == Timestamp(50)

  test "add multiple read entries":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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
    txn.addRead("key3", Timestamp(90))

    check txn.readSet.keys.len == 3
    check txn.readSet.timestamps.len == 3
    check txn.getReadTimestamp("key1") == Timestamp(50)
    check txn.getReadTimestamp("key2") == Timestamp(75)
    check txn.getReadTimestamp("key3") == Timestamp(90)

  test "getReadTimestamp for nonexistent key":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    check txn.getReadTimestamp("nonexistent") == INVALID_TIMESTAMP

  test "clear read set":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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
    check txn.readSet.timestamps.len == 0

  test "hasRead check":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    check txn.hasRead("key1") == false

    txn.addRead("key1", Timestamp(50))
    txn.addRead("key2", Timestamp(75))

    check txn.hasRead("key1") == true
    check txn.hasRead("key2") == true
    check txn.hasRead("key3") == false

  test "getReadCount":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    check txn.getReadCount() == 0

    txn.addRead("key1", Timestamp(50))
    check txn.getReadCount() == 1

    txn.addRead("key2", Timestamp(75))
    txn.addRead("key3", Timestamp(90))
    check txn.getReadCount() == 3

suite "MVCCTransaction WriteSet Operations":
  test "add single write entry":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    check txn.writeSet.entries.len == 1
    check txn.writeSet.entries[0].key == "key1"
    check txn.writeSet.entries[0].value == "value1"
    check txn.writeSet.entries[0].isDelete == false

  test "add write entry as delete":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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
    txn.addWrite("key2", "", true)

    check txn.writeSet.entries.len == 2
    check txn.writeSet.entries[0].key == "key1"
    check txn.writeSet.entries[0].isDelete == false
    check txn.writeSet.entries[1].key == "key2"
    check txn.writeSet.entries[1].value == ""
    check txn.writeSet.entries[1].isDelete == true

  test "add multiple write entries":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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
    txn.addWrite("key2", "value2", false)
    txn.addWrite("key3", "value3", false)

    check txn.writeSet.entries.len == 3
    check txn.getWriteCount() == 3

  test "hasWrite check":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    txn.addWrite("key1", "value1", false)
    txn.addWrite("key2", "value2", false)

    check txn.hasWrite("key1") == true
    check txn.hasWrite("key2") == true
    check txn.hasWrite("key3") == false

  test "getWriteCount":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    txn.addWrite("key1", "value1", false)
    check txn.getWriteCount() == 1

    txn.addWrite("key2", "value2", false)
    check txn.getWriteCount() == 2

suite "MVCCTransaction Epoch and Retry":
  test "initial epoch is zero":
    let txnId = genTransactionID()
    let txn = MVCCTransaction(
      id: txnId,
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

  test "incrementEpoch increases epoch":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    txn.incrementEpoch()
    check txn.epoch == 1

    txn.incrementEpoch()
    check txn.epoch == 2

    txn.incrementEpoch()
    check txn.epoch == 3

  test "canRetry with default max retries":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    check txn.canRetry(DEFAULT_MAX_RETRIES) == true

    for i in 1 ..< DEFAULT_MAX_RETRIES:
      txn.incrementEpoch()
      check txn.canRetry(DEFAULT_MAX_RETRIES) == true

    txn.epoch = DEFAULT_MAX_RETRIES
    check txn.canRetry(DEFAULT_MAX_RETRIES) == false

  test "canRetry with custom max retries":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    check txn.canRetry(5) == true

    txn.epoch = 4
    check txn.canRetry(5) == true

    txn.epoch = 5
    check txn.canRetry(5) == false

  test "resetForRetry clears state and increments epoch":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
      status: TXN_ABORTED,
      startTimestamp: Timestamp(100),
      commitTimestamp: Timestamp(150),
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[WriteEntry(key: "key1", value: "value1",
          isDelete: false)]),
      readSet: ReadSet(keys: @["key2"], timestamps: @[Timestamp(50)]),
      lockedKeys: 0,
      epoch: 2
    )

    check txn.status == TXN_ABORTED
    check txn.getWriteCount() == 1
    check txn.getReadCount() == 1
    check txn.epoch == 2
    check txn.commitTimestamp == Timestamp(150)

    txn.resetForRetry(Timestamp(200))

    check txn.status == TXN_PENDING
    check txn.startTimestamp == Timestamp(200)
    check txn.commitTimestamp == INVALID_TIMESTAMP
    check txn.getWriteCount() == 0
    check txn.getReadCount() == 0
    check txn.epoch == 3

suite "MVCCTransaction Deadline Checking":
  test "checkDeadline before deadline":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    check txn.checkDeadline(Timestamp(500)) == false
    check txn.isExpired(Timestamp(500)) == false

  test "checkDeadline at deadline boundary":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    check txn.checkDeadline(Timestamp(1000)) == false

  test "checkDeadline after deadline":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    check txn.checkDeadline(Timestamp(1001)) == true
    check txn.checkDeadline(Timestamp(1500)) == true
    check txn.isExpired(Timestamp(1500)) == true

  test "checkDeadline with no deadline set":
    let txnId = genTransactionID()
    var txn = MVCCTransaction(
      id: txnId,
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

    check txn.checkDeadline(Timestamp(999999)) == false
    check txn.checkDeadline(MAX_TIMESTAMP) == false

suite "TransactionOptions":
  test "default options creation":
    let options = newTransactionOptions()

    check options.priority == DEFAULT_PRIORITY
    check options.name == ""
    check options.isolationLevel == ilSerializable
    check options.maxRetries == DEFAULT_MAX_RETRIES

  test "options with all custom values":
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

  test "options with priority only":
    let options = newTransactionOptions(priority = 100)

    check options.priority == 100
    check options.name == ""
    check options.maxRetries == DEFAULT_MAX_RETRIES

  test "options with max retries only":
    let options = newTransactionOptions(maxRetries = 3)

    check options.priority == DEFAULT_PRIORITY
    check options.maxRetries == 3

  test "options timeoutMs zero sets deadline to MAX_TIMESTAMP":
    let options = newTransactionOptions(timeoutMs = 0)

    check options.deadline == MAX_TIMESTAMP

  test "options timeoutMs positive sets deadline":
    let options = newTransactionOptions(timeoutMs = 10000)

    check options.deadline == Timestamp(0)

  test "IsolationLevel enum values":
    check ilSerializable.ord == 0
    check ilReadCommitted.ord == 1
    check ilRepeatableRead.ord == 2

suite "CommitResult":
  test "commitSuccess creates successful result":
    let result = commitSuccess(Timestamp(1000))

    check result.success == true
    check result.commitTimestamp == Timestamp(1000)

  test "commitError creates failed result":
    let result = commitError(ceWriteConflict, "conflict on key1", true)

    check result.success == false
    check result.commitTimestamp == INVALID_TIMESTAMP
    check result.error.code == ceWriteConflict
    check result.error.retryable == true
    check result.error.msg == "conflict on key1"

  test "commitError non-retryable":
    let result = commitError(ceAborted, "transaction aborted", false)

    check result.success == false
    check result.error.code == ceAborted
    check result.error.retryable == false

  test "CommitErrorCode enum values":
    check ceWriteConflict.ord == 0
    check ceReadSnapshotError.ord == 1
    check ceSerializationFailure.ord == 2
    check ceTimeout.ord == 3
    check ceAborted.ord == 4
    check ceInvalidState.ord == 5

suite "TransactionCommitError Constructors":
  test "transactionAbortedError":
    let err = transactionAbortedError("txn was aborted")

    check err.code == ceAborted
    check err.retryable == false
    check err.msg == "txn was aborted"

  test "writeConflictError":
    let err = writeConflictError("key123")

    check err.code == ceWriteConflict
    check err.retryable == true
    check err.msg == "Write conflict on key: key123"

  test "serializationError":
    let err = serializationError("could not serialize")

    check err.code == ceSerializationFailure
    check err.retryable == true
    check err.msg == "Serialization failure: could not serialize"

suite "Core Transaction Type (types.nim)":
  test "Transaction creation":
    let txnId = genTransactionID()
    let txn = Transaction(
      id: txnId,
      timestamp: 1000'i64,
      status: tsActive,
      readSnapshot: 500'i64,
      mutatedTables: initHashSet[string]()
    )

    check txn.id == txnId
    check txn.timestamp == 1000
    check txn.status == tsActive
    check txn.readSnapshot == 500

  test "Transaction mutatedTables tracking":
    let txnId = genTransactionID()
    var txn = Transaction(
      id: txnId,
      timestamp: 1000'i64,
      status: tsActive,
      readSnapshot: 500'i64,
      mutatedTables: initHashSet[string]()
    )

    check txn.mutatedTables.len == 0

    txn.mutatedTables.incl("table1")
    check txn.mutatedTables.len == 1
    check "table1" in txn.mutatedTables

    txn.mutatedTables.incl("table2")
    txn.mutatedTables.incl("table3")
    check txn.mutatedTables.len == 3
    check "table2" in txn.mutatedTables
    check "table3" in txn.mutatedTables

  test "Transaction readSnapshot handling":
    let txnId = genTransactionID()
    let txn = Transaction(
      id: txnId,
      timestamp: 1000'i64,
      status: tsActive,
      readSnapshot: 750'i64,
      mutatedTables: initHashSet[string]()
    )

    check txn.readSnapshot == 750
    check txn.readSnapshot < txn.timestamp

  test "Transaction status transitions":
    let txnId = genTransactionID()
    var txn = Transaction(
      id: txnId,
      timestamp: 1000'i64,
      status: tsActive,
      readSnapshot: 500'i64,
      mutatedTables: initHashSet[string]()
    )

    check txn.status == tsActive

    txn.status = tsCommitted
    check txn.status == tsCommitted

    txn.status = tsAborted
    check txn.status == tsAborted

  test "TransactionStatus enum values":
    check tsActive.ord == 0
    check tsCommitted.ord == 1
    check tsAborted.ord == 2

suite "Constants and Priority":
  test "INVALID_TIMESTAMP constant":
    check INVALID_TIMESTAMP == Timestamp(0)

  test "MAX_TIMESTAMP constant":
    check MAX_TIMESTAMP == high(Timestamp)

  test "priority constants":
    check MAX_PRIORITY == 1000
    check DEFAULT_PRIORITY == 500
    check MIN_PRIORITY == 1

  test "DEFAULT_MAX_RETRIES constant":
    check DEFAULT_MAX_RETRIES == 15

  test "DEFAULT_TIMEOUT_MS constant":
    check DEFAULT_TIMEOUT_MS == 10_000

suite "WriteEntry Type":
  test "WriteEntry construction":
    let entry = WriteEntry(key: "test_key", value: "test_value",
        isDelete: false)

    check entry.key == "test_key"
    check entry.value == "test_value"
    check entry.isDelete == false

  test "WriteEntry for delete":
    let entry = WriteEntry(key: "delete_key", value: "", isDelete: true)

    check entry.key == "delete_key"
    check entry.value == ""
    check entry.isDelete == true

suite "ReadSet Type":
  test "ReadSet construction":
    let readSet = ReadSet(keys: @["key1", "key2"], timestamps: @[Timestamp(100),
        Timestamp(200)])

    check readSet.keys.len == 2
    check readSet.timestamps.len == 2
    check readSet.keys[0] == "key1"
    check readSet.timestamps[0] == Timestamp(100)

  test "ReadSet empty":
    let readSet = ReadSet(keys: @[], timestamps: @[])

    check readSet.keys.len == 0
    check readSet.timestamps.len == 0

suite "WriteSet Type":
  test "WriteSet construction":
    let writeSet = WriteSet(entries: @[
      WriteEntry(key: "key1", value: "value1", isDelete: false),
      WriteEntry(key: "key2", value: "value2", isDelete: true)
    ])

    check writeSet.entries.len == 2
    check writeSet.entries[0].key == "key1"
    check writeSet.entries[1].isDelete == true

  test "WriteSet empty":
    let writeSet = WriteSet(entries: @[])

    check writeSet.entries.len == 0
