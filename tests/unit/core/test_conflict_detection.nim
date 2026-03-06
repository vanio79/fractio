# Unit tests for Conflict Detection and Resolution

import unittest
import tables
import options
import fractio/core/transaction
import fractio/core/timestamp_provider
import fractio/core/conflict_detection
import fractio/core/types
import fractio/storage/mvcc/engine
import fractio/storage/mvcc/types
import fractio/storage/backend

# Constants for testing
const
  INVALID_TIMESTAMP* = Timestamp(0)
  MAX_TIMESTAMP* = high(Timestamp)
  DEFAULT_MAX_OFFSET_NS* = 1_000_000'i64
  DEFAULT_PRIORITY* = 500
  DEFAULT_MAX_RETRIES* = 15

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

suite "Conflict Detection":
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

  test "conflict info creation":
    let conflict = ConflictInfo(
      conflictType: ctWriteWrite,
      key: "test_key",
      conflictingTxnId: TransactionID(123),
      timestamp: Timestamp(1000),
      retryable: true
    )

    check conflict.conflictType == ctWriteWrite
    check conflict.key == "test_key"
    check conflict.retryable == true

  test "conflict statistics":
    var stats = newConflictStatistics()

    stats.recordConflict(ctWriteWrite)
    stats.recordConflict(ctWriteRead)
    stats.recordConflict(ctWriteWrite)

    check stats.totalConflicts == 3
    check stats.writeWriteConflicts == 2
    check stats.writeReadConflicts == 1

    stats.recordResolution(crRetry)
    stats.recordResolution(crWait)

    check stats.resolvedByRetry == 1
    check stats.resolvedByWait == 1

    let conflictRate = stats.getConflictRate(100)
    check conflictRate == 0.03 # 3/100

    let retryRate = stats.getRetryRate()
    check retryRate == 0.5 # 1/2

  test "conflict rate calculation":
    var stats = newConflictStatistics()

    # No transactions
    check stats.getConflictRate(0) == 0.0

    # Some conflicts
    stats.recordConflict(ctWriteWrite)
    stats.recordConflict(ctWriteRead)

    check stats.getConflictRate(10) == 0.2 # 2/10

  test "retry rate calculation":
    var stats = newConflictStatistics()

    # No resolutions
    check stats.getRetryRate() == 0.0

    # All retries
    stats.recordResolution(crRetry)
    stats.recordResolution(crRetry)
    stats.recordResolution(crRetry)

    check stats.getRetryRate() == 1.0 # 3/3

    # Mixed resolutions
    stats.recordResolution(crWait)
    stats.recordResolution(crPush)

    check stats.getRetryRate() == 0.5 # 3/6

  test "detect write-write conflict - no conflict":
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

    let conflict = mvccEngine.detectWriteWriteConflict(txn, "key1")

    check conflict.isNone() == true

  test "detect write-write conflict - with intent":
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

    # Write an intent from another transaction
    let intentKey = makeIntentKey("key1", TransactionID(2))
    let intentValue = encodeMVCCValue("value2", Timestamp(150), false,
        TransactionID(2))
    discard mockBackend.put(intentKey, intentValue)

    let conflict = mvccEngine.detectWriteWriteConflict(txn, "key1")

    check conflict.isSome() == true
    check conflict.get().conflictType == ctWriteWrite
    check conflict.get().conflictingTxnId == TransactionID(2)

  test "detect write-read conflict":
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

    # No conflict initially
    let conflict = mvccEngine.detectWriteReadConflict(txn, "key1")
    check conflict.isNone() == true

    # Add a committed version after our write
    let committedKey = makeVersionKey("key1", Timestamp(150))
    let committedValue = encodeMVCCValue("value2", Timestamp(150), false,
        TransactionID(2))
    discard mockBackend.put(committedKey, committedValue)

    let conflict2 = mvccEngine.detectWriteReadConflict(txn, "key1")
    check conflict2.isSome() == true
    check conflict2.get().conflictType == ctWriteRead

  test "detect read-write conflict":
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

    txn.addRead("key1", Timestamp(100))

    # No conflict initially
    let conflict = mvccEngine.detectReadWriteConflict(txn, "key1")
    check conflict.isNone() == true

    # Add an intent from another transaction
    let intentKey = makeIntentKey("key1", TransactionID(2))
    let intentValue = encodeMVCCValue("value2", Timestamp(150), false,
        TransactionID(2))
    discard mockBackend.put(intentKey, intentValue)

    let conflict2 = mvccEngine.detectReadWriteConflict(txn, "key1")
    check conflict2.isSome() == true
    check conflict2.get().conflictType == ctReadWrite

  test "detect all conflicts":
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
    txn.addWrite("key2", "value2", false)
    txn.addRead("key3", Timestamp(100))
    txn.addRead("key4", Timestamp(100))

    # Add some conflicts
    let intentKey = makeIntentKey("key1", TransactionID(2))
    let intentValue = encodeMVCCValue("value2", Timestamp(150), false,
        TransactionID(2))
    discard mockBackend.put(intentKey, intentValue)

    let conflicts = mvccEngine.detectAllConflicts(txn)

    check conflicts.len >= 1 # At least the intent conflict

suite "Conflict Resolution":
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
    var resolver = newConflictResolver(mvccEngine)

  test "conflict resolver creation":
    check resolver != nil
    check resolver.engine == mvccEngine
    check resolver.enablePriority == true
    check resolver.maxWaitTimeMs == 10_000

  test "conflict resolver with options":
    let resolver2 = newConflictResolver(mvccEngine,
      enablePriority = false,
      maxWaitTimeMs = 5000)

    check resolver2.enablePriority == false
    check resolver2.maxWaitTimeMs == 5000

  test "resolve conflict with committed transaction":
    var txn1 = MVCCTransaction(
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

    var txn2 = MVCCTransaction(
      id: TransactionID(2),
      status: TXN_COMMITTED,
      startTimestamp: Timestamp(50),
      commitTimestamp: Timestamp(200),
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(50),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let conflict = ConflictInfo(
      conflictType: ctWriteWrite,
      key: "key1",
      conflictingTxnId: TransactionID(2),
      timestamp: Timestamp(200),
      retryable: true
    )

    let result = resolver.resolveConflict(txn1, conflict, txn2)

    check result.resolution == crRetry
    check result.newTimestamp == Timestamp(201)
    check result.shouldAbort == false

  test "resolve conflict with aborted transaction":
    var txn1 = MVCCTransaction(
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

    var txn2 = MVCCTransaction(
      id: TransactionID(2),
      status: TXN_ABORTED,
      startTimestamp: Timestamp(50),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(50),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let conflict = ConflictInfo(
      conflictType: ctWriteWrite,
      key: "key1",
      conflictingTxnId: TransactionID(2),
      timestamp: Timestamp(150),
      retryable: true
    )

    let result = resolver.resolveConflict(txn1, conflict, txn2)

    check result.resolution == crRetry
    check result.newTimestamp == Timestamp(100) # Keep our timestamp
    check result.shouldAbort == false

  test "resolve conflict with priority - higher priority wins":
    var txn1 = MVCCTransaction(
      id: TransactionID(1),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: 800, # Higher priority
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    var txn2 = MVCCTransaction(
      id: TransactionID(2),
      status: TXN_PENDING,
      startTimestamp: Timestamp(50),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: 500, # Lower priority
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(50),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let conflict = ConflictInfo(
      conflictType: ctWriteWrite,
      key: "key1",
      conflictingTxnId: TransactionID(2),
      timestamp: Timestamp(150),
      retryable: true
    )

    let result = resolver.resolveConflict(txn1, conflict, txn2)

    check result.resolution == crPush
    check result.shouldAbort == false

suite "Transaction Push Mechanism":
  test "push transaction timestamp":
    var txn1 = MVCCTransaction(
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

    var txn2 = MVCCTransaction(
      id: TransactionID(2),
      status: TXN_PENDING,
      startTimestamp: Timestamp(50),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(50),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let newTs = pushTransaction(txn1, txn2, Timestamp(75))

    check newTs == Timestamp(76)
    check txn2.startTimestamp == Timestamp(76)

  test "push aborted transaction":
    var txn1 = MVCCTransaction(
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

    var txn2 = MVCCTransaction(
      id: TransactionID(2),
      status: TXN_ABORTED,
      startTimestamp: Timestamp(50),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(50),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let newTs = pushTransaction(txn1, txn2, Timestamp(75))

    check newTs == txn1.startTimestamp # No push needed

  test "push committed transaction":
    var txn1 = MVCCTransaction(
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

    var txn2 = MVCCTransaction(
      id: TransactionID(2),
      status: TXN_COMMITTED,
      startTimestamp: Timestamp(50),
      commitTimestamp: Timestamp(200),
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(50),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let newTs = pushTransaction(txn1, txn2, Timestamp(75))

    check newTs == txn2.commitTimestamp # Return committed timestamp

  test "can push check":
    var txn1 = MVCCTransaction(
      id: TransactionID(1),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: 800, # Higher priority
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    var txn2 = MVCCTransaction(
      id: TransactionID(2),
      status: TXN_PENDING,
      startTimestamp: Timestamp(50),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: 500, # Lower priority
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(50),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    check canPush(txn1, txn2) == true

    txn1.priority = 300 # Lower priority now
    check canPush(txn1, txn2) == false

suite "Wait-Die Deadlock Prevention":
  test "younger waits for older":
    var txn1 = MVCCTransaction(
      id: TransactionID(1),
      status: TXN_PENDING,
      startTimestamp: Timestamp(200), # Younger
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(200),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    var txn2 = MVCCTransaction(
      id: TransactionID(2),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100), # Older
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

    # Younger waits for older
    check shouldWaitOrDie(txn1, txn2) == true
    check shouldWaitOrDie(txn2, txn1) == false

  test "should abort transaction":
    var txn1 = MVCCTransaction(
      id: TransactionID(1),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100), # Older
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

    var txn2 = MVCCTransaction(
      id: TransactionID(2),
      status: TXN_PENDING,
      startTimestamp: Timestamp(200), # Younger
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(200),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    # Older should abort when conflicting with younger
    check shouldAbortTransaction(txn1, txn2) == true
    check shouldAbortTransaction(txn2, txn1) == false

  test "committed transaction doesn't cause abort":
    var txn1 = MVCCTransaction(
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

    var txn2 = MVCCTransaction(
      id: TransactionID(2),
      status: TXN_COMMITTED,
      startTimestamp: Timestamp(50),
      commitTimestamp: Timestamp(200),
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(50),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    # Committed transaction doesn't cause abort
    check shouldAbortTransaction(txn1, txn2) == false
    check shouldAbortTransaction(txn2, txn1) == false
