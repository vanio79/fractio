# Unit tests for Conflict Detection and Resolution
# Migrated to use DI patterns where applicable
# Note: Uses inline mocks for StorageBackend/Iterator compatibility

import unittest
import tables
import options
import sequtils
import algorithm
import fractio/core/transaction
import fractio/core/timestamp_provider
import fractio/core/conflict_detection
import fractio/core/types
import fractio/storage/mvcc/engine
import fractio/storage/mvcc/types
import fractio/storage/backend
import fractio/distributed/sharedtimer/mock as sharedtimerMock
import fractio/di/container
import fractio/app/bootstrap

# Constants for testing
const
  INVALID_TIMESTAMP* = Timestamp(0)
  MAX_TIMESTAMP* = high(Timestamp)
  DEFAULT_MAX_OFFSET_NS* = 1_000_000'i64
  DEFAULT_PRIORITY* = 500
  DEFAULT_MAX_RETRIES* = 15

# Inline mock for MVCCEngine compatibility (must inherit from StorageBackend)
type
  InlineMockStorageBackend = ref object of StorageBackend
    data: tables.Table[string, string]

  InlineMockStorageIterator = ref object of StorageIterator
    backendRef: InlineMockStorageBackend
    keys: seq[string]
    position: int

proc newInlineMockStorageIterator(backend: InlineMockStorageBackend): InlineMockStorageIterator =
  new(result)
  result.backendRef = backend
  result.keys = toSeq(backend.data.keys).sorted()
  result.position = 0

method valid(iter: InlineMockStorageIterator): bool =
  return iter.position < iter.keys.len

method seekToFirst(iter: InlineMockStorageIterator): bool =
  iter.position = 0
  return iter.valid()

method seekToLast(iter: InlineMockStorageIterator): bool =
  iter.position = iter.keys.len - 1
  return iter.valid()

method seek(iter: InlineMockStorageIterator, key: string): bool =
  var left = 0
  var right = iter.keys.len - 1
  while left <= right:
    let mid = (left + right) div 2
    if iter.keys[mid] == key:
      iter.position = mid
      return true
    elif iter.keys[mid] < key:
      left = mid + 1
    else:
      right = mid - 1
  iter.position = left
  return iter.valid()

method next(iter: InlineMockStorageIterator): bool =
  if iter.position < iter.keys.len - 1:
    inc iter.position
    return true
  return false

method prev(iter: InlineMockStorageIterator): bool =
  if iter.position > 0:
    dec iter.position
    return true
  return false

method key(iter: InlineMockStorageIterator): string =
  if iter.valid():
    return iter.keys[iter.position]
  return ""

method value(iter: InlineMockStorageIterator): string =
  if iter.valid():
    let k = iter.keys[iter.position]
    return iter.backendRef.data[k]
  return ""

method destroy(iter: InlineMockStorageIterator) =
  discard

proc newInlineMockStorageBackend(): InlineMockStorageBackend =
  new(result)
  result.data = initTable[string, string]()

method put(backend: InlineMockStorageBackend, key: string,
    value: string): bool =
  backend.data[key] = value
  return true

method get(backend: InlineMockStorageBackend, key: string): Option[string] =
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
  return newInlineMockStorageIterator(backend)

suite "Conflict Detection - Pure Tests":
  # Tests that don't need any mocks
  test "conflict info creation":
    let conflict = ConflictInfo(
      conflictType: ctWriteWrite,
      key: "test_key",
      conflictingTxnId: genTransactionIDLocal(),
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
    check stats.getConflictRate(100) == 0.03
    check stats.getRetryRate() == 0.5

  test "conflict rate calculation":
    var stats = newConflictStatistics()
    check stats.getConflictRate(0) == 0.0
    stats.recordConflict(ctWriteWrite)
    stats.recordConflict(ctWriteRead)
    check stats.getConflictRate(10) == 0.2

  test "retry rate calculation":
    var stats = newConflictStatistics()
    check stats.getRetryRate() == 0.0
    stats.recordResolution(crRetry)
    stats.recordResolution(crRetry)
    stats.recordResolution(crRetry)
    check stats.getRetryRate() == 1.0
    stats.recordResolution(crWait)
    stats.recordResolution(crPush)
    check stats.getRetryRate() == 0.6

suite "Conflict Resolution":
  var mockBackend: InlineMockStorageBackend
  var mockTimer: sharedtimerMock.MockTimeProvider
  var tsProvider: TimestampProvider
  var mvccEngine: MVCCEngine
  var resolver: ConflictResolver

  setup:
    mockBackend = newInlineMockStorageBackend()
    mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
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
    resolver = newConflictResolver(mvccEngine)

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
      id: genTransactionIDLocal(),
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
      id: genTransactionIDLocal(),
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
      conflictingTxnId: genTransactionIDLocal(),
      timestamp: Timestamp(200),
      retryable: true
    )
    let result = resolver.resolveConflict(txn1, conflict, txn2)
    check result.resolution == crRetry
    check result.newTimestamp == Timestamp(201)
    check result.shouldAbort == false

  test "resolve conflict with aborted transaction":
    var txn1 = MVCCTransaction(
      id: genTransactionIDLocal(),
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
      id: genTransactionIDLocal(),
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
      conflictingTxnId: genTransactionIDLocal(),
      timestamp: Timestamp(150),
      retryable: true
    )
    let result = resolver.resolveConflict(txn1, conflict, txn2)
    check result.resolution == crRetry
    check result.newTimestamp == Timestamp(100)
    check result.shouldAbort == false

  test "resolve conflict with priority - higher priority wins":
    var txn1 = MVCCTransaction(
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: 800,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )
    var txn2 = MVCCTransaction(
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(50),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: 500,
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
      conflictingTxnId: genTransactionIDLocal(),
      timestamp: Timestamp(150),
      retryable: true
    )
    let result = resolver.resolveConflict(txn1, conflict, txn2)
    check result.resolution == crPush
    check result.shouldAbort == false

suite "Transaction Push Mechanism - Pure Tests":
  # Tests that don't need backend/engine
  test "push transaction timestamp":
    var txn1 = MVCCTransaction(
      id: genTransactionIDLocal(),
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
      id: genTransactionIDLocal(),
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
      id: genTransactionIDLocal(),
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
      id: genTransactionIDLocal(),
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
    check newTs == txn1.startTimestamp

  test "push committed transaction":
    var txn1 = MVCCTransaction(
      id: genTransactionIDLocal(),
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
      id: genTransactionIDLocal(),
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
    check newTs == txn2.commitTimestamp

  test "can push check":
    var txn1 = MVCCTransaction(
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: 800,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )
    var txn2 = MVCCTransaction(
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(50),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: 500,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(50),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )
    check canPush(txn1, txn2) == true
    txn1.priority = 300
    check canPush(txn1, txn2) == false

suite "Wait-Die Deadlock Prevention - Pure Tests":
  test "younger waits for older":
    var txn1 = MVCCTransaction(
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(200),
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
      id: genTransactionIDLocal(),
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
    check shouldWaitOrDie(txn1, txn2) == true
    check shouldWaitOrDie(txn2, txn1) == false

  test "should abort transaction":
    var txn1 = MVCCTransaction(
      id: genTransactionIDLocal(),
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
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(200),
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
    check shouldAbortTransaction(txn1, txn2) == true
    check shouldAbortTransaction(txn2, txn1) == false

  test "committed transaction doesn't cause abort":
    var txn1 = MVCCTransaction(
      id: genTransactionIDLocal(),
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
      id: genTransactionIDLocal(),
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
    check shouldAbortTransaction(txn1, txn2) == false
    check shouldAbortTransaction(txn2, txn1) == false
