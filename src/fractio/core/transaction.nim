# Transaction Types - Extended transaction structures for MVCC
# Provides serializable optimistic transaction support

import std/[sets, times]
import ./types as core_types
import ./timestamp_provider
import ../storage/mvcc/types

# Re-export core transaction types
export core_types.TransactionID
export core_types.TransactionStatus

type
  MVCCTransaction* = ref object
    ## MVCC transaction with optimistic concurrency control
    id*: TransactionID
      ## Unique transaction ID

    status*: MVCCTransactionStatus
      ## Current transaction status

    startTimestamp*: Timestamp
      ## Read snapshot timestamp - all reads see data as of this time

    commitTimestamp*: Timestamp
      ## Commit timestamp - assigned at commit time

    priority*: int32
      ## Transaction priority (higher = more important, can push others)

    maxTimestamp*: Timestamp
      ## Maximum timestamp this transaction can read

    deadline*: Timestamp
      ## Deadline for transaction completion (for cancellation)

    createdAt*: Timestamp
      ## When transaction was created

    # Write set - keys modified by this transaction
    writeSet*: WriteSet

    # Read set - keys read by this transaction (for serialization check)
    readSet*: ReadSet

    # For debugging/metrics
    name*: string
      ## Optional transaction name

    # Internal state
    lockedKeys*: int
      ## Number of keys being locked (for monitoring)

    epoch*: int
      ## Transaction epoch (for retry tracking)

  # Note: MVCCTransactionStatus is defined in storage/mvcc/types
  # Using import from there

  IsolationLevel* = enum
    ## Transaction isolation levels
    ilSerializable
      ## Serializable isolation (only supported level)
    ilReadCommitted
      ## Read committed (not yet implemented)
    ilRepeatableRead
      ## Repeatable read (not yet implemented)

  TransactionOptions* = object
    ## Options for creating a transaction
    priority*: int32
      ## Transaction priority
    deadline*: Timestamp
      ## Optional deadline for transaction completion
    name*: string
      ## Optional transaction name for debugging
    isolationLevel*: IsolationLevel
      ## Isolation level (only serializable supported)
    maxRetries*: int
      ## Maximum number of retries on conflict

  WriteSet* = object
    ## Keys modified by a transaction
    entries*: seq[WriteEntry]

  WriteEntry* = object
    ## A single write operation
    key*: string
    value*: string
    isDelete*: bool

  ReadSet* = object
    ## Keys read by a transaction for serialization validation
    keys*: seq[string]
    timestamps*: seq[Timestamp]
      ## Timestamp each key was read at

  CommitResult* = object
    ## Result of transaction commit
    success*: bool
    commitTimestamp*: Timestamp
    error*: TransactionCommitError

  TransactionCommitError* = object of CatchableError
    ## Errors during commit
    code*: CommitErrorCode
    retryable*: bool

  CommitErrorCode* = enum
    ceWriteConflict
    ceReadSnapshotError
    ceSerializationFailure
    ceTimeout
    ceAborted
    ceInvalidState

  # TransactionManager is defined in transaction_manager.nim

const
  INVALID_TIMESTAMP*: Timestamp = Timestamp(0)
  MAX_TIMESTAMP*: Timestamp = high(Timestamp)
  MAX_PRIORITY*: int32 = 1000
  DEFAULT_PRIORITY*: int32 = 500
  MIN_PRIORITY*: int32 = 1
  DEFAULT_MAX_RETRIES*: int = 15
  DEFAULT_TIMEOUT_MS*: int64 = 10_000 # 10 seconds

# Transaction creation

proc newMVCCTransaction*(tsProvider: TimestampProvider,
    priority: int32 = DEFAULT_PRIORITY): MVCCTransaction =
  ## Begin a new MVCC transaction
  new(result)
  result.startTimestamp = tsProvider.acquireStartTimestamp()
  result.id = genTransactionID(result.startTimestamp)
  result.status = TXN_PENDING
  result.commitTimestamp = INVALID_TIMESTAMP
  result.priority = priority
  result.maxTimestamp = MAX_TIMESTAMP
  result.deadline = MAX_TIMESTAMP
  result.createdAt = result.startTimestamp
  result.writeSet = WriteSet(entries: @[])
  result.readSet = ReadSet(keys: @[], timestamps: @[])
  result.lockedKeys = 0
  result.epoch = 0

proc newMVCCTransaction*(tsProvider: TimestampProvider,
    options: TransactionOptions): MVCCTransaction =
  ## Begin a new MVCC transaction with options
  new(result)
  result.startTimestamp = tsProvider.acquireStartTimestamp()
  result.id = genTransactionID(result.startTimestamp)
  result.status = TXN_PENDING
  result.commitTimestamp = INVALID_TIMESTAMP
  result.priority = options.priority
  result.maxTimestamp = MAX_TIMESTAMP
  result.deadline = options.deadline
  result.createdAt = result.startTimestamp
  result.writeSet = WriteSet(entries: @[])
  result.readSet = ReadSet(keys: @[], timestamps: @[])
  result.lockedKeys = 0
  result.epoch = 0
  result.name = options.name

proc newTransactionOptions*(priority: int32 = DEFAULT_PRIORITY,
    timeoutMs: int64 = DEFAULT_TIMEOUT_MS,
    name: string = "",
    maxRetries: int = DEFAULT_MAX_RETRIES): TransactionOptions =
  ## Create transaction options
  result.priority = priority
  result.deadline = if timeoutMs > 0: Timestamp(0) else: MAX_TIMESTAMP
  result.name = name
  result.isolationLevel = ilSerializable
  result.maxRetries = maxRetries

# Transaction operations

proc addWrite*(txn: MVCCTransaction, key: string, value: string,
    isDelete: bool = false) =
  ## Add a write to the transaction's write set
  txn.writeSet.entries.add(WriteEntry(
    key: key,
    value: value,
    isDelete: isDelete
  ))

proc addRead*(txn: MVCCTransaction, key: string, timestamp: Timestamp) =
  ## Add a read to the transaction's read set
  txn.readSet.keys.add(key)
  txn.readSet.timestamps.add(timestamp)

proc clearReadSet*(txn: MVCCTransaction) =
  ## Clear the read set (used when retrying)
  txn.readSet.keys = @[]
  txn.readSet.timestamps = @[]

proc getReadTimestamp*(txn: MVCCTransaction, key: string): Timestamp =
  ## Get the timestamp when a key was read
  for i, k in txn.readSet.keys:
    if k == key:
      return txn.readSet.timestamps[i]
  return INVALID_TIMESTAMP

proc isPending*(txn: MVCCTransaction): bool =
  ## Check if transaction is pending
  result = txn.status == TXN_PENDING

proc isCommitted*(txn: MVCCTransaction): bool =
  ## Check if transaction is committed
  result = txn.status == TXN_COMMITTED

proc isAborted*(txn: MVCCTransaction): bool =
  ## Check if transaction is aborted
  result = txn.status == TXN_ABORTED

proc hasWrite*(txn: MVCCTransaction, key: string): bool =
  ## Check if transaction has written to a key
  for entry in txn.writeSet.entries:
    if entry.key == key:
      return true
  return false

proc hasRead*(txn: MVCCTransaction, key: string): bool =
  ## Check if transaction has read a key
  for k in txn.readSet.keys:
    if k == key:
      return true
  return false

proc getWriteCount*(txn: MVCCTransaction): int =
  ## Get number of writes in transaction
  result = txn.writeSet.entries.len

proc getReadCount*(txn: MVCCTransaction): int =
  ## Get number of reads in transaction
  result = txn.readSet.keys.len

proc isActive*(txn: MVCCTransaction): bool =
  ## Check if transaction is active (pending or prepared)
  result = txn.status == TXN_PENDING or txn.status == TXN_PREPARED

proc canRetry*(txn: MVCCTransaction, maxRetries: int = DEFAULT_MAX_RETRIES): bool =
  ## Check if transaction can be retried
  result = txn.epoch < maxRetries

proc incrementEpoch*(txn: MVCCTransaction) =
  ## Increment transaction epoch (for retry tracking)
  inc txn.epoch

proc resetForRetry*(txn: MVCCTransaction, newTimestamp: Timestamp) =
  ## Reset transaction for retry
  txn.status = TXN_PENDING
  txn.startTimestamp = newTimestamp
  txn.commitTimestamp = INVALID_TIMESTAMP
  txn.writeSet.entries = @[]
  txn.clearReadSet()
  txn.incrementEpoch()

proc checkDeadline*(txn: MVCCTransaction, currentTime: Timestamp): bool =
  ## Check if transaction has exceeded its deadline
  if txn.deadline == MAX_TIMESTAMP:
    return false # No deadline set
  result = currentTime > txn.deadline

proc isExpired*(txn: MVCCTransaction, currentTime: Timestamp): bool =
  ## Alias for checkDeadline
  result = txn.checkDeadline(currentTime)

# Commit result helpers

proc commitSuccess*(commitTs: Timestamp): CommitResult =
  CommitResult(success: true, commitTimestamp: commitTs)

proc commitError*(code: CommitErrorCode, message: string,
    retryable: bool = false): CommitResult =
  CommitResult(
    success: false,
    commitTimestamp: INVALID_TIMESTAMP,
    error: TransactionCommitError(
      msg: message,
      code: code,
      retryable: retryable
    )
  )

# Error constructors

proc transactionAbortedError*(message: string): TransactionCommitError =
  TransactionCommitError(msg: message, code: ceAborted, retryable: false)

proc writeConflictError*(key: string): TransactionCommitError =
  TransactionCommitError(
    msg: "Write conflict on key: " & key,
    code: ceWriteConflict,
    retryable: true
  )

proc serializationError*(message: string): TransactionCommitError =
  TransactionCommitError(
    msg: "Serialization failure: " & message,
    code: ceSerializationFailure,
    retryable: true
  )

# Unit tests
when isMainModule:
  import unittest

  suite "MVCCTransaction":
    test "transaction creation":
      # Test basic transaction creation
      let txnId = genTransactionID(0)
      let txn = MVCCTransaction(
        id: txnId,
        status: TXN_PENDING,
        startTimestamp: Timestamp(1000),
        priority: DEFAULT_PRIORITY,
        writeSet: WriteSet(entries: @[]),
        readSet: ReadSet(keys: @[], timestamps: @[])
      )

      check txn.id == txnId
      check txn.status == TXN_PENDING
      check txn.priority == DEFAULT_PRIORITY

    test "add write entry":
      var txn = MVCCTransaction(
        id: genTransactionID(0),
        status: TXN_PENDING,
        startTimestamp: Timestamp(100),
        priority: DEFAULT_PRIORITY,
        writeSet: WriteSet(entries: @[]),
        readSet: ReadSet(keys: @[], timestamps: @[])
      )

      txn.addWrite("key1", "value1", false)
      txn.addWrite("key2", "", true) # delete

      check txn.writeSet.entries.len == 2
      check txn.writeSet.entries[0].key == "key1"
      check txn.writeSet.entries[0].isDelete == false
      check txn.writeSet.entries[1].isDelete == true

    test "add read entry":
      var txn = MVCCTransaction(
        id: genTransactionID(0),
        status: TXN_PENDING,
        startTimestamp: Timestamp(100),
        priority: DEFAULT_PRIORITY,
        writeSet: WriteSet(entries: @[]),
        readSet: ReadSet(keys: @[], timestamps: @[])
      )

      txn.addRead("key1", Timestamp(50))
      txn.addRead("key2", Timestamp(75))

      check txn.readSet.keys.len == 2
      check txn.getReadTimestamp("key1") == Timestamp(50)
      check txn.getReadTimestamp("key2") == Timestamp(75)
      check txn.getReadTimestamp("nonexistent") == INVALID_TIMESTAMP
