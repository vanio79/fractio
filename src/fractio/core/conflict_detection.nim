# Conflict Detection and Resolution for MVCC Transactions
# Implements serializable conflict detection and optimistic concurrency control

import std/[options, tables, sets]
import ../core/types
import ../core/transaction
import ../storage/mvcc/engine
import ../storage/mvcc/types

type
  ConflictType* = enum
    ## Types of conflicts between transactions
    ctWriteWrite
      ## Two transactions write to the same key
    ctWriteRead
      ## Transaction A writes to a key that Transaction B read
    ctReadWrite
      ## Transaction A reads a key that Transaction B writes
    ctIntentConflict
      ## Intent conflict between transactions

  ConflictInfo* = object
    ## Information about a detected conflict
    conflictType*: ConflictType
    key*: string
    conflictingTxnId*: TransactionID
    timestamp*: Timestamp
    retryable*: bool

  ConflictResolution* = enum
    ## How to resolve a conflict
    crRetry
      ## Retry the transaction with new timestamp
    crWait
      ## Wait for conflicting transaction to complete
    crAbort
      ## Abort the transaction
    crPush
      ## Push the conflicting transaction's timestamp

  ConflictResolver* = ref object
    ## Resolves conflicts between transactions
    engine*: MVCCEngine
    enablePriority*: bool
      ## Whether to use priority-based resolution
    maxWaitTimeMs*: int64
      ## Maximum time to wait for a transaction

  ConflictResult* = object
    ## Result of conflict resolution
    resolution*: ConflictResolution
    newTimestamp*: Timestamp
      ## New timestamp if retrying
    waitForTxn*: TransactionID
      ## Transaction to wait for (if waiting)
    shouldAbort*: bool
      ## Whether transaction should abort

# Conflict detection

proc detectWriteWriteConflict*(engine: MVCCEngine,
    txn: MVCCTransaction, key: string): Option[ConflictInfo] =
  ## Detect write-write conflict
  ## Returns conflict info if another transaction has written to the key

  # Check if we've written to this key
  if not txn.hasWrite(key):
    return none(ConflictInfo)

  # Check for intents from other transactions
  let intents = engine.getIntentsForKey(key)
  for intent in intents:
    if intent.txnId != txn.id:
      # Another transaction has an intent on this key
      return some(ConflictInfo(
        conflictType: ctWriteWrite,
        key: key,
        conflictingTxnId: intent.txnId,
        timestamp: intent.timestamp,
        retryable: true
      ))

  # Check for committed writes after our start timestamp
  let latestVersion = engine.getLatestVersion(key)
  if latestVersion.isSome():
    let latest = latestVersion.get()
    if latest.timestamp > txn.startTimestamp and
       latest.txnId != txn.id:
      # Someone committed after we started
      return some(ConflictInfo(
        conflictType: ctWriteWrite,
        key: key,
        conflictingTxnId: latest.txnId,
        timestamp: latest.timestamp,
        retryable: true
      ))

  return none(ConflictInfo)

proc detectWriteReadConflict*(engine: MVCCEngine,
    txn: MVCCTransaction, key: string): Option[ConflictInfo] =
  ## Detect write-read conflict
  ## We wrote to a key that another transaction read

  # Check if we've written to this key
  if not txn.hasWrite(key):
    return none(ConflictInfo)

  # Check if any other transaction has read this key
  # In a real implementation, we'd check a global read registry
  # For now, we check if there's a committed version after our write
  let latestVersion = engine.getLatestVersion(key)
  if latestVersion.isSome():
    let latest = latestVersion.get()
    if latest.timestamp > txn.startTimestamp:
      # Someone committed after we wrote
      return some(ConflictInfo(
        conflictType: ctWriteRead,
        key: key,
        conflictingTxnId: latest.txnId,
        timestamp: latest.timestamp,
        retryable: true
      ))

  return none(ConflictInfo)

proc detectReadWriteConflict*(engine: MVCCEngine,
    txn: MVCCTransaction, key: string): Option[ConflictInfo] =
  ## Detect read-write conflict
  ## We read a key that another transaction wrote

  # Check if we've read this key
  if not txn.hasRead(key):
    return none(ConflictInfo)

  let readTs = txn.getReadTimestamp(key)

  # Check for intents from other transactions
  let intents = engine.getIntentsForKey(key)
  for intent in intents:
    if intent.txnId != txn.id and intent.timestamp > readTs:
      # Another transaction has an intent after our read
      return some(ConflictInfo(
        conflictType: ctReadWrite,
        key: key,
        conflictingTxnId: intent.txnId,
        timestamp: intent.timestamp,
        retryable: true
      ))

  # Check for committed writes after our read
  let latestVersion = engine.getLatestVersion(key)
  if latestVersion.isSome():
    let latest = latestVersion.get()
    if latest.timestamp > readTs and latest.txnId != txn.id:
      # Someone committed after we read
      return some(ConflictInfo(
        conflictType: ctReadWrite,
        key: key,
        conflictingTxnId: latest.txnId,
        timestamp: latest.timestamp,
        retryable: true
      ))

  return none(ConflictInfo)

proc detectAllConflicts*(engine: MVCCEngine,
    txn: MVCCTransaction): seq[ConflictInfo] =
  ## Detect all conflicts for a transaction
  ## Returns list of conflicts (empty if no conflicts)

  var conflicts: seq[ConflictInfo] = @[]

  # Check write-write conflicts for all keys we've written
  for entry in txn.writeSet.entries:
    let conflict = engine.detectWriteWriteConflict(txn, entry.key)
    if conflict.isSome():
      conflicts.add(conflict.get())

  # Check write-read conflicts for all keys we've written
  for entry in txn.writeSet.entries:
    let conflict = engine.detectWriteReadConflict(txn, entry.key)
    if conflict.isSome():
      conflicts.add(conflict.get())

  # Check read-write conflicts for all keys we've read
  for key in txn.readSet.keys:
    let conflict = engine.detectReadWriteConflict(txn, key)
    if conflict.isSome():
      conflicts.add(conflict.get())

  return conflicts

# Conflict resolution

proc newConflictResolver*(engine: MVCCEngine,
    enablePriority: bool = true,
    maxWaitTimeMs: int64 = 10_000): ConflictResolver =
  ## Create a new conflict resolver
  new(result)
  result.engine = engine
  result.enablePriority = enablePriority
  result.maxWaitTimeMs = maxWaitTimeMs

proc resolveConflict*(resolver: ConflictResolver,
    txn: MVCCTransaction,
    conflict: ConflictInfo,
    conflictingTxn: MVCCTransaction): ConflictResult =
  ## Resolve a conflict between two transactions
  ## Uses priority-based resolution if enabled

  # If priority is enabled and we have higher priority, push the other txn
  if resolver.enablePriority and txn.priority > conflictingTxn.priority:
    return ConflictResult(
      resolution: crPush,
      newTimestamp: INVALID_TIMESTAMP,
      waitForTxn: engine.InvalidTransactionID,
      shouldAbort: false
    )

  # If the other transaction is committed, we must retry
  if conflictingTxn.isCommitted():
    return ConflictResult(
      resolution: crRetry,
      newTimestamp: conflictingTxn.commitTimestamp + 1,
      waitForTxn: engine.InvalidTransactionID,
      shouldAbort: false
    )

  # If the other transaction is aborted, we can proceed
  if conflictingTxn.isAborted():
    return ConflictResult(
      resolution: crRetry,
      newTimestamp: txn.startTimestamp,
      waitForTxn: engine.InvalidTransactionID,
      shouldAbort: false
    )

  # If the other transaction is pending, wait or retry
  if conflictingTxn.isPending():
    # If we have higher priority, push the other txn
    if resolver.enablePriority and txn.priority > conflictingTxn.priority:
      return ConflictResult(
        resolution: crPush,
        newTimestamp: INVALID_TIMESTAMP,
        waitForTxn: engine.InvalidTransactionID,
        shouldAbort: false
      )

    # Otherwise, wait for the other transaction
    return ConflictResult(
      resolution: crWait,
      newTimestamp: INVALID_TIMESTAMP,
      waitForTxn: conflictingTxn.id,
      shouldAbort: false
    )

  # Default: retry with new timestamp
  return ConflictResult(
    resolution: crRetry,
    newTimestamp: conflict.timestamp + 1,
    waitForTxn: engine.InvalidTransactionID,
    shouldAbort: false
  )

proc shouldRetryTransaction*(txn: MVCCTransaction,
    conflicts: seq[ConflictInfo]): bool =
  ## Determine if a transaction should be retried based on conflicts
  ## Returns true if retry is possible

  if conflicts.len == 0:
    return true # No conflicts, can proceed

  # Check if all conflicts are retryable
  for conflict in conflicts:
    if not conflict.retryable:
      return false

  # Check if we haven't exceeded retry limit
  if not txn.canRetry(DEFAULT_MAX_RETRIES):
    return false

  return true

proc getRetryTimestamp*(txn: MVCCTransaction,
    conflicts: seq[ConflictInfo]): Timestamp =
  ## Get the timestamp to use for retry
  ## Returns the maximum timestamp from all conflicts + 1

  if conflicts.len == 0:
    return txn.startTimestamp

  var maxTs = txn.startTimestamp
  for conflict in conflicts:
    if conflict.timestamp > maxTs:
      maxTs = conflict.timestamp

  return maxTs + 1

# Transaction push mechanism

proc pushTransaction*(pusher: MVCCTransaction,
    pushee: MVCCTransaction,
    minTimestamp: Timestamp): Timestamp =
  ## Push a transaction's timestamp forward
  ## Returns the new timestamp, or INVALID_TIMESTAMP if cannot push

  if pushee.status == TXN_ABORTED:
    # Already aborted, can ignore
    return pusher.startTimestamp

  if pushee.status == TXN_COMMITTED:
    # Already committed, return its commit timestamp
    return pushee.commitTimestamp

  # Try to push the timestamp forward
  let newTs = minTimestamp + 1

  if newTs < pushee.maxTimestamp:
    pushee.startTimestamp = newTs
    return newTs

  # Cannot push - return invalid to signal waiting
  return INVALID_TIMESTAMP

proc canPush*(pusher: MVCCTransaction,
    pushee: MVCCTransaction): bool =
  ## Check if pusher can push pushee's timestamp
  ## Returns true if pusher has higher priority

  if pushee.status != TXN_PENDING:
    return false

  # Pusher must have higher priority
  return pusher.priority > pushee.priority

# Deadlock prevention (wait-die)

proc shouldWaitOrDie*(waiter: MVCCTransaction,
    holder: MVCCTransaction): bool =
  ## Wait-die deadlock prevention
  ## Returns true if waiter should wait, false if it should die (abort)
  ## Younger transactions (higher timestamp) wait for older ones

  # If holder is committed or aborted, don't wait
  if holder.isCommitted() or holder.isAborted():
    return false

  # Wait-die: younger waits for older
  # Higher timestamp = younger
  return waiter.startTimestamp > holder.startTimestamp

proc shouldAbortTransaction*(txn: MVCCTransaction,
    conflictingTxn: MVCCTransaction): bool =
  ## Determine if transaction should abort due to conflict
  ## Uses wait-die policy

  # If conflicting transaction is committed, we can retry
  if conflictingTxn.isCommitted():
    return false

  # If conflicting transaction is aborted, we can proceed
  if conflictingTxn.isAborted():
    return false

  # Wait-die: younger transactions wait, older abort
  # Higher timestamp = younger
  if txn.startTimestamp > conflictingTxn.startTimestamp:
    # We're younger, we should wait
    return false
  else:
    # We're older, we should abort
    return true

# Conflict statistics

type
  ConflictStatistics* = object
    ## Statistics about conflicts
    writeWriteConflicts*: int64
    writeReadConflicts*: int64
    readWriteConflicts*: int64
    intentConflicts*: int64
    totalConflicts*: int64
    resolvedByRetry*: int64
    resolvedByWait*: int64
    resolvedByPush*: int64
    resolvedByAbort*: int64

proc newConflictStatistics*(): ConflictStatistics =
  ## Create new conflict statistics
  ConflictStatistics(
    writeWriteConflicts: 0,
    writeReadConflicts: 0,
    readWriteConflicts: 0,
    intentConflicts: 0,
    totalConflicts: 0,
    resolvedByRetry: 0,
    resolvedByWait: 0,
    resolvedByPush: 0,
    resolvedByAbort: 0
  )

proc recordConflict*(stats: var ConflictStatistics,
    conflictType: ConflictType) =
  ## Record a conflict
  case conflictType:
    of ctWriteWrite:
      stats.writeWriteConflicts += 1
    of ctWriteRead:
      stats.writeReadConflicts += 1
    of ctReadWrite:
      stats.readWriteConflicts += 1
    of ctIntentConflict:
      stats.intentConflicts += 1
  stats.totalConflicts += 1

proc recordResolution*(stats: var ConflictStatistics,
    resolution: ConflictResolution) =
  ## Record a conflict resolution
  case resolution:
    of crRetry:
      stats.resolvedByRetry += 1
    of crWait:
      stats.resolvedByWait += 1
    of crPush:
      stats.resolvedByPush += 1
    of crAbort:
      stats.resolvedByAbort += 1

proc getConflictRate*(stats: ConflictStatistics,
    totalTransactions: int64): float64 =
  ## Calculate conflict rate (conflicts per transaction)
  if totalTransactions == 0:
    return 0.0
  return float64(stats.totalConflicts) / float64(totalTransactions)

proc getRetryRate*(stats: ConflictStatistics): float64 =
  ## Calculate retry rate (retries / total resolutions)
  let totalResolutions = stats.resolvedByRetry + stats.resolvedByWait +
                       stats.resolvedByPush + stats.resolvedByAbort
  if totalResolutions == 0:
    return 0.0
  return float64(stats.resolvedByRetry) / float64(totalResolutions)

# Unit tests
when isMainModule:
  import unittest

  suite "Conflict Detection":
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
