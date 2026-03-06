# Transaction Manager - Commit/Abort logic for MVCC transactions

import std/[atomics, locks, sets, options]
import ./transaction
import ./timestamp_provider
import ./types as core_types
import ./conflict_detection
import ../storage/mvcc/engine
import ../storage/mvcc/types

type
  TransactionManager* = ref object
    ## Manages transactions and coordinates with MVCC storage
    timestampProvider*: TimestampProvider
    mvccEngine*: MVCCEngine
    activeTransactions*: int
      ## Count of active transactions
    committedCount*: int64
      ## Total committed transactions
    abortedCount*: int64
      ## Total aborted transactions
    retryCount*: int64
      ## Total retries
    maxRetries*: int
      ## Maximum retries per transaction
    defaultTimeoutMs*: int64
      ## Default timeout for transactions
    conflictResolver*: ConflictResolver
      ## Conflict detection and resolution
    conflictStats*: ConflictStatistics
      ## Conflict statistics

  ActiveTransaction* = object
    ## Tracks an active transaction
    txn*: MVCCTransaction
    manager*: TransactionManager
    startTime*: Timestamp

proc newTransactionManager*(tsProvider: TimestampProvider,
    mvccEngine: MVCCEngine,
    maxRetries: int = DEFAULT_MAX_RETRIES,
    defaultTimeoutMs: int64 = DEFAULT_TIMEOUT_MS): TransactionManager =
  ## Create a new transaction manager
  new(result)
  result.timestampProvider = tsProvider
  result.mvccEngine = mvccEngine
  result.activeTransactions = 0
  result.committedCount = 0
  result.abortedCount = 0
  result.retryCount = 0
  result.maxRetries = maxRetries
  result.defaultTimeoutMs = defaultTimeoutMs
  result.conflictResolver = newConflictResolver(mvccEngine)
  result.conflictStats = newConflictStatistics()

proc commitTransaction*(manager: TransactionManager,
    txn: MVCCTransaction): CommitResult =
  ## Commit an MVCC transaction
  ## 1. Acquire commit timestamp
  ## 2. Detect and resolve conflicts (serializability check)
  ## 3. Upgrade intents to committed values

  if txn.status != TXN_PENDING:
    return commitError(ceInvalidState, "Transaction not in pending state")

  # Step 1: Acquire commit timestamp (must be > start timestamp)
  let commitTs = manager.timestampProvider.acquireCommitTimestamp(
      txn.startTimestamp)

  # Step 2: Detect conflicts using conflict detection module
  let conflicts = manager.mvccEngine.detectAllConflicts(txn)

  if conflicts.len > 0:
    # Record conflicts in statistics
    for conflict in conflicts:
      manager.conflictStats.recordConflict(conflict.conflictType)

    # Check if we should retry
    if not shouldRetryTransaction(txn, conflicts):
      # Cannot retry, must abort
      manager.abortedCount += 1
      return commitError(ceSerializationFailure,
        "Transaction has unresolvable conflicts")

    # Resolve conflicts
    for conflict in conflicts:
      # Try to push conflicting transactions
      # In a real implementation, we'd need access to the conflicting transaction
      # For now, we'll just retry
      discard conflict

    # Retry with new timestamp
    manager.retryCount += 1
    return commitError(ceSerializationFailure,
      "Transaction has conflicts, retry required")

  # Step 3: Validate read set - check for write-write conflicts
  # For each key we read, check if a newer version was committed
  for i, key in txn.readSet.keys:
    let readTs = txn.readSet.timestamps[i]

    # Get latest committed version
    let latestVersion = manager.mvccEngine.getLatestVersion(key)

    if latestVersion.isSome():
      let latest = latestVersion.get()

      # If a newer version was committed after we read, we have a conflict
      # (someone wrote after our read but before our commit)
      if latest.timestamp > readTs and latest.timestamp < commitTs:
        # Check if it's from a different transaction
        if latest.txnId != txn.id and latest.txnId !=
            engine.InvalidTransactionID:
          manager.abortedCount += 1
          return commitError(ceSerializationFailure,
            "Serialization failure: key " & key & " modified after read")

  # Step 4: Upgrade intents to committed values
  for entry in txn.writeSet.entries:
    let resolved = manager.mvccEngine.resolveIntent(
      entry.key,
      txn.id,
      true, # commit
      commitTs
    )

    if not resolved.success:
      # Intent resolution failed - this shouldn't happen
      manager.abortedCount += 1
      return commitError(ceWriteConflict,
        "Failed to commit key: " & entry.key)

  # Mark transaction as committed
  txn.status = TXN_COMMITTED
  txn.commitTimestamp = commitTs

  manager.committedCount += 1

  return commitSuccess(commitTs)

proc abortTransaction*(manager: TransactionManager,
    txn: MVCCTransaction): CommitResult =
  ## Abort a transaction - rollback all writes

  if txn.status != TXN_PENDING:
    return commitError(ceInvalidState, "Transaction not in pending state")

  # Clean up all intents written by this transaction
  for entry in txn.writeSet.entries:
    let cleaned = manager.mvccEngine.cleanupIntent(entry.key, txn.id)
    if not cleaned:
      # Log error but continue cleanup
      discard

  txn.status = TXN_ABORTED

  manager.abortedCount += 1

  return commitError(ceAborted, "Transaction aborted")

proc rollbackTransaction*(txn: MVCCTransaction): bool =
  ## Rollback a transaction (alias for abort)
  ## Note: This doesn't clean up intents - use abortTransaction for that
  result = true
  txn.status = TXN_ABORTED

proc validateReadSet*(manager: TransactionManager,
    txn: MVCCTransaction, commitTs: Timestamp): bool =
  ## Validate the read set for serializability
  ## Returns true if validation passes

  for i, key in txn.readSet.keys:
    let readTs = txn.readSet.timestamps[i]

    let latestVersion = manager.mvccEngine.getLatestVersion(key)

    if latestVersion.isSome():
      let latest = latestVersion.get()

      # If a newer version was committed, check for conflict
      if latest.timestamp > readTs and latest.timestamp < commitTs:
        if latest.txnId != txn.id:
          return false

  return true

proc pushTimestamp*(pusher: MVCCTransaction,
    pushee: MVCCTransaction): Timestamp =
  ## Push a transaction's timestamp forward
  ## Returns the new timestamp or INVALID_TIMESTAMP if cannot push

  if pushee.status == TXN_ABORTED:
    # Already aborted, can ignore
    return pusher.startTimestamp

  if pushee.status == TXN_COMMITTED:
    # Already committed, return its commit timestamp
    return pushee.commitTimestamp

  # Try to push the timestamp forward
  let newTs = pusher.startTimestamp + 1

  if newTs < pushee.maxTimestamp:
    pushee.startTimestamp = newTs
    return newTs

  # Cannot push - return invalid to signal waiting
  return INVALID_TIMESTAMP

proc waitForTransaction*(manager: TransactionManager,
    txn: MVCCTransaction, key: string,
    timeoutNs: int64 = 10_000_000_000): bool =
  ## Wait for another transaction to commit/abort
  ## Returns true if transaction was committed, false if aborted/timeout

  # Simple implementation - just check a few times
  # In production, would use condition variables
  for i in 0..100:
    # Check if intent still exists
    let intents = manager.mvccEngine.getIntentsForKey(key)

    if intents.len == 0:
      # No more intents - we're done
      return true

    # Simple spin wait
    # TODO: Implement proper waiting with condition variables

  return false

# Helper functions

proc incRetries*(manager: TransactionManager) =
  ## Increment retry counter
  manager.retryCount += 1

proc calculateBackoff*(retryCount: int): int64 =
  ## Calculate exponential backoff in milliseconds
  ## Base: 10ms, max: 1000ms
  let baseMs = 10
  let maxMs = 1000
  let backoff = baseMs * (1 shl retryCount)
  result = if backoff < maxMs: backoff else: maxMs

# Enhanced commit protocol with retry logic

proc commitWithRetry*(manager: TransactionManager,
    txn: MVCCTransaction,
    maxRetries: int = DEFAULT_MAX_RETRIES): CommitResult =
  ## Commit a transaction with automatic retry on conflict
  ## Uses exponential backoff between retries

  var currentTxn = txn
  var retries = 0

  while retries <= maxRetries:
    # Check deadline
    let currentTime = manager.timestampProvider.now()
    if currentTxn.isExpired(currentTime):
      return commitError(ceTimeout, "Transaction deadline exceeded")

    # Attempt commit
    let result = manager.commitTransaction(currentTxn)

    if result.success:
      return result

    # Check if error is retryable
    if result.error.retryable and currentTxn.canRetry(maxRetries):
      # Backoff before retry
      let backoffMs = calculateBackoff(retries)
      # TODO: Implement actual sleep/backoff
      discard backoffMs

      # Create new transaction for retry
      let newTs = manager.timestampProvider.acquireStartTimestamp()
      currentTxn.resetForRetry(newTs)
      retries += 1
      manager.incRetries()
      continue

    # Non-retryable error or max retries exceeded
    return result

  return commitError(ceSerializationFailure,
    "Transaction failed after " & $maxRetries & " retries")

proc executeInTransaction*(manager: TransactionManager,
    operation: proc(txn: MVCCTransaction): CommitResult,
    maxRetries: int = DEFAULT_MAX_RETRIES): CommitResult =
  ## Execute an operation within a transaction with automatic retry
  ## Creates a new transaction, executes the operation, commits with retry

  var txn = newMVCCTransaction(manager.timestampProvider)

  # Execute the operation
  let opResult = operation(txn)

  if not opResult.success:
    # Operation failed, abort transaction
    discard manager.abortTransaction(txn)
    return opResult

  # Commit the transaction with retry
  return manager.commitWithRetry(txn, maxRetries)

# Transaction validation

proc validateTransactionState*(manager: TransactionManager,
    txn: MVCCTransaction): bool =
  ## Validate transaction is in a valid state for operations
  if not txn.isActive():
    return false

  # Check deadline
  let currentTime = manager.timestampProvider.now()
  if txn.isExpired(currentTime):
    return false

  return true

proc checkWriteConflict*(manager: TransactionManager,
    txn: MVCCTransaction, key: string): bool =
  ## Check if there's a write conflict for a key
  ## Returns true if conflict exists (transaction should abort or wait)

  # Check for intents from other transactions
  let intents = manager.mvccEngine.getIntentsForKey(key)

  for intent in intents:
    if intent.txnId != txn.id:
      # Another transaction has an intent on this key
      # Check if that transaction is still active
      # For now, assume conflict
      return true

  # Check for committed writes after our read timestamp
  let latestVersion = manager.mvccEngine.getLatestVersion(key)
  if latestVersion.isSome():
    let latest = latestVersion.get()
    let readTs = txn.getReadTimestamp(key)
    if readTs != INVALID_TIMESTAMP and latest.timestamp > readTs:
      # Someone committed after we read
      return true

  return false

# Transaction lifecycle management

proc beginTransaction*(manager: TransactionManager,
    options: TransactionOptions = TransactionOptions()): MVCCTransaction =
  ## Begin a new transaction with the manager's timestamp provider
  if options.name == "":
    result = newMVCCTransaction(manager.timestampProvider, options.priority)
  else:
    result = newMVCCTransaction(manager.timestampProvider, options)

  manager.activeTransactions += 1

proc endTransaction*(manager: TransactionManager,
    txn: MVCCTransaction) =
  ## End a transaction (decrement active count)
  if txn.isActive():
    manager.activeTransactions -= 1

# Statistics and monitoring

proc getStatistics*(manager: TransactionManager): tuple[
    committed: int64,
    aborted: int64,
    retries: int64,
    active: int,
    successRate: float64] =
  ## Get transaction statistics
  let total = manager.committedCount + manager.abortedCount
  let successRate = if total > 0:
    float64(manager.committedCount) / float64(total)
  else:
    0.0

  result = (
    committed: manager.committedCount,
    aborted: manager.abortedCount,
    retries: manager.retryCount,
    active: manager.activeTransactions,
    successRate: successRate
  )

proc resetStatistics*(manager: TransactionManager) =
  ## Reset transaction statistics
  manager.committedCount = 0
  manager.abortedCount = 0
  manager.retryCount = 0
  manager.conflictStats = newConflictStatistics()

# Conflict statistics

proc getConflictStatistics*(manager: TransactionManager): ConflictStatistics =
  ## Get conflict statistics
  result = manager.conflictStats

proc getConflictRate*(manager: TransactionManager): float64 =
  ## Get conflict rate (conflicts per transaction)
  let totalTxns = manager.committedCount + manager.abortedCount
  return manager.conflictStats.getConflictRate(totalTxns)

proc getRetryRate*(manager: TransactionManager): float64 =
  ## Get retry rate (retries / total resolutions)
  return manager.conflictStats.getRetryRate()

proc getConflictBreakdown*(manager: TransactionManager): tuple[
    writeWrite: int64,
    writeRead: int64,
    readWrite: int64,
    intents: int64] =
  ## Get breakdown of conflict types
  result = (
    writeWrite: manager.conflictStats.writeWriteConflicts,
    writeRead: manager.conflictStats.writeReadConflicts,
    readWrite: manager.conflictStats.readWriteConflicts,
    intents: manager.conflictStats.intentConflicts
  )

# Unit tests
when isMainModule:
  import unittest

  suite "TransactionManager":
    test "commit result helpers":
      let success = commitSuccess(Timestamp(1000))
      check success.success == true
      check success.commitTimestamp == Timestamp(1000)

      let fail = commitError(ceWriteConflict, "conflict", true)
      check fail.success == false
      check fail.error.code == ceWriteConflict
      check fail.error.retryable == true

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
