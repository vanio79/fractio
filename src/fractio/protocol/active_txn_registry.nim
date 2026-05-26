# Active Transaction Registry — fast liveness tracking for conflict resolution.
#
# This module provides an in-memory registry of all active transactions on a
# server node. It enables two critical operations:
#
#   1. Keepalive: clients periodically confirm their transaction is still alive
#   2. Conflict-driven override: when a transaction T2 encounters an intent from
#      transaction T1, it can check if T1 is stale (>5s without activity) and
#      forcibly roll it back instead of waiting for the 60s scavenger.
#
# Architecture:
#   - KV operations update lastActivityNs INLINE (atomic store, ~1ns)
#   - Keepalive packets update lastActivityNs ASYNC via lock-free channel
#   - Conflict checks read lastActivityNs INLINE (atomic load, ~1ns)
#   - Stale-txn cleaner runs every 1s in a background thread
#   - Intent cleanup after forceRollback runs in a background thread
#
# The registry is per-node, in-memory only. On leader failover all in-flight
# transactions are dead anyway; the 60s intent scavenger is the crash-recovery
# safety net.
#
# Thread safety:
#   - lastActivityNs: Atomic[int64], moRelaxed for reads/writes
#   - sessions table: protected by mu lock
#   - intentIndex: protected by mu lock (write-once-per-txn on addIntentKey,
#     read-only during cleanup)
#   - cleanupChan: lock-free Channel (single producer, single consumer)

import std/[tables as stdtables, locks, atomics, options, os]
import ../core/types as coreTypes
import ../storage/backend
import ../utils/logging

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  STALE_TXN_THRESHOLD_NS* = 5_000_000_000'i64 ## 5 seconds
  STALE_CLEANER_INTERVAL_MS* = 1_000          ## 1 second
  CLEANUP_CHAN_CAPACITY* = 4096               ## cleanup job queue depth

# ---------------------------------------------------------------------------
# Transaction status in the registry
# ---------------------------------------------------------------------------

type
  TxnRegistryStatus* = enum
    txsActive     ## Transaction is active (receiving KV ops or keepalives)
    txsCommitting ## Transaction is in the process of committing
    txsAborted    ## Transaction has been aborted (pending intent cleanup)

  TxnEntry* = ref object
    txnId*: coreTypes.TransactionID
    sessionId*: uint64
    lastActivityNs*: Atomic[int64] ## Updated by KV ops (inline) and keepalives (async)
    status*: TxnRegistryStatus ## Protected by registry.mu
    intentKeys*: seq[string]   ## All intent keys written by this txn

  CleanupJob* = object
    txnId*: coreTypes.TransactionID
    intentKeys*: seq[string] ## Keys to delete from storage

  ActiveTxnRegistry* = ref object
    sessions*: stdtables.Table[coreTypes.TransactionID, TxnEntry]
    intentIndex*: stdtables.Table[coreTypes.TransactionID, seq[string]]
    mu*: Lock
    cleanupChan*: Channel[CleanupJob]
    running*: Atomic[bool]
    cleanerThread*: Thread[ActiveTxnRegistry]
    cleanerRunning*: Atomic[bool]
    backendPtr*: pointer ## StorageBackend pointer (avoids circular import)
    logger*: Logger

# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

proc newActiveTxnRegistry*(): ActiveTxnRegistry =
  result = ActiveTxnRegistry(
    sessions: stdtables.initTable[coreTypes.TransactionID, TxnEntry](),
    intentIndex: stdtables.initTable[coreTypes.TransactionID, seq[string]](),
    backendPtr: nil,
    logger: newLogger("protocol.active_txn_registry"),
  )
  result.running.store(false)
  result.cleanerRunning.store(false)
  initLock(result.mu)
  result.cleanupChan.open(CLEANUP_CHAN_CAPACITY)

proc setBackendPtr*(registry: ActiveTxnRegistry, ptrVal: pointer) {.gcsafe,
    raises: [].} =
  ## Set the StorageBackend pointer. Uses void pointer to avoid circular import.
  ## The server is responsible for casting this pointer back.
  acquire(registry.mu)
  registry.backendPtr = ptrVal
  release(registry.mu)

proc getBackend(registry: ActiveTxnRegistry): StorageBackend {.gcsafe,
    raises: [].} =
  ## Get the StorageBackend from the stored pointer.
  if registry.backendPtr != nil:
    cast[StorageBackend](registry.backendPtr)
  else:
    nil

# ---------------------------------------------------------------------------
# Transaction lifecycle
# ---------------------------------------------------------------------------

proc register*(registry: ActiveTxnRegistry, txnId: coreTypes.TransactionID,
    sessionId: uint64) {.gcsafe, raises: [].} =
  ## Register a new active transaction.
  let nowNs = coreTypes.localTimeNs()
  let entry = TxnEntry(
    txnId: txnId,
    sessionId: sessionId,
    status: txsActive,
    intentKeys: @[],
  )
  entry.lastActivityNs.store(nowNs, moRelaxed)
  acquire(registry.mu)
  registry.sessions[txnId] = entry
  registry.intentIndex[txnId] = @[]
  release(registry.mu)

proc unregister*(registry: ActiveTxnRegistry,
    txnId: coreTypes.TransactionID) {.gcsafe, raises: [].} =
  ## Remove a transaction from the registry (after commit or abort completes).
  acquire(registry.mu)
  registry.sessions.del(txnId)
  registry.intentIndex.del(txnId)
  release(registry.mu)

proc setCommitting*(registry: ActiveTxnRegistry,
    txnId: coreTypes.TransactionID) {.gcsafe, raises: [].} =
  ## Mark a transaction as committing. This prevents the stale cleaner from
  ## aborting it while the commit is in progress.
  acquire(registry.mu)
  let entry = registry.sessions.getOrDefault(txnId)
  if not entry.isNil:
    entry.status = txsCommitting
    entry.lastActivityNs.store(coreTypes.localTimeNs(), moRelaxed)
  release(registry.mu)

proc setAborted*(registry: ActiveTxnRegistry,
    txnId: coreTypes.TransactionID) {.gcsafe, raises: [].} =
  ## Mark a transaction as aborted in the registry.
  acquire(registry.mu)
  let entry = registry.sessions.getOrDefault(txnId)
  if not entry.isNil:
    entry.status = txsAborted
  release(registry.mu)

# ---------------------------------------------------------------------------
# Activity tracking
# ---------------------------------------------------------------------------

proc touch*(registry: ActiveTxnRegistry,
    txnId: coreTypes.TransactionID) {.gcsafe, raises: [].} =
  ## Update lastActivityNs for a transaction. Called INLINE by KV operations.
  ## Uses atomic store (moRelaxed) — ~1ns cost, no lock needed.
  ##
  ## This is safe to call without the registry lock because:
  ## 1. The Atomic[int64] field is always valid while the entry exists
  ## 2. moRelaxed is sufficient — we only need eventual visibility
  ## 3. The stale cleaner reads with moRelaxed too; worst case it sees
  ##    a slightly stale value and gives the txn one more cycle
  acquire(registry.mu)
  let entry = registry.sessions.getOrDefault(txnId)
  release(registry.mu)
  if not entry.isNil:
    entry.lastActivityNs.store(coreTypes.localTimeNs(), moRelaxed)

proc touchAsync*(registry: ActiveTxnRegistry,
    txnId: coreTypes.TransactionID) {.gcsafe, raises: [].} =
  ## Queue a keepalive touch for async processing by the cleaner thread.
  ## This avoids blocking the client I/O thread on the registry lock.
  ## The cleaner thread will drain the channel and apply the touch.
  ##
  ## If the channel is full, the keepalive is dropped — this is safe because
  ## keepalives are advisory, not authoritative. The next one will succeed.
  {.cast(raises: []).}:
    try:
      discard registry.cleanupChan.trySend(CleanupJob(txnId: txnId,
          intentKeys: @[]))
    except:
      discard

# ---------------------------------------------------------------------------
# Intent key tracking
# ---------------------------------------------------------------------------

proc addIntentKey*(registry: ActiveTxnRegistry, txnId: coreTypes.TransactionID,
    intentKey: string) {.gcsafe, raises: [].} =
  ## Record that a transaction has written an intent key.
  ## Called when raftBufferIntent writes the intent to the backend.
  acquire(registry.mu)
  let entry = registry.sessions.getOrDefault(txnId)
  if not entry.isNil:
    entry.intentKeys.add(intentKey)
  # Also add to the intent index for fast lookup
  var idx = registry.intentIndex.getOrDefault(txnId)
  idx.add(intentKey)
  registry.intentIndex[txnId] = idx
  release(registry.mu)

proc getIntentKeys*(registry: ActiveTxnRegistry,
    txnId: coreTypes.TransactionID): seq[string] {.gcsafe, raises: [].} =
  ## Get all intent keys for a transaction. Used for targeted cleanup.
  acquire(registry.mu)
  result = registry.intentIndex.getOrDefault(txnId, @[])
  release(registry.mu)

# ---------------------------------------------------------------------------
# Liveness checking (the hot path)
# ---------------------------------------------------------------------------

proc isActive*(registry: ActiveTxnRegistry,
    txnId: coreTypes.TransactionID): bool {.gcsafe, raises: [].} =
  ## Check if a transaction is registered and active.
  ## Returns false for unknown transactions or aborted/committing ones.
  acquire(registry.mu)
  let entry = registry.sessions.getOrDefault(txnId)
  release(registry.mu)
  if entry.isNil:
    return false
  entry.status == txsActive

proc isStale*(registry: ActiveTxnRegistry,
    txnId: coreTypes.TransactionID,
    thresholdNs: int64 = STALE_TXN_THRESHOLD_NS): bool {.gcsafe, raises: [].} =
  ## Check if a transaction's last activity is older than the threshold.
  ## Returns true if:
  ##   - The transaction is not in the registry (unknown = dead)
  ##   - The transaction is in the registry but lastActivityNs < (now - threshold)
  ##
  ## This is the HOT PATH called during intent conflict resolution.
  ## Cost: 1 lock acquire/release + 1 atomic load = ~20-50ns.
  acquire(registry.mu)
  let entry = registry.sessions.getOrDefault(txnId)
  release(registry.mu)
  if entry.isNil:
    return true # Not in registry = dead
  let nowNs = coreTypes.localTimeNs()
  let lastActivity = entry.lastActivityNs.load(moRelaxed)
  (nowNs - lastActivity) > thresholdNs

proc getStaleTxnIds*(registry: ActiveTxnRegistry,
    thresholdNs: int64 = STALE_TXN_THRESHOLD_NS): seq[
        coreTypes.TransactionID] {.
    gcsafe, raises: [].} =
  ## Return all transaction IDs that are stale (older than threshold).
  ## Used by the background stale-txn cleaner.
  let nowNs = coreTypes.localTimeNs()
  acquire(registry.mu)
  for txnId, entry in registry.sessions:
    if entry.status == txsActive:
      let lastActivity = entry.lastActivityNs.load(moRelaxed)
      if (nowNs - lastActivity) > thresholdNs:
        result.add(txnId)
  release(registry.mu)

# ---------------------------------------------------------------------------
# Force rollback (conflict-driven override)
# ---------------------------------------------------------------------------

proc forceRollback*(registry: ActiveTxnRegistry,
    txnId: coreTypes.TransactionID): bool {.gcsafe, raises: [].} =
  ## Force-rollback a stale transaction. Called when T2 encounters an intent
  ## from T1 and T1 is stale.
  ##
  ## Steps:
  ##   1. Mark the transaction as aborted in the registry
  ##   2. Queue a cleanup job to delete intent keys from storage
  ##
  ## Returns true if the transaction was successfully marked for rollback.
  ## Returns false if the transaction was not found or already aborted/committing.
  var intentKeys: seq[string] = @[]
  acquire(registry.mu)
  let entry = registry.sessions.getOrDefault(txnId)
  if entry.isNil or entry.status != txsActive:
    release(registry.mu)
    return false
  entry.status = txsAborted
  intentKeys = registry.intentIndex.getOrDefault(txnId, @[])
  release(registry.mu)

  # Queue the cleanup job (deletes intent keys from storage)
  if intentKeys.len > 0:
    {.cast(raises: []).}:
      try:
        discard registry.cleanupChan.trySend(CleanupJob(
          txnId: txnId, intentKeys: intentKeys))
      except:
        discard

  {.cast(raises: []).}:
    try: registry.logger.info("Force-rolled back stale transaction")
    except: discard
  return true

# ---------------------------------------------------------------------------
# Background cleanup thread
# ---------------------------------------------------------------------------

proc processCleanupJobs(registry: ActiveTxnRegistry) {.gcsafe, raises: [].} =
  ## Drain the cleanup channel and process all pending jobs.
  ## This runs in the cleaner thread.
  {.cast(raises: []).}:
    try:
      while true:
        let jobOpt = registry.cleanupChan.tryRecv()
        if not jobOpt[0]: # dataAvailable
          break
        let job = jobOpt[1] # value

        # If the job has no intent keys, it's a keepalive touch
        if job.intentKeys.len == 0:
          acquire(registry.mu)
          let entry = registry.sessions.getOrDefault(job.txnId)
          release(registry.mu)
          if not entry.isNil and entry.status == txsActive:
            entry.lastActivityNs.store(coreTypes.localTimeNs(), moRelaxed)
          continue

        # Delete intent keys from storage
        let backend = registry.getBackend()
        if backend != nil:
          {.cast(raises: []).}:
            try:
              if backend.isOpen:
                discard backend.writeBatchNoSync(@[], job.intentKeys)
            except:
              discard

        # Remove the transaction from the registry after cleanup
        registry.unregister(job.txnId)

        {.cast(raises: []).}:
          try: registry.logger.debug("Cleaned up intents for aborted transaction")
          except: discard
    except:
      discard

proc cleanerThreadProc(registry: ActiveTxnRegistry) {.thread, gcsafe,
    raises: [].} =
  ## Background thread that:
  ##   1. Processes pending cleanup jobs (force-rollback intent deletions)
  ##   2. Processes keepalive touches from the async channel
  ##   3. Scans for stale transactions and force-rolls them back
  {.cast(raises: []).}:
    try: registry.logger.info("Stale-txn cleaner thread started")
    except: discard
  registry.cleanerRunning.store(true, moRelaxed)

  while registry.running.load(moRelaxed):
    # 1. Process all pending cleanup jobs and keepalive touches
    registry.processCleanupJobs()

    # 2. Scan for stale transactions
    let staleIds = registry.getStaleTxnIds()
    for txnId in staleIds:
      discard registry.forceRollback(txnId)

    # 3. Sleep for the cleaner interval in 10ms increments
    let startNs = coreTypes.localTimeNs()
    let intervalNs = int64(STALE_CLEANER_INTERVAL_MS) * 1_000_000
    while registry.running.load(moRelaxed):
      let elapsed = coreTypes.localTimeNs() - startNs
      if elapsed >= intervalNs:
        break
      sleep(10)

  # Final drain of cleanup jobs before exit
  registry.processCleanupJobs()
  registry.cleanerRunning.store(false, moRelaxed)
  {.cast(raises: []).}:
    try: registry.logger.info("Stale-txn cleaner thread stopped")
    except: discard

proc start*(registry: ActiveTxnRegistry) {.gcsafe, raises: [].} =
  ## Start the background cleaner thread.
  if registry.running.load(moRelaxed):
    return
  registry.running.store(true, moRelaxed)
  {.cast(raises: []).}:
    createThread(registry.cleanerThread, cleanerThreadProc, registry)

proc stop*(registry: ActiveTxnRegistry) {.gcsafe, raises: [].} =
  ## Stop the background cleaner thread and drain remaining cleanup jobs.
  if not registry.running.load(moRelaxed):
    return
  registry.running.store(false, moRelaxed)
  joinThread(registry.cleanerThread)
  # Final drain
  registry.processCleanupJobs()
  registry.cleanupChan.close()

# ---------------------------------------------------------------------------
# Stats / inspection
# ---------------------------------------------------------------------------

proc activeCount*(registry: ActiveTxnRegistry): int {.gcsafe, raises: [].} =
  ## Count of transactions with status == txsActive.
  acquire(registry.mu)
  for _, entry in registry.sessions:
    if entry.status == txsActive:
      inc result
  release(registry.mu)

proc totalCount*(registry: ActiveTxnRegistry): int {.gcsafe, raises: [].} =
  ## Total count of all transactions in the registry.
  acquire(registry.mu)
  result = registry.sessions.len
  release(registry.mu)

proc hasTransaction*(registry: ActiveTxnRegistry,
    txnId: coreTypes.TransactionID): bool {.gcsafe, raises: [].} =
  ## Check if a transaction is in the registry.
  acquire(registry.mu)
  result = txnId in registry.sessions
  release(registry.mu)

proc getStatus*(registry: ActiveTxnRegistry,
    txnId: coreTypes.TransactionID): Option[TxnRegistryStatus] {.gcsafe,
    raises: [].} =
  ## Get the status of a transaction. Returns none if not found.
  acquire(registry.mu)
  let entry = registry.sessions.getOrDefault(txnId)
  release(registry.mu)
  if entry.isNil:
    none(TxnRegistryStatus)
  else:
    some(entry.status)

proc getLastActivityNs*(registry: ActiveTxnRegistry,
    txnId: coreTypes.TransactionID): int64 {.gcsafe, raises: [].} =
  ## Get the last activity timestamp for a transaction. Returns 0 if not found.
  acquire(registry.mu)
  let entry = registry.sessions.getOrDefault(txnId)
  release(registry.mu)
  if entry.isNil:
    return 0
  entry.lastActivityNs.load(moRelaxed)
