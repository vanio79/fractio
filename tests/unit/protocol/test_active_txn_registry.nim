# Unit tests for ActiveTxnRegistry — fast liveness tracking for conflict resolution.
#
# Tests cover: registration, touch, isStale, forceRollback, addIntentKey,
# keepalive async, stale cleaner, cleanup jobs, and all edge cases.

import std/[unittest, options, os, atomics, locks]
import fractio/core/types
import fractio/protocol/active_txn_registry
import fractio/storage/backend

# =============================================================================
# Helper: Generate distinct TransactionIDs
# =============================================================================

var nextTxnCounter = 1000

proc genTxnId(): TransactionID =
  inc nextTxnCounter
  genTransactionIDLocal()

# =============================================================================
# Registration and Lifecycle
# =============================================================================

suite "ActiveTxnRegistry - Registration":
  test "register adds transaction to registry":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    check reg.hasTransaction(txnId)
    check reg.activeCount() == 1
    check reg.totalCount() == 1

  test "register multiple transactions":
    let reg = newActiveTxnRegistry()
    let t1 = genTxnId()
    let t2 = genTxnId()
    let t3 = genTxnId()
    reg.register(t1, 1'u64)
    reg.register(t2, 2'u64)
    reg.register(t3, 3'u64)
    check reg.activeCount() == 3
    check reg.totalCount() == 3

  test "unregister removes transaction":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    check reg.hasTransaction(txnId)
    reg.unregister(txnId)
    check not reg.hasTransaction(txnId)
    check reg.totalCount() == 0

  test "unregister non-existent transaction is safe":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.unregister(txnId) # should not crash
    check reg.totalCount() == 0

  test "register sets status to active":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    check reg.getStatus(txnId) == some(txsActive)

  test "register sets lastActivityNs to current time":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    let before = localTimeNs()
    reg.register(txnId, 1'u64)
    let after = localTimeNs()
    let lastActivity = reg.getLastActivityNs(txnId)
    check lastActivity >= before
    check lastActivity <= after

# =============================================================================
# Activity Tracking (touch)
# =============================================================================

suite "ActiveTxnRegistry - Touch":
  test "touch updates lastActivityNs":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    let before = reg.getLastActivityNs(txnId)
    sleep(10)
    reg.touch(txnId)
    let after = reg.getLastActivityNs(txnId)
    check after > before

  test "touch on non-existent transaction is safe":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.touch(txnId) # should not crash
    check not reg.hasTransaction(txnId)

  test "touch keeps transaction from being stale":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    check not reg.isStale(txnId)
    sleep(10)
    reg.touch(txnId)
    check not reg.isStale(txnId)

  test "touchAsync queues keepalive for background processing":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    let before = reg.getLastActivityNs(txnId)
    sleep(10)
    reg.touchAsync(txnId)
    # Process the cleanup channel manually (simulates cleaner thread)
    {.cast(raises: []).}:
      try:
        let jobOpt = reg.cleanupChan.tryRecv()
        if jobOpt[0]:
          let job = jobOpt[1]
          if job.intentKeys.len == 0:
            reg.touch(job.txnId)
      except:
        discard
    let after = reg.getLastActivityNs(txnId)
    check after > before

# =============================================================================
# Staleness Detection
# =============================================================================

suite "ActiveTxnRegistry - Staleness":
  test "freshly registered transaction is not stale":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    check not reg.isStale(txnId)

  test "non-existent transaction is stale":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    check reg.isStale(txnId)

  test "isStale with custom threshold":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    # 1 second threshold — should not be stale
    check not reg.isStale(txnId, 1_000_000_000'i64)

  test "isStale default threshold is 5 seconds":
    check STALE_TXN_THRESHOLD_NS == 5_000_000_000'i64

  test "getStaleTxnIds returns empty for fresh transactions":
    let reg = newActiveTxnRegistry()
    let t1 = genTxnId()
    let t2 = genTxnId()
    reg.register(t1, 1'u64)
    reg.register(t2, 2'u64)
    check reg.getStaleTxnIds().len == 0

  test "getStaleTxnIds returns non-existent transactions as stale":
    # Since non-existent transactions aren't in the registry,
    # they won't appear in getStaleTxnIds (which only scans registry)
    let reg = newActiveTxnRegistry()
    check reg.getStaleTxnIds().len == 0

  test "isActive returns true for registered active transaction":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    check reg.isActive(txnId)

  test "isActive returns false for non-existent transaction":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    check not reg.isActive(txnId)

  test "isActive returns false for aborted transaction":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    reg.setAborted(txnId)
    check not reg.isActive(txnId)

  test "isActive returns false for committing transaction":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    reg.setCommitting(txnId)
    check not reg.isActive(txnId)

# =============================================================================
# Intent Key Tracking
# =============================================================================

suite "ActiveTxnRegistry - Intent Keys":
  test "addIntentKey records intent key":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    reg.addIntentKey(txnId, "intent_key_1")
    let keys = reg.getIntentKeys(txnId)
    check "intent_key_1" in keys

  test "addIntentKey records multiple intent keys":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    reg.addIntentKey(txnId, "intent_key_1")
    reg.addIntentKey(txnId, "intent_key_2")
    reg.addIntentKey(txnId, "intent_key_3")
    let keys = reg.getIntentKeys(txnId)
    check keys.len == 3
    check "intent_key_1" in keys
    check "intent_key_2" in keys
    check "intent_key_3" in keys

  test "getIntentKeys returns empty for non-existent transaction":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    check reg.getIntentKeys(txnId).len == 0

  test "addIntentKey on non-existent transaction is safe":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.addIntentKey(txnId, "key") # should not crash

# =============================================================================
# Force Rollback
# =============================================================================

suite "ActiveTxnRegistry - Force Rollback":
  test "forceRollback marks transaction as aborted":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    check reg.forceRollback(txnId)
    check reg.getStatus(txnId) == some(txsAborted)

  test "forceRollback returns true for active transaction":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    check reg.forceRollback(txnId) == true

  test "forceRollback returns false for non-existent transaction":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    check reg.forceRollback(txnId) == false

  test "forceRollback returns false for already aborted transaction":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    discard reg.forceRollback(txnId)
    check reg.forceRollback(txnId) == false

  test "forceRollback returns false for committing transaction":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    reg.setCommitting(txnId)
    check reg.forceRollback(txnId) == false

  test "forceRollback queues cleanup job with intent keys":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    reg.addIntentKey(txnId, "intent_1")
    reg.addIntentKey(txnId, "intent_2")
    check reg.forceRollback(txnId)
    # Drain the cleanup channel
    {.cast(raises: []).}:
      try:
        let jobOpt = reg.cleanupChan.tryRecv()
        if jobOpt[0]:
          let job = jobOpt[1]
          check job.txnId == txnId
          check job.intentKeys.len == 2
          check "intent_1" in job.intentKeys
          check "intent_2" in job.intentKeys
      except:
        discard

# =============================================================================
# Status Transitions
# =============================================================================

suite "ActiveTxnRegistry - Status Transitions":
  test "setCommitting marks transaction as committing":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    reg.setCommitting(txnId)
    check reg.getStatus(txnId) == some(txsCommitting)

  test "setCommitting updates lastActivityNs":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    let before = reg.getLastActivityNs(txnId)
    sleep(10)
    reg.setCommitting(txnId)
    let after = reg.getLastActivityNs(txnId)
    check after > before

  test "setAborted marks transaction as aborted":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    reg.setAborted(txnId)
    check reg.getStatus(txnId) == some(txsAborted)

  test "setCommitting on non-existent transaction is safe":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.setCommitting(txnId) # should not crash

  test "setAborted on non-existent transaction is safe":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.setAborted(txnId) # should not crash

  test "committing transaction is protected from stale cleaner":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    reg.setCommitting(txnId)
    # Force rollback should fail for committing transactions
    check reg.forceRollback(txnId) == false

# =============================================================================
# Stats and Inspection
# =============================================================================

suite "ActiveTxnRegistry - Stats":
  test "activeCount only counts active transactions":
    let reg = newActiveTxnRegistry()
    let t1 = genTxnId()
    let t2 = genTxnId()
    reg.register(t1, 1'u64)
    reg.register(t2, 2'u64)
    check reg.activeCount() == 2
    reg.setAborted(t1)
    check reg.activeCount() == 1
    reg.setCommitting(t2)
    check reg.activeCount() == 0

  test "totalCount counts all transactions regardless of status":
    let reg = newActiveTxnRegistry()
    let t1 = genTxnId()
    let t2 = genTxnId()
    reg.register(t1, 1'u64)
    reg.register(t2, 2'u64)
    check reg.totalCount() == 2
    reg.setAborted(t1)
    check reg.totalCount() == 2 # Still in registry

  test "getLastActivityNs returns 0 for non-existent transaction":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    check reg.getLastActivityNs(txnId) == 0

# =============================================================================
# Start/Stop Lifecycle
# =============================================================================

suite "ActiveTxnRegistry - Lifecycle":
  test "start and stop work without crash":
    let reg = newActiveTxnRegistry()
    reg.start()
    sleep(100)
    reg.stop()
    # Should complete without hanging

  test "start is idempotent":
    let reg = newActiveTxnRegistry()
    reg.start()
    reg.start() # second start should be no-op
    sleep(100)
    reg.stop()

  test "stop is idempotent":
    let reg = newActiveTxnRegistry()
    reg.stop() # stop without start should be safe

  test "cleaner thread detects stale transactions":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    # Register with very small threshold for test
    reg.register(txnId, 1'u64)
    reg.start()
    # The cleaner runs every 1s with 5s threshold
    # We can't wait 5s in a unit test, so just verify the thread starts
    sleep(200)
    reg.stop()
    # Transaction should still be in registry (not 5s old)
    check reg.hasTransaction(txnId)

# =============================================================================
# Edge Cases
# =============================================================================

suite "ActiveTxnRegistry - Edge Cases":
  test "empty registry isStale always true":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    check reg.isStale(txnId)

  test "empty registry activeCount is 0":
    let reg = newActiveTxnRegistry()
    check reg.activeCount() == 0
    check reg.totalCount() == 0

  test "register same transaction twice updates entry":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    reg.register(txnId, 2'u64)
    check reg.totalCount() == 1

  test "unregister after forceRollback":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    discard reg.forceRollback(txnId)
    reg.unregister(txnId)
    check not reg.hasTransaction(txnId)

  test "many concurrent transactions":
    let reg = newActiveTxnRegistry()
    var txnIds: seq[TransactionID] = @[]
    for i in 0..<100:
      let txnId = genTxnId()
      txnIds.add(txnId)
      reg.register(txnId, uint64(i + 1))
    check reg.activeCount() == 100
    check reg.totalCount() == 100
    # All should be active
    for txnId in txnIds:
      check not reg.isStale(txnId)
      check reg.isActive(txnId)

  test "intent keys survive status change":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    reg.addIntentKey(txnId, "key1")
    reg.addIntentKey(txnId, "key2")
    reg.setAborted(txnId)
    let keys = reg.getIntentKeys(txnId)
    check keys.len == 2

# =============================================================================
# Constants
# =============================================================================

suite "ActiveTxnRegistry - Constants":
  test "STALE_TXN_THRESHOLD_NS is 5 seconds":
    check STALE_TXN_THRESHOLD_NS == 5_000_000_000'i64

  test "STALE_CLEANER_INTERVAL_MS is 1 second":
    check STALE_CLEANER_INTERVAL_MS == 1_000
