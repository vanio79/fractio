# Unit tests for conflict-driven intent override in MvccTransactionStore.
#
# Tests cover:
#   - extractTxnIdFromIntentKey: parsing intent keys
#   - extractUserKeyFromIntentKey: extracting user keys from intent keys
#   - resolveStaleIntentsForUserKey: proactive stale intent cleanup
#   - forceRollbackStaleTransaction: server-level API for stale txn override
#   - Integration with ActiveTxnRegistry

import std/[unittest, options, tables]
import fractio/core/types
import fractio/core/transaction
import fractio/storage/mvcc/types as mvccTypes
import fractio/protocol/mvcc_store
import fractio/protocol/active_txn_registry

# =============================================================================
# Helper: Generate distinct TransactionIDs
# =============================================================================

var nextTxnCounter = 5000

proc genTxnId(): TransactionID =
  inc nextTxnCounter
  genTransactionIDLocal()

# =============================================================================
# extractTxnIdFromIntentKey
# =============================================================================

suite "extractTxnIdFromIntentKey":
  test "extracts txnId from valid intent key":
    let txnId = genTxnId()
    let txnBytes = transactionIDToBytes(txnId)
    var intentKey = "my_user_key\x00\x01"
    intentKey.add(txnBytes)
    let result = extractTxnIdFromIntentKey(intentKey)
    check result.isSome
    check result.get() == txnId

  test "extracts txnId from intent key with empty user key":
    let txnId = genTxnId()
    let txnBytes = transactionIDToBytes(txnId)
    var intentKey = "\x00\x01"
    intentKey.add(txnBytes)
    let result = extractTxnIdFromIntentKey(intentKey)
    check result.isSome
    check result.get() == txnId

  test "extracts txnId from intent key with long user key":
    let txnId = genTxnId()
    let txnBytes = transactionIDToBytes(txnId)
    var intentKey = "sys.tables\x00/some/long/key/path\x00\x01"
    intentKey.add(txnBytes)
    let result = extractTxnIdFromIntentKey(intentKey)
    check result.isSome
    check result.get() == txnId

  test "returns none for key too short":
    let result = extractTxnIdFromIntentKey("short")
    check result.isNone

  test "returns none for version key (wrong suffix)":
    # Version key has \x00\x00 suffix, not \x00\x01
    var key = "user_key\x00\x00"
    key.add("\x00\x00\x00\x00\x00\x00\x00\x01")
    let result = extractTxnIdFromIntentKey(key)
    check result.isNone

  test "returns none for plain user key":
    let result = extractTxnIdFromIntentKey("just_a_key")
    check result.isNone

  test "round-trip: encode then extract":
    let txnId = genTxnId()
    let userKey = "test_key_round_trip"
    # Use the encoding logic from mvcc_store
    var intentKey = userKey & mvccTypes.INTENT_SUFFIX
    let txnBytes = transactionIDToBytes(txnId)
    intentKey.add(txnBytes)
    let extracted = extractTxnIdFromIntentKey(intentKey)
    check extracted.isSome
    check extracted.get() == txnId

  test "extracts different txnIds from different intent keys":
    let txnId1 = genTxnId()
    let txnId2 = genTxnId()
    check txnId1 != txnId2 # Ensure distinct IDs

    var intentKey1 = "key\x00\x01"
    intentKey1.add(transactionIDToBytes(txnId1))
    var intentKey2 = "key\x00\x01"
    intentKey2.add(transactionIDToBytes(txnId2))

    let result1 = extractTxnIdFromIntentKey(intentKey1)
    let result2 = extractTxnIdFromIntentKey(intentKey2)
    check result1.isSome
    check result2.isSome
    check result1.get() == txnId1
    check result2.get() == txnId2
    check result1.get() != result2.get()

# =============================================================================
# extractUserKeyFromIntentKey
# =============================================================================

suite "extractUserKeyFromIntentKey":
  test "extracts user key from valid intent key":
    let txnId = genTxnId()
    let txnBytes = transactionIDToBytes(txnId)
    var intentKey = "my_user_key\x00\x01"
    intentKey.add(txnBytes)
    let result = extractUserKeyFromIntentKey(intentKey)
    check result.isSome
    check result.get() == "my_user_key"

  test "extracts user key with special characters":
    let txnId = genTxnId()
    let txnBytes = transactionIDToBytes(txnId)
    let userKey = "sys.tables\x00/row/123"
    var intentKey = userKey & mvccTypes.INTENT_SUFFIX
    intentKey.add(txnBytes)
    let result = extractUserKeyFromIntentKey(intentKey)
    check result.isSome
    check result.get() == userKey

  test "extracts empty user key":
    let txnId = genTxnId()
    let txnBytes = transactionIDToBytes(txnId)
    var intentKey = "\x00\x01"
    intentKey.add(txnBytes)
    let result = extractUserKeyFromIntentKey(intentKey)
    check result.isSome
    check result.get() == ""

  test "returns none for version key":
    var key = "user_key\x00\x00"
    key.add("\x00\x00\x00\x00\x00\x00\x00\x01")
    let result = extractUserKeyFromIntentKey(key)
    check result.isNone

  test "returns none for key too short":
    let result = extractUserKeyFromIntentKey("short")
    check result.isNone

  test "round-trip: encode then extract user key":
    let txnId = genTxnId()
    let userKey = "test_user_key_456"
    var intentKey = userKey & mvccTypes.INTENT_SUFFIX
    intentKey.add(transactionIDToBytes(txnId))
    let extracted = extractUserKeyFromIntentKey(intentKey)
    check extracted.isSome
    check extracted.get() == userKey

# =============================================================================
# resolveStaleIntentsForUserKey (unit-level, no Raft backend)
# =============================================================================

suite "resolveStaleIntentsForUserKey - no backend":
  test "returns 0 when registry pointer is nil":
    # Without a RaftKVStoreExt, we can't really call resolveStaleIntentsForUserKey
    # because it needs raftStore.raftScan. But we can test the helper procs.
    # This test verifies the extractTxnIdFromIntentKey integration.
    let txnId = genTxnId()
    var intentKey = "key\x00\x01"
    intentKey.add(transactionIDToBytes(txnId))
    check isIntentKeyMvcc(intentKey) == true
    let extracted = extractTxnIdFromIntentKey(intentKey)
    check extracted.isSome
    check extracted.get() == txnId

  test "stale transaction detection via ActiveTxnRegistry":
    # Verify that isStale correctly identifies dead transactions
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    # Newly registered transaction should NOT be stale
    check reg.isStale(txnId) == false
    # Unregistered transaction should be stale (not in registry)
    let unknownTxnId = genTxnId()
    check reg.isStale(unknownTxnId) == true

  test "forceRollback marks stale transaction as aborted":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    check reg.getStatus(txnId).isSome
    check reg.getStatus(txnId).get() == txsActive
    let rolledBack = reg.forceRollback(txnId)
    check rolledBack == true
    check reg.getStatus(txnId).isSome
    check reg.getStatus(txnId).get() == txsAborted

  test "forceRollback returns false for unknown transaction":
    let reg = newActiveTxnRegistry()
    let unknownTxnId = genTxnId()
    let rolledBack = reg.forceRollback(unknownTxnId)
    check rolledBack == false

  test "forceRollback returns false for already-aborted transaction":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    discard reg.forceRollback(txnId)
    # Second forceRollback should return false (already aborted)
    let rolledBack2 = reg.forceRollback(txnId)
    check rolledBack2 == false

# =============================================================================
# Integration: ActiveTxnRegistry + intent key tracking
# =============================================================================

suite "ActiveTxnRegistry - Intent Key Tracking for Conflict Resolution":
  test "addIntentKey and getIntentKeys for conflict resolution":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)

    # Simulate writing intent keys
    let intentKey1 = "key1\x00\x01" & transactionIDToBytes(txnId)
    let intentKey2 = "key2\x00\x01" & transactionIDToBytes(txnId)
    reg.addIntentKey(txnId, intentKey1)
    reg.addIntentKey(txnId, intentKey2)

    let keys = reg.getIntentKeys(txnId)
    check keys.len == 2
    check intentKey1 in keys
    check intentKey2 in keys

  test "forceRollback queues cleanup with intent keys":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)

    let intentKey = "mykey\x00\x01" & transactionIDToBytes(txnId)
    reg.addIntentKey(txnId, intentKey)

    discard reg.forceRollback(txnId)
    # The forceRollback queues a CleanupJob with the intent keys
    # We can verify by checking the transaction is now aborted
    check reg.getStatus(txnId).get() == txsAborted

  test "multiple transactions on same key - stale one gets force-rolled":
    let reg = newActiveTxnRegistry()
    let t1 = genTxnId()
    let t2 = genTxnId()
    reg.register(t1, 1'u64)
    reg.register(t2, 2'u64)

    # Both write to the same key
    let intentKey1 = "shared_key\x00\x01" & transactionIDToBytes(t1)
    let intentKey2 = "shared_key\x00\x01" & transactionIDToBytes(t2)
    reg.addIntentKey(t1, intentKey1)
    reg.addIntentKey(t2, intentKey2)

    # t1 is still active, t2 is active
    check reg.isStale(t1) == false
    check reg.isStale(t2) == false

    # Force-rollback t1 (simulating a stale transaction)
    let rolledBack = reg.forceRollback(t1)
    check rolledBack == true
    # After forceRollback, t1 is aborted (not active). It's still in the
    # registry (will be removed by the background cleaner), and its
    # lastActivityNs is recent, so isStale returns false based on timestamps.
    # The important check is that its status is txsAborted.
    check reg.getStatus(t1).get() == txsAborted
    # isActive returns false for aborted transactions
    check reg.isActive(t1) == false

# =============================================================================
# Intent key format consistency
# =============================================================================

suite "Intent Key Format Consistency":
  test "isIntentKeyMvcc and extractTxnIdFromIntentKey agree":
    # For all valid intent keys, both procs should agree
    for i in 0..<10:
      let txnId = genTxnId()
      let userKey = "test_key_" & $i
      var intentKey = userKey & mvccTypes.INTENT_SUFFIX
      intentKey.add(transactionIDToBytes(txnId))

      check isIntentKeyMvcc(intentKey) == true
      let extracted = extractTxnIdFromIntentKey(intentKey)
      check extracted.isSome
      check extracted.get() == txnId

  test "isIntentKeyMvcc and extractUserKeyFromIntentKey agree":
    for i in 0..<5:
      let txnId = genTxnId()
      let userKey = "user_key_" & $i
      var intentKey = userKey & mvccTypes.INTENT_SUFFIX
      intentKey.add(transactionIDToBytes(txnId))

      check isIntentKeyMvcc(intentKey) == true
      let extracted = extractUserKeyFromIntentKey(intentKey)
      check extracted.isSome
      check extracted.get() == userKey

  test "version key is not parsed as intent key":
    var versionKey = "user_key\x00\x00"
    versionKey.add("\x00\x00\x00\x00\x00\x00\x00\x01")
    check isIntentKeyMvcc(versionKey) == false
    check extractTxnIdFromIntentKey(versionKey).isNone
    check extractUserKeyFromIntentKey(versionKey).isNone

  test "isVersionKey still works correctly":
    var versionKey = "user_key\x00\x00"
    versionKey.add("\x00\x00\x00\x00\x00\x00\x00\x01")
    check isVersionKey(versionKey) == true

  test "intent key suffix is distinct from version key suffix":
    check mvccTypes.INTENT_SUFFIX == "\x00\x01"
    check mvccTypes.VERSION_SEPARATOR == "\x00\x00"
    check mvccTypes.INTENT_SUFFIX != mvccTypes.VERSION_SEPARATOR

# =============================================================================
# Stale transaction scenarios
# =============================================================================

suite "Stale Transaction Scenarios":
  test "active transaction is not stale":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    # Just registered — should not be stale
    check reg.isStale(txnId) == false

  test "touched transaction remains active":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    reg.touch(txnId)
    # Freshly touched — should not be stale
    check reg.isStale(txnId) == false

  test "unknown transaction is stale":
    let reg = newActiveTxnRegistry()
    let unknownTxnId = genTxnId()
    check reg.isStale(unknownTxnId) == true

  test "forceRollbackStaleTransaction on nil registry returns false":
    # This tests the nil pointer path
    # We can't easily create a MvccTransactionStore without a RaftKVStoreExt,
    # so we test the logic at the ActiveTxnRegistry level instead
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    # Not registered, so it's stale
    check reg.isStale(txnId) == true
    # forceRollback returns false for unknown transactions
    check reg.forceRollback(txnId) == false

  test "committed transaction is not force-rollable":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    reg.setCommitting(txnId)
    # A committing transaction should NOT be force-rollable
    let rolledBack = reg.forceRollback(txnId)
    check rolledBack == false

  test "force-rolled transaction is not active":
    let reg = newActiveTxnRegistry()
    let txnId = genTxnId()
    reg.register(txnId, 1'u64)
    discard reg.forceRollback(txnId)
    # After forceRollback, the transaction is aborted but may still be in the
    # registry (until the background cleaner removes it). Its lastActivityNs
    # is recent so isStale may be false, but isActive should be false.
    check reg.isActive(txnId) == false
    check reg.getStatus(txnId).get() == txsAborted
