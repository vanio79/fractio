# Unit tests for intent scavenger (scavengeBackendIntents, IntentScavengerStats,
# ActiveTxnChecker) and GC thread lifecycle.
#
# Uses MockScavengerBackend (based on MockGCBackend pattern) for isolated testing
# without real LevelDB/WiscKey/NuRaft infrastructure.

import std/[unittest, tables, options, locks, atomics, sequtils, algorithm,
    strutils, os]
import fractio/core/types
import fractio/core/transaction
import fractio/protocol/txn_manager
import fractio/storage/mvcc/types as mvccTypes
import fractio/storage/backend
import fractio/protocol/raft_store

# =============================================================================
# Mock Storage Backend for Scavenger Testing
# =============================================================================

type
  MockScavengerBackend = ref object of StorageBackend
    ## Mock backend with full iterator and writeBatchNoSync support for
    ## intent scavenger testing.
    data: tables.Table[string, string]
    sortedKeys: seq[string]
    isOpenFlag: bool
    writeBatchCount: int
    writeBatchDeleteCount: int
    lock: Lock

  MockScavengerIterator = ref object of StorageIterator
    currentIndex: int
    validFlag: bool

proc getMockBackend(iter: MockScavengerIterator): MockScavengerBackend =
  cast[MockScavengerBackend](iter.backend)

proc newMockScavengerBackend(): MockScavengerBackend =
  new(result)
  result.data = initTable[string, string]()
  result.sortedKeys = @[]
  result.isOpenFlag = true
  result.writeBatchCount = 0
  result.writeBatchDeleteCount = 0
  initLock(result.lock)

method open(backend: MockScavengerBackend, config: StorageConfig): bool =
  acquire(backend.lock)
  backend.isOpenFlag = true
  release(backend.lock)
  return true

method close(backend: MockScavengerBackend) =
  acquire(backend.lock)
  backend.isOpenFlag = false
  release(backend.lock)

method isOpen(backend: MockScavengerBackend): bool =
  acquire(backend.lock)
  result = backend.isOpenFlag
  release(backend.lock)

method put(backend: MockScavengerBackend, key: string, value: string): bool =
  acquire(backend.lock)
  backend.data[key] = value
  if key notin backend.sortedKeys:
    backend.sortedKeys.add(key)
    backend.sortedKeys.sort()
  release(backend.lock)
  return true

method get(backend: MockScavengerBackend, key: string): Option[string] =
  acquire(backend.lock)
  if key in backend.data:
    result = some(backend.data[key])
  else:
    result = none(string)
  release(backend.lock)

method delete(backend: MockScavengerBackend, key: string): bool =
  acquire(backend.lock)
  if key in backend.data:
    backend.data.del(key)
    backend.sortedKeys = backend.sortedKeys.filterIt(it != key)
    release(backend.lock)
    return true
  release(backend.lock)
  return false

method exists(backend: MockScavengerBackend, key: string): bool =
  acquire(backend.lock)
  result = key in backend.data
  release(backend.lock)

method writeBatchNoSync(backend: MockScavengerBackend,
    pairs: seq[KeyValuePair], deletes: seq[string]): bool =
  acquire(backend.lock)
  backend.writeBatchCount += 1
  backend.writeBatchDeleteCount += deletes.len
  for kv in pairs:
    backend.data[kv.key] = kv.value
    if kv.key notin backend.sortedKeys:
      backend.sortedKeys.add(kv.key)
  for d in deletes:
    if d in backend.data:
      backend.data.del(d)
    backend.sortedKeys = backend.sortedKeys.filterIt(it != d)
  backend.sortedKeys.sort()
  release(backend.lock)
  return true

method newIterator(backend: MockScavengerBackend): StorageIterator =
  acquire(backend.lock)
  var iter: MockScavengerIterator
  new(iter)
  iter.backend = backend
  iter.currentIndex = -1
  iter.validFlag = false
  release(backend.lock)
  result = iter

method seekToFirst(iter: MockScavengerIterator): bool =
  let mb = iter.getMockBackend()
  acquire(mb.lock)
  if mb.sortedKeys.len > 0:
    iter.currentIndex = 0
    iter.validFlag = true
    release(mb.lock)
    return true
  iter.validFlag = false
  release(mb.lock)
  return false

method seekToLast(iter: MockScavengerIterator): bool =
  let mb = iter.getMockBackend()
  acquire(mb.lock)
  if mb.sortedKeys.len > 0:
    iter.currentIndex = mb.sortedKeys.len - 1
    iter.validFlag = true
    release(mb.lock)
    return true
  iter.validFlag = false
  release(mb.lock)
  return false

method seek(iter: MockScavengerIterator, key: string): bool =
  let mb = iter.getMockBackend()
  acquire(mb.lock)
  for i, k in mb.sortedKeys:
    if k >= key:
      iter.currentIndex = i
      iter.validFlag = true
      release(mb.lock)
      return true
  iter.validFlag = false
  release(mb.lock)
  return false

method next(iter: MockScavengerIterator): bool =
  let mb = iter.getMockBackend()
  acquire(mb.lock)
  if iter.currentIndex >= 0 and iter.currentIndex < mb.sortedKeys.len - 1:
    iter.currentIndex += 1
    iter.validFlag = true
    release(mb.lock)
    return true
  iter.validFlag = false
  release(mb.lock)
  return false

method prev(iter: MockScavengerIterator): bool =
  let mb = iter.getMockBackend()
  acquire(mb.lock)
  if iter.currentIndex > 0:
    iter.currentIndex -= 1
    iter.validFlag = true
    release(mb.lock)
    return true
  iter.validFlag = false
  release(mb.lock)
  return false

method valid(iter: MockScavengerIterator): bool =
  let mb = iter.getMockBackend()
  acquire(mb.lock)
  result = iter.validFlag and iter.currentIndex >= 0 and
           iter.currentIndex < mb.sortedKeys.len
  release(mb.lock)

method key(iter: MockScavengerIterator): string =
  let mb = iter.getMockBackend()
  acquire(mb.lock)
  if iter.validFlag and iter.currentIndex >= 0 and iter.currentIndex <
      mb.sortedKeys.len:
    result = mb.sortedKeys[iter.currentIndex]
  else:
    result = ""
  release(mb.lock)

method value(iter: MockScavengerIterator): string =
  let mb = iter.getMockBackend()
  acquire(mb.lock)
  if iter.validFlag and iter.currentIndex >= 0 and iter.currentIndex <
      mb.sortedKeys.len:
    let k = mb.sortedKeys[iter.currentIndex]
    if k in mb.data:
      result = mb.data[k]
    else:
      result = ""
  else:
    result = ""
  release(mb.lock)

method destroy(iter: MockScavengerIterator) =
  iter.validFlag = false
  iter.currentIndex = -1

# =============================================================================
# Helper Functions
# =============================================================================

proc addProtocolIntent(backend: MockScavengerBackend, txnId: uint64,
    userKey: string, timestamp: int64, value: string = "test_data") =
  ## Add a protocol-layer intent key: "\x00INTENT\x00<8-byte txnId BE><userKey>"
  let intentKey = encodeIntentKey(txnId, userKey)
  let txnUlid = genTransactionIDLocal()
  let mvccValue = encodeMVCCValue(value, timestamp, false, txnUlid)
  discard backend.put(intentKey, mvccValue)

proc addMvccIntent(backend: MockScavengerBackend, userKey: string,
    txnId: TransactionID, timestamp: int64, value: string = "test_data") =
  ## Add an MVCC-layer intent key: <userKey> + "\x00\x01" + 16-byte ULID txnId
  let intentKey = mvccTypes.encodeIntentKey(userKey, txnId)
  let mvccValue = encodeMVCCValue(value, timestamp, false, txnId)
  discard backend.put(intentKey, mvccValue)

proc addVersionKey(backend: MockScavengerBackend, userKey: string,
    timestamp: int64, value: string = "committed_data") =
  ## Add a regular MVCC version key (not an intent).
  let versionKey = mvccTypes.makeVersionKey(userKey, timestamp)
  let txnUlid = genTransactionIDLocal()
  let mvccValue = encodeMVCCValue(value, timestamp, false, txnUlid)
  discard backend.put(versionKey, mvccValue)

proc addRawKey(backend: MockScavengerBackend, key: string, value: string) =
  ## Add a raw key-value pair (no MVCC encoding).
  discard backend.put(key, value)

# =============================================================================
# IntentScavengerStats Tests
# =============================================================================

suite "IntentScavengerStats":
  test "default construction":
    let stats = IntentScavengerStats()
    check stats.intentsScanned == 0
    check stats.orphanIntentsCleaned == 0
    check stats.protocolIntentsScanned == 0
    check stats.mvccIntentsScanned == 0
    check stats.scanCount == 0
    check stats.lastScanTimeNs == 0

  test "field assignment":
    var stats = IntentScavengerStats()
    stats.intentsScanned = 10
    stats.orphanIntentsCleaned = 3
    stats.protocolIntentsScanned = 4
    stats.mvccIntentsScanned = 6
    stats.scanCount = 1
    stats.lastScanTimeNs = 1234567890
    check stats.intentsScanned == 10
    check stats.orphanIntentsCleaned == 3
    check stats.protocolIntentsScanned == 4
    check stats.mvccIntentsScanned == 6
    check stats.scanCount == 1
    check stats.lastScanTimeNs == 1234567890

  test "accumulation":
    var stats = IntentScavengerStats()
    stats.intentsScanned += 5
    stats.intentsScanned += 3
    stats.orphanIntentsCleaned += 2
    check stats.intentsScanned == 8
    check stats.orphanIntentsCleaned == 2

# =============================================================================
# ActiveTxnChecker Tests
# =============================================================================

suite "ActiveTxnChecker":
  test "nil callback - old orphaned intents are cleaned":
    ## When activeTxnChecker is nil, the scavenger should still work
    ## (it treats nil as "no active txn check available" and relies on age)
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64 # 200 seconds in ns
    let oldTxn = genTransactionIDLocal()
    addMvccIntent(backend, "key1", oldTxn, 100_000_000_000'i64) # 100s old

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.intentsScanned >= 1
    check stats.mvccIntentsScanned >= 1
    # With nil checker, orphaned intents should be cleaned (age-based only)
    check stats.orphanIntentsCleaned >= 1

  test "callback reports active transaction - intent preserved":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64 # 200 seconds in ns
    let activeTxn = genTransactionIDLocal()
    addMvccIntent(backend, "key1", activeTxn, 100_000_000_000'i64) # old but active

    # Checker says this txn IS active
    let checker: ActiveTxnChecker = proc(txnId: TransactionID): bool {.gcsafe,
        raises: [].} =
      txnId == activeTxn

    let stats = scavengeBackendIntents(backend, checker, nowNs)
    check stats.intentsScanned >= 1
    # Active txn intent should NOT be cleaned
    check stats.orphanIntentsCleaned == 0

  test "callback reports inactive transaction - intent cleaned":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    let inactiveTxn = genTransactionIDLocal()
    addMvccIntent(backend, "key1", inactiveTxn, 100_000_000_000'i64)

    # Checker says this txn is NOT active
    let checker: ActiveTxnChecker = proc(txnId: TransactionID): bool {.gcsafe,
        raises: [].} =
      false

    let stats = scavengeBackendIntents(backend, checker, nowNs)
    check stats.intentsScanned >= 1
    check stats.orphanIntentsCleaned >= 1

  test "callback distinguishes between multiple transactions":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    let activeTxn = genTransactionIDLocal()
    sleep(2) # Ensure distinct ULIDs
    let inactiveTxn = genTransactionIDLocal()
    addMvccIntent(backend, "key1", activeTxn, 100_000_000_000'i64)
    addMvccIntent(backend, "key2", inactiveTxn, 100_000_000_000'i64)

    let checker: ActiveTxnChecker = proc(txnId: TransactionID): bool {.gcsafe,
        raises: [].} =
      txnId == activeTxn

    let stats = scavengeBackendIntents(backend, checker, nowNs)
    check stats.intentsScanned == 2
    check stats.orphanIntentsCleaned == 1

# =============================================================================
# scavengeBackendIntents - Protocol-Layer Intent Tests
# =============================================================================

suite "scavengeBackendIntents - Protocol-Layer Intents":
  test "old protocol intent is cleaned":
    let backend = newMockScavengerBackend()
    # nowNs = 200s, intent timestamp = 100s (100s old > 60s threshold)
    let nowNs = 200_000_000_000'i64
    addProtocolIntent(backend, 42'u64, "user_key1", 100_000_000_000'i64)

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.protocolIntentsScanned == 1
    check stats.intentsScanned == 1
    check stats.orphanIntentsCleaned == 1
    check stats.scanCount == 1

  test "young protocol intent is NOT cleaned":
    let backend = newMockScavengerBackend()
    # nowNs = 200s, intent timestamp = 180s (20s old < 60s threshold)
    let nowNs = 200_000_000_000'i64
    addProtocolIntent(backend, 42'u64, "user_key1", 180_000_000_000'i64)

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.protocolIntentsScanned == 1
    check stats.orphanIntentsCleaned == 0

  test "protocol intent exactly at cutoff is NOT cleaned":
    let backend = newMockScavengerBackend()
    # INTENT_SCAVENGE_AGE_NS = 60_000_000_000 (60s)
    # The check is intentTs < cutoffNs, so exactly-at-cutoff should NOT be cleaned
    let nowNs = 200_000_000_000'i64
    let intentTs = nowNs - INTENT_SCAVENGE_AGE_NS
    addProtocolIntent(backend, 42'u64, "user_key1", intentTs)

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.protocolIntentsScanned == 1
    check stats.orphanIntentsCleaned == 0

  test "protocol intent 1ns before cutoff IS cleaned":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    let intentTs = nowNs - INTENT_SCAVENGE_AGE_NS - 1
    addProtocolIntent(backend, 42'u64, "user_key1", intentTs)

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.protocolIntentsScanned == 1
    check stats.orphanIntentsCleaned == 1

  test "protocol intent with zero timestamp is NOT cleaned":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    addProtocolIntent(backend, 42'u64, "user_key1", 0'i64)

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.protocolIntentsScanned == 1
    check stats.orphanIntentsCleaned == 0

  test "multiple protocol intents - mix of old and young":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    addProtocolIntent(backend, 1'u64, "old_key1", 50_000_000_000'i64) # 150s old -> cleaned
    addProtocolIntent(backend, 2'u64, "old_key2", 100_000_000_000'i64) # 100s old -> cleaned
    addProtocolIntent(backend, 3'u64, "new_key1", 180_000_000_000'i64) # 20s old -> kept
    addProtocolIntent(backend, 4'u64, "new_key2", 195_000_000_000'i64) # 5s old -> kept

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.protocolIntentsScanned == 4
    check stats.intentsScanned == 4
    check stats.orphanIntentsCleaned == 2

# =============================================================================
# scavengeBackendIntents - MVCC-Layer Intent Tests
# =============================================================================

suite "scavengeBackendIntents - MVCC-Layer Intents":
  test "old MVCC intent is cleaned":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    let oldTxn = genTransactionIDLocal()
    addMvccIntent(backend, "user_key1", oldTxn, 100_000_000_000'i64)

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.mvccIntentsScanned == 1
    check stats.intentsScanned == 1
    check stats.orphanIntentsCleaned == 1

  test "young MVCC intent is NOT cleaned":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    let youngTxn = genTransactionIDLocal()
    addMvccIntent(backend, "user_key1", youngTxn, 180_000_000_000'i64)

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.mvccIntentsScanned == 1
    check stats.orphanIntentsCleaned == 0

  test "MVCC intent with active transaction is NOT cleaned even if old":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    let activeTxn = genTransactionIDLocal()
    addMvccIntent(backend, "user_key1", activeTxn, 50_000_000_000'i64) # very old

    let checker: ActiveTxnChecker = proc(txnId: TransactionID): bool {.gcsafe,
        raises: [].} =
      txnId == activeTxn

    let stats = scavengeBackendIntents(backend, checker, nowNs)
    check stats.mvccIntentsScanned == 1
    check stats.orphanIntentsCleaned == 0

  test "MVCC intent with inactive transaction IS cleaned if old":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    let inactiveTxn = genTransactionIDLocal()
    addMvccIntent(backend, "user_key1", inactiveTxn, 50_000_000_000'i64)

    let checker: ActiveTxnChecker = proc(txnId: TransactionID): bool {.gcsafe,
        raises: [].} =
      false # All transactions inactive

    let stats = scavengeBackendIntents(backend, checker, nowNs)
    check stats.mvccIntentsScanned == 1
    check stats.orphanIntentsCleaned == 1

  test "MVCC intent at exactly the age cutoff is NOT cleaned":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    let cutoffTxn = genTransactionIDLocal()
    let cutoffTs = nowNs - INTENT_SCAVENGE_AGE_NS
    addMvccIntent(backend, "user_key1", cutoffTxn, cutoffTs)

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.mvccIntentsScanned == 1
    check stats.orphanIntentsCleaned == 0

  test "MVCC intent 1ns before cutoff IS cleaned":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    let oldTxn = genTransactionIDLocal()
    let justOldTs = nowNs - INTENT_SCAVENGE_AGE_NS - 1
    addMvccIntent(backend, "user_key1", oldTxn, justOldTs)

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.mvccIntentsScanned == 1
    check stats.orphanIntentsCleaned == 1

  test "MVCC intent with zero timestamp is NOT cleaned":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    let zeroTsTxn = genTransactionIDLocal()
    addMvccIntent(backend, "user_key1", zeroTsTxn, 0'i64)

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.mvccIntentsScanned == 1
    check stats.orphanIntentsCleaned == 0

# =============================================================================
# scavengeBackendIntents - Mixed Key Types
# =============================================================================

suite "scavengeBackendIntents - Mixed Key Types":
  test "regular version keys are not affected":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    # Add a regular version key (not an intent)
    addVersionKey(backend, "regular_key1", 50_000_000_000'i64)
    addVersionKey(backend, "regular_key2", 150_000_000_000'i64)

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    # Version keys should not be counted as intents
    check stats.intentsScanned == 0
    check stats.orphanIntentsCleaned == 0

  test "raw non-MVCC keys are not affected":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    addRawKey(backend, "some_key", "some_value")

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.intentsScanned == 0
    check stats.orphanIntentsCleaned == 0

  test "mix of protocol intents, MVCC intents, and version keys":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    let oldTxn = genTransactionIDLocal()
    sleep(2)
    let youngTxn = genTransactionIDLocal()

    # Old protocol intent -> cleaned
    addProtocolIntent(backend, 1'u64, "proto_old", 100_000_000_000'i64)
    # Young protocol intent -> kept
    addProtocolIntent(backend, 2'u64, "proto_new", 180_000_000_000'i64)
    # Old MVCC intent (no checker = treated as orphaned) -> cleaned
    addMvccIntent(backend, "mvcc_old", oldTxn, 50_000_000_000'i64)
    # Young MVCC intent -> kept
    addMvccIntent(backend, "mvcc_new", youngTxn, 180_000_000_000'i64)
    # Regular version keys -> untouched
    addVersionKey(backend, "version1", 100_000_000_000'i64)
    addVersionKey(backend, "version2", 150_000_000_000'i64)

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.protocolIntentsScanned == 2
    check stats.mvccIntentsScanned == 2
    check stats.intentsScanned == 4
    check stats.orphanIntentsCleaned == 2 # proto_old + mvcc_old

  test "keys are actually deleted from backend":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    addProtocolIntent(backend, 1'u64, "old_key", 50_000_000_000'i64)
    addRawKey(backend, "regular", "value")

    check backend.data.len == 2
    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.orphanIntentsCleaned == 1
    # The old intent should be removed; the regular key should remain
    check backend.data.len == 1
    check "regular" in backend.data

# =============================================================================
# scavengeBackendIntents - Edge Cases
# =============================================================================

suite "scavengeBackendIntents - Edge Cases":
  test "empty backend - no crash, zero stats":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.intentsScanned == 0
    check stats.orphanIntentsCleaned == 0
    check stats.protocolIntentsScanned == 0
    check stats.mvccIntentsScanned == 0
    check stats.scanCount == 1

  test "nil backend - returns zero stats":
    let nowNs = 200_000_000_000'i64
    let stats = scavengeBackendIntents(nil, nil, nowNs)
    check stats.intentsScanned == 0
    check stats.orphanIntentsCleaned == 0
    check stats.scanCount == 0

  test "closed backend - returns zero stats":
    let backend = newMockScavengerBackend()
    backend.close()
    let nowNs = 200_000_000_000'i64

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.intentsScanned == 0
    check stats.scanCount == 0

  test "lastScanTimeNs is set to nowNs":
    let backend = newMockScavengerBackend()
    let nowNs = 987_654_321_000'i64

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.lastScanTimeNs == nowNs

  test "scanCount is always 1 per call":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.scanCount == 1

  test "MVCC intent with non-MVCC value (bad value) is scanned but not cleaned":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    let txnId = genTransactionIDLocal()
    # Manually create an MVCC intent key but with a non-MVCC value
    let intentKey = mvccTypes.encodeIntentKey("user_key1", txnId)
    discard backend.put(intentKey, "not_mvcc_value")

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.mvccIntentsScanned == 1
    # Non-MVCC value means timestamp is 0, so not cleaned
    check stats.orphanIntentsCleaned == 0

  test "protocol intent with non-MVCC value is scanned but not cleaned":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    let intentKey = encodeIntentKey(42'u64, "user_key1")
    discard backend.put(intentKey, "raw_value_no_mvcc_header")

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.protocolIntentsScanned == 1
    check stats.orphanIntentsCleaned == 0

  test "writeBatchNoSync is called once for batched deletes":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    # Add multiple old protocol intents
    addProtocolIntent(backend, 1'u64, "key1", 50_000_000_000'i64)
    addProtocolIntent(backend, 2'u64, "key2", 60_000_000_000'i64)
    addProtocolIntent(backend, 3'u64, "key3", 70_000_000_000'i64)

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.orphanIntentsCleaned == 3
    # All deletes should be batched into a single writeBatchNoSync call
    check backend.writeBatchCount == 1
    check backend.writeBatchDeleteCount == 3

  test "writeBatchNoSync is not called when no intents to delete":
    let backend = newMockScavengerBackend()
    let nowNs = 200_000_000_000'i64
    # Add only young intents
    addProtocolIntent(backend, 1'u64, "key1", 180_000_000_000'i64)

    let stats = scavengeBackendIntents(backend, nil, nowNs)
    check stats.orphanIntentsCleaned == 0
    check backend.writeBatchCount == 0

# =============================================================================
# Constants
# =============================================================================

suite "Intent Scavenger Constants":
  test "INTENT_SCAVENGE_AGE_NS is 60 seconds":
    check INTENT_SCAVENGE_AGE_NS == 60_000_000_000'i64

  test "GC_SCAN_INTERVAL_MS is 30 seconds":
    check GC_SCAN_INTERVAL_MS == 30_000

  test "INTENT_SCAVENGE_AGE_NS is 2x DEFAULT_TXN_TIMEOUT_MS":
    ## Safety guarantee: the scavenge age is 2x the default txn timeout,
    ## ensuring we never clean up an intent for a still-valid transaction.
    check INTENT_SCAVENGE_AGE_NS == int64(DEFAULT_TXN_TIMEOUT_MS) * 2_000_000
