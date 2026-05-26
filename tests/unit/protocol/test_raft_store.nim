# Unit tests for fractio/protocol/raft_store.nim
# Tests intent key encoding, routing, and result types

import std/[unittest, tables, hashes, strutils]
import fractio/protocol/raft_store
import fractio/core/types
import fractio/distributed/raft/group_types
import fractio/distributed/meta/system_tables

suite "Intent Key Encoding":

  test "encodeIntentKey basic":
    let txnId = 12345'u64
    let userKey = "test_key"
    let encoded = encodeIntentKey(txnId, userKey)
    check encoded.len == INTENT_PREFIX.len + 8 + userKey.len
    check encoded.startsWith(INTENT_PREFIX)

  test "encodeIntentKey empty user key":
    let txnId = 0'u64
    let encoded = encodeIntentKey(txnId, "")
    check encoded.len == INTENT_PREFIX.len + 8

  test "encodeIntentKey with binary user key":
    let txnId = 0xFFFFFFFF'u64
    let userKey = "\x00\x01\x02"
    let encoded = encodeIntentKey(txnId, userKey)
    check encoded.len == INTENT_PREFIX.len + 8 + 3

  test "decodeIntentTxnId":
    let txnId = 0x123456789ABCDEF0'u64
    let encoded = encodeIntentKey(txnId, "key")
    let decoded = decodeIntentTxnId(encoded)
    check decoded == txnId

  test "decodeIntentTxnId zero":
    let encoded = encodeIntentKey(0'u64, "key")
    let decoded = decodeIntentTxnId(encoded)
    check decoded == 0'u64

  test "decodeIntentUserKey":
    let userKey = "user_key_data"
    let encoded = encodeIntentKey(100'u64, userKey)
    let decoded = decodeIntentUserKey(encoded)
    check decoded == userKey

  test "decodeIntentUserKey empty":
    let encoded = encodeIntentKey(100'u64, "")
    let decoded = decodeIntentUserKey(encoded)
    check decoded == ""

suite "Coordinator Key Encoding":

  test "encodeCoordKey":
    let txnId = 999'u64
    let encoded = encodeCoordKey(txnId)
    check encoded.len == COORD_PREFIX.len + 8
    check encoded.startsWith(COORD_PREFIX)

  test "isCoordKey true":
    let encoded = encodeCoordKey(100'u64)
    check isCoordKey(encoded) == true

  test "isCoordKey false":
    let key = "regular_key"
    check isCoordKey(key) == false

  test "isCoordKey false - wrong prefix":
    let key = "\x00INTENT\x00\x00\x00\x00\x00\x00\x00\x00\x01"
    check isCoordKey(key) == false

suite "Intent Key Detection":

  test "isIntentKey true":
    let encoded = encodeIntentKey(1'u64, "key")
    check isIntentKey(encoded) == true

  test "isIntentKey false - regular key":
    check isIntentKey("regular_key") == false

  test "isIntentKey false - too short":
    check isIntentKey("\x00INTENT") == false

suite "MVCC Suffix Detection":

  test "isMVCCSuffixKey version key":
    var key = "user"
    key.add('\x00')
    key.add('\x00')
    for i in 0..<8:
      key.add('\x00')
    check key.len >= 10
    check isMVCCSuffixKey(key) == true

  test "isMVCCSuffixKey intent key":
    var key = "user"
    key.add('\x00')
    key.add('\x01')
    for i in 0..<8:
      key.add('\x00')
    check key.len >= 10
    check isMVCCSuffixKey(key) == true

  test "isMVCCSuffixKey false - too short":
    check isMVCCSuffixKey("short") == false

  test "isMVCCSuffixKey false - no suffix":
    check isMVCCSuffixKey("regular_key_no_suffix") == false

suite "Strip MVCC Suffix":

  test "stripMVCCSuffix version key":
    var key = "user_key"
    key.add('\x00')
    key.add('\x00')
    for i in 0..<8:
      key.add('\x00')
    let stripped = stripMVCCSuffix(key)
    check stripped == "user_key"

  test "stripMVCCSuffix intent key":
    var key = "user_key"
    key.add('\x00')
    key.add('\x01')
    for i in 0..<8:
      key.add('\x00')
    let stripped = stripMVCCSuffix(key)
    check stripped == "user_key"

  test "stripMVCCSuffix no change":
    let key = "regular_key"
    let stripped = stripMVCCSuffix(key)
    check stripped == key

suite "Route to Group":

  test "routeToGroup single group":
    let groupId = genGroupIDLocal()
    let groupIds = @[groupId]
    let result = routeToGroup("any_key", groupIds)
    check result == groupId

  test "routeToGroup deterministic routing":
    let g1 = genGroupIDLocal()
    let g2 = genGroupIDLocal()
    let g3 = genGroupIDLocal()
    let groupIds = @[g1, g2, g3]
    let r1 = routeToGroup("key1", groupIds)
    let r2 = routeToGroup("key1", groupIds)
    check r1 == r2

  test "routeToGroup empty groups returns META_GROUP_ID":
    let groupIds: seq[GroupID] = @[]
    let result = routeToGroup("key", groupIds)
    check result == META_GROUP_ID

  test "routeToGroup different keys different groups":
    let g1 = genGroupIDLocal()
    let g2 = genGroupIDLocal()
    let groupIds = @[g1, g2]
    let r1 = routeToGroup("key_a", groupIds)
    let r2 = routeToGroup("key_b_different_hash", groupIds)
    check r1 in groupIds
    check r2 in groupIds

suite "RaftStoreError Types":

  test "rseNotLeader":
    let err = RaftStoreError(kind: rseNotLeader, msg: "not leader",
        leaderHint: 42)
    check err.kind == rseNotLeader
    check err.msg == "not leader"
    check err.leaderHint == 42

  test "rseGroupNotFound":
    let err = RaftStoreError(kind: rseGroupNotFound, msg: "no group", leaderHint: 0)
    check err.kind == rseGroupNotFound
    check err.leaderHint == 0

  test "rseTimeout":
    let err = RaftStoreError(kind: rseTimeout, msg: "timed out", leaderHint: 0)
    check err.kind == rseTimeout

  test "rseInternal":
    let err = RaftStoreError(kind: rseInternal, msg: "internal error", leaderHint: 0)
    check err.kind == rseInternal

  test "rseBadRouting":
    let err = RaftStoreError(kind: rseBadRouting, msg: "bad routing", leaderHint: 0)
    check err.kind == rseBadRouting

suite "RSResult Helpers":

  test "rsOk with value":
    let result = rsOk(42)
    check result.isOk == true
    check result.value == 42

  test "rsOk with string":
    let result = rsOk("test")
    check result.isOk == true
    check result.value == "test"

  test "rsErr":
    let err = RaftStoreError(kind: rseNotLeader, msg: "error", leaderHint: 0)
    let result = rsErr[int](err)
    check result.isOk == false
    check result.error.kind == rseNotLeader

  test "rsVOk":
    let result = rsVOk()
    check result.isOk == true

  test "rsVErr":
    let err = RaftStoreError(kind: rseTimeout, msg: "timeout", leaderHint: 0)
    let result = rsVErr(err)
    check result.isOk == false
    check result.error.kind == rseTimeout

suite "RaftKVEntry":

  test "basic entry":
    let entry = RaftKVEntry(value: "data", version: 5'u64, timestamp: 1000'u64)
    check entry.value == "data"
    check entry.version == 5'u64
    check entry.timestamp == 1000'u64

  test "empty entry":
    let entry = RaftKVEntry(value: "", version: 0'u64, timestamp: 0'u64)
    check entry.value == ""
    check entry.version == 0'u64

suite "Constants":

  test "INTENT_PREFIX":
    check INTENT_PREFIX == "\x00INTENT\x00"

  test "COORD_PREFIX":
    check COORD_PREFIX == "\x00COORD\x00"

suite "Error Kind Enumeration":

  test "all error kinds are distinct":
    check rseNotLeader != rseGroupNotFound
    check rseGroupNotFound != rseTimeout
    check rseTimeout != rseInternal
    check rseInternal != rseBadRouting

suite "SpaceInfo Type":

  test "SpaceInfo default":
    let info = SpaceInfo(
      spaceId: SpaceID(ZeroULID()),
      name: "test_space",
      replicas: 3,
      groupIds: @[genGroupIDLocal()],
      oldGroupIds: @[],
      workerState: wsIdle,
      workerNodeId: 0,
      workerHeartbeat: 0,
      checkpoint: MigrationCheckpoint(
        completedTables: @[],
        currentTable: zeroTableId(),
        currentCursor: "",
        keysMigrated: 0,
        startedAtNs: 0,
        lastProgressNs: 0,
      )
    )
    check info.name == "test_space"
    check info.replicas == 3
    check info.groupIds.len == 1
    check info.workerState == wsIdle

suite "NodeInfo Type":

  test "NodeInfo tuple":
    let info: raft_store.NodeInfo = (host: "localhost", clientPort: 8080)
    check info.host == "localhost"
    check info.clientPort == 8080

suite "ActiveTxnChecker Type":
  test "ActiveTxnChecker is a proc type":
    ## ActiveTxnChecker is defined as proc(txnId: TransactionID): bool
    var checkerCalls = 0
    let checker: ActiveTxnChecker = proc(txnId: TransactionID): bool {.gcsafe,
        raises: [].} =
      inc checkerCalls
      false
    # Can call the checker
    let result = checker(zeroTransactionID())
    check result == false
    check checkerCalls == 1

  test "ActiveTxnChecker returns true for active txn":
    let activeTxn = genTransactionIDLocal()
    let checker: ActiveTxnChecker = proc(txnId: TransactionID): bool {.gcsafe,
        raises: [].} =
      txnId == activeTxn
    check checker(activeTxn) == true
    check checker(zeroTransactionID()) == false

suite "IntentScavengerStats Type":
  test "IntentScavengerStats default values":
    let stats = IntentScavengerStats()
    check stats.intentsScanned == 0
    check stats.orphanIntentsCleaned == 0
    check stats.protocolIntentsScanned == 0
    check stats.mvccIntentsScanned == 0
    check stats.scanCount == 0
    check stats.lastScanTimeNs == 0

  test "IntentScavengerStats field updates":
    var stats = IntentScavengerStats()
    stats.intentsScanned = 100
    stats.orphanIntentsCleaned = 25
    stats.protocolIntentsScanned = 40
    stats.mvccIntentsScanned = 60
    stats.scanCount = 3
    stats.lastScanTimeNs = 1234567890'i64
    check stats.intentsScanned == 100
    check stats.orphanIntentsCleaned == 25
    check stats.protocolIntentsScanned + stats.mvccIntentsScanned == 100
    check stats.scanCount == 3

suite "Intent Scavenger Constants":
  test "INTENT_SCAVENGE_AGE_NS value":
    check INTENT_SCAVENGE_AGE_NS == 60_000_000_000'i64

  test "GC_SCAN_INTERVAL_MS value":
    check GC_SCAN_INTERVAL_MS == 30_000
