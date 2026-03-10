# Phase 5 — Unit tests for RaftKVStore and RaftTxnCoordinator.
#
# Tests the Raft-backed KV store in isolation (no TCP, no ProtocolServer).
# Uses a single-node MultiRaftCoordinator with the node acting as leader.
#
# No port usage — all purely in-memory/local.
# Temp storage: /tmp/fractio_raft_test_<N>/ (cleaned up per suite invocation).

import std/[unittest, os, times, sets, options, locks, tables]
import fractio/protocol/raft_store
import fractio/protocol/raft_txn
import fractio/protocol/txn_manager
import fractio/protocol/router
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/distributed/sharedtimer/timeprovider as tp
import fractio/distributed/sharedtimer/mock

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc cleanDir(path: string) =
  try: removeDir(path) except CatchableError: discard
  try: createDir(path) except CatchableError: discard

proc makeStore(storagePath: string): tuple[
    coord: MultiRaftCoordinator, store: RaftKVStoreExt, rid: GroupID] =
  cleanDir(storagePath)
  let cfg = CoordinatorConfig(
    nodeId: NodeID(1),
    numWorkers: 1,
    electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
    heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
    storagePath: storagePath,
  )
  let coord = newMultiRaftCoordinator(cfg)
  # Create Raft groups for both meta range (1) and data range (2)
  for rid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let desc = newGroupDescriptor(rid)
    let rep = desc.addReplica(NodeID(1))
    let group = coord.createGroup(desc, rep.replicaId)
    group.becomeLeader()
  coord.start()
  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 2000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])
  (coord, store, DATA_GROUP_START_ID)

proc teardownStore(coord: MultiRaftCoordinator, storagePath: string) =
  coord.stop()
  try: removeDir(storagePath) except CatchableError: discard

# ---------------------------------------------------------------------------
# Suite: basic put/get/delete
# ---------------------------------------------------------------------------

suite "RaftKVStore - basic put/get/delete":
  test "put and get a key":
    let (coord, store, rid) = makeStore("/tmp/fractio_raft_t01")
    defer: teardownStore(coord, "/tmp/fractio_raft_t01")
    let wr = store.raftPut("hello", "world")
    check wr.isOk
    check wr.value.value == "world"
    let gr = store.raftGet("hello")
    check gr.isOk
    check gr.value.isSome
    check gr.value.get().value == "world"

  test "get missing key returns none":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t02")
    defer: teardownStore(coord, "/tmp/fractio_raft_t02")
    let gr = store.raftGet("no_such_key")
    check gr.isOk
    check gr.value.isNone

  test "overwrite a key":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t03")
    defer: teardownStore(coord, "/tmp/fractio_raft_t03")
    discard store.raftPut("k1", "v1")
    discard store.raftPut("k1", "v2")
    let gr = store.raftGet("k1")
    check gr.isOk
    check gr.value.isSome
    check gr.value.get().value == "v2"

  test "delete an existing key":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t04")
    defer: teardownStore(coord, "/tmp/fractio_raft_t04")
    discard store.raftPut("del_me", "gone")
    let dr = store.raftDelete("del_me")
    check dr.isOk
    check dr.value.isSome
    check dr.value.get().value == "gone"
    let gr = store.raftGet("del_me")
    check gr.isOk
    check gr.value.isNone

  test "delete a missing key returns none":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t05")
    defer: teardownStore(coord, "/tmp/fractio_raft_t05")
    let dr = store.raftDelete("never_existed")
    check dr.isOk
    check dr.value.isNone

  test "multiple independent keys":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t06")
    defer: teardownStore(coord, "/tmp/fractio_raft_t06")
    discard store.raftPut("a", "1")
    discard store.raftPut("b", "2")
    discard store.raftPut("c", "3")
    check store.raftGet("a").value.get().value == "1"
    check store.raftGet("b").value.get().value == "2"
    check store.raftGet("c").value.get().value == "3"

# ---------------------------------------------------------------------------
# Suite: scan
# ---------------------------------------------------------------------------

suite "RaftKVStore - scan":
  test "scan empty store":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t10")
    defer: teardownStore(coord, "/tmp/fractio_raft_t10")
    let sr = store.raftScan("", "", 0)
    check sr.isOk
    check sr.value.len == 0

  test "scan all keys":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t11")
    defer: teardownStore(coord, "/tmp/fractio_raft_t11")
    discard store.raftPut("x1", "a")
    discard store.raftPut("x2", "b")
    discard store.raftPut("x3", "c")
    let sr = store.raftScan("x1", "x4", 0)
    check sr.isOk
    check sr.value.len == 3
    check sr.value[0][0] == "x1"
    check sr.value[1][0] == "x2"
    check sr.value[2][0] == "x3"

  test "scan with limit":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t12")
    defer: teardownStore(coord, "/tmp/fractio_raft_t12")
    discard store.raftPut("x1", "a")
    discard store.raftPut("x2", "b")
    discard store.raftPut("x3", "c")
    let sr = store.raftScan("x1", "x4", 2)
    check sr.isOk
    check sr.value.len == 2

  test "scan with start/end key range":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t13")
    defer: teardownStore(coord, "/tmp/fractio_raft_t13")
    discard store.raftPut("y1", "p")
    discard store.raftPut("y2", "q")
    discard store.raftPut("y3", "r")
    let sr = store.raftScan("y2", "y3", 0)
    check sr.isOk
    check sr.value.len == 1
    check sr.value[0][0] == "y2"

  test "scan excludes internal intent/coord keys":
    let (coord, store, rid) = makeStore("/tmp/fractio_raft_t14")
    defer: teardownStore(coord, "/tmp/fractio_raft_t14")
    discard store.raftPut("user_key", "user_val")
    # Write an intent key directly into the SM (bypassing Raft for test speed)
    let intentKey = encodeIntentKey(99'u64, "internal_key")
    let sm = store.getOrCreateSM(rid)
    acquire(store.smMu)
    sm.kvStore[intentKey] = "intent_value"
    release(store.smMu)
    let sr = store.raftScan("", "", 0)
    check sr.isOk
    for (k, _) in sr.value:
      check not isIntentKey(k)
      check not isCoordKey(k)

# ---------------------------------------------------------------------------
# Suite: version and timestamp
# ---------------------------------------------------------------------------

suite "RaftKVStore - version monotonic":
  test "each put gets a unique increasing version":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t20")
    defer: teardownStore(coord, "/tmp/fractio_raft_t20")
    let r1 = store.raftPut("vk", "v1")
    let r2 = store.raftPut("vk", "v2")
    check r1.isOk and r2.isOk
    check r2.value.version > r1.value.version

  test "timestamp is non-zero":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t21")
    defer: teardownStore(coord, "/tmp/fractio_raft_t21")
    let r = store.raftPut("ts_key", "ts_val")
    check r.isOk
    check r.value.timestamp > 0'u64

# ---------------------------------------------------------------------------
# Suite: Intent API
# ---------------------------------------------------------------------------

suite "RaftKVStore - intent API":
  test "write and read intent key encoding":
    let txnId = 42'u64
    let key = "intent_key"
    let intentKey = encodeIntentKey(txnId, key)
    check isIntentKey(intentKey)
    check decodeIntentTxnId(intentKey) == txnId
    check decodeIntentUserKey(intentKey) == key

  test "raftPutIntent stores under intent key":
    let (coord, store, rid) = makeStore("/tmp/fractio_raft_t30")
    defer: teardownStore(coord, "/tmp/fractio_raft_t30")
    let txnId = 100'u64
    let vr = store.raftPutIntent(txnId, "mykey", "myval")
    check vr.isOk
    # User key should NOT be visible yet
    let gr = store.raftGet("mykey")
    check gr.isOk
    check gr.value.isNone
    # Intent key should exist in SM
    let intentKey = encodeIntentKey(txnId, "mykey")
    let sm = store.getOrCreateSM(rid)
    acquire(store.smMu)
    let intentExists = sm.kvStore.hasKey(intentKey)
    release(store.smMu)
    check intentExists

  test "raftResolveIntent commit makes value visible":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t31")
    defer: teardownStore(coord, "/tmp/fractio_raft_t31")
    let txnId = 101'u64
    discard store.raftPutIntent(txnId, "committed_key", "committed_val")
    let vr = store.raftResolveIntent(txnId, "committed_key", true, "committed_val")
    check vr.isOk
    let gr = store.raftGet("committed_key")
    check gr.isOk
    check gr.value.isSome
    check gr.value.get().value == "committed_val"

  test "raftResolveIntent abort removes intent":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t32")
    defer: teardownStore(coord, "/tmp/fractio_raft_t32")
    let txnId = 102'u64
    discard store.raftPutIntent(txnId, "aborted_key", "aborted_val")
    let vr = store.raftResolveIntent(txnId, "aborted_key", false)
    check vr.isOk
    let gr = store.raftGet("aborted_key")
    check gr.isOk
    check gr.value.isNone

  test "raftDeleteIntent removes intent directly":
    let (coord, store, rid) = makeStore("/tmp/fractio_raft_t33")
    defer: teardownStore(coord, "/tmp/fractio_raft_t33")
    let txnId = 103'u64
    discard store.raftPutIntent(txnId, "del_intent_key", "val")
    let vr = store.raftDeleteIntent(txnId, "del_intent_key")
    check vr.isOk
    let intentKey = encodeIntentKey(txnId, "del_intent_key")
    let sm = store.getOrCreateSM(rid)
    acquire(store.smMu)
    let intentGone = not sm.kvStore.hasKey(intentKey)
    release(store.smMu)
    check intentGone

# ---------------------------------------------------------------------------
# Suite: coordinator record API
# ---------------------------------------------------------------------------

suite "RaftKVStore - coordinator record API":
  test "write and read coordinator record":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t40")
    defer: teardownStore(coord, "/tmp/fractio_raft_t40")
    let txnId = 200'u64
    let data = "PREPARED:200:0:key1,key2"
    let wr = store.raftWriteCoordRecord(txnId, data)
    check wr.isOk
    let rr = store.raftReadCoordRecord(txnId)
    check rr.isSome
    check rr.get() == data

  test "delete coordinator record":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t41")
    defer: teardownStore(coord, "/tmp/fractio_raft_t41")
    let txnId = 201'u64
    discard store.raftWriteCoordRecord(txnId, "PREPARED:201:0:k")
    let dr = store.raftDeleteCoordRecord(txnId)
    check dr.isOk
    let rr = store.raftReadCoordRecord(txnId)
    check rr.isNone

  test "coord record encode/decode roundtrip":
    let keys = @["k1", "k2", "k3"]
    let data = encodeCoordRecord(999'u64, CoordStatePrepared, 12345'u64, keys)
    let (state, txnId, commitTs, decKeys) = decodeCoordRecord(data)
    check state == CoordStatePrepared
    check txnId == 999'u64
    check commitTs == 12345'u64
    check decKeys == keys

# ---------------------------------------------------------------------------
# Suite: RaftTxnCoordinator — single-shard commit
# ---------------------------------------------------------------------------

suite "RaftTxnCoordinator - single-shard commit":
  test "single-shard commit resolves intents":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t50")
    defer: teardownStore(coord, "/tmp/fractio_raft_t50")
    let txnMgr = newTransactionManager()
    let raftCoord = newRaftTxnCoordinator(store, txnMgr)

    let txnId = 300'u64
    discard store.raftPutIntent(txnId, "sc_key1", "val1")
    discard store.raftPutIntent(txnId, "sc_key2", "val2")

    var ws = initHashSet[string]()
    ws.incl("sc_key1")
    ws.incl("sc_key2")

    let ok = raftCoord.commitSingleShard(txnId, ws, 1234'u64)
    check ok
    check store.raftGet("sc_key1").value.get().value == "val1"
    check store.raftGet("sc_key2").value.get().value == "val2"

  test "single-shard rollback removes intents":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t51")
    defer: teardownStore(coord, "/tmp/fractio_raft_t51")
    let txnMgr = newTransactionManager()
    let raftCoord = newRaftTxnCoordinator(store, txnMgr)

    let txnId = 301'u64
    discard store.raftPutIntent(txnId, "rb_key1", "rbv1")
    discard store.raftPutIntent(txnId, "rb_key2", "rbv2")

    var ws = initHashSet[string]()
    ws.incl("rb_key1")
    ws.incl("rb_key2")

    let ok = raftCoord.rollbackSingleShard(txnId, ws)
    check ok
    check store.raftGet("rb_key1").value.isNone
    check store.raftGet("rb_key2").value.isNone

# ---------------------------------------------------------------------------
# Suite: RaftTxnCoordinator — cross-shard 2PC
# ---------------------------------------------------------------------------

suite "RaftTxnCoordinator - cross-shard 2PC":
  test "cross-shard commit succeeds when intents exist":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t60")
    defer: teardownStore(coord, "/tmp/fractio_raft_t60")
    let txnMgr = newTransactionManager()
    let raftCoord = newRaftTxnCoordinator(store, txnMgr)

    let txnId = 400'u64
    discard store.raftPutIntent(txnId, "xs_key1", "xsv1")
    discard store.raftPutIntent(txnId, "xs_key2", "xsv2")

    var ws = initHashSet[string]()
    ws.incl("xs_key1")
    ws.incl("xs_key2")

    let resp = raftCoord.coordinateCrossShardCommit(txnId, ws, 5000'u64)
    check resp.status == TxnCommitOK
    check resp.commitTimestamp == 5000'u64
    check store.raftGet("xs_key1").value.get().value == "xsv1"
    check store.raftGet("xs_key2").value.get().value == "xsv2"

  test "cross-shard commit aborts when intent missing":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t61")
    defer: teardownStore(coord, "/tmp/fractio_raft_t61")
    let txnMgr = newTransactionManager()
    let raftCoord = newRaftTxnCoordinator(store, txnMgr)

    let txnId = 401'u64
    discard store.raftPutIntent(txnId, "xs_present", "pv")
    # xs_missing has no intent

    var ws = initHashSet[string]()
    ws.incl("xs_present")
    ws.incl("xs_missing")

    let resp = raftCoord.coordinateCrossShardCommit(txnId, ws, 6000'u64)
    check resp.status == TxnCommitConflict
    check resp.commitTimestamp == 0'u64

  test "recovery of PREPARED coord record aborts":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t62")
    defer: teardownStore(coord, "/tmp/fractio_raft_t62")
    let txnMgr = newTransactionManager()
    let raftCoord = newRaftTxnCoordinator(store, txnMgr)

    let txnId = 402'u64
    let keys = @["crash_key"]
    let data = encodeCoordRecord(txnId, CoordStatePrepared, 0'u64, keys)
    discard store.raftWriteCoordRecord(txnId, data)
    discard store.raftPutIntent(txnId, "crash_key", "cv")

    raftCoord.recoverPendingCoords()

    check store.raftReadCoordRecord(txnId).isNone
    check store.raftGet("crash_key").value.isNone

  test "recovery of COMMITTING coord record re-commits":
    let (coord, store, _) = makeStore("/tmp/fractio_raft_t63")
    defer: teardownStore(coord, "/tmp/fractio_raft_t63")
    let txnMgr = newTransactionManager()
    let raftCoord = newRaftTxnCoordinator(store, txnMgr)

    let txnId = 403'u64
    discard store.raftPutIntent(txnId, "recommit_key", "rcv")
    let keys = @["recommit_key"]
    let data = encodeCoordRecord(txnId, CoordStateCommitting, 9000'u64, keys)
    discard store.raftWriteCoordRecord(txnId, data)

    raftCoord.recoverPendingCoords()

    check store.raftReadCoordRecord(txnId).isNone
    check store.raftGet("recommit_key").value.get().value == "rcv"

# ---------------------------------------------------------------------------
# Suite: RouterTable Phase 5 additions
# ---------------------------------------------------------------------------

suite "RouterTable - Phase 5 leader-change callback":
  test "setLeaderChangeCallback fires on updateLeader":
    let rt = newRouterTable(localNodeId = 1)
    rt.bootstrapSingleShard(shardId = 1, raftGroupId = 1)

    var callbackFired = false
    var capturedShardId: uint32 = 0

    rt.setLeaderChangeCallback(proc(shardId: uint32,
        leader: LeaderInfo) {.gcsafe, raises: [].} =
      callbackFired = true
      capturedShardId = shardId
    )

    let newLeader = LeaderInfo(nodeId: 2, nodeAddr: "10.0.0.2:9000",
        lastSeenMs: 1)
    rt.updateLeader(1, newLeader)

    check callbackFired
    check capturedShardId == 1

  test "notLeaderRedirect updates routing table":
    let rt = newRouterTable(localNodeId = 1)
    rt.bootstrapSingleShard(shardId = 1, raftGroupId = 1)
    let hint = LeaderInfo(nodeId: 3, nodeAddr: "10.0.0.3:9000", lastSeenMs: 42)
    rt.notLeaderRedirect(1, hint)
    let r = rt.routeKey("any_key")
    check r.isOk
    check r.val.nodeId == 3

  test "touchLeader refreshes TTL":
    let rt = newRouterTable(localNodeId = 1, leaderTtlMs = 60_000)
    rt.bootstrapSingleShard(shardId = 1, raftGroupId = 1)
    rt.touchLeader(1)
    let r = rt.routeKey("k")
    check r.isOk
    check r.val.nodeId == 1

# ---------------------------------------------------------------------------
# Suite: txn_manager TimeProvider wiring
# ---------------------------------------------------------------------------

suite "txn_manager - TimeProvider integration":
  test "setTimeProvider updates timestamp source":
    let mgr = newTransactionManager()
    let mockTp = MockTimeProvider(currentTime: 1_000_000_000'i64)
    mgr.setTimeProvider(mockTp)
    let rec1 = mgr.beginTransaction()
    let rec2 = mgr.beginTransaction()
    check rec2.readTimestamp > rec1.readTimestamp

  test "setRaftCoordPtr stores pointer":
    let mgr = newTransactionManager()
    var dummy = 42
    mgr.setRaftCoordPtr(addr dummy)
    check mgr.raftCoordPtr != nil
