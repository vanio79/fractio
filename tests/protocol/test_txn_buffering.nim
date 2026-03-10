# Phase 11 — Transactional write buffering tests.
#
# Verifies:
#   1. getWriteSet returns the correct keys
#   2. Transactional Put is buffered as intent (not visible as real key)
#   3. Intent key is present in SM after buffering (reads-your-own-writes)
#   4. Commit resolves intents → real key readable, intent gone
#   5. Rollback deletes intents → real key absent, intent gone
#   6. raftCommitTxn with empty write-set succeeds immediately
#   7. Multiple keys committed in a single Raft batch
#   8. E2E via TCP: txn Put → Commit → Get visible
#   9. E2E via TCP: txn Put → Rollback → Get absent
#  10. Non-transactional Put is immediately visible (not buffered)
#  11. Multiple keys in one txn all committed atomically
#  12. Sequential txns do not interfere
#
# Port range: 20600–20649 (Phase 11 txn buffering)
# Temp storage: /tmp/fractio_txnbuf_<N>/ (cleaned per test)

import std/[unittest, os, times, options, locks, tables]
import fractio/protocol/types
import fractio/protocol/server
import fractio/protocol/client
import fractio/protocol/raft_store
import fractio/protocol/txn_manager
import fractio/protocol/messages/kv
import fractio/protocol/messages/txn as txnMsgs
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/distributed/raft/state_machine

# ---------------------------------------------------------------------------
# Shared helpers
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
  for rid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let desc = newGroupDescriptor(rid)
    let rep = desc.addReplica(NodeID(1))
    let group = coord.createGroup(desc, rep.replicaId)
    group.becomeLeader()
  coord.start()
  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 3000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])
  (coord, store, DATA_GROUP_START_ID)

proc teardownStore(coord: MultiRaftCoordinator, storagePath: string) =
  coord.stop()
  try: removeDir(storagePath) except CatchableError: discard

proc smHasKey(store: RaftKVStoreExt, rid: GroupID, key: string): bool =
  let sm = store.getOrCreateSM(rid)
  acquire(store.smMu)
  result = sm.kvStore.hasKey(key)
  release(store.smMu)

proc smGetVal(store: RaftKVStoreExt, rid: GroupID, key: string): string =
  let sm = store.getOrCreateSM(rid)
  acquire(store.smMu)
  result = sm.kvStore.getOrDefault(key)
  release(store.smMu)

proc makeRaftServer(port: int, storagePath: string): ProtocolServer =
  let coordCfg = CoordinatorConfig(
    nodeId: NodeID(1),
    numWorkers: 1,
    electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
    heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
    storagePath: storagePath,
  )
  let coord = newMultiRaftCoordinator(coordCfg)
  for rid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let desc = newGroupDescriptor(rid)
    let rep = desc.addReplica(NodeID(1))
    let group = coord.createGroup(desc, rep.replicaId)
    group.becomeLeader()
  coord.start()
  let raftSt = newRaftKVStoreExt(coord, proposeTimeoutMs = 3000)
  raftSt.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])
  var cfg = defaultServerConfig()
  cfg.host = "127.0.0.1"
  cfg.port = port
  cfg.idleTimeoutSecs = 120
  let srv = newProtocolServer(cfg)
  srv.raftStore = raftSt
  srv.start()
  sleep(80)
  srv

proc connectClient(port: int): ProtocolClient =
  var cfg = defaultClientConfig("127.0.0.1", port)
  cfg.timeoutMs = 10_000
  result = newProtocolClient(cfg)
  let r = result.connect()
  doAssert r.isOk, "client connect failed: " & $r.err

template withRaftServer(port: int, storagePath: string,
    body: untyped) =
  block:
    cleanDir(storagePath)
    let srv {.inject.} = makeRaftServer(port, storagePath)
    let cli {.inject.} = connectClient(port)
    try:
      body
    finally:
      cli.disconnect()
      srv.stop()
      sleep(60)
      try: removeDir(storagePath) except CatchableError: discard

# ---------------------------------------------------------------------------
# Suite 1: TransactionManager.getWriteSet unit tests
# ---------------------------------------------------------------------------

suite "TxnManager - getWriteSet":

  test "getWriteSet returns empty seq for unknown txnId":
    let mgr = newTransactionManager()
    let ws = mgr.getWriteSet(9999'u64)
    check ws.len == 0

  test "getWriteSet returns empty seq for txn with no writes":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    let ws = mgr.getWriteSet(rec.id)
    check ws.len == 0

  test "getWriteSet returns recorded write keys":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    discard mgr.recordWrite(rec.id, "alpha")
    discard mgr.recordWrite(rec.id, "beta")
    discard mgr.recordWrite(rec.id, "gamma")
    let ws = mgr.getWriteSet(rec.id)
    check ws.len == 3
    check "alpha" in ws
    check "beta" in ws
    check "gamma" in ws

  test "getWriteSet does not include read-only keys":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    discard mgr.recordRead(rec.id, "read-key")
    discard mgr.recordWrite(rec.id, "write-key")
    let ws = mgr.getWriteSet(rec.id)
    check ws.len == 1
    check "write-key" in ws
    check "read-key" notin ws

# ---------------------------------------------------------------------------
# Suite 2: raftBufferIntent / raftDeleteIntent / raftCommitTxn unit tests
# ---------------------------------------------------------------------------

suite "RaftKVStore - intent buffering (unit)":

  test "raftBufferIntent: real key absent, intent key in SM":
    let (coord, store, rid) = makeStore("/tmp/fractio_txnbuf_u01")
    defer: teardownStore(coord, "/tmp/fractio_txnbuf_u01")

    let txnId = 42'u64
    let key = "user:1"
    let val = "alice"

    let br = store.raftBufferIntent(txnId, key, val)
    check br.isOk

    # Real key must NOT be visible
    let getReal = store.raftGet(key)
    check getReal.isOk
    check getReal.value.isNone

    # Intent key must be in SM with correct value
    let intentKey = encodeIntentKey(txnId, key)
    check smHasKey(store, rid, intentKey)
    check smGetVal(store, rid, intentKey) == val

  test "raftDeleteIntent: removes intent key from SM":
    let (coord, store, rid) = makeStore("/tmp/fractio_txnbuf_u02")
    defer: teardownStore(coord, "/tmp/fractio_txnbuf_u02")

    let txnId = 77'u64
    let key = "user:2"
    discard store.raftBufferIntent(txnId, key, "bob")

    let dr = store.raftDeleteIntent(txnId, key)
    check dr.isOk

    let intentKey = encodeIntentKey(txnId, key)
    check not smHasKey(store, rid, intentKey)

  test "raftCommitTxn with empty writeSet returns OK":
    let (coord, store, _) = makeStore("/tmp/fractio_txnbuf_u03")
    defer: teardownStore(coord, "/tmp/fractio_txnbuf_u03")

    let cr = store.raftCommitTxn(1'u64, @[])
    check cr.isOk

  test "raftCommitTxn resolves intent: real key readable, intent gone":
    let (coord, store, rid) = makeStore("/tmp/fractio_txnbuf_u04")
    defer: teardownStore(coord, "/tmp/fractio_txnbuf_u04")

    let txnId = 100'u64
    let key = "product:1"
    let val = "widget"
    discard store.raftBufferIntent(txnId, key, val)

    let cr = store.raftCommitTxn(txnId, @[key])
    check cr.isOk

    # Real key readable
    let getReal = store.raftGet(key)
    check getReal.isOk
    check getReal.value.isSome
    check getReal.value.get().value == val

    # Intent key gone
    let intentKey = encodeIntentKey(txnId, key)
    check not smHasKey(store, rid, intentKey)

  test "raftCommitTxn resolves multiple keys in single batch":
    let (coord, store, rid) = makeStore("/tmp/fractio_txnbuf_u05")
    defer: teardownStore(coord, "/tmp/fractio_txnbuf_u05")

    let txnId = 200'u64
    let keys = @["k1", "k2", "k3"]
    let vals = @["v1", "v2", "v3"]
    for i in 0 ..< keys.len:
      discard store.raftBufferIntent(txnId, keys[i], vals[i])

    let cr = store.raftCommitTxn(txnId, keys)
    check cr.isOk

    for i in 0 ..< keys.len:
      let gr = store.raftGet(keys[i])
      check gr.isOk
      check gr.value.isSome
      check gr.value.get().value == vals[i]
      let ik = encodeIntentKey(txnId, keys[i])
      check not smHasKey(store, rid, ik)

  test "raftCommitTxn: key with no intent is silently skipped":
    let (coord, store, _) = makeStore("/tmp/fractio_txnbuf_u06")
    defer: teardownStore(coord, "/tmp/fractio_txnbuf_u06")

    # No intent was ever buffered
    let cr = store.raftCommitTxn(300'u64, @["ghost-key"])
    check cr.isOk
    let gr = store.raftGet("ghost-key")
    check gr.isOk
    check gr.value.isNone

# ---------------------------------------------------------------------------
# Suite 3: End-to-end via TCP server
# ---------------------------------------------------------------------------

suite "TxnBuffering E2E - commit path":

  test "txn Put then Commit makes value readable":
    withRaftServer(20600, "/tmp/fractio_txnbuf_e01"):
      let beginR = cli.beginTxn()
      check beginR.isOk
      let txnId = beginR.value.txnId

      let putR = cli.kvPut("e2e:key1", "hello", txnId = txnId)
      check putR.isOk
      check putR.value.status == PutStatusOK

      # Value must NOT be visible before commit
      let getBeforeCommit = cli.kvGet("e2e:key1")
      check getBeforeCommit.isOk
      check not getBeforeCommit.value.found

      let commitR = cli.commitTxn(txnId)
      check commitR.isOk
      check commitR.value.status == TxnCommitOK

      # Value must now be visible after commit
      let getAfterCommit = cli.kvGet("e2e:key1")
      check getAfterCommit.isOk
      check getAfterCommit.value.found
      check getAfterCommit.value.value == "hello"

  test "txn Put then Rollback leaves value absent":
    withRaftServer(20601, "/tmp/fractio_txnbuf_e02"):
      let beginR = cli.beginTxn()
      check beginR.isOk
      let txnId = beginR.value.txnId

      let putR = cli.kvPut("e2e:key2", "world", txnId = txnId)
      check putR.isOk

      let rollR = cli.rollbackTxn(txnId)
      check rollR.isOk
      check rollR.value.status == TxnRollbackOK

      # Value must be absent after rollback
      let getAfterRollback = cli.kvGet("e2e:key2")
      check getAfterRollback.isOk
      check not getAfterRollback.value.found

  test "non-txn Put is immediately visible (no buffering)":
    withRaftServer(20602, "/tmp/fractio_txnbuf_e03"):
      let putR = cli.kvPut("e2e:key3", "direct")
      check putR.isOk
      check putR.value.status == PutStatusOK

      let getR = cli.kvGet("e2e:key3")
      check getR.isOk
      check getR.value.found
      check getR.value.value == "direct"

  test "multiple keys in one txn all committed atomically":
    withRaftServer(20603, "/tmp/fractio_txnbuf_e04"):
      let beginR = cli.beginTxn()
      check beginR.isOk
      let txnId = beginR.value.txnId

      for i in 1 .. 4:
        let putR = cli.kvPut("batch:k" & $i, "v" & $i, txnId = txnId)
        check putR.isOk

      let commitR = cli.commitTxn(txnId)
      check commitR.isOk
      check commitR.value.status == TxnCommitOK

      for i in 1 .. 4:
        let getR = cli.kvGet("batch:k" & $i)
        check getR.isOk
        check getR.value.found
        check getR.value.value == "v" & $i

  test "sequential txns do not interfere":
    withRaftServer(20604, "/tmp/fractio_txnbuf_e05"):
      # Txn A: write and commit
      let beginA = cli.beginTxn()
      check beginA.isOk
      discard cli.kvPut("seq:a", "first", txnId = beginA.value.txnId)
      let commitA = cli.commitTxn(beginA.value.txnId)
      check commitA.isOk
      check commitA.value.status == TxnCommitOK

      # Txn B: write and rollback
      let beginB = cli.beginTxn()
      check beginB.isOk
      discard cli.kvPut("seq:b", "lost", txnId = beginB.value.txnId)
      discard cli.rollbackTxn(beginB.value.txnId)

      # Txn C: overwrite seq:a and commit
      let beginC = cli.beginTxn()
      check beginC.isOk
      discard cli.kvPut("seq:a", "second", txnId = beginC.value.txnId)
      let commitC = cli.commitTxn(beginC.value.txnId)
      check commitC.isOk
      check commitC.value.status == TxnCommitOK

      let getA = cli.kvGet("seq:a")
      check getA.isOk
      check getA.value.found
      check getA.value.value == "second"

      let getB = cli.kvGet("seq:b")
      check getB.isOk
      check not getB.value.found
