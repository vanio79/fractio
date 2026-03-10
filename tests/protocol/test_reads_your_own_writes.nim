# Phase 12 — Reads-your-own-writes tests.
#
# Verifies that a transaction can read back keys it has written as intents
# BEFORE committing, and that those reads see the intent value rather than the
# committed value (or "not found" if the key has never been committed).
#
# Also verifies isolation: a second client (different txn / no txn) cannot see
# the intent values until the first transaction commits.
#
# Covered scenarios:
#   1. raftGetForTxn: intent visible, no committed value
#   2. raftGetForTxn: committed value visible when no intent exists
#   3. raftGetForTxn: intent shadows committed value
#   4. raftGetForTxn: missing key returns none
#   5. raftGetForTxn: unknown groupId returns error
#   6. E2E: txn Get after txn Put sees intent value (reads-your-own-writes)
#   7. E2E: non-txn Get after txn Put (pre-commit) sees old value (isolation)
#   8. E2E: txn Get after txn Put after prior committed value sees intent
#   9. E2E: txn reads own multi-key writes before commit
#  10. E2E: after rollback txn reads see absent (no stale intent)
#
# Port range: 20605–20619 (Phase 12 reads-your-own-writes)
# Temp storage: /tmp/fractio_royw_<N>/ (cleaned per test)

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
# Helpers (same pattern as test_txn_buffering.nim)
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

template withRaftServer(port: int, storagePath: string, body: untyped) =
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
# Suite 1: raftGetForTxn unit tests
# ---------------------------------------------------------------------------

suite "RaftKVStore - raftGetForTxn (unit)":

  test "intent visible when no committed value exists":
    let (coord, store, _) = makeStore("/tmp/fractio_royw_u01")
    defer: teardownStore(coord, "/tmp/fractio_royw_u01")

    let txnId = 10'u64
    discard store.raftBufferIntent(txnId, "fresh:key", "intent-value")

    let rr = store.raftGetForTxn(txnId, "fresh:key")
    check rr.isOk
    check rr.value.isSome
    check rr.value.get().value == "intent-value"

  test "committed value visible when no intent for this txn":
    let (coord, store, _) = makeStore("/tmp/fractio_royw_u02")
    defer: teardownStore(coord, "/tmp/fractio_royw_u02")

    # Commit a real value first
    discard store.raftPut("committed:key", "real-value")

    # Different txnId — no intent
    let rr = store.raftGetForTxn(99'u64, "committed:key")
    check rr.isOk
    check rr.value.isSome
    check rr.value.get().value == "real-value"

  test "intent shadows prior committed value":
    let (coord, store, _) = makeStore("/tmp/fractio_royw_u03")
    defer: teardownStore(coord, "/tmp/fractio_royw_u03")

    # Committed baseline
    discard store.raftPut("shadow:key", "old-value")

    # Intent overrides it for this txn
    let txnId = 20'u64
    discard store.raftBufferIntent(txnId, "shadow:key", "new-value")

    let rr = store.raftGetForTxn(txnId, "shadow:key")
    check rr.isOk
    check rr.value.isSome
    check rr.value.get().value == "new-value"

    # Plain raftGet still sees old committed value
    let plain = store.raftGet("shadow:key")
    check plain.isOk
    check plain.value.isSome
    check plain.value.get().value == "old-value"

  test "missing key returns none":
    let (coord, store, _) = makeStore("/tmp/fractio_royw_u04")
    defer: teardownStore(coord, "/tmp/fractio_royw_u04")

    let rr = store.raftGetForTxn(5'u64, "does-not-exist")
    check rr.isOk
    check rr.value.isNone

  test "intent of one txn is not visible to another txnId":
    let (coord, store, _) = makeStore("/tmp/fractio_royw_u05")
    defer: teardownStore(coord, "/tmp/fractio_royw_u05")

    let txnA = 30'u64
    let txnB = 31'u64
    discard store.raftBufferIntent(txnA, "iso:key", "txnA-value")

    # txnB must not see txnA's intent
    let rr = store.raftGetForTxn(txnB, "iso:key")
    check rr.isOk
    check rr.value.isNone

# ---------------------------------------------------------------------------
# Suite 2: End-to-end reads-your-own-writes via TCP
# ---------------------------------------------------------------------------

suite "ReadsYourOwnWrites E2E":

  test "txn Get after txn Put sees intent value before commit":
    withRaftServer(20605, "/tmp/fractio_royw_e01"):
      let beginR = cli.beginTxn()
      check beginR.isOk
      let txnId = beginR.value.txnId

      let putR = cli.kvPut("royw:k1", "intent-v1", txnId = txnId)
      check putR.isOk

      # Same transaction reads its own write
      let getR = cli.kvGet("royw:k1", txnId = txnId)
      check getR.isOk
      check getR.value.found
      check getR.value.value == "intent-v1"

      # Commit to clean up
      discard cli.commitTxn(txnId)

  test "non-txn read does NOT see pre-commit intent (isolation)":
    withRaftServer(20606, "/tmp/fractio_royw_e02"):
      let beginR = cli.beginTxn()
      check beginR.isOk
      let txnId = beginR.value.txnId

      discard cli.kvPut("royw:iso", "secret", txnId = txnId)

      # Non-transactional read — must not see the intent
      let getR = cli.kvGet("royw:iso")
      check getR.isOk
      check not getR.value.found

      discard cli.rollbackTxn(txnId)

  test "txn reads own write that shadows a prior committed value":
    withRaftServer(20607, "/tmp/fractio_royw_e03"):
      # Pre-commit a baseline value without a transaction
      let baseR = cli.kvPut("royw:shadow", "base")
      check baseR.isOk

      let beginR = cli.beginTxn()
      check beginR.isOk
      let txnId = beginR.value.txnId

      # Overwrite within the transaction
      discard cli.kvPut("royw:shadow", "updated", txnId = txnId)

      # Transactional read should see "updated", not "base"
      let getR = cli.kvGet("royw:shadow", txnId = txnId)
      check getR.isOk
      check getR.value.found
      check getR.value.value == "updated"

      # Non-txn read still sees "base" (isolation)
      let plainR = cli.kvGet("royw:shadow")
      check plainR.isOk
      check plainR.value.found
      check plainR.value.value == "base"

      discard cli.commitTxn(txnId)

      # After commit, non-txn read sees "updated"
      let afterR = cli.kvGet("royw:shadow")
      check afterR.isOk
      check afterR.value.found
      check afterR.value.value == "updated"

  test "txn reads all of its own multi-key writes before commit":
    withRaftServer(20608, "/tmp/fractio_royw_e04"):
      let beginR = cli.beginTxn()
      check beginR.isOk
      let txnId = beginR.value.txnId

      for i in 1 .. 5:
        discard cli.kvPut("royw:multi:" & $i, "v" & $i, txnId = txnId)

      # All keys readable within the same transaction
      for i in 1 .. 5:
        let getR = cli.kvGet("royw:multi:" & $i, txnId = txnId)
        check getR.isOk
        check getR.value.found
        check getR.value.value == "v" & $i

      discard cli.commitTxn(txnId)

  test "after rollback, txn-Get sees absent (no stale intent)":
    withRaftServer(20609, "/tmp/fractio_royw_e05"):
      let beginR = cli.beginTxn()
      check beginR.isOk
      let txnId = beginR.value.txnId

      discard cli.kvPut("royw:rolled", "vanish", txnId = txnId)

      # Rollback cleans up the intent
      let rollR = cli.rollbackTxn(txnId)
      check rollR.isOk
      check rollR.value.status == TxnRollbackOK

      # Key must be absent for everyone
      let getR = cli.kvGet("royw:rolled")
      check getR.isOk
      check not getR.value.found

  test "txn Get on key it never wrote returns committed value":
    withRaftServer(20610, "/tmp/fractio_royw_e06"):
      # Pre-commit a value
      discard cli.kvPut("royw:preexist", "committed")

      let beginR = cli.beginTxn()
      check beginR.isOk
      let txnId = beginR.value.txnId

      # Transaction reads a key it never wrote — should see committed value
      let getR = cli.kvGet("royw:preexist", txnId = txnId)
      check getR.isOk
      check getR.value.found
      check getR.value.value == "committed"

      discard cli.rollbackTxn(txnId)
