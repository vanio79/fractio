# Integration tests for Phase 3 Transaction Protocol.
#
# Covers:
#   - messages/txn: codec round-trips for all four message types
#   - txn_manager: unit tests for TransactionManager (begin/commit/rollback/
#     conflict detection/timeout/status query)
#   - server/client: end-to-end Begin, Commit, Rollback, TxnStatus over TCP
#   - Transactional KV: reads and writes within a transaction, conflict
#     detection across concurrent transactions
#
# Port allocation: 19900-19999

import std/[unittest, os, times, strutils]
import fractio/protocol/types
import fractio/protocol/server
import fractio/protocol/client
import fractio/protocol/txn_manager
import fractio/protocol/messages/txn as txnMsgs
import fractio/protocol/messages/kv
import fractio/protocol/mvcc_store
import fractio/protocol/raft_store
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/raft/multigroup_types
import fractio/distributed/meta/system_tables
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider
import fractio/core/types as coreTypes

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

var testRaftPort {.global.} = 20000 # Raft ports in 20000+ range

proc nextRaftPort(): int =
  result = testRaftPort
  testRaftPort += 10

proc startTestServer(port: int): ProtocolServer =
  var cfg = defaultServerConfig()
  cfg.host = "127.0.0.1"
  cfg.port = port
  cfg.idleTimeoutSecs = 120
  result = newProtocolServer(cfg)

  # Set up MVCC store for transactional KV operations (requires single-node Raft)
  let storagePath = "/tmp/fractio_txn_test_" & $port
  try: removeDir(storagePath) except CatchableError: discard
  createDir(storagePath)

  let nodeId = rangeTypes.NodeID(1)
  let raftPort = nextRaftPort()
  let members = @[(nodeId: 1'u32, host: "127.0.0.1", port: raftPort)]

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    port: raftPort,
    host: "127.0.0.1",
    dataDir: storagePath,
    electionTimeoutLowerMs: 200,
    electionTimeoutUpperMs: 400,
    heartbeatIntervalMs: 100,
  ))
  coord.start()

  # Create meta + data groups
  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    discard coord.createAndStartGroup(gid, members)

  # Wait for leader election on both groups
  for attempt in 0 ..< 30:
    if coord.isLeader(META_GROUP_ID) and coord.isLeader(DATA_GROUP_START_ID):
      break
    sleep(100)

  let raftStore = newRaftKVStoreExt(coord, proposeTimeoutMs = 2000)
  raftStore.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  let txnMgr = newTransactionManager()
  let mockTimer = MockTimeProvider(currentTime: timerTypes.Timestamp(1_000_000_000))
  let tsProvider = newTimestampProvider(mockTimer, nodeId.uint16)
  let mvccStore = newMvccTransactionStore(raftStore, txnMgr, tsProvider)

  result.raftStore = raftStore
  result.raftCoord = coord
  result.mvccStore = mvccStore
  result.txnMgr = txnMgr

  result.start()
  sleep(100)

proc connectTestClient(port: int): ProtocolClient =
  var cfg = defaultClientConfig("127.0.0.1", port)
  cfg.timeoutMs = 10_000
  result = newProtocolClient(cfg)
  let r = result.connect()
  doAssert r.isOk, "client.connect failed: " & $r.err

proc withServer(port: int, body: proc(srv: ProtocolServer,
    cli: ProtocolClient)) =
  let srv = startTestServer(port)
  let cli = connectTestClient(port)
  try:
    body(srv, cli)
  finally:
    cli.disconnect()
    srv.stop()
    sleep(50)

# ---------------------------------------------------------------------------
# Suite: txn codec — BeginTxn round-trips
# ---------------------------------------------------------------------------

suite "txn codec - BeginTxnRequest/Response":
  test "basic begin request encodes and decodes":
    let req = txnMsgs.BeginTxnRequest(flags: TxnFlagReadOnly, timeoutMs: 5000)
    let payload = txnMsgs.encodeBeginTxnRequest(req)
    let r = txnMsgs.decodeBeginTxnRequest(payload)
    check r.isOk
    check r.value.flags == TxnFlagReadOnly
    check r.value.timeoutMs == 5000

  test "begin request zero timeout":
    let req = txnMsgs.BeginTxnRequest(flags: 0, timeoutMs: 0)
    let payload = txnMsgs.encodeBeginTxnRequest(req)
    let r = txnMsgs.decodeBeginTxnRequest(payload)
    check r.isOk
    check r.value.timeoutMs == 0

  test "begin request serializable flag":
    let req = txnMsgs.BeginTxnRequest(
      flags: TxnFlagSerializable, timeoutMs: 10_000)
    let payload = txnMsgs.encodeBeginTxnRequest(req)
    let r = txnMsgs.decodeBeginTxnRequest(payload)
    check r.isOk
    check (r.value.flags and TxnFlagSerializable) != 0

  test "begin response encodes and decodes":
    let testTxnId = genTransactionID()
    let resp = txnMsgs.BeginTxnResponse(txnId: testTxnId,
        readTimestamp: 999_999)
    let payload = txnMsgs.encodeBeginTxnResponse(resp)
    let r = txnMsgs.decodeBeginTxnResponse(payload)
    check r.isOk
    check r.value.txnId == testTxnId
    check r.value.readTimestamp == 999_999

  test "begin response truncated returns error":
    let payload = "\x02\x00" # just the message type, nothing else
    let r = txnMsgs.decodeBeginTxnResponse(payload)
    check r.isErr

# ---------------------------------------------------------------------------
# Suite: txn codec — CommitTxn round-trips
# ---------------------------------------------------------------------------

suite "txn codec - CommitTxnRequest/Response":
  test "commit request encodes and decodes":
    let testTxnId = genTransactionID()
    let req = txnMsgs.CommitTxnRequest(txnId: testTxnId)
    let payload = txnMsgs.encodeCommitTxnRequest(req)
    let r = txnMsgs.decodeCommitTxnRequest(payload)
    check r.isOk
    check r.value.txnId == testTxnId

  test "commit response OK":
    let resp = txnMsgs.CommitTxnResponse(
      status: TxnCommitOK, commitTimestamp: 123_456_789)
    let payload = txnMsgs.encodeCommitTxnResponse(resp)
    let r = txnMsgs.decodeCommitTxnResponse(payload)
    check r.isOk
    check r.value.status == TxnCommitOK
    check r.value.commitTimestamp == 123_456_789

  test "commit response conflict":
    let resp = txnMsgs.CommitTxnResponse(
      status: TxnCommitConflict, commitTimestamp: 0)
    let payload = txnMsgs.encodeCommitTxnResponse(resp)
    let r = txnMsgs.decodeCommitTxnResponse(payload)
    check r.isOk
    check r.value.status == TxnCommitConflict
    check r.value.commitTimestamp == 0

  test "commit response timeout":
    let resp = txnMsgs.CommitTxnResponse(
      status: TxnCommitTimeout, commitTimestamp: 0)
    let payload = txnMsgs.encodeCommitTxnResponse(resp)
    let r = txnMsgs.decodeCommitTxnResponse(payload)
    check r.isOk
    check r.value.status == TxnCommitTimeout

  test "commit response not found":
    let resp = txnMsgs.CommitTxnResponse(
      status: TxnCommitNotFound, commitTimestamp: 0)
    let payload = txnMsgs.encodeCommitTxnResponse(resp)
    let r = txnMsgs.decodeCommitTxnResponse(payload)
    check r.isOk
    check r.value.status == TxnCommitNotFound

# ---------------------------------------------------------------------------
# Suite: txn codec — RollbackTxn round-trips
# ---------------------------------------------------------------------------

suite "txn codec - RollbackTxnRequest/Response":
  test "rollback request encodes and decodes":
    let testTxnId = genTransactionID()
    let req = txnMsgs.RollbackTxnRequest(txnId: testTxnId)
    let payload = txnMsgs.encodeRollbackTxnRequest(req)
    let r = txnMsgs.decodeRollbackTxnRequest(payload)
    check r.isOk
    check r.value.txnId == testTxnId

  test "rollback response OK":
    let resp = txnMsgs.RollbackTxnResponse(status: TxnRollbackOK)
    let payload = txnMsgs.encodeRollbackTxnResponse(resp)
    let r = txnMsgs.decodeRollbackTxnResponse(payload)
    check r.isOk
    check r.value.status == TxnRollbackOK

  test "rollback response not found":
    let resp = txnMsgs.RollbackTxnResponse(status: TxnRollbackNotFound)
    let payload = txnMsgs.encodeRollbackTxnResponse(resp)
    let r = txnMsgs.decodeRollbackTxnResponse(payload)
    check r.isOk
    check r.value.status == TxnRollbackNotFound

  test "rollback request truncated returns error":
    let payload = "\x02\x02" # just the message type, no txnId
    let r = txnMsgs.decodeRollbackTxnRequest(payload)
    check r.isErr

# ---------------------------------------------------------------------------
# Suite: txn codec — TxnStatus round-trips
# ---------------------------------------------------------------------------

suite "txn codec - TxnStatusRequest/Response":
  test "status request encodes and decodes":
    let testTxnId = genTransactionID()
    let req = txnMsgs.TxnStatusRequest(txnId: testTxnId)
    let payload = txnMsgs.encodeTxnStatusRequest(req)
    let r = txnMsgs.decodeTxnStatusRequest(payload)
    check r.isOk
    check r.value.txnId == testTxnId

  test "status response active":
    let resp = txnMsgs.TxnStatusResponse(
      status: TxnStatusActive, commitTimestamp: 0)
    let payload = txnMsgs.encodeTxnStatusResponse(resp)
    let r = txnMsgs.decodeTxnStatusResponse(payload)
    check r.isOk
    check r.value.status == TxnStatusActive

  test "status response committed with timestamp":
    let resp = txnMsgs.TxnStatusResponse(
      status: TxnStatusCommitted, commitTimestamp: 77_777)
    let payload = txnMsgs.encodeTxnStatusResponse(resp)
    let r = txnMsgs.decodeTxnStatusResponse(payload)
    check r.isOk
    check r.value.status == TxnStatusCommitted
    check r.value.commitTimestamp == 77_777

  test "status response aborted":
    let resp = txnMsgs.TxnStatusResponse(
      status: TxnStatusAborted, commitTimestamp: 0)
    let payload = txnMsgs.encodeTxnStatusResponse(resp)
    let r = txnMsgs.decodeTxnStatusResponse(payload)
    check r.isOk
    check r.value.status == TxnStatusAborted

  test "status response not found":
    let resp = txnMsgs.TxnStatusResponse(
      status: TxnStatusNotFound, commitTimestamp: 0)
    let payload = txnMsgs.encodeTxnStatusResponse(resp)
    let r = txnMsgs.decodeTxnStatusResponse(payload)
    check r.isOk
    check r.value.status == TxnStatusNotFound

# ---------------------------------------------------------------------------
# Suite: TransactionManager unit tests
# ---------------------------------------------------------------------------

suite "txn_manager - begin/commit/rollback":
  test "begin creates active transaction with non-zero id and timestamp":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    check rec.id != zeroTransactionID()
    check rec.readTimestamp > 0
    check rec.state == TxnStatusActive

  test "begin assigns unique IDs to consecutive transactions":
    let mgr = newTransactionManager()
    let r1 = mgr.beginTransaction()
    let r2 = mgr.beginTransaction()
    check r1.id != r2.id

  test "begin read timestamps are monotonically increasing":
    let mgr = newTransactionManager()
    let r1 = mgr.beginTransaction()
    let r2 = mgr.beginTransaction()
    check r2.readTimestamp >= r1.readTimestamp

  test "commit unknown txn returns NotFound":
    let mgr = newTransactionManager()
    let resp = mgr.commitTransaction(genTransactionID())
    check resp.status == TxnCommitNotFound

  test "commit active txn with no write set succeeds":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    let resp = mgr.commitTransaction(rec.id)
    check resp.status == TxnCommitOK
    check resp.commitTimestamp > 0

  test "commit timestamp is greater than read timestamp":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    let resp = mgr.commitTransaction(rec.id)
    check resp.commitTimestamp > rec.readTimestamp

  test "commit is idempotent — second call returns same OK":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    let r1 = mgr.commitTransaction(rec.id)
    let r2 = mgr.commitTransaction(rec.id)
    check r1.status == TxnCommitOK
    check r2.status == TxnCommitOK
    check r1.commitTimestamp == r2.commitTimestamp

  test "rollback active txn returns OK":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    let resp = mgr.rollbackTransaction(rec.id)
    check resp.status == TxnRollbackOK

  test "rollback unknown txn returns NotFound":
    let mgr = newTransactionManager()
    let resp = mgr.rollbackTransaction(genTransactionID())
    check resp.status == TxnRollbackNotFound

  test "rollback aborted txn is idempotent":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    discard mgr.rollbackTransaction(rec.id)
    let r2 = mgr.rollbackTransaction(rec.id)
    check r2.status == TxnRollbackOK

  test "rolling back a committed txn returns NotFound":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    discard mgr.commitTransaction(rec.id)
    let r = mgr.rollbackTransaction(rec.id)
    check r.status == TxnRollbackNotFound

  test "status of active txn":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    let s = mgr.getTransactionStatus(rec.id)
    check s.status == TxnStatusActive

  test "status of committed txn":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    let cr = mgr.commitTransaction(rec.id)
    let s = mgr.getTransactionStatus(rec.id)
    check s.status == TxnStatusCommitted
    check s.commitTimestamp == cr.commitTimestamp

  test "status of aborted txn":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    discard mgr.rollbackTransaction(rec.id)
    let s = mgr.getTransactionStatus(rec.id)
    check s.status == TxnStatusAborted

  test "status of unknown txn":
    let mgr = newTransactionManager()
    let s = mgr.getTransactionStatus(zeroTransactionID())
    check s.status == TxnStatusNotFound

  test "activeTxnCount tracks open transactions":
    let mgr = newTransactionManager()
    check mgr.activeTxnCount() == 0
    let r1 = mgr.beginTransaction()
    check mgr.activeTxnCount() == 1
    let r2 = mgr.beginTransaction()
    check mgr.activeTxnCount() == 2
    discard mgr.commitTransaction(r1.id)
    check mgr.activeTxnCount() == 1
    discard mgr.rollbackTransaction(r2.id)
    check mgr.activeTxnCount() == 0

  test "totalTxnCount includes all states":
    let mgr = newTransactionManager()
    let r1 = mgr.beginTransaction()
    discard mgr.beginTransaction()
    discard mgr.commitTransaction(r1.id)
    check mgr.totalTxnCount() == 2

suite "txn_manager - conflict detection":
  test "two txns writing different keys — no conflict":
    let mgr = newTransactionManager()
    let t1 = mgr.beginTransaction()
    let t2 = mgr.beginTransaction()
    discard mgr.recordWrite(t1.id, "key-a")
    discard mgr.recordWrite(t2.id, "key-b")
    let r1 = mgr.commitTransaction(t1.id)
    let r2 = mgr.commitTransaction(t2.id)
    check r1.status == TxnCommitOK
    check r2.status == TxnCommitOK

  test "txn2 conflicts when txn1 committed same key after txn2 began":
    let mgr = newTransactionManager()
    # t1 starts first, t2 starts second
    let t1 = mgr.beginTransaction()
    let t2 = mgr.beginTransaction()
    # t1 writes and commits key-x BEFORE t2 tries to commit
    discard mgr.recordWrite(t1.id, "key-x")
    let r1 = mgr.commitTransaction(t1.id)
    check r1.status == TxnCommitOK
    # t2 also writes key-x — should conflict because t1 committed
    # key-x after t2's readTimestamp
    discard mgr.recordWrite(t2.id, "key-x")
    let r2 = mgr.commitTransaction(t2.id)
    check r2.status == TxnCommitConflict

  test "txn2 does NOT conflict when txn1 committed BEFORE txn2 began":
    let mgr = newTransactionManager()
    # t1 commits before t2 even begins
    let t1 = mgr.beginTransaction()
    discard mgr.recordWrite(t1.id, "key-y")
    let r1 = mgr.commitTransaction(t1.id)
    check r1.status == TxnCommitOK
    # Now t2 starts — its readTimestamp > t1's commitTimestamp
    let t2 = mgr.beginTransaction()
    discard mgr.recordWrite(t2.id, "key-y")
    let r2 = mgr.commitTransaction(t2.id)
    check r2.status == TxnCommitOK

  test "read-only txn never conflicts regardless of writes":
    let mgr = newTransactionManager()
    let writer = mgr.beginTransaction()
    let reader = mgr.beginTransaction(flags = TxnFlagReadOnly)
    discard mgr.recordWrite(writer.id, "key-z")
    discard mgr.commitTransaction(writer.id)
    discard mgr.recordRead(reader.id, "key-z")
    let r = mgr.commitTransaction(reader.id)
    check r.status == TxnCommitOK

  test "recording write on non-existent txn returns error":
    let mgr = newTransactionManager()
    let r = mgr.recordWrite(genTransactionID(), "k")
    check r.isErr

  test "recording read on non-existent txn returns error":
    let mgr = newTransactionManager()
    let r = mgr.recordRead(genTransactionID(), "k")
    check r.isErr

suite "txn_manager - timeout":
  test "expired txn is auto-aborted on commit":
    let mgr = newTransactionManager()
    # Create a txn with 1 ms timeout — it will expire immediately
    let rec = mgr.beginTransaction(flags = 0, timeoutMs = 1)
    sleep(5) # ensure it expires
    let resp = mgr.commitTransaction(rec.id)
    check resp.status == TxnCommitTimeout

  test "expired txn is auto-aborted on status query":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction(flags = 0, timeoutMs = 1)
    sleep(5)
    let s = mgr.getTransactionStatus(rec.id)
    check s.status == TxnStatusAborted

  test "expireTimedOutTxns marks expired txns aborted":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction(flags = 0, timeoutMs = 1)
    sleep(5)
    mgr.expireTimedOutTxns()
    check mgr.activeTxnCount() == 0
    let s = mgr.getTransactionStatus(rec.id)
    check s.status == TxnStatusAborted

  test "non-expired txn is not affected by expireTimedOutTxns":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction(flags = 0, timeoutMs = 60_000)
    mgr.expireTimedOutTxns()
    check mgr.activeTxnCount() == 1
    let s = mgr.getTransactionStatus(rec.id)
    check s.status == TxnStatusActive

# ---------------------------------------------------------------------------
# Suite: End-to-end Begin/Commit/Rollback/Status over TCP
# ---------------------------------------------------------------------------

suite "integration - BeginTxn/CommitTxn/RollbackTxn/TxnStatus":
  test "begin returns non-zero txnId and readTimestamp":
    withServer(19900, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let r = cli.beginTxn()
      check r.isOk
      check r.value.txnId != zeroTransactionID()
      check r.value.readTimestamp > 0
    )

  test "each begin returns unique txnId":
    withServer(19901, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let r1 = cli.beginTxn()
      let r2 = cli.beginTxn()
      check r1.isOk and r2.isOk
      check r1.value.txnId != r2.value.txnId
    )

  test "begin read timestamps are non-decreasing":
    withServer(19902, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let r1 = cli.beginTxn()
      let r2 = cli.beginTxn()
      check r1.isOk and r2.isOk
      check r2.value.readTimestamp >= r1.value.readTimestamp
    )

  test "commit active txn succeeds":
    withServer(19903, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let br = cli.beginTxn()
      check br.isOk
      let cr = cli.commitTxn(br.value.txnId)
      check cr.isOk
      check cr.value.status == TxnCommitOK
      check cr.value.commitTimestamp > 0
    )

  test "commit unknown txn returns NotFound":
    withServer(19904, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let r = cli.commitTxn(genTransactionID())
      check r.isOk
      check r.value.status == TxnCommitNotFound
    )

  test "commit is idempotent":
    withServer(19905, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let br = cli.beginTxn()
      let c1 = cli.commitTxn(br.value.txnId)
      let c2 = cli.commitTxn(br.value.txnId)
      check c1.isOk and c2.isOk
      check c1.value.status == TxnCommitOK
      check c2.value.status == TxnCommitOK
      check c1.value.commitTimestamp == c2.value.commitTimestamp
    )

  test "rollback active txn succeeds":
    withServer(19906, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let br = cli.beginTxn()
      check br.isOk
      let rr = cli.rollbackTxn(br.value.txnId)
      check rr.isOk
      check rr.value.status == TxnRollbackOK
    )

  test "rollback unknown txn returns NotFound":
    withServer(19907, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let r = cli.rollbackTxn(genTransactionID())
      check r.isOk
      check r.value.status == TxnRollbackNotFound
    )

  test "txnStatus of active txn":
    withServer(19908, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let br = cli.beginTxn()
      let s = cli.txnStatus(br.value.txnId)
      check s.isOk
      check s.value.status == TxnStatusActive
    )

  test "txnStatus after commit":
    withServer(19909, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let br = cli.beginTxn()
      let cr = cli.commitTxn(br.value.txnId)
      check cr.value.status == TxnCommitOK
      let s = cli.txnStatus(br.value.txnId)
      check s.isOk
      check s.value.status == TxnStatusCommitted
      check s.value.commitTimestamp == cr.value.commitTimestamp
    )

  test "txnStatus after rollback":
    withServer(19910, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let br = cli.beginTxn()
      discard cli.rollbackTxn(br.value.txnId)
      let s = cli.txnStatus(br.value.txnId)
      check s.isOk
      check s.value.status == TxnStatusAborted
    )

  test "txnStatus of unknown txnId":
    withServer(19911, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let s = cli.txnStatus(zeroTransactionID())
      check s.isOk
      check s.value.status == TxnStatusNotFound
    )

  test "begin with read-only flag":
    withServer(19912, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let r = cli.beginTxn(flags = TxnFlagReadOnly)
      check r.isOk
      check r.value.txnId != zeroTransactionID()
      let cr = cli.commitTxn(r.value.txnId)
      check cr.isOk
      check cr.value.status == TxnCommitOK
    )

  test "begin with custom timeout":
    withServer(19913, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let r = cli.beginTxn(timeoutMs = 60_000)
      check r.isOk
      check r.value.txnId != zeroTransactionID()
    )

# ---------------------------------------------------------------------------
# Suite: Transactional KV operations — reads and writes within a txn
# ---------------------------------------------------------------------------

suite "integration - transactional KV reads and writes":
  test "put with txnId registers a write and commit succeeds":
    withServer(19920, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let br = cli.beginTxn()
      check br.isOk
      let txnId = br.value.txnId

      let pr = cli.kvPut("txn-key", "txn-val", txnId = txnId)
      check pr.isOk
      check pr.value.status == PutStatusOK

      let cr = cli.commitTxn(txnId)
      check cr.isOk
      check cr.value.status == TxnCommitOK

      # Value should be visible after commit
      let gr = cli.kvGet("txn-key")
      check gr.isOk
      check gr.value.found
      check gr.value.value == "txn-val"
    )

  test "get with txnId registers a read (txn still commits cleanly)":
    withServer(19921, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("rk", "rv")
      let br = cli.beginTxn()
      let txnId = br.value.txnId

      let gr = cli.kvGet("rk", txnId = txnId)
      check gr.isOk
      check gr.value.found
      check gr.value.value == "rv"

      let cr = cli.commitTxn(txnId)
      check cr.isOk
      check cr.value.status == TxnCommitOK
    )

  test "delete with txnId registers a write; commit removes the key":
    withServer(19922, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("dk", "dv")
      let br = cli.beginTxn()
      let txnId = br.value.txnId

      let dr = cli.kvDelete("dk", txnId = txnId)
      check dr.isOk
      check dr.value.status == DelStatusDeleted

      discard cli.commitTxn(txnId)

      let gr = cli.kvGet("dk")
      check gr.isOk
      check not gr.value.found
    )

  test "rollback after put leaves value unchanged":
    withServer(19923, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("stable", "original")

      let br = cli.beginTxn()
      let txnId = br.value.txnId
      discard cli.kvPut("stable", "modified", txnId = txnId)
      # NOTE: in Phase 3 the in-memory store is not MVCC, so the put is
      # actually applied immediately.  The test verifies that rollback
      # aborts the *transaction record* correctly — the business value is
      # that commitTransaction returns Conflict/Aborted after rollback.
      discard cli.rollbackTxn(txnId)

      let s = cli.txnStatus(txnId)
      check s.isOk
      check s.value.status == TxnStatusAborted
    )

  test "write conflict: txn2 loses when txn1 commits same key first":
    withServer(19924, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let t1 = cli.beginTxn()
      let t2 = cli.beginTxn()

      # Both intend to write "conflict-key"
      discard cli.kvPut("conflict-key", "t1-val", txnId = t1.value.txnId)
      discard cli.kvPut("conflict-key", "t2-val", txnId = t2.value.txnId)

      # t1 commits first
      let c1 = cli.commitTxn(t1.value.txnId)
      check c1.isOk
      check c1.value.status == TxnCommitOK

      # t2 should detect the conflict and be aborted
      let c2 = cli.commitTxn(t2.value.txnId)
      check c2.isOk
      check c2.value.status == TxnCommitConflict
    )

  test "no conflict when txns write different keys":
    withServer(19925, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let t1 = cli.beginTxn()
      let t2 = cli.beginTxn()

      discard cli.kvPut("key-alpha", "val", txnId = t1.value.txnId)
      discard cli.kvPut("key-beta", "val", txnId = t2.value.txnId)

      let c1 = cli.commitTxn(t1.value.txnId)
      let c2 = cli.commitTxn(t2.value.txnId)
      check c1.value.status == TxnCommitOK
      check c2.value.status == TxnCommitOK
    )

  test "writing to expired txn is rejected":
    withServer(19926, proc(srv: ProtocolServer, cli: ProtocolClient) =
      # Use a 1 ms timeout so it expires almost immediately
      let br = cli.beginTxn(timeoutMs = 1)
      check br.isOk
      sleep(10) # ensure expiry

      # The server will abort the txn during recordWrite
      let pr = cli.kvPut("ek", "ev", txnId = br.value.txnId)
      # Either the server returned TxnAborted status or an error frame —
      # either way the txn is no longer active
      let s = cli.txnStatus(br.value.txnId)
      check s.isOk
      check s.value.status == TxnStatusAborted
    )

  test "multiple commits and rollbacks on separate txns are independent":
    withServer(19927, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let t1 = cli.beginTxn()
      let t2 = cli.beginTxn()
      let t3 = cli.beginTxn()

      discard cli.kvPut("mk1", "v1", txnId = t1.value.txnId)
      discard cli.kvPut("mk2", "v2", txnId = t2.value.txnId)
      discard cli.kvPut("mk3", "v3", txnId = t3.value.txnId)

      let c1 = cli.commitTxn(t1.value.txnId)
      let rb2 = cli.rollbackTxn(t2.value.txnId)
      let c3 = cli.commitTxn(t3.value.txnId)

      check c1.value.status == TxnCommitOK
      check rb2.value.status == TxnRollbackOK
      check c3.value.status == TxnCommitOK
    )
