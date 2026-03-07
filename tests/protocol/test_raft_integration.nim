# Phase 5 — End-to-end integration tests: Raft-backed ProtocolServer + client.
#
# These tests start a ProtocolServer with server.raftStore set to a
# RaftKVStoreExt backed by a single-node MultiRaftCoordinator (leader forced).
# The client speaks the full binary wire protocol over TCP.
#
# Port allocation: 20150–20199 (no overlap with Phase 1–4 test ports).
# Temp storage: /tmp/fractio_raft_int_<port>/ (cleaned up per suite).

import std/[unittest, os, times]
import fractio/protocol/types
import fractio/protocol/codec
import fractio/protocol/server
import fractio/protocol/client
import fractio/protocol/raft_store
import fractio/protocol/raft_txn
import fractio/protocol/txn_manager
import fractio/protocol/messages/kv
import fractio/protocol/messages/txn as txnMsgs
import fractio/protocol/messages/admin as adminMsgs
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/range/types as rangeTypes

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc makeRaftServer(port: int, storagePath: string): ProtocolServer =
  ## Spin up a ProtocolServer with Raft-backed KV store.
  let coordCfg = CoordinatorConfig(
    nodeId: RangeNodeID(1),
    numWorkers: 1,
    electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
    heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
    storagePath: storagePath,
  )
  let coord = newMultiRaftCoordinator(coordCfg)
  let rid = RangeID(1)
  let desc = newRangeDescriptor(rid, @[], @[])
  let rep = desc.addReplica(RangeNodeID(1))
  let group = coord.createGroup(desc, rep.replicaId)
  group.becomeLeader()
  coord.start()

  let raftSt = newRaftKVStoreExt(coord, proposeTimeoutMs = 3000)
  raftSt.bootstrapSingleShardExt(rid)

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

proc withRaftServer(port: int, storagePath: string,
    body: proc(srv: ProtocolServer, cli: ProtocolClient)) =
  try: removeDir(storagePath) except CatchableError: discard
  try: createDir(storagePath) except CatchableError: discard
  let srv = makeRaftServer(port, storagePath)
  let cli = connectClient(port)
  try:
    body(srv, cli)
  finally:
    cli.disconnect()
    srv.stop()
    sleep(60)
    try: removeDir(storagePath) except CatchableError: discard

# ---------------------------------------------------------------------------
# Suite: Raft-backed put / get / delete over TCP
# ---------------------------------------------------------------------------

suite "Raft integration - put/get/delete":
  test "put and get a value":
    withRaftServer(20150, "/tmp/fractio_raft_int_20150"):
      proc(srv: ProtocolServer, cli: ProtocolClient) =
        let pr = cli.kvPut("raft_key", "raft_value")
        check pr.isOk
        check pr.value.status == PutStatusOK

        let gr = cli.kvGet("raft_key")
        check gr.isOk
        check gr.value.found
        check gr.value.value == "raft_value"

  test "get missing key returns not found":
    withRaftServer(20151, "/tmp/fractio_raft_int_20151"):
      proc(srv: ProtocolServer, cli: ProtocolClient) =
        let gr = cli.kvGet("no_such_key_xyz")
        check gr.isOk
        check not gr.value.found

  test "overwrite key":
    withRaftServer(20152, "/tmp/fractio_raft_int_20152"):
      proc(srv: ProtocolServer, cli: ProtocolClient) =
        discard cli.kvPut("overwrite_k", "v1")
        discard cli.kvPut("overwrite_k", "v2")
        let gr = cli.kvGet("overwrite_k")
        check gr.isOk
        check gr.value.value == "v2"

  test "delete existing key":
    withRaftServer(20153, "/tmp/fractio_raft_int_20153"):
      proc(srv: ProtocolServer, cli: ProtocolClient) =
        discard cli.kvPut("del_k", "del_v")
        let dr = cli.kvDelete("del_k")
        check dr.isOk
        check dr.value.status == DelStatusDeleted

        let gr = cli.kvGet("del_k")
        check gr.isOk
        check not gr.value.found

  test "delete missing key":
    withRaftServer(20154, "/tmp/fractio_raft_int_20154"):
      proc(srv: ProtocolServer, cli: ProtocolClient) =
        let dr = cli.kvDelete("phantom_key")
        check dr.isOk
        check dr.value.status == DelStatusNotFound

# ---------------------------------------------------------------------------
# Suite: Raft-backed scan over TCP
# ---------------------------------------------------------------------------

suite "Raft integration - scan":
  test "scan returns all keys in range":
    withRaftServer(20155, "/tmp/fractio_raft_int_20155"):
      proc(srv: ProtocolServer, cli: ProtocolClient) =
        discard cli.kvPut("s_a", "1")
        discard cli.kvPut("s_b", "2")
        discard cli.kvPut("s_c", "3")
        let sr = cli.kvScan("s_a", "s_d", 0)
        check sr.isOk
        check sr.value.pairs.len == 3
        check sr.value.pairs[0].key == "s_a"
        check sr.value.pairs[1].key == "s_b"
        check sr.value.pairs[2].key == "s_c"

  test "scan with limit":
    withRaftServer(20156, "/tmp/fractio_raft_int_20156"):
      proc(srv: ProtocolServer, cli: ProtocolClient) =
        discard cli.kvPut("lim_1", "a")
        discard cli.kvPut("lim_2", "b")
        discard cli.kvPut("lim_3", "c")
        let sr = cli.kvScan("lim_", "lim_9", 2)
        check sr.isOk
        check sr.value.pairs.len == 2

  test "scan empty range":
    withRaftServer(20157, "/tmp/fractio_raft_int_20157"):
      proc(srv: ProtocolServer, cli: ProtocolClient) =
        let sr = cli.kvScan("empty_z", "empty_z9", 0)
        check sr.isOk
        check sr.value.pairs.len == 0

# ---------------------------------------------------------------------------
# Suite: Raft-backed batch over TCP
# ---------------------------------------------------------------------------

suite "Raft integration - batch":
  test "batch put+get succeeds":
    withRaftServer(20158, "/tmp/fractio_raft_int_20158"):
      proc(srv: ProtocolServer, cli: ProtocolClient) =
        var op1Data = ""
        op1Data.writeBytes("b_k1")
        op1Data.writeBytes("b_v1")
        var op2Data = ""
        op2Data.writeBytes("b_k2")
        op2Data.writeBytes("b_v2")
        let batchReq = BatchRequest(
          flags: 0,
          txnId: 0,
          operations: @[
            BatchOp(kind: BatchOpPut, flags: 0, data: op1Data),
            BatchOp(kind: BatchOpPut, flags: 0, data: op2Data),
          ],
        )
        let br = cli.kvBatch(batchReq)
        check br.isOk
        check br.value.status == BatchStatusAllOK
        check br.value.results.len == 2
        check br.value.results[0].status == 0x00'u8
        check br.value.results[1].status == 0x00'u8

# ---------------------------------------------------------------------------
# Suite: Raft-backed ping (core protocol still works over Raft server)
# ---------------------------------------------------------------------------

suite "Raft integration - core protocol":
  test "ping over Raft-backed server":
    withRaftServer(20159, "/tmp/fractio_raft_int_20159"):
      proc(srv: ProtocolServer, cli: ProtocolClient) =
        let pr = cli.ping()
        check pr.isOk
        check pr.value > 0'u64

  test "echo over Raft-backed server":
    withRaftServer(20160, "/tmp/fractio_raft_int_20160"):
      proc(srv: ProtocolServer, cli: ProtocolClient) =
        let er = cli.echo("hello raft")
        check er.isOk
        check er.value == "hello raft"

# ---------------------------------------------------------------------------
# Suite: Raft-backed metrics reflect Raft KV operations
# ---------------------------------------------------------------------------

suite "Raft integration - metrics":
  test "kvGets and kvPuts counters increment":
    withRaftServer(20161, "/tmp/fractio_raft_int_20161"):
      proc(srv: ProtocolServer, cli: ProtocolClient) =
        discard cli.kvPut("metrics_key", "metrics_val")
        discard cli.kvGet("metrics_key")
        discard cli.kvGet("metrics_key")

        let mr = cli.metrics(0)
        check mr.isOk
        check mr.value.kvGets >= 2'u64
        check mr.value.kvPuts >= 1'u64

# ---------------------------------------------------------------------------
# Suite: Raft-backed server — transactions (Phase 3 manager still in use)
# ---------------------------------------------------------------------------

suite "Raft integration - transactions":
  test "begin/commit transaction":
    withRaftServer(20162, "/tmp/fractio_raft_int_20162"):
      proc(srv: ProtocolServer, cli: ProtocolClient) =
        let br = cli.beginTxn(0, 30_000)
        check br.isOk
        let txnId = br.value.txnId
        check txnId > 0'u64

        discard cli.kvPut("txn_key", "txn_val")
        let cr = cli.commitTxn(txnId)
        check cr.isOk
        check cr.value.status == TxnCommitOK

  test "begin/rollback transaction":
    withRaftServer(20163, "/tmp/fractio_raft_int_20163"):
      proc(srv: ProtocolServer, cli: ProtocolClient) =
        let br = cli.beginTxn(0, 30_000)
        check br.isOk
        let txnId = br.value.txnId
        let rr = cli.rollbackTxn(txnId)
        check rr.isOk
        check rr.value.status == TxnRollbackOK

  test "server health OK":
    withRaftServer(20164, "/tmp/fractio_raft_int_20164"):
      proc(srv: ProtocolServer, cli: ProtocolClient) =
        let hr = cli.health()
        check hr.isOk
        check hr.value.status == HealthOK

# ---------------------------------------------------------------------------
# Suite: NOT_LEADER detection (simulated — raftStore returns rseNotLeader
#         by demoting the group and retrying)
# ---------------------------------------------------------------------------

suite "Raft integration - leader detection":
  test "put on leader node succeeds":
    withRaftServer(20165, "/tmp/fractio_raft_int_20165"):
      proc(srv: ProtocolServer, cli: ProtocolClient) =
        # Node is leader (set up in makeRaftServer), so puts should succeed
        let pr = cli.kvPut("leader_key", "leader_val")
        check pr.isOk
        check pr.value.status == PutStatusOK
