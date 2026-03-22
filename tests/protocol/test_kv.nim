# Integration tests for Phase 2 KV Operations.
#
# Covers:
#   - messages/kv: codec round-trips for Get/Put/Delete/Batch/Scan
#   - router: RouterTable routing logic
#   - server/client: end-to-end Get, Put, Delete, Batch, Scan over TCP
#
# Port allocation: each test suite group uses a distinct port in 19800-19899
# to avoid TIME_WAIT conflicts.

import std/[unittest, os, times, strutils]
import fractio/protocol/types
import fractio/protocol/codec
import fractio/protocol/server
import fractio/protocol/client
import fractio/protocol/router
import fractio/protocol/messages/kv
import fractio/protocol/mvcc_store
import fractio/protocol/txn_manager
import fractio/protocol/raft_store
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/raft/multigroup_types
import fractio/distributed/meta/system_tables
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider

# ---------------------------------------------------------------------------
# Helpers (mirrors test_core.nim conventions)
# ---------------------------------------------------------------------------

var testBasePort {.global.} = 19900

proc nextRaftPort(): int =
  result = testBasePort
  testBasePort += 10

proc startTestServer(port: int): ProtocolServer =
  var cfg = defaultServerConfig()
  cfg.host = "127.0.0.1"
  cfg.port = port
  cfg.idleTimeoutSecs = 120
  result = newProtocolServer(cfg)

  # Set up MVCC store for KV operations (requires single-node Raft)
  let storagePath = "/tmp/fractio_kv_test_" & $port
  try: removeDir(storagePath) except CatchableError: discard
  createDir(storagePath)

  let nodeId = rangeTypes.NodeID(1)
  let raftPort = nextRaftPort()
  let members = @[(nodeId: 1'u32, host: "127.0.0.1", basePort: raftPort)]

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    basePort: raftPort,
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

  # Wait for leader election
  for attempt in 0 ..< 30:
    if coord.isLeader(META_GROUP_ID):
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
# Suite: KV codec — GetRequest/GetResponse round-trips
# ---------------------------------------------------------------------------

suite "kv codec - GetRequest round-trip":
  test "basic get request encodes and decodes":
    let req = GetRequest(flags: 0x03, txnId: 42, readTimestamp: 100,
                         key: "hello")
    let payload = encodeGetRequest(req)
    let r = decodeGetRequest(payload)
    check r.isOk
    let got = r.value
    check got.flags == 0x03
    check got.txnId == 42
    check got.readTimestamp == 100
    check got.key == "hello"

  test "get request with empty key":
    let req = GetRequest(flags: 0, txnId: 0, readTimestamp: 0, key: "")
    let payload = encodeGetRequest(req)
    let r = decodeGetRequest(payload)
    check r.isOk
    check r.value.key == ""

  test "get request with binary key":
    let bk = "\x00\xFF\x42\x00"
    let req = GetRequest(flags: 0, txnId: 0, readTimestamp: 0, key: bk)
    let payload = encodeGetRequest(req)
    let r = decodeGetRequest(payload)
    check r.isOk
    check r.value.key == bk

  test "get response found with all optional fields":
    let resp = GetResponse(found: true, hasTimestamp: true, hasVersion: true,
                           timestamp: 999999, version: 7, value: "world")
    let payload = encodeGetResponse(resp)
    let r = decodeGetResponse(payload)
    check r.isOk
    let got = r.value
    check got.found == true
    check got.hasTimestamp == true
    check got.hasVersion == true
    check got.timestamp == 999999
    check got.version == 7
    check got.value == "world"

  test "get response found without optional fields":
    let resp = GetResponse(found: true, value: "val")
    let payload = encodeGetResponse(resp)
    let r = decodeGetResponse(payload)
    check r.isOk
    let got = r.value
    check got.found == true
    check got.hasTimestamp == false
    check got.hasVersion == false
    check got.value == "val"

  test "get response not found":
    let resp = GetResponse(found: false)
    let payload = encodeGetResponse(resp)
    let r = decodeGetResponse(payload)
    check r.isOk
    check r.value.found == false
    check r.value.value == ""

  test "get response truncated returns error":
    let payload = "\x01\x00" # just the message type, no flags byte
    let r = decodeGetResponse(payload)
    check r.isErr

# ---------------------------------------------------------------------------
# Suite: KV codec — PutRequest/PutResponse round-trips
# ---------------------------------------------------------------------------

suite "kv codec - PutRequest round-trip":
  test "basic put request":
    let req = PutRequest(flags: PutFlagReturnPrev, txnId: 0,
                         expectedVersion: 0, key: "k", value: "v")
    let payload = encodePutRequest(req)
    let r = decodePutRequest(payload)
    check r.isOk
    let got = r.value
    check got.flags == PutFlagReturnPrev
    check got.key == "k"
    check got.value == "v"

  test "put request with CAS flag and expected version":
    let req = PutRequest(flags: PutFlagCAS, txnId: 10,
                         expectedVersion: 42, key: "cas_key", value: "new")
    let payload = encodePutRequest(req)
    let r = decodePutRequest(payload)
    check r.isOk
    check r.value.flags == PutFlagCAS
    check r.value.txnId == 10
    check r.value.expectedVersion == 42

  test "put request with large value":
    let bigVal = repeat("X", 65536)
    let req = PutRequest(flags: 0, txnId: 0, expectedVersion: 0,
                         key: "big", value: bigVal)
    let payload = encodePutRequest(req)
    let r = decodePutRequest(payload)
    check r.isOk
    check r.value.value.len == 65536

  test "put response OK with previous value":
    let resp = PutResponse(status: PutStatusOK, timestamp: 123456,
                           version: 3, hasPreviousValue: true,
                           previousValue: "old")
    let payload = encodePutResponse(resp)
    let r = decodePutResponse(payload)
    check r.isOk
    let got = r.value
    check got.status == PutStatusOK
    check got.timestamp == 123456
    check got.version == 3
    check got.hasPreviousValue == true
    check got.previousValue == "old"

  test "put response OK without previous value":
    let resp = PutResponse(status: PutStatusOK, timestamp: 1,
                           version: 1, hasPreviousValue: false)
    let payload = encodePutResponse(resp)
    let r = decodePutResponse(payload)
    check r.isOk
    check r.value.hasPreviousValue == false

  test "put response CAS failed":
    let resp = PutResponse(status: PutStatusCASFailed, timestamp: 0, version: 0)
    let payload = encodePutResponse(resp)
    let r = decodePutResponse(payload)
    check r.isOk
    check r.value.status == PutStatusCASFailed

# ---------------------------------------------------------------------------
# Suite: KV codec — DeleteRequest/DeleteResponse round-trips
# ---------------------------------------------------------------------------

suite "kv codec - DeleteRequest round-trip":
  test "basic delete request":
    let req = DeleteRequest(flags: 0, txnId: 0, key: "del_key")
    let payload = encodeDeleteRequest(req)
    let r = decodeDeleteRequest(payload)
    check r.isOk
    check r.value.key == "del_key"

  test "delete request return previous":
    let req = DeleteRequest(flags: DelFlagReturnPrev, txnId: 5, key: "x")
    let payload = encodeDeleteRequest(req)
    let r = decodeDeleteRequest(payload)
    check r.isOk
    check r.value.flags == DelFlagReturnPrev
    check r.value.txnId == 5

  test "delete response deleted with previous value":
    let resp = DeleteResponse(status: DelStatusDeleted,
                              hasPreviousValue: true, previousValue: "prev")
    let payload = encodeDeleteResponse(resp)
    let r = decodeDeleteResponse(payload)
    check r.isOk
    let got = r.value
    check got.status == DelStatusDeleted
    check got.hasPreviousValue == true
    check got.previousValue == "prev"

  test "delete response not found":
    let resp = DeleteResponse(status: DelStatusNotFound)
    let payload = encodeDeleteResponse(resp)
    let r = decodeDeleteResponse(payload)
    check r.isOk
    check r.value.status == DelStatusNotFound
    check r.value.hasPreviousValue == false

# ---------------------------------------------------------------------------
# Suite: KV codec — BatchRequest/BatchResponse round-trips
# ---------------------------------------------------------------------------

suite "kv codec - BatchRequest round-trip":
  test "empty batch":
    let req = BatchRequest(flags: 0, txnId: 0, operations: @[])
    let payload = encodeBatchRequest(req)
    let r = decodeBatchRequest(payload)
    check r.isOk
    check r.value.operations.len == 0

  test "batch with Get, Put, Delete ops":
    var opGet: BatchOp
    opGet.kind = BatchOpGet
    opGet.flags = 0
    var opGetBuf = ""
    opGetBuf.writeBytes("mykey")
    opGet.data = opGetBuf

    var opPut: BatchOp
    opPut.kind = BatchOpPut
    opPut.flags = 0
    var opPutBuf = ""
    opPutBuf.writeBytes("pkey")
    opPutBuf.writeBytes("pval")
    opPut.data = opPutBuf

    var opDel: BatchOp
    opDel.kind = BatchOpDelete
    opDel.flags = 0
    var opDelBuf = ""
    opDelBuf.writeBytes("dkey")
    opDel.data = opDelBuf

    let req = BatchRequest(flags: BatchFlagAllOrNothing, txnId: 7,
                           operations: @[opGet, opPut, opDel])
    let payload = encodeBatchRequest(req)
    let r = decodeBatchRequest(payload)
    check r.isOk
    let got = r.value
    check got.flags == BatchFlagAllOrNothing
    check got.txnId == 7
    check got.operations.len == 3
    check got.operations[0].kind == BatchOpGet
    check got.operations[1].kind == BatchOpPut
    check got.operations[2].kind == BatchOpDelete

  test "batch response all OK":
    let resp = BatchResponse(
      status: BatchStatusAllOK,
      results: @[
        BatchOpResult(status: 0x00, data: ""),
        BatchOpResult(status: 0x00, data: ""),
      ],
    )
    let payload = encodeBatchResponse(resp)
    let r = decodeBatchResponse(payload)
    check r.isOk
    check r.value.status == BatchStatusAllOK
    check r.value.results.len == 2

  test "batch response partial failure":
    let resp = BatchResponse(
      status: BatchStatusPartialFailure,
      results: @[
        BatchOpResult(status: 0x00, data: ""),
        BatchOpResult(status: 0x01, data: ""),
      ],
    )
    let payload = encodeBatchResponse(resp)
    let r = decodeBatchResponse(payload)
    check r.isOk
    check r.value.status == BatchStatusPartialFailure
    check r.value.results[0].status == 0x00
    check r.value.results[1].status == 0x01

# ---------------------------------------------------------------------------
# Suite: KV codec — ScanRequest/ScanResponseFrame round-trips
# ---------------------------------------------------------------------------

suite "kv codec - ScanRequest round-trip":
  test "basic scan request":
    let req = ScanRequest(flags: 0, txnId: 0, readTimestamp: 0,
                          startKey: "a", endKey: "z", limit: 100)
    let payload = encodeScanRequest(req)
    let r = decodeScanRequest(payload)
    check r.isOk
    let got = r.value
    check got.startKey == "a"
    check got.endKey == "z"
    check got.limit == 100

  test "scan request full range (empty keys)":
    let req = ScanRequest(flags: 0, txnId: 0, readTimestamp: 0,
                          startKey: "", endKey: "", limit: 0)
    let payload = encodeScanRequest(req)
    let r = decodeScanRequest(payload)
    check r.isOk
    check r.value.startKey == ""
    check r.value.endKey == ""
    check r.value.limit == 0

  test "scan request with flags":
    let req = ScanRequest(
      flags: ScanFlagIncludeTimestamp or ScanFlagIncludeVersion,
      txnId: 0, readTimestamp: 999,
      startKey: "start", endKey: "end", limit: 50,
    )
    let payload = encodeScanRequest(req)
    let r = decodeScanRequest(payload)
    check r.isOk
    check r.value.flags == (ScanFlagIncludeTimestamp or ScanFlagIncludeVersion)

  test "scan response frame with pairs and optional fields":
    let reqFlags = ScanFlagIncludeTimestamp or ScanFlagIncludeVersion
    let rf = ScanResponseFrame(
      respFlags: ScanRespFlagEndOfScan,
      reqFlags: reqFlags,
      pairs: @[
        ScanPair(key: "k1", value: "v1", timestamp: 100, version: 1),
        ScanPair(key: "k2", value: "v2", timestamp: 200, version: 2),
      ],
    )
    let payload = encodeScanResponseFrame(rf)
    let r = decodeScanResponseFrame(payload, reqFlags)
    check r.isOk
    let got = r.value
    check got.pairs.len == 2
    check got.pairs[0].key == "k1"
    check got.pairs[0].value == "v1"
    check got.pairs[0].timestamp == 100
    check got.pairs[0].version == 1
    check got.pairs[1].key == "k2"
    check got.pairs[1].timestamp == 200

  test "scan response frame keys-only (no value)":
    let reqFlags = ScanFlagKeysOnly
    let rf = ScanResponseFrame(
      respFlags: ScanRespFlagEndOfScan,
      reqFlags: reqFlags,
      pairs: @[ScanPair(key: "only_key", value: "")],
    )
    let payload = encodeScanResponseFrame(rf)
    let r = decodeScanResponseFrame(payload, reqFlags)
    check r.isOk
    check r.value.pairs[0].key == "only_key"
    check r.value.pairs[0].value == ""

  test "scan response frame with HasMore flag":
    let rf = ScanResponseFrame(
      respFlags: ScanRespFlagHasMore,
      reqFlags: 0,
      pairs: @[ScanPair(key: "k", value: "v")],
    )
    let payload = encodeScanResponseFrame(rf)
    let r = decodeScanResponseFrame(payload, 0)
    check r.isOk
    check (r.value.respFlags and ScanRespFlagHasMore) != 0

# ---------------------------------------------------------------------------
# Suite: Router
# ---------------------------------------------------------------------------

suite "router - routing table":
  test "empty router returns error":
    let rt = newRouterTable(1)
    let r = rt.routeKey("anything")
    check r.isErr
    check r.error.kind == peNotLeader

  test "single shard routes all keys to local node":
    let rt = newRouterTable(localNodeId = 1)
    rt.bootstrapSingleShard(shardId = 1, raftGroupId = 1,
                            leaderAddr = "127.0.0.1:9000")
    for key in ["", "a", "hello", "zzzzz", "\x00", "\xFF"]:
      let r = rt.routeKey(key)
      check r.isOk
      check r.value.nodeId == 1

  test "isLocalLeader returns true for local shard":
    let rt = newRouterTable(localNodeId = 1)
    rt.bootstrapSingleShard(shardId = 1, raftGroupId = 1)
    check rt.isLocalLeader("anykey") == true

  test "isLocalLeader returns false for empty router":
    let rt = newRouterTable(localNodeId = 1)
    check rt.isLocalLeader("anykey") == false

  test "shardCount returns correct value":
    let rt = newRouterTable(1)
    check rt.shardCount() == 0
    rt.bootstrapSingleShard()
    check rt.shardCount() == 1

  test "updateRoute adds a new shard":
    let rt = newRouterTable(localNodeId = 2)
    let shard = ShardRange(startKey: "a", endKey: "m",
                           shardId: 10, raftGroupId: 10)
    let leader = LeaderInfo(nodeId: 2, nodeAddr: "host:1234", lastSeenMs: 0)
    rt.updateRoute(shard, leader)
    check rt.shardCount() == 1
    let r = rt.routeKey("b")
    check r.isOk
    check r.value.nodeId == 2

  test "updateRoute replaces existing shard":
    let rt = newRouterTable(localNodeId = 1)
    rt.bootstrapSingleShard(shardId = 1, raftGroupId = 1)
    # Replace with new leader
    let shard = ShardRange(startKey: "", endKey: "",
                           shardId: 1, raftGroupId: 1)
    let leader = LeaderInfo(nodeId: 3, nodeAddr: "new-leader:9000")
    rt.updateRoute(shard, leader)
    let r = rt.routeKey("key")
    check r.isOk
    check r.value.nodeId == 3

  test "updateLeader updates leader without touching shard range":
    let rt = newRouterTable(localNodeId = 1)
    rt.bootstrapSingleShard(shardId = 5, raftGroupId = 5)
    rt.updateLeader(5, LeaderInfo(nodeId: 7, nodeAddr: "host2:9001"))
    let r = rt.routeKey("k")
    check r.isOk
    check r.value.nodeId == 7

  test "routeKeys returns pairs for all keys":
    let rt = newRouterTable(localNodeId = 1)
    rt.bootstrapSingleShard()
    let r = rt.routeKeys(@["a", "b", "c"])
    check r.isOk
    check r.value.len == 3

  test "two non-overlapping shards route correctly":
    let rt = newRouterTable(localNodeId = 1)
    let shard1 = ShardRange(startKey: "", endKey: "m",
                            shardId: 1, raftGroupId: 1)
    let leader1 = LeaderInfo(nodeId: 1, nodeAddr: "n1:9000")
    rt.updateRoute(shard1, leader1)
    let shard2 = ShardRange(startKey: "m", endKey: "",
                            shardId: 2, raftGroupId: 2)
    let leader2 = LeaderInfo(nodeId: 2, nodeAddr: "n2:9000")
    rt.updateRoute(shard2, leader2)
    check rt.shardCount() == 2
    let r1 = rt.routeKey("apple")
    check r1.isOk
    check r1.value.nodeId == 1
    let r2 = rt.routeKey("mango")
    check r2.isOk
    check r2.value.nodeId == 2
    let r3 = rt.routeKey("zebra")
    check r3.isOk
    check r3.value.nodeId == 2

# ---------------------------------------------------------------------------
# Suite: End-to-end KV via server + client
# ---------------------------------------------------------------------------

suite "integration - KV Get/Put/Delete":
  test "put then get returns value":
    withServer(19800, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let putR = cli.kvPut("greeting", "hello")
      check putR.isOk
      check putR.value.status == PutStatusOK
      check putR.value.version > 0

      let getR = cli.kvGet("greeting")
      check getR.isOk
      check getR.value.found == true
      check getR.value.value == "hello"
    )

  test "get non-existent key returns not found":
    withServer(19801, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let r = cli.kvGet("nosuchkey")
      check r.isOk
      check r.value.found == false
    )

  test "put overwrites existing key":
    withServer(19802, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("k", "first")
      discard cli.kvPut("k", "second")
      let r = cli.kvGet("k")
      check r.isOk
      check r.value.value == "second"
    )

  test "delete existing key":
    withServer(19803, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("todelete", "bye")
      let delR = cli.kvDelete("todelete")
      check delR.isOk
      check delR.value.status == DelStatusDeleted

      let getR = cli.kvGet("todelete")
      check getR.isOk
      check getR.value.found == false
    )

  test "delete non-existent key returns not found":
    withServer(19804, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let r = cli.kvDelete("ghost")
      check r.isOk
      check r.value.status == DelStatusNotFound
    )

  test "put with ReturnPrev flag returns previous value":
    withServer(19805, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("pk", "initial")
      let r = cli.kvPut("pk", "updated", flags = PutFlagReturnPrev)
      check r.isOk
      check r.value.hasPreviousValue == true
      check r.value.previousValue == "initial"
    )

  test "put with ReturnPrev on new key has no previous value":
    withServer(19806, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let r = cli.kvPut("newpk", "val", flags = PutFlagReturnPrev)
      check r.isOk
      check r.value.hasPreviousValue == false
    )

  test "get with IncludeVersion flag returns version":
    withServer(19807, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("vk", "v")
      let r = cli.kvGet("vk", flags = GetFlagIncludeVersion)
      check r.isOk
      check r.value.found == true
      check r.value.hasVersion == true
      check r.value.version > 0
    )

  test "get with IncludeTimestamp flag returns timestamp":
    withServer(19808, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("tk", "v")
      let r = cli.kvGet("tk", flags = GetFlagIncludeTimestamp)
      check r.isOk
      check r.value.found == true
      check r.value.hasTimestamp == true
      check r.value.timestamp > 0
    )

  test "version increments on successive puts":
    withServer(19809, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let r1 = cli.kvPut("vv", "a", flags = 0)
      let r2 = cli.kvPut("vv", "b", flags = 0)
      check r1.isOk and r2.isOk
      check r2.value.version > r1.value.version
    )

  test "CAS succeeds with correct expected version":
    withServer(19810, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let r1 = cli.kvPut("cas", "v1", flags = 0)
      check r1.isOk
      let v1 = r1.value.version
      let r2 = cli.kvPut("cas", "v2",
                          flags = PutFlagCAS, expectedVersion = v1)
      check r2.isOk
      check r2.value.status == PutStatusOK
    )

  test "CAS fails with incorrect expected version":
    withServer(19811, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("cas2", "v1", flags = 0)
      let r = cli.kvPut("cas2", "v2",
                        flags = PutFlagCAS, expectedVersion = 9999)
      check r.isOk
      check r.value.status == PutStatusCASFailed
    )

  test "delete with ReturnPrev returns the old value":
    withServer(19812, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("drp", "old_val")
      let r = cli.kvDelete("drp", flags = DelFlagReturnPrev)
      check r.isOk
      check r.value.hasPreviousValue == true
      check r.value.previousValue == "old_val"
    )

  test "large value round-trip":
    withServer(19813, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let bigVal = repeat("A", 128 * 1024) # 128 KB
      let pr = cli.kvPut("big", bigVal)
      check pr.isOk
      let gr = cli.kvGet("big")
      check gr.isOk
      check gr.value.found == true
      check gr.value.value.len == 128 * 1024
    )

# ---------------------------------------------------------------------------
# Suite: End-to-end Batch
# ---------------------------------------------------------------------------

suite "integration - Batch":
  test "batch put + get succeeds":
    withServer(19820, proc(srv: ProtocolServer, cli: ProtocolClient) =
      # Build: Put "bk1"="bv1", Put "bk2"="bv2"
      var op1Data = ""
      op1Data.writeBytes("bk1")
      op1Data.writeBytes("bv1")
      var op2Data = ""
      op2Data.writeBytes("bk2")
      op2Data.writeBytes("bv2")
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

      # Verify both keys are present
      let r1 = cli.kvGet("bk1")
      check r1.isOk and r1.value.found and r1.value.value == "bv1"
      let r2 = cli.kvGet("bk2")
      check r2.isOk and r2.value.found and r2.value.value == "bv2"
    )

  test "batch get returns values for existing keys":
    withServer(19821, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("kg1", "kv1")
      discard cli.kvPut("kg2", "kv2")

      var op1Data = ""
      op1Data.writeBytes("kg1")
      var op2Data = ""
      op2Data.writeBytes("kg2")
      let batchReq = BatchRequest(
        flags: 0,
        txnId: 0,
        operations: @[
          BatchOp(kind: BatchOpGet, flags: 0, data: op1Data),
          BatchOp(kind: BatchOpGet, flags: 0, data: op2Data),
        ],
      )
      let br = cli.kvBatch(batchReq)
      check br.isOk
      check br.value.status == BatchStatusAllOK
    )

  test "batch get of non-existent key reports partial failure":
    withServer(19822, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("exists", "yes")
      var op1Data = ""
      op1Data.writeBytes("exists")
      var op2Data = ""
      op2Data.writeBytes("missing")
      let batchReq = BatchRequest(
        flags: 0,
        txnId: 0,
        operations: @[
          BatchOp(kind: BatchOpGet, flags: 0, data: op1Data),
          BatchOp(kind: BatchOpGet, flags: 0, data: op2Data),
        ],
      )
      let br = cli.kvBatch(batchReq)
      check br.isOk
      # First op succeeded, second failed → PartialFailure
      check br.value.status == BatchStatusPartialFailure
      check br.value.results[0].status == 0x00
      check br.value.results[1].status == 0x01
    )

  test "batch delete removes keys":
    withServer(19823, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("bd1", "x")
      discard cli.kvPut("bd2", "y")

      var op1Data = ""
      op1Data.writeBytes("bd1")
      var op2Data = ""
      op2Data.writeBytes("bd2")
      let batchReq = BatchRequest(
        flags: 0,
        txnId: 0,
        operations: @[
          BatchOp(kind: BatchOpDelete, flags: 0, data: op1Data),
          BatchOp(kind: BatchOpDelete, flags: 0, data: op2Data),
        ],
      )
      let br = cli.kvBatch(batchReq)
      check br.isOk
      check br.value.status == BatchStatusAllOK

      check (cli.kvGet("bd1")).value.found == false
      check (cli.kvGet("bd2")).value.found == false
    )

  test "empty batch returns all OK":
    withServer(19824, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let br = cli.kvBatch(BatchRequest(flags: 0, txnId: 0, operations: @[]))
      check br.isOk
      check br.value.status == BatchStatusAllOK
      check br.value.results.len == 0
    )

# ---------------------------------------------------------------------------
# Suite: End-to-end Scan
# ---------------------------------------------------------------------------

suite "integration - Scan":
  test "scan full range returns all keys in order":
    withServer(19830, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("apple", "1")
      discard cli.kvPut("cherry", "3")
      discard cli.kvPut("banana", "2")
      let r = cli.kvScan()
      check r.isOk
      let pairs = r.value.pairs
      check pairs.len == 3
      check pairs[0].key == "apple"
      check pairs[1].key == "banana"
      check pairs[2].key == "cherry"
    )

  test "scan with start key (inclusive)":
    withServer(19831, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("aa", "1")
      discard cli.kvPut("bb", "2")
      discard cli.kvPut("cc", "3")
      let r = cli.kvScan(startKey = "bb")
      check r.isOk
      let pairs = r.value.pairs
      check pairs.len == 2
      check pairs[0].key == "bb"
      check pairs[1].key == "cc"
    )

  test "scan with end key (exclusive)":
    withServer(19832, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("aa", "1")
      discard cli.kvPut("bb", "2")
      discard cli.kvPut("cc", "3")
      let r = cli.kvScan(endKey = "cc")
      check r.isOk
      let pairs = r.value.pairs
      check pairs.len == 2
      check pairs[0].key == "aa"
      check pairs[1].key == "bb"
    )

  test "scan with limit":
    withServer(19833, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("a1", "1")
      discard cli.kvPut("a2", "2")
      discard cli.kvPut("a3", "3")
      discard cli.kvPut("a4", "4")
      let r = cli.kvScan(limit = 2)
      check r.isOk
      check r.value.pairs.len == 2
    )

  test "scan empty store returns no pairs":
    withServer(19834, proc(srv: ProtocolServer, cli: ProtocolClient) =
      let r = cli.kvScan()
      check r.isOk
      check r.value.pairs.len == 0
    )

  test "scan with IncludeTimestamp returns timestamps":
    withServer(19835, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("ts1", "v1")
      let r = cli.kvScan(flags = ScanFlagIncludeTimestamp)
      check r.isOk
      check r.value.pairs.len == 1
      check r.value.pairs[0].timestamp > 0
    )

  test "scan with IncludeVersion returns versions":
    withServer(19836, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("ver1", "v1")
      let r = cli.kvScan(flags = ScanFlagIncludeVersion)
      check r.isOk
      check r.value.pairs.len == 1
      check r.value.pairs[0].version > 0
    )

  test "scan EndOfScan flag is set":
    withServer(19837, proc(srv: ProtocolServer, cli: ProtocolClient) =
      discard cli.kvPut("x", "y")
      let r = cli.kvScan()
      check r.isOk
      check (r.value.respFlags and ScanRespFlagEndOfScan) != 0
    )

  test "scan start-end subrange":
    withServer(19838, proc(srv: ProtocolServer, cli: ProtocolClient) =
      for ch in ["a", "b", "c", "d", "e"]:
        discard cli.kvPut(ch, ch)
      let r = cli.kvScan(startKey = "b", endKey = "d")
      check r.isOk
      let pairs = r.value.pairs
      check pairs.len == 2
      check pairs[0].key == "b"
      check pairs[1].key == "c"
    )
