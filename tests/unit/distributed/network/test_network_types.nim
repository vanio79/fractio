# Unit tests for fractio/distributed/network/types.nim
# Tests message types, enums, constants, and helper procs
#
# Note: group_types.nim re-defines NodeID as distinct uint32, but network/types.nim
# uses NodeID as distinct string from core/types. We use `except NodeID` to avoid conflict.

import std/[unittest, strutils]
import fractio/core/types
import fractio/distributed/network/types
import fractio/distributed/raft/group_types except NodeID

suite "RaftMessageType":

  test "all raft message types defined":
    check rmtRequestVote.ord == 1
    check rmtRequestVoteResponse.ord == 2
    check rmtAppendEntries.ord == 3
    check rmtAppendEntriesResponse.ord == 4
    check rmtInstallSnapshot.ord == 5
    check rmtInstallSnapshotResponse.ord == 6
    check rmtTimeoutNow.ord == 7
    check rmtReadIndex.ord == 8
    check rmtReadIndexResponse.ord == 9

suite "ClientMessageType":

  test "all client message types defined":
    check cmtBatchRequest.ord == 100
    check cmtBatchResponse.ord == 101
    check cmtScanRequest.ord == 102
    check cmtScanResponse.ord == 103
    check cmtTxnPrepare.ord == 104
    check cmtTxnPrepareResponse.ord == 105
    check cmtTxnCommit.ord == 106
    check cmtTxnCommitResponse.ord == 107
    check cmtTxnRollback.ord == 108
    check cmtTxnRollbackResponse.ord == 109
    check cmtHeartbeat.ord == 110
    check cmtHeartbeatResponse.ord == 111
    check cmtError.ord == 112

suite "RequestKind":

  test "all request kinds defined":
    check rkGet.ord == 0
    check rkPut.ord == 1
    check rkDelete.ord == 2
    check rkScan.ord == 3

suite "AdminMessageType":

  test "all admin message types defined":
    check amtMetrics.ord == 200
    check amtMetricsResponse.ord == 201
    check amtHealth.ord == 202
    check amtHealthResponse.ord == 203
    check amtConfig.ord == 204
    check amtConfigResponse.ord == 205
    check amtNodeInfo.ord == 206
    check amtNodeInfoResponse.ord == 207

suite "ConnectionState":

  test "all connection states defined":
    check csIdle.ord == 0
    check csConnecting.ord == 1
    check csConnected.ord == 2
    check csFailed.ord == 3
    check csClosed.ord == 4

suite "NetworkErrorCode":

  test "all network error codes defined":
    check necConnectionRefused.ord == 0
    check necConnectionReset.ord == 1
    check necTimeout.ord == 2
    check necInvalidMessage.ord == 3
    check necChecksumMismatch.ord == 4
    check necUnknownNode.ord == 5
    check necNodeUnhealthy.ord == 6
    check necBufferOverflow.ord == 7
    check necSocketError.ord == 8
    check necUnknownMessageType.ord == 9
    check necSerializationError.ord == 10

suite "Constants":

  test "FRAME_HEADER_SIZE":
    check FRAME_HEADER_SIZE == 8

  test "MESSAGE_HEADER_SIZE":
    check MESSAGE_HEADER_SIZE == 48

  test "MAX_MESSAGE_SIZE":
    check MAX_MESSAGE_SIZE == 16 * 1024 * 1024

  test "MAX_FRAME_SIZE":
    check MAX_FRAME_SIZE == MAX_MESSAGE_SIZE + FRAME_HEADER_SIZE

  test "DEFAULT_CONNECT_TIMEOUT_MS":
    check DEFAULT_CONNECT_TIMEOUT_MS == 5000

  test "DEFAULT_READ_TIMEOUT_MS":
    check DEFAULT_READ_TIMEOUT_MS == 30000

  test "DEFAULT_WRITE_TIMEOUT_MS":
    check DEFAULT_WRITE_TIMEOUT_MS == 30000

  test "DEFAULT_MAX_CONNECTIONS_PER_NODE":
    check DEFAULT_MAX_CONNECTIONS_PER_NODE == 4

  test "DEFAULT_IDLE_TIMEOUT_MS":
    check DEFAULT_IDLE_TIMEOUT_MS == 60000

  test "DEFAULT_HEALTH_CHECK_INTERVAL_MS":
    check DEFAULT_HEALTH_CHECK_INTERVAL_MS == 1000

  test "DEFAULT_FAILURE_THRESHOLD":
    check DEFAULT_FAILURE_THRESHOLD == 3

  test "DEFAULT_RECOVERY_THRESHOLD":
    check DEFAULT_RECOVERY_THRESHOLD == 2

suite "MessageHeader":

  test "newMessageHeader":
    let source = NodeID("node-1")
    let target = NodeID("node-2")
    let groupId = genGroupIDLocal()
    let header = newMessageHeader(uint16(rmtAppendEntries), 123'u64, source,
        target, 5'u64, groupId)

    check header.messageType == uint16(rmtAppendEntries)
    check header.messageId == 123'u64
    check header.sourceNodeId == source
    check header.targetNodeId == target
    check header.term == 5'u64
    check header.timestamp == 0'u64
    check header.groupId == groupId

  test "newMessageHeader default term":
    let source = NodeID("node-1")
    let target = NodeID("node-2")
    let header = newMessageHeader(uint16(cmtBatchRequest), 1'u64, source, target)

    check header.term == 0'u64
    check header.groupId == ZeroGroupID()

  test "MessageHeader string representation":
    let source = NodeID("node-1")
    let target = NodeID("node-2")
    let header = newMessageHeader(1'u16, 123'u64, source, target)
    let str = $header

    check "type=1" in str
    check "id=123" in str
    check "src=node-1" in str
    check "dst=node-2" in str

suite "NetworkError":

  test "newNetworkError":
    let err = newNetworkError(necTimeout, "Connection timed out")
    check err.code == necTimeout
    check err.msg == "Connection timed out"

  test "NetworkError is CatchableError":
    let err = newNetworkError(necConnectionRefused, "test")
    check err of CatchableError

suite "RequestVoteMsg":

  test "RequestVoteMsg construction":
    var msg: RequestVoteMsg
    msg.header = newMessageHeader(uint16(rmtRequestVote), 1'u64, NodeID(
        "node-1"), NodeID("node-2"))
    msg.candidateId = NodeID("node-1")
    msg.lastLogIndex = 10'u64
    msg.lastLogTerm = 5'u64

    check msg.header.messageType == uint16(rmtRequestVote)
    check msg.candidateId == NodeID("node-1")
    check msg.lastLogIndex == 10'u64
    check msg.lastLogTerm == 5'u64

suite "RequestVoteResponseMsg":

  test "RequestVoteResponseMsg construction":
    var msg: RequestVoteResponseMsg
    msg.header = newMessageHeader(uint16(rmtRequestVoteResponse), 1'u64, NodeID(
        "node-2"), NodeID("node-1"))
    msg.voteGranted = true
    msg.term = 6'u64

    check msg.header.messageType == uint16(rmtRequestVoteResponse)
    check msg.voteGranted == true
    check msg.term == 6'u64

suite "AppendEntriesMsg":

  test "AppendEntriesMsg construction":
    var msg: AppendEntriesMsg
    msg.header = newMessageHeader(uint16(rmtAppendEntries), 1'u64, NodeID(
        "node-1"), NodeID("node-2"))
    msg.leaderId = NodeID("node-1")
    msg.prevLogIndex = 5'u64
    msg.prevLogTerm = 3'u64
    msg.commitIndex = 10'u64
    msg.numEntries = 3'u32
    msg.entriesData = "test data"

    check msg.header.messageType == uint16(rmtAppendEntries)
    check msg.leaderId == NodeID("node-1")
    check msg.prevLogIndex == 5'u64
    check msg.prevLogTerm == 3'u64
    check msg.commitIndex == 10'u64
    check msg.numEntries == 3'u32
    check msg.entriesData == "test data"

suite "AppendEntriesResponseMsg":

  test "AppendEntriesResponseMsg success":
    var msg: AppendEntriesResponseMsg
    msg.header = newMessageHeader(uint16(rmtAppendEntriesResponse), 1'u64,
        NodeID("node-2"), NodeID("node-1"))
    msg.success = true
    msg.term = 3'u64
    msg.matchIndex = 8'u64
    msg.rejectHint = 0'u64

    check msg.success == true
    check msg.matchIndex == 8'u64

  test "AppendEntriesResponseMsg failure":
    var msg: AppendEntriesResponseMsg
    msg.success = false
    msg.rejectHint = 5'u64

    check msg.success == false
    check msg.rejectHint == 5'u64

suite "InstallSnapshotMsg":

  test "InstallSnapshotMsg construction":
    var msg: InstallSnapshotMsg
    msg.header = newMessageHeader(uint16(rmtInstallSnapshot), 1'u64, NodeID(
        "node-1"), NodeID("node-2"))
    msg.leaderId = NodeID("node-1")
    msg.lastIncludedIndex = 100'u64
    msg.lastIncludedTerm = 5'u64
    msg.offset = 0'u64
    msg.done = false
    msg.data = "snapshot chunk"

    check msg.header.messageType == uint16(rmtInstallSnapshot)
    check msg.lastIncludedIndex == 100'u64
    check msg.done == false

suite "InstallSnapshotResponseMsg":

  test "InstallSnapshotResponseMsg construction":
    var msg: InstallSnapshotResponseMsg
    msg.header = newMessageHeader(uint16(rmtInstallSnapshotResponse), 1'u64,
        NodeID("node-2"), NodeID("node-1"))
    msg.term = 5'u64
    msg.offset = 1024'u64

    check msg.header.messageType == uint16(rmtInstallSnapshotResponse)
    check msg.offset == 1024'u64

suite "TimeoutNowMsg":

  test "TimeoutNowMsg construction":
    var msg: TimeoutNowMsg
    msg.header = newMessageHeader(uint16(rmtTimeoutNow), 1'u64, NodeID(
        "node-1"), NodeID("node-2"))

    check msg.header.messageType == uint16(rmtTimeoutNow)

suite "ReadIndexMsg":

  test "ReadIndexMsg construction":
    var msg: ReadIndexMsg
    msg.header = newMessageHeader(uint16(rmtReadIndex), 1'u64, NodeID("node-1"),
        NodeID("node-2"))
    msg.readRequestId = 42'u64

    check msg.header.messageType == uint16(rmtReadIndex)
    check msg.readRequestId == 42'u64

suite "ReadIndexResponseMsg":

  test "ReadIndexResponseMsg construction":
    var msg: ReadIndexResponseMsg
    msg.header = newMessageHeader(uint16(rmtReadIndexResponse), 1'u64, NodeID(
        "node-2"), NodeID("node-1"))
    msg.readRequestId = 42'u64
    msg.index = 100'u64

    check msg.header.messageType == uint16(rmtReadIndexResponse)
    check msg.index == 100'u64

suite "KVRequest":

  test "KVRequest Get":
    var req: KVRequest
    req = KVRequest(kind: rkGet)
    req.getKey = "test-key"
    req.getTimestamp = 1000'u64

    check req.kind == rkGet
    check req.getKey == "test-key"

  test "KVRequest Put":
    var req: KVRequest
    req = KVRequest(kind: rkPut)
    req.putKey = "test-key"
    req.putValue = "test-value"

    check req.kind == rkPut
    check req.putKey == "test-key"
    check req.putValue == "test-value"

  test "KVRequest Delete":
    var req: KVRequest
    req = KVRequest(kind: rkDelete)
    req.deleteKey = "test-key"

    check req.kind == rkDelete
    check req.deleteKey == "test-key"

  test "KVRequest Scan":
    var req: KVRequest
    req = KVRequest(kind: rkScan)
    req.scanStartKey = "a"
    req.scanEndKey = "z"
    req.scanLimit = 100'u32
    req.scanTimestamp = 1000'u64

    check req.kind == rkScan
    check req.scanStartKey == "a"
    check req.scanEndKey == "z"
    check req.scanLimit == 100'u32

suite "KVResponse":

  test "KVResponse Get found":
    var resp: KVResponse
    resp = KVResponse(kind: rkGet, success: true, errorMessage: "")
    resp.getValue = "value"
    resp.getValueTimestamp = 1000'u64
    resp.getFound = true

    check resp.success == true
    check resp.kind == rkGet
    check resp.getFound == true

  test "KVResponse Get not found":
    var resp: KVResponse
    resp = KVResponse(kind: rkGet, success: true)
    resp.getFound = false

    check resp.getFound == false

  test "KVResponse Put":
    var resp: KVResponse
    resp = KVResponse(kind: rkPut, success: true)
    resp.putCommitTimestamp = 1000'u64

    check resp.putCommitTimestamp == 1000'u64

  test "KVResponse Delete":
    var resp: KVResponse
    resp = KVResponse(kind: rkDelete, success: true)
    resp.deleteCommitTimestamp = 1000'u64

    check resp.deleteCommitTimestamp == 1000'u64

  test "KVResponse Scan":
    var resp: KVResponse
    resp = KVResponse(kind: rkScan, success: true)
    resp.scanKeyValues = @[("key1", "value1"), ("key2", "value2")]
    resp.scanHasMore = true

    check resp.scanKeyValues.len == 2
    check resp.scanHasMore == true

suite "BatchRequestMsg":

  test "BatchRequestMsg construction":
    var msg: BatchRequestMsg
    msg.header = newMessageHeader(uint16(cmtBatchRequest), 1'u64, NodeID(
        "client"), NodeID("node"))
    msg.requestId = 42'u64
    msg.groupId = 1'u32
    msg.transactionId = 0'u64
    msg.requests = @[
      KVRequest(kind: rkGet),
      KVRequest(kind: rkPut)
    ]

    check msg.header.messageType == uint16(cmtBatchRequest)
    check msg.requests.len == 2

suite "BatchResponseMsg":

  test "BatchResponseMsg construction":
    var msg: BatchResponseMsg
    msg.header = newMessageHeader(uint16(cmtBatchResponse), 1'u64, NodeID(
        "node"), NodeID("client"))
    msg.requestId = 42'u64
    msg.success = true
    msg.errorMessage = ""
    msg.responses = @[
      KVResponse(success: true),
      KVResponse(success: true)
    ]

    check msg.header.messageType == uint16(cmtBatchResponse)
    check msg.success == true
    check msg.responses.len == 2

suite "ScanRequestMsg":

  test "ScanRequestMsg construction":
    var msg: ScanRequestMsg
    msg.header = newMessageHeader(uint16(cmtScanRequest), 1'u64, NodeID(
        "client"), NodeID("node"))
    msg.requestId = 42'u64
    msg.groupId = 1'u32
    msg.startKey = "a"
    msg.endKey = "z"
    msg.limit = 100'u32
    msg.timestamp = 1000'u64

    check msg.header.messageType == uint16(cmtScanRequest)
    check msg.limit == 100'u32

suite "ScanResponseMsg":

  test "ScanResponseMsg construction":
    var msg: ScanResponseMsg
    msg.header = newMessageHeader(uint16(cmtScanResponse), 1'u64, NodeID(
        "node"), NodeID("client"))
    msg.requestId = 42'u64
    msg.success = true
    msg.keyValues = @[("k1", "v1")]
    msg.hasMore = false
    msg.continuationToken = ""

    check msg.header.messageType == uint16(cmtScanResponse)
    check msg.hasMore == false

suite "TxnPrepareMsg":

  test "TxnPrepareMsg construction":
    var msg: TxnPrepareMsg
    msg.header = newMessageHeader(uint16(cmtTxnPrepare), 1'u64, NodeID("coord"),
        NodeID("part"))
    msg.txnId = 123'u64
    msg.coordinatorId = NodeID("coord")
    msg.participantIds = @[NodeID("p1"), NodeID("p2")]
    msg.timestamp = 1000'u64

    check msg.header.messageType == uint16(cmtTxnPrepare)
    check msg.participantIds.len == 2

suite "TxnPrepareResponseMsg":

  test "TxnPrepareResponseMsg commit":
    var msg: TxnPrepareResponseMsg
    msg.header = newMessageHeader(uint16(cmtTxnPrepareResponse), 1'u64, NodeID(
        "part"), NodeID("coord"))
    msg.txnId = 123'u64
    msg.vote = true

    check msg.vote == true

  test "TxnPrepareResponseMsg abort":
    var msg: TxnPrepareResponseMsg
    msg.vote = false
    msg.errorMessage = "conflict detected"

    check msg.vote == false

suite "TxnCommitMsg":

  test "TxnCommitMsg construction":
    var msg: TxnCommitMsg
    msg.header = newMessageHeader(uint16(cmtTxnCommit), 1'u64, NodeID("coord"),
        NodeID("part"))
    msg.txnId = 123'u64
    msg.commitTimestamp = 1000'u64

    check msg.header.messageType == uint16(cmtTxnCommit)

suite "TxnCommitResponseMsg":

  test "TxnCommitResponseMsg construction":
    var msg: TxnCommitResponseMsg
    msg.header = newMessageHeader(uint16(cmtTxnCommitResponse), 1'u64, NodeID(
        "part"), NodeID("coord"))
    msg.txnId = 123'u64
    msg.success = true

    check msg.success == true

suite "TxnRollbackMsg":

  test "TxnRollbackMsg construction":
    var msg: TxnRollbackMsg
    msg.header = newMessageHeader(uint16(cmtTxnRollback), 1'u64, NodeID(
        "coord"), NodeID("part"))
    msg.txnId = 123'u64

    check msg.header.messageType == uint16(cmtTxnRollback)

suite "TxnRollbackResponseMsg":

  test "TxnRollbackResponseMsg construction":
    var msg: TxnRollbackResponseMsg
    msg.header = newMessageHeader(uint16(cmtTxnRollbackResponse), 1'u64, NodeID(
        "part"), NodeID("coord"))
    msg.txnId = 123'u64
    msg.success = true

    check msg.success == true

suite "HeartbeatMsg":

  test "HeartbeatMsg ping":
    var msg: HeartbeatMsg
    msg.header = newMessageHeader(uint16(cmtHeartbeat), 1'u64, NodeID("a"),
        NodeID("b"))
    msg.ping = true

    check msg.ping == true

suite "HeartbeatResponseMsg":

  test "HeartbeatResponseMsg pong":
    var msg: HeartbeatResponseMsg
    msg.header = newMessageHeader(uint16(cmtHeartbeatResponse), 1'u64, NodeID(
        "b"), NodeID("a"))
    msg.pong = true

    check msg.pong == true

suite "ErrorMsg":

  test "ErrorMsg construction":
    var msg: ErrorMsg
    msg.header = newMessageHeader(uint16(cmtError), 1'u64, NodeID("node"),
        NodeID("client"))
    msg.errorCode = 1'u32
    msg.errorMessage = "Internal error"

    check msg.header.messageType == uint16(cmtError)
    check msg.errorCode == 1'u32

suite "MetricsRequestMsg":

  test "MetricsRequestMsg construction":
    var msg: MetricsRequestMsg
    msg.header = newMessageHeader(uint16(amtMetrics), 1'u64, NodeID("admin"),
        NodeID("node"))

    check msg.header.messageType == uint16(amtMetrics)

suite "MetricsResponseMsg":

  test "MetricsResponseMsg construction":
    var msg: MetricsResponseMsg
    msg.header = newMessageHeader(uint16(amtMetricsResponse), 1'u64, NodeID(
        "node"), NodeID("admin"))
    msg.metricsJson = "{\"cpu\": 50}"

    check msg.header.messageType == uint16(amtMetricsResponse)

suite "HealthRequestMsg":

  test "HealthRequestMsg construction":
    var msg: HealthRequestMsg
    msg.header = newMessageHeader(uint16(amtHealth), 1'u64, NodeID("admin"),
        NodeID("node"))

    check msg.header.messageType == uint16(amtHealth)

suite "HealthResponseMsg":

  test "HealthResponseMsg healthy":
    var msg: HealthResponseMsg
    msg.header = newMessageHeader(uint16(amtHealthResponse), 1'u64, NodeID(
        "node"), NodeID("admin"))
    msg.healthy = true
    msg.details = "All services running"

    check msg.healthy == true

  test "HealthResponseMsg unhealthy":
    var msg: HealthResponseMsg
    msg.healthy = false
    msg.details = "Connection pool exhausted"

    check msg.healthy == false

suite "ConfigRequestMsg":

  test "ConfigRequestMsg construction":
    var msg: ConfigRequestMsg
    msg.header = newMessageHeader(uint16(amtConfig), 1'u64, NodeID("admin"),
        NodeID("node"))

    check msg.header.messageType == uint16(amtConfig)

suite "ConfigResponseMsg":

  test "ConfigResponseMsg construction":
    var msg: ConfigResponseMsg
    msg.header = newMessageHeader(uint16(amtConfigResponse), 1'u64, NodeID(
        "node"), NodeID("admin"))
    msg.configJson = "{\"port\": 8080}"

    check msg.header.messageType == uint16(amtConfigResponse)

suite "NodeInfoRequestMsg":

  test "NodeInfoRequestMsg construction":
    var msg: NodeInfoRequestMsg
    msg.header = newMessageHeader(uint16(amtNodeInfo), 1'u64, NodeID("admin"),
        NodeID("node"))
    msg.targetNodeId = NodeID("node-1")

    check msg.header.messageType == uint16(amtNodeInfo)

suite "NodeInfoResponseMsg":

  test "NodeInfoResponseMsg construction":
    var msg: NodeInfoResponseMsg
    msg.header = newMessageHeader(uint16(amtNodeInfoResponse), 1'u64, NodeID(
        "node"), NodeID("admin"))
    msg.nodeId = NodeID("node-1")
    msg.raftAddr = "localhost:7000"
    msg.clientAddr = "localhost:8000"
    msg.adminAddr = "localhost:9000"
    msg.isHealthy = true
    msg.uptime = 3600'u64

    check msg.header.messageType == uint16(amtNodeInfoResponse)
    check msg.uptime == 3600'u64

suite "FrameHeader":

  test "FrameHeader construction":
    var header: FrameHeader
    header.payloadLen = 1024'u32
    header.checksum = 12345'u32

    check header.payloadLen == 1024'u32
    check header.checksum == 12345'u32

suite "Frame":

  test "Frame construction":
    var frame: Frame
    frame.header.payloadLen = 100'u32
    frame.header.checksum = 0'u32
    frame.payload = "test payload"

    check frame.header.payloadLen == 100'u32
    check frame.payload == "test payload"
