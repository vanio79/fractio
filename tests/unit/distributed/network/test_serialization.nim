# Unit tests for network serialization

import unittest
import fractio/distributed/network/types
import fractio/distributed/network/serialization
import fractio/core/types

suite "CRC32 Checksum":
  test "computeCRC32 empty string":
    let checksum = computeCRC32("")
    check checksum == 0'u32

  test "computeCRC32 known value":
    let checksum = computeCRC32("hello")
    check checksum == 0x3610A686'u32

  test "computeCRC32 binary data":
    let data = @[0x01'u8, 0x02, 0x03, 0x04]
    let checksum = computeCRC32(data)
    check checksum > 0'u32

suite "Binary Writer/Reader":
  test "write and read uint8":
    var w = newBinaryWriter()
    w.writeUint8(0x42)
    let data = w.getString()
    var r = newBinaryReader(data)
    check r.readUint8() == 0x42'u8

  test "write and read uint16 BE":
    var w = newBinaryWriter()
    w.writeUint16BE(0x1234)
    let data = w.getString()
    check data[0] == '\x12'
    check data[1] == '\x34'
    var r = newBinaryReader(data)
    check r.readUint16BE() == 0x1234'u16

  test "write and read uint32 BE":
    var w = newBinaryWriter()
    w.writeUint32BE(0x12345678)
    let data = w.getString()
    check data[0] == '\x12'
    check data[1] == '\x34'
    check data[2] == '\x56'
    check data[3] == '\x78'
    var r = newBinaryReader(data)
    check r.readUint32BE() == 0x12345678'u32

  test "write and read uint64 BE":
    var w = newBinaryWriter()
    w.writeUint64BE(0x0123456789ABCDEF'u64)
    let data = w.getString()
    var r = newBinaryReader(data)
    check r.readUint64BE() == 0x0123456789ABCDEF'u64

  test "write and read bool":
    var w = newBinaryWriter()
    w.writeBool(true)
    w.writeBool(false)
    let data = w.getString()
    var r = newBinaryReader(data)
    check r.readBool() == true
    check r.readBool() == false

  test "write and read string":
    var w = newBinaryWriter()
    w.writeString("hello world")
    let data = w.getString()
    var r = newBinaryReader(data)
    check r.readString() == "hello world"

  test "write and read empty string":
    var w = newBinaryWriter()
    w.writeString("")
    let data = w.getString()
    var r = newBinaryReader(data)
    check r.readString() == ""

  test "write and read NodeID":
    var w = newBinaryWriter()
    w.writeNodeID(NodeID("node-123"))
    let data = w.getString()
    var r = newBinaryReader(data)
    let nodeId = r.readNodeID()
    check string(nodeId) == "node-123"

suite "Frame Encoding/Decoding":
  test "encode and decode frame":
    let payload = "test payload data"
    let frame = encodeFrame(payload)
    check frame.len == FRAME_HEADER_SIZE + payload.len
    let decoded = decodeFrame(frame)
    check decoded.header.payloadLen == payload.len.uint32
    check decoded.payload == payload

  test "frame header is correct":
    let payload = "hello"
    let frame = encodeFrame(payload)
    let (header, payloadStart) = decodeFrameHeader(frame)
    check header.payloadLen == 5'u32
    check header.checksum == computeCRC32(payload)
    check payloadStart == FRAME_HEADER_SIZE

  test "verify checksum":
    let payload = "test data"
    let frame = encodeFrame(payload)
    check verifyFrameChecksum(frame) == true

  test "detect corrupted frame":
    let payload = "test data"
    var frame = encodeFrame(payload)
    frame[frame.len - 1] = '\xFF'
    check verifyFrameChecksum(frame) == false

  test "empty payload frame":
    let payload = ""
    let frame = encodeFrame(payload)
    check frame.len == FRAME_HEADER_SIZE
    let decoded = decodeFrame(frame)
    check decoded.header.payloadLen == 0'u32
    check decoded.payload == ""

suite "Message Header Encoding/Decoding":
  test "encode and decode header":
    let header = newMessageHeader(
      msgType = 1'u16,
      msgId = 123'u64,
      source = NodeID("node1"),
      target = NodeID("node2"),
      term = 5'u64
    )
    let encoded = encodeHeader(header)
    check encoded.len > 0
    let decoded = decodeHeader(encoded)
    check decoded.messageType == 1'u16
    check decoded.messageId == 123'u64
    check string(decoded.sourceNodeId) == "node1"
    check string(decoded.targetNodeId) == "node2"
    check decoded.term == 5'u64

suite "KVRequest Encoding/Decoding":
  test "Get request":
    var req = KVRequest(kind: rkGet)
    req.getKey = "mykey"
    req.getTimestamp = 12345'u64
    let encoded = encodeKVRequest(req)
    let decoded = decodeKVRequest(encoded)
    check decoded.kind == rkGet
    check decoded.getKey == "mykey"
    check decoded.getTimestamp == 12345'u64

  test "Put request":
    var req = KVRequest(kind: rkPut)
    req.putKey = "mykey"
    req.putValue = "myvalue"
    let encoded = encodeKVRequest(req)
    let decoded = decodeKVRequest(encoded)
    check decoded.kind == rkPut
    check decoded.putKey == "mykey"
    check decoded.putValue == "myvalue"

  test "Delete request":
    var req = KVRequest(kind: rkDelete)
    req.deleteKey = "mykey"
    let encoded = encodeKVRequest(req)
    let decoded = decodeKVRequest(encoded)
    check decoded.kind == rkDelete
    check decoded.deleteKey == "mykey"

suite "KVResponse Encoding/Decoding":
  test "Get response found":
    var resp = KVResponse(kind: rkGet)
    resp.success = true
    resp.errorMessage = ""
    resp.getValue = "myvalue"
    resp.getValueTimestamp = 12345'u64
    resp.getFound = true
    let encoded = encodeKVResponse(resp)
    let decoded = decodeKVResponse(encoded)
    check decoded.success == true
    check decoded.getValue == "myvalue"
    check decoded.getValueTimestamp == 12345'u64
    check decoded.getFound == true

  test "Scan response":
    var resp = KVResponse(kind: rkScan)
    resp.success = true
    resp.errorMessage = ""
    resp.scanKeyValues = @[("key1", "value1"), ("key2", "value2")]
    resp.scanHasMore = true
    let encoded = encodeKVResponse(resp)
    let decoded = decodeKVResponse(encoded)
    check decoded.scanKeyValues.len == 2
    check decoded.scanKeyValues[0][0] == "key1"
    check decoded.scanKeyValues[0][1] == "value1"
    check decoded.scanHasMore == true

suite "Raft Message Encoding/Decoding":
  test "RequestVote":
    var msg: RequestVoteMsg
    msg.header = newMessageHeader(1'u16, 1'u64, NodeID("node1"), NodeID(
        "node2"), 1'u64)
    msg.candidateId = NodeID("node1")
    msg.lastLogIndex = 10'u64
    msg.lastLogTerm = 1'u64
    let encoded = encodeRequestVoteMsg(msg)
    let decoded = decodeRequestVoteMsg(encoded)
    check string(decoded.candidateId) == "node1"
    check decoded.lastLogIndex == 10'u64
    check decoded.lastLogTerm == 1'u64

  test "AppendEntries":
    var msg: AppendEntriesMsg
    msg.header = newMessageHeader(3'u16, 1'u64, NodeID("leader"), NodeID(
        "follower"), 2'u64)
    msg.leaderId = NodeID("leader")
    msg.prevLogIndex = 5'u64
    msg.prevLogTerm = 1'u64
    msg.commitIndex = 10'u64
    msg.numEntries = 2'u32
    msg.entriesData = "entry1entry2"
    let encoded = encodeAppendEntriesMsg(msg)
    let decoded = decodeAppendEntriesMsg(encoded)
    check string(decoded.leaderId) == "leader"
    check decoded.prevLogIndex == 5'u64
    check decoded.prevLogTerm == 1'u64
    check decoded.commitIndex == 10'u64
    check decoded.numEntries == 2'u32

  test "AppendEntriesResponse":
    var msg: AppendEntriesResponseMsg
    msg.header = newMessageHeader(4'u16, 1'u64, NodeID("follower"), NodeID(
        "leader"), 2'u64)
    msg.success = true
    msg.term = 2'u64
    msg.matchIndex = 10'u64
    msg.rejectHint = 0'u64
    let encoded = encodeAppendEntriesResponseMsg(msg)
    let decoded = decodeAppendEntriesResponseMsg(encoded)
    check decoded.success == true
    check decoded.term == 2'u64
    check decoded.matchIndex == 10'u64

suite "Client Message Encoding/Decoding":
  test "BatchRequest":
    var msg: BatchRequestMsg
    msg.header = newMessageHeader(100'u16, 1'u64, NodeID("client"), NodeID(
        "server"), 0'u64)
    msg.requestId = 123'u64
    msg.groupId = 1'u32
    msg.transactionId = 0'u64
    var req = KVRequest(kind: rkPut)
    req.putKey = "key"
    req.putValue = "value"
    msg.requests = @[req]
    let encoded = encodeBatchRequestMsg(msg)
    let decoded = decodeBatchRequestMsg(encoded)
    check decoded.requestId == 123'u64
    check decoded.groupId == 1'u32
    check decoded.requests.len == 1
    check decoded.requests[0].kind == rkPut
    check decoded.requests[0].putKey == "key"
    check decoded.requests[0].putValue == "value"

  test "BatchResponse":
    var msg: BatchResponseMsg
    msg.header = newMessageHeader(101'u16, 1'u64, NodeID("server"), NodeID(
        "client"), 0'u64)
    msg.requestId = 123'u64
    msg.success = true
    msg.errorMessage = ""
    var resp = KVResponse(kind: rkPut)
    resp.success = true
    resp.putCommitTimestamp = 99999'u64
    msg.responses = @[resp]
    let encoded = encodeBatchResponseMsg(msg)
    let decoded = decodeBatchResponseMsg(encoded)
    check decoded.requestId == 123'u64
    check decoded.success == true
    check decoded.responses.len == 1
    check decoded.responses[0].putCommitTimestamp == 99999'u64

suite "2PC Message Encoding/Decoding":
  test "TxnPrepare":
    var msg: TxnPrepareMsg
    msg.header = newMessageHeader(104'u16, 1'u64, NodeID("coord"), NodeID(
        "part"), 0'u64)
    msg.txnId = 123'u64
    msg.coordinatorId = NodeID("coord")
    msg.participantIds = @[NodeID("part1"), NodeID("part2")]
    msg.timestamp = 99999'u64
    let encoded = encodeTxnPrepareMsg(msg)
    let decoded = decodeTxnPrepareMsg(encoded)
    check decoded.txnId == 123'u64
    check string(decoded.coordinatorId) == "coord"
    check decoded.participantIds.len == 2
    check decoded.timestamp == 99999'u64

  test "TxnCommit":
    var msg: TxnCommitMsg
    msg.header = newMessageHeader(106'u16, 1'u64, NodeID("coord"), NodeID(
        "part"), 0'u64)
    msg.txnId = 123'u64
    msg.commitTimestamp = 99999'u64
    let encoded = encodeTxnCommitMsg(msg)
    let decoded = decodeTxnCommitMsg(encoded)
    check decoded.txnId == 123'u64
    check decoded.commitTimestamp == 99999'u64
