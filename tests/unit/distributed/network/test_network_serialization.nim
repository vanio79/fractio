# Comprehensive unit tests for network serialization

import unittest
import tables
import endians
import strutils
import fractio/distributed/network/types as network_types
import fractio/distributed/network/serialization
import fractio/core/types as core_types
import fractio/distributed/raft/group_types as group_types

suite "CRC32 Checksum - Extended":
  test "computeCRC32 empty string":
    let checksum = computeCRC32("")
    check checksum == 0'u32

  test "computeCRC32 known value hello":
    let checksum = computeCRC32("hello")
    check checksum == 0x3610A686'u32

  test "computeCRC32 known value test":
    let checksum = computeCRC32("test")
    check checksum > 0'u32

  test "computeCRC32 binary data":
    let data = @[0x01'u8, 0x02, 0x03, 0x04]
    let checksum = computeCRC32(data)
    check checksum > 0'u32

  test "computeCRC32 consistent results":
    let data = "consistent"
    let checksum1 = computeCRC32(data)
    let checksum2 = computeCRC32(data)
    check checksum1 == checksum2

  test "computeCRC32 different data different checksum":
    let checksum1 = computeCRC32("data1")
    let checksum2 = computeCRC32("data2")
    check checksum1 != checksum2

  test "computeCRC32 byte array":
    let data = @[0xFF'u8, 0xFE, 0xFD, 0xFC, 0xFB]
    let checksum = computeCRC32(data)
    check checksum > 0'u32

  test "computeCRC32 large data":
    let data = "x".repeat(10000)
    let checksum = computeCRC32(data)
    check checksum > 0'u32

suite "Binary Writer - Extended":
  test "write and read uint8":
    var w = newBinaryWriter()
    w.writeUint8(0x42)
    let data = w.getString()
    var r = newBinaryReader(data)
    check r.readUint8() == 0x42'u8

  test "write and read uint8 max value":
    var w = newBinaryWriter()
    w.writeUint8(0xFF)
    let data = w.getString()
    var r = newBinaryReader(data)
    check r.readUint8() == 0xFF'u8

  test "write and read uint8 zero":
    var w = newBinaryWriter()
    w.writeUint8(0)
    let data = w.getString()
    var r = newBinaryReader(data)
    check r.readUint8() == 0'u8

  test "write and read uint16 BE":
    var w = newBinaryWriter()
    w.writeUint16BE(0x1234)
    let data = w.getString()
    check data[0] == '\x12'
    check data[1] == '\x34'
    var r = newBinaryReader(data)
    check r.readUint16BE() == 0x1234'u16

  test "write and read uint16 BE max value":
    var w = newBinaryWriter()
    w.writeUint16BE(0xFFFF)
    let data = w.getString()
    var r = newBinaryReader(data)
    check r.readUint16BE() == 0xFFFF'u16

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

  test "write and read uint32 BE max value":
    var w = newBinaryWriter()
    w.writeUint32BE(0xFFFFFFFF'u32)
    let data = w.getString()
    var r = newBinaryReader(data)
    check r.readUint32BE() == 0xFFFFFFFF'u32

  test "write and read uint64 BE":
    var w = newBinaryWriter()
    w.writeUint64BE(0x0123456789ABCDEF'u64)
    let data = w.getString()
    var r = newBinaryReader(data)
    check r.readUint64BE() == 0x0123456789ABCDEF'u64

  test "write and read uint64 BE max value":
    var w = newBinaryWriter()
    w.writeUint64BE(0xFFFFFFFFFFFFFFFF'u64)
    let data = w.getString()
    var r = newBinaryReader(data)
    check r.readUint64BE() == 0xFFFFFFFFFFFFFFFF'u64

  test "write and read bool true":
    var w = newBinaryWriter()
    w.writeBool(true)
    let data = w.getString()
    var r = newBinaryReader(data)
    check r.readBool() == true

  test "write and read bool false":
    var w = newBinaryWriter()
    w.writeBool(false)
    let data = w.getString()
    var r = newBinaryReader(data)
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
    w.writeNodeID(core_types.NodeID("node-123"))
    let data = w.getString()
    var r = newBinaryReader(data)
    let nodeId = r.readNodeID()
    check string(nodeId) == "node-123"

  test "write and read long string":
    var w = newBinaryWriter()
    let longStr = "x".repeat(10000)
    w.writeString(longStr)
    let data = w.getString()
    var r = newBinaryReader(data)
    check r.readString() == longStr

  test "write multiple values":
    var w = newBinaryWriter()
    w.writeUint8(1)
    w.writeUint16BE(2)
    w.writeUint32BE(3)
    w.writeUint64BE(4)
    w.writeBool(true)
    w.writeString("test")

    let data = w.getString()
    var r = newBinaryReader(data)
    check r.readUint8() == 1'u8
    check r.readUint16BE() == 2'u16
    check r.readUint32BE() == 3'u32
    check r.readUint64BE() == 4'u64
    check r.readBool() == true
    check r.readString() == "test"

  test "getBytes returns correct sequence":
    var w = newBinaryWriter()
    w.writeUint8(0x01)
    w.writeUint8(0x02)
    w.writeUint8(0x03)

    let bytes = w.getBytes()
    check bytes.len == 3
    check bytes[0] == 0x01
    check bytes[1] == 0x02
    check bytes[2] == 0x03

  test "getString returns correct string":
    var w = newBinaryWriter()
    w.writeUint8(0x41)
    w.writeUint8(0x42)
    w.writeUint8(0x43)

    let str = w.getString()
    check str.len == 3
    check str == "ABC"

suite "Binary Reader - Error Handling":
  test "readUint8 error on empty data":
    var r = newBinaryReader("")
    var gotError = false
    try:
      discard r.readUint8()
    except SerializationError:
      gotError = true
    check gotError

  test "readUint16BE error on short data":
    var r = newBinaryReader("a")
    var gotError = false
    try:
      discard r.readUint16BE()
    except SerializationError:
      gotError = true
    check gotError

  test "readUint32BE error on short data":
    var r = newBinaryReader("abc")
    var gotError = false
    try:
      discard r.readUint32BE()
    except SerializationError:
      gotError = true
    check gotError

  test "readUint64BE error on short data":
    var r = newBinaryReader("abcdefg")
    var gotError = false
    try:
      discard r.readUint64BE()
    except SerializationError:
      gotError = true
    check gotError

  test "readString error on truncated data":
    var w = newBinaryWriter()
    w.writeUint32BE(1000'u32)
    w.writeString("short")
    let data = w.getString()

    var r = newBinaryReader(data)
    var gotError = false
    try:
      discard r.readString()
    except SerializationError:
      gotError = true
    check gotError

suite "Binary Reader - Remaining":
  test "remaining returns correct value":
    var w = newBinaryWriter()
    w.writeUint8(1)
    w.writeUint8(2)
    w.writeUint8(3)

    var r = newBinaryReader(w.getString())
    check r.remaining() == 3

    discard r.readUint8()
    check r.remaining() == 2

    discard r.readUint8()
    check r.remaining() == 1

    discard r.readUint8()
    check r.remaining() == 0

suite "Frame Encoding/Decoding - Extended":
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

  test "large payload frame":
    let payload = "x".repeat(100000)
    let frame = encodeFrame(payload)
    check frame.len == FRAME_HEADER_SIZE + payload.len
    let decoded = decodeFrame(frame)
    check decoded.payload.len == 100000

  test "frame roundtrip preserves data":
    for payload in ["", "a", "hello", "x".repeat(1000), "binary\x00data"]:
      let frame = encodeFrame(payload)
      let decoded = decodeFrame(frame)
      check decoded.payload == payload

  test "decodeFrameHeader error on short data":
    let shortData = "abc"
    var gotError = false
    try:
      discard decodeFrameHeader(shortData)
    except SerializationError:
      gotError = true
    check gotError

  test "decodeFrame error on short payload":
    var w = newBinaryWriter()
    w.writeUint32BE(100'u32)
    w.writeUint32BE(0'u32)
    let headerOnly = w.getString()

    var gotError = false
    try:
      discard decodeFrame(headerOnly)
    except SerializationError:
      gotError = true
    check gotError

suite "Message Header Encoding/Decoding - Extended":
  test "encode and decode header":
    let header = newMessageHeader(
      msgType = 1'u16,
      msgId = 123'u64,
      source = core_types.NodeID("node1"),
      target = core_types.NodeID("node2"),
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

  test "header with GroupID":
    let groupId = group_types.GroupID(ulidFromString("0123456789ABCDEFGHJKMNPQRS"))
    let header = newMessageHeader(
      msgType = 1'u16,
      msgId = 123'u64,
      source = core_types.NodeID("node1"),
      target = core_types.NodeID("node2"),
      term = 5'u64,
      groupId = groupId
    )
    let encoded = encodeHeader(header)
    let decoded = decodeHeader(encoded)
    check decoded.groupId == groupId

  test "header with zero GroupID":
    let header = newMessageHeader(
      msgType = 1'u16,
      msgId = 123'u64,
      source = core_types.NodeID("node1"),
      target = core_types.NodeID("node2"),
      term = 5'u64,
      groupId = group_types.ZeroGroupID()
    )
    let encoded = encodeHeader(header)
    let decoded = decodeHeader(encoded)
    check decoded.groupId == group_types.ZeroGroupID()

  test "header with large values":
    let header = newMessageHeader(
      msgType = 0xFFFF'u16,
      msgId = 0xFFFFFFFFFFFFFFFF'u64,
      source = core_types.NodeID("source_node"),
      target = core_types.NodeID("target_node"),
      term = 0xFFFFFFFFFFFFFFFF'u64
    )
    let encoded = encodeHeader(header)
    let decoded = decodeHeader(encoded)
    check decoded.messageType == 0xFFFF'u16
    check decoded.messageId == 0xFFFFFFFFFFFFFFFF'u64

  test "header encoding size":
    let header = newMessageHeader(
      msgType = 1'u16,
      msgId = 123'u64,
      source = core_types.NodeID("node1"),
      target = core_types.NodeID("node2"),
      term = 5'u64
    )
    let encoded = encodeHeader(header)
    check encoded.len > MESSAGE_HEADER_SIZE # NodeID strings add variable length

  test "decodeHeader error on short data":
    var gotError = false
    try:
      discard decodeHeader("short")
    except SerializationError:
      gotError = true
    check gotError

suite "KVRequest Encoding/Decoding - Extended":
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

  test "Scan request":
    var req = KVRequest(kind: rkScan)
    req.scanStartKey = "start"
    req.scanEndKey = "end"
    req.scanLimit = 100'u32
    req.scanTimestamp = 12345'u64
    let encoded = encodeKVRequest(req)
    let decoded = decodeKVRequest(encoded)
    check decoded.kind == rkScan
    check decoded.scanStartKey == "start"
    check decoded.scanEndKey == "end"
    check decoded.scanLimit == 100'u32
    check decoded.scanTimestamp == 12345'u64

  test "KVRequest with empty key":
    var req = KVRequest(kind: rkGet)
    req.getKey = ""
    req.getTimestamp = 0'u64
    let encoded = encodeKVRequest(req)
    let decoded = decodeKVRequest(encoded)
    check decoded.getKey == ""

  test "KVRequest with large value":
    var req = KVRequest(kind: rkPut)
    req.putKey = "key"
    req.putValue = "v".repeat(10000)
    let encoded = encodeKVRequest(req)
    let decoded = decodeKVRequest(encoded)
    check decoded.putValue.len == 10000

suite "KVResponse Encoding/Decoding - Extended":
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

  test "Get response not found":
    var resp = KVResponse(kind: rkGet)
    resp.success = true
    resp.errorMessage = ""
    resp.getValue = ""
    resp.getValueTimestamp = 0'u64
    resp.getFound = false
    let encoded = encodeKVResponse(resp)
    let decoded = decodeKVResponse(encoded)
    check decoded.getFound == false

  test "Put response":
    var resp = KVResponse(kind: rkPut)
    resp.success = true
    resp.errorMessage = ""
    resp.putCommitTimestamp = 99999'u64
    let encoded = encodeKVResponse(resp)
    let decoded = decodeKVResponse(encoded)
    check decoded.putCommitTimestamp == 99999'u64

  test "Delete response":
    var resp = KVResponse(kind: rkDelete)
    resp.success = true
    resp.errorMessage = ""
    resp.deleteCommitTimestamp = 99999'u64
    let encoded = encodeKVResponse(resp)
    let decoded = decodeKVResponse(encoded)
    check decoded.deleteCommitTimestamp == 99999'u64

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

  test "Scan response empty results":
    var resp = KVResponse(kind: rkScan)
    resp.success = true
    resp.errorMessage = ""
    resp.scanKeyValues = @[]
    resp.scanHasMore = false
    let encoded = encodeKVResponse(resp)
    let decoded = decodeKVResponse(encoded)
    check decoded.scanKeyValues.len == 0

  test "Error response":
    var resp = KVResponse(kind: rkGet)
    resp.success = false
    resp.errorMessage = "error message"
    let encoded = encodeKVResponse(resp)
    let decoded = decodeKVResponse(encoded)
    check decoded.success == false
    check decoded.errorMessage == "error message"

suite "Raft Message Encoding/Decoding - Extended":
  test "RequestVote":
    var msg: RequestVoteMsg
    msg.header = newMessageHeader(1'u16, 1'u64, core_types.NodeID(
        "node1"), core_types.NodeID("node2"), 1'u64)
    msg.candidateId = core_types.NodeID("node1")
    msg.lastLogIndex = 10'u64
    msg.lastLogTerm = 1'u64
    let encoded = encodeRequestVoteMsg(msg)
    let decoded = decodeRequestVoteMsg(encoded)
    check string(decoded.candidateId) == "node1"
    check decoded.lastLogIndex == 10'u64
    check decoded.lastLogTerm == 1'u64

  test "RequestVoteResponse":
    var msg: RequestVoteResponseMsg
    msg.header = newMessageHeader(2'u16, 1'u64, core_types.NodeID("node2"),
        core_types.NodeID("node1"), 1'u64)
    msg.voteGranted = true
    msg.term = 5'u64
    let encoded = encodeRequestVoteResponseMsg(msg)
    let decoded = decodeRequestVoteResponseMsg(encoded)
    check decoded.voteGranted == true
    check decoded.term == 5'u64

  test "AppendEntries":
    var msg: AppendEntriesMsg
    msg.header = newMessageHeader(3'u16, 1'u64, core_types.NodeID("leader"),
        core_types.NodeID("follower"), 2'u64)
    msg.leaderId = core_types.NodeID("leader")
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
    msg.header = newMessageHeader(4'u16, 1'u64, core_types.NodeID("follower"),
        core_types.NodeID("leader"), 2'u64)
    msg.success = true
    msg.term = 2'u64
    msg.matchIndex = 10'u64
    msg.rejectHint = 0'u64
    let encoded = encodeAppendEntriesResponseMsg(msg)
    let decoded = decodeAppendEntriesResponseMsg(encoded)
    check decoded.success == true
    check decoded.term == 2'u64
    check decoded.matchIndex == 10'u64

  test "AppendEntriesResponse rejection":
    var msg: AppendEntriesResponseMsg
    msg.header = newMessageHeader(4'u16, 1'u64, core_types.NodeID("follower"),
        core_types.NodeID("leader"), 2'u64)
    msg.success = false
    msg.term = 3'u64
    msg.matchIndex = 0'u64
    msg.rejectHint = 5'u64
    let encoded = encodeAppendEntriesResponseMsg(msg)
    let decoded = decodeAppendEntriesResponseMsg(encoded)
    check decoded.success == false
    check decoded.rejectHint == 5'u64

  test "InstallSnapshot":
    var msg: InstallSnapshotMsg
    msg.header = newMessageHeader(5'u16, 1'u64, core_types.NodeID("leader"),
        core_types.NodeID("follower"), 2'u64)
    msg.leaderId = core_types.NodeID("leader")
    msg.lastIncludedIndex = 100'u64
    msg.lastIncludedTerm = 5'u64
    msg.offset = 0'u64
    msg.done = false
    msg.data = "snapshot_chunk"
    let encoded = encodeInstallSnapshotMsg(msg)
    let decoded = decodeInstallSnapshotMsg(encoded)
    check decoded.lastIncludedIndex == 100'u64
    check decoded.lastIncludedTerm == 5'u64
    check decoded.done == false
    check decoded.data == "snapshot_chunk"

  test "InstallSnapshotResponse":
    var msg: InstallSnapshotResponseMsg
    msg.header = newMessageHeader(6'u16, 1'u64, core_types.NodeID("follower"),
        core_types.NodeID("leader"), 2'u64)
    msg.term = 2'u64
    msg.offset = 100'u64
    let encoded = encodeInstallSnapshotResponseMsg(msg)
    let decoded = decodeInstallSnapshotResponseMsg(encoded)
    check decoded.term == 2'u64
    check decoded.offset == 100'u64

  test "TimeoutNow":
    var msg: TimeoutNowMsg
    msg.header = newMessageHeader(7'u16, 1'u64, core_types.NodeID("leader"),
        core_types.NodeID("follower"), 2'u64)
    let encoded = encodeTimeoutNowMsg(msg)
    let decoded = decodeTimeoutNowMsg(encoded)
    check decoded.header.messageType == 7'u16

  test "ReadIndex":
    var msg: ReadIndexMsg
    msg.header = newMessageHeader(8'u16, 1'u64, core_types.NodeID("client"),
        core_types.NodeID("leader"), 2'u64)
    msg.readRequestId = 123'u64
    let encoded = encodeReadIndexMsg(msg)
    let decoded = decodeReadIndexMsg(encoded)
    check decoded.readRequestId == 123'u64

  test "ReadIndexResponse":
    var msg: ReadIndexResponseMsg
    msg.header = newMessageHeader(9'u16, 1'u64, core_types.NodeID("leader"),
        core_types.NodeID("client"), 2'u64)
    msg.readRequestId = 123'u64
    msg.index = 100'u64
    let encoded = encodeReadIndexResponseMsg(msg)
    let decoded = decodeReadIndexResponseMsg(encoded)
    check decoded.readRequestId == 123'u64
    check decoded.index == 100'u64

suite "Client Message Encoding/Decoding - Extended":
  test "BatchRequest":
    var msg: BatchRequestMsg
    msg.header = newMessageHeader(100'u16, 1'u64, core_types.NodeID("client"),
        core_types.NodeID("server"), 0'u64)
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

  test "BatchRequest multiple operations":
    var msg: BatchRequestMsg
    msg.header = newMessageHeader(100'u16, 1'u64, core_types.NodeID("client"),
        core_types.NodeID("server"), 0'u64)
    msg.requestId = 123'u64
    msg.groupId = 1'u32
    msg.transactionId = 0'u64

    var req1 = KVRequest(kind: rkPut)
    req1.putKey = "key1"
    req1.putValue = "value1"

    var req2 = KVRequest(kind: rkGet)
    req2.getKey = "key2"
    req2.getTimestamp = 100'u64

    var req3 = KVRequest(kind: rkDelete)
    req3.deleteKey = "key3"

    msg.requests = @[req1, req2, req3]

    let encoded = encodeBatchRequestMsg(msg)
    let decoded = decodeBatchRequestMsg(encoded)

    check decoded.requests.len == 3
    check decoded.requests[0].kind == rkPut
    check decoded.requests[1].kind == rkGet
    check decoded.requests[2].kind == rkDelete

  test "BatchResponse":
    var msg: BatchResponseMsg
    msg.header = newMessageHeader(101'u16, 1'u64, core_types.NodeID("server"),
        core_types.NodeID("client"), 0'u64)
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

  test "BatchResponse with error":
    var msg: BatchResponseMsg
    msg.header = newMessageHeader(101'u16, 1'u64, core_types.NodeID("server"),
        core_types.NodeID("client"), 0'u64)
    msg.requestId = 123'u64
    msg.success = false
    msg.errorMessage = "transaction failed"
    msg.responses = @[]
    let encoded = encodeBatchResponseMsg(msg)
    let decoded = decodeBatchResponseMsg(encoded)
    check decoded.success == false
    check decoded.errorMessage == "transaction failed"

  test "ScanRequest":
    var msg: ScanRequestMsg
    msg.header = newMessageHeader(102'u16, 1'u64, core_types.NodeID("client"),
        core_types.NodeID("server"), 0'u64)
    msg.requestId = 123'u64
    msg.groupId = 1'u32
    msg.startKey = "start"
    msg.endKey = "end"
    msg.limit = 100'u32
    msg.timestamp = 12345'u64
    let encoded = encodeScanRequestMsg(msg)
    let decoded = decodeScanRequestMsg(encoded)
    check decoded.requestId == 123'u64
    check decoded.startKey == "start"
    check decoded.endKey == "end"
    check decoded.limit == 100'u32

  test "ScanResponse":
    var msg: ScanResponseMsg
    msg.header = newMessageHeader(103'u16, 1'u64, core_types.NodeID("server"),
        core_types.NodeID("client"), 0'u64)
    msg.requestId = 123'u64
    msg.success = true
    msg.errorMessage = ""
    msg.keyValues = @[("k1", "v1"), ("k2", "v2"), ("k3", "v3")]
    msg.hasMore = true
    msg.continuationToken = "token123"
    let encoded = encodeScanResponseMsg(msg)
    let decoded = decodeScanResponseMsg(encoded)
    check decoded.keyValues.len == 3
    check decoded.hasMore == true
    check decoded.continuationToken == "token123"

suite "2PC Message Encoding/Decoding - Extended":
  test "TxnPrepare":
    var msg: TxnPrepareMsg
    msg.header = newMessageHeader(104'u16, 1'u64, core_types.NodeID("coord"),
        core_types.NodeID("part"), 0'u64)
    msg.txnId = 123'u64
    msg.coordinatorId = core_types.NodeID("coord")
    msg.participantIds = @[core_types.NodeID("part1"), core_types.NodeID("part2")]
    msg.timestamp = 99999'u64
    let encoded = encodeTxnPrepareMsg(msg)
    let decoded = decodeTxnPrepareMsg(encoded)
    check decoded.txnId == 123'u64
    check decoded.participantIds.len == 2

  test "TxnPrepareResponse commit vote":
    var msg: TxnPrepareResponseMsg
    msg.header = newMessageHeader(105'u16, 1'u64, core_types.NodeID("part"),
        core_types.NodeID("coord"), 0'u64)
    msg.txnId = 123'u64
    msg.vote = true
    msg.errorMessage = ""
    let encoded = encodeTxnPrepareResponseMsg(msg)
    let decoded = decodeTxnPrepareResponseMsg(encoded)
    check decoded.vote == true

  test "TxnPrepareResponse abort vote":
    var msg: TxnPrepareResponseMsg
    msg.header = newMessageHeader(105'u16, 1'u64, core_types.NodeID("part"),
        core_types.NodeID("coord"), 0'u64)
    msg.txnId = 123'u64
    msg.vote = false
    msg.errorMessage = "conflict"
    let encoded = encodeTxnPrepareResponseMsg(msg)
    let decoded = decodeTxnPrepareResponseMsg(encoded)
    check decoded.vote == false
    check decoded.errorMessage == "conflict"

  test "TxnCommit":
    var msg: TxnCommitMsg
    msg.header = newMessageHeader(106'u16, 1'u64, core_types.NodeID("coord"),
        core_types.NodeID("part"), 0'u64)
    msg.txnId = 123'u64
    msg.commitTimestamp = 99999'u64
    let encoded = encodeTxnCommitMsg(msg)
    let decoded = decodeTxnCommitMsg(encoded)
    check decoded.txnId == 123'u64
    check decoded.commitTimestamp == 99999'u64

  test "TxnCommitResponse":
    var msg: TxnCommitResponseMsg
    msg.header = newMessageHeader(107'u16, 1'u64, core_types.NodeID("part"),
        core_types.NodeID("coord"), 0'u64)
    msg.txnId = 123'u64
    msg.success = true
    msg.errorMessage = ""
    let encoded = encodeTxnCommitResponseMsg(msg)
    let decoded = decodeTxnCommitResponseMsg(encoded)
    check decoded.success == true

  test "TxnRollback":
    var msg: TxnRollbackMsg
    msg.header = newMessageHeader(108'u16, 1'u64, core_types.NodeID("coord"),
        core_types.NodeID("part"), 0'u64)
    msg.txnId = 123'u64
    let encoded = encodeTxnRollbackMsg(msg)
    let decoded = decodeTxnRollbackMsg(encoded)
    check decoded.txnId == 123'u64

  test "TxnRollbackResponse":
    var msg: TxnRollbackResponseMsg
    msg.header = newMessageHeader(109'u16, 1'u64, core_types.NodeID("part"),
        core_types.NodeID("coord"), 0'u64)
    msg.txnId = 123'u64
    msg.success = true
    let encoded = encodeTxnRollbackResponseMsg(msg)
    let decoded = decodeTxnRollbackResponseMsg(encoded)
    check decoded.success == true

  test "Heartbeat ping":
    var msg: HeartbeatMsg
    msg.header = newMessageHeader(110'u16, 1'u64, core_types.NodeID("client"),
        core_types.NodeID("server"), 0'u64)
    msg.ping = true
    let encoded = encodeHeartbeatMsg(msg)
    let decoded = decodeHeartbeatMsg(encoded)
    check decoded.ping == true

  test "HeartbeatResponse pong":
    var msg: HeartbeatResponseMsg
    msg.header = newMessageHeader(111'u16, 1'u64, core_types.NodeID("server"),
        core_types.NodeID("client"), 0'u64)
    msg.pong = true
    let encoded = encodeHeartbeatResponseMsg(msg)
    let decoded = decodeHeartbeatResponseMsg(encoded)
    check decoded.pong == true

  test "ErrorMsg":
    var msg: ErrorMsg
    msg.header = newMessageHeader(112'u16, 1'u64, core_types.NodeID("server"),
        core_types.NodeID("client"), 0'u64)
    msg.errorCode = 100'u32
    msg.errorMessage = "internal error"
    let encoded = encodeErrorMsg(msg)
    let decoded = decodeErrorMsg(encoded)
    check decoded.errorCode == 100'u32
    check decoded.errorMessage == "internal error"

suite "Admin Message Encoding/Decoding - Extended":
  test "MetricsRequest":
    var msg: MetricsRequestMsg
    msg.header = newMessageHeader(200'u16, 1'u64, core_types.NodeID("client"),
        core_types.NodeID("server"), 0'u64)
    let encoded = encodeMetricsRequestMsg(msg)
    let decoded = decodeMetricsRequestMsg(encoded)
    check decoded.header.messageType == 200'u16

  test "MetricsResponse":
    var msg: MetricsResponseMsg
    msg.header = newMessageHeader(201'u16, 1'u64, core_types.NodeID("server"),
        core_types.NodeID("client"), 0'u64)
    msg.metricsJson = "{\"cpu\": 50, \"memory\": 1024}"
    let encoded = encodeMetricsResponseMsg(msg)
    let decoded = decodeMetricsResponseMsg(encoded)
    check decoded.metricsJson == "{\"cpu\": 50, \"memory\": 1024}"

  test "HealthRequest":
    var msg: HealthRequestMsg
    msg.header = newMessageHeader(202'u16, 1'u64, core_types.NodeID("client"),
        core_types.NodeID("server"), 0'u64)
    let encoded = encodeHealthRequestMsg(msg)
    let decoded = decodeHealthRequestMsg(encoded)
    check decoded.header.messageType == 202'u16

  test "HealthResponse healthy":
    var msg: HealthResponseMsg
    msg.header = newMessageHeader(203'u16, 1'u64, core_types.NodeID("server"),
        core_types.NodeID("client"), 0'u64)
    msg.healthy = true
    msg.details = "all systems operational"
    let encoded = encodeHealthResponseMsg(msg)
    let decoded = decodeHealthResponseMsg(encoded)
    check decoded.healthy == true
    check decoded.details == "all systems operational"

  test "HealthResponse unhealthy":
    var msg: HealthResponseMsg
    msg.header = newMessageHeader(203'u16, 1'u64, core_types.NodeID("server"),
        core_types.NodeID("client"), 0'u64)
    msg.healthy = false
    msg.details = "disk full"
    let encoded = encodeHealthResponseMsg(msg)
    let decoded = decodeHealthResponseMsg(encoded)
    check decoded.healthy == false
    check decoded.details == "disk full"

  test "ConfigRequest":
    var msg: ConfigRequestMsg
    msg.header = newMessageHeader(204'u16, 1'u64, core_types.NodeID("client"),
        core_types.NodeID("server"), 0'u64)
    let encoded = encodeConfigRequestMsg(msg)
    let decoded = decodeConfigRequestMsg(encoded)
    check decoded.header.messageType == 204'u16

  test "ConfigResponse":
    var msg: ConfigResponseMsg
    msg.header = newMessageHeader(205'u16, 1'u64, core_types.NodeID("server"),
        core_types.NodeID("client"), 0'u64)
    msg.configJson = "{\"port\": 9000}"
    let encoded = encodeConfigResponseMsg(msg)
    let decoded = decodeConfigResponseMsg(encoded)
    check decoded.configJson == "{\"port\": 9000}"

  test "NodeInfoRequest":
    var msg: NodeInfoRequestMsg
    msg.header = newMessageHeader(206'u16, 1'u64, core_types.NodeID("client"),
        core_types.NodeID("server"), 0'u64)
    msg.targetNodeId = core_types.NodeID("node2")
    let encoded = encodeNodeInfoRequestMsg(msg)
    let decoded = decodeNodeInfoRequestMsg(encoded)
    check string(decoded.targetNodeId) == "node2"

  test "NodeInfoResponse":
    var msg: NodeInfoResponseMsg
    msg.header = newMessageHeader(207'u16, 1'u64, core_types.NodeID("server"),
        core_types.NodeID("client"), 0'u64)
    msg.nodeId = core_types.NodeID("node1")
    msg.raftAddr = "localhost:9000"
    msg.clientAddr = "localhost:9001"
    msg.adminAddr = "localhost:9002"
    msg.isHealthy = true
    msg.uptime = 3600'u64
    let encoded = encodeNodeInfoResponseMsg(msg)
    let decoded = decodeNodeInfoResponseMsg(encoded)
    check string(decoded.nodeId) == "node1"
    check decoded.raftAddr == "localhost:9000"
    check decoded.isHealthy == true
    check decoded.uptime == 3600'u64

suite "Generic Message Encoding":
  test "encodeMessage wraps payload in frame":
    let payload = "test payload"
    let frame = encodeMessage(1'u16, payload)
    check frame.len == FRAME_HEADER_SIZE + payload.len

  test "getPayload extracts payload from frame":
    let payload = "test payload"
    let frame = encodeFrame(payload)
    let decoded = decodeFrame(frame)
    check getPayload(decoded) == payload

suite "GroupID Encoding/Decoding":
  test "writeGroupID and readGroupID":
    var w = newBinaryWriter()
    let groupId = genGroupIDLocal()
    w.writeGroupID(groupId)
    let data = w.getString()
    var r = newBinaryReader(data)
    let decoded = r.readGroupID()
    check decoded == groupId

  test "writeGroupID for ZeroGroupID":
    var w = newBinaryWriter()
    w.writeGroupID(ZeroGroupID())
    let data = w.getString()
    var r = newBinaryReader(data)
    let decoded = r.readGroupID()
    check decoded == ZeroGroupID()

  test "GroupID roundtrip in header":
    let groupId = genGroupIDLocal()
    let header = newMessageHeader(
      msgType = 1'u16,
      msgId = 123'u64,
      source = core_types.NodeID("node1"),
      target = core_types.NodeID("node2"),
      term = 5'u64,
      groupId = groupId
    )
    let encoded = encodeHeader(header)
    let decoded = decodeHeader(encoded)
    check decoded.groupId == groupId

suite "Serialization Roundtrips":
  test "All Raft message types roundtrip":
    let header = newMessageHeader(1'u16, 1'u64, core_types.NodeID("src"),
        core_types.NodeID("dst"), 1'u64)

    var rv: RequestVoteMsg
    rv.header = header
    rv.candidateId = core_types.NodeID("candidate")
    rv.lastLogIndex = 100'u64
    rv.lastLogTerm = 5'u64
    check decodeRequestVoteMsg(encodeRequestVoteMsg(rv)).lastLogIndex == 100'u64

    var ae: AppendEntriesMsg
    ae.header = header
    ae.leaderId = core_types.NodeID("leader")
    ae.prevLogIndex = 50'u64
    ae.prevLogTerm = 3'u64
    ae.commitIndex = 100'u64
    ae.numEntries = 2'u32
    ae.entriesData = "entries"
    check decodeAppendEntriesMsg(encodeAppendEntriesMsg(ae)).prevLogIndex == 50'u64

    var instSnap: InstallSnapshotMsg
    instSnap.header = header
    instSnap.leaderId = core_types.NodeID("leader")
    instSnap.lastIncludedIndex = 200'u64
    instSnap.lastIncludedTerm = 10'u64
    instSnap.offset = 0'u64
    instSnap.done = true
    instSnap.data = "snapshot"
    check decodeInstallSnapshotMsg(encodeInstallSnapshotMsg(
        instSnap)).lastIncludedIndex == 200'u64

suite "BinaryWriter Capacity":
  test "Writer expands capacity automatically":
    var w = newBinaryWriter(10)
    w.writeString("this is a much longer string than the initial capacity")
    check w.getString().len > 10

  test "Writer handles sequential writes":
    var w = newBinaryWriter()
    for i in 0..<100:
      w.writeUint64BE(uint64(i))
    let data = w.getString()
    check data.len == 800

suite "Constants Validation":
  test "FRAME_HEADER_SIZE is 8":
    check FRAME_HEADER_SIZE == 8

  test "MESSAGE_HEADER_SIZE is correct":
    check MESSAGE_HEADER_SIZE == 48

  test "MAX_MESSAGE_SIZE is 16MB":
    check MAX_MESSAGE_SIZE == 16 * 1024 * 1024

  test "MAX_FRAME_SIZE includes header":
    check MAX_FRAME_SIZE == MAX_MESSAGE_SIZE + FRAME_HEADER_SIZE
