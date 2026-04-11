# Comprehensive unit tests for raft_transport.nim

import unittest
import tables
import options
import locks
import strutils
import fractio/distributed/network/types
import fractio/distributed/network/serialization
import fractio/distributed/network/tcp_transport
import fractio/distributed/network/connection_manager
import fractio/distributed/network/connection_pool
import fractio/distributed/network/raft_transport
import fractio/distributed/network/config
import fractio/distributed/raft/types as raft_types
import fractio/core/types

suite "Raft Transport Conversion Helpers - Full Coverage":
  test "toNodeID converts int32 server ID to NodeID":
    let nodeId = toNodeID(1'i32)
    check string(nodeId) == "raft_1"

    let nodeId2 = toNodeID(42'i32)
    check string(nodeId2) == "raft_42"

    let nodeId3 = toNodeID(-1'i32)
    check string(nodeId3) == "raft_-1"

    let nodeId4 = toNodeID(0'i32)
    check string(nodeId4) == "raft_0"

    let nodeId5 = toNodeID(2147483647'i32)
    check string(nodeId5) == "raft_2147483647"

  test "toServerId converts NodeID to int32 server ID":
    let serverId = toServerId(NodeID("raft_1"))
    check serverId == 1'i32

    let serverId2 = toServerId(NodeID("raft_42"))
    check serverId2 == 42'i32

    let serverId3 = toServerId(NodeID("raft_0"))
    check serverId3 == 0'i32

    let serverId4 = toServerId(NodeID("raft_100"))
    check serverId4 == 100'i32

  test "toServerId returns -1 for invalid NodeID formats":
    check toServerId(NodeID("invalid")) == -1'i32
    check toServerId(NodeID("raft_")) == -1'i32
    check toServerId(NodeID("")) == -1'i32
    check toServerId(NodeID("raft_abc")) == -1'i32
    check toServerId(NodeID("RAFT_1")) == -1'i32
    check toServerId(NodeID("node_1")) == -1'i32

  test "toNodeID and toServerId are inverses for valid IDs":
    for i in [0'i32, 1, 5, 10, 100, 1000, 10000]:
      let nodeId = toNodeID(i)
      let converted = toServerId(nodeId)
      check converted == i

suite "Raft Transport Log Entry Encoding - Full Coverage":
  test "encodeLogEntries empty sequence":
    let entries: seq[raft_types.LogEntry] = @[]
    let encoded = encodeLogEntries(entries)
    check encoded.len == 4

    var r = newBinaryReader(encoded)
    check r.readUint32BE() == 0'u32
    check r.remaining() == 0

  test "encodeLogEntries single normal entry":
    var entry: raft_types.LogEntry
    entry.term = 1'i64
    entry.entryType = raft_types.LogEntryType.LET_NORMAL
    entry.data = "test data"

    let entries = @[entry]
    let encoded = encodeLogEntries(entries)
    check encoded.len > 0

    var r = newBinaryReader(encoded)
    check r.readUint32BE() == 1'u32
    check r.readUint64BE() == 1'u64
    check r.readUint8() == uint8(raft_types.LogEntryType.LET_NORMAL)
    check r.readString() == "test data"

  test "encodeLogEntries single configuration entry":
    var entry: raft_types.LogEntry
    entry.term = 5'i64
    entry.entryType = raft_types.LogEntryType.LET_CONFIG_CHANGE
    entry.data = "config_data"

    let entries = @[entry]
    let encoded = encodeLogEntries(entries)
    check encoded.len > 0

    var r = newBinaryReader(encoded)
    check r.readUint32BE() == 1'u32
    check r.readUint64BE() == 5'u64
    check r.readUint8() == uint8(raft_types.LogEntryType.LET_CONFIG_CHANGE)
    check r.readString() == "config_data"

  test "encodeLogEntries multiple entries":
    var entries: seq[raft_types.LogEntry] = @[]
    for i in 1..5:
      var entry: raft_types.LogEntry
      entry.term = int64(i)
      entry.entryType = raft_types.LogEntryType.LET_NORMAL
      entry.data = "entry" & $i
      entries.add(entry)

    let encoded = encodeLogEntries(entries)
    check encoded.len > 0

    let decoded = decodeLogEntries(encoded)
    check decoded.len == 5
    for i in 0..<5:
      check decoded[i].term == int64(i + 1)
      check decoded[i].entryType == raft_types.LogEntryType.LET_NORMAL
      check decoded[i].data == "entry" & $(i + 1)

  test "encodeLogEntries with empty data":
    var entry: raft_types.LogEntry
    entry.term = 1'i64
    entry.entryType = raft_types.LogEntryType.LET_NORMAL
    entry.data = ""

    let entries = @[entry]
    let encoded = encodeLogEntries(entries)

    let decoded = decodeLogEntries(encoded)
    check decoded.len == 1
    check decoded[0].term == 1'i64
    check decoded[0].data == ""

  test "encodeLogEntries with large data":
    var entry: raft_types.LogEntry
    entry.term = 1'i64
    entry.entryType = raft_types.LogEntryType.LET_NORMAL
    entry.data = "x".repeat(10000)

    let entries = @[entry]
    let encoded = encodeLogEntries(entries)

    let decoded = decodeLogEntries(encoded)
    check decoded.len == 1
    check decoded[0].data.len == 10000

  test "decodeLogEntries roundtrip":
    var entries: seq[raft_types.LogEntry] = @[]
    for i in 1..3:
      var entry: raft_types.LogEntry
      entry.term = int64(i * 10)
      entry.entryType = if i mod 2 == 0: raft_types.LogEntryType.LET_CONFIG_CHANGE else: raft_types.LogEntryType.LET_NORMAL
      entry.data = "data_" & $i
      entries.add(entry)

    let encoded = encodeLogEntries(entries)
    let decoded = decodeLogEntries(encoded)

    check decoded.len == entries.len
    for i in 0..<entries.len:
      check decoded[i].term == entries[i].term
      check decoded[i].entryType == entries[i].entryType
      check decoded[i].data == entries[i].data

suite "Raft Transport Creation and Lifecycle":
  test "Create Raft transport with valid parameters":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    check rt != nil
    check rt.serverId == 1'i32
    check string(rt.nodeId) == "raft_1"
    check rt.connManager != nil
    check rt.handlers.len == 0

    rt.close()
    connMgr.close()

  test "Create Raft transport with different server IDs":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)

    for serverId in [0'i32, 1, 5, 10, 100]:
      let rt = newRaftTransport(connMgr, serverId)
      check rt.serverId == serverId
      check string(rt.nodeId) == "raft_" & $serverId
      rt.close()

    connMgr.close()

  test "Raft transport has empty handlers initially":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    check rt.getHandler(uint16(rmtRequestVote)).isNone
    check rt.getHandler(uint16(rmtAppendEntries)).isNone
    check rt.getHandler(uint16(rmtInstallSnapshot)).isNone
    check rt.getHandler(uint16(rmtTimeoutNow)).isNone
    check rt.getHandler(uint16(rmtReadIndex)).isNone
    check rt.getHandler(uint16(999)).isNone

    rt.close()
    connMgr.close()

  test "Multiple close calls are safe":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    rt.close()
    rt.close()
    rt.close()

    connMgr.close()

suite "Raft Transport Handler Registration - Full Coverage":
  test "Register and retrieve single handler":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    proc testHandler(data: string): string {.gcsafe.} =
      result = "response"

    rt.registerHandler(uint16(rmtRequestVote), testHandler)

    let handler = rt.getHandler(uint16(rmtRequestVote))
    check handler.isSome

    rt.close()
    connMgr.close()

  test "Register multiple handlers for different message types":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    proc handlerRV(data: string): string {.gcsafe.} = "rv"
    proc handlerAE(data: string): string {.gcsafe.} = "ae"
    proc handlerIS(data: string): string {.gcsafe.} = "is"
    proc handlerTN(data: string): string {.gcsafe.} = "tn"
    proc handlerRI(data: string): string {.gcsafe.} = "ri"

    rt.registerHandler(uint16(rmtRequestVote), handlerRV)
    rt.registerHandler(uint16(rmtAppendEntries), handlerAE)
    rt.registerHandler(uint16(rmtInstallSnapshot), handlerIS)
    rt.registerHandler(uint16(rmtTimeoutNow), handlerTN)
    rt.registerHandler(uint16(rmtReadIndex), handlerRI)

    check rt.getHandler(uint16(rmtRequestVote)).isSome
    check rt.getHandler(uint16(rmtAppendEntries)).isSome
    check rt.getHandler(uint16(rmtInstallSnapshot)).isSome
    check rt.getHandler(uint16(rmtTimeoutNow)).isSome
    check rt.getHandler(uint16(rmtReadIndex)).isSome

    rt.close()
    connMgr.close()

  test "Replace handler for same message type":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    var callCount = 0

    proc handler1(data: string): string {.gcsafe.} =
      callCount = 1
      result = "h1"

    proc handler2(data: string): string {.gcsafe.} =
      callCount = 2
      result = "h2"

    rt.registerHandler(uint16(rmtRequestVote), handler1)
    rt.registerHandler(uint16(rmtRequestVote), handler2)

    var msg: RequestVoteMsg
    msg.header = newMessageHeader(uint16(rmtRequestVote), 1'u64,
                                  NodeID("raft_2"), NodeID("raft_1"), 1'u64)
    msg.candidateId = NodeID("raft_2")
    msg.lastLogIndex = 0'u64
    msg.lastLogTerm = 0'u64

    discard rt.handleRequestVote(encodeRequestVoteMsg(msg))
    check callCount == 2

    rt.close()
    connMgr.close()

  test "Get handler for unregistered message type returns none":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    proc handler(data: string): string {.gcsafe.} = "response"

    rt.registerHandler(uint16(rmtRequestVote), handler)

    check rt.getHandler(uint16(rmtAppendEntries)).isNone
    check rt.getHandler(uint16(999)).isNone

    rt.close()
    connMgr.close()

suite "Raft Transport RequestVote Handling - Full Coverage":
  test "handleRequestVote with no handler returns default deny response":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    var msg: RequestVoteMsg
    msg.header = newMessageHeader(uint16(rmtRequestVote), 1'u64,
                                  NodeID("raft_2"), NodeID("raft_1"), 1'u64)
    msg.candidateId = NodeID("raft_2")
    msg.lastLogIndex = 10'u64
    msg.lastLogTerm = 1'u64

    let payload = encodeRequestVoteMsg(msg)
    let response = rt.handleRequestVote(payload)

    check response.len > 0

    let respMsg = decodeRequestVoteResponseMsg(response)
    check respMsg.voteGranted == false
    check respMsg.term == 0'u64
    check respMsg.header.messageType == uint16(rmtRequestVoteResponse)

    rt.close()
    connMgr.close()

  test "handleRequestVote with custom handler":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    proc customHandler(data: string): string {.gcsafe.} =
      let msg = decodeRequestVoteMsg(data)
      var resp: RequestVoteResponseMsg
      resp.header = newMessageHeader(uint16(rmtRequestVoteResponse), msg.header.messageId,
                                      msg.header.targetNodeId,
                                          msg.header.sourceNodeId, 5'u64)
      resp.voteGranted = true
      resp.term = 5'u64
      result = encodeRequestVoteResponseMsg(resp)

    rt.registerHandler(uint16(rmtRequestVote), customHandler)

    var msg: RequestVoteMsg
    msg.header = newMessageHeader(uint16(rmtRequestVote), 42'u64,
                                  NodeID("raft_2"), NodeID("raft_1"), 1'u64)
    msg.candidateId = NodeID("raft_2")
    msg.lastLogIndex = 0'u64
    msg.lastLogTerm = 0'u64

    let response = rt.handleRequestVote(encodeRequestVoteMsg(msg))
    let respMsg = decodeRequestVoteResponseMsg(response)

    check respMsg.voteGranted == true
    check respMsg.term == 5'u64
    check respMsg.header.messageId == 42'u64

    rt.close()
    connMgr.close()

  test "handleRequestVote preserves message correlation":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    var msg: RequestVoteMsg
    msg.header = newMessageHeader(uint16(rmtRequestVote), 999'u64,
                                  NodeID("raft_candidate"), NodeID("raft_1"), 7'u64)
    msg.candidateId = NodeID("raft_candidate")
    msg.lastLogIndex = 100'u64
    msg.lastLogTerm = 5'u64

    let response = rt.handleRequestVote(encodeRequestVoteMsg(msg))
    let respMsg = decodeRequestVoteResponseMsg(response)

    check respMsg.header.messageId == 999'u64
    check string(respMsg.header.sourceNodeId) == "raft_1"
    check string(respMsg.header.targetNodeId) == "raft_candidate"

    rt.close()
    connMgr.close()

suite "Raft Transport AppendEntries Handling - Full Coverage":
  test "handleAppendEntries with no handler returns default success response":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    var msg: AppendEntriesMsg
    msg.header = newMessageHeader(uint16(rmtAppendEntries), 1'u64,
                                  NodeID("raft_2"), NodeID("raft_1"), 1'u64)
    msg.leaderId = NodeID("raft_2")
    msg.prevLogIndex = 5'u64
    msg.prevLogTerm = 1'u64
    msg.commitIndex = 10'u64
    msg.numEntries = 2'u32
    msg.entriesData = encodeLogEntries(@[])

    let payload = encodeAppendEntriesMsg(msg)
    let response = rt.handleAppendEntries(payload)

    check response.len > 0

    let respMsg = decodeAppendEntriesResponseMsg(response)
    check respMsg.success == true
    check respMsg.term == 0'u64
    check respMsg.matchIndex == msg.prevLogIndex + uint64(msg.numEntries)
    check respMsg.rejectHint == 0'u64

    rt.close()
    connMgr.close()

  test "handleAppendEntries with entries":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    var entries: seq[raft_types.LogEntry] = @[]
    for i in 1..3:
      var entry: raft_types.LogEntry
      entry.term = 1'i64
      entry.entryType = raft_types.LogEntryType.LET_NORMAL
      entry.data = "entry" & $i
      entries.add(entry)

    var msg: AppendEntriesMsg
    msg.header = newMessageHeader(uint16(rmtAppendEntries), 1'u64,
                                  NodeID("raft_leader"), NodeID("raft_1"), 2'u64)
    msg.leaderId = NodeID("raft_leader")
    msg.prevLogIndex = 10'u64
    msg.prevLogTerm = 1'u64
    msg.commitIndex = 15'u64
    msg.numEntries = 3'u32
    msg.entriesData = encodeLogEntries(entries)

    let payload = encodeAppendEntriesMsg(msg)
    let response = rt.handleAppendEntries(payload)

    let respMsg = decodeAppendEntriesResponseMsg(response)
    check respMsg.success == true
    check respMsg.matchIndex == 10'u64 + 3'u64

    rt.close()
    connMgr.close()

  test "handleAppendEntries with custom handler":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    proc customHandler(data: string): string {.gcsafe.} =
      let msg = decodeAppendEntriesMsg(data)
      var resp: AppendEntriesResponseMsg
      resp.header = newMessageHeader(uint16(rmtAppendEntriesResponse), msg.header.messageId,
                                      msg.header.targetNodeId,
                                          msg.header.sourceNodeId, 3'u64)
      resp.success = false
      resp.term = 3'u64
      resp.matchIndex = 0'u64
      resp.rejectHint = msg.prevLogIndex - 1'u64
      result = encodeAppendEntriesResponseMsg(resp)

    rt.registerHandler(uint16(rmtAppendEntries), customHandler)

    var msg: AppendEntriesMsg
    msg.header = newMessageHeader(uint16(rmtAppendEntries), 1'u64,
                                  NodeID("raft_2"), NodeID("raft_1"), 2'u64)
    msg.leaderId = NodeID("raft_2")
    msg.prevLogIndex = 5'u64
    msg.prevLogTerm = 1'u64
    msg.commitIndex = 10'u64
    msg.numEntries = 0'u32
    msg.entriesData = ""

    let response = rt.handleAppendEntries(encodeAppendEntriesMsg(msg))
    let respMsg = decodeAppendEntriesResponseMsg(response)

    check respMsg.success == false
    check respMsg.term == 3'u64
    check respMsg.rejectHint == 4'u64

    rt.close()
    connMgr.close()

  test "handleAppendEntries zero entries":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    var msg: AppendEntriesMsg
    msg.header = newMessageHeader(uint16(rmtAppendEntries), 1'u64,
                                  NodeID("raft_leader"), NodeID("raft_1"), 1'u64)
    msg.leaderId = NodeID("raft_leader")
    msg.prevLogIndex = 100'u64
    msg.prevLogTerm = 5'u64
    msg.commitIndex = 100'u64
    msg.numEntries = 0'u32
    msg.entriesData = encodeLogEntries(@[])

    let response = rt.handleAppendEntries(encodeAppendEntriesMsg(msg))
    let respMsg = decodeAppendEntriesResponseMsg(response)

    check respMsg.success == true
    check respMsg.matchIndex == 100'u64

    rt.close()
    connMgr.close()

suite "Raft Transport InstallSnapshot Handling - Full Coverage":
  test "handleInstallSnapshot with no handler returns default response":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    var msg: InstallSnapshotMsg
    msg.header = newMessageHeader(uint16(rmtInstallSnapshot), 1'u64,
                                  NodeID("raft_2"), NodeID("raft_1"), 1'u64)
    msg.leaderId = NodeID("raft_2")
    msg.lastIncludedIndex = 100'u64
    msg.lastIncludedTerm = 2'u64
    msg.offset = 0'u64
    msg.done = true
    msg.data = "snapshot_data"

    let payload = encodeInstallSnapshotMsg(msg)
    let response = rt.handleInstallSnapshot(payload)

    check response.len > 0

    let respMsg = decodeInstallSnapshotResponseMsg(response)
    check respMsg.term == 0'u64
    check respMsg.offset == uint64("snapshot_data".len)

    rt.close()
    connMgr.close()

  test "handleInstallSnapshot with empty data":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    var msg: InstallSnapshotMsg
    msg.header = newMessageHeader(uint16(rmtInstallSnapshot), 1'u64,
                                  NodeID("raft_2"), NodeID("raft_1"), 1'u64)
    msg.leaderId = NodeID("raft_2")
    msg.lastIncludedIndex = 100'u64
    msg.lastIncludedTerm = 2'u64
    msg.offset = 50'u64
    msg.done = false
    msg.data = ""

    let payload = encodeInstallSnapshotMsg(msg)
    let response = rt.handleInstallSnapshot(payload)

    let respMsg = decodeInstallSnapshotResponseMsg(response)
    check respMsg.offset == 50'u64

    rt.close()
    connMgr.close()

  test "handleInstallSnapshot with custom handler":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    proc customHandler(data: string): string {.gcsafe.} =
      let msg = decodeInstallSnapshotMsg(data)
      var resp: InstallSnapshotResponseMsg
      resp.header = newMessageHeader(uint16(rmtInstallSnapshotResponse), msg.header.messageId,
                                      msg.header.targetNodeId,
                                          msg.header.sourceNodeId, 7'u64)
      resp.term = 7'u64
      resp.offset = msg.offset + uint64(msg.data.len) + 100'u64
      result = encodeInstallSnapshotResponseMsg(resp)

    rt.registerHandler(uint16(rmtInstallSnapshot), customHandler)

    var msg: InstallSnapshotMsg
    msg.header = newMessageHeader(uint16(rmtInstallSnapshot), 1'u64,
                                  NodeID("raft_2"), NodeID("raft_1"), 1'u64)
    msg.leaderId = NodeID("raft_2")
    msg.lastIncludedIndex = 100'u64
    msg.lastIncludedTerm = 2'u64
    msg.offset = 10'u64
    msg.done = false
    msg.data = "data"

    let response = rt.handleInstallSnapshot(encodeInstallSnapshotMsg(msg))
    let respMsg = decodeInstallSnapshotResponseMsg(response)

    check respMsg.term == 7'u64
    check respMsg.offset == 10'u64 + 4'u64 + 100'u64

    rt.close()
    connMgr.close()

suite "Raft Transport TimeoutNow Handling - Full Coverage":
  test "handleTimeoutNow with no handler returns empty string":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    var msg: TimeoutNowMsg
    msg.header = newMessageHeader(uint16(rmtTimeoutNow), 1'u64,
                                  NodeID("raft_2"), NodeID("raft_1"), 1'u64)

    let payload = encodeTimeoutNowMsg(msg)
    let response = rt.handleTimeoutNow(payload)

    check response == ""

    rt.close()
    connMgr.close()

  test "handleTimeoutNow with custom handler":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    var handlerCalled = false
    proc customHandler(data: string): string {.gcsafe.} =
      handlerCalled = true
      result = "processed"

    rt.registerHandler(uint16(rmtTimeoutNow), customHandler)

    var msg: TimeoutNowMsg
    msg.header = newMessageHeader(uint16(rmtTimeoutNow), 1'u64,
                                  NodeID("raft_2"), NodeID("raft_1"), 1'u64)

    let response = rt.handleTimeoutNow(encodeTimeoutNowMsg(msg))
    check handlerCalled
    check response == "processed"

    rt.close()
    connMgr.close()

suite "Raft Transport ReadIndex Handling - Full Coverage":
  test "handleReadIndex with no handler returns default response":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    var msg: ReadIndexMsg
    msg.header = newMessageHeader(uint16(rmtReadIndex), 1'u64,
                                  NodeID("raft_2"), NodeID("raft_1"), 1'u64)
    msg.readRequestId = 123'u64

    let payload = encodeReadIndexMsg(msg)
    let response = rt.handleReadIndex(payload)

    check response.len > 0

    let respMsg = decodeReadIndexResponseMsg(response)
    check respMsg.readRequestId == 123'u64
    check respMsg.index == 0'u64

    rt.close()
    connMgr.close()

  test "handleReadIndex with custom handler":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    proc customHandler(data: string): string {.gcsafe.} =
      let msg = decodeReadIndexMsg(data)
      var resp: ReadIndexResponseMsg
      resp.header = newMessageHeader(uint16(rmtReadIndexResponse), msg.header.messageId,
                                      msg.header.targetNodeId,
                                          msg.header.sourceNodeId, 5'u64)
      resp.readRequestId = msg.readRequestId
      resp.index = 999'u64
      result = encodeReadIndexResponseMsg(resp)

    rt.registerHandler(uint16(rmtReadIndex), customHandler)

    var msg: ReadIndexMsg
    msg.header = newMessageHeader(uint16(rmtReadIndex), 1'u64,
                                  NodeID("raft_2"), NodeID("raft_1"), 1'u64)
    msg.readRequestId = 456'u64

    let response = rt.handleReadIndex(encodeReadIndexMsg(msg))
    let respMsg = decodeReadIndexResponseMsg(response)

    check respMsg.readRequestId == 456'u64
    check respMsg.index == 999'u64

    rt.close()
    connMgr.close()

suite "Raft Transport Setup Handlers":
  test "setupHandlers registers all default handlers":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    rt.setupHandlers()

    var rvMsg: RequestVoteMsg
    rvMsg.header = newMessageHeader(uint16(rmtRequestVote), 1'u64,
                                    NodeID("raft_2"), NodeID("raft_1"), 1'u64)
    rvMsg.candidateId = NodeID("raft_2")
    rvMsg.lastLogIndex = 0'u64
    rvMsg.lastLogTerm = 0'u64

    let rvResponse = rt.handleRequestVote(encodeRequestVoteMsg(rvMsg))
    check rvResponse.len > 0

    var aeMsg: AppendEntriesMsg
    aeMsg.header = newMessageHeader(uint16(rmtAppendEntries), 1'u64,
                                    NodeID("raft_2"), NodeID("raft_1"), 1'u64)
    aeMsg.leaderId = NodeID("raft_2")
    aeMsg.prevLogIndex = 0'u64
    aeMsg.prevLogTerm = 0'u64
    aeMsg.commitIndex = 0'u64
    aeMsg.numEntries = 0'u32
    aeMsg.entriesData = ""

    let aeResponse = rt.handleAppendEntries(encodeAppendEntriesMsg(aeMsg))
    check aeResponse.len > 0

    var isMsg: InstallSnapshotMsg
    isMsg.header = newMessageHeader(uint16(rmtInstallSnapshot), 1'u64,
                                    NodeID("raft_2"), NodeID("raft_1"), 1'u64)
    isMsg.leaderId = NodeID("raft_2")
    isMsg.lastIncludedIndex = 0'u64
    isMsg.lastIncludedTerm = 0'u64
    isMsg.offset = 0'u64
    isMsg.done = true
    isMsg.data = ""

    let isResponse = rt.handleInstallSnapshot(encodeInstallSnapshotMsg(isMsg))
    check isResponse.len > 0

    var tnMsg: TimeoutNowMsg
    tnMsg.header = newMessageHeader(uint16(rmtTimeoutNow), 1'u64,
                                    NodeID("raft_2"), NodeID("raft_1"), 1'u64)

    let tnResponse = rt.handleTimeoutNow(encodeTimeoutNowMsg(tnMsg))
    check tnResponse == ""

    var riMsg: ReadIndexMsg
    riMsg.header = newMessageHeader(uint16(rmtReadIndex), 1'u64,
                                    NodeID("raft_2"), NodeID("raft_1"), 1'u64)
    riMsg.readRequestId = 1'u64

    let riResponse = rt.handleReadIndex(encodeReadIndexMsg(riMsg))
    check riResponse.len > 0

    rt.close()
    connMgr.close()

suite "Raft Transport Broadcast Operations":
  test "broadcastRequestVote with no remote nodes returns 0":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    let count = rt.broadcastRequestVote(1'u64, 1'i32, 0'u64, 0'u64)
    check count == 0

    rt.close()
    connMgr.close()

  test "broadcastAppendEntries with no remote nodes returns 0":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    let count = rt.broadcastAppendEntries(1'u64, 1'i32, 0'u64, 0'u64, 0'u64, @[])
    check count == 0

    rt.close()
    connMgr.close()

suite "Raft Transport Message ID Generation":
  test "nextMessageId generates sequential IDs":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    check rt.connManager.raftTransport.nextMessageId() == 1'u64
    check rt.connManager.raftTransport.nextMessageId() == 2'u64
    check rt.connManager.raftTransport.nextMessageId() == 3'u64

    rt.close()
    connMgr.close()

suite "Raft Transport Thread Safety":
  test "Handler registration is thread-safe":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    proc handler1(data: string): string {.gcsafe.} = "h1"
    proc handler2(data: string): string {.gcsafe.} = "h2"

    rt.registerHandler(uint16(rmtRequestVote), handler1)
    rt.registerHandler(uint16(rmtAppendEntries), handler2)

    check rt.getHandler(uint16(rmtRequestVote)).isSome
    check rt.getHandler(uint16(rmtAppendEntries)).isSome

    rt.close()
    connMgr.close()
