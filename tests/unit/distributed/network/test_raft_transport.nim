# Unit tests for raft_transport.nim

import unittest
import tables
import options
import fractio/distributed/network/types
import fractio/distributed/network/serialization
import fractio/distributed/network/tcp_transport
import fractio/distributed/network/connection_manager
import fractio/distributed/network/raft_transport
import fractio/distributed/network/config
import fractio/distributed/raft/types as raft_types
import fractio/core/types

suite "Raft Transport Conversion Helpers":
  test "toNodeID converts int32 server ID to NodeID":
    let nodeId = toNodeID(1'i32)
    check string(nodeId) == "raft_1"

    let nodeId2 = toNodeID(42'i32)
    check string(nodeId2) == "raft_42"

  test "toServerId converts NodeID to int32 server ID":
    let serverId = toServerId(NodeID("raft_1"))
    check serverId == 1'i32

    let serverId2 = toServerId(NodeID("raft_42"))
    check serverId2 == 42'i32

  test "toServerId returns -1 for invalid NodeID":
    let serverId = toServerId(NodeID("invalid"))
    check serverId == -1'i32

    let serverId2 = toServerId(NodeID("raft_"))
    check serverId2 == -1'i32

  test "toNodeID and toServerId are inverses":
    let original = 5'i32
    let nodeId = toNodeID(original)
    let converted = toServerId(nodeId)
    check converted == original

suite "Raft Transport Log Entry Encoding":
  test "encodeLogEntries empty sequence":
    let entries: seq[raft_types.LogEntry] = @[]
    let encoded = encodeLogEntries(entries)
    check encoded.len > 0

    var r = newBinaryReader(encoded)
    check r.readUint32BE() == 0'u32

  test "encodeLogEntries single entry":
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

  test "encodeLogEntries multiple entries":
    var entries: seq[raft_types.LogEntry] = @[]
    for i in 1..3:
      var entry: raft_types.LogEntry
      entry.term = int64(i)
      entry.entryType = raft_types.LogEntryType.LET_NORMAL
      entry.data = "entry" & $i
      entries.add(entry)

    let encoded = encodeLogEntries(entries)
    check encoded.len > 0

    var r = newBinaryReader(encoded)
    check r.readUint32BE() == 3'u32

    for i in 1..3:
      check r.readUint64BE() == uint64(i)
      discard r.readUint8() # entryType
      check r.readString() == "entry" & $i

suite "Raft Transport Creation":
  test "Create Raft transport":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    check rt != nil
    check rt.serverId == 1'i32
    check string(rt.nodeId) == "raft_1"

    rt.close()
    connMgr.close()

  test "Raft transport has empty handlers initially":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    let handler = rt.getHandler(uint16(rmtRequestVote))
    check handler.isNone

    rt.close()
    connMgr.close()

suite "Raft Transport Handler Registration":
  test "Register and retrieve handler":
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

  test "Multiple handlers can be registered":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    proc handler1(data: string): string {.gcsafe.} = result = "h1"
    proc handler2(data: string): string {.gcsafe.} = result = "h2"

    rt.registerHandler(uint16(rmtRequestVote), handler1)
    rt.registerHandler(uint16(rmtAppendEntries), handler2)

    check rt.getHandler(uint16(rmtRequestVote)).isSome
    check rt.getHandler(uint16(rmtAppendEntries)).isSome

    rt.close()
    connMgr.close()

suite "Raft Transport Message Handling":
  test "handleRequestVote with no handler returns default response":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    # Create a RequestVote message
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

    rt.close()
    connMgr.close()

  test "handleAppendEntries with no handler returns default response":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    # Create an AppendEntries message
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

    rt.close()
    connMgr.close()

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
    check respMsg.offset == uint64("snapshot_data".len)

    rt.close()
    connMgr.close()

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

suite "Raft Transport Custom Handlers":
  test "Custom handler is called for RequestVote":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    var handlerCalled = false
    proc customHandler(data: string): string {.gcsafe.} =
      handlerCalled = true
      result = ""

    rt.registerHandler(uint16(rmtRequestVote), customHandler)

    var msg: RequestVoteMsg
    msg.header = newMessageHeader(uint16(rmtRequestVote), 1'u64,
                                   NodeID("raft_2"), NodeID("raft_1"), 1'u64)
    msg.candidateId = NodeID("raft_2")
    msg.lastLogIndex = 0'u64
    msg.lastLogTerm = 0'u64

    discard rt.handleRequestVote(encodeRequestVoteMsg(msg))

    check handlerCalled

    rt.close()
    connMgr.close()

  test "Custom handler can modify response":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    proc customHandler(data: string): string {.gcsafe.} =
      let msg = decodeRequestVoteMsg(data)
      var resp: RequestVoteResponseMsg
      resp.header = newMessageHeader(uint16(rmtRequestVoteResponse), msg.header.messageId,
                                      msg.header.targetNodeId,
                                          msg.header.sourceNodeId, 1'u64)
      resp.voteGranted = true
      resp.term = 5'u64
      result = encodeRequestVoteResponseMsg(resp)

    rt.registerHandler(uint16(rmtRequestVote), customHandler)

    var msg: RequestVoteMsg
    msg.header = newMessageHeader(uint16(rmtRequestVote), 1'u64,
                                   NodeID("raft_2"), NodeID("raft_1"), 0'u64)
    msg.candidateId = NodeID("raft_2")
    msg.lastLogIndex = 0'u64
    msg.lastLogTerm = 0'u64

    let response = rt.handleRequestVote(encodeRequestVoteMsg(msg))
    let respMsg = decodeRequestVoteResponseMsg(response)

    check respMsg.voteGranted == true
    check respMsg.term == 5'u64

    rt.close()
    connMgr.close()

suite "Raft Transport Setup Handlers":
  test "setupHandlers registers handlers with connection manager":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    rt.setupHandlers()

    # The handlers are registered with the connection manager, not the RaftTransport's
    # internal handlers. The internal handlers are for custom user handlers.
    # Test that message handling works through the default handlers.
    var msg: RequestVoteMsg
    msg.header = newMessageHeader(uint16(rmtRequestVote), 1'u64,
                                   NodeID("raft_2"), NodeID("raft_1"), 1'u64)
    msg.candidateId = NodeID("raft_2")
    msg.lastLogIndex = 0'u64
    msg.lastLogTerm = 0'u64

    let response = rt.handleRequestVote(encodeRequestVoteMsg(msg))
    check response.len > 0

    rt.close()
    connMgr.close()

suite "Raft Transport Multiple Close":
  test "Multiple close calls are safe":
    let config = newNetworkConfig(NodeID("raft_1"), 9000)
    let connMgr = newConnectionManager(config)
    let rt = newRaftTransport(connMgr, 1'i32)

    rt.close()
    rt.close() # Should not crash

    connMgr.close()
