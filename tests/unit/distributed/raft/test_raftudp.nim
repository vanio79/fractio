## Unit tests for RaftUDPTransport and Raft message codec.

import unittest
import std/[random, os, times, atomics, net, tables]
import fractio/distributed/raft/types
import fractio/distributed/raft/udp
import fractio/core/errors
import fractio/utils/logging

type
  ReceivedMsg* = object
    sender*: NodeId
    msg*: RaftMessage
    now*: uint64

  TestReceiver* = object
    buffer*: array[32, ReceivedMsg] # fixed capacity
    count*: Atomic[int]             # number of messages received

proc initTestReceiver*(): TestReceiver =
  result.count.store(0, moRelaxed)

proc record*(self: var TestReceiver, sender: NodeId, msg: RaftMessage,
    now: uint64) {.gcsafe.} =
  let idx = self.count.fetchAdd(1, moRelaxed)
  if idx < self.buffer.len:
    self.buffer[idx] = ReceivedMsg(sender: sender, msg: msg, now: now)
  # else drop (overflow)

proc mkHandler*(rcv: var TestReceiver): RaftMessageHandler =
  let rcvPtr = addr(rcv)
  result = cast[RaftMessageHandler](
    proc(sender: NodeId, msg: RaftMessage, now: uint64) {.gcsafe.} =
    rcvPtr[].record(sender, msg, now))

# Helper to get a free UDP port
proc getFreePort*(): uint16 =
  let sock = newSocket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)
  try:
    sock.bindAddr(Port(0), "0.0.0.0")
    let (_, p) = sock.getLocalAddr()
    result = uint16(p)
  finally:
    sock.close()

# Helper to create NodeAddress
proc makeAddr*(host: string, port: int): NodeAddress =
  NodeAddress(host: host, port: uint16(port))

suite "RaftUDPTransport codec":
  test "RequestVote roundtrip":
    let orig = RaftMessage(kind: rmRequestVote,
      requestVote: RequestVoteArgs(term: 1'u64, candidateId: NodeId(2'u64),
        lastLogIndex: 10'u64, lastLogTerm: 1'u64))
    let data = encodeRaftMessage(orig)
    let decoded = decodeRaftMessage(data)
    check decoded.kind == rmRequestVote
    check decoded.requestVote.term == 1'u64
    check decoded.requestVote.candidateId == NodeId(2'u64)
    check decoded.requestVote.lastLogIndex == 10'u64
    check decoded.requestVote.lastLogTerm == 1'u64

  test "RequestVoteReply roundtrip":
    let orig = RaftMessage(kind: rmRequestVoteReply,
      requestVoteReply: RequestVoteReply(term: 5'u64, voteGranted: true))
    let data = encodeRaftMessage(orig)
    let decoded = decodeRaftMessage(data)
    check decoded.kind == rmRequestVoteReply
    check decoded.requestVoteReply.term == 5'u64
    check decoded.requestVoteReply.voteGranted == true

  test "AppendEntries with empty entries roundtrip":
    let orig = RaftMessage(kind: rmAppendEntries,
      appendEntries: AppendEntriesArgs(term: 2, leaderId: NodeId(1),
        prevLogIndex: 0, prevLogTerm: 0, entries: @[], leaderCommit: 0))
    let data = encodeRaftMessage(orig)
    let decoded = decodeRaftMessage(data)
    check decoded.kind == rmAppendEntries
    check decoded.appendEntries.term == 2
    check decoded.appendEntries.entries.len == 0

  test "AppendEntries with single entry roundtrip":
    var entries: seq[RaftEntry] = @[]
    let cmd = RaftCommand(kind: rckNoop, data: @[byte(1), byte(2)])
    let entry = RaftEntry(term: 1, index: 1, command: cmd, checksum: 0)
    entries.add(entry)
    let orig = RaftMessage(kind: rmAppendEntries,
      appendEntries: AppendEntriesArgs(term: 3, leaderId: NodeId(2),
        prevLogIndex: 1, prevLogTerm: 1, entries: entries, leaderCommit: 1))
    let data = encodeRaftMessage(orig)
    let decoded = decodeRaftMessage(data)
    check decoded.kind == rmAppendEntries
    check decoded.appendEntries.term == 3
    check decoded.appendEntries.leaderId == NodeId(2)
    check decoded.appendEntries.entries.len == 1
    check decoded.appendEntries.entries[0].term == 1
    check decoded.appendEntries.entries[0].index == 1
    check decoded.appendEntries.entries[0].command.kind == rckNoop
    check decoded.appendEntries.entries[0].command.data == @[byte(1), byte(2)]

  test "AppendEntries with multiple entries roundtrip":
    var entries: seq[RaftEntry] = @[]
    for i in 1..3:
      let cmd = RaftCommand(kind: rckClientCommand, data: @[byte(i)])
      let entry = RaftEntry(term: 1, index: uint64(i), command: cmd, checksum: 0)
      entries.add(entry)
    let orig = RaftMessage(kind: rmAppendEntries,
      appendEntries: AppendEntriesArgs(term: 1, leaderId: NodeId(1),
        prevLogIndex: 0, prevLogTerm: 0, entries: entries, leaderCommit: 3))
    let data = encodeRaftMessage(orig)
    let decoded = decodeRaftMessage(data)
    check decoded.kind == rmAppendEntries
    check decoded.appendEntries.entries.len == 3
    for i in 0..2:
      check decoded.appendEntries.entries[i].index == uint64(i+1)
      check decoded.appendEntries.entries[i].command.data[0] == byte(i+1)

  test "AppendEntriesReply roundtrip":
    let orig = RaftMessage(kind: rmAppendEntriesReply,
      appendEntriesReply: AppendEntriesReply(term: 2, success: false,
        conflictTerm: 5, conflictIndex: 10))
    let data = encodeRaftMessage(orig)
    let decoded = decodeRaftMessage(data)
    check decoded.kind == rmAppendEntriesReply
    check decoded.appendEntriesReply.term == 2
    check decoded.appendEntriesReply.success == false
    check decoded.appendEntriesReply.conflictTerm == 5'u64
    check decoded.appendEntriesReply.conflictIndex == 10'u64

  test "InstallSnapshot roundtrip":
    let snapData = @[byte(255), byte(0), byte(128), byte(64)]
    let orig = RaftMessage(kind: rmInstallSnapshot,
      installSnapshot: InstallSnapshotArgs(term: 1'u64, leaderId: NodeId(1),
        lastIncludedIndex: 100'u64, lastIncludedTerm: 1'u64, data: snapData))
    let data = encodeRaftMessage(orig)
    let decoded = decodeRaftMessage(data)
    check decoded.kind == rmInstallSnapshot
    check decoded.installSnapshot.term == 1'u64
    check decoded.installSnapshot.leaderId == NodeId(1)
    check decoded.installSnapshot.lastIncludedIndex == 100'u64
    check decoded.installSnapshot.lastIncludedTerm == 1'u64
    check decoded.installSnapshot.data == snapData

  test "decode raises on empty data":
    expect ValueError:
      discard decodeRaftMessage(@[])
    expect ValueError:
      discard decodeRaftMessage(@[0'u8]) # only kind

  test "decode raises on truncated uint64":
    # RequestVote needs 1 + 4*8 = 33 bytes; give 32
    var data = newSeq[byte](32)
    data[0] = byte(rmRequestVote.ord)
    expect ValueError:
      discard decodeRaftMessage(data)

  test "decode raises on trailing bytes":
    # Valid RequestVote (33 bytes) plus extra
    var data = newSeq[byte](33 + 5)
    data[0] = byte(rmRequestVote.ord)
    # fill the required fields with zeros for simplicity (valid enough)
    for i in 1..<33:
      data[i] = 0'u8
    data[33] = 1'u8 # extra
    expect ValueError:
      discard decodeRaftMessage(data)

suite "RaftUDPTransport integration":
  # Simple node counter for unique IDs in test
  var nodeCounter {.global.} = 0

  proc makeTestNodeId(): NodeId =
    inc nodeCounter
    result = NodeId(nodeCounter)

  setup:
    # Ensure logger available
    discard

  teardown:
    # nothing
    discard

  test "transport start and close":
    let port = getFreePort()
    let localAddr = makeAddr("127.0.0.1", int(port))
    var peers = initTable[NodeId, NodeAddress]()
    var rcv = initTestReceiver()
    let t = newRaftUDPTransport(makeTestNodeId(), localAddr, peers,
       mkHandler(rcv),
      newLogger("Test"))
    check not load(t.serverRunning, moRelaxed)
    t.start()
    check load(t.serverRunning, moRelaxed)
    t.close()
    check not load(t.serverRunning, moRelaxed)

  test "send and receive between two transports":
    let host = "127.0.0.1"
    let port1 = getFreePort()
    let port2 = getFreePort()
    let addr1 = makeAddr(host, int(port1))
    let addr2 = makeAddr(host, int(port2))
    # Receiver for each side
    var rcv1 = initTestReceiver()
    var rcv2 = initTestReceiver()
    # Build peer mappings
    let nid1 = NodeId(1)
    let nid2 = NodeId(2)
    var peersFor1 = initTable[NodeId, NodeAddress]()
    peersFor1[nid2] = addr2
    var peersFor2 = initTable[NodeId, NodeAddress]()
    peersFor2[nid1] = addr1
    let t1 = newRaftUDPTransport(nid1, addr1, peersFor1,
       mkHandler(rcv1),
      newLogger("T1"))
    let t2 = newRaftUDPTransport(nid2, addr2, peersFor2,
       mkHandler(rcv2),
      newLogger("T2"))
    t1.start()
    t2.start()
    # Send a RequestVote from T1 to T2
    let rv = RaftMessage(kind: rmRequestVote,
      requestVote: RequestVoteArgs(term: 1'u64, candidateId: nid1,
        lastLogIndex: 0, lastLogTerm: 0))
    t1.send(nid2, rv)
    # Wait for delivery (poll)
    var waited = 0
    while rcv2.count.load(moRelaxed) == 0 and waited < 50:
      os.sleep(1) # 1 ms
      inc waited
    check rcv2.count.load(moRelaxed) == 1
    let received = rcv2.buffer[0]
    check received.sender == nid1
    check received.msg.kind == rmRequestVote
    check received.msg.requestVote.term == 1'u64
    # Exchange in opposite direction
    let rvReply = RaftMessage(kind: rmRequestVoteReply,
      requestVoteReply: RequestVoteReply(term: 1'u64, voteGranted: true))
    t2.send(nid1, rvReply)
    waited = 0
    while rcv1.count.load(moRelaxed) == 0 and waited < 50:
      os.sleep(1)
      inc waited
    check rcv1.count.load(moRelaxed) == 1
    let receivedReply = rcv1.buffer[0]
    check receivedReply.msg.kind == rmRequestVoteReply
    check receivedReply.msg.requestVoteReply.term == 1'u64
    check receivedReply.msg.requestVoteReply.voteGranted == true
    t1.close()
    t2.close()

  test "send to unknown node raises FractioError":
    let port = getFreePort()
    let localAddr = makeAddr("127.0.0.1", int(port))
    var peers = initTable[NodeId, NodeAddress]()
    var rcv = initTestReceiver()
    let t = newRaftUDPTransport(makeTestNodeId(), localAddr, peers,
       mkHandler(rcv),
      newLogger("Test"))
    t.start()
    let msg = RaftMessage(kind: rmRequestVoteReply,
      requestVoteReply: RequestVoteReply(term: 1, voteGranted: true))
    expect FractioError:
      t.send(NodeId(999), msg)
    t.close()

  test "send after close raises error":
    let port = getFreePort()
    let localAddr = makeAddr("127.0.0.1", int(port))
    var peers = initTable[NodeId, NodeAddress]()
    var rcv = initTestReceiver()
    let t = newRaftUDPTransport(makeTestNodeId(), localAddr, peers,
       mkHandler(rcv),
      newLogger("Test"))
    t.start()
    t.close()
    let msg = RaftMessage(kind: rmRequestVote, requestVote: RequestVoteArgs(
        term: 1, candidateId: NodeId(1), lastLogIndex: 0, lastLogTerm: 0))
    expect FractioError:
      t.send(NodeId(2), msg)

  test "large AppendEntries message roundtrip through transport":
    # Create a large number of entries to approach packet size limit
    let host = "127.0.0.1"
    let port1 = getFreePort()
    let port2 = getFreePort()
    let addr1 = makeAddr(host, int(port1))
    let addr2 = makeAddr(host, int(port2))
    var rcv1 = initTestReceiver()
    var rcv2 = initTestReceiver()
    let nid1 = NodeId(1)
    let nid2 = NodeId(2)
    var peers1 = initTable[NodeId, NodeAddress]()
    peers1[nid2] = addr2
    var peers2 = initTable[NodeId, NodeAddress]()
    peers2[nid1] = addr1
    let t1 = newRaftUDPTransport(nid1, addr1, peers1,
       mkHandler(rcv1),
      newLogger("T1"))
    let t2 = newRaftUDPTransport(nid2, addr2, peers2,
       mkHandler(rcv2),
      newLogger("T2"))
    t1.start()
    t2.start()
    # Build AppendEntries with many entries having large data
    var entries: seq[RaftEntry] = @[]
    for i in 1..100:
      let dataSize = 500 # bytes
      var data = newSeq[byte](dataSize)
      for j in 0..<dataSize:
        data[j] = byte((i+j) mod 256)
      let cmd = RaftCommand(kind: rckClientCommand, data: data)
      let entry = RaftEntry(term: 1, index: uint64(i), command: cmd, checksum: 0)
      entries.add(entry)
    let ae = RaftMessage(kind: rmAppendEntries,
      appendEntries: AppendEntriesArgs(term: 1, leaderId: nid1,
        prevLogIndex: 0, prevLogTerm: 0, entries: entries, leaderCommit: uint64(entries.len)))
    let data = encodeRaftMessage(ae)
    check data.len < MAX_UDP_PACKET_SIZE # should fit
    t1.send(nid2, ae)
    var waited = 0
    while rcv2.count.load(moRelaxed) == 0 and waited < 100:
      os.sleep(2)
      inc waited
    check rcv2.count.load(moRelaxed) == 1
    let receivedAe = rcv2.buffer[0].msg
    check receivedAe.kind == rmAppendEntries
    check receivedAe.appendEntries.entries.len == entries.len
    # Spot-check last entry data size
    let lastEntry = receivedAe.appendEntries.entries[^1]
    check lastEntry.command.data.len == 500
    t1.close()
    t2.close()
