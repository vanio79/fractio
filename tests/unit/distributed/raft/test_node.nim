import unittest
import times
import std/random
import std/tables
import os
import locks
import fractio/distributed/raft/types
import fractio/distributed/raft/node
import fractio/distributed/raft/log
import fractio/core/errors
import fractio/utils/logging

type
  MockTransport* = ref object of RaftTransport
    node*: RaftNode
    sentMsgs*: seq[tuple[dest: NodeId, msg: RaftMessage]]
    receivedMsgs*: seq[RaftMessage]

method send*(self: MockTransport, dest: NodeId, msg: RaftMessage) =
  self.sentMsgs.add((dest, msg))

proc newMockTransport*(node: RaftNode): MockTransport =
  result = MockTransport(node: node, sentMsgs: @[], receivedMsgs: @[])

proc deliver*(t: MockTransport, fromNodeId: NodeId, msg: RaftMessage,
    now: uint64) =
  t.receivedMsgs.add(msg)
  t.node.receiveMessage(fromNodeId, msg, now)

proc newNode*(nodeId: NodeId, peers: seq[NodeId], transport: RaftTransport,
    now: int64, onApply: proc(entry: RaftEntry) {.raises: [].}): RaftNode =
  if not existsDir("tmp"):
    createDir("tmp")
  let logDir = "tmp/raft_test_" & $nodeId
  if not existsDir(logDir):
    createDir(logDir)
  let logger = Logger(name: "RaftTest_" & $nodeId, minLevel: llWarn,
      handlers: @[])
  var log = newRaftLog(logDir, logger)
  log.setCurrentTerm(0)
  log.setVotedFor(0)
  let config = RaftConfig(
    electionTimeoutMin: 150_000_000,
    electionTimeoutMax: 150_000_000,
    heartbeatInterval: 50_000_000,
    clusterSize: peers.len + 1,
    peerIds: peers
  )
  result = newRaftNode(nodeId, log, transport, logger, config, onApply, now.uint64)

var appliedEntries = newSeq[(NodeId, RaftEntry)]()

suite "RaftNode state machine":

  var
    n1, n2, n3: RaftNode
    t1, t2, t3: MockTransport
    startTime: int64
    currentTime: uint64

  setup:
    appliedEntries = @[]
    startTime = 1_000_000_000
    currentTime = startTime.uint64

    t1 = newMockTransport(nil)
    t2 = newMockTransport(nil)
    t3 = newMockTransport(nil)

    n1 = newNode(NodeId(1'u64), @[NodeId(2'u64), NodeId(3'u64)], t1, startTime,
        proc(entry: RaftEntry) {.raises: [].} =
      appliedEntries.add((NodeId(1'u64), entry)))
    n2 = newNode(NodeId(2'u64), @[NodeId(1'u64), NodeId(3'u64)], t2, startTime,
        proc(entry: RaftEntry) {.raises: [].} =
      appliedEntries.add((NodeId(2'u64), entry)))
    n3 = newNode(NodeId(3'u64), @[NodeId(1'u64), NodeId(2'u64)], t3, startTime,
        proc(entry: RaftEntry) {.raises: [].} =
      appliedEntries.add((NodeId(3'u64), entry)))

    t1.node = n1
    t2.node = n2
    t3.node = n3

  teardown:
    if not n1.isNil:
      deinitLock(n1.lock)
      n1 = nil
    if not n2.isNil:
      deinitLock(n2.lock)
      n2 = nil
    if not n3.isNil:
      deinitLock(n3.lock)
      n3 = nil
    for dir in ["tmp/raft_test_1", "tmp/raft_test_2", "tmp/raft_test_3"]:
      if existsDir(dir):
        try:
          removeFile(dir / "raft.log")
          removeFile(dir / "raft.meta")
        except:
          discard
        removeDir(dir)

  test "Single node election after timeout":
    check n1.state == rsFollower
    check n1.term == 0

    let timeoutTick = n1.electionDeadline + 1
    n1.tick(timeoutTick)
    currentTime = timeoutTick

    check n1.state == rsCandidate
    check n1.term == 1
    check n1.votedFor == NodeId(1'u64)

    check t1.sentMsgs.len >= 2
    for (dest, msg) in t1.sentMsgs:
      check msg.kind == rmRequestVote

    let (_, reqVote) = t1.sentMsgs[0]
    let resp1 = RaftMessage(kind: rmRequestVoteReply,
        requestVoteReply: RequestVoteReply(term: 1, voteGranted: true))
    t1.deliver(NodeId(2'u64), resp1, currentTime)
    t1.deliver(NodeId(3'u64), resp1, currentTime)

    check n1.state == rsLeader
    check n1.term == 1
    check n1.commitIndex >= 0

  test "Leader sends heartbeats":
    let timeoutTick = n1.electionDeadline + 1
    n1.tick(timeoutTick)
    currentTime = timeoutTick

    for i in 0..<t1.sentMsgs.len:
      let resp = RaftMessage(kind: rmRequestVoteReply,
          requestVoteReply: RequestVoteReply(term: 1, voteGranted: true))
      t1.deliver(NodeId(2'u64), resp, currentTime)
      t1.deliver(NodeId(3'u64), resp, currentTime)

    check n1.state == rsLeader
    t1.sentMsgs.setLen(0)

    let hbTime = n1.heartbeatDeadline
    n1.tick(hbTime)
    currentTime = hbTime

    check t1.sentMsgs.len == 2
    for (dest, msg) in t1.sentMsgs:
      check msg.kind == rmAppendEntries
      check msg.appendEntries.entries.len == 0

  test "Log replication and commit":
    let timeoutTick = n1.electionDeadline + 1
    n1.tick(timeoutTick)
    currentTime = timeoutTick

    for i in 0..<t1.sentMsgs.len:
      let resp = RaftMessage(kind: rmRequestVoteReply,
          requestVoteReply: RequestVoteReply(term: 1, voteGranted: true))
      t1.deliver(NodeId(2'u64), resp, currentTime)
      t1.deliver(NodeId(3'u64), resp, currentTime)

    check n1.state == rsLeader
    t1.sentMsgs.setLen(0)

    let expectedCmd = RaftCommand(kind: rckNoop)
    let idx = n1.submitCommand(expectedCmd)
    check idx == 1'u64

    check n1.log.getLastLogIndex() == 1
    check n1.log.getEntry(1).command.kind == expectedCmd.kind

    let hbTime = n1.heartbeatDeadline
    n1.tick(hbTime)
    currentTime = hbTime

    check t1.sentMsgs.len == 2
    for (dest, msg) in t1.sentMsgs:
      check msg.kind == rmAppendEntries
      check msg.appendEntries.entries.len == 1
      check msg.appendEntries.entries[0].command.kind == expectedCmd.kind
      check msg.appendEntries.prevLogIndex == 0
      check msg.appendEntries.prevLogTerm == 0

    for (dest, msg) in t1.sentMsgs:
      let resp = RaftMessage(kind: rmAppendEntriesReply,
          appendEntriesReply: AppendEntriesReply(term: 1, success: true,
          conflictTerm: 0, conflictIndex: 0))
      if dest == NodeId(2'u64):
        let appended = n2.log.append(msg.appendEntries.entries[0])
        check appended != 0'u64
        t1.deliver(NodeId(2'u64), resp, currentTime)
      elif dest == NodeId(3'u64):
        let appended = n3.log.append(msg.appendEntries.entries[0])
        check appended != 0'u64
        t1.deliver(NodeId(3'u64), resp, currentTime)

    check n1.matchIndex[NodeId(2'u64)] == 1
    check n1.matchIndex[NodeId(3'u64)] == 1
    check n1.commitIndex == 1

    check appliedEntries.len >= 1
    let (applier, appliedEntry) = appliedEntries[^1]
    check applier == NodeId(1'u64)
    check appliedEntry.command.kind == expectedCmd.kind

  test "Non-leader command submission is rejected":
    check n2.state == rsFollower
    expect FractioError:
      discard n2.submitCommand(RaftCommand(kind: rckClientCommand))
    check t2.sentMsgs.len == 0

  test "Leader step-down when higher term seen":
    # Establish n1 as leader
    let timeoutTick = n1.electionDeadline + 1
    n1.tick(timeoutTick)
    currentTime = timeoutTick

    for i in 0..<t1.sentMsgs.len:
      let resp = RaftMessage(kind: rmRequestVoteReply,
          requestVoteReply: RequestVoteReply(term: 1, voteGranted: true))
      t1.deliver(NodeId(2'u64), resp, currentTime)
      t1.deliver(NodeId(3'u64), resp, currentTime)

    check n1.state == rsLeader
    check n1.term == 1
    t1.sentMsgs.setLen(0)

    # Receive AppendEntries from a higher term leader (n2)
    let hb = RaftMessage(kind: rmAppendEntries,
        appendEntries: AppendEntriesArgs(term: 2, leaderId: NodeId(2'u64),
            prevLogIndex: 0, prevLogTerm: 0, entries: @[], leaderCommit: 0))
    n1.receiveMessage(NodeId(2'u64), hb, currentTime)

    check n1.state == rsFollower
    check n1.term == 2
    check n1.leaderId == NodeId(2'u64)

  test "Term never decreases":
    check n1.state == rsFollower
    n1.term = 1
    let staleHb = RaftMessage(kind: rmAppendEntries,
        appendEntries: AppendEntriesArgs(term: 0, leaderId: NodeId(2'u64),
            prevLogIndex: 0, prevLogTerm: 0, entries: @[], leaderCommit: 0))
    n1.receiveMessage(NodeId(2'u64), staleHb, currentTime)
    check n1.term == 1
    check n1.state == rsFollower

  test "Re-election when leader fails":
    # Establish n1 as leader and replicate an entry to followers
    let timeoutTick = n1.electionDeadline + 1
    n1.tick(timeoutTick)
    currentTime = timeoutTick

    for i in 0..<t1.sentMsgs.len:
      let resp = RaftMessage(kind: rmRequestVoteReply,
          requestVoteReply: RequestVoteReply(term: 1, voteGranted: true))
      t1.deliver(NodeId(2'u64), resp, currentTime)
      t1.deliver(NodeId(3'u64), resp, currentTime)

    check n1.state == rsLeader
    check n1.term == 1
    t1.sentMsgs.setLen(0)

    # Submit a command to have a log entry
    discard n1.submitCommand(RaftCommand(kind: rckNoop))

    # Send heartbeats to replicate the entry to followers
    let hbTime = n1.heartbeatDeadline
    n1.tick(hbTime)
    currentTime = hbTime

    # Process AppendEntries sent to followers
    for (dest, msg) in t1.sentMsgs:
      if dest == NodeId(2'u64):
        # Append entry to n2's log
        discard n2.log.append(msg.appendEntries.entries[0])
        # Send success reply to leader
        let reply = RaftMessage(kind: rmAppendEntriesReply,
            appendEntriesReply: AppendEntriesReply(term: 1, success: true,
            conflictTerm: 0, conflictIndex: 0))
        t1.deliver(NodeId(2'u64), reply, currentTime)
      elif dest == NodeId(3'u64):
        discard n3.log.append(msg.appendEntries.entries[0])
        let reply = RaftMessage(kind: rmAppendEntriesReply,
            appendEntriesReply: AppendEntriesReply(term: 1, success: true,
            conflictTerm: 0, conflictIndex: 0))
        t1.deliver(NodeId(3'u64), reply, currentTime)

    # Verify replication
    check n1.matchIndex[NodeId(2'u64)] == 1
    check n1.matchIndex[NodeId(3'u64)] == 1
    check n1.commitIndex == 1

    # Clear transports to isolate election messages
    t1.sentMsgs.setLen(0)
    t2.sentMsgs.setLen(0)
    t3.sentMsgs.setLen(0)

    # Ensure followers have seen term 1
    n2.term = 1
    n3.term = 1

    # Simulate leader failure: advance time so that n2's election timeout fires,
    # while n3's timeout is set far in the future to avoid simultaneous candidacy.
    n3.electionDeadline = n2.electionDeadline + 500_000_000'u64
    let electionTick = n2.electionDeadline + 1
    n2.tick(electionTick)
    currentTime = electionTick

    check n2.state == rsCandidate
    check n2.term == 2
    check n2.votedFor == NodeId(2'u64)

    # n2 has sent RequestVote RPCs to n1 and n3 (recorded in t2.sentMsgs)
    # Deliver those requests to n1 and n3
    for (dest, msg) in t2.sentMsgs:
      if dest == NodeId(1'u64):
        t1.deliver(NodeId(2'u64), msg, currentTime)
      elif dest == NodeId(3'u64):
        t3.deliver(NodeId(2'u64), msg, currentTime)

    # n1 and n3 will respond with RequestVoteReply. Their responses are sent via their transports.
    # Collect and deliver those replies to n2.
    let votesToN2FromN1 = t1.sentMsgs
    let votesToN2FromN3 = t3.sentMsgs
    for (dest, msg) in votesToN2FromN1:
      if dest == NodeId(2'u64):
        t2.deliver(NodeId(1'u64), msg, currentTime)
    for (dest, msg) in votesToN2FromN3:
      if dest == NodeId(2'u64):
        t2.deliver(NodeId(3'u64), msg, currentTime)

    # n2 should become leader after receiving votes from majority (at least 2 including self)
    check n2.state == rsLeader
    check n2.term == 2

    # n2 as leader should have sent initial heartbeats (via becomeLeader)
    var hbCount = 0
    for (dest, msg) in t2.sentMsgs:
      if msg.kind == rmAppendEntries:
        inc hbCount
    check hbCount == 2

    # Deliver those heartbeats to followers
    for (dest, msg) in t2.sentMsgs:
      if msg.kind == rmAppendEntries:
        if dest == NodeId(1'u64):
          t1.deliver(NodeId(2'u64), msg, currentTime)
        elif dest == NodeId(3'u64):
          t3.deliver(NodeId(3'u64), msg, currentTime)

    # n1 and n3 should now be followers of n2
    check n1.state == rsFollower
    check n1.term == 2
    check n1.leaderId == NodeId(2'u64)
    check n3.state == rsFollower
    check n3.term == 2
    check n3.leaderId == NodeId(2'u64)

  test "InstallSnapshot":
    # Establish n1 as leader
    let timeoutTick = n1.electionDeadline + 1
    n1.tick(timeoutTick)
    currentTime = timeoutTick

    for i in 0..<t1.sentMsgs.len:
      let resp = RaftMessage(kind: rmRequestVoteReply,
          requestVoteReply: RequestVoteReply(term: 1, voteGranted: true))
      t1.deliver(NodeId(2'u64), resp, currentTime)
      t1.deliver(NodeId(3'u64), resp, currentTime)

    check n1.state == rsLeader
    check n1.term == 1
    t1.sentMsgs.setLen(0)

    # Ensure n2 has term 1 so it processes the snapshot without stepping down
    n2.term = 1

      # Create a log entry to snapshot
    discard n1.submitCommand(RaftCommand(kind: rckNoop))
    check n1.log.getLastLogIndex() == 1

    # Create a snapshot on the leader at index 1, term 1
    let snapData = @[byte(1), byte(2), byte(3)]
    let leaderSnap = n1.log.createSnapshot(1'u64, 1'u64, snapData)
    check n1.log.getSnapshotIndex() == 1'u64
    check n1.log.getSnapshotTerm() == 1'u64

    # Leader sends InstallSnapshot to n2
    let installMsg = RaftMessage(kind: rmInstallSnapshot,
        installSnapshot: InstallSnapshotArgs(
          term: n1.term,
          leaderId: NodeId(1'u64),
          lastIncludedIndex: 1'u64,
          lastIncludedTerm: 1'u64,
          data: snapData))

    # Deliver to follower n2
    t2.deliver(NodeId(1'u64), installMsg, currentTime)

    # Verify n2 installed the snapshot
    check n2.log.getSnapshotIndex() == 1'u64
    check n2.log.getSnapshotTerm() == 1'u64

    # Entries at or before index 1 should not be accessible
    expect FractioError:
      discard n2.log.getEntry(1)

    # After installation, n2's state should be follower and leaderId set
    check n2.state == rsFollower
    check n2.leaderId == NodeId(1'u64)
