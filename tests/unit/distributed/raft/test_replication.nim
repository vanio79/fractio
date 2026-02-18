import unittest
import std/tables
import os
import locks
import fractio/distributed/raft/types
import fractio/distributed/raft/node
import fractio/distributed/raft/log
import fractio/core/errors
import fractio/utils/logging

type
  AppliedCollector = object
    data: array[1024, uint64]
    count: int

proc initAppliedCollector(c: var AppliedCollector) =
  ## Resets the collector to empty.
  c.count = 0

proc lenApplied(c: AppliedCollector): int =
  ## Returns number of collected entries.
  c.count

proc containsApplied(c: AppliedCollector, index: uint64): bool =
  ## Checks if an entry index was collected.
  for i in 0..<c.count:
    if c.data[i] == index:
      return true
  false

proc clearApplied(c: var AppliedCollector) =
  ## Clears all collected entries.
  c.count = 0

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
    now: int64, onApply: proc(entry: RaftEntry) {.closure, gcsafe, raises: [
        CatchableError].}): RaftNode =
  if not dirExists("tmp"):
    createDir("tmp")
  let logDir = "tmp/raft_replication_test_" & $nodeId
  if not dirExists(logDir):
    createDir(logDir)
  let logger = Logger(name: "RaftReplTest_" & $nodeId, minLevel: llWarn,
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

suite "Log Replication":

  var
    n1, n2: RaftNode
    t1, t2: MockTransport
    startTime: int64
    currentTime: uint64

  setup:
    startTime = 1_000_000_000
    currentTime = startTime.uint64

    t1 = newMockTransport(nil)
    t2 = newMockTransport(nil)

  teardown:
    if not n1.isNil:
      deinitLock(n1.lock)
      n1 = nil
    if not n2.isNil:
      deinitLock(n2.lock)
      n2 = nil
    for dir in ["tmp/raft_replication_test_1", "tmp/raft_replication_test_2"]:
      if existsDir(dir):
        try:
          removeFile(dir / "raft.log")
          removeFile(dir / "raft.meta")
        except:
          discard
        removeDir(dir)

  proc electLeader(leader: RaftNode, followerTransport: MockTransport, now: uint64) =
    leader.tick(now)
    # Deliver RequestVote from leader to follower
    for (dest, msg) in t1.sentMsgs:
      if dest == NodeId(2'u64):
        t2.deliver(NodeId(1'u64), msg, now)
    # Deliver follower's RequestVoteReply to leader
    for (dest, msg) in t2.sentMsgs:
      if dest == NodeId(1'u64):
        t1.deliver(NodeId(2'u64), msg, now)
    check leader.state == rsLeader
    t1.sentMsgs.setLen(0)
    t2.sentMsgs.setLen(0)

  test "Basic append and commit":
    var applied: AppliedCollector
    initAppliedCollector(applied)
    let onApply = proc(entry: RaftEntry) {.closure, gcsafe, raises: [
        CatchableError].} =
      if applied.count < applied.data.len:
        applied.data[applied.count] = entry.index
        inc applied.count
    n1 = newNode(NodeId(1'u64), @[NodeId(2'u64)], t1, startTime, onApply)
    n2 = newNode(NodeId(2'u64), @[NodeId(1'u64)], t2, startTime,
        proc(entry: RaftEntry) {.closure, gcsafe, raises: [CatchableError].} =
      if applied.count < applied.data.len:
        applied.data[applied.count] = entry.index
        inc applied.count)
    t1.node = n1
    t2.node = n2

    let now = startTime.uint64 + 1_000_000_000'u64
    electLeader(n1, t2, now)

    # Submit command to leader
    let cmd = RaftCommand(kind: rckNoop)
    let idx = n1.submitCommand(cmd)
    check idx == 1'u64

    # Deliver AppendEntries from n1 to n2
    for (dest, msg) in t1.sentMsgs:
      if dest == NodeId(2'u64):
        t2.deliver(NodeId(1'u64), msg, now)
    t1.sentMsgs.setLen(0)

    # n2 should have appended the entry
    check n2.log.getLastLogIndex() == 1'u64
    check n2.log.getEntry(1).command.kind == cmd.kind

    # n2 sends AppendEntriesReply; deliver to n1
    for (dest, msg) in t2.sentMsgs:
      if dest == NodeId(1'u64):
        t1.deliver(NodeId(2'u64), msg, now)
    t2.sentMsgs.setLen(0)

    # n1 should now commit (majority) and apply
    check n1.commitIndex == 1'u64
    check n1.lastApplied == 1'u64

    # Propagate commit to follower via heartbeat
    let hb = RaftMessage(kind: rmAppendEntries,
        appendEntries: AppendEntriesArgs(
          term: n1.term,
          leaderId: n1.nodeId,
          prevLogIndex: n1.log.getLastLogIndex(),
          prevLogTerm: n1.log.getLastLogTerm(),
          entries: @[],
          leaderCommit: n1.commitIndex))
    t2.deliver(NodeId(1'u64), hb, now)
    check n2.commitIndex == 1'u64
    check n2.lastApplied == 1'u64

  test "Conflict resolution: follower truncates and appends":
    var applied: AppliedCollector
    initAppliedCollector(applied)
    let onApply = proc(entry: RaftEntry) {.closure, gcsafe, raises: [
        CatchableError].} =
      if applied.count < applied.data.len:
        applied.data[applied.count] = entry.index
        inc applied.count
    n1 = newNode(NodeId(1'u64), @[NodeId(2'u64)], t1, startTime, onApply)
    n2 = newNode(NodeId(2'u64), @[NodeId(1'u64)], t2, startTime,
        proc(entry: RaftEntry) {.closure, gcsafe, raises: [CatchableError].} =
      if applied.count < applied.data.len:
        applied.data[applied.count] = entry.index
        inc applied.count)
    t1.node = n1
    t2.node = n2

    let now = startTime.uint64 + 1_000_000_000'u64
    electLeader(n1, t2, now)

    # Manually create conflicting entry on n2 (term 2, index 1)
    discard n2.log.append(RaftEntry(term: 2'u64, index: 1'u64,
        command: RaftCommand(kind: rckNoop), checksum: 0'u32))

    # Submit new command to n1 (term 1, index 1)
    discard n1.submitCommand(RaftCommand(kind: rckNoop))

    # Deliver AppendEntries to n2
    for (dest, msg) in t1.sentMsgs:
      if dest == NodeId(2'u64):
        t2.deliver(NodeId(1'u64), msg, now)
    t1.sentMsgs.setLen(0)

    # n2 should have truncated and now have only the leader's entry
    check n2.log.getLastLogIndex() == 1'u64
    check n2.log.getEntry(1).term == 1'u64

    # n2 replies; deliver to n1
    for (dest, msg) in t2.sentMsgs:
      if dest == NodeId(1'u64):
        t1.deliver(NodeId(2'u64), msg, now)
    t2.sentMsgs.setLen(0)

    check n1.commitIndex == 1'u64
    check n1.lastApplied == 1'u64

    # Propagate commit
    let hb = RaftMessage(kind: rmAppendEntries,
        appendEntries: AppendEntriesArgs(
          term: n1.term,
          leaderId: n1.nodeId,
          prevLogIndex: n1.log.getLastLogIndex(),
          prevLogTerm: n1.log.getLastLogTerm(),
          entries: @[],
          leaderCommit: n1.commitIndex))
    t2.deliver(NodeId(1'u64), hb, now)
    check n2.commitIndex == 1'u64
    check n2.lastApplied == 1'u64

  test "onApply exception does not destabilize node":
    var applied: AppliedCollector
    initAppliedCollector(applied)
    let onApplyThrow = proc(entry: RaftEntry) {.closure, gcsafe, raises: [
        CatchableError].} =
      if entry.index == 1:
        raise newException(CatchableError, "apply fail")
      if applied.count < applied.data.len:
        applied.data[applied.count] = entry.index
        inc applied.count
    # Leader's onApply is no-op; we only check follower
    n1 = newNode(NodeId(1'u64), @[NodeId(2'u64)], t1, startTime,
        proc(entry: RaftEntry) {.closure, gcsafe, raises: [
            CatchableError].} = discard)
    n2 = newNode(NodeId(2'u64), @[NodeId(1'u64)], t2, startTime, onApplyThrow)
    t1.node = n1
    t2.node = n2

    let now = startTime.uint64 + 1_000_000_000'u64
    electLeader(n1, t2, now)

    # Submit first command (index 1)
    discard n1.submitCommand(RaftCommand(kind: rckNoop))
    # Deliver AppendEntries to n2
    for (dest, msg) in t1.sentMsgs:
      if dest == NodeId(2'u64): t2.deliver(NodeId(1'u64), msg, now)
    t1.sentMsgs.setLen(0)
    # n2 replies
    for (dest, msg) in t2.sentMsgs:
      if dest == NodeId(1'u64): t1.deliver(NodeId(2'u64), msg, now)
    t2.sentMsgs.setLen(0)
    check n1.commitIndex == 1'u64
    check n1.lastApplied == 1'u64

    # Propagate commit to n2 via heartbeat
    let hb1 = RaftMessage(kind: rmAppendEntries,
        appendEntries: AppendEntriesArgs(
          term: n1.term,
          leaderId: n1.nodeId,
          prevLogIndex: n1.log.getLastLogIndex(),
          prevLogTerm: n1.log.getLastLogTerm(),
          entries: @[],
          leaderCommit: n1.commitIndex))
    t2.deliver(NodeId(1'u64), hb1, now)
    # onApply for entry 1 threw; but lastApplied should still be set to 1
    check n2.lastApplied == 1'u64
    check n2.state == rsFollower # node still alive

    # Submit second command (index 2)
    discard n1.submitCommand(RaftCommand(kind: rckNoop))
    # Deliver AppendEntries for entry2
    for (dest, msg) in t1.sentMsgs:
      if dest == NodeId(2'u64): t2.deliver(NodeId(1'u64), msg, now)
    t1.sentMsgs.setLen(0)
    # n2 replies
    for (dest, msg) in t2.sentMsgs:
      if dest == NodeId(1'u64): t1.deliver(NodeId(2'u64), msg, now)
    t2.sentMsgs.setLen(0)
    check n1.commitIndex == 2'u64
    check n1.lastApplied == 2'u64

    # Propagate commit for entry2
    let hb2 = RaftMessage(kind: rmAppendEntries,
        appendEntries: AppendEntriesArgs(
          term: n1.term,
          leaderId: n1.nodeId,
          prevLogIndex: n1.log.getLastLogIndex(),
          prevLogTerm: n1.log.getLastLogTerm(),
          entries: @[],
          leaderCommit: n1.commitIndex))
    t2.deliver(NodeId(1'u64), hb2, now)
    check n2.lastApplied == 2'u64
    check containsApplied(applied, 2'u64) # second entry applied successfully

  test "applyEntries idempotent":
    var applied: AppliedCollector
    initAppliedCollector(applied)
    let onApply = proc(entry: RaftEntry) {.closure, gcsafe, raises: [
        CatchableError].} =
      if applied.count < applied.data.len:
        applied.data[applied.count] = entry.index
        inc applied.count
    n1 = newNode(NodeId(1'u64), @[NodeId(2'u64)], t1, startTime, onApply)
    n2 = newNode(NodeId(2'u64), @[NodeId(1'u64)], t2, startTime,
        proc(entry: RaftEntry) {.closure, gcsafe, raises: [
            CatchableError].} = discard)
    t1.node = n1
    t2.node = n2

    let now = startTime.uint64 + 1_000_000_000'u64
    electLeader(n1, t2, now)

    # Submit and replicate entry 1
    discard n1.submitCommand(RaftCommand(kind: rckNoop))
    for (dest, msg) in t1.sentMsgs:
      if dest == NodeId(2'u64): t2.deliver(NodeId(1'u64), msg, now)
    t1.sentMsgs.setLen(0)
    for (dest, msg) in t2.sentMsgs:
      if dest == NodeId(1'u64): t1.deliver(NodeId(2'u64), msg, now)
    t2.sentMsgs.setLen(0)

    check n1.commitIndex == 1'u64
    check n1.lastApplied == 1'u64
    clearApplied(applied)
    # Call applyEntries again manually; no new application should occur
    n1.applyEntries()
    check n1.lastApplied == 1'u64
    check lenApplied(applied) == 0

  test "Leader change defers commit of previous-term entry":
    var applied: AppliedCollector
    initAppliedCollector(applied)
    let onApply = proc(entry: RaftEntry) {.closure, gcsafe, raises: [
        CatchableError].} =
      if applied.count < applied.data.len:
        applied.data[applied.count] = entry.index
        inc applied.count
    n1 = newNode(NodeId(1'u64), @[NodeId(2'u64)], t1, startTime,
        proc(entry: RaftEntry) {.closure, gcsafe, raises: [
            CatchableError].} = discard)
    n2 = newNode(NodeId(2'u64), @[NodeId(1'u64)], t2, startTime, onApply)
    t1.node = n1
    t2.node = n2

    let now = startTime.uint64 + 1_000_000_000'u64
    electLeader(n1, t2, now)

    # Replicate entry 1 from n1 (term 1)
    discard n1.submitCommand(RaftCommand(kind: rckNoop))
    for (dest, msg) in t1.sentMsgs:
      if dest == NodeId(2'u64): t2.deliver(NodeId(1'u64), msg, now)
    t1.sentMsgs.setLen(0)
    for (dest, msg) in t2.sentMsgs:
      if dest == NodeId(1'u64): t1.deliver(NodeId(2'u64), msg, now)
    t2.sentMsgs.setLen(0)

    check n1.commitIndex == 1'u64
    check n1.lastApplied == 1'u64

    # Propagate commit to n2
    let hb1 = RaftMessage(kind: rmAppendEntries,
        appendEntries: AppendEntriesArgs(
          term: n1.term,
          leaderId: n1.nodeId,
          prevLogIndex: n1.log.getLastLogIndex(),
          prevLogTerm: n1.log.getLastLogTerm(),
          entries: @[],
          leaderCommit: n1.commitIndex))
    t2.deliver(NodeId(1'u64), hb1, now)
    check n2.commitIndex == 1'u64
    check n2.lastApplied == 1'u64
    clearApplied(applied)

    # Force n2 to become new leader (term 2)
    n2.electionDeadline = now - 1'u64
    n2.tick(now)
    for (dest, msg) in t2.sentMsgs:
      if dest == NodeId(1'u64): t1.deliver(NodeId(2'u64), msg, now)
    for (dest, msg) in t1.sentMsgs:
      if dest == NodeId(2'u64): t2.deliver(NodeId(1'u64), msg, now)
    check n2.state == rsLeader
    # As new leader, n2's commitIndex persists from previous state (entry1 already committed)
    check n2.commitIndex == 1'u64

    # n2 submits its own command (term 2)
    discard n2.submitCommand(RaftCommand(kind: rckNoop))
    for (dest, msg) in t2.sentMsgs:
      if dest == NodeId(1'u64): t1.deliver(NodeId(2'u64), msg, now)
    t2.sentMsgs.setLen(0)
    for (dest, msg) in t1.sentMsgs:
      if dest == NodeId(2'u64): t2.deliver(NodeId(1'u64), msg, now)
    t1.sentMsgs.setLen(0)

    check n2.commitIndex == 2'u64
    check n2.lastApplied == 2'u64
    # n2 should have applied entry2; entry1 was already applied earlier
    check containsApplied(applied, 2'u64)

  test "InstallSnapshot and subsequent AppendEntries accepted":
    var applied: AppliedCollector
    initAppliedCollector(applied)
    let onApply = proc(entry: RaftEntry) {.closure, gcsafe, raises: [
        CatchableError].} =
      if applied.count < applied.data.len:
        applied.data[applied.count] = entry.index
        inc applied.count
    n1 = newNode(NodeId(1'u64), @[NodeId(2'u64)], t1, startTime, onApply)
    n2 = newNode(NodeId(2'u64), @[NodeId(1'u64)], t2, startTime,
        proc(entry: RaftEntry) {.closure, gcsafe, raises: [CatchableError].} =
      if applied.count < applied.data.len:
        applied.data[applied.count] = entry.index
        inc applied.count)
    t1.node = n1
    t2.node = n2

    let now = startTime.uint64 + 1_000_000_000'u64
    electLeader(n1, t2, now)

    # Manually create entries 1,2,3 on n1 (bypassing submit for simplicity)
    for i in 1..3:
      discard n1.log.append(RaftEntry(term: 1'u64, index: uint64(i),
          command: RaftCommand(kind: rckNoop), checksum: 0'u32))
    # Create snapshot at index 3, term 1
    let snapData = @[byte(1), byte(2)]
    let snap = n1.log.createSnapshot(3'u64, 1'u64, snapData)
    check n1.log.getSnapshotIndex() == 3'u64

    # Add an entry 4 on n1 after snapshot
    discard n1.log.append(RaftEntry(term: 1'u64, index: 4'u64,
        command: RaftCommand(kind: rckNoop), checksum: 0'u32))

    # Send InstallSnapshot to n2
    let installMsg = RaftMessage(kind: rmInstallSnapshot,
        installSnapshot: InstallSnapshotArgs(
          term: n1.term,
          leaderId: n1.nodeId,
          lastIncludedIndex: 3'u64,
          lastIncludedTerm: 1'u64,
          data: snapData))
    t2.deliver(NodeId(1'u64), installMsg, now)
    check n2.log.getSnapshotIndex() == 3'u64
    check n2.log.getSnapshotTerm() == 1'u64
    # check matchIndex and nextIndex updated
    check n2.matchIndex[NodeId(1'u64)] == 3'u64
    check n2.nextIndex[NodeId(1'u64)] == 4'u64

    # Now send AppendEntries for entry 4 from n1 to n2
    let entry4 = n1.log.getEntry(4)
    let ae = RaftMessage(kind: rmAppendEntries,
        appendEntries: AppendEntriesArgs(
          term: n1.term,
          leaderId: n1.nodeId,
          prevLogIndex: 3'u64,
          prevLogTerm: 1'u64,
          entries: @[entry4],
          leaderCommit: 0'u64))
    t2.deliver(NodeId(1'u64), ae, now)
    check n2.log.getLastLogIndex() == 4'u64
    check n2.log.getEntry(4).term == 1'u64

    # Propagate commit (simulate leader commits entry 4 later)
    n1.commitIndex = 4'u64
    n1.log.setCommitIndex(4'u64)
    let hb = RaftMessage(kind: rmAppendEntries,
        appendEntries: AppendEntriesArgs(
          term: n1.term,
          leaderId: n1.nodeId,
          prevLogIndex: n1.log.getLastLogIndex(),
          prevLogTerm: n1.log.getLastLogTerm(),
          entries: @[],
          leaderCommit: n1.commitIndex))
    t2.deliver(NodeId(1'u64), hb, now)
    check n2.commitIndex == 4'u64
    # applyEntries will be called; entry4 should be applied
    check n2.lastApplied == 4'u64
    check containsApplied(applied, 4'u64)

  test "HandleAppendEntriesReply failure decrements nextIndex":
    # Setup a leader and simulate an AppendEntries failure reply
    n1 = newNode(NodeId(1'u64), @[NodeId(2'u64)], t1, startTime,
        proc(entry: RaftEntry) {.closure, gcsafe, raises: [
            CatchableError].} = discard)
    n2 = newNode(NodeId(2'u64), @[NodeId(1'u64)], t2, startTime,
        proc(entry: RaftEntry) {.closure, gcsafe, raises: [
            CatchableError].} = discard)
    t1.node = n1
    t2.node = n2

    let now = startTime.uint64 + 1_000_000_000'u64
    electLeader(n1, t2, now)

    # Submit a command to create an entry and send AppendEntries
    discard n1.submitCommand(RaftCommand(kind: rckNoop))
    # There should be an AppendEntries in t1.sentMsgs
    check t1.sentMsgs.len >= 1
    # Clear n2's reply to avoid it being automatically delivered
    t2.sentMsgs.setLen(0)

    # Simulate failure reply from follower (n2) instead of using the real reply
    # Clear any pending AppendEntriesReply that would be generated by n2
    # Manually send failure reply
    let failReply = RaftMessage(kind: rmAppendEntriesReply,
        appendEntriesReply: AppendEntriesReply(term: n1.term, success: false,
        conflictTerm: 0, conflictIndex: 0))
    t1.deliver(NodeId(2'u64), failReply, now)

    # nextIndex for n2 should have been decremented (from 2 to 1)
    let nextAfter = n1.nextIndex.getOrDefault(NodeId(2'u64), 0'u64)
    check nextAfter == 1'u64
