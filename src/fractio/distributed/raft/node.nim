## RaftNode state machine for Fractio
## Implements leader election, log replication, commitment, and application.
## Thread-safe with lock protection. Designed for unit testing with mock transports.

import system
import locks
import tables
import std/random
import ../../core/errors
import ../../utils/logging
import ./types
import ./log

type
  RaftState* = enum
    rsFollower, rsCandidate, rsLeader

  RaftConfig* = object
    electionTimeoutMin*: uint64 ## Minimum election timeout in nanoseconds
    electionTimeoutMax*: uint64 ## Maximum election timeout in nanoseconds
    heartbeatInterval*: uint64  ## Leader heartbeat interval in nanoseconds
    clusterSize*: int           ## Total number of nodes in the cluster
    peerIds*: seq[NodeId]       ## IDs of all peers (excluding self)

  RaftNode* = ref object
    nodeId*: NodeId
    state*: RaftState
    term*: uint64
    votedFor*: uint64
    log*: RaftLog
    commitIndex*: uint64
    lastApplied*: uint64
    leaderId*: uint64
    transport*: RaftTransport
    logger*: Logger
    lock*: Lock

    electionTimeoutMin*: uint64
    electionTimeoutMax*: uint64
    heartbeatInterval*: uint64
    clusterSize*: int
    peerIds*: seq[NodeId]

    electionDeadline*: uint64
    heartbeatDeadline*: uint64

    nextIndex*: Table[NodeId, uint64]
    matchIndex*: Table[NodeId, uint64]
    lastSentIndex*: Table[NodeId, uint64] ## Tracks in-flight batch highest index (0 if none)

    votesGranted*: int
    votesFrom*: Table[NodeId, bool]

    onApply*: proc(entry: RaftEntry) {.closure, gcsafe, raises: [CatchableError].}

# Helper: majority threshold
proc majority(self: RaftNode): int {.gcsafe.} =
  result = self.clusterSize div 2 + 1

proc acquire(self: RaftNode) = self.lock.acquire()
proc release(self: RaftNode) = self.lock.release()

# Forward declarations for procs used in tick (defined later)
proc becomeCandidate(self: RaftNode, now: uint64) {.gcsafe, raises: [FractioError].}
proc sendHeartbeats(self: RaftNode) {.gcsafe, raises: [FractioError].}

# Forward declarations for RPC handlers used in receiveMessage (defined later)
proc handleRequestVote(self: RaftNode, sender: NodeId, args: RequestVoteArgs,
    now: uint64) {.gcsafe, raises: [FractioError].}
proc handleRequestVoteReply(self: RaftNode, sender: NodeId,
    reply: RequestVoteReply, now: uint64) {.gcsafe, raises: [FractioError].}
proc handleAppendEntries(self: RaftNode, sender: NodeId,
    args: AppendEntriesArgs, now: uint64) {.gcsafe, raises: [FractioError].}
proc handleAppendEntriesReply(self: RaftNode, sender: NodeId,
    reply: AppendEntriesReply, now: uint64) {.gcsafe, raises: [FractioError].}
proc handleInstallSnapshot(self: RaftNode, sender: NodeId,
    args: InstallSnapshotArgs, now: uint64) {.gcsafe, raises: [FractioError].}

# Forward declarations for other procs used before definition
proc maybeCommit(self: RaftNode) {.gcsafe, raises: [FractioError].}
## Applies all committed but not yet applied entries to the state machine.
## Calls the onApply callback for each new entry. Idempotent: already applied entries are skipped.
## Errors from onApply are caught and logged but do not stop processing.
proc applyEntries*(self: RaftNode) {.gcsafe, raises: [FractioError].}

# Helper to safely log errors without raising exceptions
proc safeLogError(logger: Logger, msg: string) {.gcsafe, raises: [].} =
  try:
    logger.error(msg)
  except Exception:
    discard

proc newRaftNode*(nodeId: NodeId, log: RaftLog, transport: RaftTransport,
    logger: Logger, config: RaftConfig, onApply: proc(
    entry: RaftEntry) {.closure, gcsafe, raises: [CatchableError].},
        now: uint64): RaftNode =
  ## Create a new RaftNode. It starts as a follower with an election timer set.
  ##
  ## - onApply: Callback invoked to apply a committed log entry to the state machine.
  ##   The callback may raise CatchableError; errors are logged but do not destabilize the node.
  result = RaftNode(
    nodeId: nodeId,
    state: rsFollower,
    term: log.getCurrentTerm(),
    votedFor: log.getVotedFor(),
    log: log,
    commitIndex: log.getCommitIndex(),
    lastApplied: log.getLastApplied(),
    leaderId: 0,
    transport: transport,
    logger: logger,
    onApply: onApply,
    electionTimeoutMin: config.electionTimeoutMin,
    electionTimeoutMax: config.electionTimeoutMax,
    heartbeatInterval: config.heartbeatInterval,
    clusterSize: config.clusterSize,
    peerIds: config.peerIds,
  )
  initLock(result.lock)
  result.nextIndex = initTable[NodeId, uint64]()
  result.matchIndex = initTable[NodeId, uint64]()
  result.lastSentIndex = initTable[NodeId, uint64]()
  result.votesFrom = initTable[NodeId, bool]()
  # Random election timeout
  result.electionDeadline = now + rand(config.electionTimeoutMin.int ..
      config.electionTimeoutMax.int).uint64
  result.heartbeatDeadline = 0

proc tick*(self: RaftNode, now: uint64) {.gcsafe, raises: [FractioError].} =
  ## Called periodically to advance the state machine. Handles election timeouts and heartbeats.
  self.acquire()
  try:
    if now >= self.electionDeadline:
      case self.state
      of rsFollower:
        self.becomeCandidate(now)
      of rsCandidate:
        self.becomeCandidate(now) # new election
      of rsLeader:
        # Leader should not have election deadline; ignore
        discard
    if self.state == rsLeader and now >= self.heartbeatDeadline:
      self.sendHeartbeats()
      self.heartbeatDeadline = now + self.heartbeatInterval
  finally:
    self.release()

proc becomeFollower(self: RaftNode, term: uint64, now: uint64) {.gcsafe,
    raises: [FractioError].} =
  ## Transition to follower state with given term, reset election timer, and persist.
  if term < self.term: return
  self.term = term
  self.state = rsFollower
  self.leaderId = 0
  self.votedFor = 0
  # Update log's term and clear votedFor, then persist
  self.log.setCurrentTerm(term)
  self.log.setVotedFor(0)
  self.log.persistMeta()
  # Reset election timer
  self.electionDeadline = now + rand(self.electionTimeoutMin.int ..
      self.electionTimeoutMax.int).uint64
  # Clear candidate-specific state
  self.votesGranted = 0
  self.votesFrom.clear()

proc becomeCandidate(self: RaftNode, now: uint64) {.gcsafe, raises: [
    FractioError].} =
  ## Transition to candidate, increment term, vote for self, and request votes.
  self.term += 1
  self.state = rsCandidate
  self.votedFor = self.nodeId
  self.log.setCurrentTerm(self.term)
  self.log.setVotedFor(self.nodeId)
  self.log.persistMeta()
  # Reset election timer
  self.electionDeadline = now + rand(self.electionTimeoutMin.int ..
      self.electionTimeoutMax.int).uint64
  # Initialize vote count (self vote)
  self.votesGranted = 1
  self.votesFrom = initTable[NodeId, bool]()
  self.votesFrom[self.nodeId] = true
  # Send RequestVote RPCs
  let lastIdx = self.log.getLastLogIndex()
  let lastTerm = self.log.getLastLogTerm()
  for peer in self.peerIds:
    let args = RequestVoteArgs(term: self.term, candidateId: self.nodeId,
        lastLogIndex: lastIdx, lastLogTerm: lastTerm)
    let msg = RaftMessage(kind: rmRequestVote, requestVote: args)
    try:
      self.transport.send(peer, msg)
    except FractioError as e:
      safeLogError(self.logger, "Failed to send RequestVote to " & $peer &
          ": " & e.msg)

proc becomeLeader(self: RaftNode, now: uint64) {.gcsafe, raises: [
    FractioError].} =
  ## Transition to leader, initialize replication state, and send initial heartbeats.
  self.state = rsLeader
  self.leaderId = self.nodeId
  self.electionDeadline = 0 # no election timer
  # Initialize nextIndex and matchIndex for all peers
  let lastLogIndex = self.log.getLastLogIndex()
  for peer in self.peerIds:
    self.nextIndex[peer] = lastLogIndex + 1
    self.matchIndex[peer] = 0
  # For self, matchIndex is lastLogIndex
  self.matchIndex[self.nodeId] = lastLogIndex
  # Reset lastSentIndex (no pending batches)
  self.lastSentIndex.clear()
  # Set immediate heartbeat
  self.heartbeatDeadline = now
  # Send initial AppendEntries (may contain new entries)
  self.sendHeartbeats()

proc sendHeartbeats(self: RaftNode) {.gcsafe, raises: [FractioError].} =
  ## Send AppendEntries RPC to all followers. Sends full batch from nextIndex to lastLogIndex, or empty heartbeat if up-to-date.
  let lastLogIndex = self.log.getLastLogIndex()
  let lastLogTerm = self.log.getLastLogTerm()
  let leaderCommit = self.commitIndex
  for peer in self.peerIds:
    # Skip if there is a pending batch for this peer (avoid pipelining)
    if self.lastSentIndex.getOrDefault(peer, 0'u64) > 0:
      continue
    let nextIdx = self.nextIndex.getOrDefault(peer, 0'u64)
    if nextIdx > lastLogIndex:
      # Heartbeat (no entries)
      let args = AppendEntriesArgs(
        term: self.term,
        leaderId: self.nodeId,
        prevLogIndex: lastLogIndex,
        prevLogTerm: lastLogTerm,
        entries: @[],
        leaderCommit: leaderCommit
      )
      try:
        self.transport.send(peer, RaftMessage(kind: rmAppendEntries,
            appendEntries: args))
      except FractioError as e:
        safeLogError(self.logger, "Failed to send heartbeat to " & $peer &
            ": " & e.msg)
    else:
      # Collect entries from nextIdx to lastLogIndex
      var entries: seq[RaftEntry] = @[]
      var i = nextIdx
      while i <= lastLogIndex:
        try:
          let entry = self.log.getEntry(i)
          entries.add(entry)
        except FractioError:
          # Should not happen; skip peer
          break
        inc i
      if entries.len == 0:
        continue
      let prevLogIndex = nextIdx - 1
      var prevLogTerm: uint64 = 0
      if prevLogIndex > 0:
        try:
          let prevEntry = self.log.getEntry(prevLogIndex)
          prevLogTerm = prevEntry.term
        except FractioError:
          prevLogTerm = 0
      let args = AppendEntriesArgs(
        term: self.term,
        leaderId: self.nodeId,
        prevLogIndex: prevLogIndex,
        prevLogTerm: prevLogTerm,
        entries: entries,
        leaderCommit: leaderCommit
      )
      # Record in-flight batch
      self.lastSentIndex[peer] = lastLogIndex
      try:
        self.transport.send(peer, RaftMessage(kind: rmAppendEntries,
            appendEntries: args))
      except FractioError as e:
        safeLogError(self.logger, "Failed to send AppendEntries to " & $peer &
            ": " & e.msg)
        self.lastSentIndex.del(peer) # clear so we can retry

proc receiveMessage*(self: RaftNode, sender: NodeId, msg: RaftMessage,
    now: uint64) {.gcsafe, raises: [FractioError].} =
  ## Called by transport when a message arrives. Processes RPC under lock.
  self.acquire()
  try:
    case msg.kind
    of rmRequestVote:
      self.handleRequestVote(sender, msg.requestVote, now)
    of rmRequestVoteReply:
      self.handleRequestVoteReply(sender, msg.requestVoteReply, now)
    of rmAppendEntries:
      self.handleAppendEntries(sender, msg.appendEntries, now)
    of rmAppendEntriesReply:
      self.handleAppendEntriesReply(sender, msg.appendEntriesReply, now)
    of rmInstallSnapshot:
      self.handleInstallSnapshot(sender, msg.installSnapshot, now)
  finally:
    self.release()

proc handleRequestVote(self: RaftNode, sender: NodeId, args: RequestVoteArgs,
    now: uint64) {.gcsafe, raises: [FractioError].} =
  if args.term > self.term:
    self.becomeFollower(args.term, now)
  if args.term < self.term:
    let reply = RequestVoteReply(term: self.term, voteGranted: false)
    try:
      self.transport.send(sender, RaftMessage(kind: rmRequestVoteReply,
          requestVoteReply: reply))
    except FractioError as e:
      safeLogError(self.logger, "Failed to send RequestVoteReply: " & e.msg)
    return
  # Now args.term == self.term
  if self.votedFor != 0 and self.votedFor != args.candidateId:
    # Reset election timer (we saw a candidate)
    self.electionDeadline = now + rand(self.electionTimeoutMin.int ..
        self.electionTimeoutMax.int).uint64
    let reply = RequestVoteReply(term: self.term, voteGranted: false)
    try:
      self.transport.send(sender, RaftMessage(kind: rmRequestVoteReply,
          requestVoteReply: reply))
    except FractioError as e:
      safeLogError(self.logger, "Failed to send RequestVoteReply: " & e.msg)
    return
  # Check log up-to-date
  let myLastIdx = self.log.getLastLogIndex()
  let myLastTerm = self.log.getLastLogTerm()
  var logOk = false
  if args.lastLogTerm > myLastTerm:
    logOk = true
  elif args.lastLogTerm == myLastTerm and args.lastLogIndex >= myLastIdx:
    logOk = true
  else:
    logOk = false
  if logOk:
    self.votedFor = args.candidateId
    self.log.setVotedFor(args.candidateId)
    self.log.persistMeta()
    # Reset election timer (we voted for a candidate)
    self.electionDeadline = now + rand(self.electionTimeoutMin.int ..
        self.electionTimeoutMax.int).uint64
    let reply = RequestVoteReply(term: self.term, voteGranted: true)
    try:
      self.transport.send(sender, RaftMessage(kind: rmRequestVoteReply,
          requestVoteReply: reply))
    except FractioError as e:
      safeLogError(self.logger, "Failed to send RequestVoteReply: " & e.msg)
  else:
    # Reset election timer (we saw a candidate even though we rejected)
    self.electionDeadline = now + rand(self.electionTimeoutMin.int ..
        self.electionTimeoutMax.int).uint64
    let reply = RequestVoteReply(term: self.term, voteGranted: false)
    try:
      self.transport.send(sender, RaftMessage(kind: rmRequestVoteReply,
          requestVoteReply: reply))
    except FractioError as e:
      safeLogError(self.logger, "Failed to send RequestVoteReply: " & e.msg)

proc handleRequestVoteReply(self: RaftNode, sender: NodeId,
    reply: RequestVoteReply, now: uint64) {.gcsafe, raises: [FractioError].} =
  if reply.term > self.term:
    self.becomeFollower(reply.term, now)
    return
  if self.state != rsCandidate or self.term != reply.term:
    return
  if reply.voteGranted:
    if not (sender in self.votesFrom):
      self.votesFrom[sender] = true
      inc(self.votesGranted)
      if self.votesGranted >= self.majority():
        self.becomeLeader(now)

proc handleAppendEntries(self: RaftNode, sender: NodeId,
    args: AppendEntriesArgs, now: uint64) {.gcsafe, raises: [FractioError].} =
  if args.term > self.term:
    self.becomeFollower(args.term, now)
  if args.term < self.term:
    let reply = AppendEntriesReply(term: self.term, success: false)
    try:
      self.transport.send(sender, RaftMessage(kind: rmAppendEntriesReply,
          appendEntriesReply: reply))
    except FractioError as e:
      safeLogError(self.logger, "Failed to send AppendEntriesReply: " & e.msg)
    return
  # At this point, args.term == self.term
  if self.state == rsCandidate:
    self.state = rsFollower
  # Reset election timer (heartbeat from leader)
  self.electionDeadline = now + rand(self.electionTimeoutMin.int ..
      self.electionTimeoutMax.int).uint64
  self.leaderId = args.leaderId
  # Consistency check on prevLogIndex
  if args.prevLogIndex > 0:
    let snapIdx = self.log.getSnapshotIndex()
    if args.prevLogIndex < snapIdx:
      # Leader is behind; follower needs snapshot
      let reply = AppendEntriesReply(term: self.term, success: false)
      try:
        self.transport.send(sender, RaftMessage(kind: rmAppendEntriesReply,
            appendEntriesReply: reply))
      except FractioError as e:
        safeLogError(self.logger, "Failed to send AppendEntriesReply: " & e.msg)
      return
    elif args.prevLogIndex == snapIdx:
      # Check against snapshot term
      if args.prevLogTerm != self.log.getSnapshotTerm():
        let reply = AppendEntriesReply(term: self.term, success: false)
        try:
          self.transport.send(sender, RaftMessage(kind: rmAppendEntriesReply,
              appendEntriesReply: reply))
        except FractioError as e:
          safeLogError(self.logger, "Failed to send AppendEntriesReply: " & e.msg)
        return
      # else: prevLog matches snapshot, continue without fetching entry
    else: # args.prevLogIndex > snapIdx
      try:
        let prevEntry = self.log.getEntry(args.prevLogIndex)
        if prevEntry.term != args.prevLogTerm:
          let reply = AppendEntriesReply(term: self.term, success: false)
          try:
            self.transport.send(sender, RaftMessage(kind: rmAppendEntriesReply,
                appendEntriesReply: reply))
          except FractioError as e:
            safeLogError(self.logger, "Failed to send AppendEntriesReply: " & e.msg)
          return
      except FractioError:
        let reply = AppendEntriesReply(term: self.term, success: false)
        try:
          self.transport.send(sender, RaftMessage(kind: rmAppendEntriesReply,
              appendEntriesReply: reply))
        except FractioError as e:
          safeLogError(self.logger, "Failed to send AppendEntriesReply: " & e.msg)
        return
  # Process entries
  if args.entries.len > 0:
    var startIdx: uint64 = 0
    # Find first index that needs to be appended (missing or term conflict)
    for entry in args.entries:
      if entry.index <= self.log.getSnapshotIndex():
        continue
      try:
        let existing = self.log.getEntry(entry.index)
        if existing.term == entry.term:
          # Already present, skip
          continue
        else:
          # Conflict: need to truncate from here
          startIdx = entry.index
          break
      except FractioError:
        # Missing entry: need to append from here
        startIdx = entry.index
        break
    if startIdx > 0:
      # Truncate from startIdx
      self.log.truncateFrom(startIdx)
      # Append all entries starting from startIdx (must be consecutive)
      for entry in args.entries:
        if entry.index < startIdx:
          continue
        # Append will validate consecutive index
        discard self.log.append(entry)
  # Update commit index based on leaderCommit
  if args.leaderCommit > self.commitIndex:
    let newCommit = min(args.leaderCommit, self.log.getLastLogIndex())
    if newCommit > self.commitIndex:
      self.commitIndex = newCommit
      self.log.setCommitIndex(newCommit)
      self.applyEntries()
  # Reply success
  let reply = AppendEntriesReply(term: self.term, success: true)
  try:
    self.transport.send(sender, RaftMessage(kind: rmAppendEntriesReply,
        appendEntriesReply: reply))
  except FractioError as e:
    safeLogError(self.logger, "Failed to send AppendEntriesReply: " & e.msg)

proc handleAppendEntriesReply(self: RaftNode, sender: NodeId,
    reply: AppendEntriesReply, now: uint64) {.gcsafe, raises: [FractioError].} =
  if reply.term > self.term:
    self.becomeFollower(reply.term, now)
    return
  if self.state != rsLeader or self.term != reply.term:
    return
  # Check if we have a pending batch for this follower
  let lastSentVal = self.lastSentIndex.getOrDefault(sender, 0'u64)
  if lastSentVal == 0:
    return
  let sentIdx = lastSentVal
  if reply.success:
    self.matchIndex[sender] = sentIdx
    self.nextIndex[sender] = sentIdx + 1
    self.lastSentIndex.del(sender)
    self.maybeCommit()
  else:
    # Decrement nextIndex to retry
    let current = self.nextIndex.getOrDefault(sender, 0'u64)
    if current > 1:
      self.nextIndex[sender] = current - 1
    else:
      self.nextIndex[sender] = 1
    self.lastSentIndex.del(sender)

proc maybeCommit(self: RaftNode) {.gcsafe, raises: [FractioError].} =
  if self.state != rsLeader: return
  let lastIdx = self.log.getLastLogIndex()
  var i = lastIdx
  while i > self.commitIndex:
    var count = 0
    for peer in self.peerIds:
      if self.matchIndex.getOrDefault(peer, 0'u64) >= i:
        inc count
    if self.matchIndex.getOrDefault(self.nodeId, 0'u64) >= i:
      inc count
    if count >= self.majority():
      # Check term of entry i
      let entry = self.log.getEntry(i)
      if entry.term == self.term:
        self.commitIndex = i
        self.log.setCommitIndex(i)
        self.applyEntries()
        break
    dec i

proc applyEntries*(self: RaftNode) {.gcsafe, raises: [FractioError].} =
  while self.lastApplied < self.commitIndex:
    inc self.lastApplied
    let entry = self.log.getEntry(self.lastApplied)
    try:
      self.onApply(entry)
    except CatchableError as e:
      safeLogError(self.logger, "onApply failed at index " & $self.lastApplied &
          ": " & e.msg)
    self.log.setLastApplied(self.lastApplied)
  self.log.persistMeta()

proc handleInstallSnapshot(self: RaftNode, sender: NodeId,
    args: InstallSnapshotArgs, now: uint64) {.gcsafe, raises: [FractioError].} =
  if args.term > self.term:
    self.becomeFollower(args.term, now)
    return
  if args.term < self.term:
    return
  if self.state == rsCandidate or self.state == rsLeader:
    self.state = rsFollower
  self.leaderId = args.leaderId
  self.electionDeadline = now + rand(self.electionTimeoutMin.int ..
      self.electionTimeoutMax.int).uint64
  let mySnapshotIdx = self.log.getSnapshotIndex()
  if args.lastIncludedIndex > mySnapshotIdx:
    let snap = Snapshot(lastIndex: args.lastIncludedIndex,
        lastTerm: args.lastIncludedTerm, data: args.data)
    self.log.installSnapshot(snap)
    # Snapshot represents all work up to lastIncludedIndex; mark as applied and committed.
    self.lastApplied = args.lastIncludedIndex
    self.commitIndex = args.lastIncludedIndex
    self.log.setLastApplied(args.lastIncludedIndex)
    self.log.setCommitIndex(args.lastIncludedIndex)
    # After installing snapshot, our state should be follower with updated log
    # Update matchIndex and nextIndex for the leader (if needed)
    self.matchIndex[sender] = args.lastIncludedIndex
    self.nextIndex[sender] = args.lastIncludedIndex + 1

proc submitCommand*(self: RaftNode, cmd: RaftCommand): uint64 {.raises: [
    FractioError].} =
  ## Submit a client command to the Raft cluster. Only the leader can accept commands.
  self.acquire()
  try:
    if self.state != rsLeader:
      raise storageError("Cannot submit command: not leader", "")
    # Determine next index
    let lastLogIndex = self.log.getLastLogIndex()
    let nextIdx = if lastLogIndex > 0: lastLogIndex +
        1 else: self.log.getSnapshotIndex() + 1
    let entry = RaftEntry(term: self.term, index: nextIdx, command: cmd,
        checksum: 0'u32)
    # Append will compute checksum and set index
    let idx = self.log.append(entry)
    # Update matchIndex for self
    self.matchIndex[self.nodeId] = idx
    # Replicate to followers immediately
    self.sendHeartbeats()
    return idx
  finally:
    self.release()
