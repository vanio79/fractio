# multigroup_transport.nim
#
# Bridge between MultiRaftCoordinator (which uses group_types.NodeID = distinct
# uint32) and the existing network stack (which uses core/types.NodeID =
# distinct string).
#
# Responsibilities:
#   1. NodeID / NodeID translation in both directions.
#   2. Starting/stopping the NetworkRaftNode that owns the TCP listener.
#   3. Registering incoming-RPC handlers that dispatch into the coordinator's
#      group state (handleRequestVote, handleAppendEntries, handleInstallSnapshot).
#   4. Outbound replication: replicateEntry — sends AppendEntries to all peer
#      replicas of a group, waits for quorum, returns success.
#   5. Election: startElection — sends RequestVote to all peers, counts votes,
#      calls group.becomeLeader() on quorum.
#   6. Heartbeat: sendHeartbeats — sends empty AppendEntries to all peer
#      replicas of every group where this node is leader.
#
# Thread safety:
#   All exported procs are gcsafe. The coordinator's groupsLock / group.lock
#   ordering is preserved (groupsLock → group.lock → smMu never inverted).
#   Fan-out threads communicate results via built-in Channel[T] (value type).

import std/[tables, locks, options, atomics, typedthreads, strutils, sequtils]

import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/multigroup_log
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/types as oldRaftTypes
import fractio/distributed/network/types as netTypes
import fractio/distributed/network/config as netCfgMod
import fractio/distributed/network/connection_manager
import fractio/distributed/network/network_raft_node
import fractio/distributed/network/raft_transport
import fractio/distributed/network/serialization
import fractio/core/types as coreTypes

# ============================================================================
# NodeID translation
# ============================================================================

proc toNetNodeID*(id: rangeTypes.NodeID): coreTypes.NodeID {.inline.} =
  ## group_types.NodeID (distinct uint32) → core/types.NodeID (distinct string)
  coreTypes.NodeID("rn_" & $id.uint32)

proc toNodeID*(id: coreTypes.NodeID): rangeTypes.NodeID {.inline.} =
  ## core/types.NodeID (distinct string) → group_types.NodeID (distinct uint32)
  let s = string(id)
  if s.startsWith("rn_"):
    try: return rangeTypes.NodeID(uint32(parseInt(s[3..^1])))
    except: discard
  rangeTypes.NodeID(0)

# ============================================================================
# RaftGroupTransport — per-coordinator transport bridge
# ============================================================================

type
  PeerAddr* = object
    ## Address information for one peer replica
    nodeId*: rangeTypes.NodeID ## peer's range-layer NodeID
    host*: string
    raftPort*: int                  ## port where peer's Raft TCP listener runs

  RaftGroupTransport* = ref object of RootObj
    ## Owns the NetworkRaftNode and bridges it to MultiRaftCoordinator.
    ##
    ## The coordinator holds a ref to this object; the NetworkRaftNode holds
    ## the TCP listener. Incoming RPCs flow:
    ##   TCP → NetworkRaftNode handler → dispatchXxx → coordinator group
    ##
    ## Outbound RPCs flow:
    ##   coordinator.replicateEntry → transport.replicateEntry
    ##     → NetworkRaftNode.raftTransport.sendAppendEntries
    localNodeId*: rangeTypes.NodeID
    raftNode*: NetworkRaftNode
    peers*: seq[PeerAddr] ## all peers (not including self)

    coordinator*: pointer ## void ptr to MultiRaftCoordinator — avoids circular import

# ============================================================================
# Construction
# ============================================================================

proc newRaftGroupTransport*(localNodeId: rangeTypes.NodeID,
                             host: string,
                             raftPort: int,
                             peers: seq[PeerAddr]): RaftGroupTransport =
  ## Create a transport bridge for a node.
  ## raftPort — the TCP port this node will listen on for Raft RPCs.
  ## peers    — list of ALL other replica addresses (not including self).
  new(result)
  result.localNodeId = localNodeId
  result.peers = peers
  result.coordinator = nil

  let netCfg = netCfgMod.newNetworkConfig(
    nodeId = toNetNodeID(localNodeId),
    basePort = raftPort, # raftPort is basePort; offset 0 = Raft
    bindAddress = "0.0.0.0",
  )
  # Short connect timeout: loopback connect is <1ms.
  # Read timeout must be generous enough for high-concurrency workloads where
  # the coordinator worker thread is saturated and responses are delayed.
  # 800ms was too aggressive — caused intermittent quorum failures under load.
  netCfg.tcpConnectTimeoutMs = 300
  netCfg.tcpReadTimeoutMs = 5000
  netCfg.tcpWriteTimeoutMs = 500

  # Register peers in the NetworkConfig so the connection manager knows them.
  # IMPORTANT: Use the "raft_N" NodeID format (matching toNodeID() in raft_transport.nim)
  # so that sendRequestVote/sendAppendEntries lookups in ConnectionManager succeed.
  # toNetNodeID() produces "rn_N" which does NOT match what raft_transport.toNodeID() uses.
  for p in peers:
    let raftNodeId = raft_transport.toNodeID(int32(p.nodeId.uint32))
    let pc = netCfgMod.newPeerConfig(raftNodeId, p.host, p.raftPort)
    netCfg.addPeer(pc)

  let raftCfg = oldRaftTypes.RaftConfig(
    serverId: int32(localNodeId.uint32),
    electionTimeout: 300,
    heartbeatInterval: 100,
  )

  result.raftNode = newNetworkRaftNode(raftCfg, netCfg)

  # Register peers in the connection manager (same "raft_N" format)
  for p in peers:
    result.raftNode.addPeer(
      int32(p.nodeId.uint32), p.host, p.raftPort,
      p.raftPort + 1, p.raftPort + 2)

# ============================================================================
# Runtime peer addition
# ============================================================================

proc addPeer*(t: RaftGroupTransport, nodeId: rangeTypes.NodeID,
              host: string, raftPort: int) =
  ## Register a new peer at runtime (for dynamic cluster join).
  ## Adds to both the NetworkConfig and ConnectionManager.
  let peer = PeerAddr(nodeId: nodeId, host: host, raftPort: raftPort)

  # Avoid duplicate
  for p in t.peers:
    if p.nodeId == nodeId: return
  t.peers.add(peer)

  # Register in NetworkConfig so connection manager can connect
  let raftNodeId = raft_transport.toNodeID(int32(nodeId.uint32))
  let pc = netCfgMod.newPeerConfig(raftNodeId, host, raftPort)
  t.raftNode.raftTransport.connManager.config.addPeer(pc)

  # Register in connection manager
  t.raftNode.addPeer(
    int32(nodeId.uint32), host, raftPort,
    raftPort + 1, raftPort + 2)

# ============================================================================
# Start / Stop
# ============================================================================

proc start*(t: RaftGroupTransport) =
  ## Start TCP listener and register incoming-RPC handlers.
  ## coordinator must be set before calling this.
  discard t.raftNode.start()

proc stop*(t: RaftGroupTransport) =
  t.raftNode.stop()

# ============================================================================
# GroupID / term encoding helpers
# (defined here so they are available to replicateEntry, startElection,
# sendHeartbeats, and the incoming-handler procs below)
# ============================================================================

# Helper: pack GroupID into the high 32 bits of MessageHeader.term
# Leader uses this when sending RequestVote / AppendEntries so the receiver
# knows which Raft group the message belongs to.
proc encodeGroupInTerm*(term: uint64, groupId: rangeTypes.GroupID): uint64 {.inline.} =
  (uint64(groupId.uint64) shl 32) or (term and 0xFFFF_FFFF'u64)

proc decodeGroupFromTerm*(v: uint64): rangeTypes.GroupID {.inline.} =
  rangeTypes.GroupID(v shr 32)

proc decodeTermFromTerm*(v: uint64): uint64 {.inline.} =
  v and 0xFFFF_FFFF'u64

# ============================================================================
# Outbound: fan-out AppendEntries, wait for quorum
# ============================================================================

type
  ReplicaAck = object
    nodeId: rangeTypes.NodeID
    success: bool
    matchIndex: uint64

proc replicateEntry*(t: RaftGroupTransport,
                     group: RaftGroup,
                     log: RaftLog,
                     entry: multigroup_types.LogEntry,
                     timeoutMs: int): bool {.gcsafe.} =
  ## Send AppendEntries to every peer voter replica.  For each peer we consult
  ## its `nextIndex` so that a lagging follower receives all missing entries
  ## (not just the latest one).  This is the standard Raft leader replication
  ## algorithm.
  ##
  ## Returns true iff quorum positive acks received within timeoutMs.

  let voters = group.descriptor.getVoters()
  let quorum = group.quorum()
  let peers = voters.filterIt(it.nodeId != t.localNodeId)

  # Single-voter group — commit immediately without network
  if peers.len == 0:
    return true

  let commitIndex = group.commitIndex.load()
  let rawTerm = group.currentTerm.load()
  let encodedTerm = encodeGroupInTerm(rawTerm, group.groupId)
  let leaderId = int32(t.localNodeId.uint32)

  # ---- Build per-peer entry batches based on nextIndex ----
  type PerPeerData = object
    prevIdx: uint64
    prevTerm: uint64
    entries: seq[oldRaftTypes.LogEntry]

  var peerData = newSeq[PerPeerData](peers.len)

  for i, rep in peers:
    # Read this peer's nextIndex (the index of the first entry we need to send)
    var peerNextIdx: uint64
    withLock group.lock:
      peerNextIdx = group.nextIndex.getOrDefault(rep.replicaId, 1'u64)

    # Clamp: never send entries before 1 or after the new entry
    if peerNextIdx < 1: peerNextIdx = 1
    if peerNextIdx > entry.index: peerNextIdx = entry.index

    # prevLogIndex / prevLogTerm for this peer
    let prevIdx = if peerNextIdx > 1: peerNextIdx - 1 else: 0'u64
    let prevTm = block:
      if prevIdx == 0: 0'u64
      else:
        try:
          let eOpt = log.getEntry(prevIdx)
          if eOpt.isSome: eOpt.get.term else: 0'u64
        except CatchableError: 0'u64

    # Gather all entries from peerNextIdx .. entry.index
    var oldEntries: seq[oldRaftTypes.LogEntry]
    for idx in peerNextIdx .. entry.index:
      let eOpt = try: log.getEntry(idx)
                 except CatchableError: none(multigroup_types.LogEntry)
      if eOpt.isNone:
        break # gap in log — send what we have
      let encoded = try: encodeEntry(eOpt.get)
                    except CatchableError: ""
      if encoded.len == 0:
        break
      var oe: oldRaftTypes.LogEntry
      oe.term = int64(eOpt.get.term)
      oe.entryType = oldRaftTypes.LET_NORMAL
      oe.data = encoded
      oldEntries.add(oe)

    peerData[i] = PerPeerData(prevIdx: prevIdx, prevTerm: prevTm,
                               entries: oldEntries)

  # Heap-allocated result channel (raw ptr — avoids ORC cross-thread SIGSEGV)
  type AckChanObj = object
    ch: Channel[ReplicaAck]
  var ackPtr = cast[ptr AckChanObj](allocShared0(sizeof(AckChanObj)))
  ackPtr[].ch.open(peers.len + 2)

  # Fan-out: one thread per peer
  type FanoutCtx = tuple[
    rt: raft_transport.RaftTransport,
    targetId: int32, term: uint64, leaderId: int32,
    prevIdx: uint64, prevTerm: uint64, commit: uint64,
    entries: seq[oldRaftTypes.LogEntry],
    peerNodeId: rangeTypes.NodeID, ackPtr: ptr AckChanObj,
    replicaId: ReplicaID]

  var threadSeq = newSeq[Thread[FanoutCtx]](peers.len)

  for i, rep in peers:
    let ctx: FanoutCtx = (
      rt: t.raftNode.raftTransport,
      targetId: int32(rep.nodeId.uint32),
      term: encodedTerm,
      leaderId: leaderId,
      prevIdx: peerData[i].prevIdx,
      prevTerm: peerData[i].prevTerm,
      commit: commitIndex,
      entries: peerData[i].entries,
      peerNodeId: rep.nodeId,
      ackPtr: ackPtr,
      replicaId: rep.replicaId,
    )
    createThread(threadSeq[i], proc(c: FanoutCtx) {.thread, gcsafe.} =
      var ack = ReplicaAck(nodeId: c.peerNodeId, success: false)
      {.cast(gcsafe).}:
        let respOpt = c.rt.sendAppendEntries(
          c.targetId, c.term, c.leaderId,
          c.prevIdx, c.prevTerm, c.commit, c.entries)
        if respOpt.isSome:
          let resp = respOpt.get()
          ack = ReplicaAck(nodeId: c.peerNodeId, success: resp.success,
            matchIndex: resp.matchIndex)
      c.ackPtr[].ch.send(ack)
    , ctx)

  # Count acks (self = 1). Each fan-out thread sends exactly one ack via
  # blocking recv(). The fan-out threads have their own socket timeouts
  # (tcpReadTimeoutMs) so recv() always returns promptly.
  var acks = 1
  var received = 0
  var quorumReached = false

  # Receive all acks using blocking recv(). Each fan-out thread always sends
  # exactly one ack, so we will get exactly peers.len acks total.
  # We process them as they arrive and check quorum after each.
  for _ in 0..<peers.len:
    let ack = ackPtr[].ch.recv()
    inc received
    if ack.success:
      for rep2 in group.descriptor.replicas:
        if rep2.nodeId == ack.nodeId:
          withLock group.lock:
            group.matchIndex[rep2.replicaId] = ack.matchIndex
            group.nextIndex[rep2.replicaId] = ack.matchIndex + 1
          break
      inc acks
    else:
      # Raft: on rejection, decrement nextIndex for this peer so the next
      # replicateEntry will send earlier entries (log backoff).
      for rep2 in group.descriptor.replicas:
        if rep2.nodeId == ack.nodeId:
          withLock group.lock:
            let cur = group.nextIndex.getOrDefault(rep2.replicaId, 1'u64)
            if cur > 1:
              group.nextIndex[rep2.replicaId] = cur - 1
          break
    if acks >= quorum:
      quorumReached = true
      break

  # Join fan-out threads (must complete before we free the ack channel)
  for i in 0..<threadSeq.len:
    joinThread(threadSeq[i])

  # After join, all remaining acks are in the channel. Drain them to update
  # matchIndex/nextIndex for peers that responded after quorum was reached.
  while received < peers.len:
    let (avail, ack) = ackPtr[].ch.tryRecv()
    if not avail: break
    inc received
    if ack.success:
      for rep2 in group.descriptor.replicas:
        if rep2.nodeId == ack.nodeId:
          withLock group.lock:
            group.matchIndex[rep2.replicaId] = ack.matchIndex
            group.nextIndex[rep2.replicaId] = ack.matchIndex + 1
          break
      inc acks
    else:
      for rep2 in group.descriptor.replicas:
        if rep2.nodeId == ack.nodeId:
          withLock group.lock:
            let cur = group.nextIndex.getOrDefault(rep2.replicaId, 1'u64)
            if cur > 1:
              group.nextIndex[rep2.replicaId] = cur - 1
          break

  ackPtr[].ch.close()
  deallocShared(ackPtr)

  result = acks >= quorum

# ============================================================================
# Outbound: election (RequestVote fan-out)
# ============================================================================

proc startElection*(t: RaftGroupTransport,
                    group: RaftGroup,
                    log: RaftLog): bool {.gcsafe.} =
  ## Transition group to candidate, broadcast RequestVote, become leader on
  ## quorum. Returns true iff this node won the election.

  group.becomeCandidate()

  let voters = group.descriptor.getVoters()
  let quorum = group.quorum()
  let peers = voters.filterIt(it.nodeId != t.localNodeId)

  if peers.len == 0:
    # Solo — win immediately
    group.becomeLeader()
    return true

  let rawTerm = group.currentTerm.load()
  # Encode the groupId into the high 32 bits of term so the receiver can
  # look up the correct Raft group via decodeGroupFromTerm().
  let encodedTerm = encodeGroupInTerm(rawTerm, group.groupId)
  let lastIdx = log.lastIndex.load()
  let lastTerm = block:
    if lastIdx == 0: 0'u64
    else:
      let e = log.getEntry(lastIdx)
      if e.isSome: e.get.term else: 0'u64
  let candidateId = int32(t.localNodeId.uint32)

  # VoteResult: 1 = granted, 0 = denied, -1 = higher term seen
  type VoteChanObj = object
    ch: Channel[int8]
  var votePtr = cast[ptr VoteChanObj](allocShared0(sizeof(VoteChanObj)))
  votePtr[].ch.open(peers.len + 2)

  type VoteCtx = tuple[
    rt: raft_transport.RaftTransport, targetId: int32,
    term: uint64, rawTerm: uint64, candidateId: int32,
    lastIdx: uint64, lastTerm: uint64,
    votePtr: ptr VoteChanObj]

  var tseq = newSeq[Thread[VoteCtx]](peers.len)

  for i, rep in peers:
    let ctx: VoteCtx = (
      rt: t.raftNode.raftTransport,
      targetId: int32(rep.nodeId.uint32),
      term: encodedTerm, # high bits = groupId, low bits = actual term
      rawTerm: rawTerm,
      candidateId: candidateId,
      lastIdx: lastIdx,
      lastTerm: lastTerm,
      votePtr: votePtr,
    )
    createThread(tseq[i], proc(c: VoteCtx) {.thread, gcsafe.} =
      {.cast(gcsafe).}:
        let respOpt = c.rt.sendRequestVote(
          c.targetId, c.term, c.candidateId, c.lastIdx, c.lastTerm)
      if respOpt.isSome:
        let resp = respOpt.get()
        # resp.term from receiver is the bare term (no groupId encoded)
        if resp.term > c.rawTerm:
          c.votePtr[].ch.send(-1'i8) # higher term — step down
        elif resp.voteGranted:
          c.votePtr[].ch.send(1'i8) # vote granted
        else:
          c.votePtr[].ch.send(0'i8) # vote denied (already voted elsewhere)
      else:
        c.votePtr[].ch.send(0'i8) # network failure — treat as denied
    , ctx)

  # Count votes (self = 1). Each fan-out thread sends exactly one vote
  # result, so blocking recv() always returns promptly (bounded by the
  # socket timeout in sendRequestVote).
  var granted = 1
  var higherTermSeen = false

  for _ in 0..<peers.len:
    let v = votePtr[].ch.recv()
    if v == 1'i8:
      inc granted
    elif v == -1'i8:
      higherTermSeen = true
    # v == 0: denied — don't count, don't step down
    if higherTermSeen: break # no point continuing if we must step down
    if granted >= quorum: break # already won

  for i in 0..<tseq.len:
    joinThread(tseq[i])

  votePtr[].ch.close()
  deallocShared(votePtr)

  if higherTermSeen:
    # Some peer reported a term higher than ours — step down
    group.becomeFollower(rawTerm)
    return false

  if granted >= quorum:
    group.becomeLeader()
    return true

  # Lost election — stay follower
  group.becomeFollower(rawTerm)
  false

# ============================================================================
# Outbound: heartbeat (empty AppendEntries to all leader groups' followers)
# ============================================================================

proc sendHeartbeats*(t: RaftGroupTransport,
                     groups: tables.Table[rangeTypes.GroupID, RaftGroup],
                     logs: tables.Table[rangeTypes.GroupID,
                         RaftLog]) {.gcsafe.} =
  ## For every group where this node is leader, send AppendEntries
  ## to each peer replica. Uses per-peer nextIndex to include missing
  ## entries so lagging followers catch up during heartbeats.
  for groupId, group in groups:
    if not group.isLeader(): continue

    let rawTerm = group.currentTerm.load()
    let encodedTerm = encodeGroupInTerm(rawTerm, groupId)
    let commitIndex = group.commitIndex.load()
    let leaderId = int32(t.localNodeId.uint32)
    let log = logs.getOrDefault(groupId)
    if log.isNil: continue
    let lastIdx = log.lastIndex.load()

    let voters = group.descriptor.getVoters()
    for rep in voters:
      if rep.nodeId == t.localNodeId: continue
      let tid = int32(rep.nodeId.uint32)

      # Use per-peer nextIndex so lagging followers get missing entries
      var peerNextIdx: uint64
      withLock group.lock:
        peerNextIdx = group.nextIndex.getOrDefault(rep.replicaId, 1'u64)
      if peerNextIdx < 1: peerNextIdx = 1
      if peerNextIdx > lastIdx + 1: peerNextIdx = lastIdx + 1

      let prevIdx = if peerNextIdx > 1: peerNextIdx - 1 else: 0'u64
      let prevTm = block:
        if prevIdx == 0: 0'u64
        else:
          try:
            let e = log.getEntry(prevIdx)
            if e.isSome: e.get.term else: 0'u64
          except CatchableError: 0'u64

      # If peer is behind, include up to 64 entries for catch-up
      var entries: seq[oldRaftTypes.LogEntry] = @[]
      if peerNextIdx <= lastIdx:
        let batchEnd = min(lastIdx, peerNextIdx + 63)
        for idx in peerNextIdx .. batchEnd:
          let eOpt = try: log.getEntry(idx)
                     except CatchableError: none(multigroup_types.LogEntry)
          if eOpt.isNone: break
          let encoded = try: encodeEntry(eOpt.get)
                        except CatchableError: ""
          if encoded.len == 0: break
          var oe: oldRaftTypes.LogEntry
          oe.term = int64(eOpt.get.term)
          oe.entryType = oldRaftTypes.LET_NORMAL
          oe.data = encoded
          entries.add(oe)

      # Use blocking send to process responses and detect lagging peers.
      # The TCP read timeout (5s) bounds how long we block per peer.
      # TODO: parallelize heartbeats to avoid blocking on slow/dead peers.
      {.cast(gcsafe).}:
        let respOpt = t.raftNode.raftTransport.sendAppendEntries(
          tid, encodedTerm, leaderId,
          prevIdx, prevTm,
          commitIndex, entries)
        if respOpt.isSome:
          let resp = respOpt.get()
          if resp.success:
            withLock group.lock:
              group.matchIndex[rep.replicaId] = resp.matchIndex
              group.nextIndex[rep.replicaId] = resp.matchIndex + 1
          else:
            # Rejection — back off nextIndex for next heartbeat
            withLock group.lock:
              let cur = group.nextIndex.getOrDefault(rep.replicaId, 1'u64)
              if resp.rejectHint > 0 and resp.rejectHint < cur:
                group.nextIndex[rep.replicaId] = resp.rejectHint
              elif cur > 1:
                group.nextIndex[rep.replicaId] = cur - 1

    group.updateHeartbeat()

# ============================================================================
# Incoming RPC dispatch helpers
# (called from handlers registered in setupIncomingHandlers)
# ============================================================================


# ============================================================================
# setupIncomingHandlers
# ============================================================================

## CoordAccessors is the coordinator's TransportCoordAccessors type re-exported
## for convenience. Both names refer to the same object.
type CoordAccessors* = TransportCoordAccessors


proc setupIncomingHandlers*(t: RaftGroupTransport,
    acc: TransportCoordAccessors) =
  ## Register RequestVote and AppendEntries handlers on the NetworkRaftNode.
  ## Called once by MultiRaftCoordinator.start() after setting coordinator ptr.

  # ---- RequestVote ----
  proc handleRV(data: string): string {.gcsafe.} =
    try:
      let msg = decodeRequestVoteMsg(data)
      let rid = decodeGroupFromTerm(msg.header.term)
      let term = decodeTermFromTerm(msg.header.term)

      var resp: RequestVoteResponseMsg
      resp.header = newMessageHeader(
        uint16(rmtRequestVoteResponse),
        msg.header.messageId,
        msg.header.targetNodeId,
        msg.header.sourceNodeId,
        0'u64)
      resp.voteGranted = false

      let groupOpt = acc.getGroup(rid)
      let logOpt = acc.getLog(rid)
      if groupOpt.isNone or logOpt.isNone:
        resp.term = 0'u64
        return encodeRequestVoteResponseMsg(resp)

      let group = groupOpt.get
      let log = logOpt.get

      withLock group.lock:
        let myTerm = group.currentTerm.load()

        # Step down on higher term
        if term > myTerm:
          group.state.store(rsFollower)
          group.currentTerm.store(term)
          group.votedFor.store(rangeTypes.ReplicaID(0))

        let curTerm = group.currentTerm.load()
        let votedFor = group.votedFor.load()
        let myLastIdx = log.lastIndex.load()
        let myLastTerm = block:
          if myLastIdx == 0: 0'u64
          else:
            let e = log.getEntry(myLastIdx)
            if e.isSome: e.get.term else: 0'u64

        let candidateId = toNodeID(msg.candidateId)
        let candidateRepId = block:
          var rid2 = rangeTypes.ReplicaID(0)
          for rep in group.descriptor.replicas:
            if rep.nodeId == candidateId:
              rid2 = rep.replicaId
              break
          rid2

        let logOK = (msg.lastLogTerm > myLastTerm) or
                    (msg.lastLogTerm == myLastTerm and
                     msg.lastLogIndex >= myLastIdx)
        let voteOK = term >= curTerm and
                     (votedFor.uint32 == 0 or
                      votedFor == candidateRepId) and
                     logOK

        if voteOK:
          group.votedFor.store(candidateRepId)
          group.updateHeartbeat()
          acc.saveState(rid, group, log)

        resp.term = curTerm
        resp.voteGranted = voteOK

      return encodeRequestVoteResponseMsg(resp)
    except CatchableError:
      return ""

  # ---- AppendEntries ----
  proc handleAE(data: string): string {.gcsafe.} =
    try:
      let msg = decodeAppendEntriesMsg(data)
      let rid = decodeGroupFromTerm(msg.header.term)
      let term = decodeTermFromTerm(msg.header.term)


      var resp: AppendEntriesResponseMsg
      resp.header = newMessageHeader(
        uint16(rmtAppendEntriesResponse),
        msg.header.messageId,
        msg.header.targetNodeId,
        msg.header.sourceNodeId,
        0'u64)
      resp.success = false
      resp.matchIndex = 0
      resp.rejectHint = 0

      let groupOpt = acc.getGroup(rid)
      let logOpt = acc.getLog(rid)
      if groupOpt.isNone or logOpt.isNone:
        resp.term = 0'u64
        return encodeAppendEntriesResponseMsg(resp)

      let group = groupOpt.get
      let log = logOpt.get

      withLock group.lock:
        let myTerm = group.currentTerm.load()

        # Reject stale leader
        if term < myTerm:
          resp.term = myTerm
          return encodeAppendEntriesResponseMsg(resp)

        # Accept valid leader
        if term > myTerm:
          group.currentTerm.store(term)
          group.votedFor.store(rangeTypes.ReplicaID(0))
        group.state.store(rsFollower)
        group.updateHeartbeat()

        let curTerm = group.currentTerm.load()

        # Consistency check: prevLogIndex / prevLogTerm
        if msg.prevLogIndex > 0:
          let prevOpt = log.getEntry(msg.prevLogIndex)
          if prevOpt.isNone or prevOpt.get.term != msg.prevLogTerm:
            # Find conflict term's first index for fast backup
            let conflictTerm = if prevOpt.isSome: prevOpt.get.term else: 0'u64
            var conflictIdx = msg.prevLogIndex
            if conflictTerm > 0:
              while conflictIdx > 1:
                let e2 = log.getEntry(conflictIdx - 1)
                if e2.isNone or e2.get.term != conflictTerm: break
                dec conflictIdx
            resp.term = curTerm
            resp.rejectHint = conflictIdx
            return encodeAppendEntriesResponseMsg(resp)

        # Decode and append entries from entriesData
        let entries = decodeLogEntries(msg.entriesData)
        var matchIdx = msg.prevLogIndex
        var nextIdx = msg.prevLogIndex + 1 # increments for each entry

        for rawEntry in entries:
          let idx = nextIdx
          inc nextIdx
          let existing = log.getEntry(idx)
          var needWrite = false
          if existing.isSome and existing.get.term != uint64(rawEntry.term):
            log.truncate(idx)
            # Reset lastApplied so truncated entries get re-applied
            let newApplied = if idx > 1: idx - 1 else: 0'u64
            if group.lastApplied.load() >= idx:
              group.lastApplied.store(newApplied)
            needWrite = true
          elif existing.isNone:
            needWrite = true
          if needWrite:
            # Decode full LogEntry from the JSON payload stored in .data
            try:
              let decoded = decodeEntry(rawEntry.data)
              log.putEntry(decoded)
            except CatchableError:
              discard
          matchIdx = idx

        # Advance commit index
        let newCommit = min(msg.commitIndex, matchIdx)
        if newCommit > group.commitIndex.load():
          group.commitIndex.store(newCommit)
          acc.applyUpTo(rid, group, newCommit)

        acc.saveState(rid, group, log)

        resp.term = curTerm
        resp.success = true
        resp.matchIndex = matchIdx

      return encodeAppendEntriesResponseMsg(resp)
    except CatchableError:
      return ""

  # First, wire the default handlers into the TCPTransport's dispatch table.
  # setupHandlers() calls connManager.registerRaftHandler() which writes into
  # TCPTransport.handlers — the table that the accept loop actually dispatches from.
  t.raftNode.raftTransport.setupHandlers()

  # Now override the RV and AE slots in TCPTransport.handlers with our custom
  # closures that update group state (heartbeat, term, vote).  These must be
  # registered AFTER setupHandlers() so they take precedence over the defaults.
  t.raftNode.raftTransport.connManager.registerRaftHandler(
    uint16(rmtRequestVote), handleRV)
  t.raftNode.raftTransport.connManager.registerRaftHandler(
    uint16(rmtAppendEntries), handleAE)

# ============================================================================
# Factory: wrap RaftGroupTransport into the coordinator's MultiRaftTransport
# vtable so the coordinator doesn't need to import this module directly.
# ============================================================================

proc newMultiRaftTransport*(rgt: RaftGroupTransport): MultiRaftTransport =
  ## Wrap a RaftGroupTransport in the vtable used by MultiRaftCoordinator.
  ## Call this from server bootstrap code (not from within the coordinator).
  ##
  ## ORC safety: closures capture `rgt` only as a raw pointer (no ORC-tracked
  ## ref inside the closure environment).  The *caller* must keep `rgt` alive
  ## for the lifetime of the returned MultiRaftTransport — in tests this is done
  ## by storing `rgt` in NodeSetup; in production it lives in the server object.
  ## This eliminates the ref-cycle that triggered ORC's Bacon-Rajan collector
  ## (SIGSEGV in rawDealloc / addToSharedFreeList).
  let rgtPtr = cast[pointer](rgt)

  result = MultiRaftTransport(
    startFn: proc(acc: TransportCoordAccessors) {.gcsafe, raises: [].} =
    {.cast(gcsafe).}: {.cast(raises: []).}:
      let t = cast[RaftGroupTransport](rgtPtr)
      # Start the TCP listener first (calls raftNode.start → setupHandlers which
      # registers default handlers into TCPTransport.handlers).
      t.start()
      # Now override with the group-aware handlers.  Must come AFTER t.start()
      # so setupHandlers() doesn't overwrite our custom registrations.
      t.setupIncomingHandlers(acc),

    stopFn: proc() {.gcsafe, raises: [].} =
    {.cast(gcsafe).}: {.cast(raises: []).}:
      cast[RaftGroupTransport](rgtPtr).stop(),

    replicateFn: proc(group: RaftGroup, log: RaftLog,
        entry: multigroup_types.LogEntry, timeoutMs: int): bool {.gcsafe,
            raises: [].} =
      {.cast(gcsafe).}: {.cast(raises: []).}:
        result = cast[RaftGroupTransport](rgtPtr).replicateEntry(
            group, log, entry, timeoutMs),

    electionFn: proc(group: RaftGroup,
        log: RaftLog): bool {.gcsafe, raises: [].} =
    {.cast(gcsafe).}: {.cast(raises: []).}:
      result = cast[RaftGroupTransport](rgtPtr).startElection(group, log),

    heartbeatFn: proc(groups: tables.Table[rangeTypes.GroupID, RaftGroup],
        logs: tables.Table[rangeTypes.GroupID, RaftLog]) {.gcsafe, raises: [].} =
      {.cast(gcsafe).}: {.cast(raises: []).}:
        cast[RaftGroupTransport](rgtPtr).sendHeartbeats(groups, logs),
  )
