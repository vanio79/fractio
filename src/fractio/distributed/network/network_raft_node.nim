# Network Raft Node - Raft node with network transport
# Part of the network transport layer for distributed Fractio

import std/[tables, locks, atomics, options, times, threadpool, os]
import ./types
import ./raft_transport
import ./connection_manager
import ./config
import ./serialization
import ../raft/types as raft_types
import ../../core/types as coretypes
import ../../utils/logging

# =============================================================================
# Network Raft Node Types
# =============================================================================

type
  NetworkRaftNode* = ref object
    ## Raft node with network transport capabilities
    serverId*: int32
    nodeState*: raft_types.RaftNodeState
    raftTransport*: RaftTransport
    connManager*: ConnectionManager
    config*: raft_types.RaftConfig
    netConfig*: NetworkConfig
    running*: Atomic[bool]
    lastHeartbeat*: int64
    lastHeartbeatLock*: Lock

    # For tracking votes
    votesReceived*: tables.Table[int32, bool]
    votesLock*: Lock

    # For tracking match indices
    matchIndex*: tables.Table[int32, uint64]
    nextIndex*: tables.Table[int32, uint64]
    indicesLock*: Lock

    # Election timer state
    electionResetEvent*: int64 # Timestamp when we last reset election timer
    electionResetLock*: Lock

# =============================================================================
# Network Raft Node Implementation
# =============================================================================

proc newNetworkRaftNode*(raftConfig: raft_types.RaftConfig,
                         netConfig: NetworkConfig): NetworkRaftNode =
  ## Create a new network-enabled Raft node
  result = NetworkRaftNode(
    serverId: raftConfig.serverId,
    config: raftConfig,
    netConfig: netConfig,
    nodeState: raft_types.RaftNodeState(
      role: SR_FOLLOWER,
      currentTerm: 0,
      votedFor: -1,
      leaderId: -1,
      commitIndex: 0,
      lastApplied: 0
    ),
    running: Atomic[bool](),
    votesReceived: tables.initTable[int32, bool](),
    matchIndex: tables.initTable[int32, uint64](),
    nextIndex: tables.initTable[int32, uint64]()
  )
  initLock(result.votesLock)
  initLock(result.indicesLock)
  initLock(result.lastHeartbeatLock)
  initLock(result.electionResetLock)

  # Create connection manager
  result.connManager = newConnectionManager(netConfig)

  # Create Raft transport
  result.raftTransport = newRaftTransport(result.connManager,
      raftConfig.serverId)

proc close*(node: NetworkRaftNode) =
  ## Close the network Raft node
  node.running.store(false)

  node.raftTransport.close()
  node.connManager.close()

  deinitLock(node.votesLock)
  deinitLock(node.indicesLock)
  deinitLock(node.lastHeartbeatLock)
  deinitLock(node.electionResetLock)

# =============================================================================
# Node Registry Management
# =============================================================================

proc addPeer*(node: NetworkRaftNode, serverId: int32, host: string,
              raftPort: int, clientPort: int, adminPort: int) =
  ## Add a peer node to the registry
  var info: connection_manager.NodeInfo
  info.nodeId = toNodeID(serverId)
  info.host = host
  info.raftPort = raftPort
  info.clientPort = clientPort
  info.adminPort = adminPort
  info.isLocal = false

  node.connManager.registerNode(info)

  # Initialize tracking for this peer
  withLock node.indicesLock:
    node.nextIndex[serverId] = 1'u64
    node.matchIndex[serverId] = 0'u64

proc removePeer*(node: NetworkRaftNode, serverId: int32) =
  ## Remove a peer node from the registry
  node.connManager.unregisterNode(toNodeID(serverId))

  withLock node.indicesLock:
    node.nextIndex.del(serverId)
    node.matchIndex.del(serverId)

# =============================================================================
# Election Timer
# =============================================================================

# Forward declarations for procedures used in election timer
proc becomeCandidate*(node: NetworkRaftNode)
proc becomeLeader*(node: NetworkRaftNode)
proc recordVote*(node: NetworkRaftNode, voterId: int32, granted: bool): bool
proc sendHeartbeat*(node: NetworkRaftNode)

proc resetElectionTimer*(node: NetworkRaftNode) =
  ## Reset the election timer (called when receiving heartbeat from leader)
  withLock node.electionResetLock:
    node.electionResetEvent = getTime().toUnix() * 1000 + getTime().nanosecond() div 1_000_000

proc getLastResetTime*(node: NetworkRaftNode): int64 =
  ## Get the last time the election timer was reset (in ms)
  withLock node.electionResetLock:
    result = node.electionResetEvent

proc checkElectionTimeout*(node: NetworkRaftNode): bool =
  ## Check if election timeout has occurred
  let nowMs = getTime().toUnix() * 1000 + getTime().nanosecond() div 1_000_000
  let lastReset = node.getLastResetTime()
  let elapsed = nowMs - lastReset

  # Use randomized timeout to avoid split votes
  let timeoutMs = node.config.electionTimeout + (node.serverId * 10 mod 50)
  result = elapsed > timeoutMs

proc electionTimerLoop(node: NetworkRaftNode) =
  ## Background thread that handles election timeouts and heartbeats
  while node.running.load(moRelaxed):
    let currentRole = node.nodeState.role

    case currentRole
    of SR_FOLLOWER:
      # Check for election timeout
      if node.checkElectionTimeout():
        # Start election
        node.becomeCandidate()
        # Send RequestVote to all peers and handle responses
        let term = uint64(node.nodeState.currentTerm)
        let nodes = node.connManager.getRemoteNodes()
        for nodeInfo in nodes:
          let targetServerId = toServerId(nodeInfo.nodeId)
          if targetServerId > 0:
            let respOpt = node.raftTransport.sendRequestVote(
              targetServerId, term, node.serverId, 0'u64, 0'u64)
            if respOpt.isSome:
              let resp = respOpt.get()
              if resp.voteGranted and node.nodeState.role == SR_CANDIDATE:
                if node.recordVote(targetServerId, true):
                  node.becomeLeader()
                  break
              elif resp.term > uint64(node.nodeState.currentTerm):
                node.nodeState.currentTerm = int64(resp.term)
                node.nodeState.role = SR_FOLLOWER
                node.nodeState.votedFor = -1
        node.resetElectionTimer()

    of SR_CANDIDATE:
      # Check for election timeout (no leader elected)
      if node.checkElectionTimeout():
        # Restart election
        node.becomeCandidate()
        let term = uint64(node.nodeState.currentTerm)
        let nodes = node.connManager.getRemoteNodes()
        for nodeInfo in nodes:
          let targetServerId = toServerId(nodeInfo.nodeId)
          if targetServerId > 0:
            let respOpt = node.raftTransport.sendRequestVote(
              targetServerId, term, node.serverId, 0'u64, 0'u64)
            if respOpt.isSome:
              let resp = respOpt.get()
              if resp.voteGranted and node.nodeState.role == SR_CANDIDATE:
                if node.recordVote(targetServerId, true):
                  node.becomeLeader()
                  break
              elif resp.term > uint64(node.nodeState.currentTerm):
                node.nodeState.currentTerm = int64(resp.term)
                node.nodeState.role = SR_FOLLOWER
                node.nodeState.votedFor = -1
        node.resetElectionTimer()

    of SR_LEADER:
      # Send heartbeats
      node.sendHeartbeat()

    # Sleep for a short interval
    sleep(20)

proc startElectionTimer*(node: NetworkRaftNode) =
  ## Start the election timer background thread
  node.resetElectionTimer()
  spawn electionTimerLoop(node)

# =============================================================================
# Start/Stop
# =============================================================================

proc start*(node: NetworkRaftNode): bool =
  ## Start the network Raft node
  if not node.connManager.start():
    return false

  # Set up message handlers
  node.raftTransport.setupHandlers()

  # Register custom handlers that bridge to Raft node.
  # Use a raw pointer to `node` inside the closures to break the ORC cycle:
  #   NetworkRaftNode → raftTransport → connManager → TCPTransport.handlers
  #     → closure environment → NetworkRaftNode   (cycle!)
  # With a raw pointer the closure environment holds no traced ref, so ORC
  # does not see a cycle and does not crash during collectCycles.
  # Safety: the handlers are removed (connManager closed) before `node` is
  # freed, so the raw pointer is always valid while the handlers are active.
  let nodePtr = cast[pointer](node)

  proc handleRV(data: string): string {.gcsafe.} =
    let n = cast[NetworkRaftNode](nodePtr)
    let msg = decodeRequestVoteMsg(data)
    var resp: RequestVoteResponseMsg
    resp.header = newMessageHeader(uint16(rmtRequestVoteResponse),
                                    msg.header.messageId,
                                    msg.header.targetNodeId,
                                    msg.header.sourceNodeId,
                                    uint64(n.nodeState.currentTerm))

    # Simplified vote logic
    let term = n.nodeState.currentTerm
    if msg.header.term > uint64(term):
      n.nodeState.currentTerm = int64(msg.header.term)
      n.nodeState.votedFor = -1

    if msg.header.term >= uint64(term) and
       (n.nodeState.votedFor == -1 or
        n.nodeState.votedFor == toServerId(msg.candidateId)):
      n.nodeState.votedFor = toServerId(msg.candidateId)
      resp.voteGranted = true
    else:
      resp.voteGranted = false

    resp.term = uint64(n.nodeState.currentTerm)
    result = encodeRequestVoteResponseMsg(resp)

  proc handleAE(data: string): string {.gcsafe.} =
    let n = cast[NetworkRaftNode](nodePtr)
    let msg = decodeAppendEntriesMsg(data)
    var resp: AppendEntriesResponseMsg
    resp.header = newMessageHeader(uint16(rmtAppendEntriesResponse),
                                    msg.header.messageId,
                                    msg.header.targetNodeId,
                                    msg.header.sourceNodeId,
                                    uint64(n.nodeState.currentTerm))

    let term = n.nodeState.currentTerm
    if msg.header.term > uint64(term):
      n.nodeState.currentTerm = int64(msg.header.term)
      n.nodeState.role = SR_FOLLOWER
      n.nodeState.votedFor = -1

    if msg.header.term < uint64(term):
      resp.success = false
      resp.term = uint64(term)
    else:
      # Accept entries / heartbeat from leader
      n.nodeState.leaderId = toServerId(msg.leaderId)
      n.lastHeartbeat = getTime().toUnix()

      # Reset election timer since we got a valid heartbeat from leader
      n.resetElectionTimer()

      # Update commit index
      if msg.commitIndex > uint64(n.nodeState.commitIndex):
        n.nodeState.commitIndex = int64(min(msg.commitIndex,
          msg.prevLogIndex + uint64(msg.numEntries)))

      resp.success = true
      resp.term = uint64(n.nodeState.currentTerm)
      resp.matchIndex = msg.prevLogIndex + uint64(msg.numEntries)

    resp.rejectHint = 0'u64
    result = encodeAppendEntriesResponseMsg(resp)

  node.raftTransport.registerHandler(uint16(rmtRequestVote), handleRV)
  node.raftTransport.registerHandler(uint16(rmtAppendEntries), handleAE)
  # Note: rmtRequestVoteResponse handler removed - automatic election disabled
  # For benchmarks, use manual leader election via becomeCandidate()/becomeLeader()

  node.running.store(true)

  # Initialize election timer state (automatic election disabled for GC-safety)
  # For benchmarks, use manual leader election via becomeCandidate()/becomeLeader()
  node.resetElectionTimer()
  # node.startElectionTimer()  # Disabled due to GC-safety complexity

  var fields = initTable[string, string]()
  fields["serverId"] = $node.serverId
  info("Network Raft node started", fields)
  return true

proc stop*(node: NetworkRaftNode) =
  ## Stop the network Raft node
  node.running.store(false)
  node.connManager.stop()

  var fields = initTable[string, string]()
  fields["serverId"] = $node.serverId
  info("Network Raft node stopped", fields)

# =============================================================================
# Raft Operations
# =============================================================================

proc becomeCandidate*(node: NetworkRaftNode) =
  ## Transition to candidate state
  node.nodeState.role = SR_CANDIDATE
  inc node.nodeState.currentTerm
  node.nodeState.votedFor = node.serverId

  withLock node.votesLock:
    node.votesReceived.clear()
    node.votesReceived[node.serverId] = true

  var fields = initTable[string, string]()
  fields["serverId"] = $node.serverId
  fields["term"] = $node.nodeState.currentTerm
  info("Became candidate", fields)

proc becomeLeader*(node: NetworkRaftNode) =
  ## Transition to leader state
  if node.nodeState.role != SR_CANDIDATE:
    return

  node.nodeState.role = SR_LEADER
  node.nodeState.leaderId = node.serverId

  var fields = initTable[string, string]()
  fields["serverId"] = $node.serverId
  fields["term"] = $node.nodeState.currentTerm
  info("Became leader", fields)

proc becomeFollower*(node: NetworkRaftNode, term: int64) =
  ## Transition to follower state
  node.nodeState.role = SR_FOLLOWER
  node.nodeState.currentTerm = term
  node.nodeState.votedFor = -1

  var fields = initTable[string, string]()
  fields["serverId"] = $node.serverId
  fields["term"] = $term
  info("Became follower", fields)

proc sendRequestVote*(node: NetworkRaftNode) =
  ## Send RequestVote to all peers
  let term = uint64(node.nodeState.currentTerm)

  let nodes = node.connManager.getRemoteNodes()
  for nodeInfo in nodes:
    let targetServerId = toServerId(nodeInfo.nodeId)
    if targetServerId > 0:
      discard node.raftTransport.sendRequestVote(
        targetServerId, term, node.serverId, 0'u64, 0'u64)

proc sendHeartbeat*(node: NetworkRaftNode) =
  ## Send heartbeat (empty AppendEntries) to all peers
  let term = uint64(node.nodeState.currentTerm)
  let commitIdx = uint64(node.nodeState.commitIndex)

  let nodes = node.connManager.getRemoteNodes()
  for nodeInfo in nodes:
    let targetServerId = toServerId(nodeInfo.nodeId)
    if targetServerId > 0:
      var nextIdx: uint64 = 1
      withLock node.indicesLock:
        if targetServerId in node.nextIndex:
          nextIdx = node.nextIndex[targetServerId]

      discard node.raftTransport.sendAppendEntries(
        targetServerId, term, node.serverId,
        nextIdx - 1, 0'u64, commitIdx, @[])

proc recordVote*(node: NetworkRaftNode, voterId: int32, granted: bool): bool =
  ## Record a vote and check if we have majority
  withLock node.votesLock:
    node.votesReceived[voterId] = granted

  # Count votes
  var yesVotes = 0
  var totalVotes = 0
  withLock node.votesLock:
    for id, voted in node.votesReceived:
      inc totalVotes
      if voted:
        inc yesVotes

  # Check majority
  let nodes = node.connManager.getAllNodes()
  let majority = (nodes.len div 2) + 1

  result = yesVotes >= majority

# =============================================================================
# Status
# =============================================================================

proc isLeader*(node: NetworkRaftNode): bool =
  ## Check if this node is the leader
  result = node.nodeState.role == SR_LEADER

proc isCandidate*(node: NetworkRaftNode): bool =
  ## Check if this node is a candidate
  result = node.nodeState.role == SR_CANDIDATE

proc isFollower*(node: NetworkRaftNode): bool =
  ## Check if this node is a follower
  result = node.nodeState.role == SR_FOLLOWER

proc getTerm*(node: NetworkRaftNode): int64 =
  ## Get current term
  result = node.nodeState.currentTerm

proc getRole*(node: NetworkRaftNode): ServerRole =
  ## Get current role
  result = node.nodeState.role

proc getCommitIndex*(node: NetworkRaftNode): int64 =
  ## Get current commit index
  result = node.nodeState.commitIndex

proc getLeaderId*(node: NetworkRaftNode): int32 =
  ## Get current leader ID
  result = node.nodeState.leaderId
