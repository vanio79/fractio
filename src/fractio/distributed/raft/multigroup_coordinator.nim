# Multi-Raft Coordinator
#
# This module manages multiple Raft groups on a single node.
# It handles proposal routing, worker threads, and group lifecycle.
#
# Fixed for Nim 2.2.8:
#   - Replaced std/channels (nonexistent) with built-in Channel[T]
#   - Replaced RwLock (nonexistent) with Lock
#   - Replaced Future-based propose with synchronous proposeAndWait
#   - Removed asyncdispatch dependency

import std/atomics
import std/locks
import std/tables
import std/sets
import std/typedthreads
import std/times
import std/options
import os # for sleep()

import fractio/distributed/range/types
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/multigroup_log
import fractio/storage/backend
import fractio/storage/wisckey_backend
import fractio/utils/logging

# ============================================================================
# Configuration
# ============================================================================

const
  DEFAULT_NUM_WORKERS* = 4
  DEFAULT_ELECTION_TIMEOUT_NS* = 1_500_000_000'i64 # 1.5 seconds
  DEFAULT_HEARTBEAT_INTERVAL_NS* = 500_000_000'i64 # 500ms
  MAX_PROPOSAL_QUEUE_SIZE* = 10000

# ============================================================================
# Coordinator Types
# ============================================================================

type
  CoordinatorConfig* = object
    ## Configuration for the coordinator
    nodeId*: NodeID
    numWorkers*: int
    electionTimeoutNs*: int64
    heartbeatIntervalNs*: int64
    storagePath*: string

  WorkerContext = object
    ## Context passed to worker threads
    coordinator: MultiRaftCoordinator
    workerId: int

  MultiRaftCoordinator* = ref object
    ## Manages all Raft groups on a single node
    nodeId*: NodeID
    config*: CoordinatorConfig

    # Group management
    groups*: Table[RangeID, RaftGroup]
    logs*: Table[RangeID, RaftLog]
    groupsLock*: Lock ## replaced RwLock with plain Lock

    # Storage
    store*: WiscKeyBackend

    # Proposal handling: Channel is a built-in type in Nim (no import needed)
    proposalCh*: Channel[Proposal]
    pendingProposals*: Table[uint64, Proposal]
    proposalIdCounter*: Atomic[uint64]

    # Worker threads
    workers*: seq[Thread[WorkerContext]]
    running*: Atomic[bool]

    # Timing
    electionTimeoutNs*: int64
    heartbeatIntervalNs*: int64

# ============================================================================
# Coordinator Lifecycle
# ============================================================================

proc newMultiRaftCoordinator*(config: CoordinatorConfig): MultiRaftCoordinator =
  ## Create a new multi-raft coordinator
  new(result)
  result.nodeId = config.nodeId
  result.config = config
  result.electionTimeoutNs = config.electionTimeoutNs
  result.heartbeatIntervalNs = config.heartbeatIntervalNs

  # Initialize storage
  result.store = newWiscKeyBackend(StorageConfig(
    path: config.storagePath,
    createIfMissing: true,
    syncWrites: true
  ))

  if not result.store.open(StorageConfig(
    path: config.storagePath,
    createIfMissing: true,
    syncWrites: true
  )):
    raise newException(MultiRaftError, "Failed to open storage backend")

  # Initialize data structures
  result.groups = initTable[RangeID, RaftGroup]()
  result.logs = initTable[RangeID, RaftLog]()
  result.pendingProposals = initTable[uint64, Proposal]()
  result.proposalIdCounter.store(0)

  # Initialize synchronization: Channel[T].open() initialises the channel
  initLock(result.groupsLock)
  result.proposalCh.open(MAX_PROPOSAL_QUEUE_SIZE)

  # Initialize workers
  result.workers = newSeq[Thread[WorkerContext]](config.numWorkers)
  result.running.store(false)

proc workerProc(ctx: WorkerContext) {.thread.} # forward declaration

proc start*(c: MultiRaftCoordinator) =
  ## Start the coordinator and worker threads
  if c.running.load:
    return

  c.running.store(true)

  # Start worker threads
  for i in 0..<c.config.numWorkers:
    let ctx = WorkerContext(coordinator: c, workerId: i)
    createThread(c.workers[i], workerProc, ctx)

  {.cast(gcsafe).}:
    var fields = initTable[string, string]()
    fields["nodeId"] = $c.nodeId
    fields["numWorkers"] = $c.config.numWorkers
    info("Multi-Raft coordinator started", fields)

proc stop*(c: MultiRaftCoordinator) =
  ## Stop the coordinator
  if not c.running.load:
    return

  c.running.store(false)

  # Send shutdown signals to workers (one per worker).
  # rangeId == 0 is used as a shutdown sentinel; resultPtr is nil (no reply needed).
  for i in 0..<c.workers.len:
    c.proposalCh.send(Proposal(
      rangeId: RangeID(0),
      command: RaftCommand(kind: ckNoop),
      resultPtr: nil,
    ))

  # Wait for workers to finish
  for i in 0..<c.workers.len:
    joinThread(c.workers[i])

  # Close the channel
  c.proposalCh.close()

  # Close all groups
  withLock c.groupsLock:
    for group in c.groups.values:
      group.close()
    for log in c.logs.values:
      log.close()

  # Close storage
  c.store.close()

  {.cast(gcsafe).}:
    var fields = initTable[string, string]()
    fields["nodeId"] = $c.nodeId
    info("Multi-Raft coordinator stopped", fields)

# ============================================================================
# Group Management
# ============================================================================

proc createGroup*(c: MultiRaftCoordinator, descriptor: RangeDescriptor,
                   replicaId: ReplicaID): RaftGroup =
  ## Create a new Raft group for a range
  withLock c.groupsLock:
    if c.groups.hasKey(descriptor.rangeId):
      raise newException(MultiRaftError, "Group already exists: " &
          $descriptor.rangeId)

    # Create log storage
    let log = newRaftLog(descriptor.rangeId, c.store)
    log.recoverLog()
    c.logs[descriptor.rangeId] = log

    # Create group
    let group = newRaftGroup(descriptor.rangeId, c.nodeId, replicaId, descriptor)
    c.groups[descriptor.rangeId] = group

    # Load persistent state
    let state = log.loadState()
    if state.isSome:
      group.currentTerm.store(state.get.currentTerm)
      group.votedFor.store(state.get.votedFor)
      group.commitIndex.store(state.get.commitIndex)
      group.lastApplied.store(state.get.lastApplied)

    {.cast(gcsafe).}:
      var fields = initTable[string, string]()
      fields["rangeId"] = $descriptor.rangeId
      fields["replicaId"] = $replicaId
      info("Created Raft group", fields)

    result = group

proc removeGroup*(c: MultiRaftCoordinator, rangeId: RangeID) =
  ## Remove a Raft group
  withLock c.groupsLock:
    if c.groups.hasKey(rangeId):
      let group = c.groups[rangeId]
      group.close()
      c.groups.del(rangeId)

      let log = c.logs[rangeId]
      log.close()
      c.logs.del(rangeId)

      {.cast(gcsafe).}:
        var fields = initTable[string, string]()
        fields["rangeId"] = $rangeId
        info("Removed Raft group", fields)

proc getGroup*(c: MultiRaftCoordinator, rangeId: RangeID): Option[RaftGroup] =
  ## Get a Raft group by range ID
  withLock c.groupsLock:
    if c.groups.hasKey(rangeId):
      result = some(c.groups[rangeId])

proc hasGroup*(c: MultiRaftCoordinator, rangeId: RangeID): bool =
  ## Check if a group exists
  withLock c.groupsLock:
    result = c.groups.hasKey(rangeId)

# ============================================================================
# Proposal Handling (synchronous — no asyncdispatch)
# ============================================================================

proc proposeAndWait*(c: MultiRaftCoordinator, rangeId: RangeID,
                     command: RaftCommand, timeoutMs: int = 5000): RaftResult =
  ## Propose a command to a Raft group and block until the result is available.
  ##
  ## Completion is signalled via a ProposalResultChannel allocated on the heap
  ## and shared with the worker thread via a raw pointer.  Using a raw pointer
  ## (instead of a GC ref inside a closure) avoids ORC cross-thread cycle
  ## tracking, which causes SIGSEGV in Nim 2.2.8.
  var prc = cast[ptr ProposalResultChannel](
    allocShared0(sizeof(ProposalResultChannel)))
  prc[].ch.open(1)

  let proposal = Proposal(
    rangeId: rangeId,
    command: command,
    resultPtr: prc,
  )

  c.proposalCh.send(proposal)

  # Busy-wait with timeout
  let deadline = getTime().toUnix * 1000 + timeoutMs
  while true:
    let (avail, res) = prc[].ch.tryRecv()
    if avail:
      prc[].ch.close()
      deallocShared(prc)
      return res
    if getTime().toUnix * 1000 >= deadline:
      prc[].ch.close()
      deallocShared(prc)
      return RaftResult(success: false, error: "Timeout waiting for proposal")
    sleep(1)

# ============================================================================
# Worker Thread
# ============================================================================

proc sendResult(p: ptr ProposalResultChannel, r: RaftResult) {.inline.} =
  ## Send the result to the waiting caller via the raw-pointer channel.
  ## The caller owns the ProposalResultChannel and frees it after recv.
  if p != nil:
    p[].ch.send(r)

proc workerProc(ctx: WorkerContext) {.thread.} =
  ## Worker thread that processes proposals
  let c = ctx.coordinator

  while c.running.load:
    let proposal = c.proposalCh.recv()
    if proposal.rangeId.uint64 == 0:
      break # Shutdown sentinel

    try:
      # Get the group
      let groupOpt = c.getGroup(proposal.rangeId)
      if groupOpt.isNone:
        sendResult(proposal.resultPtr, RaftResult(
          success: false,
          error: "Range not found: " & $proposal.rangeId
        ))
        continue

      let group = groupOpt.get

      # Check if we're the leader
      if not group.isLeader():
        sendResult(proposal.resultPtr, RaftResult(
          success: false,
          error: "Not the leader"
        ))
        continue

      # Append to log
      withLock c.groupsLock:
        let log = c.logs[proposal.rangeId]
        let term = group.getTerm()
        let index = log.lastIndex.load + 1

        let entry = newLogEntry(term, index, proposal.command)
        log.putEntry(entry)

        # Update commit index (simplified — single-node quorum)
        group.commitIndex.store(index)

        # Signal completion to the waiting caller
        sendResult(proposal.resultPtr, RaftResult(success: true, index: index))

    except CatchableError as e:
      try:
        sendResult(proposal.resultPtr, RaftResult(success: false, error: e.msg))
      except CatchableError: discard

# ============================================================================
# Election and Heartbeat
# ============================================================================

proc checkElectionTimeout*(c: MultiRaftCoordinator) =
  ## Check all groups for election timeout
  withLock c.groupsLock:
    for group in c.groups.values:
      if group.state.load == rsLeader:
        continue

      let timeSince = group.timeSinceHeartbeat()
      if timeSince > c.electionTimeoutNs:
        # Start election
        group.becomeCandidate()

        var fields = initTable[string, string]()
        fields["rangeId"] = $group.rangeId
        fields["term"] = $group.getTerm()
        debug("Starting election", fields)

proc sendHeartbeats*(c: MultiRaftCoordinator) =
  ## Send heartbeats for all leader groups
  withLock c.groupsLock:
    for group in c.groups.values:
      if group.state.load != rsLeader:
        continue

      # In a real implementation, send AppendEntries RPCs
      # For now, just update the heartbeat time
      group.updateHeartbeat()

# ============================================================================
# Utility
# ============================================================================

proc getLeaderCount*(c: MultiRaftCoordinator): int =
  ## Count groups where this node is leader
  withLock c.groupsLock:
    for group in c.groups.values:
      if group.isLeader():
        inc result

proc getGroupCount*(c: MultiRaftCoordinator): int =
  ## Get total number of groups
  withLock c.groupsLock:
    result = c.groups.len
