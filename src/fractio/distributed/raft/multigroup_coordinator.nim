# Multi-Raft Coordinator
#
# This module manages multiple Raft groups on a single node.
# It handles proposal routing, worker threads, and group lifecycle.

import std/atomics
import std/locks
import std/tables
import std/sets
import std/channels
import std/typedthreads
import std/times
import std/options

import fractio/distributed/range/types
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/multigroup_log
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
    groupsLock*: RwLock

    # Storage
    store*: WiscKeyBackend

    # Proposal handling
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

  # Initialize synchronization
  initLock(result.groupsLock)
  result.proposalCh = newChannel[Proposal](MAX_PROPOSAL_QUEUE_SIZE)

  # Initialize workers
  result.workers = newSeq[Thread[WorkerContext]](config.numWorkers)
  result.running.store(false)

proc start*(c: MultiRaftCoordinator) =
  ## Start the coordinator and worker threads
  if c.running.load:
    return

  c.running.store(true)

  # Start worker threads
  for i in 0..<c.config.numWorkers:
    let ctx = WorkerContext(coordinator: c, workerId: i)
    createThread(c.workers[i], workerProc, ctx)

  var fields = initTable[string, string]()
  fields["nodeId"] = $c.nodeId
  fields["numWorkers"] = $c.config.numWorkers
  info("Multi-Raft coordinator started", fields)

proc stop*(c: MultiRaftCoordinator) =
  ## Stop the coordinator
  if not c.running.load:
    return

  c.running.store(false)

  # Close channel to unblock workers
  c.proposalCh.close()

  # Wait for workers to finish
  for worker in c.workers:
    joinThread(worker)

  # Close all groups
  withLock c.groupsLock:
    for group in c.groups.values:
      group.close()
    for log in c.logs.values:
      log.close()

  # Close storage
  c.store.close()

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
# Proposal Handling
# ============================================================================

proc propose*(c: MultiRaftCoordinator, rangeId: RangeID,
              command: RaftCommand): Future[RaftResult] =
  ## Propose a command to a Raft group
  let proposal = Proposal(
    rangeId: rangeId,
    command: command,
    callback: nil
  )

  # Create a future for the result
  var future = newFuture[RaftResult]()
  proposal.callback = proc(result: RaftResult) =
    future.complete(result)

  # Send to proposal channel
  c.proposalCh.send(proposal)

  return future

proc proposeAndWait*(c: MultiRaftCoordinator, rangeId: RangeID,
                     command: RaftCommand, timeoutMs: int = 5000): RaftResult =
  ## Propose a command and wait for result
  let future = c.propose(rangeId, command)

  # Wait with timeout
  let startTime = getTime().toUnix * 1000
  while not future.finished:
    let elapsed = (getTime().toUnix * 1000) - startTime
    if elapsed > timeoutMs:
      return RaftResult(success: false, error: "Timeout waiting for proposal")
    sleep(10)

  if future.completed:
    result = future.read
  else:
    result = RaftResult(success: false, error: "Proposal failed")

# ============================================================================
# Worker Thread
# ============================================================================

proc workerProc(ctx: WorkerContext) {.thread.} =
  ## Worker thread that processes proposals
  let c = ctx.coordinator

  var fields = initTable[string, string]()
  fields["workerId"] = $ctx.workerId
  debug("Worker thread started", fields)

  while c.running.load:
    try:
      let proposal = c.proposalCh.recv()
      if proposal.rangeId.uint64 == 0:
        break # Shutdown signal
      
      # Get the group
      let groupOpt = c.getGroup(proposal.rangeId)
      if groupOpt.isNone:
        proposal.callback(RaftResult(
          success: false,
          error: "Range not found: " & $proposal.rangeId
        ))
        continue

      let group = groupOpt.get

      # Check if we're the leader
      if not group.isLeader():
        proposal.callback(RaftResult(
          success: false,
          error: "Not the leader"
        ))
        continue

      # Append to log
      let log = c.logs[proposal.rangeId]
      let term = group.getTerm()
      let index = log.lastIndex.load + 1

      let entry = newLogEntry(term, index, proposal.command)
      log.putEntry(entry)

      # Update commit index (simplified - in real implementation, wait for quorum)
      group.commitIndex.store(index)

      # Complete the proposal
      proposal.callback(RaftResult(
        success: true,
        index: index
      ))

    except CatchableError as e:
      var errFields = initTable[string, string]()
      errFields["workerId"] = $ctx.workerId
      errFields["error"] = e.msg
      error("Worker error", errFields)

  fields = initTable[string, string]()
  fields["workerId"] = $ctx.workerId
  debug("Worker thread stopped", fields)

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
