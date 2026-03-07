# Multi-Raft Coordinator
#
# Manages multiple Raft groups on a single node.
# Handles proposal routing, log replication, worker threads, and group lifecycle.
#
# Multi-node wiring (Phase 6):
#   - CoordinatorConfig.transport: optional RaftGroupTransport; nil = single-node mode
#   - workerProc calls replicateAndWait (quorum replication) when transport != nil,
#     falls back to single-node commit when voters.len == 1 or transport == nil
#   - Timer thread calls startElection / sendHeartbeats via transport
#   - Incoming RPCs are dispatched via CoordAccessors callbacks set in start()
#
# Nim 2.2.8 constraints:
#   - No std/channels; use built-in Channel[T] (.open/.close/.send/.tryRecv)
#   - No RwLock; use plain Lock
#   - Cross-thread completion uses raw ptr ProposalResultChannel (avoids ORC SIGSEGV)

import std/atomics
import std/locks
import std/tables
import std/typedthreads
import std/times
import std/options
import os

import fractio/distributed/range/types
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/multigroup_log
import fractio/distributed/raft/group_commit
import fractio/storage/backend
import fractio/storage/wisckey_backend
import fractio/utils/logging

# ============================================================================
# Configuration
# ============================================================================

const
  DEFAULT_NUM_WORKERS* = 4
  DEFAULT_ELECTION_TIMEOUT_NS* = 1_500_000_000'i64 # 1.5 s
  DEFAULT_HEARTBEAT_INTERVAL_NS* = 500_000_000'i64 # 500 ms
  MAX_PROPOSAL_QUEUE_SIZE* = 10000
  LOG_COMPACTION_THRESHOLD* = 10_000               # entries before snapshot

# ============================================================================
# Coordinator Types
# ============================================================================

type
  # -------------------------------------------------------------------------
  # MultiRaftTransport — vtable interface (no concrete type imported here).
  # multigroup_transport.nim implements this and injects via attachTransport().
  # This decouples coordinator.nim from network/types.nim, preventing the
  # Nim 2.2.8 C-codegen RangeNodeID distinct-type collision.
  # -------------------------------------------------------------------------

  ## Callbacks handed to the transport so it can read coordinator state
  ## without a circular import (mirrors CoordAccessors in multigroup_transport).
  TransportCoordAccessors* = object
    getGroup*: proc(rid: RangeID): Option[RaftGroup] {.gcsafe, raises: [].}
    getLog*: proc(rid: RangeID): Option[RaftLog] {.gcsafe, raises: [].}
    applyUpTo*: proc(rid: RangeID, g: RaftGroup,
        idx: uint64) {.gcsafe, raises: [].}
    saveState*: proc(rid: RangeID, g: RaftGroup,
        log: RaftLog) {.gcsafe, raises: [].}

  ## Abstract transport vtable — filled in by multigroup_transport after create.
  MultiRaftTransport* = ref object
    ## Called once in start() to register incoming-RPC handlers and begin
    ## listening for connections.
    startFn*: proc(acc: TransportCoordAccessors) {.gcsafe, raises: [].}
    ## Called in stop() to shut down the listener and all connections.
    stopFn*: proc() {.gcsafe, raises: [].}
    ## Fan-out AppendEntries to peers, wait for quorum. Returns true on quorum.
    replicateFn*: proc(group: RaftGroup, log: RaftLog,
        entry: LogEntry, timeoutMs: int): bool {.gcsafe, raises: [].}
    ## Broadcast RequestVote, become leader if quorum. Returns true if won.
    electionFn*: proc(group: RaftGroup,
        log: RaftLog): bool {.gcsafe, raises: [].}
    ## Send empty AppendEntries (heartbeat) to all followers of leader groups.
    heartbeatFn*: proc(groups: Table[RangeID, RaftGroup],
        logs: Table[RangeID, RaftLog]) {.gcsafe, raises: [].}

  CoordinatorConfig* = object
    nodeId*: RangeNodeID
    numWorkers*: int
    electionTimeoutNs*: int64
    heartbeatIntervalNs*: int64
    storagePath*: string
    proposeTimeoutMs*: int        ## default 5000
                             ## Optional multi-node transport; nil = single-node mode (all existing tests)
    transport*: MultiRaftTransport
    ## Group commit — coalesce concurrent writes into one fsync.
    ## Disabled by default so all existing tests continue to use the
    ## one-proposal-per-entry path unchanged.
    groupCommitEnabled*: bool
    groupCommitMaxBatch*: int ## 0 → use GC_DEFAULT_MAX_BATCH_SIZE (256)
    groupCommitMaxDelayNs*: int64 ## 0 → use GC_DEFAULT_MAX_DELAY_NS  (2 ms)

  TimerContext = object
    coordinator: MultiRaftCoordinator

  WorkerContext = object
    coordinator: MultiRaftCoordinator
    workerId: int

  MultiRaftCoordinator* = ref object
    nodeId*: RangeNodeID
    config*: CoordinatorConfig

    # Group management
    groups*: Table[RangeID, RaftGroup]
    logs*: Table[RangeID, RaftLog]
    groupsLock*: Lock

    # Storage
    store*: WiscKeyBackend

    # Proposal queue
    proposalCh*: Channel[Proposal]
    pendingProposals*: Table[uint64, Proposal]
    proposalIdCounter*: Atomic[uint64]

    # Worker threads
    workers*: seq[Thread[WorkerContext]]
    running*: Atomic[bool]

    # Timer thread (election + heartbeat) — only active when transport != nil
    timerThread*: Thread[TimerContext]
    timerRunning*: bool

    # Timing
    electionTimeoutNs*: int64
    heartbeatIntervalNs*: int64

    # Multi-node transport vtable (nil = single-node)
    transport*: MultiRaftTransport

    # Back-reference set by raft_store so the coordinator can apply committed
    # entries to the KVStateMachine on followers. Stored as pointer to break
    # the circular import (raft_store → coordinator → raft_store).
    kvStorePtr*: pointer # *RaftKVStoreExt

    # Group commit batcher (single-node path only).
    # Heap-allocated so it can be passed as a raw pointer to the flush thread
    # without ORC cross-thread cycle issues.
    groupCommitEnabled*: bool
    groupCommitBatcherPtr*: ptr GroupCommitBatcher

# ============================================================================
# Forward declarations
# ============================================================================

proc workerProc(ctx: WorkerContext) {.thread.}
proc timerProc(ctx: TimerContext) {.thread.}
proc getGroup*(c: MultiRaftCoordinator, rangeId: RangeID): Option[
    RaftGroup] {.gcsafe.}

# ============================================================================
# Internal helpers
# ============================================================================

proc getLog*(c: MultiRaftCoordinator, rangeId: RangeID): Option[RaftLog] =
  withLock c.groupsLock:
    if c.logs.hasKey(rangeId):
      result = some(c.logs[rangeId])

proc saveGroupState(c: MultiRaftCoordinator, group: RaftGroup,
    log: RaftLog) {.gcsafe.} =
  ## Persist Raft state (term, vote, commitIndex) to the log's WiscKey store.
  try:
    log.saveState(RaftPersistentState(
      currentTerm: group.currentTerm.load(),
      votedFor: group.votedFor.load(),
      commitIndex: group.commitIndex.load(),
      lastApplied: group.lastApplied.load(),
    ))
  except CatchableError: discard

# Module-level callback set once by raft_store after bootstrapping.
# Breaks the coordinator → raft_store → coordinator circular import.
var applyBatchCallback*: proc(storePtr: pointer, rid: RangeID,
    batch: WriteBatch) {.gcsafe, raises: [].} = nil

proc applyUpTo*(c: MultiRaftCoordinator, rangeId: RangeID,
    group: RaftGroup, upToIndex: uint64) {.gcsafe.} =
  ## Apply all log entries from lastApplied+1 through upToIndex to the KV SM.
  ## Calls back into raft_store via the kvStorePtr function pointer to avoid
  ## a circular import. Safe to call with group.lock held.
  let startIdx = group.lastApplied.load() + 1
  if startIdx > upToIndex: return

  let logOpt = c.getLog(rangeId)
  if logOpt.isNone: return
  let log = logOpt.get

  for idx in startIdx..upToIndex:
    let entryOpt = try: log.getEntry(idx) except CatchableError: none(LogEntry)
    if entryOpt.isNone: break
    let entry = entryOpt.get

    # Apply write batches to the KV state machine via the back-pointer.
    # The function signature is:  applyBatchCallback(kvStorePtr, rangeId, batch)
    # We cast and call it only when the pointer is set.
    if c.kvStorePtr != nil and entry.command.kind == ckWrite:
      {.cast(gcsafe).}:
        applyBatchCallback(c.kvStorePtr, rangeId, entry.command.writeBatch)

    group.lastApplied.store(idx)



# ============================================================================
# Coordinator Lifecycle
# ============================================================================

proc newMultiRaftCoordinator*(config: CoordinatorConfig): MultiRaftCoordinator =
  new(result)
  result.nodeId = config.nodeId
  result.config = config
  result.electionTimeoutNs = config.electionTimeoutNs
  result.heartbeatIntervalNs = config.heartbeatIntervalNs
  result.transport = config.transport
  result.kvStorePtr = nil
  result.timerRunning = false

  result.store = newWiscKeyBackend(StorageConfig(
    path: config.storagePath, createIfMissing: true, syncWrites: true))

  if not result.store.open(StorageConfig(
      path: config.storagePath, createIfMissing: true, syncWrites: true)):
    raise newException(MultiRaftError, "Failed to open storage backend")

  result.groups = initTable[RangeID, RaftGroup]()
  result.logs = initTable[RangeID, RaftLog]()
  result.pendingProposals = initTable[uint64, Proposal]()
  result.proposalIdCounter.store(0)

  initLock(result.groupsLock)
  result.proposalCh.open(MAX_PROPOSAL_QUEUE_SIZE)

  result.workers = newSeq[Thread[WorkerContext]](config.numWorkers)
  result.running.store(false)

  # Group commit batcher — allocate on the heap when enabled.
  result.groupCommitEnabled = config.groupCommitEnabled
  result.groupCommitBatcherPtr = nil
  if config.groupCommitEnabled:
    result.groupCommitBatcherPtr = cast[ptr GroupCommitBatcher](
      allocShared0(sizeof(GroupCommitBatcher)))
    let maxBatch = if config.groupCommitMaxBatch > 0: config.groupCommitMaxBatch
                   else: GC_DEFAULT_MAX_BATCH_SIZE
    let maxDelay = if config.groupCommitMaxDelayNs >
        0: config.groupCommitMaxDelayNs
                   else: GC_DEFAULT_MAX_DELAY_NS
    initGroupCommitBatcher(result.groupCommitBatcherPtr, maxBatch, maxDelay)

proc start*(c: MultiRaftCoordinator) =
  if c.running.load: return
  c.running.store(true)

  # Wire + start group commit batcher (single-node path only).
  # The flushFn is injected here so it has access to `c` via a raw pointer,
  # avoiding ORC cycles.  The batcher must be started BEFORE worker threads
  # so callers that use proposeGroupCommit can enqueue immediately.
  if c.groupCommitEnabled and c.groupCommitBatcherPtr != nil:
    let cPtr = cast[pointer](c)
    proc gcFlush(rangeId: RangeID, batch: WriteBatch,
        resultPtrs: seq[ptr ProposalResultChannel]) {.gcsafe, raises: [].} =
      let coord = cast[MultiRaftCoordinator](cPtr)
      var groupOpt: Option[RaftGroup]
      {.cast(raises: []).}: groupOpt = coord.getGroup(rangeId)
      if groupOpt.isNone:
        for rp in resultPtrs:
          if rp != nil:
            rp[].ch.send(RaftResult(success: false,
                error: "Range not found: " & $rangeId))
        return
      let group = groupOpt.get
      if not group.isLeader():
        for rp in resultPtrs:
          if rp != nil:
            rp[].ch.send(RaftResult(success: false, error: "Not the leader"))
        return
      # Append ONE combined log entry for the entire batch.
      var index: uint64
      var entry: LogEntry
      {.cast(raises: []).}:
        acquire(coord.groupsLock)
        let log = coord.logs.getOrDefault(rangeId)
        let term = group.getTerm()
        let idx = log.lastIndex.load + 1
        let cmd = RaftCommand(kind: ckWrite, writeBatch: batch)
        let e = newLogEntry(term, idx, cmd)
        log.putEntry(e)
        release(coord.groupsLock)
        coord.saveGroupState(group, log)
        entry = e
        index = idx
      # Single-node: commit immediately.
      group.commitIndex.store(index)
      {.cast(raises: []).}: coord.applyUpTo(rangeId, group, index)
      # Signal all waiters with the same index.
      let res = RaftResult(success: true, index: index)
      for rp in resultPtrs:
        if rp != nil:
          rp[].ch.send(res)
    c.groupCommitBatcherPtr[].flushFn = gcFlush
    startBatcher(c.groupCommitBatcherPtr)

  # Start worker threads
  for i in 0..<c.config.numWorkers:
    createThread(c.workers[i], workerProc, WorkerContext(
        coordinator: c, workerId: i))

  # Wire transport incoming handlers and start the transport TCP listener
  if c.transport != nil:
    # Use a raw pointer to `c` inside the closures to break the ORC cycle:
    #   MultiRaftCoordinator(c) → transport → RaftGroupTransport → NetworkRaftNode
    #     → ConnectionManager → TCPTransport.handlers → closure env → c  (cycle!)
    # With a raw pointer the closure environment holds no traced ref, so ORC
    # does not detect a cycle and does not crash during collectCycles.
    # Safety: the transport is stopped (handlers cleared) before `c` is freed.
    let cPtr = cast[pointer](c)

    proc coordGetGroup(rid: RangeID): Option[RaftGroup] {.gcsafe, raises: [].} =
      let coord = cast[MultiRaftCoordinator](cPtr)
      acquire(coord.groupsLock)
      defer: release(coord.groupsLock)
      let g = coord.groups.getOrDefault(rid)
      if g != nil: result = some(g)
      else: result = none(RaftGroup)

    proc coordGetLog(rid: RangeID): Option[RaftLog] {.gcsafe, raises: [].} =
      let coord = cast[MultiRaftCoordinator](cPtr)
      acquire(coord.groupsLock)
      defer: release(coord.groupsLock)
      let l = coord.logs.getOrDefault(rid)
      if l != nil: result = some(l)
      else: result = none(RaftLog)

    proc coordApplyUpTo(rid: RangeID, g: RaftGroup,
        idx: uint64) {.gcsafe, raises: [].} =
      {.cast(raises: []).}:
        cast[MultiRaftCoordinator](cPtr).applyUpTo(rid, g, idx)

    proc coordSaveState(rid: RangeID, g: RaftGroup,
        log: RaftLog) {.gcsafe, raises: [].} =
      {.cast(raises: []).}:
        cast[MultiRaftCoordinator](cPtr).saveGroupState(g, log)

    let acc = TransportCoordAccessors(
      getGroup: coordGetGroup,
      getLog: coordGetLog,
      applyUpTo: coordApplyUpTo,
      saveState: coordSaveState,
    )
    c.transport.startFn(acc)

    # Reset election clocks BEFORE spawning the timer thread so the timeout is
    # measured from when the coordinator actually starts, not from when the
    # groups were created.  Without this, time spent in LevelDB/storage
    # initialization (which can be hundreds of ms) counts against the election
    # timeout and causes spurious elections on the very first timer tick.
    # NOTE: must happen before createThread so there is no race with timerProc.
    acquire(c.groupsLock)
    for group in c.groups.values:
      group.updateHeartbeat()
    release(c.groupsLock)

    # Timer thread for elections and heartbeats
    createThread(c.timerThread, timerProc, TimerContext(coordinator: c))
    c.timerRunning = true

  {.cast(gcsafe).}:
    var fields = initTable[string, string]()
    fields["nodeId"] = $c.nodeId
    fields["numWorkers"] = $c.config.numWorkers
    info("Multi-Raft coordinator started", fields)

proc stop*(c: MultiRaftCoordinator) =
  if not c.running.load: return
  c.running.store(false)

  # Stop group commit batcher first so no new flushes race with worker shutdown.
  if c.groupCommitEnabled and c.groupCommitBatcherPtr != nil:
    stopBatcher(c.groupCommitBatcherPtr)
    deinitGroupCommitBatcher(c.groupCommitBatcherPtr)
    deallocShared(c.groupCommitBatcherPtr)
    c.groupCommitBatcherPtr = nil

  # Shutdown sentinel per worker (rangeId == 0)
  for _ in 0..<c.workers.len:
    c.proposalCh.send(Proposal(
      rangeId: RangeID(0),
      command: RaftCommand(kind: ckNoop),
      resultPtr: nil,
    ))

  for i in 0..<c.workers.len:
    joinThread(c.workers[i])

  c.proposalCh.close()

  if c.transport != nil:
    # Join timer thread BEFORE stopping transports so the timer thread can
    # finish its current election/heartbeat cycle without hitting closed sockets.
    if c.timerRunning:
      joinThread(c.timerThread)
    c.transport.stopFn()

  withLock c.groupsLock:
    for group in c.groups.values: group.close()
    for log in c.logs.values: log.close()

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
  withLock c.groupsLock:
    if c.groups.hasKey(descriptor.rangeId):
      raise newException(MultiRaftError,
          "Group already exists: " & $descriptor.rangeId)

    let log = newRaftLog(descriptor.rangeId, c.store)
    log.recoverLog()
    c.logs[descriptor.rangeId] = log

    let group = newRaftGroup(descriptor.rangeId, c.nodeId, replicaId, descriptor)
    c.groups[descriptor.rangeId] = group

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
  withLock c.groupsLock:
    if c.groups.hasKey(rangeId):
      c.groups[rangeId].close()
      c.groups.del(rangeId)
      c.logs[rangeId].close()
      c.logs.del(rangeId)

proc getGroup*(c: MultiRaftCoordinator, rangeId: RangeID): Option[
    RaftGroup] {.gcsafe.} =
  withLock c.groupsLock:
    if c.groups.hasKey(rangeId):
      result = some(c.groups[rangeId])

proc hasGroup*(c: MultiRaftCoordinator, rangeId: RangeID): bool =
  withLock c.groupsLock:
    result = c.groups.hasKey(rangeId)

# ============================================================================
# Proposal Handling
# ============================================================================

proc proposeAndWait*(c: MultiRaftCoordinator, rangeId: RangeID,
    command: RaftCommand, timeoutMs: int = 5000): RaftResult =
  ## Propose a command and block until committed (or timeout).
  ## When group commit is enabled (single-node only), delegates to the
  ## GroupCommitBatcher so many concurrent callers share one fsync.
  ## Falls back to the classic one-entry-per-proposal path otherwise.
  var prc = cast[ptr ProposalResultChannel](
    allocShared0(sizeof(ProposalResultChannel)))
  prc[].ch.open(1)

  if c.groupCommitEnabled and c.groupCommitBatcherPtr != nil and
      c.transport == nil and command.kind == ckWrite:
    # Group commit path: enqueue and block on the result channel.
    enqueue(c.groupCommitBatcherPtr, rangeId, command, prc)
  else:
    # Classic path: send to worker proposalCh.
    c.proposalCh.send(Proposal(
      rangeId: rangeId,
      command: command,
      resultPtr: prc,
    ))

  let deadline = int64(getTime().toUnixFloat * 1000) + int64(timeoutMs)
  while true:
    let (avail, res) = prc[].ch.tryRecv()
    if avail:
      prc[].ch.close()
      deallocShared(prc)
      return res
    if int64(getTime().toUnixFloat * 1000) >= deadline:
      prc[].ch.close()
      deallocShared(prc)
      return RaftResult(success: false, error: "Timeout waiting for proposal")
    sleep(1)

# ============================================================================
# Worker Thread
# ============================================================================

proc sendResult(p: ptr ProposalResultChannel, r: RaftResult) {.inline.} =
  if p != nil: p[].ch.send(r)

proc computeNewCommitIndex(group: RaftGroup, proposed: uint64): uint64 =
  ## Return the highest index that a quorum of voters have acknowledged,
  ## considering that this leader's own local append counts as 1.
  ## matchIndex values updated by replicateEntry; local replica is at proposed.
  let currentCommit = group.commitIndex.load()
  let voters = group.descriptor.getVoters()
  let quorum = group.quorum()

  # Collect acknowledged indices (self = proposed, peers = matchIndex)
  var indices = newSeq[uint64](voters.len)
  var i = 0
  for rep in voters:
    if rep.nodeId == group.nodeId:
      indices[i] = proposed
    else:
      indices[i] = group.matchIndex.getOrDefault(rep.replicaId, 0'u64)
    inc i

  # Sort descending to find the quorum-th highest
  for a in 0..<indices.len:
    for b in a+1..<indices.len:
      if indices[b] > indices[a]:
        let tmp = indices[a]; indices[a] = indices[b]; indices[b] = tmp

  let candidate = indices[quorum - 1]
  if candidate > currentCommit: candidate else: currentCommit

proc workerProc(ctx: WorkerContext) {.thread.} =
  let c = ctx.coordinator

  while c.running.load:
    let proposal = c.proposalCh.recv()
    if proposal.rangeId.uint64 == 0:
      break # Shutdown sentinel

    try:
      let groupOpt = c.getGroup(proposal.rangeId)
      if groupOpt.isNone:
        sendResult(proposal.resultPtr, RaftResult(
          success: false, error: "Range not found: " & $proposal.rangeId))
        continue

      let group = groupOpt.get

      if not group.isLeader():
        sendResult(proposal.resultPtr, RaftResult(
          success: false, error: "Not the leader"))
        continue

      # --- Append to local log ---
      var entry: LogEntry
      var index: uint64
      block appendBlock:
        acquire(c.groupsLock)
        let log = c.logs.getOrDefault(proposal.rangeId)
        let term = group.getTerm()
        let idx = log.lastIndex.load + 1
        let e = newLogEntry(term, idx, proposal.command)
        log.putEntry(e)
        release(c.groupsLock)
        c.saveGroupState(group, log)
        entry = e
        index = idx

      # --- Determine replication path ---
      let voters = group.descriptor.getVoters()
      let useTransport = c.transport != nil and voters.len > 1

      if useTransport:
        # Multi-node: fan-out AppendEntries and wait for quorum
        let logOpt = c.getLog(proposal.rangeId)
        if logOpt.isNone:
          sendResult(proposal.resultPtr, RaftResult(
            success: false, error: "Log not found"))
          continue

        let timeoutMs = if c.config.proposeTimeoutMs > 0:
                          c.config.proposeTimeoutMs else: 5000
        let ok = c.transport.replicateFn(group, logOpt.get, entry, timeoutMs)

        if not ok:
          sendResult(proposal.resultPtr, RaftResult(
            success: false, error: "Failed to reach quorum"))
          continue

        let newCommit = computeNewCommitIndex(group, index)
        if newCommit > group.commitIndex.load():
          group.commitIndex.store(newCommit)
          c.applyUpTo(proposal.rangeId, group, newCommit)
          # getLog acquires groupsLock — call it without holding the lock
          let logOpt2 = c.getLog(proposal.rangeId)
          if logOpt2.isSome:
            c.saveGroupState(group, logOpt2.get)

        sendResult(proposal.resultPtr, RaftResult(
          success: true, index: index))

      else:
        # Single-node (existing behaviour): commit immediately
        group.commitIndex.store(index)
        c.applyUpTo(proposal.rangeId, group, index)
        sendResult(proposal.resultPtr, RaftResult(
          success: true, index: index))

    except CatchableError as e:
      try:
        sendResult(proposal.resultPtr, RaftResult(
          success: false, error: e.msg))
      except CatchableError: discard

# ============================================================================
# Timer Thread (election + heartbeat)
# ============================================================================

proc timerProc(ctx: TimerContext) {.thread.} =
  ## Runs every 10 ms. Checks each group independently:
  ##   Leader   → send heartbeats when heartbeatInterval elapsed
  ##   Follower/Candidate → start election when electionTimeout elapsed
  let c = ctx.coordinator

  while c.running.load:
    sleep(10)

    # Snapshot group/log tables under the lock, then iterate without it
    var groupSnap: seq[(RangeID, RaftGroup)]
    withLock c.groupsLock:
      for rid, g in c.groups:
        groupSnap.add((rid, g))

    for (rid, group) in groupSnap:
      let logOpt = c.getLog(rid)
      if logOpt.isNone: continue
      let log = logOpt.get

      case group.state.load()
      of rsLeader:
        if group.timeSinceHeartbeat() >= c.heartbeatIntervalNs:
          var groupsCopy: Table[RangeID, RaftGroup]
          var logsCopy: Table[RangeID, RaftLog]
          withLock c.groupsLock:
            groupsCopy = c.groups
            logsCopy = c.logs
          c.transport.heartbeatFn(groupsCopy, logsCopy)

      of rsFollower, rsCandidate:
        let elapsed = group.timeSinceHeartbeat()
        # Deterministic jitter to spread election timeouts and avoid split votes.
        # Uses a multiplicative hash of nodeId and rangeId to produce a value in
        # [0, electionTimeoutNs).  For nodes 1..5 with rangeId 1 this gives
        # spreads of hundreds of milliseconds, unlike the previous XOR approach
        # which gave 0-3 nanoseconds for small IDs.
        let hashVal = (group.nodeId.uint64 * 2654435761'u64 +
                       group.rangeId.uint64 * 2246822519'u64) and 0xFFFF_FFFF'u64
        let jitterNs = int64(hashVal mod uint64(c.electionTimeoutNs))
        let effectiveTimeout = c.electionTimeoutNs + jitterNs

        if elapsed >= effectiveTimeout:
          {.cast(gcsafe).}:
            var fields = initTable[string, string]()
            fields["rangeId"] = $rid
            fields["term"] = $group.getTerm()
            info("Starting election", fields)

          let won = c.transport.electionFn(group, log)
          if won:
            {.cast(gcsafe).}:
              var fields = initTable[string, string]()
              fields["rangeId"] = $rid
              fields["term"] = $group.getTerm()
              info("Won election, became leader", fields)
            c.saveGroupState(group, log)
            # Send immediate no-op heartbeat to establish leadership
            var groupsCopy: Table[RangeID, RaftGroup]
            var logsCopy: Table[RangeID, RaftLog]
            withLock c.groupsLock:
              groupsCopy = c.groups
              logsCopy = c.logs
            c.transport.heartbeatFn(groupsCopy, logsCopy)
          else:
            # Lost election — reset heartbeat so we don't immediately retry.
            # This gives other candidates time to win before we try again.
            group.updateHeartbeat()

# ============================================================================
# Election and Heartbeat (legacy stubs — kept for backward compatibility)
# ============================================================================

proc checkElectionTimeout*(c: MultiRaftCoordinator) =
  ## Legacy stub — election is now driven by timerProc when transport != nil.
  if c.transport != nil: return
  withLock c.groupsLock:
    for group in c.groups.values:
      if group.state.load == rsLeader: continue
      if group.timeSinceHeartbeat() > c.electionTimeoutNs:
        group.becomeCandidate()

proc sendHeartbeats*(c: MultiRaftCoordinator) =
  ## Legacy stub — heartbeats are now sent by timerProc when transport != nil.
  if c.transport != nil: return
  withLock c.groupsLock:
    for group in c.groups.values:
      if group.state.load == rsLeader:
        group.updateHeartbeat()

# ============================================================================
# Utility
# ============================================================================

proc getLeaderCount*(c: MultiRaftCoordinator): int =
  withLock c.groupsLock:
    for group in c.groups.values:
      if group.isLeader(): inc result

proc getGroupCount*(c: MultiRaftCoordinator): int =
  withLock c.groupsLock:
    result = c.groups.len
