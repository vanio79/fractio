# Multi-Group Raft Types
#
# This module extends the basic Raft types for multi-group support.
# Each group has its own independent Raft group.

import std/atomics
import std/locks
import std/options
import std/tables
import std/times
import std/sets

import fractio/distributed/raft/group_types

# ============================================================================
# Raft State
# ============================================================================

type
  RaftState* = enum
    ## Raft node state
    rsFollower  ## Following a leader
    rsCandidate ## Running for election
    rsLeader    ## Leading the group

# ============================================================================
# Log Entry Types
# ============================================================================

type
  CommandKind* = enum
    ## Types of commands in Raft log
    ckNoop           ## No-op (heartbeat)
    ckWrite          ## Write batch
    ckSplit          ## Split group
    ckMerge          ## Merge groups
    ckChangeReplicas ## Add/remove replica
    ckTransferLease  ## Transfer lease to another node
    ckAcquireLease   ## Acquire lease

  WriteBatch* = ref object
    ## Batch of write operations
    puts*: seq[tuple[key: seq[byte], value: seq[byte]]]
    deletes*: seq[seq[byte]]

  RaftCommand* = object
    ## Command in a Raft log entry
    case kind*: CommandKind
    of ckNoop:
      discard
    of ckWrite:
      writeBatch*: WriteBatch
    of ckSplit:
      splitKey*: seq[byte]
      newRangeId*: GroupID
    of ckMerge:
      otherRangeId*: GroupID
    of ckChangeReplicas:
      changeType*: ReplicaChangeType
      replica*: ReplicaDescriptor
    of ckTransferLease:
      targetNode*: NodeID
    of ckAcquireLease:
      leaseStart*: int64
      leaseExpiration*: int64

  ReplicaChangeType* = enum
    ## Type of replica change
    rctAddVoter
    rctRemoveVoter
    rctAddNonVoter
    rctRemoveNonVoter
    rctPromoteToVoter
    rctDemoteToNonVoter

  LogEntry* = ref object
    ## Single log entry
    term*: uint64
    index*: uint64
    command*: RaftCommand

# ============================================================================
# Raft Persistent State
# ============================================================================

type
  RaftPersistentState* = object
    ## State that must be persisted before responding to RPCs
    currentTerm*: uint64
    votedFor*: ReplicaID
    commitIndex*: uint64
    lastApplied*: uint64

# ============================================================================
# Raft Group State
# ============================================================================

type
  RaftGroup* = ref object
    ## A single Raft group (one per group)
    groupId*: GroupID
    nodeId*: NodeID
    replicaId*: ReplicaID

    # Persistent state (stored in WiscKey)
    currentTerm*: Atomic[uint64]
    votedFor*: Atomic[ReplicaID]

    # Volatile state
    commitIndex*: Atomic[uint64]
    lastApplied*: Atomic[uint64]
    state*: Atomic[RaftState]

    # Leader volatile state
    nextIndex*: Table[ReplicaID, uint64]
    matchIndex*: Table[ReplicaID, uint64]

    # Election state
    votesGranted*: HashSet[ReplicaID]
    lastHeartbeat*: Atomic[int64] # nanoseconds

    # Preferred leader rebalancing cooldown (nanoseconds since epoch).
    # Prevents step-down → re-election cycling when preferred leader keeps
    # losing elections.
    lastPreferredLeaderStepdownNs*: Atomic[int64]

    # Thread safety
    lock*: Lock

    # Configuration
    descriptor*: GroupDescriptor

# ============================================================================
# Lease State
# ============================================================================

type
  Lease* = object
    ## Leader lease for a group
    leaseholder*: NodeID
    startTs*: int64      # nanoseconds
    expirationTs*: int64 # nanoseconds
    epoch*: uint64       # For compatibility, deprecated

  LeaseState* = enum
    lsNone
    lsAcquiring
    lsHeld
    lsTransferring
    lsExpired

# ============================================================================
# Snapshot
# ============================================================================

type
  Snapshot* = ref object
    ## Snapshot of group state
    groupId*: GroupID
    raftSnap*: RaftSnapshotMeta
    stateMachineSnap*: seq[byte]

  RaftSnapshotMeta* = object
    lastIncludedIndex*: uint64
    lastIncludedTerm*: uint64
    configuration*: seq[ReplicaDescriptor]

# ============================================================================
# Proposal
# ============================================================================

type
  RaftResult* = object
    ## Result of a Raft proposal
    success*: bool
    index*: uint64
    error*: string

  ProposalResultChannel* = object
    ## One-shot channel used to return a RaftResult to a waiting caller.
    ## Allocated on the heap and accessed via raw pointer so ORC does not
    ## attempt cross-thread cycle tracking (which causes SIGSEGV).
    ch*: Channel[RaftResult]

  Proposal* = ref object
    ## A pending proposal to a Raft group.
    groupId*: GroupID
    command*: RaftCommand
    ## Raw pointer to the caller's heap-allocated ProposalResultChannel.
    ## The worker sends into ch; the caller receives from ch.
    ## Using a raw pointer sidesteps ORC's cross-thread ref counting.
    resultPtr*: ptr ProposalResultChannel

# ============================================================================
# Errors
# ============================================================================

type
  MultiRaftError* = object of CatchableError
    ## Base error for multi-raft operations

  NotLeaderError* = object of MultiRaftError
    ## Current node is not the leader
    leaderHint*: Option[NodeID]

  GroupNotFoundError* = object of MultiRaftError
    ## Group not found on this node
    groupId*: GroupID

  LeaseExpiredError* = object of MultiRaftError
    ## Lease has expired

  QuorumError* = object of MultiRaftError
    ## Cannot achieve quorum

# ============================================================================
# Time utilities
# ============================================================================

proc nowNs*(): int64 {.inline.} =
  ## Current wall-clock time in nanoseconds with sub-second precision.
  ## Both the seconds and nanosecond fields of getTime() are used so that
  ## heartbeat / election-timeout comparisons work correctly within a single
  ## second (i.e. toUnix alone would truncate to whole-second resolution).
  let t = getTime()
  t.toUnix * 1_000_000_000 + t.nanosecond.int64

# ============================================================================
# Raft Group Operations
# ============================================================================

proc newRaftGroup*(groupId: GroupID, nodeId: NodeID,
                   replicaId: ReplicaID,
                   descriptor: GroupDescriptor): RaftGroup =
  ## Create a new Raft group
  new(result)
  result.groupId = groupId
  result.nodeId = nodeId
  result.replicaId = replicaId
  result.descriptor = descriptor

  # Initialize atomic state
  result.currentTerm.store(0)
  result.votedFor.store(ReplicaID(0))
  result.commitIndex.store(0)
  result.lastApplied.store(0)
  result.state.store(rsFollower)
  # Initialize to now so the election timer doesn't fire immediately.
  # A follower that just joined needs a full electionTimeout before it starts
  # its first election, giving the existing leader time to send a heartbeat.
  result.lastHeartbeat.store(nowNs())
  result.lastPreferredLeaderStepdownNs.store(0)

  # Initialize leader state
  for rep in descriptor.replicas:
    result.nextIndex[rep.replicaId] = 1
    result.matchIndex[rep.replicaId] = 0

  initLock(result.lock)

proc close*(group: RaftGroup) =
  ## Clean up Raft group resources
  deinitLock(group.lock)

proc isLeader*(group: RaftGroup): bool =
  ## Check if this group is the leader
  group.state.load() == rsLeader

proc getTerm*(group: RaftGroup): uint64 =
  ## Get current term
  group.currentTerm.load()

proc getCommitIndex*(group: RaftGroup): uint64 =
  ## Get commit index
  group.commitIndex.load()

proc getLastApplied*(group: RaftGroup): uint64 =
  ## Get last applied index
  group.lastApplied.load()

proc becomeFollower*(group: RaftGroup, term: uint64) =
  ## Transition to follower state
  withLock group.lock:
    group.state.store(rsFollower)
    group.currentTerm.store(term)
    group.votesGranted.clear()

proc becomeCandidate*(group: RaftGroup) =
  ## Transition to candidate state and start election
  withLock group.lock:
    group.state.store(rsCandidate)
    discard group.currentTerm.fetchAdd(1)
    group.votedFor.store(group.replicaId)
    group.votesGranted.clear()
    group.votesGranted.incl(group.replicaId)

proc becomeLeader*(group: RaftGroup) =
  ## Transition to leader state
  withLock group.lock:
    group.state.store(rsLeader)
    # Reset heartbeat so the heartbeat timer doesn't think we're overdue
    # immediately after becoming leader, which would block the timer thread.
    group.lastHeartbeat.store(nowNs())
    # Initialize leader state
    let lastIndex = group.lastApplied.load
    for rep in group.descriptor.replicas:
      if rep.replicaId != group.replicaId:
        group.nextIndex[rep.replicaId] = lastIndex + 1
        group.matchIndex[rep.replicaId] = 0

proc updateHeartbeat*(group: RaftGroup) =
  ## Update last heartbeat time (nanosecond precision)
  group.lastHeartbeat.store(nowNs())

proc timeSinceHeartbeat*(group: RaftGroup): int64 =
  ## Time since last heartbeat in nanoseconds (nanosecond precision)
  nowNs() - group.lastHeartbeat.load

proc quorum*(group: RaftGroup): int =
  ## Calculate quorum size for this group
  group.descriptor.quorumSize()

proc hasQuorum*(group: RaftGroup, votes: int): bool =
  ## Check if votes constitute a quorum
  votes >= group.quorum()

# ============================================================================
# Log Entry Operations
# ============================================================================

proc newLogEntry*(term, index: uint64, command: RaftCommand): LogEntry =
  ## Create a new log entry
  new(result)
  result.term = term
  result.index = index
  result.command = command

proc newNoopEntry*(term, index: uint64): LogEntry =
  ## Create a no-op log entry (for heartbeats)
  newLogEntry(term, index, RaftCommand(kind: ckNoop))

proc newWriteEntry*(term, index: uint64, batch: WriteBatch): LogEntry =
  ## Create a write log entry
  newLogEntry(term, index, RaftCommand(kind: ckWrite, writeBatch: batch))

# ============================================================================
# Write Batch Operations
# ============================================================================

proc newWriteBatch*(): WriteBatch =
  ## Create an empty write batch
  new(result)
  result.puts = @[]
  result.deletes = @[]

proc put*(batch: WriteBatch, key, value: seq[byte]) =
  ## Add a put operation to the batch
  batch.puts.add((key, value))

proc delete*(batch: WriteBatch, key: seq[byte]) =
  ## Add a delete operation to the batch
  batch.deletes.add(key)

proc len*(batch: WriteBatch): int =
  ## Total number of operations in the batch
  batch.puts.len + batch.deletes.len

proc isEmpty*(batch: WriteBatch): bool =
  ## Check if batch is empty
  batch.puts.len == 0 and batch.deletes.len == 0
