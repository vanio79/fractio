# Multi-Group Raft Types
#
# This module extends the basic Raft types for multi-group support.
# Each range has its own independent Raft group.

import std/atomics
import std/locks
import std/options
import std/tables
import std/times
import std/sets

import fractio/distributed/range/types

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
    ckSplit          ## Split range
    ckMerge          ## Merge ranges
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
      newRangeId*: RangeID
    of ckMerge:
      otherRangeId*: RangeID
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
    ## A single Raft group (one per range)
    rangeId*: RangeID
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

    # Thread safety
    lock*: Lock

    # Configuration
    descriptor*: RangeDescriptor

# ============================================================================
# Lease State
# ============================================================================

type
  Lease* = object
    ## Leader lease for a range
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
    ## Snapshot of range state
    rangeId*: RangeID
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
  Proposal* = ref object
    ## A pending proposal to a Raft group
    rangeId*: RangeID
    command*: RaftCommand
    callback*: proc(result: RaftResult) {.closure, gcsafe.}

  RaftResult* = object
    ## Result of a Raft proposal
    success*: bool
    index*: uint64
    error*: string

# ============================================================================
# Errors
# ============================================================================

type
  MultiRaftError* = object of CatchableError
    ## Base error for multi-raft operations

  NotLeaderError* = object of MultiRaftError
    ## Current node is not the leader
    leaderHint*: Option[NodeID]

  RangeNotFoundError* = object of MultiRaftError
    ## Range not found on this node
    rangeId*: RangeID

  LeaseExpiredError* = object of MultiRaftError
    ## Lease has expired

  QuorumError* = object of MultiRaftError
    ## Cannot achieve quorum

# ============================================================================
# Raft Group Operations
# ============================================================================

proc newRaftGroup*(rangeId: RangeID, nodeId: NodeID,
                   replicaId: ReplicaID,
                   descriptor: RangeDescriptor): RaftGroup =
  ## Create a new Raft group for a range
  new(result)
  result.rangeId = rangeId
  result.nodeId = nodeId
  result.replicaId = replicaId
  result.descriptor = descriptor

  # Initialize atomic state
  result.currentTerm.store(0)
  result.votedFor.store(ReplicaID(0))
  result.commitIndex.store(0)
  result.lastApplied.store(0)
  result.state.store(rsFollower)
  result.lastHeartbeat.store(0)

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
    # Initialize leader state
    let lastIndex = group.lastApplied.load
    for rep in group.descriptor.replicas:
      if rep.replicaId != group.replicaId:
        group.nextIndex[rep.replicaId] = lastIndex + 1
        group.matchIndex[rep.replicaId] = 0

proc updateHeartbeat*(group: RaftGroup) =
  ## Update last heartbeat time
  group.lastHeartbeat.store(getTime().toUnix * 1_000_000_000)

proc timeSinceHeartbeat*(group: RaftGroup): int64 =
  ## Time since last heartbeat in nanoseconds
  getTime().toUnix * 1_000_000_000 - group.lastHeartbeat.load

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
