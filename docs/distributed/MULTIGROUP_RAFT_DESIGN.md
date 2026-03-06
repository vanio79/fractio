# Multi-Group Raft Design Document

## Executive Summary

This document describes the design for Fractio's multi-group Raft implementation, inspired by CockroachDB's architecture. The key innovation is treating each data range as an independent Raft group, enabling horizontal scalability, fault isolation, and parallel processing across the cluster.

## 1. Architecture Overview

### 1.1 Core Concepts

Fractio's multi-group Raft follows CockroachDB's model where:

1. **Range**: A contiguous chunk of the key-space (default 64MB). Each range is an independent Raft group.
2. **Replica**: A copy of a range stored on a node. Default replication factor is 3.
3. **Leaseholder**: The replica that holds the "range lease" - coordinates all reads and writes for the range.
4. **Raft Leader**: The replica that coordinates writes through the Raft consensus protocol.

**Key Design Decision**: In Fractio, the leaseholder is always the Raft leader (Leader Leases). This eliminates the complexity of leader-leaseholder splits and reduces network round-trips.

### 1.2 Layer Stack

```
┌─────────────────────────────────────────────────────────────┐
│ SQL Layer                                                   │
│ (Parser, Planner, Executor - translates SQL to KV ops)      │
├─────────────────────────────────────────────────────────────┤
│ Distribution Layer                                          │
│ (Range routing, DistSender, Meta ranges, gRPC)              │
├─────────────────────────────────────────────────────────────┤
│ Replication Layer                                           │
│ (Multi-Group Raft, Leader Leases, Snapshots)                │
├─────────────────────────────────────────────────────────────┤
│ Storage Layer                                               │
│ (WiscKey backend, MVCC storage, SSTables)                   │
└─────────────────────────────────────────────────────────────┘
```

**Note**: Transaction support (MVCC, 2PC) will be implemented as a separate work item.

### 1.3 Data Flow

```
Client Request
      │
      ▼
┌─────────────┐
│  SQL Layer  │ ─── Parse SQL, generate KV operations
└─────────────┘
      │
      ▼
┌─────────────────┐
│ Distribution    │ ─── Route to correct range(s) via meta ranges
│ Layer           │     (DistSender splits BatchRequest by range)
└─────────────────┘
      │
      ▼
┌─────────────────┐
│ Replication     │ ─── If leaseholder: propose to Raft
│ Layer           │     If not: redirect to leaseholder
└─────────────────┘
      │
      ▼
┌─────────────────┐
│ Raft Consensus  │ ─── Replicate to majority, commit
└─────────────────┘
      │
      ▼
┌─────────────────┐
│ Storage Layer   │ ─── Apply to state machine, persist
└─────────────────┘
```

---

## 2. Range Architecture

### 2.1 Range Definition

A range is the fundamental unit of data distribution and replication:

```nim
type
  RangeID* = distinct uint64
    ## Unique identifier for a range
  
  RangeDescriptor* = ref object
    ## Metadata describing a range
    rangeId*: RangeID
    startKey*: seq[byte]       ## Inclusive start of key range
    endKey*: seq[byte]         ## Exclusive end of key range
    replicas*: seq[ReplicaDescriptor]
    nextReplicaId*: uint64
    generation*: uint64        ## Incremented on every change
  
  ReplicaDescriptor* = object
    ## Describes a single replica
    nodeId*: NodeID
    replicaId*: ReplicaID
    type*: ReplicaType
  
  ReplicaType* = enum
    rtVoter        ## Participates in Raft quorum
    rtNonVoter     ## For follower reads, no quorum participation
  
  ReplicaID* = distinct uint32
    ## Unique identifier for a replica within a range
```

### 2.2 Range Size and Splitting

**Default Range Size**: 64MB (configurable)

**Split Trigger**: When a range exceeds `range_max_bytes`, it splits into two:
1. Leader proposes a split command
2. Split point chosen to divide data roughly in half
3. Two new range descriptors created
4. Meta2 range updated atomically

**Merge Trigger**: When adjacent ranges are both below `range_min_bytes` (default 1MB):
1. Left range absorbs right range
2. Single transaction updates descriptors

### 2.3 Range State Machine

Each range has an independent state machine:

```nim
type
  RangeState* = ref object
    ## Per-range state machine
    rangeId*: RangeID
    raftGroup*: RaftGroup
    lease*: Lease
    stats*: RangeStats
    pendingLease*: Option[Lease]
    
  Lease* = object
    ## Leader lease for a range
    leaseholder*: NodeID
    startTs*: Timestamp
    expirationTs*: Timestamp
    epoch*: uint64           ## For epoch-based leases (deprecated)
    
  RangeStats* = object
    ## Statistics for load-based splitting/rebalancing
    keyCount*: int64
    totalBytes*: int64
    writesPerSecond*: float64
    readsPerSecond*: float64
```

---

## 3. Multi-Group Raft Architecture

### 3.1 Raft Group Structure

Each range forms an independent Raft group:

```nim
type
  RaftGroup* = ref object
    ## A single Raft group (one per range)
    rangeId*: RangeID
    nodeId*: NodeID
    replicaId*: ReplicaID
    
    # Persistent state (stored in WiscKey)
    currentTerm*: Atomic[uint64]
    votedFor*: Atomic[ReplicaID]
    log*: RaftLog
    
    # Volatile state
    commitIndex*: Atomic[uint64]
    lastApplied*: Atomic[uint64]
    state*: Atomic[RaftState]
    
    # Leader state
    nextIndex*: Table[ReplicaID, uint64]
    matchIndex*: Table[ReplicaID, uint64]
    
    # Lease state
    lease*: Atomic[Lease]
    
    # Thread safety
    lock*: Lock
    
  RaftState* = enum
    rsFollower
    rsCandidate
    rsLeader
```

### 3.2 Multi-Raft Coordinator

A single node hosts multiple Raft groups:

```nim
type
  MultiRaftCoordinator* = ref object
    ## Manages all Raft groups on a single node
    nodeId*: NodeID
    groups*: Table[RangeID, RaftGroup]
    transport*: RaftTransport
    store*: WiscKeyBackend
    
    # Lease management
    storeLiveness*: StoreLiveness
    
    # Thread pools
    raftWorkers*: seq[Thread[void]]
    proposalWorkers*: seq[Thread[void]]
    
    # Synchronization
    groupsLock*: RwLock
    proposalQueue*: Channel[Proposal]
    
  Proposal* = object
    ## A pending proposal to a Raft group
    rangeId*: RangeID
    command*: RaftCommand
    callback*: proc(result: RaftResult) {.closure, gcsafe.}
```

### 3.3 Store Liveness (Leader Leases)

Fractio uses store-level liveness for leader lease management (like CockroachDB v25.2+):

```nim
type
  StoreLiveness* = ref object
    ## Store-level failure detection for leader leases
    nodeId*: NodeID
    stores*: Table[NodeID, StoreState]
    heartbeatInterval*: Duration
    supportExpiration*: Duration
    
  StoreState* = object
    lastHeartbeat*: Timestamp
    supportedUntil*: Timestamp
```

**Benefits over Epoch-based Leases**:
- No single point of failure (no node liveness range)
- Faster failover (< 1 second vs. seconds)
- Network partitions heal in ~20 seconds
- Simpler mental model (leaseholder = Raft leader)

---

## 4. Log Storage

### 4.1 Raft Log Structure

Each Raft group maintains its own log, stored in WiscKey:

```nim
type
  RaftLog* = ref object
    ## Raft log for a single group
    rangeId*: RangeID
    store*: WiscKeyBackend
    firstIndex*: uint64
    lastIndex*: Atomic[uint64]
    lock*: Lock
    
  LogEntry* = object
    ## Single log entry
    term*: uint64
    index*: uint64
    command*: RaftCommand
    
  RaftCommand* = object
    case kind*: CommandKind
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
    of ckNoop:
      discard
```

### 4.2 Log Key Encoding

Log entries are stored with keys encoding range and index:

```
/raft/<range_id>/log/<index> -> <encoded_entry>
```

This allows:
- Efficient range scans for log tail
- Prefix deletion during compaction
- Per-range log management

### 4.3 Log Compaction (Snapshots)

When log exceeds threshold, create snapshot:

```nim
type
  Snapshot* = ref object
    ## Snapshot of range state
    rangeId*: RangeID
    raftSnap*: RaftSnapshotMeta
    stateMachineSnap*: seq[byte]  ## Serialized KV state
    
  RaftSnapshotMeta* = object
    lastIncludedIndex*: uint64
    lastIncludedTerm*: uint64
    configuration*: seq[ReplicaDescriptor]
```

---

## 5. RPC Protocol

### 5.1 Raft RPCs

```nim
type
  RaftMessageKind* = enum
    rmkAppendEntries
    rmkAppendEntriesResponse
    rmkRequestVote
    rmkRequestVoteResponse
    rmkSnapshot
    rmkHeartbeat
    rmkMsgAppend
    rmkMsgPropose
    
  RaftMessage* = object
    ## Generic Raft message envelope
    rangeId*: RangeID
    fromReplica*: ReplicaID
    toReplica*: ReplicaID
    term*: uint64
    case kind*: RaftMessageKind
    of rmkAppendEntries:
      prevLogIndex*: uint64
      prevLogTerm*: uint64
      entries*: seq[LogEntry]
      leaderCommit*: uint64
    of rmkAppendEntriesResponse:
      reject*: bool
      rejectHint*: uint64
      lastLogIndex*: uint64
    of rmkRequestVote:
      lastLogIndex*: uint64
      lastLogTerm*: uint64
      candidateEpoch*: uint64
    of rmkRequestVoteResponse:
      voteGranted*: bool
    of rmkSnapshot:
      snapshot*: Snapshot
    else:
      discard
```

### 5.2 Batch Request/Response

Client operations are batched:

```nim
type
  BatchRequest* = object
    ## Batch of KV operations
    header*: RequestHeader
    requests*: seq[Request]
    
  RequestHeader* = object
    txn*: TransactionRecord
    timestamp*: Timestamp
    priority*: int32
    userPriority*: int32
    
  Request* = object
    case kind*: RequestKind
    of rkGet:
      getKey*: seq[byte]
    of rkPut:
      putKey*: seq[byte]
      putValue*: seq[byte]
    of rkDelete:
      deleteKey*: seq[byte]
    of rkScan:
      scanStart*: seq[byte]
      scanEnd*: seq[byte]
      scanLimit*: int64
    of rkEndTxn:
      commit*: bool
```

---

## 6. Distribution Layer

### 6.1 Meta Ranges

Two-level index for range location:

```
meta1: /sys/meta1/<key> -> RangeDescriptor (points to meta2 range)
meta2: /sys/meta2/<key> -> RangeDescriptor (points to data range)
```

**Lookup Process**:
1. Check local meta2 cache for key
2. If miss, query meta1 for meta2 location
3. Query meta2 for data range location
4. Cache result

### 6.2 DistSender

Routes requests to correct leaseholders:

```nim
type
  DistSender* = ref object
    ## Distributes requests across ranges
    nodeDescriptor*: NodeDescriptor
    rangeCache*: RangeCache
    transport*: RaftTransport
    
    # Retry configuration
    maxRetries*: int
    retryBackoff*: Duration
    
  RangeCache* = ref object
    ## Cache of range descriptors
    entries*: Table[RangeID, CacheEntry]
    meta2Cache*: Table[seq[byte], RangeDescriptor]
    lock*: RwLock
```

---

## 7. Rebalancing and Load Distribution

### 8.1 Replica Rebalancing

Automatic rebalancing based on:
- Replica count per node
- CPU utilization (configurable)
- Disk usage

```nim
type
  RebalanceScheduler* = ref object
    ## Schedules replica movements
    allocator*: Allocator
    storePool*: StorePool
    
  Allocator* = ref object
    ## Decides replica placement
    stores*: Table[NodeID, StoreInfo]
    
  StoreInfo* = object
    nodeLocality*: Locality
    replicaCount*: int
    cpuUsage*: float64
    diskUsed*: int64
    diskCapacity*: int64
```

### 8.2 Lease Rebalancing

Leases move to optimize latency:
- Track request locality (where requests come from)
- Periodically evaluate if lease should move
- Transfer lease if beneficial

---

## 8. Configuration Changes

### 9.1 Joint Consensus

For safe membership changes:

```nim
type
  ConfigurationChange* = object
    ## Configuration change command
    rangeId*: RangeID
    changeType*: ChangeType
    replica*: ReplicaDescriptor
    
  ChangeType* = enum
    ctAddVoter
    ctRemoveVoter
    ctAddNonVoter
    ctRemoveNonVoter
    ctPromoteVoter
    ctDemoteVoter
```

**Process**:
1. Leader proposes joint configuration (old + new)
2. Wait for joint config to commit
3. Leader proposes new configuration only
4. Wait for new config to commit
5. Old members not in new config step down

---

## 9. Error Handling

### 10.1 Error Types

```nim
type
  RaftError* = object of CatchableError
  
  NotLeaderError* = object of RaftError
    ## Current node is not the leader
    leaderHint*: Option[NodeID]
  
  RangeNotFoundError* = object of RaftError
    ## Range not found on this node
  
  LeaseExpiredError* = object of RaftError
    ## Lease has expired
  
  QuorumError* = object of RaftError
    ## Cannot achieve quorum
  
  ReplicaUnavailableError* = object of RaftError
    ## Replica circuit breaker tripped
```

### 10.2 Circuit Breakers

Per-replica circuit breakers prevent cascading failures:
- Trip after 60 seconds of unavailability
- Return fast error instead of hanging
- Async probe for recovery

---

## 10. Thread Safety Model

### 11.1 Concurrency Design

```
┌────────────────────────────────────────────────────────────┐
│                    Network Thread                           │
│  (Receives Raft messages, dispatches to workers)           │
└────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────────┐
│                    Raft Worker Pool                         │
│  (Process Raft messages, state machine application)        │
│  - Each worker handles multiple ranges                      │
│  - Lock-free message passing                                │
│  - Per-range locking for state updates                      │
└────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────────┐
│                    Storage Thread                           │
│  (WiscKey writes, log persistence)                          │
│  - Batch writes for efficiency                              │
│  - Async fsync                                              │
└────────────────────────────────────────────────────────────┘
```

### 11.2 Locking Strategy

- **Per-Range Lock**: Protects RaftGroup state
- **Global RwLock**: Protects group map (read-heavy)
- **Lock-Free**: Message queues, atomic counters

---

## 11. Metrics and Monitoring

### 12.1 Key Metrics

```nim
type
  RaftMetrics* = object
    ## Metrics for monitoring
    # Leader metrics
    proposalsCommitted*: Counter
    proposalsPending*: Gauge
    proposalsFailed*: Counter
    
    # Replication metrics
    replicationLag*: Histogram
    logSizeBytes*: Gauge
    
    # Election metrics
    electionsStarted*: Counter
    electionsWon*: Counter
    
    # Lease metrics
    leaseTransfers*: Counter
    leaseErrors*: Counter
    
    # Range metrics
    rangesTotal*: Gauge
    rangesLeader*: Gauge
    rangesReplica*: Gauge
```

---

## 12. Implementation Priorities

### Phase 1: Core Multi-Group Infrastructure
1. RangeDescriptor and RangeID types
2. RaftGroup per-range state machine
3. MultiRaftCoordinator
4. Basic message routing

### Phase 2: Leader Leases
1. StoreLiveness implementation
2. Lease acquisition and renewal
3. Lease transfer protocol

### Phase 3: Distribution Layer
1. Meta ranges
2. DistSender
3. Range cache

### Phase 4: Rebalancing
1. Replica rebalancing
2. Lease rebalancing
3. Range splits/merges

---

## 13. References

- CockroachDB Architecture Documentation
- Raft Paper: "In Search of an Understandable Consensus Algorithm"
- CockroachDB Leader Leases RFC
