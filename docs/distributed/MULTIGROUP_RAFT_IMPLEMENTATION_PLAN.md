# Multi-Group Raft Implementation Plan

## Overview

This document outlines the implementation plan for Fractio's multi-group Raft system, following the design in `MULTIGROUP_RAFT_DESIGN.md`. The implementation is divided into phases with clear milestones and deliverables.

---

## Phase 1: Core Multi-Group Infrastructure (Weeks 1-3)

### 1.1 Range Types and Descriptors

**Files to Create/Modify:**
- `src/fractio/distributed/range/types.nim`
- `src/fractio/distributed/range/descriptor.nim`

**Tasks:**
1. Define `RangeID`, `ReplicaID`, `NodeID` distinct types
2. Implement `RangeDescriptor` with serialization
3. Implement `ReplicaDescriptor` with voter/non-voter types
4. Add range key encoding/decoding utilities
5. Unit tests for all types

**Acceptance Criteria:**
- All types compile with `--checks:on`
- 100% test coverage for serialization
- No memory leaks in valgrind tests

### 1.2 Raft Group Per-Range State Machine

**Files to Create/Modify:**
- `src/fractio/distributed/raft/group.nim`
- `src/fractio/distributed/raft/log.nim`
- `src/fractio/distributed/raft/storage.nim`

**Tasks:**
1. Implement `RaftGroup` with per-range state
2. Create `RaftLog` with WiscKey backend
3. Implement log entry encoding with range prefix
4. Add atomic term/vote storage
5. Implement log truncation and compaction
6. Unit tests for log operations

**Key Implementation:**

```nim
# src/fractio/distributed/raft/log.nim

proc encodeLogKey*(rangeId: RangeID, index: uint64): string =
  ## Encode a log key as: /raft/<range_id>/log/<index>
  result = "/raft/" & $rangeId.uint64 & "/log/" & $index

proc putEntry*(log: RaftLog, entry: LogEntry) =
  ## Store a log entry
  let key = encodeLogKey(log.rangeId, entry.index)
  let value = encodeEntry(entry)
  log.store.put(key, value)

proc getEntry*(log: RaftLog, index: uint64): Option[LogEntry] =
  ## Retrieve a log entry
  let key = encodeLogKey(log.rangeId, index)
  let value = log.store.get(key)
  if value.isSome:
    result = decodeEntry(value.get)
```

### 1.3 Multi-Raft Coordinator

**Files to Create/Modify:**
- `src/fractio/distributed/raft/coordinator.nim`
- `src/fractio/distributed/raft/proposal.nim`

**Tasks:**
1. Implement `MultiRaftCoordinator` with group map
2. Create proposal queue with channel
3. Implement worker thread pool
4. Add group lifecycle (create, destroy)
5. Implement message routing to correct group
6. Integration tests for multi-group operations

**Key Implementation:**

```nim
# src/fractio/distributed/raft/coordinator.nim

type
  MultiRaftCoordinator* = ref object
    nodeId*: NodeID
    groups*: Table[RangeID, RaftGroup]
    groupsLock*: RwLock
    proposalCh*: Channel[Proposal]
    workers*: seq[Thread[void]]
    running*: Atomic[bool]

proc start*(c: MultiRaftCoordinator) =
  ## Start the coordinator and worker threads
  c.running.store(true)
  for i in 0..<c.numWorkers:
    createThread(c.workers[i], workerProc, c)

proc propose*(c: MultiRaftCoordinator, rangeId: RangeID, 
              cmd: RaftCommand): Future[RaftResult] =
  ## Propose a command to a specific range
  let proposal = Proposal(
    rangeId: rangeId,
    command: cmd,
    future: newFuture[RaftResult]()
  )
  c.proposalCh.send(proposal)
  return proposal.future

proc workerProc(c: MultiRaftCoordinator) {.thread.} =
  ## Worker thread processes proposals
  while c.running.load:
    let proposal = c.proposalCh.recv()
    c.processProposal(proposal)
```

### 1.4 Raft Message Types

**Files to Create/Modify:**
- `src/fractio/distributed/raft/message.nim`
- `src/fractio/distributed/raft/codec.nim`

**Tasks:**
1. Define all Raft message types
2. Implement binary encoding/decoding
3. Add message validation
4. Unit tests for codec

---

## Phase 2: Leader Leases (Weeks 4-5)

### 2.1 Store Liveness

**Files to Create/Modify:**
- `src/fractio/distributed/raft/liveness.nim`
- `src/fractio/distributed/raft/heartbeat.nim`

**Tasks:**
1. Implement `StoreLiveness` for failure detection
2. Create heartbeat mechanism between stores
3. Implement support/withdraw protocol
4. Add liveness state persistence
5. Unit tests for liveness transitions

**Key Implementation:**

```nim
# src/fractio/distributed/raft/liveness.nim

type
  StoreLiveness* = ref object
    nodeId*: NodeID
    stores*: Table[NodeID, LivenessState]
    heartbeatInterval*: Duration
    supportExpiration*: Duration
    lock*: Lock

proc heartbeat*(sl: StoreLiveness, fromNode: NodeID) =
  ## Process a heartbeat from another store
  withLock sl.lock:
    let state = sl.stores.mgetOrDefault(fromNode)
    state.lastHeartbeat = getMonotonicTime()
    state.supportedUntil = state.lastHeartbeat + sl.supportExpiration

proc isAlive*(sl: StoreLiveness, nodeId: NodeID): bool =
  ## Check if a store is considered alive
  withLock sl.lock:
    if sl.stores.hasKey(nodeId):
      let state = sl.stores[nodeId]
      return getMonotonicTime() < state.supportedUntil
    return false
```

### 2.2 Lease Management

**Files to Create/Modify:**
- `src/fractio/distributed/raft/lease.nim`

**Tasks:**
1. Implement `Lease` type with expiration
2. Create lease acquisition protocol
3. Implement lease renewal via Raft
4. Add lease transfer mechanism
5. Handle lease expiration
6. Unit tests for lease lifecycle

**Key Implementation:**

```nim
# src/fractio/distributed/raft/lease.nim

type
  LeaseState* = enum
    lsNone
    lsAcquiring
    lsHeld
    lsTransferring
    lsExpired

proc acquireLease*(group: RaftGroup): Future[Lease] =
  ## Acquire lease through Raft
  ## Must be Raft leader
  if group.state.load != rsLeader:
    raise newException(NotLeaderError, "Not the leader")
  
  let lease = Lease(
    leaseholder: group.nodeId,
    startTs: getMonotonicTime(),
    expirationTs: getMonotonicTime() + LEASE_DURATION
  )
  
  # Propose lease acquisition through Raft
  let cmd = RaftCommand(kind: ckAcquireLease, lease: lease)
  let result = await group.propose(cmd)
  
  if result.success:
    group.lease.store(lease)
    return lease
  else:
    raise newException(LeaseError, "Failed to acquire lease")

proc transferLease*(group: RaftGroup, target: NodeID): Future[void] =
  ## Transfer lease to another node
  if group.state.load != rsLeader:
    raise newException(NotLeaderError, "Not the leader")
  
  let cmd = RaftCommand(kind: ckTransferLease, targetNode: target)
  discard await group.propose(cmd)
```

---

## Phase 3: Distribution Layer (Weeks 6-7)

### 3.1 Meta Ranges

**Files to Create/Modify:**
- `src/fractio/distributed/meta/types.nim`
- `src/fractio/distributed/meta/range_cache.nim`
- `src/fractio/distributed/meta/lookup.nim`

**Tasks:**
1. Implement meta1 and meta2 range structures
2. Create range cache with TTL
3. Implement range lookup protocol
4. Add cache invalidation
5. Unit tests for lookup

**Key Implementation:**

```nim
# src/fractio/distributed/meta/lookup.nim

proc lookupRange*(cache: RangeCache, key: seq[byte]): Future[RangeDescriptor] =
  ## Look up the range containing a key
  # Check cache first
  withLock cache.lock:
    for desc in cache.meta2Cache.values:
      if key >= desc.startKey and key < desc.endKey:
        return desc
  
  # Cache miss - query meta2
  let meta2Key = encodeMeta2Key(key)
  let meta2Range = await lookupMeta2Range(cache, meta2Key)
  let desc = await queryRange(cache.transport, meta2Range, meta2Key)
  
  # Update cache
  withLock cache.lock:
    cache.meta2Cache[desc.startKey] = desc
  
  return desc
```

### 3.2 DistSender

**Files to Create/Modify:**
- `src/fractio/distributed/sender.nim`
- `src/fractio/distributed/batch.nim`

**Tasks:**
1. Implement `DistSender` with request routing
2. Create `BatchRequest` handling
3. Implement retry logic with backoff
4. Add request splitting by range
5. Integration tests for routing

**Key Implementation:**

```nim
# src/fractio/distributed/sender.nim

proc send*(ds: DistSender, batch: BatchRequest): Future[BatchResponse] =
  ## Send a batch request to the correct ranges
  # Split batch by range
  let rangeBatches = ds.splitByRange(batch)
  
  var responses: seq[Future[BatchResponse]]
  for rangeId, rangeBatch in rangeBatches:
    let desc = await ds.cache.lookupRange(rangeBatch.key)
    let leaseholder = desc.getLeaseholder()
    responses.add(ds.sendToNode(leaseholder, rangeBatch))
  
  # Wait for all responses
  let results = await all(responses)
  return ds.mergeResponses(results)

proc sendToNode*(ds: DistSender, nodeId: NodeID, 
                 batch: BatchRequest): Future[BatchResponse] =
  ## Send request to a specific node with retry
  var lastError: ref Exception
  for attempt in 0..ds.maxRetries:
    try:
      return await ds.transport.sendRPC(nodeId, batch)
    except NotLeaderError as e:
      # Update cache with leader hint
      ds.cache.updateLeader(batch.rangeId, e.leaderHint)
      lastError = e
    except RaftError as e:
      lastError = e
      sleep(ds.retryBackoff * (1 shl attempt))
  
  raise lastError
```

---

## Phase 4: Rebalancing (Weeks 8-9)

**Note**: Transaction support (MVCC, 2PC) will be implemented as a separate work item.

### 4.1 Replica Rebalancing

**Files to Create/Modify:**
- `src/fractio/distributed/rebalance/allocator.nim`
- `src/fractio/distributed/rebalance/scheduler.nim`

**Tasks:**
1. Implement allocator decision logic
2. Create rebalance scheduler
3. Implement replica addition/removal
4. Add load-based rebalancing
5. Integration tests for rebalancing

### 4.2 Range Splits and Merges

**Files to Create/Modify:**
- `src/fractio/distributed/range/split.nim`
- `src/fractio/distributed/range/merge.nim`

**Tasks:**
1. Implement split trigger and execution
2. Create merge trigger and execution
3. Update meta ranges atomically
4. Handle split/merge during requests
5. Integration tests for splits/merges

---

## Phase 5: Testing and Hardening (Weeks 10-11)

### 5.1 Concurrency Tests

**Files to Create:**
- `tests/concurrency/distributed/raft/test_multigroup_stress.nim`
- `tests/concurrency/distributed/raft/test_election_races.nim`
- `tests/concurrency/distributed/raft/test_lease_transfers.nim`

**Tasks:**
1. Stress test with multiple ranges
2. Election race condition tests
3. Lease transfer under load
4. Network partition simulation
5. Chaos testing framework

### 5.2 Recovery Tests

**Files to Create:**
- `tests/integration/distributed/raft/test_node_recovery.nim`
- `tests/integration/distributed/raft/test_range_recovery.nim`
- `tests/integration/distributed/raft/test_crash_consistency.nim`

**Tasks:**
1. Test node crash and recovery
2. Test range log replay
3. Test crash during split/merge
4. Test crash during configuration change
5. Verify no data loss

---

## Testing Strategy

### Unit Tests
- Each module has corresponding test file
- 100% line coverage required
- Use mock time for deterministic tests
- Test edge cases (empty log, single node, etc.)

### Integration Tests
- Multi-node cluster simulation
- Use fork() for crash recovery tests
- Test with real network stack (localhost)
- Verify linearizability

### Concurrency Tests
- Thread sanitizer enabled
- Multiple threads per test
- Stress test with randomized operations
- Race condition detection

### Performance Tests
- Benchmark throughput (ops/sec)
- Benchmark latency (p50, p99, p999)
- Memory usage under load
- Compare single-group vs multi-group

---

## File Structure

```
src/fractio/distributed/
├── raft/
│   ├── types.nim          # Core Raft types
│   ├── group.nim          # Per-range Raft group
│   ├── log.nim            # Raft log storage
│   ├── storage.nim        # Persistent state
│   ├── message.nim        # RPC message types
│   ├── codec.nim          # Message encoding
│   ├── coordinator.nim    # Multi-raft coordinator
│   ├── proposal.nim       # Proposal handling
│   ├── liveness.nim       # Store liveness
│   ├── heartbeat.nim      # Heartbeat protocol
│   ├── lease.nim          # Lease management
│   └── config.nim         # Configuration changes
├── range/
│   ├── types.nim          # Range types
│   ├── descriptor.nim     # Range descriptor
│   ├── split.nim          # Range splitting
│   └── merge.nim          # Range merging
├── meta/
│   ├── types.nim          # Meta range types
│   ├── range_cache.nim    # Range cache
│   └── lookup.nim         # Range lookup
├── rebalance/
│   ├── allocator.nim      # Allocation decisions
│   └── scheduler.nim      # Rebalance scheduling
├── sender.nim             # DistSender
└── batch.nim              # Batch request handling

tests/
├── unit/distributed/
│   ├── raft/
│   │   ├── test_group.nim
│   │   ├── test_log.nim
│   │   ├── test_lease.nim
│   │   └── test_liveness.nim
│   ├── range/
│   │   └── test_descriptor.nim
│   └── meta/
│       └── test_lookup.nim
├── integration/distributed/
│   └── raft/
│       ├── test_multigroup.nim
│       ├── test_election.nim
│       └── test_recovery.nim
└── concurrency/distributed/
    └── raft/
        ├── test_stress.nim
        └── test_races.nim
```

---

## Dependencies

### Internal Dependencies
- `fractio/storage/wisckey_backend` - Log and state storage
- `fractio/utils/logging` - Structured logging
- `fractio/core/types` - Core types
- `fractio/distributed/sharedtimer` - Time synchronization

### External Dependencies
- Nim standard library (locks, atomics, channels)
- LevelDB (via WiscKey backend)

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Complex state machine bugs | Extensive unit tests, formal verification of critical sections |
| Race conditions | Thread sanitizer, lock ordering documentation, deadlock detection |
| Performance degradation | Benchmarks at each phase, profiling, optimization passes |
| Memory leaks | Valgrind tests, custom allocators for hot paths |
| Network partitions | Partition simulation tests, timeout tuning |

---

## Success Criteria

1. **Correctness**: All tests pass, no data loss under failures
2. **Performance**: Within 10% of single-group throughput per range
3. **Scalability**: Linear scaling up to 1000 ranges per node
4. **Availability**: Automatic failover within 20 seconds
5. **Consistency**: Linearizable reads and writes

---

## Timeline

| Phase | Duration | Start | End |
|-------|----------|-------|-----|
| Phase 1: Core Infrastructure | 3 weeks | Week 1 | Week 3 |
| Phase 2: Leader Leases | 2 weeks | Week 4 | Week 5 |
| Phase 3: Distribution Layer | 2 weeks | Week 6 | Week 7 |
| Phase 4: Rebalancing | 2 weeks | Week 8 | Week 9 |
| Phase 5: Testing | 2 weeks | Week 10 | Week 11 |

**Total Duration**: 11 weeks

---

## Next Steps

1. Review and approve design document
2. Set up development environment
3. Begin Phase 1 implementation
4. Weekly progress reviews
5. Continuous integration setup
