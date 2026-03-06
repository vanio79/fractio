# MVCC Transaction Implementation Plan

## Overview

This document outlines the implementation plan for MVCC and ACID lockless optimistic transactions in Fractio. Follows the design at `docs/distributed/MVCC_TRANSACTION_DESIGN.md`.

## Implementation Phases

### Phase 1: Core MVCC Infrastructure (Week 1-2)

#### 1.1 Timestamp Provider Integration

**Files:**
- `src/fractio/core/timestamp_provider.nim` (NEW)

**Tasks:**
- [ ] Create `TimestampProvider` wrapping shared timer
- [ ] Implement HLC (hybrid logical clock) timestamp generation
- [ ] Implement `acquireCommitTimestamp(startTs)` for monotonic commits
- [ ] Add clock offset validation
- [ ] Unit tests for timestamp ordering

**Deliverables:**
```nim
type
  TimestampProvider* = ref object
    timer*: SharedTimer
    lastTimestamp*: Atomic[int64]
    lastCounter*: Atomic[int32]
    maxOffset*: int64

proc newTimestampProvider*(timer: SharedTimer): TimestampProvider
proc now*(tp: TimestampProvider): Timestamp
proc acquireCommitTimestamp*(tp: TimestampProvider, minTs: Timestamp): Timestamp
```

---

#### 1.2 MVCC Key-Value Storage

**Files:**
- `src/fractio/storage/mvcc/types.nim` (NEW)
- `src/fractio/storage/mvcc/engine.nim` (NEW)
- `src/fractio/storage/mvcc/iterator.nim` (NEW)

**Tasks:**
- [ ] Define MVCC key encoding (`<user_key><timestamp_desc>`)
- [ ] Define MVCC value format with metadata
- [ ] Implement `MVCCEngine` wrapping `StorageBackend`
- [ ] Implement `get(key, timestamp)` - point read
- [ ] Implement `put(key, value, txn)` - write with intent
- [ ] Implement `delete(key, txn)` - delete intent
- [ ] Implement `scan(start, end, timestamp)` - range read
- [ ] Implement `getLatestVersion(key)` for conflict detection

**Deliverables:**
```nim
type
  MVCCKey* = object
    userKey*: string
    timestamp*: Timestamp
    isIntent*: bool

  MVCCValue* = object
    value*: string
    timestamp*: Timestamp
    isDeleted*: bool
    txnId*: TransactionID

  MVCCEngine* = ref object
    backend*: StorageBackend
    timestampProvider*: TimestampProvider

proc mvccGet*(engine: MVCCEngine, key: string, ts: Timestamp): MVCCResult
proc mvccPut*(engine: MVCCEngine, key: string, value: string, txn: Transaction): MVCCResult
proc mvccDelete*(engine: MVCCEngine, key: string, txn: Transaction): MVCCResult
proc mvccScan*(engine: MVCCEngine, startKey, endKey: string, ts: Timestamp): MVCCScanResult
```

---

#### 1.3 Intent Resolution

**Tasks:**
- [ ] Implement `resolveIntent(key, txnId, commit)`
- [ ] Handle transaction push scenarios
- [ ] Add intent waiting mechanism

---

### Phase 2: Transaction Manager (Week 2-3)

#### 2.1 Transaction Core

**Files:**
- `src/fractio/core/transaction.nim` (NEW)
- `src/fractio/core/transaction_manager.nim` (NEW)

**Tasks:**
- [ ] Define `Transaction` type with all fields
- [ ] Define `TransactionStatus` enum
- [ ] Implement `beginTransaction()`
- [ ] Generate transaction IDs
- [ ] Track read/write sets

**Deliverables:**
```nim
type
  Transaction* = ref object
    id*: TransactionID
    status*: TransactionStatus
    startTimestamp*: Timestamp
    priority*: int32
    maxTimestamp*: Timestamp
    writeSet*: WriteSet
    readSet*: ReadSet
    deadline*: Timestamp
    createdAt*: Timestamp

  WriteSet* = object
    entries*: seq[WriteEntry]

  ReadSet* = object
    keys*: seq[string]
    timestamps*: seq[Timestamp]

  WriteEntry* = object
    key*: string
    value*: string
    isDelete*: bool

  TransactionStatus* = enum
    TXN_PENDING
    TXN_PREPARED
    TXN_COMMITTED
    TXN_ABORTED
```

---

#### 2.2 Commit Protocol

**Tasks:**
- [ ] Implement `commitTransaction(txn)`
- [ ] Acquire commit timestamp
- [ ] Validate read set (serializability check)
- [ ] Upgrade intents to committed values
- [ ] Handle commit failures

**Deliverables:**
```nim
proc commitTransaction*(tm: TransactionManager, txn: Transaction): CommitResult
proc validateReadSet*(tm: TransactionManager, txn: Transaction): bool
proc upgradeIntents*(tm: TransactionManager, txn: Transaction, commitTs: Timestamp)
```

---

#### 2.3 Abort and Rollback

**Tasks:**
- [ ] Implement `abortTransaction(txn)`
- [ ] Implement `rollbackTransaction(txn)`
- [ ] Clean up intents on abort
- [ ] Release resources

---

### Phase 3: Conflict Detection (Week 3)

#### 3.1 Serializability Validation

**Tasks:**
- [ ] Implement write-write conflict detection
- [ ] Implement write-read conflict detection
- [ ] Implement read-write conflict detection

#### 3.2 Transaction Push

**Tasks:**
- [ ] Implement timestamp push for waiting transactions
- [ ] Handle priority-based pushing
- [ ] Implement wait/die deadlock prevention

---

### Phase 4: Distributed Transactions (Week 4)

#### 4.1 Two-Phase Commit (2PC)

**Files:**
- `src/fractio/core/two_phase_commit.nim` (NEW)

**Tasks:**
- [ ] Define coordinator/participant roles
- [ ] Implement prepare phase
- [ ] Implement commit/rollback phase
- [ ] Handle coordinator failures

**Deliverables:**
```nim
type
  Coordinator* = ref object
    transaction*: Transaction
    participants*: seq[NodeID]

  Participant* = ref object
    nodeId*: NodeID
    prepared*: bool

proc prepare*(coord: Coordinator): PrepareResult
proc commit*(coord: Coordinator): CommitResult
proc rollback*(coord: Coordinator)
```

---

#### 4.2 Integration with Raft

**Files to modify:**
- `src/fractio/distributed/raft/node.nim`

**Tasks:**
- [ ] Add transaction coordination to Raft commands
- [ ] Ensure atomicity across range boundaries

---

### Phase 5: Garbage Collection (Week 4-5)

#### 5.1 MVCC GC

**Files:**
- `src/fractio/storage/mvcc/garbage_collector.nim` (NEW)

**Tasks:**
- [ ] Define GC policy
- [ ] Implement version collection
- [ ] Add background GC thread
- [ ] Integrate with compaction

**Deliverables:**
```nim
type
  GCPolicy* = object
    minTimestamp*: Timestamp
    maxVersionsPerKey*: int

  GarbageCollector* = ref object
    engine*: MVCCEngine
    running*: Atomic[bool]

proc startGC*(gc: GarbageCollector, policy: GCPolicy)
proc stopGC*(gc: GarbageCollector)
proc collectVersions*(gc: GarbageCollector, key: string, minTs: Timestamp)
```

---

### Phase 6: Testing (Week 5)

#### 6.1 Unit Tests

**Files:**
- `tests/unit/storage/test_mvcc_engine.nim`
- `tests/unit/core/test_transaction.nim`
- `tests/unit/core/test_timestamp_provider.nim`

**Tasks:**
- [ ] Test MVCC get/put/delete/scan
- [ ] Test transaction begin/commit/abort
- [ ] Test serializability validation
- [ ] Test timestamp ordering

#### 6.2 Integration Tests

**Files:**
- `tests/integration/storage/test_mvcc_transactions.nim`

**Tasks:**
- [ ] Test single-node transactions
- [ ] Test write-write conflicts
- [ ] Test serializable isolation
- [ ] Test rollback behavior
- [ ] Test GC behavior

---

## File Structure

```
src/fractio/
├── core/
│   ├── transaction.nim
│   ├── transaction_manager.nim
│   ├── timestamp_provider.nim
│   └── two_phase_commit.nim
└── storage/
    └── mvcc/
        ├── types.nim
        ├── engine.nim
        ├── iterator.nim
        └── garbage_collector.nim
```

## Dependencies

- `sharedtimer` - timestamp generation
- `storage/backend` - storage interface
- `storage/wisckey` - existing storage
- `distributed/raft` - for distributed tx
- `utils/logging` - logging

## Success Criteria

- [ ] All unit tests pass
- [ ] Integration tests pass
- [ ] Serializable isolation verified
- [ ] ACID properties maintained
- [ ] Performance: >10K txn/sec single-node
- [ ] GC prevents unbounded growth

## Timeline

| Phase | Duration | Deliverables |
|-------|----------|---------------|
| Phase 1 | 2 weeks | Timestamp provider, MVCC storage |
| Phase 2 | 1 week | Transaction manager |
| Phase 3 | 1 week | Conflict detection |
| Phase 4 | 1 week | Distributed 2PC |
| Phase 5 | 1 week | Garbage collection |
| Phase 6 | 1 week | Testing, polish |

**Total: 7 weeks**
