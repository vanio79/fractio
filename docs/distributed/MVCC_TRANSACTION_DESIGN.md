# MVCC and ACID Lockless Optimistic Transactions Design

## Executive Summary

This document describes the design for Fractio's MVCC (Multi-Version Concurrency Control) and ACID lockless optimistic transactions, inspired by CockroachDB's transaction model. The implementation provides serializable isolation level using timestamp-based ordering and optimistic concurrency control without requiring locks.

## 1. Architecture Overview

### 1.1 Core Concepts

Fractio's transaction system follows CockroachDB's approach:

1. **MVCC (Multi-Version Concurrency Control)**: Stores multiple versions of each key-value pair, allowing readers to see consistent snapshots without blocking writers.

2. **Optimistic Transactions**: Transactions proceed without acquiring locks, validating at commit time. This works well when conflicts are rare.

3. **Serializable Isolation**: Uses timestamp ordering to ensure serializable semantics. Transactions are ordered by their commit timestamps.

4. **Shared Timer Integration**: All timestamps are generated from the distributed shared timer, providing a globally consistent ordering across all nodes.

### 1.2 Layer Stack

```
┌─────────────────────────────────────────────────────────────┐
│ SQL Layer                                                   │
│ (Parser, Planner, Executor - translates SQL to KV ops)      │
├─────────────────────────────────────────────────────────────┤
│ Transaction Layer (NEW)                                      │
│ (MVCC Engine, Transaction Manager, Timestamp Provider)      │
├─────────────────────────────────────────────────────────────┤
│ Distribution Layer                                          │
│ (Range routing, DistSender, Meta ranges, gRPC)               │
├─────────────────────────────────────────────────────────────┤
│ Replication Layer                                           │
│ (Multi-Group Raft, Leader Leases, Snapshots)               │
├─────────────────────────────────────────────────────────────┤
│ Storage Layer                                               │
│ (WiscKey backend, MVCC storage, SSTables)                   │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. MVCC Storage Model

### 2.1 Key Encoding

Each MVCC key consists of:
```
<user_key><timestamp_desc> = <key><wall_time><logical_counter>
```

- **user_key**: The original key requested by the user
- **wall_time**: Wall clock time in nanoseconds (int64, descending for writes)
- **logical_counter**: Logical counter for same-wall-time conflicts (int32)

The timestamp is stored in **descending** order for writes so that the most recent version appears first during forward iteration.

### 2.2 Value Format

Each MVCC value contains:
```nim
type MVCCValue* = object
  value*: string          # Actual value data
  timestamp*: Timestamp  # Commit timestamp
  isDeleted*: bool        # Whether this is a deletion marker
  txnId*: TransactionID  # Transaction ID that wrote this (or InvalidTxn)
```

### 2.3 Metadata Key

A special metadata key stores the latest timestamp for each user key:
```
<user_key>\x00 METADATA_SUFFIX
```

This allows quick determination of the latest version without scanning all versions.

### 2.4 Write Intent

When a transaction has uncommitted changes, it writes "intents":
```
<user_key><timestamp> INTENT_SUFFIX = <txn_id><value>
```

Intents act as provisional writes that other transactions must resolve:
- **Read**: If seeing an intent, must push the transaction or wait
- **Write**: If seeing an intent from another transaction, must detect write-write conflict

---

## 3. Transaction Manager

### 3.1 Transaction States

```
                    ┌──────────────────┐
                    │                  │
                    │     PENDING      │
                    │                  │
                    └────────┬─────────┘
                             │ begin
                             ▼
                    ┌──────────────────┐
                    │                  │
                    │     PREPARED     │ (optional, for 2PC)
                    │                  │
                    └────────┬─────────┘
                             │ commit
                             ▼
                    ┌──────────────────┐
                    │                  │
                    │   COMMITTED      │
                    │                  │
                    └────────┬─────────┘
                             │ (after gc)
                             ▼
                    ┌──────────────────┐
                    │                  │
                    │      FINALIZED   │
                    │                  │
                    └──────────────────┘
```

### 3.2 Transaction Structure

```nim
type
  Transaction* = ref object
    id*: TransactionID              # Unique transaction ID
    status*: TransactionStatus      # PENDING, COMMITTED, ABORTED
    startTimestamp*: Timestamp      # Read snapshot timestamp
    
    # For serialization
    priority*: int32                 # Transaction priority (for pushing)
    maxTimestamp*: Timestamp        # Maximum timestamp this txn can read
    
    # Write set - keys modified by this transaction
    writeSet*: WriteSet             # seq[(key, value)]
    
    # Read set - keys read by this transaction  
    readSet*: ReadSet               # seq[key] for serialization checks
    
    # Coordinator info (for distributed 2PC)
    coordinator*: NodeID
    
    # Timing
    createdAt*: Timestamp
    deadline*: Timestamp            # For deadline-based cancellation
  
  TransactionStatus* = enum
    TXN_PENDING
    TXN_PREPARED
    TXN_COMMITTED
    TXN_ABORTED
  
  WriteSet* = object
    entries*: seq[WriteEntry]
  
  WriteEntry* = object
    key*: string
    value*: string
    isDelete*: bool
  
  ReadSet* = object
    keys*: seq[string]
    timestamps*: seq[Timestamp]  # Timestamp each key was read at
```

### 3.3 Timestamp Provider

The timestamp provider uses the shared timer to generate globally consistent timestamps:

```nim
type
  TimestampProvider* = ref object
    timer*: SharedTimer
    lastTimestamp*: Atomic[int64]
    maxOffset*: int64  # Maximum clock offset allowed
  
  TimestampProviderError* = object of CatchableError
```

**Key Design**: Uses a hybrid logical clock (HLC) approach:
- Wall clock time from shared timer
- Logical counter for same-wall-time timestamps
- This provides both causality guarantees and real-time ordering

---

## 4. Optimistic Transaction Protocol

### 4.1 Transaction Lifecycle

```
┌─────────────────────────────────────────────────────────────┐
│ 1. BEGIN TRANSACTION                                       │
│    - Generate unique TransactionID                          │
│    - Acquire start timestamp from TimestampProvider         │
│    - Initialize read/write sets                              │
├─────────────────────────────────────────────────────────────┤
│ 2. READ PHASE                                              │
│    - For each key read:                                     │
│      a. Check for intents from other transactions           │
│      b. If intent: push timestamp or wait                  │
│      c. Otherwise: read latest version ≤ startTimestamp    │
│    - Record each key in readSet                             │
├─────────────────────────────────────────────────────────────┤
│ 3. WRITE PHASE                                             │
│    - For each key written:                                  │
│      a. Check for conflicting intents                       │
│      b. Write intent to storage                             │
│    - Store in writeSet                                      │
├─────────────────────────────────────────────────────────────┤
│ 4. COMMIT PHASE                                            │
│    a. Acquire commit timestamp (must be > startTimestamp)   │
│    b. Validate: check no new versions in readSet          │
│    c. If validation fails: ABORT and clean up intents     │
│    d. If validation passes:                                │
│       - Upgrade intents to committed values                 │
│       - Mark transaction as COMMITTED                       │
│       - Notify waiting transactions                        │
├─────────────────────────────────────────────────────────────┤
│ 5. CLEANUP (if ABORT)                                      │
│    - Remove all intents written by this transaction        │
│    - Release any held resources                            │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 Serialization Conflict Detection

The key to serializable isolation is detecting conflicts at commit time:

1. **Write-Read Conflict (WW)**:
   - Transaction T1 reads key K at timestamp TS1
   - Transaction T2 writes key K at timestamp TS2 > TS1
   - T2's commit is rejected if TS2 ≤ T1's commit timestamp

2. **Write-Write Conflict (WW)**:
   - Two transactions write to the same key
   - Second writer must abort (or retry with new timestamp)

3. **Read-Write Conflict (RW)**:
   - Transaction T1 reads key K at TS1
   - Transaction T2 writes K at TS2 where TS1 < TS2 < T1's commit
   - T1 must abort (or retry)

### 4.3 Timestamp Push (Handling Contention)

When a transaction encounters an intent:

```nim
proc pushTimestamp*(pusher: Transaction, pushee: Transaction): Timestamp =
  # Pusher wants to read/write; pushee has an intent
  
  if pushee.status == TXN_ABORTED:
    # Pushee aborted, can ignore its intent
    return pusher.startTimestamp
  
  if pushee.status == TXN_COMMITTED:
    # Pushee committed, return its commit timestamp
    return pushee.commitTimestamp
  
  # Pushee is PENDING - try to push its timestamp
  # Give pushee a chance to commit quickly
  let newTimestamp = pusher.startTimestamp + 1
  
  if newTimestamp < pushee.maxTimestamp:
    # Push pushee's timestamp forward
    pushee.startTimestamp = newTimestamp
    return newTimestamp
  
  # Cannot push - must wait for pushee to commit/abort
  return NO_TIMESTAMP  # Signal to wait
```

### 4.4 Deadlock Detection

Optimistic transactions can still deadlock. We handle this via:

1. **Timestamp Priority**: Higher timestamp wins (wait-die approach)
2. **Transaction Priority**: Explicit priority field; higher priority can push lower
3. **Deadline**: Transactions can set a deadline; if exceeded, abort

---

## 5. Isolation Level: Serializable

### 5.1 Why Serializable?

Serializable is the strongest isolation level - it guarantees that transactions execute as if sequentially. This prevents:
- **Phantom reads**: New rows appearing between reads
- **Non-repeatable reads**: Same row showing different values
- **Dirty reads**: Reading uncommitted data
- **Write skew**: Two transactions reading and writing overlapping data

### 5.2 Implementation

Serializable is implemented through:

1. **Timestamp Ordering**: All transactions are totally ordered by commit timestamp
2. **Single-Version Reads**: Each read sees exactly one version (not a snapshot)
3. **Commit Timestamp Assignment**: Must be after all read timestamps in the read set

```nim
proc commitTransaction*(txn: Transaction): CommitResult =
  # 1. Acquire commit timestamp
  let commitTs = timestampProvider.acquireCommitTimestamp(txn.startTimestamp)
  
  # 2. Validate read set
  for i, key in txn.readSet:
    let latestVersion = storage.getLatestVersion(key)
    if latestVersion.timestamp > txn.readTimestamps[i]:
      # Conflict detected - another transaction committed after we read
      return err(TransactionAbortedError)
  
  # 3. Upgrade intents to committed values
  for entry in txn.writeSet:
    storage.upgradeIntentToCommitted(entry.key, commitTs)
  
  # 4. Mark committed
  txn.status = TXN_COMMITTED
  txn.commitTimestamp = commitTs
  
  return ok(CommitResult(txnId: txn.id, commitTimestamp: commitTs))
```

---

## 6. Integration with Storage Layer

### 6.1 MVCC Backend Interface

```nim
type
  MVCCStorage* = ref object of RootObj
    ## MVCC-aware storage interface
    backend*: StorageBackend
    timestampProvider*: TimestampProvider
  
  MVCCIterator* = ref object
    storage*: MVCCStorage
    currentKey*: string
    currentValue*: MVCCValue
  
  # Core MVCC operations
  method get*(s: MVCCStorage, key: string, timestamp: Timestamp): GetResult {.base.}
  method put*(s: MVCCStorage, key: string, value: string, txn: Transaction) {.base.}
  method delete*(s: MVCCStorage, key: string, txn: Transaction) {.base.}
  method scan*(s: MVCCStorage, startKey: string, endKey: string, 
               timestamp: Timestamp): seq[KV] {.base.}
  method getLatestVersion*(s: MVCCStorage, key: string): VersionedValue {.base.}
  method resolveIntent*(s: MVCCStorage, key: string, txnId: TransactionID,
                        commit: bool) {.base.}
```

### 6.2 Garbage Collection

Old MVCC versions must be cleaned up to prevent unbounded growth:

```nim
type
  MVCCGarbageCollector* = ref object
    storage*: MVCCStorage
    mvccStats*: MVCCStats
  
  GCPolicy* = object
    minTimestamp*: Timestamp       # Don't collect versions newer than this
    maxVersionsPerKey*: int        # Max versions to keep per key
    deleteQueueSize*: int          # Batch size for GC
  
  # Background GC runs periodically
  method runGC*(gc: MVCCGarbageCollector, policy: GCPolicy) {.base.}
  method collectVersions*(gc: MVCCGarbageCollector, key: string,
                          minTs: Timestamp) {.base.}
```

GC triggers when:
- Version count exceeds `maxVersionsPerKey`
- Explicit `VACUUM` command
- Compaction in storage layer

---

## 7. Distributed Transactions

### 7.1 Two-Phase Commit (2PC)

For transactions spanning multiple ranges/nodes:

```
Phase 1: Prepare
──────────────
┌──────────────┐     ┌──────────────┐
│  Coordinator │────▶│ Participant 1 │
└──────────────┘     └──────────────┘
       │                    │
       │              PREPARE
       │                    │
       ▼                    ▼
┌──────────────┐     ┌──────────────┐
│  Participant │     │ Participant 2 │
│      2       │     └──────────────┘
└──────────────┘
       │
       │         PREPARE
       │─────────────▶ (all participants must vote YES)
       │
       ▼
┌──────────────┐
│   DECISION   │  (all prepared = commit, any aborted = rollback)
└──────────────┘

Phase 2: Commit/Rollback
─────────────────────────
       │
       ▼
┌──────────────┐     ┌──────────────┐
│  Coordinator │────▶│ Participant 1 │
└──────────────┘     └──────────────┘
       │                    │
       │    COMMIT/ABORT   │
       │                    │
       ▼                    ▼
   (notify all participants)
```

### 7.2 Transaction Distribution

Each range operation goes through:
1. **DistSender**: Routes request to correct range
2. **RangeLeaseholder**: Coordinates the operation
3. **Raft Consensus**: Replicates within the range
4. **MVCC Storage**: Applies versioned changes

---

## 8. Error Handling

### 8.1 Transaction Errors

```nim
type
  TransactionError* = object of CatchableError
    code*: TransactionErrorCode
  
  TransactionErrorCode* = enum
    # Abort errors (transaction must restart)
    TE_ABORTED               # Transaction was aborted
    TE_WRITE_CONFLICT        # Write-write conflict
    TE_READ_SNAPSHOT_ERROR  # Read timestamp moved forward
    TE_TIMEOUT               # Transaction deadline exceeded
    
    # Retryable errors (can retry same transaction)
    TE_RETRY                 # Serializable retry needed
    TE txnPushFailure        # Could not push other transaction
    
    # Non-retryable errors
    TE_INVALID_TRANSACTION  # Invalid transaction state
    TE_COMMIT_FAILURE       # Commit failed (e.g., coordinator failure)
```

### 8.2 Retry Logic

```nim
proc executeWithRetry*(op: TransactionOperation): TransactionResult =
  var txn = beginTransaction()
  var retries = 0
  const MAX_RETRIES = 15
  
  while retries < MAX_RETRIES:
    try:
      return op(txn)
    except TransactionError as e:
      if e.code == TE_RETRY or e.code == TE txnPushFailure:
        # Clean up and retry with new transaction
        cleanupTransaction(txn)
        txn = beginTransaction()
        inc retries
        continue
      elif e.code == TE_WRITE_CONFLICT or e.code == TE_READ_SNAPSHOT_ERROR:
        # Backoff and retry
        backoff(randomized)
        cleanupTransaction(txn)
        txn = beginTransaction()
        inc retries
        continue
      else:
        raise
```

---

## 9. Performance Considerations

### 9.1 Optimization Strategies

1. **Batch Writes**: Group multiple writes in a single storage batch
2. **Read Caching**: Cache frequently read keys with their timestamps
3. **Intent Resolution**: Background thread resolves stale intents
4. **Parallel Scans**: Parallel MVCC scans for large range queries
5. **Timestamp Cache**: Cache timestamp provider responses

### 9.2 Monitoring

```nim
type
  TransactionMetrics* = object
    commits*: int64
    aborts*: int64
    retries*: int64
    avgCommitTime*: float64
    writeConflicts*: int64
    readConflicts*: int64
    avgVersionsPerKey*: float64
    gcVersionsCollected*: int64
```

---

## 10. API Surface

### 10.1 Public API

```nim
# Transaction management
proc beginTransaction*(): Transaction
proc commitTransaction*(txn: Transaction): CommitResult
proc abortTransaction*(txn: Transaction)
proc rollbackTransaction*(txn: Transaction)

# Timestamp provider
proc getSharedTimestampProvider*(): TimestampProvider

# MVCC operations
proc mvccGet*(key: string, timestamp: Timestamp): GetResult
proc mvccPut*(key: string, value: string, txn: Transaction): PutResult
proc mvccDelete*(key: string, txn: Transaction): DeleteResult
proc mvccScan*(start: string, end: string, timestamp: Timestamp): ScanResult

# Intent resolution
proc resolveIntent*(key: string, txnId: TransactionID, commit: bool)

# Garbage collection
proc runMVCCGC*(policy: GCPolicy)
proc vacuum*()

# SQL integration
proc executeSQLInTransaction*(sql: string): QueryResult
```

---

## 11. Design Decisions Summary

| Decision | Rationale |
|----------|------------|
| Optimistic concurrency | Better performance when conflicts are rare |
| Serializable only | Simpler correctness; no isolation bugs |
| HLC timestamps | Combines causality with real-time ordering |
| Intent-based writes | Enables non-blocking reads |
| 2PC for distributed | Standard protocol for atomic multi-range |
| Background GC | Prevents unbounded version growth |

---

## 12. Future Extensions

- **Read Committed**: Add weaker isolation for specific workloads
- **Pessimistic Mode**: Lock-based transactions for high-contention
- **SSI**: Serializable Snapshot Isolation for better performance
- **Change Data Capture**: Stream MVCC versions to external systems
