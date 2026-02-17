# MEMORY.md - Important Knowledge for Fractio Development

## Architecture Overview

Fractio is a distributed SQL database built in Nim with the following key components:

### 1. Core Principles
- **MVCC (Multi-Version Concurrency Control)**: Every write creates a new version; readers access snapshots
- **Lock-free reads**: No reader-writer locks; readers never block writers
- **Distributed consensus**: Raft for replication and leader election
- **Sharding**: Consistent hashing over 160-bit space
- **ACID transactions**: Full ACID guarantees across single and multi-shard operations

### 2. Data Flow

```
Client → Query Router → Shard Manager → Transaction Manager → MVCC Storage
                                    ↓
                            Replication Manager (Raft)
                                    ↓
                            Secondary Replicas
```

### 3. Critical Implementation Details

#### MVCC Version Chains
- Each row has a `version` field
- Old versions are retained until no active transaction needs them
- Garbage collection runs periodically based on oldest active snapshot
- Snapshots are identified by timestamp (monotonic increasing)

#### Sharding Algorithm
- Hash function: SHA-1 (160-bit)
- Consistent hashing ring: 2^160 buckets
- Shard keys: Must be immutable; hash of all shard key columns
- Range scans: Use ordered shards based on hash ranges

#### Raft Consensus
- Terms: 64-bit integers
- Leader lease: 150ms (configurable)
- Election timeout: 300-500ms randomized
- Log entries: Commands (INSERT, UPDATE, DELETE, etc.)
- Commit index: Wait for majority of replicas

#### Transaction Lifecycle
1. `beginTransaction()` → Assigns unique timestamp
2. Reads → See snapshot at transaction start time
3. Writes → Buffered in write-set, assigned version = transaction timestamp
4. `commit()` → Two-phase commit for multi-shard, or single-shard fast path
5. After commit → Versions become visible at transaction timestamp

### 4. Common Pitfalls & Solutions

#### Pitfall: Long-running transactions cause version explosion
**Solution**: Monitor transaction age, warn after threshold, force rollback after TTL

#### Pitfall: Hotspots on sequential shard keys
**Solution**: Use salt column or hash-based sharding for sequential workloads

#### Pitfall: Write skew anomalies under snapshot isolation
**Solution**: Use predicate locks or SERIALIZABLE isolation for critical workloads

#### Pitfall: Network partitions causing split-brain
**Solution**: Raft prevents split-brain; nodes without leader reject writes

#### Pitfall: Garbage collection blocking
**Solution**: Run GC in background, mark-and-sweep old versions during low load

### 5. Important Patterns

#### Error Propagation
```nim
proc someOperation(): FractioError =
  let err = validate()
  if isError(err):
    return err
  # proceed
```

#### Atomic Operations
```nim
var counter: Atomic[int]
atomicInc(counter)
let value = atomicLoad(counter)
```

#### Lock-Free Reads
```nim
# Readers use snapshot timestamps, no locks needed
let rows = storage.getTableSnapshot(tableName, tx.readSnapshot)
```

#### Mutex Protection (Schema Changes Only)
```nim
withLock(storage.mutex):
  # Modify shared structure
  storage.tables[tableName] = newTable
```

### 6. Testing Strategy

- **Unit tests**: Each module has dedicated test file with 100% coverage
- **Concurrency tests**: Use `spawn` and `sync` to verify thread safety
- **Integration tests**: Multi-component scenarios (tx across shards, failover)
- **Property tests**: Generate random SQL and verify against expected output
- **Race detection**: Run with `--threads:on --tlsEmulation:off` and ThreadSanitizer

### 7. Known Limitations (As Of)

- No query optimizer (uses naive plan)
- No secondary indexes (only primary key scans)
- No WAL persistence (in-memory only)
- No TLS/encryption for network
- No authentication/authorization
- No backup/restore
- No multi-datacenter replication

### 8. Performance Targets

- **Single-shard read**: < 100µs (lock-free)
- **Single-shard write**: < 1ms (with replication)
- **Cross-shard transaction**: < 10ms (2-phase commit)
- **Throughput**: 100K+ ops/sec per node
- **Linear scalability**: Adding node increases capacity ~proportionally

### 9. Configuration Parameters

```nim
type
  FractioConfig* = object
    nodeId*: string
    raftPort*: uint16 = 5200
    clientPort*: uint16 = 5201
    dataDir*: string = "/var/lib/fractio"
    replicationFactor*: int = 3
    shardCount*: int = 1024
    snapshotRetention*: int = 100
    transactionTimeout*: int64 = 30000 # ms
    electionTimeoutMin*: int = 300
    electionTimeoutMax*: int = 500
```

### 10. Networking Protocols

**Internal (Raft + replication)**: msgpack-encoded binary protocol
**Client (SQL)**: Simple line-based protocol (similar to PostgreSQL)
- Request: `QUERY <sql>\nPARAMS <json>\n`
- Response: `OK <row_count>\nDATA <msgpack>\n` or `ERROR <code> <message>\n`

### 11. Deployment Checklist

- [ ] Configure `nodeId` (unique across cluster)
- [ ] Set `raftPort` and `clientPort` (no conflicts)
- [ ] Create `dataDir` with proper permissions
- [ ] Ensure time synchronization (NTP) across nodes
- [ ] Open firewall ports for inter-node communication
- [ ] Set `replicationFactor` to match cluster size
- [ ] Configure monitoring (metrics endpoint on client port + 1)
- [ ] Test failover scenarios
- [ ] Backup strategy for `dataDir`

### 12. Monitoring Metrics

- `transactions_active`: Number of active transactions
- `transactions_committed`: Total committed
- `transactions_aborted`: Total aborted
- `mvcc_versions`: Total version objects stored
- `shard_primary_leadership`: Whether node is primary for any shard
- `raft_term`: Current Raft term
- `raft_leader`: Current leader ID (empty if none)
- `network_bytes_in/out`: Network traffic
- `gc_pause_seconds`: GC pause time

### 13. Upgrade Path

1. Add new nodes with higher version
2. Rebalance shards gradually
3. Decommission old nodes after all shards moved
4. No downtime if replicationFactor maintained

### 14. Glossary

- **MVCC**: Multi-Version Concurrency Control
- **WAL**: Write-Ahead Log (not yet implemented)
- **Raft**: Consensus algorithm (not RAFT, it's an acronym)
- **Shard**: Horizontal partition of table data
- **Snapshot**: Consistent view of database at point in time
- **Primary**: Leader node for a shard
- **Secondary**: Follower node replicating from primary

---

**Last updated**: Initial project setup
