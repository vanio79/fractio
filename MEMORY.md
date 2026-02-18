# MEMORY.md - Important Knowledge for Fractio Development

## Architecture Overview

Fractio is a distributed SQL database built in Nim.

### Core Principles
- **MVCC**: Every write creates a new version; readers access snapshots
- **Lock-free reads**: No reader-writer locks; readers never block writers
- **Raft consensus**: For replication and leader election
- **Consistent hashing**: 160-bit ring for sharding
- **ACID transactions**: Full guarantees across single and multi-shard ops

### Data Flow
```
Client → Query Router → Shard Manager → Transaction Manager → MVCC Storage
                                     ↓
                             Replication Manager (Raft)
                                     ↓
                             Secondary Replicas
```

### Critical Implementation Details

**MVCC Version Chains**
- Each row has a `version` field (timestamp)
- Old versions retained until no active transaction needs them
- GC runs periodically based on oldest active snapshot
- Snapshots identified by monotonic timestamps

**Sharding Algorithm**
- Hash: SHA-1 (160-bit)
- Ring: 2^160 buckets
- Shard keys: immutable; hash of all key columns
- Range scans: use ordered shard ranges

**Raft Consensus**
- Terms: 64-bit integers
- Leader lease: 150ms (configurable)
- Election timeout: 300-500ms randomized
- Log entries: Commands (INSERT, UPDATE, DELETE)
- Commit: majority of replicas

**Transaction Lifecycle**
1. `beginTransaction()` → unique timestamp
2. Reads → snapshot at transaction start time
3. Writes → buffered, version = transaction timestamp
4. `commit()` → 2PC for multi-shard, fast path for single-shard
5. Versions become visible at transaction timestamp

### Common Pitfalls & Solutions

**Long-running transactions**: Monitor age, warn after threshold, rollback after TTL

**Hotspots on sequential keys**: Use salt column or hash-based sharding

**Write skew**: Use predicate locks or SERIALIZABLE isolation

**Split-brain**: Raft prevents; nodes without leader reject writes

**GC blocking**: Run in background, mark-and-sweep during low load

### Important Patterns

**Error Propagation**
```nim
proc someOperation(): FractioError =
  let err = validate()
  if isError(err): return err
  # proceed
```

**Atomic Operations**
```nim
var counter: Atomic[int]
atomicInc(counter)
let value = atomicLoad(counter)
```

**Lock-Free Reads**
```nim
let rows = storage.getTableSnapshot(tableName, tx.readSnapshot)
```

**Mutex Protection (Schema Changes Only)**
```nim
withLock(storage.mutex):
  storage.tables[tableName] = newTable
```

### Testing Strategy
- **Unit**: 100% coverage per module, deterministic, edge cases
- **Concurrency**: `spawn`/`sync` to verify thread safety
- **Integration**: Multi-component scenarios (cross-shard tx, failover)
- **Race detection**: `--threads:on --tlsEmulation:off` + ThreadSanitizer

### Known Limitations
- No query optimizer (naive plan)
- No secondary indexes (only PK scans)
- No WAL persistence (in-memory only)
- No TLS/encryption
- No auth/authz
- No backup/restore
- No multi-dc replication

### Performance Targets
- Single-shard read: < 100µs (lock-free)
- Single-shard write: < 1ms (with replication)
- Cross-shard transaction: < 10ms (2PC)
- Throughput: 100K+ ops/sec per node
- Linear scalability with node count

### Deployment Checklist
- [ ] Unique `nodeId` across cluster
- [ ] Non-conflicting `raftPort` (5200) and `clientPort` (5201)
- [ ] `dataDir` with proper permissions
- [ ] Time sync (NTP) across nodes
- [ ] Firewall ports for inter-node communication
- [ ] `replicationFactor` matching cluster size
- [ ] Monitoring on client port + 1
- [ ] Test failover scenarios
- [ ] Backup strategy for `dataDir`

### Glossary
- **MVCC**: Multi-Version Concurrency Control
- **WAL**: Write-Ahead Log (not yet implemented)
- **Raft**: Consensus algorithm
- **Shard**: Horizontal partition of table data
- **Snapshot**: Consistent view at point in time
- **Primary**: Leader node for a shard
- **Secondary**: Follower replica
- **NTP**: Network Time Protocol (inspiration)
- **Weighted median**: Outlier-resistant consensus method

---

## Network Layer: PacketCodec and UDPTransport

The P2P time sync implementation includes a production-ready UDP transport with BOM-based packet serialization.

**PacketCodec** (`network/packetcodec.nim`)
- Thread-safe, stateless, bounds-checked
- Endianness: big/little via BOM (0xFEFF / 0xFFFE)
- Request: `[BOM:u16][MSG:u8][t1:u64]` (11 bytes)
- Response: `[BOM:u16][MSG:u8][t2:u64][t3:u64]` (19 bytes)
- Timestamps: cast to uint64 and back (two's complement)
- Methods: read/write uint16/64, timestampToBytes, bytesToBytes, encodeRequest/Response, decodeRequest/Response

**UDPTransport** (`distributed/sharedtimer/udptransport.nim`)
- Real UDP sockets, configurable port
- Server mode: auto-responds to sync requests
- Client mode: `syncRound()` collects offsets
- Thread-safe: mutex serializes `syncRound`
- Features: 100ms timeout, delay filter, statistics

**SharedTimer** (`distributed/sharedtimer/sharedtimer_impl.nim`)
- Default transport: `newUDPTransport(port=0)` (no simulation)
- `tick()` performs sync round, updates consensus
- `getTransactionID()` uses synchronized time or fallback
- States: uninitialized → syncing → synchronized (or failed)

**Raft Transport** (`distributed/raft/`)
- `RaftTransport` abstract base class with virtual `send`, `start`, `close`.
- `RaftUDPTransport`: UDP-based, thread-safe, uses receive thread.
- Clean shutdown: `shutdown(fd, SHUT_RD)` unblocks `recvFrom`.
- Start: set `serverRunning` atomic before launching thread.
- Socket creation: use `newSocket(net.AF_INET, net.SOCK_DGRAM, net.IPPROTO_UDP)` when `posix` imported.
- Test suite: `test_raftudp.nim` – codec + integration (15 tests total).

**Testing**
- `test_packetcodec.nim`: 33 tests (100% coverage)
- `test_udptransport.nim`: 11 tests (lifecycle, stats, threading)
- `test_sharedtimer.nim`: 19 tests (unit, state, consensus)
- `test_sharedtimer_udp_integration.nim`: 9 integration tests
- Scalability: 200 peers, 3 ticks in ~1.2ms, drift <1ms
- Accuracy: consensus error ~28µs vs naive average ~677µs

---

## P2P Time Synchronization

**Algorithm**
- NTP-style 4-timestamp exchange (t1, t2, t3, t4)
- Offset: `((t2 - t1) + (t3 - t4)) / 2`
- Delay: `(t4 - t1) - (t3 - t2)` (discard if >100ms)
- Outliers: filter beyond 2σ from mean
- Consensus: weighted median (confidence × peer weight)

**Transaction ID**
- Synchronized: `[42-bit time(ms)][10-bit nodeId][12-bit counter]`
  - Time: ms since epoch (~139k year wrap)
  - NodeId: from consistent hashing (max 1024)
  - Counter: 4096 IDs/ms/node
- Unsynchronized: `[16-bit nodeId][48-bit counter]`

**Constants**
```nim
DEFAULT_SYNC_INTERVAL = 1_000_000_000  # 1s
REQUEST_TIMEOUT_NS = 100_000_000       # 100ms
MAX_HISTORY_SIZE = 100                 # ring buffer
OUTLIER_STDDEV_FACTOR = 2.0
MIN_PEERS_FOR_CONSENSUS = 2
MAX_CLOCK_DRIFT_NS = 1_000_000         # 1ms
```

**Integration**
- `TransactionManager.beginTransaction()` → `timeSync.getTransactionID()`
- `TimestampProvider` → `timeSync.getSynchronizedTime()`
- Scheduler calls `tick()` every `DEFAULT_SYNC_INTERVAL`

**Limitations & Next**
- No peer weight configuration (all equal)
- No heartbeat/failure detection beyond timeout
- No persisted offset history
- To do: add weights, failure detection, persistence
