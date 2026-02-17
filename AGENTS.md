# AGENTS.md - Guidelines for AI Agents Working on Fractio

## Project Overview

**Fractio** is a distributed SQL database implemented in Nim, featuring:
- Sharding with consistent hashing
- Replication with Raft consensus
- ACID transactions with MVCC
- Lock-free reads using version chains
- High performance and thread safety

## Core Directives

### 1. Code Quality Standards

**All code must be:**
- Production-ready (no simplified examples)
- Thread-safe (avoid shared mutable state, use atomic operations)
- Fully unit tested with 100% coverage
- Type-safe and compiled with strict Nim checks (`--checks:on`)
- Error-handled with proper FractioError propagation

**Never:**
- Use `// ... rest of code` or `// logic goes here` comments
- Leave unimplemented stubs
- Introduce blocking locks (distributed locks are forbidden unless explicitly requested)
- Compromise on performance for "simplified" examples

### 2. Architecture Principles

**MVCC (Multi-Version Concurrency Control):**
- All writes create new versions
- Readers never block writers, writers never block readers
- Use atomic timestamps from TimestampProvider
- Implement version garbage collection

**Sharding:**
- Use consistent hashing ring (160-bit hash space)
- Shard key must be immutable for row placement
- Range-based sharding for sequential scans
- Cross-shard queries use scatter-gather

**Replication:**
- Primary-secondary topology with failover
- Raft consensus for leader election and log replication
- Synchronous replication for writes
- Automatic rebalancing on node join/leave

**Thread Safety:**
- Immutable data structures preferred
- Use `atomic` operations for counters
- Mutexes only for mutating shared structures (schema changes)
- Each connection gets its own transaction context

### 3. File Organization

```
src/fractio/
├── core/           # Types, errors, fundamental structures
├── sql/            # Parser, planner, optimizer
├── storage/        # MVCC engine, WAL, indexes
├── distributed/    # Sharding, replication, consensus
├── network/        # RPC, protocols, connection handling
└── utils/          # Logging, metrics, config

tests/
├── unit/           # Unit tests for each module
├── integration/    # Multi-component tests
└── concurrency/    # Stress tests, race detection

benchmarks/
└── suite/          # Performance benchmarks

simulations/
└── cluster/        # Multi-node cluster simulations

docs/
├── api/            # API reference
├── architecture/   # Design documents
├── deployment/     # Production deployment guides
└── examples/      # Usage examples
```

### 4. Testing Requirements

- **100% line coverage required** - All code paths must be tested
- Use Nim's built-in `unittest` framework
- Tests must be deterministic (no timing dependencies)
- Include edge cases and error conditions
- Concurrent tests must demonstrate thread safety
- Save latest test results to `tmp/test_results.txt`

### 5. Implementation Order

Build in this sequence:
1. Core types and error handling
2. MVCC storage engine
3. Transaction manager
4. SQL parser (minimal: SELECT, INSERT, CREATE)
5. Query planner and executor
6. Sharding manager
7. Replication (Raft)
8. Network layer
9. Client protocol
10. Integration tests

### 6. Performance Considerations

**Optimization priorities:**
1. Lock-free reads (always)
2. Zero-copy operations where possible
3. Minimize allocations in hot paths
4. Use `seq` instead of `array` for flexibility
5. Cache frequent lookups (e.g., column positions)
6. `{.noSideEffect.}` pragmas for pure functions

**Do NOT optimize prematurely** - write clear code first, then profile.

### 7. Error Handling

- All public procedures return `FractioError` or use `Result[T, FractioError]`
- Check and validate all inputs
- Use specific error types from `errors.nim`
- Log errors with context at appropriate level
- Fail fast on unrecoverable errors

### 8. Distributed Lock Policy

**Absolute Rule:** Never introduce distributed locks (e.g., Zookeeper, etcd) unless:
- User explicitly requests in issue/PR
- Absolutely necessary for correctness (prove impossibility without)
- Documented with justification in code comments

**Alternatives:**
- MVCC for read/write conflicts
- Raft for consensus
- CRDTs for convergent state
- Gossip protocols for metadata

### 9. Nim-Specific Guidelines

**Style:**
- 2-space indentation
- `camelCase` for variables/procs
- `UpperCamelCase` for types
- `snake_case` for filenames
- `[]` for generic parameters (no `<T>`)

**Memory:**
- Use `ref` objects for large structs
- Prefer value semantics for small types
- Explicit `new` for reference types
- `destroy` for cleanup (GC is fine for most cases)

**Concurrency:**
- `async`/`await` for I/O operations
- `atomic` for counters and flags
- `Mutex` from `system` for critical sections
- `spawn` from `threads` for CPU parallelism

### 10. Documentation Requirements

Every public proc/type must have:
- Brief description (1-2 sentences)
- Parameter descriptions
- Return value description
- Raised exceptions (if any)
- Thread safety guarantees
- Example usage (for complex APIs)

Use Nim's doc comments: `##` for descriptions, `#` for inline.

### 11. Git Workflow

- Feature branches: `feature/feature-name`
- Bugfix branches: `fix/bug-description`
- Commit messages: imperative present tense ("Add X", not "Added X")
- Include test coverage changes in PRs
- Ensure `nimble test` passes before pushing

### 12. When Stuck

If you encounter:
1. **Insufficient context** - Use `Read` on nearby files
2. **Missing dependencies** - Check `fractio.nimble`
3. **Unclear requirements** - Document assumptions and ask user
4. **Performance vs simplicity** - Favor simplicity, add TODO comment

---

## Quick Reference

**Key types:**
- `MVCCStorage` - storage engine
- `TransactionManager` - transaction coordinator
- `ShardManager` - shard placement
- `RaftNode` - replication leader
- `Query` - parsed SQL

**Important modules:**
- `types.nim` - All core types
- `errors.nim` - Error definitions
- `mvcc.nim` - Storage engine
- `transaction.nim` - Transaction manager
- `sharding.nim` - Sharding logic
- `raft.nim` - Consensus protocol

**Testing commands:**
```bash
nimble test          # Run all tests
nimble docs          # Generate documentation
koch test --metrics  # With coverage metrics
```

---

## 13. P2P Time Synchronization

Fractio uses a custom P2P time synchronization protocol to generate globally unique transaction IDs without依赖 on NTP or a master node.

### Design Requirements
- Scale to hundreds of nodes
- Clock drift < 1ms
- Fully decentralized (no master)
- Inspired by NTP/PTP but simplified

### Implementation (`src/fractio/distributed/timesync.nim`)

**Key Types:**
- `P2PTimeSynchronizer`: Main orchestrator, one per node
- `PeerConfig`: Defines a peer (address, port, weight)
- `ClockOffset`: Measured offset and delay from a peer
- `TimeSyncState`: tssUninitialized, tssSyncing, tssSynchronized, tssFailed

**Algorithm (per sync tick):**
1. Send request to all peers (simulated or real network)
2. Measure round-trip delay and compute candidate offset per peer using NTP formula:
   - `delay = (t4 - t1) - (t3 - t2)`
   - `offset = ((t2 - t1) + (t3 - t4)) / 2`
3. Discard measurements with delay > 100ms (unreliable)
4. Filter outliers using 2σ standard deviation
5. Compute consensus via weighted median (weight = confidence × peer weight)
6. Update `consensusOffset` if enough peers remain

**Transaction ID Generation:**
- Synchronized mode: `[42-bit time(ms)][10-bit nodeId][12-bit counter]`
  - Time: milliseconds since epoch, covers ~139,000 years
  - NodeId: 10 bits (0-1023, assigned via consistent hashing ring)
  - Counter: 12 bits (4096 IDs per ms per node)
- Unsynchronized fallback: `[16-bit nodeId][48-bit counter]`
  - Guarantees uniqueness without synchronized time

**Thread Safety:**
- `mutex` protects `offsets`, `consensusOffset`, `state`
- `txCounter` is an `Atomic[int]` using `atomicInc`/`load`
- `localClock` is a proc pointer (allows mocking in tests)

**Configuration:**
```nim
const
  DEFAULT_SYNC_INTERVAL = 1_000_000_000'i64  # 1 second
  REQUEST_TIMEOUT_NS = 100_000_000'i64       # 100ms
  MAX_HISTORY_SIZE = 100                     # ring buffer
  OUTLIER_STDDEV_FACTOR = 2.0                # filter beyond 2σ
  MIN_PEERS_FOR_CONSENSUS = 2                # need ≥2
  MAX_CLOCK_DRIFT_NS = 1_000_000'i64         # 1ms drift threshold
```

**Integration Points:**
- Replace `TransactionManager`'s `nextTxId` counter with `timeSync.getTransactionID()`
- Call `synchronizer.tick()` periodically (e.g., via timer every `syncInterval`)
- In `TimestampProvider`, use `getSynchronizedTime()` for MVCC timestamps

**Testing:**
- Unit tests in `tests/test_timesync.nim` (21 tests, 100% coverage)
- Scalability test: 200 peers, 3 ticks in ~1.2ms, drift <1ms
- Accuracy test: consensus reduces error from ~677µs to ~28µs vs naive average
- Run via `nimble test`

**Important:**
- Remove before production: `syncRound` simulation uses local time; implement real RPC in `network/`
- Ensure `numericNodeId` is unique (assign via consistent hashing ring)
- Monitor `getCurrentOffset()`; if drift exceeds 1ms, investigate network issues
- Peer weights can be adjusted for trust (e.g., higher weight for low-latency nodes)

### 14. SQL Parser Status

**IMPORTANT**: The SQL parser and AST types (`Query`, `SQLElement`, `ConditionRef`, etc.) have been temporarily removed from `types.nim` to unblock development on core distributed features. They should be reintroduced in a dedicated `sql/` module when query processing is needed.

---

**Remember:** Fractio must handle production loads. Think about concurrent access, memory usage, failure recovery, and monitoring from day one.
