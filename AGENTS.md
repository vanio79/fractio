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
- Callbacks crossing thread boundaries (e.g., `onApply`) must be `{.closure, gcsafe.}`; never capture GC-managed globals

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
- `raft/{types,log,node,udp}.nim` - Raft consensus (state machine, log, transport)

**Testing commands:**
```bash
nimble test          # Run all tests
nimble docs          # Generate documentation
koch test --metrics  # With coverage metrics
```

---

## 13. P2P Time Synchronization

Fractio uses decentralized time sync for cluster-wide unique transaction IDs (no NTP/master).

### Architecture
- `SharedTimer`: orchestrator + ID generation
- `UDPTransport`: production UDP with real sockets
- `PacketCodec`: BOM-based packet serialization (thread-safe)
- `MonotonicTimeProvider`: local monotonic clock

### Protocol (BOM + NTP)
- Request: `[BOM:u16][MSG:u8][t1:u64]` (11 bytes)
- Response: `[BOM:u16][MSG:u8][t2:u64][t3:u64]` (19 bytes)
- Offset: `((t2-t1)+(t3-t4))/2`
- Delay: `(t4-t1)-(t3-t2)` (discard >100ms)
- Filter: 2σ outlier rejection
- Consensus: weighted median (confidence × weight)

### Transaction IDs
- Synced: `[42-bit ms-time][10-bit nodeId][12-bit counter]`
- Fallback: `[16-bit nodeId][48-bit counter]`

### Constants
```nim
DEFAULT_SYNC_INTERVAL = 1_000_000_000  # 1s
REQUEST_TIMEOUT_NS = 100_000_000       # 100ms
MAX_HISTORY_SIZE = 100
OUTLIER_STDDEV_FACTOR = 2.0
MIN_PEERS_FOR_CONSENSUS = 2
MAX_CLOCK_DRIFT_NS = 1_000_000         # 1ms
```

### Integration
- `TM.beginTransaction()` → `timeSync.getTransactionID()`
- `TimestampProvider` → `timeSync.getSynchronizedTime()`
- Scheduler: call `tick()` every `DEFAULT_SYNC_INTERVAL`

### Testing
- Unit: `test_packetcodec.nim` (33 tests), `test_udptransport.nim` (11), `test_sharedtimer.nim` (19)
- Integration: `test_sharedtimer_udp_integration.nim` (9)
- Scalability: 200 peers, 3 ticks ≈ 1.2ms, drift <1ms
- Accuracy: consensus error ~28µs vs naive ~677µs

### Important
- `numericNodeId` must be unique (consistent hashing ring)
- Monitor `getCurrentOffset()`; >1ms drift = network issues
- Peer weights adjustable for trust

### 14. SQL Parser Status

**IMPORTANT**: SQL parser and AST types (`Query`, `SQLElement`, `ConditionRef`, etc.) have been temporarily removed from `types.nim` to unblock distributed features. Reintroduce in dedicated `sql/` module when needed.

### 15. Raft Consensus Implementation

Fractio uses Raft for leader election and log replication. The implementation is split across modules:

- `raft/types.nim` - Core types: `RaftNode`, `RaftLog`, `RaftTransport` (abstract base), message types
- `raft/log.nim` - Write-ahead log with atomic meta persistence, checksums, snapshot support
- `raft/node.nim` - State machine (follower/candidate/leader), election timeouts, heartbeats, commit rule
- `raft/udp.nim` - UDP transport with receiver thread, clean shutdown via `posix.shutdown`

**Key Patterns**
- **gcsafe callbacks**: `onApply` passed to `RaftNode` must be `{.closure, gcsafe, raises: [].}`. Never capture GC-managed globals; use node state directly.
- **Thread safety**: `RaftLog` uses mutex for writes; reads via atomic snapshots. `RaftNode` protects `state`, `term`, `votedFor`, `commitIndex` with lock.
- **Log replication**: Leader appends entry, followers ack, commit when `matchIndex` from majority reaches entry. `lastApplied` drives `onApply`.
- **Snapshots**: Triggered when log grows too large; `createSnapshot(index, term, data)` compacts log to point, `InstallSnapshot` syncs followers.

**Testing**
- `test_node.nim`: 8 scenarios (election, heartbeats, replication, step-down, re-election, snapshots). 100% coverage of node.nim.
- `test_log.nim`: 16 tests (append, truncate, snapshot, recovery, checksums).
- `test_raftudp.nim`: 15 tests (transport codec + integration: send/receive, large messages, errors).

**Gotchas**
- Base class: `RaftTransport` uses `method ... {.base.}`; derived overrides use `method name*` without `override`.
- Start order: set `serverRunning` atomic BEFORE spawning receiver thread.
- Shutdown: call `posix.shutdown(fd, SHUT_RD)` to unblock `recvFrom`, then `joinThread`.
- Socket creation: when both `net` and `posix` are imported, use `newSocket(net.AF_INET, net.SOCK_DGRAM, net.IPPROTO_UDP)`.

---

**Remember:** Fractio must handle production loads. Think about concurrent access, memory usage, failure recovery, and monitoring from day one.
