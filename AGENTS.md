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

**Remember:** Fractio must handle production loads. Think about concurrent access, memory usage, failure recovery, and monitoring from day one.
