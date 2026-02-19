# AGENTS.md - Guidelines for AI Agents Working on Fractio

## 1. Memory & Context Protocol

**Dual-Stack Memory System** (OpenMemory + ChromaDB):
- **Hot Memory**: Current sub-tasks, plans, local variables → `openmemory.recall_memory_abstract()` at session start; `openmemory_save_memory()` after edits.
- **Cold Memory**: Full codebase index, architecture history → `chroma_search()` when entering new directories.
- **Cleanup**: After feature completion, promote summary to ChromaDB and delete from OpenMemory.

### Using OpenMemory (Short-term Memory)
```nim
# At the start of a session, recall previous context
let memoryAbstract = openmemory.recall_memory_abstract()

# Save important information during work
openmemory_save_memory(
  speaker="agent",
  message="Completed implementation of snapshot tracking in LSMTree",
  context="fractio-storage-snapshots"
)

# Get recent memories for detailed context
let recentMemories = openmemory_get_recent_memories(max_days=3)
```

### Using ChromaDB (Long-term Memory)
```nim
# Create collections for different types of knowledge
chroma_chroma_create_collection(collection_name="fractio-architecture")
chroma_chroma_create_collection(collection_name="fractio-api-docs")

# Add documents to collections
chroma_chroma_add_document_with_metadata(
  collection_name="fractio-architecture",
  document="LSMTree implementation uses WAL for durability and SSTables for persistence",
  metadata=`{"module": "storage", "component": "lsm_tree"}`
)

# Query for relevant information
let results = chroma_chroma_query_documents(
  collection_name="fractio-architecture",
  query_texts=@["How does LSMTree handle snapshots?"],
  n_results=5
)
```

### Memory Cleanup Protocol
After completing a feature:
1. Summarize the work in a memory abstract
2. Add the summary to the appropriate ChromaDB collection
3. Clear temporary OpenMemory entries to avoid clutter

```nim
# Example cleanup after implementing snapshot functionality
let summary = "Implemented snapshot tracking in LSMTree with watermark-based garbage collection"
chroma_chroma_add_document(
  collection_name="fractio-architecture",
  document=summary
)
# OpenMemory cleanup happens automatically after promoting to ChromaDB
```

---

## 2. Build & Test Commands

```bash
# Run all tests (unit, integration, concurrency)
nimble test

# Run a single test file
nim c -r --checks:on -p:src tests/unit/distributed/raft/test_node.nim

# Run with race detector (if enabled)
nim c -r --checks:on -d:release --threads:on -t:on tests/unit/...

# Build the project (library)
nimble build

# Generate documentation
nimble docs
nim doc --project.json src/fractio/core/types.nim

# coverage requires koch
koch test --metrics  # with coverage metrics

# Clean build artifacts
nimble clean
rm -rf tmp/
```

**Single Test Tip**: Find the exact path first with `find tests -name "test_*.nim"` then compile with `-r` to run immediately.

---

## 3. Code Style Guidelines

### Formatting & Naming
- **Indentation**: 2 spaces (no tabs)
- **Procedures/variables**: `camelCase` (e.g., `beginTransaction`, `raftLog`)
- **Types**: `UpperCamelCase` (e.g., `TransactionManager`, `RaftNode`)
- **Filenames**: `snake_case.nim` (e.g., `raft_node.nim`, `mvcc_engine.nim`)
- **Constants**: `SCREAMING_SNAKE_CASE` (e.g., `DEFAULT_TIMEOUT_NS`)

### Imports
- Use absolute imports from `src/fractio` root:
  ```nim
  import fractio/core/types
  import fractio/distributed/raft/node
  import fractio/storage/mvcc
  ```
- Group imports: std lib first, then third-party, then local; separate groups with blank line.
- Avoid `*` wildcard imports.

### Types & Structs
- Prefer `ref object` for large/mutable structures; value objects for small, immutable data.
- Discriminated unions (`case kind: DataType`) for sum types.
- Use `distinct` for new types (e.g., `TransactionID = distinct int64`).

### Error Handling
- All public procs return `FractioError` or `Result[T, FractioError]`.
- Use specific constructors: `syntaxError()`, `transactionError()`, etc. from `fractio/core/errors`.
- Include context string for debugging.
- Never use bare `raise`; wrap all errors.
- Log errors via `fractio/utils/logging` with appropriate level.

### Thread Safety
- Immutable data preferred; share by copying.
- Use `atomic` operations for counters/flags (`atomicInc`, `atomicLoad`).
- Use `Mutex` only for mutating shared structures (e.g., schema changes). Protect with `acquire()`/`release()`.
- Callbacks crossing threads must be `{.closure, gcsafe.}`; never capture GC-managed globals.
- After `joinThread()`, access shared state directly; no manual `GC_fullCollect()`.

### Memory
- `ref object` for large structs; value semantics for small types (< 2 words).
- Explicit `new()` for reference types.
- `pointer` fields for OS mutexes; initialize via `createMutex()`.
- ` destroy` only for non-GC resources; otherwise rely on GC.

---

## 4. Testing Standards

- **100% line coverage required** - test every branch and edge case.
- Framework: Nim `unittest` with `suite`, `test`, `setup`, `teardown`.
- Tests must be deterministic (no `sleep` or wall-clock time); use mock clocks (`uint64` nanosecond time).
- Concurrent tests: spawn threads, use barriers/latches, verify race conditions with `--threads:on -t:on`.
- Save results to `tmp/test_results.txt` only for benchmark runs; otherwise rely on nimble output.
- File organization:
  ```
  tests/
    unit/        # module-level tests
    integration/ # multi-component
    concurrency/ # stress/race tests
  ```

---

## 5. Project Architecture (Quick Reference)

```
src/fractio/
├── core/         # types, errors, fundamental structs
├── storage/      # MVCC engine, WAL, SSTables, indexes
├── distributed/  # sharding, Raft consensus, time sync
├── network/      # RPC, protocols, sockets
├── sql/          # parser, planner (currently stubbed)
└── utils/        # logging, config, metrics
```

**Key Modules**:
- `core/types.nim` - Core domain types (`Row`, `Transaction`, `Shard`)
- `core/errors.nim` - Error hierarchy (`FractioErrorKind`, constructors)
- `storage/mvcc.nim` - Multi-version concurrency control
- `distributed/raft/{types,log,node,udp}.nim` - Raft consensus
- `distributed/sharedtimer.nim` - P2P time synchronization
- `utils/logging.nim` - Structured logger

---

## 6. Critical Gotchas

### Raft
- Base class: `RaftTransport` uses `method ... {.base.}`; overrides use `method name*` (no `override`).
- Start order: set atomic `serverRunning` BEFORE spawning receiver thread.
- Shutdown: call `posix.shutdown(fd, SHUT_RD)` to unblock `recvFrom`, then `joinThread`.
- Socket creation: if both `net` and `posix` imported, use `newSocket(net.AF_INET, net.SOCK_DGRAM, net.IPPROTO_UDP)`.
- `onApply` callback must be `{.closure, gcsafe, raises: [].}`; use node state, not globals.

### MVCC
- All writes create new versions; readers see consistent snapshot via `readSnapshot` timestamp.
- Use `atomic` timestamps from `TimestampProvider`; never `getTime()` directly in transaction path.
- Garbage collect old versions based on oldest active transaction.

### Sharding
- Shard key is immutable; row placement determined at INSERT.
- Consistent hashing ring uses 160-bit hash space (`sha1`).
- Cross-shard queries use scatter-gather; no distributed locks.

---

## 7. Nim-Specific Notes

- Compiler flags: Always `--checks:on` during development; `--define:release` for performance.
- Concurrency: `std/typedthreads` with `createThread(threadVar, workerProc, args)`. Worker proc must be `{.thread.}` and accept typed tuple.
- ORC GC: Sharing `ref` across threads allowed but must pass explicitly; do not capture in `gcsafe` closures.
- Library target: Set `skipDirs` in `.nimble` to exclude `tests`, `benchmarks`, `docs`, `tmp`.

---

## 8. When Stuck

1. **Missing context** → `Read` nearby files first.
2. **Missing dependencies** → Check `fractio.nimble` `requires` section.
3. **Unclear requirements** → Document assumptions, ask user; add `TODO` comments.
4. **Performance vs clarity** → Favor clarity; profile before optimizing.

---

**Remember**: Fractio is production infrastructure. Prioritize correctness, thread safety, and deterministic behavior over cleverness.