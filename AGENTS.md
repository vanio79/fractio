# AGENTS.md - Guidelines for AI Agents Working on Fractio

## 1. Memory & Context Protocol

**Dual-Stack Memory System** (OpenMemory + ChromaDB):
- **Hot Memory**: Current sub-tasks, plans, local variables → `openmemory_recall_memory_abstract()` at session start; `openmemory_save_memory()` after edits.
- **Cold Memory**: Full codebase index, architecture history → `chroma_chroma_query_documents()` when needing architecture knowledge.
- **Cleanup**: After feature completion, promote summary to ChromaDB and clear temporary OpenMemory entries.

### Using OpenMemory (Short-term Memory)
At the start of a session, recall previous context to understand ongoing work:
- `openmemory_recall_memory_abstract()` - Get current memory summary
- `openmemory_save_memory(speaker, message, context)` - Record important milestones
- `openmemory_get_recent_memories(max_days=3)` - Get detailed recent context

### Using ChromaDB (Long-term Memory)
The `fractio-architecture` collection stores verified architecture knowledge:
- `chroma_chroma_query_documents(collection_name="fractio-architecture", query_texts=["How does MVCC work?"])`
- `chroma_chroma_add_document_with_metadata()` - Add new architecture findings

### Memory Cleanup Protocol
After completing a feature:
1. Summarize the work in a memory abstract
2. Add verified findings to `fractio-architecture` collection
3. Update OpenMemory abstract to reflect completion

---

## 2. Build & Test Commands

**IMPORTANT**: All builds MUST use `--mm:atomicArc` (Atomic ARC GC, NOT ORC).

```bash
# Run all tests (unit, integration)
nimble test

# Run unit tests only
nimble test_unit

# Run unit core tests (fast subset)
nimble test_unit_core

# Run integration tests (real infrastructure)
nimble test_integration

# Run a single test file (atomicArc REQUIRED)
nim c -r --checks:on --mm:atomicArc -p:src tests/unit/storage/test_backend.nim

# Build the CLI binary
nim c --mm:atomicArc --threads:on -p:src -o:bin/fractio src/fractio/cli/main.nim

# Build with web dashboard
nimble build_web

# Generate documentation
nimble docs

# Clean build artifacts
nimble clean
rm -rf tmp/
```

### Code Coverage Workflow

```bash
# Run unit tests with coverage
nimble coverage_unit

# Run core tests with coverage (fast subset)
nimble coverage_unit_core

# Clean coverage data
nimble coverage_clean

# Generate coverage summary
nimble coverage_summary

# Full coverage workflow
nimble coverage_clean && nimble coverage_unit_core && nimble coverage_summary
```

Coverage files stored in `/tmp/fractio-coverage/`:
- `/tmp/fractio-coverage/cache_<test>/` - Unique nimcache per test
- `/tmp/fractio-coverage/*.gcda` - Coverage data files
- `/tmp/fractio-coverage/html_core/` - HTML coverage report

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
  import fractio/distributed/raft/nuraft_coordinator
  import fractio/storage/wisckey_backend
  ```
- Group imports: std lib first, then third-party, then local; separate groups with blank line.
- Avoid `*` wildcard imports.

### Types & Structs
- Prefer `ref object` for large/mutable structures; value objects for small, immutable data.
- Discriminated unions (`case kind: DataType`) for sum types.
- Use `distinct` for new types (e.g., `TransactionID = distinct ULID`).
- ULID-based IDs: TransactionID, RowID, ShardID, TableId, SpaceID are all `distinct ULID`.

### Error Handling
- Use result types: `KVOpResult[T]`, `RSResult[T]`, `MvccResult[T]`.
- Use specific constructors from `fractio/core/errors`: `syntaxError()`, `transactionError()`, etc.
- Include context string for debugging.
- Never use bare `raise`; wrap all errors.
- Log errors via `fractio/utils/logging` with appropriate level.

### Thread Safety (AtomicArc)
- **Memory mode**: ALWAYS `--mm:atomicArc`, NOT ORC.
- Immutable data preferred; share by copying.
- Use `Atomic[T]` fields for counters/flags: `.load(moRelaxed)`, `.store(val, moRelaxed)`.
- Use `Lock` only for complex shared structures (buffers, tables).
- **Raw pointers** for cross-thread refs to break cycles: `coordPtr`, `resultPtr`, `transportPtr`.
- **DO NOT**: clear tables while other threads may hold refs.
- Callbacks crossing threads must be `{.closure, gcsafe.}`.
- After `joinThread()`, access shared state directly.

### Memory (AtomicArc)
- `ref object` for large structs; value semantics for small types.
- AtomicArc allows sharing `ref` across threads with atomic reference counting.
- **C malloc/c_free** for NuRaft instances (avoids GC with C++ objects).
- `pointer` fields for OS mutexes; initialize via `createMutex()`.

---

## 4. Testing Standards

- **100% line coverage required** - test every branch and edge case.
- Framework: Nim `unittest` with `suite`, `test`, `setup`, `teardown`.
- Tests must be deterministic (no `sleep` or wall-clock time); use mock clocks (`uint64` nanosecond time).
- Concurrent tests: spawn threads, use barriers/latches, verify race conditions with `--threads:on`.
- File organization:
  ```
  tests/
    unit/        # module-level tests (113 files)
    integration/ # multi-component (31 files)
    concurrency/ # stress/race tests
  ```

---

## 5. Project Architecture (Verified 2026-04-27)

```
src/fractio/
├── core/         # types, errors, MVCC transaction, 2PC, timestamp provider (10 files, 3816 lines)
├── storage/      # WiscKey backend (LevelDB), MVCC engine, garbage collector (6 files, 2983 lines)
├── distributed/  # NuRaft coordinator, multi-group Raft, SharedTimer, system tables, rebalance (47 files, 14154 lines)
├── protocol/     # Wire format, client/server, message types, raft_store, mvcc_store (19 files, 12422 lines)
├── sql/          # Lexer, parser, planner, executor, streaming results (8 files, 5182 lines)
├── network/      # TCP transport, connection pool (1 file, 187 lines)
├── web/          # httpbeast + Nimja + HTMX dashboard (1 file, 715 lines)
├── client/       # FractioClient, routing, SQL client (3 files, 1532 lines)
├── utils/        # Logging, binary serialization, external merge sort (5 files, 1494 lines)
├── di/           # Dependency injection container, mocks, adapters (5 files, 3150 lines)
├── app/          # Bootstrap, production config (2 files, 591 lines)
└── cli/          # Commands, daemonization (3 files, 1160 lines)
```

**Key Files**:
- `core/types.nim` - ULID IDs, ValueRef discriminated union, Row, Transaction
- `core/errors.nim` - FractioError hierarchy with constructors
- `core/kv_interface.nim` - KVOpResult[T], KVStore abstract interface
- `storage/wisckey_backend.nim` - LevelDB C bindings, streaming result sets
- `storage/mvcc/engine.nim` - MVCC version storage, backward iteration
- `distributed/raft/nuraft_coordinator.nim` - NuRaft C++ wrapper, multi-group management
- `distributed/sharedtimer/sharedtimer_impl.nim` - P2P NTP-style time sync
- `distributed/meta/system_tables.nim` - sys.databases, sys.tables, sys.groups, etc.
- `protocol/frame.nim` - Wire protocol (12B header + payload)
- `sql/executor.nim` - Streaming execution, ORDER BY optimization
- `web/dashboard.nim` - httpbeast handler with Nimja templates

---

## 6. Critical Gotchas

### Memory Management (AtomicArc)
- **ALWAYS use `--mm:atomicArc`**, NOT ORC. This is the project standard.
- Raw pointers (`coordPtr`, `resultPtr`, `transportPtr`) to break reference cycles.
- C malloc/c_free for NuRaft instances to avoid GC involvement with C++ objects.
- **DO NOT** clear `Table` or `Deque` while other threads may hold refs (causes SIGSEGV).

### Raft (NuRaft)
- Single TCP port multiplexed across ALL groups (GroupID in frame header).
- NuRaftGroupInstance allocated with `c_malloc/c_free` for AtomicArc safety.
- Global timer thread polls every 5ms for expired timers.
- Shutdown order: stop timer thread FIRST, mark stopped, sleep 500ms, stop transport, destroy instances.
- Valid context tracking prevents callbacks on destroyed contexts.
- Group creation: preferred leader creates, sends JoinGroup RPCs before election timer.

### MVCC
- **Backward iteration**: `seekToLast()` + `prev()` for newest versions first.
- Intent key format: `userKey + \x00\x01 + txnId(16 bytes ULID)`
- Version key format: `userKey + \x00\x00 + timestamp(8 bytes BE)`
- Value format: `MVCC_MAGIC(4) + timestamp(8) + txnULID(16) + delFlag(1) + data`
- Intent resolution: promote to version key on commit, delete on abort.

### Streaming Result Sets
- Producer-consumer pattern with background prefetch thread.
- StreamSharedData: buffer (Deque), bufferLock (Lock), state (Atomic[StreamState]).
- Backpressure: `os.sleep()` when buffer full.
- Cleanup: `joinThread()` + `deinitLock()` + `dealloc(sharedData)`.

### SQL Execution
- `erkStreamingRows` requires iterator pattern (most queries return this, not `erkRows`):
  ```nim
  let iter = execResult.streamIterator
  while iter.hasNextRow():
    let rowOpt = iter.nextRow()
    if rowOpt.isSome: process(rowOpt.get())
  iter.closeIterator()
  ```
- ORDER BY optimization: PK ASC (skip sorting), PK DESC (reverse iteration), full sort.
- LIMIT handling: pushed to scan for PK ASC, applied after sort otherwise.
- DDL forbidden inside explicit transactions.

### Shutdown Pattern
For TCP/UDP transports and threads:
```nim
# 1. Set running flag false
t.running.store(false)

# 2. Shutdown socket to unblock recv/accept
posix.shutdown(fd, SHUT_RDWR)

# 3. Close socket
socket.close()

# 4. Join threads
joinThread(threadVar)
```

---

## 7. Nim-Specific Notes

- **Compiler flags**: Always `--checks:on` during development; `--mm:atomicArc` always; `--threads:on` for concurrent tests.
- **Concurrency**: `std/typedthreads` with `createThread(threadVar, workerProc, args)`. Worker proc must be `{.thread.}` and accept typed tuple.
- **AtomicArc GC**: Required for this project. Allows sharing `ref` across threads. Raw pointers for cycle breaking.
- **Atomic operations**: Use method syntax `x.load(moRelaxed)`, `x.store(val, moRelaxed)` - NOT `atomicLoad()` function.
- **Memory orders**: `moRelaxed` most common; `moRelease` for ready flags.
- **Library target**: `skipDirs` in `.nimble` excludes `tests`, `benchmarks`, `docs`, `tmp`.

---

## 8. Key Patterns Reference

### Result Types
```nim
# KVOpResult[T] - simple object
KVOpResult[T] = object
  isOk*: bool
  val*: T
  err*: string

# Constructors
kvOpOk[T](v)
kvOpErr[T](msg)

# RSResult[T] - discriminated union
RSResult[T] = object
  case isOk*: bool
  of true: value*: T
  of false: error*: RaftStoreError
```

### Wire Protocol Frame
```
[PayloadLen:4 BE][RequestId:4 BE][Flags:2 BE][CRC16:2 BE][MessageType:2 BE][Payload]
```
ULID IDs (TransactionID, GroupID, SpaceID) are 16 raw bytes (no length prefix).

### System Tables
| Table ID | Name | Purpose |
|----------|------|---------|
| 1 | sys.databases | Database catalog |
| 2 | sys.schemas | Schema catalog |
| 3 | sys.tables | Table descriptors |
| 4 | sys.groups | Raft group metadata |
| 5 | sys.nodes | Cluster node registry |
| 6 | sys.settings | Cluster config |
| 7 | sys.spaces | Space catalog |

META_GROUP_ID = ULID with last byte = 1 (all-node replication).

### Web Dashboard
- httpbeast + Nimja + HTMX + Shoelace
- Templates in `src/fractio/web/templates/*.nimja`
- Nimja: `varname="html"` (handler returns Future[void]), `blockToRender="content"` for partials
- HTMX endpoints at `/htmx/*`

---

## 9. When Stuck

1. **Missing context** → Query ChromaDB `fractio-architecture` collection or read nearby files.
2. **Missing dependencies** → Check `fractio.nimble` `requires` section.
3. **Unclear requirements** → Document assumptions, ask user; add `TODO` comments.
4. **Performance vs clarity** → Favor clarity; profile before optimizing.
5. **Thread safety issues** → Check for raw pointers, atomicArc patterns, shutdown order.

---

## 10. Project Statistics

- **Source files**: 113 Nim files
- **Lines of code**: ~47,720
- **Tests**: 175 (113 unit + 31 integration)
- **Dependencies**: nim >= 2.0.0, httpbeast >= 0.4.0, nimja >= 0.1.0, zippy >= 0.10.0, parsetoml >= 0.7.0

---

**Remember**: Fractio is production infrastructure. Prioritize correctness, thread safety, and deterministic behavior over cleverness. Always use `--mm:atomicArc`.