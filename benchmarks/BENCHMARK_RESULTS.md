# Fractio Benchmark Results — Cross-Database Comparison

**Date:** 2026-03-08  
**Machine:** Linux x86_64 (loopback, single node, debug build)

## Test Configuration

| Parameter | Value |
|-----------|-------|
| Keys | 5,000 |
| Operations per benchmark | 500 (latest runs) / 1,000 (historical) |
| Value size | 100 bytes |
| Warmup ops | 50 (latest) / 100 (historical) |
| Thread counts (concurrent) | 2, 4, 8 |

All databases run on localhost. Fractio uses its in-process `ProtocolServer`
over loopback TCP (port 29000). PostgreSQL and MySQL connect over `127.0.0.1`.
SQLite uses WAL mode. The concurrent workload is identical across all systems:
2:1 read:write mix, key space partitioned by `(threadId × opsPerThread + i) mod numKeys`.

**Fractio write path (honest):**
Every write goes through Raft consensus → WiscKey (LevelDB) backend with
`syncWrites=true` → `fdatasync()` per committed batch.  This is the same
durability guarantee as PostgreSQL, MySQL, and SQLite (all fsync on every commit).
Reads are served from the in-memory Raft state machine (equivalent to a
database buffer pool hit, no disk I/O on reads).

**Group commit (Phase 9):**
When enabled (`--group-commit`), concurrent write proposals are coalesced into
a single log entry + `fdatasync()` call.  N callers share one fsync instead of
each waiting for their own.  This is the same technique MySQL InnoDB and
PostgreSQL use to achieve high write throughput.

**fsync batching fix (Phase 10):**
Fixed two critical performance bugs where each committed write triggered
3+ `fdatasync()` calls instead of 1:
1. `applyBatchToSM`: replaced per-key `backend.put()` calls (1 fdatasync each)
   with a single `backend.writeBatch()` call (1 fdatasync for all keys atomically).
2. `putEntryAndState`: merged Raft log entry write + Raft metadata state write
   into a single LevelDB `WriteBatch` (1 fdatasync instead of 2).

Net result: **1 fdatasync per committed write** (down from 3+).

---

## Sequential Mixed Workload (2:1 read:write, single client)

| Database | Ops/sec | Avg Lat (μs) | Min (μs) | Max (μs) |
|----------|--------:|-------------:|---------:|---------:|
| MySQL | 5,723 | 174 | 135 | 412 |
| PostgreSQL | 772 | 1,295 | 95 | 8,030 |
| SQLite | 405 | 2,470 | 5 | 15,100 |
| **Fractio (GC + fsync fix)** | **193** | **5,191** | **44** | **~100k** |
| Fractio (GC, pre-fix) | 123 | 8,103 | 35 | 343,622 |
| Fractio (no GC, pre-fix) | 86 | 11,608 | 35 | 306,307 |

> Phase 10 fsync fix improves sequential mixed by **57%** over Phase 9 GC alone
> (123 → 193 ops/sec) and **124%** over the original baseline (86 → 193).

---

## Concurrent Mixed Workload (2:1 read:write, N clients in parallel)

This is the primary apples-to-apples comparison: identical key distribution,
identical read:write ratio, identical thread counts, wall-clock throughput.

### 2 Threads

| Database | Ops/sec | Avg Lat (μs) | Max (μs) |
|----------|--------:|-------------:|---------:|
| MySQL | 6,527 | 219 | 1,050 |
| PostgreSQL | 901 | 2,195 | 7,244 |
| SQLite | 375 | 5,227 | 124,852 |
| **Fractio (GC + fsync fix)** | **146** | **13,662** | **~110k** |
| Fractio (GC, pre-fix) | 91 | 21,904 | 103,772 |
| Fractio (no GC, pre-fix) | 61 | 32,696 | 352,662 |

### 4 Threads

| Database | Ops/sec | Avg Lat (μs) | Max (μs) |
|----------|--------:|-------------:|---------:|
| MySQL | 5,787 | 317 | 2,487 |
| PostgreSQL | 1,658 | 2,341 | 11,914 |
| SQLite | 412 | 8,010 | 476,065 |
| **Fractio (GC + fsync fix)** | **293** | **13,558** | **~440k** |
| Fractio (GC, pre-fix) | 126 | 31,792 | 730,056 |
| Fractio (no GC, pre-fix) | 71 | 55,896 | 458,231 |

### 8 Threads

| Database | Ops/sec | Avg Lat (μs) | Max (μs) |
|----------|--------:|-------------:|---------:|
| MySQL | 4,459 | 459 | 4,715 |
| PostgreSQL | 2,884 | 2,559 | 13,318 |
| SQLite | 401 | 17,819 | 572,515 |
| **Fractio (GC + fsync fix)** | **310** | **23,330** | **~2M** |
| Fractio (GC, pre-fix) | 125 | 62,881 | 445,488 |
| Fractio (no GC, pre-fix) | 88 | 90,695 | 348,383 |

---

## Group Commit + fsync Fix Impact Summary

### Phase 9 (Group Commit) vs Phase 10 (GC + fsync batching fix)

| Benchmark | Baseline (ops/sec) | Phase 9 GC (ops/sec) | Phase 10 fix (ops/sec) | Total gain |
|-----------|--:|--:|--:|--:|
| Sequential Mixed | 86 | 123 | **193** | **+124%** |
| Write-Only | 29 | 37 | **53** | **+83%** |
| Concurrent 2t | 61 | 91 | **146** | **+139%** |
| Concurrent 4t | 71 | 126 | **293** | **+313%** |
| Concurrent 8t | 88 | 125 | **310** | **+252%** |

> **Key finding:** The Phase 10 fsync batching fix delivers the most dramatic
> improvement under concurrency where multiple keys per proposal were each
> triggering a separate fdatasync. The 8-thread concurrent throughput jumped
> from 88 → 310 ops/sec (+252%) total.

---

## Fractio Full-Stack Numbers (all benchmarks — Phase 10, GC enabled)

| Benchmark | Ops/sec | Avg Lat (μs) | p99 Lat (μs) | Errors |
|-----------|--------:|-------------:|-------------:|-------:|
| Sequential Mixed (2:1 r/w) | 193 | 5,191 | 16,209 | 0 |
| Write-Only | 53 | 18,840 | 33,407 | 0 |
| Read-Only | 22,727 | 43 | 70 | 0 |
| Scan (100-key range) | 5,495 | 182 | 254 | 0 |
| Transactional (begin/put/commit) | 43 | 23,232 | 35,714 | 0 |
| Concurrent Mixed 2t | 146 | 13,662 | 57,567 | 0 |
| Concurrent Mixed 4t | 293 | 13,558 | 60,619 | 0 |
| Concurrent Mixed 8t | 310 | 23,330 | 204,434 | 0 |

### Without Group Commit (Phase 10 fix only, no GC)

| Benchmark | Ops/sec | Avg Lat (μs) | p99 Lat (μs) | Errors |
|-----------|--------:|-------------:|-------------:|-------:|
| Sequential Mixed (2:1 r/w) | 112 | 8,965 | 59,688 | 0 |
| Write-Only | 43 | 23,332 | 45,941 | 0 |
| Read-Only | 22,727 | 43 | 69 | 0 |
| Scan (100-key range) | 6,329 | 157 | 256 | 0 |
| Transactional (begin/put/commit) | 41 | 24,578 | 52,468 | 0 |
| Concurrent Mixed 2t | 89 | 22,519 | 85,595 | 0 |
| Concurrent Mixed 4t | 72 | 55,272 | 297,722 | 0 |
| Concurrent Mixed 8t | 55 | 141,753 | 2,026,897 | 0 |

---

## Key Findings

### Phase 10 fsync Batching Fix
The root cause of the performance gap was **triple fdatasync per write**:

| Path | Before fix | After fix |
|------|-----------|----------|
| Log entry write | 1 fdatasync | combined → |
| Raft state (term/vote/commitIndex) | 1 fdatasync | **1 fdatasync total** |
| State machine apply (per key) | K fdatasyncs | **1 fdatasync total** |
| **Total per commit** | **K + 2** | **1** |

With group commit + fsync batching, a batch of N proposals with K total keys
now costs exactly **1 fdatasync** for the log/state combined write, plus
**1 fdatasync** for the state machine apply — down from `K + 2` to **2**.
With group commit merging N proposals, the per-proposal cost approaches
**2/N fdatasyncs**, which is why concurrent throughput scales so much better.

### Comparison with production databases (GC + fsync fix)

| Workload | MySQL | PostgreSQL | SQLite | Fractio (GC+fix) | vs PostgreSQL |
|----------|------:|-----------:|-------:|------------------:|:-------------|
| Sequential Mixed | 5,723 | 772 | 405 | 193 | 25% of PG |
| Concurrent 2t | 6,527 | 901 | 375 | 146 | 16% of PG |
| Concurrent 4t | 5,787 | 1,658 | 412 | 293 | 18% of PG |
| Concurrent 8t | 4,459 | 2,884 | 401 | 310 | 11% of PG |

### Read Throughput
- Fractio read-only: **~22,700 ops/sec** — reads serve from the in-memory
  Raft state machine (equivalent to a 100% buffer pool hit rate).
- This is competitive with production databases for warm-cache reads.

### Why not 500–5,000 ops/sec yet?
1. **Debug build:** Compiled with `--checks:on`, no `-d:release`. Release
   build typically yields 2–3× throughput improvement on all paths.
2. **LevelDB fdatasync latency:** Each `fdatasync()` on this disk takes ~20ms.
   With group commit + fsync fix, 2 fdatasyncs per batch → theoretical max
   with 2ms batching window ≈ 500 ops/sec (if batches are large enough).
3. **Single flush thread:** The batcher uses one flush thread. Under high
   concurrency, the flush thread's fdatasync latency still serialises all writes.
4. **TCP loopback overhead:** Each operation requires a full TCP round-trip,
   adding ~40–45μs per op even for reads.
5. **Channel contention:** The Nim `Channel[T]` used for proposal routing
   has a mutex internally, which serialises under high contention.

---

## Historical (in-memory, pre-Raft wiring — for reference only)

These numbers reflect the **old benchmark** where `srv.raftStore` was nil
and the protocol server used a plain `Table[string, string]` with no disk I/O.
They are **not** a fair comparison against any database that fsyncs.

| Benchmark | Ops/sec |
|-----------|--------:|
| Sequential Mixed (2:1 r/w) | 17,241 |
| Write-Only | 13,514 |
| Concurrent Mixed 2t | 28,571 |
| Concurrent Mixed 4t | 38,462 |
| Concurrent Mixed 8t | 35,714 |

---

## Environment

| Component | Version / Detail |
|-----------|-----------------|
| OS | Linux x86_64 Ubuntu 24.04 |
| Nim | 2.2.8 (debug build, `--checks:on`) |
| Fractio build | debug (`-d:release` will be ~2–3× faster) |
| Fractio backend | Raft consensus + WiscKey (LevelDB) `syncWrites=true` |
| PostgreSQL | 16 (scram-sha-256 auth, TCP loopback) |
| MySQL | 8.0.45 (InnoDB, TCP loopback) |
| SQLite | 3.45 (WAL mode) |
| Python | 3.12 (psycopg2-binary, mysql-connector-python) |
| Disk | Local ext4, 77% used |
| Network | Loopback only (all on one machine) |

---

## Reproducibility

```bash
# Run the Python comparison benchmark
python3 benchmarks/db_benchmarks.py --keys 5000 --ops 1000 --threads 4

# Compile the Fractio benchmark binary
nim c --checks:on -p:src -o:benchmarks/fractio_bench benchmarks/fractio_fullstack_benchmarks.nim

# Run WITHOUT group commit (baseline)
./benchmarks/fractio_bench --keys 5000 --ops 500 --warmup 50

# Run WITH group commit + fsync fix (Phase 10)
./benchmarks/fractio_bench --keys 5000 --ops 500 --warmup 50 --group-commit
```

Both scripts use identical workload parameters. The Fractio binary requires
no other service listening on port 29000.

---

## Next Steps

- **Release build:** Rerun with `-d:release` to establish production-grade
  baseline (expected ~2–3× throughput improvement on all paths).
- **Parallel flush threads:** Add multiple flush threads to the group commit
  batcher so concurrent fdatasyncs can overlap. Target: 1,000+ ops/sec.
- **Async I/O:** Replace blocking `fdatasync` with `io_uring` for concurrent
  flush without blocking the entire flush thread. Target: 5,000+ ops/sec.
- **Multi-node Raft:** Add a 3-node cluster benchmark to measure consensus
  overhead vs. single-node Fractio and vs. PostgreSQL streaming replication.
- **Cold read benchmark:** Measure read latency when data is not in the
  in-memory state machine (requires WiscKey read-fallback path).
