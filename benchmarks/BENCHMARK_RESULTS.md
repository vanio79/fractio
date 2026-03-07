# Fractio Benchmark Results — Cross-Database Comparison

**Date:** 2026-03-07  
**Machine:** Linux x86_64 (loopback, single node, debug build)

## Test Configuration

| Parameter | Value |
|-----------|-------|
| Keys | 5,000 |
| Operations per benchmark | 1,000 |
| Value size | 100 bytes |
| Warmup ops | 100 |
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

---

## Sequential Mixed Workload (2:1 read:write, single client)

| Database | Ops/sec | Avg Lat (μs) | Min (μs) | Max (μs) |
|----------|--------:|-------------:|---------:|---------:|
| MySQL | 5,655 | 176 | 135 | 484 |
| PostgreSQL | 449 | 2,226 | 100 | 90,927 |
| **Fractio (Raft+WiscKey)** | **115** | **8,666** | **34** | 943,169 |
| SQLite | 173 | 5,779 | 5 | 171,974 |

---

## Concurrent Mixed Workload (2:1 read:write, N clients in parallel)

This is the primary apples-to-apples comparison: identical key distribution,
identical read:write ratio, identical thread counts, wall-clock throughput.

### 2 Threads

| Database | Ops/sec | Avg Lat (μs) | Max (μs) |
|----------|--------:|-------------:|---------:|
| MySQL | 7,451 | 192 | 2,503 |
| PostgreSQL | 371 | 5,348 | 181,804 |
| SQLite | 266 | 7,042 | 485,036 |
| **Fractio (Raft+WiscKey)** | **56** | **35,914** | **2,035,495** |

### 4 Threads

| Database | Ops/sec | Avg Lat (μs) | Max (μs) |
|----------|--------:|-------------:|---------:|
| MySQL | 4,723 | 477 | 3,989 |
| PostgreSQL | 780 | 5,069 | 160,058 |
| SQLite | 252 | 14,183 | 714,722 |
| **Fractio (Raft+WiscKey)** | **72** | **55,553** | **460,393** |

### 8 Threads

| Database | Ops/sec | Avg Lat (μs) | Max (μs) |
|----------|--------:|-------------:|---------:|
| MySQL | 3,741 | 779 | 135,906 |
| PostgreSQL | 1,618 | 4,746 | 161,983 |
| SQLite | 312 | 21,212 | 664,278 |
| **Fractio (Raft+WiscKey)** | **59** | **133,561** | **2,316,012** |

---

## Fractio Full-Stack Numbers (all benchmarks)

Measured over the Fractio protocol stack (TCP handshake → Raft propose →
WiscKey fdatasync → in-memory SM → response). Reads serve from in-memory
state machine (no disk I/O). Writes go through full Raft consensus + LevelDB
fsync.

| Benchmark | Ops/sec | Avg Lat (μs) | p99 Lat (μs) | Errors |
|-----------|--------:|-------------:|-------------:|-------:|
| Sequential Mixed (2:1 r/w) | 115 | 8,666 | 51,316 | 0 |
| Write-Only | 24 | 41,156 | 95,752 | 0 |
| Read-Only | 18,519 | 53 | 121 | 0 |
| Scan (100-key range) | 3,571 | 279 | 908 | 0 |
| Transactional (begin/put/commit) | 25 | 40,274 | 192,840 | 0 |
| Concurrent Mixed 2t | 56 | 35,914 | 225,932 | 0 |
| Concurrent Mixed 4t | 72 | 55,553 | 306,081 | 0 |
| Concurrent Mixed 8t | 59 | 133,561 | 1,183,298 | 0 |

---

## Key Findings

### Write Throughput (honest, fsync-equivalent)
- Fractio write-only: **~24 ops/sec** — each write goes through Raft consensus
  and `fdatasync()` via LevelDB's sync write path.
- MySQL sequential: **~5,655 ops/sec** — InnoDB has a highly optimised group
  commit path that batches many transactions per fsync. Fractio currently
  fsyncs individually per write (no group commit yet).
- The performance gap on writes is primarily due to **no group commit** in the
  current Raft implementation. Each `proposeAndWait` results in one fsync.
  MySQL and PostgreSQL batch many writes per fsync via their group commit
  mechanisms.

### Read Throughput
- Fractio read-only: **~18,519 ops/sec** — reads serve from the in-memory
  Raft state machine (equivalent to a 100% buffer pool hit rate).
- This is a realistic read workload for a warm cache. Cold reads from disk
  are not yet benchmarked.

### What makes Fractio writes slower than MySQL
1. **No group commit:** every Raft proposal results in one `fdatasync()`. MySQL
   and PostgreSQL batch hundreds of commits per fsync via group commit.
2. **Single Raft worker thread per write:** proposals queue through a channel to
   a single worker per range. MySQL uses InnoDB's concurrent write path.
3. **Debug build:** compiled with `--checks:on`, no `-d:release`. Release build
   is expected to be 2–3× faster but will not close the gap against MySQL's
   group commit.

### Comparison is now honest
Both Fractio and the SQL databases fsync on every commit. The previous
measurement used an in-memory KV store with no disk I/O, which was
not a fair comparison.

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

# Compile and run the Fractio benchmark (Raft+WiscKey backend)
nim c --checks:on --threads:on -p:src benchmarks/fractio_fullstack_benchmarks.nim
./benchmarks/fractio_fullstack_benchmarks --keys 5000 --ops 1000 --threads 4
```

Both scripts use identical workload parameters. The Fractio binary requires
no other service listening on port 29000.

---

## Next Steps

- **Group commit:** batch multiple Raft proposals into a single fsync to
  approach MySQL's group commit throughput (target: 1,000–5,000 writes/sec).
- **Release build:** rerun with `-d:release` to establish production-grade
  baseline (expected ~2–3× throughput improvement on all paths).
- **Multi-node Raft:** add a 3-node cluster benchmark to measure consensus
  overhead vs. single-node Fractio and vs. PostgreSQL streaming replication.
- **Cold read benchmark:** measure read latency when data is not in the
  in-memory state machine (requires WiscKey read-fallback path).
- **Write-ahead log:** replace LevelDB key-per-write with a batched WAL
  (write multiple entries, single fsync) to implement group commit.
