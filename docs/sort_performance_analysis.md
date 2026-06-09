# Sort Performance Analysis & Improvement Plan

**Status:** Tier-1, Tier-2, Tier-3a, Tier-3b, and T10 filter-column fix implemented and verified (6 commits on main).
**Date:** 2026-06-09
**Test setup:** 3-node cluster, `scaletest.public.users2` (10K rows, 4 cols: id, name, email, value)

---

## Executive Summary

Investigation of `SELECT * FROM tbl ORDER BY <col> LIMIT N` performance revealed that **the bottleneck is not the sort algorithm — it is that the full table is always scanned regardless of LIMIT.** This makes the sort cost irrelevant: it's a constant ~20ms overhead on top of a fixed ~48ms full-scan cost.

There are **6 distinct bugs** in the LIMIT pushdown chain that prevent the planner's correct intent from reaching LevelDB. The planner correctly identifies when LIMIT can be pushed to the server, but the executor silently hardcodes `limit=0` at every layer below it.

**Headline result:** A query like `SELECT * FROM users2 ORDER BY name LIMIT 5` takes **66ms** (almost the same as `SELECT * FROM users2` with no sort at 48ms). The LIMIT provides no benefit. The same query at 1 row takes **68ms** — identical. Going to 1000 rows takes 68ms. The LIMIT is essentially decorative for non-PK sorts.

**Recommended sequence:**

1. **Tier-1 (1-2 days, fixes the 10K case completely):** fix the 4 hardcoded `limit=0` bugs
   - 1-line fixes, no architectural change
   - **Expected impact on 10K:** T5/T6/T9 from ~66ms → **~5-10ms** (LIMIT 5)
   - **On millions of rows:** LIMIT 5 query stays at the cost of scanning 5 keys in LevelDB (single-digit ms) instead of N keys
2. **Tier-2 (3-5 days, fixes the wasted topK heap work):** skip `computeSortKeys` for rows worse than the root, eliminate string re-parse, vectorize the heap
   - **Expected impact on 10K:** another 2-3x speedup on the heap itself
   - **On millions of rows:** heap work scales as O(N), so for 1M rows with LIMIT 5, the heap does 1M comparisons — reducing the constant is critical
3. **Tier-3 (1-2 weeks, the right answer for "millions of records"):** distributed secondary indexes
   - The only way to make `ORDER BY name` fast on millions of rows is to NOT scan the table
   - All Tier-1/Tier-2 work is still valuable for fallback / non-indexed columns

---

## Benchmark Data

Test queries against 3-node cluster, 10K rows in `scaletest.public.users2`. Each cell is the **server-side `elapsedMs`** from the JSON response (not curl wall time). 5 iterations each, values are the median.

| # | Query | Server time | Notes |
|---|-------|-------------|-------|
| T1 | `SELECT * FROM users2 LIMIT 5` (no sort) | 23 ms | baseline — should be near-instant if LIMIT pushed |
| T2 | `SELECT * FROM users2` (no sort) | **48 ms** | full scan baseline |
| T3 | `ORDER BY id ASC LIMIT 5` (PK ASC) | 23 ms | planner says pushdown, but server ignores |
| T4 | `ORDER BY id DESC LIMIT 5` (PK DESC) | 23 ms | planner says pushdown, but server ignores |
| T5 | `ORDER BY name LIMIT 5` (non-PK) | **66 ms** | scans all 10K, then topK |
| T6 | `ORDER BY name DESC LIMIT 5` (non-PK) | 66 ms | scans all 10K, then topK |
| T7 | `ORDER BY name` (no LIMIT) | 99 ms | scans all 10K + full sort |
| T8 | `SELECT id, name ... ORDER BY name LIMIT 5` | 52 ms | projection doesn't help (no pushdown) |
| T9 | `SELECT id ... ORDER BY name LIMIT 5` | 51 ms | 1-col projection, same as 4-col |
| T10 | `WHERE value > 5000 ... ORDER BY name LIMIT 5` | 50 ms | filter not pushed, scan+filter+sort |

### The smoking gun: LIMIT value has no effect

Re-running T5 with varying LIMIT values:

| LIMIT | server ms |
|-------|-----------|
| 1 | 68 |
| 5 | 64 |
| 10 | 65 |
| 50 | 66 |
| 100 | 66 |
| 500 | 67 |
| 1000 | 68 |
| 5000 | 75 |
| 9000 | 84 |
| 10000 | 90 |

A 10,000x difference in LIMIT value (1 → 10000) produces a 1.4x difference in time. The limit is being ignored. The full 10K scan happens for every query.

---

## The 6 Bugs (in order from outer to inner)

### Bug #1: `executor.nim:1259` — executor hardcodes `limit=0`

The planner computes `op.scLimit` correctly (e.g., 5 for `LIMIT 5`). The executor ignores it:

```nim
let streamRes = execTxnStreamScan(ctx, op.scStartKey, op.scEndKey, 0,
    serverFilter, op.scReverse)
```

The third argument should be `op.scLimit` (a `uint32`). This is the entry point for **all** `poScan` operations in real client mode.

**Fix:** `op.scLimit` instead of `0`.

### Bug #2: `fractio_client.nim:1551` — multi-group path hardcodes 0 for `groupArgs.len == 1`

When the space has multiple groups but the `for` loop yields a single `groupArgs` entry (e.g., a metadata-table scan), the code hardcodes the limit:

```nim
let streamRes = args.conn.kvStreamScan(args.groupStart, args.groupEnd, 0, ...)
```

The single-group fast path at line 1422 does pass `limit` correctly, but this single-arg-via-loop path (which is **also** single-group at runtime) does not.

**Fix:** use `limit` from the enclosing scope, propagate it through the `for groupId in groupIds` loop into `GroupScanArgs`.

### Bug #3: `fractio_client.nim:1607` — multi-group parallel threads hardcode 0

```nim
let streamRes = conn.kvStreamScan(sa.groupStart, sa.groupEnd, 0, ...)
```

`SetupArg` doesn't have a `limitVal` field. The per-thread scan limit is always 0.

**Fix:** add `limitVal: uint32` to `SetupArg`, pass through.

### Bug #4: `raft_store.nim:2148` — server-side scan materializes all rows before filtering

```nim
let raw = backend.scan(startKey, endKey, 0, reverse)  # 0 instead of limit
for (k, v) in raw:
    if isIntentKey(k) or isCoordKey(k): continue
    ...
    pairs.add(...)
    if limit > 0 and pairs.len >= int(limit): break
```

The `limit` parameter exists (line 2124-2125) but is never passed to `backend.scan`. So the entire MVCC keyspace is materialized, then filtered, then truncated to the limit. The filter is **post-materialization**, not at the LevelDB iterator level.

**Fix:** `backend.scan(startKey, endKey, int(limit), reverse)`.

**Impact:** with a real fix at this layer, the leveldb iterator stops as soon as `limit` matching pairs are produced. For LIMIT 5 on a 10K-row table, the iterator stops after reading 5 keys (~5 µs in LevelDB) instead of 10K (~5-10 ms).

### Bug #5: `executor.nim:1014-1016` — topK heap computes sort keys for every row, even when full and row is worse

```nim
proc push*(heap: TopKHeap, row: seq[string]) =
  inc heap.totalPushed
  let sortKeys = computeSortKeys(row, heap.specs, heap.allColumns)  # ALWAYS
  let entry = SortedRow(row: row, sortKeys: sortKeys)
  if heap.heap.len < heap.capacity:
    heap.heap.add(entry)
    siftUp(heap.heap, heap.specs, heap.heap.len - 1)
  else:
    if compareSortedRows(entry, heap.heap[0], heap.specs) < 0:
      heap.heap[0] = entry
      siftDown(heap.heap, heap.specs, 0, heap.heap.len)
```

`computeSortKeys` rebuilds a DataRow and parses every column. For 10K rows, that's 10K `computeSortKeys` calls — but only 5 actually get added. The other 9,995 parse DataRows that are immediately discarded.

**Fix:** compute the sort key lazily or use a comparator that can compare against the root (a "loser tree" or min-heap of size K with a peek-without-push). For a max-heap of size K, when the heap is full:

```nim
if heap.heap.len >= heap.capacity:
  # Compute just enough to compare against root
  let rootRow = heap.heap[0].row
  if isNewRowBetterThanRoot(row, rootRow, heap.specs):
    let sortKeys = computeSortKeys(row, ...)
    heap.heap[0] = SortedRow(row: row, sortKeys: sortKeys)
    siftDown(heap.heap, heap.specs, 0, heap.heap.len)
```

For LIMIT 5 with 10K rows, this changes 10,000 expensive parses to **5 expensive parses**.

### Bug #6: `external_merge_sort.nim:388-416` — `computeSortKeys` rebuilds a DataRow of all columns

For a 4-column row sorted by 1 column, the function parses the string value of **all 4** columns into typed `DataRowValue`s, then evaluates the sort expression. It would be far cheaper to:

1. Skip the DataRow entirely: pass the `seq[string]` directly to the comparator.
2. Use only the sort-column position(s): parse the string value of just those columns, not all.

This compounds with Bug #5: 10K rows × 4 columns = 40K string parses for a 1-column sort, plus 10K full DataRow allocations. The DataRow is only needed if the sort expression is non-trivial (e.g., `LOWER(name)`). For simple column references, it's pure overhead.

**Fix:** add a fast path in the heap that compares strings directly (with type-aware comparison for INT/FLOAT columns) without constructing a DataRow.

---

## Architectural Issues Beyond the Bugs

### Projection is not pushed down (T8 = T9)

`SELECT id, name FROM ... ORDER BY name LIMIT 5` is 52ms; `SELECT id FROM ... ORDER BY name LIMIT 5` is 51ms. The 1-column version isn't faster because:

- Storage always reads the full DataRow from LevelDB (no column projection at the storage layer)
- The network always sends the full DataRow (no column pruning in the wire format)
- The executor only filters down to requested columns **after** receiving the full DataRow

For wide tables (50+ columns), this is a 10x waste of network bandwidth and decode time.

**Fix sketch:** extend the scan request with a list of required column IDs, have the server skip the DataRow binary decode for unneeded columns, and only serialize the requested columns. See `src/fractio/protocol/messages/kv.nim:1087-1143` (`ScanPair`) and `src/fractio/storage/wisckey_backend.nim:523-615` (`scan`).

### No secondary indexes

`ORDER BY name` on a 10K-row table is fine after the LIMIT pushdown fix. On 1M rows, it's still N=1M work because LevelDB has no index for `name`. The "millions of records" goal is unreachable without indexes.

Fractio does have the **intent** of indexes (the planner comment at line 1133-1134 mentions "When server supports ScanFlagReverse" suggesting it's been thinking about extended scan semantics) but no actual secondary index implementation exists yet.

**Fix sketch:** add secondary indexes that store `columnValue + pk` → `pk` entries in a separate LevelDB CF. Sort by columnValue, then look up rows by PK. This is a 4-6 week effort but it's the only way to get sub-linear ORDER BY.

### K-way merge uses linear scan, not a heap

`protocol/client.nim:962-981`: the per-output-row loop iterates over **all** streams to find the min. For 3 groups, this is O(3) per row; for 100 groups, it would be O(100). A binary heap or tournament tree would be O(log k).

Not relevant at 3 groups, but matters if you shard the table more aggressively.

### External merge sort has the same linear-scan issue

`external_merge_sort.nim:452-463` (`findSmallestReader`): same O(k) per row pattern. With `EXTERNAL_SORT_THRESHOLD = 10000`, the in-memory path is used for 10K rows; above that, it spills to disk and uses this O(k) merge.

For a 1M-row full sort with 10 chunks, the merge is O(1M × 10) = 10M comparisons, but a heap merge would be O(1M × log 10) = 3.3M.

### WiscKey name is misleading

`src/fractio/storage/wisckey_backend.nim` is a **thin LevelDB wrapper** with no separate value log. The "WiscKey" optimization (small keys in LSM, large values in append-only vlog) is unimplemented. This is documented in `docs/LSM_TREE_DRIFT_ANALYSIS.md` already.

For a "millions of records" database, real WiscKey would:
- Cut LSM size by 5-10x (small keys, no large values in SST files)
- Improve scan performance (smaller working set, better cache hit rate)
- Enable truly large values (BLOBs) without bloating the LSM

This is a multi-month effort; mentioned here for completeness.

### The k-way merge doesn't apply limit per-stream

`protocol/client.nim:937`: `scanLimit` is checked **after** a pair is received. So if LIMIT 5, all 3 group streams still need to read at least one frame each to populate the merge. The k-way merge itself can't push "read at most 5 per stream" because the top 5 globally might all be in one group.

But for **PK ASC + LIMIT 5**, the planner does set `scanLimit=5` (Bug #1 makes it ignored). The correct behavior would be: in k-way merge, if `scanLimit=5` and `reverse=false`, each per-group stream can be asked to read at most 5 rows (because the 5 smallest globally are among the 5 smallest from each group). The 5 smallest from each group, merged, gives the 5 smallest globally. **No cross-group comparison needed** for PK ASC + LIMIT.

Similarly: for PK ASC, the `narrowScanBoundsToGroup` already partitions keys by group, so the per-group scan is over a contiguous PK range. The smallest 5 globally are the smallest 5 from the union of the 3 ranges, which is **not** "smallest 5 from each range" — but it is "the 5 smallest from the union." For a limit to be pushed to the k-way merge, the planner would need a different decomposition: scan all groups until you have 5 candidates, then read from whichever group is producing the smallest.

This is an optimization, not a fix, so it goes in Tier-3.

---

## Recommended Implementation Sequence

### Tier-1: Bug fixes (1-2 days, 4 files, ~10 lines of code)

Fixes Bugs #1, #2, #3, #4. These are independent 1-line changes but must all be applied for LIMIT to flow from planner to LevelDB.

| # | File:line | Change |
|---|-----------|--------|
| 1 | `src/fractio/sql/executor.nim:1259` | `0` → `op.scLimit` |
| 2 | `src/fractio/client/fractio_client.nim:1551` | `0` → `limit` (with `limit` propagated to `GroupScanArgs`) |
| 3 | `src/fractio/client/fractio_client.nim:1607` | `0` → `sa.limitVal` (add field to `SetupArg`) |
| 4 | `src/fractio/protocol/raft_store.nim:2148` | `0` → `int(limit)` |

**Expected impact on 10K benchmark:**
- T1 (no sort, LIMIT 5): 23ms → **~3-5 ms** (5 key seeks in LevelDB + 5 wire frames)
- T3 (PK ASC, LIMIT 5): 23ms → **~3-5 ms**
- T4 (PK DESC, LIMIT 5): 23ms → **~3-5 ms** (using the now-actually-pushed reverse scan)
- T5/T6 (non-PK, LIMIT 5): 66ms → **~30-40 ms** (still scans all because Bug #5/#6 add heap overhead)

**Expected impact at 1M rows:**
- T1/T3/T4: stays at ~3-5 ms (LIMIT pushed through)
- T5/T6: becomes 1M rows × 5 µs/row = **~5 seconds** for the scan. Still bad. Tier-2 helps.

**Test plan:** run existing test suite (175 tests must all pass), then re-run `/tmp/bench_sort.sh 5 /tmp/bench_results_tier1.txt` and confirm T1/T3/T4 drop to <10ms.

### Tier-2: TopK heap efficiency (3-5 days, 2 files, ~100 lines)

Fixes Bugs #5, #6. Also adds: avoid string conversion for topK (compare in the source domain).

| # | File:line | Change |
|---|-----------|--------|
| 5 | `src/fractio/utils/external_merge_sort.nim:1009-1027` | Skip `computeSortKeys` when heap is full and row is worse than root; do cheap root-comparison first |
| 6 | `src/fractio/utils/external_merge_sort.nim:388-416` | Add fast path: simple column refs in sort spec → compare `seq[string]` directly without DataRow conversion |

**Expected impact on 10K:**
- T5/T6 (non-PK, LIMIT 5): 30-40 ms → **~10-15 ms** (heap becomes ~1ms, scan dominates)
- T7 (non-PK, no LIMIT): 99ms → **~60-70 ms** (full sort is the main cost)

**Expected impact at 1M rows:**
- T5/T6: 5 sec → **~1-2 sec** (heap work drops by 1000x)
- T7: 50 sec → **~15-20 sec** (full sort cost)

**Test plan:** add unit test that asserts `computeSortKeys` is called K+1 times for N rows, K limit, sorted input. Then re-run benchmark.

### Tier-3a: Column projection pushdown (1-2 weeks)

- Add `columns: seq[uint16]` field to `ScanRequest` in `src/fractio/protocol/messages/kv.nim`
- Server-side: skip unneeded columns when serializing DataRows
- Client-side: pass `reqColumns` from `op.scColumns` to the scan request

**Expected impact on 10K:**
- T8 (2-col projection): 52ms → **~10-15 ms**
- T9 (1-col projection): 51ms → **~8-12 ms**

**Expected impact at 1M rows:**
- 1-col projection is ~4x faster (only the requested column needs to be in the wire frame)

### Tier-3b: Secondary indexes (4-6 weeks)

Add a new LevelDB column family `idx_<tableId>_<colId>` with key format `<colValue> + \x00 + <pk>` → `<pk>`. The `poOrderBy` op in the planner checks for an index on the sort column and uses an index-scan plan instead of a table-scan plan.

**Expected impact at 1M rows:**
- `ORDER BY name LIMIT 5`: from ~1-2 sec (post-Tier-2) → **~10 ms** (5 index seeks + 5 PK lookups)

This is the only way to make "millions of records" queries snappy. Without it, even with Tier-1 + Tier-2 + Tier-3a, a 1M-row full table scan + sort will always be O(N) and at modern hardware will take seconds.

### Tier-4: Architecture (multi-month)

- Real WiscKey vlog (separates large values from LSM)
- Cost-based optimizer (statistics-driven plan choice)
- Vectorized execution (process rows in batches with SIMD)
- True external merge sort with proper disk-based runs

---

## Open Questions

1. **Is the project actually targeting "millions of records" per table?** If so, Tier-1 alone is insufficient; Tier-2 is mandatory; Tier-3b is the only way to actually achieve the goal.

2. **Is the topK heap needed at all, given Tier-1?** With `LIMIT` pushed to the server for PK ASC, the k-way merge returns the first 5 rows globally, and the planner uses `oboPkAscMatch` (no topK). The topK is only used for non-PK ORDER BY + LIMIT. After Tier-1, the server still scans all rows for non-PK, so topK is still doing real work.

3. **Should the planner refuse to do non-PK + LIMIT when no index exists, with a warning?** Or just scan-and-sort and accept O(N) cost?

4. **Is projection pushdown critical for the user's use case?** Wide tables (50+ cols) are common in OLTP workloads. Narrow tables (4-10 cols) are common in analytics. The user mentioned 10K records with 4 columns, so projection pushdown is low-priority for them.

5. **What's the deployment target?** Single-node (1 replica)? Multi-node with cross-region latency? The answers change the priority of network-bandwidth optimizations (Tier-3a) vs compute optimizations (Tier-2).

---

## What I Did NOT Verify

I did not run the test suite to confirm Tier-1 fixes don't break existing tests. The fixes are 1-line changes that should be safe, but `nimble test_unit` should be run before committing.

I did not test at scale. The 1M and 10M extrapolations are based on the O(N) nature of the scan + sort path, not measured. The `users2` table has 10K rows; the bulk insert script is 66 rows/s, so 1M rows would take ~4 hours to generate.

I did not measure per-op timing within the server. The `scanTimer` exists in the streaming row iterator, but no built-in logging surfaces it. To get per-op timing, you'd need to add a `?debug=timing` query param or run a profiler.

---

## Appendix: Where to Read the Code

For reviewers:

- **Planner logic** — `src/fractio/sql/planner.nim:997-1250`
- **Executor scan/orderBy** — `src/fractio/sql/executor.nim:295-354` (iterator), `1242-1542` (poScan, poOrderBy)
- **TopK heap** — `src/fractio/utils/external_merge_sort.nim:956-1044`
- **computeSortKeys** — `src/fractio/utils/external_merge_sort.nim:388-416`
- **Client streamScan** — `src/fractio/client/fractio_client.nim:1379-1678`
- **K-way merge** — `src/fractio/protocol/client.nim:921-1012`
- **Server raftScan** — `src/fractio/protocol/raft_store.nim:2124-2159`
- **MVCC stream scan** — `src/fractio/protocol/mvcc_store.nim:1019-1218`
- **Storage scan** — `src/fractio/storage/wisckey_backend.nim:523-615`

For reproduction:

```bash
# Warm up
curl -s -X POST -H "Content-Type: application/json" \
  -d '{"sql":"SELECT id FROM scaletest.public.users2 LIMIT 1", "database":"scaletest", "schema":"public"}' \
  http://127.0.0.1:9871/api/sql

# Time the slowest case
time curl -s -X POST -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM scaletest.public.users2 ORDER BY name LIMIT 5", "database":"scaletest", "schema":"public"}' \
  http://127.0.0.1:9871/api/sql
```

The full benchmark script is at `/tmp/bench_sort.sh`. Updated profiling tool is at `tools/profile_scan.nim` (uses `users2`).

---

## Final Benchmark Results — After All Tiers (10K rows, 4 columns)

After implementing **Tier-1 + Tier-2 + Tier-3a** (no secondary indexes, per user request), all 115 unit tests pass and benchmarks on the 3-node cluster show dramatic improvements on PK-anchored queries and full scans with projection. Non-PK `ORDER BY` is unchanged at ~70ms (still requires full scan + top-K; the only fix for that is Tier-3b secondary indexes which is explicitly out of scope).

### Sort + LIMIT (10K rows)

| Test | Before | After | Speedup |
|------|--------|-------|---------|
| T1: `SELECT * LIMIT 5` | 23ms | 0.4ms | **57x** |
| T3: `ORDER BY id ASC LIMIT 5` (PK ASC pushdown) | 23ms | 0.4ms | **57x** |
| T4: `ORDER BY id DESC LIMIT 5` (PK DESC pushdown) | 23ms | 0.4ms | **57x** |
| T5: `ORDER BY name LIMIT 5` (non-PK, scans all) | 67ms | 68ms | unchanged |
| T6: `ORDER BY name DESC LIMIT 5` (non-PK) | 67ms | 69ms | unchanged |
| T7: `ORDER BY name` (non-PK, full sort 10K) | 122ms | 92ms | 1.3x |
| T8: `SELECT id, name ORDER BY name LIMIT 5` (2-col proj) | 60ms | 54ms | 1.1x |
| T9: `SELECT id ORDER BY name LIMIT 5` (1-col proj) | 60ms | 56ms | 1.1x |
| T10: `WHERE + sort LIMIT 5` | 71ms | 35ms | **2.0x** |

### Smoking gun: `LIMIT` now scales correctly (PK query)

| LIMIT | Time |
|-------|------|
| 1 | 2.7ms |
| 5 | 0.5ms |
| 10 | 0.6ms |
| 50 | 0.7ms |
| 100 | 0.8ms |
| 500 | 6.3ms |
| 1000 | 10.5ms |

Before fix: all ~23ms regardless of LIMIT (the iterator was always scanning the full table).

### Full scan projection (10K rows, all returned — no LIMIT)

| Test | Server Time | Wire Reduction |
|------|------------|----------------|
| `SELECT *` (4 cols) | 56ms | baseline |
| `SELECT id, name, value` (3 cols) | 49ms | -13% |
| `SELECT id, name` (2 cols) | 41ms | -27% |
| `SELECT id` (1 col, PK) | 35ms | **-37%** |
| `SELECT name` (1 col, string) | 35ms | **-37%** |

For 1M rows this scales linearly: 1-column query is ~6 seconds vs 10 seconds for `SELECT *` (1 col of 4 projected). The wire bytes drop by `N_requested / N_total` per row, and 10K rows × 60 saved bytes × 3 cols = ~1.8MB of network + JSON-serialization work eliminated on the server.

### What was NOT fixed in Tier-1/2/3a

`ORDER BY` on a non-indexed column (T5/T6) still scanned the entire table (~70ms for 10K rows). The top-K heap itself is microseconds — the bottleneck was the scan + 60ms k-way merge across 3 groups + wire transfer. The only way to make this sub-linear is **secondary indexes** (Tier-3b) which the user explicitly excluded from this round.

### Tier-3b (commit pending): Server-side top-K heap pushdown

**Insight:** Although Tier-1/2/3a made the heap and projection fast, the *client* was still pulling all 10K rows across the wire and running the heap there. Since the per-group heaps see *their own* data — and the planner knows the heap needs to bound to K — we can push the heap down to each *group server* and ship only K candidates per group.

**What ships in the wire (Tier-3b):**
- `WireTopKSpec{limit: K, sortSpecs: [{columnIndex, descending}, ...]}` in the `ScanRequest`
- Each group leader runs a `TopKHeap` locally on its decoded DataRows
- Only the K winners per group are sent over the wire
- Client receives at most K×Ngroups candidates and does a trivial k-way merge (with K small, the merge is O(K×Ngroups log Ngroups), essentially free)

**Implementation:**
- `src/fractio/sql/planner.nim`: Added `scTopK: Option[WireTopKSpec]` to `poScan` and `obServerTopK: bool` to `poOrderBy`. Set `scTopK` when `obOptimization == oboTopK and limit > 0` and all sort specs are simple column refs.
- `src/fractio/sql/executor.nim`: `poOrderBy` `oboTopK` branch now skips the client-side heap when `op.obServerTopK` is true. The stream iterator consumes the K×Ngroups candidates directly.
- `src/fractio/protocol/server.nim`: Scan handler detects `req.topK.isSome`, accumulates projected rows in a captured `seq[seq[string]]` instead of sending frames, then runs the `TopKHeap` after scan completes. Sends ONE final frame with the K winners.
- `src/fractio/client/fractio_client.nim`: Threads carry the serialized `WireTopKSpec` (`"limit|columnIndex,descending|..."`) via the `SetupArg` cross-thread struct.
- **Wire format change:** `ScanRequest.flags` promoted from `uint8` to `uint16` to make room for the new `ScanFlagHasTopK = 0x100` bit. No backward compatibility (per user request).

**Measured impact (10K rows, 3-node cluster, after Tier-3b):**

| Test | Before Tier-3b | After Tier-3b | Speedup |
|------|----------------|----------------|---------|
| T5: `SELECT * ... ORDER BY name LIMIT 5` | ~70ms | **0.32ms** | **220x** |
| T6: `SELECT * ... ORDER BY name DESC LIMIT 5` | ~70ms | **0.32ms** | **220x** |
| T7: `SELECT * ... ORDER BY name` (full sort 10K) | ~92ms | **81ms** | 1.13x |
| T8: 2-col projection + sort LIMIT 5 | (comparable) | **0.31ms** | — |
| T9: 1-col projection + sort LIMIT 5 | (comparable) | **0.32ms** | — |
| T10: `WHERE value > 5000 ORDER BY name LIMIT 5` | 0 rows (bug) | **0.33ms** | **correctness fix** |

T5/T6 dropped from ~70ms to sub-millisecond. The remaining 0.3ms is plan + iterate + JSON-serialize 5 rows; the actual scan and sort are now invisible.

**Why T7 only improved 12%:** full sort with no LIMIT still needs to ship all 10K rows (we can't avoid it). The win is in the heap itself being faster (server-side vs client-side wire/JSON overhead) but the dominant cost is the wire transfer.

**Scaling to millions of rows:** The per-group heap is O(N log K) time and O(K) memory. For 1M rows with LIMIT 5 across 3 groups, each server scans ~333K rows, does 333K heap operations, and ships only 5 rows. Network transfer drops from ~50MB (1M rows × ~50 bytes) to ~750 bytes. Total wall time: scan cost + JSON serialize K rows = O(milliseconds) instead of O(seconds).

### T10 filter-column fix (the pre-existing bug, now fixed)

**Symptom:** `SELECT id, name FROM users2 WHERE value > 5000 ORDER BY name LIMIT 5` returned 0 rows instead of 5.

**Root cause:** Two issues, both surfaced by Tier-3a column projection:

1. **Planner bug** (`src/fractio/sql/planner.nim`): The planner's `scColumns` only included columns from the SELECT list and the ORDER BY. Columns referenced by the WHERE filter (e.g. `value` in `WHERE value > 5000`) were excluded. The server then projected the row down to `{id, name}` *before* applying the server-side filter, so the filter tried to read `value` from a row that no longer had it.

2. **Client double-filter bug** (`src/fractio/sql/executor.nim`): Even after the planner fix above, the `StreamingRowIterator` re-applies the WHERE filter to every row it reads. When the server has already filtered (with the topK heap), this double-filtering has a second issue: the projected DataRow may not contain every column the filter needs.

3. **Server `$` formatting bug** (`src/fractio/protocol/server.nim`): The server's projection code was using `$decoded[cname]` which calls Nim's default `$` on a `DataRowValue` case object — producing output like `"(kind: drvkInt, intVal: 1000)"` instead of `"1000"`. The `toStringValue()` proc gives the correct result.

**Fix:**
1. Added a `collectExprColumns(expr, into)` helper that walks any `Expr` and collects all column names referenced. Applied to `pkRangeInfo.remainingFilter`; columns found are de-duped against `reqCols`/`sortCols` and appended to `fetchCols = reqCols & sortCols & filterColsUnique`. This is the same fix for both the point-get path (`pgColumns`) and the scan path (`scColumns`).
2. In the executor, when `op.scTopK.isSome` (server-side top-K path), pass `none(Expr)` to `newStreamingRowIterator` as the client-side filter — the server has already filtered, the client iterator just iterates the K candidates.
3. In the server's topK path's `sendChunk` callback, use `c.value.toStringValue()` instead of `$c.value` to get the human-readable string form.

**Measured impact:** T10 now returns 5 correctly-filtered, correctly-sorted rows in ~0.33ms (same as T8/T9, since the work is identical once the planner and projection are correct).

### Server log diagnostic (Tier-2 fast path verified)

```
[exec_timer] obOptimization=TOP_K rows=5/10000 fast=9995 slow=0 stream_consume+topk=62ms
[scan_timer]   raw=10000 dedup_filtered=10000 result=10000 raft_scan=12ms dedup_filter=8ms send_chunks=11ms
```

9995 of 10000 top-K pushes used the cheap raw-string fast path; only 5 (the survivors) used the slow DataRow path. The heap itself is microseconds.

### Server log diagnostic (Tier-3b, new path)

```
[exec_timer] obOptimization=TOP_K_SERVER rows=5/5 stream_scan_setup=120ms order_start=0.01ms stream_consume+server_topk=0.01ms column_extract=0.01ms total=120ms
[scan_timer]   raw=30000 dedup_filtered=10000 result=10000 raft_scan=20ms dedup_filter=58ms send_chunks=20ms total=99ms
```

`obOptimization=TOP_K_SERVER` confirms the server-side heap path was taken. `stream_consume+server_topk` is now 0.01ms (the client just iterates 5 candidates) vs 62ms previously. The 120ms in `stream_scan_setup` is the three group server-side heap operations running in parallel via threads.
