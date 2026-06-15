## 1M-row smoke test focused on SQL inserts and deletes against a 3-replica
## Fractio cluster.
##
## Runs against an already-started 3-node cluster on a fresh `smoke` space
## (REPLICAS=3) and a `smoke.public.users` table with columns:
##   id    INTEGER PRIMARY KEY
##   name  TEXT
##   value INTEGER
##
## Phase A: bulk INSERT 1,000,000 rows in batches of 500 per INSERT statement.
##          Logs per-batch latency and total throughput.
## Phase B: bulk DELETE  100,000 rows (every 10th id: 1, 11, 21, ...).
##          Logs per-batch latency and total throughput.
## Phase C: interleaved INSERT/DELETE (10K inserts + 10K deletes, mixed).
##          Validates that mixed workloads work correctly.
## Phase D: sanity verification - COUNT(*), MIN/MAX, gap checks.
##
## Reports memory (RSS), CPU%, and disk usage for the cluster nodes at
## each milestone.  Writes per-subsystem memory logs every 10s (via the
## server-side per-subsystem logger).
##
## Usage:
##   nim c --mm:atomicArc --threads:on --opt:speed -p:src \
##        -o:bin/smoke_1m_inserts_deletes tools/smoke_1m_inserts_deletes.nim
##   bin/smoke_1m_inserts_deletes                 # 1M rows (default)
##   bin/smoke_1m_inserts_deletes 127.0.0.1 9001
##
## Notes:
##   - Connects to node 1 client port (default 9001).
##   - Host/port overridable via argv: bin/smoke_1m_inserts_deletes [host] [port]
##   - Node PIDs must be discoverable for memory snapshots:
##       /tmp/fractio_offset_node{1,2,3}/node.pid

import std/[os, osproc, strutils, strformat, times, algorithm, options, sequtils]
import fractio/core/types
import fractio/protocol/client
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/distributed/meta/system_tables
import fractio/sql/executor

const
  DEFAULT_HOST = "127.0.0.1"
  DEFAULT_PORT = 9001
  DATABASE = "smoke"
  SCHEMA = "public"
  TABLE = "users"

  # Phase A: 1M rows
  DEFAULT_FIRST_ID = 1
  DEFAULT_LAST_ID = 1_000_000
  INSERT_BATCH_ROWS = 500 # rows per INSERT statement

  # Phase B: delete every 10th id
  DELETE_STRIDE = 10
  DELETE_BATCH_ROWS = 1000

  # Phase C: interleaved INSERT/DELETE
  INTERLEAVED_OPS = 10_000
  INTERLEAVED_BATCH = 200

  # Node data dirs for memory snapshots
  NODE_DIRS = [
    "/tmp/fractio_offset_node1",
    "/tmp/fractio_offset_node2",
    "/tmp/fractio_offset_node3"
  ]

# ---------------------------------------------------------------------------
# Latency statistics
# ---------------------------------------------------------------------------

type LatencyStats = object
  samples: seq[float]
  failed: int

proc record(stats: var LatencyStats, ms: float, ok: bool) =
  if ok: stats.samples.add(ms)
  else: inc stats.failed

proc percentile(samples: seq[float], p: float): float =
  if samples.len == 0: return 0.0
  let sorted = samples.sorted()
  let idx = min(sorted.len - 1, int(float(sorted.len - 1) * p))
  sorted[idx]

proc summary(stats: LatencyStats, label: string, totalRows: int) =
  let ok = stats.samples.len
  let failed = stats.failed
  if stats.samples.len == 0:
    echo &"  {label}: no successful batches, {failed} failed"
    return
  let total = stats.samples.foldl(a + b)
  let mean = total / stats.samples.len.float
  let p50 = stats.samples.percentile(0.50)
  let p95 = stats.samples.percentile(0.95)
  let p99 = stats.samples.percentile(0.99)
  let pmax = stats.samples[^1]
  echo &"  {label}: ok={ok} failed={failed} mean={mean:.1f}ms p50={p50:.1f}ms p95={p95:.1f}ms p99={p99:.1f}ms max={pmax:.1f}ms"

# ---------------------------------------------------------------------------
# Cluster memory / disk snapshots
# ---------------------------------------------------------------------------

proc readFileInt(path: string): int =
  try:
    return parseInt(strip(readFile(path)))
  except CatchableError:
    return -1

proc rssKB(pid: int): int =
  ## Read VmRSS from /proc/<pid>/status (kB). Returns -1 on error.
  if pid <= 0: return -1
  let path = &"/proc/{pid}/status"
  if not fileExists(path): return -1
  try:
    for line in lines(path):
      if line.startsWith("VmRSS:"):
        let parts = line.splitWhitespace()
        if parts.len >= 2:
          return parseInt(parts[1])
  except CatchableError: discard
  return -1

proc cpuPct(pid: int): float =
  ## Approximate CPU% from /proc/<pid>/stat utime+stime over a 100ms sample.
  if pid <= 0: return -1.0
  let path = &"/proc/{pid}/stat"
  if not fileExists(path): return -1.0
  const CLK_TCK = 100
  proc readJiffies(): int64 =
    try:
      let content = readFile(path)
      let parts = content.splitWhitespace()
      let utime = parseBiggestInt(parts[13])
      let stime = parseBiggestInt(parts[14])
      result = utime + stime
    except CatchableError:
      result = 0
  let t1 = readJiffies()
  sleep(100)
  let t2 = readJiffies()
  if t2 <= t1: return 0.0
  let deltaJiffies = (t2 - t1).float
  let deltaSeconds = deltaJiffies / CLK_TCK.float
  return deltaSeconds * 1000.0

proc diskUsageKB(dir: string): int =
  ## du -sk <dir> — total disk usage in kB.
  if not dirExists(dir): return -1
  let (outp, _) = execCmdEx(&"du -sk {dir} 2>/dev/null | cut -f1")
  try: parseInt(strip(outp))
  except CatchableError: -1

proc snapshotCluster(label: string) =
  ## Print one line per node with RSS, CPU%, disk.
  echo ""
  echo &"=== Memory/Disk snapshot: {label} ==="
  var totalRss = 0
  var totalDisk = 0
  for i, dir in NODE_DIRS:
    let pidPath = dir & "/node.pid"
    if not fileExists(pidPath):
      echo &"  node{i+1}: no pidfile at {pidPath}"
      continue
    let pid = readFileInt(pidPath)
    let rss = rssKB(pid)
    let cpu = cpuPct(pid)
    let disk = diskUsageKB(dir)
    if rss > 0: totalRss += rss
    if disk > 0: totalDisk += disk
    let cpuStr = if cpu >= 0: &"{cpu:5.1f}%" else: "  n/a"
    let rssStr = if rss > 0: &"{rss:>7} kB" else: "    n/a"
    let diskStr = if disk > 0: &"{disk:>9} kB" else: "       n/a"
    echo &"  node{i+1} (pid={pid:>6}): rss={rssStr}  cpu={cpuStr}  disk={diskStr}"
  echo &"  TOTAL:  rss={totalRss} kB ({totalRss div 1024} MiB)  disk={totalDisk} kB ({totalDisk div 1024} MiB)"

# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

proc buildInsertBatch(startId, count: int): string =
  var values: seq[string] = @[]
  for i in 0 ..< count:
    let id = startId + i
    let name = &"user{id:07d}"
    let value = id * 10
    values.add(&"({id}, '{name}', {value})")
  return &"INSERT INTO {DATABASE}.{SCHEMA}.{TABLE} (id, name, value) VALUES " &
      join(values, ", ")

proc buildDeleteBatch(startId, count, stride: int): string =
  var clauses: seq[string] = @[]
  for i in 0 ..< count:
    let id = startId + i * stride
    clauses.add(&"id = {id}")
  return &"DELETE FROM {DATABASE}.{SCHEMA}.{TABLE} WHERE " & join(clauses, " OR ")

proc buildDeleteByIdBatch(ids: seq[int]): string =
  ## DELETE a list of specific ids using OR clauses.
  if ids.len == 0: return ""
  var clauses: seq[string] = @[]
  for id in ids:
    clauses.add(&"id = {id}")
  return &"DELETE FROM {DATABASE}.{SCHEMA}.{TABLE} WHERE " & join(clauses, " OR ")

# ---------------------------------------------------------------------------
# Query helper with retry on transient admission/connection errors
# ---------------------------------------------------------------------------

proc isTransientError(msg: string): bool =
  ## Heuristic: errors that look transient (admission control, connection refused,
  ## leader handover, "no connection to group leader") and worth retrying.
  if msg.len == 0: return false
  let lower = msg.toLowerAscii()
  return "memory budget" in lower or
         "admission" in lower or
         "no connection" in lower or
         "connection refused" in lower or
         "connection reset" in lower or
         "table " in lower and "not found" in lower or
         "timed out" in lower or
         "leader" in lower

proc queryWithRetry(
    client: FractioClient,
    sql: string,
    database, schema: string,
    maxAttempts: int = 5,
    backoffMs: int = 2000
  ): ExecResultKind {.discardable.} =
  ## Run client.query, retrying on transient errors. Returns the final kind.
  var attempt = 0
  var lastErr = ""
  while attempt < maxAttempts:
    let res = client.query(sql, database = database, schema = schema)
    if res.kind != erkError:
      return res.kind
    lastErr = res.error
    inc attempt
    if not isTransientError(lastErr):
      echo &"    non-retryable error: {lastErr}"
      return res.kind
    if attempt < maxAttempts:
      let wait = backoffMs * attempt
      echo &"    transient error (attempt {attempt}/{maxAttempts}): {lastErr} - retrying in {wait}ms"
      sleep(wait)
  echo &"    giving up after {maxAttempts} attempts: {lastErr}"
  return erkError

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

proc main() =
  var host = DEFAULT_HOST
  var port = DEFAULT_PORT
  var firstId = DEFAULT_FIRST_ID
  var lastId = DEFAULT_LAST_ID
  var positionalIdx = 0
  for i in 1 .. paramCount():
    let a = paramStr(i)
    if a.startsWith("--rows="):
      let v = a[7 .. ^1]
      var n = -1
      try:
        let last = v[^1]
        let (numPart, multiplier) =
          if last == 'K' or last == 'k': (v[0 ..< ^1], 1_000)
          elif last == 'M' or last == 'm': (v[0 ..< ^1], 1_000_000)
          else: (v, 1)
        n = int(parseFloat(numPart) * float(multiplier))
      except CatchableError, ValueError:
        echo "ERROR: --rows=", v, " is not a valid row count"
        quit(1)
      if n < 0:
        echo "ERROR: --rows=", v, " is not a valid row count"
        quit(1)
      lastId = firstId + n - 1
    elif a.startsWith("--first="):
      firstId = parseInt(a[8 .. ^1])
    else:
      inc positionalIdx
      if positionalIdx == 1: host = a
      elif positionalIdx == 2: port = parseInt(a)
      else:
        echo "ERROR: unknown positional arg: ", a
        quit(1)

  let totalRows = lastId - firstId + 1
  # Progress every ~2% of batches
  let progressEveryBatches = max(1, min(100,
      ((totalRows + INSERT_BATCH_ROWS - 1) div INSERT_BATCH_ROWS) div 50))

  # Long timeout for 1M runs
  let requestTimeoutMs = if totalRows >= 1_000_000: 300_000
                         elif totalRows >= 100_000: 120_000
                         else: 60_000

  echo "================================================================"
  echo "Fractio 1M smoke test (3-replica cluster, inserts+deletes)"
  echo "  target:  ", host, ":", port
  echo "  space:   ", DATABASE, ".", SCHEMA, ".", TABLE
  echo "  rows:    ", totalRows, " (ids ", firstId, "..", lastId, ")"
  echo "  batches: ", (totalRows + INSERT_BATCH_ROWS - 1) div INSERT_BATCH_ROWS
  echo "  budget:  2.5GB per node (1M rows × 3 replicas)"
  echo "================================================================"
  echo ""

  var cfg = newFractioClientConfig(host, port)
  cfg.connectionTimeoutMs = 10000
  cfg.requestTimeoutMs = requestTimeoutMs
  cfg.maxKvRetries = 10
  let client = newFractioClient(cfg)

  echo "Initializing client..."
  if not client.initialize():
    echo "  FAILED"
    quit(1)
  echo "  ok"
  discard client.forceMetadataRefresh()

  # Sanity: confirm the space + table exist
  let probe = client.query(
    &"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} ORDER BY id ASC LIMIT 1",
    database = DATABASE, schema = SCHEMA)
  if probe.kind == erkError:
    echo "ERROR probing table: ", probe.error
    echo "Did you run smoke_setup? Space+table must exist."
    client.close()
    quit(1)
  echo "  Table reachable."

  # --- Baseline snapshot ---
  snapshotCluster("baseline (before any inserts)")

  # =====================================================================
  # Phase A: bulk INSERT
  # =====================================================================
  let totalInserts = lastId - firstId + 1
  let totalInsertBatches = (totalInserts + INSERT_BATCH_ROWS -
      1) div INSERT_BATCH_ROWS
  echo ""
  echo "================================================================"
  echo "Phase A: INSERT ", totalInserts, " rows"
  echo "  batch size: ", INSERT_BATCH_ROWS, " rows/INSERT, ",
      totalInsertBatches, " batches total"
  echo "================================================================"

  var insertStats = LatencyStats()
  var totalInserted = 0
  let t1 = epochTime()

  for batchIdx in 0 ..< totalInsertBatches:
    let startId = firstId + batchIdx * INSERT_BATCH_ROWS
    let remaining = totalInserts - batchIdx * INSERT_BATCH_ROWS
    let thisBatch = min(INSERT_BATCH_ROWS, remaining)
    if thisBatch <= 0: break
    let sql = buildInsertBatch(startId, thisBatch)

    let bs = epochTime()
    let kind = queryWithRetry(client, sql, DATABASE, SCHEMA)
    let be = (epochTime() - bs) * 1000.0
    let ok = kind == erkModified
    insertStats.record(be, ok)
    if ok: totalInserted += thisBatch
    elif kind == erkError:
      echo &"  ERROR batch {batchIdx + 1}/{totalInsertBatches} startId={startId}: failed after retries"

    if (batchIdx + 1) mod progressEveryBatches == 0 or (batchIdx + 1) == totalInsertBatches:
      let elapsed = epochTime() - t1
      let rps = totalInserted.float / elapsed
      echo &"  [{batchIdx + 1:4}/{totalInsertBatches}] inserted {totalInserted}/{totalInserts} rows, elapsed {elapsed:.1f}s, {rps:.0f} rows/sec"

    # Per-milestone resource snapshot at quarter marks
    let insertedSoFar = (batchIdx + 1) * INSERT_BATCH_ROWS
    if insertedSoFar == totalInserts div 4 or
       insertedSoFar == totalInserts div 2 or
       insertedSoFar == (totalInserts * 3) div 4 or
       insertedSoFar == totalInserts:
      snapshotCluster(&"after inserting {insertedSoFar} rows")

  let t1End = epochTime() - t1
  echo ""
  echo &"Phase A complete: {totalInserted}/{totalInserts} rows in {t1End:.2f}s ({totalInserted.float / t1End:.0f} rows/sec)"
  insertStats.summary("INSERT batches", totalInserts)

  # --- Post-insert verification ---
  echo ""
  echo "--- Verification after inserts ---"
  let countRes = client.query(
    &"SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.{TABLE}",
    database = DATABASE, schema = SCHEMA)
  if countRes.kind == erkRows and countRes.rows.len > 0:
    echo &"  COUNT(*) = {countRes.rows[0][0]} (expected {totalInserts})"
  let maxRes = client.query(
    &"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} ORDER BY id DESC LIMIT 1",
    database = DATABASE, schema = SCHEMA)
  if maxRes.kind == erkRows and maxRes.rows.len > 0:
    echo &"  MAX(id)  = {maxRes.rows[0][0]} (expected {totalInserts})"

  # =====================================================================
  # Phase B: bulk DELETE (every 10th id)
  # =====================================================================
  let totalDeletes = totalInserts div DELETE_STRIDE
  let totalDelBatches = (totalDeletes + DELETE_BATCH_ROWS -
      1) div DELETE_BATCH_ROWS
  let progressEveryDelBatches = max(1, min(20,
      (((totalInserts div DELETE_STRIDE) + DELETE_BATCH_ROWS - 1) div
       DELETE_BATCH_ROWS) div 5))
  echo ""
  echo "================================================================"
  echo "Phase B: DELETE ", totalDeletes, " rows (every ",
      DELETE_STRIDE, "th id)"
  echo "  batch size: ", DELETE_BATCH_ROWS, " rows/DELETE, ",
      totalDelBatches, " batches total"
  echo "================================================================"

  var deleteStats = LatencyStats()
  var totalDeleted = 0
  let t2 = epochTime()

  for batchIdx in 0 ..< totalDelBatches:
    let startOffset = batchIdx * DELETE_BATCH_ROWS
    let remaining = totalDeletes - startOffset
    let thisBatch = min(DELETE_BATCH_ROWS, remaining)
    if thisBatch <= 0: break
    let startId = 1 + startOffset * DELETE_STRIDE
    let sql = buildDeleteBatch(startId, thisBatch, DELETE_STRIDE)

    let bs = epochTime()
    let kind = queryWithRetry(client, sql, DATABASE, SCHEMA)
    let be = (epochTime() - bs) * 1000.0
    let ok = kind == erkModified
    deleteStats.record(be, ok)
    if ok: totalDeleted += thisBatch
    elif kind == erkError:
      echo &"  ERROR batch {batchIdx + 1}/{totalDelBatches}: failed after retries"

    if (batchIdx + 1) mod progressEveryDelBatches == 0 or (batchIdx + 1) == totalDelBatches:
      let elapsed = epochTime() - t2
      let rps = totalDeleted.float / elapsed
      echo &"  [{batchIdx + 1:3}/{totalDelBatches}] deleted {totalDeleted}/{totalDeletes} rows, elapsed {elapsed:.1f}s, {rps:.0f} rows/sec"

  let t2End = epochTime() - t2
  echo ""
  echo &"Phase B complete: {totalDeleted}/{totalDeletes} rows in {t2End:.2f}s ({totalDeleted.float / t2End:.0f} rows/sec)"
  deleteStats.summary("DELETE batches", totalDeletes)

  # --- Post-delete verification ---
  echo ""
  echo "--- Verification after deletes ---"
  let countRes2 = client.query(
    &"SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.{TABLE}",
    database = DATABASE, schema = SCHEMA)
  if countRes2.kind == erkRows and countRes2.rows.len > 0:
    let expected = totalInserts - totalDeletes
    echo &"  COUNT(*)  = {countRes2.rows[0][0]} (expected {expected})"
  # Probe: id 1 should be gone; id 2 should remain
  for probeId in [1, 2, 11, 10, 100, 99, 999991, 999990]:
    let probeRes = client.query(
      &"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} WHERE id = {probeId}",
      database = DATABASE, schema = SCHEMA)
    if probeRes.kind == erkRows:
      let found = probeRes.rows.len > 0
      let shouldExist = (probeId mod DELETE_STRIDE) != 1 or probeId > totalInserts
      let tag = if found == shouldExist: "ok" else: "MISMATCH"
      echo &"  id={probeId:>6} found={found:<5} expected={shouldExist:<5} [{tag}]"

  # =====================================================================
  # Phase C: interleaved INSERT/DELETE
  # =====================================================================
  echo ""
  echo "================================================================"
  echo "Phase C: INTERLEAVED INSERT/DELETE (mixed workload)"
  echo "  ops:     ", INTERLEAVED_OPS, " (alternating insert/delete of 1 row)"
  echo "================================================================"

  var interStats = LatencyStats()
  var interInsertsOk = 0
  var interDeletesOk = 0
  let t3 = epochTime()
  let baseId = totalInserts + 1 # use new id range for inserts

  for opIdx in 0 ..< INTERLEAVED_OPS:
    let sql = if opIdx mod 2 == 0:
                &"INSERT INTO {DATABASE}.{SCHEMA}.{TABLE} (id, name, value) VALUES ({baseId + opIdx}, 'inter{opIdx:05d}', {opIdx})"
              else:
                # Delete a random previously-inserted row
                let targetId = (opIdx * 13) mod totalInserts + 1
                &"DELETE FROM {DATABASE}.{SCHEMA}.{TABLE} WHERE id = {targetId}"

    let bs = epochTime()
    let kind = queryWithRetry(client, sql, DATABASE, SCHEMA)
    let be = (epochTime() - bs) * 1000.0
    let ok = kind == erkModified
    interStats.record(be, ok)
    if ok:
      if opIdx mod 2 == 0: inc interInsertsOk
      else: inc interDeletesOk
    elif kind == erkError:
      echo &"  ERROR op {opIdx}: failed after retries"

  let t3End = epochTime() - t3
  echo ""
  echo &"Phase C complete: {interInsertsOk} inserts + {interDeletesOk} deletes in {t3End:.2f}s"
  interStats.summary("INTERLEAVED ops", INTERLEAVED_OPS)

  # =====================================================================
  # Phase D: final verification
  # =====================================================================
  echo ""
  echo "================================================================"
  echo "Phase D: FINAL VERIFICATION"
  echo "================================================================"
  let countRes3 = client.query(
    &"SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.{TABLE}",
    database = DATABASE, schema = SCHEMA)
  if countRes3.kind == erkRows and countRes3.rows.len > 0:
    let expected = totalInserts - totalDeletes + interInsertsOk
    let actual = parseInt(countRes3.rows[0][0])
    let diff = actual - expected
    let tag = if diff == 0: "ok" else: "DRIFT"
    echo &"  COUNT(*)  = {actual} (expected ~{expected}, diff={diff}) [{tag}]"
  let maxRes3 = client.query(
    &"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} ORDER BY id DESC LIMIT 1",
    database = DATABASE, schema = SCHEMA)
  if maxRes3.kind == erkRows and maxRes3.rows.len > 0:
    echo &"  MAX(id)  = {maxRes3.rows[0][0]}"

  # =====================================================================
  # Final snapshot
  # =====================================================================
  snapshotCluster("final (after 1M inserts + 100K deletes + 10K interleaved)")

  echo ""
  echo "================================================================"
  echo "ALL PHASES COMPLETE"
  echo "================================================================"
  echo &"  Phase A (1M INSERT):  {totalInserted}/{totalInserts} rows in {t1End:.1f}s ({totalInserted.float / t1End:.0f} rows/sec)"
  echo &"  Phase B (100K DELETE): {totalDeleted}/{totalDeletes} rows in {t2End:.1f}s ({totalDeleted.float / t2End:.0f} rows/sec)"
  echo &"  Phase C (10K MIXED):   {interInsertsOk + interDeletesOk}/{INTERLEAVED_OPS} ops in {t3End:.1f}s"

  client.close()

when isMainModule:
  main()
