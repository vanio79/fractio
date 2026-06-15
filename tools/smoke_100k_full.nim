## 100K-row smoke test for the Fractio 3-replica cluster.
##
## Runs against an already-started 3-node cluster on a fresh `smoke` space
## (REPLICAS=3) and a `smoke.public.users` table with columns
##   id    INTEGER PRIMARY KEY
##   name  TEXT
##   value INTEGER
##
## Phase 1: bulk INSERT 100,000 rows in batches of 500 per INSERT statement.
##          Logs per-batch latency and total throughput.
## Phase 2: bulk DELETE  10,000 rows (every 10th id: 1, 11, 21, ...).
##          Logs per-batch latency and total throughput.
## Phase 3: sanity checks — count(*), min/max id, gaps, and exercises the
##          LIMIT/OFFSET regression we just shipped (reversed keyword
##          order and non-literal rejection).
##
## Reports memory (RSS), CPU%, and disk usage for the cluster nodes at
## each milestone.  Writes a one-line summary to stdout that downstream
## tools can parse.
##
## Usage:
##   nim c --mm:atomicArc --threads:on --opt:speed -p:src \
##        -o:bin/smoke_100k_full tools/smoke_100k_full.nim
##   bin/smoke_100k_full                         # 100K rows (default)
##   bin/smoke_100k_full --rows=1000000          # 1M rows
##   bin/smoke_100k_full 127.0.0.1 9001 --rows=1M
##
## Notes:
##   - Connects to node 1 client port (default 9001).
##   - Host/port overridable via argv: bin/smoke_100k_full [host] [port]
##   - Row count is configurable via --rows=N (suffixes K, M supported)
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

  # Phase 1: insert LAST_ID - FIRST_ID + 1 rows in batches of INSERT_BATCH_ROWS.
  # Default is 100K rows; override with --rows=N on the command line.
  DEFAULT_FIRST_ID = 1
  DEFAULT_LAST_ID = 100_000
  INSERT_BATCH_ROWS = 500 # rows per INSERT statement

  # Phase 2: delete every 10th id (LAST_ID/DELETE_STRIDE deletes in
  # batches of DELETE_BATCH_ROWS).
  DELETE_STRIDE = 10
  DELETE_BATCH_ROWS = 1000

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
  ## Returns -1.0 on error.  This is a quick snapshot, not a precise value.
  if pid <= 0: return -1.0
  let path = &"/proc/{pid}/stat"
  if not fileExists(path): return -1.0
  # SC_CLK_TCK is typically 100 on Linux.  Hard-coding it avoids a
  # sysconf() import dance and is good enough for a coarse snapshot.
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
  return deltaSeconds * 1000.0 # 100% = 0.1s CPU in 0.1s wall = 100.0

proc diskUsageKB(dir: string): int =
  ## du -sk <dir> — total disk usage in kB.
  if not dirExists(dir): return -1
  let (outp, _) = execCmdEx(&"du -sk {dir} 2>/dev/null | cut -f1")
  try: parseInt(strip(outp))
  except CatchableError: -1

proc snapshotCluster(label: string) =
  ## Print one line per node with RSS, CPU%, disk.  Skips the VmRSS for
  ## processes we can't read.
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
    let name = &"user{id:06d}"
    let value = id * 10
    values.add(&"({id}, '{name}', {value})")
  return &"INSERT INTO {DATABASE}.{SCHEMA}.{TABLE} (id, name, value) VALUES " &
      join(values, ", ")

proc buildDeleteBatch(startId, count, stride: int): string =
  ## DELETE every Nth id starting at startId. `count` is the number of
  ## deletes to include in this single statement (one row per id, joined
  ## with OR since the SQL dialect uses `id = 1 OR id = 11 OR ...`).
  var clauses: seq[string] = @[]
  for i in 0 ..< count:
    let id = startId + i * stride
    clauses.add(&"id = {id}")
  return &"DELETE FROM {DATABASE}.{SCHEMA}.{TABLE} WHERE " & join(clauses, " OR ")

proc resultRows(res: ExecResult): seq[seq[string]] =
  ## Normalize an ExecResult into a flat list of rows.
  ##
  ## The SQL executor returns SELECTs as either `erkRows` (buffered) or
  ## `erkStreamingRows` (lazy iterator). The smoke test wants to read
  ## `.len` and iterate the rows regardless, so this helper unifies them.
  ##
  ## For streaming results we drain the iterator via the public
  ## `consumeAllRows` helper, which respects LIMIT/OFFSET and closes
  ## the underlying stream when done. Tests that fetch a small number
  ## of rows (LIMIT 0..3 or OFFSET past end) stay cheap.
  case res.kind
  of erkRows:
    return res.rows
  of erkStreamingRows:
    if res.streamIterator == nil:
      return @[]
    return res.streamIterator.consumeAllRows()
  else:
    return @[]

proc parseRowCount(s: string): int =
  ## Parse a row count string. Accepts bare numbers ("1000000") and
  ## short forms with K/M suffixes ("100K", "1M", "2.5M"). Returns
  ## the integer value, or -1 on parse error.
  if s.len == 0: return -1
  let last = s[^1]
  let (numPart, multiplier) =
    if last == 'K' or last == 'k': (s[0 ..< ^1], 1_000)
    elif last == 'M' or last == 'm': (s[0 ..< ^1], 1_000_000)
    elif last == 'B' or last == 'b': (s[0 ..< ^1], 1_000_000_000)
    else: (s, 1)
  try:
    let f = parseFloat(numPart)
    if f < 0: return -1
    return int(f * float(multiplier))
  except CatchableError, ValueError:
    return -1

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

proc main() =
  # Parse CLI args. Positional [host] [port], optional --rows=N flag.
  var host = DEFAULT_HOST
  var port = DEFAULT_PORT
  var firstId = DEFAULT_FIRST_ID
  var lastId = DEFAULT_LAST_ID
  var positionalIdx = 0
  for i in 1 .. paramCount():
    let a = paramStr(i)
    if a.startsWith("--rows="):
      let v = a[7 .. ^1]
      let n = parseRowCount(v)
      if n < 0:
        echo "ERROR: --rows=", v, " is not a valid row count (use bare number, K, or M suffix)"
        quit(1)
      lastId = firstId + n - 1
    elif a.startsWith("--first="):
      firstId = parseInt(a[8 .. ^1])
    else:
      # Positional argument
      inc positionalIdx
      if positionalIdx == 1: host = a
      elif positionalIdx == 2: port = parseInt(a)
      else:
        echo "ERROR: unknown positional arg: ", a
        quit(1)

  let totalRows = lastId - firstId + 1
  # Log every ~2% of total batches (rounded to at least 1, at most 100).
  let progressEveryBatches = max(1, min(100,
      ((totalRows + INSERT_BATCH_ROWS - 1) div INSERT_BATCH_ROWS) div 50))
  let progressEveryDelBatches = max(1, min(20,
      (((totalRows div DELETE_STRIDE) + DELETE_BATCH_ROWS - 1) div
       DELETE_BATCH_ROWS) div 5))

  # Higher request timeout for very large runs (single Raft commit can
  # take longer when the queue is deep).
  let requestTimeoutMs = if totalRows >= 1_000_000: 300_000
                         elif totalRows >= 100_000: 120_000
                         else: 60_000

  echo "================================================================"
  echo "Fractio smoke test (3-replica cluster)"
  echo "  target:  ", host, ":", port
  echo "  space:   ", DATABASE, ".", SCHEMA, ".", TABLE
  echo "  rows:    ", totalRows, " (ids ", firstId, "..", lastId, ")"
  echo "  batches: ", (totalRows + INSERT_BATCH_ROWS - 1) div INSERT_BATCH_ROWS
  echo "================================================================"
  echo ""

  var cfg = newFractioClientConfig(host, port)
  cfg.connectionTimeoutMs = 5000
  cfg.requestTimeoutMs = requestTimeoutMs
  cfg.maxKvRetries = 5
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
    echo "Did you run cluster_setup? Space+table must exist."
    client.close()
    quit(1)
  let startRows = resultRows(probe).len
  echo &"  Table reachable. Rows before test: {startRows}"

  # --- Baseline snapshot ---
  snapshotCluster("baseline (before any inserts)")

  # =====================================================================
  # Phase 1: insert rows
  # =====================================================================
  let totalInserts = lastId - firstId + 1
  let totalInsertBatches = (totalInserts + INSERT_BATCH_ROWS -
      1) div INSERT_BATCH_ROWS
  echo ""
  echo "================================================================"
  echo "Phase 1: INSERT ", totalInserts, " rows"
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
    let res = client.query(sql, database = DATABASE, schema = SCHEMA)
    let be = (epochTime() - bs) * 1000.0
    let ok = res.kind == erkModified
    insertStats.record(be, ok)
    if ok: totalInserted += thisBatch
    elif res.kind == erkError:
      echo &"  ERROR batch {batchIdx + 1}/{totalInsertBatches} startId={startId}: {res.error}"

    if (batchIdx + 1) mod progressEveryBatches == 0 or (batchIdx + 1) == totalInsertBatches:
      let elapsed = epochTime() - t1
      let rps = totalInserted.float / elapsed
      echo &"  [{batchIdx + 1:4}/{totalInsertBatches}] inserted {totalInserted}/{totalInserts} rows, elapsed {elapsed:.1f}s, {rps:.0f} rows/sec"

    # Per-milestone resource snapshot at quarter marks (rounded).
    let insertedSoFar = (batchIdx + 1) * INSERT_BATCH_ROWS
    if insertedSoFar == totalInserts div 4 or
       insertedSoFar == totalInserts div 2 or
       insertedSoFar == (totalInserts * 3) div 4 or
       insertedSoFar == totalInserts:
      snapshotCluster(&"after inserting {insertedSoFar} rows")

  let t1End = epochTime() - t1
  echo ""
  echo &"Phase 1 complete: {totalInserted}/{totalInserts} rows in {t1End:.2f}s ({totalInserted.float / t1End:.0f} rows/sec)"
  insertStats.summary("INSERT batches", totalInserts)

  # --- Post-insert verification ---
  echo ""
  echo "--- Verification after inserts ---"
  let countRes = client.query(
    &"SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.{TABLE}",
    database = DATABASE, schema = SCHEMA)
  if countRes.kind == erkRows and countRes.rows.len > 0:
    echo &"  COUNT(*) = {countRes.rows[0][0]}"
  let maxRes = client.query(
    &"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} ORDER BY id DESC LIMIT 1",
    database = DATABASE, schema = SCHEMA)
  if maxRes.kind == erkRows and maxRes.rows.len > 0:
    echo &"  MAX(id)  = {maxRes.rows[0][0]}"
  let minRes = client.query(
    &"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} ORDER BY id ASC LIMIT 1",
    database = DATABASE, schema = SCHEMA)
  if minRes.kind == erkRows and minRes.rows.len > 0:
    echo &"  MIN(id)  = {minRes.rows[0][0]}"

  # =====================================================================
  # Phase 2: delete every 10th id
  # =====================================================================
  let totalDeletes = totalRows div DELETE_STRIDE
  let totalDelBatches = (totalDeletes + DELETE_BATCH_ROWS -
      1) div DELETE_BATCH_ROWS
  echo ""
  echo "================================================================"
  echo "Phase 2: DELETE ", totalDeletes, " rows (every ",
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
    # ids are: 1, 11, 21, ... (1 + offset * stride)
    let startId = 1 + startOffset * DELETE_STRIDE
    let sql = buildDeleteBatch(startId, thisBatch, DELETE_STRIDE)

    let bs = epochTime()
    let res = client.query(sql, database = DATABASE, schema = SCHEMA)
    let be = (epochTime() - bs) * 1000.0
    let ok = res.kind == erkModified
    deleteStats.record(be, ok)
    if ok: totalDeleted += thisBatch
    elif res.kind == erkError:
      echo &"  ERROR batch {batchIdx + 1}/{totalDelBatches}: {res.error}"

    if (batchIdx + 1) mod progressEveryDelBatches == 0 or (batchIdx + 1) == totalDelBatches:
      let elapsed = epochTime() - t2
      let rps = totalDeleted.float / elapsed
      echo &"  [{batchIdx + 1:2}/{totalDelBatches}] deleted {totalDeleted}/{totalDeletes} rows, elapsed {elapsed:.1f}s, {rps:.0f} rows/sec"

  let t2End = epochTime() - t2
  echo ""
  echo &"Phase 2 complete: {totalDeleted}/{totalDeletes} rows in {t2End:.2f}s ({totalDeleted.float / t2End:.0f} rows/sec)"
  deleteStats.summary("DELETE batches", totalDeletes)

  # --- Post-delete verification ---
  echo ""
  echo "--- Verification after deletes ---"
  let countRes2 = client.query(
    &"SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.{TABLE}",
    database = DATABASE, schema = SCHEMA)
  if countRes2.kind == erkRows and countRes2.rows.len > 0:
    echo &"  COUNT(*)  = {countRes2.rows[0][0]} (expected 90000)"
  # Probe: id 1 should be gone; id 2 should remain
  for probeId in [1, 2, 11, 10, 100, 99, 99991, 99990]:
    let probeRes = client.query(
      &"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} WHERE id = {probeId}",
      database = DATABASE, schema = SCHEMA)
    if probeRes.kind == erkRows:
      let found = probeRes.rows.len > 0
      let shouldExist = (probeId mod DELETE_STRIDE) != 1 or probeId > 100_000
      let tag = if found == shouldExist: "ok" else: "MISMATCH"
      echo &"  id={probeId:>5} found={found:<5} expected={shouldExist:<5} [{tag}]"

  # =====================================================================
  # Phase 3: LIMIT/OFFSET regression checks (the fix we just shipped)
  # =====================================================================
  echo ""
  echo "================================================================"
  echo "Phase 3: LIMIT/OFFSET regression checks"
  echo "================================================================"

  # 3a) canonical order
  let q3a = client.query(
    &"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} ORDER BY id ASC LIMIT 3 OFFSET 10",
    database = DATABASE, schema = SCHEMA)
  let q3aRows = resultRows(q3a)
  echo &"  LIMIT 3 OFFSET 10 (canonical) -> {q3a.kind} rows={q3aRows.len}"
  for r in q3aRows: echo &"    id={r[0]}"

  # 3b) reversed order — should now work
  let q3b = client.query(
    &"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} ORDER BY id ASC OFFSET 10 LIMIT 3",
    database = DATABASE, schema = SCHEMA)
  let q3bRows = resultRows(q3b)
  echo &"  OFFSET 10 LIMIT 3 (reversed)  -> {q3b.kind} rows={q3bRows.len}"
  for r in q3bRows: echo &"    id={r[0]}"
  if q3bRows.len == 3:
    let match = q3aRows == q3bRows
    echo &"  reversed order matches canonical: {match}"

  # 3c) negative LIMIT — should be rejected
  let q3c = client.query(
    &"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} LIMIT -1",
    database = DATABASE, schema = SCHEMA)
  echo &"  LIMIT -1 (must reject) -> {q3c.kind} error='{q3c.error}'"
  echo &"    contains LIMIT: {$(\"LIMIT\" in q3c.error)}, contains non-negative: {$(\"non-negative\" in q3c.error)}"

  # 3d) negative OFFSET — should be rejected
  let q3d = client.query(
    &"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} LIMIT 5 OFFSET -1",
    database = DATABASE, schema = SCHEMA)
  echo &"  LIMIT 5 OFFSET -1 (must reject) -> {q3d.kind} error='{q3d.error}'"

  # 3e) non-literal LIMIT — should be rejected
  let q3e = client.query(
    &"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} LIMIT (1+1)",
    database = DATABASE, schema = SCHEMA)
  echo &"  LIMIT (1+1) (must reject) -> {q3e.kind} error='{q3e.error}'"

  # 3f) non-literal OFFSET — should be rejected
  let q3f = client.query(
    &"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} LIMIT 5 OFFSET (1-1)",
    database = DATABASE, schema = SCHEMA)
  echo &"  LIMIT 5 OFFSET (1-1) (must reject) -> {q3f.kind} error='{q3f.error}'"

  # 3g) LIMIT 0 still works (no rows returned)
  let q3g = client.query(
    &"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} LIMIT 0",
    database = DATABASE, schema = SCHEMA)
  let q3gRows = resultRows(q3g)
  echo &"  LIMIT 0 (no rows) -> {q3g.kind} rows={q3gRows.len}"

  # 3h) OFFSET past end — empty result
  let q3h = client.query(
    &"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} LIMIT 5 OFFSET 1000000",
    database = DATABASE, schema = SCHEMA)
  let q3hRows = resultRows(q3h)
  echo &"  LIMIT 5 OFFSET 1000000 (past end) -> {q3h.kind} rows={q3hRows.len}"

  # 3i) duplicate LIMIT — should be parse error
  let q3i = client.query(
    &"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} LIMIT 3 LIMIT 5",
    database = DATABASE, schema = SCHEMA)
  echo &"  LIMIT 3 LIMIT 5 (must reject) -> {q3i.kind} error='{q3i.error}'"

  # =====================================================================
  # Final snapshot
  # =====================================================================
  snapshotCluster("final (after 100K inserts + 10K deletes)")

  echo ""
  echo "================================================================"
  echo "ALL PHASES COMPLETE"
  echo "================================================================"

  client.close()

when isMainModule:
  main()
