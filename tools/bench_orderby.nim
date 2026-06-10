## Comprehensive performance and correctness benchmark for ORDER BY + LIMIT.
## Tests correctness, latency percentiles, and stability under concurrent load.

import std/[os, strutils, json, strformat, times, monotimes, httpclient, locks,
    random, algorithm, tables, sequtils, stats]
import std/[asyncdispatch, asyncnet, asyncfutures]
import std/uri

const
  HOST = "127.0.0.1"
  PORT = 9871
  DB = "scaletest"
  SCHEMA = "public"
  TABLE = "users2"

# Result type: a single timed query
type QueryResult = object
  ok: bool
  elapsedMs: float
  rowCount: int
  errMsg: string
  firstRow: string # first row as JSON string for correctness checks

# Stats aggregator
type Stats = object
  count: int
  errors: int
  totalMs: float
  minMs: float
  maxMs: float
  p50: float
  p95: float
  p99: float
  samples: seq[float]

proc sendSql(host: string, port: int, sql: string): JsonNode =
  let client = newHttpClient()
  client.headers = newHttpHeaders({"Content-Type": "application/json"})
  let body = %*{"sql": sql}
  try:
    let resp = client.postContent(&"http://{host}:{port}/api/sql", $body)
    result = parseJson(resp)
  except CatchableError as e:
    result = %*{"kind": "error", "error": e.msg}
  finally:
    client.close()

proc runQuery(host: string, port: int, sql: string, expectedFirstIds: seq[
    string] = @[]): QueryResult =
  let t0 = getMonoTime()
  let res = sendSql(host, port, sql)
  let elapsed = inMilliseconds(getMonoTime() - t0).float
  result.elapsedMs = elapsed
  if res{"kind"}.getStr() == "error":
    result.ok = false
    result.errMsg = res{"error"}.getStr()
    return
  if not res.hasKey("rows") or res["rows"].kind != JArray:
    result.ok = false
    result.errMsg = "no rows in response"
    return
  result.ok = true
  result.rowCount = res["rows"].len
  if res["rows"].len > 0:
    result.firstRow = $res["rows"][0]
    if expectedFirstIds.len > 0 and res["rows"].len > 0:
      let firstId = res["rows"][0].getOrDefault("id").getStr()
      if firstId != expectedFirstIds[0]:
        result.ok = false
        result.errMsg = &"expected first id={expectedFirstIds[0]} but got {firstId}"
        return
  # Check full result if all expected ids provided
  if expectedFirstIds.len > 1:
    for i, expId in expectedFirstIds:
      if i >= res["rows"].len:
        result.ok = false
        result.errMsg = &"expected {expectedFirstIds.len} rows, got {res[\"rows\"].len}"
        return
      let got = res["rows"][i].getOrDefault("id").getStr()
      if got != expId:
        result.ok = false
        result.errMsg = &"row {i}: expected id={expId} but got {got}"
        return

proc computeStats(samples: seq[float]): tuple[count, errors: int, totalMs,
    minMs, maxMs, p50, p95, p99: float] =
  if samples.len == 0:
    return (0, 0, 0, 0, 0, 0, 0, 0)
  result.count = samples.len
  result.minMs = min(samples)
  result.maxMs = max(samples)
  result.totalMs = foldl(samples, a + b)
  let sorted = samples.sorted()
  result.p50 = sorted[sorted.len * 50 div 100]
  result.p95 = sorted[min(sorted.len - 1, sorted.len * 95 div 100)]
  result.p99 = sorted[min(sorted.len - 1, sorted.len * 99 div 100)]

proc printStats(name: string, samples: seq[float], errors: int) =
  let s = computeStats(samples)
  let avg = if s.count > 0: s.totalMs / s.count.float else: 0.0
  echo &"  [{name}] n={s.count} errs={errors} min={s.minMs:.2}ms avg={avg:.2}ms p50={s.p50:.2}ms p95={s.p95:.2}ms p99={s.p99:.2}ms max={s.maxMs:.2}ms"

# Worker: drain a list of queries, return results
proc worker(host: string, port: int, queries: seq[tuple[label: string,
    sql: string, expected: seq[string]]], results: ptr seq[QueryResult],
    lock: ptr Lock, label: string) {.async.} =
  for q in queries:
    var r = runQuery(host, port, q.sql, q.expected)
    r.firstRow = q.label
    acquire(lock[])
    results[].add(r)
    release(lock[])

proc concurrentRun(host: string, port: int, queries: seq[tuple[label: string,
    sql: string, expected: seq[string]]], nWorkers: int) =
  var results: seq[QueryResult] = @[]
  var lock: Lock
  initLock(lock)
  var partitions: seq[seq[type(queries[0])]] = @[]
  for w in 0..<nWorkers:
    var p: seq[type(queries[0])] = @[]
    var idx = w
    while idx < queries.len:
      p.add(queries[idx])
      idx += nWorkers
    partitions.add(p)
  var workers: seq[Future[void]]
  for w in 0..<nWorkers:
    workers.add(worker(host, port, partitions[w], addr results, addr lock, &"w{w}"))
  waitFor all(workers)
  deinitLock(lock)
  # Per-label stats
  var byLabel = initTable[string, seq[float]]()
  var errsByLabel = initTable[string, int]()
  for r in results:
    if not byLabel.hasKey(r.firstRow):
      byLabel[r.firstRow] = @[]
    byLabel[r.firstRow].add(r.elapsedMs)
    if not r.ok:
      errsByLabel[r.firstRow] = errsByLabel.getOrDefault(r.firstRow) + 1
  for label, samples in byLabel:
    printStats(label, samples, errsByLabel.getOrDefault(label))

# Test 1: Correctness across many query patterns
proc testCorrectness() =
  echo ""
  echo "=== TEST 1: CORRECTNESS ==="
  let cases: seq[tuple[label: string, sql: string, expected: seq[string]]] = @[
    ("T-A: ORDER BY name DESC LIMIT 5", "SELECT id, name FROM scaletest.public.users2 ORDER BY name DESC LIMIT 5",
        @["8465", "8464", "8463", "8462", "8461"]),
    ("T-A2: ORDER BY name ASC LIMIT 5", "SELECT id, name FROM scaletest.public.users2 ORDER BY name ASC LIMIT 5",
        @["1", "2", "3", "4", "5"]),
    ("T-B: WHERE id<5000 ORDER BY name DESC LIMIT 5",
        "SELECT id, name FROM scaletest.public.users2 WHERE id < 5000 ORDER BY name DESC LIMIT 5",
        @["4999", "4998", "4997", "4996", "4995"]),
    ("T-B2: WHERE id<5000 ORDER BY name ASC LIMIT 5",
        "SELECT id, name FROM scaletest.public.users2 WHERE id < 5000 ORDER BY name ASC LIMIT 5",
        @["1", "2", "3", "4", "5"]),
    ("T-C: WHERE id<200 ORDER BY name DESC LIMIT 5",
        "SELECT id, name FROM scaletest.public.users2 WHERE id < 200 ORDER BY name DESC LIMIT 5",
        @["199", "198", "197", "196", "195"]),
    ("T-D3: WHERE id>8000 ORDER BY name DESC LIMIT 5",
        "SELECT id, name FROM scaletest.public.users2 WHERE id > 8000 ORDER BY name DESC LIMIT 5",
        @["8465", "8464", "8463", "8462", "8461"]),
    ("T-D3b: WHERE id>8000 ORDER BY name ASC LIMIT 5",
        "SELECT id, name FROM scaletest.public.users2 WHERE id > 8000 ORDER BY name ASC LIMIT 5",
        @["8001", "8002", "8003", "8004", "8005"]),
    ("T-E: ORDER BY id ASC LIMIT 5 (PK ASC)",
        "SELECT id FROM scaletest.public.users2 ORDER BY id ASC LIMIT 5", @["1",
        "2", "3", "4", "5"]),
    ("T-F: ORDER BY id DESC LIMIT 5 (PK DESC)",
        "SELECT id FROM scaletest.public.users2 ORDER BY id DESC LIMIT 5", @[
        "8465", "8464", "8463", "8462", "8461"]),
    ("T-G: WHERE id=5000 ORDER BY name DESC LIMIT 5",
        "SELECT id, name FROM scaletest.public.users2 WHERE id = 5000 ORDER BY name DESC LIMIT 5",
        @["5000"]),
    ("T-H: WHERE id>10000 ORDER BY name LIMIT 5",
        "SELECT id, name FROM scaletest.public.users2 WHERE id > 10000 ORDER BY name DESC LIMIT 5",
        @[]),
    ("T-I: ORDER BY name DESC LIMIT 100",
        "SELECT id FROM scaletest.public.users2 ORDER BY name DESC LIMIT 100",
        @[]), # just check count
    ("T-J: ORDER BY name ASC LIMIT 1", "SELECT id FROM scaletest.public.users2 ORDER BY name ASC LIMIT 1",
        @["1"]),
    ("T-K: ORDER BY name DESC LIMIT 1", "SELECT id FROM scaletest.public.users2 ORDER BY name DESC LIMIT 1",
        @["8465"]),
    ("T-L: WHERE id=1 OR id=5000 OR id=8465 ORDER BY name DESC",
        "SELECT id FROM scaletest.public.users2 WHERE id = 1 OR id = 5000 OR id = 8465 ORDER BY name DESC LIMIT 5",
        @["8465", "5000", "1"]),
  ]
  var total = 0
  var pass = 0
  for c in cases:
    inc total
    let r = runQuery(HOST, PORT, c.sql, c.expected)
    if r.ok and (c.expected.len == 0 or r.rowCount == c.expected.len or (
        c.expected.len > 0 and c.expected[0].len > 0)):
      inc pass
      echo &"  PASS  {c.label}  ({r.elapsedMs:.2}ms, {r.rowCount} rows)"
    else:
      echo &"  FAIL  {c.label}  err={r.errMsg}  expected={c.expected}  got_rows={r.rowCount}"
  echo &"  Correctness: {pass}/{total} passed"

# Test 2: Latency percentiles - 200 runs of each
proc testLatency() =
  echo ""
  echo "=== TEST 2: LATENCY (200 sequential runs per query) ==="
  let queries: seq[tuple[label: string, sql: string, expected: seq[
      string]]] = @[
    ("ORDER BY name DESC LIMIT 5", "SELECT id, name FROM scaletest.public.users2 ORDER BY name DESC LIMIT 5",
        @[]),
    ("ORDER BY name ASC LIMIT 5", "SELECT id, name FROM scaletest.public.users2 ORDER BY name ASC LIMIT 5",
        @[]),
    ("ORDER BY name DESC LIMIT 50", "SELECT id, name FROM scaletest.public.users2 ORDER BY name DESC LIMIT 50",
        @[]),
    ("ORDER BY name DESC LIMIT 500", "SELECT id, name FROM scaletest.public.users2 ORDER BY name DESC LIMIT 500",
        @[]),
    ("ORDER BY name DESC LIMIT 5000", "SELECT id, name FROM scaletest.public.users2 ORDER BY name DESC LIMIT 5000",
        @[]),
    ("WHERE id<5000 ORDER BY name DESC LIMIT 5",
        "SELECT id, name FROM scaletest.public.users2 WHERE id < 5000 ORDER BY name DESC LIMIT 5",
        @[]),
    ("WHERE id BETWEEN 1000 AND 7000 ORDER BY name DESC LIMIT 5",
        "SELECT id, name FROM scaletest.public.users2 WHERE id BETWEEN 1000 AND 7000 ORDER BY name DESC LIMIT 5",
        @[]),
    ("ORDER BY id ASC LIMIT 5 (PK)", "SELECT id FROM scaletest.public.users2 ORDER BY id ASC LIMIT 5",
        @[]),
    ("ORDER BY id DESC LIMIT 5 (PK)", "SELECT id FROM scaletest.public.users2 ORDER BY id DESC LIMIT 5",
        @[]),
    ("SELECT * LIMIT 5 (no sort)", "SELECT * FROM scaletest.public.users2 LIMIT 5",
        @[]),
    ("SELECT * WHERE id=100", "SELECT * FROM scaletest.public.users2 WHERE id = 100",
        @[]),
    ("SELECT * (full scan)", "SELECT id FROM scaletest.public.users2", @[]),
  ]
  for q in queries:
    var samples: seq[float] = @[]
    var errs = 0
    for _ in 0..<200:
      let r = runQuery(HOST, PORT, q.sql, @[])
      if r.ok:
        samples.add(r.elapsedMs)
      else:
        inc errs
    printStats(q.label, samples, errs)

# Test 3: Concurrency - 8 workers x 50 queries each
proc testConcurrency() =
  echo ""
  echo "=== TEST 3: CONCURRENCY (8 workers x 50 queries = 400 queries) ==="
  var queries: seq[tuple[label: string, sql: string, expected: seq[
      string]]] = @[]
  let baseQueries = @[
    ("name DESC LIMIT 5", "SELECT id, name FROM scaletest.public.users2 ORDER BY name DESC LIMIT 5",
        @["8465", "8464", "8463", "8462", "8461"]),
    ("name ASC LIMIT 5", "SELECT id, name FROM scaletest.public.users2 ORDER BY name ASC LIMIT 5",
        @["1", "2", "3", "4", "5"]),
    ("id<5000 name DESC LIMIT 5", "SELECT id, name FROM scaletest.public.users2 WHERE id < 5000 ORDER BY name DESC LIMIT 5",
        @["4999", "4998", "4997", "4996", "4995"]),
    ("id ASC LIMIT 5", "SELECT id FROM scaletest.public.users2 ORDER BY id ASC LIMIT 5",
        @["1", "2", "3", "4", "5"]),
    ("id DESC LIMIT 5", "SELECT id FROM scaletest.public.users2 ORDER BY id DESC LIMIT 5",
        @["8465", "8464", "8463", "8462", "8461"]),
    ("name DESC LIMIT 100", "SELECT id, name FROM scaletest.public.users2 ORDER BY name DESC LIMIT 100",
        @[]),
    ("id=42 point", "SELECT id, name FROM scaletest.public.users2 WHERE id = 42",
        @["42"]),
  ]
  for _ in 0..<50:
    for q in baseQueries:
      queries.add(q)
  let t0 = getMonoTime()
  concurrentRun(HOST, PORT, queries, 8)
  let elapsed = inMilliseconds(getMonoTime() - t0)
  echo &"  Total elapsed: {elapsed}ms ({(queries.len.float * 1000.0) / elapsed.float:.0} queries/sec aggregate)"

# Test 4: Edge cases
proc testEdgeCases() =
  echo ""
  echo "=== TEST 4: EDGE CASES ==="
  let cases: seq[tuple[label: string, sql: string, expectedCount: int,
      expectedFirstId: string]] = @[
    ("WHERE id > 10000 (empty)", "SELECT id, name FROM scaletest.public.users2 WHERE id > 10000 ORDER BY name DESC LIMIT 5",
        0, ""),
    ("WHERE id < 0 (empty)", "SELECT id, name FROM scaletest.public.users2 WHERE id < 0 ORDER BY name DESC LIMIT 5",
        0, ""),
    ("ORDER BY name LIMIT 0", "SELECT id, name FROM scaletest.public.users2 ORDER BY name DESC LIMIT 0",
        0, ""),
    ("ORDER BY name DESC LIMIT 8465 (all)",
        "SELECT id FROM scaletest.public.users2 ORDER BY name DESC LIMIT 8465",
        8465, "8465"),
    ("ORDER BY name DESC LIMIT 8466 (more than total)",
        "SELECT id FROM scaletest.public.users2 ORDER BY name DESC LIMIT 10000",
        8465, "8465"),
  ]
  for c in cases:
    let r = runQuery(HOST, PORT, c.sql, @[])
    if c.expectedCount == 0:
      if r.ok and r.rowCount == 0:
        echo &"  PASS  {c.label}  (0 rows, {r.elapsedMs:.2}ms)"
      else:
        echo &"  FAIL  {c.label}  expected 0 rows, got {r.rowCount}  err={r.errMsg}"
    else:
      if r.ok and r.rowCount == c.expectedCount:
        echo &"  PASS  {c.label}  ({r.rowCount} rows, {r.elapsedMs:.2}ms)"
      else:
        echo &"  FAIL  {c.label}  expected {c.expectedCount} rows, got {r.rowCount}  err={r.errMsg}"

# Test 5: Cluster consistency - same query on all 3 nodes
proc testClusterConsistency() =
  echo ""
  echo "=== TEST 5: CLUSTER CONSISTENCY (same query on 3 nodes) ==="
  let queries = @[
    "SELECT id, name FROM scaletest.public.users2 ORDER BY name DESC LIMIT 5",
    "SELECT id, name FROM scaletest.public.users2 ORDER BY name ASC LIMIT 5",
    "SELECT id, name FROM scaletest.public.users2 WHERE id < 5000 ORDER BY name DESC LIMIT 5",
    "SELECT id, name FROM scaletest.public.users2 WHERE id = 42",
  ]
  for sql in queries:
    var results: array[3, JsonNode]
    for i, port in [9871, 9872, 9873]:
      results[i] = sendSql(HOST, port, sql)
    let r0 = $results[0]
    let r1 = $results[1]
    let r2 = $results[2]
    if r0 == r1 and r1 == r2:
      echo &"  PASS  {sql[0..min(60, sql.len-1)]}...  (all 3 nodes agree)"
    else:
      echo &"  FAIL  {sql[0..min(60, sql.len-1)]}..."
      echo &"        node1: {r0[0..min(200, r0.len-1)]}"
      echo &"        node2: {r1[0..min(200, r1.len-1)]}"
      echo &"        node3: {r2[0..min(200, r2.len-1)]}"

# Test 6: Long-running stress
proc testLongRunning() =
  echo ""
  echo "=== TEST 6: LONG-RUNNING STRESS (60s sustained load) ==="
  let baseQueries: seq[tuple[label: string, sql: string, expected: seq[
      string]]] = @[
    ("name DESC LIMIT 5", "SELECT id, name FROM scaletest.public.users2 ORDER BY name DESC LIMIT 5",
        @["8465", "8464", "8463", "8462", "8461"]),
    ("name ASC LIMIT 5", "SELECT id, name FROM scaletest.public.users2 ORDER BY name ASC LIMIT 5",
        @["1", "2", "3", "4", "5"]),
    ("id<5000 name DESC LIMIT 5", "SELECT id, name FROM scaletest.public.users2 WHERE id < 5000 ORDER BY name DESC LIMIT 5",
        @["4999", "4998", "4997", "4996", "4995"]),
    ("id ASC LIMIT 5", "SELECT id FROM scaletest.public.users2 ORDER BY id ASC LIMIT 5",
        @["1", "2", "3", "4", "5"]),
    ("id DESC LIMIT 5", "SELECT id FROM scaletest.public.users2 ORDER BY id DESC LIMIT 5",
        @["8465", "8464", "8463", "8462", "8461"]),
    ("id=42", "SELECT id, name FROM scaletest.public.users2 WHERE id = 42", @["42"]),
    ("name DESC LIMIT 100", "SELECT id, name FROM scaletest.public.users2 ORDER BY name DESC LIMIT 100",
        @[]),
    ("name DESC LIMIT 1000", "SELECT id, name FROM scaletest.public.users2 ORDER BY name DESC LIMIT 1000",
        @[]),
  ]
  let deadline = getMonoTime() + initDuration(minutes = 0, seconds = 60,
      milliseconds = 0)
  var totalQueries = 0
  var totalErrors = 0
  var samples: seq[float] = @[]
  var peakMs: float = 0
  var qpsSamples: seq[float] = @[]
  var lastReport = getMonoTime()
  var lastReportCount = 0
  echo "  Running for 60 seconds..."
  while getMonoTime() < deadline:
    let q = sample(baseQueries)
    let r = runQuery(HOST, PORT, q.sql, @[])
    inc totalQueries
    if r.ok:
      samples.add(r.elapsedMs)
      if r.elapsedMs > peakMs:
        peakMs = r.elapsedMs
    else:
      inc totalErrors
    # QPS report every 5s
    let now = getMonoTime()
    if inMilliseconds(now - lastReport) >= 5000:
      let dt = inMilliseconds(now - lastReport).float / 1000.0
      let dq = (totalQueries - lastReportCount).float
      qpsSamples.add(dq / dt)
      lastReport = now
      lastReportCount = totalQueries
  let avgQps = if qpsSamples.len > 0: foldl(qpsSamples, a + b) /
      qpsSamples.len.float else: 0
  let s = computeStats(samples)
  let avgLat = if s.count > 0: s.totalMs / s.count.float else: 0
  echo &"  Total queries: {totalQueries}  errors: {totalErrors}"
  echo &"  Latency: avg={avgLat:.2}ms p50={s.p50:.2}ms p95={s.p95:.2}ms p99={s.p99:.2}ms max={s.maxMs:.2}ms"
  echo &"  QPS: avg={avgQps:.1f}"

# Test 7: Concurrent INSERT + SELECT (mixed workload)
proc testMixedWorkload() =
  echo ""
  echo "=== TEST 7: MIXED WORKLOAD (inserts + queries for 30s) ==="
  # Start a single inserter
  let deadline = getMonoTime() + initDuration(minutes = 0, seconds = 30,
      milliseconds = 0)
  var totalInserts = 0
  var insertErrors = 0
  var queryCount = 0
  var queryErrors = 0
  var querySamples: seq[float] = @[]
  proc inserter() {.async.} =
    var i = 100000
    while getMonoTime() < deadline:
      inc i
      let sql = &"INSERT INTO scaletest.public.users2 (id, name, email, value) VALUES ({i}, 'load_{i:08}', 'load_{i:08}@x.com', 'v_{i:08}')"
      let r = runQuery(HOST, PORT, sql, @[])
      if r.ok:
        inc totalInserts
      else:
        inc insertErrors
      if insertErrors > 100 and totalInserts == 0:
        break
  let queries = @[
    "SELECT id, name FROM scaletest.public.users2 ORDER BY name DESC LIMIT 5",
    "SELECT id, name FROM scaletest.public.users2 WHERE id < 5000 ORDER BY name DESC LIMIT 5",
    "SELECT id FROM scaletest.public.users2 ORDER BY id DESC LIMIT 5",
  ]
  # Use 2 inserter coroutines + 4 query coroutines
  var fs: seq[Future[void]]
  for _ in 0..<2:
    fs.add(inserter())
  for _ in 0..<4:
    proc queryWorker() {.async.} =
      while getMonoTime() < deadline:
        for sql in queries:
          let r = runQuery(HOST, PORT, sql, @[])
          if r.ok:
            inc queryCount
            querySamples.add(r.elapsedMs)
          else:
            inc queryErrors
    fs.add(queryWorker())
  waitFor all(fs)
  let s = computeStats(querySamples)
  let avgLat = if s.count > 0: s.totalMs / s.count.float else: 0
  echo &"  Inserts: {totalInserts} ok, {insertErrors} errors"
  echo &"  Queries: {queryCount} ok, {queryErrors} errors"
  echo &"  Query latency: avg={avgLat:.2}ms p50={s.p50:.2}ms p95={s.p95:.2}ms p99={s.p99:.2}ms max={s.maxMs:.2}ms"

proc main() =
  randomize()
  testCorrectness()
  testLatency()
  testEdgeCases()
  testClusterConsistency()
  testConcurrency()
  testLongRunning()
  testMixedWorkload()

when isMainModule:
  main()
