# Profile scan timing: measure each phase of a SELECT query.
# Usage: nim c --mm:atomicArc --threads:on -p:src -o:bin/profile_scan tools/profile_scan.nim
#        ./bin/profile_scan
#
# Notes:
#   - Uses `scaletest.public.users2` (10K rows: id, name, email, value)
#   - Measures plan phase, consume phase, and total time
#   - Prints per-test RSS before/after
#   - Tests four sort paths: no sort, PK ASC, PK DESC, non-PK
#   - Compares LIMIT 5 (server pushdown possible) vs LIMIT 0 (full scan)

import std/[times, strutils, os, monotimes, options, atomics, strformat]
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/sql/executor

proc readRss(): int =
  ## Read RSS (KB) for the current process from /proc/self/status.
  let f = open("/proc/self/status")
  defer: f.close()
  for line in f.lines:
    if line.startsWith("VmRSS:"):
      result = parseInt(line.splitWhitespace()[1])
      return

proc runQuery(client: FractioClient, sql, db, schema: string): (int, int64,
    int64, int64) =
  ## Returns (row_count, plan_us, consume_us, total_us)
  let t0 = getMonoTime()
  let res = client.query(sql, db, schema)
  let t1 = getMonoTime()
  if res.kind == erkError:
    echo "  ERROR: ", res.error
    return (0, 0, 0, 0)

  var count = 0
  let t2 = getMonoTime()
  case res.kind
  of erkStreamingRows:
    while res.streamIterator.hasNextRow():
      let rowOpt = res.streamIterator.nextRow()
      if rowOpt.isSome:
        inc count
    res.streamIterator.closeIterator()
  of erkRows:
    count = res.rows.len
  else: discard
  let t3 = getMonoTime()
  let planUs = inMicroseconds(t1 - t0)
  let consumeUs = inMicroseconds(t3 - t2)
  let totalUs = inMicroseconds(t3 - t0)
  (count, planUs, consumeUs, totalUs)

proc printResult(run: int, count: int, planUs, consumeUs, totalUs: int64) =
  echo &"  Run {run}: rows={count} plan={planUs}us consume={consumeUs}us total={totalUs}us"

proc runTest(client: FractioClient, label, sql, db, schema: string) =
  echo ""
  echo "--- ", label, " ---"
  let rss0 = readRss()
  echo "  RSS before: ", rss0, " KB (", rss0 div 1024, " MB)"
  for i in 0..<3:
    let (count, planUs, consumeUs, totalUs) = runQuery(client, sql, db, schema)
    printResult(i+1, count, planUs, consumeUs, totalUs)
  let rss1 = readRss()
  echo "  RSS after:  ", rss1, " KB (", rss1 div 1024, " MB)  delta=", (
    rss1-rss0) div 1024, "MB"

proc main() =
  let host = "127.0.0.1"
  let port = 9001

  echo "=== Scan Timing Profile (scaletest.public.users2) ==="
  echo ""

  var cfg = newFractioClientConfig(host, port)
  cfg.connectionTimeoutMs = 5000
  cfg.requestTimeoutMs = 30000
  cfg.maxKvRetries = 10
  let client = newFractioClient(cfg)

  if not client.initialize():
    echo "Failed to initialize client"
    quit(1)
  echo "Client initialized"
  discard client.forceMetadataRefresh()
  sleep(500)

  # Warmup
  echo "Warmup query..."
  discard client.query("SELECT id FROM scaletest.public.users2 LIMIT 1",
      "scaletest", "public")
  discard client.forceMetadataRefresh()
  sleep(200)

  let db = "scaletest"
  let schema = "public"
  let tbl = "users2"

  # Test 1: SELECT * LIMIT 5 (no ORDER BY — baseline, should be fast)
  runTest(client, "Test 1: SELECT * LIMIT 5 (no ORDER BY)",
    &"SELECT * FROM {db}.{schema}.{tbl} LIMIT 5", db, schema)

  # Test 2: SELECT * ORDER BY id ASC LIMIT 5 (PK ASC — fast path, LIMIT pushed)
  runTest(client, "Test 2: SELECT * ORDER BY id ASC LIMIT 5 (PK ASC pushdown)",
    &"SELECT * FROM {db}.{schema}.{tbl} ORDER BY id ASC LIMIT 5", db, schema)

  # Test 3: SELECT * ORDER BY id DESC LIMIT 5 (PK DESC — topK heap, scans all)
  runTest(client, "Test 3: SELECT * ORDER BY id DESC LIMIT 5 (PK DESC topK)",
    &"SELECT * FROM {db}.{schema}.{tbl} ORDER BY id DESC LIMIT 5", db, schema)

  # Test 4: SELECT * ORDER BY name LIMIT 5 (non-PK ASC — topK heap, scans all)
  runTest(client, "Test 4: SELECT * ORDER BY name LIMIT 5 (non-PK ASC topK)",
    &"SELECT * FROM {db}.{schema}.{tbl} ORDER BY name LIMIT 5", db, schema)

  # Test 5: SELECT * ORDER BY name DESC LIMIT 5 (non-PK DESC — topK heap, scans all)
  runTest(client, "Test 5: SELECT * ORDER BY name DESC LIMIT 5 (non-PK DESC topK)",
    &"SELECT * FROM {db}.{schema}.{tbl} ORDER BY name DESC LIMIT 5", db, schema)

  # Test 6: SELECT * (no ORDER BY, no LIMIT — full table scan, no sort)
  runTest(client, "Test 6: SELECT * (full scan, no ORDER BY)",
    &"SELECT * FROM {db}.{schema}.{tbl}", db, schema)

  # Test 7: SELECT * ORDER BY name (no LIMIT — full sort)
  runTest(client, "Test 7: SELECT * ORDER BY name (full sort, no LIMIT)",
    &"SELECT * FROM {db}.{schema}.{tbl} ORDER BY name", db, schema)

  # Test 8: SELECT id, name ORDER BY name LIMIT 5 (projection: only 2 cols, not 4)
  runTest(client, "Test 8: SELECT id, name ORDER BY name LIMIT 5 (2-col projection)",
    &"SELECT id, name FROM {db}.{schema}.{tbl} ORDER BY name LIMIT 5", db, schema)

  echo ""
  echo "=== Profile Complete ==="
  client.close()

when isMainModule:
  main()
