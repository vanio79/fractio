# Profile scan timing: measure each phase of a SELECT query.
# Usage: nim c --mm:atomicArc --threads:on -p:src -o:bin/profile_scan tools/profile_scan.nim
#        ./bin/profile_scan

import std/[times, strutils, os, monotimes, options, atomics]
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/sql/executor

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
  echo "  Run ", run, ": rows=", count, " plan=", planUs,
      "us consume=", consumeUs, "us total=", totalUs, "us"

proc main() =
  let host = "127.0.0.1"
  let port = 9001

  echo "=== Scan Timing Profile ==="
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
  discard client.query("SELECT * FROM scaletest.public.users LIMIT 1",
      "scaletest", "public")
  discard client.forceMetadataRefresh()
  sleep(200)

  # Test 1: SELECT * LIMIT 5 (no ORDER BY — should be fast, LIMIT pushed to scan)
  echo ""
  echo "--- Test 1: SELECT * LIMIT 5 (no ORDER BY) ---"
  for i in 0..<3:
    let (count, planUs, consumeUs, totalUs) = runQuery(client,
      "SELECT * FROM scaletest.public.users LIMIT 5", "scaletest", "public")
    printResult(i+1, count, planUs, consumeUs, totalUs)

  # Test 2: SELECT * ORDER BY id ASC LIMIT 5 (PK ASC — fast path, LIMIT pushed)
  echo ""
  echo "--- Test 2: SELECT * ORDER BY id ASC LIMIT 5 ---"
  for i in 0..<3:
    let (count, planUs, consumeUs, totalUs) = runQuery(client,
      "SELECT * FROM scaletest.public.users ORDER BY id ASC LIMIT 5",
      "scaletest", "public")
    printResult(i+1, count, planUs, consumeUs, totalUs)

  # Test 3: SELECT * ORDER BY id DESC LIMIT 5 (PK DESC — topK heap, scans all)
  echo ""
  echo "--- Test 3: SELECT * ORDER BY id DESC LIMIT 5 ---"
  for i in 0..<3:
    let (count, planUs, consumeUs, totalUs) = runQuery(client,
      "SELECT * FROM scaletest.public.users ORDER BY id DESC LIMIT 5",
      "scaletest", "public")
    printResult(i+1, count, planUs, consumeUs, totalUs)

  # Test 4: SELECT * ORDER BY name LIMIT 5 (non-PK ASC — topK heap, scans all)
  echo ""
  echo "--- Test 4: SELECT * ORDER BY name LIMIT 5 ---"
  for i in 0..<3:
    let (count, planUs, consumeUs, totalUs) = runQuery(client,
      "SELECT * FROM scaletest.public.users ORDER BY name LIMIT 5", "scaletest", "public")
    printResult(i+1, count, planUs, consumeUs, totalUs)

  # Test 5: SELECT * ORDER BY name DESC LIMIT 5 (non-PK DESC — topK heap, scans all)
  echo ""
  echo "--- Test 5: SELECT * ORDER BY name DESC LIMIT 5 ---"
  for i in 0..<3:
    let (count, planUs, consumeUs, totalUs) = runQuery(client,
      "SELECT * FROM scaletest.public.users ORDER BY name DESC LIMIT 5",
      "scaletest", "public")
    printResult(i+1, count, planUs, consumeUs, totalUs)

  # Test 6: SELECT * (no ORDER BY, no LIMIT — full table scan, no sort)
  echo ""
  echo "--- Test 6: SELECT * (full scan, no ORDER BY) ---"
  for i in 0..<3:
    let (count, planUs, consumeUs, totalUs) = runQuery(client,
      "SELECT * FROM scaletest.public.users", "scaletest", "public")
    printResult(i+1, count, planUs, consumeUs, totalUs)

  # Test 7: SELECT * ORDER BY name (no LIMIT — full sort)
  echo ""
  echo "--- Test 7: SELECT * ORDER BY name (full sort, no LIMIT) ---"
  for i in 0..<3:
    let (count, planUs, consumeUs, totalUs) = runQuery(client,
      "SELECT * FROM scaletest.public.users ORDER BY name", "scaletest", "public")
    printResult(i+1, count, planUs, consumeUs, totalUs)

  echo ""
  echo "=== Profile Complete ==="
  client.close()

when isMainModule:
  main()
