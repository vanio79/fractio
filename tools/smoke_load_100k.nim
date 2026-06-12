## Bulk load 95,500 rows (ids 4501..100000) into smoke.public.users via the
## web SQL API. Uses batches of 100 rows per HTTP call. Assumes the cluster
## is already running and `smoke` space + `smoke.public.users` table exist.
##
## Usage:
##   nim c --mm:atomicArc --threads:on --opt:speed -p:src \
##        -o:bin/smoke_load_100k tools/smoke_load_100k.nim
##   bin/smoke_load_100k
##
## Notes:
##   - Web URL: http://127.0.0.1:9870/api/sql
##   - Existing rows: 1..4500 (5000 inserted, 500 deleted in earlier tests)
##   - Target: insert 95,500 rows (ids 4501..100000) in batches of 100
##   - Each INSERT statement contains 100 rows
##   - Print progress every 50 batches (5,000 rows)
##   - Print final stats (total time, rows/sec, latency percentiles)

import std/[os, strutils, times, httpclient, json, osproc, strformat, algorithm]

const
  HOST = "127.0.0.1"
  WEB_PORT = 9870
  BATCH_SIZE = 100
  FIRST_ID = 4501     # resume from where 5000-row test left off
  LAST_ID = 100_000
  PROGRESS_EVERY = 50 # log every N batches (5,000 rows)

proc buildInsertBatch(startId, count: int): string =
  ## Build a single INSERT statement with `count` rows starting at startId.
  let totalRows = LAST_ID - FIRST_ID + 1
  let actualCount = min(count, totalRows - (startId - FIRST_ID))
  if actualCount <= 0:
    return ""
  var parts: seq[string] = @[]
  parts.add(&"INSERT INTO smoke.public.users (id, name, value) VALUES ")
  var values: seq[string] = @[]
  for i in 0 ..< actualCount:
    let id = startId + i
    let name = &"user{id:06d}"
    let value = id * 10
    values.add(&"({id}, '{name}', {value})")
  parts.add(join(values, ", "))
  return join(parts, "")

proc postSql(sql: string): JsonNode =
  let client = newHttpClient(timeout = 30_000)
  try:
    client.headers = newHttpHeaders({
      "Content-Type": "application/json"
    })
    let body = %*{"sql": sql}
    if sql.startsWith("SELECT"):
      echo "DEBUG postSql first 200 chars: ", sql[0 ..< min(200, sql.len)]
    let resp = client.postContent(
      &"http://{HOST}:{WEB_PORT}/api/sql",
      $body
    )
    return parseJson(resp)
  finally:
    client.close()

proc main() =
  echo "=== Fractio 100K-row smoke load ==="
  echo "Cluster: ", HOST, ":", WEB_PORT
  echo "Inserting ids ", FIRST_ID, "..", LAST_ID, " (", LAST_ID - FIRST_ID + 1, " rows)"
  echo "Batch size: ", BATCH_SIZE, " rows per INSERT statement"
  echo ""

  # Verify cluster is reachable
  let healthRes = postSql("SELECT id FROM smoke.public.users ORDER BY id DESC LIMIT 1")
  if healthRes.kind != JObject or not healthRes.hasKey("kind"):
    echo "ERROR: cluster not reachable at ", HOST, ":", WEB_PORT
    quit(1)
  echo "Cluster reachable. Current max id: ", healthRes
  echo ""

  let totalRows = LAST_ID - FIRST_ID + 1
  let totalBatches = (totalRows + BATCH_SIZE - 1) div BATCH_SIZE
  echo "Total batches to send: ", totalBatches
  echo ""

  let startTime = epochTime()
  var batchLatencies: seq[float] = @[]
  var totalInserted = 0
  var failedBatches = 0

  for batchIdx in 0 ..< totalBatches:
    let startId = FIRST_ID + batchIdx * BATCH_SIZE
    let sql = buildInsertBatch(startId, BATCH_SIZE)
    if sql.len == 0:
      break

    let batchStart = epochTime()
    let res = postSql(sql)
    let batchElapsed = (epochTime() - batchStart) * 1000.0
    batchLatencies.add(batchElapsed)

    let kind = res{"kind"}.getStr("")
    if kind == "modified":
      totalInserted += BATCH_SIZE
    elif kind == "error":
      let err = res{"error"}.getStr("unknown")
      echo &"  ERROR batch {batchIdx + 1}/{totalBatches} startId={startId}: {err}"
      failedBatches += 1
    else:
      echo &"  UNEXPECTED batch {batchIdx + 1}/{totalBatches} kind={kind}"
      failedBatches += 1

    if (batchIdx + 1) mod PROGRESS_EVERY == 0:
      let elapsed = epochTime() - startTime
      let rps = totalInserted.float / elapsed
      echo &"  [{batchIdx + 1:4}/{totalBatches}] inserted {totalInserted}/{totalRows} rows, elapsed {elapsed:.1f}s, {rps:.1f} rows/sec"

  let totalElapsed = epochTime() - startTime
  let rps = totalInserted.float / totalElapsed

  echo ""
  echo "=== Done ==="
  echo &"Total batches:       {totalBatches}"
  echo &"Total inserted:      {totalInserted}"
  echo &"Failed batches:      {failedBatches}"
  echo &"Total time:          {totalElapsed:.2f}s"
  echo &"Throughput:          {rps:.1f} rows/sec"
  if batchLatencies.len > 0:
    # Sort and compute percentiles
    batchLatencies.sort()
    let p50 = batchLatencies[batchLatencies.len div 2]
    let p95 = batchLatencies[batchLatencies.len * 95 div 100]
    let p99 = batchLatencies[batchLatencies.len * 99 div 100]
    let pmax = batchLatencies[^1]
    echo &"Batch latency p50:   {p50:.1f}ms"
    echo &"Batch latency p95:   {p95:.1f}ms"
    echo &"Batch latency p99:   {p99:.1f}ms"
    echo &"Batch latency max:   {pmax:.1f}ms"

  echo ""
  echo "=== Verification ==="
  let maxRes = postSql("SELECT id FROM smoke.public.users ORDER BY id DESC LIMIT 1")
  echo "Max id: ", maxRes
  let minRes = postSql("SELECT id FROM smoke.public.users ORDER BY id ASC LIMIT 1")
  echo "Min id: ", minRes

when isMainModule:
  main()
