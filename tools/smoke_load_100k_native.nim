## Bulk load 100,000 rows (ids 1..100000) into smoke.public.users via the
## native FractioClient (TCP, not HTTP). Uses batches of 100 rows per query.
## Assumes the cluster is already running and `smoke` space + `smoke.public.users`
## table exist.
##
## Usage:
##   nim c --mm:atomicArc --threads:on --opt:speed -p:src \
##        -o:bin/smoke_load_100k_native tools/smoke_load_100k_native.nim
##   bin/smoke_load_100k_native
##
## Why native instead of HTTP:
##   - The web layer has FD_SETSIZE limits that crash the web server thread
##     under sustained load (it leaks client fds).
##   - Native FractioClient uses a single long-lived TCP connection per node
##     and doesn't have this issue.
##   - Also gives a more accurate benchmark of the database's true performance.
##
## Notes:
##   - Connects to node 1 client port 9000
##   - Inserts ids 1..100,000 in batches of 100 rows per INSERT
##   - Each INSERT statement contains 100 rows = 1000 INSERT statements total
##   - Prints progress every 50 batches (5,000 rows)
##   - Prints final stats (total time, rows/sec, latency percentiles)

import std/[os, strutils, times, strformat, algorithm]
import fractio/core/types
import fractio/protocol/client
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/distributed/meta/system_tables
import fractio/sql/executor

const
  HOST = "127.0.0.1"
  CLIENT_PORT = 9000
  BATCH_SIZE = 100
  FIRST_ID = 1
  LAST_ID = 100_000
  PROGRESS_EVERY = 50 # log every N batches (5,000 rows)
  DATABASE = "smoke"
  SCHEMA = "public"
  TABLE = "users"

proc buildInsertBatch(startId, count: int): string =
  ## Build a single INSERT statement with `count` rows starting at startId.
  let totalRows = LAST_ID - FIRST_ID + 1
  let actualCount = min(count, totalRows - (startId - FIRST_ID))
  if actualCount <= 0:
    return ""
  var values: seq[string] = @[]
  for i in 0 ..< actualCount:
    let id = startId + i
    let name = &"user{id:06d}"
    let value = id * 10
    values.add(&"({id}, '{name}', {value})")
  return &"INSERT INTO {DATABASE}.{SCHEMA}.{TABLE} (id, name, value) VALUES " &
      join(values, ", ")

proc main() =
  echo "=== Fractio 100K-row smoke load (NATIVE CLIENT) ==="
  echo "Cluster: ", HOST, ":", CLIENT_PORT
  echo "Inserting ids ", FIRST_ID, "..", LAST_ID, " (", LAST_ID - FIRST_ID + 1, " rows)"
  echo "Batch size: ", BATCH_SIZE, " rows per INSERT statement"
  echo ""

  var cfg = newFractioClientConfig(HOST, CLIENT_PORT)
  cfg.connectionTimeoutMs = 5000
  cfg.requestTimeoutMs = 30000
  cfg.maxKvRetries = 5
  let client = newFractioClient(cfg)

  echo "Initializing client..."
  if not client.initialize():
    echo "Failed to initialize client"
    quit(1)
  echo "Client initialized"

  echo "Refreshing metadata..."
  discard client.forceMetadataRefresh()
  echo "Metadata refreshed"
  echo ""

  # Sanity check
  let probe = client.query(&"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} ORDER BY id DESC LIMIT 1",
                            database = DATABASE, schema = SCHEMA)
  if probe.kind == ExecResultKind.erkError:
    echo "ERROR probing table: ", probe.error
    quit(1)
  echo "Table reachable. Current rows: ", probe.rows.len, " (should be 0 for fresh load)"
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
    let res = client.query(sql, database = DATABASE, schema = SCHEMA)
    let batchElapsed = (epochTime() - batchStart) * 1000.0
    batchLatencies.add(batchElapsed)

    if res.kind == ExecResultKind.erkModified:
      totalInserted += BATCH_SIZE
    elif res.kind == ExecResultKind.erkError:
      echo &"  ERROR batch {batchIdx + 1}/{totalBatches} startId={startId}: {res.error}"
      failedBatches += 1
    else:
      echo &"  UNEXPECTED batch {batchIdx + 1}/{totalBatches} kind={res.kind}"
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
  let maxQuery = client.query(&"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} ORDER BY id DESC LIMIT 1",
                              database = DATABASE, schema = SCHEMA)
  if maxQuery != nil:
    echo "Max id: kind=", maxQuery.kind, " rows.len=", maxQuery.rows.len
    if maxQuery.rows.len > 0:
      echo "  first row: ", maxQuery.rows[0]
  let minQuery = client.query(&"SELECT id FROM {DATABASE}.{SCHEMA}.{TABLE} ORDER BY id ASC LIMIT 1",
                              database = DATABASE, schema = SCHEMA)
  if minQuery != nil:
    echo "Min id: kind=", minQuery.kind, " rows.len=", minQuery.rows.len
    if minQuery.rows.len > 0:
      echo "  first row: ", minQuery.rows[0]

  client.close()

when isMainModule:
  main()
