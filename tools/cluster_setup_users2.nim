## Scale test setup for `scaletest.public.users2` (8465 rows, 4 columns).
##
## Uses the sequential FractioClient pattern (proven in tools/bench_setup.nim)
## instead of concurrent async inserts, which is what the previous version
## of this tool used and which segfaulted (root cause: shared Counters ref
## with a global Lock, plus 8 concurrent workers spawning new async clients
## that hit the FD_SETSIZE limit and caused double-frees during shutdown).
##
## Inserts are batched in groups of 100 via INSERT INTO ... VALUES (...),
## (...); this trades a little per-row throughput for dramatically better
## cluster stability during the setup phase.
##
## Usage:
##   nim c --mm:atomicArc --threads:on -p:src -o:bin/cluster_setup_users2 \
##       tools/cluster_setup_users2.nim
##   ./bin/cluster_setup_users2 [host] [port]
##
## Defaults: host=127.0.0.1, port=9001.

import std/[os, strutils, options, monotimes, times]
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/sql/executor
import fractio/core/types
import fractio/distributed/meta/system_tables

const
  DEFAULT_HOST = "127.0.0.1"
  DEFAULT_PORT = 9001
  SPACE_NAME = "scaletest"
  SCHEMA_NAME = "public"
  TABLE_NAME = "users2"
  NUM_ROWS = 8465
  BATCH_SIZE = 100 # rows per INSERT statement

proc main() =
  let host = if paramCount() >= 1: paramStr(1) else: DEFAULT_HOST
  let port = if paramCount() >= 2: parseInt(paramStr(2)) else: DEFAULT_PORT

  echo "=== Cluster Setup: ", SPACE_NAME, ".", SCHEMA_NAME, ".",
      TABLE_NAME, " (", NUM_ROWS, " rows) ==="
  echo ""
  echo "Target: ", host, ":", port
  echo ""

  # Connect
  echo "Connecting..."
  var cfg = newFractioClientConfig(host, port)
  cfg.connectionTimeoutMs = 5000
  cfg.requestTimeoutMs = 30000
  cfg.maxKvRetries = 10
  let client = newFractioClient(cfg)

  if not client.initialize():
    echo "  FAILED: client initialize"
    quit(1)
  echo "  Client initialized"
  discard client.forceMetadataRefresh()

  # Wait for META group leader to stabilize (8s) — this is the same pattern
  # used by tools/bench_setup.nim; without it, the createSpace call often
  # hits a transient "no leader" error during initial cluster bootstrap.
  echo "Waiting for META group leader to stabilize (8s)..."
  sleep(8000)
  discard client.forceMetadataRefresh()
  sleep(1000)

  # Step 1: Create space with 3 replicas
  echo ""
  echo "Step 1: Creating space '", SPACE_NAME, "' (3 replicas)..."
  let spaceRes = client.createSpace(SPACE_NAME, 3)
  if not spaceRes.isOk:
    if "already exists" in spaceRes.err or "duplicate" in spaceRes.err:
      echo "  Space already exists (idempotent), continuing"
    else:
      echo "  FAILED: ", spaceRes.err
      client.close()
      quit(1)
  else:
    echo "  Space created: id=", $spaceRes.spaceId, " groups=",
        spaceRes.groupCount

  # Step 2: Wait for the new space's groups to elect leaders
  echo ""
  echo "Step 2: Waiting for space group leaders (10s)..."
  sleep(10000)
  discard client.forceMetadataRefresh()
  sleep(2000)

  # Step 3: Create table via SQL (the planner sets keyEncoding=tkeDataRow)
  echo ""
  echo "Step 3: Creating table '", SPACE_NAME, ".", SCHEMA_NAME, ".",
      TABLE_NAME, "'..."
  let createSql = "CREATE TABLE " & SPACE_NAME & "." & SCHEMA_NAME & "." &
      TABLE_NAME & " (id INTEGER PRIMARY KEY, name TEXT, email TEXT, value TEXT)"
  var ctRes = client.query(createSql,
      database = SPACE_NAME, schema = SCHEMA_NAME)
  if ctRes.kind == erkError:
    if "already exists" in ctRes.error or "duplicate" in ctRes.error:
      echo "  Table already exists, continuing"
    else:
      echo "  FAILED: ", ctRes.error
      client.close()
      quit(1)
  else:
    echo "  Table created"

  discard client.forceMetadataRefresh()
  sleep(2000)

  # Step 4: Insert NUM_ROWS rows in batches of BATCH_SIZE via SQL
  echo ""
  echo "Step 4: Inserting ", NUM_ROWS, " rows (", BATCH_SIZE, " per batch)..."
  var inserted = 0
  var errors = 0
  let t0 = getMonoTime()

  var batchStart = 1
  while batchStart <= NUM_ROWS:
    let batchEnd = min(batchStart + BATCH_SIZE - 1, NUM_ROWS)

    # Build INSERT INTO ... VALUES (...), (...), (...)
    var sql = "INSERT INTO " & SPACE_NAME & "." & SCHEMA_NAME & "." &
        TABLE_NAME & " (id, name, email, value) VALUES "
    var first = true
    for i in batchStart .. batchEnd:
      if not first: sql.add(", ")
      first = false
      let name = "item" & align($i, 4, '0') # zero-padded for predictable ORDER BY
      sql.add("(" & $i & ", '" & name & "', '" & name & "@example.com', " &
          "'value_" & $i & "')")

    let res = client.query(sql, database = SPACE_NAME, schema = SCHEMA_NAME)
    if res.kind == erkError:
      inc errors
      if errors <= 5:
        echo "  Error at batch ", batchStart, "-", batchEnd, ": ", res.error
    else:
      inserted += (batchEnd - batchStart + 1)

    if batchStart == 1 or ((batchStart - 1) mod 1000) == 0 or batchEnd == NUM_ROWS:
      let elapsed = inMilliseconds(getMonoTime() - t0).float
      let rate = if elapsed > 0: inserted.float / (elapsed / 1000.0) else: 0.0
      echo "  ", inserted, "/", NUM_ROWS, " rows, ", elapsed.int, "ms, ",
          rate.int, " rows/sec"

    batchStart = batchEnd + 1

  let elapsed = inMilliseconds(getMonoTime() - t0).float
  let rate = if elapsed > 0: inserted.float / (elapsed / 1000.0) else: 0.0
  echo ""
  echo "  Result: ", inserted, "/", NUM_ROWS, " rows in ", elapsed.int,
      "ms (", rate.int, " rows/sec)"
  if errors > 0:
    echo "  Errors: ", errors, " (some batches may have been partially applied)"

  # Step 5: Verify
  echo ""
  echo "Step 5: Verifying row count..."
  discard client.forceMetadataRefresh()
  # Note: COUNT(*) is not supported by the parser (parentheses in expressions),
  # so we use a subquery trick via ORDER BY id DESC LIMIT 1 to read the last id.
  let cnt = client.query(
      "SELECT id FROM " & SPACE_NAME & "." & SCHEMA_NAME & "." &
      TABLE_NAME & " ORDER BY id DESC LIMIT 1",
      database = SPACE_NAME, schema = SCHEMA_NAME)
  case cnt.kind
  of erkStreamingRows:
    let rows = cnt.streamIterator.consumeAllRows()
    if rows.len > 0:
      echo "  Max id (proxy for total rows): ", rows[0][0]
  of erkRows:
    if cnt.rows.len > 0:
      echo "  Max id (proxy for total rows): ", cnt.rows[0][0]
  of erkError:
    echo "  Verify error: ", cnt.error
  else:
    discard

  echo ""
  echo "=== Setup complete ==="
  client.close()

when isMainModule:
  main()
