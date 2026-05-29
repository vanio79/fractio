# Setup: Create a 3-replica space, create table, insert 10K rows.
#
# Usage:
#   nim c --mm:atomicArc --threads:on -p:src -o:bin/bench_setup tools/bench_setup.nim
#   ./bin/bench_setup
#
# Then you can play with the cluster:
#   ./bin/fractio --port=9001 cluster info
#   ./bin/bench_insert 127.0.0.1 9001 10000 1
#   etc.

import std/[os, strutils, atomics, monotimes, options, times]
import fractio/core/types
import fractio/distributed/meta/system_tables
import fractio/protocol/client
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/sql/executor

proc main() =
  let host = "127.0.0.1"
  let port = 9001

  echo "=== 3-Replica Setup: Space + Table + 10K Rows ==="
  echo ""

  # Connect
  echo "Connecting to ", host, ":", port, "..."
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

  # Wait for cluster to stabilize (leader elections, peer replication)
  echo "Waiting for cluster to stabilize (5s)..."
  sleep(5000)
  discard client.forceMetadataRefresh()

  # Verify META group has a stable leader before attempting createSpace
  var stableLeader = false
  var lastLeader: uint16 = 0
  var stableCount = 0
  for checkAttempt in 0 ..< 20:
    let connOpt = client.getGroupLeaderConnection(META_GROUP_ID)
    if connOpt.isSome:
      let conn = connOpt.get()
      let infoRes = conn.serverInfo()
      if infoRes.isOk:
        let currentLeader = infoRes.val.nodeId
        if currentLeader == lastLeader and currentLeader > 0:
          inc stableCount
          if stableCount >= 3:
            stableLeader = true
            echo "  META leader stable: node ", currentLeader
            break
        else:
          lastLeader = currentLeader
          stableCount = 1
    sleep(500)
    discard client.forceMetadataRefresh()

  if not stableLeader:
    echo "WARNING: META leader not stable after 10s, proceeding anyway"

  # Step 1: Create space with 3 replicas
  echo ""
  echo "Step 1: Creating space 'benchspace' with 3 replicas..."
  let spaceResult = client.createSpace("benchspace", 3)
  if not spaceResult.isOk:
    echo "FAILED: Space creation error: ", spaceResult.err
    client.close()
    quit(1)
  echo "  Space created: id=", $spaceResult.spaceId, " groups=",
      spaceResult.groupCount

  # Step 2: Wait for leader elections
  echo ""
  echo "Step 2: Waiting for leader elections (8s)..."
  sleep(8000)
  discard client.forceMetadataRefresh()
  sleep(1000)

  # Step 3: Create table
  echo ""
  echo "Step 3: Creating table 'benchspace.public.users'..."
  var tableRes = client.query(
    "CREATE TABLE benchspace.public.users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
    database = "benchspace",
    schema = "public"
  )
  # Retry once if connection issue
  if tableRes.kind == erkError and "no connection" in tableRes.error:
    echo "  Retrying CREATE TABLE..."
    discard client.forceMetadataRefresh()
    sleep(2000)
    tableRes = client.query(
      "CREATE TABLE benchspace.public.users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
      database = "benchspace",
      schema = "public"
    )
  if tableRes.kind == erkError:
    echo "  CREATE TABLE error: ", tableRes.error
    client.close()
    quit(1)
  echo "  Table created"

  # Step 4: Insert 10,000 rows
  echo ""
  echo "Step 4: Inserting 10,000 rows..."
  discard client.forceMetadataRefresh()

  var inserted = 0
  var errors = 0
  let t0 = getMonoTime()

  for i in 1..10000:
    let sql = "INSERT INTO benchspace.public.users (id, name, email) VALUES (" &
        $i & ", 'user_" & $i & "', 'user_" & $i & "@example.com')"
    let res = client.query(sql, database = "benchspace", schema = "public")
    if res.kind == erkError:
      if errors < 5:
        echo "  Error at row ", i, ": ", res.error
      inc errors
    else:
      inc inserted

    # Progress every 1000 rows
    if i mod 1000 == 0:
      let elapsed = inMilliseconds(getMonoTime() - t0).float
      let rate = if elapsed > 0: inserted.float / (elapsed / 1000.0) else: 0.0
      echo "  ", inserted, "/10000 rows, ", elapsed.int, "ms, ", rate.int, " rows/sec"

  let elapsed = inMilliseconds(getMonoTime() - t0).float
  let rate = if elapsed > 0: inserted.float / (elapsed / 1000.0) else: 0.0
  echo ""
  echo "  Result: ", inserted, "/10000 rows inserted in ", elapsed.int,
      "ms (", rate.int, " rows/sec)"
  if errors > 0:
    echo "  Errors: ", errors

  # Step 5: Quick SELECT to verify
  echo ""
  echo "Step 5: Verifying row count..."
  discard client.forceMetadataRefresh()
  let selectRes = client.query(
    "SELECT * FROM benchspace.public.users ORDER BY id LIMIT 5",
    database = "benchspace",
    schema = "public"
  )
  if selectRes.kind == erkRows:
    echo "  First 5 rows:"
    for row in selectRes.rows:
      echo "    ", row
  elif selectRes.kind == erkStreamingRows:
    let rows = selectRes.streamIterator.consumeAllRows()
    echo "  First ", rows.len, " rows (streaming):"
    for row in rows:
      echo "    ", row
  elif selectRes.kind == erkError:
    echo "  SELECT error: ", selectRes.error

  echo ""
  echo "=== Setup Complete ==="
  echo ""
  echo "You can now query the cluster:"
  echo "  ./bin/fractio --port=9001 cluster info"
  echo "  ./bin/fractio --port=9001 node ls"
  echo ""
  echo "The space 'benchspace' has 3 replicas across 3 nodes."
  echo "Table 'benchspace.public.users' has 10,000 rows (id, name, email)."

  client.close()

when isMainModule:
  main()
