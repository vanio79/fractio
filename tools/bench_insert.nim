# Benchmark: Insert rows into a Fractio table via SQL.
# Uses FractioClient SQL interface for proper key routing.
#
# Usage:
#   bench_insert <host> <port> <totalRows> <startId> [space]
#
# Defaults:
#   space = "testspace" (table: testspace.public.users)
#
# Example:
#   bench_insert 127.0.0.1 9001 10000 11
#   bench_insert 127.0.0.1 9001 10000 1 myspace

import std/[times, os, strutils, atomics, monotimes]
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/core/types
import fractio/sql/executor

proc main() =
  let host = paramStr(1)
  let port = parseInt(paramStr(2))
  let totalRows = parseInt(paramStr(3))
  let startId = parseInt(paramStr(4))
  let space = if paramCount() >= 5: paramStr(5) else: "testspace"
  let batchSize = 100

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

  # Force metadata refresh to pick up newly created tables/spaces
  discard client.forceMetadataRefresh()
  echo "Metadata refreshed"

  let tableName = space & ".public.users"

  var inserted = 0
  var errors = 0
  let t0 = getMonoTime()

  for i in countup(startId, startId + totalRows - 1, batchSize):
    let endId = min(i + batchSize - 1, startId + totalRows - 1)
    var values: seq[string] = @[]
    for j in countup(i, endId):
      values.add("(" & $j & ", 'user_" & $j & "', 'user_" & $j & "@example.com')")
    let sql = "INSERT INTO " & tableName & " (id, name, email) VALUES " &
        values.join(", ")

    let res = client.query(sql, database = space, schema = "public")
    if res.kind == erkError:
      if errors < 5:
        echo "Error at batch ", i, ": ", res.error
      inc errors
    else:
      inserted += (endId - i + 1)

    if endId mod 2000 == 0 or endId == startId + totalRows - 1:
      let elapsed = inMilliseconds(getMonoTime() - t0).float
      let rate = if elapsed > 0: inserted.float / (elapsed / 1000.0) else: 0.0
      echo "  ", inserted, "/", totalRows, " rows, ", elapsed.int, "ms, ",
          rate.int, " rows/sec"

  let elapsed = inMilliseconds(getMonoTime() - t0).float
  let rate = if elapsed > 0: inserted.float / (elapsed / 1000.0) else: 0.0
  echo ""
  echo "Result: ", inserted, "/", totalRows, " rows inserted in ", elapsed.int,
      "ms (", rate.int, " rows/sec)"
  if errors > 0:
    echo "Errors: ", errors

  client.close()

when isMainModule:
  main()
