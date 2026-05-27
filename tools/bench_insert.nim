# Benchmark: Insert rows into a Fractio table via direct KV operations.
# Bypasses SQL parsing/planning for maximum throughput.
#
# Each row is inserted as a direct KV put with auto-commit (txnId=zero).
# Rows are grouped into logical batches of 500 for progress reporting.
#
# Usage:
#   bench_insert <host> <port> <totalRows> <startId> [space]
#
# Defaults:
#   space = "testspace" (table: testspace.public.users)
#
# Example:
#   bench_insert 127.0.0.1 9001 10000 1
#   bench_insert 127.0.0.1 9001 10000 1 myspace

import std/[times, os, strutils, atomics, monotimes, options]
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/core/types
import fractio/core/primary_key
import fractio/core/kv_interface
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/sql/planner
import fractio/sql/ast_types
import fractio/sql/data_row
import fractio/sql/executor

const BATCH_SIZE = 500

proc main() =
  let host = paramStr(1)
  let port = parseInt(paramStr(2))
  let totalRows = parseInt(paramStr(3))
  let startId = parseInt(paramStr(4))
  let space = if paramCount() >= 5: paramStr(5) else: "testspace"

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
  echo "Metadata refreshed"

  # Resolve the table to get tableId via planner
  let descOpt = resolveQualifiedTableRef(client, space, "public", TableRef(
      database: space, schema: "public", table: "users"))
  if descOpt.isNone:
    echo "Table ", space, ".public.users not found"
    quit(1)
  let desc = descOpt.unsafeGet()
  echo "Table ID: ", desc.tableId, " columns: ", desc.columns.len

  var inserted = 0
  var errors = 0
  let t0 = getMonoTime()

  for i in countup(startId, startId + totalRows - 1):
    # Encode PK: sign-bit-flipped big-endian int64
    let pkBytes = encodeInt64BE(int64(i))
    var pkStr = newString(8)
    copyMem(unsafeAddr pkStr[0], unsafeAddr pkBytes[0], 8)

    # Encode key: /t/<tableId>/d/<pk> (scan-bound format, server adds groupId)
    let key = encodeDataRowScanBound(desc.tableId, pkStr)

    # Encode value: DataRow binary format
    var row = DataRow(columns: @[
      DataRowColumn(name: "id", value: DataRowValue(kind: drvkInt,
          intVal: int64(i))),
      DataRowColumn(name: "name", value: DataRowValue(kind: drvkString,
          strVal: "user_" & $i)),
      DataRowColumn(name: "email", value: DataRowValue(kind: drvkString,
          strVal: "user_" & $i & "@example.com")),
    ])
    let value = encodeDataRow(row)

    # Direct KV put (auto-commit, txnId=zero)
    let putRes = client.put(key, value)
    if isErr(putRes):
      if errors < 5:
        echo "Error at row ", i, ": ", putRes.err
      inc errors
    else:
      inc inserted

    # Progress report every BATCH_SIZE rows
    if (i - startId + 1) mod BATCH_SIZE == 0 or i == startId + totalRows - 1:
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
