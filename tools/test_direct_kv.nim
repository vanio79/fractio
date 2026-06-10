# Direct KV scan test - bypasses the SQL planner to verify data is on disk
import std/[os, strutils, tables, options]
import fractio/core/types
import fractio/protocol/client
import fractio/client/fractio_client
import fractio/distributed/meta/system_tables
import fractio/distributed/space_manager

proc main() =
  let host = "127.0.0.1"
  let port = 9001

  echo "=== Direct KV Scan Test ==="

  var cfg = newFractioClientConfig(host, port)
  cfg.connectionTimeoutMs = 5000
  cfg.requestTimeoutMs = 10000
  cfg.maxKvRetries = 5
  let client = newFractioClient(cfg)

  if not client.initialize():
    echo "Failed to initialize client"
    quit(1)

  discard client.forceMetadataRefresh()
  sleep(2000)
  discard client.forceMetadataRefresh()

  # Get the table ID for default.public.test
  var tableId: TableId = zeroTableId()
  for tid, tinfo in client.tables:
    if tinfo.name == "test":
      tableId = tid
      break
  echo "  Found 'test' table ID: ", $tableId, " in space: ",
      $client.tables.getOrDefault(tableId).spaceId

  if tableId == zeroTableId():
    echo "  Table not found"
    quit(1)

  # Try a scan across a wide key range
  let startKey = encodeTableKey(tableId, "")
  let endKey = encodeTableKey(tableId, "\xFF\xFF\xFF\xFF")

  echo ""
  echo "=== Scan ==="
  let scanR = client.kvScan(startKey, endKey, 100)
  echo "  Result: ", if scanR.isOk: "ok" else: "error"
  if not scanR.isOk: echo "  Error: ", scanR.err
  if scanR.isOk:
    echo "  Entries: ", scanR.val.len
    for entry in scanR.val:
      echo "    key=", entry.key.toHex, " val_len=", entry.value.len

  client.close()

when isMainModule:
  main()
