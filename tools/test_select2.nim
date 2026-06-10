import std/[os, strutils, tables]
import fractio/core/types
import fractio/protocol/client
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/distributed/meta/system_tables
import fractio/sql/executor
import fractio/distributed/space_manager

proc main() =
  let host = "127.0.0.1"
  let port = 9001

  echo "=== Test: Space manager and tables ==="

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

  # Get all spaces from the client
  let spacesCount = client.spaces.len
  echo "  Spaces count: ", spacesCount
  for sid, sinfo in client.spaces:
    echo "    Space: ", sinfo.name, " id=", $sid, " groups=", sinfo.groupIds.len

  # List tables in default
  echo ""
  echo "=== SHOW TABLES in default ==="
  let res2 = client.query("SHOW TABLES", database = "default")
  echo "  Kind: ", $res2.kind
  if res2.kind == ExecResultKind.erkError: echo "  Error: ", res2.error
  if res2.kind == ExecResultKind.erkRows or res2.kind ==
      ExecResultKind.erkStreamingRows:
    echo "  Rows: ", res2.rows.len
    for row in res2.rows: echo "    ", row

  # Try the select on default.public.test
  echo ""
  echo "=== SELECT * FROM default.public.test ==="
  let res3 = client.query(
    "SELECT * FROM default.public.test ORDER BY id",
    database = "default",
    schema = "public"
  )
  echo "  Kind: ", $res3.kind
  if res3.kind == ExecResultKind.erkError: echo "  Error: ", res3.error
  if res3.kind == ExecResultKind.erkRows or res3.kind ==
      ExecResultKind.erkStreamingRows:
    echo "  Rows: ", res3.rows.len
    for row in res3.rows: echo "    ", row

  client.close()

when isMainModule:
  main()
