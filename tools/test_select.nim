# Quick connectivity test - runs a SELECT against a known cluster
import std/[os, strutils]
import fractio/core/types
import fractio/protocol/client
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/distributed/meta/system_tables
import fractio/sql/executor

proc main() =
  let host = "127.0.0.1"
  let port = 9001

  echo "=== Test: Connect and SELECT ==="

  var cfg = newFractioClientConfig(host, port)
  cfg.connectionTimeoutMs = 5000
  cfg.requestTimeoutMs = 10000
  cfg.maxKvRetries = 5
  let client = newFractioClient(cfg)

  if not client.initialize():
    echo "Failed to initialize client"
    quit(1)
  echo "Client initialized"

  discard client.forceMetadataRefresh()
  sleep(2000)
  discard client.forceMetadataRefresh()

  echo ""
  echo "Test 1: SELECT * FROM scaletest.public.test ORDER BY id"
  let res1 = client.query(
    "SELECT * FROM scaletest.public.test ORDER BY id",
    database = "scaletest",
    schema = "public"
  )
  echo "  Kind: ", $res1.kind
  if res1.kind == ExecResultKind.erkError:
    echo "  Error: ", res1.error
  if res1.kind == ExecResultKind.erkRows or res1.kind ==
      ExecResultKind.erkStreamingRows:
    echo "  Row count: ", res1.rows.len
    for row in res1.rows:
      echo "    ", row

  echo ""
  echo "Test 2: SELECT * FROM scaletest.public.test (no ORDER BY)"
  let res2 = client.query(
    "SELECT * FROM scaletest.public.test",
    database = "scaletest",
    schema = "public"
  )
  echo "  Kind: ", $res2.kind
  if res2.kind == ExecResultKind.erkError:
    echo "  Error: ", res2.error
  if res2.kind == ExecResultKind.erkRows or res2.kind ==
      ExecResultKind.erkStreamingRows:
    echo "  Row count: ", res2.rows.len
    for row in res2.rows:
      echo "    ", row

  client.close()

when isMainModule:
  main()
