# Simple test: Insert a few rows via FractioClient to verify it works.
import std/[os, strutils, atomics]
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/core/types
import fractio/sql/executor

proc main() =
  let host = "127.0.0.1"
  let port = 9001

  echo "Connecting to ", host, ":", port, "..."
  var cfg = newFractioClientConfig(host, port)
  cfg.connectionTimeoutMs = 5000
  cfg.requestTimeoutMs = 10000
  cfg.maxKvRetries = 3
  let client = newFractioClient(cfg)

  echo "Initializing..."
  if not client.initialize():
    echo "Failed to initialize client"
    quit(1)
  echo "Client initialized"

  echo "Refreshing metadata..."
  discard client.forceMetadataRefresh()
  echo "Metadata refreshed"

  # Single row INSERT
  echo "Inserting single row..."
  let res = client.query("INSERT INTO myspace.public.users (id, name, email) VALUES (1, 'test', 'test@example.com')",
      database = "myspace", schema = "public")
  echo "Result kind: ", res.kind
  if res.kind == erkError:
    echo "Error: ", res.error
  elif res.kind == erkRows:
    echo "Rows: ", res.rows.len
  elif res.kind == erkModified:
    echo "Modified: ", res.count

  client.close()
  echo "Done"

when isMainModule:
  main()
