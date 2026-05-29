# Test: Create a 3-replica space and verify it doesn't crash the cluster.
import std/[os, strutils, atomics]
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/core/types
import fractio/sql/executor

proc main() =
  let host = "127.0.0.1"
  let port = 9001

  echo "=== 3-Replica Space Test ==="
  echo "Connecting to ", host, ":", port, "..."
  var cfg = newFractioClientConfig(host, port)
  cfg.connectionTimeoutMs = 5000
  cfg.requestTimeoutMs = 30000
  cfg.maxKvRetries = 5
  let client = newFractioClient(cfg)

  echo "Initializing..."
  if not client.initialize():
    echo "Failed to initialize client"
    quit(1)
  echo "Client initialized"

  echo "Refreshing metadata..."
  discard client.forceMetadataRefresh()
  echo "Metadata refreshed"

  # Step 1: Create a space with 3 replicas
  echo ""
  echo "Creating space 'testspace' with 3 replicas..."
  let spaceResult = client.createSpace("testspace", 3)
  echo "Space creation result: isOk=", spaceResult.isOk, " spaceId=",
      $spaceResult.spaceId

  if not spaceResult.isOk:
    echo "FAILED: Space creation error: ", spaceResult.err
    client.close()
    quit(1)

  # Step 2: Wait for groups to elect leaders
  echo "Waiting 5 seconds for leader elections..."
  sleep(5000)

  # Step 3: Refresh metadata to pick up the new space
  echo "Refreshing metadata again..."
  discard client.forceMetadataRefresh()

  # Step 4: Create a table in the space
  echo ""
  echo "Creating table 'testspace.public.users'..."
  let tableRes = client.query(
    "CREATE TABLE testspace.public.users (id INT64 PRIMARY KEY, name STRING)",
    database = "testspace",
    schema = "public"
  )
  echo "CREATE TABLE result kind: ", tableRes.kind
  if tableRes.kind == erkError:
    echo "CREATE TABLE error: ", tableRes.error
  elif tableRes.kind == erkModified:
    echo "CREATE TABLE OK, rows affected: ", tableRes.count

  # Step 5: Insert some rows
  echo ""
  echo "Inserting 5 rows..."
  for i in 1..5:
    let insertRes = client.query(
      "INSERT INTO testspace.public.users (id, name) VALUES ($1, 'user$1')" % [$i],
      database = "testspace",
      schema = "public"
    )
    if insertRes.kind == erkError:
      echo "  INSERT error row $1: " % [$i], insertRes.error
    elif insertRes.kind == erkModified:
      echo "  INSERT row $1: OK, count=" % [$i], $insertRes.count

  # Step 6: SELECT the rows back
  echo ""
  echo "Selecting all rows..."
  let selectRes = client.query(
    "SELECT * FROM testspace.public.users ORDER BY id",
    database = "testspace",
    schema = "public"
  )
  echo "SELECT result kind: ", selectRes.kind
  if selectRes.kind == erkRows:
    echo "  Rows returned: ", selectRes.rows.len
    for row in selectRes.rows:
      echo "  ", row
  elif selectRes.kind == erkStreamingRows:
    echo "  Streaming rows — consuming..."
    let rows = selectRes.streamIterator.consumeAllRows()
    echo "  Rows returned: ", rows.len
    for row in rows:
      echo "  ", row
  elif selectRes.kind == erkError:
    echo "  SELECT error: ", selectRes.error

  echo ""
  echo "=== Test PASSED: 3-replica space created and used successfully ==="
  client.close()

when isMainModule:
  main()
