# One-shot setup: create 'smoke' space with 3 replicas and 'smoke.public.users' table.
#
# Usage:
#   nim c --mm:atomicArc --threads:on -p:src -o:bin/smoke_setup tools/smoke_setup.nim
#   bin/smoke_setup [host] [port]

import std/[os, strutils, options, times]
import fractio/core/types
import fractio/distributed/meta/system_tables
import fractio/protocol/client
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/sql/executor

const
  DEFAULT_HOST = "127.0.0.1"
  DEFAULT_PORT = 9001
  SPACE = "smoke"
  SCHEMA = "public"
  TABLE = "users"

proc main() =
  let host = if paramCount() >= 1: paramStr(1) else: DEFAULT_HOST
  let port = if paramCount() >= 2: parseInt(paramStr(2)) else: DEFAULT_PORT

  echo "=== Smoke Setup: '", SPACE, "' space (3 replicas) + ", SPACE, ".",
      SCHEMA, ".", TABLE, " table ==="
  echo ""

  var cfg = newFractioClientConfig(host, port)
  cfg.connectionTimeoutMs = 5000
  cfg.requestTimeoutMs = 30000
  cfg.maxKvRetries = 10
  let client = newFractioClient(cfg)

  if not client.initialize():
    echo "Failed to initialize client"
    quit(1)
  discard client.forceMetadataRefresh()
  echo "Client initialized."

  # Wait for META leader stability
  echo "Waiting for META leader stability (up to 10s)..."
  var stableLeader = false
  var lastLeader: uint16 = 0
  var stableCount = 0
  for attempt in 0 ..< 20:
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

  # Try createSpace; ignore "already exists" errors.
  echo ""
  echo "Step 1: createSpace('", SPACE, "', 3)..."
  let spaceResult = client.createSpace(SPACE, 3)
  if spaceResult.isOk:
    echo "  Space created: id=", $spaceResult.spaceId, " groups=",
        spaceResult.groupCount
  else:
    if "exists" in spaceResult.err or "already" in spaceResult.err:
      echo "  Space already exists (continuing): ", spaceResult.err
    else:
      echo "  FAILED: ", spaceResult.err
      client.close()
      quit(1)

  # Wait for elections
  echo ""
  echo "Step 2: Waiting 8s for user-data group leader elections..."
  sleep(8000)
  discard client.forceMetadataRefresh()
  sleep(1000)

  # Probe: does the table already exist?
  echo ""
  echo "Step 3: probe ", SPACE, ".", SCHEMA, ".", TABLE, "..."
  let probe = client.query(
    "SELECT id FROM " & SPACE & "." & SCHEMA & "." & TABLE &
    " ORDER BY id ASC LIMIT 1",
    database = SPACE, schema = SCHEMA)
  if probe.kind == erkRows or probe.kind == erkStreamingRows:
    echo "  Table already exists (continuing)."
  elif probe.kind == erkError:
    if "not found" in probe.error or "does not exist" in probe.error or
        "unknown" in probe.error:
      echo "  Table missing -> creating it..."
      var tableRes = client.query(
        "CREATE TABLE " & SPACE & "." & SCHEMA & "." & TABLE &
        " (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)",
        database = SPACE, schema = SCHEMA)
      if tableRes.kind == erkError and "no connection" in tableRes.error:
        echo "  Retrying CREATE TABLE after refresh..."
        discard client.forceMetadataRefresh()
        sleep(2000)
        tableRes = client.query(
          "CREATE TABLE " & SPACE & "." & SCHEMA & "." & TABLE &
          " (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)",
          database = SPACE, schema = SCHEMA)
      if tableRes.kind == erkError:
        echo "  CREATE TABLE error: ", tableRes.error
        client.close()
        quit(1)
      echo "  Table created."
    else:
      echo "  Unexpected probe error: ", probe.error
      echo "  Attempting CREATE TABLE anyway..."
      var tableRes = client.query(
        "CREATE TABLE " & SPACE & "." & SCHEMA & "." & TABLE &
        " (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)",
        database = SPACE, schema = SCHEMA)
      if tableRes.kind == erkError:
        echo "  CREATE TABLE error: ", tableRes.error
        client.close()
        quit(1)
      echo "  Table created."

  echo ""
  echo "=== Smoke setup complete ==="
  client.close()

when isMainModule:
  main()
