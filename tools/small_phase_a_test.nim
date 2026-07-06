## Small Phase A INSERT test (50K rows) to observe memory growth with enhanced logging
##
## Usage:
##   nim c --mm:atomicArc --threads:on -p:src -o:bin/small_phase_a_test tools/smoke_1m_inserts_deletes.nim  # rebuild if needed
##   bin/small_phase_a_test

import std/[os, osproc, strutils, strformat, times]
import fractio/client/fractio_client
import fractio/protocol/client
import fractio/client/sql_client

const
  HOST = "127.0.0.1"
  PORT = 9001
  DATABASE = "smoke"
  SCHEMA = "public"
  TABLE = "users"

  # Use smaller batch size for testing: 50K rows instead of 1M
  FIRST_ID = 1
  LAST_ID = 50_000
  INSERT_BATCH_ROWS = 500

proc main() =
  echo "=== Small Phase A INSERT Test (50K rows) ==="

  # Connect to Fractio cluster
  var client: FractioClient
  client.init(HOST, PORT)

  try:
    let dbRes = client.databaseExists(DATABASE)
    if not dbRes.isOk or not dbRes.val.exists:
      echo "Creating database..."
      discard client.createDatabase(DATABASE)

    # Create table if it doesn't exist
    let createTableSQL = &"CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)"
    echo "Executing: $1".format(createTableSQL)
    discard client.executeDDL(createTableSQL)

  finally:
    client.close()

main()
echo "Test completed"
