## Insert N test rows into smoke.public.users for ORDER BY testing.
## Usage: ./insert_test_rows HOST PORT COUNT
##
## Inserts rows with id=1..COUNT, names like "User_000001" through "User_COUNT"
## so we can verify ORDER BY name DESC LIMIT 10 ordering.
##
## Schema: CREATE TABLE smoke.public.users (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)

import os, strutils, options
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/sql/executor

when isMainModule:
  let host = if paramCount() >= 1: paramStr(1) else: "127.0.0.1"
  let port = if paramCount() >= 2: parseInt(paramStr(2)) else: 9001
  let count = if paramCount() >= 3: parseInt(paramStr(3)) else: 100

  echo "Connecting to ", host, ":", port, "..."
  let client = newFractioClient(host, port)
  if not client.initialize():
    echo "ERROR: failed to initialize client"
    quit(1)

  echo "Refreshing metadata..."
  discard client.refreshMetadata()

  echo "Inserting ", count, " rows into smoke.public.users..."

  # Insert rows one at a time with explicit id (PK is required since no auto-increment)
  var inserted = 0
  var errors = 0
  for i in 1..count:
    let name = "User_" & intToStr(i, 6)
    let value = i * 10
    let sql = "INSERT INTO smoke.public.users (id, name, value) VALUES (" & $i &
        ", '" & name & "', " & $value & ")"
    let result = client.query(sql, database = "smoke")
    if result.kind == erkError:
      echo "ERROR on row ", i, ": ", result.error
      inc errors
    else:
      inc inserted
    if i mod 100 == 0:
      echo "  inserted ", i, "/", count

  echo ""
  echo "Done: ", inserted, " inserted, ", errors, " errors"
  client.close()
