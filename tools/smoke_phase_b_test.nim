## Quick Phase B DELETE test - verify basic DELETE works after inserts
import std/[os, times]
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/sql/executor

proc main() =
  echo "=== Phase B DELETE Test ==="
  
  let client = newFractioClient(newFractioClientConfig("127.0.0.1", 9001))
  if not client.initialize():
    quit("Failed to initialize")
  discard client.forceMetadataRefresh()
  
  # Insert small batch first
  echo "Step 1: Insert 10 rows..."
  for i in 1..10:
    let res = client.query(
      "INSERT INTO smoke.public.users (id, name, value) VALUES (" & $i & ", 'test', " & $(i*10) & ")")
    if res.kind == erkError:
      echo "  INSERT error at i=" & $i & ": " & res.error
  
  # Verify count
  let countRes = client.query("SELECT COUNT(*) as cnt FROM smoke.public.users", database="smoke", schema="public")
  if countRes.kind == erkRows:
    echo "  Count after insert: " & countRes.rows[0][0].stringValue
  
  # Delete half the rows  
  echo "Step 2: DELETE first 5 rows..."
  var deleted = 0
  for i in 1..5:
    let res = client.query("DELETE FROM smoke.public.users WHERE id=" & $i)
    if res.kind == erkError:
      echo "  DELETE error at i=" & $i & ": " & res.error
    else:
      inc deleted
  
  # Verify final count
  let finalRes = client.query("SELECT COUNT(*) as cnt FROM smoke.public.users", database="smoke", schema="public")
  if finalRes.kind == erkRows:
    echo "  Final count: " & finalRes.rows[0][0].stringValue
    echo "  Deleted: " & $deleted & " rows"
  
  client.close()
  echo "=== Phase B test complete ==="

when isMainModule:
  main()
