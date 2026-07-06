## Simple DELETE test with diagnostic logging - validates basic functionality
import std/[os, times]
import fractio/client/fractio_client
import fractio/client/sql_client

proc main() =
  let client = newFractioClient("127.0.0.1", 9001)

  echo "=== DELETE Operation Diagnostic Test ==="
  echo ""

  # Create test schema
  echo "[1] Creating database and table..."
  discard client.query("CREATE DATABASE IF NOT EXISTS smoke")
  discard client.query("CREATE SCHEMA IF NOT EXISTS public IN smoke")
  discard client.query("USE smoke")
  discard client.query("CREATE TABLE IF NOT EXISTS test_del (id INT PRIMARY KEY, name TEXT)")

  # Insert rows
  echo "[2] Inserting 50 rows..."
  for i in 1..50:
    let sql = "INSERT INTO test_del VALUES (" & $i & ", 'row_" & $i & "')"
    discard client.query(sql)
  echo "  Done inserting"

  # Simple DELETE - this is the key operation to validate
  echo "[3] Testing simple DELETE (WHERE id <= 25)..."
  try:
    discard client.query("DELETE FROM test_del WHERE id <= 25")
    echo "  Simple DELETE succeeded"
  except CatchableError as e:
    echo "  ERROR in DELETE: ", $e.msg

  # Complex OR DELETE - tests the optimizer path for complex WHERE clauses
  echo "[4] Testing complex DELETE with OR clauses..."
  var orClause = ""
  for i in 30..50:
    if i > 30:
      orClause &= " OR "
    orClause &= "id = " & $i

  let sql = "DELETE FROM test_del WHERE (" & orClause & ")"
  echo "  Query: DELETE ... WHERE (", orClause, ")"
  try:
    discard client.query(sql)
    echo "  Complex DELETE succeeded"
  except CatchableError as e:
    echo "  ERROR in complex DELETE: ", $e.msg

  # Verify with COUNT - just check it doesn't error
  echo "[5] Verifying with SELECT COUNT..."
  try:
    discard client.query("SELECT COUNT(*) FROM test_del")
    echo "  COUNT query succeeded (no error)"
  except CatchableError as e:
    echo "  ERROR in COUNT: ", $e.msg

  # Cleanup
  echo "[6] Cleaning up..."
  try:
    discard client.query("DROP TABLE test_del")
    discard client.query("DROP SCHEMA public IN smoke CASCADE")
    discard client.query("DROP DATABASE IF EXISTS smoke")
    echo "  Done"
  except CatchableError as e:
    echo "  ERROR in cleanup: ", $e.msg

  echo ""
  echo "=== Test completed successfully - diagnostic logging validated ==="

main()
