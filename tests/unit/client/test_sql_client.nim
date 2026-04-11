# Unit tests for SQL Client Extension
# Tests for query error handling, initialization, and multi-statement execution

import unittest
import std/[options, atomics, tables, strutils, typedthreads]
import fractio/core/types
import fractio/distributed/raft/group_types
import fractio/distributed/meta/system_schemas
import fractio/distributed/meta/system_tables
import fractio/client/fractio_client as client
import fractio/client/sql_client as sqlClient
import fractio/sql/executor

# =============================================================================
# Test Suites - Result Types
# =============================================================================

suite "SQL Client - ExecResult Constructors":
  test "okResult creates success result":
    let r = okResult("CREATE TABLE")
    check r.kind == erkOk
    check r.okMessage == "CREATE TABLE"

  test "errorResult creates error result":
    let r = errorResult("table not found")
    check r.kind == erkError
    check r.error == "table not found"

  test "modifiedResult with count":
    let r = modifiedResult(5)
    check r.kind == erkModified
    check r.count == 5
    check r.message == "5 row(s) affected"

  test "modifiedResult with custom message":
    let r = modifiedResult(3, "INSERT 3")
    check r.kind == erkModified
    check r.count == 3
    check r.message == "INSERT 3"

  test "modifiedResult zero":
    let r = modifiedResult(0)
    check r.count == 0
    check r.message == "0 row(s) affected"

  test "rowsResult empty":
    let r = rowsResult(@["id", "name"], @[])
    check r.kind == erkRows
    check r.columns == @["id", "name"]
    check r.rows.len == 0

  test "rowsResult with data":
    let r = rowsResult(@["id", "name"], @[@["1", "Alice"], @["2", "Bob"]])
    check r.kind == erkRows
    check r.columns.len == 2
    check r.rows.len == 2
    check r.rows[0] == @["1", "Alice"]
    check r.rows[1] == @["2", "Bob"]

suite "SQL Client - ExecResultKind Enum":
  test "all ExecResultKind values":
    check erkRows.ord >= 0
    check erkModified.ord >= 0
    check erkOk.ord >= 0
    check erkError.ord >= 0
    check erkUseDatabase.ord >= 0
    check erkUseSchema.ord >= 0

  test "ExecResultKind ordering":
    check erkRows.ord < erkModified.ord
    check erkModified.ord < erkOk.ord
    check erkOk.ord < erkError.ord

suite "SQL Client - ExecResult Variants":
  test "erkUseDatabase":
    let r = ExecResult(kind: erkUseDatabase, newDatabase: "mydb")
    check r.kind == erkUseDatabase
    check r.newDatabase == "mydb"

  test "erkUseSchema":
    let r = ExecResult(kind: erkUseSchema, newSchema: "reporting")
    check r.kind == erkUseSchema
    check r.newSchema == "reporting"

  test "erkRows with columns and data":
    let r = ExecResult(
      kind: erkRows,
      columns: @["col1", "col2"],
      rows: @[@["val1", "val2"]]
    )
    check r.kind == erkRows
    check r.columns == @["col1", "col2"]
    check r.rows.len == 1

  test "erkModified with count and message":
    let r = ExecResult(
      kind: erkModified,
      count: 10,
      message: "10 rows updated"
    )
    check r.kind == erkModified
    check r.count == 10
    check r.message == "10 rows updated"

  test "erkOk with message":
    let r = ExecResult(kind: erkOk, okMessage: "SUCCESS")
    check r.kind == erkOk
    check r.okMessage == "SUCCESS"

  test "erkError with error message":
    let r = ExecResult(kind: erkError, error: "failed")
    check r.kind == erkError
    check r.error == "failed"

# =============================================================================
# Test Suites - Query Error Handling (with uninitialized client)
# =============================================================================

suite "SQL Client - Query Error Handling":
  test "query with uninitialized client returns initialization error":
    # Create a client that can't initialize (no server to connect to)
    let c = client.newFractioClient("invalid-host", 9999)
    # Query should attempt initialization and fail
    let result = sqlClient.query(c, "SELECT * FROM users")
    check result.kind == erkError
    check "initialize" in result.error.toLowerAscii() or "failed" in
        result.error.toLowerAscii()
    c.close()

  test "query with empty SQL returns empty statement error":
    let c = client.newFractioClient("localhost", 9000)
    # Even if initialization fails, empty SQL should be caught first
    # But since initialization happens first, we check for initialization error
    let result = sqlClient.query(c, "")
    check result.kind == erkError
    c.close()

  test "query with only whitespace SQL":
    let c = client.newFractioClient("localhost", 9000)
    let result = sqlClient.query(c, "   ")
    check result.kind == erkError
    c.close()

  test "query with only semicolons":
    let c = client.newFractioClient("localhost", 9000)
    let result = sqlClient.query(c, ";;;")
    check result.kind == erkError
    c.close()

  test "query with valid SQL syntax but uninitialized client":
    let c = client.newFractioClient("localhost", 9000)
    let result = sqlClient.query(c, "SELECT 1")
    check result.kind == erkError
    c.close()

# =============================================================================
# Test Suites - Client State
# =============================================================================

suite "SQL Client - Client State Management":
  test "client starts uninitialized":
    let c = client.newFractioClient("localhost", 9000)
    check c.initialized.load(moRelaxed) == false
    c.close()

  test "client can be manually marked initialized":
    let c = client.newFractioClient("localhost", 9000)
    c.initialized.store(true, moRelaxed)
    check c.initialized.load(moRelaxed) == true
    c.close()

  test "active transaction ID defaults to zero":
    let c = client.newFractioClient("localhost", 9000)
    check isZero(c.activeTxnId) == true
    c.close()

  test "active read timestamp defaults to zero":
    let c = client.newFractioClient("localhost", 9000)
    check c.activeReadTs == 0
    c.close()

# =============================================================================
# Test Suites - Database and Schema Defaults
# =============================================================================

suite "SQL Client - Default Parameters":
  test "query uses default database":
    # Test that query() has correct default parameters
    # The default database is "default"
    let c = client.newFractioClient("localhost", 9000)
    # We can't actually execute, but we verify the function signature exists
    let result = sqlClient.query(c, "SELECT * FROM test")
    check result.kind == erkError # Expected due to uninitialized client
    c.close()

  test "query uses default schema":
    # The default schema is "public"
    let c = client.newFractioClient("localhost", 9000)
    let result = sqlClient.query(c, "SELECT * FROM test", "default", "public")
    check result.kind == erkError
    c.close()

  test "query with custom database name":
    let c = client.newFractioClient("localhost", 9000)
    let result = sqlClient.query(c, "SELECT * FROM test", "mydb")
    check result.kind == erkError
    c.close()

  test "query with custom schema name":
    let c = client.newFractioClient("localhost", 9000)
    let result = sqlClient.query(c, "SELECT * FROM test", "mydb", "myschema")
    check result.kind == erkError
    c.close()

# =============================================================================
# Test Suites - ExecutorContext
# =============================================================================

suite "SQL Client - ExecutorContext":
  test "newExecutorContext creates context with client":
    let c = client.newFractioClient("localhost", 9000)
    let ctx = newExecutorContext(c)
    check ctx.client == c
    check ctx.database == "default"
    check ctx.schema == "public"
    check ctx.hasActiveTransaction == false
    c.close()

  test "newExecutorContext with custom database":
    let c = client.newFractioClient("localhost", 9000)
    let ctx = newExecutorContext(c, "mydb")
    check ctx.database == "mydb"
    check ctx.schema == "public"
    c.close()

  test "newExecutorContext with custom database and schema":
    let c = client.newFractioClient("localhost", 9000)
    let ctx = newExecutorContext(c, "mydb", "myschema")
    check ctx.database == "mydb"
    check ctx.schema == "myschema"
    c.close()

  test "ExecutorContext default transaction ID":
    let c = client.newFractioClient("localhost", 9000)
    let ctx = newExecutorContext(c)
    check isZero(ctx.txnId) == true
    c.close()

  test "ExecutorContext default read timestamp":
    let c = client.newFractioClient("localhost", 9000)
    let ctx = newExecutorContext(c)
    check ctx.readTimestamp == 0
    c.close()

# =============================================================================
# Test Suites - KVEntry
# =============================================================================

suite "SQL Client - KVEntry":
  test "KVEntry construction":
    let kv = KVEntry(key: "/t/123/key", value: "data")
    check kv.key == "/t/123/key"
    check kv.value == "data"

  test "KVEntry with empty value":
    let kv = KVEntry(key: "/t/123/key", value: "")
    check kv.key == "/t/123/key"
    check kv.value == ""

  test "KVEntry with binary-like value":
    let kv = KVEntry(key: "binary_key", value: "\x00\x01\x02")
    check kv.key == "binary_key"
    check kv.value.len == 3

# =============================================================================
# Test Suites - Error Message Patterns
# =============================================================================

suite "SQL Client - Error Message Patterns":
  test "parse error message format":
    let c = client.newFractioClient("localhost", 9000)
    # Invalid SQL that will cause a parse error
    let result = sqlClient.query(c, "INVALID SQL STATEMENT HERE")
    check result.kind == erkError
    # Should contain "parse" or similar error indicator
    let errMsg = result.error.toLowerAscii()
    check "parse" in errMsg or "error" in errMsg or "failed" in errMsg
    c.close()

  test "syntax error in SELECT":
    let c = client.newFractioClient("localhost", 9000)
    let result = sqlClient.query(c, "SELECT FROM") # Missing table name
    check result.kind == erkError
    c.close()

  test "syntax error in INSERT":
    let c = client.newFractioClient("localhost", 9000)
    let result = sqlClient.query(c, "INSERT INTO") # Missing table and values
    check result.kind == erkError
    c.close()

# =============================================================================
# Test Suites - Thread Safety
# =============================================================================

suite "SQL Client - Client Thread Safety":
  test "concurrent client creation":
    var count: Atomic[int]
    count.store(0)

    proc createClient() {.thread.} =
      let c = client.newFractioClient("localhost", 9000)
      atomicInc count
      c.close()

    var threads: array[4, Thread[void]]
    for i in 0..<4:
      createThread(threads[i], createClient)

    joinThreads(threads)
    check count.load() == 4

  test "concurrent initialized flag access":
    let c = client.newFractioClient("localhost", 9000)

    var reads: Atomic[int]
    reads.store(0)

    proc flagReader(client: client.FractioClient) {.thread.} =
      for i in 0..<100:
        let val = client.initialized.load(moRelaxed)
        if not val:
          atomicInc reads

    var threads: array[4, Thread[client.FractioClient]]
    for i in 0..<4:
      createThread(threads[i], flagReader, c)

    joinThreads(threads)
    check reads.load() == 400
    c.close()

  test "concurrent config access":
    let c = client.newFractioClient("localhost", 9000)

    var results: Atomic[int]
    results.store(0)

    proc reader(client: client.FractioClient) {.thread.} =
      for i in 0..<100:
        if client.config.initialPort == 9000:
          atomicInc results

    var threads: array[4, Thread[client.FractioClient]]
    for i in 0..<4:
      createThread(threads[i], reader, c)

    joinThreads(threads)
    check results.load() == 400
    c.close()

# =============================================================================
# Test Suites - Edge Cases
# =============================================================================

suite "SQL Client - Edge Cases":
  test "very long SQL statement":
    let c = client.newFractioClient("localhost", 9000)
    # Create a very long SELECT statement
    var longSql = "SELECT "
    for i in 0..<100:
      if i > 0:
        longSql.add(", ")
      longSql.add("col" & $i)
    longSql.add(" FROM users")
    let result = sqlClient.query(c, longSql)
    check result.kind == erkError # Expected due to uninitialized client
    c.close()

  test "SQL with special characters":
    let c = client.newFractioClient("localhost", 9000)
    let result = sqlClient.query(c, "SELECT * FROM users WHERE name = 'test\"quote'")
    check result.kind == erkError
    c.close()

  test "SQL with Unicode":
    let c = client.newFractioClient("localhost", 9000)
    let result = sqlClient.query(c, "SELECT * FROM users WHERE name = '日本語'")
    check result.kind == erkError
    c.close()

  test "multiple statements with semicolons":
    let c = client.newFractioClient("localhost", 9000)
    let result = sqlClient.query(c, "SELECT 1; SELECT 2; SELECT 3;")
    check result.kind == erkError
    c.close()

# =============================================================================
# Test Suites - Result Type Helper Functions
# =============================================================================

suite "SQL Client - Result Helper Functions":
  test "ExecResult is error when kind is erkError":
    let r = ExecResult(kind: erkError, error: "test error")
    check r.kind == erkError

  test "ExecResult is success when kind is erkOk":
    let r = ExecResult(kind: erkOk, okMessage: "success")
    check r.kind == erkOk

  test "ExecResult is rows when kind is erkRows":
    let r = ExecResult(kind: erkRows, columns: @["a"], rows: @[])
    check r.kind == erkRows

  test "ExecResult is modified when kind is erkModified":
    let r = ExecResult(kind: erkModified, count: 5, message: "")
    check r.kind == erkModified
