# Integration tests for EXPLAIN SQL statement using external cluster
#
# Tests the complete flow via HTTP API with a real Fractio cluster.
# EXPLAIN is stateless - it doesn't mutate any data.
#
# Port range: Uses dynamic ports assigned by test_cluster

import std/[unittest, json, strutils, sequtils, os]
import ../../test_cluster

# Kill orphaned daemons from previous test runs at startup
killOrphanedDaemons()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc checkExplainResult(res: JsonNode, expectedOp: string) =
  ## Check that an EXPLAIN result contains the expected operation
  if res.hasKey("error"):
    checkpoint("SQL execution error: " & res["error"].getStr)
    check false
    return
  check res.hasKey("kind")
  check res["kind"].getStr == "rows"
  check "plan" in res["columns"].getElems.mapIt(it.getStr)
  check res["rows"].len >= 1
  let planRow = res["rows"][0]
  let planText = planRow["plan"].getStr
  check expectedOp in planText

proc execSQL(cluster: TestCluster, sql: string,
    database = "default"): JsonNode =
  ## Execute SQL and return the result
  cluster.executeSQL(sql, database = database)

# ---------------------------------------------------------------------------
# Suite 1: EXPLAIN round-trip with real data
# ---------------------------------------------------------------------------

suite "EXPLAIN external cluster — round-trip":
  var cluster: TestCluster

  setup:
    cluster = newTestCluster(1, 1, basePort = 100)
    check cluster.start()
    # Wait for leader to be ready
    discard cluster.waitForLeader()
    # Wait for web dashboard to be ready
    check cluster.waitForWeb()

    # Setup test database and data
    discard execSQL(cluster, "CREATE DATABASE testdb")
    discard execSQL(cluster, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT, email TEXT)",
        database = "testdb")
    discard execSQL(cluster,
        "INSERT INTO users (id, name, age, email) VALUES (1, 'Alice', 30, 'alice@test.com')",
        database = "testdb")
    discard execSQL(cluster,
        "INSERT INTO users (id, name, age, email) VALUES (2, 'Bob', 25, 'bob@test.com')",
        database = "testdb")
    discard execSQL(cluster,
        "INSERT INTO users (id, name, age, email) VALUES (3, 'Charlie', 35, 'charlie@test.com')",
        database = "testdb")

  teardown:
    cluster.stop()

  test "EXPLAIN SELECT * returns Scan plan":
    let res = execSQL(cluster, "EXPLAIN SELECT * FROM users",
        database = "testdb")
    checkExplainResult(res, "Scan")
    check "table_id=" in res["rows"][0]["plan"].getStr

  test "EXPLAIN SELECT WHERE pk = val returns PointGet plan":
    let res = execSQL(cluster, "EXPLAIN SELECT * FROM users WHERE id = 2",
        database = "testdb")
    checkExplainResult(res, "PointGet")
    # Note: pgKey is now binary-encoded, so we don't check for "key=2" in the string

  test "EXPLAIN SELECT with non-PK filter returns Scan with filter":
    let res = execSQL(cluster, "EXPLAIN SELECT name FROM users WHERE age > 28",
        database = "testdb")
    checkExplainResult(res, "Scan")
    let planText = res["rows"][0]["plan"].getStr
    check "filter=(age > 28)" in planText
    check "\"name\"" in planText

  test "EXPLAIN SELECT with LIMIT":
    let res = execSQL(cluster, "EXPLAIN SELECT * FROM users LIMIT 10",
        database = "testdb")
    checkExplainResult(res, "Scan")
    check "limit=10" in res["rows"][0]["plan"].getStr

  test "EXPLAIN SELECT specific columns":
    let res = execSQL(cluster, "EXPLAIN SELECT name, email FROM users",
        database = "testdb")
    checkExplainResult(res, "Scan")
    let planText = res["rows"][0]["plan"].getStr
    check "\"name\"" in planText
    check "\"email\"" in planText

  test "EXPLAIN INSERT shows row count":
    let res = execSQL(cluster,
        "EXPLAIN INSERT INTO users (id, name, age, email) VALUES (10, 'Dan', 40, 'd@t.com'), (11, 'Eve', 22, 'e@t.com')",
        database = "testdb")
    checkExplainResult(res, "Insert")
    check "rows=2" in res["rows"][0]["plan"].getStr

  test "EXPLAIN UPDATE shows filter and set count":
    let res = execSQL(cluster, "EXPLAIN UPDATE users SET name = 'Z', age = 99 WHERE id = 1",
        database = "testdb")
    checkExplainResult(res, "Update")
    let planText = res["rows"][0]["plan"].getStr
    check "filter=(id = 1)" in planText
    check "set=[2 cols]" in planText

  test "EXPLAIN DELETE shows filter":
    let res = execSQL(cluster, "EXPLAIN DELETE FROM users WHERE age < 30",
        database = "testdb")
    checkExplainResult(res, "Delete")
    check "filter=(age < 30)" in res["rows"][0]["plan"].getStr

# ---------------------------------------------------------------------------
# Suite 2: EXPLAIN does not mutate state
# ---------------------------------------------------------------------------

suite "EXPLAIN external cluster — no side effects":
  var cluster: TestCluster

  setup:
    cluster = newTestCluster(1, 1, basePort = 200)
    check cluster.start()
    discard cluster.waitForLeader()
    check cluster.waitForWeb()

  teardown:
    cluster.stop()

  test "EXPLAIN CREATE DATABASE does not create the database":
    let res = execSQL(cluster, "EXPLAIN CREATE DATABASE phantom")
    checkExplainResult(res, "CreateDatabase")

    # Database should NOT exist
    let showRes = execSQL(cluster, "SHOW DATABASES")
    for row in showRes["rows"].getElems:
      check row["database_name"].getStr != "phantom"

  test "EXPLAIN CREATE TABLE does not create the table":
    discard execSQL(cluster, "CREATE DATABASE testdb")
    let res = execSQL(cluster, "EXPLAIN CREATE TABLE t1 (id INT PRIMARY KEY)",
        database = "testdb")
    checkExplainResult(res, "CreateTable")

    let showRes = execSQL(cluster, "SHOW TABLES", database = "testdb")
    check showRes["rows"].len == 0

  test "EXPLAIN INSERT does not insert rows":
    discard execSQL(cluster, "CREATE DATABASE testdb")
    discard execSQL(cluster, "CREATE TABLE items (id INT PRIMARY KEY, val TEXT)",
        database = "testdb")
    let res = execSQL(cluster, "EXPLAIN INSERT INTO items (id, val) VALUES (1, 'x')",
        database = "testdb")
    checkExplainResult(res, "Insert")

    # Table should be empty
    let selRes = execSQL(cluster, "SELECT * FROM items", database = "testdb")
    check selRes["rows"].len == 0

  test "EXPLAIN UPDATE does not modify rows":
    discard execSQL(cluster, "CREATE DATABASE testdb")
    discard execSQL(cluster, "CREATE TABLE items (id INT PRIMARY KEY, val TEXT)",
        database = "testdb")
    discard execSQL(cluster, "INSERT INTO items (id, val) VALUES (1, 'original')",
        database = "testdb")
    let res = execSQL(cluster, "EXPLAIN UPDATE items SET val = 'changed' WHERE id = 1",
        database = "testdb")
    checkExplainResult(res, "Update")

    # Value should still be original
    let selRes = execSQL(cluster, "SELECT * FROM items WHERE id = 1",
        database = "testdb")
    check selRes["rows"][0]["val"].getStr == "original"

  test "EXPLAIN DELETE does not delete rows":
    discard execSQL(cluster, "CREATE DATABASE testdb")
    discard execSQL(cluster, "CREATE TABLE items (id INT PRIMARY KEY, val TEXT)",
        database = "testdb")
    discard execSQL(cluster, "INSERT INTO items (id, val) VALUES (1, 'keep')",
        database = "testdb")
    let res = execSQL(cluster, "EXPLAIN DELETE FROM items WHERE id = 1",
        database = "testdb")
    checkExplainResult(res, "Delete")

    # Row should still exist
    let selRes = execSQL(cluster, "SELECT * FROM items", database = "testdb")
    check selRes["rows"].len == 1

  test "EXPLAIN DROP TABLE does not drop the table":
    discard execSQL(cluster, "CREATE DATABASE testdb")
    discard execSQL(cluster, "CREATE TABLE items (id INT PRIMARY KEY)",
        database = "testdb")
    let res = execSQL(cluster, "EXPLAIN DROP TABLE items", database = "testdb")
    checkExplainResult(res, "DropTable")

    let showRes = execSQL(cluster, "SHOW TABLES", database = "testdb")
    check showRes["rows"].len == 1
    check showRes["rows"][0]["table_name"].getStr == "items"

  test "EXPLAIN DROP DATABASE does not drop the database":
    discard execSQL(cluster, "CREATE DATABASE keepme")
    let res = execSQL(cluster, "EXPLAIN DROP DATABASE keepme")
    checkExplainResult(res, "DropDatabase")

    let showRes = execSQL(cluster, "SHOW DATABASES")
    var found = false
    for row in showRes["rows"].getElems:
      if row["database_name"].getStr == "keepme":
        found = true
    check found

# ---------------------------------------------------------------------------
# Suite 3: EXPLAIN for every DDL/utility statement
# ---------------------------------------------------------------------------

suite "EXPLAIN external cluster — DDL and utility statements":
  var cluster: TestCluster

  setup:
    cluster = newTestCluster(1, 1, basePort = 300)
    check cluster.start()
    discard cluster.waitForLeader()
    check cluster.waitForWeb()

  teardown:
    cluster.stop()

  test "EXPLAIN CREATE DATABASE":
    let res = execSQL(cluster, "EXPLAIN CREATE DATABASE mydb")
    checkExplainResult(res, "CreateDatabase")
    check "mydb" in res["rows"][0]["plan"].getStr

  test "EXPLAIN DROP DATABASE":
    let res = execSQL(cluster, "EXPLAIN DROP DATABASE mydb")
    checkExplainResult(res, "DropDatabase")

  test "EXPLAIN CREATE SCHEMA":
    let res = execSQL(cluster, "EXPLAIN CREATE SCHEMA myschema")
    checkExplainResult(res, "CreateSchema")

  test "EXPLAIN DROP SCHEMA":
    let res = execSQL(cluster, "EXPLAIN DROP SCHEMA myschema")
    checkExplainResult(res, "DropSchema")

  test "EXPLAIN SHOW DATABASES":
    let res = execSQL(cluster, "EXPLAIN SHOW DATABASES")
    checkExplainResult(res, "ShowDatabases")

  test "EXPLAIN SHOW SCHEMAS":
    let res = execSQL(cluster, "EXPLAIN SHOW SCHEMAS")
    checkExplainResult(res, "ShowSchemas")

  test "EXPLAIN SHOW SCHEMAS IN mydb":
    let res = execSQL(cluster, "EXPLAIN SHOW SCHEMAS IN mydb")
    checkExplainResult(res, "ShowSchemas")
    check "db=mydb" in res["rows"][0]["plan"].getStr

  test "EXPLAIN SHOW TABLES":
    let res = execSQL(cluster, "EXPLAIN SHOW TABLES")
    checkExplainResult(res, "ShowTables")

  test "EXPLAIN SHOW TABLES IN mydb.myschema":
    let res = execSQL(cluster, "EXPLAIN SHOW TABLES IN mydb.myschema")
    checkExplainResult(res, "ShowTables")
    let planText = res["rows"][0]["plan"].getStr
    check "db=mydb" in planText
    check "schema=myschema" in planText

  test "EXPLAIN BEGIN":
    let res = execSQL(cluster, "EXPLAIN BEGIN")
    checkExplainResult(res, "BeginTxn")

  test "EXPLAIN COMMIT":
    let res = execSQL(cluster, "EXPLAIN COMMIT")
    checkExplainResult(res, "CommitTxn")

  test "EXPLAIN ROLLBACK":
    let res = execSQL(cluster, "EXPLAIN ROLLBACK")
    checkExplainResult(res, "RollbackTxn")

  test "EXPLAIN USE DATABASE":
    let res = execSQL(cluster, "EXPLAIN USE DATABASE mydb")
    checkExplainResult(res, "UseDatabase")
    check "mydb" in res["rows"][0]["plan"].getStr

  test "EXPLAIN USE SCHEMA":
    let res = execSQL(cluster, "EXPLAIN USE SCHEMA myschema")
    checkExplainResult(res, "UseSchema")

  test "EXPLAIN CREATE SPACE":
    let res = execSQL(cluster, "EXPLAIN CREATE SPACE myspace WITH REPLICAS = 3")
    checkExplainResult(res, "CreateSpace")
    check "replicas=3" in res["rows"][0]["plan"].getStr

  test "EXPLAIN for non-existent table returns error":
    let res = execSQL(cluster, "EXPLAIN SELECT * FROM nonexistent")
    check res["kind"].getStr == "error"
    check "not found" in res["error"].getStr

# ---------------------------------------------------------------------------
# Suite 4: EXPLAIN output format consistency
# ---------------------------------------------------------------------------

suite "EXPLAIN external cluster — output format":
  var cluster: TestCluster

  setup:
    cluster = newTestCluster(1, 1, basePort = 400)
    check cluster.start()
    discard cluster.waitForLeader()
    check cluster.waitForWeb()
    discard execSQL(cluster, "CREATE DATABASE testdb")
    discard execSQL(cluster, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT, qty INT)",
        database = "testdb")

  teardown:
    cluster.stop()

  test "result always has exactly one column named 'plan'":
    let testStatements = [
      "EXPLAIN SELECT * FROM items",
      "EXPLAIN INSERT INTO items (id, name, qty) VALUES (1, 'a', 10)",
      "EXPLAIN UPDATE items SET qty = 0 WHERE id = 1",
      "EXPLAIN DELETE FROM items WHERE id = 1",
      "EXPLAIN CREATE TABLE t2 (id INT PRIMARY KEY)",
      "EXPLAIN DROP TABLE items",
      "EXPLAIN SHOW TABLES",
      "EXPLAIN BEGIN",
    ]

    for sql in testStatements:
      let res = execSQL(cluster, sql, database = "testdb")
      check res["kind"].getStr == "rows"
      check res["columns"].getElems.mapIt(it.getStr) == @["plan"]
      check res["rows"].len >= 1

  test "each plan row is a single non-empty string":
    let res = execSQL(cluster, "EXPLAIN SELECT * FROM items",
        database = "testdb")
    for row in res["rows"].getElems:
      check row.len == 1 # Single field per row
      check row["plan"].getStr.len > 0

  test "EXPLAIN with complex WHERE produces readable filter":
    let res = execSQL(cluster,
        "EXPLAIN SELECT * FROM items WHERE qty > 5 AND name = 'widget'",
        database = "testdb")
    check res["kind"].getStr == "rows"
    let plan = res["rows"][0]["plan"].getStr
    check "filter=" in plan
    check "AND" in plan
    check "qty > 5" in plan
    check "'widget'" in plan
