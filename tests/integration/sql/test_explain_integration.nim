# Integration tests for EXPLAIN SQL statement
#
# Full round-trip tests: parse → plan → execute via executeSQL().
# Uses a real single-node RaftKVStoreExt with Raft groups.
# Verifies end-to-end behavior including:
#   - EXPLAIN returns rows with correct column/format
#   - EXPLAIN does not mutate any state
#   - EXPLAIN works for every statement type
#   - EXPLAIN output contains expected plan details

import std/[unittest, options, json, os, strutils]
import fractio/sql/executor
import fractio/sql/planner
import fractio/distributed/meta/system_tables
import fractio/protocol/raft_store
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc createTestStore(testDir: string): RaftKVStoreExt =
  createDir(testDir)
  let nodeId = NodeID(1)
  let coord = newMultiRaftCoordinator(CoordinatorConfig(
    nodeId: nodeId,
    numWorkers: 1,
    electionTimeoutNs: 5_000_000_000'i64,
    heartbeatIntervalNs: 1_000_000_000'i64,
    storagePath: testDir,
    proposeTimeoutMs: 5000,
  ))
  for groupId in [META_GROUP_ID, DATA_GROUP_START_ID]:
    var desc = newGroupDescriptor(groupId)
    let myReplica = desc.addReplica(nodeId, rtVoter)
    let group = coord.createGroup(desc, myReplica.replicaId)
    group.becomeLeader()
  coord.start()
  result = newRaftKVStoreExt(coord)
  result.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

proc cleanupTestDir(testDir: string) =
  if dirExists(testDir):
    removeDir(testDir)

proc exec(store: RaftKVStoreExt, sql: string,
    database = "default", schema = "public"): ExecResult =
  executeSQL(sql, store, database, schema)

# ---------------------------------------------------------------------------
# Suite 1: EXPLAIN round-trip with real data
# ---------------------------------------------------------------------------

suite "EXPLAIN integration — round-trip":
  var store: RaftKVStoreExt
  let testDir = "/tmp/fractio_test_explain_integ_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    store = createTestStore(testDir)
    # Seed a database with schema and table
    discard exec(store, "CREATE DATABASE testdb")
    discard exec(store, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT, email TEXT)",
        database = "testdb")
    discard exec(store,
        "INSERT INTO users (id, name, age, email) VALUES (1, 'Alice', 30, 'alice@test.com')",
        database = "testdb")
    discard exec(store,
        "INSERT INTO users (id, name, age, email) VALUES (2, 'Bob', 25, 'bob@test.com')",
        database = "testdb")
    discard exec(store,
        "INSERT INTO users (id, name, age, email) VALUES (3, 'Charlie', 35, 'charlie@test.com')",
        database = "testdb")

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "EXPLAIN SELECT * returns Scan plan":
    let res = exec(store, "EXPLAIN SELECT * FROM users", database = "testdb")
    check res.kind == erkRows
    check res.columns == @["plan"]
    check res.rows.len == 1
    check "Scan" in res.rows[0][0]
    check "table_id=" in res.rows[0][0]

  test "EXPLAIN SELECT WHERE pk = val returns PointGet plan":
    let res = exec(store, "EXPLAIN SELECT * FROM users WHERE id = 2",
        database = "testdb")
    check res.kind == erkRows
    check "PointGet" in res.rows[0][0]
    check "key=2" in res.rows[0][0]

  test "EXPLAIN SELECT with non-PK filter returns Scan with filter":
    let res = exec(store, "EXPLAIN SELECT name FROM users WHERE age > 28",
        database = "testdb")
    check res.kind == erkRows
    check "Scan" in res.rows[0][0]
    check "filter=(age > 28)" in res.rows[0][0]
    check "\"name\"" in res.rows[0][0]

  test "EXPLAIN SELECT with LIMIT":
    let res = exec(store, "EXPLAIN SELECT * FROM users LIMIT 10",
        database = "testdb")
    check res.kind == erkRows
    check "limit=10" in res.rows[0][0]

  test "EXPLAIN SELECT specific columns":
    let res = exec(store, "EXPLAIN SELECT name, email FROM users",
        database = "testdb")
    check res.kind == erkRows
    check "\"name\"" in res.rows[0][0]
    check "\"email\"" in res.rows[0][0]

  test "EXPLAIN INSERT shows row count":
    let res = exec(store,
        "EXPLAIN INSERT INTO users (id, name, age, email) VALUES (10, 'Dan', 40, 'd@t.com'), (11, 'Eve', 22, 'e@t.com')",
        database = "testdb")
    check res.kind == erkRows
    check "Insert" in res.rows[0][0]
    check "rows=2" in res.rows[0][0]

  test "EXPLAIN UPDATE shows filter and set count":
    let res = exec(store, "EXPLAIN UPDATE users SET name = 'Z', age = 99 WHERE id = 1",
        database = "testdb")
    check res.kind == erkRows
    check "Update" in res.rows[0][0]
    check "filter=(id = 1)" in res.rows[0][0]
    check "set=[2 cols]" in res.rows[0][0]

  test "EXPLAIN DELETE shows filter":
    let res = exec(store, "EXPLAIN DELETE FROM users WHERE age < 30",
        database = "testdb")
    check res.kind == erkRows
    check "Delete" in res.rows[0][0]
    check "filter=(age < 30)" in res.rows[0][0]

# ---------------------------------------------------------------------------
# Suite 2: EXPLAIN does not mutate state
# ---------------------------------------------------------------------------

suite "EXPLAIN integration — no side effects":
  var store: RaftKVStoreExt
  let testDir = "/tmp/fractio_test_explain_nosideeff_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    store = createTestStore(testDir)

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "EXPLAIN CREATE DATABASE does not create the database":
    let res = exec(store, "EXPLAIN CREATE DATABASE phantom")
    check res.kind == erkRows
    check "CreateDatabase" in res.rows[0][0]
    # Database should NOT exist
    let showRes = exec(store, "SHOW DATABASES")
    for row in showRes.rows:
      check row[0] != "phantom"

  test "EXPLAIN CREATE TABLE does not create the table":
    discard exec(store, "CREATE DATABASE testdb")
    let res = exec(store, "EXPLAIN CREATE TABLE t1 (id INT PRIMARY KEY)",
        database = "testdb")
    check res.kind == erkRows
    let showRes = exec(store, "SHOW TABLES", database = "testdb")
    check showRes.rows.len == 0

  test "EXPLAIN INSERT does not insert rows":
    discard exec(store, "CREATE DATABASE testdb")
    discard exec(store, "CREATE TABLE items (id INT PRIMARY KEY, val TEXT)",
        database = "testdb")
    let res = exec(store, "EXPLAIN INSERT INTO items (id, val) VALUES (1, 'x')",
        database = "testdb")
    check res.kind == erkRows
    # Table should be empty
    let selRes = exec(store, "SELECT * FROM items", database = "testdb")
    check selRes.kind == erkRows
    check selRes.rows.len == 0

  test "EXPLAIN UPDATE does not modify rows":
    discard exec(store, "CREATE DATABASE testdb")
    discard exec(store, "CREATE TABLE items (id INT PRIMARY KEY, val TEXT)",
        database = "testdb")
    discard exec(store, "INSERT INTO items (id, val) VALUES (1, 'original')",
        database = "testdb")
    let res = exec(store, "EXPLAIN UPDATE items SET val = 'changed' WHERE id = 1",
        database = "testdb")
    check res.kind == erkRows
    # Value should still be original
    let selRes = exec(store, "SELECT * FROM items WHERE id = 1", database = "testdb")
    check selRes.rows[0][1] == "original"

  test "EXPLAIN DELETE does not delete rows":
    discard exec(store, "CREATE DATABASE testdb")
    discard exec(store, "CREATE TABLE items (id INT PRIMARY KEY, val TEXT)",
        database = "testdb")
    discard exec(store, "INSERT INTO items (id, val) VALUES (1, 'keep')",
        database = "testdb")
    let res = exec(store, "EXPLAIN DELETE FROM items WHERE id = 1",
        database = "testdb")
    check res.kind == erkRows
    # Row should still exist
    let selRes = exec(store, "SELECT * FROM items", database = "testdb")
    check selRes.rows.len == 1

  test "EXPLAIN DROP TABLE does not drop the table":
    discard exec(store, "CREATE DATABASE testdb")
    discard exec(store, "CREATE TABLE items (id INT PRIMARY KEY)",
        database = "testdb")
    let res = exec(store, "EXPLAIN DROP TABLE items", database = "testdb")
    check res.kind == erkRows
    let showRes = exec(store, "SHOW TABLES", database = "testdb")
    check showRes.rows.len == 1
    check showRes.rows[0][0] == "items"

  test "EXPLAIN DROP DATABASE does not drop the database":
    discard exec(store, "CREATE DATABASE keepme")
    let res = exec(store, "EXPLAIN DROP DATABASE keepme")
    check res.kind == erkRows
    let showRes = exec(store, "SHOW DATABASES")
    var found = false
    for row in showRes.rows:
      if row[0] == "keepme": found = true
    check found

# ---------------------------------------------------------------------------
# Suite 3: EXPLAIN for every DDL/utility statement
# ---------------------------------------------------------------------------

suite "EXPLAIN integration — DDL and utility statements":
  var store: RaftKVStoreExt
  let testDir = "/tmp/fractio_test_explain_ddl_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    store = createTestStore(testDir)

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "EXPLAIN CREATE DATABASE":
    let res = exec(store, "EXPLAIN CREATE DATABASE mydb")
    check res.kind == erkRows
    check "CreateDatabase" in res.rows[0][0]
    check "mydb" in res.rows[0][0]

  test "EXPLAIN DROP DATABASE":
    let res = exec(store, "EXPLAIN DROP DATABASE mydb")
    check res.kind == erkRows
    check "DropDatabase" in res.rows[0][0]

  test "EXPLAIN CREATE SCHEMA":
    let res = exec(store, "EXPLAIN CREATE SCHEMA myschema")
    check res.kind == erkRows
    check "CreateSchema" in res.rows[0][0]

  test "EXPLAIN DROP SCHEMA":
    let res = exec(store, "EXPLAIN DROP SCHEMA myschema")
    check res.kind == erkRows
    check "DropSchema" in res.rows[0][0]

  test "EXPLAIN SHOW DATABASES":
    let res = exec(store, "EXPLAIN SHOW DATABASES")
    check res.kind == erkRows
    check "ShowDatabases" in res.rows[0][0]

  test "EXPLAIN SHOW SCHEMAS":
    let res = exec(store, "EXPLAIN SHOW SCHEMAS")
    check res.kind == erkRows
    check "ShowSchemas" in res.rows[0][0]

  test "EXPLAIN SHOW SCHEMAS IN mydb":
    let res = exec(store, "EXPLAIN SHOW SCHEMAS IN mydb")
    check res.kind == erkRows
    check "db=mydb" in res.rows[0][0]

  test "EXPLAIN SHOW TABLES":
    let res = exec(store, "EXPLAIN SHOW TABLES")
    check res.kind == erkRows
    check "ShowTables" in res.rows[0][0]

  test "EXPLAIN SHOW TABLES IN mydb.myschema":
    let res = exec(store, "EXPLAIN SHOW TABLES IN mydb.myschema")
    check res.kind == erkRows
    check "db=mydb" in res.rows[0][0]
    check "schema=myschema" in res.rows[0][0]

  test "EXPLAIN BEGIN":
    let res = exec(store, "EXPLAIN BEGIN")
    check res.kind == erkRows
    check "BeginTxn" in res.rows[0][0]

  test "EXPLAIN COMMIT":
    let res = exec(store, "EXPLAIN COMMIT")
    check res.kind == erkRows
    check "CommitTxn" in res.rows[0][0]

  test "EXPLAIN ROLLBACK":
    let res = exec(store, "EXPLAIN ROLLBACK")
    check res.kind == erkRows
    check "RollbackTxn" in res.rows[0][0]

  test "EXPLAIN USE DATABASE":
    let res = exec(store, "EXPLAIN USE DATABASE mydb")
    check res.kind == erkRows
    check "UseDatabase" in res.rows[0][0]
    check "mydb" in res.rows[0][0]

  test "EXPLAIN USE SCHEMA":
    let res = exec(store, "EXPLAIN USE SCHEMA myschema")
    check res.kind == erkRows
    check "UseSchema" in res.rows[0][0]

  test "EXPLAIN CREATE SPACE":
    let res = exec(store, "EXPLAIN CREATE SPACE myspace WITH REPLICAS = 3")
    check res.kind == erkRows
    check "CreateSpace" in res.rows[0][0]
    check "replicas=3" in res.rows[0][0]

  test "EXPLAIN for non-existent table returns error":
    let res = exec(store, "EXPLAIN SELECT * FROM nonexistent")
    check res.kind == erkError
    check "not found" in res.error

# ---------------------------------------------------------------------------
# Suite 4: EXPLAIN output format consistency
# ---------------------------------------------------------------------------

suite "EXPLAIN integration — output format":
  var store: RaftKVStoreExt
  let testDir = "/tmp/fractio_test_explain_format_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    store = createTestStore(testDir)
    discard exec(store, "CREATE DATABASE testdb")
    discard exec(store, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT, qty INT)",
        database = "testdb")

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "result always has exactly one column named 'plan'":
    for sql in [
      "EXPLAIN SELECT * FROM items",
      "EXPLAIN INSERT INTO items (id, name, qty) VALUES (1, 'a', 10)",
      "EXPLAIN UPDATE items SET qty = 0 WHERE id = 1",
      "EXPLAIN DELETE FROM items WHERE id = 1",
      "EXPLAIN CREATE TABLE t2 (id INT PRIMARY KEY)",
      "EXPLAIN DROP TABLE items",
      "EXPLAIN SHOW TABLES",
      "EXPLAIN BEGIN",
    ]:
      let res = exec(store, sql, database = "testdb")
      check res.kind == erkRows
      check res.columns == @["plan"]
      check res.rows.len >= 1

  test "each plan row is a single non-empty string":
    let res = exec(store, "EXPLAIN SELECT * FROM items", database = "testdb")
    for row in res.rows:
      check row.len == 1
      check row[0].len > 0

  test "EXPLAIN with complex WHERE produces readable filter":
    let res = exec(store,
        "EXPLAIN SELECT * FROM items WHERE qty > 5 AND name = 'widget'",
        database = "testdb")
    check res.kind == erkRows
    let plan = res.rows[0][0]
    check "filter=" in plan
    check "AND" in plan
    check "qty > 5" in plan
    check "name = 'widget'" in plan
