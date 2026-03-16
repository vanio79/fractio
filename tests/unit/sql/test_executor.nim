# Tests for the SQL Executor
#
# Integration-style tests: parse SQL → plan → execute → verify KV state.
# Uses a real single-node RaftKVStoreExt.

import std/[unittest, options, json, os, strutils]
import fractio/sql/parser
import fractio/sql/ast
import fractio/sql/planner
import fractio/sql/executor
import fractio/distributed/meta/system_tables
import fractio/protocol/raft_store
import fractio/protocol/mvcc_store
import fractio/protocol/txn_manager
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types
import fractio/distributed/raft/multigroup_types
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider

# ---------------------------------------------------------------------------
# Test helper: create a single-node RaftKVStoreExt with MVCC store
# ---------------------------------------------------------------------------

var testBasePort {.global.} = 17000

proc nextBasePort(): int =
  result = testBasePort
  testBasePort += 100

proc createTestEnv(testDir: string): tuple[store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore] =
  if dirExists(testDir): removeDir(testDir)
  createDir(testDir)
  let nodeId = NodeID(1)
  let basePort = nextBasePort()
  let members = @[(nodeId: 1'u32, host: "127.0.0.1", basePort: basePort)]

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    basePort: basePort,
    host: "127.0.0.1",
    dataDir: testDir,
    electionTimeoutLowerMs: 200,
    electionTimeoutUpperMs: 400,
    heartbeatIntervalMs: 100,
  ))
  coord.start()

  doAssert coord.createAndStartGroup(GroupID(1), members)
  doAssert coord.createAndStartGroup(GroupID(2), members)

  for attempt in 0 ..< 50:
    if coord.isLeader(GroupID(1)) and coord.isLeader(GroupID(2)):
      break
    os.sleep(100)

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 5000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  let txnMgr = newTransactionManager()
  let mockTimer = MockTimeProvider(currentTime: timerTypes.Timestamp(1_000_000_000))
  let tsProvider = newTimestampProvider(mockTimer, nodeId.uint16)
  let mvccStore = newMvccTransactionStore(store, txnMgr, tsProvider)

  result = (store, mvccStore)

proc cleanupTestDir(testDir: string) =
  if dirExists(testDir):
    removeDir(testDir)

# ---------------------------------------------------------------------------
# Helper: execute SQL and return result
# ---------------------------------------------------------------------------

proc exec(store: RaftKVStoreExt, mvccStore: MvccTransactionStore, sql: string,
    database = "default", schema = "public"): ExecResult =
  executeSQL(sql, store, mvccStore, database, schema)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

suite "SQL Executor — DDL":
  var store: RaftKVStoreExt
  var mvccStore: MvccTransactionStore
  let testDir = "/tmp/fractio_test_executor_ddl_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    (store, mvccStore) = createTestEnv(testDir)

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "CREATE DATABASE":
    let res = exec(store, mvccStore, "CREATE DATABASE testdb")
    check res.kind == erkOk
    check res.okMessage == "CREATE DATABASE"

    # Verify in catalog
    let key = encodeTableKey(SYS_DATABASES_TABLE_ID, "testdb")
    let got = mvccStore.directGet(key)
    check got.isOk
    check got.value.isSome

  test "CREATE DATABASE duplicate error":
    discard exec(store, mvccStore, "CREATE DATABASE testdb")
    let res = exec(store, mvccStore, "CREATE DATABASE testdb")
    check res.kind == erkError
    check "already exists" in res.error

  test "CREATE DATABASE IF NOT EXISTS":
    discard exec(store, mvccStore, "CREATE DATABASE testdb")
    let res = exec(store, mvccStore, "CREATE DATABASE IF NOT EXISTS testdb")
    check res.kind == erkOk

  test "DROP DATABASE":
    discard exec(store, mvccStore, "CREATE DATABASE testdb")
    let res = exec(store, mvccStore, "DROP DATABASE testdb")
    check res.kind == erkOk
    # Verify removed
    let key = encodeTableKey(SYS_DATABASES_TABLE_ID, "testdb")
    let got = mvccStore.directGet(key)
    check got.isOk
    check got.value.isNone

  test "DROP DATABASE non-existent error":
    let res = exec(store, mvccStore, "DROP DATABASE nope")
    check res.kind == erkError

  test "DROP DATABASE IF EXISTS":
    let res = exec(store, mvccStore, "DROP DATABASE IF EXISTS nope")
    check res.kind == erkOk

  test "CREATE SCHEMA":
    let res = exec(store, mvccStore, "CREATE SCHEMA myschema",
        database = "testdb")
    check res.kind == erkOk

  test "DROP SCHEMA":
    discard exec(store, mvccStore, "CREATE SCHEMA myschema",
        database = "testdb")
    let res = exec(store, mvccStore, "DROP SCHEMA myschema",
        database = "testdb")
    check res.kind == erkOk

  test "CREATE TABLE":
    let res = exec(store, mvccStore,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
    check res.kind == erkOk
    check res.okMessage == "CREATE TABLE"

    # Verify catalog entry
    let key = encodeTableKey(SYS_TABLES_TABLE_ID,
        "default.public.users")
    let got = mvccStore.directGet(key)
    check got.isOk
    check got.value.isSome
    let j = parseJson(got.value.get())
    check j["name"].getStr == "users"
    check j["columns"].len == 3

  test "CREATE TABLE IF NOT EXISTS":
    discard exec(store, mvccStore,
        "CREATE TABLE users (id INT PRIMARY KEY)")
    let res = exec(store, mvccStore,
        "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY)")
    check res.kind == erkOk

  test "CREATE TABLE duplicate error":
    discard exec(store, mvccStore,
        "CREATE TABLE users (id INT PRIMARY KEY)")
    let res = exec(store, mvccStore,
        "CREATE TABLE users (id INT PRIMARY KEY)")
    check res.kind == erkError
    check "already exists" in res.error

  test "DROP TABLE":
    discard exec(store, mvccStore,
        "CREATE TABLE users (id INT PRIMARY KEY)")
    let res = exec(store, mvccStore, "DROP TABLE users")
    check res.kind == erkOk

  test "DROP TABLE IF EXISTS":
    let res = exec(store, mvccStore, "DROP TABLE IF EXISTS nope")
    check res.kind == erkOk


suite "SQL Executor — DML":
  var store: RaftKVStoreExt
  var mvccStore: MvccTransactionStore
  let testDir = "/tmp/fractio_test_executor_dml_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    (store, mvccStore) = createTestEnv(testDir)
    # Create a test table
    discard exec(store, mvccStore,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "INSERT single row":
    let res = exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    check res.kind == erkModified
    check res.count == 1

    # Verify data row exists via MVCC-aware read
    let key = encodeDataRowKey(100, "1")
    let got = mvccStore.directGet(key)
    check got.isOk
    check got.value.isSome
    let row = parseJson(got.value.get())
    check row["name"].getStr == "Alice"
    check row["age"].getInt == 30

  test "INSERT multiple rows":
    let res = exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
    check res.kind == erkModified
    check res.count == 2

  test "INSERT into non-existent table":
    let res = exec(store, mvccStore,
        "INSERT INTO nonexistent (id) VALUES (1)")
    check res.kind == erkError
    check ("not found" in res.error or "nonexistent" in res.error)

  test "SELECT all rows":
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")

    let res = exec(store, mvccStore, "SELECT * FROM users")
    check res.kind == erkRows
    check res.columns == @["id", "name", "age"]
    check res.rows.len == 2

  test "SELECT with point get":
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")

    let res = exec(store, mvccStore, "SELECT * FROM users WHERE id = 1")
    check res.kind == erkRows
    check res.rows.len == 1
    check res.rows[0][1] == "Alice" # name column

  test "SELECT with filter":
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (3, 'Carol', 35)")

    let res = exec(store, mvccStore, "SELECT * FROM users WHERE age > 28")
    check res.kind == erkRows
    check res.rows.len == 2 # Alice (30) and Carol (35)

  test "SELECT with LIMIT":
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (3, 'Carol', 35)")

    let res = exec(store, mvccStore, "SELECT * FROM users LIMIT 2")
    check res.kind == erkRows
    check res.rows.len == 2

  test "SELECT specific columns":
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")

    let res = exec(store, mvccStore, "SELECT name, age FROM users")
    check res.kind == erkRows
    check res.columns == @["name", "age"]
    check res.rows.len == 1
    check res.rows[0][0] == "Alice"
    check res.rows[0][1] == "30"

  test "SELECT from empty table":
    let res = exec(store, mvccStore, "SELECT * FROM users")
    check res.kind == erkRows
    check res.rows.len == 0

  test "SELECT from non-existent table":
    let res = exec(store, mvccStore, "SELECT * FROM nonexistent")
    check res.kind == erkError
    check ("not found" in res.error or "nonexistent" in res.error)

  test "UPDATE rows":
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")

    let res = exec(store, mvccStore, "UPDATE users SET age = 31 WHERE id = 1")
    check res.kind == erkModified
    check res.count == 1

    # Verify the update
    let sel = exec(store, mvccStore, "SELECT * FROM users WHERE id = 1")
    check sel.kind == erkRows
    check sel.rows[0][2] == "31" # age column

  test "UPDATE all rows (no WHERE)":
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")

    let res = exec(store, mvccStore, "UPDATE users SET name = 'Unknown'")
    check res.kind == erkModified
    check res.count == 2

  test "DELETE rows":
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")

    let res = exec(store, mvccStore, "DELETE FROM users WHERE id = 1")
    check res.kind == erkModified
    check res.count == 1

    # Verify deletion
    let sel = exec(store, mvccStore, "SELECT * FROM users")
    check sel.kind == erkRows
    check sel.rows.len == 1

  test "DELETE all rows (no WHERE)":
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    discard exec(store, mvccStore,
        "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")

    let res = exec(store, mvccStore, "DELETE FROM users")
    check res.kind == erkModified
    check res.count == 2

    let sel = exec(store, mvccStore, "SELECT * FROM users")
    check sel.rows.len == 0


suite "SQL Executor — Transactions":
  var store: RaftKVStoreExt
  var mvccStore: MvccTransactionStore
  let testDir = "/tmp/fractio_test_executor_txn_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    (store, mvccStore) = createTestEnv(testDir)

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "BEGIN returns OK":
    let res = exec(store, mvccStore, "BEGIN")
    check res.kind == erkOk
    check res.okMessage == "BEGIN"

  test "COMMIT returns OK":
    let res = exec(store, mvccStore, "COMMIT")
    check res.kind == erkOk
    # COMMIT without active transaction returns a message
    check "COMMIT" in res.okMessage

  test "ROLLBACK returns OK":
    let res = exec(store, mvccStore, "ROLLBACK")
    check res.kind == erkOk
    # ROLLBACK without active transaction returns a message
    check "ROLLBACK" in res.okMessage


suite "SQL Executor — SHOW statements":
  var store: RaftKVStoreExt
  var mvccStore: MvccTransactionStore
  let testDir = "/tmp/fractio_test_executor_show_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    (store, mvccStore) = createTestEnv(testDir)

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "SHOW DATABASES empty":
    let res = exec(store, mvccStore, "SHOW DATABASES")
    check res.kind == erkRows
    check res.columns == @["database_name"]
    check res.rows.len == 0

  test "SHOW DATABASES after creating some":
    discard exec(store, mvccStore, "CREATE DATABASE alpha")
    discard exec(store, mvccStore, "CREATE DATABASE beta")
    discard exec(store, mvccStore, "CREATE DATABASE gamma")
    let res = exec(store, mvccStore, "SHOW DATABASES")
    check res.kind == erkRows
    check res.rows.len == 3
    # Check all names are present (order may vary by key sort)
    var names: seq[string]
    for row in res.rows:
      names.add(row[0])
    check "alpha" in names
    check "beta" in names
    check "gamma" in names

  test "SHOW DATABASES reflects drops":
    discard exec(store, mvccStore, "CREATE DATABASE db1")
    discard exec(store, mvccStore, "CREATE DATABASE db2")
    discard exec(store, mvccStore, "DROP DATABASE db1")
    let res = exec(store, mvccStore, "SHOW DATABASES")
    check res.rows.len == 1
    check res.rows[0][0] == "db2"

  test "SHOW SCHEMAS empty":
    let res = exec(store, mvccStore, "SHOW SCHEMAS", database = "mydb")
    check res.kind == erkRows
    check res.columns == @["schema_name"]
    check res.rows.len == 0

  test "SHOW SCHEMAS after creating some":
    discard exec(store, mvccStore, "CREATE SCHEMA api", database = "mydb")
    discard exec(store, mvccStore, "CREATE SCHEMA internal", database = "mydb")
    discard exec(store, mvccStore, "CREATE SCHEMA other", database = "otherdb")
    let res = exec(store, mvccStore, "SHOW SCHEMAS", database = "mydb")
    check res.kind == erkRows
    check res.rows.len == 2
    var names: seq[string]
    for row in res.rows:
      names.add(row[0])
    check "api" in names
    check "internal" in names

  test "SHOW SCHEMAS IN specific_db":
    discard exec(store, mvccStore, "CREATE SCHEMA s1", database = "db1")
    discard exec(store, mvccStore, "CREATE SCHEMA s2", database = "db2")
    let res = exec(store, mvccStore, "SHOW SCHEMAS IN db1")
    check res.rows.len == 1
    check res.rows[0][0] == "s1"

  test "SHOW TABLES empty":
    let res = exec(store, mvccStore, "SHOW TABLES")
    check res.kind == erkRows
    check res.columns == @["table_name"]
    check res.rows.len == 0

  test "SHOW TABLES after creating some":
    discard exec(store, mvccStore, "CREATE TABLE users (id INT PRIMARY KEY)")
    discard exec(store, mvccStore, "CREATE TABLE orders (id INT PRIMARY KEY)")
    let res = exec(store, mvccStore, "SHOW TABLES")
    check res.kind == erkRows
    check res.rows.len == 2
    var names: seq[string]
    for row in res.rows:
      names.add(row[0])
    check "users" in names
    check "orders" in names

  test "SHOW TABLES filters by schema":
    discard exec(store, mvccStore, "CREATE TABLE t1 (id INT PRIMARY KEY)",
        database = "mydb", schema = "api")
    discard exec(store, mvccStore, "CREATE TABLE t2 (id INT PRIMARY KEY)",
        database = "mydb", schema = "internal")
    let res = exec(store, mvccStore, "SHOW TABLES IN api", database = "mydb")
    check res.rows.len == 1
    check res.rows[0][0] == "t1"

  test "SHOW TABLES IN db.schema":
    discard exec(store, mvccStore, "CREATE TABLE t1 (id INT PRIMARY KEY)",
        database = "db1", schema = "s1")
    discard exec(store, mvccStore, "CREATE TABLE t2 (id INT PRIMARY KEY)",
        database = "db1", schema = "s2")
    discard exec(store, mvccStore, "CREATE TABLE t3 (id INT PRIMARY KEY)",
        database = "db2", schema = "s1")
    let res = exec(store, mvccStore, "SHOW TABLES IN db1.s1")
    check res.rows.len == 1
    check res.rows[0][0] == "t1"

  test "SHOW TABLES reflects drops":
    discard exec(store, mvccStore, "CREATE TABLE t1 (id INT PRIMARY KEY)")
    discard exec(store, mvccStore, "CREATE TABLE t2 (id INT PRIMARY KEY)")
    discard exec(store, mvccStore, "DROP TABLE t1")
    let res = exec(store, mvccStore, "SHOW TABLES")
    check res.rows.len == 1
    check res.rows[0][0] == "t2"


suite "SQL Executor — USE statements":
  var store: RaftKVStoreExt
  var mvccStore: MvccTransactionStore
  let testDir = "/tmp/fractio_test_executor_use_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    (store, mvccStore) = createTestEnv(testDir)

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "USE DATABASE succeeds when database exists":
    discard exec(store, mvccStore, "CREATE DATABASE mydb")
    let res = exec(store, mvccStore, "USE DATABASE mydb")
    check res.kind == erkUseDatabase
    check res.newDatabase == "mydb"

  test "USE DATABASE fails when database does not exist":
    let res = exec(store, mvccStore, "USE DATABASE nope")
    check res.kind == erkError
    check "does not exist" in res.error

  test "USE (bare) defaults to USE DATABASE":
    discard exec(store, mvccStore, "CREATE DATABASE mydb")
    let res = exec(store, mvccStore, "USE mydb")
    check res.kind == erkUseDatabase
    check res.newDatabase == "mydb"

  test "USE SCHEMA succeeds when schema exists":
    discard exec(store, mvccStore, "CREATE DATABASE mydb")
    discard exec(store, mvccStore, "CREATE SCHEMA api", database = "mydb")
    let res = exec(store, mvccStore, "USE SCHEMA api", database = "mydb")
    check res.kind == erkUseSchema
    check res.newSchema == "api"

  test "USE SCHEMA fails when schema does not exist":
    discard exec(store, mvccStore, "CREATE DATABASE mydb")
    let res = exec(store, mvccStore, "USE SCHEMA nope", database = "mydb")
    check res.kind == erkError
    check "does not exist" in res.error

  test "USE SCHEMA fails when schema is in different database":
    discard exec(store, mvccStore, "CREATE DATABASE db1")
    discard exec(store, mvccStore, "CREATE DATABASE db2")
    discard exec(store, mvccStore, "CREATE SCHEMA api", database = "db1")
    let res = exec(store, mvccStore, "USE SCHEMA api", database = "db2")
    check res.kind == erkError


suite "SQL Executor — Full round-trip":
  var store: RaftKVStoreExt
  var mvccStore: MvccTransactionStore
  let testDir = "/tmp/fractio_test_executor_roundtrip_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    (store, mvccStore) = createTestEnv(testDir)

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "full DDL + DML round-trip":
    # Create database
    var res = exec(store, mvccStore, "CREATE DATABASE myapp")
    check res.kind == erkOk

    # Create schema
    res = exec(store, mvccStore, "CREATE SCHEMA api", database = "myapp")
    check res.kind == erkOk

    # Create table
    res = exec(store, mvccStore,
        "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)",
        database = "myapp", schema = "api")
    check res.kind == erkOk

    # Insert rows
    res = exec(store, mvccStore,
        "INSERT INTO products (id, name, price) VALUES (1, 'Widget', 999), (2, 'Gadget', 1999)",
        database = "myapp", schema = "api")
    check res.kind == erkModified
    check res.count == 2

    # Select all
    res = exec(store, mvccStore, "SELECT * FROM products",
        database = "myapp", schema = "api")
    check res.kind == erkRows
    check res.rows.len == 2

    # Update one
    res = exec(store, mvccStore, "UPDATE products SET price = 1099 WHERE id = 1",
        database = "myapp", schema = "api")
    check res.kind == erkModified
    check res.count == 1

    # Verify update
    res = exec(store, mvccStore, "SELECT * FROM products WHERE id = 1",
        database = "myapp", schema = "api")
    check res.kind == erkRows
    check res.rows.len == 1
    check res.rows[0][2] == "1099"

    # Delete one
    res = exec(store, mvccStore, "DELETE FROM products WHERE id = 2",
        database = "myapp", schema = "api")
    check res.kind == erkModified
    check res.count == 1

    # Verify only one remains
    res = exec(store, mvccStore, "SELECT * FROM products",
        database = "myapp", schema = "api")
    check res.kind == erkRows
    check res.rows.len == 1

    # Drop table
    res = exec(store, mvccStore, "DROP TABLE products",
        database = "myapp", schema = "api")
    check res.kind == erkOk

    # Drop schema
    res = exec(store, mvccStore, "DROP SCHEMA api", database = "myapp")
    check res.kind == erkOk

    # Drop database
    res = exec(store, mvccStore, "DROP DATABASE myapp")
    check res.kind == erkOk


suite "SQL Executor — Expression evaluation":
  var store: RaftKVStoreExt
  var mvccStore: MvccTransactionStore
  let testDir = "/tmp/fractio_test_executor_expr_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    (store, mvccStore) = createTestEnv(testDir)
    discard exec(store, mvccStore,
        "CREATE TABLE items (id INT PRIMARY KEY, name TEXT, qty INT, active BOOL)")
    discard exec(store, mvccStore,
        "INSERT INTO items (id, name, qty, active) VALUES (1, 'apple', 10, true)")
    discard exec(store, mvccStore,
        "INSERT INTO items (id, name, qty, active) VALUES (2, 'banana', 0, false)")
    discard exec(store, mvccStore,
        "INSERT INTO items (id, name, qty, active) VALUES (3, 'cherry', 5, true)")

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "WHERE with AND":
    let res = exec(store, mvccStore,
        "SELECT * FROM items WHERE qty > 0 AND active = true")
    check res.kind == erkRows
    check res.rows.len == 2 # apple and cherry

  test "WHERE with OR":
    let res = exec(store, mvccStore,
        "SELECT * FROM items WHERE qty = 0 OR qty = 10")
    check res.kind == erkRows
    check res.rows.len == 2 # apple and banana

  test "WHERE with comparison operators":
    var res = exec(store, mvccStore, "SELECT * FROM items WHERE qty >= 5")
    check res.rows.len == 2 # apple (10) and cherry (5)

    res = exec(store, mvccStore, "SELECT * FROM items WHERE qty <= 5")
    check res.rows.len == 2 # banana (0) and cherry (5)


suite "SQL Executor — EXPLAIN":
  var store: RaftKVStoreExt
  var mvccStore: MvccTransactionStore
  let testDir = "/tmp/fractio_test_executor_explain_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    (store, mvccStore) = createTestEnv(testDir)

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "EXPLAIN SELECT full scan":
    discard exec(store, mvccStore,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
    let res = exec(store, mvccStore, "EXPLAIN SELECT * FROM users")
    check res.kind == erkRows
    check res.columns == @["plan"]
    check res.rows.len == 1
    check "Scan" in res.rows[0][0]

  test "EXPLAIN SELECT point get":
    discard exec(store, mvccStore,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
    let res = exec(store, mvccStore, "EXPLAIN SELECT * FROM users WHERE id = 42")
    check res.kind == erkRows
    check res.rows.len == 1
    check "PointGet" in res.rows[0][0]
    check "42" in res.rows[0][0]

  test "EXPLAIN SELECT with filter":
    discard exec(store, mvccStore,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
    let res = exec(store, mvccStore, "EXPLAIN SELECT * FROM users WHERE age > 21")
    check res.kind == erkRows
    check "Scan" in res.rows[0][0]
    check "filter" in res.rows[0][0]

  test "EXPLAIN INSERT":
    discard exec(store, mvccStore,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
    let res = exec(store, mvccStore,
        "EXPLAIN INSERT INTO users (id, name) VALUES (1, 'Alice')")
    check res.kind == erkRows
    check "Insert" in res.rows[0][0]
    check "rows=1" in res.rows[0][0]

  test "EXPLAIN CREATE TABLE":
    let res = exec(store, mvccStore,
        "EXPLAIN CREATE TABLE t1 (id INT PRIMARY KEY)")
    check res.kind == erkRows
    check "CreateTable" in res.rows[0][0]

  test "EXPLAIN does not execute the statement":
    let res = exec(store, mvccStore,
        "EXPLAIN CREATE TABLE invisible (id INT PRIMARY KEY)")
    check res.kind == erkRows
    # The table should NOT have been created
    let showRes = exec(store, mvccStore, "SHOW TABLES")
    check showRes.kind == erkRows
    check showRes.rows.len == 0
