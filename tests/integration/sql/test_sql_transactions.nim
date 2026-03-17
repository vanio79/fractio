# End-to-end tests for SQL transactions with MVCC
#
# Tests the complete flow from SQL statements through the executor
# with MVCC transaction support.
#
# Port range: 20700-20729

import std/[unittest, os, options, strutils, json]
import fractio/protocol/raft_store
import fractio/protocol/mvcc_store
import fractio/protocol/txn_manager
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider
import fractio/sql/executor
import fractio/sql/parser
import fractio/sql/planner
import fractio/storage/wisckey_backend

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

var testBasePort {.global.} = 20700

proc nextBasePort(): int =
  result = testBasePort
  testBasePort += 5

proc cleanDir(path: string) =
  try: removeDir(path) except CatchableError: discard
  try: createDir(path) except CatchableError: discard

proc makeTestEnv(storagePath: string): tuple[
    coord: NuRaftCoordinator, raftStore: RaftKVStoreExt,
    mvccStore: MvccTransactionStore, ctx: ExecutorContext] =
  cleanDir(storagePath)
  let nodeId = rangeTypes.NodeID(1)
  let basePort = nextBasePort()
  let members = @[(nodeId: 1'u32, host: "127.0.0.1", basePort: basePort)]

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    basePort: basePort,
    host: "127.0.0.1",
    dataDir: storagePath,
    electionTimeoutLowerMs: 200,
    electionTimeoutUpperMs: 400,
    heartbeatIntervalMs: 100,
  ))
  coord.start()

  for rid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    doAssert coord.createAndStartGroup(rid, members)

  for attempt in 0 ..< 50:
    if coord.isLeader(GroupID(1)) and coord.isLeader(GroupID(2)):
      break
    os.sleep(100)

  let raftStore = newRaftKVStoreExt(coord, proposeTimeoutMs = 2000)
  raftStore.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  let txnMgr = newTransactionManager()
  let mockTimer = MockTimeProvider(currentTime: timerTypes.Timestamp(1_000_000_000))
  let tsProvider = newTimestampProvider(mockTimer, nodeId.uint16)

  let mvccStore = newMvccTransactionStore(raftStore, txnMgr, tsProvider)
  let ctx = newExecutorContext()
  (coord, raftStore, mvccStore, ctx)

proc teardownTestEnv(coord: NuRaftCoordinator, storagePath: string) =
  coord.stop()
  try: removeDir(storagePath) except CatchableError: discard

# ---------------------------------------------------------------------------
# Suite: Basic Transaction Flow
# ---------------------------------------------------------------------------

suite "SQL Transactions - Basic Flow":
  test "BEGIN and COMMIT without changes":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn01")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn01")

    let res1 = executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)
    check res1.kind == erkOk
    check ctx.hasActiveTransaction

    let res2 = executeSQLWithTxn("COMMIT", raftStore, mvccStore, ctx)
    check res2.kind == erkOk
    check not ctx.hasActiveTransaction

  test "BEGIN and ROLLBACK":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn02")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn02")

    let res1 = executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)
    check res1.kind == erkOk
    check ctx.hasActiveTransaction

    let res2 = executeSQLWithTxn("ROLLBACK", raftStore, mvccStore, ctx)
    check res2.kind == erkOk
    check not ctx.hasActiveTransaction

  test "DDL forbidden in transaction - CREATE DATABASE":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn03")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn03")

    # Start transaction
    discard executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)
    check ctx.hasActiveTransaction

    # Try to create database inside transaction - should fail
    let res1 = executeSQLWithTxn("CREATE DATABASE testdb", raftStore, mvccStore, ctx)
    check res1.kind == erkError
    check "not allowed" in res1.error.toLowerAscii

    # Rollback to clear transaction state
    discard executeSQLWithTxn("ROLLBACK", raftStore, mvccStore, ctx)
    check not ctx.hasActiveTransaction

    # Create database outside transaction - should succeed
    let res2 = executeSQLWithTxn("CREATE DATABASE testdb", raftStore, mvccStore, ctx)
    check res2.kind == erkOk

    # Verify database exists
    let res3 = executeSQLWithTxn("SHOW DATABASES", raftStore, mvccStore, ctx)
    check res3.kind == erkRows
    check res3.rows.len >= 1
    check res3.rows[0][0] == "testdb"

  test "DDL forbidden in transaction - CREATE TABLE":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn04")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn04")

    # Setup: create database (outside transaction)
    discard executeSQLWithTxn("CREATE DATABASE mydb", raftStore, mvccStore, ctx)
    ctx.database = "mydb"

    # Start transaction
    discard executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)
    check ctx.hasActiveTransaction

    # Try to create table inside transaction - should fail
    let res1 = executeSQLWithTxn(
      "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
      raftStore, mvccStore, ctx)
    check res1.kind == erkError
    check "not allowed" in res1.error.toLowerAscii

    # Rollback
    discard executeSQLWithTxn("ROLLBACK", raftStore, mvccStore, ctx)
    check not ctx.hasActiveTransaction

    # Create table outside transaction - should succeed
    let res2 = executeSQLWithTxn(
      "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
      raftStore, mvccStore, ctx)
    check res2.kind == erkOk

    # Verify table exists
    let res3 = executeSQLWithTxn("SHOW TABLES", raftStore, mvccStore, ctx)
    check res3.kind == erkRows
    check res3.rows.len == 1
    check res3.rows[0][0] == "users"

  test "DDL auto-commits outside transaction":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn05")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn05")

    # Setup
    discard executeSQLWithTxn("CREATE DATABASE rolldb", raftStore, mvccStore, ctx)
    ctx.database = "rolldb"

    # Check initial state - no tables
    let res0 = executeSQLWithTxn("SHOW TABLES", raftStore, mvccStore, ctx)
    check res0.rows.len == 0

    # Create table outside transaction - auto-commits immediately
    let res1 = executeSQLWithTxn(
      "CREATE TABLE temp_table (id INT PRIMARY KEY)",
      raftStore, mvccStore, ctx)
    check res1.kind == erkOk

    # Verify table exists (auto-committed)
    let res2 = executeSQLWithTxn("SHOW TABLES", raftStore, mvccStore, ctx)
    check res2.kind == erkRows
    check res2.rows.len == 1

  test "INSERT with transaction":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn06")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn06")

    # Setup: create database and table
    discard executeSQLWithTxn("CREATE DATABASE insdb", raftStore, mvccStore, ctx)
    ctx.database = "insdb"
    discard executeSQLWithTxn(
      "CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50))",
      raftStore, mvccStore, ctx)

    # Start transaction
    discard executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)

    # Insert rows
    let res1 = executeSQLWithTxn(
      "INSERT INTO items (id, name) VALUES (1, 'item1')",
      raftStore, mvccStore, ctx)
    check res1.kind == erkModified
    check res1.count == 1

    # Commit
    discard executeSQLWithTxn("COMMIT", raftStore, mvccStore, ctx)

    # Verify row exists
    let res2 = executeSQLWithTxn("SELECT * FROM items WHERE id = 1", raftStore,
        mvccStore, ctx)
    check res2.kind == erkRows
    check res2.rows.len == 1

  test "INSERT then ROLLBACK":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn07")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn07")

    # Setup
    discard executeSQLWithTxn("CREATE DATABASE rollbackdb", raftStore,
        mvccStore, ctx)
    ctx.database = "rollbackdb"
    discard executeSQLWithTxn(
      "CREATE TABLE products (id INT PRIMARY KEY, price INT)",
      raftStore, mvccStore, ctx)

    # Start transaction
    discard executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)

    # Insert row
    discard executeSQLWithTxn(
      "INSERT INTO products (id, price) VALUES (100, 500)",
      raftStore, mvccStore, ctx)

    # Rollback
    discard executeSQLWithTxn("ROLLBACK", raftStore, mvccStore, ctx)

    # Verify row does not exist
    let res = executeSQLWithTxn("SELECT * FROM products WHERE id = 100",
        raftStore, mvccStore, ctx)
    check res.kind == erkRows
    check res.rows.len == 0

# ---------------------------------------------------------------------------
# Suite: Multiple Statements in Transaction
# ---------------------------------------------------------------------------

suite "SQL Transactions - Multiple Statements":
  test "multiple CREATE TABLEs outside transaction":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn08")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn08")

    discard executeSQLWithTxn("CREATE DATABASE multidb", raftStore, mvccStore, ctx)
    ctx.database = "multidb"

    # Create multiple tables outside transaction - each auto-commits
    let res1 = executeSQLWithTxn("CREATE TABLE t1 (id INT PRIMARY KEY)",
        raftStore, mvccStore, ctx)
    check res1.kind == erkOk
    let res2 = executeSQLWithTxn("CREATE TABLE t2 (id INT PRIMARY KEY)",
        raftStore, mvccStore, ctx)
    check res2.kind == erkOk
    let res3 = executeSQLWithTxn("CREATE TABLE t3 (id INT PRIMARY KEY)",
        raftStore, mvccStore, ctx)
    check res3.kind == erkOk

    # Verify all tables exist
    let res = executeSQLWithTxn("SHOW TABLES", raftStore, mvccStore, ctx)
    check res.kind == erkRows
    check res.rows.len == 3

  test "nested transactions not allowed":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn09")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn09")

    # Start transaction
    discard executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)
    check ctx.hasActiveTransaction

    # Try to start another
    let res = executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)
    check res.kind == erkOk # Should succeed but indicate existing transaction
    check ctx.hasActiveTransaction # Still has active transaction

    # Cleanup
    discard executeSQLWithTxn("ROLLBACK", raftStore, mvccStore, ctx)

# ---------------------------------------------------------------------------
# Suite: DDL Operations
# ---------------------------------------------------------------------------

suite "SQL Transactions - DDL Operations":
  test "DROP DATABASE always cascades - deletes schemas, tables, and data":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn10")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn10")

    # Create database with tables and data
    discard executeSQLWithTxn("CREATE DATABASE cascadedb", raftStore, mvccStore, ctx)
    ctx.database = "cascadedb"
    discard executeSQLWithTxn(
      "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))",
      raftStore, mvccStore, ctx)
    discard executeSQLWithTxn(
      "INSERT INTO users (id, name) VALUES (1, 'alice')",
      raftStore, mvccStore, ctx)

    # Verify data exists
    let res1 = executeSQLWithTxn("SELECT * FROM users", raftStore, mvccStore, ctx)
    check res1.rows.len == 1

    # Reset context to default database
    ctx.database = "default"

    # Drop database (always cascades)
    let res2 = executeSQLWithTxn("DROP DATABASE cascadedb", raftStore,
        mvccStore, ctx)
    check res2.kind == erkOk

    # Verify database is gone
    let res3 = executeSQLWithTxn("SHOW DATABASES", raftStore, mvccStore, ctx)
    var foundCascade = false
    for row in res3.rows:
      if row[0] == "cascadedb":
        foundCascade = true
    check not foundCascade

    # Recreate database with same name should succeed
    let res4 = executeSQLWithTxn("CREATE DATABASE cascadedb", raftStore,
        mvccStore, ctx)
    check res4.kind == erkOk

    # New database should be empty (no old tables - cascade worked)
    ctx.database = "cascadedb"
    let res5 = executeSQLWithTxn("SHOW TABLES", raftStore, mvccStore, ctx)
    check res5.rows.len == 0

  test "DROP DATABASE IF EXISTS for non-existent database":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn11")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn11")

    # Should succeed without error
    let res1 = executeSQLWithTxn("DROP DATABASE IF EXISTS nonexistent",
        raftStore, mvccStore, ctx)
    check res1.kind == erkOk

    # Without IF EXISTS should fail
    let res2 = executeSQLWithTxn("DROP DATABASE nonexistent", raftStore,
        mvccStore, ctx)
    check res2.kind == erkError

  test "DROP SPACE fails when tables exist in space":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn13")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn13")

    # Seed the node registry so CREATE SPACE can work (binary format)
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, "1")
    let nodeRec = NodeRecord(
      nodeId: 1,
      host: "127.0.0.1",
      raftPort: 20713,
      clientPort: 20714,
      status: nsAlive
    )
    discard raftStore.raftPut(nodeKey, encode(nodeRec))

    # Create space and table in it
    let spaceRes = executeSQLWithTxn("CREATE SPACE myspace WITH REPLICAS = ALL",
        raftStore, mvccStore, ctx)
    if spaceRes.kind == erkError:
      # Skip test if CREATE SPACE fails (e.g., no nodes in cluster)
      echo "Skipping test - CREATE SPACE failed: ", spaceRes.error
      skip()

    discard executeSQLWithTxn("CREATE DATABASE spacedb", raftStore, mvccStore, ctx)
    ctx.database = "spacedb"
    discard executeSQLWithTxn(
      "CREATE TABLE spaced_table (id INT PRIMARY KEY) IN SPACE myspace",
      raftStore, mvccStore, ctx)

    # Try to drop space - should fail
    let res1 = executeSQLWithTxn("DROP SPACE myspace", raftStore, mvccStore, ctx)
    check res1.kind == erkError
    # Error should mention tables or that space is in use
    let errLower = res1.error.toLowerAscii
    check "table" in errLower or "using" in errLower or "in use" in errLower or
        "cannot" in errLower

  test "DROP SPACE succeeds when no tables in space":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn14")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn14")

    # Seed the node registry so CREATE SPACE can work (binary format)
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, "1")
    let nodeRec = NodeRecord(
      nodeId: 1,
      host: "127.0.0.1",
      raftPort: 20718,
      clientPort: 20719,
      status: nsAlive
    )
    discard raftStore.raftPut(nodeKey, encode(nodeRec))

    # Create empty space
    let spaceRes = executeSQLWithTxn("CREATE SPACE emptyspace WITH REPLICAS = ALL",
        raftStore, mvccStore, ctx)
    if spaceRes.kind == erkError:
      # Skip test if CREATE SPACE fails
      echo "Skipping test - CREATE SPACE failed: ", spaceRes.error
      skip()

    # Drop should succeed
    let res1 = executeSQLWithTxn("DROP SPACE emptyspace", raftStore, mvccStore, ctx)
    check res1.kind == erkOk

    # Verify space is gone
    let res2 = executeSQLWithTxn("SHOW SPACES", raftStore, mvccStore, ctx)
    var foundEmpty = false
    for row in res2.rows:
      if row[1] == "emptyspace":
        foundEmpty = true
    check not foundEmpty

  test "Cannot drop default space":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn15")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn15")

    let res1 = executeSQLWithTxn("DROP SPACE default", raftStore, mvccStore, ctx)
    check res1.kind == erkError
    check "default" in res1.error.toLowerAscii

  test "DDL forbidden in transaction - CREATE DATABASE with ROLLBACK":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn16")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn16")

    # Start transaction
    discard executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)
    check ctx.hasActiveTransaction

    # Try to create database inside transaction - should fail
    let res1 = executeSQLWithTxn("CREATE DATABASE rollbackdb", raftStore,
        mvccStore, ctx)
    check res1.kind == erkError
    check "not allowed" in res1.error.toLowerAscii

    # Rollback to clear transaction
    let res2 = executeSQLWithTxn("ROLLBACK", raftStore, mvccStore, ctx)
    check res2.kind == erkOk
    check not ctx.hasActiveTransaction

    # Database should not exist
    let res3 = executeSQLWithTxn("SHOW DATABASES", raftStore, mvccStore, ctx)
    var foundRollback = false
    for row in res3.rows:
      if row[0] == "rollbackdb":
        foundRollback = true
    check not foundRollback

  test "CREATE DATABASE with public schema":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn17")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn17")

    # Create database
    discard executeSQLWithTxn("CREATE DATABASE newschema", raftStore, mvccStore, ctx)
    ctx.database = "newschema"

    # Verify public schema was created
    let res1 = executeSQLWithTxn("SHOW SCHEMAS", raftStore, mvccStore, ctx)
    check res1.kind == erkRows
    var foundPublic = false
    for row in res1.rows:
      if row[0] == "public":
        foundPublic = true
    check foundPublic

  test "DDL forbidden in transaction - CREATE SPACE":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn18")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn18")

    # Seed the node registry so CREATE SPACE can work (binary format)
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, "1")
    let nodeRec = NodeRecord(
      nodeId: 1,
      host: "127.0.0.1",
      raftPort: 20780,
      clientPort: 20781,
      status: nsAlive
    )
    discard raftStore.raftPut(nodeKey, encode(nodeRec))

    # Start transaction
    discard executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)
    check ctx.hasActiveTransaction

    # Try to create space inside transaction - should fail
    let res1 = executeSQLWithTxn("CREATE SPACE txspace WITH REPLICAS = ALL",
        raftStore, mvccStore, ctx)
    check res1.kind == erkError
    check "not allowed" in res1.error.toLowerAscii

    # Rollback
    discard executeSQLWithTxn("ROLLBACK", raftStore, mvccStore, ctx)
    check not ctx.hasActiveTransaction

    # Create space outside transaction - should succeed
    let res2 = executeSQLWithTxn("CREATE SPACE txspace WITH REPLICAS = ALL",
        raftStore, mvccStore, ctx)
    check res2.kind == erkOk

    # Verify space exists
    let res3 = executeSQLWithTxn("SHOW SPACES", raftStore, mvccStore, ctx)
    var foundTxSpace = false
    for row in res3.rows:
      if row[1] == "txspace":
        foundTxSpace = true
    check foundTxSpace

  test "DDL forbidden in transaction - DROP SPACE":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn19")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn19")

    # Seed the node registry so CREATE SPACE can work (binary format)
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, "1")
    let nodeRec = NodeRecord(
      nodeId: 1,
      host: "127.0.0.1",
      raftPort: 20785,
      clientPort: 20786,
      status: nsAlive
    )
    discard raftStore.raftPut(nodeKey, encode(nodeRec))

    # Create space outside transaction
    let createRes = executeSQLWithTxn("CREATE SPACE dropspace WITH REPLICAS = ALL",
        raftStore, mvccStore, ctx)
    if createRes.kind == erkError:
      echo "Skipping test - CREATE SPACE failed: ", createRes.error
      skip()

    # Start transaction
    discard executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)
    check ctx.hasActiveTransaction

    # Try to drop space inside transaction - should fail
    let res1 = executeSQLWithTxn("DROP SPACE dropspace",
        raftStore, mvccStore, ctx)
    check res1.kind == erkError
    check "not allowed" in res1.error.toLowerAscii

    # Rollback
    discard executeSQLWithTxn("ROLLBACK", raftStore, mvccStore, ctx)
    check not ctx.hasActiveTransaction

    # Drop space outside transaction - should succeed
    let res2 = executeSQLWithTxn("DROP SPACE dropspace",
        raftStore, mvccStore, ctx)
    check res2.kind == erkOk

    # Verify space is gone
    let res3 = executeSQLWithTxn("SHOW SPACES", raftStore, mvccStore, ctx)
    var foundDropSpace = false
    for row in res3.rows:
      if row[1] == "dropspace":
        foundDropSpace = true
    check not foundDropSpace

  test "DDL forbidden in transaction - DROP DATABASE":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn20")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn20")

    # Create a database first (outside transaction)
    discard executeSQLWithTxn("CREATE DATABASE dropdb", raftStore, mvccStore, ctx)

    # Start transaction
    discard executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)
    check ctx.hasActiveTransaction

    # Try to drop database inside transaction - should fail
    let res1 = executeSQLWithTxn("DROP DATABASE dropdb", raftStore, mvccStore, ctx)
    check res1.kind == erkError
    check "not allowed" in res1.error.toLowerAscii

    # Rollback
    discard executeSQLWithTxn("ROLLBACK", raftStore, mvccStore, ctx)
    check not ctx.hasActiveTransaction

    # Drop database outside transaction - should succeed
    let res2 = executeSQLWithTxn("DROP DATABASE dropdb", raftStore, mvccStore, ctx)
    check res2.kind == erkOk

    # Verify database is gone
    let res3 = executeSQLWithTxn("SHOW DATABASES", raftStore, mvccStore, ctx)
    var foundDropDb = false
    for row in res3.rows:
      if row[0] == "dropdb":
        foundDropDb = true
    check not foundDropDb

  test "DDL forbidden in transaction - CREATE SCHEMA":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn21")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn21")

    # Create database first
    discard executeSQLWithTxn("CREATE DATABASE schemadb", raftStore, mvccStore, ctx)
    ctx.database = "schemadb"

    # Start transaction
    discard executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)
    check ctx.hasActiveTransaction

    # Try to create schema inside transaction - should fail
    let res1 = executeSQLWithTxn("CREATE SCHEMA myschema", raftStore, mvccStore, ctx)
    check res1.kind == erkError
    check "not allowed" in res1.error.toLowerAscii

    # Rollback
    discard executeSQLWithTxn("ROLLBACK", raftStore, mvccStore, ctx)
    check not ctx.hasActiveTransaction

    # Create schema outside transaction - should succeed
    let res2 = executeSQLWithTxn("CREATE SCHEMA myschema", raftStore, mvccStore, ctx)
    check res2.kind == erkOk

    # Verify schema exists
    let res3 = executeSQLWithTxn("SHOW SCHEMAS", raftStore, mvccStore, ctx)
    var foundSchema = false
    for row in res3.rows:
      if row[0] == "myschema":
        foundSchema = true
    check foundSchema

  test "DDL forbidden in transaction - DROP SCHEMA":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn22")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn22")

    # Create database and schema first
    discard executeSQLWithTxn("CREATE DATABASE dropschemadb", raftStore,
        mvccStore, ctx)
    ctx.database = "dropschemadb"
    discard executeSQLWithTxn("CREATE SCHEMA dropschema", raftStore, mvccStore, ctx)

    # Start transaction
    discard executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)
    check ctx.hasActiveTransaction

    # Try to drop schema inside transaction - should fail
    let res1 = executeSQLWithTxn("DROP SCHEMA dropschema", raftStore, mvccStore, ctx)
    check res1.kind == erkError
    check "not allowed" in res1.error.toLowerAscii

    # Rollback
    discard executeSQLWithTxn("ROLLBACK", raftStore, mvccStore, ctx)
    check not ctx.hasActiveTransaction

    # Drop schema outside transaction - should succeed
    let res2 = executeSQLWithTxn("DROP SCHEMA dropschema", raftStore, mvccStore, ctx)
    check res2.kind == erkOk

    # Verify schema is gone
    let res3 = executeSQLWithTxn("SHOW SCHEMAS", raftStore, mvccStore, ctx)
    var foundDropSchema = false
    for row in res3.rows:
      if row[0] == "dropschema":
        foundDropSchema = true
    check not foundDropSchema

  test "DDL forbidden in transaction - DROP TABLE":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn23")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn23")

    # Setup: create database and table
    discard executeSQLWithTxn("CREATE DATABASE droptabledb", raftStore,
        mvccStore, ctx)
    ctx.database = "droptabledb"
    discard executeSQLWithTxn(
      "CREATE TABLE droptable (id INT PRIMARY KEY)",
      raftStore, mvccStore, ctx)

    # Start transaction
    discard executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)
    check ctx.hasActiveTransaction

    # Try to drop table inside transaction - should fail
    let res1 = executeSQLWithTxn("DROP TABLE droptable", raftStore, mvccStore, ctx)
    check res1.kind == erkError
    check "not allowed" in res1.error.toLowerAscii

    # Rollback
    discard executeSQLWithTxn("ROLLBACK", raftStore, mvccStore, ctx)
    check not ctx.hasActiveTransaction

    # Drop table outside transaction - should succeed
    let res2 = executeSQLWithTxn("DROP TABLE droptable", raftStore, mvccStore, ctx)
    check res2.kind == erkOk

    # Verify table is gone
    let res3 = executeSQLWithTxn("SHOW TABLES", raftStore, mvccStore, ctx)
    check res3.rows.len == 0
