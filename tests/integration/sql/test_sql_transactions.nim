# End-to-end tests for SQL transactions with MVCC
#
# Tests the complete flow from SQL statements through the executor
# with MVCC transaction support.
#
# Port range: 20700-20729

import std/[unittest, os, options]
import fractio/protocol/raft_store
import fractio/protocol/mvcc_store
import fractio/protocol/txn_manager
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
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

  test "CREATE DATABASE in transaction":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn03")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn03")

    # Start transaction
    discard executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)
    check ctx.hasActiveTransaction

    # Create database
    let res1 = executeSQLWithTxn("CREATE DATABASE testdb", raftStore, mvccStore, ctx)
    check res1.kind == erkOk

    # Commit
    let res2 = executeSQLWithTxn("COMMIT", raftStore, mvccStore, ctx)
    check res2.kind == erkOk

    # Verify database exists
    let res3 = executeSQLWithTxn("SHOW DATABASES", raftStore, mvccStore, ctx)
    check res3.kind == erkRows
    # Should have 'testdb'
    check res3.rows.len >= 1
    check res3.rows[0][0] == "testdb"

  test "CREATE TABLE in transaction":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn04")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn04")

    # Setup: create database
    discard executeSQLWithTxn("CREATE DATABASE mydb", raftStore, mvccStore, ctx)
    ctx.database = "mydb"

    # Start transaction
    discard executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)
    check ctx.hasActiveTransaction

    # Create table
    let res1 = executeSQLWithTxn(
      "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
      raftStore, mvccStore, ctx)
    check res1.kind == erkOk

    # Commit
    let res2 = executeSQLWithTxn("COMMIT", raftStore, mvccStore, ctx)
    check res2.kind == erkOk

    # Verify table exists
    let res3 = executeSQLWithTxn("SHOW TABLES", raftStore, mvccStore, ctx)
    check res3.kind == erkRows
    check res3.rows.len == 1
    check res3.rows[0][0] == "users"

  test "ROLLBACK cancels CREATE TABLE":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn05")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn05")

    # Setup
    discard executeSQLWithTxn("CREATE DATABASE rolldb", raftStore, mvccStore, ctx)
    ctx.database = "rolldb"

    # Check initial state - no tables
    let res0 = executeSQLWithTxn("SHOW TABLES", raftStore, mvccStore, ctx)
    check res0.rows.len == 0

    # Start transaction
    discard executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)

    # Create table
    discard executeSQLWithTxn(
      "CREATE TABLE temp_table (id INT PRIMARY KEY)",
      raftStore, mvccStore, ctx)

    # Rollback
    let res1 = executeSQLWithTxn("ROLLBACK", raftStore, mvccStore, ctx)
    check res1.kind == erkOk
    check not ctx.hasActiveTransaction

    # Verify table does not exist
    let res2 = executeSQLWithTxn("SHOW TABLES", raftStore, mvccStore, ctx)
    check res2.kind == erkRows
    check res2.rows.len == 0

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
  test "multiple CREATE TABLEs in one transaction":
    let (coord, raftStore, mvccStore, ctx) = makeTestEnv("/tmp/fractio_sql_txn08")
    defer: teardownTestEnv(coord, "/tmp/fractio_sql_txn08")

    discard executeSQLWithTxn("CREATE DATABASE multidb", raftStore, mvccStore, ctx)
    ctx.database = "multidb"

    # Start transaction
    discard executeSQLWithTxn("BEGIN", raftStore, mvccStore, ctx)

    # Create multiple tables
    discard executeSQLWithTxn("CREATE TABLE t1 (id INT PRIMARY KEY)", raftStore,
        mvccStore, ctx)
    discard executeSQLWithTxn("CREATE TABLE t2 (id INT PRIMARY KEY)", raftStore,
        mvccStore, ctx)
    discard executeSQLWithTxn("CREATE TABLE t3 (id INT PRIMARY KEY)", raftStore,
        mvccStore, ctx)

    # Commit
    discard executeSQLWithTxn("COMMIT", raftStore, mvccStore, ctx)

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
