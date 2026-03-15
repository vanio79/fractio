# Tests for the SQL Planner
#
# Verifies that each statement kind produces the correct PlanOp(s)
# with correct KV key generation.

import std/[unittest, options, json, os, strutils]
import fractio/sql/parser
import fractio/sql/ast
import fractio/sql/planner
import fractio/distributed/meta/system_tables
import fractio/protocol/raft_store
import fractio/protocol/mvcc_store
import fractio/protocol/txn_manager
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider

# ---------------------------------------------------------------------------
# Test helper: create a single-node RaftKVStoreExt
# ---------------------------------------------------------------------------

var testBasePort {.global.} = 17000

proc nextBasePort(): int =
  result = testBasePort
  testBasePort += 100

proc createTestStore(testDir: string): tuple[store: RaftKVStoreExt,
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

  # Create MVCC store for catalog operations
  let txnMgr = newTransactionManager()
  let mockTimer = MockTimeProvider(currentTime: timerTypes.Timestamp(1_000_000_000))
  let tsProvider = newTimestampProvider(mockTimer, nodeId.uint16)
  let mvccStore = newMvccTransactionStore(store, txnMgr, tsProvider)

  result = (store, mvccStore)

proc cleanupTestDir(testDir: string) =
  if dirExists(testDir):
    removeDir(testDir)

# ---------------------------------------------------------------------------
# Helper: seed a table into the catalog
# ---------------------------------------------------------------------------

proc seedTable(store: RaftKVStoreExt, database, schema, name: string,
    tableId: uint32, columns: seq[tuple[name: string, typ: string]],
    pk: seq[string]) =
  var colsJson = newJArray()
  for (cname, ctype) in columns:
    colsJson.add(%*{"name": cname, "type": ctype, "notNull": false,
        "primaryKey": cname in pk})
  let value = %*{
    "tableId": int(tableId),
    "name": name,
    "schema": schema,
    "database": database,
    "columns": colsJson,
    "primaryKey": pk,
  }
  let key = encodeTableKey(SYS_TABLES_TABLE_ID,
      database & "." & schema & "." & name)
  discard store.raftPut(key, $value)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

suite "SQL Planner":
  var store: RaftKVStoreExt
  var mvccStore: MvccTransactionStore
  let testDir = "/tmp/fractio_test_planner_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    (store, mvccStore) = createTestStore(testDir)

  teardown:
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "plan CREATE DATABASE":
    let stmt = parseStatement("CREATE DATABASE mydb")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poCreateDatabase
    check plan.ops[0].cdbName == "mydb"
    check plan.ops[0].cdbIfNotExists == false

  test "plan CREATE DATABASE IF NOT EXISTS":
    let stmt = parseStatement("CREATE DATABASE IF NOT EXISTS mydb")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poCreateDatabase
    check plan.ops[0].cdbIfNotExists == true

  test "plan CREATE DATABASE WITH REPLICAS":
    let stmt = parseStatement("CREATE DATABASE mydb WITH REPLICAS = 3")
    let plan = planStatement(stmt, store)
    check plan.ops[0].cdbReplicas == some(3)

  test "plan DROP DATABASE":
    let stmt = parseStatement("DROP DATABASE mydb")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poDropDatabase
    check plan.ops[0].ddbName == "mydb"
    check plan.ops[0].ddbIfExists == false

  test "plan DROP DATABASE IF EXISTS":
    let stmt = parseStatement("DROP DATABASE IF EXISTS mydb")
    let plan = planStatement(stmt, store)
    check plan.ops[0].ddbIfExists == true

  test "plan CREATE SCHEMA":
    let stmt = parseStatement("CREATE SCHEMA myschema")
    let plan = planStatement(stmt, store, database = "testdb")
    check plan.ops.len == 1
    check plan.ops[0].kind == poCreateSchema
    check plan.ops[0].csName == "myschema"
    check plan.ops[0].csDatabase == "testdb"

  test "plan DROP SCHEMA":
    let stmt = parseStatement("DROP SCHEMA myschema")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poDropSchema
    check plan.ops[0].dsName == "myschema"

  test "plan CREATE TABLE":
    let stmt = parseStatement(
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poCreateTable
    check plan.ops[0].ctName == "users"
    # Verify the JSON descriptor contains column info
    let j = parseJson(plan.ops[0].ctValue)
    check j["name"].getStr == "users"
    check j["columns"].len == 3
    check j["tableId"].getInt >= int(FIRST_USER_TABLE_ID)

  test "plan CREATE TABLE IF NOT EXISTS":
    let stmt = parseStatement(
        "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY)")
    let plan = planStatement(stmt, store)
    check plan.ops[0].ctIfNotExists == true

  test "plan DROP TABLE":
    let stmt = parseStatement("DROP TABLE users")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poDropTable
    check plan.ops[0].dtName == "users"

  test "plan INSERT":
    seedTable(store, "default", "public", "users", 100,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement(
        "INSERT INTO users (id, name) VALUES (1, 'Alice')")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poInsert
    check plan.ops[0].insTableId == 100'u32
    check plan.ops[0].insRows.len == 1
    let row = parseJson(plan.ops[0].insRows[0])
    check row["id"].getInt == 1
    check row["name"].getStr == "Alice"

  test "plan INSERT multiple rows":
    seedTable(store, "default", "public", "items", 101,
      @[("id", "INT"), ("val", "TEXT")], @["id"])
    let stmt = parseStatement(
        "INSERT INTO items (id, val) VALUES (1, 'a'), (2, 'b')")
    let plan = planStatement(stmt, store)
    check plan.ops[0].insRows.len == 2

  test "plan INSERT with table not found raises":
    expect(PlanError):
      let stmt = parseStatement("INSERT INTO nonexistent VALUES (1)")
      discard planStatement(stmt, store)

  test "plan SELECT with point get":
    seedTable(store, "default", "public", "users", 100,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("SELECT * FROM users WHERE id = 42")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poPointGet
    check plan.ops[0].pgTableId == 100'u32
    check plan.ops[0].pgKey == "42"

  test "plan SELECT full scan":
    seedTable(store, "default", "public", "users", 100,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("SELECT * FROM users")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poScan
    check plan.ops[0].scTableId == 100'u32
    check plan.ops[0].scFilter.isNone

  test "plan SELECT with filter (not point get)":
    seedTable(store, "default", "public", "users", 100,
      @[("id", "INT"), ("name", "TEXT"), ("age", "INT")], @["id"])
    let stmt = parseStatement("SELECT * FROM users WHERE age > 21")
    let plan = planStatement(stmt, store)
    check plan.ops[0].kind == poScan
    check plan.ops[0].scFilter.isSome

  test "plan SELECT with LIMIT":
    seedTable(store, "default", "public", "users", 100,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("SELECT * FROM users LIMIT 10")
    let plan = planStatement(stmt, store)
    check plan.ops[0].kind == poScan
    check plan.ops[0].scLimit == 10'u32

  test "plan SELECT specific columns":
    seedTable(store, "default", "public", "users", 100,
      @[("id", "INT"), ("name", "TEXT"), ("age", "INT")], @["id"])
    let stmt = parseStatement("SELECT name, age FROM users")
    let plan = planStatement(stmt, store)
    check plan.ops[0].scColumns == @["name", "age"]

  test "plan UPDATE":
    seedTable(store, "default", "public", "users", 100,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("UPDATE users SET name = 'Bob' WHERE id = 1")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poUpdate
    check plan.ops[0].upTableId == 100'u32
    check plan.ops[0].upSets.len == 1
    check plan.ops[0].upSets[0].col == "name"

  test "plan DELETE":
    seedTable(store, "default", "public", "users", 100,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("DELETE FROM users WHERE id = 1")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poDelete
    check plan.ops[0].delTableId == 100'u32

  test "plan BEGIN":
    let stmt = parseStatement("BEGIN")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poBeginTxn
    check plan.ops[0].btReadOnly == false

  test "plan COMMIT":
    let stmt = parseStatement("COMMIT")
    let plan = planStatement(stmt, store)
    check plan.ops[0].kind == poCommitTxn

  test "plan ROLLBACK":
    let stmt = parseStatement("ROLLBACK")
    let plan = planStatement(stmt, store)
    check plan.ops[0].kind == poRollbackTxn

  test "plan SHOW DATABASES":
    let stmt = parseStatement("SHOW DATABASES")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poShowDatabases

  test "plan SHOW SCHEMAS":
    let stmt = parseStatement("SHOW SCHEMAS")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poShowSchemas
    check plan.ops[0].ssDatabase == "default" # uses default database

  test "plan SHOW SCHEMAS IN mydb":
    let stmt = parseStatement("SHOW SCHEMAS IN mydb")
    let plan = planStatement(stmt, store)
    check plan.ops[0].kind == poShowSchemas
    check plan.ops[0].ssDatabase == "mydb"

  test "plan SHOW TABLES":
    let stmt = parseStatement("SHOW TABLES")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poShowTables
    check plan.ops[0].stDatabase == "default"
    check plan.ops[0].stSchema == "public"

  test "plan SHOW TABLES IN myschema":
    let stmt = parseStatement("SHOW TABLES IN myschema")
    let plan = planStatement(stmt, store)
    check plan.ops[0].kind == poShowTables
    check plan.ops[0].stSchema == "myschema"

  test "plan SHOW TABLES IN mydb.myschema":
    let stmt = parseStatement("SHOW TABLES IN mydb.myschema")
    let plan = planStatement(stmt, store)
    check plan.ops[0].kind == poShowTables
    check plan.ops[0].stDatabase == "mydb"
    check plan.ops[0].stSchema == "myschema"

  test "plan USE DATABASE":
    let stmt = parseStatement("USE DATABASE mydb")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poUseDatabase
    check plan.ops[0].udName == "mydb"

  test "plan USE (bare, defaults to database)":
    let stmt = parseStatement("USE mydb")
    let plan = planStatement(stmt, store)
    check plan.ops[0].kind == poUseDatabase
    check plan.ops[0].udName == "mydb"

  test "plan USE SCHEMA":
    let stmt = parseStatement("USE SCHEMA myschema")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poUseSchema
    check plan.ops[0].usName == "myschema"

  test "plan EXPLAIN SELECT":
    seedTable(store, "default", "public", "users", 100,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("EXPLAIN SELECT * FROM users")
    let plan = planStatement(stmt, store)
    check plan.ops.len == 1
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops.len == 1
    check plan.ops[0].exInnerPlan.ops[0].kind == poScan

  test "plan EXPLAIN SELECT point get":
    seedTable(store, "default", "public", "users", 100,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("EXPLAIN SELECT * FROM users WHERE id = 1")
    let plan = planStatement(stmt, store)
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops[0].kind == poPointGet

  test "plan EXPLAIN INSERT":
    seedTable(store, "default", "public", "users", 100,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("EXPLAIN INSERT INTO users (id, name) VALUES (1, 'Alice')")
    let plan = planStatement(stmt, store)
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops[0].kind == poInsert

  test "plan EXPLAIN CREATE TABLE":
    let stmt = parseStatement("EXPLAIN CREATE TABLE t1 (id INT PRIMARY KEY)")
    let plan = planStatement(stmt, store)
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops[0].kind == poCreateTable

  test "nextTableId allocates incrementally":
    let id1 = nextTableId(store, mvccStore)
    check id1 == FIRST_USER_TABLE_ID

    # Seed a table and check the next ID
    seedTable(store, "default", "public", "t1", id1,
      @[("id", "INT")], @["id"])
    let id2 = nextTableId(store, mvccStore)
    check id2 == id1 + 1
