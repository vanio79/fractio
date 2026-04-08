# Tests for the SQL Planner
#
# Verifies that each statement kind produces the correct PlanOp(s)
# with correct KV key generation.

import std/[unittest, options, json, os, strutils, random]
import fractio/sql/parser
import fractio/sql/ast
import fractio/sql/planner
import fractio/core/types except NodeID
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/protocol/raft_store
import fractio/protocol/mvcc_store
import fractio/protocol/txn_manager
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types
import fractio/protocol/server
import fractio/client/fractio_client

# Helper for tests: create a deterministic test table ID
var testTableIdCounter {.global.} = 0
proc testTableId(): TableId =
  inc testTableIdCounter
  # Use a deterministic ULID for test purposes
  var ulid: ULID
  for i in 0..<5:
    ulid.data[i] = 0'u8 # timestamp part (zero for testing)
  for i in 5..<15:
    ulid.data[i] = 0'u8 # randomness part (zero for testing)
  ulid.data[15] = uint8(testTableIdCounter) # test number
  TableId(ulid)

# ---------------------------------------------------------------------------
# Test helper: create a single-node test environment
# ---------------------------------------------------------------------------

var testBasePort {.global.} = 18000

proc nextBasePort(): int =
  result = testBasePort
  testBasePort += 100

proc createTestEnv(suiteName: string): tuple[client: FractioClient,
    server: ProtocolServer, store: RaftKVStoreExt, testDir: string] =
  randomize()
  let randomId = $rand(10000..99999)
  let testDir = "/tmp/fractio_test_planner_" & suiteName & "_" & randomId
  if dirExists(testDir): removeDir(testDir)
  createDir(testDir)

  let nodeId = NodeID(1)
  let raftPort = nextBasePort()
  let clientPort = nextBasePort()
  let members = @[(nodeId: 1'u32, host: "127.0.0.1", port: raftPort)]

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    port: raftPort,
    host: "127.0.0.1",
    dataDir: testDir,
    electionTimeoutLowerMs: 200,
    electionTimeoutUpperMs: 400,
    heartbeatIntervalMs: 100,
  ))
  coord.start()

  doAssert coord.createAndStartGroup(META_GROUP_ID, members)
  doAssert coord.createAndStartGroup(DATA_GROUP_START_ID, members)

  for attempt in 0 ..< 50:
    if coord.isLeader(META_GROUP_ID) and coord.isLeader(DATA_GROUP_START_ID):
      break
    os.sleep(100)

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 5000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  let txnMgr = newTransactionManager()
  let mvccStore = newMvccTransactionStore(store, txnMgr, nil)

  # Seed system tables via batch write for efficiency
  let nodeRec = NodeRecord(
    nodeId: 1,
    host: "127.0.0.1",
    raftPort: raftPort.uint16,
    clientPort: clientPort.uint16,
    status: nsAlive
  )
  let metaGroupRec = GroupRecord(
    groupId: groupIDToULID(META_GROUP_ID),
    spaceId: ZeroULID(),
    preferredLeader: 1, leader: 1,
    replicas: @[GroupReplicaBin(nodeId: 1, replicaType: rtVoter)]
  )
  let dataGroupRec = GroupRecord(
    groupId: groupIDToULID(DATA_GROUP_START_ID),
    spaceId: ZeroULID(),
    preferredLeader: 1, leader: 1,
    replicas: @[GroupReplicaBin(nodeId: 1, replicaType: rtVoter)]
  )
  discard store.sysTablePutBatch(@[
    (key: encodeTableKey(SYS_NODES_TABLE_ID, "1"), value: encode(nodeRec)),
    (key: encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(META_GROUP_ID)),
        value: encode(metaGroupRec)),
    (key: encodeTableKey(SYS_GROUPS_TABLE_ID, $groupIDToULID(
        DATA_GROUP_START_ID)), value: encode(dataGroupRec))
  ])

  # Start ProtocolServer
  var srvConfig = defaultServerConfig()
  srvConfig.port = clientPort
  srvConfig.host = "127.0.0.1"
  srvConfig.serverId = nodeId.uint16
  srvConfig.dataDir = testDir
  let server = newProtocolServer(srvConfig)
  server.raftStore = store
  server.mvccStore = mvccStore
  server.txnMgr = txnMgr
  server.start()

  # Create client
  let client = newFractioClient("127.0.0.1", clientPort)
  if not client.initialize():
    raise newException(CatchableError, "Failed to initialize client")

  result = (client, server, store, testDir)

proc cleanupTestDir(testDir: string) =
  if dirExists(testDir):
    removeDir(testDir)

# ---------------------------------------------------------------------------
# Helper: seed a table into the catalog
# ---------------------------------------------------------------------------

proc seedTable(client: FractioClient, database, schema, name: string,
    tableId: TableId, columns: seq[tuple[name: string, typ: string]],
    pk: seq[string]) =
  # Build binary TableRecord
  var cols: seq[ColumnDefBin] = @[]
  for (cname, ctype) in columns:
    var dt = cdtString
    case ctype.toLowerAscii()
    of "int", "integer": dt = cdtInt
    of "float", "double": dt = cdtFloat
    of "text", "string", "varchar": dt = cdtString
    of "bool", "boolean": dt = cdtBool
    of "bytes", "blob": dt = cdtBytes
    of "date": dt = cdtDate
    of "datetime", "timestamp": dt = cdtDateTime
    var flags: uint8 = 0
    if cname in pk:
      flags = flags or 0x01 # primaryKey
    cols.add(ColumnDefBin(name: cname, dataType: dt, flags: flags))
  let tableRec = TableRecord(
    tableId: tableId,
    name: name,
    database: database,
    schema: schema,
    spaceId: zeroSpaceID(), # default space (zero SpaceID)
    primaryKey: pk,
    columns: cols,
  )
  let key = encodeTableKey(SYS_TABLES_TABLE_ID,
      database & "." & schema & "." & name)
  let putRes = client.kvPut(key, encode(tableRec))
  doAssert putRes.isOk, "seedTable failed: " & putRes.err

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

suite "SQL Planner":
  var client: FractioClient
  var server: ProtocolServer
  var store: RaftKVStoreExt
  var testDir: string

  setup:
    (client, server, store, testDir) = createTestEnv("planner")

  teardown:
    if client != nil: client.close()
    os.sleep(100) # Allow connections to drain
    if server != nil:
      server.stop()
    os.sleep(100) # Allow server to fully stop
    if store != nil and store.coordinator != nil:
      store.coordinator.stop()
    os.sleep(50) # Allow coordinator shutdown
    cleanupTestDir(testDir)

  test "plan CREATE DATABASE":
    let stmt = parseStatement("CREATE DATABASE mydb")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poCreateDatabase
    check plan.ops[0].cdbName == "mydb"
    check plan.ops[0].cdbIfNotExists == false

  test "plan CREATE DATABASE IF NOT EXISTS":
    let stmt = parseStatement("CREATE DATABASE IF NOT EXISTS mydb")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poCreateDatabase
    check plan.ops[0].cdbIfNotExists == true

  test "plan CREATE DATABASE WITH REPLICAS":
    let stmt = parseStatement("CREATE DATABASE mydb WITH REPLICAS = 3")
    let plan = planStatement(stmt, client)
    check plan.ops[0].cdbReplicas == some(3)

  test "plan DROP DATABASE":
    let stmt = parseStatement("DROP DATABASE mydb")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poDropDatabase
    check plan.ops[0].ddbName == "mydb"
    check plan.ops[0].ddbIfExists == false

  test "plan DROP DATABASE IF EXISTS":
    let stmt = parseStatement("DROP DATABASE IF EXISTS mydb")
    let plan = planStatement(stmt, client)
    check plan.ops[0].ddbIfExists == true

  test "plan CREATE SCHEMA":
    let stmt = parseStatement("CREATE SCHEMA myschema")
    let plan = planStatement(stmt, client, database = "testdb")
    check plan.ops.len == 1
    check plan.ops[0].kind == poCreateSchema
    check plan.ops[0].csName == "myschema"
    check plan.ops[0].csDatabase == "testdb"

  test "plan DROP SCHEMA":
    let stmt = parseStatement("DROP SCHEMA myschema")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poDropSchema
    check plan.ops[0].dsName == "myschema"

  test "plan CREATE TABLE":
    let stmt = parseStatement(
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poCreateTable
    check plan.ops[0].ctName == "users"
    # Verify the binary TableRecord contains column info
    let rec = decodeTableRecord(plan.ops[0].ctValue)
    check rec.name == "users"
    check rec.columns.len == 3
    # tableId is now a ULID, just verify it's set (non-zero or valid)
    check rec.tableId != zeroTableId()

  test "plan CREATE TABLE IF NOT EXISTS":
    let stmt = parseStatement(
        "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY)")
    let plan = planStatement(stmt, client)
    check plan.ops[0].ctIfNotExists == true

  test "plan DROP TABLE":
    let stmt = parseStatement("DROP TABLE users")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poDropTable
    check plan.ops[0].dtName == "users"

  test "plan INSERT":
    let tid = testTableId()
    seedTable(client, "default", "public", "users", tid,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement(
        "INSERT INTO users (id, name) VALUES (1, 'Alice')")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poInsert
    check plan.ops[0].insTableId == tid
    check plan.ops[0].insRows.len == 1
    let row = parseJson(plan.ops[0].insRows[0])
    check row["id"].getInt == 1
    check row["name"].getStr == "Alice"

  test "plan INSERT multiple rows":
    let tid = testTableId()
    seedTable(client, "default", "public", "items", tid,
      @[("id", "INT"), ("val", "TEXT")], @["id"])
    let stmt = parseStatement(
        "INSERT INTO items (id, val) VALUES (1, 'a'), (2, 'b')")
    let plan = planStatement(stmt, client)
    check plan.ops[0].insRows.len == 2

  test "plan INSERT with table not found raises":
    expect(PlanError):
      let stmt = parseStatement("INSERT INTO nonexistent VALUES (1)")
      discard planStatement(stmt, client)

  test "plan SELECT with point get":
    let tid = testTableId()
    seedTable(client, "default", "public", "users", tid,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("SELECT * FROM users WHERE id = 42")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poPointGet
    check plan.ops[0].pgTableId == tid
    check plan.ops[0].pgKey == "42"

  test "plan SELECT full scan":
    let tid = testTableId()
    seedTable(client, "default", "public", "users", tid,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("SELECT * FROM users")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poScan
    check plan.ops[0].scTableId == tid
    check plan.ops[0].scFilter.isNone

  test "plan SELECT with filter (not point get)":
    let tid = testTableId()
    seedTable(client, "default", "public", "users", tid,
      @[("id", "INT"), ("name", "TEXT"), ("age", "INT")], @["id"])
    let stmt = parseStatement("SELECT * FROM users WHERE age > 21")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poScan
    check plan.ops[0].scFilter.isSome

  test "plan SELECT with LIMIT":
    let tid = testTableId()
    seedTable(client, "default", "public", "users", tid,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("SELECT * FROM users LIMIT 10")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poScan
    check plan.ops[0].scLimit == 10'u32

  test "plan SELECT specific columns":
    let tid = testTableId()
    seedTable(client, "default", "public", "users", tid,
      @[("id", "INT"), ("name", "TEXT"), ("age", "INT")], @["id"])
    let stmt = parseStatement("SELECT name, age FROM users")
    let plan = planStatement(stmt, client)
    check plan.ops[0].scColumns == @["name", "age"]

  test "plan UPDATE":
    let tid = testTableId()
    seedTable(client, "default", "public", "users", tid,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("UPDATE users SET name = 'Bob' WHERE id = 1")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poUpdate
    check plan.ops[0].upTableId == tid
    check plan.ops[0].upSets.len == 1
    check plan.ops[0].upSets[0].col == "name"

  test "plan DELETE":
    let tid = testTableId()
    seedTable(client, "default", "public", "users", tid,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("DELETE FROM users WHERE id = 1")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poDelete
    check plan.ops[0].delTableId == tid

  test "plan BEGIN":
    let stmt = parseStatement("BEGIN")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poBeginTxn
    check plan.ops[0].btReadOnly == false

  test "plan COMMIT":
    let stmt = parseStatement("COMMIT")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poCommitTxn

  test "plan ROLLBACK":
    let stmt = parseStatement("ROLLBACK")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poRollbackTxn

  test "plan SHOW DATABASES":
    let stmt = parseStatement("SHOW DATABASES")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poShowDatabases

  test "plan SHOW SCHEMAS":
    let stmt = parseStatement("SHOW SCHEMAS")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poShowSchemas
    check plan.ops[0].ssDatabase == "default" # uses default database

  test "plan SHOW SCHEMAS IN mydb":
    let stmt = parseStatement("SHOW SCHEMAS IN mydb")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poShowSchemas
    check plan.ops[0].ssDatabase == "mydb"

  test "plan SHOW TABLES":
    let stmt = parseStatement("SHOW TABLES")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poShowTables
    check plan.ops[0].stDatabase == "default"
    check plan.ops[0].stSchema == "public"

  test "plan SHOW TABLES IN myschema":
    let stmt = parseStatement("SHOW TABLES IN myschema")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poShowTables
    check plan.ops[0].stSchema == "myschema"

  test "plan SHOW TABLES IN mydb.myschema":
    let stmt = parseStatement("SHOW TABLES IN mydb.myschema")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poShowTables
    check plan.ops[0].stDatabase == "mydb"
    check plan.ops[0].stSchema == "myschema"

  test "plan USE DATABASE":
    let stmt = parseStatement("USE DATABASE mydb")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poUseDatabase
    check plan.ops[0].udName == "mydb"

  test "plan USE (bare, defaults to database)":
    let stmt = parseStatement("USE mydb")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poUseDatabase
    check plan.ops[0].udName == "mydb"

  test "plan USE SCHEMA":
    let stmt = parseStatement("USE SCHEMA myschema")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poUseSchema
    check plan.ops[0].usName == "myschema"

  test "plan EXPLAIN SELECT":
    let tid = testTableId()
    seedTable(client, "default", "public", "users", tid,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("EXPLAIN SELECT * FROM users")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops.len == 1
    check plan.ops[0].exInnerPlan.ops[0].kind == poScan

  test "plan EXPLAIN SELECT point get":
    let tid = testTableId()
    seedTable(client, "default", "public", "users", tid,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("EXPLAIN SELECT * FROM users WHERE id = 1")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops[0].kind == poPointGet

  test "plan EXPLAIN INSERT":
    let tid = testTableId()
    seedTable(client, "default", "public", "users", tid,
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("EXPLAIN INSERT INTO users (id, name) VALUES (1, 'Alice')")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops[0].kind == poInsert

  test "plan EXPLAIN CREATE TABLE":
    let stmt = parseStatement("EXPLAIN CREATE TABLE t1 (id INT PRIMARY KEY)")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops[0].kind == poCreateTable

  test "genTableId allocates unique IDs":
    let id1 = genTableId()
    let id2 = genTableId()
    check id1 != id2 # Each call should generate a unique ULID
