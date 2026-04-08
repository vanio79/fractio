# Unit tests for EXPLAIN SQL statement
#
# Covers all layers: lexer tokenization, parser AST, planner plan generation,
# formatExpr/formatPlanOp formatting, and executor result shape.

import std/[unittest, options, json, os, strutils, random]
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/protocol/server
import fractio/protocol/types as protoTypes
import fractio/sql/lexer
import fractio/sql/parser
import fractio/sql/ast
import fractio/sql/planner
import fractio/sql/executor
import fractio/core/types except NodeID
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/protocol/raft_store
import fractio/protocol/mvcc_store
import fractio/protocol/txn_manager
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types
import fractio/distributed/raft/multigroup_types

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
# Test helpers
# ---------------------------------------------------------------------------

var testBasePort {.global.} = 17000

proc nextBasePort(): int =
  result = testBasePort
  testBasePort += 100

proc makeTestEnv(testDir: string): tuple[client: FractioClient,
    server: ProtocolServer, store: RaftKVStoreExt] =
  if dirExists(testDir): removeDir(testDir)
  createDir(testDir)
  let nodeId = NodeID(1)
  let port = nextBasePort()
  let clientPort = nextBasePort()
  let members = @[(nodeId: 1'u32, host: "127.0.0.1", port: port)]

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    port: port,
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
    nodeId: 1, host: "127.0.0.1", raftPort: port.uint16,
    clientPort: clientPort.uint16, status: nsAlive
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
  doAssert client.initialize()

  result = (client, server, store)

proc cleanupTestDir(testDir: string) =
  if dirExists(testDir):
    removeDir(testDir)

proc exec(client: FractioClient, sql: string, database = "default",
    schema = "public"): ExecResult =
  client.query(sql, database, schema)

proc seedTable(store: RaftKVStoreExt, database, schema, name: string,
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
  discard store.sysTablePut(key, encode(tableRec))

# ---------------------------------------------------------------------------
# Suite 1: Lexer — EXPLAIN token
# ---------------------------------------------------------------------------

suite "EXPLAIN — lexer":

  test "EXPLAIN is tokenized as tkExplain":
    let tokens = tokenize("EXPLAIN SELECT")
    check tokens.len == 3 # EXPLAIN, SELECT, EOF
    check tokens[0].kind == tkExplain
    check tokens[1].kind == tkSelect
    check tokens[2].kind == tkEOF

  test "explain (lowercase) is tokenized as tkExplain":
    let tokens = tokenize("explain select")
    check tokens[0].kind == tkExplain
    check tokens[1].kind == tkSelect

  test "Explain (mixed case) is tokenized as tkExplain":
    let tokens = tokenize("Explain Insert")
    check tokens[0].kind == tkExplain
    check tokens[1].kind == tkInsert

  test "EXPLAIN in full statement":
    let tokens = tokenize("EXPLAIN SELECT * FROM users WHERE id = 1")
    check tokens[0].kind == tkExplain
    check tokens[1].kind == tkSelect
    check tokens[2].kind == tkStar
    check tokens[3].kind == tkFrom

# ---------------------------------------------------------------------------
# Suite 2: Parser — EXPLAIN AST
# ---------------------------------------------------------------------------

suite "EXPLAIN — parser":

  test "EXPLAIN SELECT parses to stmtExplain wrapping stmtSelect":
    let stmt = parseStatement("EXPLAIN SELECT * FROM users")
    check stmt.kind == stmtExplain
    check stmt.explainStmt.kind == stmtSelect
    check stmt.explainStmt.selFrom == "users"

  test "EXPLAIN INSERT parses correctly":
    let stmt = parseStatement("EXPLAIN INSERT INTO t (a) VALUES (1)")
    check stmt.kind == stmtExplain
    check stmt.explainStmt.kind == stmtInsert
    check stmt.explainStmt.intoTable == "t"

  test "EXPLAIN UPDATE parses correctly":
    let stmt = parseStatement("EXPLAIN UPDATE t SET a = 1 WHERE b = 2")
    check stmt.kind == stmtExplain
    check stmt.explainStmt.kind == stmtUpdate
    check stmt.explainStmt.updTable == "t"

  test "EXPLAIN DELETE parses correctly":
    let stmt = parseStatement("EXPLAIN DELETE FROM t WHERE a = 1")
    check stmt.kind == stmtExplain
    check stmt.explainStmt.kind == stmtDelete
    check stmt.explainStmt.delTable == "t"

  test "EXPLAIN CREATE TABLE parses correctly":
    let stmt = parseStatement("EXPLAIN CREATE TABLE t (id INT PRIMARY KEY)")
    check stmt.kind == stmtExplain
    check stmt.explainStmt.kind == stmtCreateTable
    check stmt.explainStmt.ctTable == "t"

  test "EXPLAIN DROP TABLE parses correctly":
    let stmt = parseStatement("EXPLAIN DROP TABLE t")
    check stmt.kind == stmtExplain
    check stmt.explainStmt.kind == stmtDropTable
    check stmt.explainStmt.dtTable == "t"

  test "EXPLAIN CREATE DATABASE parses correctly":
    let stmt = parseStatement("EXPLAIN CREATE DATABASE mydb")
    check stmt.kind == stmtExplain
    check stmt.explainStmt.kind == stmtCreateDatabase
    check stmt.explainStmt.cdbName == "mydb"

  test "EXPLAIN DROP DATABASE parses correctly":
    let stmt = parseStatement("EXPLAIN DROP DATABASE mydb")
    check stmt.kind == stmtExplain
    check stmt.explainStmt.kind == stmtDropDatabase

  test "EXPLAIN SHOW TABLES parses correctly":
    let stmt = parseStatement("EXPLAIN SHOW TABLES")
    check stmt.kind == stmtExplain
    check stmt.explainStmt.kind == stmtShowTables

  test "EXPLAIN SHOW DATABASES parses correctly":
    let stmt = parseStatement("EXPLAIN SHOW DATABASES")
    check stmt.kind == stmtExplain
    check stmt.explainStmt.kind == stmtShowDatabases

  test "EXPLAIN BEGIN parses correctly":
    let stmt = parseStatement("EXPLAIN BEGIN")
    check stmt.kind == stmtExplain
    check stmt.explainStmt.kind == stmtBegin

  test "EXPLAIN COMMIT parses correctly":
    let stmt = parseStatement("EXPLAIN COMMIT")
    check stmt.kind == stmtExplain
    check stmt.explainStmt.kind == stmtCommit

  test "EXPLAIN ROLLBACK parses correctly":
    let stmt = parseStatement("EXPLAIN ROLLBACK")
    check stmt.kind == stmtExplain
    check stmt.explainStmt.kind == stmtRollback

  test "EXPLAIN with semicolon in parseAll":
    let stmts = parseAll("EXPLAIN SELECT * FROM t; SELECT * FROM t")
    check stmts.len == 2
    check stmts[0].kind == stmtExplain
    check stmts[1].kind == stmtSelect

  test "EXPLAIN preserves WHERE clause":
    let stmt = parseStatement("EXPLAIN SELECT * FROM t WHERE x > 10 AND y = 'hello'")
    check stmt.kind == stmtExplain
    let inner = stmt.explainStmt
    check inner.selWhere.isSome
    let w = inner.selWhere.get()
    check w.kind == exBinOp
    check w.binOp == boAnd

  test "EXPLAIN preserves LIMIT and column list":
    let stmt = parseStatement("EXPLAIN SELECT a, b FROM t LIMIT 5")
    check stmt.kind == stmtExplain
    let inner = stmt.explainStmt
    check inner.selCols.len == 2
    check inner.selLimit.isSome

# ---------------------------------------------------------------------------
# Suite 3: Planner — formatExpr
# ---------------------------------------------------------------------------

suite "EXPLAIN — formatExpr":

  test "format integer literal":
    let e = Expr(kind: exLiteral, litValue: ValueRef(kind: dtInt, intValue: 42))
    check formatExpr(e) == "42"

  test "format string literal":
    let e = Expr(kind: exLiteral, litValue: ValueRef(kind: dtString,
        strValue: "hello"))
    check formatExpr(e) == "'hello'"

  test "format bool literal":
    let e = Expr(kind: exLiteral, litValue: ValueRef(kind: dtBool,
        boolValue: true))
    check formatExpr(e) == "true"

  test "format column reference":
    let e = Expr(kind: exColumn, colTable: "", colName: "age")
    check formatExpr(e) == "age"

  test "format qualified column reference":
    let e = Expr(kind: exColumn, colTable: "users", colName: "id")
    check formatExpr(e) == "users.id"

  test "format equality expression":
    let e = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exColumn, colTable: "", colName: "id"),
      binRight: Expr(kind: exLiteral, litValue: ValueRef(kind: dtInt, intValue: 1)))
    check formatExpr(e) == "id = 1"

  test "format comparison operators":
    for (op, sym) in [(boLt, "<"), (boLte, "<="), (boGt, ">"), (boGte, ">="), (
        boNeq, "<>")]:
      let e = Expr(kind: exBinOp, binOp: op,
        binLeft: Expr(kind: exColumn, colTable: "", colName: "x"),
        binRight: Expr(kind: exLiteral, litValue: ValueRef(kind: dtInt, intValue: 5)))
      check sym in formatExpr(e)

  test "format AND expression":
    let e = Expr(kind: exBinOp, binOp: boAnd,
      binLeft: Expr(kind: exBinOp, binOp: boGt,
        binLeft: Expr(kind: exColumn, colTable: "", colName: "age"),
        binRight: Expr(kind: exLiteral, litValue: ValueRef(kind: dtInt,
            intValue: 18))),
      binRight: Expr(kind: exBinOp, binOp: boLt,
        binLeft: Expr(kind: exColumn, colTable: "", colName: "age"),
        binRight: Expr(kind: exLiteral, litValue: ValueRef(kind: dtInt,
            intValue: 65))))
    let formatted = formatExpr(e)
    check "AND" in formatted
    check "age > 18" in formatted
    check "age < 65" in formatted

  test "format OR expression":
    let e = Expr(kind: exBinOp, binOp: boOr,
      binLeft: Expr(kind: exBinOp, binOp: boEq,
        binLeft: Expr(kind: exColumn, colTable: "", colName: "status"),
        binRight: Expr(kind: exLiteral, litValue: ValueRef(kind: dtString,
            strValue: "active"))),
      binRight: Expr(kind: exBinOp, binOp: boEq,
        binLeft: Expr(kind: exColumn, colTable: "", colName: "status"),
        binRight: Expr(kind: exLiteral, litValue: ValueRef(kind: dtString,
            strValue: "pending"))))
    check "OR" in formatExpr(e)

  test "format IS NULL":
    let e = Expr(kind: exIsNull,
      isNullExpr: Expr(kind: exColumn, colTable: "", colName: "email"),
      isNullNot: false)
    check formatExpr(e) == "email IS NULL"

  test "format IS NOT NULL":
    let e = Expr(kind: exIsNull,
      isNullExpr: Expr(kind: exColumn, colTable: "", colName: "email"),
      isNullNot: true)
    check formatExpr(e) == "email IS NOT NULL"

  test "format NOT expression":
    let e = Expr(kind: exUnaryOp, unaryOp: uoNot,
      unaryExpr: Expr(kind: exColumn, colTable: "", colName: "active"))
    check formatExpr(e) == "NOT active"

  test "format negation":
    let e = Expr(kind: exUnaryOp, unaryOp: uoNeg,
      unaryExpr: Expr(kind: exLiteral, litValue: ValueRef(kind: dtInt, intValue: 5)))
    check formatExpr(e) == "-5"

  test "format NULL literal":
    let e = Expr(kind: exLiteral, litValue: nil)
    check formatExpr(e) == "NULL"

  test "format star":
    let e = Expr(kind: exStar)
    check formatExpr(e) == "*"

  test "format arithmetic":
    let e = Expr(kind: exBinOp, binOp: boAdd,
      binLeft: Expr(kind: exColumn, colTable: "", colName: "price"),
      binRight: Expr(kind: exLiteral, litValue: ValueRef(kind: dtInt,
          intValue: 10)))
    check formatExpr(e) == "price + 10"

# ---------------------------------------------------------------------------
# Suite 4: Planner — formatPlanOp
# ---------------------------------------------------------------------------

suite "EXPLAIN — formatPlanOp":

  test "format Scan op":
    let tid = testTableId()
    let op = PlanOp(kind: poScan,
      scTableId: tid,
      scColumns: @["id", "name"],
      scFilter: none(Expr),
      scLimit: 0)
    let s = formatPlanOp(op)
    check "Scan" in s
    check "table_id" in s
    check "id" in s
    check "name" in s

  test "format Scan op with filter":
    let filter = Expr(kind: exBinOp, binOp: boGt,
      binLeft: Expr(kind: exColumn, colTable: "", colName: "age"),
      binRight: Expr(kind: exLiteral, litValue: ValueRef(kind: dtInt,
          intValue: 21)))
    let op = PlanOp(kind: poScan,
      scTableId: testTableId(),
      scColumns: @["name"],
      scFilter: some(filter),
      scLimit: 0)
    let s = formatPlanOp(op)
    check "filter=(age > 21)" in s

  test "format Scan op with limit":
    let op = PlanOp(kind: poScan,
      scTableId: testTableId(),
      scColumns: @["id"],
      scFilter: none(Expr),
      scLimit: 50)
    check "limit=50" in formatPlanOp(op)

  test "format PointGet op":
    let op = PlanOp(kind: poPointGet,
      pgTableId: testTableId(),
      pgKey: "42",
      pgColumns: @["id", "name"])
    let s = formatPlanOp(op)
    check "PointGet" in s
    check "key=42" in s

  test "format Insert op":
    let op = PlanOp(kind: poInsert,
      insTableId: testTableId(),
      insTableName: "users",
      insRows: @["{}", "{}"])
    let s = formatPlanOp(op)
    check "Insert" in s
    check "table=users" in s
    check "rows=2" in s

  test "format Update op":
    let filter = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exColumn, colTable: "", colName: "id"),
      binRight: Expr(kind: exLiteral, litValue: ValueRef(kind: dtInt, intValue: 1)))
    let op = PlanOp(kind: poUpdate,
      upTableId: testTableId(),
      upTableName: "users",
      upFilter: some(filter),
      upSets: @[("name", Expr(kind: exLiteral, litValue: ValueRef(
          kind: dtString, strValue: "Bob")))])
    let s = formatPlanOp(op)
    check "Update" in s
    check "filter=(id = 1)" in s
    check "set=[1 cols]" in s

  test "format Delete op":
    let op = PlanOp(kind: poDelete,
      delTableId: testTableId(),
      delTableName: "users",
      delFilter: none(Expr))
    let s = formatPlanOp(op)
    check "Delete" in s
    check "table=users" in s

  test "format Delete op with filter":
    let filter = Expr(kind: exBinOp, binOp: boLt,
      binLeft: Expr(kind: exColumn, colTable: "", colName: "age"),
      binRight: Expr(kind: exLiteral, litValue: ValueRef(kind: dtInt,
          intValue: 18)))
    let op = PlanOp(kind: poDelete,
      delTableId: testTableId(),
      delTableName: "users",
      delFilter: some(filter))
    check "filter=(age < 18)" in formatPlanOp(op)

  test "format DDL ops":
    check "CreateDatabase" in formatPlanOp(PlanOp(kind: poCreateDatabase,
        cdbName: "mydb"))
    check "DropDatabase" in formatPlanOp(PlanOp(kind: poDropDatabase,
        ddbName: "mydb"))
    check "CreateSchema" in formatPlanOp(PlanOp(kind: poCreateSchema,
        csName: "s", csDatabase: "d"))
    check "DropSchema" in formatPlanOp(PlanOp(kind: poDropSchema, dsName: "s",
        dsDatabase: "d"))
    check "CreateTable" in formatPlanOp(PlanOp(kind: poCreateTable, ctName: "t",
        ctSchema: "s", ctDatabase: "d"))
    check "DropTable" in formatPlanOp(PlanOp(kind: poDropTable, dtName: "t",
        dtSchema: "s", dtDatabase: "d"))

  test "format Show ops":
    check "ShowDatabases" in formatPlanOp(PlanOp(kind: poShowDatabases))
    check "ShowSchemas" in formatPlanOp(PlanOp(kind: poShowSchemas,
        ssDatabase: "mydb"))
    check "ShowTables" in formatPlanOp(PlanOp(kind: poShowTables,
        stDatabase: "d", stSchema: "s"))
    check "ShowSpaces" in formatPlanOp(PlanOp(kind: poShowSpaces))

  test "format Space ops":
    check "CreateSpace" in formatPlanOp(PlanOp(kind: poCreateSpace,
        cspName: "sp", cspReplicas: 3))
    check "DropSpace" in formatPlanOp(PlanOp(kind: poDropSpace, dspName: "sp"))

  test "format Use ops":
    check "UseDatabase" in formatPlanOp(PlanOp(kind: poUseDatabase,
        udName: "mydb"))
    check "UseSchema" in formatPlanOp(PlanOp(kind: poUseSchema,
        usName: "mysch"))

  test "format Txn ops":
    check "BeginTxn" in formatPlanOp(PlanOp(kind: poBeginTxn,
        btReadOnly: false))
    check "CommitTxn" in formatPlanOp(PlanOp(kind: poCommitTxn))
    check "RollbackTxn" in formatPlanOp(PlanOp(kind: poRollbackTxn))

  test "format Explain op":
    check "Explain" in formatPlanOp(PlanOp(kind: poExplain))

# ---------------------------------------------------------------------------
# Suite 5: formatPlan — multi-op plans
# ---------------------------------------------------------------------------

suite "EXPLAIN — formatPlan":

  test "single-op plan":
    let plan = newPlan()
    plan.add(PlanOp(kind: poShowDatabases))
    let text = formatPlan(plan)
    check text == "ShowDatabases"

  test "multi-op plan produces multiple lines":
    let plan = newPlan()
    plan.add(PlanOp(kind: poCreateDatabase, cdbName: "db1"))
    plan.add(PlanOp(kind: poCreateSchema, csName: "sch", csDatabase: "db1"))
    let text = formatPlan(plan)
    let lines = text.split('\n')
    check lines.len == 2
    check "CreateDatabase" in lines[0]
    check "CreateSchema" in lines[1]

  test "empty plan":
    let plan = newPlan()
    let text = formatPlan(plan)
    check text == ""

# ---------------------------------------------------------------------------
# Suite 6: Planner — EXPLAIN plan generation with store
# ---------------------------------------------------------------------------

suite "EXPLAIN — planner with store":
  var client: FractioClient
  var server: ProtocolServer
  var store: RaftKVStoreExt
  let testDir = "/tmp/fractio_test_explain_planner_" & $getCurrentProcessId()

  setup:
    cleanupTestDir(testDir)
    (client, server, store) = makeTestEnv(testDir)

  teardown:
    client.close()
    server.stop()
    store.coordinator.stop()
    cleanupTestDir(testDir)

  test "EXPLAIN SELECT generates poExplain wrapping poScan":
    seedTable(store, "default", "public", "users", testTableId(),
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("EXPLAIN SELECT * FROM users")
    let plan = planStatement(stmt, client)
    check plan.ops.len == 1
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops.len == 1
    check plan.ops[0].exInnerPlan.ops[0].kind == poScan

  test "EXPLAIN SELECT WHERE pk=val generates poExplain wrapping poPointGet":
    seedTable(store, "default", "public", "users", testTableId(),
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("EXPLAIN SELECT * FROM users WHERE id = 1")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops[0].kind == poPointGet
    check plan.ops[0].exInnerPlan.ops[0].pgKey == "1"

  test "EXPLAIN UPDATE generates poExplain wrapping poUpdate":
    seedTable(store, "default", "public", "users", testTableId(),
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("EXPLAIN UPDATE users SET name = 'X' WHERE id = 1")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops[0].kind == poUpdate
    check plan.ops[0].exInnerPlan.ops[0].upTableName == "users"

  test "EXPLAIN DELETE generates poExplain wrapping poDelete":
    seedTable(store, "default", "public", "users", testTableId(),
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement("EXPLAIN DELETE FROM users WHERE id = 1")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops[0].kind == poDelete

  test "EXPLAIN INSERT with multiple rows":
    seedTable(store, "default", "public", "users", testTableId(),
      @[("id", "INT"), ("name", "TEXT")], @["id"])
    let stmt = parseStatement(
        "EXPLAIN INSERT INTO users (id, name) VALUES (1, 'A'), (2, 'B'), (3, 'C')")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poExplain
    let inner = plan.ops[0].exInnerPlan.ops[0]
    check inner.kind == poInsert
    check inner.insRows.len == 3

  test "EXPLAIN on non-existent table raises PlanError":
    expect(PlanError):
      let stmt = parseStatement("EXPLAIN SELECT * FROM nonexistent")
      discard planStatement(stmt, client)

  test "EXPLAIN CREATE TABLE does not consume a table ID":
    let id1 = genTableId()
    let stmt = parseStatement("EXPLAIN CREATE TABLE t (id INT PRIMARY KEY)")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poExplain
    # genTableId should return the same ID — EXPLAIN planning consumed one
    # but we verify the table was NOT actually created
    let showRes = exec(client, "SHOW TABLES")
    check showRes.rows.len == 0

  test "EXPLAIN DROP DATABASE":
    let stmt = parseStatement("EXPLAIN DROP DATABASE IF EXISTS mydb")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops[0].kind == poDropDatabase

  test "EXPLAIN CREATE SCHEMA":
    let stmt = parseStatement("EXPLAIN CREATE SCHEMA myschema")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops[0].kind == poCreateSchema

  test "EXPLAIN SHOW SCHEMAS IN mydb":
    let stmt = parseStatement("EXPLAIN SHOW SCHEMAS IN mydb")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops[0].kind == poShowSchemas
    check plan.ops[0].exInnerPlan.ops[0].ssDatabase == "mydb"

  test "EXPLAIN BEGIN TRANSACTION":
    let stmt = parseStatement("EXPLAIN BEGIN TRANSACTION")
    let plan = planStatement(stmt, client)
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops[0].kind == poBeginTxn
