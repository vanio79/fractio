import std/[unittest, options, json, strutils]
import fractio/sql/ast
import fractio/sql/planner
import fractio/sql/parser
import fractio/sql/data_row
import fractio/core/types
import fractio/core/primary_key
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/protocol/messages/kv # for WireFilterExpr types
import fractio/utils/external_merge_sort # for SortSpec

suite "Planner Result Constructors":

  test "newPlan creates empty plan":
    let p = newPlan()
    check p.ops.len == 0

  test "Plan.add appends operation":
    let p = newPlan()
    p.add(PlanOp(kind: poShowDatabases))
    check p.ops.len == 1
    check p.ops[0].kind == poShowDatabases

  test "Plan.add multiple operations":
    let p = newPlan()
    p.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    p.add(PlanOp(kind: poCommitTxn))
    check p.ops.len == 2

suite "Planner dataTypeToString":

  test "all data types":
    check dataTypeToString(dtInt) == "INT"
    check dataTypeToString(dtFloat) == "FLOAT"
    check dataTypeToString(dtString) == "TEXT"
    check dataTypeToString(dtBool) == "BOOL"
    check dataTypeToString(dtDate) == "DATE"
    check dataTypeToString(dtDateTime) == "DATETIME"
    check dataTypeToString(dtBytes) == "BYTES"
    check dataTypeToString(dtULID) == "ULID"

suite "Planner TableDescriptor":

  test "findPkColumn from column primaryKey flag":
    var desc = TableDescriptor(columns: @[
      ColDef(name: "id", primaryKey: true),
      ColDef(name: "name", primaryKey: false)
    ])
    check findPkColumn(desc) == "id"

  test "findPkColumn from table-level primary key":
    var desc = TableDescriptor(
      primaryKey: @["user_id", "order_id"],
      columns: @[ColDef(name: "user_id"), ColDef(name: "order_id")]
    )
    check findPkColumn(desc) == "user_id"

  test "findPkColumn defaults to first column":
    var desc = TableDescriptor(columns: @[
      ColDef(name: "first_col"),
      ColDef(name: "second_col")
    ])
    check findPkColumn(desc) == "first_col"

  test "findPkColumn empty columns":
    var desc = TableDescriptor(columns: @[])
    check findPkColumn(desc) == ""

  test "columnNames":
    var desc = TableDescriptor(columns: @[
      ColDef(name: "id"),
      ColDef(name: "name"),
      ColDef(name: "email")
    ])
    let names = columnNames(desc)
    check names == @["id", "name", "email"]

suite "Planner System Table Descriptor Conversion":
  test "getSystemTableDescriptor finds databases":
    let opt = getSystemTableDescriptor("databases")
    check opt.isSome
    let desc = opt.get()
    check desc.name == "databases"
    check desc.schema == "sys"
    check desc.database == "sys"
    check desc.tableId == SYS_DATABASES_TABLE_ID
    check desc.columns.len > 0
    check desc.primaryKey == @["_key"]
    check desc.pkSpec.columns.len > 0

  test "getSystemTableDescriptor finds nodes":
    let opt = getSystemTableDescriptor("nodes")
    check opt.isSome
    let desc = opt.get()
    check desc.name == "nodes"
    check desc.columns.len >= 6 # _key, nodeId, host, raftPort, clientPort, status

  test "getSystemTableDescriptor is case insensitive":
    let opt1 = getSystemTableDescriptor("Databases")
    let opt2 = getSystemTableDescriptor("DATABASES")
    check opt1.isSome
    check opt2.isSome
    check opt1.get().name == "databases"
    check opt2.get().name == "databases"

  test "getSystemTableDescriptor returns none for unknown table":
    let opt = getSystemTableDescriptor("nonexistent_table")
    check opt.isNone

  test "sysColDef conversion preserves all fields":
    let info = getSystemTableInfoByName("databases").get()
    let desc = getSystemTableDescriptor("databases").get()
    # Check column count matches
    check desc.columns.len == info.columns.len
    # Check column names match
    for i in 0..<info.columns.len:
      check desc.columns[i].name == info.columns[i].name
      check desc.columns[i].dataType == info.columns[i].dataType
      check desc.columns[i].maxLen == info.columns[i].maxLen
      check desc.columns[i].primaryKey == info.columns[i].primaryKey
      check desc.columns[i].notNull == info.columns[i].notNull

  test "getSystemTableDescriptor returns correct tableId for all system tables":
    check getSystemTableDescriptor("databases").get().tableId == SYS_DATABASES_TABLE_ID
    check getSystemTableDescriptor("schemas").get().tableId == SYS_SCHEMAS_TABLE_ID
    check getSystemTableDescriptor("tables").get().tableId == SYS_TABLES_TABLE_ID
    check getSystemTableDescriptor("groups").get().tableId == SYS_GROUPS_TABLE_ID
    check getSystemTableDescriptor("nodes").get().tableId == SYS_NODES_TABLE_ID
    check getSystemTableDescriptor("settings").get().tableId == SYS_SETTINGS_TABLE_ID
    check getSystemTableDescriptor("spaces").get().tableId == SYS_SPACES_TABLE_ID
    check getSystemTableDescriptor("node_metrics").get().tableId == SYS_NODE_METRICS_ID
    check getSystemTableDescriptor("group_metrics").get().tableId == SYS_GROUP_METRICS_ID
    check getSystemTableDescriptor("events").get().tableId == SYS_EVENTS_TABLE_ID

suite "Planner exprToDataRowValue":

  test "integer literal":
    let e = Expr(kind: exLiteral, litValue: newValueRef(42'i64))
    let v = exprToDataRowValue(e)
    check v.kind == drvkInt
    check v.intVal == 42

  test "float literal":
    let e = Expr(kind: exLiteral, litValue: newValueRef(3.14))
    let v = exprToDataRowValue(e)
    check v.kind == drvkFloat
    check v.floatVal == 3.14

  test "string literal":
    let e = Expr(kind: exLiteral, litValue: newValueRef("hello"))
    let v = exprToDataRowValue(e)
    check v.kind == drvkString
    check v.strVal == "hello"

  test "bool literal":
    let e = Expr(kind: exLiteral, litValue: newValueRef(true))
    let v = exprToDataRowValue(e)
    check v.kind == drvkBool
    check v.boolVal == true

  test "non-literal returns null":
    let e = Expr(kind: exColumn, colName: "id")
    let v = exprToDataRowValue(e)
    check v.kind == drvkNull

  test "null litValue returns null":
    let e = Expr(kind: exLiteral, litValue: nil)
    let v = exprToDataRowValue(e)
    check v.kind == drvkNull

suite "Planner exprToJsonValue":

  test "integer to JSON":
    let e = Expr(kind: exLiteral, litValue: newValueRef(42'i64))
    let j = exprToJsonValue(e)
    check j.kind == JInt
    check j.getInt == 42

  test "float to JSON":
    let e = Expr(kind: exLiteral, litValue: newValueRef(3.14))
    let j = exprToJsonValue(e)
    check j.kind == JFloat
    check j.getFloat == 3.14

  test "string to JSON":
    let e = Expr(kind: exLiteral, litValue: newValueRef("hello"))
    let j = exprToJsonValue(e)
    check j.kind == JString
    check j.getStr == "hello"

  test "bool to JSON":
    let e = Expr(kind: exLiteral, litValue: newValueRef(true))
    let j = exprToJsonValue(e)
    check j.kind == JBool
    check j.getBool == true

  test "non-literal returns null JSON":
    let e = Expr(kind: exColumn, colName: "id")
    let j = exprToJsonValue(e)
    check j.kind == JNull

suite "Planner formatExpr":

  test "format integer literal":
    let e = Expr(kind: exLiteral, litValue: newValueRef(42'i64))
    check formatExpr(e) == "42"

  test "format float literal":
    let e = Expr(kind: exLiteral, litValue: newValueRef(3.14))
    check formatExpr(e) == "3.14"

  test "format string literal":
    let e = Expr(kind: exLiteral, litValue: newValueRef("hello"))
    check formatExpr(e) == "'hello'"

  test "format bool literal":
    let e = Expr(kind: exLiteral, litValue: newValueRef(true))
    check formatExpr(e) == "true"
    let e2 = Expr(kind: exLiteral, litValue: newValueRef(false))
    check formatExpr(e2) == "false"

  test "format null literal":
    let e = Expr(kind: exLiteral, litValue: nil)
    check formatExpr(e) == "NULL"

  test "format column without table":
    let e = Expr(kind: exColumn, colTable: "", colName: "id")
    check formatExpr(e) == "id"

  test "format column with table":
    let e = Expr(kind: exColumn, colTable: "users", colName: "name")
    check formatExpr(e) == "users.name"

  test "format binary operator equality":
    let e = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exColumn, colName: "id"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(1'i64)))
    check formatExpr(e) == "id = 1"

  test "format binary operator inequality":
    let e = Expr(kind: exBinOp, binOp: boNeq,
      binLeft: Expr(kind: exColumn, colName: "status"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef("active")))
    check formatExpr(e) == "status <> 'active'"

  test "format binary operator less than":
    let e = Expr(kind: exBinOp, binOp: boLt,
      binLeft: Expr(kind: exColumn, colName: "age"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(18'i64)))
    check formatExpr(e) == "age < 18"

  test "format binary operator greater than":
    let e = Expr(kind: exBinOp, binOp: boGt,
      binLeft: Expr(kind: exColumn, colName: "score"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(100'i64)))
    check formatExpr(e) == "score > 100"

  test "format binary operator less or equal":
    let e = Expr(kind: exBinOp, binOp: boLte,
      binLeft: Expr(kind: exColumn, colName: "age"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(65'i64)))
    check formatExpr(e) == "age <= 65"

  test "format binary operator greater or equal":
    let e = Expr(kind: exBinOp, binOp: boGte,
      binLeft: Expr(kind: exColumn, colName: "level"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(5'i64)))
    check formatExpr(e) == "level >= 5"

  test "format binary operator AND":
    let e = Expr(kind: exBinOp, binOp: boAnd,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(true)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(false)))
    check formatExpr(e) == "true AND false"

  test "format binary operator OR":
    let e = Expr(kind: exBinOp, binOp: boOr,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(true)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(true)))
    check formatExpr(e) == "true OR true"

  test "format arithmetic operators":
    check formatExpr(Expr(kind: exBinOp, binOp: boAdd,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(1'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(2'i64)))) == "1 + 2"
    check formatExpr(Expr(kind: exBinOp, binOp: boSub,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(3'i64)))) == "5 - 3"
    check formatExpr(Expr(kind: exBinOp, binOp: boMul,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(2'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(3'i64)))) == "2 * 3"
    check formatExpr(Expr(kind: exBinOp, binOp: boDiv,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(10'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(2'i64)))) == "10 / 2"
    check formatExpr(Expr(kind: exBinOp, binOp: boMod,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(7'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(3'i64)))) == "7 % 3"

  test "format unary NOT":
    let e = Expr(kind: exUnaryOp, unaryOp: uoNot,
      unaryExpr: Expr(kind: exLiteral, litValue: newValueRef(true)))
    check formatExpr(e) == "NOT true"

  test "format unary negation":
    let e = Expr(kind: exUnaryOp, unaryOp: uoNeg,
      unaryExpr: Expr(kind: exLiteral, litValue: newValueRef(42'i64)))
    check formatExpr(e) == "-42"

  test "format IS NULL":
    let e = Expr(kind: exIsNull,
      isNullExpr: Expr(kind: exColumn, colName: "nullable"),
      isNullNot: false)
    check formatExpr(e) == "nullable IS NULL"

  test "format IS NOT NULL":
    let e = Expr(kind: exIsNull,
      isNullExpr: Expr(kind: exColumn, colName: "nullable"),
      isNullNot: true)
    check formatExpr(e) == "nullable IS NOT NULL"

  test "format star":
    let e = Expr(kind: exStar)
    check formatExpr(e) == "*"

  test "format unknown expression":
    let e = Expr(kind: exParam, paramIdx: 1)
    check formatExpr(e) == "?"

  test "format exIn expression":
    let expr = Expr(kind: exColumn, colName: "status")
    let items = @[
      Expr(kind: exLiteral, litValue: newValueRef("active")),
      Expr(kind: exLiteral, litValue: newValueRef("pending"))
    ]
    let e = Expr(kind: exIn, inExpr: expr, inNot: false, inList: items)
    check formatExpr(e) == "?" # exIn falls through to else branch

  test "format exIn NOT IN expression":
    let e = Expr(kind: exIn, inExpr: Expr(kind: exColumn, colName: "x"),
                 inNot: true, inList: @[])
    check formatExpr(e) == "?" # exIn falls through to else branch

  test "format exBetween expression":
    let e = Expr(kind: exBetween, betweenExpr: Expr(kind: exColumn, colName: "age"),
                 betweenNot: false, betweenLo: Expr(kind: exLiteral,
                     litValue: newValueRef(18'i64)),
                 betweenHi: Expr(kind: exLiteral, litValue: newValueRef(65'i64)))
    check formatExpr(e) == "?" # exBetween falls through to else branch

  test "format exBetween NOT BETWEEN expression":
    let e = Expr(kind: exBetween, betweenExpr: Expr(kind: exColumn, colName: "x"),
                 betweenNot: true, betweenLo: Expr(kind: exLiteral,
                     litValue: nil), betweenHi: Expr(kind: exLiteral,
                     litValue: nil))
    check formatExpr(e) == "?"

  test "format exLike expression":
    let e = Expr(kind: exLike, likeExpr: Expr(kind: exColumn, colName: "name"),
                 likeNot: false, likePattern: Expr(kind: exLiteral,
                     litValue: newValueRef("A%")))
    check formatExpr(e) == "?" # exLike falls through to else branch

  test "format exLike NOT LIKE expression":
    let e = Expr(kind: exLike, likeExpr: Expr(kind: exColumn, colName: "name"),
                 likeNot: true, likePattern: Expr(kind: exLiteral,
                     litValue: nil))
    check formatExpr(e) == "?"

  test "format exList expression":
    let items = @[
      Expr(kind: exLiteral, litValue: newValueRef(1'i64)),
      Expr(kind: exLiteral, litValue: newValueRef(2'i64))
    ]
    let e = Expr(kind: exList, listItems: items)
    check formatExpr(e) == "?" # exList falls through to else branch

  test "format literal with dtDate falls through":
    # dtDate has dateValue field
    let v = ValueRef(kind: dtDate, dateValue: 12345)
    let e = Expr(kind: exLiteral, litValue: v)
    check formatExpr(e) == "?" # dtDate falls through to else

  test "format literal with dtDateTime falls through":
    # dtDateTime has datetimeValue field
    let v = ValueRef(kind: dtDateTime, datetimeValue: 12345)
    let e = Expr(kind: exLiteral, litValue: v)
    check formatExpr(e) == "?" # dtDateTime falls through to else

  test "format literal with dtBytes falls through":
    let v = ValueRef(kind: dtBytes, bytesValue: @[1.uint8, 2, 3])
    let e = Expr(kind: exLiteral, litValue: v)
    check formatExpr(e) == "?" # dtBytes falls through to else

  test "format literal with dtULID falls through":
    let v = newValueRef(genULIDLocal())
    let e = Expr(kind: exLiteral, litValue: v)
    check formatExpr(e) == "?" # dtULID falls through to else

suite "Planner formatPlanOp":

  test "format CreateDatabase":
    let op = PlanOp(kind: poCreateDatabase, cdbName: "mydb")
    check formatPlanOp(op) == "CreateDatabase name=mydb"

  test "format DropDatabase":
    let op = PlanOp(kind: poDropDatabase, ddbName: "mydb")
    check formatPlanOp(op) == "DropDatabase name=mydb"

  test "format CreateSchema":
    let op = PlanOp(kind: poCreateSchema, csName: "reporting",
        csDatabase: "mydb")
    check formatPlanOp(op) == "CreateSchema name=mydb.reporting"

  test "format DropSchema":
    let op = PlanOp(kind: poDropSchema, dsName: "old", dsDatabase: "mydb")
    check formatPlanOp(op) == "DropSchema name=mydb.old"

  test "format CreateTable":
    let op = PlanOp(kind: poCreateTable, ctName: "users",
                    ctDatabase: "mydb", ctSchema: "public")
    check formatPlanOp(op) == "CreateTable name=mydb.public.users"

  test "format DropTable":
    let op = PlanOp(kind: poDropTable, dtName: "users",
                    dtDatabase: "mydb", dtSchema: "public")
    check formatPlanOp(op) == "DropTable name=mydb.public.users"

  test "format ShowDatabases":
    let op = PlanOp(kind: poShowDatabases)
    check formatPlanOp(op) == "ShowDatabases"

  test "format ShowSchemas":
    let op = PlanOp(kind: poShowSchemas, ssDatabase: "mydb")
    check formatPlanOp(op) == "ShowSchemas db=mydb"

  test "format ShowTables":
    let op = PlanOp(kind: poShowTables, stDatabase: "mydb", stSchema: "public")
    check formatPlanOp(op) == "ShowTables db=mydb schema=public"

  test "format ShowSpaces":
    let op = PlanOp(kind: poShowSpaces)
    check formatPlanOp(op) == "ShowSpaces"

  test "format CreateSpace":
    let op = PlanOp(kind: poCreateSpace, cspName: "space1", cspReplicas: 3)
    check formatPlanOp(op) == "CreateSpace name=space1 replicas=3"

  test "format DropSpace":
    let op = PlanOp(kind: poDropSpace, dspName: "old_space")
    check formatPlanOp(op) == "DropSpace name=old_space"

  test "format UseDatabase":
    let op = PlanOp(kind: poUseDatabase, udName: "mydb")
    check formatPlanOp(op) == "UseDatabase name=mydb"

  test "format UseSchema":
    let op = PlanOp(kind: poUseSchema, usName: "reporting")
    check formatPlanOp(op) == "UseSchema name=reporting"

  test "format BeginTxn":
    let op = PlanOp(kind: poBeginTxn, btReadOnly: false)
    check formatPlanOp(op) == "BeginTxn readOnly=false"
    let op2 = PlanOp(kind: poBeginTxn, btReadOnly: true)
    check formatPlanOp(op2) == "BeginTxn readOnly=true"

  test "format CommitTxn":
    let op = PlanOp(kind: poCommitTxn)
    check formatPlanOp(op) == "CommitTxn"

  test "format RollbackTxn":
    let op = PlanOp(kind: poRollbackTxn)
    check formatPlanOp(op) == "RollbackTxn"

  test "format Explain":
    let op = PlanOp(kind: poExplain)
    check formatPlanOp(op) == "Explain"

suite "Planner formatPlan":

  test "format empty plan":
    let p = newPlan()
    check formatPlan(p) == ""

  test "format single operation":
    let p = newPlan()
    p.add(PlanOp(kind: poShowDatabases))
    check formatPlan(p) == "ShowDatabases"

  test "format multiple operations":
    let p = newPlan()
    p.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    p.add(PlanOp(kind: poCommitTxn))
    let output = formatPlan(p)
    check "BeginTxn" in output
    check "CommitTxn" in output
    check '\n' in output

suite "Planner PlanOpKind Coverage":

  test "all PlanOpKind values defined":
    let kinds = [
      poCreateDatabase, poDropDatabase, poCreateSchema, poDropSchema,
      poCreateTable, poDropTable, poInsert, poPointGet, poScan,
      poUpdate, poDelete, poShowDatabases, poShowSchemas, poShowTables,
      poShowSpaces, poCreateSpace, poDropSpace, poUseDatabase, poUseSchema,
      poBeginTxn, poCommitTxn, poRollbackTxn, poExplain
    ]
    for k in kinds:
      check k.ord >= 0

suite "Planner PlanOp Variants":

  test "poInsert fields":
    let op = PlanOp(kind: poInsert, insTableName: "users", insRows: @["row1", "row2"])
    check op.kind == poInsert
    check op.insTableName == "users"
    check op.insRows.len == 2

  test "poPointGet fields":
    let op = PlanOp(kind: poPointGet, pgColumns: @["id", "name"])
    check op.kind == poPointGet
    check op.pgColumns == @["id", "name"]

  test "poScan fields":
    let op = PlanOp(kind: poScan, scLimit: 100)
    check op.kind == poScan
    check op.scLimit == 100

  test "poUpdate fields":
    let op = PlanOp(kind: poUpdate, upTableName: "users")
    check op.kind == poUpdate
    check op.upTableName == "users"

  test "poDelete fields":
    let op = PlanOp(kind: poDelete, delTableName: "logs")
    check op.kind == poDelete
    check op.delTableName == "logs"

suite "Planner genNewTableId":
  test "generates unique TableId":
    let id1 = genNewTableId()
    let id2 = genNewTableId()
    # ULID-based IDs should be unique
    check id1 != id2

  test "generates non-zero TableId":
    let id = genNewTableId()
    # TableId is ULID-based, string representation is never all zeros
    check $id != "00000000000000000000000000"

suite "Planner dataRowValueToPkValue":
  test "int value":
    let v = newRowValue(42'i64)
    let colSpec = (name: "id", dataType: cdtInt, maxLen: 0)
    let pk = dataRowValueToPkValue(v, colSpec)
    check pk.isNull == false
    check pk.kind == cdtInt
    check pk.intVal == 42

  test "float value":
    let v = newRowValue(3.14)
    let colSpec = (name: "val", dataType: cdtFloat, maxLen: 0)
    let pk = dataRowValueToPkValue(v, colSpec)
    check pk.isNull == false
    check pk.kind == cdtFloat
    check pk.floatVal == 3.14

  test "string value":
    let v = newRowValue("hello")
    let colSpec = (name: "name", dataType: cdtString, maxLen: 32)
    let pk = dataRowValueToPkValue(v, colSpec)
    check pk.isNull == false
    check pk.kind == cdtString
    check pk.strVal == "hello"
    check pk.strMaxLen == 32

  test "bool value":
    let v = newRowValue(true)
    let colSpec = (name: "active", dataType: cdtBool, maxLen: 0)
    let pk = dataRowValueToPkValue(v, colSpec)
    check pk.isNull == false
    check pk.kind == cdtBool
    check pk.boolVal == true

  test "null value int":
    let v = newRowValue()
    let colSpec = (name: "id", dataType: cdtInt, maxLen: 0)
    let pk = dataRowValueToPkValue(v, colSpec)
    check pk.isNull == true
    check pk.kind == cdtInt

  test "null value float":
    let v = newRowValue()
    let colSpec = (name: "val", dataType: cdtFloat, maxLen: 0)
    let pk = dataRowValueToPkValue(v, colSpec)
    check pk.isNull == true
    check pk.kind == cdtFloat

  test "null value string":
    let v = newRowValue()
    let colSpec = (name: "name", dataType: cdtString, maxLen: 32)
    let pk = dataRowValueToPkValue(v, colSpec)
    check pk.isNull == true
    check pk.kind == cdtString

  test "null value bool":
    let v = newRowValue()
    let colSpec = (name: "active", dataType: cdtBool, maxLen: 0)
    let pk = dataRowValueToPkValue(v, colSpec)
    check pk.isNull == true
    check pk.kind == cdtBool

  test "null value bytes":
    let v = newRowValue()
    let colSpec = (name: "data", dataType: cdtBytes, maxLen: 64)
    let pk = dataRowValueToPkValue(v, colSpec)
    check pk.isNull == true
    check pk.kind == cdtBytes
    check pk.bytesMaxLen == 64

  test "null value date":
    let v = newRowValue()
    let colSpec = (name: "dt", dataType: cdtDate, maxLen: 0)
    let pk = dataRowValueToPkValue(v, colSpec)
    check pk.isNull == true
    check pk.kind == cdtDate

  test "null value datetime":
    let v = newRowValue()
    let colSpec = (name: "ts", dataType: cdtDateTime, maxLen: 0)
    let pk = dataRowValueToPkValue(v, colSpec)
    check pk.isNull == true
    check pk.kind == cdtDateTime

  test "null value ULID":
    let v = newRowValue()
    let colSpec = (name: "ulid", dataType: cdtULID, maxLen: 0)
    let pk = dataRowValueToPkValue(v, colSpec)
    check pk.isNull == true
    check pk.kind == cdtULID

  test "bytes value":
    let v = newRowValue("ABC")
    let colSpec = (name: "data", dataType: cdtBytes, maxLen: 64)
    let pk = dataRowValueToPkValue(v, colSpec)
    check pk.isNull == false
    check pk.kind == cdtBytes
    check pk.bytesVal.len == 3
    check pk.bytesVal[0] == uint8('A')

  test "date value":
    let v = newRowValue(12345'i64)
    let colSpec = (name: "dt", dataType: cdtDate, maxLen: 0)
    let pk = dataRowValueToPkValue(v, colSpec)
    check pk.isNull == false
    check pk.kind == cdtDate
    check pk.dateVal == 12345

  test "datetime value":
    let v = newRowValue(12345'i64)
    let colSpec = (name: "ts", dataType: cdtDateTime, maxLen: 0)
    let pk = dataRowValueToPkValue(v, colSpec)
    check pk.isNull == false
    check pk.kind == cdtDateTime
    check pk.datetimeVal == 12345

suite "Planner formatPlanOp DML":
  test "format Insert":
    let tid = genTableIdLocal()
    let op = PlanOp(kind: poInsert, insTableName: "users", insTableId: tid,
        insRows: @["r1", "r2"])
    let s = formatPlanOp(op)
    check "Insert" in s
    check "users" in s
    check "rows=2" in s

  test "format PointGet":
    let tid = genTableIdLocal()
    let op = PlanOp(kind: poPointGet, pgTableId: tid, pgKey: "abc",
        pgColumns: @["id", "name"])
    let s = formatPlanOp(op)
    check "PointGet" in s
    check "abc" in s
    check "id" in s

  test "format Scan without filter":
    let tid = genTableIdLocal()
    let op = PlanOp(kind: poScan, scTableId: tid, scColumns: @["id"], scLimit: 0)
    let s = formatPlanOp(op)
    check "Scan" in s
    check "id" in s

  test "format Scan with filter":
    let tid = genTableIdLocal()
    let filter = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exColumn, colName: "status"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef("active")))
    let op = PlanOp(kind: poScan, scTableId: tid, scColumns: @["id"],
                    scFilter: some(filter), scLimit: 10)
    let s = formatPlanOp(op)
    check "Scan" in s
    check "filter=" in s
    check "status" in s
    check "limit=10" in s

  test "format Update without filter":
    let tid = genTableIdLocal()
    let op = PlanOp(kind: poUpdate, upTableName: "users", upTableId: tid,
                    upSets: @[("name", Expr(kind: exLiteral,
                        litValue: newValueRef("test")))])
    let s = formatPlanOp(op)
    check "Update" in s
    check "users" in s
    check "1 cols" in s

  test "format Update with filter":
    let tid = genTableIdLocal()
    let filter = Expr(kind: exBinOp, binOp: boGt,
      binLeft: Expr(kind: exColumn, colName: "age"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(18'i64)))
    let op = PlanOp(kind: poUpdate, upTableName: "users", upTableId: tid,
                    upFilter: some(filter), upSets: @[("age", Expr(
                        kind: exLiteral))])
    let s = formatPlanOp(op)
    check "Update" in s
    check "filter=" in s
    check "age" in s

  test "format Delete without filter":
    let tid = genTableIdLocal()
    let op = PlanOp(kind: poDelete, delTableName: "logs", delTableId: tid)
    let s = formatPlanOp(op)
    check "Delete" in s
    check "logs" in s

  test "format Delete with filter":
    let tid = genTableIdLocal()
    let filter = Expr(kind: exBinOp, binOp: boLt,
      binLeft: Expr(kind: exColumn, colName: "created"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(100'i64)))
    let op = PlanOp(kind: poDelete, delTableName: "logs", delTableId: tid,
                    delFilter: some(filter))
    let s = formatPlanOp(op)
    check "Delete" in s
    check "filter=" in s
    check "created" in s

suite "Planner PlanError":
  test "PlanError type exists":
    var e: ref PlanError
    new e
    e.msg = "table not found"
    check e.msg == "table not found"

suite "Planner planStatement DDL (no client required)":

  test "planCreateDatabase creates correct plan":
    let stmt = Stmt(
      kind: stmtCreateDatabase,
      cdbName: "testdb",
      cdbIfNotExists: false,
      cdbReplicas: some(3)
    )
    let plan = planStatement(stmt, nil)
    check plan.ops.len == 1
    check plan.ops[0].kind == poCreateDatabase
    check plan.ops[0].cdbName == "testdb"
    check plan.ops[0].cdbIfNotExists == false
    check plan.ops[0].cdbReplicas.isSome
    check plan.ops[0].cdbReplicas.get == 3

  test "planCreateDatabase with IF NOT EXISTS":
    let stmt = Stmt(
      kind: stmtCreateDatabase,
      cdbName: "mydb",
      cdbIfNotExists: true,
      cdbReplicas: none(int)
    )
    let plan = planStatement(stmt, nil)
    check plan.ops.len == 1
    check plan.ops[0].cdbIfNotExists == true
    check plan.ops[0].cdbReplicas.isNone

  test "planDropDatabase creates correct plan":
    let stmt = Stmt(
      kind: stmtDropDatabase,
      ddbName: "testdb",
      ddbIfExists: false
    )
    let plan = planStatement(stmt, nil)
    check plan.ops.len == 1
    check plan.ops[0].kind == poDropDatabase
    check plan.ops[0].ddbName == "testdb"
    check plan.ops[0].ddbIfExists == false

  test "planDropDatabase with IF EXISTS":
    let stmt = Stmt(
      kind: stmtDropDatabase,
      ddbName: "old_db",
      ddbIfExists: true
    )
    let plan = planStatement(stmt, nil)
    check plan.ops[0].ddbIfExists == true

  test "planCreateSchema creates correct plan":
    let stmt = Stmt(
      kind: stmtCreateSchema,
      csName: "reporting",
      csIfNotExists: false,
      csReplicas: none(int)
    )
    let plan = planStatement(stmt, nil, database = "mydb")
    check plan.ops.len == 1
    check plan.ops[0].kind == poCreateSchema
    check plan.ops[0].csName == "reporting"
    check plan.ops[0].csDatabase == "mydb"

  test "planCreateSchema with replicas":
    let stmt = Stmt(
      kind: stmtCreateSchema,
      csName: "analytics",
      csIfNotExists: true,
      csReplicas: some(5)
    )
    let plan = planStatement(stmt, nil, database = "production")
    check plan.ops[0].csIfNotExists == true
    check plan.ops[0].csReplicas.isSome
    check plan.ops[0].csReplicas.get == 5

  test "planDropSchema creates correct plan":
    let stmt = Stmt(
      kind: stmtDropSchema,
      dsName: "old_schema",
      dsIfExists: false
    )
    let plan = planStatement(stmt, nil, database = "mydb")
    check plan.ops.len == 1
    check plan.ops[0].kind == poDropSchema
    check plan.ops[0].dsName == "old_schema"
    check plan.ops[0].dsDatabase == "mydb"

  test "planDropSchema with IF EXISTS":
    let stmt = Stmt(
      kind: stmtDropSchema,
      dsName: "deprecated",
      dsIfExists: true
    )
    let plan = planStatement(stmt, nil, database = "testdb")
    check plan.ops[0].dsIfExists == true

  test "planCreateSpace creates correct plan":
    let stmt = Stmt(
      kind: stmtCreateSpace,
      csSpaceName: "production",
      csSpaceReplicas: 3
    )
    let plan = planStatement(stmt, nil)
    check plan.ops.len == 1
    check plan.ops[0].kind == poCreateSpace
    check plan.ops[0].cspName == "production"
    check plan.ops[0].cspReplicas == 3

  test "planCreateSpace with 0 replicas (ALL)":
    let stmt = Stmt(
      kind: stmtCreateSpace,
      csSpaceName: "all_nodes_space",
      csSpaceReplicas: 0
    )
    let plan = planStatement(stmt, nil)
    check plan.ops[0].cspReplicas == 0

  test "planDropSpace creates correct plan":
    let stmt = Stmt(
      kind: stmtDropSpace,
      dsSpaceName: "old_space"
    )
    let plan = planStatement(stmt, nil)
    check plan.ops.len == 1
    check plan.ops[0].kind == poDropSpace
    check plan.ops[0].dspName == "old_space"

  test "planStatement stmtShowDatabases":
    let stmt = Stmt(kind: stmtShowDatabases)
    let plan = planStatement(stmt, nil)
    check plan.ops.len == 1
    check plan.ops[0].kind == poShowDatabases

  test "planStatement stmtShowSchemas":
    let stmt = Stmt(kind: stmtShowSchemas, showSchemasDb: "mydb")
    let plan = planStatement(stmt, nil)
    check plan.ops.len == 1
    check plan.ops[0].kind == poShowSchemas
    check plan.ops[0].ssDatabase == "mydb"

  test "planStatement stmtShowSchemas with empty db uses current":
    let stmt = Stmt(kind: stmtShowSchemas, showSchemasDb: "")
    let plan = planStatement(stmt, nil, database = "default")
    check plan.ops[0].ssDatabase == "default"

  test "planStatement stmtShowTables":
    let stmt = Stmt(
      kind: stmtShowTables,
      showTablesDb: "mydb",
      showTablesSchema: "public"
    )
    let plan = planStatement(stmt, nil)
    check plan.ops.len == 1
    check plan.ops[0].kind == poShowTables
    check plan.ops[0].stDatabase == "mydb"
    check plan.ops[0].stSchema == "public"

  test "planStatement stmtShowTables with empty fields uses defaults":
    let stmt = Stmt(
      kind: stmtShowTables,
      showTablesDb: "",
      showTablesSchema: ""
    )
    let plan = planStatement(stmt, nil, database = "mydb", schema = "reporting")
    check plan.ops[0].stDatabase == "mydb"
    check plan.ops[0].stSchema == "reporting"

  test "planStatement stmtShowSpaces":
    let stmt = Stmt(kind: stmtShowSpaces)
    let plan = planStatement(stmt, nil)
    check plan.ops.len == 1
    check plan.ops[0].kind == poShowSpaces

  test "planStatement stmtUseDatabase":
    let stmt = Stmt(kind: stmtUseDatabase, useDbName: "newdb")
    let plan = planStatement(stmt, nil)
    check plan.ops.len == 1
    check plan.ops[0].kind == poUseDatabase
    check plan.ops[0].udName == "newdb"

  test "planStatement stmtUseSchema":
    let stmt = Stmt(kind: stmtUseSchema, useSchemaName: "analytics")
    let plan = planStatement(stmt, nil)
    check plan.ops.len == 1
    check plan.ops[0].kind == poUseSchema
    check plan.ops[0].usName == "analytics"

  test "planStatement stmtBegin":
    let stmt = Stmt(kind: stmtBegin, beginReadOnly: false)
    let plan = planStatement(stmt, nil)
    check plan.ops.len == 1
    check plan.ops[0].kind == poBeginTxn
    check plan.ops[0].btReadOnly == false

  test "planStatement stmtBegin read-only":
    let stmt = Stmt(kind: stmtBegin, beginReadOnly: true)
    let plan = planStatement(stmt, nil)
    check plan.ops[0].btReadOnly == true

  test "planStatement stmtCommit":
    let stmt = Stmt(kind: stmtCommit)
    let plan = planStatement(stmt, nil)
    check plan.ops.len == 1
    check plan.ops[0].kind == poCommitTxn

  test "planStatement stmtRollback":
    let stmt = Stmt(kind: stmtRollback)
    let plan = planStatement(stmt, nil)
    check plan.ops.len == 1
    check plan.ops[0].kind == poRollbackTxn

suite "Planner planCreateTable":
  test "planCreateTable creates correct plan structure":
    let stmt = Stmt(
      kind: stmtCreateTable,
      ctTableRef: TableRef(table: "users"),
      ctIfNotExists: false,
      ctColumns: @[
        ColDef(name: "id", dataType: dtInt, primaryKey: true, notNull: true),
        ColDef(name: "name", dataType: dtString, maxLen: 100)
      ],
      ctPrimaryKey: @[],
      ctReplicas: none(int),
      ctSpaceName: none(string)
    )
    let plan = planStatement(stmt, nil, database = "mydb", schema = "public")
    check plan.ops.len == 1
    check plan.ops[0].kind == poCreateTable
    check plan.ops[0].ctName == "users"
    check plan.ops[0].ctDatabase == "mydb"
    check plan.ops[0].ctSchema == "public"
    check plan.ops[0].ctIfNotExists == false
    check plan.ops[0].ctSpaceName.isNone

  test "planCreateTable with IF NOT EXISTS":
    let stmt = Stmt(
      kind: stmtCreateTable,
      ctTableRef: TableRef(table: "products"),
      ctIfNotExists: true,
      ctColumns: @[ColDef(name: "id", dataType: dtInt)],
      ctPrimaryKey: @[],
      ctReplicas: none(int),
      ctSpaceName: none(string)
    )
    let plan = planStatement(stmt, nil)
    check plan.ops[0].ctIfNotExists == true

  test "planCreateTable with IN SPACE":
    let stmt = Stmt(
      kind: stmtCreateTable,
      ctTableRef: TableRef(table: "orders"),
      ctIfNotExists: false,
      ctColumns: @[ColDef(name: "id", dataType: dtInt)],
      ctPrimaryKey: @["id"],
      ctReplicas: none(int),
      ctSpaceName: some("production_space")
    )
    let plan = planStatement(stmt, nil)
    check plan.ops[0].ctSpaceName.isSome
    check plan.ops[0].ctSpaceName.get == "production_space"

  test "planCreateTable with table-level primary key":
    let stmt = Stmt(
      kind: stmtCreateTable,
      ctTableRef: TableRef(table: "composite_pk_table"),
      ctIfNotExists: false,
      ctColumns: @[
        ColDef(name: "user_id", dataType: dtInt),
        ColDef(name: "order_id", dataType: dtInt)
      ],
      ctPrimaryKey: @["user_id", "order_id"],
      ctReplicas: none(int),
      ctSpaceName: none(string)
    )
    let plan = planStatement(stmt, nil)
    check plan.ops[0].kind == poCreateTable

suite "Planner planCreateTable auto-resolve space name":
  test "nil client with database name does not crash":
    # When client is nil, auto-resolution is skipped gracefully
    let stmt = Stmt(
      kind: stmtCreateTable,
      ctTableRef: TableRef(database: "myspace", table: "users"),
      ctIfNotExists: false,
      ctColumns: @[ColDef(name: "id", dataType: dtInt, primaryKey: true)],
      ctPrimaryKey: @[],
      ctReplicas: none(int),
      ctSpaceName: none(string)
    )
    let plan = planStatement(stmt, nil, database = "myspace", schema = "public")
    check plan.ops.len == 1
    check plan.ops[0].kind == poCreateTable
    # With nil client, space name is not auto-resolved
    check plan.ops[0].ctSpaceName.isNone

  test "nil client without database name does not crash":
    let stmt = Stmt(
      kind: stmtCreateTable,
      ctTableRef: TableRef(table: "users"),
      ctIfNotExists: false,
      ctColumns: @[ColDef(name: "id", dataType: dtInt, primaryKey: true)],
      ctPrimaryKey: @[],
      ctReplicas: none(int),
      ctSpaceName: none(string)
    )
    let plan = planStatement(stmt, nil, database = "", schema = "public")
    check plan.ops.len == 1
    check plan.ops[0].ctSpaceName.isNone

  test "explicit IN SPACE overrides auto-resolution":
    # When ctSpaceName is already set, auto-resolution is skipped
    let stmt = Stmt(
      kind: stmtCreateTable,
      ctTableRef: TableRef(database: "myspace", table: "users"),
      ctIfNotExists: false,
      ctColumns: @[ColDef(name: "id", dataType: dtInt, primaryKey: true)],
      ctPrimaryKey: @[],
      ctReplicas: none(int),
      ctSpaceName: some("other_space")
    )
    let plan = planStatement(stmt, nil, database = "myspace", schema = "public")
    check plan.ops.len == 1
    check plan.ops[0].ctSpaceName.isSome
    check plan.ops[0].ctSpaceName.get == "other_space"

suite "Planner planDropTable":
  test "planDropTable creates correct plan":
    let stmt = Stmt(
      kind: stmtDropTable,
      dtTableRef: TableRef(table: "old_table"),
      dtIfExists: false
    )
    let plan = planStatement(stmt, nil, database = "mydb", schema = "public")
    check plan.ops.len == 1
    check plan.ops[0].kind == poDropTable
    check plan.ops[0].dtName == "old_table"
    check plan.ops[0].dtDatabase == "mydb"
    check plan.ops[0].dtSchema == "public"
    check plan.ops[0].dtIfExists == false

  test "planDropTable with IF EXISTS":
    let stmt = Stmt(
      kind: stmtDropTable,
      dtTableRef: TableRef(table: "deprecated_table"),
      dtIfExists: true
    )
    let plan = planStatement(stmt, nil)
    check plan.ops[0].dtIfExists == true

suite "Planner stmtExplain":
  test "planStatement stmtExplain wraps inner plan":
    let innerStmt = Stmt(kind: stmtShowDatabases)
    let stmt = Stmt(kind: stmtExplain, explainStmt: innerStmt)
    let plan = planStatement(stmt, nil)
    check plan.ops.len == 1
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan != nil
    check plan.ops[0].exInnerPlan.ops.len == 1
    check plan.ops[0].exInnerPlan.ops[0].kind == poShowDatabases

  test "planStatement stmtExplain with CREATE DATABASE":
    let innerStmt = Stmt(
      kind: stmtCreateDatabase,
      cdbName: "explained_db",
      cdbIfNotExists: false,
      cdbReplicas: none(int)
    )
    let stmt = Stmt(kind: stmtExplain, explainStmt: innerStmt)
    let plan = planStatement(stmt, nil)
    check plan.ops[0].kind == poExplain
    check plan.ops[0].exInnerPlan.ops[0].kind == poCreateDatabase
    check plan.ops[0].exInnerPlan.ops[0].cdbName == "explained_db"

suite "Planner exprToDataRowValue":

  test "int literal to DataRowValue":
    let e = Expr(kind: exLiteral, litValue: ValueRef(kind: dtInt, intValue: 42))
    let v = exprToDataRowValue(e)
    check v.kind == drvkInt
    check v.intVal == 42

  test "float literal to DataRowValue":
    let e = Expr(kind: exLiteral, litValue: ValueRef(kind: dtFloat,
        floatValue: 3.14))
    let v = exprToDataRowValue(e)
    check v.kind == drvkFloat
    check v.floatVal == 3.14

  test "string literal to DataRowValue":
    let e = Expr(kind: exLiteral, litValue: ValueRef(kind: dtString,
        strValue: "hello"))
    let v = exprToDataRowValue(e)
    check v.kind == drvkString
    check v.strVal == "hello"

  test "bool literal to DataRowValue":
    let e = Expr(kind: exLiteral, litValue: ValueRef(kind: dtBool,
        boolValue: true))
    let v = exprToDataRowValue(e)
    check v.kind == drvkBool
    check v.boolVal == true

  test "null literal to DataRowValue":
    let e = Expr(kind: exLiteral, litValue: nil)
    let v = exprToDataRowValue(e)
    check v.kind == drvkNull

  test "column expr returns null":
    let e = Expr(kind: exColumn, colName: "id", colTable: "")
    let v = exprToDataRowValue(e)
    check v.kind == drvkNull

suite "Planner dataRowValueToPkValue":

  test "int value to PK":
    let v = DataRowValue(kind: drvkInt, intVal: 123)
    let spec = ("id", cdtInt, 0)
    let pkVal = dataRowValueToPkValue(v, spec)
    check pkVal.isNull == false
    check pkVal.kind == cdtInt
    check pkVal.intVal == 123

  test "string value to PK":
    let v = DataRowValue(kind: drvkString, strVal: "abc")
    let spec = ("name", cdtString, 10)
    let pkVal = dataRowValueToPkValue(v, spec)
    check pkVal.isNull == false
    check pkVal.kind == cdtString

  test "null value to PK":
    let v = DataRowValue(kind: drvkNull)
    let spec = ("id", cdtInt, 0)
    let pkVal = dataRowValueToPkValue(v, spec)
    check pkVal.isNull == true

suite "Planner genNewTableId":

  test "generates unique IDs":
    let id1 = genNewTableId()
    let id2 = genNewTableId()
    check id1 != id2
    # ULIDs are 26 characters
    check tableIdToBytes(id1).len == 16

suite "Planner exprToWireFilterExpr":

  test "converts literal int":
    let e = Expr(kind: exLiteral, litValue: newValueRef(42'i64))
    let wire = exprToWireFilterExpr(e)
    check wire.kind == wekLiteral
    check wire.litDataType == wdtInt
    check wire.litIntVal == 42'i64

  test "converts literal string":
    let e = Expr(kind: exLiteral, litValue: newValueRef("active"))
    let wire = exprToWireFilterExpr(e)
    check wire.kind == wekLiteral
    check wire.litDataType == wdtString
    check wire.litStringVal == "active"

  test "converts literal bool":
    let e = Expr(kind: exLiteral, litValue: newValueRef(true))
    let wire = exprToWireFilterExpr(e)
    check wire.kind == wekLiteral
    check wire.litDataType == wdtBool
    check wire.litBoolVal == true

  test "converts literal float":
    let e = Expr(kind: exLiteral, litValue: newValueRef(3.14))
    let wire = exprToWireFilterExpr(e)
    check wire.kind == wekLiteral
    check wire.litDataType == wdtFloat
    check wire.litFloatVal == 3.14

  test "converts literal null":
    let e = Expr(kind: exLiteral, litValue: nil)
    let wire = exprToWireFilterExpr(e)
    check wire.kind == wekLiteral
    check wire.litDataType == wdtNull

  test "converts column reference":
    let e = Expr(kind: exColumn, colName: "status")
    let wire = exprToWireFilterExpr(e)
    check wire.kind == wekColumn
    check wire.colName == "status"

  test "converts binary op equality":
    let left = Expr(kind: exColumn, colName: "id")
    let right = Expr(kind: exLiteral, litValue: newValueRef(1'i64))
    let e = Expr(kind: exBinOp, binOp: boEq, binLeft: left, binRight: right)
    let wire = exprToWireFilterExpr(e)
    check wire.kind == wekBinOp
    check wire.binOpKind == wboEq
    check wire.binLeft.kind == wekColumn
    check wire.binLeft.colName == "id"
    check wire.binRight.kind == wekLiteral
    check wire.binRight.litIntVal == 1'i64

  test "converts binary op less than":
    let left = Expr(kind: exColumn, colName: "age")
    let right = Expr(kind: exLiteral, litValue: newValueRef(18'i64))
    let e = Expr(kind: exBinOp, binOp: boLt, binLeft: left, binRight: right)
    let wire = exprToWireFilterExpr(e)
    check wire.kind == wekBinOp
    check wire.binOpKind == wboLt

  test "converts binary op greater than":
    let left = Expr(kind: exColumn, colName: "age")
    let right = Expr(kind: exLiteral, litValue: newValueRef(18'i64))
    let e = Expr(kind: exBinOp, binOp: boGt, binLeft: left, binRight: right)
    let wire = exprToWireFilterExpr(e)
    check wire.kind == wekBinOp
    check wire.binOpKind == wboGt

  test "converts AND expression":
    let leftInner = Expr(kind: exColumn, colName: "id")
    let leftLit = Expr(kind: exLiteral, litValue: newValueRef(1'i64))
    let left = Expr(kind: exBinOp, binOp: boEq, binLeft: leftInner,
        binRight: leftLit)
    let rightInner = Expr(kind: exColumn, colName: "status")
    let rightLit = Expr(kind: exLiteral, litValue: newValueRef("active"))
    let right = Expr(kind: exBinOp, binOp: boEq, binLeft: rightInner,
        binRight: rightLit)
    let e = Expr(kind: exBinOp, binOp: boAnd, binLeft: left, binRight: right)
    let wire = exprToWireFilterExpr(e)
    check wire.kind == wekBinOp
    check wire.binOpKind == wboAnd
    check wire.binLeft.kind == wekBinOp
    check wire.binLeft.binOpKind == wboEq
    check wire.binRight.kind == wekBinOp
    check wire.binRight.binOpKind == wboEq

  test "converts OR expression":
    let leftInner = Expr(kind: exColumn, colName: "status")
    let leftLit = Expr(kind: exLiteral, litValue: newValueRef("active"))
    let left = Expr(kind: exBinOp, binOp: boEq, binLeft: leftInner,
        binRight: leftLit)
    let rightLit = Expr(kind: exLiteral, litValue: newValueRef("pending"))
    let right = Expr(kind: exBinOp, binOp: boEq, binLeft: leftInner,
        binRight: rightLit)
    let e = Expr(kind: exBinOp, binOp: boOr, binLeft: left, binRight: right)
    let wire = exprToWireFilterExpr(e)
    check wire.kind == wekBinOp
    check wire.binOpKind == wboOr

  test "converts NOT expression":
    let innerCol = Expr(kind: exColumn, colName: "status")
    let innerLit = Expr(kind: exLiteral, litValue: newValueRef("active"))
    let inner = Expr(kind: exBinOp, binOp: boEq, binLeft: innerCol,
        binRight: innerLit)
    let e = Expr(kind: exUnaryOp, unaryOp: uoNot, unaryExpr: inner)
    let wire = exprToWireFilterExpr(e)
    check wire.kind == wekUnaryOp
    check wire.unaryOpKind == wuoNot
    check wire.unaryExpr.kind == wekBinOp

  test "converts IS NULL expression":
    let inner = Expr(kind: exColumn, colName: "deleted_at")
    let e = Expr(kind: exIsNull, isNullExpr: inner, isNullNot: false)
    let wire = exprToWireFilterExpr(e)
    check wire.kind == wekIsNull
    check wire.isNullNot == false
    check wire.isNullExpr.kind == wekColumn

  test "converts IS NOT NULL expression":
    let inner = Expr(kind: exColumn, colName: "deleted_at")
    let e = Expr(kind: exIsNull, isNullExpr: inner, isNullNot: true)
    let wire = exprToWireFilterExpr(e)
    check wire.kind == wekIsNull
    check wire.isNullNot == true

  test "converts BETWEEN expression":
    let exprCol = Expr(kind: exColumn, colName: "age")
    let lo = Expr(kind: exLiteral, litValue: newValueRef(18'i64))
    let hi = Expr(kind: exLiteral, litValue: newValueRef(65'i64))
    let e = Expr(kind: exBetween, betweenExpr: exprCol, betweenLo: lo,
        betweenHi: hi, betweenNot: false)
    let wire = exprToWireFilterExpr(e)
    check wire.kind == wekBetween
    check wire.betweenNot == false
    check wire.betweenExpr.colName == "age"
    check wire.betweenLo.litIntVal == 18'i64
    check wire.betweenHi.litIntVal == 65'i64

  test "converts LIKE expression":
    let exprCol = Expr(kind: exColumn, colName: "name")
    let pattern = Expr(kind: exLiteral, litValue: newValueRef("%test%"))
    let e = Expr(kind: exLike, likeExpr: exprCol, likePattern: pattern,
        likeNot: false)
    let wire = exprToWireFilterExpr(e)
    check wire.kind == wekLike
    check wire.likeNot == false
    check wire.likeExpr.colName == "name"
    check wire.likePattern.litStringVal == "%test%"

suite "Planner ORDER BY":

  test "formatSortSpecs single ascending":
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "id"),
        descending: false)]
    let text = formatSortSpecs(specs)
    check text == "id ASC"

  test "formatSortSpecs single descending":
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "name"),
        descending: true)]
    let text = formatSortSpecs(specs)
    check text == "name DESC"

  test "formatSortSpecs multiple specs":
    let specs = @[
      SortSpec(expr: Expr(kind: exColumn, colName: "age"), descending: false),
      SortSpec(expr: Expr(kind: exColumn, colName: "score"), descending: true)
    ]
    let text = formatSortSpecs(specs)
    check text == "age ASC, score DESC"

  test "formatSortSpecs empty":
    let specs: seq[SortSpec] = @[]
    let text = formatSortSpecs(specs)
    check text == ""

  test "OrderItem to SortSpec conversion":
    let orderItems = @[
      OrderItem(expr: Expr(kind: exColumn, colName: "id"), desc: false),
      OrderItem(expr: Expr(kind: exColumn, colName: "name"), desc: true)
    ]
    let columns = @["id", "name", "age"]
    let specs = orderItemsToSortSpecs(orderItems, columns)
    check specs.len == 2
    check specs[0].expr.colName == "id"
    check specs[0].descending == false
    check specs[1].expr.colName == "name"
    check specs[1].descending == true

  test "poOrderBy PlanOp structure":
    let sortExpr = Expr(kind: exColumn, colName: "price")
    let op = PlanOp(
      kind: poOrderBy,
      obSortSpecs: @[SortSpec(expr: sortExpr, descending: true)],
      obColumns: @["id", "name", "price"],
      obAllColumns: @["id", "name", "price"]
    )
    check op.kind == poOrderBy
    check op.obSortSpecs.len == 1
    check op.obSortSpecs[0].expr.colName == "price"
    check op.obSortSpecs[0].descending == true
    check op.obColumns.len == 3

  test "formatPlanOp poOrderBy single column":
    let op = PlanOp(
      kind: poOrderBy,
      obSortSpecs: @[SortSpec(expr: Expr(kind: exColumn, colName: "name"),
          descending: false)],
      obColumns: @["id", "name"]
    )
    let text = formatPlanOp(op)
    check "OrderBy" in text
    check "name ASC" in text

  test "formatPlanOp poOrderBy multiple columns":
    let op = PlanOp(
      kind: poOrderBy,
      obSortSpecs: @[
        SortSpec(expr: Expr(kind: exColumn, colName: "age"), descending: true),
        SortSpec(expr: Expr(kind: exColumn, colName: "score"),
            descending: false)
      ],
      obColumns: @["id", "age", "score"]
    )
    let text = formatPlanOp(op)
    check "OrderBy" in text
    check "age DESC" in text
    check "score ASC" in text

suite "Planner ORDER BY PK Optimization":

  test "detectOrderByPkOptimization ASC match - single PK":
    let orderItems = @[OrderItem(expr: Expr(kind: exColumn, colName: "id"), desc: false)]
    let opt = detectOrderByPkOptimization(orderItems, @["id"])
    check opt == oboPkAscMatch

  test "detectOrderByPkOptimization DESC match - single PK":
    let orderItems = @[OrderItem(expr: Expr(kind: exColumn, colName: "id"), desc: true)]
    let opt = detectOrderByPkOptimization(orderItems, @["id"])
    check opt == oboPkDescMatch

  test "detectOrderByPkOptimization no match - different column":
    let orderItems = @[OrderItem(expr: Expr(kind: exColumn, colName: "name"), desc: false)]
    let opt = detectOrderByPkOptimization(orderItems, @["id"])
    check opt == oboNone

  test "detectOrderByPkOptimization no match - expression":
    let orderItems = @[OrderItem(
      expr: Expr(kind: exBinOp, binOp: boAdd,
        binLeft: Expr(kind: exColumn, colName: "id"),
        binRight: Expr(kind: exLiteral, litValue: newValueRef(10'i64))),
      desc: false)
    ]
    let opt = detectOrderByPkOptimization(orderItems, @["id"])
    check opt == oboNone

  test "detectOrderByPkOptimization no match - extra ORDER BY columns":
    let orderItems = @[
      OrderItem(expr: Expr(kind: exColumn, colName: "id"), desc: false),
      OrderItem(expr: Expr(kind: exColumn, colName: "name"), desc: true)
    ]
    let opt = detectOrderByPkOptimization(orderItems, @["id"])
    check opt == oboNone

  test "detectOrderByPkOptimization composite PK ASC match":
    let orderItems = @[
      OrderItem(expr: Expr(kind: exColumn, colName: "tenant_id"), desc: false),
      OrderItem(expr: Expr(kind: exColumn, colName: "user_id"), desc: false)
    ]
    let opt = detectOrderByPkOptimization(orderItems, @["tenant_id", "user_id"])
    check opt == oboPkAscMatch

  test "detectOrderByPkOptimization composite PK DESC match":
    let orderItems = @[
      OrderItem(expr: Expr(kind: exColumn, colName: "tenant_id"), desc: true),
      OrderItem(expr: Expr(kind: exColumn, colName: "user_id"), desc: true)
    ]
    let opt = detectOrderByPkOptimization(orderItems, @["tenant_id", "user_id"])
    check opt == oboPkDescMatch

  test "detectOrderByPkOptimization composite PK no match - wrong order":
    let orderItems = @[
      OrderItem(expr: Expr(kind: exColumn, colName: "user_id"), desc: false),
      OrderItem(expr: Expr(kind: exColumn, colName: "tenant_id"), desc: false)
    ]
    let opt = detectOrderByPkOptimization(orderItems, @["tenant_id", "user_id"])
    check opt == oboNone

  test "detectOrderByPkOptimization composite PK no match - mixed direction":
    let orderItems = @[
      OrderItem(expr: Expr(kind: exColumn, colName: "tenant_id"), desc: false),
      OrderItem(expr: Expr(kind: exColumn, colName: "user_id"), desc: true)
    ]
    let opt = detectOrderByPkOptimization(orderItems, @["tenant_id", "user_id"])
    check opt == oboNone

  test "detectOrderByPkOptimization composite PK no match - partial columns":
    let orderItems = @[
      OrderItem(expr: Expr(kind: exColumn, colName: "tenant_id"), desc: false)
    ]
    let opt = detectOrderByPkOptimization(orderItems, @["tenant_id", "user_id"])
    check opt == oboNone

  test "formatPlanOp poOrderBy with PK ASC optimization":
    let op = PlanOp(
      kind: poOrderBy,
      obSortSpecs: @[],
      obColumns: @["id", "name"],
      obOptimization: oboPkAscMatch
    )
    let text = formatPlanOp(op)
    check "PK_ASC_SKIP" in text
    check "specs" notin text

  test "formatPlanOp poOrderBy with PK DESC optimization":
    let op = PlanOp(
      kind: poOrderBy,
      obSortSpecs: @[],
      obColumns: @["id", "name"],
      obLimit: 10,
      obOptimization: oboPkDescMatch
    )
    let text = formatPlanOp(op)
    check "PK_DESC_REVERSE" in text
    check "limit=10" in text

  test "formatPlanOp poOrderBy with top-K optimization":
    let op = PlanOp(
      kind: poOrderBy,
      obSortSpecs: @[SortSpec(expr: Expr(kind: exColumn, colName: "name"),
          descending: true)],
      obColumns: @["id", "name"],
      obLimit: 10,
      obOptimization: oboTopK
    )
    let text = formatPlanOp(op)
    check "TOP_K" in text
    check "limit=10" in text

  test "formatPlanOp poScan with reverse=true":
    let op = PlanOp(
      kind: poScan,
      scTableId: zeroTableId(),
      scStartKey: "/t/01KSPTJ47SXS9BR2WTTKV7N6BV/d/",
      scEndKey: "/t/01KSPTJ47SXS9BR2WTTKV7N6BV/e",
      scLimit: 10,
      scReverse: true,
      scFilter: none(Expr),
      scColumns: @["id", "name"],
      scAllColumns: @["id", "name", "email"]
    )
    let text = formatPlanOp(op)
    check "reverse=true" in text
    check "limit=10" in text

suite "Planner ORDER BY + LIMIT Optimization (scanLimit and obOptimization)":

  test "oboTopK formatPlanOp":
    let op = PlanOp(
      kind: poOrderBy,
      obSortSpecs: @[SortSpec(expr: Expr(kind: exColumn, colName: "name"),
          descending: true)],
      obColumns: @["id", "name"],
      obAllColumns: @["id", "name", "email"],
      obLimit: 10,
      obOptimization: oboTopK
    )
    let text = formatPlanOp(op)
    check "TOP_K" in text
    check "name" in text
