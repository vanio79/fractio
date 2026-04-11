import std/[unittest, options, json, strutils]
import fractio/sql/ast
import fractio/sql/planner
import fractio/sql/parser
import fractio/sql/data_row
import fractio/core/types

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

suite "Planner PlanError":
  test "PlanError type exists":
    var e: ref PlanError
    new e
    e.msg = "table not found"
    check e.msg == "table not found"
