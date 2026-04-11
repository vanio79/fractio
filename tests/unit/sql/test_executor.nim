import std/[unittest, options, json]
import fractio/sql/ast
import fractio/sql/executor
import fractio/sql/data_row
import fractio/core/types

suite "Executor Result Constructors":

  test "okResult":
    let r = okResult("CREATE TABLE")
    check r.kind == erkOk
    check r.okMessage == "CREATE TABLE"

  test "errorResult":
    let r = errorResult("table not found")
    check r.kind == erkError
    check r.error == "table not found"

  test "modifiedResult with count":
    let r = modifiedResult(5)
    check r.kind == erkModified
    check r.count == 5
    check r.message == "5 row(s) affected"

  test "modifiedResult with custom message":
    let r = modifiedResult(3, "INSERT 3")
    check r.kind == erkModified
    check r.count == 3
    check r.message == "INSERT 3"

  test "modifiedResult zero":
    let r = modifiedResult(0)
    check r.count == 0
    check r.message == "0 row(s) affected"

  test "rowsResult empty":
    let r = rowsResult(@["id", "name"], @[])
    check r.kind == erkRows
    check r.columns == @["id", "name"]
    check r.rows.len == 0

  test "rowsResult with data":
    let r = rowsResult(@["id", "name"], @[@["1", "Alice"], @["2", "Bob"]])
    check r.kind == erkRows
    check r.columns.len == 2
    check r.rows.len == 2
    check r.rows[0] == @["1", "Alice"]
    check r.rows[1] == @["2", "Bob"]

suite "Executor ExecResultKind":

  test "all ExecResultKind values":
    check erkRows.ord >= 0
    check erkModified.ord >= 0
    check erkOk.ord >= 0
    check erkError.ord >= 0
    check erkUseDatabase.ord >= 0
    check erkUseSchema.ord >= 0

suite "Executor ExecResult Variants":

  test "erkUseDatabase":
    let r = ExecResult(kind: erkUseDatabase, newDatabase: "mydb")
    check r.kind == erkUseDatabase
    check r.newDatabase == "mydb"

  test "erkUseSchema":
    let r = ExecResult(kind: erkUseSchema, newSchema: "reporting")
    check r.kind == erkUseSchema
    check r.newSchema == "reporting"

suite "Executor evalExprDataRow Literals":

  test "integer literal":
    let e = Expr(kind: exLiteral, litValue: newValueRef(42'i64))
    let row = newDataRow()
    let v = evalExprDataRow(e, row)
    check v.kind == drvkInt
    check v.intVal == 42

  test "float literal":
    let e = Expr(kind: exLiteral, litValue: newValueRef(3.14))
    let row = newDataRow()
    let v = evalExprDataRow(e, row)
    check v.kind == drvkFloat
    check v.floatVal == 3.14

  test "string literal":
    let e = Expr(kind: exLiteral, litValue: newValueRef("hello"))
    let row = newDataRow()
    let v = evalExprDataRow(e, row)
    check v.kind == drvkString
    check v.strVal == "hello"

  test "bool literal":
    let e = Expr(kind: exLiteral, litValue: newValueRef(true))
    let row = newDataRow()
    let v = evalExprDataRow(e, row)
    check v.kind == drvkBool
    check v.boolVal == true

  test "null literal":
    let e = Expr(kind: exLiteral, litValue: nil)
    let row = newDataRow()
    let v = evalExprDataRow(e, row)
    check v.kind == drvkNull

suite "Executor evalExprDataRow Column":

  test "column reference":
    let e = Expr(kind: exColumn, colName: "id")
    let row = newDataRow(@[newColumn("id", newRowValue(123'i64))])
    let v = evalExprDataRow(e, row)
    check v.kind == drvkInt
    check v.intVal == 123

  test "column reference missing":
    let e = Expr(kind: exColumn, colName: "missing")
    let row = newDataRow()
    let v = evalExprDataRow(e, row)
    check v.kind == drvkNull

  test "column reference string":
    let e = Expr(kind: exColumn, colName: "name")
    let row = newDataRow(@[newColumn("name", newRowValue("Alice"))])
    let v = evalExprDataRow(e, row)
    check v.kind == drvkString
    check v.strVal == "Alice"

suite "Executor evalExprDataRow Binary Operators":

  test "equality true":
    let e = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(42'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(42'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.kind == drvkBool
    check v.boolVal == true

  test "equality false":
    let e = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(42'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(43'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "inequality":
    let e = Expr(kind: exBinOp, binOp: boNeq,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(42'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(43'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "less than":
    let e = Expr(kind: exBinOp, binOp: boLt,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(10'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(20'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "less than or equal":
    let e = Expr(kind: exBinOp, binOp: boLte,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(20'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(20'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "greater than":
    let e = Expr(kind: exBinOp, binOp: boGt,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(20'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(10'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "greater than or equal":
    let e = Expr(kind: exBinOp, binOp: boGte,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(20'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(20'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "AND true true":
    let e = Expr(kind: exBinOp, binOp: boAnd,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(true)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(true)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "AND true false":
    let e = Expr(kind: exBinOp, binOp: boAnd,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(true)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(false)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "OR false false":
    let e = Expr(kind: exBinOp, binOp: boOr,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(false)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(false)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "OR true false":
    let e = Expr(kind: exBinOp, binOp: boOr,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(true)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(false)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "addition":
    let e = Expr(kind: exBinOp, binOp: boAdd,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(3'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.kind == drvkInt
    check v.intVal == 8

  test "subtraction":
    let e = Expr(kind: exBinOp, binOp: boSub,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(10'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(3'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.intVal == 7

  test "multiplication":
    let e = Expr(kind: exBinOp, binOp: boMul,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(6'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(7'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.intVal == 42

  test "division":
    let e = Expr(kind: exBinOp, binOp: boDiv,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(20'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(4'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.intVal == 5

  test "modulo":
    let e = Expr(kind: exBinOp, binOp: boMod,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(17'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(5'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.intVal == 2

  test "division by zero returns null":
    let e = Expr(kind: exBinOp, binOp: boDiv,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(10'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(0'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.kind == drvkNull

suite "Executor evalExprDataRow Unary Operators":

  test "NOT true":
    let e = Expr(kind: exUnaryOp, unaryOp: uoNot,
      unaryExpr: Expr(kind: exLiteral, litValue: newValueRef(true)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "NOT false":
    let e = Expr(kind: exUnaryOp, unaryOp: uoNot,
      unaryExpr: Expr(kind: exLiteral, litValue: newValueRef(false)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "negation":
    let e = Expr(kind: exUnaryOp, unaryOp: uoNeg,
      unaryExpr: Expr(kind: exLiteral, litValue: newValueRef(42'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.intVal == -42

suite "Executor evalExprDataRow IS NULL":

  test "IS NULL true":
    let e = Expr(kind: exIsNull,
      isNullExpr: Expr(kind: exLiteral, litValue: nil),
      isNullNot: false)
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "IS NULL false for non-null":
    let e = Expr(kind: exIsNull,
      isNullExpr: Expr(kind: exLiteral, litValue: newValueRef(42'i64)),
      isNullNot: false)
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "IS NOT NULL true for non-null":
    let e = Expr(kind: exIsNull,
      isNullExpr: Expr(kind: exLiteral, litValue: newValueRef(42'i64)),
      isNullNot: true)
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "IS NOT NULL false for null":
    let e = Expr(kind: exIsNull,
      isNullExpr: Expr(kind: exLiteral, litValue: nil),
      isNullNot: true)
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

suite "Executor evalExprDataRow IN":

  test "IN list match":
    let e = Expr(kind: exIn,
      inExpr: Expr(kind: exLiteral, litValue: newValueRef(2'i64)),
      inNot: false,
      inList: @[
        Expr(kind: exLiteral, litValue: newValueRef(1'i64)),
        Expr(kind: exLiteral, litValue: newValueRef(2'i64)),
        Expr(kind: exLiteral, litValue: newValueRef(3'i64))
      ])
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "IN list no match":
    let e = Expr(kind: exIn,
      inExpr: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      inNot: false,
      inList: @[
        Expr(kind: exLiteral, litValue: newValueRef(1'i64)),
        Expr(kind: exLiteral, litValue: newValueRef(2'i64))
      ])
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "NOT IN":
    let e = Expr(kind: exIn,
      inExpr: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      inNot: true,
      inList: @[
        Expr(kind: exLiteral, litValue: newValueRef(1'i64)),
        Expr(kind: exLiteral, litValue: newValueRef(2'i64))
      ])
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

suite "Executor evalExprDataRow BETWEEN":

  test "BETWEEN in range":
    let e = Expr(kind: exBetween,
      betweenExpr: Expr(kind: exLiteral, litValue: newValueRef(30'i64)),
      betweenNot: false,
      betweenLo: Expr(kind: exLiteral, litValue: newValueRef(18'i64)),
      betweenHi: Expr(kind: exLiteral, litValue: newValueRef(65'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "BETWEEN below range":
    let e = Expr(kind: exBetween,
      betweenExpr: Expr(kind: exLiteral, litValue: newValueRef(10'i64)),
      betweenNot: false,
      betweenLo: Expr(kind: exLiteral, litValue: newValueRef(18'i64)),
      betweenHi: Expr(kind: exLiteral, litValue: newValueRef(65'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "BETWEEN above range":
    let e = Expr(kind: exBetween,
      betweenExpr: Expr(kind: exLiteral, litValue: newValueRef(70'i64)),
      betweenNot: false,
      betweenLo: Expr(kind: exLiteral, litValue: newValueRef(18'i64)),
      betweenHi: Expr(kind: exLiteral, litValue: newValueRef(65'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "NOT BETWEEN":
    let e = Expr(kind: exBetween,
      betweenExpr: Expr(kind: exLiteral, litValue: newValueRef(10'i64)),
      betweenNot: true,
      betweenLo: Expr(kind: exLiteral, litValue: newValueRef(18'i64)),
      betweenHi: Expr(kind: exLiteral, litValue: newValueRef(65'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

suite "Executor evalExprDataRow LIKE":

  test "LIKE exact match":
    let e = Expr(kind: exLike,
      likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello")),
      likeNot: false,
      likePattern: Expr(kind: exLiteral, litValue: newValueRef("hello")))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "LIKE prefix match":
    let e = Expr(kind: exLike,
      likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello world")),
      likeNot: false,
      likePattern: Expr(kind: exLiteral, litValue: newValueRef("hello%")))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "LIKE suffix match":
    let e = Expr(kind: exLike,
      likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello world")),
      likeNot: false,
      likePattern: Expr(kind: exLiteral, litValue: newValueRef("%world")))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "LIKE contains match":
    let e = Expr(kind: exLike,
      likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello world")),
      likeNot: false,
      likePattern: Expr(kind: exLiteral, litValue: newValueRef("%lo wo%")))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "LIKE no match":
    let e = Expr(kind: exLike,
      likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello")),
      likeNot: false,
      likePattern: Expr(kind: exLiteral, litValue: newValueRef("world%")))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "NOT LIKE":
    let e = Expr(kind: exLike,
      likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello")),
      likeNot: true,
      likePattern: Expr(kind: exLiteral, litValue: newValueRef("world%")))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

suite "Executor matchesFilterDataRow":

  test "empty filter matches all":
    let row = newDataRow()
    check matchesFilterDataRow(none(Expr), row) == true

  test "filter matches":
    let filter = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exColumn, colName: "id"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(1'i64)))
    let row = newDataRow(@[newColumn("id", newRowValue(1'i64))])
    check matchesFilterDataRow(some(filter), row) == true

  test "filter does not match":
    let filter = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exColumn, colName: "id"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(1'i64)))
    let row = newDataRow(@[newColumn("id", newRowValue(2'i64))])
    check matchesFilterDataRow(some(filter), row) == false

  test "complex filter AND":
    let filter = Expr(kind: exBinOp, binOp: boAnd,
      binLeft: Expr(kind: exBinOp, binOp: boGt,
        binLeft: Expr(kind: exColumn, colName: "age"),
        binRight: Expr(kind: exLiteral, litValue: newValueRef(18'i64))),
      binRight: Expr(kind: exBinOp, binOp: boLt,
        binLeft: Expr(kind: exColumn, colName: "age"),
        binRight: Expr(kind: exLiteral, litValue: newValueRef(65'i64))))
    let row1 = newDataRow(@[newColumn("age", newRowValue(30'i64))])
    check matchesFilterDataRow(some(filter), row1) == true
    let row2 = newDataRow(@[newColumn("age", newRowValue(10'i64))])
    check matchesFilterDataRow(some(filter), row2) == false

suite "Executor evalExpr (JSON legacy)":

  test "integer literal to JSON":
    let e = Expr(kind: exLiteral, litValue: newValueRef(42'i64))
    let j = evalExpr(e, newJObject())
    check j.kind == JInt
    check j.getInt == 42

  test "string literal to JSON":
    let e = Expr(kind: exLiteral, litValue: newValueRef("hello"))
    let j = evalExpr(e, newJObject())
    check j.kind == JString
    check j.getStr == "hello"

  test "bool literal to JSON":
    let e = Expr(kind: exLiteral, litValue: newValueRef(true))
    let j = evalExpr(e, newJObject())
    check j.kind == JBool
    check j.getBool == true

  test "null literal to JSON":
    let e = Expr(kind: exLiteral, litValue: nil)
    let j = evalExpr(e, newJObject())
    check j.kind == JNull

  test "column from JSON":
    let e = Expr(kind: exColumn, colName: "name")
    let row = %*{"name": "Alice", "age": 30}
    let j = evalExpr(e, row)
    check j.kind == JString
    check j.getStr == "Alice"

  test "equality JSON":
    let e = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(42'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(42'i64)))
    let j = evalExpr(e, newJObject())
    check j.kind == JBool
    check j.getBool == true

  test "less than JSON":
    let e = Expr(kind: exBinOp, binOp: boLt,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(10'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(20'i64)))
    let j = evalExpr(e, newJObject())
    check j.getBool == true

suite "Executor matchesFilter (JSON legacy)":

  test "empty filter":
    let row = %*{"id": 1}
    check matchesFilter(none(Expr), row) == true

  test "filter matches JSON":
    let filter = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exColumn, colName: "status"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef("active")))
    let row = %*{"status": "active"}
    check matchesFilter(some(filter), row) == true

  test "filter does not match JSON":
    let filter = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exColumn, colName: "status"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef("active")))
    let row = %*{"status": "pending"}
    check matchesFilter(some(filter), row) == false

suite "Executor evalExprDataRow Other ExprKinds":

  test "exStar returns null":
    let e = Expr(kind: exStar)
    let v = evalExprDataRow(e, newDataRow())
    check v.kind == drvkNull

  test "exParam returns null":
    let e = Expr(kind: exParam, paramIdx: 1)
    let v = evalExprDataRow(e, newDataRow())
    check v.kind == drvkNull

  test "exList returns null":
    let e = Expr(kind: exList, listItems: @[])
    let v = evalExprDataRow(e, newDataRow())
    check v.kind == drvkNull

suite "Executor KVEntry":

  test "KVEntry construction":
    let kv = KVEntry(key: "/t/123/key", value: "data")
    check kv.key == "/t/123/key"
    check kv.value == "data"
