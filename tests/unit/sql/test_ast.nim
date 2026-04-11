import std/[unittest, options]
import fractio/sql/ast
import fractio/core/types

suite "AST Expr Types":

  test "exLiteral with integer":
    let v = newValueRef(42'i64)
    let e = Expr(kind: exLiteral, litValue: v)
    check e.kind == exLiteral
    check e.litValue != nil
    check e.litValue.kind == dtInt
    check e.litValue.intValue == 42

  test "exLiteral with float":
    let v = newValueRef(3.14)
    let e = Expr(kind: exLiteral, litValue: v)
    check e.kind == exLiteral
    check e.litValue.kind == dtFloat
    check e.litValue.floatValue == 3.14

  test "exLiteral with string":
    let v = newValueRef("hello")
    let e = Expr(kind: exLiteral, litValue: v)
    check e.kind == exLiteral
    check e.litValue.kind == dtString
    check e.litValue.strValue == "hello"

  test "exLiteral with bool":
    let v = newValueRef(true)
    let e = Expr(kind: exLiteral, litValue: v)
    check e.kind == exLiteral
    check e.litValue.kind == dtBool
    check e.litValue.boolValue == true

  test "exLiteral nil represents NULL":
    let e = Expr(kind: exLiteral, litValue: nil)
    check e.kind == exLiteral
    check e.litValue == nil

  test "exColumn without table":
    let e = Expr(kind: exColumn, colTable: "", colName: "id")
    check e.kind == exColumn
    check e.colTable == ""
    check e.colName == "id"

  test "exColumn with table":
    let e = Expr(kind: exColumn, colTable: "users", colName: "name")
    check e.colTable == "users"
    check e.colName == "name"

  test "exParam positional":
    let e = Expr(kind: exParam, paramIdx: 1)
    check e.kind == exParam
    check e.paramIdx == 1

suite "AST Binary Operators":

  test "exBinOp equality":
    let left = Expr(kind: exColumn, colTable: "", colName: "a")
    let right = Expr(kind: exLiteral, litValue: newValueRef(1'i64))
    let e = Expr(kind: exBinOp, binOp: boEq, binLeft: left, binRight: right)
    check e.kind == exBinOp
    check e.binOp == boEq
    check e.binLeft.kind == exColumn
    check e.binRight.kind == exLiteral

  test "exBinOp comparison operators":
    let left = Expr(kind: exColumn, colName: "x")
    let right = Expr(kind: exLiteral, litValue: newValueRef(10'i64))
    check Expr(kind: exBinOp, binOp: boLt, binLeft: left,
        binRight: right).binOp == boLt
    check Expr(kind: exBinOp, binOp: boLte, binLeft: left,
        binRight: right).binOp == boLte
    check Expr(kind: exBinOp, binOp: boGt, binLeft: left,
        binRight: right).binOp == boGt
    check Expr(kind: exBinOp, binOp: boGte, binLeft: left,
        binRight: right).binOp == boGte
    check Expr(kind: exBinOp, binOp: boNeq, binLeft: left,
        binRight: right).binOp == boNeq

  test "exBinOp logical operators":
    let a = Expr(kind: exLiteral, litValue: newValueRef(true))
    let b = Expr(kind: exLiteral, litValue: newValueRef(false))
    check Expr(kind: exBinOp, binOp: boAnd, binLeft: a, binRight: b).binOp == boAnd
    check Expr(kind: exBinOp, binOp: boOr, binLeft: a, binRight: b).binOp == boOr

  test "exBinOp arithmetic operators":
    let a = Expr(kind: exLiteral, litValue: newValueRef(5'i64))
    let b = Expr(kind: exLiteral, litValue: newValueRef(3'i64))
    check Expr(kind: exBinOp, binOp: boAdd, binLeft: a, binRight: b).binOp == boAdd
    check Expr(kind: exBinOp, binOp: boSub, binLeft: a, binRight: b).binOp == boSub
    check Expr(kind: exBinOp, binOp: boMul, binLeft: a, binRight: b).binOp == boMul
    check Expr(kind: exBinOp, binOp: boDiv, binLeft: a, binRight: b).binOp == boDiv
    check Expr(kind: exBinOp, binOp: boMod, binLeft: a, binRight: b).binOp == boMod

  test "exBinOp nested":
    let inner = Expr(kind: exBinOp, binOp: boAdd,
      binLeft: Expr(kind: exColumn, colName: "a"),
      binRight: Expr(kind: exColumn, colName: "b"))
    let outer = Expr(kind: exBinOp, binOp: boMul,
      binLeft: inner,
      binRight: Expr(kind: exLiteral, litValue: newValueRef(2'i64)))
    check outer.kind == exBinOp
    check outer.binLeft.kind == exBinOp
    check outer.binLeft.binOp == boAdd

suite "AST Unary Operators":

  test "exUnaryOp NOT":
    let inner = Expr(kind: exLiteral, litValue: newValueRef(true))
    let e = Expr(kind: exUnaryOp, unaryOp: uoNot, unaryExpr: inner)
    check e.kind == exUnaryOp
    check e.unaryOp == uoNot
    check e.unaryExpr.kind == exLiteral

  test "exUnaryOp negation":
    let inner = Expr(kind: exLiteral, litValue: newValueRef(42'i64))
    let e = Expr(kind: exUnaryOp, unaryOp: uoNeg, unaryExpr: inner)
    check e.kind == exUnaryOp
    check e.unaryOp == uoNeg

suite "AST Special Expressions":

  test "exIsNull":
    let inner = Expr(kind: exColumn, colName: "nullable_col")
    let e = Expr(kind: exIsNull, isNullExpr: inner, isNullNot: false)
    check e.kind == exIsNull
    check e.isNullNot == false

  test "exIsNull NOT NULL":
    let inner = Expr(kind: exColumn, colName: "nullable_col")
    let e = Expr(kind: exIsNull, isNullExpr: inner, isNullNot: true)
    check e.isNullNot == true

  test "exIn list":
    let expr = Expr(kind: exColumn, colName: "status")
    let items = @[
      Expr(kind: exLiteral, litValue: newValueRef("active")),
      Expr(kind: exLiteral, litValue: newValueRef("pending"))
    ]
    let e = Expr(kind: exIn, inExpr: expr, inNot: false, inList: items)
    check e.kind == exIn
    check e.inNot == false
    check e.inList.len == 2

  test "exIn NOT IN":
    let expr = Expr(kind: exColumn, colName: "status")
    let e = Expr(kind: exIn, inExpr: expr, inNot: true, inList: @[])
    check e.inNot == true

  test "exBetween":
    let expr = Expr(kind: exColumn, colName: "age")
    let lo = Expr(kind: exLiteral, litValue: newValueRef(18'i64))
    let hi = Expr(kind: exLiteral, litValue: newValueRef(65'i64))
    let e = Expr(kind: exBetween, betweenExpr: expr, betweenNot: false,
                 betweenLo: lo, betweenHi: hi)
    check e.kind == exBetween
    check e.betweenNot == false
    check e.betweenLo.kind == exLiteral
    check e.betweenHi.kind == exLiteral

  test "exBetween NOT BETWEEN":
    let e = Expr(kind: exBetween, betweenExpr: Expr(kind: exColumn, colName: "x"),
                 betweenNot: true, betweenLo: Expr(kind: exLiteral,
                     litValue: nil),
                 betweenHi: Expr(kind: exLiteral, litValue: nil))
    check e.betweenNot == true

  test "exLike":
    let expr = Expr(kind: exColumn, colName: "name")
    let pat = Expr(kind: exLiteral, litValue: newValueRef("A%"))
    let e = Expr(kind: exLike, likeExpr: expr, likeNot: false, likePattern: pat)
    check e.kind == exLike
    check e.likeNot == false
    check e.likePattern.kind == exLiteral

  test "exLike NOT LIKE":
    let e = Expr(kind: exLike, likeExpr: Expr(kind: exColumn, colName: "name"),
                 likeNot: true, likePattern: Expr(kind: exLiteral,
                     litValue: nil))
    check e.likeNot == true

  test "exList":
    let items = @[
      Expr(kind: exLiteral, litValue: newValueRef(1'i64)),
      Expr(kind: exLiteral, litValue: newValueRef(2'i64))
    ]
    let e = Expr(kind: exList, listItems: items)
    check e.kind == exList
    check e.listItems.len == 2

  test "exStar":
    let e = Expr(kind: exStar)
    check e.kind == exStar

suite "AST SelectCol":

  test "SelectCol without alias":
    let expr = Expr(kind: exColumn, colName: "id")
    let sc = SelectCol(expr: expr, alias: "")
    check sc.expr.kind == exColumn
    check sc.alias == ""

  test "SelectCol with alias":
    let expr = Expr(kind: exColumn, colName: "user_id")
    let sc = SelectCol(expr: expr, alias: "uid")
    check sc.alias == "uid"

  test "SelectCol with star":
    let sc = SelectCol(expr: Expr(kind: exStar), alias: "")
    check sc.expr.kind == exStar

suite "AST OrderItem":

  test "OrderItem ASC":
    let expr = Expr(kind: exColumn, colName: "name")
    let oi = OrderItem(expr: expr, desc: false)
    check oi.desc == false

  test "OrderItem DESC":
    let expr = Expr(kind: exColumn, colName: "created_at")
    let oi = OrderItem(expr: expr, desc: true)
    check oi.desc == true

suite "AST ColDef":

  test "ColDef basic":
    let cd = ColDef(name: "id", dataType: dtInt, maxLen: 0)
    check cd.name == "id"
    check cd.dataType == dtInt
    check cd.maxLen == 0
    check cd.notNull == false
    check cd.primaryKey == false
    check cd.unique == false
    check cd.defaultExpr.isNone

  test "ColDef with constraints":
    let cd = ColDef(name: "email", dataType: dtString, maxLen: 255,
                    notNull: true, unique: true)
    check cd.notNull == true
    check cd.unique == true

  test "ColDef with default":
    let defaultExpr = Expr(kind: exLiteral, litValue: newValueRef(0'i64))
    let cd = ColDef(name: "count", dataType: dtInt, defaultExpr: some(defaultExpr))
    check cd.defaultExpr.isSome
    check cd.defaultExpr.get().kind == exLiteral

  test "ColDef VARCHAR with maxLen":
    let cd = ColDef(name: "name", dataType: dtString, maxLen: 100)
    check cd.dataType == dtString
    check cd.maxLen == 100

  test "ColDef all data types":
    check ColDef(name: "a", dataType: dtInt).dataType == dtInt
    check ColDef(name: "b", dataType: dtFloat).dataType == dtFloat
    check ColDef(name: "c", dataType: dtString).dataType == dtString
    check ColDef(name: "d", dataType: dtBool).dataType == dtBool
    check ColDef(name: "e", dataType: dtDate).dataType == dtDate
    check ColDef(name: "f", dataType: dtDateTime).dataType == dtDateTime
    check ColDef(name: "g", dataType: dtBytes).dataType == dtBytes
    check ColDef(name: "h", dataType: dtULID).dataType == dtULID

suite "AST Stmt Kind":

  test "stmtCreateTable basic":
    let s = Stmt(kind: stmtCreateTable, ctTable: "users", ctIfNotExists: false,
                 ctColumns: @[], ctPrimaryKey: @[])
    check s.kind == stmtCreateTable
    check s.ctTable == "users"
    check s.ctIfNotExists == false

  test "stmtCreateTable IF NOT EXISTS":
    let s = Stmt(kind: stmtCreateTable, ctTable: "t", ctIfNotExists: true)
    check s.ctIfNotExists == true

  test "stmtCreateTable with columns":
    let cols = @[
      ColDef(name: "id", dataType: dtInt, primaryKey: true),
      ColDef(name: "name", dataType: dtString, maxLen: 100)
    ]
    let s = Stmt(kind: stmtCreateTable, ctTable: "users", ctColumns: cols)
    check s.ctColumns.len == 2
    check s.ctColumns[0].primaryKey == true

  test "stmtCreateTable with replicas":
    let s = Stmt(kind: stmtCreateTable, ctTable: "t", ctColumns: @[],
                 ctReplicas: some(3))
    check s.ctReplicas.isSome
    check s.ctReplicas.get() == 3

  test "stmtCreateTable with space":
    let s = Stmt(kind: stmtCreateTable, ctTable: "t", ctColumns: @[],
                 ctSpaceName: some("space1"))
    check s.ctSpaceName.isSome
    check s.ctSpaceName.get() == "space1"

  test "stmtDropTable":
    let s = Stmt(kind: stmtDropTable, dtTable: "old_table", dtIfExists: false)
    check s.kind == stmtDropTable
    check s.dtTable == "old_table"
    check s.dtIfExists == false

  test "stmtDropTable IF EXISTS":
    let s = Stmt(kind: stmtDropTable, dtTable: "t", dtIfExists: true)
    check s.dtIfExists == true

suite "AST Stmt DML":

  test "stmtSelect basic":
    let cols = @[SelectCol(expr: Expr(kind: exStar), alias: "")]
    let s = Stmt(kind: stmtSelect, selDistinct: false, selCols: cols,
                 selFrom: "users")
    check s.kind == stmtSelect
    check s.selDistinct == false
    check s.selCols.len == 1
    check s.selFrom == "users"

  test "stmtSelect DISTINCT":
    let s = Stmt(kind: stmtSelect, selDistinct: true, selCols: @[], selFrom: "t")
    check s.selDistinct == true

  test "stmtSelect with WHERE":
    let whereExpr = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exColumn, colName: "id"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(1'i64)))
    let s = Stmt(kind: stmtSelect, selCols: @[], selFrom: "t",
                 selWhere: some(whereExpr))
    check s.selWhere.isSome
    check s.selWhere.get().kind == exBinOp

  test "stmtSelect with ORDER BY":
    let orderBy = @[OrderItem(expr: Expr(kind: exColumn, colName: "name"), desc: false)]
    let s = Stmt(kind: stmtSelect, selCols: @[], selFrom: "t",
        selOrderBy: orderBy)
    check s.selOrderBy.len == 1
    check s.selOrderBy[0].desc == false

  test "stmtSelect with LIMIT/OFFSET":
    let limitExpr = Expr(kind: exLiteral, litValue: newValueRef(10'i64))
    let offsetExpr = Expr(kind: exLiteral, litValue: newValueRef(5'i64))
    let s = Stmt(kind: stmtSelect, selCols: @[], selFrom: "t",
                 selLimit: some(limitExpr), selOffset: some(offsetExpr))
    check s.selLimit.isSome
    check s.selOffset.isSome

  test "stmtInsert basic":
    let values = @[
      @[Expr(kind: exLiteral, litValue: newValueRef(1'i64)),
        Expr(kind: exLiteral, litValue: newValueRef("Alice"))]
    ]
    let s = Stmt(kind: stmtInsert, intoTable: "users", intoCols: @["id", "name"],
                 intoValues: values)
    check s.kind == stmtInsert
    check s.intoTable == "users"
    check s.intoCols.len == 2
    check s.intoValues.len == 1
    check s.intoValues[0].len == 2

  test "stmtInsert multi-row":
    let values = @[
      @[Expr(kind: exLiteral, litValue: newValueRef(1'i64))],
      @[Expr(kind: exLiteral, litValue: newValueRef(2'i64))],
      @[Expr(kind: exLiteral, litValue: newValueRef(3'i64))]
    ]
    let s = Stmt(kind: stmtInsert, intoTable: "t", intoValues: values)
    check s.intoValues.len == 3

  test "stmtUpdate basic":
    let sets = @[("name", Expr(kind: exLiteral, litValue: newValueRef("Bob")))]
    let s = Stmt(kind: stmtUpdate, updTable: "users", updSets: sets)
    check s.kind == stmtUpdate
    check s.updTable == "users"
    check s.updSets.len == 1
    check s.updSets[0].col == "name"

  test "stmtUpdate with WHERE":
    let whereExpr = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exColumn, colName: "id"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(1'i64)))
    let s = Stmt(kind: stmtUpdate, updTable: "t", updSets: @[],
                 updWhere: some(whereExpr))
    check s.updWhere.isSome

  test "stmtDelete basic":
    let s = Stmt(kind: stmtDelete, delTable: "logs")
    check s.kind == stmtDelete
    check s.delTable == "logs"

  test "stmtDelete with WHERE":
    let whereExpr = Expr(kind: exBinOp, binOp: boGt,
      binLeft: Expr(kind: exColumn, colName: "age"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(90'i64)))
    let s = Stmt(kind: stmtDelete, delTable: "t", delWhere: some(whereExpr))
    check s.delWhere.isSome

suite "AST Stmt Transactions":

  test "stmtBegin":
    let s = Stmt(kind: stmtBegin, beginReadOnly: false)
    check s.kind == stmtBegin
    check s.beginReadOnly == false

  test "stmtBegin READ ONLY":
    let s = Stmt(kind: stmtBegin, beginReadOnly: true)
    check s.beginReadOnly == true

  test "stmtCommit":
    let s = Stmt(kind: stmtCommit)
    check s.kind == stmtCommit

  test "stmtRollback":
    let s = Stmt(kind: stmtRollback)
    check s.kind == stmtRollback

suite "AST Stmt DDL Extended":

  test "stmtCreateDatabase":
    let s = Stmt(kind: stmtCreateDatabase, cdbName: "mydb",
        cdbIfNotExists: false)
    check s.kind == stmtCreateDatabase
    check s.cdbName == "mydb"
    check s.cdbIfNotExists == false

  test "stmtCreateDatabase with replicas":
    let s = Stmt(kind: stmtCreateDatabase, cdbName: "mydb", cdbReplicas: some(5))
    check s.cdbReplicas.isSome
    check s.cdbReplicas.get() == 5

  test "stmtDropDatabase":
    let s = Stmt(kind: stmtDropDatabase, ddbName: "mydb", ddbIfExists: true)
    check s.kind == stmtDropDatabase
    check s.ddbIfExists == true

  test "stmtCreateSchema":
    let s = Stmt(kind: stmtCreateSchema, csName: "reporting",
        csIfNotExists: true)
    check s.kind == stmtCreateSchema
    check s.csName == "reporting"

  test "stmtDropSchema":
    let s = Stmt(kind: stmtDropSchema, dsName: "old_schema")
    check s.kind == stmtDropSchema

  test "stmtCreateSpace":
    let s = Stmt(kind: stmtCreateSpace, csSpaceName: "space1",
        csSpaceReplicas: 3)
    check s.kind == stmtCreateSpace
    check s.csSpaceName == "space1"
    check s.csSpaceReplicas == 3

  test "stmtCreateSpace ALL replicas":
    let s = Stmt(kind: stmtCreateSpace, csSpaceName: "space1",
        csSpaceReplicas: 0)
    check s.csSpaceReplicas == 0

  test "stmtDropSpace":
    let s = Stmt(kind: stmtDropSpace, dsSpaceName: "old_space")
    check s.kind == stmtDropSpace

suite "AST Stmt SHOW/USE":

  test "stmtShowDatabases":
    let s = Stmt(kind: stmtShowDatabases)
    check s.kind == stmtShowDatabases

  test "stmtShowSchemas":
    let s = Stmt(kind: stmtShowSchemas, showSchemasDb: "mydb")
    check s.kind == stmtShowSchemas
    check s.showSchemasDb == "mydb"

  test "stmtShowTables":
    let s = Stmt(kind: stmtShowTables, showTablesDb: "mydb",
        showTablesSchema: "public")
    check s.kind == stmtShowTables
    check s.showTablesDb == "mydb"
    check s.showTablesSchema == "public"

  test "stmtShowSpaces":
    let s = Stmt(kind: stmtShowSpaces)
    check s.kind == stmtShowSpaces

  test "stmtUseDatabase":
    let s = Stmt(kind: stmtUseDatabase, useDbName: "mydb")
    check s.kind == stmtUseDatabase
    check s.useDbName == "mydb"

  test "stmtUseSchema":
    let s = Stmt(kind: stmtUseSchema, useSchemaName: "reporting")
    check s.kind == stmtUseSchema
    check s.useSchemaName == "reporting"

suite "AST Stmt EXPLAIN":

  test "stmtExplain":
    let inner = Stmt(kind: stmtSelect, selCols: @[], selFrom: "t")
    let s = Stmt(kind: stmtExplain, explainStmt: inner)
    check s.kind == stmtExplain
    check s.explainStmt.kind == stmtSelect
