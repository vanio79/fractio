import std/[unittest, options]
import fractio/sql/parser
import fractio/sql/ast
import fractio/sql/lexer

suite "Parser Edge Cases - Expression Parsing":

  test "nested parentheses":
    let s = parseStatement("SELECT * FROM t WHERE ((a = 1) AND (b = 2))")
    check s.kind == stmtSelect
    let w = s.selWhere.get()
    check w.kind == exBinOp
    check w.binOp == boAnd

  test "deeply nested binary operators":
    let s = parseStatement("SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3 AND d = 4")
    check s.kind == stmtSelect
    check s.selWhere.isSome

  test "operator precedence - AND before OR":
    let s = parseStatement("SELECT * FROM t WHERE a OR b AND c")
    let w = s.selWhere.get()
    check w.kind == exBinOp
    check w.binOp == boOr

  test "operator precedence - comparison before AND":
    let s = parseStatement("SELECT * FROM t WHERE a = 1 AND b = 2")
    let w = s.selWhere.get()
    check w.kind == exBinOp
    check w.binOp == boAnd

  test "operator precedence - arithmetic before comparison":
    let s = parseStatement("SELECT * FROM t WHERE a + 1 > 5")
    let w = s.selWhere.get()
    check w.kind == exBinOp
    check w.binOp == boGt
    check w.binLeft.kind == exBinOp
    check w.binLeft.binOp == boAdd

  test "parentheses override precedence":
    let s = parseStatement("SELECT * FROM t WHERE (a OR b) AND c")
    let w = s.selWhere.get()
    check w.kind == exBinOp
    check w.binOp == boAnd
    check w.binLeft.kind == exBinOp
    check w.binLeft.binOp == boOr

  test "arithmetic precedence - multiplication before addition":
    let s = parseStatement("SELECT a + b * c FROM t")
    check s.selCols[0].expr.kind == exBinOp
    check s.selCols[0].expr.binOp == boAdd

  test "arithmetic precedence - multiplication before subtraction":
    let s = parseStatement("SELECT a - b * c FROM t")
    check s.selCols[0].expr.kind == exBinOp
    check s.selCols[0].expr.binOp == boSub

  test "mixed arithmetic and comparison":
    let s = parseStatement("SELECT * FROM t WHERE a * 2 = b + 1")
    let w = s.selWhere.get()
    check w.kind == exBinOp
    check w.binOp == boEq

  test "unary NOT with parentheses":
    let s = parseStatement("SELECT * FROM t WHERE NOT (a = 1)")
    let w = s.selWhere.get()
    check w.kind == exUnaryOp
    check w.unaryOp == uoNot

  test "double negation":
    let s = parseStatement("SELECT - -5 FROM t")
    let expr = s.selCols[0].expr
    check expr.kind == exUnaryOp
    check expr.unaryOp == uoNeg

  test "negation of expression":
    let s = parseStatement("SELECT -(a + b) FROM t")
    let expr = s.selCols[0].expr
    check expr.kind == exUnaryOp
    check expr.unaryOp == uoNeg
    check expr.unaryExpr.kind == exBinOp

suite "Parser Edge Cases - Column References":

  test "qualified column reference":
    let s = parseStatement("SELECT u.id FROM users u")
    check s.selCols[0].expr.kind == exColumn
    check s.selCols[0].expr.colTable == "u"
    check s.selCols[0].expr.colName == "id"

  test "qualified column in WHERE":
    let s = parseStatement("SELECT * FROM users u WHERE u.id = 1")
    let w = s.selWhere.get()
    check w.binLeft.kind == exColumn
    check w.binLeft.colTable == "u"
    check w.binLeft.colName == "id"

  test "multiple qualified columns":
    let s = parseStatement("SELECT u.id, u.name FROM users u")
    check s.selCols[0].expr.colTable == "u"
    check s.selCols[1].expr.colTable == "u"

suite "Parser Edge Cases - String Literals":

  test "empty string":
    let s = parseStatement("INSERT INTO t (x) VALUES ('')")
    check s.intoValues[0][0].kind == exLiteral
    check s.intoValues[0][0].litValue.strValue == ""

  test "string with special characters":
    let s = parseStatement("INSERT INTO t (x) VALUES ('hello!@#$%^&*()')")
    check s.intoValues[0][0].litValue.strValue == "hello!@#$%^&*()"

  test "string with unicode":
    let s = parseStatement("INSERT INTO t (x) VALUES ('日本語')")
    check s.intoValues[0][0].kind == exLiteral

  test "escaped quotes multiple":
    let s = parseStatement("INSERT INTO t (x) VALUES ('a''b''c')")
    check s.intoValues[0][0].litValue.strValue == "a'b'c"

suite "Parser Edge Cases - Numbers":

  test "zero integer":
    let s = parseStatement("INSERT INTO t (x) VALUES (0)")
    check s.intoValues[0][0].litValue.intValue == 0

  test "negative integer":
    let s = parseStatement("INSERT INTO t (x) VALUES (-42)")
    check s.intoValues[0][0].kind == exUnaryOp
    check s.intoValues[0][0].unaryOp == uoNeg

  test "float with many decimals":
    let s = parseStatement("INSERT INTO t (x) VALUES (3.141592653589793)")
    check s.intoValues[0][0].litValue.floatValue == 3.141592653589793

  test "large integer":
    let s = parseStatement("INSERT INTO t (x) VALUES (9223372036854775807)")
    check s.intoValues[0][0].litValue.intValue == 9223372036854775807

suite "Parser Edge Cases - NULL Handling":

  test "multiple NULLs in insert":
    let s = parseStatement("INSERT INTO t (a, b, c) VALUES (NULL, NULL, NULL)")
    check s.intoValues[0][0].litValue == nil
    check s.intoValues[0][1].litValue == nil
    check s.intoValues[0][2].litValue == nil

  test "NULL in comparison":
    let s = parseStatement("SELECT * FROM t WHERE x = NULL")
    let w = s.selWhere.get()
    check w.binRight.kind == exLiteral
    check w.binRight.litValue == nil

suite "Parser Edge Cases - Boolean Values":

  test "TRUE in expression":
    let s = parseStatement("SELECT * FROM t WHERE active = TRUE")
    let w = s.selWhere.get()
    check w.binRight.kind == exLiteral
    check w.binRight.litValue.boolValue == true

  test "FALSE in expression":
    let s = parseStatement("SELECT * FROM t WHERE active = FALSE")
    let w = s.selWhere.get()
    check w.binRight.litValue.boolValue == false

  test "TRUE in insert":
    let s = parseStatement("INSERT INTO t (x) VALUES (TRUE)")
    check s.intoValues[0][0].litValue.boolValue == true

suite "Parser Edge Cases - IN Clause":

  test "IN with single value":
    let s = parseStatement("SELECT * FROM t WHERE x IN (1)")
    let w = s.selWhere.get()
    check w.kind == exIn
    check w.inList.len == 1

  test "IN with many values":
    let s = parseStatement("SELECT * FROM t WHERE x IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)")
    let w = s.selWhere.get()
    check w.inList.len == 10

  test "IN with strings":
    let s = parseStatement("SELECT * FROM t WHERE x IN ('a', 'b', 'c')")
    let w = s.selWhere.get()
    check w.inList.len == 3
    check w.inList[0].litValue.strValue == "a"

  test "IN with mixed types":
    let s = parseStatement("SELECT * FROM t WHERE x IN (1, 'two', TRUE)")
    let w = s.selWhere.get()
    check w.inList.len == 3

  test "empty IN list":
    let s = parseStatement("SELECT * FROM t WHERE x IN ()")
    let w = s.selWhere.get()
    check w.kind == exIn
    check w.inList.len == 0

suite "Parser Edge Cases - BETWEEN":

  test "BETWEEN with integers":
    let s = parseStatement("SELECT * FROM t WHERE x BETWEEN 1 AND 100")
    let w = s.selWhere.get()
    check w.kind == exBetween
    check w.betweenLo.litValue.intValue == 1
    check w.betweenHi.litValue.intValue == 100

  test "BETWEEN with strings":
    let s = parseStatement("SELECT * FROM t WHERE x BETWEEN 'a' AND 'z'")
    let w = s.selWhere.get()
    check w.betweenLo.litValue.strValue == "a"
    check w.betweenHi.litValue.strValue == "z"

suite "Parser Edge Cases - LIKE":

  test "LIKE exact pattern":
    let s = parseStatement("SELECT * FROM t WHERE x LIKE 'exact'")
    let w = s.selWhere.get()
    check w.kind == exLike
    check w.likePattern.litValue.strValue == "exact"

  test "LIKE with multiple wildcards":
    let s = parseStatement("SELECT * FROM t WHERE x LIKE '%test%pattern%'")
    let w = s.selWhere.get()
    check w.likePattern.litValue.strValue == "%test%pattern%"

suite "Parser Edge Cases - ORDER BY":

  test "ORDER BY multiple columns mixed ASC/DESC":
    let s = parseStatement("SELECT * FROM t ORDER BY a ASC, b DESC, c ASC")
    check s.selOrderBy.len == 3
    check s.selOrderBy[0].desc == false
    check s.selOrderBy[1].desc == true
    check s.selOrderBy[2].desc == false

  test "ORDER BY with expression":
    let s = parseStatement("SELECT * FROM t ORDER BY a + b DESC")
    check s.selOrderBy[0].expr.kind == exBinOp

  test "ORDER BY default ASC":
    let s = parseStatement("SELECT * FROM t ORDER BY a")
    check s.selOrderBy[0].desc == false

suite "Parser Edge Cases - LIMIT/OFFSET":

  test "LIMIT zero":
    let s = parseStatement("SELECT * FROM t LIMIT 0")
    check s.selLimit.get().litValue.intValue == 0

  test "OFFSET zero":
    let s = parseStatement("SELECT * FROM t OFFSET 0")
    check s.selOffset.get().litValue.intValue == 0

  test "large LIMIT":
    let s = parseStatement("SELECT * FROM t LIMIT 1000000")
    check s.selLimit.get().litValue.intValue == 1000000

suite "Parser Edge Cases - CREATE TABLE":

  test "CREATE TABLE with single column":
    let s = parseStatement("CREATE TABLE t (x INT)")
    check s.ctColumns.len == 1

  test "CREATE TABLE with many columns":
    var sql = "CREATE TABLE t ("
    for i in 1..50:
      if i > 1: sql.add(", ")
      sql.add("col" & $i & " INT")
    sql.add(")")
    let s = parseStatement(sql)
    check s.ctColumns.len == 50

  test "CREATE TABLE all constraint types on one column":
    let s = parseStatement("CREATE TABLE t (x INT PRIMARY KEY NOT NULL UNIQUE)")
    check s.ctColumns[0].primaryKey == true
    check s.ctColumns[0].notNull == true
    check s.ctColumns[0].unique == true

  test "CREATE TABLE with default expression":
    let s = parseStatement("CREATE TABLE t (x INT DEFAULT 42)")
    check s.ctColumns[0].defaultExpr.isSome
    check s.ctColumns[0].defaultExpr.get().litValue.intValue == 42

  test "CREATE TABLE with default string":
    let s = parseStatement("CREATE TABLE t (x TEXT DEFAULT 'default')")
    check s.ctColumns[0].defaultExpr.get().litValue.strValue == "default"

  test "CREATE TABLE with default bool":
    let s = parseStatement("CREATE TABLE t (x BOOL DEFAULT TRUE)")
    check s.ctColumns[0].defaultExpr.get().litValue.boolValue == true

  test "CREATE TABLE multi-column primary key":
    let s = parseStatement("CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b))")
    check s.ctPrimaryKey.len == 2
    check s.ctPrimaryKey == @["a", "b"]

  test "CREATE TABLE VARCHAR with length":
    let s = parseStatement("CREATE TABLE t (x VARCHAR(255))")
    check s.ctColumns[0].dataType == dtString
    check s.ctColumns[0].maxLen == 255

  test "CREATE TABLE all data types":
    let s = parseStatement("""
      CREATE TABLE t (
        a INT, b FLOAT, c TEXT, d BOOL, e DATE, f DATETIME, g BLOB
      )
    """)
    check s.ctColumns[0].dataType == dtInt
    check s.ctColumns[1].dataType == dtFloat
    check s.ctColumns[2].dataType == dtString
    check s.ctColumns[3].dataType == dtBool
    check s.ctColumns[4].dataType == dtDate
    check s.ctColumns[5].dataType == dtDateTime
    check s.ctColumns[6].dataType == dtBytes

suite "Parser Edge Cases - INSERT":

  test "INSERT empty column list not supported":
    try:
      discard parseStatement("INSERT INTO t () VALUES ()")
      fail()
    except ParseError:
      discard

  test "INSERT many rows":
    var sql = "INSERT INTO t (x) VALUES "
    for i in 1..100:
      if i > 1: sql.add(", ")
      sql.add("(" & $i & ")")
    let s = parseStatement(sql)
    check s.intoValues.len == 100

  test "INSERT row with many columns":
    var sql = "INSERT INTO t ("
    for i in 1..20:
      if i > 1: sql.add(", ")
      sql.add("col" & $i)
    sql.add(") VALUES (")
    for i in 1..20:
      if i > 1: sql.add(", ")
      sql.add($i)
    sql.add(")")
    let s = parseStatement(sql)
    check s.intoCols.len == 20
    check s.intoValues[0].len == 20

suite "Parser Edge Cases - UPDATE":

  test "UPDATE all columns":
    var sql = "UPDATE t SET "
    for i in 1..10:
      if i > 1: sql.add(", ")
      sql.add("col" & $i & " = " & $i)
    let s = parseStatement(sql)
    check s.updSets.len == 10

  test "UPDATE with complex expression":
    let s = parseStatement("UPDATE t SET x = a + b * c")
    check s.updSets[0].val.kind == exBinOp

suite "Parser Edge Cases - DELETE":

  test "DELETE with complex WHERE":
    let s = parseStatement("DELETE FROM t WHERE a > 1 AND b < 2 AND c = 3")
    let w = s.delWhere.get()
    check w.kind == exBinOp
    check w.binOp == boAnd

suite "Parser Edge Cases - Multi-statement":

  test "parseAll empty string":
    let stmts = parseAll("")
    check stmts.len == 0

  test "parseAll only whitespace":
    let stmts = parseAll("   \t\n  ")
    check stmts.len == 0

  test "parseAll only comments":
    let stmts = parseAll("-- comment\n/* block */")
    check stmts.len == 0

  test "parseAll with embedded comments":
    let stmts = parseAll("SELECT 1 /* comment */ FROM t; -- trailing\nINSERT INTO t VALUES (2);")
    check stmts.len == 2
    check stmts[0].kind == stmtSelect
    check stmts[1].kind == stmtInsert

suite "Parser Error Cases - Invalid Syntax":

  test "missing table name":
    try:
      discard parseStatement("CREATE TABLE")
      fail()
    except ParseError:
      discard

  test "missing column list":
    try:
      discard parseStatement("CREATE TABLE t")
      fail()
    except ParseError:
      discard

  test "unclosed parentheses":
    try:
      discard parseStatement("SELECT * FROM t WHERE (a = 1")
      fail()
    except ParseError:
      discard

  test "missing VALUES keyword":
    try:
      discard parseStatement("INSERT INTO t (1, 2)")
      fail()
    except ParseError:
      discard

  test "missing FROM keyword":
    try:
      discard parseStatement("SELECT *")
      fail()
    except ParseError:
      discard

  test "invalid operator produces error token":
    let toks = tokenize("@#$")
    check toks[0].kind == tkError

  test "missing semicolon causes parse error":
    try:
      discard parseAll("SELECT 1 INSERT 2")
      fail()
    except ParseError:
      discard

suite "Parser Error Cases - Type Errors":

  test "REPLICAS negative":
    try:
      discard parseStatement("CREATE DATABASE d WITH REPLICAS = -1")
      fail()
    except ParseError:
      discard

suite "Parser Keyword Reuse":

  test "keywords as identifiers in column definitions":
    let s = parseStatement("CREATE TABLE t (select INT, from INT, where INT)")
    check s.ctColumns[0].name == "select"
    check s.ctColumns[1].name == "from"
    check s.ctColumns[2].name == "where"

  test "keywords as table name":
    let s = parseStatement("CREATE TABLE select (x INT)")
    check s.ctTable == "select"

  test "keywords in SHOW":
    let s = parseStatement("SHOW SCHEMAS IN database")
    check s.showSchemasDb == "database"

suite "Parser EXPLAIN":

  test "EXPLAIN SELECT":
    let s = parseStatement("EXPLAIN SELECT * FROM t")
    check s.kind == stmtExplain
    check s.explainStmt.kind == stmtSelect

  test "EXPLAIN INSERT":
    let s = parseStatement("EXPLAIN INSERT INTO t VALUES (1)")
    check s.kind == stmtExplain
    check s.explainStmt.kind == stmtInsert

  test "EXPLAIN nested":
    let inner = parseStatement("SELECT * FROM t")
    let s = Stmt(kind: stmtExplain, explainStmt: inner)
    check s.explainStmt.selFrom == "t"
