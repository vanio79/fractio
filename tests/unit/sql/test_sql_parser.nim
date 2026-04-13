import std/[unittest, options]
import fractio/sql/parser
import fractio/sql/ast
import fractio/sql/lexer

suite "SQL Lexer":

  test "tokenises basic keywords":
    let toks = tokenize("SELECT * FROM users WHERE id = 1;")
    check toks[0].kind == tkSelect
    check toks[1].kind == tkStar
    check toks[2].kind == tkFrom
    check toks[3].kind == tkIdent
    check toks[3].value == "users"
    check toks[4].kind == tkWhere
    check toks[5].kind == tkIdent
    check toks[6].kind == tkEq
    check toks[7].kind == tkInt
    check toks[7].value == "1"

  test "tokenises string literal with escaped quote":
    let toks = tokenize("'it''s'")
    check toks[0].kind == tkString
    check toks[0].value == "it's"

  test "tokenises operators":
    let toks = tokenize("<= >= <> != < >")
    check toks[0].kind == tkLte
    check toks[1].kind == tkGte
    check toks[2].kind == tkNeq
    check toks[3].kind == tkNeq
    check toks[4].kind == tkLt
    check toks[5].kind == tkGt

  test "tokenises float":
    let toks = tokenize("3.14 1e10 2.5e-3")
    check toks[0].kind == tkFloat
    check toks[1].kind == tkFloat
    check toks[2].kind == tkFloat

  test "skips line comments":
    let toks = tokenize("SELECT -- this is a comment\n 1")
    check toks[0].kind == tkSelect
    check toks[1].kind == tkInt

  test "skips block comments":
    let toks = tokenize("SELECT /* block */ 1")
    check toks[0].kind == tkSelect
    check toks[1].kind == tkInt

suite "SQL Parser — DDL":

  test "CREATE TABLE basic":
    let s = parseStatement("""
      CREATE TABLE users (
        id      INT PRIMARY KEY,
        name    TEXT NOT NULL,
        email   TEXT UNIQUE,
        age     INT  DEFAULT 0,
        active  BOOLEAN
      )
    """)
    check s.kind == stmtCreateTable
    check s.ctTable == "users"
    check s.ctIfNotExists == false
    check s.ctColumns.len == 5
    check s.ctColumns[0].name == "id"
    check s.ctColumns[0].dataType == dtInt
    check s.ctColumns[0].primaryKey == true
    check s.ctColumns[1].notNull == true
    check s.ctColumns[2].unique == true
    check s.ctColumns[3].defaultExpr.isSome

  test "CREATE TABLE IF NOT EXISTS":
    let s = parseStatement("CREATE TABLE IF NOT EXISTS t (x INT)")
    check s.kind == stmtCreateTable
    check s.ctIfNotExists == true
    check s.ctTable == "t"

  test "CREATE TABLE with table-level PRIMARY KEY":
    let s = parseStatement("""
      CREATE TABLE orders (
        id    INT,
        item  TEXT,
        PRIMARY KEY (id)
      )
    """)
    check s.kind == stmtCreateTable
    check s.ctPrimaryKey == @["id"]

  test "DROP TABLE":
    let s = parseStatement("DROP TABLE products")
    check s.kind == stmtDropTable
    check s.dtTable == "products"
    check s.dtIfExists == false

  test "DROP TABLE IF EXISTS":
    let s = parseStatement("DROP TABLE IF EXISTS products")
    check s.kind == stmtDropTable
    check s.dtIfExists == true

suite "SQL Parser — DML: SELECT":

  test "SELECT star":
    let s = parseStatement("SELECT * FROM users")
    check s.kind == stmtSelect
    check s.selCols.len == 1
    check s.selCols[0].expr.kind == exStar
    check s.selFrom == "users"

  test "SELECT with WHERE":
    let s = parseStatement("SELECT id, name FROM users WHERE age > 18")
    check s.kind == stmtSelect
    check s.selCols.len == 2
    check s.selWhere.isSome
    let w = s.selWhere.get
    check w.kind == exBinOp
    check w.binOp == boGt

  test "SELECT DISTINCT":
    let s = parseStatement("SELECT DISTINCT email FROM users")
    check s.selDistinct == true

  test "SELECT with ORDER BY and LIMIT":
    let s = parseStatement("SELECT * FROM users ORDER BY name ASC LIMIT 10 OFFSET 5")
    check s.selOrderBy.len == 1
    check s.selOrderBy[0].desc == false
    check s.selLimit.isSome
    check s.selOffset.isSome

  test "SELECT with ORDER BY DESC":
    let s = parseStatement("SELECT * FROM t ORDER BY created_at DESC")
    check s.selOrderBy[0].desc == true

  test "SELECT with AND/OR in WHERE":
    let s = parseStatement("SELECT * FROM t WHERE a = 1 AND b = 2 OR c = 3")
    check s.selWhere.isSome

  test "SELECT with IS NULL":
    let s = parseStatement("SELECT * FROM t WHERE col IS NULL")
    let w = s.selWhere.get
    check w.kind == exIsNull
    check w.isNullNot == false

  test "SELECT with IS NOT NULL":
    let s = parseStatement("SELECT * FROM t WHERE col IS NOT NULL")
    let w = s.selWhere.get
    check w.isNullNot == true

  test "SELECT with IN list":
    let s = parseStatement("SELECT * FROM t WHERE id IN (1, 2, 3)")
    let w = s.selWhere.get
    check w.kind == exIn
    check w.inList.len == 3
    check w.inNot == false

  test "SELECT with NOT IN":
    let s = parseStatement("SELECT * FROM t WHERE status NOT IN ('active', 'pending')")
    let w = s.selWhere.get
    check w.kind == exIn
    check w.inNot == true

  test "SELECT with BETWEEN":
    let s = parseStatement("SELECT * FROM t WHERE age BETWEEN 18 AND 65")
    let w = s.selWhere.get
    check w.kind == exBetween

  test "SELECT with LIKE":
    let s = parseStatement("SELECT * FROM t WHERE name LIKE 'A%'")
    let w = s.selWhere.get
    check w.kind == exLike

  test "SELECT with table alias":
    let s = parseStatement("SELECT u.name FROM users u WHERE u.id = 1")
    check s.selFrom == "users"
    check s.selFromAlias == "u"

  test "SELECT with column alias":
    let s = parseStatement("SELECT id AS user_id FROM users")
    check s.selCols[0].alias == "user_id"

suite "SQL Parser — DML: INSERT":

  test "INSERT with column list":
    let s = parseStatement("INSERT INTO users (id, name) VALUES (1, 'Alice')")
    check s.kind == stmtInsert
    check s.intoTable == "users"
    check s.intoCols == @["id", "name"]
    check s.intoValues.len == 1
    check s.intoValues[0].len == 2

  test "INSERT without column list":
    let s = parseStatement("INSERT INTO t VALUES (42, 'hello', TRUE)")
    check s.intoCols.len == 0
    check s.intoValues[0].len == 3

  test "INSERT multi-row":
    let s = parseStatement("INSERT INTO t (x) VALUES (1), (2), (3)")
    check s.intoValues.len == 3

  test "INSERT with NULL":
    let s = parseStatement("INSERT INTO t (a, b) VALUES (NULL, 1)")
    check s.intoValues[0][0].kind == exLiteral
    check s.intoValues[0][0].litValue == nil

suite "SQL Parser — DML: UPDATE":

  test "UPDATE basic":
    let s = parseStatement("UPDATE users SET name = 'Bob' WHERE id = 1")
    check s.kind == stmtUpdate
    check s.updTable == "users"
    check s.updSets.len == 1
    check s.updSets[0].col == "name"
    check s.updWhere.isSome

  test "UPDATE multiple columns":
    let s = parseStatement("UPDATE users SET name = 'Bob', age = 30")
    check s.updSets.len == 2

  test "UPDATE without WHERE":
    let s = parseStatement("UPDATE t SET active = FALSE")
    check s.updWhere.isNone

suite "SQL Parser — DML: DELETE":

  test "DELETE with WHERE":
    let s = parseStatement("DELETE FROM users WHERE id = 99")
    check s.kind == stmtDelete
    check s.delTable == "users"
    check s.delWhere.isSome

  test "DELETE without WHERE":
    let s = parseStatement("DELETE FROM logs")
    check s.delWhere.isNone

suite "SQL Parser — Transactions":

  test "BEGIN":
    let s = parseStatement("BEGIN")
    check s.kind == stmtBegin
    check s.beginReadOnly == false

  test "BEGIN TRANSACTION":
    let s = parseStatement("BEGIN TRANSACTION")
    check s.kind == stmtBegin

  test "BEGIN WORK":
    let s = parseStatement("BEGIN WORK")
    check s.kind == stmtBegin

  test "COMMIT":
    let s = parseStatement("COMMIT")
    check s.kind == stmtCommit

  test "COMMIT TRANSACTION":
    let s = parseStatement("COMMIT TRANSACTION")
    check s.kind == stmtCommit

  test "ROLLBACK":
    let s = parseStatement("ROLLBACK")
    check s.kind == stmtRollback

  test "ROLLBACK WORK":
    let s = parseStatement("ROLLBACK WORK")
    check s.kind == stmtRollback

suite "SQL Parser — multi-statement":

  test "multiple statements separated by semicolons":
    let stmts = parseAll("BEGIN; INSERT INTO t (x) VALUES (1); COMMIT;")
    check stmts.len == 3
    check stmts[0].kind == stmtBegin
    check stmts[1].kind == stmtInsert
    check stmts[2].kind == stmtCommit

  test "handles trailing semicolons gracefully":
    let stmts = parseAll("COMMIT;;;")
    check stmts.len == 1

suite "SQL Parser — DDL: DATABASE":

  test "CREATE DATABASE":
    let s = parseStatement("CREATE DATABASE mydb")
    check s.kind == stmtCreateDatabase
    check s.cdbName == "mydb"
    check s.cdbIfNotExists == false

  test "CREATE DATABASE IF NOT EXISTS":
    let s = parseStatement("CREATE DATABASE IF NOT EXISTS mydb")
    check s.kind == stmtCreateDatabase
    check s.cdbName == "mydb"
    check s.cdbIfNotExists == true

  test "DROP DATABASE":
    let s = parseStatement("DROP DATABASE mydb")
    check s.kind == stmtDropDatabase
    check s.ddbName == "mydb"
    check s.ddbIfExists == false

  test "DROP DATABASE IF EXISTS":
    let s = parseStatement("DROP DATABASE IF EXISTS mydb")
    check s.kind == stmtDropDatabase
    check s.ddbName == "mydb"
    check s.ddbIfExists == true

suite "SQL Parser — DDL: SCHEMA":

  test "CREATE SCHEMA":
    let s = parseStatement("CREATE SCHEMA reporting")
    check s.kind == stmtCreateSchema
    check s.csName == "reporting"
    check s.csIfNotExists == false

  test "CREATE SCHEMA IF NOT EXISTS":
    let s = parseStatement("CREATE SCHEMA IF NOT EXISTS reporting")
    check s.kind == stmtCreateSchema
    check s.csName == "reporting"
    check s.csIfNotExists == true

  test "DROP SCHEMA":
    let s = parseStatement("DROP SCHEMA reporting")
    check s.kind == stmtDropSchema
    check s.dsName == "reporting"
    check s.dsIfExists == false

  test "DROP SCHEMA IF EXISTS":
    let s = parseStatement("DROP SCHEMA IF EXISTS reporting")
    check s.kind == stmtDropSchema
    check s.dsName == "reporting"
    check s.dsIfExists == true

  test "DATABASE and SCHEMA names are usable as identifiers in table DDL":
    let s = parseStatement("CREATE TABLE schema (database INT)")
    check s.kind == stmtCreateTable
    check s.ctTable == "schema"
    check s.ctColumns[0].name == "database"

suite "SQL Parser — WITH REPLICAS":

  test "CREATE DATABASE with replicas":
    let s = parseStatement("CREATE DATABASE mydb WITH REPLICAS = 5")
    check s.kind == stmtCreateDatabase
    check s.cdbName == "mydb"
    check s.cdbReplicas == some(5)

  test "CREATE DATABASE without replicas":
    let s = parseStatement("CREATE DATABASE mydb")
    check s.kind == stmtCreateDatabase
    check s.cdbReplicas.isNone

  test "CREATE DATABASE IF NOT EXISTS with replicas":
    let s = parseStatement("CREATE DATABASE IF NOT EXISTS mydb WITH REPLICAS = 3")
    check s.cdbIfNotExists == true
    check s.cdbName == "mydb"
    check s.cdbReplicas == some(3)

  test "CREATE SCHEMA with replicas":
    let s = parseStatement("CREATE SCHEMA reporting WITH REPLICAS = 7")
    check s.kind == stmtCreateSchema
    check s.csName == "reporting"
    check s.csReplicas == some(7)

  test "CREATE SCHEMA without replicas":
    let s = parseStatement("CREATE SCHEMA reporting")
    check s.csReplicas.isNone

  test "CREATE SCHEMA IF NOT EXISTS with replicas":
    let s = parseStatement("CREATE SCHEMA IF NOT EXISTS reporting WITH REPLICAS = 1")
    check s.csIfNotExists == true
    check s.csReplicas == some(1)

  test "CREATE TABLE with replicas":
    let s = parseStatement("""
      CREATE TABLE users (
        id   INT PRIMARY KEY,
        name TEXT
      ) WITH REPLICAS = 5
    """)
    check s.kind == stmtCreateTable
    check s.ctTable == "users"
    check s.ctColumns.len == 2
    check s.ctReplicas == some(5)

  test "CREATE TABLE without replicas":
    let s = parseStatement("CREATE TABLE t (x INT)")
    check s.ctReplicas.isNone

  test "CREATE TABLE IF NOT EXISTS with replicas":
    let s = parseStatement("CREATE TABLE IF NOT EXISTS t (x INT) WITH REPLICAS = 3")
    check s.ctIfNotExists == true
    check s.ctReplicas == some(3)

  test "REPLICAS = 1 is valid (single-replica)":
    let s = parseStatement("CREATE DATABASE dev WITH REPLICAS = 1")
    check s.cdbReplicas == some(1)

  test "'with' and 'replicas' can be used as identifiers":
    let s = parseStatement("CREATE TABLE with (replicas INT)")
    check s.ctTable == "with"
    check s.ctColumns[0].name == "replicas"

  test "replicas value must be >= 1":
    try:
      discard parseStatement("CREATE DATABASE bad WITH REPLICAS = 0")
      fail()
    except ParseError:
      discard

  test "replicas requires integer value":
    try:
      discard parseStatement("CREATE DATABASE bad WITH REPLICAS = abc")
      fail()
    except ParseError:
      discard

  test "WITH without REPLICAS raises error":
    try:
      discard parseStatement("CREATE DATABASE bad WITH SOMETHING = 3")
      fail()
    except ParseError:
      discard

suite "SQL Parser — error cases":

  test "raises ParseError on garbage input":
    try:
      discard parseStatement("GRBLX !!!!")
      fail()
    except ParseError:
      discard

  test "raises ParseError on incomplete CREATE TABLE":
    try:
      discard parseStatement("CREATE TABLE")
      fail()
    except ParseError:
      discard

suite "SQL Parser — SHOW Commands":

  test "SHOW DATABASES":
    let s = parseStatement("SHOW DATABASES")
    check s.kind == stmtShowDatabases

  test "SHOW SCHEMAS":
    let s = parseStatement("SHOW SCHEMAS")
    check s.kind == stmtShowSchemas
    check s.showSchemasDb == ""

  test "SHOW SCHEMAS IN database":
    let s = parseStatement("SHOW SCHEMAS IN mydb")
    check s.kind == stmtShowSchemas
    check s.showSchemasDb == "mydb"

  test "SHOW TABLES":
    let s = parseStatement("SHOW TABLES")
    check s.kind == stmtShowTables
    check s.showTablesDb == ""
    check s.showTablesSchema == ""

  test "SHOW TABLES IN schema":
    let s = parseStatement("SHOW TABLES IN public")
    check s.kind == stmtShowTables
    check s.showTablesDb == ""
    check s.showTablesSchema == "public"

  test "SHOW TABLES IN database.schema":
    let s = parseStatement("SHOW TABLES IN mydb.public")
    check s.kind == stmtShowTables
    check s.showTablesDb == "mydb"
    check s.showTablesSchema == "public"

  test "SHOW SPACES":
    let s = parseStatement("SHOW SPACES")
    check s.kind == stmtShowSpaces

suite "SQL Parser — USE Commands":

  test "USE DATABASE name":
    let s = parseStatement("USE DATABASE mydb")
    check s.kind == stmtUseDatabase
    check s.useDbName == "mydb"

  test "USE SCHEMA name":
    let s = parseStatement("USE SCHEMA public")
    check s.kind == stmtUseSchema
    check s.useSchemaName == "public"

  test "USE bare name defaults to database":
    let s = parseStatement("USE mydb")
    check s.kind == stmtUseDatabase
    check s.useDbName == "mydb"

suite "SQL Parser — EXPLAIN":

  test "EXPLAIN SELECT":
    let s = parseStatement("EXPLAIN SELECT * FROM users")
    check s.kind == stmtExplain
    check s.explainStmt.kind == stmtSelect

  test "EXPLAIN INSERT":
    let s = parseStatement("EXPLAIN INSERT INTO t VALUES (1)")
    check s.kind == stmtExplain
    check s.explainStmt.kind == stmtInsert

  test "EXPLAIN nested":
    let inner = parseStatement("EXPLAIN SELECT id FROM users WHERE id > 5")
    check inner.kind == stmtExplain
    check inner.explainStmt.selCols.len == 1

suite "SQL Parser — SPACE Commands":

  test "CREATE SPACE with replicas":
    let s = parseStatement("CREATE SPACE myspace WITH REPLICAS = 3")
    check s.kind == stmtCreateSpace
    check s.csSpaceName == "myspace"
    check s.csSpaceReplicas == 3

  test "CREATE SPACE with ALL replicas":
    let s = parseStatement("CREATE SPACE global WITH REPLICAS = ALL")
    check s.kind == stmtCreateSpace
    check s.csSpaceName == "global"
    check s.csSpaceReplicas == 0 # 0 means ALL

  test "DROP SPACE":
    let s = parseStatement("DROP SPACE myspace")
    check s.kind == stmtDropSpace
    check s.dsSpaceName == "myspace"

  test "CREATE SPACE replicas minimum":
    try:
      discard parseStatement("CREATE SPACE bad WITH REPLICAS = 0")
      fail()
    except ParseError:
      discard

suite "SQL Parser — parseAll Multi-Statement":

  test "parseAll multiple statements":
    let stmts = parseAll("SELECT * FROM t; INSERT INTO t VALUES (2); COMMIT;")
    check stmts.len == 3
    check stmts[0].kind == stmtSelect
    check stmts[1].kind == stmtInsert
    check stmts[2].kind == stmtCommit

  test "parseAll handles trailing semicolons":
    let stmts = parseAll("SELECT * FROM t;; ;")
    check stmts.len == 1

  test "parseAll empty input":
    let stmts = parseAll("")
    check stmts.len == 0

  test "parseAll only semicolons":
    let stmts = parseAll(";; ; ;")
    check stmts.len == 0

suite "SQL Parser — READ ONLY Transaction":

  test "BEGIN READ ONLY":
    let s = parseStatement("BEGIN READ ONLY")
    check s.kind == stmtBegin
    check s.beginReadOnly == true

  test "BEGIN TRANSACTION READ ONLY":
    let s = parseStatement("BEGIN TRANSACTION READ ONLY")
    check s.kind == stmtBegin
    check s.beginReadOnly == true

suite "SQL Parser — IN SPACE Clause":

  test "CREATE TABLE IN SPACE":
    let s = parseStatement("CREATE TABLE users (id INT) IN SPACE myspace")
    check s.kind == stmtCreateTable
    check s.ctTable == "users"
    check s.ctSpaceName == some("myspace")

suite "SQL Parser — Error Recovery":

  test "SHOW without target raises error":
    try:
      discard parseStatement("SHOW")
      fail()
    except ParseError:
      discard

  test "USE without name raises error":
    try:
      discard parseStatement("USE")
      fail()
    except ParseError:
      discard

  test "CREATE SPACE without REPLICAS raises error":
    try:
      discard parseStatement("CREATE SPACE myspace WITH SOMETHING = 3")
      fail()
    except ParseError:
      discard
