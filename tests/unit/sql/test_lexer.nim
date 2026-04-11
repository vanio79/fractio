import std/[unittest, strutils]
import fractio/sql/lexer

suite "Lexer Token Types":

  test "TokenKind enum values":
    check tkInt.ord >= 0
    check tkFloat.ord >= 0
    check tkString.ord >= 0
    check tkIdent.ord >= 0
    check tkEOF.ord >= 0
    check tkError.ord >= 0

suite "Lexer Basic Tokenization":

  test "empty input":
    let toks = tokenize("")
    check toks.len == 1
    check toks[0].kind == tkEOF

  test "whitespace only":
    let toks = tokenize("   \t\n\r  ")
    check toks.len == 1
    check toks[0].kind == tkEOF

  test "single integer":
    let toks = tokenize("42")
    check toks.len == 2
    check toks[0].kind == tkInt
    check toks[0].value == "42"
    check toks[1].kind == tkEOF

  test "integer with leading zeros":
    let toks = tokenize("007")
    check toks[0].kind == tkInt
    check toks[0].value == "007"

  test "large integer":
    let toks = tokenize("9223372036854775807")
    check toks[0].kind == tkInt
    check toks[0].value == "9223372036854775807"

  test "negative number handled as operator + number":
    let toks = tokenize("-42")
    check toks[0].kind == tkMinus
    check toks[1].kind == tkInt
    check toks[1].value == "42"

suite "Lexer Float Tokenization":

  test "simple float":
    let toks = tokenize("3.14")
    check toks[0].kind == tkFloat
    check toks[0].value == "3.14"

  test "float with trailing zeros":
    let toks = tokenize("1.00")
    check toks[0].kind == tkFloat
    check toks[0].value == "1.00"

  test "float scientific notation e":
    let toks = tokenize("1e10")
    check toks[0].kind == tkFloat
    check toks[0].value == "1e10"

  test "float scientific notation E":
    let toks = tokenize("1E10")
    check toks[0].kind == tkFloat
    check toks[0].value == "1E10"

  test "float scientific with sign":
    let toks = tokenize("2.5e-3")
    check toks[0].kind == tkFloat
    check toks[0].value == "2.5e-3"

  test "float scientific positive sign":
    let toks = tokenize("1.5e+3")
    check toks[0].kind == tkFloat
    check toks[0].value == "1.5e+3"

  test "multiple floats":
    let toks = tokenize("3.14 2.71 1.41")
    check toks.len == 4
    check toks[0].value == "3.14"
    check toks[1].value == "2.71"
    check toks[2].value == "1.41"

suite "Lexer String Tokenization":

  test "simple string":
    let toks = tokenize("'hello'")
    check toks[0].kind == tkString
    check toks[0].value == "hello"

  test "empty string":
    let toks = tokenize("''")
    check toks[0].kind == tkString
    check toks[0].value == ""

  test "string with spaces":
    let toks = tokenize("'hello world'")
    check toks[0].value == "hello world"

  test "escaped single quote":
    let toks = tokenize("'it''s'")
    check toks[0].kind == tkString
    check toks[0].value == "it's"

  test "string with multiple escaped quotes":
    let toks = tokenize("'a''b''c'")
    check toks[0].value == "a'b'c"

  test "unterminated string returns error":
    let toks = tokenize("'hello")
    check toks[0].kind == tkError
    check "unterminated" in toks[0].value

suite "Lexer Identifiers":

  test "simple identifier":
    let toks = tokenize("users")
    check toks[0].kind == tkIdent
    check toks[0].value == "users"

  test "identifier with underscore":
    let toks = tokenize("user_id")
    check toks[0].kind == tkIdent
    check toks[0].value == "user_id"

  test "identifier starting with underscore":
    let toks = tokenize("_private")
    check toks[0].kind == tkIdent
    check toks[0].value == "_private"

  test "case preserved in identifier":
    let toks = tokenize("MyTable")
    check toks[0].value == "MyTable"

suite "Lexer Quoted Identifiers":

  test "double-quoted identifier":
    let toks = tokenize("\"myColumn\"")
    check toks[0].kind == tkIdent
    check toks[0].value == "myColumn"

  test "double-quoted with escaped quote":
    let toks = tokenize("\"col\"\"name\"")
    check toks[0].value == "col\"name"

  test "MySQL backtick identifier":
    let toks = tokenize("`myColumn`")
    check toks[0].kind == tkIdent
    check toks[0].value == "myColumn"

  test "quoted identifier allows keywords as names":
    let toks = tokenize("\"select\"")
    check toks[0].kind == tkIdent
    check toks[0].value == "select"

  test "unterminated quoted identifier error":
    let toks = tokenize("\"open")
    check toks[0].kind == tkError

suite "Lexer Keywords":

  test "DDL keywords":
    check tokenize("CREATE")[0].kind == tkCreate
    check tokenize("DROP")[0].kind == tkDrop
    check tokenize("TABLE")[0].kind == tkTable
    check tokenize("DATABASE")[0].kind == tkDatabase
    check tokenize("SCHEMA")[0].kind == tkSchema

  test "DML keywords":
    check tokenize("SELECT")[0].kind == tkSelect
    check tokenize("INSERT")[0].kind == tkInsert
    check tokenize("UPDATE")[0].kind == tkUpdate
    check tokenize("DELETE")[0].kind == tkDelete
    check tokenize("VALUES")[0].kind == tkValues
    check tokenize("SET")[0].kind == tkSet

  test "clause keywords":
    check tokenize("FROM")[0].kind == tkFrom
    check tokenize("WHERE")[0].kind == tkWhere
    check tokenize("ORDER")[0].kind == tkOrder
    check tokenize("BY")[0].kind == tkBy
    check tokenize("LIMIT")[0].kind == tkLimit
    check tokenize("OFFSET")[0].kind == tkOffset

  test "logical keywords":
    check tokenize("AND")[0].kind == tkAnd
    check tokenize("OR")[0].kind == tkOr
    check tokenize("NOT")[0].kind == tkNot
    check tokenize("IN")[0].kind == tkIn
    check tokenize("IS")[0].kind == tkIs
    check tokenize("BETWEEN")[0].kind == tkBetween
    check tokenize("LIKE")[0].kind == tkLike

  test "ordering keywords":
    check tokenize("ASC")[0].kind == tkAsc
    check tokenize("DESC")[0].kind == tkDesc

  test "transaction keywords":
    check tokenize("BEGIN")[0].kind == tkBegin
    check tokenize("COMMIT")[0].kind == tkCommit
    check tokenize("ROLLBACK")[0].kind == tkRollback
    check tokenize("TRANSACTION")[0].kind == tkTransaction
    check tokenize("WORK")[0].kind == tkWork

  test "SHOW/USE keywords":
    check tokenize("SHOW")[0].kind == tkShow
    check tokenize("USE")[0].kind == tkUse
    check tokenize("DATABASES")[0].kind == tkDatabases
    check tokenize("SCHEMAS")[0].kind == tkSchemas
    check tokenize("TABLES")[0].kind == tkTables

  test "space keywords":
    check tokenize("SPACE")[0].kind == tkSpace
    check tokenize("SPACES")[0].kind == tkSpaces

  test "misc keywords":
    check tokenize("DISTINCT")[0].kind == tkDistinct
    check tokenize("ALL")[0].kind == tkAll
    check tokenize("EXPLAIN")[0].kind == tkExplain
    check tokenize("WITH")[0].kind == tkWith

  test "keywords are case-insensitive":
    check tokenize("select")[0].kind == tkSelect
    check tokenize("SELECT")[0].kind == tkSelect
    check tokenize("SeLeCt")[0].kind == tkSelect

  test "literal keywords":
    check tokenize("NULL")[0].kind == tkNull
    check tokenize("TRUE")[0].kind == tkTrue
    check tokenize("FALSE")[0].kind == tkFalse

suite "Lexer Data Type Keywords":

  test "integer type aliases":
    check tokenize("INT")[0].kind == tkTkInt
    check tokenize("INTEGER")[0].kind == tkTkInt
    check tokenize("BIGINT")[0].kind == tkTkInt
    check tokenize("SMALLINT")[0].kind == tkTkInt
    check tokenize("TINYINT")[0].kind == tkTkInt

  test "float type aliases":
    check tokenize("FLOAT")[0].kind == tkTkFloat
    check tokenize("DOUBLE")[0].kind == tkTkFloat
    check tokenize("REAL")[0].kind == tkTkFloat
    check tokenize("NUMERIC")[0].kind == tkTkFloat
    check tokenize("DECIMAL")[0].kind == tkTkFloat

  test "text type aliases":
    check tokenize("TEXT")[0].kind == tkTkText
    check tokenize("VARCHAR")[0].kind == tkTkText
    check tokenize("CHAR")[0].kind == tkTkText
    check tokenize("STRING")[0].kind == tkTkText

  test "bool type aliases":
    check tokenize("BOOLEAN")[0].kind == tkTkBool
    check tokenize("BOOL")[0].kind == tkTkBool

  test "date/time types":
    check tokenize("DATE")[0].kind == tkTkDate
    check tokenize("DATETIME")[0].kind == tkTkDateTime
    check tokenize("TIMESTAMP")[0].kind == tkTkDateTime

  test "bytes type aliases":
    check tokenize("BLOB")[0].kind == tkTkBytes
    check tokenize("BYTES")[0].kind == tkTkBytes
    check tokenize("BYTEA")[0].kind == tkTkBytes
    check tokenize("BINARY")[0].kind == tkTkBytes

suite "Lexer Operators":

  test "comparison operators":
    let toks = tokenize("= <> != < <= > >=")
    check toks[0].kind == tkEq
    check toks[1].kind == tkNeq
    check toks[2].kind == tkNeq
    check toks[3].kind == tkLt
    check toks[4].kind == tkLte
    check toks[5].kind == tkGt
    check toks[6].kind == tkGte

  test "arithmetic operators":
    let toks = tokenize("+ - * / %")
    check toks[0].kind == tkPlus
    check toks[1].kind == tkMinus
    check toks[2].kind == tkStar
    check toks[3].kind == tkSlash
    check toks[4].kind == tkPercent

  test "punctuation":
    let toks = tokenize(", . ; ( )")
    check toks[0].kind == tkComma
    check toks[1].kind == tkDot
    check toks[2].kind == tkSemicolon
    check toks[3].kind == tkLParen
    check toks[4].kind == tkRParen

  test "neq operators equivalent":
    let toks1 = tokenize("<>")
    let toks2 = tokenize("!=")
    check toks1[0].kind == tkNeq
    check toks2[0].kind == tkNeq

suite "Lexer Position Tracking":

  test "line and column tracking":
    let toks = tokenize("SELECT\n  FROM")
    check toks[0].line == 1
    check toks[0].col == 1
    check toks[1].kind == tkFrom
    check toks[2].kind == tkEOF

  test "column increments":
    let toks = tokenize("a b c")
    check toks[0].col == 1
    check toks[1].col == 3
    check toks[2].col == 5

  test "line resets column":
    let toks = tokenize("a\nb")
    check toks[0].line == 1
    check toks[0].col == 1
    check toks[1].line == 2
    check toks[1].col == 1

suite "Lexer Comments":

  test "single-line comment":
    let toks = tokenize("SELECT -- comment\n1")
    check toks.len == 3
    check toks[0].kind == tkSelect
    check toks[1].kind == tkInt
    check toks[2].kind == tkEOF

  test "single-line comment at end":
    let toks = tokenize("1 -- trailing comment")
    check toks.len == 2
    check toks[0].kind == tkInt

  test "block comment":
    let toks = tokenize("SELECT /* comment */ 1")
    check toks.len == 3
    check toks[0].kind == tkSelect
    check toks[1].kind == tkInt

  test "multi-line block comment":
    let toks = tokenize("SELECT /* line1\nline2 */ 1")
    check toks[0].kind == tkSelect
    check toks[1].kind == tkInt

  test "nested content in block comment":
    let toks = tokenize("/* SELECT INSERT */ 1")
    check toks[0].kind == tkInt

suite "Lexer Error Handling":

  test "invalid character":
    let toks = tokenize("@#$")
    check toks[0].kind == tkError

  test "standalone exclamation is error":
    let toks = tokenize("!")
    check toks[0].kind == tkError

  test "unterminated block comment":
    let toks = tokenize("SELECT /* open")
    check toks[0].kind == tkSelect
    check toks[1].kind == tkEOF

suite "Lexer Complex Inputs":

  test "SQL statement":
    let toks = tokenize("SELECT id, name FROM users WHERE age > 18;")
    check toks[0].kind == tkSelect
    check toks[1].kind == tkIdent
    check toks[2].kind == tkComma
    check toks[3].kind == tkIdent
    check toks[4].kind == tkFrom
    check toks[5].kind == tkIdent
    check toks[6].kind == tkWhere
    check toks[7].kind == tkIdent
    check toks[8].kind == tkGt
    check toks[9].kind == tkInt
    check toks[10].kind == tkSemicolon

  test "CREATE TABLE":
    let toks = tokenize("CREATE TABLE users (id INT PRIMARY KEY)")
    check toks[0].kind == tkCreate
    check toks[1].kind == tkTable
    check toks[2].kind == tkIdent
    check toks[3].kind == tkLParen
    check toks[4].kind == tkIdent
    check toks[5].kind == tkTkInt

  test "mixed case statement":
    let toks = tokenize("Select * From Users")
    check toks[0].kind == tkSelect
    check toks[1].kind == tkStar
    check toks[2].kind == tkFrom
    check toks[3].kind == tkIdent
    check toks[3].value == "Users"

  test "quoted strings with special chars":
    let toks = tokenize("'hello world! @#$'")
    check toks[0].kind == tkString
    check toks[0].value == "hello world! @#$"

suite "Lexer State":

  test "newLexer initializes correctly":
    let l = newLexer("test")
    check l.src == "test"
    check l.pos == 0
    check l.line == 1
    check l.col == 1

  test "tokenize returns complete sequence":
    let toks = tokenize("SELECT")
    check toks[^1].kind == tkEOF

suite "Lexer Edge Cases":

  test "consecutive operators":
    let toks = tokenize("+-*/%")
    check toks.len == 6
    check toks[0].kind == tkPlus
    check toks[1].kind == tkMinus
    check toks[2].kind == tkStar
    check toks[3].kind == tkSlash
    check toks[4].kind == tkPercent
    check toks[5].kind == tkEOF

  test "dot operator":
    let toks = tokenize("mytable.mycolumn")
    check toks[0].kind == tkIdent
    check toks[1].kind == tkDot
    check toks[2].kind == tkIdent

  test "multiple semicolons":
    let toks = tokenize(";;;")
    check toks.len == 4
    check toks[0].kind == tkSemicolon
    check toks[1].kind == tkSemicolon
    check toks[2].kind == tkSemicolon
    check toks[3].kind == tkEOF

  test "parentheses":
    let toks = tokenize("((()))")
    check toks.len == 7
    check toks[0].kind == tkLParen
    check toks[1].kind == tkLParen
    check toks[2].kind == tkLParen
    check toks[3].kind == tkRParen
    check toks[4].kind == tkRParen
    check toks[5].kind == tkRParen

  test "numeric dot vs float":
    let toks = tokenize("1.2")
    check toks[0].kind == tkFloat

  test "identifier dot identifier":
    let toks = tokenize("a.b")
    check toks.len == 4
    check toks[0].kind == tkIdent
    check toks[1].kind == tkDot
    check toks[2].kind == tkIdent
    check toks[3].kind == tkEOF

  test "IF token as identifier":
    let toks = tokenize("IF")
    check toks[0].kind == tkIdent
    check toks[0].value == "IF"
