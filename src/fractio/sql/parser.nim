# SQL Parser for Fractio
#
# Recursive-descent parser that transforms a Token stream into a Stmt AST.
# Supported statements:
#   CREATE TABLE [IF NOT EXISTS] t (col type [constraints], ...)
#   DROP TABLE [IF EXISTS] t
#   SELECT [DISTINCT] cols FROM t [WHERE expr] [ORDER BY ...] [LIMIT n] [OFFSET n]
#   INSERT INTO t [(cols)] VALUES (row), ...
#   UPDATE t SET col=expr [, col=expr] [WHERE expr]
#   DELETE FROM t [WHERE expr]
#   BEGIN [TRANSACTION | WORK]
#   COMMIT [TRANSACTION | WORK]
#   ROLLBACK [TRANSACTION | WORK]

import std/[strutils, strformat, options]
import ./lexer
import ./ast
import ../core/types as coreTypes

# ---------------------------------------------------------------------------
# Error
# ---------------------------------------------------------------------------

type
  ParseError* = object of CatchableError
    line*: int
    col*:  int

proc parseError(msg: string, tok: Token): ref ParseError =
  let e = newException(ParseError, &"[line {tok.line}:{tok.col}] {msg}")
  e.line = tok.line
  e.col  = tok.col
  e

# ---------------------------------------------------------------------------
# Parser state
# ---------------------------------------------------------------------------

type
  Parser* = object
    tokens: seq[Token]
    pos:    int

proc newParser*(tokens: seq[Token]): Parser =
  Parser(tokens: tokens, pos: 0)

proc peek(p: Parser, offset: int = 0): Token {.inline.} =
  let i = p.pos + offset
  if i < p.tokens.len: p.tokens[i]
  else: Token(kind: tkEOF)

proc peekKind(p: Parser, offset: int = 0): TokenKind {.inline.} =
  p.peek(offset).kind

proc advance(p: var Parser): Token =
  result = p.tokens[p.pos]
  if p.pos < p.tokens.len: inc p.pos

proc check(p: Parser, k: TokenKind): bool {.inline.} =
  p.peekKind == k

proc match(p: var Parser, k: TokenKind): bool =
  if p.check(k): discard p.advance; true else: false

proc expect(p: var Parser, k: TokenKind): Token =
  if p.peekKind != k:
    raise parseError(&"expected {k} but got '{p.peek.value}' ({p.peekKind})", p.peek)
  p.advance

proc expectIdent(p: var Parser): string =
  ## Accept any token that can serve as an identifier (including keyword-reuse).
  let t = p.peek
  case t.kind
  of tkIdent, tkCreate, tkDrop, tkTable, tkDatabase, tkSchema,
     tkPrimary, tkKey, tkUnique,
     tkNot, tkDefault, tkSelect, tkInsert, tkUpdate, tkDelete, tkInto,
     tkValues, tkSet, tkFrom, tkWhere, tkAnd, tkOr, tkIn, tkIs,
     tkBetween, tkLike, tkLimit, tkOffset, tkOrder, tkBy, tkAsc, tkDesc,
     tkAll, tkDistinct, tkBegin, tkCommit, tkRollback, tkTransaction, tkWork,
     tkWith, tkShow, tkDatabases, tkSchemas, tkTables,
     tkTkInt, tkTkFloat, tkTkText, tkTkBool, tkTkDate, tkTkDateTime, tkTkBytes:
    discard p.advance
    t.value
  else:
    raise parseError(&"expected identifier but got '{t.value}' ({t.kind})", t)

# ---------------------------------------------------------------------------
# Literal helpers
# ---------------------------------------------------------------------------

proc litNull(): Expr = Expr(kind: exLiteral, litValue: nil)
proc litBool(b: bool): Expr = Expr(kind: exLiteral, litValue: newValueRef(b))
proc litInt(s: string): Expr =
  Expr(kind: exLiteral, litValue: newValueRef(parseBiggestInt(s).int64))
proc litFloat(s: string): Expr =
  Expr(kind: exLiteral, litValue: newValueRef(parseFloat(s)))
proc litStr(s: string): Expr =
  Expr(kind: exLiteral, litValue: newValueRef(s))

# ---------------------------------------------------------------------------
# Forward declarations
# ---------------------------------------------------------------------------

proc parseExpr(p: var Parser, minPrec: int = 0): Expr
proc parseSelect(p: var Parser): Stmt
proc parseInsert(p: var Parser): Stmt
proc parseUpdate(p: var Parser): Stmt
proc parseDelete(p: var Parser): Stmt
proc parseCreateTable(p: var Parser): Stmt
proc parseDropTable(p: var Parser): Stmt
proc parseIfNotExists(p: var Parser): bool
proc parseIfExists(p: var Parser): bool
proc parseCreateDatabase(p: var Parser): Stmt
proc parseDropDatabase(p: var Parser): Stmt
proc parseCreateSchema(p: var Parser): Stmt
proc parseDropSchema(p: var Parser): Stmt
proc parseWithReplicas(p: var Parser): Option[int]

# ---------------------------------------------------------------------------
# Expression parsing — Pratt/precedence-climbing
# ---------------------------------------------------------------------------

proc tokenBinOp(k: TokenKind): tuple[op: BinOpKind, prec: int, found: bool] =
  case k
  of tkOr:      (boOr,  1, true)
  of tkAnd:     (boAnd, 2, true)
  of tkEq:      (boEq,  3, true)
  of tkNeq:     (boNeq, 3, true)
  of tkLt:      (boLt,  4, true)
  of tkLte:     (boLte, 4, true)
  of tkGt:      (boGt,  4, true)
  of tkGte:     (boGte, 4, true)
  of tkPlus:    (boAdd, 5, true)
  of tkMinus:   (boSub, 5, true)
  of tkStar:    (boMul, 6, true)
  of tkSlash:   (boDiv, 6, true)
  of tkPercent: (boMod, 6, true)
  else: (boEq, 0, false)

proc parsePrimary(p: var Parser): Expr =
  let t = p.peek
  case t.kind
  of tkNull:
    discard p.advance
    return litNull()
  of tkTrue:
    discard p.advance
    return litBool(true)
  of tkFalse:
    discard p.advance
    return litBool(false)
  of tkInt:
    discard p.advance
    return litInt(t.value)
  of tkFloat:
    discard p.advance
    return litFloat(t.value)
  of tkString:
    discard p.advance
    return litStr(t.value)
  of tkMinus:
    discard p.advance
    let e = p.parsePrimary
    return Expr(kind: exUnaryOp, unaryOp: uoNeg, unaryExpr: e)
  of tkNot:
    discard p.advance
    let e = p.parsePrimary
    return Expr(kind: exUnaryOp, unaryOp: uoNot, unaryExpr: e)
  of tkStar:
    discard p.advance
    return Expr(kind: exStar)
  of tkLParen:
    discard p.advance
    let inner = p.parseExpr
    discard p.expect(tkRParen)
    return inner
  of tkIdent, tkCreate, tkDrop, tkTable, tkDatabase, tkSchema,
     tkPrimary, tkKey, tkUnique,
     tkDefault, tkSelect, tkInsert, tkUpdate, tkDelete, tkInto, tkValues,
     tkSet, tkFrom, tkWhere, tkAnd, tkOr, tkIn, tkIs, tkBetween, tkLike,
     tkLimit, tkOffset, tkOrder, tkBy, tkAsc, tkDesc, tkAll, tkDistinct,
     tkBegin, tkCommit, tkRollback, tkTransaction, tkWork, tkWith,
     tkShow, tkDatabases, tkSchemas, tkTables,
     tkTkInt, tkTkFloat, tkTkText, tkTkBool, tkTkDate, tkTkDateTime, tkTkBytes:
    let name = t.value
    discard p.advance
    if p.check(tkDot):
      discard p.advance
      let col = p.expectIdent
      return Expr(kind: exColumn, colTable: name, colName: col)
    return Expr(kind: exColumn, colTable: "", colName: name)
  else:
    raise parseError(&"unexpected token '{t.value}' in expression", t)

proc parseExpr(p: var Parser, minPrec: int = 0): Expr =
  var left = p.parsePrimary

  # postfix / infix extensions
  while true:
    let t = p.peek

    # IS [NOT] NULL
    if t.kind == tkIs:
      discard p.advance
      let notNull = p.match(tkNot)
      discard p.expect(tkNull)
      left = Expr(kind: exIsNull, isNullExpr: left, isNullNot: notNull)
      continue

    # [NOT] IN (list)
    if t.kind in {tkIn, tkNot}:
      var negIn = false
      if t.kind == tkNot:
        negIn = true
        discard p.advance
        discard p.expect(tkIn)
      else:
        discard p.advance
      discard p.expect(tkLParen)
      var items: seq[Expr]
      if not p.check(tkRParen):
        items.add(p.parseExpr)
        while p.match(tkComma):
          items.add(p.parseExpr)
      discard p.expect(tkRParen)
      left = Expr(kind: exIn, inExpr: left, inNot: negIn, inList: items)
      continue

    # [NOT] BETWEEN lo AND hi
    if t.kind in {tkBetween}:
      discard p.advance
      let lo = p.parseExpr(5)  # above AND precedence
      discard p.expect(tkAnd)
      let hi = p.parseExpr(5)
      left = Expr(kind: exBetween, betweenExpr: left, betweenNot: false,
                  betweenLo: lo, betweenHi: hi)
      continue

    # [NOT] LIKE pattern
    if t.kind == tkLike:
      discard p.advance
      let pat = p.parseExpr(5)
      left = Expr(kind: exLike, likeExpr: left, likeNot: false, likePattern: pat)
      continue

    # Binary operators
    let (op, prec, found) = tokenBinOp(t.kind)
    if not found or prec <= minPrec: break
    discard p.advance
    let right = p.parseExpr(prec)  # left-associative
    left = Expr(kind: exBinOp, binOp: op, binLeft: left, binRight: right)

  left

# ---------------------------------------------------------------------------
# Column type parsing
# ---------------------------------------------------------------------------

proc parseDataType(p: var Parser): DataType =
  let t = p.peek
  case t.kind
  of tkTkInt:      discard p.advance; dtInt
  of tkTkFloat:    discard p.advance; dtFloat
  of tkTkText:
    discard p.advance
    # optional (n) precision — skip it
    if p.check(tkLParen):
      discard p.advance
      discard p.expect(tkInt)
      discard p.expect(tkRParen)
    dtString
  of tkTkBool:     discard p.advance; dtBool
  of tkTkDate:     discard p.advance; dtDate
  of tkTkDateTime: discard p.advance; dtDateTime
  of tkTkBytes:    discard p.advance; dtBytes
  else:
    raise parseError(&"expected a data type but got '{t.value}'", t)

# ---------------------------------------------------------------------------
# CREATE TABLE
# ---------------------------------------------------------------------------

proc parseColDef(p: var Parser): ColDef =
  result.name = p.expectIdent
  result.dataType = p.parseDataType
  # constraints
  while true:
    case p.peekKind
    of tkNot:
      discard p.advance
      discard p.expect(tkNull)
      result.notNull = true
    of tkNull:
      discard p.advance  # explicit nullable — default
    of tkPrimary:
      discard p.advance
      discard p.expect(tkKey)
      result.primaryKey = true
      result.notNull = true
    of tkUnique:
      discard p.advance
      result.unique = true
    of tkDefault:
      discard p.advance
      result.defaultExpr = some(p.parseExpr(5))
    else:
      break

proc parseCreateTable(p: var Parser): Stmt =
  ## Called after CREATE TABLE has been consumed.
  let ifNotExists = p.parseIfNotExists

  let tableName = p.expectIdent
  discard p.expect(tkLParen)

  var cols: seq[ColDef]
  var tablePK: seq[string]

  while not p.check(tkRParen) and not p.check(tkEOF):
    # Table-level PRIMARY KEY constraint
    if p.peekKind == tkPrimary:
      discard p.advance
      discard p.expect(tkKey)
      discard p.expect(tkLParen)
      tablePK.add(p.expectIdent)
      while p.match(tkComma):
        tablePK.add(p.expectIdent)
      discard p.expect(tkRParen)
    elif p.peekKind == tkUnique:
      # UNIQUE (col, ...) — parse but don't error; we mark columns later
      discard p.advance
      discard p.expect(tkLParen)
      discard p.expectIdent
      while p.match(tkComma): discard p.expectIdent
      discard p.expect(tkRParen)
    else:
      cols.add(p.parseColDef)
    if not p.match(tkComma): break

  discard p.expect(tkRParen)
  let replicas = p.parseWithReplicas
  result = Stmt(kind: stmtCreateTable, ctTable: tableName,
                ctIfNotExists: ifNotExists, ctColumns: cols,
                ctPrimaryKey: tablePK, ctReplicas: replicas)

# ---------------------------------------------------------------------------
# DROP TABLE
# ---------------------------------------------------------------------------

proc parseDropTable(p: var Parser): Stmt =
  ## Called after DROP TABLE has been consumed.
  let ifExists = p.parseIfExists
  let tableName = p.expectIdent
  result = Stmt(kind: stmtDropTable, dtTable: tableName, dtIfExists: ifExists)

# ---------------------------------------------------------------------------
# SELECT
# ---------------------------------------------------------------------------

proc parseSelectCols(p: var Parser): seq[SelectCol] =
  if p.check(tkStar):
    discard p.advance
    return @[SelectCol(expr: Expr(kind: exStar), alias: "")]

  while true:
    let e = p.parseExpr
    var alias = ""
    # optional AS alias or bare alias
    if p.peekKind == tkIdent and p.peek.value.toUpperAscii == "AS":
      discard p.advance
      alias = p.expectIdent
    elif p.peekKind == tkIdent:
      alias = p.expectIdent
    result.add(SelectCol(expr: e, alias: alias))
    if not p.match(tkComma): break

proc parseSelect(p: var Parser): Stmt =
  ## Called after SELECT has been consumed.
  let isDistinct = p.match(tkDistinct) or p.match(tkAll)
  let cols = p.parseSelectCols

  discard p.expect(tkFrom)
  let fromTable = p.expectIdent
  var fromAlias = ""
  if p.peekKind == tkIdent and p.peek.value.toUpperAscii == "AS":
    discard p.advance
    fromAlias = p.expectIdent
  elif p.peekKind == tkIdent and
       p.peek.value.toUpperAscii notin ["WHERE","ORDER","LIMIT","OFFSET","GROUP","HAVING"]:
    fromAlias = p.expectIdent

  var whereExpr: Option[Expr]
  if p.match(tkWhere):
    whereExpr = some(p.parseExpr)

  var orderBy: seq[OrderItem]
  if p.match(tkOrder):
    discard p.expect(tkBy)
    while true:
      let e = p.parseExpr
      let desc = p.match(tkDesc)
      if not desc: discard p.match(tkAsc)
      orderBy.add(OrderItem(expr: e, desc: desc))
      if not p.match(tkComma): break

  var limitExpr: Option[Expr]
  if p.match(tkLimit):
    limitExpr = some(p.parseExpr)

  var offsetExpr: Option[Expr]
  if p.match(tkOffset):
    offsetExpr = some(p.parseExpr)

  result = Stmt(kind: stmtSelect,
    selDistinct: isDistinct,
    selCols: cols,
    selFrom: fromTable,
    selFromAlias: fromAlias,
    selWhere: whereExpr,
    selOrderBy: orderBy,
    selLimit: limitExpr,
    selOffset: offsetExpr)

# ---------------------------------------------------------------------------
# INSERT
# ---------------------------------------------------------------------------

proc parseInsert(p: var Parser): Stmt =
  ## Called after INSERT has been consumed.
  discard p.expect(tkInto)
  let tableName = p.expectIdent

  # optional column list
  var cols: seq[string]
  if p.match(tkLParen):
    cols.add(p.expectIdent)
    while p.match(tkComma):
      cols.add(p.expectIdent)
    discard p.expect(tkRParen)

  discard p.expect(tkValues)

  # one or more rows
  var rows: seq[seq[Expr]]
  while true:
    discard p.expect(tkLParen)
    var row: seq[Expr]
    row.add(p.parseExpr)
    while p.match(tkComma):
      row.add(p.parseExpr)
    discard p.expect(tkRParen)
    rows.add(row)
    if not p.match(tkComma): break

  result = Stmt(kind: stmtInsert, intoTable: tableName,
                intoCols: cols, intoValues: rows)

# ---------------------------------------------------------------------------
# UPDATE
# ---------------------------------------------------------------------------

proc parseUpdate(p: var Parser): Stmt =
  ## Called after UPDATE has been consumed.
  let tableName = p.expectIdent
  var alias = ""
  if p.peekKind == tkIdent and p.peek.value.toUpperAscii == "AS":
    discard p.advance
    alias = p.expectIdent
  elif p.peekKind == tkIdent and p.peek.value.toUpperAscii != "SET":
    alias = p.expectIdent

  discard p.expect(tkSet)

  var sets: seq[tuple[col: string, val: Expr]]
  while true:
    let col = p.expectIdent
    discard p.expect(tkEq)
    let val = p.parseExpr
    sets.add((col, val))
    if not p.match(tkComma): break

  var whereExpr: Option[Expr]
  if p.match(tkWhere):
    whereExpr = some(p.parseExpr)

  result = Stmt(kind: stmtUpdate, updTable: tableName, updAlias: alias,
                updSets: sets, updWhere: whereExpr)

# ---------------------------------------------------------------------------
# DELETE
# ---------------------------------------------------------------------------

proc parseDelete(p: var Parser): Stmt =
  ## Called after DELETE has been consumed.
  discard p.expect(tkFrom)
  let tableName = p.expectIdent
  var alias = ""
  if p.peekKind == tkIdent and p.peek.value.toUpperAscii == "AS":
    discard p.advance
    alias = p.expectIdent
  elif p.peekKind == tkIdent and p.peek.value.toUpperAscii != "WHERE":
    alias = p.expectIdent

  var whereExpr: Option[Expr]
  if p.match(tkWhere):
    whereExpr = some(p.parseExpr)

  result = Stmt(kind: stmtDelete, delTable: tableName, delAlias: alias,
                delWhere: whereExpr)

# ---------------------------------------------------------------------------
# CREATE / DROP DATABASE and SCHEMA
# ---------------------------------------------------------------------------

proc parseIfNotExists(p: var Parser): bool =
  ## Consume IF NOT EXISTS and return true, or return false if absent.
  if p.peekKind == tkIdent and p.peek.value.toUpperAscii == "IF":
    discard p.advance
    discard p.expect(tkNot)
    let ex = p.expectIdent
    if ex.toUpperAscii != "EXISTS":
      raise parseError("expected EXISTS after IF NOT", p.peek)
    return true
  false

proc parseIfExists(p: var Parser): bool =
  ## Consume IF EXISTS and return true, or return false if absent.
  if p.peekKind == tkIdent and p.peek.value.toUpperAscii == "IF":
    discard p.advance
    let ex = p.expectIdent
    if ex.toUpperAscii != "EXISTS":
      raise parseError("expected EXISTS after IF", p.peek)
    return true
  false

proc parseWithReplicas(p: var Parser): Option[int] =
  ## Parse optional WITH REPLICAS = N clause.
  ## Returns none if absent; raises ParseError if malformed.
  if p.peekKind != tkWith:
    return none(int)
  discard p.advance  # consume WITH
  let kw = p.expectIdent
  if kw.toUpperAscii != "REPLICAS":
    raise parseError("expected REPLICAS after WITH but got '" & kw & "'", p.peek)
  discard p.expect(tkEq)
  let tok = p.expect(tkInt)
  let n = parseInt(tok.value)
  if n < 1:
    raise parseError("REPLICAS must be >= 1, got " & $n, tok)
  some(n)

proc parseCreateDatabase(p: var Parser): Stmt =
  let ine = p.parseIfNotExists
  let name = p.expectIdent
  let replicas = p.parseWithReplicas
  Stmt(kind: stmtCreateDatabase, cdbName: name, cdbIfNotExists: ine,
       cdbReplicas: replicas)

proc parseDropDatabase(p: var Parser): Stmt =
  let ie = p.parseIfExists
  let name = p.expectIdent
  Stmt(kind: stmtDropDatabase, ddbName: name, ddbIfExists: ie)

proc parseCreateSchema(p: var Parser): Stmt =
  let ine = p.parseIfNotExists
  let name = p.expectIdent
  let replicas = p.parseWithReplicas
  Stmt(kind: stmtCreateSchema, csName: name, csIfNotExists: ine,
       csReplicas: replicas)

proc parseDropSchema(p: var Parser): Stmt =
  let ie = p.parseIfExists
  let name = p.expectIdent
  Stmt(kind: stmtDropSchema, dsName: name, dsIfExists: ie)

# ---------------------------------------------------------------------------
# Transaction statements
# ---------------------------------------------------------------------------

proc skipTxnSuffix(p: var Parser) =
  ## Consume optional TRANSACTION or WORK keyword.
  discard p.match(tkTransaction) or p.match(tkWork)

# ---------------------------------------------------------------------------
# Top-level parse entry
# ---------------------------------------------------------------------------

proc parseOne*(p: var Parser): Stmt =
  ## Parse exactly one statement (without the trailing semicolon).
  let t = p.peek
  case t.kind
  of tkCreate:
    discard p.advance
    case p.peekKind
    of tkTable:
      discard p.advance
      return p.parseCreateTable
    of tkDatabase:
      discard p.advance
      return p.parseCreateDatabase
    of tkSchema:
      discard p.advance
      return p.parseCreateSchema
    else:
      raise parseError(&"expected TABLE, DATABASE, or SCHEMA after CREATE but got '{p.peek.value}'", p.peek)
  of tkDrop:
    discard p.advance
    case p.peekKind
    of tkTable:
      discard p.advance
      return p.parseDropTable
    of tkDatabase:
      discard p.advance
      return p.parseDropDatabase
    of tkSchema:
      discard p.advance
      return p.parseDropSchema
    else:
      raise parseError(&"expected TABLE, DATABASE, or SCHEMA after DROP but got '{p.peek.value}'", p.peek)
  of tkSelect:
    discard p.advance
    return p.parseSelect
  of tkInsert:
    discard p.advance
    return p.parseInsert
  of tkUpdate:
    discard p.advance
    return p.parseUpdate
  of tkDelete:
    discard p.advance
    return p.parseDelete
  of tkBegin:
    discard p.advance
    p.skipTxnSuffix
    # optional READ ONLY
    var readOnly = false
    if p.peekKind == tkIdent and p.peek.value.toUpperAscii == "READ":
      discard p.advance
      if p.peekKind == tkIdent and p.peek.value.toUpperAscii == "ONLY":
        discard p.advance
        readOnly = true
    return Stmt(kind: stmtBegin, beginReadOnly: readOnly)
  of tkCommit:
    discard p.advance
    p.skipTxnSuffix
    return Stmt(kind: stmtCommit)
  of tkRollback:
    discard p.advance
    p.skipTxnSuffix
    return Stmt(kind: stmtRollback)
  of tkShow:
    discard p.advance
    case p.peekKind
    of tkDatabases:
      discard p.advance
      return Stmt(kind: stmtShowDatabases)
    of tkSchemas:
      discard p.advance
      # optional IN <database>
      var db = ""
      if p.peekKind == tkIn:
        discard p.advance
        db = p.expectIdent
      return Stmt(kind: stmtShowSchemas, showSchemasDb: db)
    of tkTables:
      discard p.advance
      # optional IN <schema> or IN <database>.<schema>
      var db = ""
      var schema = ""
      if p.peekKind == tkIn:
        discard p.advance
        let first = p.expectIdent
        if p.check(tkDot):
          discard p.advance
          let second = p.expectIdent
          db = first
          schema = second
        else:
          schema = first
      return Stmt(kind: stmtShowTables, showTablesDb: db,
                  showTablesSchema: schema)
    else:
      raise parseError(&"expected DATABASES, SCHEMAS, or TABLES after SHOW but got '{p.peek.value}'", p.peek)
  else:
    raise parseError(&"expected a SQL statement but got '{t.value}'", t)

proc parseAll*(src: string): seq[Stmt] =
  ## Tokenise and parse all semicolon-separated statements in `src`.
  ## Trailing/lone semicolons are silently skipped.
  let tokens = tokenize(src)
  var p = newParser(tokens)
  while true:
    # skip any number of bare semicolons
    while p.match(tkSemicolon): discard
    if p.peekKind in {tkEOF, tkError}: break
    result.add(p.parseOne)

proc parseStatement*(sql: string): Stmt =
  ## Parse a single SQL statement.  Raises ParseError on failure.
  let tokens = tokenize(sql)
  var p = newParser(tokens)
  p.parseOne
