# SQL Parser for Fractio
#
# Recursive-descent parser that transforms a Token stream into a Stmt AST.
# Supported statements:
#   CREATE TABLE [IF NOT EXISTS] [database].[schema].table (col type [constraints], ...)
#   DROP TABLE [IF EXISTS] [database].[schema].table
#   SELECT [DISTINCT] cols FROM [database].[schema].table [WHERE expr] [ORDER BY ...] [LIMIT n] [OFFSET n]
#   INSERT INTO [database].[schema].table [(cols)] VALUES (row), ...
#   UPDATE [database].[schema].table SET col=expr [, col=expr] [WHERE expr]
#   DELETE FROM [database].[schema].table [WHERE expr]
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
    col*: int

proc parseError(msg: string, tok: Token): ref ParseError =
  let e = newException(ParseError, &"[line {tok.line}:{tok.col}] {msg}")
  e.line = tok.line
  e.col = tok.col
  e

# ---------------------------------------------------------------------------
# Parser state
# ---------------------------------------------------------------------------

type
  Parser* = object
    tokens: seq[Token]
    pos: int

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
     tkWith, tkShow, tkUse, tkDatabases, tkSchemas, tkTables,
     tkSpace, tkSpaces,
     tkTkInt, tkTkFloat, tkTkText, tkTkBool, tkTkDate, tkTkDateTime, tkTkBytes:
    discard p.advance
    t.value
  else:
    raise parseError(&"expected identifier but got '{t.value}' ({t.kind})", t)

# ---------------------------------------------------------------------------
# Qualified table reference parsing
# ---------------------------------------------------------------------------

proc parseQualifiedTableRef(p: var Parser): TableRef =
  ## Parse a qualified table reference: [database].[schema].table
  ## Supports:
  ##   "table"                  -> (database: "", schema: "", table: "table")
  ##   "schema.table"           -> (database: "", schema: "schema", table: "table")
  ##   "database.schema.table"  -> (database: "database", schema: "schema", table: "table")
  let first = p.expectIdent

  if p.check(tkDot):
    discard p.advance # consume first dot
    let second = p.expectIdent

    if p.check(tkDot):
      discard p.advance # consume second dot
      let third = p.expectIdent
      # database.schema.table
      TableRef(database: first, schema: second, table: third)
    else:
      # schema.table
      TableRef(database: "", schema: first, table: second)
  else:
    # just table
    TableRef(database: "", schema: "", table: first)

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
proc parseCreateSpace(p: var Parser): Stmt
proc parseDropSpace(p: var Parser): Stmt
proc parseInSpace(p: var Parser): Option[string]

# ---------------------------------------------------------------------------
# Expression parsing — Pratt/precedence-climbing
# ---------------------------------------------------------------------------

proc tokenBinOp(k: TokenKind): tuple[op: BinOpKind, prec: int, found: bool] =
  case k
  of tkOr: (boOr, 1, true)
  of tkAnd: (boAnd, 2, true)
  of tkEq: (boEq, 3, true)
  of tkNeq: (boNeq, 3, true)
  of tkLt: (boLt, 4, true)
  of tkLte: (boLte, 4, true)
  of tkGt: (boGt, 4, true)
  of tkGte: (boGte, 4, true)
  of tkPlus: (boAdd, 5, true)
  of tkMinus: (boSub, 5, true)
  of tkStar: (boMul, 6, true)
  of tkSlash: (boDiv, 6, true)
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
     tkSpace, tkSpaces,
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
      let lo = p.parseExpr(5) # above AND precedence
      discard p.expect(tkAnd)
      let hi = p.parseExpr(5)
      left = Expr(kind: exBetween, betweenExpr: left, betweenNot: false,
                  betweenLo: lo, betweenHi: hi)
      continue

    # [NOT] LIKE pattern
    if t.kind == tkLike:
      discard p.advance
      let pat = p.parseExpr(5)
      left = Expr(kind: exLike, likeExpr: left, likeNot: false,
          likePattern: pat)
      continue

    # Binary operators
    let (op, prec, found) = tokenBinOp(t.kind)
    if not found or prec <= minPrec: break
    discard p.advance
    let right = p.parseExpr(prec) # left-associative
    left = Expr(kind: exBinOp, binOp: op, binLeft: left, binRight: right)

  left

# ---------------------------------------------------------------------------
# Column type parsing
# ---------------------------------------------------------------------------

proc parseDataType(p: var Parser): tuple[dt: DataType, maxLen: int] =
  ## Parse a data type, returning the type and max length for strings/bytes.
  ## For VARCHAR(n), maxLen is n. For other types, maxLen is 0.
  let t = p.peek
  case t.kind
  of tkTkInt:
    discard p.advance
    (dtInt, 0)
  of tkTkFloat:
    discard p.advance
    (dtFloat, 0)
  of tkTkText:
    discard p.advance
    var maxLen = 64 # Default VARCHAR length
    if p.check(tkLParen):
      discard p.advance
      let lenToken = p.expect(tkInt)
      maxLen = parseInt(lenToken.value)
      discard p.expect(tkRParen)
    (dtString, maxLen)
  of tkTkBool:
    discard p.advance
    (dtBool, 0)
  of tkTkDate:
    discard p.advance
    (dtDate, 0)
  of tkTkDateTime:
    discard p.advance
    (dtDateTime, 0)
  of tkTkBytes:
    discard p.advance
    var maxLen = 1024 # Default BYTES length
    if p.check(tkLParen):
      discard p.advance
      let lenToken = p.expect(tkInt)
      maxLen = parseInt(lenToken.value)
      discard p.expect(tkRParen)
    (dtBytes, maxLen)
  else:
    raise parseError(&"expected a data type but got '{t.value}'", t)

proc parseColDef(p: var Parser): ColDef =
  result.name = p.expectIdent
  let (dt, maxLen) = p.parseDataType
  result.dataType = dt
  result.maxLen = maxLen
  # constraints
  while true:
    case p.peekKind
    of tkNot:
      discard p.advance
      discard p.expect(tkNull)
      result.notNull = true
    of tkNull:
      discard p.advance # explicit nullable — default
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

  let tableRef = p.parseQualifiedTableRef
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
  let spaceName = p.parseInSpace
  result = Stmt(kind: stmtCreateTable, ctTableRef: tableRef,
                ctIfNotExists: ifNotExists, ctColumns: cols,
                ctPrimaryKey: tablePK, ctReplicas: replicas,
                ctSpaceName: spaceName)

# ---------------------------------------------------------------------------
# DROP TABLE
# ---------------------------------------------------------------------------

proc parseDropTable(p: var Parser): Stmt =
  ## Called after DROP TABLE has been consumed.
  let ifExists = p.parseIfExists
  let tableRef = p.parseQualifiedTableRef
  result = Stmt(kind: stmtDropTable, dtTableRef: tableRef, dtIfExists: ifExists)

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
  let fromTable = p.parseQualifiedTableRef
  var fromAlias = ""
  if p.peekKind == tkIdent and p.peek.value.toUpperAscii == "AS":
    discard p.advance
    fromAlias = p.expectIdent
  elif p.peekKind == tkIdent and
       p.peek.value.toUpperAscii notin ["WHERE", "ORDER", "LIMIT", "OFFSET",
           "GROUP", "HAVING"]:
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

  # LIMIT and OFFSET can appear in either order: "LIMIT n OFFSET m" or
  # "OFFSET m LIMIT n" — both are accepted by the SQL standard. We loop
  # until neither keyword is next, and reject duplicate clauses with a
  # parse error rather than silently letting the second one win.
  var limitExpr: Option[Expr]
  var offsetExpr: Option[Expr]
  while p.peekKind in {tkLimit, tkOffset}:
    if p.match(tkLimit):
      if limitExpr.isSome:
        raise parseError("duplicate LIMIT clause", p.peek)
      limitExpr = some(p.parseExpr)
    elif p.match(tkOffset):
      if offsetExpr.isSome:
        raise parseError("duplicate OFFSET clause", p.peek)
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
  let tableRef = p.parseQualifiedTableRef

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

  result = Stmt(kind: stmtInsert, intoTableRef: tableRef,
                intoCols: cols, intoValues: rows)

# ---------------------------------------------------------------------------
# UPDATE
# ---------------------------------------------------------------------------

proc parseUpdate(p: var Parser): Stmt =
  ## Called after UPDATE has been consumed.
  let tableRef = p.parseQualifiedTableRef
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

  result = Stmt(kind: stmtUpdate, updTableRef: tableRef, updAlias: alias,
                updSets: sets, updWhere: whereExpr)

# ---------------------------------------------------------------------------
# DELETE
# ---------------------------------------------------------------------------

proc parseDelete(p: var Parser): Stmt =
  ## Called after DELETE has been consumed.
  discard p.expect(tkFrom)
  let tableRef = p.parseQualifiedTableRef
  var alias = ""
  if p.peekKind == tkIdent and p.peek.value.toUpperAscii == "AS":
    discard p.advance
    alias = p.expectIdent
  elif p.peekKind == tkIdent and p.peek.value.toUpperAscii != "WHERE":
    alias = p.expectIdent

  var whereExpr: Option[Expr]
  if p.match(tkWhere):
    whereExpr = some(p.parseExpr)

  result = Stmt(kind: stmtDelete, delTableRef: tableRef, delAlias: alias,
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
  discard p.advance # consume WITH
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
       csReplicas: replicas, csDatabase: "")

proc parseDropSchema(p: var Parser): Stmt =
  let ie = p.parseIfExists
  let name = p.expectIdent
  Stmt(kind: stmtDropSchema, dsName: name, dsIfExists: ie, dsDatabase: "")

# ---------------------------------------------------------------------------
# CREATE / DROP SPACE
# ---------------------------------------------------------------------------

proc parseCreateSpace(p: var Parser): Stmt =
  ## Called after CREATE SPACE has been consumed.
  let name = p.expectIdent
  discard p.expect(tkWith)
  let kw = p.expectIdent
  if kw.toUpperAscii != "REPLICAS":
    raise parseError("expected REPLICAS after WITH but got '" & kw & "'", p.peek)
  discard p.expect(tkEq)
  let tok = p.peek
  var replicas: int
  if tok.kind == tkInt:
    discard p.advance
    replicas = parseInt(tok.value)
    if replicas < 1:
      raise parseError("REPLICAS must be >= 1, got " & $replicas, tok)
  elif tok.kind == tkAll:
    discard p.advance
    replicas = 0 # 0 means ALL
  else:
    raise parseError("expected integer or ALL for REPLICAS but got '" &
        tok.value & "'", tok)
  Stmt(kind: stmtCreateSpace, csSpaceName: name, csSpaceReplicas: replicas)

proc parseDropSpace(p: var Parser): Stmt =
  ## Called after DROP SPACE has been consumed.
  let name = p.expectIdent
  Stmt(kind: stmtDropSpace, dsSpaceName: name)

proc parseInSpace(p: var Parser): Option[string] =
  ## Parse optional IN SPACE <name> clause.
  if p.peekKind != tkIn:
    return none(string)
  discard p.advance # consume IN
  if p.peekKind != tkSpace:
    raise parseError("expected SPACE after IN but got '" & p.peek.value & "'", p.peek)
  discard p.advance # consume SPACE
  let name = p.expectIdent
  some(name)

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
    of tkSpace:
      discard p.advance
      return p.parseCreateSpace
    else:
      raise parseError(&"expected TABLE, DATABASE, SCHEMA, or SPACE after CREATE but got '{p.peek.value}'", p.peek)
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
    of tkSpace:
      discard p.advance
      return p.parseDropSpace
    else:
      raise parseError(&"expected TABLE, DATABASE, SCHEMA, or SPACE after DROP but got '{p.peek.value}'", p.peek)
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
    of tkSpaces:
      discard p.advance
      return Stmt(kind: stmtShowSpaces)
    else:
      raise parseError(&"expected DATABASES, SCHEMAS, TABLES, or SPACES after SHOW but got '{p.peek.value}'", p.peek)
  of tkUse:
    discard p.advance
    case p.peekKind
    of tkDatabase:
      discard p.advance
      let name = p.expectIdent
      return Stmt(kind: stmtUseDatabase, useDbName: name)
    of tkSchema:
      discard p.advance
      let name = p.expectIdent
      return Stmt(kind: stmtUseSchema, useSchemaName: name)
    else:
      # Bare "USE <name>" defaults to USE DATABASE
      let name = p.expectIdent
      return Stmt(kind: stmtUseDatabase, useDbName: name)
  of tkExplain:
    discard p.advance
    let inner = p.parseOne
    return Stmt(kind: stmtExplain, explainStmt: inner)
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
