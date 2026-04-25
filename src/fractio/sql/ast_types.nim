# SQL AST node types for Fractio
#
# Covers: CREATE TABLE, DROP TABLE, SELECT, INSERT, UPDATE, DELETE,
#         BEGIN TRANSACTION, COMMIT, ROLLBACK.
# No JOINs.

import std/[options, strutils]
import ../core/types as coreTypes # DataType, ColumnDef, Constraint, ValueRef

# ---------------------------------------------------------------------------
# Column type (re-uses core DataType)
# ---------------------------------------------------------------------------

export coreTypes.DataType, coreTypes.ColumnDef, coreTypes.Constraint,
       coreTypes.ValueRef

# ---------------------------------------------------------------------------
# Qualified table reference
# ---------------------------------------------------------------------------

type
  TableRef* = object
    ## A qualified table reference: [database].[schema].table
    ## All components except table are optional.
    ## Examples:
    ##   "users"                  -> database="", schema="", table="users"
    ##   "public.users"           -> database="", schema="public", table="users"
    ##   "mydb.public.users"      -> database="mydb", schema="public", table="users"
    ##   "sys.spaces"             -> database="", schema="sys", table="spaces"
    database*: string ## Optional database name (empty = current database)
    schema*: string ## Optional schema name (empty = current schema)
    table*: string ## Required table name

proc `==`*(a, b: TableRef): bool =
  ## Equality check for TableRef (case-insensitive for names)
  a.database.toLowerAscii == b.database.toLowerAscii and
  a.schema.toLowerAscii == b.schema.toLowerAscii and
  a.table.toLowerAscii == b.table.toLowerAscii

proc fullName*(t: TableRef): string =
  ## Returns the fully qualified name for debugging/logging
  if t.database != "" and t.schema != "":
    t.database & "." & t.schema & "." & t.table
  elif t.schema != "":
    t.schema & "." & t.table
  else:
    t.table

# ---------------------------------------------------------------------------
# Expressions
# ---------------------------------------------------------------------------

type
  ExprKind* = enum
    exLiteral ## constant value
    exColumn  ## column reference: [table.]col
    exParam   ## positional parameter $N or ?
    exBinOp   ## binary operation: left op right
    exUnaryOp ## unary operation: op expr
    exIn      ## expr IN (list)
    exIsNull  ## expr IS [NOT] NULL
    exBetween ## expr BETWEEN lo AND hi
    exLike    ## expr LIKE pattern
    exList    ## parenthesised list of expressions (for IN)
    exStar    ## * (used in SELECT)

  BinOpKind* = enum
    boEq, boNeq, boLt, boLte, boGt, boGte,
    boAnd, boOr,
    boAdd, boSub, boMul, boDiv, boMod

  UnaryOpKind* = enum
    uoNot, uoNeg

  Expr* = ref object
    case kind*: ExprKind
    of exLiteral:
      litValue*: ValueRef
    of exColumn:
      colTable*: string ## may be empty
      colName*: string
    of exParam:
      paramIdx*: int    ## 1-based
    of exBinOp:
      binOp*: BinOpKind
      binLeft*: Expr
      binRight*: Expr
    of exUnaryOp:
      unaryOp*: UnaryOpKind
      unaryExpr*: Expr
    of exIn:
      inExpr*: Expr
      inNot*: bool
      inList*: seq[Expr]
    of exIsNull:
      isNullExpr*: Expr
      isNullNot*: bool
    of exBetween:
      betweenExpr*: Expr
      betweenNot*: bool
      betweenLo*: Expr
      betweenHi*: Expr
    of exLike:
      likeExpr*: Expr
      likeNot*: bool
      likePattern*: Expr
    of exList:
      listItems*: seq[Expr]
    of exStar:
      discard

# ---------------------------------------------------------------------------
# SELECT columns
# ---------------------------------------------------------------------------

type
  SelectCol* = object
    ## A single item in the SELECT list.
    expr*: Expr
    alias*: string ## AS alias, may be empty

# ---------------------------------------------------------------------------
# ORDER BY
# ---------------------------------------------------------------------------

type
  OrderItem* = object
    expr*: Expr
    desc*: bool ## true = DESC, false = ASC (default)

# ---------------------------------------------------------------------------
# Column definition (for CREATE TABLE)
# ---------------------------------------------------------------------------

type
  ColDef* = object
    name*: string
    dataType*: DataType
    maxLen*: int ## Max length for VARCHAR/bytes types (0 = unspecified, use default)
    notNull*: bool
    primaryKey*: bool
    unique*: bool
    defaultExpr*: Option[Expr]

# ---------------------------------------------------------------------------
# Statement nodes
# ---------------------------------------------------------------------------

type
  StmtKind* = enum
    stmtCreateTable
    stmtDropTable
    stmtCreateDatabase
    stmtDropDatabase
    stmtCreateSchema
    stmtDropSchema
    stmtSelect
    stmtInsert
    stmtUpdate
    stmtDelete
    stmtShowDatabases
    stmtShowSchemas
    stmtShowTables
    stmtUseDatabase
    stmtUseSchema
    stmtCreateSpace
    stmtDropSpace
    stmtShowSpaces
    stmtBegin
    stmtCommit
    stmtRollback
    stmtExplain

  Stmt* = ref object
    case kind*: StmtKind

    # ---- CREATE TABLE ----
    of stmtCreateTable:
      ctTableRef*: TableRef        ## qualified table reference
      ctIfNotExists*: bool
      ctColumns*: seq[ColDef]
      ctPrimaryKey*: seq[string]   ## multi-column PK from table constraint
      ctReplicas*: Option[int]     ## WITH REPLICAS = N; none → inherit from schema
      ctSpaceName*: Option[string] ## IN SPACE <name>; none → default space

    # ---- DROP TABLE ----
    of stmtDropTable:
      dtTableRef*: TableRef        ## qualified table reference
      dtIfExists*: bool

    # ---- CREATE DATABASE ----
    of stmtCreateDatabase:
      cdbName*: string
      cdbIfNotExists*: bool
      cdbReplicas*: Option[int]    ## WITH REPLICAS = N; none → cluster default

    # ---- DROP DATABASE ----
    of stmtDropDatabase:
      ddbName*: string
      ddbIfExists*: bool

    # ---- CREATE SCHEMA ----
    of stmtCreateSchema:
      csName*: string
      csIfNotExists*: bool
      csReplicas*: Option[int]     ## WITH REPLICAS = N; none → inherit from database
      csDatabase*: string          ## database to create schema in (empty = current)

    # ---- DROP SCHEMA ----
    of stmtDropSchema:
      dsName*: string
      dsIfExists*: bool
      dsDatabase*: string          ## database containing schema (empty = current)

    # ---- SELECT ----
    of stmtSelect:
      selDistinct*: bool
      selCols*: seq[SelectCol]     ## empty slice = SELECT *
      selFrom*: TableRef           ## qualified table reference (no JOINs)
      selFromAlias*: string
      selWhere*: Option[Expr]
      selOrderBy*: seq[OrderItem]
      selLimit*: Option[Expr]
      selOffset*: Option[Expr]

    # ---- INSERT ----
    of stmtInsert:
      intoTableRef*: TableRef      ## qualified table reference
      intoCols*: seq[string]       ## may be empty (insert positionally)
      intoValues*: seq[seq[Expr]]  ## one seq per row

    # ---- UPDATE ----
    of stmtUpdate:
      updTableRef*: TableRef       ## qualified table reference
      updAlias*: string
      updSets*: seq[tuple[col: string, val: Expr]]
      updWhere*: Option[Expr]

    # ---- DELETE ----
    of stmtDelete:
      delTableRef*: TableRef       ## qualified table reference
      delAlias*: string
      delWhere*: Option[Expr]

    # ---- SPACE ----
    of stmtCreateSpace:
      csSpaceName*: string
      csSpaceReplicas*: int        ## 0 = ALL nodes
    of stmtDropSpace:
      dsSpaceName*: string
    of stmtShowSpaces:
      discard

    # ---- SHOW ----
    of stmtShowDatabases:
      discard
    of stmtShowSchemas:
      showSchemasDb*: string ## database to list schemas from (may be empty → use current)
    of stmtShowTables:
      showTablesDb*: string        ## database (may be empty → use current)
      showTablesSchema*: string    ## schema (may be empty → use current)

    # ---- USE ----
    of stmtUseDatabase:
      useDbName*: string
    of stmtUseSchema:
      useSchemaName*: string

    # ---- Transaction ----
    of stmtBegin:
      beginReadOnly*: bool
    of stmtCommit:
      discard
    of stmtRollback:
      discard

    # ---- EXPLAIN ----
    of stmtExplain:
      explainStmt*: Stmt           ## the inner statement being explained
