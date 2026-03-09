# SQL AST node types for Fractio
#
# Covers: CREATE TABLE, DROP TABLE, SELECT, INSERT, UPDATE, DELETE,
#         BEGIN TRANSACTION, COMMIT, ROLLBACK.
# No JOINs.

import std/options
import ../core/types as coreTypes   # DataType, ColumnDef, Constraint, ValueRef

# ---------------------------------------------------------------------------
# Column type (re-uses core DataType)
# ---------------------------------------------------------------------------

export coreTypes.DataType, coreTypes.ColumnDef, coreTypes.Constraint,
       coreTypes.ValueRef

# ---------------------------------------------------------------------------
# Expressions
# ---------------------------------------------------------------------------

type
  ExprKind* = enum
    exLiteral      ## constant value
    exColumn       ## column reference: [table.]col
    exParam        ## positional parameter $N or ?
    exBinOp        ## binary operation: left op right
    exUnaryOp      ## unary operation: op expr
    exIn           ## expr IN (list)
    exIsNull       ## expr IS [NOT] NULL
    exBetween      ## expr BETWEEN lo AND hi
    exLike         ## expr LIKE pattern
    exList         ## parenthesised list of expressions (for IN)
    exStar         ## * (used in SELECT)

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
      colTable*: string  ## may be empty
      colName*:  string
    of exParam:
      paramIdx*: int     ## 1-based
    of exBinOp:
      binOp*:   BinOpKind
      binLeft*: Expr
      binRight*: Expr
    of exUnaryOp:
      unaryOp*:   UnaryOpKind
      unaryExpr*: Expr
    of exIn:
      inExpr*:   Expr
      inNot*:    bool
      inList*:   seq[Expr]
    of exIsNull:
      isNullExpr*: Expr
      isNullNot*:  bool
    of exBetween:
      betweenExpr*: Expr
      betweenNot*:  bool
      betweenLo*:   Expr
      betweenHi*:   Expr
    of exLike:
      likeExpr*:    Expr
      likeNot*:     bool
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
    expr*:  Expr
    alias*: string  ## AS alias, may be empty

# ---------------------------------------------------------------------------
# ORDER BY
# ---------------------------------------------------------------------------

type
  OrderItem* = object
    expr*:  Expr
    desc*:  bool  ## true = DESC, false = ASC (default)

# ---------------------------------------------------------------------------
# Column definition (for CREATE TABLE)
# ---------------------------------------------------------------------------

type
  ColDef* = object
    name*:        string
    dataType*:    DataType
    notNull*:     bool
    primaryKey*:  bool
    unique*:      bool
    defaultExpr*: Option[Expr]

# ---------------------------------------------------------------------------
# Statement nodes
# ---------------------------------------------------------------------------

type
  StmtKind* = enum
    stmtCreateTable
    stmtDropTable
    stmtSelect
    stmtInsert
    stmtUpdate
    stmtDelete
    stmtBegin
    stmtCommit
    stmtRollback

  Stmt* = ref object
    case kind*: StmtKind

    # ---- CREATE TABLE ----
    of stmtCreateTable:
      ctTable*:       string
      ctIfNotExists*: bool
      ctColumns*:     seq[ColDef]
      ctPrimaryKey*:  seq[string]  ## multi-column PK from table constraint

    # ---- DROP TABLE ----
    of stmtDropTable:
      dtTable*:    string
      dtIfExists*: bool

    # ---- SELECT ----
    of stmtSelect:
      selDistinct*: bool
      selCols*:     seq[SelectCol]  ## empty slice = SELECT *
      selFrom*:     string          ## table name (no JOINs)
      selFromAlias*: string
      selWhere*:    Option[Expr]
      selOrderBy*:  seq[OrderItem]
      selLimit*:    Option[Expr]
      selOffset*:   Option[Expr]

    # ---- INSERT ----
    of stmtInsert:
      intoTable*:   string
      intoCols*:    seq[string]    ## may be empty (insert positionally)
      intoValues*:  seq[seq[Expr]] ## one seq per row

    # ---- UPDATE ----
    of stmtUpdate:
      updTable*:   string
      updAlias*:   string
      updSets*:    seq[tuple[col: string, val: Expr]]
      updWhere*:   Option[Expr]

    # ---- DELETE ----
    of stmtDelete:
      delTable*:   string
      delAlias*:   string
      delWhere*:   Option[Expr]

    # ---- Transaction ----
    of stmtBegin:
      beginReadOnly*: bool
    of stmtCommit:
      discard
    of stmtRollback:
      discard
