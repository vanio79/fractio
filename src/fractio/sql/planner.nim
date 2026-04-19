# SQL Planner for Fractio
#
# Translates a Stmt AST into a Plan — a sequence of KV operations.
# The planner resolves table names to table IDs via catalog lookups
# and generates the appropriate key encodings for reads/writes.

import std/[options, json, strutils, strformat, sequtils]
import ./ast
import ./data_row
import ../distributed/meta/system_tables
import ../distributed/meta/system_schemas
import ../client/fractio_client
import ../core/types as coreTypes
import ../core/primary_key
import ../core/kv_interface # for KVOpResult isErr/isOk procs
import ../protocol/messages/kv # for WireFilterExpr types
import ../utils/external_merge_sort # for SortSpec, orderItemsToSortSpecs

# ---------------------------------------------------------------------------
# Plan types
# ---------------------------------------------------------------------------

type
  PlanOpKind* = enum
    poCreateDatabase
    poDropDatabase
    poCreateSchema
    poDropSchema
    poCreateTable
    poDropTable
    poInsert
    poPointGet
    poScan
    poOrderBy ## Sort results by ORDER BY expressions
    poUpdate
    poDelete
    poShowDatabases
    poShowSchemas
    poShowTables
    poShowSpaces
    poCreateSpace
    poDropSpace
    poUseDatabase
    poUseSchema
    poBeginTxn
    poCommitTxn
    poRollbackTxn
    poExplain

  PlanOp* = ref object
    case kind*: PlanOpKind
    of poCreateDatabase:
      cdbName*: string
      cdbIfNotExists*: bool
      cdbReplicas*: Option[int]
      cdbValue*: string            # JSON value to store

    of poDropDatabase:
      ddbName*: string
      ddbIfExists*: bool

    of poCreateSchema:
      csName*: string
      csIfNotExists*: bool
      csReplicas*: Option[int]
      csValue*: string
      csDatabase*: string          # owning database

    of poDropSchema:
      dsName*: string
      dsIfExists*: bool
      dsDatabase*: string

    of poCreateTable:
      ctName*: string
      ctIfNotExists*: bool
      ctValue*: string             # JSON table descriptor
      ctSchema*: string
      ctDatabase*: string
      ctSpaceName*: Option[string] # IN SPACE <name>

    of poDropTable:
      dtName*: string
      dtIfExists*: bool
      dtSchema*: string
      dtDatabase*: string

    of poInsert:
      insTableId*: TableId
      insTableName*: string
      insColumns*: seq[string]     # column names in order
      insPkColumn*: string         # primary key column name
      insPkSpec*: PrimaryKeySpec   # primary key spec for binary encoding
      insRows*: seq[string]        # binary-encoded DataRow objects
      insPkValues*: seq[string]    # binary-encoded primary key values

    of poPointGet:
      pgTableId*: TableId
      pgKey*: string               # binary-encoded primary key value
      pgPkSpec*: PrimaryKeySpec    # primary key spec for decoding
      pgColumns*: seq[string]      # columns to return (empty = all)
      pgAllColumns*: seq[string]   # all table columns for decoding
      pgFilter*: Option[Expr]      # remaining filter after PK extraction (optional)

    of poScan:
      scTableId*: TableId
      scStartKey*: string
      scEndKey*: string
      scLimit*: uint32
      scFilter*: Option[Expr]
      scColumns*: seq[string]      # columns to return (empty = all)
      scAllColumns*: seq[string]   # all table columns for decoding

    of poOrderBy:
      obSortSpecs*: seq[SortSpec]  ## Sort specifications from ORDER BY
      obColumns*: seq[string]      ## Columns to return (passed from scan)
      obAllColumns*: seq[string]   ## All fetched columns for expression evaluation
      obLimit*: uint32             ## LIMIT to apply after sorting (0 = no limit)

    of poUpdate:
      upTableId*: TableId
      upTableName*: string
      upFilter*: Option[Expr]
      upSets*: seq[tuple[col: string, val: Expr]]
      upAllColumns*: seq[string]
      upPkColumn*: string

    of poDelete:
      delTableId*: TableId
      delTableName*: string
      delFilter*: Option[Expr]
      delAllColumns*: seq[string]
      delPkColumn*: string

    of poShowDatabases:
      discard

    of poShowSchemas:
      ssDatabase*: string          # filter by database (empty = current)

    of poShowTables:
      stDatabase*: string          # filter by database (empty = current)
      stSchema*: string            # filter by schema (empty = current)

    of poShowSpaces:
      discard

    of poCreateSpace:
      cspName*: string
      cspReplicas*: int            # 0 = ALL
      cspValue*: string            # JSON value to store

    of poDropSpace:
      dspName*: string

    of poUseDatabase:
      udName*: string

    of poUseSchema:
      usName*: string

    of poBeginTxn:
      btReadOnly*: bool

    of poCommitTxn:
      discard

    of poRollbackTxn:
      discard

    of poExplain:
      exInnerPlan*: Plan           ## the plan being explained

  Plan* = ref object
    ops*: seq[PlanOp]

# ---------------------------------------------------------------------------
# Plan construction helpers
# ---------------------------------------------------------------------------

proc newPlan*(): Plan =
  Plan(ops: @[])

proc add*(p: Plan, op: PlanOp) =
  p.ops.add(op)

# ---------------------------------------------------------------------------
# Planner errors
# ---------------------------------------------------------------------------

type
  PlanError* = object of CatchableError

proc planError(msg: string): ref PlanError =
  newException(PlanError, msg)

# ---------------------------------------------------------------------------
# Table descriptor helpers
# ---------------------------------------------------------------------------

type
  TableDescriptor* = object
    tableId*: TableId
    name*: string
    schema*: string
    database*: string
    columns*: seq[ColDef]
    primaryKey*: seq[string]
    pkSpec*: PrimaryKeySpec ## Primary key spec for binary encoding
    spaceId*: SpaceID

proc findPkColumn*(desc: TableDescriptor): string =
  ## Find the primary key column name. Returns the first PK column or
  ## the first column from the table-level PK constraint.
  if desc.primaryKey.len > 0:
    return desc.primaryKey[0]
  for col in desc.columns:
    if col.primaryKey:
      return col.name
  if desc.columns.len > 0:
    return desc.columns[0].name
  ""

proc columnNames*(desc: TableDescriptor): seq[string] =
  for col in desc.columns:
    result.add(col.name)

# ---------------------------------------------------------------------------
# Primary Key Range Extraction from WHERE Clause
# ---------------------------------------------------------------------------

# Forward declarations (functions defined later in this file)
proc exprToDataRowValue*(e: Expr): DataRowValue
proc dataRowValueToPkValue*(v: DataRowValue, colSpec: tuple[name: string,
    dataType: ColumnDataType, maxLen: int]): PrimaryKeyColumnValue

type
  PkRangeBound* = object
    ## A bound for primary key range scan
    value*: string     ## Encoded primary key value
    isInclusive*: bool ## Whether the bound is inclusive (<= or >=)
    isExact*: bool     ## Whether this is an exact match (pk = value)

  PkRangeInfo* = object
    ## Information extracted from WHERE clause about PK range
    startBound*: Option[PkRangeBound] ## Lower bound (start key)
    endBound*: Option[PkRangeBound]   ## Upper bound (end key)
    exactMatch*: Option[string]       ## Exact PK value for point get
    remainingFilter*: Option[Expr]    ## Remaining conditions after PK extraction
    isPointGet*: bool                 ## True if this is a single-row lookup

proc extractPkValueFromLiteral(expr: Expr, pkSpec: PrimaryKeySpec): Option[string] =
  ## Extract and encode a primary key value from a literal expression.
  ## Returns the encoded PK value, or none if not a valid literal.
  if expr.kind != exLiteral or expr.litValue == nil:
    return none(string)

  let dataVal = exprToDataRowValue(expr)
  if pkSpec.columns.len == 1:
    # Single-column PK
    let colSpec = pkSpec.columns[0]
    var pk: PrimaryKey = @[dataRowValueToPkValue(dataVal, colSpec)]
    return some(encodePrimaryKey(pk, pkSpec))
  else:
    # Composite PK - not supported for simple literal extraction
    # Would need tuple/list expression
    return none(string)

proc extractPkRangeFromCondition(cond: Expr, pkCol: string,
    pkSpec: PrimaryKeySpec): Option[PkRangeInfo] =
  ## Extract PK range info from a single condition.
  ## Handles: pk = value, pk > value, pk >= value, pk < value, pk <= value
  if cond.kind != exBinOp:
    return none(PkRangeInfo)

  let left = cond.binLeft
  let right = cond.binRight

  # Check if condition involves PK column
  var pkLiteral: Expr = nil
  var opKind = cond.binOp

  # pk = value or value = pk
  if left.kind == exColumn and left.colName == pkCol and right.kind == exLiteral:
    pkLiteral = right
  elif right.kind == exColumn and right.colName == pkCol and left.kind == exLiteral:
    pkLiteral = left
    # Flip operator for swapped operands
    case opKind
    of boLt: opKind = boGt
    of boLte: opKind = boGte
    of boGt: opKind = boLt
    of boGte: opKind = boLte
    else: discard # Eq and Neq don't need flipping

  if pkLiteral == nil:
    return none(PkRangeInfo)

  let pkValueOpt = extractPkValueFromLiteral(pkLiteral, pkSpec)
  if pkValueOpt.isNone:
    return none(PkRangeInfo)

  let pkValue = pkValueOpt.get()

  result = some(PkRangeInfo())

  case opKind
  of boEq:
    # Exact match - point get
    result.get().exactMatch = some(pkValue)
    result.get().isPointGet = true
  of boGt:
    # pk > value - exclusive lower bound
    result.get().startBound = some(PkRangeBound(
      value: pkValue, isInclusive: false, isExact: false
    ))
  of boGte:
    # pk >= value - inclusive lower bound
    result.get().startBound = some(PkRangeBound(
      value: pkValue, isInclusive: true, isExact: false
    ))
  of boLt:
    # pk < value - exclusive upper bound
    result.get().endBound = some(PkRangeBound(
      value: pkValue, isInclusive: false, isExact: false
    ))
  of boLte:
    # pk <= value - inclusive upper bound
    result.get().endBound = some(PkRangeBound(
      value: pkValue, isInclusive: true, isExact: false
    ))
  else:
    # Other operators (neq, and, or) - not handled as range
    return none(PkRangeInfo)

proc extractPkRangeFromWhere*(where: Option[Expr], pkCol: string,
    pkSpec: PrimaryKeySpec): PkRangeInfo =
  ## Extract primary key range information from WHERE clause.
  ##
  ## Handles:
  ## - pk = value → PointGet (single row)
  ## - pk > value AND other_cond → Scan with start key + remaining filter
  ## - pk >= value AND pk <= value → Range scan
  ## - pk = value AND other_cond → PointGet with remaining filter
  ##
  ## Returns PkRangeInfo with:
  ## - exactMatch: for point get
  ## - startBound/endBound: for range scan
  ## - remainingFilter: conditions not pushed to key range
  ## - isPointGet: whether this should be a single-row lookup

  result = PkRangeInfo()

  if where.isNone:
    return result

  let w = where.get()

  # Handle AND conditions - extract PK conditions and collect remaining
  if w.kind == exBinOp and w.binOp == boAnd:
    # Split AND into individual conditions
    var pkConditions: seq[Expr] = @[]
    var otherConditions: seq[Expr] = @[]

    # Recursively collect conditions from AND tree
    proc collectAndConditions(expr: Expr, pkCol: string,
        pkConditions: var seq[Expr], otherConditions: var seq[Expr]) =
      if expr.kind == exBinOp and expr.binOp == boAnd:
        collectAndConditions(expr.binLeft, pkCol, pkConditions, otherConditions)
        collectAndConditions(expr.binRight, pkCol, pkConditions, otherConditions)
      else:
        # Single condition - check if it involves PK
        if expr.kind == exBinOp and
           (expr.binLeft.kind == exColumn and expr.binLeft.colName == pkCol or
            expr.binRight.kind == exColumn and expr.binRight.colName == pkCol):
          pkConditions.add(expr)
        else:
          otherConditions.add(expr)

    collectAndConditions(w, pkCol, pkConditions, otherConditions)

    # Process PK conditions
    for pkCond in pkConditions:
      let pkRangeOpt = extractPkRangeFromCondition(pkCond, pkCol, pkSpec)
      if pkRangeOpt.isSome:
        let pkRange = pkRangeOpt.get()

        # Handle exact match (pk = value)
        if pkRange.isPointGet:
          result.exactMatch = pkRange.exactMatch
          result.isPointGet = true
          # Don't combine exact match with range bounds - it's a point get
          break
        elif pkRange.startBound.isSome:
          # Merge start bound (take the tighter one)
          if result.startBound.isNone or
             pkRange.startBound.get().value > result.startBound.get().value:
            result.startBound = pkRange.startBound
        elif pkRange.endBound.isSome:
          # Merge end bound (take the tighter one)
          if result.endBound.isNone or
             pkRange.endBound.get().value < result.endBound.get().value:
            result.endBound = pkRange.endBound

    # Build remaining filter from other conditions
    if otherConditions.len > 0:
      if otherConditions.len == 1:
        result.remainingFilter = some(otherConditions[0])
      else:
        # Reconstruct AND tree
        var combined = otherConditions[0]
        for i in 1..<otherConditions.len:
          combined = Expr(kind: exBinOp, binOp: boAnd,
                          binLeft: combined, binRight: otherConditions[i])
        result.remainingFilter = some(combined)

    return result

  # Handle single condition (not AND)
  let pkRangeOpt = extractPkRangeFromCondition(w, pkCol, pkSpec)
  if pkRangeOpt.isSome:
    result = pkRangeOpt.get()
    return result

  # No PK condition found - full scan with original filter
  result.remainingFilter = where

proc makeScanKeysFromRange*(tableId: TableId, rangeInfo: PkRangeInfo): tuple[
    startKey: string, endKey: string] =
  ## Generate start and end keys for scan from PK range info.
  ## For exact match, both keys are the same (point get).
  ## For range scan, generates appropriate bounds.

  if rangeInfo.isPointGet and rangeInfo.exactMatch.isSome:
    # Point get - single key
    let pkVal = rangeInfo.exactMatch.get()
    result.startKey = encodeDataRowKey(tableId, pkVal)
    result.endKey = result.startKey
    return result

  # Range scan
  if rangeInfo.startBound.isSome:
    let bound = rangeInfo.startBound.get()
    result.startKey = encodeDataRowKey(tableId, bound.value)
    # For exclusive lower bound (>), we need to skip exact match
    # The scan will naturally skip it since we filter rows
  else:
    # No lower bound - start from beginning of table
    result.startKey = encodeDataRowKey(tableId, "")

  if rangeInfo.endBound.isSome:
    let bound = rangeInfo.endBound.get()
    # For upper bound, we need to create a key that includes/excludes the bound
    # Key comparison is lexicographic, so:
    # - For <= (inclusive): scan up to pk + 1 byte (to include pk)
    # - For < (exclusive): scan up to pk (excludes pk)
    if bound.isInclusive:
      # Include the bound by appending a high byte
      result.endKey = encodeDataRowKey(tableId, bound.value & "\xFF")
    else:
      # Exclude the bound - scan up to but not including
      result.endKey = encodeDataRowKey(tableId, bound.value)
  else:
    # No upper bound - scan to end of table
    result.endKey = makeDataRowScanEndKey(tableId)

# ---------------------------------------------------------------------------
# Catalog lookups
# ---------------------------------------------------------------------------

proc columnDataTypeToDataType(cdt: ColumnDataType): DataType =
  ## Convert binary format column type to core DataType
  case cdt
  of cdtInt: dtInt
  of cdtFloat: dtFloat
  of cdtString: dtString
  of cdtBool: dtBool
  of cdtBytes: dtBytes
  of cdtDate: dtDate
  of cdtDateTime: dtDateTime
  of cdtULID: dtULID

proc resolveTable*(client: FractioClient,
    database, schema, tableName: string): Option[TableDescriptor] =
  ## Look up a table descriptor from the system catalog.
  ## Key format: /t/<SYS_TABLES_TABLE_ID>/<database>.<schema>.<tableName>
  let catalogKey = encodeTableKey(SYS_TABLES_TABLE_ID,
      database & "." & schema & "." & tableName)

  let res = client.kvGet(catalogKey)
  if res.isErr or res.val.isNone:
    return none(TableDescriptor)

  let raw = res.val.get()
  let rec = decodeTableRecord(raw)
  var desc = TableDescriptor(
    tableId: rec.tableId,
    name: rec.name,
    schema: rec.schema,
    database: rec.database,
    spaceId: rec.spaceId,
    pkSpec: primaryKeySpecFromTable(rec),
  )
  # Copy primary key columns
  for pk in rec.primaryKey:
    desc.primaryKey.add(pk)
  # Convert columns
  for col in rec.columns:
    var cd = ColDef(name: col.name)
    cd.dataType = columnDataTypeToDataType(col.dataType)
    cd.maxLen = int(col.maxLen)
    cd.primaryKey = (col.flags and 0x01) != 0
    cd.notNull = (col.flags and 0x02) != 0
    desc.columns.add(cd)
  some(desc)

proc genNewTableId*(): TableId =
  ## Generate a new globally unique TableId using ULID.
  ## ULID-based table IDs are globally unique and lexicographically sortable.
  genTableId()

# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

proc dataTypeToString*(dt: DataType): string =
  case dt
  of dtInt: "INT"
  of dtFloat: "FLOAT"
  of dtString: "TEXT"
  of dtBool: "BOOL"
  of dtDate: "DATE"
  of dtDateTime: "DATETIME"
  of dtBytes: "BYTES"
  of dtULID: "ULID"

proc dataTypeToColumnDataType(dt: DataType): ColumnDataType =
  ## Convert core DataType to binary format ColumnDataType
  case dt
  of dtInt: cdtInt
  of dtFloat: cdtFloat
  of dtString: cdtString
  of dtBool: cdtBool
  of dtBytes: cdtBytes
  of dtDate: cdtDate
  of dtDateTime: cdtDateTime
  of dtULID: cdtULID

proc exprToDataRowValue*(e: Expr): DataRowValue =
  ## Convert a literal expression to a DataRowValue.
  if e.kind != exLiteral:
    return newRowValue()
  if e.litValue == nil:
    return newRowValue()
  case e.litValue.kind
  of dtInt: newRowValue(e.litValue.intValue)
  of dtFloat: newRowValue(e.litValue.floatValue)
  of dtString: newRowValue(e.litValue.strValue)
  of dtBool: newRowValue(e.litValue.boolValue)
  else: newRowValue()

proc dataRowValueToPkValue*(v: DataRowValue, colSpec: tuple[name: string,
    dataType: ColumnDataType, maxLen: int]): PrimaryKeyColumnValue =
  ## Convert a DataRowValue to a PrimaryKeyColumnValue for encoding.
  if v.kind == drvkNull:
    case colSpec.dataType
    of cdtInt: result = pkValueFromInt(0, isNull = true)
    of cdtFloat: result = pkValueFromFloat(0.0, isNull = true)
    of cdtString: result = pkValueFromString("", colSpec.maxLen, isNull = true)
    of cdtBool: result = pkValueFromBool(false, isNull = true)
    of cdtBytes: result = PrimaryKeyColumnValue(isNull: true, kind: cdtBytes,
        bytesMaxLen: colSpec.maxLen)
    of cdtDate: result = pkValueFromDate(0, isNull = true)
    of cdtDateTime: result = pkValueFromDateTime(0, isNull = true)
    of cdtULID: result = PrimaryKeyColumnValue(isNull: true, kind: cdtULID)
  else:
    case colSpec.dataType
    of cdtInt: result = pkValueFromInt(v.intVal)
    of cdtFloat: result = pkValueFromFloat(v.floatVal)
    of cdtString: result = pkValueFromString(v.strVal, colSpec.maxLen)
    of cdtBool: result = pkValueFromBool(v.boolVal)
    of cdtBytes:
      var bytes: seq[uint8]
      for c in v.strVal:
        bytes.add(uint8(c))
      result = PrimaryKeyColumnValue(isNull: false, kind: cdtBytes,
          bytesVal: bytes, bytesMaxLen: colSpec.maxLen)
    of cdtDate: result = pkValueFromDate(v.intVal)
    of cdtDateTime: result = pkValueFromDateTime(v.intVal)
    of cdtULID:
      var ulid: array[16, uint8]
      # Assume strVal contains 16-byte binary or parse from string
      for i in 0..<min(v.strVal.len, 16):
        ulid[i] = uint8(v.strVal[i])
      result = pkValueFromULID(ulid)

proc exprToJsonValue*(e: Expr): JsonNode =
  ## Convert a literal expression to a JSON value.
  ## Kept for backward compatibility with some utility functions.
  if e.kind != exLiteral:
    return newJNull()
  if e.litValue == nil:
    return newJNull()
  case e.litValue.kind
  of dtInt: newJInt(e.litValue.intValue)
  of dtFloat: newJFloat(e.litValue.floatValue)
  of dtString: newJString(e.litValue.strValue)
  of dtBool: newJBool(e.litValue.boolValue)
  else: newJNull()

proc exprToWireFilterExpr*(e: Expr): WireFilterExpr =
  ## Convert an SQL Expr to a WireFilterExpr for server-side filtering.
  ## Only handles filter-compatible expression types (literals, columns,
  ## comparison operators, AND/OR, IS NULL, BETWEEN, LIKE).
  ## Arithmetic operators (Add, Sub, Mul, Div, Mod) and Neg are not supported.
  case e.kind
  of exLiteral:
    result = WireFilterExpr(kind: wekLiteral)
    if e.litValue == nil:
      result.litDataType = wdtNull
    else:
      case e.litValue.kind
      of dtInt:
        result.litDataType = wdtInt
        result.litIntVal = e.litValue.intValue
      of dtFloat:
        result.litDataType = wdtFloat
        result.litFloatVal = e.litValue.floatValue
      of dtString:
        result.litDataType = wdtString
        result.litStringVal = e.litValue.strValue
      of dtBool:
        result.litDataType = wdtBool
        result.litBoolVal = e.litValue.boolValue
      else:
        # Other types (Date, DateTime, Bytes, ULID) encoded as null for now
        result.litDataType = wdtNull

  of exColumn:
    result = WireFilterExpr(kind: wekColumn, colName: e.colName)

  of exBinOp:
    result = WireFilterExpr(kind: wekBinOp)
    # Convert BinOpKind to WireBinOp
    case e.binOp
    of boEq: result.binOpKind = wboEq
    of boNeq: result.binOpKind = wboNeq
    of boLt: result.binOpKind = wboLt
    of boLte: result.binOpKind = wboLte
    of boGt: result.binOpKind = wboGt
    of boGte: result.binOpKind = wboGte
    of boAnd: result.binOpKind = wboAnd
    of boOr: result.binOpKind = wboOr
    else:
      # Arithmetic operators - convert to a placeholder (equality with false)
      # This effectively filters out all rows if arithmetic is used in filter
      result = WireFilterExpr(kind: wekLiteral, litDataType: wdtBool,
          litBoolVal: false)
      return result
    result.binLeft = exprToWireFilterExpr(e.binLeft)
    result.binRight = exprToWireFilterExpr(e.binRight)

  of exUnaryOp:
    case e.unaryOp
    of uoNot:
      result = WireFilterExpr(kind: wekUnaryOp, unaryOpKind: wuoNot)
      result.unaryExpr = exprToWireFilterExpr(e.unaryExpr)
    of uoNeg:
      # Negation not supported in filters - return false literal
      result = WireFilterExpr(kind: wekLiteral, litDataType: wdtBool,
          litBoolVal: false)

  of exIsNull:
    result = WireFilterExpr(kind: wekIsNull, isNullNot: e.isNullNot)
    result.isNullExpr = exprToWireFilterExpr(e.isNullExpr)

  of exBetween:
    result = WireFilterExpr(kind: wekBetween, betweenNot: e.betweenNot)
    result.betweenExpr = exprToWireFilterExpr(e.betweenExpr)
    result.betweenLo = exprToWireFilterExpr(e.betweenLo)
    result.betweenHi = exprToWireFilterExpr(e.betweenHi)

  of exLike:
    result = WireFilterExpr(kind: wekLike, likeNot: e.likeNot)
    result.likeExpr = exprToWireFilterExpr(e.likeExpr)
    result.likePattern = exprToWireFilterExpr(e.likePattern)

  of exIn, exParam, exList, exStar:
    # Not supported in wire filters - return true literal (no filtering)
    # The client will need to apply these filters locally
    result = WireFilterExpr(kind: wekLiteral, litDataType: wdtBool,
        litBoolVal: true)

# ---------------------------------------------------------------------------
# Statement planners
# ---------------------------------------------------------------------------

proc planCreateDatabase(stmt: Stmt): Plan =
  let plan = newPlan()
  # Use binary encoding for DatabaseRecord
  let rec = DatabaseRecord(
    name: stmt.cdbName,
    createdAtNs: nowNs()
  )
  plan.add(PlanOp(kind: poCreateDatabase,
    cdbName: stmt.cdbName,
    cdbIfNotExists: stmt.cdbIfNotExists,
    cdbReplicas: stmt.cdbReplicas,
    cdbValue: encode(rec),
  ))
  plan

proc planDropDatabase(stmt: Stmt): Plan =
  let plan = newPlan()
  plan.add(PlanOp(kind: poDropDatabase,
    ddbName: stmt.ddbName,
    ddbIfExists: stmt.ddbIfExists,
  ))
  plan

proc planCreateSchema(stmt: Stmt, database: string): Plan =
  let plan = newPlan()
  # Use binary encoding for SchemaRecord
  let rec = SchemaRecord(
    name: stmt.csName,
    database: database,
    createdAtNs: nowNs()
  )
  plan.add(PlanOp(kind: poCreateSchema,
    csName: stmt.csName,
    csIfNotExists: stmt.csIfNotExists,
    csReplicas: stmt.csReplicas,
    csValue: encode(rec),
    csDatabase: database,
  ))
  plan

proc planDropSchema(stmt: Stmt, database: string): Plan =
  let plan = newPlan()
  plan.add(PlanOp(kind: poDropSchema,
    dsName: stmt.dsName,
    dsIfExists: stmt.dsIfExists,
    dsDatabase: database,
  ))
  plan

proc planCreateTable(stmt: Stmt, client: FractioClient,
    database, schema: string): Plan =
  let plan = newPlan()

  # Build column definitions in binary format
  var columns: seq[ColumnDefBin]
  for col in stmt.ctColumns:
    var flags: uint8 = 0
    if col.primaryKey: flags = flags or 0x01
    if col.notNull: flags = flags or 0x02
    if col.unique: flags = flags or 0x04
    columns.add(ColumnDefBin(
      name: col.name,
      dataType: dataTypeToColumnDataType(col.dataType),
      maxLen: uint16(col.maxLen),
      flags: flags
    ))

  # Determine primary key
  var pk: seq[string]
  if stmt.ctPrimaryKey.len > 0:
    pk = stmt.ctPrimaryKey
  else:
    for col in stmt.ctColumns:
      if col.primaryKey:
        pk.add(col.name)

  # Generate globally unique TableId using ULID
  let tableId = genNewTableId()

  # Note: spaceId will be assigned at execution time - use placeholder
  let placeholderSpaceId = zeroSpaceID()
  let rec = TableRecord(
    tableId: tableId,
    name: stmt.ctTable,
    schema: schema,
    database: database,
    spaceId: placeholderSpaceId, # Will be resolved at execution time
    primaryKey: pk,
    columns: columns
  )

  plan.add(PlanOp(kind: poCreateTable,
    ctName: stmt.ctTable,
    ctIfNotExists: stmt.ctIfNotExists,
    ctValue: encode(rec),
    ctSchema: schema,
    ctDatabase: database,
    ctSpaceName: stmt.ctSpaceName,
  ))
  plan

proc planInsert(stmt: Stmt, client: FractioClient,
    database, schema: string): Plan =
  let plan = newPlan()
  let descOpt = resolveTable(client, database, schema, stmt.intoTable)
  if descOpt.isNone:
    raise planError(&"table '{stmt.intoTable}' not found")
  let desc = descOpt.get()
  let pkCol = findPkColumn(desc)
  let colNames = if stmt.intoCols.len > 0: stmt.intoCols
                 else: columnNames(desc)

  var rows: seq[string]
  var pkValues: seq[string]

  for row in stmt.intoValues:
    var dataRow = newDataRow()
    for i, expr in row:
      if i < colNames.len:
        dataRow[colNames[i]] = exprToDataRowValue(expr)
    rows.add(encodeDataRow(dataRow))

    # Build binary primary key
    var pk: PrimaryKey
    for pkColName in desc.primaryKey:
      let colSpec = desc.pkSpec.columns[desc.pkSpec.columns.findIt(it.name == pkColName)]
      let dataVal = if dataRow.hasColumn(pkColName): dataRow[
          pkColName] else: newRowValue()
      pk.add(dataRowValueToPkValue(dataVal, colSpec))
    pkValues.add(encodePrimaryKey(pk, desc.pkSpec))

  plan.add(PlanOp(kind: poInsert,
    insTableId: desc.tableId,
    insTableName: desc.name,
    insColumns: colNames,
    insPkColumn: pkCol,
    insPkSpec: desc.pkSpec,
    insRows: rows,
    insPkValues: pkValues,
  ))
  plan

proc planSelect(stmt: Stmt, client: FractioClient,
    database, schema: string): Plan =
  let plan = newPlan()
  let descOpt = resolveTable(client, database, schema, stmt.selFrom)
  if descOpt.isNone:
    raise planError(&"table '{stmt.selFrom}' not found")
  let desc = descOpt.get()
  let allCols = columnNames(desc)

  # Determine requested columns
  var reqCols: seq[string]
  if stmt.selCols.len == 1 and stmt.selCols[0].expr.kind == exStar:
    reqCols = allCols
  else:
    for sc in stmt.selCols:
      if sc.expr.kind == exColumn:
        reqCols.add(sc.expr.colName)
      elif sc.alias.len > 0:
        reqCols.add(sc.alias)
      else:
        reqCols.add("?")

  # Extract PK range information from WHERE clause
  let pkCol = findPkColumn(desc)
  let pkRangeInfo = extractPkRangeFromWhere(stmt.selWhere, pkCol, desc.pkSpec)

  # Extract LIMIT value
  var limit: uint32 = 0
  if stmt.selLimit.isSome:
    let limExpr = stmt.selLimit.get()
    if limExpr.kind == exLiteral and limExpr.litValue != nil and
       limExpr.litValue.kind == dtInt:
      limit = uint32(limExpr.litValue.intValue)

  # Convert ORDER BY items to SortSpecs and determine sort columns
  var sortSpecs: seq[SortSpec] = @[]
  var sortCols: seq[string] = @[] # Columns needed for sorting
  if stmt.selOrderBy.len > 0:
    sortSpecs = orderItemsToSortSpecs(stmt.selOrderBy, allCols)
    # Extract column names referenced in ORDER BY expressions
    for item in stmt.selOrderBy:
      if item.expr.kind == exColumn:
        let colName = item.expr.colName
        # Add to sortCols if not already in reqCols (avoid duplicates)
        if colName notin reqCols and colName notin sortCols:
          sortCols.add(colName)

  # Columns to fetch from storage = requested + ORDER BY referenced columns
  let fetchCols = reqCols & sortCols

  # Generate plan based on PK range info
  if pkRangeInfo.isPointGet and pkRangeInfo.exactMatch.isSome:
    # Point get: single row lookup with optional remaining filter
    # ORDER BY on a single row is trivial - still add the op for consistency
    let pkVal = pkRangeInfo.exactMatch.get()
    plan.add(PlanOp(kind: poPointGet,
      pgTableId: desc.tableId,
      pgKey: pkVal,
      pgPkSpec: desc.pkSpec,
      pgColumns: fetchCols, # Fetch columns needed for ORDER BY
      pgAllColumns: allCols,
      pgFilter: pkRangeInfo.remainingFilter, # Apply remaining conditions to row
    ))
    # ORDER BY is applied after point get for consistency
    if sortSpecs.len > 0:
      plan.add(PlanOp(kind: poOrderBy,
        obSortSpecs: sortSpecs,
        obColumns: reqCols, # Output columns (original requested)
        obAllColumns: fetchCols, # Columns in the rows (for expression evaluation)
      ))
    return plan

  # Range scan (with optimized key bounds if available)
  let (startKey, endKey) = makeScanKeysFromRange(desc.tableId, pkRangeInfo)

  # When ORDER BY is present, fetch all rows for sorting, then apply LIMIT after
  # When no ORDER BY, apply LIMIT during scan for efficiency
  let scanLimit = if sortSpecs.len > 0: 0'u32 else: limit

  plan.add(PlanOp(kind: poScan,
    scTableId: desc.tableId,
    scStartKey: startKey,
    scEndKey: endKey,
    scLimit: scanLimit,
    scFilter: pkRangeInfo.remainingFilter, # Only non-PK conditions remain
    scColumns: fetchCols, # Fetch columns needed for ORDER BY
    scAllColumns: allCols,
  ))

  # Add ORDER BY plan op if specified
  if sortSpecs.len > 0:
    plan.add(PlanOp(kind: poOrderBy,
      obSortSpecs: sortSpecs,
      obColumns: reqCols, # Output columns (original requested)
      obAllColumns: fetchCols, # Columns in the rows (for expression evaluation)
      obLimit: limit, # Apply LIMIT after sorting
    ))

  plan

proc planUpdate(stmt: Stmt, client: FractioClient,
    database, schema: string): Plan =
  let plan = newPlan()
  let descOpt = resolveTable(client, database, schema, stmt.updTable)
  if descOpt.isNone:
    raise planError(&"table '{stmt.updTable}' not found")
  let desc = descOpt.get()

  plan.add(PlanOp(kind: poUpdate,
    upTableId: desc.tableId,
    upTableName: desc.name,
    upFilter: stmt.updWhere,
    upSets: stmt.updSets,
    upAllColumns: columnNames(desc),
    upPkColumn: findPkColumn(desc),
  ))
  plan

proc planDelete(stmt: Stmt, client: FractioClient,
    database, schema: string): Plan =
  let plan = newPlan()
  let descOpt = resolveTable(client, database, schema, stmt.delTable)
  if descOpt.isNone:
    raise planError(&"table '{stmt.delTable}' not found")
  let desc = descOpt.get()

  plan.add(PlanOp(kind: poDelete,
    delTableId: desc.tableId,
    delTableName: desc.name,
    delFilter: stmt.delWhere,
    delAllColumns: columnNames(desc),
    delPkColumn: findPkColumn(desc),
  ))
  plan

# ---------------------------------------------------------------------------
# Space planners
# ---------------------------------------------------------------------------

proc planCreateSpace(stmt: Stmt): Plan =
  let plan = newPlan()
  # Use binary encoding for SpaceRecord
  # spaceId will be assigned at execution time - use placeholder
  var placeholderSpaceId: SpaceID
  let rec = SpaceRecord(
    spaceId: placeholderSpaceId, # Will be assigned at execution time
    name: stmt.csSpaceName,
    replicas: int32(stmt.csSpaceReplicas),
    groupCount: 0,
    groupIds: @[],
    oldGroupIds: @[],
    rebalancing: false,
    createdAtNs: nowNs()
  )
  plan.add(PlanOp(kind: poCreateSpace,
    cspName: stmt.csSpaceName,
    cspReplicas: stmt.csSpaceReplicas,
    cspValue: encode(rec),
  ))
  plan

proc planDropSpace(stmt: Stmt): Plan =
  let plan = newPlan()
  plan.add(PlanOp(kind: poDropSpace, dspName: stmt.dsSpaceName))
  plan

# ---------------------------------------------------------------------------
# EXPLAIN formatting
# ---------------------------------------------------------------------------

proc formatExpr*(e: Expr): string =
  ## Format an expression for EXPLAIN output.
  case e.kind
  of exLiteral:
    if e.litValue == nil: return "NULL"
    case e.litValue.kind
    of dtInt: return $e.litValue.intValue
    of dtFloat: return $e.litValue.floatValue
    of dtString: return "'" & e.litValue.strValue & "'"
    of dtBool: return $e.litValue.boolValue
    else: return "?"
  of exColumn:
    if e.colTable.len > 0: return e.colTable & "." & e.colName
    return e.colName
  of exBinOp:
    let opStr = case e.binOp
      of boEq: "="
      of boNeq: "<>"
      of boLt: "<"
      of boLte: "<="
      of boGt: ">"
      of boGte: ">="
      of boAnd: "AND"
      of boOr: "OR"
      of boAdd: "+"
      of boSub: "-"
      of boMul: "*"
      of boDiv: "/"
      of boMod: "%"
    return formatExpr(e.binLeft) & " " & opStr & " " & formatExpr(e.binRight)
  of exUnaryOp:
    case e.unaryOp
    of uoNot: return "NOT " & formatExpr(e.unaryExpr)
    of uoNeg: return "-" & formatExpr(e.unaryExpr)
  of exIsNull:
    if e.isNullNot: return formatExpr(e.isNullExpr) & " IS NOT NULL"
    return formatExpr(e.isNullExpr) & " IS NULL"
  of exStar: return "*"
  else: return "?"

proc formatPlanOp*(op: PlanOp): string =
  ## Format a single PlanOp as a human-readable string.
  case op.kind
  of poCreateDatabase: &"CreateDatabase name={op.cdbName}"
  of poDropDatabase: &"DropDatabase name={op.ddbName}"
  of poCreateSchema: &"CreateSchema name={op.csDatabase}.{op.csName}"
  of poDropSchema: &"DropSchema name={op.dsDatabase}.{op.dsName}"
  of poCreateTable: &"CreateTable name={op.ctDatabase}.{op.ctSchema}.{op.ctName}"
  of poDropTable: &"DropTable name={op.dtDatabase}.{op.dtSchema}.{op.dtName}"
  of poInsert:
    &"Insert table={op.insTableName} (id={op.insTableId}) rows={op.insRows.len}"
  of poPointGet:
    &"PointGet table_id={op.pgTableId} key={op.pgKey} cols={op.pgColumns}"
  of poScan:
    var s = &"Scan table_id={op.scTableId} cols={op.scColumns}"
    if op.scFilter.isSome:
      s &= &" filter=({formatExpr(op.scFilter.get())})"
    if op.scLimit > 0:
      s &= &" limit={op.scLimit}"
    s
  of poOrderBy:
    var s = &"OrderBy specs=[{formatSortSpecs(op.obSortSpecs)}] cols={op.obColumns}"
    if op.obLimit > 0:
      s &= &" limit={op.obLimit}"
    s
  of poUpdate:
    var s = &"Update table={op.upTableName} (id={op.upTableId})"
    if op.upFilter.isSome:
      s &= &" filter=({formatExpr(op.upFilter.get())})"
    s &= &" set=[{op.upSets.len} cols]"
    s
  of poDelete:
    var s = &"Delete table={op.delTableName} (id={op.delTableId})"
    if op.delFilter.isSome:
      s &= &" filter=({formatExpr(op.delFilter.get())})"
    s
  of poShowDatabases: "ShowDatabases"
  of poShowSchemas: &"ShowSchemas db={op.ssDatabase}"
  of poShowTables: &"ShowTables db={op.stDatabase} schema={op.stSchema}"
  of poShowSpaces: "ShowSpaces"
  of poCreateSpace: &"CreateSpace name={op.cspName} replicas={op.cspReplicas}"
  of poDropSpace: &"DropSpace name={op.dspName}"
  of poUseDatabase: &"UseDatabase name={op.udName}"
  of poUseSchema: &"UseSchema name={op.usName}"
  of poBeginTxn: &"BeginTxn readOnly={op.btReadOnly}"
  of poCommitTxn: "CommitTxn"
  of poRollbackTxn: "RollbackTxn"
  of poExplain: "Explain"

proc formatPlan*(plan: Plan): string =
  ## Format a Plan as a multi-line EXPLAIN output.
  var lines: seq[string]
  for i, op in plan.ops:
    lines.add(formatPlanOp(op))
  lines.join("\n")

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

proc planStatement*(stmt: Stmt, client: FractioClient,
    database: string = "default",
    schema: string = "public"): Plan =
  ## Translate a Stmt AST into a Plan (sequence of KV operations).
  case stmt.kind
  of stmtCreateDatabase: planCreateDatabase(stmt)
  of stmtDropDatabase: planDropDatabase(stmt)
  of stmtCreateSchema: planCreateSchema(stmt, database)
  of stmtDropSchema: planDropSchema(stmt, database)
  of stmtCreateTable: planCreateTable(stmt, client, database, schema)
  of stmtDropTable:
    let plan = newPlan()
    plan.add(PlanOp(kind: poDropTable,
      dtName: stmt.dtTable,
      dtIfExists: stmt.dtIfExists,
      dtSchema: schema,
      dtDatabase: database,
    ))
    plan
  of stmtInsert: planInsert(stmt, client, database, schema)
  of stmtSelect: planSelect(stmt, client, database, schema)
  of stmtUpdate: planUpdate(stmt, client, database, schema)
  of stmtDelete: planDelete(stmt, client, database, schema)
  of stmtShowDatabases:
    let plan = newPlan()
    plan.add(PlanOp(kind: poShowDatabases))
    plan
  of stmtShowSchemas:
    let plan = newPlan()
    let db = if stmt.showSchemasDb.len > 0: stmt.showSchemasDb else: database
    plan.add(PlanOp(kind: poShowSchemas, ssDatabase: db))
    plan
  of stmtShowTables:
    let plan = newPlan()
    let db = if stmt.showTablesDb.len > 0: stmt.showTablesDb else: database
    let sc = if stmt.showTablesSchema.len >
        0: stmt.showTablesSchema else: schema
    plan.add(PlanOp(kind: poShowTables, stDatabase: db, stSchema: sc))
    plan
  of stmtCreateSpace: planCreateSpace(stmt)
  of stmtDropSpace: planDropSpace(stmt)
  of stmtShowSpaces:
    let plan = newPlan()
    plan.add(PlanOp(kind: poShowSpaces))
    plan
  of stmtUseDatabase:
    let plan = newPlan()
    plan.add(PlanOp(kind: poUseDatabase, udName: stmt.useDbName))
    plan
  of stmtUseSchema:
    let plan = newPlan()
    plan.add(PlanOp(kind: poUseSchema, usName: stmt.useSchemaName))
    plan
  of stmtBegin:
    let plan = newPlan()
    plan.add(PlanOp(kind: poBeginTxn, btReadOnly: stmt.beginReadOnly))
    plan
  of stmtCommit:
    let plan = newPlan()
    plan.add(PlanOp(kind: poCommitTxn))
    plan
  of stmtRollback:
    let plan = newPlan()
    plan.add(PlanOp(kind: poRollbackTxn))
    plan
  of stmtExplain:
    let innerPlan = planStatement(stmt.explainStmt, client, database, schema)
    let plan = newPlan()
    plan.add(PlanOp(kind: poExplain, exInnerPlan: innerPlan))
    plan
