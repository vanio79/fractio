# SQL Planner for Fractio
#
# Translates a Stmt AST into a Plan — a sequence of KV operations.
# The planner resolves table names to table IDs via catalog lookups
# and generates the appropriate key encodings for reads/writes.

import std/[options, json, strutils, strformat, sequtils, times]
import ./ast
import ./data_row
import ../distributed/meta/system_tables
import ../distributed/meta/system_schemas
import ../distributed/sharedtimer/timeprovider
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

  OrderByOptimization* = enum
    ## Optimization type for ORDER BY based on primary key ordering
    oboNone        ## No optimization - use full sort algorithm
    oboPkAscMatch  ## Data already sorted by PK ASC - skip sorting
    oboPkDescMatch ## Data sorted by PK, needs reverse - use streaming reverse
    oboTopK        ## ORDER BY + LIMIT: use bounded top-K heap instead of full sort

  PlanOp* = ref object
    case kind*: PlanOpKind
    of poCreateDatabase:
      cdbName*: string
      cdbIfNotExists*: bool
      cdbReplicas*: Option[int]
      cdbValue*: string                # JSON value to store

    of poDropDatabase:
      ddbName*: string
      ddbIfExists*: bool

    of poCreateSchema:
      csName*: string
      csIfNotExists*: bool
      csReplicas*: Option[int]
      csValue*: string
      csDatabase*: string              # owning database

    of poDropSchema:
      dsName*: string
      dsIfExists*: bool
      dsDatabase*: string

    of poCreateTable:
      ctName*: string
      ctIfNotExists*: bool
      ctValue*: string                 # JSON table descriptor
      ctSchema*: string
      ctDatabase*: string
      ctSpaceName*: Option[string]     # IN SPACE <name>
      ctColumns*: seq[ColumnDefBin]    # column definitions for sys.columns
      ctTableId*: TableId              # generated table id

    of poDropTable:
      dtName*: string
      dtIfExists*: bool
      dtSchema*: string
      dtDatabase*: string

    of poInsert:
      insTableId*: TableId
      insTableName*: string
      insColumns*: seq[string]         # column names in order
      insPkColumn*: string             # primary key column name
      insPkSpec*: PrimaryKeySpec       # primary key spec for binary encoding
      insRows*: seq[string]            # binary-encoded DataRow objects
      insPkValues*: seq[string]        # binary-encoded primary key values

    of poPointGet:
      pgTableId*: TableId
      pgKey*: string                   # binary-encoded primary key value
      pgPkSpec*: PrimaryKeySpec        # primary key spec for decoding
      pgColumns*: seq[string]          # columns to return (empty = all)
      pgAllColumns*: seq[string]       # all table columns for decoding
      pgFilter*: Option[Expr]          # remaining filter after PK extraction (optional)
      pgKeyEncoding*: TableKeyEncoding # key encoding strategy for this table

    of poScan:
      scTableId*: TableId
      scStartKey*: string
      scEndKey*: string
      scLimit*: uint32
      scReverse*: bool ## true = scan in reverse key order (for PK DESC + LIMIT)
      scFilter*: Option[Expr]
      scColumns*: seq[string]          # columns to return (empty = all)
      scAllColumns*: seq[string]       # all table columns for decoding
      scKeyEncoding*: TableKeyEncoding # key encoding strategy for this table
      scTopK*: Option[WireTopKSpec]    ## Tier-3b: server-side top-K heap
                                      ## pushdown. When set, each group server runs a bounded top-K heap
                                      ## locally and ships only the K winners over the wire. Used for
                                      ## `ORDER BY non_pk_col LIMIT K` to cut wire traffic and client-side
                                      ## decode work from O(N) to O(K). When `none`, no server-side top-K.

    of poOrderBy:
      obSortSpecs*: seq[SortSpec]      ## Sort specifications from ORDER BY
      obColumns*: seq[string]          ## Columns to return (passed from scan)
      obAllColumns*: seq[string]       ## All fetched columns for expression evaluation
      obLimit*: uint32                 ## LIMIT to apply after sorting (0 = no limit)
      obOptimization*: OrderByOptimization ## Optimization type for PK-based sorting
      obServerTopK*: bool              ## Tier-3b: true if the server already ran
                            ## the top-K heap (each group returned ≤K candidates). When true, the
                            ## executor just merges the K×Ngroups candidates via the k-way merge
                            ## (no client-side heap needed). When false, the executor runs the
                                       ## top-K heap locally on the streamed rows.

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
      ssDatabase*: string              # filter by database (empty = current)

    of poShowTables:
      stDatabase*: string              # filter by database (empty = current)
      stSchema*: string                # filter by schema (empty = current)

    of poShowSpaces:
      discard

    of poCreateSpace:
      cspName*: string
      cspReplicas*: int                # 0 = ALL
      cspValue*: string                # JSON value to store

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
      exInnerPlan*: Plan               ## the plan being explained

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
    pkSpec*: PrimaryKeySpec        ## Primary key spec for binary encoding
    spaceId*: SpaceID
    keyEncoding*: TableKeyEncoding ## Key encoding strategy for this table

proc findPkColumn*(desc: TableDescriptor): string =
  ## Find the primary key column name. Returns the first PK column or
  ## the first column from the table-level PK constraint.
  ## For composite PKs, returns only the first column.
  if desc.primaryKey.len > 0:
    return desc.primaryKey[0]
  for col in desc.columns:
    if col.primaryKey:
      return col.name
  if desc.columns.len > 0:
    return desc.columns[0].name
  ""

proc findPkColumns*(desc: TableDescriptor): seq[string] =
  ## Find all primary key column names for composite PK support.
  ## Returns ordered sequence of PK columns.
  if desc.primaryKey.len > 0:
    return desc.primaryKey
  var pkCols: seq[string] = @[]
  for col in desc.columns:
    if col.primaryKey:
      pkCols.add(col.name)
  if pkCols.len > 0:
    return pkCols
  # Fallback: first column as single PK
  if desc.columns.len > 0:
    return @[desc.columns[0].name]
  @[]

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

proc makeScanKeysFromRange*(tableId: TableId, rangeInfo: PkRangeInfo,
    keyEncoding: TableKeyEncoding = tkeDataRow): tuple[
        startKey: string, endKey: string] =
  ## Generate start and end keys for scan from PK range info.
  ## For exact match, both keys are the same (point get).
  ## For range scan, generates appropriate bounds.
  ##
  ## Tables with tkeSystemTable encoding use encodeTableKey (no "d/" prefix).
  ## Tables with tkeDataRow encoding use encodeDataRowScanBound (with "d/" prefix but no groupId).
  ##
  ## These are table-wide scan bounds. For multi-group tables, the client
  ## narrows them to per-group bounds using narrowScanBoundsToGroup().

  let isSysTable = keyEncoding == tkeSystemTable

  if rangeInfo.isPointGet and rangeInfo.exactMatch.isSome:
    # Point get - single key
    let pkVal = rangeInfo.exactMatch.get()
    if isSysTable:
      result.startKey = encodeTableKey(tableId, pkVal)
    else:
      result.startKey = encodeDataRowScanBound(tableId, pkVal)
    result.endKey = result.startKey
    return result

  # Range scan
  if rangeInfo.startBound.isSome:
    let bound = rangeInfo.startBound.get()
    if isSysTable:
      result.startKey = encodeTableKey(tableId, bound.value)
    else:
      result.startKey = encodeDataRowScanBound(tableId, bound.value)
    # For exclusive lower bound (>), we need to skip exact match
    # The scan will naturally skip it since we filter rows
  else:
    # No lower bound - start from beginning of table
    if isSysTable:
      result.startKey = encodeTableKey(tableId, "")
    else:
      result.startKey = encodeDataRowScanBound(tableId, "")

  if rangeInfo.endBound.isSome:
    let bound = rangeInfo.endBound.get()
    # For upper bound, we need to create a key that includes/excludes the bound
    # Key comparison is lexicographic, so:
    # - For <= (inclusive): scan up to pk + 1 byte (to include pk)
    # - For < (exclusive): scan up to pk (excludes pk)
    if bound.isInclusive:
      # Include the bound by appending a high byte
      if isSysTable:
        result.endKey = encodeTableKey(tableId, bound.value & "\xFF")
      else:
        result.endKey = encodeDataRowScanBound(tableId, bound.value & "\xFF")
    else:
      # Exclude the bound - scan up to but not including
      if isSysTable:
        result.endKey = encodeTableKey(tableId, bound.value)
      else:
        result.endKey = encodeDataRowScanBound(tableId, bound.value)
  else:
    # No upper bound - scan to end of table
    if isSysTable:
      result.endKey = makeScanEndKey(tableId)
    else:
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

# ---------------------------------------------------------------------------
# System table descriptors for sys schema queries
# ---------------------------------------------------------------------------

proc sysColDefToColDef(sysCol: SysColDef): ColDef =
  ## Convert a SysColDef (self-contained system table column def) to a planner ColDef.
  ColDef(
    name: sysCol.name,
    dataType: sysCol.dataType,
    maxLen: sysCol.maxLen,
    notNull: sysCol.notNull,
    primaryKey: sysCol.primaryKey,
    unique: false,
    defaultExpr: none(Expr)
  )

proc sysPkSpecToPrimaryKeySpec(sysPk: SysPrimaryKeySpec): PrimaryKeySpec =
  ## Convert a SysPrimaryKeySpec to a PrimaryKeySpec.
  PrimaryKeySpec(columns: sysPk.columns)

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
    keyEncoding: rec.keyEncoding,
  )
  # Copy primary key columns
  for pk in rec.primaryKey:
    desc.primaryKey.add(pk)

  # Fetch columns from sys.columns
  var colBins: seq[ColumnDefBin] = @[]
  let colStart = encodeTableKey(SYS_COLUMNS_TABLE_ID, $(rec.tableId) & "/")
  let colEnd = encodeTableKey(SYS_COLUMNS_TABLE_ID, $(rec.tableId) & "/{")
  let colScan = client.kvScan(colStart, colEnd, 0)
  if colScan.isOk:
    for entry in colScan.val:
      try:
        let colRec = decodeColumnRecord(entry.value)
        var cd = ColDef(name: colRec.name)
        cd.dataType = columnDataTypeToDataType(colRec.dataType)
        cd.maxLen = int(colRec.maxLen)
        cd.primaryKey = (colRec.flags and 0x01) != 0
        cd.notNull = (colRec.flags and 0x02) != 0
        desc.columns.add(cd)
        colBins.add(ColumnDefBin(
          name: colRec.name,
          dataType: colRec.dataType,
          maxLen: colRec.maxLen,
          flags: colRec.flags
        ))
      except ValueError:
        discard # skip malformed column record

  # Build pkSpec from columns
  desc.pkSpec = primaryKeySpecFromTable(rec, colBins)
  some(desc)

proc resolveQualifiedTableRef*(client: FractioClient,
    defaultDatabase, defaultSchema: string,
    tableRef: TableRef): Option[TableDescriptor] =
  ## Resolve a qualified table reference to a table descriptor.
  ## Handles:
  ##   - "table" (uses default database/schema)
  ##   - "schema.table" (uses default database, explicit schema)
  ##   - "database.schema.table" (fully qualified)
  ##
  ## System tables and user tables are resolved identically via the
  ## sys.tables catalog. Every table (including sys.*) must have an entry.
  ##
  ## The "sys" schema is special: it is a virtual schema accessible from any
  ## database, and all system table entries in sys.tables use database="sys".
  ## When the target schema is "sys", we must look up the catalog using
  ## database="sys" rather than the current default database.

  # Resolve database and schema from tableRef or defaults
  let dbName = if tableRef.database != "": tableRef.database else: defaultDatabase
  let scName = if tableRef.schema != "": tableRef.schema else: defaultSchema

  # System tables live in the "sys" database namespace regardless of
  # the current database. The "sys" schema is virtual and accessible
  # from any database.
  let catalogDbName = if scName == "sys": "sys" else: dbName

  resolveTable(client, catalogDbName, scName, tableRef.table)

proc genNewTableId*(timeProvider: TimeProvider = nil): TableId =
  ## Generate a new globally unique TableId using ULID.
  ## ULID-based table IDs are globally unique and lexicographically sortable.
  let tsNs = if timeProvider != nil: timeProvider.now()
             else:
               let t = getTime()
               t.toUnix * 1_000_000_000 + t.nanosecond.int64
  genTableId(tsNs)

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

proc planCreateDatabase(stmt: Stmt, timeProvider: TimeProvider = nil): Plan =
  let plan = newPlan()
  # Use binary encoding for DatabaseRecord
  let rec = DatabaseRecord(
    name: stmt.cdbName,
    createdAtNs: nowNs(timeProvider)
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

proc planCreateSchema(stmt: Stmt, database: string,
    timeProvider: TimeProvider = nil): Plan =
  let plan = newPlan()
  # Use binary encoding for SchemaRecord
  let rec = SchemaRecord(
    name: stmt.csName,
    database: database,
    createdAtNs: nowNs(timeProvider)
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

  # Resolve database and schema from tableRef or defaults
  let dbName = if stmt.ctTableRef.database !=
      "": stmt.ctTableRef.database else: database
  let scName = if stmt.ctTableRef.schema !=
      "": stmt.ctTableRef.schema else: schema
  let tableName = stmt.ctTableRef.table

  # Auto-resolve space name: if the database part matches a known space name,
  # automatically assign the table to that space. This allows
  # "CREATE TABLE myspace.public.users (...)" to work without "IN SPACE myspace".
  var spaceName = stmt.ctSpaceName
  if spaceName.isNone and dbName != "" and client != nil:
    # Look up the database name in sys.spaces to see if it's a space name
    let spaceKey = encodeTableKey(SYS_SPACES_TABLE_ID, "")
    let spaceEnd = makeScanEndKey(SYS_SPACES_TABLE_ID)
    let spaceScan = client.kvScan(spaceKey, spaceEnd, 0)
    if spaceScan.isOk:
      for entry in spaceScan.val:
        let rec = decodeSpaceRecord(entry.value)
        if rec.name == dbName:
          spaceName = some(dbName)
          break

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
    name: tableName,
    schema: scName,
    database: dbName,
    spaceId: placeholderSpaceId, # Will be resolved at execution time
    primaryKey: pk,
    keyEncoding: tkeDataRow
  )

  plan.add(PlanOp(kind: poCreateTable,
    ctName: tableName,
    ctIfNotExists: stmt.ctIfNotExists,
    ctValue: encode(rec),
    ctSchema: scName,
    ctDatabase: dbName,
    ctSpaceName: spaceName,
    ctColumns: columns,
    ctTableId: tableId,
  ))
  plan

proc planInsert(stmt: Stmt, client: FractioClient,
    database, schema: string): Plan =
  let plan = newPlan()
  let descOpt = resolveQualifiedTableRef(client, database, schema,
      stmt.intoTableRef)
  if descOpt.isNone:
    raise planError(&"table '{stmt.intoTableRef.fullName()}' not found")
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

proc detectOrderByPkOptimization*(orderItems: seq[OrderItem],
                                  pkColumns: seq[string]): OrderByOptimization =
  ## Detect if ORDER BY matches PK ordering for optimization.
  ## Returns optimization type:
  ## - oboPkAscMatch: ORDER BY PK ASC (data already sorted)
  ## - oboPkDescMatch: ORDER BY PK DESC (needs reverse, memory-limited)
  ## - oboNone: No optimization possible
  ##
  ## Supports both single-column and composite primary keys.
  ## ORDER BY must match all PK columns in exact order with same direction.
  ## Complex expressions require full sorting.
  if orderItems.len == 0 or pkColumns.len == 0:
    return oboNone

  # ORDER BY must have exactly the same number of columns as PK
  if orderItems.len != pkColumns.len:
    return oboNone

  # Check each ORDER BY item matches corresponding PK column
  var allAsc = true
  var allDesc = true
  for i, item in orderItems:
    # Must be a simple column reference, not an expression
    if item.expr.kind != exColumn:
      return oboNone

    # Must match PK column in exact order
    if item.expr.colName != pkColumns[i]:
      return oboNone

    # Track direction consistency
    if item.desc:
      allAsc = false
    else:
      allDesc = false

  # All directions must be the same
  if allAsc:
    return oboPkAscMatch # Data is already sorted
  elif allDesc:
    return oboPkDescMatch # Data needs to be reversed
  else:
    return oboNone # Mixed directions require full sort

proc planSelect(stmt: Stmt, client: FractioClient,
    database, schema: string): Plan =
  let plan = newPlan()
  let descOpt = resolveQualifiedTableRef(client, database, schema, stmt.selFrom)
  if descOpt.isNone:
    raise planError(&"table '{stmt.selFrom.fullName()}' not found")
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
  let pkColumns = findPkColumns(desc) # All PK columns for optimization detection
  let pkRangeInfo = extractPkRangeFromWhere(stmt.selWhere, pkCol, desc.pkSpec)

  # Extract LIMIT value
  var limit: uint32 = 0
  if stmt.selLimit.isSome:
    let limExpr = stmt.selLimit.get()
    if limExpr.kind == exLiteral and limExpr.litValue != nil and
       limExpr.litValue.kind == dtInt:
      limit = uint32(limExpr.litValue.intValue)

  # Detect ORDER BY PK optimization
  # When ORDER BY matches PK ordering, we can skip or simplify sorting.
  # The k-way merge uses a PK extractor for data table scans to produce
  # globally sorted output across groups, so the PK optimization is valid
  # for both single-group and multi-group tables.
  var pkOptimization = oboNone
  if stmt.selOrderBy.len > 0:
    pkOptimization = detectOrderByPkOptimization(stmt.selOrderBy, pkColumns)

  # Convert ORDER BY items to SortSpecs and determine sort columns
  # Skip this if we have PK optimization (no extra columns needed)
  var sortSpecs: seq[SortSpec] = @[]
  var sortCols: seq[string] = @[] # Columns needed for sorting
  if stmt.selOrderBy.len > 0 and pkOptimization == oboNone:
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
      pgKeyEncoding: desc.keyEncoding,
    ))
    # ORDER BY is applied after point get for consistency
    # Note: optimization doesn't matter for single row
    if stmt.selOrderBy.len > 0 and pkOptimization == oboNone:
      plan.add(PlanOp(kind: poOrderBy,
        obSortSpecs: sortSpecs,
        obColumns: reqCols, # Output columns (original requested)
        obAllColumns: fetchCols, # Columns in the rows (for expression evaluation)
        obOptimization: oboNone,
        obServerTopK: false,
      ))
    return plan

  # Range scan (with optimized key bounds if available)
  let (startKey, endKey) = makeScanKeysFromRange(desc.tableId, pkRangeInfo,
      desc.keyEncoding)

  # LIMIT handling:
  # - PK ASC + LIMIT: data already sorted, push LIMIT to scan (scanLimit = limit)
  # - PK DESC + LIMIT: use top-K heap with reverse comparator (scanLimit = 0, oboPkDescMatch + oboTopK)
  #   TODO: When server supports ScanFlagReverse, use reverse scan + LIMIT pushdown instead
  # - No ORDER BY + LIMIT: push LIMIT to scan (scanLimit = limit)
  # - ORDER BY (non-PK) + LIMIT: scan all rows, use top-K heap (scanLimit = 0)
  # - ORDER BY without LIMIT: scan all rows, full sort (scanLimit = 0)
  var scanLimit: uint32
  var scanReverse: bool = false
  var obOptimization: OrderByOptimization = oboNone

  if pkOptimization == oboPkAscMatch:
    # Data already sorted by PK ASC, can apply LIMIT during scan
    # The k-way merge uses primaryKeyFromDataRowKey to produce globally
    # sorted output across groups, so PK ASC optimization is valid for
    # both single-group and multi-group tables.
    scanLimit = limit
    obOptimization = oboPkAscMatch
  elif pkOptimization == oboPkDescMatch:
    if limit > 0:
      # PK DESC + LIMIT: use server-side reverse scan with LIMIT pushdown.
      # The server scans each group in descending key order, returning only
      # the top K (largest) rows from each group. The k-way merge in
      # reverse mode picks the K largest rows across all groups, giving
      # us the correct global top K by PK DESC. This avoids scanning all
      # N rows and using a top-K heap (was O(N log K) heap operations).
      scanLimit = limit
      scanReverse = true
      # The data arrives already in PK DESC order, so the executor can
      # treat it as if it were PK ASC (the merge already ordered it).
      obOptimization = oboPkAscMatch
    else:
      # PK DESC without LIMIT: server-side reverse scan gives us all
      # rows in DESC order directly. The executor just iterates and
      # applies LIMIT (or none) — no client-side reversal needed.
      scanLimit = 0
      scanReverse = true
      obOptimization = oboPkAscMatch
  elif stmt.selOrderBy.len > 0 and limit > 0:
    # Non-PK ORDER BY + LIMIT: scan all rows, use bounded top-K heap.
    # We can't push LIMIT per-group here because each group's storage
    # order is by PK (not by the ORDER BY column), so the per-group
    # "top K" in storage order is not the per-group "top K" by the
    # ORDER BY column. Must scan all rows; heap keeps O(K) memory.
    scanLimit = 0
    obOptimization = oboTopK
  elif stmt.selOrderBy.len > 0:
    # Non-PK ORDER BY without LIMIT: scan all, full sort
    scanLimit = 0
    obOptimization = oboNone
  else:
    # No ORDER BY, apply LIMIT during scan
    scanLimit = limit

  plan.add(PlanOp(kind: poScan,
    scTableId: desc.tableId,
    scStartKey: startKey,
    scEndKey: endKey,
    scLimit: scanLimit,
    scReverse: scanReverse,
    scFilter: pkRangeInfo.remainingFilter, # Only non-PK conditions remain
    scColumns: fetchCols, # Fetch columns needed for ORDER BY
    scAllColumns: allCols,
    scKeyEncoding: desc.keyEncoding,
    # Tier-3b: server-side top-K heap pushdown. Only set for oboTopK cases
    # (non-PK ORDER BY + LIMIT, and PK DESC + LIMIT). The `limit` field on
    # the spec is the per-group candidate size — same K as obLimit since each
    # group independently selects its top K and the client merges the global
    # top K via k-way merge. We only ship specs whose columnIndex is set
    # (skip sort specs that need expression evaluation — those need a slow
    # path we'll handle later if needed).
    scTopK: if obOptimization == oboTopK and limit > 0:
      var wireSpecs: seq[WireSortSpec] = @[]
      for spec in (if stmt.selOrderBy.len > 0 and pkOptimization == oboNone:
                     sortSpecs
                   else: @[]):
        if spec.columnIndex >= 0:
          wireSpecs.add(WireSortSpec(
            columnIndex: int32(spec.columnIndex),
            descending: spec.descending
          ))
      if wireSpecs.len > 0:
        some(WireTopKSpec(limit: limit, sortSpecs: wireSpecs))
      else:
        none(WireTopKSpec)
    else:
      none(WireTopKSpec),
  ))

  # Add ORDER BY plan op if specified
  if stmt.selOrderBy.len > 0:
    if obOptimization == oboPkAscMatch:
      # PK ASC: data is already sorted, skip sorting — just apply column
      # extraction and LIMIT pushdown (scan already has LIMIT if present)
      plan.add(PlanOp(kind: poOrderBy,
        obSortSpecs: @[], # No sort needed
        obColumns: reqCols,
        obAllColumns: fetchCols,
        obLimit: limit,
        obOptimization: oboPkAscMatch,
        obServerTopK: false,
      ))
    elif obOptimization == oboPkDescMatch:
      if limit > 0:
        # PK DESC + LIMIT: use top-K heap with PK DESC sort specs.
        # This avoids materializing all N rows for reversal — only K rows in memory.
        # Generate PK DESC sort specs from the ORDER BY items.
        let pkSortSpecs = orderItemsToSortSpecs(stmt.selOrderBy, allCols)
        # Server-side top-K pushdown: signal the executor that the server
        # already applied the heap (each group returned ≤K candidates).
        let hasServerTopK = stmt.selOrderBy.len > 0 and
            stmt.selOrderBy.allIt(it.expr.kind == exColumn) and
            (let specs = orderItemsToSortSpecs(stmt.selOrderBy, allCols);
             specs.allIt(it.columnIndex >= 0))
        plan.add(PlanOp(kind: poOrderBy,
          obSortSpecs: pkSortSpecs,
          obColumns: reqCols,
          obAllColumns: fetchCols,
          obLimit: limit,
          obOptimization: oboTopK,
          obServerTopK: hasServerTopK,
        ))
      else:
        # PK DESC without LIMIT: data needs full reversal, no sort specs needed
        plan.add(PlanOp(kind: poOrderBy,
          obSortSpecs: @[], # No sort specs - just reverse
          obColumns: reqCols,
          obAllColumns: fetchCols,
          obLimit: limit,
          obOptimization: oboPkDescMatch,
          obServerTopK: false,
        ))
    elif obOptimization == oboTopK:
      # Non-PK ORDER BY + LIMIT: use bounded top-K heap
      # Server-side top-K pushdown: signal the executor that the server
      # already applied the heap (each group returned ≤K candidates).
      let hasServerTopK = sortSpecs.len > 0 and
          sortSpecs.allIt(it.columnIndex >= 0)
      plan.add(PlanOp(kind: poOrderBy,
        obSortSpecs: sortSpecs,
        obColumns: reqCols,
        obAllColumns: fetchCols,
        obLimit: limit,
        obOptimization: oboTopK,
        obServerTopK: hasServerTopK,
      ))
    else:
      # No optimization - full sort
      plan.add(PlanOp(kind: poOrderBy,
        obSortSpecs: sortSpecs,
        obColumns: reqCols,
        obAllColumns: fetchCols,
        obLimit: limit,
        obOptimization: oboNone,
        obServerTopK: false,
      ))

  plan

proc planUpdate(stmt: Stmt, client: FractioClient,
    database, schema: string): Plan =
  let plan = newPlan()
  let descOpt = resolveQualifiedTableRef(client, database, schema,
      stmt.updTableRef)
  if descOpt.isNone:
    raise planError(&"table '{stmt.updTableRef.fullName()}' not found")
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
  let descOpt = resolveQualifiedTableRef(client, database, schema,
      stmt.delTableRef)
  if descOpt.isNone:
    raise planError(&"table '{stmt.delTableRef.fullName()}' not found")
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

proc planCreateSpace(stmt: Stmt, timeProvider: TimeProvider = nil): Plan =
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
    workerState: uint8(wsrIdle),
    workerNodeId: 0,
    workerHeartbeat: 0,
    checkpoint: MigrationCheckpointRecord(
      completedTables: @[],
      currentTable: zeroTableId(),
      currentCursor: "",
      keysMigrated: 0,
      startedAtNs: 0,
      lastProgressNs: 0,
    ),
    createdAtNs: nowNs(timeProvider)
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
    if op.scReverse:
      s &= " reverse=true"
    s
  of poOrderBy:
    var s = "OrderBy"
    case op.obOptimization:
    of oboNone:
      s &= &" specs=[{formatSortSpecs(op.obSortSpecs)}]"
    of oboPkAscMatch:
      s &= " optimization=PK_ASC_SKIP"
    of oboPkDescMatch:
      s &= " optimization=PK_DESC_REVERSE"
    of oboTopK:
      s &= &" optimization=TOP_K specs=[{formatSortSpecs(op.obSortSpecs)}]"
    s &= &" cols={op.obColumns}"
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
    schema: string = "public",
    timeProvider: TimeProvider = nil): Plan =
  ## Translate a Stmt AST into a Plan (sequence of KV operations).
  case stmt.kind
  of stmtCreateDatabase: planCreateDatabase(stmt, timeProvider)
  of stmtDropDatabase: planDropDatabase(stmt)
  of stmtCreateSchema: planCreateSchema(stmt, database, timeProvider)
  of stmtDropSchema: planDropSchema(stmt, database)
  of stmtCreateTable: planCreateTable(stmt, client, database, schema)
  of stmtDropTable:
    let plan = newPlan()
    # Resolve database and schema from tableRef or defaults
    let dbName = if stmt.dtTableRef.database !=
        "": stmt.dtTableRef.database else: database
    let scName = if stmt.dtTableRef.schema !=
        "": stmt.dtTableRef.schema else: schema
    plan.add(PlanOp(kind: poDropTable,
      dtName: stmt.dtTableRef.table,
      dtIfExists: stmt.dtIfExists,
      dtSchema: scName,
      dtDatabase: dbName,
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
  of stmtCreateSpace: planCreateSpace(stmt, timeProvider)
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
    let innerPlan = planStatement(stmt.explainStmt, client, database, schema, timeProvider)
    let plan = newPlan()
    plan.add(PlanOp(kind: poExplain, exInnerPlan: innerPlan))
    plan
