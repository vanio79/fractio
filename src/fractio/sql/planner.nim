# SQL Planner for Fractio
#
# Translates a Stmt AST into a Plan — a sequence of KV operations.
# The planner resolves table names to table IDs via catalog lookups
# and generates the appropriate key encodings for reads/writes.

import std/[options, json, strutils, strformat, sequtils, times, os]
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
import ../utils/logging as fractioLogging

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
      scHasLimit*: bool                ## True if the user wrote a LIMIT clause (even LIMIT 0).
                                       ## This distinguishes "no LIMIT clause" (scLimit unused)
                                       ## from "LIMIT 0" (scLimit=0, scHasLimit=true) which
                                       ## means "return zero rows".
      scAppliesLimit*: bool            ## True if this scan is responsible for applying the
                                       ## LIMIT (i.e., the planner decided scanLimit is the
                                       ## authoritative limit). False when the scan returns
                                       ## all rows because a downstream op (e.g. poOrderBy
                                       ## with oboTopK heap) will apply the limit. Only when
                                       ## scAppliesLimit is true can the executor safely
                                       ## short-circuit on scLimit==0.
      scReverse*: bool ## true = scan in reverse key order (for PK DESC + LIMIT)
      scFilter*: Option[Expr]
      scColumns*: seq[string]          # columns to return (empty = all)
      scAllColumns*: seq[string]       # all table columns for decoding
      scKeyEncoding*: TableKeyEncoding # key encoding strategy for this table
      scOffset*: uint32                ## OFFSET to apply during scan (skip first M
                                       ## rows in storage order). 0 = no offset. Only
                                       ## set when the planner can push the offset
                                       ## down safely (PK ASC + LIMIT + OFFSET). The
                                       ## executor still applies the same offset
                                       ## post-sort via poOrderBy.obOffset, but when
                                       ## pushed down, the scan only returns the
                                       ## (limit+offset) rows it needs.
      scHasOffset*: bool               ## True if scOffset is meaningful.
      scTopK*: Option[WireTopKSpec]    ## Tier-3b: server-side top-K heap
                                      ## pushdown. When set, each group server runs a bounded top-K heap
                                      ## locally and ships only the K winners over the wire. Used for
                                      ## `ORDER BY non_pk_col LIMIT K` to cut wire traffic and client-side
                                      ## decode work from O(N) to O(K). When `none`, no server-side top-K.

    of poOrderBy:
      obSortSpecs*: seq[SortSpec]      ## Sort specifications from ORDER BY
      obColumns*: seq[string]          ## Columns to return (passed from scan)
      obAllColumns*: seq[string]       ## All fetched columns for expression evaluation
      obLimit*: uint32 ## LIMIT to apply after sorting (0 = no limit, but see hasLimit)
      hasLimit*: bool                  ## True if the user wrote a LIMIT clause (even LIMIT 0).
                                       ## This distinguishes "no LIMIT clause" (obLimit unused)
                                       ## from "LIMIT 0" (obLimit=0, hasLimit=true) which means
                                       ## "return zero rows".
      obOffset*: uint32                ## OFFSET to apply AFTER sorting/ordering. Skip
                                       ## the first obOffset rows of the sorted result,
                                       ## then apply LIMIT. 0 = no offset. Distinct from
                                       ## "user wrote OFFSET 0" (hasOffset=true, obOffset=0
                                       ## means skip nothing — same as no offset clause).
      hasOffset*: bool                 ## True if the user wrote an OFFSET clause (even
                                       ## OFFSET 0). Lets the executor distinguish "no
                                       ## OFFSET clause" from "OFFSET 0".
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
      delPkSpec*: PrimaryKeySpec ## PK spec for binary encoding (for point-lookup optimisation)
      delPkPointLookups*: seq[string]  ## Binary-encoded PK values for point-lookup DELETE.
                                       ## When non-empty, the executor does GET+DELETE per
                                       ## key instead of a full table scan + client filter.
                                       ## Populated only when the WHERE clause is a
                                       ## disjunction of `pk = literal` conditions.

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
# Expression helpers
# ---------------------------------------------------------------------------

proc extractLiteralInt(e: Expr): Option[int64] =
  ## Try to extract a constant integer from an expression AST node.
  ## Returns:
  ##   - Some(n)  for a positive integer literal (exLiteral/dtInt/n)
  ##   - Some(-n) for a unary-minus integer literal (exUnaryOp/uoNeg/exLiteral/dtInt/n)
  ##   - None     for everything else (parameters, columns, complex expressions)
  ## The parser produces exUnaryOp(uoNeg, litInt(n)) for inputs like `-1`,
  ## not exBinOp("0 - 1"), so we only handle that one case. Anything that
  ## requires runtime evaluation (parameters, columns, computed expressions)
  ## must be rejected from a LIMIT/OFFSET clause since those values must be
  ## known at plan time to push the bound into the scan.
  case e.kind
  of exLiteral:
    if e.litValue != nil and e.litValue.kind == dtInt:
      return some(int64(e.litValue.intValue))
    return none(int64)
  of exUnaryOp:
    if e.unaryOp == uoNeg and e.unaryExpr.kind == exLiteral and
       e.unaryExpr.litValue != nil and e.unaryExpr.litValue.kind == dtInt:
      return some(int64(-e.unaryExpr.litValue.intValue))
    return none(int64)
  else:
    return none(int64)

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

proc collectExprColumns*(e: Expr, into: var seq[string]) =
  ## Walk an expression tree and append every column name referenced in it to
  ## `into`. Duplicates may be added; callers should de-dupe if they care.
  ## Used by the planner to discover which columns a WHERE / filter
  ## expression depends on, so we can keep them in the projection list when
  ## the SELECT list is narrower than the filter.
  if e == nil:
    return
  case e.kind
  of exColumn:
    into.add(e.colName)
  of exBinOp:
    collectExprColumns(e.binLeft, into)
    collectExprColumns(e.binRight, into)
  of exUnaryOp:
    collectExprColumns(e.unaryExpr, into)
  of exIn:
    collectExprColumns(e.inExpr, into)
    for item in e.inList:
      collectExprColumns(item, into)
  of exIsNull:
    collectExprColumns(e.isNullExpr, into)
  of exBetween:
    collectExprColumns(e.betweenExpr, into)
    collectExprColumns(e.betweenLo, into)
    collectExprColumns(e.betweenHi, into)
  of exLike:
    collectExprColumns(e.likeExpr, into)
    collectExprColumns(e.likePattern, into)
  of exList:
    for item in e.listItems:
      collectExprColumns(item, into)
  of exLiteral, exParam, exStar:
    discard

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
  ##
  ## Retries on "table not found" (catalog miss) to handle transient
  ## states during META group leader changes: NuRaft's become_leader()
  ## sets is_initialized=true BEFORE the state machine has caught up
  ## to the committed log. A read during that window succeeds at the
  ## protocol level (no error) but returns found=false because WiscKey
  ## has not yet replayed the batch. Refreshing metadata and retrying
  ## routes subsequent attempts through the new (caught-up) leader.
  let catalogKey = encodeTableKey(SYS_TABLES_TABLE_ID,
      database & "." & schema & "." & tableName)

  # Bumped from 5/25 (May 2026) to 8/50 to handle SM-replay windows up
  # to ~6.4s. Empirically observed that 1M-row load on a 3-replica cluster
  # can trigger a META group leader change with a multi-second replay
  # window during which kvGet returns "not found". The previous 5x25/
  # 50/100/200/400ms schedule (775ms total) was insufficient — the test
  # saw hundreds of "table not found" errors. New schedule: 50/100/200/
  # 400/800/1600/3200/6400ms = ~12.75s total. The retry also now handles
  # transport errors (NOT_LEADER, connection reset) that occur during
  # the META leader transition.
  #
  # Bumped again (Jun 2026) from 8/50 to 16/100 to handle sustained
  # META leadership instability on 3-replica clusters during heavy load.
  # CRITICAL: Backoff is CAPPED at maxCatalogMissBackoffMs (5000) to avoid
  # exponentially exploding waits — raw exponential at attempt 16 would be
  # 100ms * 2^15 = ~55 minutes per retry. Capped schedule:
  # 100,200,400,800,1600,3200,5000,... ~52s total. Outer queryWithRetry
  # provides the longer-window retry budget.
  #
  # Bumped again (Jun 29 2026): increased to 40/200 with higher backoff cap
  # to handle extended WiscKey replay windows during heavy INSERT load on
  # 3-replica clusters. Previously, all 16 retries exhausted within ~12.75s
  # while the META group leader was stuck in a long replay window (catalog_miss
  # on every attempt). New schedule: 200ms * (1..40 with cap at 8s) = ~33s total,
  # with max elapsed time guard of 60s to prevent runaway waits.
  const maxCatalogMissRetries = 40
  const catalogMissBaseBackoffMs = 200
  const maxCatalogMissBackoffMs = 8000 # Cap backoff at 8s (was 5s)
  const maxCatalogMissElapsedSec = 60 # Hard timeout: stop after 60s total

  var recOpt: Option[TableRecord] = none(TableRecord)
  let retryStartTime = epochTime() # Track elapsed time across retries (seconds since epoch)

  for attempt in 0 ..< maxCatalogMissRetries:
    # Elapsed time guard: abort after maxCatalogMissElapsedSec to avoid
    # runaway waits if the META group is permanently stuck.
    let elapsedSec = epochTime() - retryStartTime
    if elapsedSec > maxCatalogMissElapsedSec:
      fractioLogging.error(&"resolveTable ABORTED: catalogKey={catalogKey} attempt={attempt} table={tableName} schema={schema} elapsedSec={elapsedSec:.1f} " &
        &"error=reached {maxCatalogMissElapsedSec}s timeout")
      return none(TableDescriptor)

    # DIAGNOSTIC LOGGING - Track resolveTable retry behavior with timing
    fractioLogging.debug(&"resolveTable: catalogKey={catalogKey} attempt={attempt} table={tableName} schema={schema} " &
      &"elapsedSec={elapsedSec:.1f}")

    let kvStart = epochTime()
    let res = client.kvGet(catalogKey)
    let kvElapsedMs = ((epochTime() - kvStart) * 1000.0).int
    if res.isErr:
      # Transport / protocol error — treat as a miss and retry. The
      # META leader transition can produce transient RPC failures
      # (NOT_LEADER, connection reset). Under sustained META instability
      # during heavy load, calling refreshMetadata() adds Raft traffic
      # that may trigger additional leadership changes — so we skip it
      # here and rely on the outer queryWithRetry budget for longer-window
      # recovery. The inner loop uses pure exponential backoff only.
      fractioLogging.warn(&"resolveTable: catalogKey={catalogKey} attempt={attempt} ERROR={res.err} kvElapsedMs={kvElapsedMs}")

      if attempt < maxCatalogMissRetries - 1:
        let backoff = min(catalogMissBaseBackoffMs * (1 shl attempt),
                          maxCatalogMissBackoffMs)
        fractioLogging.debug(&"resolveTable: catalogKey={catalogKey} attempt={attempt} status=transport_error " &
          &"backoffMs={backoff}")
        sleep(backoff)
        continue
      return none(TableDescriptor)

    if res.val.isSome:
      try:
        recOpt = some(decodeTableRecord(res.val.get()))
        fractioLogging.debug(&"resolveTable: catalogKey={catalogKey} attempt={attempt} SUCCESS table={recOpt.get().name} " &
          &"kvElapsedMs={kvElapsedMs}")
        break
      except ValueError:
        # Malformed record — treat as miss; retry may yield a fresh leader.
        fractioLogging.warn(&"resolveTable: catalogKey={catalogKey} attempt={attempt} ERROR=malformed table record")

    # Catalog miss during WiscKey replay window after META leadership change.
    # The new leader's state machine hasn't caught up yet, so sys.tables reads
    # return found=false even though the table was written before the election.
    # We skip refreshMetadata() here to avoid adding Raft traffic that could
    # trigger additional elections during heavy load. Pure backoff + wait for
    # WiscKey replay to complete.
    if attempt < maxCatalogMissRetries - 1:
      fractioLogging.debug(&"resolveTable: catalogKey={catalogKey} attempt={attempt} status=catalog_miss " &
        &"kvElapsedMs={kvElapsedMs}")

      let backoff = min(catalogMissBaseBackoffMs * (1 shl attempt),
                        maxCatalogMissBackoffMs)
      fractioLogging.debug(&"resolveTable: catalogKey={catalogKey} attempt={attempt} status=catalog_miss " &
        &"backoffMs={backoff}")

      sleep(backoff)

  if recOpt.isNone:
    # DIAGNOSTIC LOGGING - Log final failure with all retry attempts exhausted
    fractioLogging.error(&"resolveTable FAILED: catalogKey={catalogKey} table={tableName} schema={schema} database={database} error=all retries exhausted")
    return none(TableDescriptor)

  let rec = recOpt.get()
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

  # DIAGNOSTIC LOGGING - Track all table resolutions
  fractioLogging.debug(&"resolveQualifiedTableRef: database={dbName} schema={scName} table={tableRef.table}")

  let result = resolveTable(client, catalogDbName, scName, tableRef.table)

  # DIAGNOSTIC LOGGING - Log resolution success/failure
  if result.isNone:
    fractioLogging.warn(&"resolveQualifiedTableRef FAILED: database={dbName} schema={scName} table={tableRef.table} error=table not found")

  return result

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

  # Extract LIMIT value. hasLimit tracks whether the user wrote a LIMIT
  # clause (even LIMIT 0). This distinguishes "no LIMIT clause" from
  # "LIMIT 0", so the executor returns 0 rows for the latter instead of
  # defaulting to a sentinel value.
  #
  # LIMIT must be a non-negative integer literal known at plan time so we
  # can push the bound into the scan. Anything else (parameters, columns,
  # computed expressions) is rejected rather than silently coerced to a
  # default — the same rule applies to OFFSET below.
  var limit: uint32 = 0
  var hasLimit: bool = false
  if stmt.selLimit.isSome:
    hasLimit = true
    let limValOpt = extractLiteralInt(stmt.selLimit.get())
    if limValOpt.isNone:
      raise planError("LIMIT must be a non-negative integer literal")
    let limVal = limValOpt.get()
    if limVal < 0:
      raise planError(&"LIMIT must be non-negative, got {limVal}")
    limit = uint32(limVal)

  # Extract OFFSET value. hasOffset tracks whether the user wrote an
  # OFFSET clause (even OFFSET 0). The executor applies offset AFTER
  # sorting/ordering, so it works uniformly with all optimizations
  # (PK ASC, PK DESC, non-PK, etc.) and with streaming or buffered
  # execution. For PK ASC + LIMIT + OFFSET we also push offset down
  # to the scan (scanLimit = limit + offset) so the server doesn't
  # materialize more rows than we need.
  #
  # OFFSET must be a non-negative integer literal known at plan time so we
  # can push the bound into the scan. A non-literal value (parameter,
  # column reference, computed expression) is rejected with a planning
  # error rather than silently coerced to 0 — silent zero would mask
  # bugs where the user expected paging to actually skip rows.
  var offset: uint32 = 0
  var hasOffset: bool = false
  if stmt.selOffset.isSome:
    hasOffset = true
    let offValOpt = extractLiteralInt(stmt.selOffset.get())
    if offValOpt.isNone:
      raise planError("OFFSET must be a non-negative integer literal")
    let offVal = offValOpt.get()
    if offVal < 0:
      raise planError(&"OFFSET must be non-negative, got {offVal}")
    offset = uint32(offVal)

  # Detect ORDER BY PK optimization
  # When ORDER BY matches PK ordering, we can skip or simplify sorting.
  # The k-way merge uses a PK extractor for data table scans to produce
  # globally sorted output across groups, so the PK optimization is valid
  # for both single-group and multi-group tables.
  var pkOptimization = oboNone
  if stmt.selOrderBy.len > 0:
    pkOptimization = detectOrderByPkOptimization(stmt.selOrderBy, pkColumns)

  # Determine which extra columns are needed for sorting (the column
  # *names*). We can't build the full SortSpecs (with columnIndex) yet
  # because that requires fetchCols (which depends on sortCols). Skip
  # this if we have PK optimization (no extra columns needed).
  var sortCols: seq[string] = @[] # Columns needed for sorting
  if stmt.selOrderBy.len > 0 and pkOptimization == oboNone:
    for item in stmt.selOrderBy:
      if item.expr.kind == exColumn:
        let colName = item.expr.colName
        # Add to sortCols if not already in reqCols (avoid duplicates)
        if colName notin reqCols and colName notin sortCols:
          sortCols.add(colName)

  # Columns to fetch from storage = requested + ORDER BY referenced columns
  # + WHERE filter referenced columns. The server applies the residual filter
  # after projecting down to scColumns, so any column the filter reads must
  # still be present in the projected row — even when the SELECT list does
  # not include it (e.g. `SELECT id, name FROM t WHERE value > 5`).
  var filterCols: seq[string] = @[]
  if pkRangeInfo.remainingFilter.isSome:
    collectExprColumns(pkRangeInfo.remainingFilter.get(), filterCols)
  # De-dupe filterCols against the columns we already plan to fetch.
  var filterColsUnique: seq[string] = @[]
  for c in filterCols:
    if c notin reqCols and c notin sortCols and c notin filterColsUnique:
      filterColsUnique.add(c)
  let fetchCols = reqCols & sortCols & filterColsUnique

  # Now build the SortSpecs. CRITICAL: pass `fetchCols` (the projected
  # row layout) rather than `allCols` (the full table layout) so that
  # `columnIndex` aligns with the row the executor actually receives.
  # Previously this used `allCols`, which produced columnIndex values
  # like "age"->2 / "score"->3 against the full `users` table, but the
  # server projects the row down to `scColumns = fetchCols` (e.g.
  # `["name", "age", "score"]` for `SELECT name, age, score ORDER BY
  # age, score`). The executor's `computeSortKeys` then read
  # `row[2]`/`row[3]` of a 3-column row, getting out-of-bounds nulls —
  # and the sort silently degraded to "no sort at all" for non-PK
  # ORDER BY clauses. See the failing tests:
  #   - "ORDER BY multiple columns mixed ASC/DESC"
  #   - "ORDER BY with LIMIT"  (non-PK + LIMIT)
  #   - "ORDER BY with WHERE"
  var sortSpecs: seq[SortSpec] = @[]
  if stmt.selOrderBy.len > 0 and pkOptimization == oboNone:
    sortSpecs = orderItemsToSortSpecs(stmt.selOrderBy, fetchCols)

  # Generate plan based on PK range info
  if pkRangeInfo.isPointGet and pkRangeInfo.exactMatch.isSome:
    # Point get: single row lookup with optional remaining filter
    # ORDER BY on a single row is trivial - still add the op for consistency
    let pkVal = pkRangeInfo.exactMatch.get()
    plan.add(PlanOp(kind: poPointGet,
      pgTableId: desc.tableId,
      pgKey: pkVal,
      pgPkSpec: desc.pkSpec,
      pgColumns: fetchCols, # SELECT cols + ORDER BY cols + WHERE filter cols
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
        obLimit: 0,
        hasLimit: false,
        obOffset: offset,
        hasOffset: hasOffset,
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
  #
  # OFFSET pushdown: for paths where scanAppliesLimit=true (the scan is
  # the one that bounds output row count), we also push offset down by
  # setting scanLimit = limit + offset. The executor then drops the first
  # `offset` rows and takes the next `limit` rows. For paths where the
  # scan returns all rows (top-K heap, full sort, etc.), the executor
  # handles both offset and limit uniformly post-sort, so no pushdown.
  var scanLimit: uint32
  var scanReverse: bool = false
  var scanAppliesLimit: bool = false # true if this scan is the one that applies the LIMIT
  var scanOffset: uint32 = 0 # pushed-down offset (see OFFSET pushdown above)
  var hasScanOffset: bool = false
  var obOptimization: OrderByOptimization = oboNone

  if pkOptimization == oboPkAscMatch:
    # Data already sorted by PK ASC, can apply LIMIT during scan
    # The k-way merge uses primaryKeyFromDataRowKey to produce globally
    # sorted output across groups, so PK ASC optimization is valid for
    # both single-group and multi-group tables.
    #
    # With OFFSET: there's a downstream poOrderBy that needs to read
    # up to (limit+offset) rows from the scan to apply offset+limit
    # post-sort. Push the combined bound (limit+offset) down so the
    # server doesn't ship more than needed, but mark scAppliesLimit=false
    # so the executor's streaming iterator doesn't pre-drop the offset
    # (the iterator's per-row offset-skip is the wrong layer for ORDER
    # BY — it would skip non-sorted scan rows, and the downstream op
    # would then re-apply the offset on an already-truncated stream,
    # producing too few rows). The poOrderBy op applies offset+limit
    # post-extract, so the scan is purely a row source here.
    if hasOffset:
      scanLimit = limit + offset
      scanAppliesLimit = false
    else:
      scanLimit = limit
      scanAppliesLimit = true
    obOptimization = oboPkAscMatch
  elif pkOptimization == oboPkDescMatch:
    if limit > 0:
      # PK DESC + LIMIT: use server-side reverse scan with LIMIT pushdown.
      # The server scans each group in descending key order, returning only
      # the top K (largest) rows from each group. The k-way merge in
      # reverse mode picks the K largest rows across all groups, giving
      # us the correct global top K by PK DESC. This avoids scanning all
      # N rows and using a top-K heap (was O(N log K) heap operations).
      #
      # With OFFSET: the per-group server can't know whether the rows it
      # skips will turn out to be in the global top-K (it only sees its
      # own group). The k-way merge in reverse mode picks the K largest
      # across all groups, so we need the server to return (limit+offset)
      # largest per group, then the merge drops the global (offset)
      # largest and keeps the next (limit). Same caveat as PK ASC + OFFSET:
      # set scAppliesLimit=false so the executor's streaming iterator
      # doesn't pre-drop the offset; the downstream poOrderBy op applies
      # offset+limit post-extract.
      if hasOffset:
        scanLimit = limit + offset
        scanAppliesLimit = false
      else:
        scanLimit = limit
        scanAppliesLimit = true
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
    #
    # With OFFSET: scan needs (limit+offset) rows so the executor can
    # drop the first `offset` and keep the next `limit`. Without
    # ORDER BY the input order is "wherever the storage layer returns
    # rows" — pushing the offset down is semantically equivalent to
    # applying it post-scan as long as the executor applies the same
    # offset to the post-scan stream.
    if hasOffset:
      scanLimit = limit + offset
      scanOffset = offset
      hasScanOffset = true
    else:
      scanLimit = limit
    scanAppliesLimit = true

  plan.add(PlanOp(kind: poScan,
    scTableId: desc.tableId,
    scStartKey: startKey,
    scEndKey: endKey,
    scLimit: scanLimit,
    # scHasLimit tracks whether the user wrote a LIMIT clause (even LIMIT 0).
      # Used by the executor to distinguish "no LIMIT clause" from "LIMIT 0".
    scHasLimit: hasLimit,
    scOffset: scanOffset,
    scHasOffset: hasScanOffset,
    # scAppliesLimit is true ONLY when the scan is the one that applies the
    # LIMIT (i.e., scanLimit is the authoritative limit, not just a placeholder
    # for a downstream op to read). When scanAppliesLimit is true and
    # scLimit == 0, the executor can safely short-circuit and return 0 rows.
    # For non-PK ORDER BY + LIMIT, the scan returns all rows and the top-K
    # heap applies the limit — scAppliesLimit must be false in that case
    # to avoid the scan dropping rows the heap needs.
    scAppliesLimit: scanAppliesLimit,
    scReverse: scanReverse,
    scFilter: pkRangeInfo.remainingFilter, # Only non-PK conditions remain
    scColumns: fetchCols, # SELECT cols + ORDER BY cols + WHERE filter cols
    scAllColumns: allCols,
    scKeyEncoding: desc.keyEncoding,
    # Tier-3b: server-side top-K heap pushdown. Only set for oboTopK cases
    # (non-PK ORDER BY + LIMIT, and PK DESC + LIMIT). The `limit` field on
    # the spec is the per-group candidate size — same K as obLimit since each
    # group independently selects its top K and the client merges the global
    # top K via k-way merge. We only ship specs whose columnIndex is set
    # (skip sort specs that need expression evaluation — those need a slow
    # path we'll handle later if needed).
    #
    # With OFFSET: the server's top-K heap doesn't know about the offset. If
    # we shipped the spec with `limit: K` (the user's LIMIT N), the server
    # would emit the top K per group, and the client would then drop the
    # first `offset` rows and keep only N — but the per-group "top K" might
    # not include rows that should be in the global [offset, offset+N)
    # window. To be correct with OFFSET, we need the server to ship the
    # top (limit+offset) per group, and we should disable the pushdown
    # here so the client runs the heap. The trade is more bytes over the
    # wire for OFFSET queries, but correctness wins.
    scTopK: if obOptimization == oboTopK and limit > 0 and not hasOffset:
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
        hasLimit: hasLimit,
        obOffset: offset,
        hasOffset: hasOffset,
        obOptimization: oboPkAscMatch,
        obServerTopK: false,
      ))
    elif obOptimization == oboPkDescMatch:
      if limit > 0:
        # PK DESC + LIMIT: use top-K heap with PK DESC sort specs.
        # This avoids materializing all N rows for reversal — only K rows in memory.
        # Generate PK DESC sort specs from the ORDER BY items. Use fetchCols
        # (projected row layout) so columnIndex aligns with the rows the
        # executor actually receives — same rationale as the non-PK sort
        # path above.
        let pkSortSpecs = orderItemsToSortSpecs(stmt.selOrderBy, fetchCols)
        # Server-side top-K pushdown: signal the executor that the server
        # already applied the heap (each group returned ≤K candidates).
        let hasServerTopK = stmt.selOrderBy.len > 0 and
            stmt.selOrderBy.allIt(it.expr.kind == exColumn) and
            (let specs = orderItemsToSortSpecs(stmt.selOrderBy, fetchCols);
             specs.allIt(it.columnIndex >= 0))
        plan.add(PlanOp(kind: poOrderBy,
          obSortSpecs: pkSortSpecs,
          obColumns: reqCols,
          obAllColumns: fetchCols,
          obLimit: limit,
          hasLimit: hasLimit,
          obOffset: offset,
          hasOffset: hasOffset,
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
          hasLimit: hasLimit,
          obOffset: offset,
          hasOffset: hasOffset,
          obOptimization: oboPkDescMatch,
          obServerTopK: false,
        ))
    elif obOptimization == oboTopK:
      # Non-PK ORDER BY + LIMIT: use bounded top-K heap
      # Server-side top-K pushdown: the server still runs a per-group top-K
      # heap (via scTopK on the scan op) and ships only K candidates per
      # group, dramatically reducing wire traffic for wide queries. However,
      # the client MUST re-heap the K×Ngroups candidates: the k-way merge
      # orders by PK (LevelDB key order), not by the ORDER BY column, so
      # the first K candidates from the merged stream are not necessarily
      # the global top-K. We therefore set obServerTopK=false so the
      # executor's client-side top-K heap path is taken (executor.nim
      # oboTopK branch). The server's per-group heap is still active via
      # scTopK; this just tells the executor not to skip the client merge.
      let hasServerTopK = false
      plan.add(PlanOp(kind: poOrderBy,
        obSortSpecs: sortSpecs,
        obColumns: reqCols,
        obAllColumns: fetchCols,
        obLimit: limit,
        hasLimit: hasLimit,
        obOffset: offset,
        hasOffset: hasOffset,
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
        hasLimit: hasLimit,
        obOffset: offset,
        hasOffset: hasOffset,
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

proc extractPkEqualityDisjunction(expr: Expr, pkCol: string,
    pkSpec: PrimaryKeySpec): seq[string] =
  ## Extract binary-encoded PK values from a WHERE clause that is a
  ## disjunction (OR-chain) of PK equality conditions:
  ##   pk = N OR pk = M OR pk = P ...
  ## Returns the encoded PK values. If the expression is not a pure
  ## disjunction of PK equalities, returns an empty seq (caller falls
  ## back to full-scan DELETE).
  ##
  ## Supported shapes:
  ##   1. Single: pk = N
  ##   2. OR-chain: pk = N OR pk = M OR ...
  ##   3. IN-list: pk IN (N, M, P)
  ##
  ## If ANY disjunct is not a PK equality (or IN-list on PK), we return
  ## empty to signal "cannot optimise — fall back to scan+filter".
  if expr == nil:
    return @[]

  # Case 3: IN-list on PK column
  if expr.kind == exIn and not expr.inNot:
    let inExpr = expr.inExpr
    if inExpr.kind == exColumn and inExpr.colName == pkCol:
      for item in expr.inList:
        if item.kind != exLiteral:
          return @[] # Non-literal in IN-list — can't optimise
        let pkValOpt = extractPkValueFromLiteral(item, pkSpec)
        if pkValOpt.isNone:
          return @[]
        result.add(pkValOpt.get())
      return

  # Case 1: single pk = N
  if expr.kind == exBinOp and expr.binOp == boEq:
    let pkValOpt = extractPkValueFromLiteral(expr.binRight, pkSpec)
    let pkValOpt2 = extractPkValueFromLiteral(expr.binLeft, pkSpec)
    if pkValOpt.isSome and expr.binLeft.kind == exColumn and
       expr.binLeft.colName == pkCol:
      result.add(pkValOpt.get())
      return
    if pkValOpt2.isSome and expr.binRight.kind == exColumn and
       expr.binRight.colName == pkCol:
      result.add(pkValOpt2.get())
      return
    # Not a PK equality — fall back
    return @[]

  # Case 2: OR-chain
  if expr.kind == exBinOp and expr.binOp == boOr:
    # Recursively extract from left and right
    let leftVals = extractPkEqualityDisjunction(expr.binLeft, pkCol, pkSpec)
    let rightVals = extractPkEqualityDisjunction(expr.binRight, pkCol, pkSpec)
    if leftVals.len > 0 or rightVals.len > 0:
      # But only valid if BOTH sides are non-empty (or one side is a
      # single equality that produced exactly 1 value). If either side
      # returned empty, it means that disjunct wasn't a PK equality.
      # Exception: a single OR of two equalities where both return 1.
      if leftVals.len == 0 or rightVals.len == 0:
        return @[] # One side wasn't a PK equality — can't optimise
      result = leftVals & rightVals
      return
    return @[]

  # Unsupported shape
  return @[]

proc planDelete(stmt: Stmt, client: FractioClient,
    database, schema: string): Plan =
  # DIAGNOSTIC LOGGING - Track DELETE table resolution
  fractioLogging.info(&"planDelete: database={database} schema={schema} table={stmt.delTableRef.table}")

  let plan = newPlan()
  let descOpt = resolveQualifiedTableRef(client, database, schema,
      stmt.delTableRef)

  # DIAGNOSTIC LOGGING - Log resolution result for DELETE
  if descOpt.isNone:
    fractioLogging.error(&"planDelete ERROR: database={database} schema={schema} table={stmt.delTableRef.table} error=table not found in catalog")
    raise planError(&"table '{stmt.delTableRef.fullName()}' not found")

  fractioLogging.info(&"planDelete SUCCESS: table={stmt.delTableRef.table} resolvedTableId={$descOpt.get().tableId}")

  let desc = descOpt.get()

  let pkCol = findPkColumn(desc)

  # Optimisation: if the WHERE clause is a disjunction of PK equality
  # conditions (pk = N OR pk = M OR ...), extract the PK values and do
  # point GET+DELETE instead of a full table scan. This is critical for
  # Phase B of the smoke test: 100 batches × 1000 OR clauses would
  # otherwise scan 67K rows per group per batch = 6.7M row scans.
  var pkPointLookups: seq[string] = @[]
  if stmt.delWhere.isSome and pkCol.len > 0 and
     desc.pkSpec.columns.len >= 1:
    pkPointLookups = extractPkEqualityDisjunction(
      stmt.delWhere.get(), pkCol, desc.pkSpec)

  plan.add(PlanOp(kind: poDelete,
    delTableId: desc.tableId,
    delTableName: desc.name,
    delFilter: stmt.delWhere,
    delAllColumns: columnNames(desc),
    delPkColumn: pkCol,
    delPkSpec: desc.pkSpec,
    delPkPointLookups: pkPointLookups,
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
    # Only show offset when the user wrote one. scHasOffset distinguishes
    # "no OFFSET clause" from "OFFSET 0" (both have scOffset=0 but only the
    # latter is meaningful to the user).
    if op.scHasOffset and op.scOffset > 0:
      s &= &" offset={op.scOffset}"
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
    # Only show offset when the user wrote one. hasOffset distinguishes
    # "no OFFSET clause" from "OFFSET 0" — the latter is meaningful
    # to the user but its visible effect is identical to no offset.
    if op.hasOffset and op.obOffset > 0:
      s &= &" offset={op.obOffset}"
    s
  of poUpdate:
    var s = &"Update table={op.upTableName} (id={op.upTableId})"
    if op.upFilter.isSome:
      s &= &" filter=({formatExpr(op.upFilter.get())})"
    s &= &" set=[{op.upSets.len} cols]"
    s
  of poDelete:
    var s = &"Delete table={op.delTableName} (id={op.delTableId})"
    if op.delPkPointLookups.len > 0:
      s &= &" pk_point_lookups={op.delPkPointLookups.len}"
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
