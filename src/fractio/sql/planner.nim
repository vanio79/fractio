# SQL Planner for Fractio
#
# Translates a Stmt AST into a Plan — a sequence of KV operations.
# The planner resolves table names to table IDs via catalog lookups
# and generates the appropriate key encodings for reads/writes.

import std/[options, json, strutils, strformat]
import ./ast
import ../distributed/meta/system_tables
import ../distributed/meta/system_schemas
import ../client/fractio_client
import ../core/types as coreTypes

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
      insRows*: seq[string]        # JSON-encoded row objects

    of poPointGet:
      pgTableId*: TableId
      pgKey*: string               # primary key value
      pgColumns*: seq[string]      # columns to return (empty = all)
      pgAllColumns*: seq[string]   # all table columns for decoding

    of poScan:
      scTableId*: TableId
      scStartKey*: string
      scEndKey*: string
      scLimit*: uint32
      scFilter*: Option[Expr]
      scColumns*: seq[string]      # columns to return (empty = all)
      scAllColumns*: seq[string]   # all table columns for decoding

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
  )
  # Copy primary key columns
  for pk in rec.primaryKey:
    desc.primaryKey.add(pk)
  # Convert columns
  for col in rec.columns:
    var cd = ColDef(name: col.name)
    cd.dataType = columnDataTypeToDataType(col.dataType)
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

proc exprToJsonValue*(e: Expr): JsonNode =
  ## Convert a literal expression to a JSON value.
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
  for row in stmt.intoValues:
    var rowObj = newJObject()
    for i, expr in row:
      if i < colNames.len:
        rowObj[colNames[i]] = exprToJsonValue(expr)
    rows.add($rowObj)

  plan.add(PlanOp(kind: poInsert,
    insTableId: desc.tableId,
    insTableName: desc.name,
    insColumns: colNames,
    insPkColumn: pkCol,
    insRows: rows,
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

  # Check for point get: WHERE pk = literal
  let pkCol = findPkColumn(desc)
  if stmt.selWhere.isSome:
    let w = stmt.selWhere.get()
    if w.kind == exBinOp and w.binOp == boEq:
      if w.binLeft.kind == exColumn and w.binLeft.colName == pkCol and
         w.binRight.kind == exLiteral:
        var pkVal: string
        if w.binRight.litValue != nil:
          case w.binRight.litValue.kind
          of dtInt: pkVal = $w.binRight.litValue.intValue
          of dtString: pkVal = w.binRight.litValue.strValue
          else: pkVal = $w.binRight.litValue.intValue
        plan.add(PlanOp(kind: poPointGet,
          pgTableId: desc.tableId,
          pgKey: pkVal,
          pgColumns: reqCols,
          pgAllColumns: allCols,
        ))
        return plan

  # Full scan with optional filter
  let startKey = encodeDataRowKey(desc.tableId, "")
  let endKey = makeDataRowScanEndKey(desc.tableId)
  var limit: uint32 = 0
  if stmt.selLimit.isSome:
    let limExpr = stmt.selLimit.get()
    if limExpr.kind == exLiteral and limExpr.litValue != nil and
       limExpr.litValue.kind == dtInt:
      limit = uint32(limExpr.litValue.intValue)

  plan.add(PlanOp(kind: poScan,
    scTableId: desc.tableId,
    scStartKey: startKey,
    scEndKey: endKey,
    scLimit: limit,
    scFilter: stmt.selWhere,
    scColumns: reqCols,
    scAllColumns: allCols,
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
