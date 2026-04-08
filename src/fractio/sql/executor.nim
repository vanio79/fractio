# SQL Executor for Fractio
#
# Executes a Plan against a FractioClient, returning results.
# Each PlanOp maps directly to KV operations via the client.
# Supports MVCC transactions through the client's transaction API.

import std/[options, json, strutils, strformat, tables, algorithm]
import ./ast
import ./parser
import ./planner
import ../distributed/meta/system_tables
import ../distributed/meta/system_schemas
import ../client/fractio_client
import ../core/types as coreTypes

# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

type
  ExecResultKind* = enum
    erkRows        ## SELECT results
    erkModified    ## INSERT/UPDATE/DELETE affected rows
    erkOk          ## DDL success
    erkError       ## Error
    erkUseDatabase ## USE DATABASE — caller should update session context
    erkUseSchema   ## USE SCHEMA — caller should update session context

  ExecResult* = ref object
    case kind*: ExecResultKind
    of erkRows:
      columns*: seq[string]
      rows*: seq[seq[string]] # each row is column values as strings
    of erkModified:
      count*: int
      message*: string
    of erkOk:
      okMessage*: string
    of erkError:
      error*: string
    of erkUseDatabase:
      newDatabase*: string
    of erkUseSchema:
      newSchema*: string

  ExecutorContext* = ref object
    ## Execution context for a session, holding transaction state
    client*: FractioClient
    txnId*: TransactionID
    readTimestamp*: uint64
    hasActiveTransaction*: bool
    database*: string
    schema*: string

  KVEntry* = object
    key*: string
    value*: string

proc okResult*(msg: string): ExecResult =
  ExecResult(kind: erkOk, okMessage: msg)

proc errorResult*(msg: string): ExecResult =
  ExecResult(kind: erkError, error: msg)

proc modifiedResult*(count: int, msg: string = ""): ExecResult =
  ExecResult(kind: erkModified, count: count,
    message: if msg.len > 0: msg else: &"{count} row(s) affected")

proc rowsResult*(columns: seq[string], rows: seq[seq[string]]): ExecResult =
  ExecResult(kind: erkRows, columns: columns, rows: rows)

# ---------------------------------------------------------------------------
# ExecutorContext helpers
# ---------------------------------------------------------------------------

proc newExecutorContext*(client: FractioClient, database: string = "default",
    schema: string = "public"): ExecutorContext =
  ## Create a new executor context with default settings
  ExecutorContext(
    client: client,
    txnId: client.activeTxnId,
    readTimestamp: client.activeReadTs,
    hasActiveTransaction: not isZero(client.activeTxnId),
    database: database,
    schema: schema
  )

# ---------------------------------------------------------------------------
# Expression evaluator (in-memory, for WHERE filters)
# ---------------------------------------------------------------------------

proc evalExpr*(expr: Expr, row: JsonNode): JsonNode =
  ## Evaluate an expression against a JSON row object.
  case expr.kind
  of exLiteral:
    if expr.litValue == nil:
      return newJNull()
    case expr.litValue.kind
    of dtInt: return newJInt(expr.litValue.intValue)
    of dtFloat: return newJFloat(expr.litValue.floatValue)
    of dtString: return newJString(expr.litValue.strValue)
    of dtBool: return newJBool(expr.litValue.boolValue)
    else: return newJNull()

  of exColumn:
    let name = expr.colName
    if row.hasKey(name):
      return row[name]
    return newJNull()

  of exBinOp:
    let left = evalExpr(expr.binLeft, row)
    let right = evalExpr(expr.binRight, row)

    case expr.binOp
    of boEq:
      return newJBool(left == right)
    of boNeq:
      return newJBool(left != right)
    of boLt:
      if left.kind == JInt and right.kind == JInt:
        return newJBool(left.getInt < right.getInt)
      if left.kind == JString and right.kind == JString:
        return newJBool(left.getStr < right.getStr)
      return newJBool(false)
    of boLte:
      if left.kind == JInt and right.kind == JInt:
        return newJBool(left.getInt <= right.getInt)
      return newJBool(false)
    of boGt:
      if left.kind == JInt and right.kind == JInt:
        return newJBool(left.getInt > right.getInt)
      return newJBool(false)
    of boGte:
      if left.kind == JInt and right.kind == JInt:
        return newJBool(left.getInt >= right.getInt)
      return newJBool(false)
    of boAnd:
      return newJBool(left.getBool(false) and right.getBool(false))
    of boOr:
      return newJBool(left.getBool(false) or right.getBool(false))
    of boAdd:
      if left.kind == JInt and right.kind == JInt:
        return newJInt(left.getInt + right.getInt)
      return newJNull()
    of boSub:
      if left.kind == JInt and right.kind == JInt:
        return newJInt(left.getInt - right.getInt)
      return newJNull()
    of boMul:
      if left.kind == JInt and right.kind == JInt:
        return newJInt(left.getInt * right.getInt)
      return newJNull()
    of boDiv:
      if left.kind == JInt and right.kind == JInt and right.getInt != 0:
        return newJInt(left.getInt div right.getInt)
      return newJNull()
    of boMod:
      if left.kind == JInt and right.kind == JInt and right.getInt != 0:
        return newJInt(left.getInt mod right.getInt)
      return newJNull()

  of exUnaryOp:
    let inner = evalExpr(expr.unaryExpr, row)
    case expr.unaryOp
    of uoNot:
      return newJBool(not inner.getBool(false))
    of uoNeg:
      if inner.kind == JInt:
        return newJInt(-inner.getInt)
      return newJNull()

  of exIsNull:
    let inner = evalExpr(expr.isNullExpr, row)
    let isNull = inner.kind == JNull
    return newJBool(if expr.isNullNot: not isNull else: isNull)

  of exIn:
    let val = evalExpr(expr.inExpr, row)
    var found = false
    for item in expr.inList:
      if evalExpr(item, row) == val:
        found = true
        break
    return newJBool(if expr.inNot: not found else: found)

  of exBetween:
    let val = evalExpr(expr.betweenExpr, row)
    let lo = evalExpr(expr.betweenLo, row)
    let hi = evalExpr(expr.betweenHi, row)
    var inRange = false
    if val.kind == JInt and lo.kind == JInt and hi.kind == JInt:
      inRange = val.getInt >= lo.getInt and val.getInt <= hi.getInt
    return newJBool(if expr.betweenNot: not inRange else: inRange)

  of exLike:
    # Simple LIKE: only handle % wildcard at start/end
    let val = evalExpr(expr.likeExpr, row)
    let pat = evalExpr(expr.likePattern, row)
    if val.kind == JString and pat.kind == JString:
      let s = val.getStr
      let p = pat.getStr
      var matches = false
      if p.startsWith("%") and p.endsWith("%"):
        matches = p[1..^2] in s
      elif p.startsWith("%"):
        matches = s.endsWith(p[1..^1])
      elif p.endsWith("%"):
        matches = s.startsWith(p[0..^2])
      else:
        matches = s == p
      return newJBool(if expr.likeNot: not matches else: matches)
    return newJBool(false)

  of exStar, exParam, exList:
    return newJNull()

proc matchesFilter*(filter: Option[Expr], row: JsonNode): bool =
  ## Check if a row passes the WHERE filter.
  if filter.isNone:
    return true
  let result = evalExpr(filter.get(), row)
  result.kind == JBool and result.getBool(false)

# ---------------------------------------------------------------------------
# Row helpers
# ---------------------------------------------------------------------------

proc jsonToStringValue(j: JsonNode): string =
  case j.kind
  of JString: j.getStr
  of JInt: $j.getInt
  of JFloat: $j.getFloat
  of JBool: $j.getBool
  of JNull: "NULL"
  else: $j

proc extractColumns(row: JsonNode, columns: seq[string]): seq[string] =
  for col in columns:
    if row.hasKey(col):
      result.add(jsonToStringValue(row[col]))
    else:
      result.add("NULL")

proc getPkValue(row: JsonNode, pkColumn: string): string =
  if row.hasKey(pkColumn):
    let v = row[pkColumn]
    case v.kind
    of JString: return v.getStr
    of JInt: return $v.getInt
    else: return $v
  ""

# ---------------------------------------------------------------------------
# Per-op executors
# ---------------------------------------------------------------------------

proc execCreateDatabase(op: PlanOp, ctx: ExecutorContext): ExecResult =
  ## Execute CREATE DATABASE with internal MVCC transaction for consistency.
  let key = encodeTableKey(SYS_DATABASES_TABLE_ID, op.cdbName)

  # Create internal transaction
  let txnRes = ctx.client.beginTxn()
  if not txnRes.isOk:
    return errorResult(&"failed to start internal transaction: {txnRes.err}")
  let internalTxnId = txnRes.val.txnId
  let internalReadTimestamp = txnRes.val.readTimestamp

  # Check for duplicate (within transaction snapshot)
  let existing = ctx.client.kvGet(key, txnId = internalTxnId,
      readTimestamp = internalReadTimestamp)
  if existing.isOk and existing.val.isSome:
    discard ctx.client.rollbackTxn(internalTxnId)
    if op.cdbIfNotExists:
      return okResult("database already exists (IF NOT EXISTS)")
    return errorResult(&"database '{op.cdbName}' already exists")

  # Write database record (binary encoded - value already encoded by planner)
  let putRes = ctx.client.kvPut(key, op.cdbValue, txnId = internalTxnId)
  if not putRes.isOk:
    discard ctx.client.rollbackTxn(internalTxnId)
    return errorResult(&"failed to create database: {putRes.err}")

  # Seed a default "public" schema for every new database
  let pubKey = encodeTableKey(SYS_SCHEMAS_TABLE_ID, op.cdbName & ".public")
  let pubRec = SchemaRecord(
    name: "public",
    database: op.cdbName,
    createdAtNs: nowNs()
  )
  let pubPutRes = ctx.client.kvPut(pubKey, encode(pubRec),
      txnId = internalTxnId)
  if not pubPutRes.isOk:
    discard ctx.client.rollbackTxn(internalTxnId)
    return errorResult(&"failed to create public schema: {pubPutRes.err}")

  # Commit the transaction
  let commitRes = ctx.client.commitTxn(internalTxnId)
  if not commitRes.isOk:
    return errorResult(&"failed to commit: {commitRes.err}")

  okResult(&"CREATE DATABASE")

proc execDropDatabase(op: PlanOp, ctx: ExecutorContext): ExecResult =
  ## Execute DROP DATABASE with internal MVCC transaction for consistency.
  let key = encodeTableKey(SYS_DATABASES_TABLE_ID, op.ddbName)

  # Create internal transaction
  let txnRes = ctx.client.beginTxn()
  if not txnRes.isOk:
    return errorResult(&"failed to start internal transaction: {txnRes.err}")
  let internalTxnId = txnRes.val.txnId
  let internalReadTimestamp = txnRes.val.readTimestamp

  # Check if database exists
  let existing = ctx.client.kvGet(key, txnId = internalTxnId,
      readTimestamp = internalReadTimestamp)
  if not existing.isOk or existing.val.isNone:
    discard ctx.client.rollbackTxn(internalTxnId)
    if op.ddbIfExists:
      return okResult("database does not exist (IF EXISTS)")
    return errorResult(&"database '{op.ddbName}' does not exist")

  # Always cascade: delete all schemas, tables, and data rows for this database
  # Delete all schemas for this database
  let schemaPrefix = op.ddbName & "."
  let schemaStart = encodeTableKey(SYS_SCHEMAS_TABLE_ID, schemaPrefix)
  let schemaEnd = encodeTableKey(SYS_SCHEMAS_TABLE_ID, schemaPrefix & "\xFF")
  let schemaScan = ctx.client.kvScan(schemaStart, schemaEnd, 0,
      txnId = internalTxnId, readTimestamp = internalReadTimestamp)
  if schemaScan.isOk:
    for entry in schemaScan.val:
      let delRes = ctx.client.kvDelete(entry.key, txnId = internalTxnId)
      if not delRes.isOk:
        discard ctx.client.rollbackTxn(internalTxnId)
        return errorResult(&"failed to delete schema: {delRes.err}")

  # Find and delete all tables and their data rows
  let tableStart = encodeTableKey(SYS_TABLES_TABLE_ID, "")
  let tableEnd = makeScanEndKey(SYS_TABLES_TABLE_ID)
  let tableScan = ctx.client.kvScan(tableStart, tableEnd, 0,
      txnId = internalTxnId, readTimestamp = internalReadTimestamp)
  if tableScan.isOk:
    for entry in tableScan.val:
      let rec = decodeTableRecord(entry.value)
      if rec.database == op.ddbName:
        let tableId = rec.tableId
        # Delete all data rows for this table
        let dataStart = encodeDataRowKey(tableId, "")
        let dataEnd = makeDataRowScanEndKey(tableId)
        let dataScan = ctx.client.kvScan(dataStart, dataEnd, 0,
            txnId = internalTxnId, readTimestamp = internalReadTimestamp)
        if dataScan.isOk:
          for dataEntry in dataScan.val:
            let delRes = ctx.client.kvDelete(dataEntry.key,
                txnId = internalTxnId)
            if not delRes.isOk:
              discard ctx.client.rollbackTxn(internalTxnId)
              return errorResult(&"failed to delete data row: {delRes.err}")
        # Delete the table record
        let delRes = ctx.client.kvDelete(entry.key, txnId = internalTxnId)
        if not delRes.isOk:
          discard ctx.client.rollbackTxn(internalTxnId)
          return errorResult(&"failed to delete table: {delRes.err}")

  # Delete the database record
  let delRes = ctx.client.kvDelete(key, txnId = internalTxnId)
  if not delRes.isOk:
    discard ctx.client.rollbackTxn(internalTxnId)
    return errorResult(&"failed to drop database: {delRes.err}")

  # Commit the transaction
  let commitRes = ctx.client.commitTxn(internalTxnId)
  if not commitRes.isOk:
    return errorResult(&"failed to commit: {commitRes.err}")

  okResult("DROP DATABASE")

proc execCreateSchema(op: PlanOp, ctx: ExecutorContext): ExecResult =
  ## Execute CREATE SCHEMA with internal MVCC transaction for consistency.
  let key = encodeTableKey(SYS_SCHEMAS_TABLE_ID,
      op.csDatabase & "." & op.csName)

  # Create internal transaction
  let txnRes = ctx.client.beginTxn()
  if not txnRes.isOk:
    return errorResult(&"failed to start internal transaction: {txnRes.err}")
  let internalTxnId = txnRes.val.txnId
  let internalReadTimestamp = txnRes.val.readTimestamp

  let existing = ctx.client.kvGet(key, txnId = internalTxnId,
      readTimestamp = internalReadTimestamp)
  if existing.isOk and existing.val.isSome:
    discard ctx.client.rollbackTxn(internalTxnId)
    if op.csIfNotExists:
      return okResult("schema already exists (IF NOT EXISTS)")
    return errorResult(&"schema '{op.csName}' already exists")

  let putRes = ctx.client.kvPut(key, op.csValue, txnId = internalTxnId)
  if not putRes.isOk:
    discard ctx.client.rollbackTxn(internalTxnId)
    return errorResult(&"failed to create schema: {putRes.err}")

  let commitRes = ctx.client.commitTxn(internalTxnId)
  if not commitRes.isOk:
    return errorResult(&"failed to commit: {commitRes.err}")

  okResult("CREATE SCHEMA")

proc execDropSchema(op: PlanOp, ctx: ExecutorContext): ExecResult =
  ## Execute DROP SCHEMA with internal MVCC transaction for consistency.
  let key = encodeTableKey(SYS_SCHEMAS_TABLE_ID,
      op.dsDatabase & "." & op.dsName)

  # Create internal transaction
  let txnRes = ctx.client.beginTxn()
  if not txnRes.isOk:
    return errorResult(&"failed to start internal transaction: {txnRes.err}")
  let internalTxnId = txnRes.val.txnId
  let internalReadTimestamp = txnRes.val.readTimestamp

  let existing = ctx.client.kvGet(key, txnId = internalTxnId,
      readTimestamp = internalReadTimestamp)
  if not existing.isOk or existing.val.isNone:
    discard ctx.client.rollbackTxn(internalTxnId)
    if op.dsIfExists:
      return okResult("schema does not exist (IF EXISTS)")
    return errorResult(&"schema '{op.dsName}' does not exist")

  let delRes = ctx.client.kvDelete(key, txnId = internalTxnId)
  if not delRes.isOk:
    discard ctx.client.rollbackTxn(internalTxnId)
    return errorResult(&"failed to drop schema: {delRes.err}")

  let commitRes = ctx.client.commitTxn(internalTxnId)
  if not commitRes.isOk:
    return errorResult(&"failed to commit: {commitRes.err}")

  okResult("DROP SCHEMA")

proc execCreateTable(op: PlanOp, ctx: ExecutorContext): ExecResult =
  ## Execute CREATE TABLE with internal MVCC transaction for consistency.
  let key = encodeTableKey(SYS_TABLES_TABLE_ID,
      op.ctDatabase & "." & op.ctSchema & "." & op.ctName)

  # Create internal transaction
  let txnRes = ctx.client.beginTxn()
  if not txnRes.isOk:
    return errorResult(&"failed to start internal transaction: {txnRes.err}")
  let internalTxnId = txnRes.val.txnId
  let internalReadTimestamp = txnRes.val.readTimestamp

  let existing = ctx.client.kvGet(key, txnId = internalTxnId,
      readTimestamp = internalReadTimestamp)
  if existing.isOk and existing.val.isSome:
    discard ctx.client.rollbackTxn(internalTxnId)
    if op.ctIfNotExists:
      return okResult("table already exists (IF NOT EXISTS)")
    return errorResult(&"table '{op.ctName}' already exists")

# Resolve space name to spaceId
  # Note: We do NOT use the transaction's read timestamp for this lookup.
  # CREATE SPACE writes the space record, and we need to see that write immediately.
  # Using the transaction's read timestamp would cause us to not see the newly created space.
  var tableValue = op.ctValue
  if op.ctSpaceName.isSome:
    let spaceName = op.ctSpaceName.get()
    let sStart = encodeTableKey(SYS_SPACES_TABLE_ID, "")
    let sEnd = makeScanEndKey(SYS_SPACES_TABLE_ID)
    # Use a fresh scan WITHOUT the transaction's read timestamp to see recent writes
    let sScan = ctx.client.kvScan(sStart, sEnd, 0, txnId = zeroTransactionID(),
        readTimestamp = 0)
    var spaceId: SpaceID
    var spaceFound = false
    if sScan.isOk:
      for entry in sScan.val:
        let rec = decodeSpaceRecord(entry.value)
        if rec.name == spaceName:
          spaceId = SpaceID(rec.spaceId) # Convert ULID to SpaceID
          spaceFound = true
          break
    if not spaceFound:
      discard ctx.client.rollbackTxn(internalTxnId)
      return errorResult(&"space '{spaceName}' does not exist")
    # Update spaceId in the binary table record
    var rec = decodeTableRecord(tableValue)
    rec.spaceId = spaceId
    tableValue = encode(rec)

  let putRes = ctx.client.kvPut(key, tableValue, txnId = internalTxnId)
  if not putRes.isOk:
    discard ctx.client.rollbackTxn(internalTxnId)
    return errorResult(&"failed to create table: {putRes.err}")

  let commitRes = ctx.client.commitTxn(internalTxnId)
  if not commitRes.isOk:
    return errorResult(&"failed to commit: {commitRes.err}")

  okResult("CREATE TABLE")

proc execDropTable(op: PlanOp, ctx: ExecutorContext): ExecResult =
  ## Execute DROP TABLE with internal MVCC transaction for consistency.
  let key = encodeTableKey(SYS_TABLES_TABLE_ID,
      op.dtDatabase & "." & op.dtSchema & "." & op.dtName)

  # Create internal transaction
  let txnRes = ctx.client.beginTxn()
  if not txnRes.isOk:
    return errorResult(&"failed to start internal transaction: {txnRes.err}")
  let internalTxnId = txnRes.val.txnId
  let internalReadTimestamp = txnRes.val.readTimestamp

  let existing = ctx.client.kvGet(key, txnId = internalTxnId,
      readTimestamp = internalReadTimestamp)
  if not existing.isOk or existing.val.isNone:
    discard ctx.client.rollbackTxn(internalTxnId)
    if op.dtIfExists:
      return okResult("table does not exist (IF EXISTS)")
    return errorResult(&"table '{op.dtName}' does not exist")

  # TODO: also delete all data rows for the table
  let delRes = ctx.client.kvDelete(key, txnId = internalTxnId)
  if not delRes.isOk:
    discard ctx.client.rollbackTxn(internalTxnId)
    return errorResult(&"failed to drop table: {delRes.err}")

  let commitRes = ctx.client.commitTxn(internalTxnId)
  if not commitRes.isOk:
    return errorResult(&"failed to commit: {commitRes.err}")

  okResult("DROP TABLE")

# ---------------------------------------------------------------------------
# Space executors
# ---------------------------------------------------------------------------

proc execCreateSpace(op: PlanOp, ctx: ExecutorContext): ExecResult =
  ## Execute CREATE SPACE via server-side RPC.
  ## The server handles:
  ##   - Validation (duplicate names, replica count)
  ##   - Creating Raft groups on all nodes
  ##   - Waiting for leaders to be elected
  ##   - Writing space/group records to sys tables via Raft
  ## The client receives updated sys table data to update its cache.

  # Call server-side createSpace RPC
  let res = ctx.client.createSpace(op.cspName, int32(op.cspReplicas))

  if not res.isOk:
    return errorResult(&"failed to create space: {res.err}")

  okResult(&"CREATE SPACE ({res.groupCount} groups)")

proc execDropSpace(op: PlanOp, ctx: ExecutorContext): ExecResult =
  ## Execute DROP SPACE via server-side RPC.
  ## The server handles:
  ##   - Validation (space exists, not "default")
  ##   - Marking space/group records as deleted
  ##   - Stopping Raft groups on all nodes
  ## The client receives deleted groupIds to update its cache.

  # Call server-side dropSpace RPC
  let res = ctx.client.dropSpace(op.dspName)

  if not res.isOk:
    return errorResult(&"failed to drop space: {res.err}")

  okResult("DROP SPACE")

# ---------------------------------------------------------------------------
# Transaction-aware KV operation helpers
# ---------------------------------------------------------------------------

proc txnGet(ctx: ExecutorContext, key: string): KVOpResult[Option[string]] =
  ## Get a value, using transactional read if in a transaction,
  ## or latest MVCC read otherwise.
  ctx.client.kvGet(key, txnId = ctx.txnId, readTimestamp = ctx.readTimestamp)

proc execTxnScan(ctx: ExecutorContext, startKey, endKey: string,
    limit: uint32 = 0): KVOpResult[seq[tuple[key, value: string]]] =
  ## Scan keys with MVCC awareness.
  ctx.client.kvScan(startKey, endKey, limit, txnId = ctx.txnId,
                    readTimestamp = ctx.readTimestamp)

# ---------------------------------------------------------------------------
# MVCC-aware show operations
# ---------------------------------------------------------------------------

proc execShowDatabasesTxn(ctx: ExecutorContext): ExecResult =
  let startKey = encodeTableKey(SYS_DATABASES_TABLE_ID, "")
  let endKey = makeScanEndKey(SYS_DATABASES_TABLE_ID)
  let res = execTxnScan(ctx, startKey, endKey, 0)
  if not res.isOk:
    return errorResult(&"failed to scan databases: {res.err}")

  var resultRows: seq[seq[string]]
  for entry in res.val:
    let (rec, isDeleted) = decodeDatabaseRecordFromMVCC(entry.value)
    if not isDeleted:
      resultRows.add(@[rec.name])

  rowsResult(@["database_name"], resultRows)

proc execShowSchemasTxn(op: PlanOp, ctx: ExecutorContext): ExecResult =
  let startKey = encodeTableKey(SYS_SCHEMAS_TABLE_ID, "")
  let endKey = makeScanEndKey(SYS_SCHEMAS_TABLE_ID)
  let res = execTxnScan(ctx, startKey, endKey, 0)
  if not res.isOk:
    return errorResult(&"failed to scan schemas: {res.err}")

  var resultRows: seq[seq[string]]
  for entry in res.val:
    let (rec, isDeleted) = decodeSchemaRecordFromMVCC(entry.value)
    if not isDeleted and (rec.database == op.ssDatabase or op.ssDatabase.len == 0):
      resultRows.add(@[rec.name])

  rowsResult(@["schema_name"], resultRows)

proc execShowTablesTxn(op: PlanOp, ctx: ExecutorContext): ExecResult =
  let startKey = encodeTableKey(SYS_TABLES_TABLE_ID, "")
  let endKey = makeScanEndKey(SYS_TABLES_TABLE_ID)
  let res = execTxnScan(ctx, startKey, endKey, 0)
  if not res.isOk:
    return errorResult(&"failed to scan tables: {res.err}")

  var resultRows: seq[seq[string]]
  for entry in res.val:
    let (rec, isDeleted) = decodeTableRecordFromMVCC(entry.value)
    if not isDeleted and
       (rec.database == op.stDatabase or op.stDatabase.len == 0) and
       (rec.schema == op.stSchema or op.stSchema.len == 0):
      resultRows.add(@[rec.name])

  rowsResult(@["table_name"], resultRows)

proc execShowSpacesTxn(ctx: ExecutorContext): ExecResult =
  ## Transaction-aware SHOW SPACES that can see MVCC-encoded space records.
  let startKey = encodeTableKey(SYS_SPACES_TABLE_ID, "")
  let endKey = makeScanEndKey(SYS_SPACES_TABLE_ID)
  let res = execTxnScan(ctx, startKey, endKey, 0)
  if not res.isOk:
    return errorResult(&"failed to scan spaces: {res.err}")

  var resultRows: seq[seq[string]]
  for entry in res.val:
    let (rec, isDeleted) = decodeSpaceRecordFromMVCC(entry.value)
    if not isDeleted:
      let replicasStr = if rec.replicas == 0: "ALL" else: $rec.replicas
      var groupIdsStr = ""
      for i, gid in rec.groupIds:
        if i > 0: groupIdsStr.add(",")
        groupIdsStr.add($gid)
      resultRows.add(@[$rec.spaceId, rec.name, replicasStr, $rec.groupCount, groupIdsStr])

  rowsResult(@["space_id", "name", "replicas", "group_count", "group_ids"],
             resultRows)

# ---------------------------------------------------------------------------
# Main entry point - unified with implicit transactions for DML
# ---------------------------------------------------------------------------

# Forward declaration
proc executeWithTxn*(plan: Plan, ctx: ExecutorContext): ExecResult

proc execute*(plan: Plan, client: FractioClient,
    database: string = "default"): ExecResult =
  ## Execute a Plan against a FractioClient, returning an ExecResult.
  ## Processes ops sequentially; returns the result of the last op
  ## (or the first error).
  ##
  ## All operations require client for consistency:
  ## - DDL operations use internal auto-commit transactions
  ## - DML operations use implicit transactions if not in an explicit one
  ##
  ## This is the simplified unified entry point.
  if client == nil:
    return errorResult("FractioClient is required for all operations")

  let ctx = newExecutorContext(client, database)
  executeWithTxn(plan, ctx)

# ---------------------------------------------------------------------------
# Transaction-aware execute
# ---------------------------------------------------------------------------

proc executeWithTxn*(plan: Plan, ctx: ExecutorContext): ExecResult =
  ## Execute a Plan with MVCC transaction support.
  ##
  ## All DML operations use MVCC transactions:
  ## - If in an explicit transaction (BEGIN), use that transaction
  ## - If not in a transaction, create an implicit auto-commit transaction
  ##
  ## DDL operations are FORBIDDEN inside explicit transactions.
  ##
  ## The ctx holds the transaction status and IDs.

  proc needsImplicitTxn(): bool =
    ## Check if we need to create an implicit transaction for this operation
    not ctx.hasActiveTransaction

  proc beginImplicitTxn(): bool =
    ## Begin an implicit transaction. Returns true on success.
    let res = ctx.client.beginTxn()
    if res.isOk:
      ctx.txnId = res.val.txnId
      ctx.readTimestamp = res.val.readTimestamp
      ctx.hasActiveTransaction = true
      true
    else:
      false

  proc commitImplicitTxn(): bool =
    ## Commit an implicit transaction. Returns true on success.
    let res = ctx.client.commitTxn(ctx.txnId)
    if res.isOk:
      ctx.hasActiveTransaction = false
      ctx.txnId = zeroTransactionID()
      ctx.readTimestamp = 0
      true
    else:
      false

  proc rollbackImplicitTxn() =
    ## Rollback an implicit transaction.
    discard ctx.client.rollbackTxn(ctx.txnId)
    ctx.hasActiveTransaction = false
    ctx.txnId = zeroTransactionID()
    ctx.readTimestamp = 0

  var lastResult = okResult("empty plan")

  for op in plan.ops:
    lastResult = case op.kind

    # DDL operations: FORBIDDEN inside transactions, auto-commit outside
    of poCreateDatabase:
      if ctx.hasActiveTransaction:
        errorResult("CREATE DATABASE is not allowed inside a transaction")
      else:
        execCreateDatabase(op, ctx)

    of poDropDatabase:
      if ctx.hasActiveTransaction:
        errorResult("DROP DATABASE is not allowed inside a transaction")
      else:
        execDropDatabase(op, ctx)

    of poCreateSchema:
      if ctx.hasActiveTransaction:
        errorResult("CREATE SCHEMA is not allowed inside a transaction")
      else:
        execCreateSchema(op, ctx)

    of poDropSchema:
      if ctx.hasActiveTransaction:
        errorResult("DROP SCHEMA is not allowed inside a transaction")
      else:
        execDropSchema(op, ctx)

    of poCreateTable:
      if ctx.hasActiveTransaction:
        errorResult("CREATE TABLE is not allowed inside a transaction")
      else:
        execCreateTable(op, ctx)

    of poDropTable:
      if ctx.hasActiveTransaction:
        errorResult("DROP TABLE is not allowed inside a transaction")
      else:
        execDropTable(op, ctx)

    of poCreateSpace:
      if ctx.hasActiveTransaction:
        errorResult("CREATE SPACE is not allowed inside a transaction")
      else:
        execCreateSpace(op, ctx)

    of poDropSpace:
      if ctx.hasActiveTransaction:
        errorResult("DROP SPACE is not allowed inside a transaction")
      else:
        execDropSpace(op, ctx)

    # DML operations: use active transaction if one exists, otherwise auto-transaction
    of poInsert:
      var count = 0
      var error: string = ""
      for rowJson in op.insRows:
        let row = parseJson(rowJson)
        let pkVal = getPkValue(row, op.insPkColumn)
        if pkVal.len == 0:
          error = "INSERT requires a primary key value"
          break
        let key = encodeDataRowKey(op.insTableId, pkVal)
        # Use active transaction if available, otherwise auto-transaction
        let res = ctx.client.kvPut(key, rowJson, txnId = ctx.txnId)
        if not res.isOk:
          error = &"failed to insert row: {res.err}"
          break
        inc count

      if error.len > 0:
        return errorResult(error)

      modifiedResult(count, &"INSERT {count}")

    of poPointGet:
      let key = encodeDataRowKey(op.pgTableId, op.pgKey)
      let res = txnGet(ctx, key)
      if not res.isOk:
        return errorResult(&"failed to read: {res.err}")
      if res.val.isNone:
        return rowsResult(op.pgColumns, @[])
      let row = parseJson(res.val.get())
      let vals = extractColumns(row, op.pgColumns)
      rowsResult(op.pgColumns, @[vals])

    of poScan:
      let res = execTxnScan(ctx, op.scStartKey, op.scEndKey, 0)
      if not res.isOk:
        return errorResult(&"failed to scan: {res.err}")

      var resultRows: seq[seq[string]] = @[]
      var count = 0
      for entry in res.val:
        try:
          let row = parseJson(entry.value)
          if matchesFilter(op.scFilter, row):
            resultRows.add(extractColumns(row, op.scColumns))
            inc count
            if op.scLimit > 0 and count >= int(op.scLimit):
              break
        except JsonParsingError:
          discard # skip malformed rows

      rowsResult(op.scColumns, resultRows)

    of poUpdate:
      # MVCC-aware UPDATE
      let startKey = encodeDataRowKey(op.upTableId, "")
      let endKey = makeDataRowScanEndKey(op.upTableId)
      # Use transaction context for consistent scan
      let res = ctx.client.kvScan(startKey, endKey, 0, txnId = ctx.txnId,
          readTimestamp = ctx.readTimestamp)

      if not res.isOk:
        return errorResult(&"failed to scan for update: {res.err}")

      var count = 0
      var error: string = ""
      for entry in res.val:
        try:
          let row = parseJson(entry.value)
          if matchesFilter(op.upFilter, row):
            var updated = row.copy()
            for (col, valExpr) in op.upSets:
              updated[col] = evalExpr(valExpr, row)
            # Use active transaction if available
            let putRes = ctx.client.kvPut(entry.key, $updated,
                txnId = ctx.txnId)
            if not putRes.isOk:
              error = &"failed to update row: {putRes.err}"
              break
            inc count
        except JsonParsingError:
          discard

      if error.len > 0:
        return errorResult(error)

      modifiedResult(count, &"UPDATE {count}")

    of poDelete:
      # MVCC-aware DELETE
      let startKey = encodeDataRowKey(op.delTableId, "")
      let endKey = makeDataRowScanEndKey(op.delTableId)
      # Use transaction context for consistent scan
      let res = ctx.client.kvScan(startKey, endKey, 0, txnId = ctx.txnId,
          readTimestamp = ctx.readTimestamp)

      if not res.isOk:
        return errorResult(&"failed to scan for delete: {res.err}")

      var count = 0
      var error: string = ""
      for entry in res.val:
        try:
          let row = parseJson(entry.value)
          if matchesFilter(op.delFilter, row):
            # Use active transaction if available
            let delRes = ctx.client.kvDelete(entry.key, txnId = ctx.txnId)
            if not delRes.isOk:
              error = &"failed to delete row: {delRes.err}"
              break
            inc count
        except JsonParsingError:
          discard

      if error.len > 0:
        return errorResult(error)

      modifiedResult(count, &"DELETE {count}")

    of poShowDatabases: execShowDatabasesTxn(ctx)
    of poShowSchemas: execShowSchemasTxn(op, ctx)
    of poShowTables: execShowTablesTxn(op, ctx)
    of poShowSpaces: execShowSpacesTxn(ctx)

    of poUseDatabase:
      let key = encodeTableKey(SYS_DATABASES_TABLE_ID, op.udName)
      let existing = txnGet(ctx, key)
      if not existing.isOk or existing.val.isNone:
        errorResult(&"database '{op.udName}' does not exist")
      else:
        ExecResult(kind: erkUseDatabase, newDatabase: op.udName)

    of poUseSchema:
      let key = encodeTableKey(SYS_SCHEMAS_TABLE_ID, ctx.database & "." & op.usName)
      let existing = txnGet(ctx, key)
      if not existing.isOk or existing.val.isNone:
        errorResult(&"schema '{op.usName}' does not exist in database '{ctx.database}'")
      else:
        ExecResult(kind: erkUseSchema, newSchema: op.usName)

    of poBeginTxn:
      if ctx.hasActiveTransaction:
        okResult("BEGIN (transaction already active)")
      else:
        let res = ctx.client.beginTxn()
        if res.isOk:
          ctx.txnId = res.val.txnId
          ctx.readTimestamp = res.val.readTimestamp
          ctx.hasActiveTransaction = true
          ctx.client.activeTxnId = res.val.txnId
          ctx.client.activeReadTs = res.val.readTimestamp
          okResult("BEGIN")
        else:
          errorResult(&"failed to begin transaction: {res.err}")

    of poCommitTxn:
      if not ctx.hasActiveTransaction:
        okResult("COMMIT (no active transaction)")
      else:
        let res = ctx.client.commitTxn(ctx.txnId)
        if res.isOk:
          ctx.hasActiveTransaction = false
          ctx.txnId = zeroTransactionID()
          ctx.readTimestamp = 0
          ctx.client.activeTxnId = zeroTransactionID()
          ctx.client.activeReadTs = 0
          okResult("COMMIT")
        else:
          errorResult(&"failed to commit transaction: {res.err}")

    of poRollbackTxn:
      if not ctx.hasActiveTransaction:
        okResult("ROLLBACK (no active transaction)")
      else:
        let res = ctx.client.rollbackTxn(ctx.txnId)
        if res.isOk:
          ctx.hasActiveTransaction = false
          ctx.txnId = zeroTransactionID()
          ctx.readTimestamp = 0
          ctx.client.activeTxnId = zeroTransactionID()
          ctx.client.activeReadTs = 0
          okResult("ROLLBACK")
        else:
          errorResult(&"failed to rollback transaction: {res.err}")

    of poExplain:
      let text = formatPlan(op.exInnerPlan)
      var rows: seq[seq[string]]
      for line in text.split('\n'):
        rows.add(@[line])
      rowsResult(@["plan"], rows)

    if lastResult.kind == erkError:
      return lastResult

  lastResult

