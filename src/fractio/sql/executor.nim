# SQL Executor for Fractio
#
# Executes a Plan against a RaftKVStoreExt, returning results.
# Each PlanOp maps directly to KV operations on the raft store.

import std/[options, json, strutils, strformat, tables]
import ./ast
import ./parser
import ./planner
import ../distributed/meta/system_tables
import ../protocol/raft_store
import ../core/types as coreTypes

# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

type
  ExecResultKind* = enum
    erkRows      ## SELECT results
    erkModified  ## INSERT/UPDATE/DELETE affected rows
    erkOk        ## DDL success
    erkError     ## Error

  ExecResult* = ref object
    case kind*: ExecResultKind
    of erkRows:
      columns*: seq[string]
      rows*: seq[seq[string]]  # each row is column values as strings
    of erkModified:
      count*: int
      message*: string
    of erkOk:
      okMessage*: string
    of erkError:
      error*: string

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

proc execCreateDatabase(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  let key = encodeTableKey(SYS_DATABASES_TABLE_ID, op.cdbName)

  # Check for duplicate
  let existing = store.raftGet(key)
  if existing.isOk and existing.value.isSome:
    if op.cdbIfNotExists:
      return okResult("database already exists (IF NOT EXISTS)")
    return errorResult(&"database '{op.cdbName}' already exists")

  let res = store.raftPut(key, op.cdbValue)
  if not res.isOk:
    return errorResult(&"failed to create database: {res.error.msg}")
  okResult(&"CREATE DATABASE")

proc execDropDatabase(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  let key = encodeTableKey(SYS_DATABASES_TABLE_ID, op.ddbName)

  let existing = store.raftGet(key)
  if not existing.isOk or existing.value.isNone:
    if op.ddbIfExists:
      return okResult("database does not exist (IF EXISTS)")
    return errorResult(&"database '{op.ddbName}' does not exist")

  let res = store.raftDelete(key)
  if not res.isOk:
    return errorResult(&"failed to drop database: {res.error.msg}")
  okResult("DROP DATABASE")

proc execCreateSchema(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  let key = encodeTableKey(SYS_SCHEMAS_TABLE_ID,
      op.csDatabase & "." & op.csName)

  let existing = store.raftGet(key)
  if existing.isOk and existing.value.isSome:
    if op.csIfNotExists:
      return okResult("schema already exists (IF NOT EXISTS)")
    return errorResult(&"schema '{op.csName}' already exists")

  let res = store.raftPut(key, op.csValue)
  if not res.isOk:
    return errorResult(&"failed to create schema: {res.error.msg}")
  okResult("CREATE SCHEMA")

proc execDropSchema(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  let key = encodeTableKey(SYS_SCHEMAS_TABLE_ID,
      op.dsDatabase & "." & op.dsName)

  let existing = store.raftGet(key)
  if not existing.isOk or existing.value.isNone:
    if op.dsIfExists:
      return okResult("schema does not exist (IF EXISTS)")
    return errorResult(&"schema '{op.dsName}' does not exist")

  let res = store.raftDelete(key)
  if not res.isOk:
    return errorResult(&"failed to drop schema: {res.error.msg}")
  okResult("DROP SCHEMA")

proc execCreateTable(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  let key = encodeTableKey(SYS_TABLES_TABLE_ID,
      op.ctDatabase & "." & op.ctSchema & "." & op.ctName)

  let existing = store.raftGet(key)
  if existing.isOk and existing.value.isSome:
    if op.ctIfNotExists:
      return okResult("table already exists (IF NOT EXISTS)")
    return errorResult(&"table '{op.ctName}' already exists")

  let res = store.raftPut(key, op.ctValue)
  if not res.isOk:
    return errorResult(&"failed to create table: {res.error.msg}")
  okResult("CREATE TABLE")

proc execDropTable(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  let key = encodeTableKey(SYS_TABLES_TABLE_ID,
      op.dtDatabase & "." & op.dtSchema & "." & op.dtName)

  let existing = store.raftGet(key)
  if not existing.isOk or existing.value.isNone:
    if op.dtIfExists:
      return okResult("table does not exist (IF EXISTS)")
    return errorResult(&"table '{op.dtName}' does not exist")

  # TODO: also delete all data rows for the table
  let res = store.raftDelete(key)
  if not res.isOk:
    return errorResult(&"failed to drop table: {res.error.msg}")
  okResult("DROP TABLE")

proc execInsert(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  var count = 0
  for rowJson in op.insRows:
    let row = parseJson(rowJson)
    let pkVal = getPkValue(row, op.insPkColumn)
    if pkVal.len == 0:
      return errorResult("INSERT requires a primary key value")
    let key = encodeDataRowKey(op.insTableId, pkVal)
    let res = store.raftPut(key, rowJson)
    if not res.isOk:
      return errorResult(&"failed to insert row: {res.error.msg}")
    inc count
  modifiedResult(count, &"INSERT {count}")

proc execPointGet(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  let key = encodeDataRowKey(op.pgTableId, op.pgKey)
  let res = store.raftGet(key)
  if not res.isOk:
    return errorResult(&"failed to read: {res.error.msg}")
  if res.value.isNone:
    return rowsResult(op.pgColumns, @[])

  let row = parseJson(res.value.get().value)
  let vals = extractColumns(row, op.pgColumns)
  rowsResult(op.pgColumns, @[vals])

proc execScan(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  # Scan uses includeSystemKeys=false for data rows (they are user table keys,
  # not system table keys), but the scan range is already scoped to the data
  # row prefix. We need includeSystemKeys=true since data row keys live under
  # /t/<tableId>/d/... which may not be system keys but are still table keys.
  let res = store.raftScan(op.scStartKey, op.scEndKey, 0,
      includeSystemKeys = true)
  if not res.isOk:
    return errorResult(&"failed to scan: {res.error.msg}")

  var resultRows: seq[seq[string]]
  var count = 0
  for (key, entry) in res.value:
    try:
      let row = parseJson(entry.value)
      if matchesFilter(op.scFilter, row):
        resultRows.add(extractColumns(row, op.scColumns))
        inc count
        if op.scLimit > 0 and count >= int(op.scLimit):
          break
    except JsonParsingError:
      discard  # skip malformed rows

  rowsResult(op.scColumns, resultRows)

proc execUpdate(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  let startKey = encodeDataRowKey(op.upTableId, "")
  let endKey = encodeDataRowKey(op.upTableId + 1, "")
  let res = store.raftScan(startKey, endKey, 0, includeSystemKeys = true)
  if not res.isOk:
    return errorResult(&"failed to scan for update: {res.error.msg}")

  var count = 0
  for (key, entry) in res.value:
    try:
      let row = parseJson(entry.value)
      if matchesFilter(op.upFilter, row):
        # Apply SET clauses
        var updated = row.copy()
        for (col, valExpr) in op.upSets:
          updated[col] = evalExpr(valExpr, row)
        let putRes = store.raftPut(key, $updated)
        if not putRes.isOk:
          return errorResult(&"failed to update row: {putRes.error.msg}")
        inc count
    except JsonParsingError:
      discard

  modifiedResult(count, &"UPDATE {count}")

proc execDelete(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  let startKey = encodeDataRowKey(op.delTableId, "")
  let endKey = encodeDataRowKey(op.delTableId + 1, "")
  let res = store.raftScan(startKey, endKey, 0, includeSystemKeys = true)
  if not res.isOk:
    return errorResult(&"failed to scan for delete: {res.error.msg}")

  var count = 0
  for (key, entry) in res.value:
    try:
      let row = parseJson(entry.value)
      if matchesFilter(op.delFilter, row):
        let delRes = store.raftDelete(key)
        if not delRes.isOk:
          return errorResult(&"failed to delete row: {delRes.error.msg}")
        inc count
    except JsonParsingError:
      discard

  modifiedResult(count, &"DELETE {count}")

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

proc execute*(plan: Plan, store: RaftKVStoreExt): ExecResult =
  ## Execute a Plan against a RaftKVStoreExt, returning an ExecResult.
  ## Processes ops sequentially; returns the result of the last op
  ## (or the first error).
  var lastResult = okResult("empty plan")

  for op in plan.ops:
    lastResult = case op.kind
    of poCreateDatabase: execCreateDatabase(op, store)
    of poDropDatabase:   execDropDatabase(op, store)
    of poCreateSchema:   execCreateSchema(op, store)
    of poDropSchema:     execDropSchema(op, store)
    of poCreateTable:    execCreateTable(op, store)
    of poDropTable:      execDropTable(op, store)
    of poInsert:         execInsert(op, store)
    of poPointGet:       execPointGet(op, store)
    of poScan:           execScan(op, store)
    of poUpdate:         execUpdate(op, store)
    of poDelete:         execDelete(op, store)
    of poBeginTxn:       okResult("BEGIN")
    of poCommitTxn:      okResult("COMMIT")
    of poRollbackTxn:    okResult("ROLLBACK")

    if lastResult.kind == erkError:
      return lastResult

  lastResult

# ---------------------------------------------------------------------------
# Convenience: parse + plan + execute in one call
# ---------------------------------------------------------------------------

proc executeSQL*(sql: string, store: RaftKVStoreExt,
    database: string = "default",
    schema: string = "public"): ExecResult =
  ## Parse a SQL statement, plan it, and execute it.
  try:
    let stmts = parseAll(sql)
    if stmts.len == 0:
      return errorResult("empty SQL statement")
    var lastResult = okResult("ok")
    for stmt in stmts:
      let plan = planStatement(stmt, store, database, schema)
      lastResult = execute(plan, store)
      if lastResult.kind == erkError:
        return lastResult
    lastResult
  except PlanError as e:
    errorResult(e.msg)
  except CatchableError as e:
    errorResult(&"SQL error: {e.msg}")

