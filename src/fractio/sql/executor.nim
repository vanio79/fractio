# SQL Executor for Fractio
#
# Executes a Plan against a RaftKVStoreExt, returning results.
# Each PlanOp maps directly to KV operations on the raft store.

import std/[options, json, strutils, strformat, tables, times, hashes, algorithm]
import ./ast
import ./parser
import ./planner
import ../distributed/meta/system_tables
import ../protocol/raft_store
import ../core/types as coreTypes
import ../distributed/raft/multigroup_coordinator
import ../distributed/raft/group_types as rangeTypes
import ../distributed/raft/multigroup_types

# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

type
  ExecResultKind* = enum
    erkRows         ## SELECT results
    erkModified     ## INSERT/UPDATE/DELETE affected rows
    erkOk           ## DDL success
    erkError        ## Error
    erkUseDatabase  ## USE DATABASE — caller should update session context
    erkUseSchema    ## USE SCHEMA — caller should update session context

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
    of erkUseDatabase:
      newDatabase*: string
    of erkUseSchema:
      newSchema*: string

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
# Space routing helper
# ---------------------------------------------------------------------------

proc getTableSpace(store: RaftKVStoreExt, tableId: uint32): Option[SpaceInfo] =
  ## Returns Some(SpaceInfo) if the table is in a non-default space with >1 group.
  ## Returns none otherwise (fall through to existing shard-based routing).
  let spaceOpt = store.getSpaceForTable(tableId)
  if spaceOpt.isSome:
    let space = spaceOpt.get()
    if space.groupIds.len > 1:
      return some(space)
  none(SpaceInfo)

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

  # Seed a default "public" schema for every new database
  let pubKey = encodeTableKey(SYS_SCHEMAS_TABLE_ID, op.cdbName & ".public")
  let pubVal = $ %* {"name": "public", "database": op.cdbName}
  discard store.raftPut(pubKey, pubVal)

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

  # Resolve space name to spaceId
  var tableValue = op.ctValue
  if op.ctSpaceName.isSome:
    let spaceName = op.ctSpaceName.get()
    # Scan sys.spaces for the named space
    let sStart = encodeTableKey(SYS_SPACES_TABLE_ID, "")
    let sEnd = encodeTableKey(SYS_SPACES_TABLE_ID + 1, "")
    let sScan = store.raftScan(sStart, sEnd, 0, includeSystemKeys = true)
    var spaceId = -1
    if sScan.isOk:
      for (sk, se) in sScan.value:
        try:
          let sj = parseJson(se.value)
          if sj["name"].getStr() == spaceName:
            spaceId = sj["spaceId"].getInt()
            break
        except JsonParsingError:
          discard
    if spaceId < 0:
      return errorResult(&"space '{spaceName}' does not exist")
    # Inject spaceId into the table descriptor JSON
    try:
      var j = parseJson(tableValue)
      j["spaceId"] = %spaceId
      tableValue = $j
    except JsonParsingError:
      discard

  let res = store.raftPut(key, tableValue)
  if not res.isOk:
    return errorResult(&"failed to create table: {res.error.msg}")

  # Reload table-space caches so the new table is immediately routable
  if op.ctSpaceName.isSome:
    store.loadTableSpaces()

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
  let spaceOpt = getTableSpace(store, op.insTableId)
  var count = 0
  for rowJson in op.insRows:
    let row = parseJson(rowJson)
    let pkVal = getPkValue(row, op.insPkColumn)
    if pkVal.len == 0:
      return errorResult("INSERT requires a primary key value")
    let key = encodeDataRowKey(op.insTableId, pkVal)
    if spaceOpt.isSome:
      let res = store.raftPutInSpace(key, rowJson, spaceOpt.get(), pkVal)
      if not res.isOk:
        return errorResult(&"failed to insert row: {res.error.msg}")
    else:
      let res = store.raftPut(key, rowJson)
      if not res.isOk:
        return errorResult(&"failed to insert row: {res.error.msg}")
    inc count
  modifiedResult(count, &"INSERT {count}")

proc execPointGet(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  let key = encodeDataRowKey(op.pgTableId, op.pgKey)
  let spaceOpt = getTableSpace(store, op.pgTableId)
  let res = if spaceOpt.isSome:
              store.raftGetInSpace(key, spaceOpt.get(), op.pgKey)
            else:
              store.raftGet(key)
  if not res.isOk:
    return errorResult(&"failed to read: {res.error.msg}")
  if res.value.isNone:
    return rowsResult(op.pgColumns, @[])

  let row = parseJson(res.value.get().value)
  let vals = extractColumns(row, op.pgColumns)
  rowsResult(op.pgColumns, @[vals])

proc execScan(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  let spaceOpt = getTableSpace(store, op.scTableId)
  let res = if spaceOpt.isSome:
              store.raftScanSpace(op.scStartKey, op.scEndKey, spaceOpt.get(),
                  0, includeSystemKeys = true)
            else:
              store.raftScan(op.scStartKey, op.scEndKey, 0,
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
  let spaceOpt = getTableSpace(store, op.upTableId)
  let res = if spaceOpt.isSome:
              store.raftScanSpace(startKey, endKey, spaceOpt.get(), 0,
                  includeSystemKeys = true)
            else:
              store.raftScan(startKey, endKey, 0, includeSystemKeys = true)
  if not res.isOk:
    return errorResult(&"failed to scan for update: {res.error.msg}")

  var count = 0
  for (key, entry) in res.value:
    try:
      let row = parseJson(entry.value)
      if matchesFilter(op.upFilter, row):
        var updated = row.copy()
        for (col, valExpr) in op.upSets:
          updated[col] = evalExpr(valExpr, row)
        let pkVal = getPkValue(updated, op.upPkColumn)
        if spaceOpt.isSome and pkVal.len > 0:
          let putRes = store.raftPutInSpace(key, $updated, spaceOpt.get(), pkVal)
          if not putRes.isOk:
            return errorResult(&"failed to update row: {putRes.error.msg}")
        else:
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
  let spaceOpt = getTableSpace(store, op.delTableId)
  let res = if spaceOpt.isSome:
              store.raftScanSpace(startKey, endKey, spaceOpt.get(), 0,
                  includeSystemKeys = true)
            else:
              store.raftScan(startKey, endKey, 0, includeSystemKeys = true)
  if not res.isOk:
    return errorResult(&"failed to scan for delete: {res.error.msg}")

  var count = 0
  for (key, entry) in res.value:
    try:
      let row = parseJson(entry.value)
      if matchesFilter(op.delFilter, row):
        let pkVal = getPkValue(row, op.delPkColumn)
        if spaceOpt.isSome and pkVal.len > 0:
          let delRes = store.raftDeleteInSpace(key, spaceOpt.get(), pkVal)
          if not delRes.isOk:
            return errorResult(&"failed to delete row: {delRes.error.msg}")
        else:
          let delRes = store.raftDelete(key)
          if not delRes.isOk:
            return errorResult(&"failed to delete row: {delRes.error.msg}")
        inc count
    except JsonParsingError:
      discard

  modifiedResult(count, &"DELETE {count}")

proc execUseDatabase(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  # Verify the database exists
  let key = encodeTableKey(SYS_DATABASES_TABLE_ID, op.udName)
  let existing = store.raftGet(key)
  if not existing.isOk or existing.value.isNone:
    return errorResult(&"database '{op.udName}' does not exist")
  ExecResult(kind: erkUseDatabase, newDatabase: op.udName)

proc execUseSchema(op: PlanOp, store: RaftKVStoreExt,
    database: string): ExecResult =
  # Verify the schema exists in the current database
  let key = encodeTableKey(SYS_SCHEMAS_TABLE_ID,
      database & "." & op.usName)
  let existing = store.raftGet(key)
  if not existing.isOk or existing.value.isNone:
    return errorResult(&"schema '{op.usName}' does not exist in database '{database}'")
  ExecResult(kind: erkUseSchema, newSchema: op.usName)

proc execShowDatabases(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  let startKey = encodeTableKey(SYS_DATABASES_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_DATABASES_TABLE_ID + 1, "")
  let res = store.raftScan(startKey, endKey, 0, includeSystemKeys = true)
  if not res.isOk:
    return errorResult(&"failed to scan databases: {res.error.msg}")

  var resultRows: seq[seq[string]]
  for (key, entry) in res.value:
    try:
      let j = parseJson(entry.value)
      resultRows.add(@[j["name"].getStr()])
    except JsonParsingError:
      # Fall back to extracting name from the key
      let decoded = decodeTableKey(key)
      resultRows.add(@[decoded.primaryKey])

  rowsResult(@["database_name"], resultRows)

proc execShowSchemas(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  let prefix = op.ssDatabase & "."
  let startKey = encodeTableKey(SYS_SCHEMAS_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_SCHEMAS_TABLE_ID + 1, "")
  let res = store.raftScan(startKey, endKey, 0, includeSystemKeys = true)
  if not res.isOk:
    return errorResult(&"failed to scan schemas: {res.error.msg}")

  var resultRows: seq[seq[string]]
  for (key, entry) in res.value:
    try:
      let j = parseJson(entry.value)
      let db = j.getOrDefault("database").getStr("")
      let name = j["name"].getStr()
      if db == op.ssDatabase or op.ssDatabase.len == 0:
        resultRows.add(@[name])
    except JsonParsingError:
      let decoded = decodeTableKey(key)
      let pk = decoded.primaryKey
      if pk.startsWith(prefix):
        resultRows.add(@[pk[prefix.len .. ^1]])

  rowsResult(@["schema_name"], resultRows)

proc execShowTables(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  let prefix = op.stDatabase & "." & op.stSchema & "."
  let startKey = encodeTableKey(SYS_TABLES_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_TABLES_TABLE_ID + 1, "")
  let res = store.raftScan(startKey, endKey, 0, includeSystemKeys = true)
  if not res.isOk:
    return errorResult(&"failed to scan tables: {res.error.msg}")

  var resultRows: seq[seq[string]]
  for (key, entry) in res.value:
    try:
      let j = parseJson(entry.value)
      let db = j.getOrDefault("database").getStr("")
      let sc = j.getOrDefault("schema").getStr("")
      let name = j["name"].getStr()
      if (db == op.stDatabase or op.stDatabase.len == 0) and
         (sc == op.stSchema or op.stSchema.len == 0):
        resultRows.add(@[name])
    except JsonParsingError:
      let decoded = decodeTableKey(key)
      let pk = decoded.primaryKey
      if pk.startsWith(prefix):
        resultRows.add(@[pk[prefix.len .. ^1]])

  rowsResult(@["table_name"], resultRows)

# ---------------------------------------------------------------------------
# Space executors
# ---------------------------------------------------------------------------

proc nextSpaceId(store: RaftKVStoreExt): int =
  ## Allocate the next available space ID by scanning sys.spaces.
  let startKey = encodeTableKey(SYS_SPACES_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_SPACES_TABLE_ID + 1, "")
  let res = store.raftScan(startKey, endKey, 0, includeSystemKeys = true)
  var maxId = 0
  if res.isOk:
    for (key, entry) in res.value:
      try:
        let j = parseJson(entry.value)
        let sid = j["spaceId"].getInt()
        if sid > maxId: maxId = sid
      except JsonParsingError:
        discard
  maxId + 1

proc execCreateSpace(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  ## Execute CREATE SPACE: allocate spaceId, compute group placement,
  ## create Raft groups, write space record.
  # Check for duplicate by name
  let startKey = encodeTableKey(SYS_SPACES_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_SPACES_TABLE_ID + 1, "")
  let scanRes = store.raftScan(startKey, endKey, 0, includeSystemKeys = true)
  if scanRes.isOk:
    for (key, entry) in scanRes.value:
      try:
        let j = parseJson(entry.value)
        if j["name"].getStr() == op.cspName:
          return errorResult(&"space '{op.cspName}' already exists")
      except JsonParsingError:
        discard

  # Count nodes in the cluster
  let nodesStart = encodeTableKey(SYS_NODES_TABLE_ID, "")
  let nodesEnd = encodeTableKey(SYS_NODES_TABLE_ID + 1, "")
  let nodesRes = store.raftScan(nodesStart, nodesEnd, 0, includeSystemKeys = true)
  var nodeCount = 0
  var nodeIds: seq[int] = @[]
  if nodesRes.isOk:
    for (key, entry) in nodesRes.value:
      try:
        let j = parseJson(entry.value)
        nodeIds.add(j["nodeId"].getInt())
        inc nodeCount
      except JsonParsingError:
        discard

  if nodeCount == 0:
    return errorResult("no nodes in cluster")

  let replicas = if op.cspReplicas == 0: nodeCount else: op.cspReplicas
  if replicas > nodeCount:
    return errorResult(&"REPLICAS ({replicas}) exceeds node count ({nodeCount})")

  # Compute group count and placement
  # For R replicas on N nodes → N groups
  let groupCount = nodeCount
  nodeIds.sort()

  # Allocate space ID
  let spaceId = nextSpaceId(store)

  # Find max existing groupId to allocate new ones
  let rangesStart = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
  let rangesEnd = encodeTableKey(SYS_GROUPS_TABLE_ID + 1, "")
  let rangesRes = store.raftScan(rangesStart, rangesEnd, 0, includeSystemKeys = true)
  var maxGroupId: uint64 = 1
  if rangesRes.isOk:
    for (key, entry) in rangesRes.value:
      try:
        let j = parseJson(entry.value)
        let rid = uint64(j["groupId"].getInt())
        if rid > maxGroupId: maxGroupId = rid
      except JsonParsingError:
        discard

  var groupIds: seq[int] = @[]
  for g in 0 ..< groupCount:
    let groupId = int(maxGroupId) + 1 + g
    groupIds.add(groupId)

    # Compute group members using ring algorithm
    var members: seq[int] = @[]
    for j in 0 ..< replicas:
      members.add(nodeIds[(g + j) mod nodeCount])

    # Write group descriptor to sys.groups
    var replicasJson = newJArray()
    for m in members:
      replicasJson.add(%*{"nodeId": m, "type": "voter"})
    let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupId)
    let groupVal = $ %*{
      "groupId": groupId,
      "spaceId": spaceId,
      "replicas": replicasJson,
      "preferredLeader": members[0],
    }
    let putRes = store.raftPut(groupKey, groupVal)
    if not putRes.isOk:
      return errorResult(&"failed to create group {groupId}: {putRes.error.msg}")

    # Create actual Raft group in the coordinator
    let coord = store.coordinator
    let gid = GroupID(uint64(groupId))
    if not coord.hasGroup(gid):
      var desc = rangeTypes.newGroupDescriptor(gid)
      for m in members:
        discard desc.addReplica(rangeTypes.NodeID(uint32(m)), rangeTypes.rtVoter)
      var myReplicaId = rangeTypes.ReplicaID(0)
      for r in desc.replicas:
        if r.nodeId == coord.nodeId:
          myReplicaId = r.replicaId
          break
      if myReplicaId != rangeTypes.ReplicaID(0):
        discard coord.createAndStartGroup(desc, myReplicaId)
        store.registerGroup(gid)
        # Single-node: become leader immediately if meta group is leader
        let metaGroup = coord.getGroup(META_GROUP_ID)
        if metaGroup.isSome and metaGroup.get().isLeader():
          let newGroup = coord.getGroup(gid)
          if newGroup.isSome:
            newGroup.get().becomeLeader()

  # Write space record
  let spaceKey = encodeSpaceKey(spaceId)
  let spaceVal = $ %*{
    "spaceId": spaceId,
    "name": op.cspName,
    "replicas": op.cspReplicas,
    "groupCount": groupCount,
    "groupIds": groupIds,
    "createdAt": $now(),
  }
  let putRes = store.raftPut(spaceKey, spaceVal)
  if not putRes.isOk:
    return errorResult(&"failed to write space record: {putRes.error.msg}")

  # Reload space caches so newly created space is immediately routable
  store.loadSpaces()
  store.loadGroupMembers()

  okResult(&"CREATE SPACE ({groupCount} groups)")

proc execDropSpace(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  if op.dspName == "default":
    return errorResult("cannot drop the default space")

  # Find space by name
  let startKey = encodeTableKey(SYS_SPACES_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_SPACES_TABLE_ID + 1, "")
  let scanRes = store.raftScan(startKey, endKey, 0, includeSystemKeys = true)
  var foundKey = ""
  if scanRes.isOk:
    for (key, entry) in scanRes.value:
      try:
        let j = parseJson(entry.value)
        if j["name"].getStr() == op.dspName:
          foundKey = key
          break
      except JsonParsingError:
        discard

  if foundKey == "":
    return errorResult(&"space '{op.dspName}' does not exist")

  let delRes = store.raftDelete(foundKey)
  if not delRes.isOk:
    return errorResult(&"failed to drop space: {delRes.error.msg}")

  okResult("DROP SPACE")

proc execShowSpaces(op: PlanOp, store: RaftKVStoreExt): ExecResult =
  let startKey = encodeTableKey(SYS_SPACES_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_SPACES_TABLE_ID + 1, "")
  let res = store.raftScan(startKey, endKey, 0, includeSystemKeys = true)
  if not res.isOk:
    return errorResult(&"failed to scan spaces: {res.error.msg}")

  var resultRows: seq[seq[string]]
  for (key, entry) in res.value:
    try:
      let j = parseJson(entry.value)
      let name = j["name"].getStr()
      let replicas = j["replicas"].getInt()
      let replicasStr = if replicas == 0: "ALL" else: $replicas
      let groupCount = j["groupCount"].getInt()
      let groupIds = if j.hasKey("groupIds"):
                       var ids: seq[string]
                       for r in j["groupIds"]: ids.add($r.getInt())
                       ids.join(",")
                     else: ""
      resultRows.add(@[$j["spaceId"].getInt(), name, replicasStr,
                        $groupCount, groupIds])
    except JsonParsingError:
      discard

  rowsResult(@["space_id", "name", "replicas", "group_count", "group_ids"],
             resultRows)

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

proc execute*(plan: Plan, store: RaftKVStoreExt,
    database: string = "default"): ExecResult =
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
    of poShowDatabases:  execShowDatabases(op, store)
    of poShowSchemas:    execShowSchemas(op, store)
    of poShowTables:     execShowTables(op, store)
    of poShowSpaces:     execShowSpaces(op, store)
    of poCreateSpace:    execCreateSpace(op, store)
    of poDropSpace:      execDropSpace(op, store)
    of poUseDatabase:    execUseDatabase(op, store)
    of poUseSchema:      execUseSchema(op, store, database)
    of poBeginTxn:       okResult("BEGIN")
    of poCommitTxn:      okResult("COMMIT")
    of poRollbackTxn:    okResult("ROLLBACK")
    of poExplain:
      let text = formatPlan(op.exInnerPlan)
      var rows: seq[seq[string]]
      for line in text.split('\n'):
        rows.add(@[line])
      rowsResult(@["plan"], rows)

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
      lastResult = execute(plan, store, database)
      if lastResult.kind == erkError:
        return lastResult
    lastResult
  except PlanError as e:
    errorResult(e.msg)
  except CatchableError as e:
    errorResult(&"SQL error: {e.msg}")

