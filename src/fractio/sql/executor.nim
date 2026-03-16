# SQL Executor for Fractio
#
# Executes a Plan against a RaftKVStoreExt, returning results.
# Each PlanOp maps directly to KV operations on the raft store.
# Supports MVCC transactions when MvccTransactionStore is provided.

import std/[options, json, strutils, strformat, tables, times, algorithm]
import ./ast
import ./parser
import ./planner
import ../distributed/meta/system_tables
import ../protocol/raft_store
from ../protocol/mvcc_store import MvccTransactionStore, MvccResult,
    MvccVoidResult, MvccStoreError, mvccOk, mvccErr, mvccVOk, mvccVErr,
        mseStorageError,
    createSession, closeSession, beginTransaction, commitTransaction,
    rollbackTransaction, txnGet, txnPut, txnDelete, txnScan, directGet,
        directScan,
    isVersionKey, isIntentKeyMvcc, snapshotGet, snapshotScan, getCurrentTimestamp
import ../core/types as coreTypes
import ../distributed/raft/nuraft_coordinator
import ../distributed/raft/group_types as rangeTypes
import ../utils/logging

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
    sessionId*: uint64          ## MVCC session ID (0 if no MVCC store)
    hasActiveTransaction*: bool ## True if a transaction is in progress
    database*: string           ## Current database context
    schema*: string             ## Current schema context

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

proc newExecutorContext*(database: string = "default",
    schema: string = "public"): ExecutorContext =
  ## Create a new executor context with default settings
  ExecutorContext(
    sessionId: 0,
    hasActiveTransaction: false,
    database: database,
    schema: schema
  )

proc initSession*(ctx: ExecutorContext, mvccStore: MvccTransactionStore) =
  ## Initialize an MVCC session for this context
  if ctx.sessionId == 0:
    ctx.sessionId = mvccStore.createSession()

proc closeSession*(ctx: ExecutorContext, mvccStore: MvccTransactionStore) =
  ## Close the MVCC session
  if ctx.sessionId != 0:
    mvccStore.closeSession(ctx.sessionId)
    ctx.sessionId = 0
    ctx.hasActiveTransaction = false

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

proc execCreateDatabase(op: PlanOp, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore): ExecResult =
  ## Execute CREATE DATABASE with internal MVCC transaction for consistency.
  let key = encodeTableKey(SYS_DATABASES_TABLE_ID, op.cdbName)

  # Create internal session and transaction
  let sessionId = mvccStore.createSession()
  defer: mvccStore.closeSession(sessionId)

  let txnRes = mvccStore.beginTransaction(sessionId)
  if not txnRes.isOk:
    return errorResult(&"failed to start internal transaction: {txnRes.error.msg}")

  # Check for duplicate (within transaction snapshot)
  let existing = mvccStore.txnGet(sessionId, key)
  if existing.isOk and existing.value.isSome:
    discard mvccStore.rollbackTransaction(sessionId)
    if op.cdbIfNotExists:
      return okResult("database already exists (IF NOT EXISTS)")
    return errorResult(&"database '{op.cdbName}' already exists")

  # Write database record
  let putRes = mvccStore.txnPut(sessionId, key, op.cdbValue)
  if not putRes.isOk:
    discard mvccStore.rollbackTransaction(sessionId)
    return errorResult(&"failed to create database: {putRes.error.msg}")

  # Seed a default "public" schema for every new database
  let pubKey = encodeTableKey(SYS_SCHEMAS_TABLE_ID, op.cdbName & ".public")
  let pubVal = $ %* {"name": "public", "database": op.cdbName}
  let pubPutRes = mvccStore.txnPut(sessionId, pubKey, pubVal)
  if not pubPutRes.isOk:
    discard mvccStore.rollbackTransaction(sessionId)
    return errorResult(&"failed to create public schema: {pubPutRes.error.msg}")

  # Commit the transaction
  let commitRes = mvccStore.commitTransaction(sessionId)
  if not commitRes.isOk:
    return errorResult(&"failed to commit: {commitRes.error.msg}")

  okResult(&"CREATE DATABASE")

proc execDropDatabase(op: PlanOp, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore): ExecResult =
  ## Execute DROP DATABASE with internal MVCC transaction for consistency.
  let key = encodeTableKey(SYS_DATABASES_TABLE_ID, op.ddbName)

  # Create internal session and transaction
  let sessionId = mvccStore.createSession()
  defer: mvccStore.closeSession(sessionId)

  let txnRes = mvccStore.beginTransaction(sessionId)
  if not txnRes.isOk:
    return errorResult(&"failed to start internal transaction: {txnRes.error.msg}")

  # Check if database exists
  let existing = mvccStore.txnGet(sessionId, key)
  if not existing.isOk or existing.value.isNone:
    discard mvccStore.rollbackTransaction(sessionId)
    if op.ddbIfExists:
      return okResult("database does not exist (IF EXISTS)")
    return errorResult(&"database '{op.ddbName}' does not exist")

  # Always cascade: delete all schemas, tables, and data rows for this database
  # Delete all schemas for this database
  let schemaPrefix = op.ddbName & "."
  let schemaStart = encodeTableKey(SYS_SCHEMAS_TABLE_ID, schemaPrefix)
  let schemaEnd = encodeTableKey(SYS_SCHEMAS_TABLE_ID, schemaPrefix & "\xFF")
  let schemaScan = mvccStore.txnScan(sessionId, schemaStart, schemaEnd, 0)
  if schemaScan.isOk:
    for (sk, sv) in schemaScan.value:
      let delRes = mvccStore.txnDelete(sessionId, sk)
      if not delRes.isOk:
        discard mvccStore.rollbackTransaction(sessionId)
        return errorResult(&"failed to delete schema: {delRes.error.msg}")

  # Find and delete all tables and their data rows
  let tableStart = encodeTableKey(SYS_TABLES_TABLE_ID, "")
  let tableEnd = encodeTableKey(SYS_TABLES_TABLE_ID + 1, "")
  let tableScan = mvccStore.txnScan(sessionId, tableStart, tableEnd, 0)
  if tableScan.isOk:
    for (tk, tv) in tableScan.value:
      try:
        let tj = parseJson(tv)
        if tj.getOrDefault("database").getStr("") == op.ddbName:
          let tableId = uint32(tj["tableId"].getInt())
          # Delete all data rows for this table
          let dataStart = encodeDataRowKey(tableId, "")
          let dataEnd = encodeDataRowKey(tableId + 1, "")
          let dataScan = mvccStore.txnScan(sessionId, dataStart, dataEnd, 0)
          if dataScan.isOk:
            for (dk, dv) in dataScan.value:
              let delRes = mvccStore.txnDelete(sessionId, dk)
              if not delRes.isOk:
                discard mvccStore.rollbackTransaction(sessionId)
                return errorResult(&"failed to delete data row: {delRes.error.msg}")
          # Delete the table record
          let delRes = mvccStore.txnDelete(sessionId, tk)
          if not delRes.isOk:
            discard mvccStore.rollbackTransaction(sessionId)
            return errorResult(&"failed to delete table: {delRes.error.msg}")
      except JsonParsingError:
        discard

  # Delete the database record
  let delRes = mvccStore.txnDelete(sessionId, key)
  if not delRes.isOk:
    discard mvccStore.rollbackTransaction(sessionId)
    return errorResult(&"failed to drop database: {delRes.error.msg}")

  # Commit the transaction
  let commitRes = mvccStore.commitTransaction(sessionId)
  if not commitRes.isOk:
    return errorResult(&"failed to commit: {commitRes.error.msg}")

  okResult("DROP DATABASE")

proc execCreateSchema(op: PlanOp, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore): ExecResult =
  ## Execute CREATE SCHEMA with internal MVCC transaction for consistency.
  let key = encodeTableKey(SYS_SCHEMAS_TABLE_ID,
      op.csDatabase & "." & op.csName)

  # Create internal session and transaction
  let sessionId = mvccStore.createSession()
  defer: mvccStore.closeSession(sessionId)

  let txnRes = mvccStore.beginTransaction(sessionId)
  if not txnRes.isOk:
    return errorResult(&"failed to start internal transaction: {txnRes.error.msg}")

  let existing = mvccStore.txnGet(sessionId, key)
  if existing.isOk and existing.value.isSome:
    discard mvccStore.rollbackTransaction(sessionId)
    if op.csIfNotExists:
      return okResult("schema already exists (IF NOT EXISTS)")
    return errorResult(&"schema '{op.csName}' already exists")

  let putRes = mvccStore.txnPut(sessionId, key, op.csValue)
  if not putRes.isOk:
    discard mvccStore.rollbackTransaction(sessionId)
    return errorResult(&"failed to create schema: {putRes.error.msg}")

  let commitRes = mvccStore.commitTransaction(sessionId)
  if not commitRes.isOk:
    return errorResult(&"failed to commit: {commitRes.error.msg}")

  okResult("CREATE SCHEMA")

proc execDropSchema(op: PlanOp, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore): ExecResult =
  ## Execute DROP SCHEMA with internal MVCC transaction for consistency.
  let key = encodeTableKey(SYS_SCHEMAS_TABLE_ID,
      op.dsDatabase & "." & op.dsName)

  # Create internal session and transaction
  let sessionId = mvccStore.createSession()
  defer: mvccStore.closeSession(sessionId)

  let txnRes = mvccStore.beginTransaction(sessionId)
  if not txnRes.isOk:
    return errorResult(&"failed to start internal transaction: {txnRes.error.msg}")

  let existing = mvccStore.txnGet(sessionId, key)
  if not existing.isOk or existing.value.isNone:
    discard mvccStore.rollbackTransaction(sessionId)
    if op.dsIfExists:
      return okResult("schema does not exist (IF EXISTS)")
    return errorResult(&"schema '{op.dsName}' does not exist")

  let delRes = mvccStore.txnDelete(sessionId, key)
  if not delRes.isOk:
    discard mvccStore.rollbackTransaction(sessionId)
    return errorResult(&"failed to drop schema: {delRes.error.msg}")

  let commitRes = mvccStore.commitTransaction(sessionId)
  if not commitRes.isOk:
    return errorResult(&"failed to commit: {commitRes.error.msg}")

  okResult("DROP SCHEMA")

proc execCreateTable(op: PlanOp, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore): ExecResult =
  ## Execute CREATE TABLE with internal MVCC transaction for consistency.
  let key = encodeTableKey(SYS_TABLES_TABLE_ID,
      op.ctDatabase & "." & op.ctSchema & "." & op.ctName)

  # Create internal session and transaction
  let sessionId = mvccStore.createSession()
  defer: mvccStore.closeSession(sessionId)

  let txnRes = mvccStore.beginTransaction(sessionId)
  if not txnRes.isOk:
    return errorResult(&"failed to start internal transaction: {txnRes.error.msg}")

  let existing = mvccStore.txnGet(sessionId, key)
  if existing.isOk and existing.value.isSome:
    discard mvccStore.rollbackTransaction(sessionId)
    if op.ctIfNotExists:
      return okResult("table already exists (IF NOT EXISTS)")
    return errorResult(&"table '{op.ctName}' already exists")

  # Resolve space name to spaceId (within transaction snapshot)
  var tableValue = op.ctValue
  if op.ctSpaceName.isSome:
    let spaceName = op.ctSpaceName.get()
    let sStart = encodeTableKey(SYS_SPACES_TABLE_ID, "")
    let sEnd = encodeTableKey(SYS_SPACES_TABLE_ID + 1, "")
    let sScan = mvccStore.txnScan(sessionId, sStart, sEnd, 0)
    var spaceId = -1
    if sScan.isOk:
      for (sk, sv) in sScan.value:
        try:
          let sj = parseJson(sv)
          if sj["name"].getStr() == spaceName:
            spaceId = sj["spaceId"].getInt()
            break
        except JsonParsingError:
          discard
    if spaceId < 0:
      discard mvccStore.rollbackTransaction(sessionId)
      return errorResult(&"space '{spaceName}' does not exist")
    # Inject spaceId into the table descriptor JSON
    try:
      var j = parseJson(tableValue)
      j["spaceId"] = %spaceId
      tableValue = $j
    except JsonParsingError:
      discard

  let putRes = mvccStore.txnPut(sessionId, key, tableValue)
  if not putRes.isOk:
    discard mvccStore.rollbackTransaction(sessionId)
    return errorResult(&"failed to create table: {putRes.error.msg}")

  let commitRes = mvccStore.commitTransaction(sessionId)
  if not commitRes.isOk:
    return errorResult(&"failed to commit: {commitRes.error.msg}")

  # Reload table-space caches so the new table is immediately routable
  if op.ctSpaceName.isSome:
    store.loadTableSpaces()

  okResult("CREATE TABLE")

proc execDropTable(op: PlanOp, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore): ExecResult =
  ## Execute DROP TABLE with internal MVCC transaction for consistency.
  let key = encodeTableKey(SYS_TABLES_TABLE_ID,
      op.dtDatabase & "." & op.dtSchema & "." & op.dtName)

  # Create internal session and transaction
  let sessionId = mvccStore.createSession()
  defer: mvccStore.closeSession(sessionId)

  let txnRes = mvccStore.beginTransaction(sessionId)
  if not txnRes.isOk:
    return errorResult(&"failed to start internal transaction: {txnRes.error.msg}")

  let existing = mvccStore.txnGet(sessionId, key)
  if not existing.isOk or existing.value.isNone:
    discard mvccStore.rollbackTransaction(sessionId)
    if op.dtIfExists:
      return okResult("table does not exist (IF EXISTS)")
    return errorResult(&"table '{op.dtName}' does not exist")

  # TODO: also delete all data rows for the table
  let delRes = mvccStore.txnDelete(sessionId, key)
  if not delRes.isOk:
    discard mvccStore.rollbackTransaction(sessionId)
    return errorResult(&"failed to drop table: {delRes.error.msg}")

  let commitRes = mvccStore.commitTransaction(sessionId)
  if not commitRes.isOk:
    return errorResult(&"failed to commit: {commitRes.error.msg}")

  okResult("DROP TABLE")

# ---------------------------------------------------------------------------
# Space executors
# ---------------------------------------------------------------------------

proc execCreateSpace(op: PlanOp, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore): ExecResult =
  ## Execute CREATE SPACE with internal MVCC transaction for consistency.
  ## Allocates spaceId, computes group placement, creates Raft groups, writes space record.

  # Create internal session and transaction
  let sessionId = mvccStore.createSession()
  defer: mvccStore.closeSession(sessionId)

  let txnRes = mvccStore.beginTransaction(sessionId)
  if not txnRes.isOk:
    return errorResult(&"failed to start internal transaction: {txnRes.error.msg}")

  # Check for duplicate by name (within transaction snapshot)
  let startKey = encodeTableKey(SYS_SPACES_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_SPACES_TABLE_ID + 1, "")
  let scanRes = mvccStore.txnScan(sessionId, startKey, endKey, 0)
  if scanRes.isOk:
    for (key, value) in scanRes.value:
      try:
        let j = parseJson(value)
        if j["name"].getStr() == op.cspName:
          discard mvccStore.rollbackTransaction(sessionId)
          return errorResult(&"space '{op.cspName}' already exists")
      except JsonParsingError:
        discard

  # Count nodes in the cluster (within transaction snapshot)
  let nodesStart = encodeTableKey(SYS_NODES_TABLE_ID, "")
  let nodesEnd = encodeTableKey(SYS_NODES_TABLE_ID + 1, "")
  let nodesRes = mvccStore.txnScan(sessionId, nodesStart, nodesEnd, 0)
  var nodeCount = 0
  var nodeIds: seq[int] = @[]
  if nodesRes.isOk:
    for (key, value) in nodesRes.value:
      try:
        let j = parseJson(value)
        nodeIds.add(j["nodeId"].getInt())
        inc nodeCount
      except JsonParsingError:
        discard

  if nodeCount == 0:
    discard mvccStore.rollbackTransaction(sessionId)
    return errorResult("no nodes in cluster")

  let replicas = if op.cspReplicas == 0: nodeCount else: op.cspReplicas
  if replicas > nodeCount:
    discard mvccStore.rollbackTransaction(sessionId)
    return errorResult(&"REPLICAS ({replicas}) exceeds node count ({nodeCount})")

  # Compute group count and placement
  # For R replicas on N nodes → N groups
  let groupCount = nodeCount
  nodeIds.sort()

  # Allocate space ID (scan for max within transaction)
  var spaceId = 1
  if scanRes.isOk:
    for (key, value) in scanRes.value:
      try:
        let j = parseJson(value)
        let sid = j["spaceId"].getInt()
        if sid >= spaceId: spaceId = sid + 1
      except JsonParsingError:
        discard

  # Find max existing groupId to allocate new ones (within transaction)
  let rangesStart = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
  let rangesEnd = encodeTableKey(SYS_GROUPS_TABLE_ID + 1, "")
  let rangesRes = mvccStore.txnScan(sessionId, rangesStart, rangesEnd, 0)
  var maxGroupId: uint64 = 1
  if rangesRes.isOk:
    for (key, value) in rangesRes.value:
      try:
        let j = parseJson(value)
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

    # Write group descriptor to sys.groups (within transaction)
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
    let putRes = mvccStore.txnPut(sessionId, groupKey, groupVal)
    if not putRes.isOk:
      discard mvccStore.rollbackTransaction(sessionId)
      return errorResult(&"failed to create group {groupId}: {putRes.error.msg}")

    # Create actual Raft group in the coordinator (outside transaction - infrastructure)
    let coord = store.coordinator
    let gid = GroupID(uint64(groupId))
    if not coord.hasGroup(gid):
      var nuraftMembers: seq[tuple[nodeId: uint32, host: string,
          basePort: int]] = @[]
      for m in members:
        let peerInfo = coord.peerInfo.getOrDefault(uint32(m),
            (host: coord.host, basePort: coord.basePort))
        nuraftMembers.add((nodeId: uint32(m), host: peerInfo.host,
            basePort: peerInfo.basePort))
      let ok = coord.createAndStartGroup(gid, nuraftMembers)
      if ok:
        store.registerGroup(gid)
      else:
        # This is expected when this node is not a member of the group.
        # Peer nodes that ARE members will create the group via onGroupMetadataApplied callback.
        try:
          {.cast(gcsafe).}:
            debug("Skipped creating group (not a member or already exists)",
                 {"groupId": $groupId, "nodeId": $coord.nodeId.uint32}.toTable)
        except:
          discard

  # Write space record (within transaction)
  let spaceKey = encodeSpaceKey(spaceId)
  let spaceVal = $ %*{
    "spaceId": spaceId,
    "name": op.cspName,
    "replicas": op.cspReplicas,
    "groupCount": groupCount,
    "groupIds": groupIds,
    "createdAt": $now(),
  }
  let putRes = mvccStore.txnPut(sessionId, spaceKey, spaceVal)
  if not putRes.isOk:
    discard mvccStore.rollbackTransaction(sessionId)
    return errorResult(&"failed to write space record: {putRes.error.msg}")

  # Commit the transaction
  let commitRes = mvccStore.commitTransaction(sessionId)
  if not commitRes.isOk:
    return errorResult(&"failed to commit: {commitRes.error.msg}")

  # Reload space caches so newly created space is immediately routable
  store.loadSpaces()
  store.loadGroupMembers()

  okResult(&"CREATE SPACE ({groupCount} groups)")

proc execDropSpace(op: PlanOp, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore): ExecResult =
  ## Execute DROP SPACE with internal MVCC transaction for consistency.
  if op.dspName == "default":
    return errorResult("cannot drop the default space")

  # Create internal session and transaction
  let sessionId = mvccStore.createSession()
  defer: mvccStore.closeSession(sessionId)

  let txnRes = mvccStore.beginTransaction(sessionId)
  if not txnRes.isOk:
    return errorResult(&"failed to start internal transaction: {txnRes.error.msg}")

  # Find space by name and extract spaceId and groupIds (within transaction)
  let startKey = encodeTableKey(SYS_SPACES_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_SPACES_TABLE_ID + 1, "")
  let scanRes = mvccStore.txnScan(sessionId, startKey, endKey, 0)
  var foundKey = ""
  var spaceId = -1
  var groupIds: seq[int] = @[]
  if scanRes.isOk:
    for (key, value) in scanRes.value:
      try:
        let j = parseJson(value)
        if j["name"].getStr() == op.dspName:
          foundKey = key
          spaceId = j["spaceId"].getInt()
          if j.hasKey("groupIds"):
            for g in j["groupIds"]:
              groupIds.add(g.getInt())
          break
      except JsonParsingError:
        discard

  if foundKey == "":
    discard mvccStore.rollbackTransaction(sessionId)
    return errorResult(&"space '{op.dspName}' does not exist")

  # Check if any tables are using this space (within transaction)
  let tableStart = encodeTableKey(SYS_TABLES_TABLE_ID, "")
  let tableEnd = encodeTableKey(SYS_TABLES_TABLE_ID + 1, "")
  let tableScan = mvccStore.txnScan(sessionId, tableStart, tableEnd, 0)
  if tableScan.isOk:
    for (tk, tv) in tableScan.value:
      try:
        let tj = parseJson(tv)
        if tj.getOrDefault("spaceId").getInt(-1) == spaceId:
          let tableName = tj.getOrDefault("name").getStr("unknown")
          discard mvccStore.rollbackTransaction(sessionId)
          return errorResult(&"cannot drop space '{op.dspName}': table '{tableName}' is using it")
      except JsonParsingError:
        discard

  # Delete all group records for this space (within transaction)
  for groupId in groupIds:
    let groupKey = encodeTableKey(SYS_GROUPS_TABLE_ID, $groupId)
    let delGroupRes = mvccStore.txnDelete(sessionId, groupKey)
    if not delGroupRes.isOk:
      # Log but continue - group might not exist
      discard

  # Delete the space record (within transaction)
  let delRes = mvccStore.txnDelete(sessionId, foundKey)
  if not delRes.isOk:
    discard mvccStore.rollbackTransaction(sessionId)
    return errorResult(&"failed to drop space: {delRes.error.msg}")

  # Commit the transaction
  let commitRes = mvccStore.commitTransaction(sessionId)
  if not commitRes.isOk:
    return errorResult(&"failed to commit: {commitRes.error.msg}")

  # Reload caches
  store.loadSpaces()
  store.loadGroupMembers()

  okResult("DROP SPACE")

# ---------------------------------------------------------------------------
# Transaction-aware KV operation helpers
# ---------------------------------------------------------------------------

proc txnGet(store: RaftKVStoreExt, mvccStore: MvccTransactionStore,
    ctx: ExecutorContext, key: string): MvccResult[Option[string]] =
  ## Get a value, using transactional read if in a transaction,
  ## or MVCC direct read otherwise.
  if ctx.hasActiveTransaction and ctx.sessionId != 0 and mvccStore != nil:
    mvccStore.txnGet(ctx.sessionId, key)
  elif mvccStore != nil:
    # Use MVCC direct get to properly decode MVCC-encoded values
    mvccStore.directGet(key)
  else:
    # Fallback: direct raft read (only for non-MVCC data)
    let res = store.raftGet(key)
    if res.isOk:
      if res.value.isSome:
        mvccOk(some(res.value.get().value))
      else:
        mvccOk(none(string))
    else:
      mvccErr[Option[string]](MvccStoreError(
        kind: mseStorageError, msg: res.error.msg))

proc execTxnScan(store: RaftKVStoreExt, mvccStore: MvccTransactionStore,
    ctx: ExecutorContext, startKey, endKey: string,
    limit: uint32 = 0): MvccResult[seq[tuple[key: string, value: string]]] =
  ## Scan keys with MVCC awareness.
  ## When in a transaction, use transactional scan.
  ## Otherwise, combine MVCC direct scan (for MVCC-encoded data) with
  ## regular scan (for non-MVCC data), merging results with MVCC priority.
  if ctx.hasActiveTransaction and ctx.sessionId != 0 and mvccStore != nil:
    return mvccStore.txnScan(ctx.sessionId, startKey, endKey, limit)

  # Build a table of key -> value, with MVCC versions taking priority
  var keyValues: tables.Table[string, string] = initTable[string, string]()

  # First, do a regular scan for non-MVCC keys
  let regularRes = store.raftScan(startKey, endKey, limit,
      includeSystemKeys = true)
  if regularRes.isOk:
    for (k, entry) in regularRes.value:
      # Skip MVCC-encoded keys (version keys and intent keys)
      if not isVersionKey(k) and not isIntentKeyMvcc(k):
        keyValues[k] = entry.value

  # Then, do MVCC direct scan for MVCC-encoded data (if MVCC store available)
  if mvccStore != nil:
    let mvccRes = mvccStore.directScan(startKey, endKey, limit)
    if mvccRes.isOk:
      for (k, v) in mvccRes.value:
        # MVCC versions take priority over regular keys
        keyValues[k] = v

  # Convert to result sequence
  var results: seq[tuple[key: string, value: string]] = @[]
  for k, v in keyValues.pairs:
    results.add((key: k, value: v))

  # Sort by key for consistent ordering
  results.sort(proc(a, b: tuple[key: string, value: string]): int = cmp(a.key, b.key))

  # Apply limit if specified
  if limit > 0 and results.len > int(limit):
    results = results[0 ..< int(limit)]

  mvccOk(results)

# ---------------------------------------------------------------------------
# MVCC-aware show operations
# ---------------------------------------------------------------------------

proc execShowDatabasesTxn(op: PlanOp, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore, ctx: ExecutorContext): ExecResult =
  let startKey = encodeTableKey(SYS_DATABASES_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_DATABASES_TABLE_ID + 1, "")
  let res = execTxnScan(store, mvccStore, ctx, startKey, endKey, 0)
  if not res.isOk:
    return errorResult(&"failed to scan databases: {res.error.msg}")

  var resultRows: seq[seq[string]]
  for (key, value) in res.value:
    try:
      let j = parseJson(value)
      resultRows.add(@[j["name"].getStr()])
    except JsonParsingError:
      # Fall back to extracting name from the key
      let decoded = decodeTableKey(key)
      resultRows.add(@[decoded.primaryKey])

  rowsResult(@["database_name"], resultRows)

proc execShowSchemasTxn(op: PlanOp, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore, ctx: ExecutorContext): ExecResult =
  let prefix = op.ssDatabase & "."
  let startKey = encodeTableKey(SYS_SCHEMAS_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_SCHEMAS_TABLE_ID + 1, "")
  let res = execTxnScan(store, mvccStore, ctx, startKey, endKey, 0)
  if not res.isOk:
    return errorResult(&"failed to scan schemas: {res.error.msg}")

  var resultRows: seq[seq[string]]
  for (key, value) in res.value:
    try:
      let j = parseJson(value)
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

proc execShowTablesTxn(op: PlanOp, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore, ctx: ExecutorContext): ExecResult =
  let prefix = op.stDatabase & "." & op.stSchema & "."
  let startKey = encodeTableKey(SYS_TABLES_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_TABLES_TABLE_ID + 1, "")
  let res = execTxnScan(store, mvccStore, ctx, startKey, endKey, 0)
  if not res.isOk:
    return errorResult(&"failed to scan tables: {res.error.msg}")

  var resultRows: seq[seq[string]]
  for (key, value) in res.value:
    try:
      let j = parseJson(value)
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

proc execShowSpacesTxn(op: PlanOp, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore, ctx: ExecutorContext): ExecResult =
  ## Transaction-aware SHOW SPACES that can see MVCC-encoded space records.
  let startKey = encodeTableKey(SYS_SPACES_TABLE_ID, "")
  let endKey = encodeTableKey(SYS_SPACES_TABLE_ID + 1, "")
  let res = execTxnScan(store, mvccStore, ctx, startKey, endKey, 0)
  if not res.isOk:
    return errorResult(&"failed to scan spaces: {res.error.msg}")

  var resultRows: seq[seq[string]]
  for (key, value) in res.value:
    try:
      let j = parseJson(value)
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
# Main entry point - unified with implicit transactions for DML
# ---------------------------------------------------------------------------

# Forward declaration
proc executeWithTxn*(plan: Plan, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore, ctx: ExecutorContext): ExecResult

proc execute*(plan: Plan, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore = nil,
    database: string = "default"): ExecResult =
  ## Execute a Plan against a RaftKVStoreExt, returning an ExecResult.
  ## Processes ops sequentially; returns the result of the last op
  ## (or the first error).
  ##
  ## All operations require mvccStore for consistency:
  ## - DDL operations use internal auto-commit transactions
  ## - DML operations use implicit transactions if not in an explicit one
  ##
  ## This is the simplified unified entry point.
  if mvccStore == nil:
    return errorResult("MVCC store is required for all operations")

  let ctx = newExecutorContext(database)
  ctx.initSession(mvccStore)
  defer: ctx.closeSession(mvccStore)

  executeWithTxn(plan, store, mvccStore, ctx)

proc executeSQL*(sql: string, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore,
    database: string = "default", schema: string = "public"): ExecResult =
  ## Parse and execute SQL with MVCC transaction support.
  ## This is the main entry point for SQL execution.
  if mvccStore == nil:
    return errorResult("MVCC store is required for all operations")

  try:
    let stmts = parseAll(sql)
    if stmts.len == 0:
      return errorResult("No SQL statements to execute")

    let plan = planStatement(stmts[0], store, mvccStore, database, schema)
    if plan.ops.len == 0:
      return okResult("empty plan")

    let ctx = newExecutorContext(database, schema)
    ctx.initSession(mvccStore)
    defer: ctx.closeSession(mvccStore)

    executeWithTxn(plan, store, mvccStore, ctx)
  except PlanError as e:
    errorResult(e.msg)
  except CatchableError as e:
    errorResult(&"SQL error: {e.msg}")

# ---------------------------------------------------------------------------
# Transaction-aware execute
# ---------------------------------------------------------------------------

proc executeWithTxn*(plan: Plan, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore, ctx: ExecutorContext): ExecResult =
  ## Execute a Plan with MVCC transaction support.
  ##
  ## All DML operations use MVCC transactions:
  ## - If in an explicit transaction (BEGIN), use that transaction
  ## - If not in a transaction, create an implicit auto-commit transaction
  ##
  ## DDL operations are FORBIDDEN inside explicit transactions.
  ##
  ## The ctx holds the session state and transaction status.

  proc needsImplicitTxn(): bool =
    ## Check if we need to create an implicit transaction for this operation
    not ctx.hasActiveTransaction

  proc beginImplicitTxn(): bool =
    ## Begin an implicit transaction. Returns true on success.
    let res = mvccStore.beginTransaction(ctx.sessionId)
    if res.isOk:
      ctx.hasActiveTransaction = true
      true
    else:
      false

  proc commitImplicitTxn(): bool =
    ## Commit an implicit transaction. Returns true on success.
    let res = mvccStore.commitTransaction(ctx.sessionId)
    if res.isOk:
      ctx.hasActiveTransaction = false
      true
    else:
      false

  proc rollbackImplicitTxn() =
    ## Rollback an implicit transaction.
    discard mvccStore.rollbackTransaction(ctx.sessionId)
    ctx.hasActiveTransaction = false

  var lastResult = okResult("empty plan")

  for op in plan.ops:
    lastResult = case op.kind

    # DDL operations: FORBIDDEN inside transactions, auto-commit outside
    of poCreateDatabase:
      if ctx.hasActiveTransaction:
        errorResult("CREATE DATABASE is not allowed inside a transaction")
      else:
        execCreateDatabase(op, store, mvccStore)

    of poDropDatabase:
      if ctx.hasActiveTransaction:
        errorResult("DROP DATABASE is not allowed inside a transaction")
      else:
        execDropDatabase(op, store, mvccStore)

    of poCreateSchema:
      if ctx.hasActiveTransaction:
        errorResult("CREATE SCHEMA is not allowed inside a transaction")
      else:
        execCreateSchema(op, store, mvccStore)

    of poDropSchema:
      if ctx.hasActiveTransaction:
        errorResult("DROP SCHEMA is not allowed inside a transaction")
      else:
        execDropSchema(op, store, mvccStore)

    of poCreateTable:
      if ctx.hasActiveTransaction:
        errorResult("CREATE TABLE is not allowed inside a transaction")
      else:
        execCreateTable(op, store, mvccStore)

    of poDropTable:
      if ctx.hasActiveTransaction:
        errorResult("DROP TABLE is not allowed inside a transaction")
      else:
        execDropTable(op, store, mvccStore)

    of poCreateSpace:
      if ctx.hasActiveTransaction:
        errorResult("CREATE SPACE is not allowed inside a transaction")
      else:
        execCreateSpace(op, store, mvccStore)

    of poDropSpace:
      if ctx.hasActiveTransaction:
        errorResult("DROP SPACE is not allowed inside a transaction")
      else:
        execDropSpace(op, store, mvccStore)

    # DML operations: always use MVCC with implicit transaction if needed
    of poInsert:
      let implicitTxn = needsImplicitTxn()
      if implicitTxn and not beginImplicitTxn():
        return errorResult("failed to begin implicit transaction")

      var count = 0
      var error: string = ""
      for rowJson in op.insRows:
        let row = parseJson(rowJson)
        let pkVal = getPkValue(row, op.insPkColumn)
        if pkVal.len == 0:
          error = "INSERT requires a primary key value"
          break
        let key = encodeDataRowKey(op.insTableId, pkVal)
        let res = mvccStore.txnPut(ctx.sessionId, key, rowJson)
        if not res.isOk:
          error = &"failed to insert row: {res.error.msg}"
          break
        inc count

      if error.len > 0:
        if implicitTxn: rollbackImplicitTxn()
        return errorResult(error)

      if implicitTxn and not commitImplicitTxn():
        return errorResult("failed to commit implicit transaction")

      modifiedResult(count, &"INSERT {count}")

    of poPointGet:
      # Use lightweight snapshot read for single SELECT statements
      # No transaction needed - just need a read timestamp
      let key = encodeDataRowKey(op.pgTableId, op.pgKey)

      if ctx.hasActiveTransaction:
        # In explicit transaction - use transaction's read timestamp
        let res = mvccStore.txnGet(ctx.sessionId, key)
        if not res.isOk:
          return errorResult(&"failed to read: {res.error.msg}")
        if res.value.isNone:
          return rowsResult(op.pgColumns, @[])
        let row = parseJson(res.value.get())
        let vals = extractColumns(row, op.pgColumns)
        rowsResult(op.pgColumns, @[vals])
      else:
        # Lightweight snapshot read - no transaction overhead
        let readTs = mvccStore.getCurrentTimestamp()
        let res = mvccStore.snapshotGet(key, readTs)
        if not res.isOk:
          return errorResult(&"failed to read: {res.error.msg}")
        if res.value.isNone:
          return rowsResult(op.pgColumns, @[])
        let row = parseJson(res.value.get())
        let vals = extractColumns(row, op.pgColumns)
        rowsResult(op.pgColumns, @[vals])

    of poScan:
      # Use lightweight snapshot scan for single SELECT statements
      # No transaction needed - just need a read timestamp
      if ctx.hasActiveTransaction:
        # In explicit transaction - use transaction's read timestamp
        let res = mvccStore.txnScan(ctx.sessionId, op.scStartKey, op.scEndKey, op.scLimit)
        if not res.isOk:
          return errorResult(&"failed to scan: {res.error.msg}")

        var resultRows: seq[seq[string]] = @[]
        var count = 0
        for (key, value) in res.value:
          try:
            let row = parseJson(value)
            if matchesFilter(op.scFilter, row):
              resultRows.add(extractColumns(row, op.scColumns))
              inc count
              if op.scLimit > 0 and count >= int(op.scLimit):
                break
          except JsonParsingError:
            discard # skip malformed rows

        rowsResult(op.scColumns, resultRows)
      else:
        # Lightweight snapshot scan - no transaction overhead
        let readTs = mvccStore.getCurrentTimestamp()
        let res = mvccStore.snapshotScan(op.scStartKey, op.scEndKey, readTs, op.scLimit)
        if not res.isOk:
          return errorResult(&"failed to scan: {res.error.msg}")

        var resultRows: seq[seq[string]] = @[]
        var count = 0
        for (key, value) in res.value:
          try:
            let row = parseJson(value)
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
      let implicitTxn = needsImplicitTxn()
      if implicitTxn and not beginImplicitTxn():
        return errorResult("failed to begin implicit transaction")

      let startKey = encodeDataRowKey(op.upTableId, "")
      let endKey = encodeDataRowKey(op.upTableId + 1, "")
      let res = mvccStore.txnScan(ctx.sessionId, startKey, endKey, 0)

      if not res.isOk:
        if implicitTxn: rollbackImplicitTxn()
        return errorResult(&"failed to scan for update: {res.error.msg}")

      var count = 0
      var error: string = ""
      for (key, value) in res.value:
        try:
          let row = parseJson(value)
          if matchesFilter(op.upFilter, row):
            var updated = row.copy()
            for (col, valExpr) in op.upSets:
              updated[col] = evalExpr(valExpr, row)
            let putRes = mvccStore.txnPut(ctx.sessionId, key, $updated)
            if not putRes.isOk:
              error = &"failed to update row: {putRes.error.msg}"
              break
            inc count
        except JsonParsingError:
          discard

      if error.len > 0:
        if implicitTxn: rollbackImplicitTxn()
        return errorResult(error)

      if implicitTxn and not commitImplicitTxn():
        return errorResult("failed to commit implicit transaction")

      modifiedResult(count, &"UPDATE {count}")

    of poDelete:
      # MVCC-aware DELETE
      let implicitTxn = needsImplicitTxn()
      if implicitTxn and not beginImplicitTxn():
        return errorResult("failed to begin implicit transaction")

      let startKey = encodeDataRowKey(op.delTableId, "")
      let endKey = encodeDataRowKey(op.delTableId + 1, "")
      let res = mvccStore.txnScan(ctx.sessionId, startKey, endKey, 0)

      if not res.isOk:
        if implicitTxn: rollbackImplicitTxn()
        return errorResult(&"failed to scan for delete: {res.error.msg}")

      var count = 0
      var error: string = ""
      for (key, value) in res.value:
        try:
          let row = parseJson(value)
          if matchesFilter(op.delFilter, row):
            let delRes = mvccStore.txnDelete(ctx.sessionId, key)
            if not delRes.isOk:
              error = &"failed to delete row: {delRes.error.msg}"
              break
            inc count
        except JsonParsingError:
          discard

      if error.len > 0:
        if implicitTxn: rollbackImplicitTxn()
        return errorResult(error)

      if implicitTxn and not commitImplicitTxn():
        return errorResult("failed to commit implicit transaction")

      modifiedResult(count, &"DELETE {count}")

    of poShowDatabases: execShowDatabasesTxn(op, store, mvccStore, ctx)
    of poShowSchemas: execShowSchemasTxn(op, store, mvccStore, ctx)
    of poShowTables: execShowTablesTxn(op, store, mvccStore, ctx)
    of poShowSpaces: execShowSpacesTxn(op, store, mvccStore, ctx)

    of poUseDatabase:
      let key = encodeTableKey(SYS_DATABASES_TABLE_ID, op.udName)
      let existing = txnGet(store, mvccStore, ctx, key)
      if not existing.isOk or existing.value.isNone:
        errorResult(&"database '{op.udName}' does not exist")
      else:
        ExecResult(kind: erkUseDatabase, newDatabase: op.udName)

    of poUseSchema:
      let key = encodeTableKey(SYS_SCHEMAS_TABLE_ID, ctx.database & "." & op.usName)
      let existing = txnGet(store, mvccStore, ctx, key)
      if not existing.isOk or existing.value.isNone:
        errorResult(&"schema '{op.usName}' does not exist in database '{ctx.database}'")
      else:
        ExecResult(kind: erkUseSchema, newSchema: op.usName)

    of poBeginTxn:
      if ctx.hasActiveTransaction:
        okResult("BEGIN (transaction already active)")
      else:
        let res = mvccStore.beginTransaction(ctx.sessionId)
        if res.isOk:
          ctx.hasActiveTransaction = true
          okResult("BEGIN")
        else:
          errorResult(&"failed to begin transaction: {res.error.msg}")

    of poCommitTxn:
      if not ctx.hasActiveTransaction:
        okResult("COMMIT (no active transaction)")
      else:
        let res = mvccStore.commitTransaction(ctx.sessionId)
        if res.isOk:
          ctx.hasActiveTransaction = false
          okResult("COMMIT")
        else:
          errorResult(&"failed to commit transaction: {res.error.msg}")

    of poRollbackTxn:
      if not ctx.hasActiveTransaction:
        okResult("ROLLBACK (no active transaction)")
      else:
        let res = mvccStore.rollbackTransaction(ctx.sessionId)
        if res.isOk:
          ctx.hasActiveTransaction = false
          okResult("ROLLBACK")
        else:
          errorResult(&"failed to rollback transaction: {res.error.msg}")

    of poExplain:
      let text = formatPlan(op.exInnerPlan)
      var rows: seq[seq[string]]
      for line in text.split('\n'):
        rows.add(@[line])
      rowsResult(@["plan"], rows)

    if lastResult.kind == erkError:
      return lastResult

  lastResult

proc executeSQLWithTxn*(sql: string, store: RaftKVStoreExt,
    mvccStore: MvccTransactionStore, ctx: ExecutorContext): ExecResult =
  ## Parse a SQL statement, plan it, and execute it with MVCC transaction support.
  ## The ctx maintains session state across calls - use the same ctx for
  ## multiple statements in a transaction.

  # Ensure session is initialized
  if ctx.sessionId == 0:
    ctx.sessionId = mvccStore.createSession()

  try:
    let stmts = parseAll(sql)
    if stmts.len == 0:
      return errorResult("empty SQL statement")
    var lastResult = okResult("ok")
    for stmt in stmts:
      let plan = planStatement(stmt, store, mvccStore, ctx.database, ctx.schema)
      lastResult = executeWithTxn(plan, store, mvccStore, ctx)

      # Update context on USE DATABASE/SCHEMA
      if lastResult.kind == erkUseDatabase:
        ctx.database = lastResult.newDatabase
      elif lastResult.kind == erkUseSchema:
        ctx.schema = lastResult.newSchema

      if lastResult.kind == erkError:
        return lastResult
    lastResult
  except PlanError as e:
    errorResult(e.msg)
  except CatchableError as e:
    errorResult(&"SQL error: {e.msg}")

