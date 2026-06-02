# SQL Executor for Fractio
#
# Executes a Plan against a FractioClient, returning results.
# Each PlanOp maps directly to KV operations via the client.
# Supports MVCC transactions through the client's transaction API.
# Streaming SELECT results for large table scans.
#
# Pure expression evaluation functions are in expr_eval.nim and are
# fully testable without I/O dependencies.

import std/[options, strutils, strformat, sequtils]
import ./ast
import ./planner
import ./data_row
import ./expr_eval # Pure expression evaluation functions
import ../distributed/meta/system_tables
import ../distributed/meta/system_schemas
import ../distributed/raft/group_types
import ../distributed/sharedtimer/timeprovider
import ../client/fractio_client
import ../core/types as coreTypes
import ../core/kv_interface # KVStore interface for mockable testing
import ../protocol/client # For StreamingScanClient
import ../protocol/types # For ProtocolError, peErr
import ../protocol/messages/kv as kvMsgs # For ScanPair
import ../utils/external_merge_sort # For SortSpec, sortRowsInMemory, StreamingSortIterator
import ../utils/query_timer
import ../utils/logging

# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

type
  ExecResultKind* = enum
    erkRows          ## SELECT results (buffered in memory)
    erkStreamingRows ## SELECT results (streaming iterator)
    erkModified      ## INSERT/UPDATE/DELETE affected rows
    erkOk            ## DDL success
    erkError         ## Error
    erkUseDatabase   ## USE DATABASE — caller should update session context
    erkUseSchema     ## USE SCHEMA — caller should update session context

  ExecResult* = ref object
    case kind*: ExecResultKind
    of erkRows:
      columns*: seq[string]
      rows*: seq[seq[string]]     # each row is column values as strings
    of erkStreamingRows:
      streamIterator*: StreamingRowIterator ## Streaming iterator for lazy row access
      streamColumns*: seq[string] ## Column names for streaming result
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

  StreamingRowIterator* = ref object
    ## Streaming iterator that wraps StreamingScanClient and yields decoded rows.
    ## Handles filtering, column extraction, and LIMIT enforcement lazily.
    stream*: StreamingScanClient ## Underlying KV stream
    filter*: Option[Expr] ## WHERE clause filter (optional)
    columns*: seq[string] ## Columns to extract
    allColumns*: seq[string] ## All table columns for decoding
    limit*: uint32 ## LIMIT value (0 = no limit)
    rowsReturned*: int ## Count of rows returned (for LIMIT)
    exhausted*: bool ## True when no more rows available
    pendingRow*: Option[seq[string]] ## Next row ready for consumption
    error*: Option[string] ## Error message if stream failed
    isSystemTable*: bool ## True if scanning a system table
    systemTableId*: TableId ## Table ID for system table decoding
    scanTimer*: QueryTimer ## Timing instrumentation for scan phases

  ExecutorContext* = ref object
    ## Execution context for a session, holding transaction state.
    ## Uses KVStore interface for mockable KV operations.
    ## The client field is kept for FractioClient-specific operations
    ## (space management, routing) that aren't part of the basic KV interface.
    kv*: KVStore
    client*: FractioClient
    txnId*: TransactionID
    readTimestamp*: uint64
    hasActiveTransaction*: bool
    database*: string
    schema*: string
    tempDir*: string ## Base directory for temporary files (sort, etc.)
    timeProvider*: TimeProvider ## Cluster time source (nil = local clock)

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

proc streamingRowsResult*(columns: seq[string],
    rowIter: StreamingRowIterator,
    scanTimer: QueryTimer = nil): ExecResult =
  ## Create a streaming result that yields rows lazily.
  if scanTimer != nil:
    rowIter.scanTimer = scanTimer
  ExecResult(kind: erkStreamingRows, streamColumns: columns,
             streamIterator: rowIter)

# ---------------------------------------------------------------------------
# System table detection and decoding helpers
# ---------------------------------------------------------------------------

proc decodeSystemTableRecord*(tableId: TableId, rawValue: string, columns: seq[
    string]): seq[string] =
  ## Decode a system table record based on its table ID.
  ## Returns column values as strings.
  let ulid = ULID(tableId)
  let sysTableNum = ulid.data[15]

  # Strip MVCC header first
  let (payload, isDeleted) = stripMVCCHeader(rawValue)
  if isDeleted or payload.len == 0:
    return @[]

  # Decode based on system table number
  case sysTableNum
  of SYS_DATABASES_TABLE_NUM:
    let rec = decodeDatabaseRecord(payload)
    result = newSeq[string](columns.len)
    for i, col in columns:
      case col.toLowerAscii()
      of "_key": result[i] = rec.name
      of "name": result[i] = rec.name
      of "createdat": result[i] = $rec.createdAtNs
      else: result[i] = ""

  of SYS_SCHEMAS_TABLE_NUM:
    let rec = decodeSchemaRecord(payload)
    result = newSeq[string](columns.len)
    for i, col in columns:
      case col.toLowerAscii()
      of "_key": result[i] = rec.database & "." & rec.name
      of "name": result[i] = rec.name
      of "database": result[i] = rec.database
      of "createdat": result[i] = $rec.createdAtNs
      else: result[i] = ""

  of SYS_TABLES_TABLE_NUM:
    let rec = decodeTableRecord(payload)
    result = newSeq[string](columns.len)
    for i, col in columns:
      case col.toLowerAscii()
      of "_key": result[i] = rec.database & "." & rec.schema & "." & rec.name
      of "tableid": result[i] = $rec.tableId
      of "name": result[i] = rec.name
      of "schema": result[i] = rec.schema
      of "database": result[i] = rec.database
      of "spaceid": result[i] = $rec.spaceId
      of "primarykey": result[i] = rec.primaryKey.join(",")
      of "columns": result[i] = "in sys.columns"
      else: result[i] = ""

  of SYS_COLUMNS_TABLE_NUM:
    let rec = decodeColumnRecord(payload)
    result = newSeq[string](columns.len)
    for i, col in columns:
      case col.toLowerAscii()
      of "_key": result[i] = $rec.tableId & "/" & $rec.ordinal
      of "tableid": result[i] = $rec.tableId
      of "name": result[i] = rec.name
      of "ordinal": result[i] = $rec.ordinal
      of "datatype":
        var dt: string
        case rec.dataType
        of cdtInt: dt = "INT"
        of cdtFloat: dt = "FLOAT"
        of cdtString: dt = "TEXT"
        of cdtBool: dt = "BOOL"
        of cdtBytes: dt = "BLOB"
        of cdtDate: dt = "DATE"
        of cdtDateTime: dt = "DATETIME"
        of cdtULID: dt = "ULID"
        result[i] = dt
      of "maxlen": result[i] = $rec.maxLen
      of "flags": result[i] = $rec.flags
      else: result[i] = ""

  of SYS_GROUPS_TABLE_NUM:
    let rec = decodeGroupRecord(payload)
    result = newSeq[string](columns.len)
    for i, col in columns:
      case col.toLowerAscii()
      of "_key": result[i] = $rec.groupId
      of "groupid": result[i] = $rec.groupId
      of "spaceid": result[i] = $rec.spaceId
      of "preferredleader": result[i] = $rec.preferredLeader
      of "leader": result[i] = $rec.leader
      of "replicas": result[i] = $rec.replicas.len & " replicas" # Summary
      else: result[i] = ""

  of SYS_NODES_TABLE_NUM:
    let rec = decodeNodeRecord(payload)
    result = newSeq[string](columns.len)
    for i, col in columns:
      case col.toLowerAscii()
      of "_key": result[i] = $rec.nodeId
      of "nodeid": result[i] = $rec.nodeId
      of "host": result[i] = rec.host
      of "raftport": result[i] = $rec.raftPort
      of "clientport": result[i] = $rec.clientPort
      of "status":
        # Strip the 'ns' prefix from enum value for cleaner output
        let statusStr = $rec.status
        if statusStr.startsWith("ns"):
          result[i] = statusStr[2..^1].toLowerAscii() # Remove 'ns' prefix and lowercase
        else:
          result[i] = statusStr.toLowerAscii()
      else: result[i] = ""

  of SYS_SETTINGS_TABLE_NUM:
    let rec = decodeSettingRecord(payload)
    result = newSeq[string](columns.len)
    for i, col in columns:
      case col.toLowerAscii()
      of "_key": result[i] = "" # Key comes from KV store key, not record
      of "value": result[i] = rec.value
      else: result[i] = ""

  of SYS_SPACES_TABLE_NUM:
    let rec = decodeSpaceRecord(payload)
    result = newSeq[string](columns.len)
    for i, col in columns:
      case col.toLowerAscii()
      of "_key": result[i] = $rec.spaceId
      of "spaceid": result[i] = $rec.spaceId
      of "name": result[i] = rec.name
      of "replicas": result[i] = $rec.replicas
      of "groupcount": result[i] = $rec.groupCount
      of "groupids": result[i] = rec.groupIds.mapIt($it).join(",")
      of "oldgroupids": result[i] = rec.oldGroupIds.mapIt($it).join(",")
      of "rebalancing": result[i] = $(rec.workerState != uint8(wsrIdle))
      of "workerstate": result[i] = $rec.workerState
      of "workernodeid": result[i] = $rec.workerNodeId
      of "createdat": result[i] = $rec.createdAtNs
      else: result[i] = ""

  of SYS_NODE_METRICS_NUM, SYS_GROUP_METRICS_NUM, SYS_EVENTS_TABLE_NUM:
    # For metrics and events tables, return raw _key and _value
    result = newSeq[string](columns.len)
    for i, col in columns:
      case col.toLowerAscii()
      of "_key": result[i] = "" # Would need key decoding
      of "_value": result[i] = payload # Raw binary
      else: result[i] = ""

  else:
    # Unknown system table - return empty
    result = newSeq[string](columns.len)

# ---------------------------------------------------------------------------
# StreamingRowIterator implementation
# ---------------------------------------------------------------------------

proc newStreamingRowIterator*(stream: StreamingScanClient,
    filter: Option[Expr], columns: seq[string], allColumns: seq[string],
    limit: uint32, isSystemTable: bool = false,
        systemTableId: TableId = zeroTableId()): StreamingRowIterator =
  ## Create a new streaming row iterator.
  new(result)
  result.stream = stream
  result.filter = filter
  result.columns = columns
  result.allColumns = allColumns
  result.limit = limit
  result.rowsReturned = 0
  result.exhausted = false
  result.pendingRow = none(seq[string])
  result.error = none(string)
  result.isSystemTable = isSystemTable
  result.systemTableId = systemTableId

proc fetchNextMatchingRow*(iter: StreamingRowIterator): Option[seq[string]] =
  ## Fetch the next row that matches the filter (if any).
  ## Returns some(row) if found, none if exhausted.
  ## This is internal - callers should use hasNext/nextRow.
  if iter.exhausted:
    return none(seq[string])

  # Check limit
  if iter.limit > 0 and iter.rowsReturned >= int(iter.limit):
    iter.exhausted = true
    return none(seq[string])

  # Debug: check if stream exists
  if iter.stream == nil:
    iter.exhausted = true
    return none(seq[string])

  # Search for next matching row
  while iter.stream.hasNext():
    let pairOpt = iter.stream.nextPair()
    if pairOpt.isNone:
      iter.exhausted = true
      return none(seq[string])

    let pair = pairOpt.get()
    try:
      if iter.isSystemTable:
        # System tables use binary encoding
        let rowVals = decodeSystemTableRecord(iter.systemTableId, pair.value, iter.columns)
        if rowVals.len > 0:
          # Note: System table filter matching needs special handling
          # For now, skip filter matching on system tables
          inc iter.rowsReturned
          return some(rowVals)
      else:
        let dataRow = decodeDataRow(pair.value)

        # Apply filter if present
        if not matchesFilterDataRow(iter.filter, dataRow):
          continue # Skip non-matching row
        
        # Extract requested columns
        let extracted = extractColumnsFromDataRow(dataRow, iter.columns)
        inc iter.rowsReturned
        return some(extracted)
    except ValueError:
      # Skip malformed rows
      continue

  iter.exhausted = true
  return none(seq[string])

proc hasNextRow*(iter: StreamingRowIterator): bool =
  ## Check if more rows are available.
  ## May fetch ahead to find a matching row.
  if iter.exhausted:
    return false

  # Check limit
  if iter.limit > 0 and iter.rowsReturned >= int(iter.limit):
    iter.exhausted = true
    return false

  # If we have a pending row, return true
  if iter.pendingRow.isSome:
    return true

  # Debug: check stream
  if iter.stream == nil:
    iter.exhausted = true
    return false

  # Try to fetch next matching row
  let nextRow = iter.fetchNextMatchingRow()
  if nextRow.isSome:
    iter.pendingRow = nextRow
    return true

  iter.exhausted = true
  return false

proc nextRow*(iter: StreamingRowIterator): Option[seq[string]] =
  ## Get the next row from the iterator.
  ## Returns some(row) if available, none if exhausted.
  if iter.exhausted:
    return none(seq[string])

  # Return pending row if we have one
  if iter.pendingRow.isSome:
    let row = iter.pendingRow.get()
    iter.pendingRow = none(seq[string])
    return some(row)

  # Fetch next matching row
  iter.fetchNextMatchingRow()

proc closeIterator*(iter: StreamingRowIterator) =
  ## Close the iterator and release resources.
  iter.exhausted = true
  if iter.stream != nil:
    iter.stream.closeStream()

proc getIteratorError*(iter: StreamingRowIterator): Option[string] =
  ## Get any error that occurred during iteration.
  iter.error

proc consumeAllRows*(iter: StreamingRowIterator): seq[seq[string]] =
  ## Consume all remaining rows from the iterator.
  ## Warning: For large result sets, this defeats the purpose of streaming.
  var rows: seq[seq[string]] = @[]
  # Debug: check state
  if iter.stream == nil:
    return rows
  if iter.exhausted:
    return rows
  while iter.hasNextRow():
    let rowOpt = iter.nextRow()
    if rowOpt.isSome:
      rows.add(rowOpt.get())
  iter.closeIterator()
  rows

proc extractRequestedColumns*(rows: seq[seq[string]],
                              requestedCols: seq[string],
                              allFetchedCols: seq[string]): seq[seq[string]] =
  ## Extract only the requested columns from rows that contain all fetched columns.
  ## Used after ORDER BY sorting to return only the original requested columns.
  ## rows: rows containing all fetched columns (requested + ORDER BY columns)
  ## requestedCols: columns to output (original SELECT columns)
  ## allFetchedCols: all columns present in each row
  if requestedCols.len == allFetchedCols.len:
    # No columns to remove - return as-is
    return rows

  # Build column index mapping
  var colIndices: seq[int] = @[]
  for reqCol in requestedCols:
    var found = false
    for i, col in allFetchedCols:
      if col == reqCol:
        colIndices.add(i)
        found = true
        break
    if not found:
      # Column not found - add placeholder
      colIndices.add(-1)

  # Extract columns for each row
  result = @[]
  for row in rows:
    var extractedRow: seq[string] = @[]
    for idx in colIndices:
      if idx >= 0 and idx < row.len:
        extractedRow.add(row[idx])
      else:
        extractedRow.add("NULL")
    result.add(extractedRow)

proc bufferRows*(res: ExecResult): ExecResult =
  ## Convert streaming rows to buffered rows.
  ## If res is erkStreamingRows, consumes all rows and returns erkRows.
  ## Otherwise returns the original res unchanged.
  ## Warning: For large result sets, this defeats the purpose of streaming.
  if res.kind == erkStreamingRows:
    # Debug: check if streamIterator exists
    if res.streamIterator == nil:
      return ExecResult(kind: erkRows, columns: res.streamColumns, rows: @[])
    # Debug: check if stream has data
    if res.streamIterator.stream == nil:
      return ExecResult(kind: erkRows, columns: res.streamColumns, rows: @[])
    let rows = res.streamIterator.consumeAllRows()
    ExecResult(kind: erkRows, columns: res.streamColumns, rows: rows)
  else:
    res

# ---------------------------------------------------------------------------
# ExecutorContext helpers
# ---------------------------------------------------------------------------

proc newExecutorContext*(client: FractioClient, database: string = "default",
    schema: string = "public", tempDir: string = ""): ExecutorContext =
  ## Create a new executor context with default settings.
  ## Uses the FractioClient as the KVStore implementation.
  ## tempDir is the base directory for temporary files; if empty, uses default.
  ExecutorContext(
    kv: client, # FractioClient implements KVStoreWithRouting
    client: client,
    txnId: client.activeTxnId,
    readTimestamp: client.activeReadTs,
    hasActiveTransaction: not isZero(client.activeTxnId),
    database: database,
    schema: schema,
    tempDir: tempDir
  )

proc newExecutorContextWithKV*(kv: KVStore, client: FractioClient = nil,
    database: string = "default", schema: string = "public",
    txnId: TransactionID = zeroTransactionID(),
    readTimestamp: uint64 = 0, tempDir: string = ""): ExecutorContext =
  ## Create a new executor context with a custom KVStore implementation.
  ## This is useful for testing with MockKVStore.
  ## If client is nil, space operations (CREATE/DROP SPACE) will fail.
  ExecutorContext(
    kv: kv,
    client: client,
    txnId: txnId,
    readTimestamp: readTimestamp,
    hasActiveTransaction: not isZero(txnId),
    database: database,
    schema: schema,
    tempDir: tempDir
  )

# ---------------------------------------------------------------------------
# Transaction-aware KV operation helpers
# ---------------------------------------------------------------------------

proc txnGet(ctx: ExecutorContext, key: string): KVOpResult[Option[string]] =
  ## Get a value, using transactional read if in a transaction,
  ## or latest MVCC read otherwise.
  ctx.kv.get(key, txnId = ctx.txnId, readTimestamp = ctx.readTimestamp)

proc execTxnScan(ctx: ExecutorContext, startKey, endKey: string,
    limit: uint32 = 0): KVOpResult[seq[tuple[key, value: string]]] =
  ## Scan keys with MVCC awareness (non-streaming, buffered).
  ctx.kv.scan(startKey, endKey, limit, txnId = ctx.txnId,
              readTimestamp = ctx.readTimestamp)

proc execTxnStreamScan(ctx: ExecutorContext, startKey, endKey: string,
    limit: uint32 = 0,
    filter: Option[kvMsgs.WireFilterExpr] = none(
        kvMsgs.WireFilterExpr)): Result[StreamingScanClient, ProtocolError] =
  ## Streaming scan keys with MVCC awareness.
  ## Returns a StreamingScanClient for lazy iteration.
  ## Requires ctx.client to be a FractioClient (uses its streamScan method).
  ## filter: optional server-side filter for reducing network traffic.
  if ctx.client == nil:
    return peErr(newProtocolError(peInternal,
        "streaming scan requires FractioClient"))
  ctx.client.streamScan(startKey, endKey, limit, ctx.txnId, ctx.readTimestamp, filter)

# ---------------------------------------------------------------------------
# Per-op executors
# ---------------------------------------------------------------------------

proc execCreateDatabase(op: PlanOp, ctx: ExecutorContext): ExecResult =
  ## Execute CREATE DATABASE with internal MVCC transaction for consistency.
  let key = encodeTableKey(SYS_DATABASES_TABLE_ID, op.cdbName)

  # Create internal transaction
  let txnRes = ctx.kv.beginTxn()
  if txnRes.isErr:
    return errorResult(&"failed to start internal transaction: {txnRes.err}")
  let internalTxnId = txnRes.val.txnId
  let internalReadTimestamp = txnRes.val.readTimestamp

  # Check for duplicate (within transaction snapshot)
  let existing = ctx.kv.get(key, txnId = internalTxnId,
      readTimestamp = internalReadTimestamp)
  if existing.isOk and existing.val.isSome:
    discard ctx.kv.rollbackTxn(internalTxnId)
    if op.cdbIfNotExists:
      return okResult("database already exists (IF NOT EXISTS)")
    return errorResult(&"database '{op.cdbName}' already exists")

  # Write database record (binary encoded - value already encoded by planner)
  let putRes = ctx.kv.put(key, op.cdbValue, txnId = internalTxnId)
  if putRes.isErr:
    discard ctx.kv.rollbackTxn(internalTxnId)
    return errorResult(&"failed to create database: {putRes.err}")

  # Seed a default "public" schema for every new database
  let pubKey = encodeTableKey(SYS_SCHEMAS_TABLE_ID, op.cdbName & ".public")
  let pubRec = SchemaRecord(
    name: "public",
    database: op.cdbName,
    createdAtNs: system_schemas.nowNs(ctx.timeProvider)
  )
  let pubPutRes = ctx.kv.put(pubKey, encode(pubRec),
      txnId = internalTxnId)
  if pubPutRes.isErr:
    discard ctx.kv.rollbackTxn(internalTxnId)
    return errorResult(&"failed to create public schema: {pubPutRes.err}")

  # Commit the transaction
  let commitRes = ctx.kv.commitTxn(internalTxnId)
  if commitRes.isErr:
    return errorResult(&"failed to commit: {commitRes.err}")

  okResult(&"CREATE DATABASE")

proc execDropDatabase(op: PlanOp, ctx: ExecutorContext): ExecResult =
  ## Execute DROP DATABASE with internal MVCC transaction for consistency.
  let key = encodeTableKey(SYS_DATABASES_TABLE_ID, op.ddbName)

  # Create internal transaction
  let txnRes = ctx.kv.beginTxn()
  if txnRes.isErr:
    return errorResult(&"failed to start internal transaction: {txnRes.err}")
  let internalTxnId = txnRes.val.txnId
  let internalReadTimestamp = txnRes.val.readTimestamp

  # Check if database exists
  let existing = ctx.kv.get(key, txnId = internalTxnId,
      readTimestamp = internalReadTimestamp)
  if existing.isErr or existing.val.isNone:
    discard ctx.kv.rollbackTxn(internalTxnId)
    if op.ddbIfExists:
      return okResult("database does not exist (IF EXISTS)")
    return errorResult(&"database '{op.ddbName}' does not exist")

  # Always cascade: delete all schemas, tables, and data rows for this database
  # Delete all schemas for this database
  let schemaPrefix = op.ddbName & "."
  let schemaStart = encodeTableKey(SYS_SCHEMAS_TABLE_ID, schemaPrefix)
  let schemaEnd = encodeTableKey(SYS_SCHEMAS_TABLE_ID, schemaPrefix & "\xFF")
  let schemaScan = ctx.kv.scan(schemaStart, schemaEnd, 0,
      txnId = internalTxnId, readTimestamp = internalReadTimestamp)
  if schemaScan.isOk:
    for entry in schemaScan.val:
      let delRes = ctx.kv.delete(entry.key, txnId = internalTxnId)
      if delRes.isErr:
        discard ctx.kv.rollbackTxn(internalTxnId)
        return errorResult(&"failed to delete schema: {delRes.err}")

  # Find and delete all tables and their data rows
  let tableStart = encodeTableKey(SYS_TABLES_TABLE_ID, "")
  let tableEnd = makeScanEndKey(SYS_TABLES_TABLE_ID)
  let tableScan = ctx.kv.scan(tableStart, tableEnd, 0,
      txnId = internalTxnId, readTimestamp = internalReadTimestamp)
  if tableScan.isOk:
    for entry in tableScan.val:
      let rec = decodeTableRecord(entry.value)
      if rec.database == op.ddbName:
        let tableId = rec.tableId
        # Delete all data rows for this table
        let dataStart = encodeDataRowScanBound(tableId, "")
        let dataEnd = makeDataRowScanEndKey(tableId)
        let dataScan = ctx.kv.scan(dataStart, dataEnd, 0,
            txnId = internalTxnId, readTimestamp = internalReadTimestamp)
        if dataScan.isOk:
          for dataEntry in dataScan.val:
            let delRes = ctx.kv.delete(dataEntry.key,
                txnId = internalTxnId)
            if delRes.isErr:
              discard ctx.kv.rollbackTxn(internalTxnId)
              return errorResult(&"failed to delete data row: {delRes.err}")
        # Delete the table record
        let delRes = ctx.kv.delete(entry.key, txnId = internalTxnId)
        if delRes.isErr:
          discard ctx.kv.rollbackTxn(internalTxnId)
          return errorResult(&"failed to delete table: {delRes.err}")

  # Delete the database record
  let delRes = ctx.kv.delete(key, txnId = internalTxnId)
  if delRes.isErr:
    discard ctx.kv.rollbackTxn(internalTxnId)
    return errorResult(&"failed to drop database: {delRes.err}")

  # Commit the transaction
  let commitRes = ctx.kv.commitTxn(internalTxnId)
  if commitRes.isErr:
    return errorResult(&"failed to commit: {commitRes.err}")

  okResult("DROP DATABASE")

proc execCreateSchema(op: PlanOp, ctx: ExecutorContext): ExecResult =
  ## Execute CREATE SCHEMA with internal MVCC transaction for consistency.
  let key = encodeTableKey(SYS_SCHEMAS_TABLE_ID,
      op.csDatabase & "." & op.csName)

  # Create internal transaction
  let txnRes = ctx.kv.beginTxn()
  if txnRes.isErr:
    return errorResult(&"failed to start internal transaction: {txnRes.err}")
  let internalTxnId = txnRes.val.txnId
  let internalReadTimestamp = txnRes.val.readTimestamp

  let existing = ctx.kv.get(key, txnId = internalTxnId,
      readTimestamp = internalReadTimestamp)
  if existing.isOk and existing.val.isSome:
    discard ctx.kv.rollbackTxn(internalTxnId)
    if op.csIfNotExists:
      return okResult("schema already exists (IF NOT EXISTS)")
    return errorResult(&"schema '{op.csName}' already exists")

  let putRes = ctx.kv.put(key, op.csValue, txnId = internalTxnId)
  if putRes.isErr:
    discard ctx.kv.rollbackTxn(internalTxnId)
    return errorResult(&"failed to create schema: {putRes.err}")

  let commitRes = ctx.kv.commitTxn(internalTxnId)
  if commitRes.isErr:
    return errorResult(&"failed to commit: {commitRes.err}")

  okResult("CREATE SCHEMA")

proc execDropSchema(op: PlanOp, ctx: ExecutorContext): ExecResult =
  ## Execute DROP SCHEMA with internal MVCC transaction for consistency.
  let key = encodeTableKey(SYS_SCHEMAS_TABLE_ID,
      op.dsDatabase & "." & op.dsName)

  # Create internal transaction
  let txnRes = ctx.kv.beginTxn()
  if txnRes.isErr:
    return errorResult(&"failed to start internal transaction: {txnRes.err}")
  let internalTxnId = txnRes.val.txnId
  let internalReadTimestamp = txnRes.val.readTimestamp

  let existing = ctx.kv.get(key, txnId = internalTxnId,
      readTimestamp = internalReadTimestamp)
  if existing.isErr or existing.val.isNone:
    discard ctx.kv.rollbackTxn(internalTxnId)
    if op.dsIfExists:
      return okResult("schema does not exist (IF EXISTS)")
    return errorResult(&"schema '{op.dsName}' does not exist")

  let delRes = ctx.kv.delete(key, txnId = internalTxnId)
  if delRes.isErr:
    discard ctx.kv.rollbackTxn(internalTxnId)
    return errorResult(&"failed to drop schema: {delRes.err}")

  let commitRes = ctx.kv.commitTxn(internalTxnId)
  if commitRes.isErr:
    return errorResult(&"failed to commit: {commitRes.err}")

  okResult("DROP SCHEMA")

proc execCreateTable(op: PlanOp, ctx: ExecutorContext): ExecResult =
  ## Execute CREATE TABLE with internal MVCC transaction for consistency.
  ## Writes the TableRecord into sys.tables and column definitions into sys.columns.
  let key = encodeTableKey(SYS_TABLES_TABLE_ID,
      op.ctDatabase & "." & op.ctSchema & "." & op.ctName)

  # Create internal transaction
  let txnRes = ctx.kv.beginTxn()
  if txnRes.isErr:
    return errorResult(&"failed to start internal transaction: {txnRes.err}")
  let internalTxnId = txnRes.val.txnId
  let internalReadTimestamp = txnRes.val.readTimestamp

  let existing = ctx.kv.get(key, txnId = internalTxnId,
      readTimestamp = internalReadTimestamp)
  if existing.isOk and existing.val.isSome:
    discard ctx.kv.rollbackTxn(internalTxnId)
    if op.ctIfNotExists:
      return okResult("table already exists (IF NOT EXISTS)")
    return errorResult(&"table '{op.ctName}' already exists")

  # Resolve space name to spaceId
  # Note: We do NOT use the transaction's read timestamp for this lookup.
  # CREATE SPACE writes the space record, and we need to see that write immediately.
  # Using the transaction's read timestamp would cause us to not see the newly created space.
  var tableValue = op.ctValue
  var tableId = op.ctTableId
  if op.ctSpaceName.isSome:
    let spaceName = op.ctSpaceName.get()
    let sStart = encodeTableKey(SYS_SPACES_TABLE_ID, "")
    let sEnd = makeScanEndKey(SYS_SPACES_TABLE_ID)
    # Use a fresh scan WITHOUT the transaction's read timestamp to see recent writes
    let sScan = ctx.kv.scan(sStart, sEnd, 0, txnId = zeroTransactionID(),
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
      discard ctx.kv.rollbackTxn(internalTxnId)
      return errorResult(&"space '{spaceName}' does not exist")
    # Update spaceId in the binary table record
    var rec = decodeTableRecord(tableValue)
    rec.spaceId = spaceId
    tableValue = encode(rec)

  # Write the table record to sys.tables
  let putRes = ctx.kv.put(key, tableValue, txnId = internalTxnId)
  if putRes.isErr:
    discard ctx.kv.rollbackTxn(internalTxnId)
    return errorResult(&"failed to create table: {putRes.err}")

  # Write column definitions to sys.columns
  var ordinal = 0
  for col in op.ctColumns:
    let colRec = ColumnRecord(
      tableId: tableId,
      name: col.name,
      ordinal: int32(ordinal),
      dataType: col.dataType,
      maxLen: col.maxLen,
      flags: col.flags
    )
    let colKey = encodeColumnKey(tableId, ordinal)
    let colPutRes = ctx.kv.put(colKey, encode(colRec), txnId = internalTxnId)
    if colPutRes.isErr:
      discard ctx.kv.rollbackTxn(internalTxnId)
      return errorResult(&"failed to create column: {colPutRes.err}")
    inc ordinal

  let commitRes = ctx.kv.commitTxn(internalTxnId)
  if commitRes.isErr:
    return errorResult(&"failed to commit: {commitRes.err}")

  okResult("CREATE TABLE")

proc execDropTable(op: PlanOp, ctx: ExecutorContext): ExecResult =
  ## Execute DROP TABLE with internal MVCC transaction for consistency.
  ## Deletes the table metadata and all associated data rows and index entries.
  ##
  ## Data rows and index entries may be distributed across multiple Raft groups
  ## in a multi-group space. Cross-group transactions are not supported (each
  ## group has its own MVCC session). Therefore, data row and index deletions
  ## use auto-transactions (per-key commit), while system table metadata
  ## operations use a single META-group transaction.
  let key = encodeTableKey(SYS_TABLES_TABLE_ID,
      op.dtDatabase & "." & op.dtSchema & "." & op.dtName)

  # Create internal transaction for system table operations (META group only)
  let txnRes = ctx.kv.beginTxn()
  if txnRes.isErr:
    return errorResult(&"failed to start internal transaction: {txnRes.err}")
  let internalTxnId = txnRes.val.txnId
  let internalReadTimestamp = txnRes.val.readTimestamp

  let existing = ctx.kv.get(key, txnId = internalTxnId,
      readTimestamp = internalReadTimestamp)
  if existing.isErr or existing.val.isNone:
    discard ctx.kv.rollbackTxn(internalTxnId)
    if op.dtIfExists:
      return okResult("table does not exist (IF EXISTS)")
    return errorResult(&"table '{op.dtName}' does not exist")

  # Extract tableId from the table metadata to delete data rows
  let tableValue = existing.val.get()
  var tableId: TableId = zeroTableId()
  try:
    let rec = decodeTableRecord(tableValue)
    tableId = rec.tableId
  except:
    discard # If we can't decode, skip data row cleanup

  # Delete all data rows for this table using auto-transactions.
  # Data rows may be on different Raft groups; cross-group transactions
  # are not supported, so each delete auto-commits independently.
  if tableId != zeroTableId():
    let dataStart = encodeTableKey(tableId, "d/")
    let dataEnd = makeDataRowScanEndKey(tableId)
    # Scan without a transaction to see all rows across all groups
    let dataScan = ctx.kv.scan(dataStart, dataEnd, 0)
    if dataScan.isOk:
      for entry in dataScan.val:
        # Auto-delete (txnId = zero) — each key auto-commmits on its group
        let delRes = ctx.kv.delete(entry.key)
        if delRes.isErr:
          discard ctx.kv.rollbackTxn(internalTxnId)
          return errorResult(&"failed to delete data row: {delRes.err}")

    # Delete all secondary index entries using auto-transactions
    let idxStart = encodeTableKey(tableId, "i/")
    let idxEnd = encodeTableKey(tableId, "j")         # "j" > "i"
    let idxScan = ctx.kv.scan(idxStart, idxEnd, 0)
    if idxScan.isOk:
      for entry in idxScan.val:
        let delRes = ctx.kv.delete(entry.key)
        if delRes.isErr:
          discard ctx.kv.rollbackTxn(internalTxnId)
          return errorResult(&"failed to delete index entry: {delRes.err}")

  # Delete the table metadata (META group — within the transaction)
  let delRes = ctx.kv.delete(key, txnId = internalTxnId)
  if delRes.isErr:
    discard ctx.kv.rollbackTxn(internalTxnId)
    return errorResult(&"failed to drop table: {delRes.err}")

  # Also delete column definitions from sys.columns
  let colStart = encodeTableKey(SYS_COLUMNS_TABLE_ID, $(tableId) & "/")
  let colEnd = encodeTableKey(SYS_COLUMNS_TABLE_ID, $(tableId) & "{")
  let colScan = ctx.kv.scan(colStart, colEnd, 0,
      txnId = internalTxnId, readTimestamp = internalReadTimestamp)
  if colScan.isOk:
    for entry in colScan.val:
      let colDelRes = ctx.kv.delete(entry.key, txnId = internalTxnId)
      if colDelRes.isErr:
        discard ctx.kv.rollbackTxn(internalTxnId)
        return errorResult(&"failed to delete column: {colDelRes.err}")

  let commitRes = ctx.kv.commitTxn(internalTxnId)
  if commitRes.isErr:
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
  ##
  ## NOTE: This operation requires a real FractioClient (not just KVStore).

  if ctx.client == nil:
    return errorResult("CREATE SPACE requires a real FractioClient connection")

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
  ##
  ## NOTE: This operation requires a real FractioClient (not just KVStore).

  if ctx.client == nil:
    return errorResult("DROP SPACE requires a real FractioClient connection")

  # Call server-side dropSpace RPC
  let res = ctx.client.dropSpace(op.dspName)

  if not res.isOk:
    return errorResult(&"failed to drop space: {res.err}")

  okResult("DROP SPACE")

# ---------------------------------------------------------------------------
# MVCC-aware show operations
# ---------------------------------------------------------------------------

proc execShowDatabasesTxn(ctx: ExecutorContext): ExecResult =
  let startKey = encodeTableKey(SYS_DATABASES_TABLE_ID, "")
  let endKey = makeScanEndKey(SYS_DATABASES_TABLE_ID)
  let res = execTxnScan(ctx, startKey, endKey, 0)
  if res.isErr:
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
  if res.isErr:
    return errorResult(&"failed to scan schemas: {res.err}")

  var resultRows: seq[seq[string]]
  # sys is a special implicit schema that always exists on all databases
  resultRows.add(@["sys"])

  for entry in res.val:
    let (rec, isDeleted) = decodeSchemaRecordFromMVCC(entry.value)
    if not isDeleted and (rec.database == op.ssDatabase or op.ssDatabase.len == 0):
      resultRows.add(@[rec.name])

  rowsResult(@["schema_name"], resultRows)

proc execShowTablesTxn(op: PlanOp, ctx: ExecutorContext): ExecResult =
  let startKey = encodeTableKey(SYS_TABLES_TABLE_ID, "")
  let endKey = makeScanEndKey(SYS_TABLES_TABLE_ID)
  let res = execTxnScan(ctx, startKey, endKey, 0)
  if res.isErr:
    return errorResult(&"failed to scan tables: {res.err}")

  var resultRows: seq[seq[string]]

  # sys schema contains implicit/virtual system tables that are always present
  if op.stDatabase == "default" and op.stSchema == "sys":
    for info in SYSTEM_TABLES_REGISTRY:
      # Only include meta-group tables (1-7) in SHOW TABLES
      # metrics/events tables (10+) are tier 2 and less commonly listed
      if info.tableNum <= MAX_META_GROUP_TABLE_NUM:
        resultRows.add(@[info.name])

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
  if res.isErr:
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
    database: string = "default", tempDir: string = ""): ExecResult =
  ## Execute a Plan against a FractioClient, returning an ExecResult.
  ## Processes ops sequentially; returns the result of the last op
  ## (or the first error).
  ##
  ## All operations require client for consistency:
  ## - DDL operations use internal auto-commit transactions
  ## - DML operations use implicit transactions if not in an explicit one
  ##
  ## tempDir is the base directory for temporary files (sort, etc.).
  ## This is the simplified unified entry point.
  if client == nil:
    return errorResult("FractioClient is required for all operations")

  let ctx = newExecutorContext(client, database, "public", tempDir)
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
    let res = ctx.kv.beginTxn()
    if res.isOk:
      ctx.txnId = res.val.txnId
      ctx.readTimestamp = res.val.readTimestamp
      ctx.hasActiveTransaction = true
      true
    else:
      false

  proc commitImplicitTxn(): bool =
    ## Commit an implicit transaction. Returns true on success.
    let res = ctx.kv.commitTxn(ctx.txnId)
    if res.isOk:
      ctx.hasActiveTransaction = false
      ctx.txnId = zeroTransactionID()
      ctx.readTimestamp = 0
      true
    else:
      false

  proc rollbackImplicitTxn() =
    ## Rollback an implicit transaction.
    discard ctx.kv.rollbackTxn(ctx.txnId)
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
      # Use binary-encoded PK values from planner (insPkValues)
      for i, rowBinary in op.insRows:
        let pkBinary = op.insPkValues[i]
        if pkBinary.len == 0:
          error = "INSERT requires a primary key value"
          break
        # Encode data row key with binary PK
        let key = encodeDataRowScanBound(op.insTableId, pkBinary)
        # Use active transaction if available, otherwise auto-transaction
        let res = ctx.kv.put(key, rowBinary, txnId = ctx.txnId)
        if res.isErr:
          error = &"failed to insert row: {res.err}"
          break
        inc count

      if error.len > 0:
        return errorResult(error)

      modifiedResult(count, &"INSERT {count}")

    of poPointGet:
      # Use correct key encoding based on table descriptor's keyEncoding
      let isSysTable = op.pgKeyEncoding == tkeSystemTable

      # Use correct key encoding based on table type
      let key = if isSysTable:
        encodeTableKey(op.pgTableId, op.pgKey)
      else:
        encodeDataRowScanBound(op.pgTableId, op.pgKey)

      if isSysTable:
        # System table lookup - use binary decoding
        let res = txnGet(ctx, key)
        if res.isErr:
          return errorResult(&"failed to read: {res.err}")
        if res.val.isNone:
          return rowsResult(op.pgColumns, @[])

        let rowVals = decodeSystemTableRecord(op.pgTableId, res.val.get(), op.pgColumns)
        if rowVals.len == 0:
          return rowsResult(op.pgColumns, @[])
        rowsResult(op.pgColumns, @[rowVals])
      else:
        # User table lookup - use DataRow decoding
        # Use server-side filter when FractioClient is available (PointGet optimization)
        # Fall back to client-side filtering for MockKVStore tests
        if ctx.client != nil and op.pgFilter.isSome:
          # Convert Expr to WireFilterExpr for server-side evaluation
          let wireFilter = exprToWireFilterExpr(op.pgFilter.get())
          let res = ctx.client.getWithFilter(key, some(wireFilter),
                                             ctx.txnId, ctx.readTimestamp)
          if res.isErr:
            return errorResult(&"failed to read: {res.err}")
          if res.val.isNone:
            # Row doesn't exist or doesn't pass server-side filter
            return rowsResult(op.pgColumns, @[])
          let row = decodeDataRow(res.val.get())
          let vals = extractColumnsFromDataRow(row, op.pgColumns)
          rowsResult(op.pgColumns, @[vals])
        else:
          # Fallback path: get value then apply filter client-side
          let res = txnGet(ctx, key)
          if res.isErr:
            return errorResult(&"failed to read: {res.err}")
          if res.val.isNone:
            return rowsResult(op.pgColumns, @[])
          let row = decodeDataRow(res.val.get())

          # Apply remaining filter if present (pk = value AND other_cond)
          if op.pgFilter.isSome and not matchesFilterDataRow(op.pgFilter, row):
            return rowsResult(op.pgColumns, @[])

          let vals = extractColumnsFromDataRow(row, op.pgColumns)
          rowsResult(op.pgColumns, @[vals])

    of poScan:
      # Use streaming scan for SELECT queries when FractioClient is available
      # Fall back to buffered scan for MockKVStore tests
      # Handle system tables specially - they use binary encoding, not DataRow

      let isSysTable = op.scKeyEncoding == tkeSystemTable

      if ctx.client != nil:
        # Convert Expr to WireFilterExpr for server-side filtering
        # Note: Server filter reduces network traffic, client-side filter handles
        # complex conditions that can't be expressed in WireFilterExpr
        var serverFilter: Option[kvMsgs.WireFilterExpr] = none(
            kvMsgs.WireFilterExpr)
        if op.scFilter.isSome:
          serverFilter = some(exprToWireFilterExpr(op.scFilter.get()))

        let scanTimer = newQueryTimer()
        let streamRes = execTxnStreamScan(ctx, op.scStartKey, op.scEndKey, 0, serverFilter)
        scanTimer.stamp("stream_scan_setup")
        if streamRes.isErr:
          return errorResult(&"failed to start streaming scan: {streamRes.error.msg}")

        # Create streaming row iterator that handles filtering and LIMIT
        # Pass original Expr filter for complex client-side conditions
        # For system tables, use special decoder
        let rowIter = newStreamingRowIterator(
          streamRes.value,
          op.scFilter,
          op.scColumns,
          op.scAllColumns,
          op.scLimit,
          isSysTable,  # iter.isSystemTable
          op.scTableId # iter.systemTableId
        )

        streamingRowsResult(op.scColumns, rowIter, scanTimer)
      else:
        # Fallback to buffered scan for mock/testing contexts
        let res = execTxnScan(ctx, op.scStartKey, op.scEndKey, 0)
        if res.isErr:
          return errorResult(&"failed to scan: {res.err}")

        var resultRows: seq[seq[string]] = @[]
        var count = 0
        for entry in res.val:
          try:
            if isSysTable:
              # System tables use binary encoding
              let rowVals = decodeSystemTableRecord(op.scTableId, entry.value, op.scColumns)
              if rowVals.len > 0:
                # Note: System table filter matching would need special handling
                resultRows.add(rowVals)
                inc count
                if op.scLimit > 0 and count >= int(op.scLimit):
                  break
            else:
              let row = decodeDataRow(entry.value)
              if matchesFilterDataRow(op.scFilter, row):
                resultRows.add(extractColumnsFromDataRow(row, op.scColumns))
                inc count
                if op.scLimit > 0 and count >= int(op.scLimit):
                  break
          except ValueError:
            discard # skip malformed rows

        rowsResult(op.scColumns, resultRows)

    of poOrderBy:
      # ORDER BY operates on the previous result (should be erkRows or erkStreamingRows)
      # The previous op is always poScan or poPointGet
      # obAllColumns contains all columns fetched for sorting (requested + ORDER BY columns)
      # obColumns contains the original requested columns for final output
      # obLimit is applied after sorting (LIMIT semantics)
      # obOptimization indicates PK-based optimization:
      # - oboPkAscMatch: data already sorted, skip sorting
      # - oboPkDescMatch: data needs reverse, use temp files
      # - oboNone: full sort algorithm

      # Handle optimization cases
      let orderTimer = if lastResult.kind == erkStreamingRows and
          lastResult.streamIterator.scanTimer != nil:
        lastResult.streamIterator.scanTimer
      else:
        newQueryTimer()

      if op.obOptimization == oboPkAscMatch:
        # Data is already sorted by PK ASC - skip sorting
        # Just extract requested columns and apply LIMIT
        if lastResult.kind == erkRows:
          var outputRows = lastResult.rows
          # Extract only requested columns if needed
          if op.obColumns.len != lastResult.columns.len:
            outputRows = extractRequestedColumns(outputRows, op.obColumns,
                op.obAllColumns)
          # Apply LIMIT
          if op.obLimit > 0 and outputRows.len > int(op.obLimit):
            outputRows = outputRows[0..<int(op.obLimit)]
          rowsResult(op.obColumns, outputRows)
        elif lastResult.kind == erkStreamingRows:
          # Data is already in PK ASC order from the scan, so no sort needed.
          # Stream rows directly with LIMIT pushdown instead of buffering all.
          orderTimer.stamp("order_start")
          var outputRows: seq[seq[string]] = @[]
          let limitRows = if op.obLimit > 0: int(op.obLimit) else: -1
          while lastResult.streamIterator.hasNextRow():
            let rowOpt = lastResult.streamIterator.nextRow()
            if rowOpt.isSome:
              outputRows.add(rowOpt.get())
              if limitRows > 0 and outputRows.len >= limitRows:
                break
          orderTimer.stamp("stream_consume")
          # CRITICAL: close the iterator to prevent TCP frame bleed.
          # When a streaming scan is abandoned before exhaustion (e.g., LIMIT
          # reached or early break), the server may have already sent additional
          # response frames into the socket buffer. Reusing the cached
          # connection for the next RPC would read those stale frames instead
          # of the expected response, causing silent data corruption.
          lastResult.streamIterator.closeIterator()
          # Extract only requested columns if needed
          if op.obColumns.len != lastResult.streamColumns.len:
            outputRows = extractRequestedColumns(outputRows, op.obColumns,
                op.obAllColumns)
          orderTimer.stamp("column_extract")
          debug &"[exec_timer] obOptimization=PK_ASC_MATCH rows={outputRows.len} {orderTimer.formatBreakdown()}"
          rowsResult(op.obColumns, outputRows)
        else:
          errorResult("ORDER BY requires row results from previous operation")

      elif op.obOptimization == oboPkDescMatch:
        # Data is sorted by PK ASC but needs to be reversed to DESC
        # Use temp-file based reversal for memory-limited operation
        if lastResult.kind == erkRows:
          if lastResult.rows.len <= 1:
            # No reversal needed for empty or single-row results
            var outputRows = lastResult.rows
            if op.obColumns.len != lastResult.columns.len:
              outputRows = extractRequestedColumns(outputRows, op.obColumns,
                  op.obAllColumns)
            rowsResult(op.obColumns, outputRows)
          else:
            # Reverse using temp files
            let reversedRows = reverseRowsWithTempFiles(lastResult.rows,
                op.obColumns, op.obAllColumns)
            var outputRows = reversedRows
            # Apply LIMIT after reversal
            if op.obLimit > 0 and outputRows.len > int(op.obLimit):
              outputRows = outputRows[0..<int(op.obLimit)]
            rowsResult(op.obColumns, outputRows)
        elif lastResult.kind == erkStreamingRows:
          # Buffer streaming rows, then reverse
          orderTimer.stamp("order_start")
          let bufferedRows = lastResult.streamIterator.consumeAllRows()
          orderTimer.stamp("stream_consume")
          if bufferedRows.len <= 1:
            var outputRows = bufferedRows
            if op.obColumns.len != lastResult.streamColumns.len:
              outputRows = extractRequestedColumns(outputRows, op.obColumns,
                  op.obAllColumns)
            rowsResult(op.obColumns, outputRows)
          else:
            let reversedRows = reverseRowsWithTempFiles(bufferedRows,
                op.obColumns, op.obAllColumns)
            orderTimer.stamp("reverse")
            var outputRows = reversedRows
            if op.obLimit > 0 and outputRows.len > int(op.obLimit):
              outputRows = outputRows[0..<int(op.obLimit)]
            debug &"[exec_timer] obOptimization=PK_DESC_MATCH rows={outputRows.len} {orderTimer.formatBreakdown()}"
            rowsResult(op.obColumns, outputRows)
        else:
          errorResult("ORDER BY requires row results from previous operation")

      elif op.obOptimization == oboTopK:
        # ORDER BY + LIMIT with bounded top-K heap.
        # Instead of buffering all N rows and sorting (O(N log N) time, O(N) memory),
        # we use a bounded max-heap that keeps only the top K rows while streaming.
        # This gives O(N log K) time and O(K) memory.
        let limitRows = if op.obLimit > 0: int(op.obLimit) else: 10
        let heap = newTopKHeap(op.obSortSpecs, op.obAllColumns, limitRows)

        if lastResult.kind == erkRows:
          # Already buffered — feed through heap
          for row in lastResult.rows:
            heap.push(row)
          let sortedRows = heap.extractSorted()
          var outputRows = extractRequestedColumns(sortedRows, op.obColumns,
              op.obAllColumns)
          rowsResult(op.obColumns, outputRows)
        elif lastResult.kind == erkStreamingRows:
          # Stream rows through the top-K heap
          orderTimer.stamp("order_start")
          while lastResult.streamIterator.hasNextRow():
            let rowOpt = lastResult.streamIterator.nextRow()
            if rowOpt.isSome:
              heap.push(rowOpt.get())
          orderTimer.stamp("stream_consume+topk")
          lastResult.streamIterator.closeIterator()
          let sortedRows = heap.extractSorted()
          orderTimer.stamp("extract_sorted")
          var outputRows = extractRequestedColumns(sortedRows, op.obColumns,
              op.obAllColumns)
          orderTimer.stamp("column_extract")
          debug &"[exec_timer] obOptimization=TOP_K rows={outputRows.len}/{heap.totalPushed} {orderTimer.formatBreakdown()}"
          rowsResult(op.obColumns, outputRows)
        else:
          errorResult("ORDER BY requires row results from previous operation")

      else:
        # No optimization - use full sort algorithm
        if lastResult.kind == erkRows:
          # In-memory sort for buffered results
          if lastResult.rows.len <= 1:
            # No sorting needed for empty or single-row results
            # Still apply LIMIT if present
            if op.obLimit > 0 and lastResult.rows.len > int(op.obLimit):
              rowsResult(op.obColumns, lastResult.rows[0..<int(op.obLimit)])
            else:
              lastResult
          else:
            let sortedRows = sortRowsInMemory(lastResult.rows, op.obSortSpecs,
                op.obAllColumns)
            # Extract only the requested columns after sorting
            var outputRows = extractRequestedColumns(sortedRows, op.obColumns,
                op.obAllColumns)
            # Apply LIMIT after sorting
            if op.obLimit > 0 and outputRows.len > int(op.obLimit):
              outputRows = outputRows[0..<int(op.obLimit)]
            rowsResult(op.obColumns, outputRows)
        elif lastResult.kind == erkStreamingRows:
          # Stream rows through external merge sort to avoid buffering
          # everything in memory. For small result sets, falls back to
          # in-memory sort after consuming the stream.
          orderTimer.stamp("order_start")
          const EXTERNAL_SORT_THRESHOLD = 10000
          let hasLimit = op.obLimit > 0
          let estimatedRows = op.obLimit.int # rough upper bound if LIMIT present

          # For small expected results or when no ORDER BY specs, buffer and
          # sort in memory. For large results, use external merge sort.
          if hasLimit and estimatedRows < EXTERNAL_SORT_THRESHOLD:
            # Small result set expected — buffer and sort in memory
            let bufferedRows = lastResult.streamIterator.consumeAllRows()
            orderTimer.stamp("stream_consume")
            if bufferedRows.len <= 1:
              if op.obLimit > 0 and bufferedRows.len > int(op.obLimit):
                rowsResult(op.obColumns, bufferedRows[0..<int(op.obLimit)])
              else:
                rowsResult(op.obColumns, bufferedRows)
            else:
              let sortedRows = sortRowsInMemory(bufferedRows, op.obSortSpecs,
                  op.obAllColumns)
              orderTimer.stamp("sort")
              var outputRows = extractRequestedColumns(sortedRows, op.obColumns,
                  op.obAllColumns)
              if op.obLimit > 0 and outputRows.len > int(op.obLimit):
                outputRows = outputRows[0..<int(op.obLimit)]
              debug &"[exec_timer] obOptimization=NONE(inmem) rows={outputRows.len} {orderTimer.formatBreakdown()}"
              rowsResult(op.obColumns, outputRows)
          else:
            # Large result set or no limit — use external merge sort
            # to avoid OOM on large datasets.
            let sortIter = newStreamingSortIterator(op.obSortSpecs,
                op.obAllColumns)
            if op.obLimit > 0:
              sortIter.limit = uint32(op.obLimit)

            # Feed streaming rows into the sorter in chunks
            const FEED_CHUNK_SIZE = 1000
            var feedChunk: seq[seq[string]] = @[]
            while lastResult.streamIterator.hasNextRow():
              let rowOpt = lastResult.streamIterator.nextRow()
              if rowOpt.isSome:
                feedChunk.add(rowOpt.get())
                if feedChunk.len >= FEED_CHUNK_SIZE:
                  sortIter.addRowsToIterator(feedChunk)
                  feedChunk = @[]
            # Flush remaining rows
            if feedChunk.len > 0:
              sortIter.addRowsToIterator(feedChunk)
            orderTimer.stamp("stream_consume+feed")

            # Finalize and read sorted rows
            sortIter.finalizeIterator()
            var sortedRows: seq[seq[string]] = @[]
            while sortIter.hasNextSortedRow():
              let rowOpt = sortIter.nextSortedRow()
              if rowOpt.isSome:
                sortedRows.add(rowOpt.get())
            sortIter.closeSortIterator()
            orderTimer.stamp("external_sort")

            # Apply column extraction (ORDER BY may reference columns
            # not in the output SELECT list)
            var outputRows = extractRequestedColumns(sortedRows, op.obColumns,
                op.obAllColumns)
            # LIMIT already applied by StreamingSortIterator if set
            debug &"[exec_timer] obOptimization=NONE(extsort) rows={outputRows.len} {orderTimer.formatBreakdown()}"
            rowsResult(op.obColumns, outputRows)
        else:
          # Previous result is not rows - ORDER BY is invalid here
          errorResult("ORDER BY requires row results from previous operation")

    of poUpdate:
      # MVCC-aware UPDATE
      let startKey = encodeDataRowScanBound(op.upTableId, "")
      let endKey = makeDataRowScanEndKey(op.upTableId)
      # Use transaction context for consistent scan
      let res = ctx.kv.scan(startKey, endKey, 0, txnId = ctx.txnId,
          readTimestamp = ctx.readTimestamp)

      if res.isErr:
        return errorResult(&"failed to scan for update: {res.err}")

      var count = 0
      var error: string = ""
      for entry in res.val:
        try:
          var row = decodeDataRow(entry.value)
          if matchesFilterDataRow(op.upFilter, row):
            for (col, valExpr) in op.upSets:
              row[col] = evalExprDataRow(valExpr, row)
            # Use active transaction if available
            let putRes = ctx.kv.put(entry.key, encodeDataRow(row),
                txnId = ctx.txnId)
            if putRes.isErr:
              error = &"failed to update row: {putRes.err}"
              break
            inc count
        except ValueError:
          discard

      if error.len > 0:
        return errorResult(error)

      modifiedResult(count, &"UPDATE {count}")

    of poDelete:
      # MVCC-aware DELETE
      let startKey = encodeDataRowScanBound(op.delTableId, "")
      let endKey = makeDataRowScanEndKey(op.delTableId)
      # Use transaction context for consistent scan
      let res = ctx.kv.scan(startKey, endKey, 0, txnId = ctx.txnId,
          readTimestamp = ctx.readTimestamp)

      if res.isErr:
        return errorResult(&"failed to scan for delete: {res.err}")

      var count = 0
      var error: string = ""
      for entry in res.val:
        try:
          let row = decodeDataRow(entry.value)
          if matchesFilterDataRow(op.delFilter, row):
            # Use active transaction if available
            let delRes = ctx.kv.delete(entry.key, txnId = ctx.txnId)
            if delRes.isErr:
              error = &"failed to delete row: {delRes.err}"
              break
            inc count
        except ValueError:
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
      if existing.isErr or existing.val.isNone:
        errorResult(&"database '{op.udName}' does not exist")
      else:
        ExecResult(kind: erkUseDatabase, newDatabase: op.udName)

    of poUseSchema:
      let key = encodeTableKey(SYS_SCHEMAS_TABLE_ID, ctx.database & "." & op.usName)
      let existing = txnGet(ctx, key)
      if existing.isErr or existing.val.isNone:
        errorResult(&"schema '{op.usName}' does not exist in database '{ctx.database}'")
      else:
        ExecResult(kind: erkUseSchema, newSchema: op.usName)

    of poBeginTxn:
      if ctx.hasActiveTransaction:
        okResult("BEGIN (transaction already active)")
      else:
        let res = ctx.kv.beginTxn()
        if res.isOk:
          ctx.txnId = res.val.txnId
          ctx.readTimestamp = res.val.readTimestamp
          ctx.hasActiveTransaction = true
          # Also update client's active txn state if client exists
          if ctx.client != nil:
            ctx.client.activeTxnId = res.val.txnId
            ctx.client.activeReadTs = res.val.readTimestamp
          okResult("BEGIN")
        else:
          errorResult(&"failed to begin transaction: {res.err}")

    of poCommitTxn:
      if not ctx.hasActiveTransaction:
        okResult("COMMIT (no active transaction)")
      else:
        let res = ctx.kv.commitTxn(ctx.txnId)
        if res.isOk:
          ctx.hasActiveTransaction = false
          ctx.txnId = zeroTransactionID()
          ctx.readTimestamp = 0
          # Also update client's active txn state if client exists
          if ctx.client != nil:
            ctx.client.activeTxnId = zeroTransactionID()
            ctx.client.activeReadTs = 0
          okResult("COMMIT")
        else:
          errorResult(&"failed to commit transaction: {res.err}")

    of poRollbackTxn:
      if not ctx.hasActiveTransaction:
        okResult("ROLLBACK (no active transaction)")
      else:
        let res = ctx.kv.rollbackTxn(ctx.txnId)
        if res.isOk:
          ctx.hasActiveTransaction = false
          ctx.txnId = zeroTransactionID()
          ctx.readTimestamp = 0
          # Also update client's active txn state if client exists
          if ctx.client != nil:
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
