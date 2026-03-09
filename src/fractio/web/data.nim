# Data fetching and WebSocket drift connection.

import happyx
import std/[jsffi, asyncjs]
import ./js_interop
import ./chart
import ./state

var gDriftWs* {.global.}: JsObject = nil

proc doRefresh*() {.async.} =
  try:
    gInfo.set(await fetchJson("/api/info"))
    gHealth.set(await fetchJson("/api/health"))
    gMetrics.set(await fetchJson("/api/metrics"))
    gNodes.set(await fetchJson("/api/nodes"))
    # Re-inject clock DOM after HappyX re-renders wipe #drift-chart
    jsSetTimeout(proc() = injectClockDom(), 0)
  except:
    discard

proc doRemoveNode*(nodeId: int) {.async.} =
  discard await fetchDelete(cstring("/api/nodes/" & $nodeId))
  gNodes.set(await fetchJson("/api/nodes"))

proc doJoinNode*() {.async.} =
  let body = newJsObject()
  body.nodeId     = jsParseInt(getInputVal("join-id"))
  body.host       = getInputVal("join-host")
  body.raftPort   = jsParseInt(getInputVal("join-raft"))
  body.clientPort = jsParseInt(getInputVal("join-client"))
  let resp = await fetchPost("/api/nodes", jsStringify(body))
  gMsgOk.set(safeInt(resp, "success") != 0)
  gMsg.set($safeStr(resp, "message"))
  if gMsgOk:
    clearInput("join-id")
    clearInput("join-host")
    clearInput("join-raft")
    clearInput("join-client")
    gNodes.set(await fetchJson("/api/nodes"))

proc sqlQuery(sql: string, db: string = "default",
    schema: string = "public"): Future[JsObject] {.async.} =
  let body = newJsObject()
  body.sql = cstring(sql)
  body.database = cstring(db)
  body.schema = cstring(schema)
  return await fetchPost("/api/sql", jsStringify(body))

# Fetch guards: fetching* = request in-flight, loaded* = result received.
# Triggers skip if loaded key matches OR fetch is in-flight for same key.
# This prevents re-render cascades (even for 0-row results).
var
  fetchingDatabases {.global.}: bool = false
  fetchingSchemas {.global.}: bool = false
  fetchingSchemasKey {.global.}: string = ""
  fetchingTables {.global.}: bool = false
  fetchingTablesKey {.global.}: string = ""
  fetchingTableData {.global.}: bool = false
  fetchingTableDataKey {.global.}: string = ""
  fetchingSysTables {.global.}: bool = false
  fetchingSysTableData {.global.}: bool = false
  fetchingSysTableDataKey {.global.}: string = ""
  loadedDatabases* {.global.}: bool = false
  loadedSchemasKey* {.global.}: string = ""
  loadedTablesKey* {.global.}: string = ""
  loadedTableDataKey* {.global.}: string = ""
  loadedSysTables* {.global.}: bool = false
  loadedSysTableDataKey* {.global.}: string = ""

proc doLoadDatabases*() {.async.} =
  let resp = await sqlQuery("SHOW DATABASES")
  fetchingDatabases = false
  loadedDatabases = true
  if safeStr(resp, "kind") == "rows":
    var dbs: seq[string]
    let rows = resp.rows
    let rowLen = safeInt(rows, "length")
    for i in 0 ..< rowLen:
      let row = rows[i]
      dbs.add($safeStr(row, "database_name"))
    gDatabases.set(dbs)

proc doLoadSchemas*(db: string) {.async.} =
  let resp = await sqlQuery("SHOW SCHEMAS IN " & db, db)
  fetchingSchemas = false
  loadedSchemasKey = fetchingSchemasKey
  if safeStr(resp, "kind") == "rows":
    var schemas: seq[string]
    let rows = resp.rows
    let rowLen = safeInt(rows, "length")
    for i in 0 ..< rowLen:
      let row = rows[i]
      schemas.add($safeStr(row, "schema_name"))
    gSchemas.set(schemas)

proc doLoadTables*(db, schema: string) {.async.} =
  let resp = await sqlQuery("SHOW TABLES IN " & db & "." & schema, db, schema)
  fetchingTables = false
  loadedTablesKey = fetchingTablesKey
  if safeStr(resp, "kind") == "rows":
    var tables: seq[string]
    let rows = resp.rows
    let rowLen = safeInt(rows, "length")
    for i in 0 ..< rowLen:
      let row = rows[i]
      tables.add($safeStr(row, "table_name"))
    gTables.set(tables)

proc doLoadTableData*(db, schema, table: string) {.async.} =
  let resp = await sqlQuery("SELECT * FROM " & table & " LIMIT 100", db, schema)
  fetchingTableData = false
  loadedTableDataKey = fetchingTableDataKey
  gTableData.set(resp)

proc doLoadSystemTables*() {.async.} =
  let resp = await fetchJson("/api/sql/system-tables")
  fetchingSysTables = false
  loadedSysTables = true
  gSysTables.set(resp)

proc doLoadSystemTableData*(tableId: int, tableName: string) {.async.} =
  let resp = await fetchJson(cstring("/api/sql/system-table/" & $tableId))
  fetchingSysTableData = false
  loadedSysTableDataKey = fetchingSysTableDataKey
  gSysTableData.set(resp)

proc sysTableIdByName*(name: string): int =
  ## Look up system table ID by name from gSysTables cache.
  let arr = gSysTables.get()
  let arrLen = jsArrayLen(arr)
  for i in 0 ..< arrLen:
    let st = arr[i]
    if $safeStr(st, "name") == name:
      return safeInt(st, "id")
  return -1

# Fire-and-forget wrappers — return int so they can be used in `let` bindings
# inside HappyX buildHtml DSL without producing text nodes.
#
# Guards prevent re-fetching: each trigger checks if data is already loaded
# OR a fetch is in-flight. This is critical because .set() on State vars
# triggers HappyX re-renders which re-execute the route body.

proc triggerLoadDatabases*(): int =
  if loadedDatabases or fetchingDatabases:
    return 0
  fetchingDatabases = true
  jsSetTimeout(proc() = discard doLoadDatabases(), 0)
  0

proc triggerLoadSchemas*(db: string): int =
  if (loadedSchemasKey == db) or (fetchingSchemas and fetchingSchemasKey == db):
    return 0
  fetchingSchemas = true
  fetchingSchemasKey = db
  let d = db
  jsSetTimeout(proc() = discard doLoadSchemas(d), 0)
  0

proc triggerLoadTables*(db, schema: string): int =
  let key = db & "." & schema
  if (loadedTablesKey == key) or (fetchingTables and fetchingTablesKey == key):
    return 0
  fetchingTables = true
  fetchingTablesKey = key
  let d = db
  let s = schema
  jsSetTimeout(proc() = discard doLoadTables(d, s), 0)
  0

proc triggerLoadTableData*(db, schema, table: string): int =
  let key = db & "." & schema & "." & table
  if (loadedTableDataKey == key) or (fetchingTableData and fetchingTableDataKey == key):
    return 0
  fetchingTableData = true
  fetchingTableDataKey = key
  let d = db
  let s = schema
  let t = table
  jsSetTimeout(proc() = discard doLoadTableData(d, s, t), 0)
  0

proc triggerLoadSystemTables*(): int =
  if loadedSysTables or fetchingSysTables:
    return 0
  fetchingSysTables = true
  jsSetTimeout(proc() = discard doLoadSystemTables(), 0)
  0

proc triggerLoadSystemTableData*(tableId: int, tableName: string): int =
  if (loadedSysTableDataKey == tableName) or (fetchingSysTableData and fetchingSysTableDataKey == tableName):
    return 0
  fetchingSysTableData = true
  fetchingSysTableDataKey = tableName
  let tid = tableId
  let tn = tableName
  jsSetTimeout(proc() = discard doLoadSystemTableData(tid, tn), 0)
  0

proc connectDriftWs*() =
  let host = jsLocation()
  let url  = cstring("ws://") & host & cstring("/ws/drift")
  let ws   = jsWsNew(url)
  gDriftWs = ws

  jsWsOnOpen(ws, proc() =
    gDriftWsStr = "live"
    injectClockDom()
  )

  jsWsOnMessage(ws, proc(ev: JsObject) =
    let data = jsEvData(ev)
    try:
      let msg = jsParseJsonStr(data)
      let offsetUs = safeFloat(msg, "offsetUs")
      gDriftSamples.add(offsetUs)
      if gDriftSamples.len > MaxSamples:
        gDriftSamples.delete(0)
      let signChar = if offsetUs >= 0.0: "+" else: ""
      gDriftLastStr = $signChar & $int(offsetUs) & " µs"
      injectClockDom()
    except:
      discard
  )

  jsWsOnClose(ws, proc() =
    gDriftWsStr = "reconnecting…"
    injectClockDom()
    jsSetTimeout(proc() = connectDriftWs(), 2000)
  )
