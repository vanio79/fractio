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

proc doLoadDatabases*() {.async.} =
  let resp = await sqlQuery("SHOW DATABASES")
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
  gTableData.set(resp)

proc doLoadSystemTables*() {.async.} =
  let resp = await fetchJson("/api/sql/system-tables")
  gSysTables.set(resp)

proc doLoadSystemTableData*(tableId: int, tableName: string) {.async.} =
  let resp = await fetchJson(cstring("/api/sql/system-table/" & $tableId))
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
proc triggerLoadDatabases*(): int =
  jsSetTimeout(proc() = discard doLoadDatabases(), 0)
  0

proc triggerLoadSchemas*(db: string): int =
  let d = db
  jsSetTimeout(proc() = discard doLoadSchemas(d), 0)
  0

proc triggerLoadTables*(db, schema: string): int =
  let d = db
  let s = schema
  jsSetTimeout(proc() = discard doLoadTables(d, s), 0)
  0

proc triggerLoadTableData*(db, schema, table: string): int =
  let d = db
  let s = schema
  let t = table
  jsSetTimeout(proc() = discard doLoadTableData(d, s, t), 0)
  0

proc triggerLoadSystemTables*(): int =
  jsSetTimeout(proc() = discard doLoadSystemTables(), 0)
  0

proc triggerLoadSystemTableData*(tableId: int, tableName: string): int =
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
