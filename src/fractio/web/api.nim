# Fractio Web Dashboard - Unified API Client
#
# This module provides all API client functions for fetching data from the backend.

import happyx
import std/[jsffi, asyncjs]
import ./js_interop
import ./store
import ./dom
import ./chart

const MaxSamples = chart.MaxSamples # Alias to avoid ambiguity

# =============================================================================
# FFI Bindings
# =============================================================================

proc fetchJson*(url: cstring): Future[JsObject]
    {.importjs: "fetch(#).then(r=>r.json())", async.}

proc fetchDelete*(url: cstring): Future[JsObject]
    {.importjs: "fetch(#,{method:'DELETE'}).then(r=>r.json())", async.}

proc fetchPost*(url: cstring, body: cstring): Future[JsObject]
    {.importjs: "fetch(#,{method:'POST',headers:{'Content-Type':'application/json'},body:#}).then(r=>r.json())", async.}

proc jsStringify*(o: JsObject): cstring
    {.importjs: "JSON.stringify(#)".}

proc numFmt*(n: float): cstring
    {.importjs: "Number(#).toLocaleString()".}

proc jsSetInterval*(fn: proc(), ms: int)
    {.importjs: "setInterval(#,#)".}

proc jsSetTimeout*(fn: proc(), ms: int)
    {.importjs: "setTimeout(#,#)".}

# =============================================================================
# Core Refresh Functions
# =============================================================================

proc fetchAllCore*() {.async.} =
  ## Fetch all core dashboard data (info, health, metrics, nodes, storage, spaces)
  try:
    setLoading("info", true)
    gInfo.set(await fetchJson("/api/info"))
    setLoading("info", false)

    setLoading("health", true)
    gHealth.set(await fetchJson("/api/health"))
    setLoading("health", false)

    setLoading("metrics", true)
    gMetrics.set(await fetchJson("/api/metrics"))
    setLoading("metrics", false)

    setLoading("nodes", true)
    gNodes.set(await fetchJson("/api/nodes"))
    setLoading("nodes", false)

    setLoading("storage", true)
    gStorage.set(await fetchJson("/api/storage"))
    setLoading("storage", false)

    setLoading("spaces", true)
    gSpaces.set(await fetchJson("/api/spaces"))
    loadedSpaces = true
    setLoading("spaces", false)

  except CatchableError:
    showError("refresh", "Failed to refresh dashboard data")
    setLoading("info", false)
    setLoading("health", false)
    setLoading("metrics", false)
    setLoading("nodes", false)
    setLoading("storage", false)
    setLoading("spaces", false)

proc refreshAll*() {.async.} =
  ## Full refresh with error handling
  await fetchAllCore()
  # Re-inject clock DOM after HappyX re-renders
  jsSetTimeout(proc() = injectClockDom(), 0)

proc startAutoRefresh*(intervalMs: int = 5000) =
  ## Start periodic auto-refresh
  jsSetInterval(proc() = discard refreshAll(), intervalMs)

# =============================================================================
# Spaces API
# =============================================================================
# Forward declaration for fetchSpaces (used by clusterJoin before definition)

proc fetchSpaces*() {.async.}

# =============================================================================
# Node Management API
# =============================================================================

proc fetchNodes*() {.async.} =
  try:
    setLoading("nodes", true)
    gNodes.set(await fetchJson("/api/nodes"))
    setLoading("nodes", false)
  except CatchableError:
    showError("nodes", "Failed to fetch nodes")
    setLoading("nodes", false)

proc joinNode*(nodeId: int, host: string, raftPort: int,
    clientPort: int) {.async.} =
  let body = newJsObject()
  body.nodeId = nodeId
  body.host = cstring(host)
  body.raftPort = raftPort
  body.clientPort = clientPort
  try:
    setLoading("joinNode", true)
    let resp = await fetchPost("/api/nodes", jsStringify(body))
    let success = safeInt(resp, "success")
    if success != 0:
      showSuccess($safeStr(resp, "message"))
      await fetchNodes()
    else:
      showError("joinNode", $safeStr(resp, "message") & $safeStr(resp, "error"))
    setLoading("joinNode", false)
  except CatchableError:
    showError("joinNode", "Failed to join node")
    setLoading("joinNode", false)

proc removeNode*(nodeId: int) {.async.} =
  try:
    setLoading("removeNode", true)
    let resp = await fetchDelete(cstring("/api/nodes/" & $nodeId))
    let success = safeInt(resp, "success")
    if success != 0:
      showSuccess("Node " & $nodeId & " removed")
      await fetchNodes()
    else:
      showError("removeNode", $safeStr(resp, "message"))
    setLoading("removeNode", false)
  except CatchableError:
    showError("removeNode", "Failed to remove node")
    setLoading("removeNode", false)

proc clusterJoin*(peerNodeId: int, peerHost: string, peerRaftPort: int,
                  peerClientPort: int, peerWebPort: int) {.async.} =
  let body = newJsObject()
  body.nodeId = peerNodeId
  body.host = cstring(peerHost)
  body.raftPort = peerRaftPort
  body.clientPort = peerClientPort
  body.webPort = peerWebPort
  try:
    setLoading("clusterJoin", true)
    let resp = await fetchPost("/api/cluster/join", jsStringify(body))
    let success = safeInt(resp, "success")
    if success != 0:
      showSuccess("Node " & $peerNodeId & " joined cluster")
      await fetchNodes()
      await fetchSpaces()
    else:
      showError("clusterJoin", $safeStr(resp, "error"))
    setLoading("clusterJoin", false)
  except CatchableError:
    showError("clusterJoin", "Failed to join cluster")
    setLoading("clusterJoin", false)

proc triggerRebalance*() {.async.} =
  try:
    setLoading("rebalance", true)
    let resp = await fetchPost("/api/rebalance", "{}")
    let success = safeInt(resp, "success")
    if success != 0:
      showSuccess("Rebalance initiated")
      await fetchSpaces()
    else:
      showError("rebalance", $safeStr(resp, "error"))
    setLoading("rebalance", false)
  except CatchableError:
    showError("rebalance", "Failed to trigger rebalance")
    setLoading("rebalance", false)

# =============================================================================
# SQL Query API
# =============================================================================

proc sqlQuery*(sql: string, db: string = "default",
    schema: string = "public"): Future[JsObject] {.async.} =
  let body = newJsObject()
  body.sql = cstring(sql)
  body.database = cstring(db)
  body.schema = cstring(schema)
  return await fetchPost("/api/sql", jsStringify(body))

proc executeSql*(sql: string, db: string, schema: string) {.async.} =
  try:
    setLoading("sqlQuery", true)
    let resp = await sqlQuery(sql, db, schema)

    # Add to history
    var hist = gSqlHistory.get()
    hist.add(sql)
    if hist.len > 50: # Keep last 50 queries
      hist.delete(0)
    gSqlHistory.set(hist)

    gSqlResult.set(resp)

    let kind = $safeStr(resp, "kind")
    if kind == "error":
      showError("sql", $safeStr(resp, "error"))
    elif kind == "rows":
      showInfo("Query returned " & $jsArrayLen(resp.rows) & " rows")
    elif kind == "modified":
      showSuccess("Modified " & $safeInt(resp, "count") & " rows")

    setLoading("sqlQuery", false)
  except CatchableError:
    showError("sql", "Failed to execute SQL query")
    setLoading("sqlQuery", false)

# =============================================================================
# Data Browser API
# =============================================================================

proc fetchDatabases*() {.async.} =
  if loadedDatabases or isLoading("databases"):
    return
  setLoading("databases", true)
  try:
    let resp = await sqlQuery("SHOW DATABASES")
    loadedDatabases = true
    if safeStr(resp, "kind") == "rows":
      var dbs: seq[string]
      let rows = resp.rows
      let rowLen = safeInt(rows, "length")
      for i in 0 ..< rowLen:
        let row = rows[i]
        dbs.add($safeStr(row, "database_name"))
      gDatabases.set(dbs)
    setLoading("databases", false)
  except CatchableError:
    showError("databases", "Failed to fetch databases")
    setLoading("databases", false)

proc fetchSchemas*(db: string) {.async.} =
  let key = db
  if loadedSchemasKey == key or isLoading("schemas"):
    return
  setLoading("schemas", true)
  try:
    let resp = await sqlQuery("SHOW SCHEMAS IN " & db, db)
    loadedSchemasKey = key
    if safeStr(resp, "kind") == "rows":
      var schemas: seq[string]
      let rows = resp.rows
      let rowLen = safeInt(rows, "length")
      for i in 0 ..< rowLen:
        let row = rows[i]
        schemas.add($safeStr(row, "schema_name"))
      gSchemas.set(schemas)
    setLoading("schemas", false)
  except CatchableError:
    showError("schemas", "Failed to fetch schemas")
    setLoading("schemas", false)

proc fetchTables*(db: string, schema: string) {.async.} =
  let key = db & "." & schema
  if loadedTablesKey == key or isLoading("tables"):
    return
  setLoading("tables", true)
  try:
    let resp = await sqlQuery("SHOW TABLES IN " & db & "." & schema, db, schema)
    loadedTablesKey = key
    if safeStr(resp, "kind") == "rows":
      var tables: seq[string]
      let rows = resp.rows
      let rowLen = safeInt(rows, "length")
      for i in 0 ..< rowLen:
        let row = rows[i]
        tables.add($safeStr(row, "table_name"))
      gTables.set(tables)
    setLoading("tables", false)
  except CatchableError:
    showError("tables", "Failed to fetch tables")
    setLoading("tables", false)

proc fetchTableData*(db: string, schema: string, table: string,
                     limit: int = 100, offset: int = 0,
                         search: string = "") {.async.} =
  let key = db & "." & schema & "." & table & "." & $limit & "." & $offset &
      "." & search
  if loadedTableDataKey == key or isLoading("tableData"):
    return
  setLoading("tableData", true)
  try:
    var sql = "SELECT * FROM " & table
    if search.len > 0:
      # Basic search - we'd need schema info for proper WHERE clause
      sql = sql # TODO: Add proper search clause
    sql = sql & " LIMIT " & $limit & " OFFSET " & $offset
    let resp = await sqlQuery(sql, db, schema)
    loadedTableDataKey = key
    gTableData.set(resp)

    # Update pagination total
    if safeStr(resp, "kind") == "rows":
      var p = gTablePagination.get()
      p.totalRows = jsArrayLen(resp.rows) # Note: this is rows in this page, not total
      gTablePagination.set(p)

    setLoading("tableData", false)
  except CatchableError:
    showError("tableData", "Failed to fetch table data")
    setLoading("tableData", false)

# =============================================================================
# System Tables API
# =============================================================================

proc fetchSystemTables*() {.async.} =
  if loadedSysTables or isLoading("sysTables"):
    return
  setLoading("sysTables", true)
  try:
    let resp = await fetchJson("/api/sql/system-tables")
    loadedSysTables = true
    gSysTables.set(resp)
    setLoading("sysTables", false)
  except CatchableError:
    showError("sysTables", "Failed to fetch system tables")
    setLoading("sysTables", false)

proc fetchSystemTableData*(tableId: int, tableName: string) {.async.} =
  let key = tableName
  if loadedSysTableDataKey == key or isLoading("sysTableData"):
    return
  setLoading("sysTableData", true)
  try:
    let resp = await fetchJson(cstring("/api/sql/system-table/" & $tableId))
    loadedSysTableDataKey = key
    gSysTableData.set(resp)

    # Update pagination
    if jsArrayLen(resp.rows) > 0:
      var p = gTablePagination.get()
      p.totalRows = jsArrayLen(resp.rows)
      gTablePagination.set(p)

    setLoading("sysTableData", false)
  except CatchableError:
    showError("sysTableData", "Failed to fetch system table data")
    setLoading("sysTableData", false)

proc sysTableIdByName*(name: string): int =
  ## Look up system table ID by name from gSysTables cache.
  let arr = gSysTables.get()
  let arrLen = jsArrayLen(arr)
  for i in 0 ..< arrLen:
    let st = arr[i]
    if $safeStr(st, "name") == name:
      return safeInt(st, "id")
  return -1

# =============================================================================
# Spaces API
# =============================================================================

proc fetchSpaces*() {.async.} =
  if loadedSpaces or isLoading("spaces"):
    return
  setLoading("spaces", true)
  try:
    let resp = await fetchJson("/api/spaces")
    loadedSpaces = true
    gSpaces.set(resp)
    setLoading("spaces", false)
  except CatchableError:
    showError("spaces", "Failed to fetch spaces")
    setLoading("spaces", false)

# =============================================================================
# WebSocket for Clock Drift
# =============================================================================

proc connectDriftWs*() =
  let host = jsLocation()
  let url = cstring("ws://") & host & cstring("/ws/drift")
  let ws = jsWsNew(url)
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

# =============================================================================
# Export Functions
# =============================================================================

proc exportToJson*(data: JsObject): cstring =
  jsStringify(data)

proc exportToCsv*(columns: JsObject, rows: JsObject): cstring =
  {.emit: """
  var cols = [];
  var colLen = `columns`.length || 0;
  for (var i = 0; i < colLen; i++) {
    cols.push(String(`columns`[i] || ''));
  }
  var csv = cols.join(',') + '\\n';
  var rowLen = `rows`.length || 0;
  for (var i = 0; i < rowLen; i++) {
    var row = [];
    var r = `rows`[i] || {};
    for (var j = 0; j < cols.length; j++) {
      var val = r[cols[j]] || '';
      // Escape quotes and wrap in quotes if contains comma/quote/newline
      if (val.includes(',') || val.includes('"') || val.includes('\\n')) {
        val = '"' + val.replace(/"/g, '" + '"') + '"';
      }
      row.push(val);
    }
    csv += row.join(',') + '\\n';
  }
  `result` = csv;
  """.}
  result

proc downloadFile*(content: cstring, filename: cstring, mimeType: cstring) =
  {.emit: """
  var blob = new Blob([`content`], {type: `mimeType`});
  var url = URL.createObjectURL(blob);
  var a = document.createElement('a');
  a.href = url;
  a.download = `filename`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
  """.}

proc downloadJsonExport*(data: JsObject, filename: string) =
  let jsonStr = exportToJson(data)
  downloadFile(jsonStr, cstring(filename), "application/json")

proc downloadCsvExport*(columns: JsObject, rows: JsObject, filename: string) =
  let csvStr = exportToCsv(columns, rows)
  downloadFile(csvStr, cstring(filename), "text/csv")

# =============================================================================
# Trigger Wrappers (for HappyX DSL compatibility)
# =============================================================================

proc triggerLoadDatabases*(): int =
  if loadedDatabases or isLoading("databases"):
    return 0
  jsSetTimeout(proc() = discard fetchDatabases(), 0)
  0

proc triggerLoadSchemas*(db: string): int =
  if loadedSchemasKey == db or isLoading("schemas"):
    return 0
  jsSetTimeout(proc() = discard fetchSchemas(db), 0)
  0

proc triggerLoadTables*(db: string, schema: string): int =
  let key = db & "." & schema
  if loadedTablesKey == key or isLoading("tables"):
    return 0
  jsSetTimeout(proc() = discard fetchTables(db, schema), 0)
  0

proc triggerLoadTableData*(db: string, schema: string, table: string): int =
  let p = gTablePagination.get()
  jsSetTimeout(proc() = discard fetchTableData(db, schema, table, p.pageSize, (
      p.page - 1) * p.pageSize, p.searchQuery), 0)
  0

proc triggerLoadSystemTables*(): int =
  if loadedSysTables or isLoading("sysTables"):
    return 0
  jsSetTimeout(proc() = discard fetchSystemTables(), 0)
  0

proc triggerLoadSystemTableData*(tableId: int, tableName: string): int =
  if loadedSysTableDataKey == tableName or isLoading("sysTableData"):
    return 0
  jsSetTimeout(proc() = discard fetchSystemTableData(tableId, tableName), 0)
  0
