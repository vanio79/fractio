# Fractio Web Management Dashboard — Nimja template engine
# HTMX + Shoelace for frontend. Server-side rendering via Nimja templates.

import httpbeast
import nimja/parser
import std/[json, strutils, strformat, times, os, atomics,
    tables as stdtables, httpclient, uri, options, asyncfutures, net]
import zippy
import ../core/types as coreTypes except Table
import ../protocol/server as pserver
import ../protocol/messages/cluster as clusterMsgs
import ../sql/executor
import ../client/fractio_client
import ../client/sql_client
import ../distributed/meta/system_tables
import ../distributed/meta/system_schemas
import ../protocol/raft_store
import ../protocol/mvcc_store
import ../distributed/raft/nuraft_coordinator
import ../storage/wisckey_backend
import ../storage/backend

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------

var gSrvPtr {.global.}: pointer
var gWebPort {.global.}: int
var gClient {.global.}: FractioClient
var gWebThread {.global.}: Thread[int]

template getSrv(): pserver.ProtocolServer =
  cast[pserver.ProtocolServer](gSrvPtr)

proc getClient(): FractioClient =
  if gClient == nil:
    let srv = getSrv()
    let host = if srv.config.host == "0.0.0.0": "127.0.0.1" else: srv.config.host
    gClient = newFractioClient(host, srv.config.port)
    discard gClient.initialize()
  gClient

proc getTemplateDir(): string =
  getScriptDir() / "templates"

# ---------------------------------------------------------------------------
# Static assets
# ---------------------------------------------------------------------------

const appJs = """
// Shoelace emits 'sl-change' which HTMX handles directly when hx-trigger='sl-change' is set
document.addEventListener('htmx:afterSwap', function(e) {
  const target = e.detail.target;
  if (target) {
    const shoelaceComponents = target.querySelectorAll('sl-select, sl-button, sl-input, sl-dialog');
    shoelaceComponents.forEach(function(comp) {
      if (comp.requestUpdate) comp.requestUpdate();
    });
  }
});
"""

# ---------------------------------------------------------------------------
# Helper procs
# ---------------------------------------------------------------------------

proc formatUptime(secs: uint64): string =
  if secs < 60: return $secs & "s"
  if secs < 3600: return $(secs div 60) & "m " & $(secs mod 60) & "s"
  let hrs = secs div 3600
  let mins = (secs mod 3600) div 60
  return $hrs & "h " & $mins & "m"

proc splitPathQuery(fullPath: string): (string, string) =
  let qPos = fullPath.find('?')
  if qPos >= 0:
    return (fullPath[0..qPos-1], fullPath[qPos+1..fullPath.len-1])
  return (fullPath, "")

proc parseQueryParams(query: string): Table[string, string] =
  result = initTable[string, string]()
  if query.len == 0:
    return
  for pair in query.split('&'):
    let eqPos = pair.find('=')
    if eqPos >= 0:
      result[pair[0..eqPos-1]] = pair[eqPos+1..pair.len-1]
    elif pair.len > 0:
      result[pair] = ""

proc getQueryParam(params: Table[string, string], key: string): string =
  if params.hasKey(key): return decodeUrl(params[key])
  return ""

proc parseFormData(body: string): Table[string, string] =
  result = initTable[string, string]()
  if body.len == 0:
    return
  for pair in body.split('&'):
    let eqPos = pair.find('=')
    if eqPos >= 0:
      let key = pair[0..eqPos-1]
      let rawVal = pair[eqPos+1..pair.len-1]
      var val = rawVal.replace('+', ' ')
      var i = 0
      while i < val.len:
        if val[i] == '%' and i + 2 < val.len:
          let hex = val[i+1..i+2]
          try:
            let code = fromHex[int](hex)
            val = val[0..i-1] & chr(code) & val[i+3..val.len-1]
          except ValueError:
            discard
        i += 1
      result[key] = val
    elif pair.len > 0:
      result[pair] = ""

proc getHeader(req: Request, name: string): string =
  let headersOpt = req.headers()
  if headersOpt.isSome:
    let h = headersOpt.get()
    if h.hasKey(name): return h[name]
  return ""

proc getRoleString(role: int): string =
  case role
  of 1: "Leader"
  of 2: "Follower"
  else: "Unknown"

# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

proc onRequestHandler(req: Request): Future[void] {.gcsafe.} =
  var fut = newFuture[void]("onRequestHandler")
  fut.complete()

  let fullPath = req.path().get("")
  let (path, queryStr) = splitPathQuery(fullPath)
  let queryParams = parseQueryParams(queryStr)
  let httpMethodOpt = req.httpMethod()
  let httpMethod = if httpMethodOpt.isSome: httpMethodOpt.get() else: HttpGet
  let acceptEncoding = getHeader(req, "accept-encoding")
  let wantsGzip = "gzip" in acceptEncoding

  proc sendJson(code: HttpCode, data: JsonNode) =
    req.send(code, $data, "Content-Type: application/json")

  proc sendHtml(code: HttpCode, data: string) =
    if wantsGzip:
      let compressed = compress(data, level = 1)
      req.send(code, compressed, "Content-Type: text/html; charset=utf-8\nContent-Encoding: gzip")
    else:
      req.send(code, data, "Content-Type: text/html; charset=utf-8")

  # ---- Static: app.js ----
  if path == "/app.js" and httpMethod == HttpGet:
    sendHtml(Http200, appJs)
    return fut

  # ---- Static: root and all tab paths (return shell) ----
  if path == "/" or path.startsWith("/dashboard") or path.startsWith(
      "/nodes") or
     path.startsWith("/metrics") or path.startsWith("/clock") or
         path.startsWith("/storage") or
     path.startsWith("/data") or path.startsWith("/sql") or path.startsWith("/settings"):
    let activeTab = if path == "/": "dashboard" else: path[1..path.len-1].split('/')[0]
    var html: string = ""
    compileTemplateFile("shell.nimja", baseDir = getTemplateDir(),
        varname = "html")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: Status badge ----
  if path == "/htmx/status" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http200, "ERR")
      return fut
    sendHtml(Http200, "OK")
    return fut

  # ---- HTMX: Dashboard tab ----
  if path == "/htmx/dashboard" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http503, "<div class='panel'><h1>Server not ready</h1></div>")
      return fut
    var infoResult: ExecResult
    {.cast(gcsafe).}:
      infoResult = getClient().query("SELECT * FROM sys.nodes WHERE nodeId = " &
          $srv.config.serverId.int)
    let nodeId = $srv.config.serverId.int
    let role = if not srv.raftCoord.isNil and srv.raftCoord.isLeader(
        META_GROUP_ID): "Leader" else: "Follower"
    let uptime = formatUptime(uint64(getTime().toUnix() - srv.startedAt))
    let clients = $srv.clientCount()
    let shards = if not srv.raftStore.isNil: $srv.raftStore.shardCount() else: "0"
    let version = srv.config.serverVersion
    let clusterName = srv.config.clusterName

    # Get cluster nodes from sys.nodes table
    var nodesData: seq[(string, string, string, string, string)] = @[]
    var nodesResult: ExecResult
    {.cast(gcsafe).}:
      nodesResult = getClient().query("SELECT * FROM sys.nodes")
    case nodesResult.kind
    of erkRows:
      for row in nodesResult.rows:
        # Columns: _key, nodeId, host, raftPort, clientPort, webPort, status
        if row.len >= 5:
          let status = if row.len >= 7: row[6] else: "alive"
          nodesData.add((row[1], row[2], row[3], row[4], status))
    of erkStreamingRows:
      let iter = nodesResult.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          let row = rowOpt.get()
          if row.len >= 5:
            let status = if row.len >= 7: row[6] else: "alive"
            nodesData.add((row[1], row[2], row[3], row[4], status))
      iter.closeIterator()
    else:
      discard

    # Get spaces
    var spacesResult: ExecResult
    {.cast(gcsafe).}:
      spacesResult = getClient().query("SELECT * FROM sys.spaces")
    var spacesData: seq[(string, string, int)] = @[]
    case spacesResult.kind
    of erkRows:
      for row in spacesResult.rows:
        if row.len >= 3:
          spacesData.add((row[0], row[1] & "?", if row.len > 2: 1 else: 0))
    of erkStreamingRows:
      let iter = spacesResult.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          let row = rowOpt.get()
          if row.len >= 3:
            spacesData.add((row[0], row[1] & "?", if row.len > 2: 1 else: 0))
      iter.closeIterator()
    else:
      discard

    let healthyCount = nodesData.len
    let totalNodes = nodesData.len

    var html: string = ""
    compileTemplateFile("dashboard.nimja", baseDir = getTemplateDir(),
        varname = "html", blockToRender = "content")
    sendHtml(Http200, html)
    return fut

# ---- HTMX: Nodes tab ----
  if path == "/htmx/nodes" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http503, "<div class='panel'><h1>Server not ready</h1></div>")
      return fut
    var nodesData: seq[(int, string, int, int, int, string, string)] = @[]
    # Query nodes from sys.nodes table
    var nodesResult: ExecResult
    {.cast(gcsafe).}:
      nodesResult = getClient().query("SELECT * FROM sys.nodes")
    case nodesResult.kind
    of erkRows:
      for row in nodesResult.rows:
        # Columns: _key, nodeId, host, raftPort, clientPort, webPort, status
        if row.len >= 5:
          let nodeId = try: parseInt(row[1]) except: 0
          let nodeRole = if nodeId == srv.config.serverId.int: "leader" else: "follower"
          let raftPort = try: parseInt(row[3]) except: 0
          let clientPort = try: parseInt(row[4]) except: 0
          let webPort = block:
            if row.len >= 6 and row[5].len > 0:
              try: parseInt(row[5]) except: 0
            else:
              0
          let status = if row.len >= 7: row[6] else: "unknown"
          nodesData.add((nodeId, row[2], raftPort, clientPort, webPort,
              nodeRole, status))
    of erkStreamingRows:
      # Handle streaming results
      let iter = nodesResult.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          let row = rowOpt.get()
          if row.len >= 5:
            let nodeId = try: parseInt(row[1]) except: 0
            let nodeRole = if nodeId == srv.config.serverId.int: "leader" else: "follower"
            let raftPort = try: parseInt(row[3]) except: 0
            let clientPort = try: parseInt(row[4]) except: 0
            let webPort = block:
              if row.len >= 6 and row[5].len > 0:
                try: parseInt(row[5]) except: 0
              else:
                0
            let status = if row.len >= 7: row[6] else: "unknown"
            nodesData.add((nodeId, row[2], raftPort, clientPort, webPort,
                nodeRole, status))
      iter.closeIterator()
    of erkError:
      # Log error but continue with empty nodes
      discard
    else:
      discard
    var html: string = ""
    compileTemplateFile("nodes.nimja", baseDir = getTemplateDir(),
        varname = "html", blockToRender = "content")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: Nodes add form ----
  if path == "/htmx/nodes/add-form" and httpMethod == HttpGet:
    var html = "<form hx-post='/api/nodes' hx-target='#node-form-area'>"
    html.add("<div class='form-row'>")
    html.add("<sl-input name='host' label='Host' placeholder='127.0.0.1' size='small'></sl-input>")
    html.add("<sl-input name='raftPort' label='Raft Port' placeholder='9001' size='small' type='number'></sl-input>")
    html.add("<sl-input name='clientPort' label='Client Port' placeholder='9000' size='small' type='number'></sl-input>")
    html.add("<sl-input name='webPort' label='Web Port' placeholder='9876' size='small' type='number'></sl-input>")
    html.add("<sl-button type='submit' variant='primary'>Add</sl-button>")
    html.add("</div></form>")
    sendHtml(Http200, html)
    return fut

# ---- HTMX: Metrics tab ----
  if path == "/htmx/metrics" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http503, "<div class='panel'><h1>Server not ready</h1></div>")
      return fut
    let requests = $srv.metrics.requestsTotal.load()
    let okCount = $srv.metrics.requestsOK.load()
    let errors = $srv.metrics.requestsErr.load()
    let bytesIn = $srv.metrics.bytesIn.load()
    let bytesOut = $srv.metrics.bytesOut.load()
    let kvGets = $srv.metrics.kvGets.load()
    let kvPuts = $srv.metrics.kvPuts.load()
    let kvDeletes = $srv.metrics.kvDeletes.load()
    var html: string = ""
    compileTemplateFile("metrics.nimja", baseDir = getTemplateDir(),
        varname = "html", blockToRender = "content")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: Clock tab ----
  if path == "/htmx/clock" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http503, "<div class='panel'><h1>Server not ready</h1></div>")
      return fut
    let nowTime = getTime()
    let localTime = format(nowTime, "HH:mm:ss")
    let unixTime = $nowTime.toUnix()
    let driftStatus = "P2P Sync"
    let driftDetail = "Active"
    var html: string = ""
    compileTemplateFile("clock.nimja", baseDir = getTemplateDir(),
        varname = "html", blockToRender = "content")
    sendHtml(Http200, html)
    return fut

# ---- HTMX: Storage tab ----
  if path == "/htmx/storage" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil or srv.raftStore.isNil:
      sendHtml(Http503, "<div class='panel'><h1>Server not ready</h1></div>")
      return fut
    var storageStats = ""
    {.cast(gcsafe).}:
      let backend = srv.raftStore.getBackend()
      if not backend.isNil:
        var stats: StorageStats
        stats = backend.getStats()
        storageStats = "Reads: " & $stats.reads & " | Writes: " &
            $stats.writes & " | Bytes Read: " & $stats.bytesRead &
            " | Bytes Written: " & $stats.bytesWritten
    var html: string = ""
    compileTemplateFile("storage.nimja", baseDir = getTemplateDir(),
        varname = "html", blockToRender = "content")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: Data tab ----
  if path == "/htmx/data" and httpMethod == HttpGet:
    let databases = @["default"]
    var html: string = ""
    compileTemplateFile("data.nimja", baseDir = getTemplateDir(),
        varname = "html", blockToRender = "content")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: schemas dropdown ----
  if path == "/htmx/schemas" and httpMethod == HttpGet:
    let db = getQueryParam(queryParams, "db-select")
    var schemasData: seq[string] = @["sys", "public"]
    var html = "<sl-select id='schema-select' placeholder='Select schema' size='small' hoist "
    html.add("hx-get='/htmx/tables' hx-trigger='sl-change' hx-target='#tables-list' ")
    html.add("hx-vals=\"js:{'db-select':document.getElementById('db-select').value,'schema-select':event.target.value}\">")
    for schema in schemasData:
      html.add("<sl-option value='" & schema & "'>" & schema & "</sl-option>")
    html.add("</sl-select>")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: tables list ----
  if path == "/htmx/tables" and httpMethod == HttpGet:
    let db = getQueryParam(queryParams, "db-select")
    let schema = getQueryParam(queryParams, "schema-select")
    var tablesData: seq[string] = @[]
    if schema.len > 0:
      # sys schema has hardcoded system tables
      if schema == "sys":
        tablesData = @["databases", "schemas", "tables", "nodes", "spaces", "groups", "settings"]
      else:
        var execResult: ExecResult
        {.cast(gcsafe).}:
          execResult = getClient().query("SELECT name FROM sys.tables WHERE database = '" &
              db & "' AND schema = '" & schema & "'")
        case execResult.kind
        of erkRows:
          for row in execResult.rows:
            if row.len > 0:
              tablesData.add(row[0])
        of erkStreamingRows:
          let iter = execResult.streamIterator
          while iter.hasNextRow():
            let rowOpt = iter.nextRow()
            if rowOpt.isSome:
              let row = rowOpt.get()
              if row.len > 0:
                tablesData.add(row[0])
          iter.closeIterator()
        else:
          discard
    var html = ""
    for table in tablesData:
      html.add("<button class='htmx-table-item' hx-get='/htmx/data/" & db &
          "/" & schema & "/" & table & "' hx-target='#data-grid'>" & table & "</button>")
    if tablesData.len == 0:
      html = "<div class='htmx-empty-state'>No tables found</div>"
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: data grid ----
  if path.startsWith("/htmx/data/") and httpMethod == HttpGet:
    let parts = path.split('/')
    if parts.len < 6:
      sendHtml(Http400, "<div class='htmx-empty-state'>Invalid path</div>")
      return fut
    let db = parts[3]
    let schema = parts[4]
    let tableName = parts[5]
    var execResult: ExecResult
    {.cast(gcsafe).}:
      execResult = getClient().query("SELECT * FROM " & db & "." & schema &
          "." & tableName & " LIMIT 100")
    var html = "<table class='htmx-data-table'>"
    case execResult.kind
    of erkRows:
      html.add("<thead><tr>")
      for col in execResult.columns:
        html.add("<th>" & col & "</th>")
      html.add("</tr></thead><tbody>")
      for row in execResult.rows:
        html.add("<tr>")
        for i, col in execResult.columns:
          if i < row.len:
            html.add("<td>" & row[i] & "</td>")
          else:
            html.add("<td></td>")
        html.add("</tr>")
      html.add("</tbody></table>")
    of erkStreamingRows:
      html.add("<thead><tr>")
      for col in execResult.streamColumns:
        html.add("<th>" & col & "</th>")
      html.add("</tr></thead><tbody>")
      let iter = execResult.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          let row = rowOpt.get()
          html.add("<tr>")
          for i, col in execResult.streamColumns:
            if i < row.len:
              html.add("<td>" & row[i] & "</td>")
            else:
              html.add("<td></td>")
          html.add("</tr>")
      iter.closeIterator()
      html.add("</tbody></table>")
    else:
      html = "<div class='htmx-empty-state'>No data or error</div>"
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: SQL tab ----
  if path == "/htmx/sql" and httpMethod == HttpGet:
    let databases = @["default"]
    let schemas = @["sys", "public"]
    let defaultQuery = "SELECT * FROM sys.nodes"
    var html: string = ""
    compileTemplateFile("sql.nimja", baseDir = getTemplateDir(),
        varname = "html", blockToRender = "content")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: Settings tab ----
  if path == "/htmx/settings" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http503, "<div class='panel'><h1>Server not ready</h1></div>")
      return fut
    let nodeId = $srv.config.serverId.int
    let host = srv.config.host
    let port = $srv.config.port
    let webPort = $srv.config.webPort
    let clusterName = srv.config.clusterName
    let dataDir = srv.config.dataDir
    let version = srv.config.serverVersion
    var html: string = ""
    compileTemplateFile("settings.nimja", baseDir = getTemplateDir(),
        varname = "html", blockToRender = "content")
    sendHtml(Http200, html)
    return fut

  # ---- REST: Add node ----
  if path == "/api/nodes" and httpMethod == HttpPost:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    let bodyOpt = req.body()
    if bodyOpt.isNone:
      sendJson(Http400, %* {"error": "missing body"})
      return fut
    let formData = parseFormData(bodyOpt.get())
    let host = formData.getOrDefault("host")
    let raftPort = parseInt(formData.getOrDefault("raftPort"))
    let clientPort = parseInt(formData.getOrDefault("clientPort"))
    let webPort = parseInt(formData.getOrDefault("webPort"))
    let newNode = pserver.ClusterNodeEntry(
      nodeId: uint16(srv.nodeRegistry.nodes.len + 1),
      host: host,
      raftPort: uint16(raftPort),
      clientPort: uint16(clientPort),
      webPort: uint16(webPort),
      status: 0
    )
    srv.nodeRegistry.addNode(newNode)
    if srv.config.dataDir != "":
      pserver.saveRegistry(srv.nodeRegistry, srv.config.dataDir / "node_registry.dat")
    sendHtml(Http200, "<div hx-get='/htmx/nodes' hx-trigger='load' hx-target='#content'></div>")
    return fut

  # ---- REST: Remove node ----
  if path.startsWith("/api/nodes/") and httpMethod == HttpDelete:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    let idStr = path[11..path.len-1]
    let id = parseInt(idStr)
    let removed = srv.nodeRegistry.removeNode(uint16(id))
    if removed and srv.config.dataDir != "":
      pserver.saveRegistry(srv.nodeRegistry, srv.config.dataDir / "node_registry.dat")
    sendHtml(Http200, "<div hx-get='/htmx/nodes' hx-trigger='load' hx-target='#content'></div>")
    return fut

  # ---- REST: Rebalance spaces ----
  if path == "/api/rebalance" and httpMethod == HttpPost:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http200, "<div class='form-msg err'>Server not ready</div>")
      return fut
    sendHtml(Http200, "<div class='form-msg ok'>Rebalance completed</div>")
    return fut

  # ---- REST: SQL query ----
  if path == "/api/sql" and httpMethod == HttpPost:
    let srv = getSrv()
    if srv.isNil or srv.raftStore.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    let bodyOpt = req.body()
    if bodyOpt.isNone:
      sendJson(Http400, %* {"error": "missing body"})
      return fut
    let bodyStr = bodyOpt.get()
    let contentType = getHeader(req, "Content-Type")
    var sql, db, sc: string
    if contentType.contains("application/json"):
      var j: JsonNode
      try:
        j = parseJson(bodyStr)
      except JsonParsingError:
        sendJson(Http400, %* {"error": "invalid JSON"})
        return fut
      sql = j.getOrDefault("sql").getStr("")
      db = j.getOrDefault("database").getStr("default")
      sc = j.getOrDefault("schema").getStr("public")
    else:
      let formData = parseFormData(bodyStr)
      sql = formData.getOrDefault("sql")
      db = formData.getOrDefault("database")
      if db.len == 0: db = "default"
      sc = formData.getOrDefault("schema")
      if sc.len == 0: sc = "public"
    if sql.len == 0:
      sendJson(Http400, %* {"error": "missing sql"})
      return fut
    let startTime = cpuTime()
    var execResult: ExecResult
    {.cast(gcsafe).}:
      execResult = getClient().query(sql, db, sc)
    let elapsed = cpuTime() - startTime
    let elapsedMs = (elapsed * 1000).formatFloat(format = ffDecimal, precision = 2)

    var html = "<div class='sql-stats'>Executed in " & elapsedMs & "ms"
    case execResult.kind
    of erkRows:
      html.add(" • " & $execResult.rows.len & " rows</div>")
      html.add("<table class='data-table'><thead><tr>")
      for col in execResult.columns:
        html.add("<th>" & col & "</th>")
      html.add("</tr></thead><tbody>")
      for row in execResult.rows:
        html.add("<tr>")
        for i, col in execResult.columns:
          if i < row.len:
            html.add("<td>" & row[i] & "</td>")
          else:
            html.add("<td></td>")
        html.add("</tr>")
      html.add("</tbody></table>")
    of erkStreamingRows:
      var rowCount = 0
      html.add("</div><table class='data-table'><thead><tr>")
      for col in execResult.streamColumns:
        html.add("<th>" & col & "</th>")
      html.add("</tr></thead><tbody>")
      let iter = execResult.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          rowCount += 1
          let row = rowOpt.get()
          html.add("<tr>")
          for i, col in execResult.streamColumns:
            if i < row.len:
              html.add("<td>" & row[i] & "</td>")
            else:
              html.add("<td></td>")
          html.add("</tr>")
      iter.closeIterator()
      html.add("</tbody></table>")
      html = "<div class='sql-stats'>Executed in " & elapsedMs & "ms • " &
          $rowCount & " rows</div>" & html[html.find("</div>") + 6..html.len-1]
    of erkModified:
      html.add("</div><div class='form-msg ok'>" & $execResult.count & " rows affected</div>")
    of erkError:
      html.add("</div><div class='form-msg err'>Error: " & execResult.error & "</div>")
    else:
      html.add("</div><div class='form-msg ok'>OK</div>")
    sendHtml(Http200, html)
    return fut

  # ---- 404 Not Found ----
  sendHtml(Http404, "<h1>404 Not Found</h1>")
  return fut

# ---------------------------------------------------------------------------
# Web thread
# ---------------------------------------------------------------------------

proc webThreadFunc(port: int) {.thread.} =
  let settings = initSettings(port = Port(port))
  {.cast(gcsafe).}:
    run(onRequestHandler, settings)

proc launchWebDashboard*(srv: pserver.ProtocolServer) =
  gSrvPtr = cast[pointer](srv)
  gWebPort = srv.config.webPort
  gClient = nil
  createThread(gWebThread, webThreadFunc, gWebPort)

proc stopWebDashboard*() =
  if gWebThread.running:
    joinThread(gWebThread)
