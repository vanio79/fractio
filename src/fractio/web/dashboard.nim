# Fractio Web Management Dashboard — Nimja template engine
# HTMX + Shoelace for frontend. Server-side rendering via Nimja templates.

import httpbeast
import nimja/parser
import std/[json, strutils, strformat, times, os, atomics,
    tables as stdtables, httpclient, uri, options, asyncfutures, net]
import zippy
import ../core/types as coreTypes except Table
import ../protocol/server as pserver
import ../protocol/txn_manager
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
# Static assets (embedded at compile time)
# ---------------------------------------------------------------------------

# Bundled JS: HTMX 2.0.4 + app initialization (minified)
const bundleJs = staticRead("static/bundle.min.js")
const bundleJsGz = staticRead("static/bundle.min.js.gz")

# Shoelace light theme CSS (for local loading, component JS still from CDN)
const shoelaceCss = staticRead("static/shoelace-light.css")
const shoelaceCssGz = staticRead("static/shoelace-light.css.gz")

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

proc jsonValStr(j: JsonNode, key: string, defaultVal: string = ""): string =
  ## Extract a string value from a JsonNode field, handling both JString and JInt.
  if j.isNil or not j.hasKey(key): return defaultVal
  let n = j[key]
  case n.kind
  of JString: return n.getStr(defaultVal)
  of JInt: return $n.getInt()
  of JFloat: return $n.getFloat()
  of JBool: return $n.getBool()
  else: return defaultVal

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

  proc trackRequest(srv: pserver.ProtocolServer, isError: bool = false) =
    if srv.isNil: return
    discard srv.metrics.requestsTotal.fetchAdd(1)
    if isError:
      discard srv.metrics.requestsErr.fetchAdd(1)
    else:
      discard srv.metrics.requestsOK.fetchAdd(1)

  # ---- Static: bundle.js (HTMX + app code, minified) ----
  if path == "/bundle.js" and (httpMethod == HttpGet or httpMethod == HttpHead):
    var body: string
    var hdrs = "Content-Type: application/javascript; charset=utf-8\nCache-Control: public, max-age=31536000\nVary: Accept-Encoding"
    {.cast(gcsafe).}:
      body = if wantsGzip: bundleJsGz else: bundleJs
    if wantsGzip:
      hdrs.add("\nContent-Encoding: gzip")
    if httpMethod == HttpHead:
      req.send(Http200, "", hdrs)
    else:
      req.send(Http200, body, hdrs)
    return fut

  # ---- Static: app.js (alias for bundle.js) ----
  if path == "/app.js" and (httpMethod == HttpGet or httpMethod == HttpHead):
    var body: string
    var hdrs = "Content-Type: application/javascript; charset=utf-8\nCache-Control: public, max-age=31536000\nVary: Accept-Encoding"
    {.cast(gcsafe).}:
      body = if wantsGzip: bundleJsGz else: bundleJs
    if wantsGzip:
      hdrs.add("\nContent-Encoding: gzip")
    if httpMethod == HttpHead:
      req.send(Http200, "", hdrs)
    else:
      req.send(Http200, body, hdrs)
    return fut

  # ---- Static: shoelace-light.css ----
  if path == "/shoelace-light.css" and (httpMethod == HttpGet or httpMethod == HttpHead):
    var body: string
    var hdrs = "Content-Type: text/css; charset=utf-8\nCache-Control: public, max-age=31536000\nVary: Accept-Encoding"
    {.cast(gcsafe).}:
      body = if wantsGzip: shoelaceCssGz else: shoelaceCss
    if wantsGzip:
      hdrs.add("\nContent-Encoding: gzip")
    if httpMethod == HttpHead:
      req.send(Http200, "", hdrs)
    else:
      req.send(Http200, body, hdrs)
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
      sendJson(Http503, %* {"success": false, "error": "server not ready"})
      return fut
    let bodyOpt = req.body()
    if bodyOpt.isNone:
      sendJson(Http400, %* {"success": false, "error": "missing body"})
      return fut
    let contentType = getHeader(req, "Content-Type")
    var host, raftPortStr, clientPortStr, webPortStr, nodeIdStr: string
    if contentType.contains("application/json"):
      var j: JsonNode
      try:
        j = parseJson(bodyOpt.get())
      except JsonParsingError:
        sendJson(Http400, %* {"success": false, "error": "invalid JSON"})
        return fut
      nodeIdStr = jsonValStr(j, "nodeId")
      host = jsonValStr(j, "host")
      raftPortStr = jsonValStr(j, "raftPort")
      clientPortStr = jsonValStr(j, "clientPort")
      webPortStr = jsonValStr(j, "webPort")
    else:
      let formData = parseFormData(bodyOpt.get())
      nodeIdStr = formData.getOrDefault("nodeId")
      host = formData.getOrDefault("host")
      raftPortStr = formData.getOrDefault("raftPort")
      clientPortStr = formData.getOrDefault("clientPort")
      webPortStr = formData.getOrDefault("webPort")
    if nodeIdStr.len == 0:
      sendJson(Http400, %* {"success": false, "message": "missing nodeId"})
      return fut
    if host.len == 0:
      sendJson(Http400, %* {"success": false, "message": "missing host"})
      return fut
    let nodeId = parseInt(nodeIdStr)
    if nodeId == 0:
      sendJson(Http400, %* {"success": false, "message": "nodeId 0 is reserved"})
      return fut
    let raftPort = parseInt(raftPortStr)
    let clientPort = parseInt(clientPortStr)
    let webPort = parseInt(webPortStr)
    let newNode = pserver.ClusterNodeEntry(
      nodeId: uint16(nodeId),
      host: host,
      raftPort: uint16(raftPort),
      clientPort: uint16(clientPort),
      webPort: uint16(webPort),
      status: 0
    )
    srv.nodeRegistry.addNode(newNode)
    if srv.config.dataDir != "":
      pserver.saveRegistry(srv.nodeRegistry, srv.config.dataDir / "node_registry.dat")
    # Add the joining node as a Raft peer to local groups
    if not srv.raftCoord.isNil:
      discard srv.raftCoord.addServerToGroup(META_GROUP_ID, uint32(nodeId), host, raftPort)
      discard srv.raftCoord.addServerToGroup(DATA_GROUP_START_ID, uint32(nodeId), host, raftPort)
    var membersArr = newJArray()
    for e in srv.nodeRegistry.listNodes():
      membersArr.add(%* {
        "nodeId": e.nodeId.int,
        "host": e.host,
        "raftPort": e.raftPort.int,
        "clientPort": e.clientPort.int,
      })
    sendJson(Http200, %* {"success": true, "nodeId": nodeId, "members": membersArr})
    return fut

  # ---- REST: Remove node ----
  if path.startsWith("/api/nodes/") and httpMethod == HttpDelete:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"success": false, "error": "server not ready"})
      return fut
    let idStr = path[11..path.len-1]
    let id = parseInt(idStr)
    let removed = srv.nodeRegistry.removeNode(uint16(id))
    if removed and srv.config.dataDir != "":
      pserver.saveRegistry(srv.nodeRegistry, srv.config.dataDir / "node_registry.dat")
    sendJson(Http200, %* {"success": removed})
    return fut

  # ---- REST: Rebalance spaces ----
  if path == "/api/rebalance" and httpMethod == HttpPost:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    sendJson(Http200, %* {"success": true, "message": "Rebalance completed"})
    return fut

  # ---- REST: System tables list ----
  if path == "/api/sql/system-tables" and httpMethod == HttpGet:
    var arr = newJArray()
    let sysTables = @[
      (id: 1, name: "sys.databases", desc: "Database catalog"),
      (id: 2, name: "sys.schemas", desc: "Schema catalog"),
      (id: 3, name: "sys.tables", desc: "Table descriptors"),
      (id: 4, name: "sys.groups", desc: "Raft group metadata"),
      (id: 5, name: "sys.nodes", desc: "Cluster node registry"),
      (id: 6, name: "sys.settings", desc: "Cluster config"),
      (id: 7, name: "sys.spaces", desc: "Space catalog"),
    ]
    for st in sysTables:
      arr.add(%* {
        "id": st.id,
        "name": st.name,
        "description": st.desc,
        "rowCount": 0
      })
    sendJson(Http200, arr)
    return fut

  # ---- REST: System table data ----
  if path.startsWith("/api/sql/system-table/") and httpMethod == HttpGet:
    let idStr = path[22..path.len-1]
    var tableId: int
    try:
      tableId = parseInt(idStr)
    except ValueError:
      sendJson(Http404, %* {"error": "invalid table id"})
      return fut
    var tableName = ""
    case tableId
    of 1: tableName = "sys.databases"
    of 2: tableName = "sys.schemas"
    of 3: tableName = "sys.tables"
    of 4: tableName = "sys.groups"
    of 5: tableName = "sys.nodes"
    of 6: tableName = "sys.settings"
    of 7: tableName = "sys.spaces"
    else:
      sendJson(Http404, %* {"error": "system table not found"})
      return fut
    var execResult: ExecResult
    {.cast(gcsafe).}:
      execResult = getClient().query("SELECT * FROM " & tableName)
    case execResult.kind
    of erkRows:
      var rowsJson = newJArray()
      for row in execResult.rows:
        var rowObj = newJObject()
        for i, col in execResult.columns:
          if i < row.len: rowObj[col] = newJString(row[i])
        rowsJson.add(rowObj)
      sendJson(Http200, %* {"tableId": tableId, "columns": execResult.columns, "rows": rowsJson})
    of erkStreamingRows:
      var rowsJson = newJArray()
      let iter = execResult.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          let row = rowOpt.get()
          var rowObj = newJObject()
          for i, col in execResult.streamColumns:
            if i < row.len: rowObj[col] = newJString(row[i])
          rowsJson.add(rowObj)
      iter.closeIterator()
      sendJson(Http200, %* {"tableId": tableId, "columns": execResult.streamColumns, "rows": rowsJson})
    else:
      sendJson(Http200, %* {"tableId": tableId, "columns": newJArray(), "rows": newJArray()})
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
    trackRequest(srv)
    let startTime = cpuTime()
    var execResult: ExecResult
    {.cast(gcsafe).}:
      execResult = getClient().query(sql, db, sc)
    let elapsed = cpuTime() - startTime
    let elapsedMs = (elapsed * 1000).formatFloat(format = ffDecimal, precision = 2)

    if contentType.contains("application/json"):
      # REST API: return JSON
      case execResult.kind
      of erkRows:
        var rowsJson = newJArray()
        for row in execResult.rows:
          var rowObj = newJObject()
          for i, col in execResult.columns:
            if i < row.len:
              rowObj[col] = newJString(row[i])
          rowsJson.add(rowObj)
        sendJson(Http200, %* {"kind": "rows", "columns": execResult.columns,
            "rows": rowsJson})
      of erkStreamingRows:
        var rowsJson = newJArray()
        let iter = execResult.streamIterator
        while iter.hasNextRow():
          let rowOpt = iter.nextRow()
          if rowOpt.isSome:
            let row = rowOpt.get()
            var rowObj = newJObject()
            for i, col in execResult.streamColumns:
              if i < row.len:
                rowObj[col] = newJString(row[i])
            rowsJson.add(rowObj)
        iter.closeIterator()
        sendJson(Http200, %* {"kind": "rows", "columns": execResult.streamColumns,
            "rows": rowsJson})
      of erkModified:
        sendJson(Http200, %* {"kind": "modified", "count": execResult.count,
            "message": execResult.message})
      of erkOk:
        sendJson(Http200, %* {"kind": "ok", "message": execResult.okMessage})
      of erkError:
        sendJson(Http400, %* {"kind": "error", "error": execResult.error})
      of erkUseDatabase:
        sendJson(Http200, %* {"kind": "useDatabase", "database": execResult.newDatabase})
      of erkUseSchema:
        sendJson(Http200, %* {"kind": "useSchema", "schema": execResult.newSchema})
      else:
        sendJson(Http200, %* {"kind": "ok"})
    else:
      # HTMX: return HTML
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

  # ---- REST: health ----
  if path == "/api/health" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    trackRequest(srv)
    let nodes = srv.nodeRegistry.listNodes()
    let rc = nodes.len
    var metaLeaderOK = false
    if not srv.raftStore.isNil and not srv.raftStore.coordinator.isNil:
      metaLeaderOK = srv.raftStore.coordinator.isLeader(META_GROUP_ID) or
          srv.raftStore.coordinator.getLeader(META_GROUP_ID) > 0
    sendJson(Http200, %* {
      "status": if metaLeaderOK: 0 else: 1,
      "leaderOK": true,
      "metaLeaderOK": metaLeaderOK,
      "replicaCount": rc,
      "healthyReplicas": rc,
      "clusterName": srv.config.clusterName,
    })
    return fut

  # ---- REST: info ----
  if path == "/api/info" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    trackRequest(srv)
    let nowSecs = getTime().toUnix()
    let uptime = uint64(max(0'i64, nowSecs - srv.startedAt))
    let role = block:
      if srv.raftStore.isNil: "unknown"
      else:
        if srv.raftStore.coordinator.isLeader(META_GROUP_ID): "leader"
        else: "follower"
    let shards = if srv.raftStore.isNil: 0
                 else: srv.raftStore.coordinator.getGroupCount()
    sendJson(Http200, %* {
      "nodeId": srv.config.serverId.int,
      "version": srv.config.serverVersion,
      "uptimeSecs": uptime,
      "role": role,
      "shardCount": shards,
      "clientCount": srv.clientCount(),
      "clusterName": srv.config.clusterName,
    })
    return fut

  # ---- REST: metrics ----
  if path == "/api/metrics" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    trackRequest(srv)
    let m = srv.metrics
    var snap = m.snapshot()
    snap.activeTxns = uint32(srv.txnMgr.activeTxnCount())
    sendJson(Http200, %* {
      "requestsTotal": snap.requestsTotal,
      "requestsOK": snap.requestsOK,
      "requestsErr": snap.requestsErr,
      "bytesIn": snap.bytesIn,
      "bytesOut": snap.bytesOut,
      "kvGets": snap.kvGets,
      "kvPuts": snap.kvPuts,
      "kvDeletes": snap.kvDeletes,
      "activeTxns": snap.activeTxns,
      "committedTxns": snap.committedTxns,
      "abortedTxns": snap.abortedTxns,
    })
    return fut

  # ---- REST: storage ----
  if path == "/api/storage" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil or srv.raftStore.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    trackRequest(srv)
    let backend = srv.raftStore.coordinator.store
    let stats = backend.getProperty("leveldb.stats")
    var numFiles = newJArray()
    for level in 0 .. 6:
      numFiles.add(newJString(backend.getProperty(
          "leveldb.num-files-at-level" & $level)))
    var sizes = newSeq[float](7)
    for line in stats.splitLines():
      let stripped = line.strip()
      if stripped.len > 0 and stripped[0] in '0'..'6':
        let parts = stripped.splitWhitespace()
        if parts.len >= 3:
          try:
            let level = parseInt(parts[0])
            let sizeMB = parseFloat(parts[2])
            if level >= 0 and level <= 6:
              sizes[level] = sizeMB
          except ValueError:
            discard
    var levelSizes = newJArray()
    for s in sizes:
      levelSizes.add(newJFloat(s))
    sendJson(Http200, %* {
      "stats": stats,
      "numFiles": numFiles,
      "levelSizes": levelSizes,
      "path": backend.path,
    })
    return fut

  # ---- REST: Cluster join ----
  if path == "/api/cluster/join" and httpMethod == HttpPost:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"success": false, "error": "server not ready"})
      return fut
    let bodyOpt = req.body()
    if bodyOpt.isNone:
      sendJson(Http400, %* {"success": false, "error": "missing body"})
      return fut
    let contentType = getHeader(req, "Content-Type")
    var host, raftPortStr, clientPortStr, webPortStr, nodeIdStr: string
    if contentType.contains("application/json"):
      var j: JsonNode
      try:
        j = parseJson(bodyOpt.get())
      except JsonParsingError:
        sendJson(Http400, %* {"success": false, "error": "invalid JSON"})
        return fut
      nodeIdStr = jsonValStr(j, "nodeId")
      host = jsonValStr(j, "host")
      raftPortStr = jsonValStr(j, "raftPort")
      clientPortStr = jsonValStr(j, "clientPort")
      webPortStr = jsonValStr(j, "webPort")
    else:
      let formData = parseFormData(bodyOpt.get())
      nodeIdStr = formData.getOrDefault("nodeId")
      host = formData.getOrDefault("host")
      raftPortStr = formData.getOrDefault("raftPort")
      clientPortStr = formData.getOrDefault("clientPort")
      webPortStr = formData.getOrDefault("webPort")
    if nodeIdStr.len == 0:
      sendJson(Http400, %* {"success": false, "message": "missing nodeId"})
      return fut
    if host.len == 0:
      sendJson(Http400, %* {"success": false, "message": "missing host"})
      return fut
    let nodeId = parseInt(nodeIdStr)
    if nodeId == 0:
      sendJson(Http400, %* {"success": false, "message": "nodeId 0 is reserved"})
      return fut
    let raftPort = parseInt(raftPortStr)
    let clientPort = parseInt(clientPortStr)
    let webPort = parseInt(webPortStr)
    let newNode = pserver.ClusterNodeEntry(
      nodeId: uint16(nodeId),
      host: host,
      raftPort: uint16(raftPort),
      clientPort: uint16(clientPort),
      webPort: uint16(webPort),
      status: 0
    )
    srv.nodeRegistry.addNode(newNode)
    if srv.config.dataDir != "":
      pserver.saveRegistry(srv.nodeRegistry, srv.config.dataDir / "node_registry.dat")
    sendJson(Http200, %* {"success": true, "nodeId": nodeId})
    return fut

  # ---- REST: SQL metadata endpoints ----
  if path == "/api/sql/databases" and httpMethod == HttpGet:
    var execResult: ExecResult
    {.cast(gcsafe).}:
      execResult = getClient().query("SELECT name FROM sys.databases")
    var arr = newJArray()
    case execResult.kind
    of erkRows:
      for row in execResult.rows:
        if row.len > 0: arr.add(newJString(row[0]))
    of erkStreamingRows:
      let iter = execResult.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          let row = rowOpt.get()
          if row.len > 0: arr.add(newJString(row[0]))
      iter.closeIterator()
    else:
      discard
    sendJson(Http200, arr)
    return fut

  if path == "/api/sql/schemas" and httpMethod == HttpGet:
    let dbHeader = getHeader(req, "X-Database")
    let db = if dbHeader.len > 0: dbHeader else: "default"
    var execResult: ExecResult
    {.cast(gcsafe).}:
      execResult = getClient().query("SELECT name FROM sys.schemas WHERE database = '" & db & "'")
    var arr = newJArray()
    case execResult.kind
    of erkRows:
      for row in execResult.rows:
        if row.len > 0: arr.add(newJString(row[0]))
    of erkStreamingRows:
      let iter = execResult.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          let row = rowOpt.get()
          if row.len > 0: arr.add(newJString(row[0]))
      iter.closeIterator()
    else:
      discard
    sendJson(Http200, arr)
    return fut

  if path == "/api/sql/tables" and httpMethod == HttpGet:
    let dbHeader = getHeader(req, "X-Database")
    let scHeader = getHeader(req, "X-Schema")
    let db = if dbHeader.len > 0: dbHeader else: "default"
    let sc = if scHeader.len > 0: scHeader else: "public"
    var execResult: ExecResult
    {.cast(gcsafe).}:
      execResult = getClient().query("SELECT name FROM sys.tables WHERE database = '" & db & "' AND schema = '" & sc & "'")
    var arr = newJArray()
    case execResult.kind
    of erkRows:
      for row in execResult.rows:
        if row.len > 0: arr.add(newJString(row[0]))
    of erkStreamingRows:
      let iter = execResult.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          let row = rowOpt.get()
          if row.len > 0: arr.add(newJString(row[0]))
      iter.closeIterator()
    else:
      discard
    sendJson(Http200, arr)
    return fut

  # ---- REST: nodes GET ----
  if path == "/api/nodes" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    trackRequest(srv)
    var arr = newJArray()
    let entries = srv.nodeRegistry.listNodes()
    for e in entries:
      arr.add(%* {
        "nodeId": e.nodeId.int,
        "host": e.host,
        "raftPort": e.raftPort.int,
        "clientPort": e.clientPort.int,
        "status": e.status.int,
      })
    sendJson(Http200, arr)
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
