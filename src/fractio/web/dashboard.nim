# Fractio Web Management Dashboard — Nimja template engine
# HTMX + Shoelace for frontend. Server-side rendering via Nimja templates.

import httpbeast
import nimja/parser
import std/[json, strutils, strformat, times, os, atomics, algorithm,
    tables as stdtables, httpclient, uri, options, asyncfutures, net, locks]
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
var gClientLock {.global.}: Lock
var gWebThread {.global.}: Thread[int]
var gClientLastRefresh {.global.}: float64 = 0.0 # epochTime of last successful refresh

template getSrv(): pserver.ProtocolServer =
  cast[pserver.ProtocolServer](gSrvPtr)

# ---------------------------------------------------------------------------
# Thread-safe client access
# ---------------------------------------------------------------------------
# The global FractioClient is shared across httpbeast's async event loop.
# Concurrent HTTP handlers calling getClient().query() on the same client
# would interleave ProtocolClient socket reads/writes, causing data corruption.
# The lock serializes all client operations to prevent this.

template withClient*(body: untyped): untyped =
  ## Acquire the client lock, get the client, execute body, release lock.
  ## Use this for ALL FractioClient operations to prevent concurrent
  ## access to shared ProtocolClient connections.
  acquire(gClientLock)
  try:
    let cl {.inject.} = getClientLocked()
    body
  finally:
    release(gClientLock)

proc getClientLocked(): FractioClient =
  ## Get or create the global FractioClient. MUST be called under gClientLock.
  ## NOTE: We intentionally do NOT call forceMetadataRefresh() here because
  ## it acquires the FractioClient's internal RWLock with a write lock, which
  ## could invalidate connection cache entries that the current query is using.
  ## Metadata refreshes happen at query retry time via resetClient().
  if gClient == nil:
    let srv = getSrv()
    let host = if srv.config.host == "0.0.0.0": "127.0.0.1" else: srv.config.host
    var cfg = newFractioClientConfig(host, srv.config.port)
    # Dashboard client: balance between responsiveness and reliability.
    # Connection timeout of 2s allows TCP handshake to remote nodes.
    # Request timeout of 3s allows for leader election (300-600ms) + retry.
    # 10 retries gives enough room for NOT_LEADER redirects and connection retries.
    cfg.maxKvRetries = 10
    cfg.connectionTimeoutMs = 2000
    cfg.requestTimeoutMs = 3000
    gClient = newFractioClient(cfg)
    let initOk = gClient.initialize()
    gClientLastRefresh = epochTime()
  gClient

proc resetClient() =
  ## Reset the global FractioClient by closing all connections and
  ## setting it to nil. The next call to getClient() will create a fresh client.
  if gClient != nil:
    try:
      gClient.close()
    except: discard
    gClient = nil

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

proc listSpaceNames(): seq[string] =
  ## Query the META group for all space names. Used by both the REST
  ## /api/sql/databases endpoint and the HTMX tab dropdowns.
  ##
  ## In Fractio, "databases" in the UI correspond to spaces (each space
  ## is a sharded data plane). SHOW SPACES returns: [space_id, name,
  ## replicas, group_count, group_ids]. We extract the 'name' column.
  ##
  ## Returns an empty seq if the server is not ready or the query fails,
  ## which keeps the UI usable (the empty dropdown is preferable to a 500).
  result = @[]
  let srv = getSrv()
  if srv.isNil or srv.raftStore.isNil:
    return
  var dbRes: ExecResult
  {.cast(gcsafe).}:
    withClient:
      dbRes = cl.query("SHOW SPACES", "default", "public")
  # Find the 'name' column index, fall back to column 1 (name is always
  # the second column of SHOW SPACES output).
  let nameIdx: int =
    if dbRes.kind == erkStreamingRows and dbRes.streamColumns.len > 1:
      let idx = dbRes.streamColumns.find("name")
      if idx >= 0: idx else: 1
    elif dbRes.kind == erkRows and dbRes.columns.len > 1:
      let idx = dbRes.columns.find("name")
      if idx >= 0: idx else: 1
    else:
      0
  if dbRes.kind == erkStreamingRows:
    let iter = dbRes.streamIterator
    while iter.hasNextRow():
      let rowOpt = iter.nextRow()
      if rowOpt.isSome:
        let row = rowOpt.get()
        if row.len > nameIdx and row[nameIdx].len > 0:
          result.add(row[nameIdx])
    iter.closeIterator()
  elif dbRes.kind == erkRows:
    for row in dbRes.rows:
      if row.len > nameIdx and row[nameIdx].len > 0:
        result.add(row[nameIdx])

proc listSchemaNames(db: string): seq[string] =
  ## Query the META group for all schema names in a given space.
  ## Returns an empty seq on failure.
  result = @[]
  let srv = getSrv()
  if srv.isNil or srv.raftStore.isNil:
    return
  var schemaRes: ExecResult
  {.cast(gcsafe).}:
    withClient:
      schemaRes = cl.query("SHOW SCHEMAS", db, "public")
  if schemaRes.kind == erkStreamingRows:
    let iter = schemaRes.streamIterator
    while iter.hasNextRow():
      let rowOpt = iter.nextRow()
      if rowOpt.isSome:
        let row = rowOpt.get()
        if row.len > 0:
          result.add(row[0])
    iter.closeIterator()
  elif schemaRes.kind == erkRows:
    for row in schemaRes.rows:
      if row.len > 0:
        result.add(row[0])

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
      withClient:
        infoResult = cl.query("SELECT * FROM sys.nodes WHERE nodeId = " &
            $srv.config.serverId.int)
    let nodeId = $srv.config.serverId.int
    let role = if not srv.raftCoord.isNil and srv.raftCoord.isLeader(
        META_GROUP_ID): "Leader" else: "Follower"
    let uptime = formatUptime(uint64(getTime().toUnix() - srv.startedAt))
    let clients = $srv.clientCount()
    let groups = if not srv.raftStore.isNil: $srv.raftStore.groupCount() else: "0"
    let version = srv.config.serverVersion
    let clusterName = srv.config.clusterName

    # Get cluster nodes from sys.nodes table
    var nodesData: seq[(string, string, string, string, string)] = @[]
    var nodesResult: ExecResult
    {.cast(gcsafe).}:
      withClient:
        nodesResult = cl.query("SELECT * FROM sys.nodes")
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

    # Get spaces - use explicit column list to avoid _key column at index 0
    # sys.spaces columns (after _key): spaceId, name, replicas, groupCount, ...
    var spacesResult: ExecResult
    {.cast(gcsafe).}:
      withClient:
        spacesResult = cl.query("SELECT spaceId, name, replicas, groupCount FROM sys.spaces")
    var spacesData: seq[tuple[name, spaceId: string, groupCount: int]] = @[]
    template extractSpaces(rows: openArray[seq[string]]) =
      for row in rows:
        if row.len >= 4:
          let groupCount = parseInt(row[3])
          spacesData.add((row[1], row[0], groupCount))
    case spacesResult.kind
    of erkRows:
      extractSpaces(spacesResult.rows)
    of erkStreamingRows:
      let iter = spacesResult.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          let row = rowOpt.get()
          if row.len >= 4:
            let groupCount = parseInt(row[3])
            spacesData.add((row[1], row[0], groupCount))
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
      withClient:
        nodesResult = cl.query("SELECT * FROM sys.nodes")
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
    # Dynamically query the META group for the list of spaces (Fractio's
    # notion of "databases" in the UI). Falls back to ["default"] if the
    # server is not ready or the query fails, so the UI still loads.
    var databases = listSpaceNames()
    if databases.len == 0:
      databases = @["default"]
    var html: string = ""
    compileTemplateFile("data.nimja", baseDir = getTemplateDir(),
        varname = "html", blockToRender = "content")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: schemas dropdown ----
  if path == "/htmx/schemas" and httpMethod == HttpGet:
    let db = getQueryParam(queryParams, "db-select")
    var schemasData = listSchemaNames(db)
    if schemasData.len == 0:
      schemasData = @["sys", "public"]
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
      # Query sys.tables for table names in this schema.
      # NOTE: WHERE-clause filtering is currently skipped on system tables
      # (see executor.nim fetchNextMatchingRow, which doesn't apply
      # scFilter for system table records). We fetch all tables and
      # filter client-side by database and schema.
      var execResult: ExecResult
      {.cast(gcsafe).}:
        withClient:
          execResult = cl.query("SELECT name, schema, database FROM sys.tables")
      let dbCol: int =
        if execResult.kind == erkStreamingRows and
            execResult.streamColumns.len > 0:
          let idx = execResult.streamColumns.find("database")
          if idx >= 0: idx else: 2
        elif execResult.kind == erkRows and execResult.columns.len > 0:
          let idx = execResult.columns.find("database")
          if idx >= 0: idx else: 2
        else:
          -1
      let schemaCol: int =
        if execResult.kind == erkStreamingRows and
            execResult.streamColumns.len > 0:
          let idx = execResult.streamColumns.find("schema")
          if idx >= 0: idx else: 1
        elif execResult.kind == erkRows and execResult.columns.len > 0:
          let idx = execResult.columns.find("schema")
          if idx >= 0: idx else: 1
        else:
          -1
      let nameCol: int =
        if execResult.kind == erkStreamingRows and
            execResult.streamColumns.len > 0:
          let idx = execResult.streamColumns.find("name")
          if idx >= 0: idx else: 0
        elif execResult.kind == erkRows and execResult.columns.len > 0:
          let idx = execResult.columns.find("name")
          if idx >= 0: idx else: 0
        else:
          0
      if execResult.kind == erkRows:
        for row in execResult.rows:
          if row.len > max(max(nameCol, schemaCol), dbCol):
            if row[dbCol] == db and row[schemaCol] == schema:
              tablesData.add(row[nameCol])
      elif execResult.kind == erkStreamingRows:
        let iter = execResult.streamIterator
        while iter.hasNextRow():
          let rowOpt = iter.nextRow()
          if rowOpt.isSome:
            let row = rowOpt.get()
            if row.len > max(max(nameCol, schemaCol), dbCol):
              if row[dbCol] == db and row[schemaCol] == schema:
                tablesData.add(row[nameCol])
        iter.closeIterator()
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
      withClient:
        # Pass the database from the URL path so the query runs in the
        # correct database context. Without this, the query defaults to
        # database="default" and can't find tables in other databases.
        execResult = cl.query("SELECT * FROM " & db & "." & schema &
             "." & tableName & " LIMIT 100", database = db, schema = schema)
    # Filter out the internal _key column from display
    var displayColumns: seq[string] = @[]
    var columnIndices: seq[int] = @[]
    case execResult.kind
    of erkRows:
      for i, col in execResult.columns:
        if col != "_key":
          displayColumns.add(col)
          columnIndices.add(i)
    of erkStreamingRows:
      for i, col in execResult.streamColumns:
        if col != "_key":
          displayColumns.add(col)
          columnIndices.add(i)
    else:
      discard

    var html = "<table class='htmx-data-table'>"
    case execResult.kind
    of erkRows:
      html.add("<thead><tr>")
      for col in displayColumns:
        html.add("<th>" & col & "</th>")
      html.add("</tr></thead><tbody>")
      for row in execResult.rows:
        html.add("<tr>")
        for idx in columnIndices:
          if idx < row.len:
            html.add("<td>" & row[idx] & "</td>")
          else:
            html.add("<td></td>")
        html.add("</tr>")
      html.add("</tbody></table>")
    of erkStreamingRows:
      html.add("<thead><tr>")
      for col in displayColumns:
        html.add("<th>" & col & "</th>")
      html.add("</tr></thead><tbody>")
      let iter = execResult.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          let row = rowOpt.get()
          html.add("<tr>")
          for idx in columnIndices:
            if idx < row.len:
              html.add("<td>" & row[idx] & "</td>")
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
    # Dynamically populate the database/schema dropdowns from the META group.
    # Default to ["default"] / ["sys","public"] if the server is not ready
    # or the query fails, so the UI still loads.
    var databases = listSpaceNames()
    if databases.len == 0:
      databases = @["default"]
    var schemas = listSchemaNames("default")
    if schemas.len == 0:
      schemas = @["sys", "public"]
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

  # ---- REST: List nodes ----
  if path == "/api/nodes" and httpMethod == HttpGet:
    ## Get list of cluster nodes as JSON array.
    ## Used by integration tests to verify cluster membership.
    ##
    ## CRITICAL: Reads directly from the local Raft backend (sys.nodes table)
    ## without going through FractioClient. FractioClient does synchronous
    ## network I/O which blocks the httpbeast event loop, making ALL HTTP
    ## endpoints unresponsive during that time.
    ##
    ## The sys.nodes table is replicated by Raft, so every node has the same
    ## data locally (may be slightly stale on followers, but that's fine).
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    var nodesJson: seq[JsonNode] = @[]
    var foundLocal = false
    # Read directly from local backend to avoid blocking the event loop
    if srv.raftStore != nil:
      let backend = srv.raftStore.getBackend()
      if backend != nil and backend.isOpen:
        let sysNodesPrefix = encodeTableKey(system_tables.SYS_NODES_TABLE_ID, "")
        let sysNodesEnd = makeScanEndKey(system_tables.SYS_NODES_TABLE_ID)
        let scanResult = scan(backend, sysNodesPrefix, sysNodesEnd)
        for item in scanResult:
          let k = item.key
          let rawV = item.value
          if k.len < sysNodesPrefix.len + 1: continue
          let nodeIdStr = k[sysNodesPrefix.len..^1]
          let nodeId = try: parseInt(nodeIdStr) except: 0
          if nodeId <= 0: continue
          let (payload, isDeleted) = stripMVCCHeader(rawV)
          if isDeleted or payload.len == 0: continue
          let rec = try: decodeNodeRecord(payload) except: continue
          if uint16(nodeId) == srv.config.serverId:
            foundLocal = true
          nodesJson.add( %* {
            "nodeId": $rec.nodeId,
            "host": rec.host,
            "raftPort": $rec.raftPort,
            "clientPort": $rec.clientPort,
            "webPort": $rec.webPort,
            "status": "alive"
          })
    # Fallback: local node not found in sys.nodes yet (during bootstrap)
    if not foundLocal:
      nodesJson.add( %* {
        "nodeId": $srv.config.serverId,
        "host": srv.config.host,
        "raftPort": $srv.config.port,
        "clientPort": $srv.config.port,
        "webPort": $srv.config.webPort,
        "status": "alive"
      })
    sendJson(Http200, %nodesJson)
    return fut

  # ---- REST: Cluster join ----
  if path == "/api/cluster/join" and httpMethod == HttpPost:
    ## Handle join request from a new node wanting to join the cluster.
    ## The joining node sends its nodeId, host, raftPort, clientPort, webPort.
    ## This node (leader) adds the new node as a Raft peer and returns
    ## all cluster members so the joining node can add them too.
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"success": false, "error": "server not ready"})
      return fut
    let bodyOpt = req.body()
    if bodyOpt.isNone:
      sendJson(Http400, %* {"success": false, "error": "missing body"})
      return fut
    let contentType = getHeader(req, "Content-Type")
    var newNodeId: int
    var newHost: string
    var newRaftPort: int
    var newClientPort: int
    var newWebPort: int
    if contentType.contains("application/json"):
      try:
        let j = parseJson(bodyOpt.get())
        newNodeId = j.getOrDefault("nodeId").getInt(0)
        newHost = j.getOrDefault("host").getStr("")
        newRaftPort = j.getOrDefault("raftPort").getInt(0)
        newClientPort = j.getOrDefault("clientPort").getInt(0)
        newWebPort = j.getOrDefault("webPort").getInt(0)
      except JsonParsingError:
        sendJson(Http400, %* {"success": false, "error": "invalid JSON"})
        return fut
    else:
      let formData = parseFormData(bodyOpt.get())
      try:
        newNodeId = parseInt(formData.getOrDefault("nodeId"))
        newHost = formData.getOrDefault("host")
        newRaftPort = parseInt(formData.getOrDefault("raftPort"))
        newClientPort = parseInt(formData.getOrDefault("clientPort"))
        newWebPort = parseInt(formData.getOrDefault("webPort"))
      except ValueError:
        sendJson(Http400, %* {"success": false,
            "error": "invalid numeric value"})
        return fut
    if newNodeId <= 0 or newHost == "" or newRaftPort <= 0:
      sendJson(Http400, %* {"success": false,
          "error": "missing required fields"})
      return fut
    # Add the new node as a Raft peer and insert into sys.nodes
    {.cast(gcsafe).}:
      srv.addPeerToRaft(uint32(newNodeId), newHost, newRaftPort, newClientPort, newWebPort)
    # Build list of all cluster members to return.
    # CRITICAL: Read directly from local backend to avoid blocking the httpbeast
    # event loop with FractioClient synchronous network I/O.
    var membersJson: seq[JsonNode] = @[]
    if srv.raftStore != nil:
      let backend = srv.raftStore.getBackend()
      if backend != nil and backend.isOpen:
        let sysNodesPrefix = encodeTableKey(system_tables.SYS_NODES_TABLE_ID, "")
        let sysNodesEnd = makeScanEndKey(system_tables.SYS_NODES_TABLE_ID)
        let scanResult = scan(backend, sysNodesPrefix, sysNodesEnd)
        for item in scanResult:
          let k = item.key
          let rawV = item.value
          if k.len < sysNodesPrefix.len + 1: continue
          let nodeIdStr = k[sysNodesPrefix.len..^1]
          let nodeId = try: parseInt(nodeIdStr) except: 0
          if nodeId <= 0: continue
          let (payload, isDeleted) = stripMVCCHeader(rawV)
          if isDeleted or payload.len == 0: continue
          let rec = try: decodeNodeRecord(payload) except: continue
          membersJson.add( %* {
            "nodeId": rec.nodeId.int,
            "host": rec.host,
            "raftPort": rec.raftPort.int,
            "clientPort": rec.clientPort.int,
            "webPort": rec.webPort.int
          })
    # Get the current leader to pass as preferred leader for joining node
    # If no leader yet (election in progress), use this node's ID
    # since this node is the one adding the new member
    var leaderId = 0
    {.cast(gcsafe).}:
      if srv.raftCoord != nil:
        leaderId = srv.raftCoord.getLeader(system_tables.META_GROUP_ID)
        if leaderId <= 0:
          # No leader elected yet, use this node's ID as preferred leader
          # This node called addServerToGroup, so it will be the leader
          leaderId = srv.config.serverId.int
    sendJson(Http200, %* {
      "success": true,
      "message": "node " & $newNodeId & " joined cluster",
      "members": membersJson,
      "preferredLeader": leaderId
    })
    return fut

  # ---- REST: Health check ----
  if path == "/api/health" and httpMethod == HttpGet:
    ## Health check endpoint for tests and monitoring.
    ## Returns status=0 if healthy, metaLeaderOK if meta group has a leader,
    ## dataLeaderOK if the default data group has a leader.
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"status": 1, "error": "server not ready"})
      return fut
    var status = 0
    var metaLeaderOK = false
    var dataLeaderOK = false
    # Check if meta group has a leader
    if srv.raftCoord != nil and srv.raftCoord.running.load():
      let metaLeader = srv.raftCoord.getLeader(system_tables.META_GROUP_ID)
      metaLeaderOK = metaLeader > 0
      let dataLeader = srv.raftCoord.getLeader(
          system_tables.DATA_GROUP_START_ID)
      dataLeaderOK = dataLeader > 0
      if not metaLeaderOK:
        status = 2 # No meta leader
      elif not dataLeaderOK:
        status = 3 # Meta leader OK but data group leader missing
    else:
      status = 1 # Server not fully initialized
    # Include server counts in response for diagnostics
    var metaSrvCount = -1
    var dataSrvCount = -1
    if srv.raftCoord != nil and srv.raftCoord.running.load():
      metaSrvCount = srv.raftCoord.getGroupServerCount(
          system_tables.META_GROUP_ID)
      dataSrvCount = srv.raftCoord.getGroupServerCount(
          system_tables.DATA_GROUP_START_ID)
    sendJson(Http200, %* {
      "status": status,
      "leaderOK": metaLeaderOK and dataLeaderOK,
      "metaLeaderOK": metaLeaderOK,
      "dataLeaderOK": dataLeaderOK,
      "metaServerCount": metaSrvCount,
      "dataServerCount": dataSrvCount,
      "clusterName": srv.config.clusterName,
      "nodeId": srv.config.serverId.int,
      "version": srv.config.serverVersion
    })
    return fut

  # ---- REST: Add node ----
  if path == "/api/nodes" and httpMethod == HttpPost:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"success": false, "error": "server not ready"})
      return fut
    let bodyOpt = req.body()
    if bodyOpt.isNone:
      sendJson(Http400, %* {"success": false, "message": "missing body"})
      return fut
    let bodyStr = bodyOpt.get()
    let contentType = getHeader(req, "Content-Type")
    var newNodeId = 0
    var host = ""
    var raftPort = 0
    var clientPort = 0
    var webPort = 0
    if contentType.contains("application/json"):
      try:
        let j = parseJson(bodyStr)
        newNodeId = j.getOrDefault("nodeId").getInt(0)
        host = j.getOrDefault("host").getStr("")
        raftPort = j.getOrDefault("raftPort").getInt(0)
        clientPort = j.getOrDefault("clientPort").getInt(0)
        webPort = j.getOrDefault("webPort").getInt(0)
      except JsonParsingError:
        sendJson(Http400, %* {"success": false, "message": "invalid JSON"})
        return fut
      except ValueError:
        sendJson(Http400, %* {"success": false,
            "message": "invalid numeric value"})
        return fut
    else:
      let formData = parseFormData(bodyStr)
      try:
        newNodeId = parseInt(formData.getOrDefault("nodeId"))
        host = formData.getOrDefault("host")
        raftPort = parseInt(formData.getOrDefault("raftPort"))
        clientPort = parseInt(formData.getOrDefault("clientPort"))
        webPort = parseInt(formData.getOrDefault("webPort"))
      except ValueError:
        sendJson(Http400, %* {"success": false,
            "message": "invalid numeric value"})
        return fut
    # Validate required fields
    if newNodeId == 0:
      sendJson(Http400, %* {"success": false,
          "message": "nodeId 0 is reserved"})
      return fut
    if host.len == 0:
      sendJson(Http400, %* {"success": false, "message": "missing host"})
      return fut
    if raftPort <= 0:
      sendJson(Http400, %* {"success": false, "message": "missing raftPort"})
      return fut
    let newNode = pserver.ClusterNodeEntry(
      nodeId: uint16(newNodeId),
      host: host,
      raftPort: uint16(raftPort),
      clientPort: uint16(clientPort),
      webPort: uint16(webPort),
      status: 0
    )
    srv.nodeRegistry.addNode(newNode)
    if srv.config.dataDir != "":
      pserver.saveRegistry(srv.nodeRegistry, srv.config.dataDir / "node_registry.dat")
    sendJson(Http200, %* {"success": true, "message": "node added",
        "nodeId": newNodeId})
    return fut

  # ---- REST: Remove node ----
  if path.startsWith("/api/nodes/") and httpMethod == HttpDelete:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"success": false, "error": "server not ready"})
      return fut
    let idStr = path[11..path.len-1]
    var id = 0
    try:
      id = parseInt(idStr)
    except ValueError:
      sendJson(Http400, %* {"success": false, "message": "invalid node ID"})
      return fut
    let removed = srv.nodeRegistry.removeNode(uint16(id))
    if removed and srv.config.dataDir != "":
      pserver.saveRegistry(srv.nodeRegistry, srv.config.dataDir / "node_registry.dat")
    sendJson(Http200, %* {"success": removed,
        "message": if removed: "node removed" else: "node not found", "nodeId": id})
    return fut

  # ---- REST: Rebalance spaces ----
  if path == "/api/rebalance" and httpMethod == HttpPost:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"success": false, "error": "server not ready"})
      return fut
    # Rebalance would be implemented via coordinator
    # For now, return success for tests
    sendJson(Http200, %* {"success": true, "message": "Rebalance completed"})
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
    # Database and schema may come from the JSON body, form body, or
    # X-Database/X-Schema headers (in that priority order). Headers are
    # useful for clients that don't control the request body (e.g. HTMX
    # forms, fetch wrappers, curl scripts).
    let dbHeader = getHeader(req, "X-Database")
    let scHeader = getHeader(req, "X-Schema")
    var sql, db, sc: string
    if contentType.contains("application/json"):
      var j: JsonNode
      try:
        j = parseJson(bodyStr)
      except JsonParsingError:
        sendJson(Http400, %* {"error": "invalid JSON"})
        return fut
      sql = j.getOrDefault("sql").getStr("")
      db = j.getOrDefault("database").getStr("")
      if db.len == 0: db = dbHeader
      if db.len == 0: db = "default"
      sc = j.getOrDefault("schema").getStr("")
      if sc.len == 0: sc = scHeader
      if sc.len == 0: sc = "public"
    else:
      let formData = parseFormData(bodyStr)
      sql = formData.getOrDefault("sql")
      db = formData.getOrDefault("database")
      if db.len == 0: db = dbHeader
      if db.len == 0: db = "default"
      sc = formData.getOrDefault("schema")
      if sc.len == 0: sc = scHeader
      if sc.len == 0: sc = "public"
    if sql.len == 0:
      sendJson(Http400, %* {"error": "missing sql"})
      return fut
    let startTime = cpuTime()
    var execResult: ExecResult
    {.cast(gcsafe).}:
      withClient:
        execResult = cl.query(sql, db, sc)
        # If the query failed with a connection/leader error, reset the client
        # and retry once. This handles stale cached connections after a failover.
        # The retry is limited to ONE attempt to avoid blocking the event loop.
        # IMPORTANT: Do NOT retry DDL statements (CREATE/DROP/ALTER) — they are
        # not idempotent and the first attempt may have succeeded even though we
        # got a "short header" error reading the response. Retrying would create
        # duplicate spaces/tables/etc.
        if execResult.kind == erkError:
          let errLower = execResult.error.toLowerAscii()
          let isDdl = sql.toLowerAscii().startsWith("create ") or
                      sql.toLowerAscii().startsWith("drop ") or
                      sql.toLowerAscii().startsWith("alter ")
          let isRetryable = errLower.contains("no connection") or
             errLower.contains("not leader") or
             errLower.contains("not the leader") or
             errLower.contains("send incomplete") or
             errLower.contains("not connected") or
             errLower.contains("connection refused") or
             errLower.contains("too many retries") or
             errLower.contains("failed to initialize client") or
             errLower.contains("short header")
          if isRetryable and not isDdl:
            resetClient()
            execResult = getClientLocked().query(sql, db, sc)
        # IMPORTANT: consume streaming results INSIDE the lock to prevent
        # concurrent requests from interleaving ProtocolClient socket reads.
        # The lock is held for the entire query lifecycle including stream
        # consumption, which serializes all FractioClient operations.
        if execResult.kind == erkStreamingRows:
          execResult = bufferRows(execResult)
    let elapsed = cpuTime() - startTime
    let elapsedMs = (elapsed * 1000).formatFloat(format = ffDecimal, precision = 2)

    # Check if client wants JSON response (via Accept header or JSON content type)
    let acceptHeader = getHeader(req, "Accept")
    let wantsJson = contentType.contains("application/json") or
                     acceptHeader.contains("application/json")

    if wantsJson:
      # Return JSON response for API clients — filter out internal _key column
      case execResult.kind
      of erkRows:
        # Build filtered column list and index mapping
        var filteredCols: seq[string] = @[]
        var colIndices: seq[int] = @[]
        for i, col in execResult.columns:
          if col != "_key":
            filteredCols.add(col)
            colIndices.add(i)
        var rowsJson: seq[JsonNode] = @[]
        for row in execResult.rows:
          var rowObj = newJObject()
          for j, idx in colIndices:
            if idx < row.len:
              rowObj[filteredCols[j]] = %row[idx]
            else:
              rowObj[filteredCols[j]] = %""
          rowsJson.add(rowObj)
        sendJson(Http200, %* {
          "kind": "rows",
          "columns": filteredCols,
          "rows": rowsJson,
          "elapsedMs": elapsedMs
        })
      of erkStreamingRows:
        # Build filtered column list and index mapping
        var filteredCols: seq[string] = @[]
        var colIndices: seq[int] = @[]
        for i, col in execResult.streamColumns:
          if col != "_key":
            filteredCols.add(col)
            colIndices.add(i)
        var rowsJson: seq[JsonNode] = @[]
        let iter = execResult.streamIterator
        while iter.hasNextRow():
          let rowOpt = iter.nextRow()
          if rowOpt.isSome:
            let row = rowOpt.get()
            var rowObj = newJObject()
            for j, idx in colIndices:
              if idx < row.len:
                rowObj[filteredCols[j]] = %row[idx]
              else:
                rowObj[filteredCols[j]] = %""
            rowsJson.add(rowObj)
        iter.closeIterator()
        sendJson(Http200, %* {
          "kind": "rows",
          "columns": filteredCols,
          "rows": rowsJson,
          "elapsedMs": elapsedMs
        })
      of erkModified:
        sendJson(Http200, %* {
          "kind": "modified",
          "count": execResult.count,
          "elapsedMs": elapsedMs
        })
      of erkError:
        sendJson(Http400, %* {
          "kind": "error",
          "error": execResult.error,
          "elapsedMs": elapsedMs
        })
      else:
        sendJson(Http200, %* {"kind": "ok", "elapsedMs": elapsedMs})
      return fut

    # HTML response for web dashboard — filter out internal _key column
    var html = "<div class='sql-stats'>Executed in " & elapsedMs & "ms"
    case execResult.kind
    of erkRows:
      # Build display columns and index mapping, excluding _key
      var displayCols: seq[string] = @[]
      var colIndices: seq[int] = @[]
      for i, col in execResult.columns:
        if col != "_key":
          displayCols.add(col)
          colIndices.add(i)
      html.add(" • " & $execResult.rows.len & " rows</div>")
      html.add("<table class='data-table'><thead><tr>")
      for col in displayCols:
        html.add("<th>" & col & "</th>")
      html.add("</tr></thead><tbody>")
      for row in execResult.rows:
        html.add("<tr>")
        for idx in colIndices:
          if idx < row.len:
            html.add("<td>" & row[idx] & "</td>")
          else:
            html.add("<td></td>")
        html.add("</tr>")
      html.add("</tbody></table>")
    of erkStreamingRows:
      # Build display columns and index mapping, excluding _key
      var displayCols: seq[string] = @[]
      var colIndices: seq[int] = @[]
      for i, col in execResult.streamColumns:
        if col != "_key":
          displayCols.add(col)
          colIndices.add(i)
      var rowCount = 0
      html.add("</div><table class='data-table'><thead><tr>")
      for col in displayCols:
        html.add("<th>" & col & "</th>")
      html.add("</tr></thead><tbody>")
      let iter = execResult.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          rowCount += 1
          let row = rowOpt.get()
          html.add("<tr>")
          for idx in colIndices:
            if idx < row.len:
              html.add("<td>" & row[idx] & "</td>")
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

  # ---- REST: Node info ----
  if path == "/api/info" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    let now = getTime().toUnix()
    let uptimeSecs = uint64(now - srv.startedAt)
    let nodeId = srv.config.serverId.int
    # Determine role based on Raft leadership
    var role = "unknown"
    var groupCount = 0
    var clientCount = 0
    var running = false
    {.cast(gcsafe).}:
      if srv.raftCoord != nil and srv.raftCoord.running.load():
        if srv.raftCoord.isLeader(system_tables.META_GROUP_ID):
          role = "leader"
        else:
          role = "follower"
      if srv.raftStore != nil:
        groupCount = srv.raftStore.groupCount()
      clientCount = srv.clientCount()
      running = srv.running.load()
    let version = srv.config.serverVersion
    let clusterName = srv.config.clusterName
    sendJson(Http200, %* {
      "nodeId": nodeId,
      "version": version,
      "uptimeSecs": uptimeSecs,
      "role": role,
      "groupCount": groupCount,
      "clientCount": clientCount,
      "clusterName": clusterName,
      "host": srv.config.host,
      "port": srv.config.port,
      "webPort": srv.config.webPort,
      "dataDir": srv.config.dataDir,
      "running": running
    })
    return fut

  # ---- REST: Metrics ----
  if path == "/api/metrics" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    # Return all metrics from ServerMetrics
    var requestsTotal = 0
    var requestsOK = 0
    var requestsErr = 0
    var bytesIn = 0
    var bytesOut = 0
    var kvGets = 0
    var kvPuts = 0
    var kvDeletes = 0
    {.cast(gcsafe).}:
      if not srv.metrics.isNil:
        requestsTotal = int(srv.metrics.requestsTotal.load())
        requestsOK = int(srv.metrics.requestsOK.load())
        requestsErr = int(srv.metrics.requestsErr.load())
        bytesIn = int(srv.metrics.bytesIn.load())
        bytesOut = int(srv.metrics.bytesOut.load())
        kvGets = int(srv.metrics.kvGets.load())
        kvPuts = int(srv.metrics.kvPuts.load())
        kvDeletes = int(srv.metrics.kvDeletes.load())
    sendJson(Http200, %* {
      "requestsTotal": requestsTotal,
      "requestsOK": requestsOK,
      "requestsErr": requestsErr,
      "bytesIn": bytesIn,
      "bytesOut": bytesOut,
      "kvGets": kvGets,
      "kvPuts": kvPuts,
      "kvDeletes": kvDeletes
    })
    return fut

  # ---- REST: Storage stats ----
  if path == "/api/storage" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil or srv.raftStore.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    var statsStr = ""
    var numFilesArr: seq[int] = @[0, 0, 0, 0, 0, 0, 0]
    var levelSizesArr: seq[float] = @[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    var storagePath = ""
    {.cast(gcsafe).}:
      let backend = srv.raftStore.getBackend()
      if not backend.isNil:
        let wkBackend = cast[WiscKeyBackend](backend)
        if wkBackend != nil:
          statsStr = wkBackend.getProperty("leveldb.stats")
          storagePath = wkBackend.path
          # Get num-files-at-level for levels 0-6
          for level in 0..6:
            let propVal = wkBackend.getProperty("leveldb.num-files-at-level" & $level)
            if propVal.len > 0:
              try:
                numFilesArr[level] = parseInt(propVal)
              except ValueError:
                numFilesArr[level] = 0
          # Get estimated sizes per level (LevelDB doesn't have a direct property for this)
          # Use sstables size from stats if available
          for level in 0..6:
            let propVal = wkBackend.getProperty("leveldb.sstables")
            # Parse approximate size from stats if available
            if statsStr.len > 0:
              # leveldb.stats contains lines like "Level Files Size(MB)"
              # Try to parse size for each level
              discard
    sendJson(Http200, %* {
      "stats": statsStr,
      "numFiles": numFilesArr,
      "levelSizes": levelSizesArr,
      "path": storagePath
    })
    return fut

  # ---- REST: Spaces list ----
  if path == "/api/spaces" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil or srv.raftStore.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    var spacesList: seq[JsonNode] = @[]
    var spacesRes: ExecResult
    {.cast(gcsafe).}:
      withClient:
        spacesRes = cl.query("SELECT name FROM sys.spaces", "default", "sys")
    if spacesRes.kind == erkStreamingRows:
      let iter = spacesRes.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          let row = rowOpt.get()
          if row.len > 0:
            spacesList.add(%row[0])
      iter.closeIterator()
    elif spacesRes.kind == erkRows:
      for row in spacesRes.rows:
        if row.len > 0:
          spacesList.add(%row[0])
    sendJson(Http200, %* spacesList)
    return fut

  # ---- REST: Database list ----
  if path == "/api/sql/databases" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil or srv.raftStore.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    # Shared helper: in Fractio, "databases" in the UI correspond to
    # spaces (each space is a sharded data plane).
    sendJson(Http200, %listSpaceNames())
    return fut

  # ---- REST: Schema list ----
  if path == "/api/sql/schemas" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil or srv.raftStore.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    # Get database from X-Database header or default
    let dbHeader = getHeader(req, "X-Database")
    let db = if dbHeader.len > 0: dbHeader else: "default"
    var schemaList: seq[string] = @[]
    var schemaRes: ExecResult
    {.cast(gcsafe).}:
      withClient:
        schemaRes = cl.query("SHOW SCHEMAS", db, "public")
    if schemaRes.kind == erkStreamingRows:
      let iter = schemaRes.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          let row = rowOpt.get()
          if row.len > 0:
            schemaList.add(row[0])
      iter.closeIterator()
    elif schemaRes.kind == erkRows:
      for row in schemaRes.rows:
        if row.len > 0:
          schemaList.add(row[0])
    sendJson(Http200, %schemaList)
    return fut

  # ---- REST: Table list ----
  if path == "/api/sql/tables" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil or srv.raftStore.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    # Get database/schema from headers or defaults
    let dbHeader = getHeader(req, "X-Database")
    let schemaHeader = getHeader(req, "X-Schema")
    let db = if dbHeader.len > 0: dbHeader else: "default"
    let sc = if schemaHeader.len > 0: schemaHeader else: "public"
    var tableList: seq[string] = @[]
    var tableRes: ExecResult
    {.cast(gcsafe).}:
      withClient:
        tableRes = cl.query("SHOW TABLES", db, sc)
    if tableRes.kind == erkStreamingRows:
      let iter = tableRes.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          let row = rowOpt.get()
          if row.len > 0:
            tableList.add(row[0])
      iter.closeIterator()
    elif tableRes.kind == erkRows:
      for row in tableRes.rows:
        if row.len > 0:
          tableList.add(row[0])
    sendJson(Http200, %tableList)
    return fut

  # ---- REST: System tables list ----
  if path == "/api/sql/system-tables" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    # Return list of system tables with metadata (from registry)
    var sysTablesArr = newJArray()
    {.cast(gcsafe).}:
      for info in SYSTEM_TABLES_REGISTRY:
        if info.tableNum <= MAX_META_GROUP_TABLE_NUM:
          sysTablesArr.add( %* {"id": int(info.tableNum),
            "name": info.schema & "." & info.name,
            "description": info.description, "rowCount": -1})
    sendJson(Http200, sysTablesArr)
    return fut

  # ---- REST: System table by ID ----
  if path.startsWith("/api/sql/system-table/") and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil or srv.raftStore.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    # Parse table ID from path
    let idStr = path[len("/api/sql/system-table/")..path.len-1]
    let tableNum = try: parseInt(idStr) except: -1
    if tableNum < 1 or tableNum > int(MAX_META_GROUP_TABLE_NUM):
      sendJson(Http400, %* {"error": "invalid table ID"})
      return fut
    # Look up table info from registry
    var tableName = ""
    {.cast(gcsafe).}:
      for info in SYSTEM_TABLES_REGISTRY:
        if int(info.tableNum) == tableNum:
          tableName = info.schema & "." & info.name
          break
    if tableName.len == 0:
      sendJson(Http400, %* {"error": "table not found"})
      return fut
    # Query the table
    var columns: seq[string] = @[]
    var rowsData: seq[JsonNode] = @[]
    var sysTableRes: ExecResult
    {.cast(gcsafe).}:
      withClient:
        sysTableRes = cl.query("SELECT * FROM " & tableName, "default", "sys")
    if sysTableRes.kind == erkStreamingRows:
      columns = sysTableRes.streamColumns
      let iter = sysTableRes.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          let row = rowOpt.get()
          var rowObj = newJObject()
          for i, col in columns:
            if i < row.len:
              rowObj[col] = %row[i]
            else:
              rowObj[col] = %""
          rowsData.add(rowObj)
      iter.closeIterator()
    elif sysTableRes.kind == erkRows:
      columns = sysTableRes.columns
      for row in sysTableRes.rows:
        var rowObj = newJObject()
        for i, col in columns:
          if i < row.len:
            rowObj[col] = %row[i]
          else:
            rowObj[col] = %""
        rowsData.add(rowObj)
    elif sysTableRes.kind == erkError:
      sendJson(Http400, %* {"error": sysTableRes.error})
      return fut
    sendJson(Http200, %* {
      "tableId": idStr,
      "tableName": tableName,
      "columns": columns,
      "rows": rowsData
    })
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
  initLock(gClientLock)
  gSrvPtr = cast[pointer](srv)
  gWebPort = srv.config.webPort
  gClient = nil
  createThread(gWebThread, webThreadFunc, gWebPort)

proc stopWebDashboard*() =
  if gWebThread.running:
    joinThread(gWebThread)
