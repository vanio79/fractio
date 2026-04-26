# Fractio Web Management Dashboard — Pure httpbeast backend
# No HappyX dependency. HTMX + Shoelace for frontend.
# All tabs rendered server-side via HTMX partials.

import httpbeast
import std/[json, strutils, strformat, times, os, atomics, random,
    tables as stdtables, httpclient, uri, sequtils, options, asyncfutures, net]
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
import ../distributed/raft/group_types
import ../storage/wisckey_backend

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

# ---------------------------------------------------------------------------
# Static assets
# ---------------------------------------------------------------------------

# Minimal JS for Shoelace HTMX integration
const appJs = """
// Shoelace emits 'sl-change' which HTMX handles directly when hx-trigger='sl-change' is set
// We just need to re-initialize Shoelace components after HTMX swaps
document.addEventListener('htmx:afterSwap', function(e) {
  const target = e.detail.target;
  if (target) {
    // Request Shoelace to update any new components
    const shoelaceComponents = target.querySelectorAll('sl-select, sl-button, sl-input, sl-dialog');
    shoelaceComponents.forEach(function(comp) {
      if (comp.requestUpdate) comp.requestUpdate();
    });
  }
});
"""

# HTMX shell template - navigation tabs load content via HTMX
const htmlShellStr = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fractio</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@shoelace-style/shoelace@2.18.0/cdn/themes/light.css">
<script type="module" src="https://cdn.jsdelivr.net/npm/@shoelace-style/shoelace@2.18.0/cdn/shoelace-autoloader.js"></script>
<script src="https://cdn.jsdelivr.net/npm/htmx.org@2.0.4/dist/htmx.min.js"></script>
<script src="/app.js"></script>
<style>
:root{--sl-color-primary-500:#e81c1c;--sl-color-primary-600:#c41010;--sl-font-sans:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%}
body{font-family:var(--sl-font-sans);background:#f8f8f8;color:#111;min-height:100vh;display:flex;flex-direction:column}
header{display:flex;align-items:center;gap:1rem;padding:0 1.75rem;height:60px;background:#e81c1c;box-shadow:0 2px 8px rgba(0,0,0,.18);position:sticky;top:0;z-index:100}
.logo{font-size:1.1rem;font-weight:800;color:#fff;letter-spacing:.1em;display:flex;align-items:center;gap:.45rem}
.status-badge{background:#fff;color:#e81c1c;padding:.25rem .5rem;border-radius:4px;font-size:.75rem;font-weight:600}
nav{background:#2d2d2d;display:flex;gap:0;padding:0 1.25rem}
nav a{color:#bbb;text-decoration:none;padding:.6rem 1rem;font-size:.82rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em;border-bottom:2px solid transparent;transition:color .15s,border-color .15s;cursor:pointer}
nav a:hover{color:#fff;border-bottom-color:#e81c1c}
nav a.active{color:#fff;border-bottom-color:#e81c1c}
main{flex:1;padding:1.75rem;max-width:1260px;width:100%;margin:0 auto}
footer{padding:.75rem 1.75rem;background:#2d2d2d;color:#999;font-size:.75rem;text-align:center;letter-spacing:.03em}
.stats-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:1rem;margin-bottom:1.5rem}
.stat-card{background:#fff;border-radius:6px;padding:1rem;border-top:3px solid #e81c1c;text-align:center}
.stat-label{font-size:.68rem;color:#666;text-transform:uppercase;letter-spacing:.07em;margin-bottom:.5rem;font-weight:600}
.stat-value{font-size:1.5rem;font-weight:700;color:#e81c1c}
.panel{background:#fff;border-radius:6px;padding:1.25rem;margin-bottom:1.5rem;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.panel-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:1rem}
.panel-title{font-size:1.05rem;font-weight:700;color:#111}
.data-table{width:100%;border-collapse:collapse;font-size:.875rem}
.data-table th{background:#3a3a3a;color:#fff;padding:.55rem .85rem;text-align:left;font-size:.7rem;text-transform:uppercase;letter-spacing:.07em;font-weight:600}
.data-table td{padding:.55rem .85rem;border-bottom:1px solid #eee;color:#222}
.data-table tbody tr:hover td{background:#fff5f5}
.data-table tbody tr:last-child td{border-bottom:none}
.collapsible{cursor:pointer}
.collapsible-header{display:flex;align-items:center;gap:.5rem;padding:.5rem;background:#f5f5f5;border-radius:4px;margin-bottom:.5rem}
.collapsible-header:hover{background:#fff5f5}
.collapsible-content{padding:.5rem;display:none}
.collapsible.open .collapsible-content{display:block}
.metrics-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:1rem}
.metric-card{background:#fff;border-radius:6px;padding:1rem;border:1px solid #e0e0e0}
.metric-title{font-size:.75rem;color:#666;margin-bottom:.5rem;text-transform:uppercase}
.metric-value{font-size:1.25rem;font-weight:700;color:#e81c1c;font-family:'SF Mono',monospace}
.metric-table{width:100%;font-size:.875rem}
.metric-table td{padding:.35rem 0;border-bottom:1px solid #f0f0f0}
.metric-table tr:last-child td{border-bottom:none}
.metric-table td:last-child{text-align:right;font-family:'SF Mono',monospace;color:#e81c1c;font-weight:600}
.form-row{display:flex;gap:.5rem;flex-wrap:wrap;align-items:flex-end;margin-bottom:.5rem}
.form-msg{font-size:.82rem;margin-top:.4rem}
.form-msg.ok{color:#1a7f37}
.form-msg.err{color:#c41010}
.sql-editor{width:100%;min-height:150px;font-family:'SF Mono',monospace;font-size:.9rem;padding:.75rem;border:1px solid #e0e0e0;border-radius:4px;resize:vertical}
.sql-stats{font-size:.8rem;color:#666;background:#f5f5f5;padding:.4rem .6rem;border-radius:4px;margin-bottom:.5rem;font-family:'SF Mono',monospace}
.sql-results{overflow-x:auto;margin-top:1rem}
.clock-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:1rem}
.clock-card{background:#fff;border-radius:6px;padding:1rem;text-align:center}
.clock-time{font-size:1.5rem;font-weight:700;color:#e81c1c;font-family:'SF Mono',monospace}
.clock-drift{font-size:.75rem;color:#666;margin-top:.5rem}
.htmx-indicator{opacity:0;transition:opacity 200ms ease-in}
.htmx-request .htmx-indicator{opacity:1}
.htmx-request.htmx-indicator{opacity:1}
</style>
</head>
<body>
<header>
  <div class="logo">⬡ FRACTIO</div>
  <div class="status-badge" id="status-badge" hx-get="/htmx/status" hx-trigger="every 5s">OK</div>
</header>
<nav>
  <a hx-get="/htmx/dashboard" hx-target="#content" hx-push-url="/dashboard" hx-trigger="click" hx-swap="innerHTML">Dashboard</a>
  <a hx-get="/htmx/nodes" hx-target="#content" hx-push-url="/nodes" hx-trigger="click" hx-swap="innerHTML">Nodes</a>
  <a hx-get="/htmx/metrics" hx-target="#content" hx-push-url="/metrics" hx-trigger="click" hx-swap="innerHTML">Metrics</a>
  <a hx-get="/htmx/clock" hx-target="#content" hx-push-url="/clock" hx-trigger="click" hx-swap="innerHTML">Clock</a>
  <a hx-get="/htmx/storage" hx-target="#content" hx-push-url="/storage" hx-trigger="click" hx-swap="innerHTML">Storage</a>
  <a hx-get="/htmx/data" hx-target="#content" hx-push-url="/data" hx-trigger="click" hx-swap="innerHTML">Data</a>
  <a hx-get="/htmx/sql" hx-target="#content" hx-push-url="/sql" hx-trigger="click" hx-swap="innerHTML">SQL</a>
  <a hx-get="/htmx/settings" hx-target="#content" hx-push-url="/settings" hx-trigger="click" hx-swap="innerHTML">Settings</a>
</nav>
<main id="content">
  <div class="htmx-indicator" style="text-align:center;padding:2rem;color:#888">Loading...</div>
</main>
<footer>Fractio Management Console · Auto-refresh every 5s</footer>
<script>
// Initial load - wait for HTMX to be ready, then trigger correct tab
document.addEventListener('DOMContentLoaded', function() {
  var path = window.location.pathname;
  var tabMap = {
    '/': '/htmx/dashboard',
    '/dashboard': '/htmx/dashboard',
    '/nodes': '/htmx/nodes',
    '/metrics': '/htmx/metrics',
    '/clock': '/htmx/clock',
    '/storage': '/htmx/storage',
    '/data': '/htmx/data',
    '/sql': '/htmx/sql',
    '/settings': '/htmx/settings'
  };
  var htmxPath = tabMap[path] || '/htmx/dashboard';
  // Update active nav state immediately
  document.querySelectorAll('nav a').forEach(function(a) {
    var href = a.getAttribute('hx-push-url');
    var isActive = (href === path) || (path === '/' && href === '/dashboard');
    a.classList.toggle('active', isActive);
  });
  // Load content via HTMX
  htmx.ajax('GET', htmxPath, {target: '#content', swap: 'innerHTML'});
});
// Update active nav state after HTMX navigation
document.addEventListener('htmx:afterSwap', function(e) {
  if (e.detail.target && e.detail.target.id === 'content') {
    var path = window.location.pathname;
    document.querySelectorAll('nav a').forEach(function(a) {
      var href = a.getAttribute('hx-push-url');
      var isActive = (href === path) || (path === '/' && href === '/dashboard');
      a.classList.toggle('active', isActive);
    });
  }
});
</script>
</body>
</html>
"""

var appJsGz {.global.}: string
var htmlShellGz {.global.}: string

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc parseLevelSizes(stats: string): seq[float] =
  result = newSeq[float](7)
  for line in stats.splitLines():
    let stripped = line.strip()
    if stripped.len > 0 and stripped[0] in '0'..'6':
      let parts = stripped.splitWhitespace()
      if parts.len >= 3:
        try:
          let level = parseInt(parts[0])
          let sizeMB = parseFloat(parts[2])
          if level >= 0 and level <= 6:
            result[level] = sizeMB
        except ValueError:
          discard

proc rowsToJsonArray(execResult: ExecResult): JsonNode =
  case execResult.kind
  of erkRows:
    result = newJArray()
    for row in execResult.rows:
      var rowObj = newJObject()
      for i, col in execResult.columns:
        if i < row.len:
          rowObj[col] = newJString(row[i])
      result.add(rowObj)
  of erkStreamingRows:
    result = newJArray()
    let iter = execResult.streamIterator
    while iter.hasNextRow():
      let rowOpt = iter.nextRow()
      if rowOpt.isSome:
        let row = rowOpt.get()
        var rowObj = newJObject()
        for i, col in execResult.streamColumns:
          if i < row.len:
            rowObj[col] = newJString(row[i])
        result.add(rowObj)
    iter.closeIterator()
  else:
    result = newJArray()

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
  if params.hasKey(key): return params[key]
  return ""

proc parseFormData(body: string): Table[string, string] =
  ## Parse application/x-www-form-urlencoded body into key-value pairs
  result = initTable[string, string]()
  if body.len == 0:
    return
  for pair in body.split('&'):
    let eqPos = pair.find('=')
    if eqPos >= 0:
      let key = pair[0..eqPos-1]
      let rawVal = pair[eqPos+1..pair.len-1]
      # URL decode: replace + with space, decode %XX
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

proc getFormField(data: Table[string, string], key: string): string =
  if data.hasKey(key): return data[key]
  return ""

proc extractPathParam(path: string, prefix: string): string =
  if path.startsWith(prefix):
    return path[prefix.len..path.len-1]
  return ""

proc getHeader(req: Request, name: string): string =
  let headersOpt = req.headers()
  if headersOpt.isSome:
    let h = headersOpt.get()
    if h.hasKey(name): return h[name]
  return ""

proc formatUptime(secs: uint64): string =
  if secs < 60: return $secs & "s"
  if secs < 3600: return $(secs div 60) & "m " & $(secs mod 60) & "s"
  let hrs = secs div 3600
  let mins = (secs mod 3600) div 60
  return $hrs & "h " & $mins & "m"

# ---------------------------------------------------------------------------
# HTTP handler (synchronous, returns completed future)
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
    req.send(code, data, "Content-Type: text/html; charset=utf-8")

  # ---- Static: root and all tab paths (return shell) ----
  if (path == "/" or path.startsWith("/dashboard") or path.startsWith(
      "/nodes") or path.startsWith("/metrics") or path.startsWith("/clock") or
      path.startsWith("/storage") or path.startsWith("/data") or
      path.startsWith("/sql") or path.startsWith("/settings")) and httpMethod == HttpGet:
    var body: string
    {.cast(gcsafe).}:
      body = if wantsGzip: htmlShellGz else: htmlShellStr
    var hdrs = "Content-Type: text/html; charset=utf-8\r\nVary: Accept-Encoding"
    if wantsGzip: hdrs.add("\r\nContent-Encoding: gzip")
    req.send(Http200, body, hdrs)
    return fut

  # ---- Static: app.js ----
  if path == "/app.js" and httpMethod == HttpGet:
    var body: string
    {.cast(gcsafe).}:
      body = if wantsGzip: appJsGz else: appJs
    var hdrs = "Content-Type: application/javascript; charset=utf-8\r\nCache-Control: no-cache\r\nVary: Accept-Encoding"
    if wantsGzip: hdrs.add("\r\nContent-Encoding: gzip")
    req.send(Http200, body, hdrs)
    return fut

  # ---- HTMX: status badge ----
  if path == "/htmx/status" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http200, "<span style='color:#c41010'>ERROR</span>")
      return fut
    var metaLeaderOK = false
    if not srv.raftStore.isNil and not srv.raftStore.coordinator.isNil:
      metaLeaderOK = srv.raftStore.coordinator.isLeader(META_GROUP_ID) or
                     srv.raftStore.coordinator.getLeader(META_GROUP_ID) > 0
    if metaLeaderOK:
      sendHtml(Http200, "OK")
    else:
      sendHtml(Http200, "<span style='color:#c41010'>WAIT</span>")
    return fut

  # ---- HTMX: Dashboard tab ----
  if path == "/htmx/dashboard" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http503, "<div class='panel'><h1>Server not ready</h1></div>")
      return fut
    let nowSecs = getTime().toUnix()
    let uptime = uint64(max(0'i64, nowSecs - srv.startedAt))
    let role = if srv.raftStore.isNil: "unknown"
               elif srv.raftStore.coordinator.isLeader(META_GROUP_ID): "Leader"
               else: "Follower"
    let shards = if srv.raftStore.isNil: 0 else: srv.raftStore.coordinator.getGroupCount()
    var nodesResult: ExecResult
    {.cast(gcsafe).}:
      nodesResult = getClient().query("SELECT * FROM sys.nodes")
    var nodesArr = rowsToJsonArray(nodesResult)
    var spacesResult: ExecResult
    {.cast(gcsafe).}:
      spacesResult = getClient().query("SELECT * FROM sys.spaces")
    var spacesArr = rowsToJsonArray(spacesResult)
    var html = ""
    html.add("<div class='stats-grid'>")
    html.add("<div class='stat-card'><div class='stat-label'>Node ID</div><div class='stat-value'>" &
        $srv.config.serverId.int & "</div></div>")
    html.add("<div class='stat-card'><div class='stat-label'>Role</div><div class='stat-value'>" &
        role & "</div></div>")
    html.add("<div class='stat-card'><div class='stat-label'>Uptime</div><div class='stat-value'>" &
        formatUptime(uptime) & "</div></div>")
    html.add("<div class='stat-card'><div class='stat-label'>Clients</div><div class='stat-value'>" &
        $srv.clientCount() & "</div></div>")
    html.add("<div class='stat-card'><div class='stat-label'>Shards</div><div class='stat-value'>" &
        $shards & "</div></div>")
    html.add("<div class='stat-card'><div class='stat-label'>Version</div><div class='stat-value'>" &
        srv.config.serverVersion & "</div></div>")
    html.add("<div class='stat-card'><div class='stat-label'>Cluster</div><div class='stat-value'>" &
        srv.config.clusterName & "</div></div>")
    var healthyReplicas = 0
    for n in nodesArr:
      if n.getOrDefault("status").getStr("unknown") ==
          "alive": healthyReplicas += 1
    html.add("<div class='stat-card'><div class='stat-label'>Healthy</div><div class='stat-value'>" &
        $healthyReplicas & "/" & $nodesArr.len & "</div></div>")
    html.add("</div>")
    # Cluster nodes section
    html.add("<div class='panel'><div class='panel-header'><h2 class='panel-title'>Cluster Nodes</h2></div>")
    html.add("<table class='data-table'><thead><tr><th>ID</th><th>Host</th><th>Raft Port</th><th>Client Port</th><th>Status</th></tr></thead><tbody>")
    for n in nodesArr:
      html.add("<tr>")
      html.add("<td>" & n.getOrDefault("nodeId").getStr("?") & "</td>")
      html.add("<td>" & n.getOrDefault("host").getStr("?") & "</td>")
      html.add("<td>" & n.getOrDefault("raftPort").getStr("?") & "</td>")
      html.add("<td>" & n.getOrDefault("clientPort").getStr("?") & "</td>")
      html.add("<td>" & n.getOrDefault("status").getStr("?") & "</td>")
      html.add("</tr>")
    html.add("</tbody></table></div>")
    # Spaces section
    html.add("<div class='panel'><div class='panel-header'><h2 class='panel-title'>Spaces</h2></div>")
    html.add("<table class='data-table'><thead><tr><th>Name</th><th>Database</th><th>Groups</th></tr></thead><tbody>")
    for s in spacesArr:
      html.add("<tr>")
      html.add("<td>" & s.getOrDefault("name").getStr("?") & "</td>")
      html.add("<td>" & s.getOrDefault("database").getStr("?") & "</td>")
      let groupIds = s.getOrDefault("groupIds").getStr("")
      let groupCount = if groupIds.len > 0: groupIds.split(',').len else: 0
      html.add("<td>" & $groupCount & " groups</td>")
      html.add("</tr>")
    html.add("</tbody></table></div>")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: Nodes tab ----
  if path == "/htmx/nodes" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http503, "<div class='panel'><h1>Server not ready</h1></div>")
      return fut
    var nodesResult: ExecResult
    {.cast(gcsafe).}:
      nodesResult = getClient().query("SELECT * FROM sys.nodes")
    var nodesArr = rowsToJsonArray(nodesResult)
    # Enrich with live status
    for entry in nodesArr.mitems():
      let entryNodeId = entry.getOrDefault("nodeId").getInt(0)
      if entryNodeId == srv.config.serverId.int:
        let role = if srv.raftStore.isNil: "unknown"
                   elif srv.raftStore.coordinator.getLeaderCount() > 0: "leader"
                   else: "follower"
        entry["role"] = %role
        entry["alive"] = %true
      else:
        let peerHost = entry.getOrDefault("host").getStr("")
        let peerWebPort = entry.getOrDefault("webPort").getInt(9876)
        if peerHost != "":
          try:
            let c = newHttpClient(timeout = 500)
            let r = c.request("http://" & peerHost & ":" & $peerWebPort & "/api/info")
            c.close()
            let info = parseJson(r.body)
            entry["role"] = %info.getOrDefault("role").getStr("unknown")
            entry["alive"] = %true
          except CatchableError:
            entry["role"] = %"unreachable"
            entry["alive"] = %false
        else:
          entry["role"] = %"unknown"
          entry["alive"] = %false
    var html = ""
    html.add("<div class='panel'><div class='panel-header'><h2 class='panel-title'>Cluster Nodes</h2>")
    html.add("<sl-button size='small' hx-get='/htmx/nodes/add-form' hx-target='#node-form-area'>Add Node</sl-button>")
    html.add("</div><div id='node-form-area'></div>")
    html.add("<table class='data-table'><thead><tr><th>ID</th><th>Host</th><th>Raft</th><th>Client</th><th>Web</th><th>Role</th><th>Status</th><th>Action</th></tr></thead><tbody>")
    for n in nodesArr:
      let nid = n.getOrDefault("nodeId").getInt(0)
      let alive = n.getOrDefault("alive").getBool(false)
      let statusColor = if alive: "#1a7f37" else: "#c41010"
      html.add("<tr>")
      html.add("<td>" & $nid & "</td>")
      html.add("<td>" & n.getOrDefault("host").getStr("?") & "</td>")
      html.add("<td>" & n.getOrDefault("raftPort").getStr("?") & "</td>")
      html.add("<td>" & n.getOrDefault("clientPort").getStr("?") & "</td>")
      html.add("<td>" & n.getOrDefault("webPort").getStr("-") & "</td>")
      html.add("<td>" & n.getOrDefault("role").getStr("?") & "</td>")
      html.add("<td style='color:" & statusColor & "'>" & (
          if alive: "alive" else: "down") & "</td>")
      if nid != srv.config.serverId.int:
        html.add("<td><sl-button size='small' variant='danger' hx-delete='/api/nodes/" &
            $nid & "' hx-target='#content' hx-get='/htmx/nodes' hx-swap='outerHTML'>Remove</sl-button></td>")
      else:
        html.add("<td>-</td>")
      html.add("</tr>")
    html.add("</tbody></table></div>")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: Add node form ----
  if path == "/htmx/nodes/add-form" and httpMethod == HttpGet:
    var html = "<div class='panel' style='margin-top:1rem'><h3>Add Node</h3>"
    html.add("<form hx-post='/api/nodes' hx-target='#node-form-area' hx-swap='innerHTML'>")
    html.add("<div class='form-row'>")
    html.add("<sl-input name='nodeId' type='number' label='Node ID' size='small' required></sl-input>")
    html.add("<sl-input name='host' label='Host' size='small' placeholder='127.0.0.1' required></sl-input>")
    html.add("<sl-input name='raftPort' type='number' label='Raft Port' size='small' value='9001'></sl-input>")
    html.add("<sl-input name='clientPort' type='number' label='Client Port' size='small' value='9000'></sl-input>")
    html.add("<sl-button type='submit' variant='primary'>Add</sl-button>")
    html.add("</div></form></div>")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: Metrics tab ----
  if path == "/htmx/metrics" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http503, "<div class='panel'><h1>Server not ready</h1></div>")
      return fut
    let m = srv.metrics
    var html = ""
    html.add("<div class='stats-grid'>")
    html.add("<div class='stat-card'><div class='stat-label'>Requests</div><div class='stat-value'>" &
        $m.requestsTotal.load() & "</div></div>")
    html.add("<div class='stat-card'><div class='stat-label'>OK</div><div class='stat-value'>" &
        $m.requestsOK.load() & "</div></div>")
    html.add("<div class='stat-card'><div class='stat-label'>Errors</div><div class='stat-value'>" &
        $m.requestsErr.load() & "</div></div>")
    html.add("<div class='stat-card'><div class='stat-label'>Bytes In</div><div class='stat-value'>" &
        $m.bytesIn.load() & "</div></div>")
    html.add("<div class='stat-card'><div class='stat-label'>Bytes Out</div><div class='stat-value'>" &
        $m.bytesOut.load() & "</div></div>")
    html.add("</div>")
    html.add("<div class='metrics-grid'>")
    html.add("<div class='metric-card'><div class='metric-title'>KV Operations</div>")
    html.add("<table class='metric-table'><tr><td>GETs</td><td>" &
        $m.kvGets.load() & "</td></tr>")
    html.add("<tr><td>PUTs</td><td>" & $m.kvPuts.load() & "</td></tr>")
    html.add("<tr><td>DELETEs</td><td>" & $m.kvDeletes.load() & "</td></tr></table></div>")
    html.add("</div>")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: Clock tab ----
  if path == "/htmx/clock" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http503, "<div class='panel'><h1>Server not ready</h1></div>")
      return fut
    let now = getTime()
    let nowStr = format(now, "HH:mm:ss")
    let nowUnix = now.toUnix()
    var html = ""
    html.add("<div class='panel'><div class='panel-header'><h2 class='panel-title'>Clock Sync</h2></div>")
    html.add("<div class='clock-grid'>")
    html.add("<div class='clock-card'><div class='stat-label'>Local Time</div>")
    html.add("<div class='clock-time'>" & nowStr & "</div>")
    html.add("<div class='clock-drift'>unix: " & $nowUnix & "</div></div>")
    if not srv.raftStore.isNil:
      let coordinator = srv.raftStore.coordinator
      if not coordinator.isNil:
        html.add("<div class='clock-card'><div class='stat-label'>Drift Status</div>")
        html.add("<div class='clock-time'>P2P Sync</div>")
        html.add("<div class='clock-drift'>Active</div></div>")
    html.add("</div></div>")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: Storage tab ----
  if path == "/htmx/storage" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil or srv.raftStore.isNil:
      sendHtml(Http503, "<div class='panel'><h1>Server not ready</h1></div>")
      return fut
    var backend: WisckeyBackend
    var stats: string
    var levelSizes: seq[float]
    {.cast(gcsafe).}:
      backend = srv.raftStore.coordinator.store
      stats = backend.getProperty("leveldb.stats")
      levelSizes = parseLevelSizes(stats)
    var html = ""
    html.add("<div class='panel'><div class='panel-header'><h2 class='panel-title'>Storage Statistics</h2></div>")
    html.add("<pre style='font-size:.85rem;background:#f5f5f5;padding:1rem;border-radius:4px;overflow-x:auto'>" &
        stats & "</pre></div>")
    html.add("<div class='panel'><div class='panel-header'><h2 class='panel-title'>Level Sizes (MB)</h2></div>")
    html.add("<table class='data-table'><thead><tr><th>Level</th><th>Size (MB)</th></tr></thead><tbody>")
    for i, s in levelSizes:
      html.add("<tr><td>L" & $i & "</td><td>" & fmt"{s:.2f}" & "</td></tr>")
    html.add("</tbody></table></div>")
    html.add("<div class='panel'><div class='panel-header'><h2 class='panel-title'>Files per Level</h2></div>")
    html.add("<table class='data-table'><thead><tr><th>Level</th><th>Files</th></tr></thead><tbody>")
    for level in 0..6:
      {.cast(gcsafe).}:
        let nf = backend.getProperty("leveldb.num-files-at-level" & $level)
      html.add("<tr><td>L" & $level & "</td><td>" & nf & "</td></tr>")
    html.add("</tbody></table></div>")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: Data tab (standalone page for data browser) ----
  if path == "/htmx/data" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http503, "<div class='panel'><h1>Server not ready</h1></div>")
      return fut
    var dbs: seq[string] = @[]
    {.cast(gcsafe).}:
      let r = getClient().query("SHOW DATABASES")
      if r.kind == erkRows:
        for row in r.rows:
          if row.len > 0: dbs.add(row[0])
    var dbOpts = ""
    for d in dbs:
      dbOpts.add("<sl-option value='" & d & "'>" & d & "</sl-option>")
    var html = ""
    html.add("<style>.htmx-data-browser{display:flex;gap:1rem;min-height:400px}")
    html.add(".htmx-left-panel{width:260px;display:flex;flex-direction:column;gap:1rem;padding:1rem;background:#fff;border-radius:6px;border:1px solid #e0e0e0}")
    html.add(".htmx-right-panel{flex:1;background:#fff;border-radius:6px;border:1px solid #e0e0e0;overflow:hidden}")
    html.add(".htmx-selector-group{margin-bottom:.5rem}")
    html.add(".htmx-label{font-size:.85rem;font-weight:600;margin-bottom:.5rem;color:#333;display:block}")
    html.add(".htmx-tables-list{flex:1;overflow-y:auto;max-height:300px}")
    html.add(".htmx-table-item{padding:.5rem .75rem;border-radius:4px;cursor:pointer;margin-bottom:.25rem;border:1px solid #e0e0e0;background:transparent;color:#333;transition:background .15s}")
    html.add(".htmx-table-item:hover{background:#fff5f5}")
    html.add(".htmx-table-item.active{background:#e81c1c;color:#fff;border-color:#e81c1c}")
    html.add(".htmx-data-grid{padding:1rem;height:100%;overflow:auto}")
    html.add(".htmx-data-table{width:100%;border-collapse:collapse;font-size:.875rem}")
    html.add(".htmx-data-table th{background:#3a3a3a;color:#fff;padding:.55rem .85rem;text-align:left;font-size:.7rem;text-transform:uppercase;letter-spacing:.07em;font-weight:600}")
    html.add(".htmx-data-table td{padding:.55rem .85rem;border-bottom:1px solid #eee;color:#222}")
    html.add(".htmx-empty-state{display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;color:#888;gap:.5rem}</style>")
    html.add("<div class='htmx-data-browser'>")
    html.add("<div class='htmx-left-panel'>")
    html.add("<div class='htmx-selector-group'><label class='htmx-label'>Database</label>")
    html.add("<sl-select id='db-select' placeholder='Select database' size='small' hoist hx-get='/htmx/schemas' hx-trigger='sl-change' hx-target='#schema-select' hx-swap='outerHTML settle:50ms' hx-vals=\"js:{'db-select':event.target.value}\">")
    html.add(dbOpts)
    html.add("</sl-select></div>")
    html.add("<div class='htmx-selector-group'><label class='htmx-label'>Schema</label>")
    html.add("<sl-select id='schema-select' placeholder='Select schema' size='small' hoist disabled hx-get='/htmx/tables' hx-trigger='sl-change' hx-target='#tables-list' hx-vals=\"js:{'db-select':document.getElementById('db-select').value,'schema-select':event.target.value}\">")
    html.add("<sl-option value='' disabled>Select database first</sl-option>")
    html.add("</sl-select></div>")
    html.add("<div class='htmx-selector-group'><label class='htmx-label'>Tables</label>")
    html.add("<div id='tables-list' class='htmx-tables-list'><div class='htmx-empty-state'>Select schema</div></div></div>")
    html.add("</div>")
    html.add("<div class='htmx-right-panel'><div id='data-grid' class='htmx-data-grid'><div class='htmx-empty-state'><span style='font-size:2rem'>📊</span><div>Select table</div></div></div></div>")
    html.add("</div>")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: SQL tab ----
  if path == "/htmx/sql" and httpMethod == HttpGet:
    var html = ""
    html.add("<div class='panel'><div class='panel-header'><h2 class='panel-title'>SQL Query</h2></div>")
    html.add("<form hx-post='/api/sql' hx-target='#sql-results'>")
    html.add("<div class='form-row'>")
    html.add("<sl-select name='database' size='small' value='default'><sl-option value='default'>default</sl-option></sl-select>")
    html.add("<sl-select name='schema' size='small' value='public'><sl-option value='sys'>sys</sl-option><sl-option value='public'>public</sl-option></sl-select>")
    html.add("<sl-button type='submit' variant='primary'>Execute</sl-button>")
    html.add("</div>")
    html.add("<textarea class='sql-editor' name='sql' placeholder='SELECT * FROM sys.nodes'></textarea>")
    html.add("</form>")
    html.add("<div id='sql-results' class='sql-results'></div></div>")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: Settings tab ----
  if path == "/htmx/settings" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http503, "<div class='panel'><h1>Server not ready</h1></div>")
      return fut
    var html = ""
    html.add("<div class='panel'><div class='panel-header'><h2 class='panel-title'>Server Configuration</h2></div>")
    html.add("<table class='data-table'><thead><tr><th>Setting</th><th>Value</th></tr></thead><tbody>")
    html.add("<tr><td>Node ID</td><td>" & $srv.config.serverId.int & "</td></tr>")
    html.add("<tr><td>Host</td><td>" & srv.config.host & "</td></tr>")
    html.add("<tr><td>Port</td><td>" & $srv.config.port & "</td></tr>")
    html.add("<tr><td>Web Port</td><td>" & $srv.config.webPort & "</td></tr>")
    html.add("<tr><td>Cluster Name</td><td>" & srv.config.clusterName & "</td></tr>")
    html.add("<tr><td>Data Directory</td><td>" & srv.config.dataDir & "</td></tr>")
    html.add("<tr><td>Version</td><td>" & srv.config.serverVersion & "</td></tr>")
    html.add("</tbody></table></div>")
    html.add("<div class='panel'><div class='panel-header'><h2 class='panel-title'>Cluster Actions</h2></div>")
    html.add("<sl-button hx-post='/api/rebalance' hx-target='#rebalance-msg'>Rebalance Spaces</sl-button>")
    html.add("<div id='rebalance-msg' class='form-msg'></div></div>")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: schemas dropdown ----
  if path == "/htmx/schemas" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http503, "<sl-select id='schema-select'><sl-option value='' disabled>Server error</sl-option></sl-select>")
      return fut
    let dbVal = getQueryParam(queryParams, "db-select")
    if dbVal.len == 0:
      sendHtml(Http200, "<sl-select id='schema-select'><sl-option value='' disabled>Select database</sl-option></sl-select>")
      return fut
    var schemas: seq[string] = @[]
    {.cast(gcsafe).}:
      let r = getClient().query("SHOW SCHEMAS IN " & dbVal, dbVal)
      if r.kind == erkRows:
        for row in r.rows:
          if row.len > 0: schemas.add(row[0])
    var html = "<sl-select id='schema-select' placeholder='Select schema' size='small' hoist hx-get='/htmx/tables' hx-trigger='sl-change' hx-target='#tables-list' hx-vals=\"js:{'db-select':document.getElementById('db-select').value,'schema-select':event.target.value}\">"
    for s in schemas:
      html.add("<sl-option value='" & s & "'>" & s & "</sl-option>")
    html.add("</sl-select><script>setTimeout(()=>{const el=document.querySelector('#schema-select');if(el&&el.requestUpdate)el.requestUpdate()},50)</script>")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: tables list ----
  if path == "/htmx/tables" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http503, "<div class='htmx-empty-state'>Server error</div>")
      return fut
    let dbVal = getQueryParam(queryParams, "db-select")
    let schVal = getQueryParam(queryParams, "schema-select")
    if dbVal.len == 0 or schVal.len == 0:
      sendHtml(Http200, "<div class='htmx-empty-state'>Select schema</div>")
      return fut
    var tables: seq[string] = @[]
    {.cast(gcsafe).}:
      let r = getClient().query("SHOW TABLES IN " & dbVal & "." & schVal, dbVal, schVal)
      if r.kind == erkRows:
        for row in r.rows:
          if row.len > 0: tables.add(row[0])
    if tables.len == 0:
      sendHtml(Http200, "<div class='htmx-empty-state'>No tables</div>")
      return fut
    var html = ""
    for t in tables:
      html.add("<div class='htmx-table-item' hx-get='/htmx/table-data' hx-trigger='click' hx-target='#data-grid' hx-vals=\"js:{'db-select':document.getElementById('db-select').value,'schema-select':document.getElementById('schema-select').value,'table':'" &
          t & "'}\">" & t & "</div>")
    sendHtml(Http200, html)
    return fut

  # ---- HTMX: table data grid ----
  if path == "/htmx/table-data" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendHtml(Http503, "<div class='htmx-empty-state'>Server error</div>")
      return fut
    let dbVal = getQueryParam(queryParams, "db-select")
    let schVal = getQueryParam(queryParams, "schema-select")
    let tblVal = getQueryParam(queryParams, "table")
    if dbVal.len == 0 or schVal.len == 0 or tblVal.len == 0:
      sendHtml(Http200, "<div class='htmx-empty-state'>Invalid request</div>")
      return fut
    var execResult: ExecResult
    {.cast(gcsafe).}:
      execResult = getClient().query("SELECT * FROM " & dbVal & "." & schVal &
          "." & tblVal, dbVal, schVal)
    case execResult.kind
    of erkRows, erkStreamingRows:
      var cols: seq[string]
      var rows: seq[seq[string]]
      if execResult.kind == erkRows:
        cols = execResult.columns
        rows = execResult.rows
      else:
        cols = execResult.streamColumns
        rows = @[]
        let iter = execResult.streamIterator
        while iter.hasNextRow():
          let rowOpt = iter.nextRow()
          if rowOpt.isSome: rows.add(rowOpt.get())
        iter.closeIterator()
      var html = "<div style='padding:.5rem;font-size:.85rem;color:#666'>" &
          $rows.len & " rows</div>"
      html.add("<table class='htmx-data-table'><thead><tr>")
      for col in cols: html.add("<th>" & col & "</th>")
      html.add("</tr></thead><tbody>")
      for row in rows:
        html.add("<tr>")
        for i, col in cols:
          if i < row.len: html.add("<td>" & row[i] & "</td>")
          else: html.add("<td></td>")
        html.add("</tr>")
      html.add("</tbody></table>")
      sendHtml(Http200, html)
    of erkError:
      sendHtml(Http200, "<div class='htmx-empty-state' style='color:#c41010'>Error: " &
          execResult.error & "</div>")
    else:
      sendHtml(Http200, "<div class='htmx-empty-state'>No data</div>")
    return fut

  # ---- REST: info ----
  if path == "/api/info" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    let nowSecs = getTime().toUnix()
    let uptime = uint64(max(0'i64, nowSecs - srv.startedAt))
    let role = if srv.raftStore.isNil: "unknown"
               elif srv.raftStore.coordinator.isLeader(META_GROUP_ID): "leader"
               else: "follower"
    let shards = if srv.raftStore.isNil: 0 else: srv.raftStore.coordinator.getGroupCount()
    sendJson(Http200, %* {
      "nodeId": srv.config.serverId.int,
      "version": srv.config.serverVersion,
      "uptimeSecs": uptime,
      "role": role,
      "shardCount": shards,
      "clientCount": srv.clientCount(),
      "clusterName": srv.config.clusterName
    })
    return fut

  # ---- REST: health ----
  if path == "/api/health" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
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
      "clusterName": srv.config.clusterName
    })
    return fut

  # ---- REST: metrics ----
  if path == "/api/metrics" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    let m = srv.metrics
    sendJson(Http200, %* {
      "requestsTotal": m.requestsTotal.load(),
      "requestsOK": m.requestsOK.load(),
      "requestsErr": m.requestsErr.load(),
      "bytesIn": m.bytesIn.load(),
      "bytesOut": m.bytesOut.load(),
      "kvGets": m.kvGets.load(),
      "kvPuts": m.kvPuts.load(),
      "kvDeletes": m.kvDeletes.load(),
      "activeTxns": 0,
      "committedTxns": 0,
      "abortedTxns": 0
    })
    return fut

  # ---- REST: storage ----
  if path == "/api/storage" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil or srv.raftStore.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    {.cast(gcsafe).}:
      let backend = srv.raftStore.coordinator.store
      let stats = backend.getProperty("leveldb.stats")
      var numFiles = newJArray()
      for level in 0..6:
        numFiles.add(newJString(backend.getProperty(
            "leveldb.num-files-at-level" & $level)))
      let sizes = parseLevelSizes(stats)
      var levelSizes = newJArray()
      for s in sizes:
        levelSizes.add(newJFloat(s))
      sendJson(Http200, %* {"stats": stats, "numFiles": numFiles,
          "levelSizes": levelSizes, "path": backend.path})
    return fut

  # ---- REST: nodes GET ----
  if path == "/api/nodes" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    var nodesResult: ExecResult
    {.cast(gcsafe).}:
      nodesResult = getClient().query("SELECT * FROM sys.nodes")
    var arr = rowsToJsonArray(nodesResult)
    if arr.len == 0 and not srv.raftStore.isNil:
      for e in srv.nodeRegistry.listNodes():
        arr.add( %* {"nodeId": e.nodeId.int, "host": e.host,
            "raftPort": e.raftPort.int, "clientPort": e.clientPort.int,
            "status": e.status.int})
    sendJson(Http200, arr)
    return fut

  # ---- REST: nodes POST ----
  if path == "/api/nodes" and httpMethod == HttpPost:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"success": false, "message": "server not ready"})
      return fut
    let bodyOpt = req.body()
    if bodyOpt.isNone:
      sendJson(Http400, %* {"success": false, "message": "missing body"})
      return fut
    var j: JsonNode
    try:
      j = parseJson(bodyOpt.get())
    except JsonParsingError:
      sendJson(Http400, %* {"success": false, "message": "invalid JSON"})
      return fut
    let nodeId = uint16(j.getOrDefault("nodeId").getInt(0))
    let host = j.getOrDefault("host").getStr("")
    let raftPort = uint16(j.getOrDefault("raftPort").getInt(9001))
    let clientPort = uint16(j.getOrDefault("clientPort").getInt(9000))
    if nodeId == 0:
      sendJson(Http400, %* {"success": false,
          "message": "nodeId 0 is reserved"})
      return fut
    if host == "":
      sendJson(Http400, %* {"success": false,
          "message": "host must not be empty"})
      return fut
    if not srv.raftStore.isNil and not srv.mvccStore.isNil:
      let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, $nodeId)
      let nodeVal = encode(NodeRecord(nodeId: nodeId, host: host,
          raftPort: raftPort, clientPort: clientPort, status: nsAlive))
      var putOk = false
      {.cast(gcsafe).}:
        let res = srv.mvccStore.withAutoTransactionResult(proc(
            sid: uint64): MvccResult[bool] =
          let r = srv.mvccStore.txnPut(sid, nodeKey, nodeVal)
          return if r.isOk: mvccOk(true) else: mvccErr[bool](r.error)
        )
        putOk = res.isOk and res.value
      if not putOk:
        sendJson(Http500, %* {"success": false, "message": "raft write failed"})
        return fut
      srv.nodeRegistry.addNode(pserver.ClusterNodeEntry(nodeId: nodeId,
          host: host, raftPort: raftPort, clientPort: clientPort,
          status: clusterMsgs.NodeStatusActive))
      if srv.config.dataDir != "":
        pserver.saveRegistry(srv.nodeRegistry, srv.config.dataDir / "node_registry.dat")
      # Return HTML for HTMX to update
      sendHtml(Http200, "<div class='form-msg ok'>Node " & $nodeId & " added successfully</div>")
      return fut
    sendJson(Http500, %* {"success": false, "message": "storage not ready"})
    return fut

  # ---- REST: cluster join ----
  if path == "/api/cluster/join" and httpMethod == HttpPost:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"success": false, "error": "server not ready"})
      return fut
    let bodyOpt = req.body()
    if bodyOpt.isNone:
      sendJson(Http400, %* {"success": false, "error": "missing body"})
      return fut
    var j: JsonNode
    try:
      j = parseJson(bodyOpt.get())
    except JsonParsingError:
      sendJson(Http400, %* {"success": false, "error": "invalid JSON"})
      return fut
    let peerNodeId = uint32(j.getOrDefault("nodeId").getInt(0))
    let peerHost = j.getOrDefault("host").getStr("")
    let peerRaftPort = j.getOrDefault("raftPort").getInt(9001)
    let peerClientPort = j.getOrDefault("clientPort").getInt(9000)
    let peerWebPort = j.getOrDefault("webPort").getInt(9876)
    if peerNodeId == 0 or peerHost == "":
      sendJson(Http400, %* {"success": false,
          "error": "nodeId and host required"})
      return fut
    srv.nodeRegistry.addNode(pserver.ClusterNodeEntry(nodeId: uint16(
        peerNodeId), host: peerHost, raftPort: uint16(peerRaftPort),
        clientPort: uint16(peerClientPort), webPort: uint16(peerWebPort),
        status: clusterMsgs.NodeStatusActive))
    {.cast(gcsafe).}:
      srv.addPeerToRaft(peerNodeId, peerHost, peerRaftPort)
    if not srv.raftStore.isNil and not srv.mvccStore.isNil:
      let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, $peerNodeId)
      let nodeVal = encode(NodeRecord(nodeId: peerNodeId, host: peerHost,
          raftPort: uint16(peerRaftPort), clientPort: uint16(peerClientPort),
          status: nsAlive))
      var putOk = false
      {.cast(gcsafe).}:
        let res = srv.mvccStore.withAutoTransactionResult(proc(
            sid: uint64): MvccResult[bool] =
          let r = srv.mvccStore.txnPut(sid, nodeKey, nodeVal)
          return if r.isOk: mvccOk(true) else: mvccErr[bool](r.error)
        )
        putOk = res.isOk and res.value
      if not putOk:
        sendJson(Http500, %* {"success": false, "error": "raft write failed"})
        return fut
      {.cast(gcsafe).}:
        srv.raftStore.rebalanceSpaces()
      sendJson(Http200, %* {"success": true})
      return fut
    sendJson(Http500, %* {"success": false, "error": "storage not ready"})
    return fut

  # ---- REST: rebalance ----
  if path == "/api/rebalance" and httpMethod == HttpPost:
    let srv = getSrv()
    if srv.isNil or srv.raftStore.isNil:
      sendJson(Http503, %* {"success": false, "error": "server not ready"})
      return fut
    {.cast(gcsafe).}:
      srv.raftStore.rebalanceSpaces()
    # Return HTML for HTMX
    sendHtml(Http200, "<div class='form-msg ok'>Rebalance initiated</div>")
    return fut

  # ---- REST: nodes DELETE ----
  if path.startsWith("/api/nodes/") and httpMethod == HttpDelete:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    let idStr = extractPathParam(path, "/api/nodes/")
    let id = try: parseInt(idStr) except ValueError: -1
    if id < 0:
      sendJson(Http400, %* {"success": false, "message": "invalid node ID"})
      return fut
    if not srv.raftStore.isNil:
      {.cast(gcsafe).}:
        discard srv.raftStore.raftDelete(encodeTableKey(SYS_NODES_TABLE_ID, $id))
    let removed = srv.nodeRegistry.removeNode(uint16(id))
    if removed and srv.config.dataDir != "":
      pserver.saveRegistry(srv.nodeRegistry, srv.config.dataDir / "node_registry.dat")
    # Return HTML to redirect back to nodes tab
    sendHtml(Http200, "<div hx-get='/htmx/nodes' hx-trigger='load' hx-target='#content'></div>")
    return fut

  # ---- REST: spaces ----
  if path == "/api/spaces" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil or srv.raftStore.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    var spacesResult: ExecResult
    {.cast(gcsafe).}:
      spacesResult = getClient().query("SELECT * FROM sys.spaces")
    var arr = rowsToJsonArray(spacesResult)
    sendJson(Http200, arr)
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
    # Support both JSON and form-encoded bodies
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
      # Parse as application/x-www-form-urlencoded (HTMX form submissions)
      let formData = parseFormData(bodyStr)
      sql = getFormField(formData, "sql")
      db = getFormField(formData, "database")
      if db.len == 0: db = "default"
      sc = getFormField(formData, "schema")
      if sc.len == 0: sc = "public"
    if sql.len == 0:
      sendJson(Http400, %* {"error": "missing sql"})
      return fut
    # Measure execution time
    let startTime = cpuTime()
    var execResult: ExecResult
    {.cast(gcsafe).}:
      execResult = getClient().query(sql, db, sc)
    let elapsed = cpuTime() - startTime
    let elapsedMs = (elapsed * 1000).formatFloat(format = ffDecimal, precision = 2)
    # Return HTML table for HTMX with execution stats
    case execResult.kind
    of erkRows:
      var html = "<div class='sql-stats'>Executed in " & elapsedMs & "ms • " &
          $execResult.rows.len & " rows</div>"
      html.add("<table class='data-table'><thead><tr>")
      for col in execResult.columns: html.add("<th>" & col & "</th>")
      html.add("</tr></thead><tbody>")
      for row in execResult.rows:
        html.add("<tr>")
        for i, col in execResult.columns:
          if i < row.len: html.add("<td>" & row[i] & "</td>")
          else: html.add("<td></td>")
        html.add("</tr>")
      html.add("</tbody></table>")
      sendHtml(Http200, html)
    of erkStreamingRows:
      var rowCount = 0
      var html = "<table class='data-table'><thead><tr>"
      for col in execResult.streamColumns: html.add("<th>" & col & "</th>")
      html.add("</tr></thead><tbody>")
      let iter = execResult.streamIterator
      while iter.hasNextRow():
        let rowOpt = iter.nextRow()
        if rowOpt.isSome:
          rowCount += 1
          let row = rowOpt.get()
          html.add("<tr>")
          for i, col in execResult.streamColumns:
            if i < row.len: html.add("<td>" & row[i] & "</td>")
            else: html.add("<td></td>")
          html.add("</tr>")
      iter.closeIterator()
      html.add("</tbody></table>")
      html = "<div class='sql-stats'>Executed in " & elapsedMs & "ms • " &
          $rowCount & " rows</div>" & html
      sendHtml(Http200, html)
    of erkModified:
      sendHtml(Http200, "<div class='sql-stats'>Executed in " & elapsedMs &
          "ms</div><div class='form-msg ok'>" & $execResult.count & " rows affected</div>")
    of erkError:
      sendHtml(Http200, "<div class='sql-stats'>Executed in " & elapsedMs &
          "ms</div><div class='form-msg err'>Error: " & execResult.error & "</div>")
    else:
      sendHtml(Http200, "<div class='sql-stats'>Executed in " & elapsedMs & "ms</div><div class='form-msg ok'>OK</div>")
    return fut

  # ---- REST: SQL databases ----
  if path == "/api/sql/databases" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    var execResult: ExecResult
    {.cast(gcsafe).}:
      execResult = getClient().query("SHOW DATABASES")
    if execResult.kind == erkRows:
      var html = ""
      for row in execResult.rows:
        if row.len > 0:
          html.add("<sl-option value='" & row[0] & "'>" & row[0] & "</sl-option>")
      sendHtml(Http200, html)
    else:
      sendHtml(Http200, "<sl-option value='default'>default</sl-option>")
    return fut

  # ---- REST: SQL schemas ----
  if path == "/api/sql/schemas" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    let db = getHeader(req, "X-Database").strip()
    let dbName = if db.len > 0: db else: "default"
    var execResult: ExecResult
    {.cast(gcsafe).}:
      execResult = getClient().query("SHOW SCHEMAS IN " & dbName, dbName)
    if execResult.kind == erkRows:
      var arr = newJArray()
      for row in execResult.rows:
        if row.len > 0: arr.add(newJString(row[0]))
      sendJson(Http200, arr)
    else:
      sendJson(Http200, newJArray())
    return fut

  # ---- REST: SQL tables ----
  if path == "/api/sql/tables" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    let db = getHeader(req, "X-Database").strip()
    let sc = getHeader(req, "X-Schema").strip()
    let dbName = if db.len > 0: db else: "default"
    let scName = if sc.len > 0: sc else: "public"
    var execResult: ExecResult
    {.cast(gcsafe).}:
      execResult = getClient().query("SHOW TABLES IN " & dbName & "." & scName,
          dbName, scName)
    if execResult.kind == erkRows:
      var arr = newJArray()
      for row in execResult.rows:
        if row.len > 0: arr.add(newJString(row[0]))
      sendJson(Http200, arr)
    else:
      sendJson(Http200, newJArray())
    return fut

  # ---- REST: system tables ----
  if path == "/api/sql/system-tables" and httpMethod == HttpGet:
    let srv = getSrv()
    if srv.isNil or srv.raftStore.isNil:
      sendJson(Http503, %* {"error": "server not ready"})
      return fut
    var arr = newJArray()
    for st in [(SYS_DATABASES_TABLE_ID, "sys.databases"), (SYS_SCHEMAS_TABLE_ID,
        "sys.schemas"), (SYS_TABLES_TABLE_ID, "sys.tables"), (
        SYS_GROUPS_TABLE_ID, "sys.groups"), (SYS_NODES_TABLE_ID, "sys.nodes"), (
        SYS_SETTINGS_TABLE_ID, "sys.settings"), (SYS_SPACES_TABLE_ID,
        "sys.spaces")]:
      var rowCount = 0
      {.cast(gcsafe).}:
        let sr = srv.raftStore.raftScan(encodeTableKey(st[0], ""),
            makeScanEndKey(st[0]), 0, true)
        if sr.isOk: rowCount = sr.value.len
      arr.add( %* {"id": systemTableNumFromId(st[0]), "name": st[1],
          "rowCount": rowCount})
    sendJson(Http200, arr)
    return fut

  # ---- 404 ----
  req.send(Http404, "Not found")
  return fut

# ---------------------------------------------------------------------------
# Server thread
# ---------------------------------------------------------------------------

proc webServeThread(_: int) {.thread.} =
  let settings = initSettings(port = Port(gWebPort))
  {.cast(gcsafe).}:
    run(onRequestHandler, settings)

# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------

proc launchWebDashboard*(srv: pserver.ProtocolServer) {.gcsafe, raises: [
    CatchableError].} =
  gSrvPtr = cast[pointer](srv)
  gWebPort = srv.config.webPort
  {.cast(gcsafe).}:
    appJsGz = compress(appJs, BestCompression, dfGzip)
    htmlShellGz = compress(htmlShellStr, BestCompression, dfGzip)
  try:
    createThread(gWebThread, webServeThread, 0)
  except CatchableError as e:
    try: echo "[web] failed to start: " & e.msg
    except CatchableError: discard
