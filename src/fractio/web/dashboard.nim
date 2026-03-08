# Fractio Web Management Dashboard — HTTP backend
#
# Runs a Mummy HTTP server on ServerConfig.webPort.
# REST API handlers read directly from ProtocolServer in-memory state.
#
# Build:
#   1. nim js -d:release -o:src/fractio/web/static/app.js src/fractio/web/frontend.nim
#   2. nim c --mm:atomicArc --threads:on -p:src -o:bin/fractio src/fractio/cli/main.nim
#
# Call launchWebDashboard(server) after server.start() to activate the dashboard.

import mummy, mummy/routers
import std/[json, strutils, strformat, times, os, atomics]
import ../protocol/server as pserver
import ../protocol/messages/cluster as clusterMsgs

# ---------------------------------------------------------------------------
# Static assets embedded at compile time
# ---------------------------------------------------------------------------

const appJs = staticRead("static/app.js")

const htmlShell = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fractio</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@shoelace-style/shoelace@2.18.0/cdn/themes/light.css">
<script type="module" src="https://cdn.jsdelivr.net/npm/@shoelace-style/shoelace@2.18.0/cdn/shoelace-autoloader.js"></script>
<style>
/* ---- Shoelace design-token overrides: red/black accent on white ---- */
:root{
  --sl-color-primary-50:#fff0f0;
  --sl-color-primary-100:#ffd6d6;
  --sl-color-primary-200:#ffadad;
  --sl-color-primary-300:#ff8080;
  --sl-color-primary-400:#ff4d4d;
  --sl-color-primary-500:#e81c1c;
  --sl-color-primary-600:#c41010;
  --sl-color-primary-700:#a00000;
  --sl-color-primary-800:#7a0000;
  --sl-color-primary-900:#550000;
  --sl-color-primary-950:#330000;
  --sl-font-sans:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
}
/* ---- Layout ---- */
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%}
body{font-family:var(--sl-font-sans);background:#f8f8f8;color:#111;min-height:100vh}
.app{display:flex;flex-direction:column;min-height:100vh}
/* ---- Header ---- */
header{
  display:flex;align-items:center;gap:1rem;
  padding:0 1.75rem;height:60px;
  background:#e81c1c;
  box-shadow:0 2px 8px rgba(0,0,0,.18);
  position:sticky;top:0;z-index:100;
}
.logo{
  font-size:1.1rem;font-weight:800;color:#fff;
  letter-spacing:.1em;display:flex;align-items:center;gap:.45rem;
}
.logo-hex{font-size:1.35rem;line-height:1}
/* ---- Main content ---- */
main{flex:1;padding:1.75rem;max-width:1260px;width:100%}
/* ---- Stat cards grid ---- */
.stats-grid{
  display:grid;
  grid-template-columns:repeat(auto-fill,minmax(160px,1fr));
  gap:1rem;
}
.stats-grid sl-card::part(base){
  border-top:3px solid #e81c1c;
  text-align:center;
}
.stat-label{
  font-size:.68rem;color:#666;text-transform:uppercase;
  letter-spacing:.07em;margin-bottom:.5rem;font-weight:600;
}
.stat-value{
  font-size:1.5rem;font-weight:700;color:#e81c1c;
}
/* ---- Panel header (nodes tab) ---- */
.panel-header{display:flex;align-items:center;gap:.75rem;margin-bottom:1rem}
.panel-header h2{font-size:1.05rem;font-weight:700;color:#111;margin:0}
/* ---- Data table ---- */
.table-wrap{overflow-x:auto;margin-bottom:1.25rem}
.data-table{width:100%;border-collapse:collapse;font-size:.875rem;background:#fff;
  border:1px solid #e0e0e0;border-radius:6px;overflow:hidden}
.data-table th{
  background:#111;color:#fff;padding:.55rem .85rem;text-align:left;
  font-size:.7rem;text-transform:uppercase;letter-spacing:.07em;font-weight:600;
}
.data-table td{padding:.55rem .85rem;border-bottom:1px solid #eee;color:#222}
.data-table tbody tr:hover td{background:#fff5f5}
.data-table tbody tr:last-child td{border-bottom:none}
/* ---- Form row inside card ---- */
.form-row{display:flex;gap:.5rem;flex-wrap:wrap;align-items:flex-end;margin-bottom:.5rem}
.form-msg{font-size:.82rem;margin-top:.4rem;min-height:1.3em}
.form-msg.ok{color:#1a7f37}
.form-msg.err{color:#c41010}
/* ---- Metrics grid ---- */
.metrics-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:1rem}
.metrics-table{width:100%;font-size:.875rem;border-collapse:collapse}
.metrics-table td{padding:.35rem 0;color:#444;border-bottom:1px solid #f0f0f0}
.metrics-table tr:last-child td{border-bottom:none}
.metrics-table td:last-child{text-align:right;font-family:'SF Mono','Fira Mono',monospace;
  color:#e81c1c;font-weight:600}
/* ---- Footer ---- */
footer{
  padding:.75rem 1.75rem;background:#111;color:#888;
  font-size:.75rem;text-align:center;letter-spacing:.03em;
}
</style>
</head>
<body>
<div id="loading" style="padding:4rem;text-align:center;color:#888;font-size:1rem">
  Loading dashboard&#8230;
</div>
<script src="/app.js"></script>
</body>
</html>
"""

# ---------------------------------------------------------------------------
# Server reference — set once before mummy starts
# ---------------------------------------------------------------------------

# We store the server as a plain pointer (not traced ref) so gcsafe handlers
# can access it without Nim's GC checker complaining.  The object lifetime is
# the process lifetime, so this is safe.
var gSrvPtr {.global.}: pointer  # stores cast[pointer](ProtocolServer)

template getSrv(): pserver.ProtocolServer =
  cast[pserver.ProtocolServer](gSrvPtr)

# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------

proc jsonResp(req: Request, code: int, body: string) =
  var h: HttpHeaders
  h["Content-Type"] = "application/json"
  h["Access-Control-Allow-Origin"] = "*"
  req.respond(code, h, body)

proc htmlResp(req: Request, code: int, body: string) =
  var h: HttpHeaders
  h["Content-Type"] = "text/html; charset=utf-8"
  req.respond(code, h, body)

proc jsResp(req: Request, code: int, body: string) =
  var h: HttpHeaders
  h["Content-Type"] = "application/javascript; charset=utf-8"
  h["Cache-Control"] = "no-cache"
  req.respond(code, h, body)

# ---------------------------------------------------------------------------
# Static handlers (no server state needed)
# ---------------------------------------------------------------------------

proc handleRoot(req: Request) {.gcsafe.} =
  htmlResp(req, 200, htmlShell)

proc handleAppJs(req: Request) {.gcsafe.} =
  jsResp(req, 200, appJs)

proc handleNotFound(req: Request) {.gcsafe.} =
  var h: HttpHeaders
  h["Content-Type"] = "text/plain"
  req.respond(404, h, "not found")

# ---------------------------------------------------------------------------
# REST handlers
# ---------------------------------------------------------------------------

proc roleStr(r: uint8): string =
  case r
  of 1'u8: "leader"
  of 2'u8: "follower"
  of 3'u8: "candidate"
  else: "unknown"

proc handleInfo(req: Request) {.gcsafe.} =
  let srv = getSrv()
  if srv.isNil:
    jsonResp(req, 503, """{"error":"server not ready"}""")
    return
  let nowSecs = getTime().toUnix()
  let uptime = uint64(max(0'i64, nowSecs - srv.startedAt))
  let j = %*{
    "nodeId":      srv.config.serverId.int,
    "version":     srv.config.serverVersion,
    "uptimeSecs":  uptime,
    "role":        roleStr(1'u8),
    "shardCount":  0,
    "clientCount": srv.clientCount(),
    "clusterName": srv.config.clusterName,
  }
  jsonResp(req, 200, $j)

proc handleHealth(req: Request) {.gcsafe.} =
  let srv = getSrv()
  if srv.isNil:
    jsonResp(req, 503, """{"error":"server not ready"}""")
    return
  let nodes = srv.nodeRegistry.listNodes()
  let rc = nodes.len
  let j = %*{
    "status":          0,
    "leaderOK":        true,
    "replicaCount":    rc,
    "healthyReplicas": rc,
    "clusterName":     srv.config.clusterName,
  }
  jsonResp(req, 200, $j)

proc handleMetrics(req: Request) {.gcsafe.} =
  let srv = getSrv()
  if srv.isNil:
    jsonResp(req, 503, """{"error":"server not ready"}""")
    return
  let m = srv.metrics
  let j = %*{
    "requestsTotal":  m.requestsTotal.load(),
    "requestsOK":     m.requestsOK.load(),
    "requestsErr":    m.requestsErr.load(),
    "bytesIn":        m.bytesIn.load(),
    "bytesOut":       m.bytesOut.load(),
    "kvGets":         m.kvGets.load(),
    "kvPuts":         m.kvPuts.load(),
    "kvDeletes":      m.kvDeletes.load(),
    "activeTxns":     0,
    "committedTxns":  0,
    "abortedTxns":    0,
  }
  jsonResp(req, 200, $j)

proc handleNodesGet(req: Request) {.gcsafe.} =
  let srv = getSrv()
  if srv.isNil:
    jsonResp(req, 503, """{"error":"server not ready"}""")
    return
  let entries = srv.nodeRegistry.listNodes()
  var arr = newJArray()
  for e in entries:
    arr.add(%*{
      "nodeId":     e.nodeId.int,
      "host":       e.host,
      "raftPort":   e.raftPort.int,
      "clientPort": e.clientPort.int,
      "status":     e.status.int,
    })
  jsonResp(req, 200, $arr)

proc handleNodesPost(req: Request) {.gcsafe.} =
  let srv = getSrv()
  if srv.isNil:
    jsonResp(req, 503, """{"error":"server not ready"}""")
    return
  var j: JsonNode
  try: j = parseJson(req.body)
  except JsonParsingError:
    jsonResp(req, 400, """{"success":false,"message":"invalid JSON body"}""")
    return

  let nodeId = uint16(j.getOrDefault("nodeId").getInt(0))
  let host = j.getOrDefault("host").getStr("")
  let raftPort = uint16(j.getOrDefault("raftPort").getInt(0))
  let clientPort = uint16(j.getOrDefault("clientPort").getInt(0))

  if nodeId == 0:
    jsonResp(req, 400, """{"success":false,"message":"nodeId 0 is reserved"}""")
    return
  if host == "":
    jsonResp(req, 400, """{"success":false,"message":"host must not be empty"}""")
    return

  let entry = pserver.ClusterNodeEntry(
    nodeId: nodeId, host: host,
    raftPort: raftPort, clientPort: clientPort,
    status: clusterMsgs.NodeStatusActive,
  )
  srv.nodeRegistry.addNode(entry)
  if srv.config.dataDir != "":
    pserver.saveRegistry(srv.nodeRegistry,
      srv.config.dataDir / "node_registry.dat")

  jsonResp(req, 200, $ %*{"success": true,
    "message": "node " & $nodeId & " joined"})

proc handleNodesDelete(req: Request) {.gcsafe.} =
  let srv = getSrv()
  if srv.isNil:
    jsonResp(req, 503, """{"error":"server not ready"}""")
    return
  # Extract ID from path /api/nodes/:id
  let parts = req.path.split('/')
  var nodeId: int
  try: nodeId = parseInt(parts[^1])
  except ValueError:
    jsonResp(req, 400, """{"success":false,"message":"invalid node id in path"}""")
    return

  let removed = srv.nodeRegistry.removeNode(uint16(nodeId))
  if removed and srv.config.dataDir != "":
    pserver.saveRegistry(srv.nodeRegistry,
      srv.config.dataDir / "node_registry.dat")

  if removed:
    jsonResp(req, 200, $ %*{"success": true,
      "message": "node " & $nodeId & " removed"})
  else:
    jsonResp(req, 404, $ %*{"success": false,
      "message": "node " & $nodeId & " not found"})

# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------

var gMummyServer {.global.}: Server

proc webServeThread(port: int) {.thread, gcsafe.} =
  {.cast(gcsafe).}:
    gMummyServer.serve(Port(port))

proc launchWebDashboard*(srv: pserver.ProtocolServer) {.gcsafe, raises: [CatchableError].} =
  ## Start the Mummy HTTP server in a background thread.
  ## Must be called after server.start().
  gSrvPtr = cast[pointer](srv)

  var router: Router
  router.notFoundHandler = handleNotFound
  router.addRoute("GET",    "/",             handleRoot)
  router.addRoute("GET",    "/app.js",       handleAppJs)
  router.addRoute("GET",    "/api/info",     handleInfo)
  router.addRoute("GET",    "/api/health",   handleHealth)
  router.addRoute("GET",    "/api/metrics",  handleMetrics)
  router.addRoute("GET",    "/api/nodes",    handleNodesGet)
  router.addRoute("POST",   "/api/nodes",    handleNodesPost)
  router.addRoute("DELETE", "/api/nodes/**", handleNodesDelete)

  try:
    gMummyServer = newServer(router)
    let tRef = new Thread[int]
    createThread(tRef[], webServeThread, srv.config.webPort)
  except CatchableError as e:
    try: echo "[web] failed to start: " & e.msg
    except CatchableError: discard
