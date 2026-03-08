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
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  background:#0f1117;color:#e2e8f0;min-height:100vh}
.app{display:flex;flex-direction:column;min-height:100vh}
header{display:flex;align-items:center;gap:1rem;padding:0 1.5rem;
  height:56px;background:#161b26;border-bottom:1px solid #2d3748;position:sticky;top:0;z-index:10}
.logo{font-size:1.05rem;font-weight:700;color:#63b3ed;letter-spacing:.06em;margin-right:1rem}
nav{display:flex;gap:.25rem;flex:1}
.tab-btn{background:transparent;border:none;color:#a0aec0;padding:.4rem .9rem;
  border-radius:4px;cursor:pointer;font-size:.85rem;transition:all .15s}
.tab-btn:hover{background:#2d3748;color:#e2e8f0}
.tab-active{background:#2b6cb0 !important;color:#fff !important}
.badge{padding:.25rem .7rem;border-radius:9999px;font-size:.75rem;font-weight:600;white-space:nowrap}
.badge-ok{background:#276749;color:#9ae6b4}
.badge-degraded{background:#744210;color:#fbd38d}
.badge-critical{background:#742a2a;color:#feb2b2}
.badge-unknown{background:#2d3748;color:#a0aec0}
main{flex:1;padding:1.5rem;max-width:1200px;width:100%}
.panel h2{font-size:1.05rem;font-weight:600;color:#e2e8f0;margin-bottom:1rem}
.panel h3{font-size:.85rem;font-weight:600;color:#a0aec0;margin-bottom:.5rem}
.stats-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:.75rem}
.stat-card{background:#161b26;border:1px solid #2d3748;border-radius:8px;
  padding:1rem;text-align:center}
.stat-label{font-size:.68rem;color:#718096;text-transform:uppercase;
  letter-spacing:.06em;margin-bottom:.4rem}
.stat-value{font-size:1.35rem;font-weight:700;color:#63b3ed}
.panel-header{display:flex;align-items:baseline;gap:1rem;margin-bottom:1rem}
.panel-header h2{margin-bottom:0}
.count-badge{font-size:.75rem;color:#a0aec0;background:#2d3748;
  padding:.2rem .6rem;border-radius:9999px}
.table-wrap{overflow-x:auto;margin-bottom:1.5rem}
.data-table{width:100%;border-collapse:collapse;font-size:.85rem}
.data-table th{background:#161b26;color:#718096;padding:.5rem .75rem;text-align:left;
  border-bottom:1px solid #2d3748;font-weight:500;text-transform:uppercase;
  font-size:.68rem;letter-spacing:.06em}
.data-table td{padding:.5rem .75rem;border-bottom:1px solid #1e2533}
.data-table tr:hover td{background:#1a2035}
.status-pill{padding:.15rem .5rem;border-radius:9999px;font-size:.75rem}
.status-active{background:#276749;color:#9ae6b4}
.status-draining{background:#744210;color:#fbd38d}
.status-down{background:#742a2a;color:#feb2b2}
.status-unknown{background:#2d3748;color:#a0aec0}
.btn-remove{background:#742a2a;color:#feb2b2;border:none;padding:.2rem .5rem;
  border-radius:4px;cursor:pointer;font-size:.75rem}
.btn-remove:hover{background:#9b2c2c}
.form-section{background:#161b26;border:1px solid #2d3748;border-radius:8px;padding:1rem}
.form-row{display:flex;gap:.5rem;flex-wrap:wrap;align-items:center;margin-bottom:.5rem}
.form-row input{background:#0f1117;border:1px solid #2d3748;color:#e2e8f0;
  padding:.4rem .6rem;border-radius:4px;font-size:.85rem;width:130px}
.form-row input:focus{outline:none;border-color:#2b6cb0}
.btn-primary{background:#2b6cb0;color:#fff;border:none;padding:.4rem .8rem;
  border-radius:4px;cursor:pointer;font-size:.85rem;font-weight:500}
.btn-primary:hover{background:#2c5282}
.form-msg{font-size:.8rem;margin-top:.4rem;padding:.15rem 0;min-height:1.2em}
.form-msg.ok{color:#9ae6b4}
.form-msg.err{color:#feb2b2}
.metrics-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:1rem}
.metrics-section{background:#161b26;border:1px solid #2d3748;border-radius:8px;padding:1rem}
.metrics-table{width:100%;font-size:.85rem;border-collapse:collapse}
.metrics-table td{padding:.3rem 0;color:#a0aec0}
.metrics-table td:last-child{text-align:right;font-family:'SF Mono',monospace;color:#63b3ed}
footer{padding:.75rem 1.5rem;background:#161b26;border-top:1px solid #2d3748;
  font-size:.72rem;color:#4a5568;text-align:center}
</style>
</head>
<body>
<div id="loading" style="padding:3rem;text-align:center;color:#718096">Loading dashboard&#8230;</div>
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
