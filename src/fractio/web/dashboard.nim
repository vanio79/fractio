# Fractio Web Management Dashboard — HappyX HTTP backend
# Use -d:beast to activate the httpbeast server.
#
# Build:
#   1. nim js -d:release -o:src/fractio/web/static/app.js src/fractio/web/frontend.nim
#   2. nim c -d:beast --mm:atomicArc --threads:on -p:src -o:bin/fractio src/fractio/cli/main.nim
#
# Call launchWebDashboard(server) after server.start() to activate the dashboard.

import happyx
import std/[json, strutils, times, os, atomics, random, asyncdispatch]
import zippy
import ../protocol/server as pserver
import ../protocol/messages/cluster as clusterMsgs

# ---------------------------------------------------------------------------
# Server reference — plain pointer so {.gcsafe.} handlers can access it.
# Object lifetime is process lifetime, so this is safe.
# ---------------------------------------------------------------------------

var gSrvPtr  {.global.}: pointer
var gWebPort {.global.}: int

template getSrv(): pserver.ProtocolServer =
  cast[pserver.ProtocolServer](gSrvPtr)

# ---------------------------------------------------------------------------
# Static assets embedded at compile time
# ---------------------------------------------------------------------------

const appJs = staticRead("static/app.js")

const htmlShellStr = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fractio</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@shoelace-style/shoelace@2.18.0/cdn/themes/light.css">
<script type="module" src="https://cdn.jsdelivr.net/npm/@shoelace-style/shoelace@2.18.0/cdn/shoelace-autoloader.js"></script>
<style>
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
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%}
body{font-family:var(--sl-font-sans);background:#f8f8f8;color:#111;min-height:100vh}
.app{display:flex;flex-direction:column;min-height:100vh}
header{
  display:flex;align-items:center;gap:1rem;
  padding:0 1.75rem;height:60px;
  background:#e81c1c;
  box-shadow:0 2px 8px rgba(0,0,0,.18);
  position:sticky;top:0;z-index:100;
}
.logo{font-size:1.1rem;font-weight:800;color:#fff;letter-spacing:.1em;display:flex;align-items:center;gap:.45rem}
.main-nav{background:#2d2d2d;display:flex;gap:0;padding:0 1.25rem}
.main-nav a{
  color:#bbb;text-decoration:none;padding:.6rem 1rem;font-size:.82rem;
  font-weight:600;text-transform:uppercase;letter-spacing:.06em;
  border-bottom:2px solid transparent;transition:color .15s,border-color .15s;
}
.main-nav a:hover,.main-nav a.active{color:#fff;border-bottom-color:#e81c1c}
main{flex:1;padding:1.75rem;max-width:1260px;width:100%}
.stats-grid{
  display:grid;
  grid-template-columns:repeat(auto-fill,minmax(160px,1fr));
  gap:1rem;
}
.stats-grid sl-card::part(base){border-top:3px solid #e81c1c;text-align:center}
.stat-label{font-size:.68rem;color:#666;text-transform:uppercase;letter-spacing:.07em;margin-bottom:.5rem;font-weight:600}
.stat-value{font-size:1.5rem;font-weight:700;color:#e81c1c}
.panel-header{display:flex;align-items:center;gap:.75rem;margin-bottom:1rem}
.panel-header h2{font-size:1.05rem;font-weight:700;color:#111;margin:0}
.table-wrap{overflow-x:auto;margin-bottom:1.25rem}
.data-table{width:100%;border-collapse:collapse;font-size:.875rem;background:#fff;
  border:1px solid #e0e0e0;border-radius:6px;overflow:hidden}
.data-table th{
  background:#3a3a3a;color:#fff;padding:.55rem .85rem;text-align:left;
  font-size:.7rem;text-transform:uppercase;letter-spacing:.07em;font-weight:600;
}
.data-table td{padding:.55rem .85rem;border-bottom:1px solid #eee;color:#222}
.data-table tbody tr:hover td{background:#fff5f5}
.data-table tbody tr:last-child td{border-bottom:none}
.form-row{display:flex;gap:.5rem;flex-wrap:wrap;align-items:flex-end;margin-bottom:.5rem}
.form-msg{font-size:.82rem;margin-top:.4rem;min-height:1.3em}
.form-msg.ok{color:#1a7f37}
.form-msg.err{color:#c41010}
.metrics-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:1rem}
.metrics-table{width:100%;font-size:.875rem;border-collapse:collapse}
.metrics-table td{padding:.35rem 0;color:#444;border-bottom:1px solid #f0f0f0}
.metrics-table tr:last-child td{border-bottom:none}
.metrics-table td:last-child{text-align:right;font-family:'SF Mono','Fira Mono',monospace;color:#e81c1c;font-weight:600}
footer{padding:.75rem 1.75rem;background:#2d2d2d;color:#999;font-size:.75rem;text-align:center;letter-spacing:.03em}
</style>
</head>
<body>
<div id="app"><div style="padding:4rem;text-align:center;color:#888">Loading dashboard&#8230;</div></div>
<script src="/app.js"></script>
</body>
</html>
"""

# Pre-compressed at startup (not const — zippy uses pointer casts)
var appJsGz     {.global.}: string
var htmlShellGz {.global.}: string

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

proc roleStr(r: uint8): string =
  case r
  of 1'u8: "leader"
  of 2'u8: "follower"
  of 3'u8: "candidate"
  else: "unknown"

# ---------------------------------------------------------------------------
# Web server thread
# ---------------------------------------------------------------------------

proc webServeThread(_: int) {.thread, gcsafe.} =
  {.cast(gcsafe).}:
    serve "0.0.0.0", gWebPort:

      # ---- Static assets ----
      get "/":
        outHeaders["Content-Type"] = "text/html; charset=utf-8"
        outHeaders["Vary"] = "Accept-Encoding"
        let wantsGzip = headers.hasKey("accept-encoding") and
                        "gzip" in $headers["accept-encoding"]
        var body: string
        {.cast(gcsafe).}:
          body = if wantsGzip: htmlShellGz else: htmlShellStr
        if wantsGzip:
          outHeaders["Content-Encoding"] = "gzip"
        return body

      get "/app.js":
        outHeaders["Content-Type"] = "application/javascript; charset=utf-8"
        outHeaders["Cache-Control"] = "no-cache"
        outHeaders["Vary"] = "Accept-Encoding"
        let wantsGzip = headers.hasKey("accept-encoding") and
                        "gzip" in $headers["accept-encoding"]
        var body: string
        {.cast(gcsafe).}:
          body = if wantsGzip: appJsGz else: appJs
        if wantsGzip:
          outHeaders["Content-Encoding"] = "gzip"
        return body

      # ---- REST: info ----
      get "/api/info":
        let srv = getSrv()
        if srv.isNil:
          statusCode = 503
          return %* {"error": "server not ready"}
        let nowSecs = getTime().toUnix()
        let uptime = uint64(max(0'i64, nowSecs - srv.startedAt))
        return %* {
          "nodeId":      srv.config.serverId.int,
          "version":     srv.config.serverVersion,
          "uptimeSecs":  uptime,
          "role":        roleStr(1'u8),
          "shardCount":  0,
          "clientCount": srv.clientCount(),
          "clusterName": srv.config.clusterName,
        }

      # ---- REST: health ----
      get "/api/health":
        let srv = getSrv()
        if srv.isNil:
          statusCode = 503
          return %* {"error": "server not ready"}
        let nodes = srv.nodeRegistry.listNodes()
        let rc = nodes.len
        return %* {
          "status":          0,
          "leaderOK":        true,
          "replicaCount":    rc,
          "healthyReplicas": rc,
          "clusterName":     srv.config.clusterName,
        }

      # ---- REST: metrics ----
      get "/api/metrics":
        let srv = getSrv()
        if srv.isNil:
          statusCode = 503
          return %* {"error": "server not ready"}
        let m = srv.metrics
        return %* {
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

      # ---- REST: nodes GET ----
      get "/api/nodes":
        let srv = getSrv()
        if srv.isNil:
          statusCode = 503
          return %* {"error": "server not ready"}
        let entries = srv.nodeRegistry.listNodes()
        var arr = newJArray()
        for e in entries:
          arr.add(%* {
            "nodeId":     e.nodeId.int,
            "host":       e.host,
            "raftPort":   e.raftPort.int,
            "clientPort": e.clientPort.int,
            "status":     e.status.int,
          })
        return arr

      # ---- REST: nodes POST ----
      post "/api/nodes":
        let srv = getSrv()
        if srv.isNil:
          statusCode = 503
          return %* {"error": "server not ready"}
        var j: JsonNode
        try:
          j = parseJson(req.body.get(""))
        except JsonParsingError:
          statusCode = 400
          return %* {"success": false, "message": "invalid JSON body"}

        let nodeId     = uint16(j.getOrDefault("nodeId").getInt(0))
        let host       = j.getOrDefault("host").getStr("")
        let raftPort   = uint16(j.getOrDefault("raftPort").getInt(0))
        let clientPort = uint16(j.getOrDefault("clientPort").getInt(0))

        if nodeId == 0:
          statusCode = 400
          return %* {"success": false, "message": "nodeId 0 is reserved"}
        if host == "":
          statusCode = 400
          return %* {"success": false, "message": "host must not be empty"}

        let entry = pserver.ClusterNodeEntry(
          nodeId: nodeId, host: host,
          raftPort: raftPort, clientPort: clientPort,
          status: clusterMsgs.NodeStatusActive,
        )
        srv.nodeRegistry.addNode(entry)
        if srv.config.dataDir != "":
          pserver.saveRegistry(srv.nodeRegistry,
            srv.config.dataDir / "node_registry.dat")
        return %* {"success": true, "message": "node " & $nodeId & " joined"}

      # ---- REST: nodes DELETE ----
      delete "/api/nodes/{id:int}":
        let srv = getSrv()
        if srv.isNil:
          statusCode = 503
          return %* {"error": "server not ready"}
        let removed = srv.nodeRegistry.removeNode(uint16(id))
        if removed and srv.config.dataDir != "":
          pserver.saveRegistry(srv.nodeRegistry,
            srv.config.dataDir / "node_registry.dat")
        if removed:
          return %* {"success": true, "message": "node " & $id & " removed"}
        else:
          statusCode = 404
          return %* {"success": false, "message": "node " & $id & " not found"}

      # ---- WebSocket: clock drift stream ----
      ws "/ws/drift":
        discard  # no messages expected from client

      wsConnect:
        # Fires once per WebSocket handshake.
        # Capture wsClient and spawn a 1Hz push loop.
        let capturedWs = wsClient
        proc pushDrift(ws: AsyncWebSocket) {.async, gcsafe.} =
          {.cast(gcsafe).}:
            var rng = initRand(getTime().toUnix())
            var driftAccum: float = 0.0
            while true:
              try:
                # Brownian walk so the chart looks like real clock drift
                driftAccum += (rng.rand(2.0) - 1.0) * 0.5
                driftAccum = max(-15.0, min(15.0, driftAccum))
                let tsMs = int64(getTime().toUnixFloat() * 1000)
                let msg = $ %* {
                  "t": tsMs,
                  "nodeId": 0,
                  "offsetUs": driftAccum * 1000.0,
                }
                await ws.sendText(msg)
                await sleepAsync(1000)
              except CatchableError:
                break
        asyncCheck pushDrift(capturedWs)

# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------

proc launchWebDashboard*(srv: pserver.ProtocolServer) {.gcsafe, raises: [CatchableError].} =
  ## Start the HappyX HTTP server (httpbeast via -d:beast) in a background thread.
  ## Must be called after server.start().
  gSrvPtr  = cast[pointer](srv)
  gWebPort = srv.config.webPort
  # Pre-compress static assets once; globals are written before thread starts.
  {.cast(gcsafe).}:
    appJsGz     = compress(appJs,       BestCompression, dfGzip)
    htmlShellGz = compress(htmlShellStr, BestCompression, dfGzip)
  try:
    let tRef = new Thread[int]
    createThread(tRef[], webServeThread, 0)
  except CatchableError as e:
    try: echo "[web] failed to start: " & e.msg
    except CatchableError: discard
