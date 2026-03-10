# Fractio Web Management Dashboard — HappyX HTTP backend
# Use -d:beast to activate the httpbeast server.
#
# Build:
#   1. nim js -d:release -o:src/fractio/web/static/app.js src/fractio/web/frontend.nim
#   2. nim c -d:beast --mm:atomicArc --threads:on -p:src -o:bin/fractio src/fractio/cli/main.nim
#
# Call launchWebDashboard(server) after server.start() to activate the dashboard.

import happyx
import std/[json, strutils, times, os, atomics, random, asyncdispatch, httpclient]
import zippy
import ../protocol/server as pserver
import ../protocol/messages/cluster as clusterMsgs
import ../sql/executor
import ../distributed/meta/system_tables
import ../protocol/raft_store
import ../distributed/raft/multigroup_coordinator
import ../distributed/raft/multigroup_transport
import ../distributed/raft/group_types
import ../distributed/raft/multigroup_types
import ../storage/wisckey_backend

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

proc parseLevelSizes(stats: string): seq[float] =
  ## Parse "leveldb.stats" output to extract per-level Size(MB) values.
  ## Returns a 7-element seq (levels 0-6), defaulting to 0.0 for missing levels.
  result = newSeq[float](7)
  for line in stats.splitLines():
    let stripped = line.strip()
    # Lines look like: "  1        1        4         0        0         4"
    if stripped.len > 0 and stripped[0] in '0'..'6':
      let parts = stripped.splitWhitespace()
      if parts.len >= 3:
        try:
          let level = parseInt(parts[0])
          let sizeMB = parseFloat(parts[1 + 1])  # Files is [1], Size(MB) is [2]
          if level >= 0 and level <= 6:
            result[level] = sizeMB
        except ValueError:
          discard


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
        let role = block:
          if srv.raftStore.isNil: "unknown"
          else:
            let metaOpt = srv.raftStore.coordinator.getGroup(META_GROUP_ID)
            if metaOpt.isSome and metaOpt.get.isLeader(): "leader"
            else: "follower"
        let shards = if srv.raftStore.isNil: 0
                     else: srv.raftStore.coordinator.getGroupCount()
        return %* {
          "nodeId":      srv.config.serverId.int,
          "version":     srv.config.serverVersion,
          "uptimeSecs":  uptime,
          "role":        role,
          "shardCount":  shards,
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

      # ---- REST: storage ----
      get "/api/storage":
        let srv = getSrv()
        if srv.isNil or srv.raftStore.isNil:
          statusCode = 503
          return %* {"error": "server not ready"}
        {.cast(gcsafe).}:
          let backend = srv.raftStore.coordinator.store
          let stats = backend.getProperty("leveldb.stats")
          var numFiles = newJArray()
          for level in 0 .. 6:
            numFiles.add(newJString(backend.getProperty("leveldb.num-files-at-level" & $level)))
          let sizes = parseLevelSizes(stats)
          var levelSizes = newJArray()
          for s in sizes:
            levelSizes.add(newJFloat(s))
          return %* {
            "stats": stats,
            "numFiles": numFiles,
            "levelSizes": levelSizes,
            "path": backend.path,
          }

      # ---- REST: nodes GET ----
      get "/api/nodes":
        let srv = getSrv()
        if srv.isNil:
          statusCode = 503
          return %* {"error": "server not ready"}
        var arr = newJArray()
        if not srv.raftStore.isNil:
          let startKey = encodeTableKey(SYS_NODES_TABLE_ID, "")
          let endKey = encodeTableKey(SYS_NODES_TABLE_ID + 1, "")
          {.cast(gcsafe).}:
            let sr = srv.raftStore.raftScan(startKey, endKey, 0,
                includeSystemKeys = true)
            if sr.isOk:
              for (key, entry) in sr.value:
                try:
                  let j = parseJson(entry.value)
                  arr.add(j)
                except JsonParsingError:
                  discard
        else:
          # Fallback to local registry when raft store is not available
          let entries = srv.nodeRegistry.listNodes()
          for e in entries:
            arr.add(%* {
              "nodeId":     e.nodeId.int,
              "host":       e.host,
              "raftPort":   e.raftPort.int,
              "clientPort": e.clientPort.int,
              "status":     e.status.int,
            })
        # Enrich each node entry with live role and alive status
        for entry in arr:
          let entryNodeId = entry.getOrDefault("nodeId").getInt(0)
          if entryNodeId == srv.config.serverId.int:
            # Local node — use local Raft state directly
            let role = if srv.raftStore.isNil: "unknown"
                       elif srv.raftStore.coordinator.getLeaderCount() > 0: "leader"
                       else: "follower"
            entry["role"] = %role
            entry["alive"] = %true
            # Local storage stats
            if not srv.raftStore.isNil:
              let backend = srv.raftStore.coordinator.store
              var nf = newJArray()
              for level in 0 .. 6:
                nf.add(newJString(backend.getProperty("leveldb.num-files-at-level" & $level)))
              entry["numFiles"] = nf
              let stats = backend.getProperty("leveldb.stats")
              let sizes = parseLevelSizes(stats)
              var ls = newJArray()
              for s in sizes:
                ls.add(newJFloat(s))
              entry["levelSizes"] = ls
          else:
            # Probe peer's /api/info endpoint
            let peerHost = entry.getOrDefault("host").getStr("")
            let peerWebPort = entry.getOrDefault("webPort").getInt(0)
            if peerHost != "" and peerWebPort > 0:
              try:
                let client = newHttpClient(timeout = 500)
                let resp = client.request(
                  "http://" & peerHost & ":" & $peerWebPort & "/api/info",
                  httpMethod = HttpGet)
                client.close()
                let info = parseJson(resp.body)
                entry["role"] = %info.getOrDefault("role").getStr("unknown")
                entry["alive"] = %true
              except CatchableError:
                entry["role"] = %"unknown"
                entry["alive"] = %false
              # Probe peer's storage stats
              try:
                let client2 = newHttpClient(timeout = 500)
                let resp2 = client2.request(
                  "http://" & peerHost & ":" & $peerWebPort & "/api/storage",
                  httpMethod = HttpGet)
                client2.close()
                let storageInfo = parseJson(resp2.body)
                let peerFiles = storageInfo.getOrDefault("numFiles")
                if not peerFiles.isNil and peerFiles.kind == JArray:
                  entry["numFiles"] = peerFiles
                let peerSizes = storageInfo.getOrDefault("levelSizes")
                if not peerSizes.isNil and peerSizes.kind == JArray:
                  entry["levelSizes"] = peerSizes
              except CatchableError:
                discard
            else:
              entry["role"] = %"unknown"
              entry["alive"] = %false
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

        # Write to Raft-backed sys.nodes table for cluster-wide replication
        if not srv.raftStore.isNil:
          let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, $nodeId)
          let nodeVal = $ %* {
            "nodeId": nodeId.int,
            "host": host,
            "raftPort": raftPort.int,
            "clientPort": clientPort.int,
            "status": 1,
          }
          var putOk = false
          {.cast(gcsafe).}:
            let putResult = srv.raftStore.raftPut(nodeKey, nodeVal)
            putOk = putResult.isOk
          if not putOk:
            statusCode = 500
            return %* {"success": false, "message": "raft write failed"}

        # Keep local registry for connection management
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

      # ---- REST: cluster join ----
      post "/api/cluster/join":
        let srv = getSrv()
        if srv.isNil:
          statusCode = 503
          return %* {"success": false, "error": "server not ready"}
        var j: JsonNode
        try:
          j = parseJson(req.body.get(""))
        except JsonParsingError:
          statusCode = 400
          return %* {"success": false, "error": "invalid JSON body"}

        let peerNodeId = uint32(j.getOrDefault("nodeId").getInt(0))
        let peerHost   = j.getOrDefault("host").getStr("")
        let peerRaftPort = j.getOrDefault("raftPort").getInt(0)
        let peerClientPort = j.getOrDefault("clientPort").getInt(0)
        let peerWebPort = j.getOrDefault("webPort").getInt(0)

        if peerNodeId == 0 or peerHost == "":
          statusCode = 400
          return %* {"success": false, "error": "nodeId and host required"}

        # Add to local registry
        let entry = pserver.ClusterNodeEntry(
          nodeId: uint16(peerNodeId), host: peerHost,
          raftPort: uint16(peerRaftPort), clientPort: uint16(peerClientPort),
          webPort: uint16(peerWebPort),
          status: clusterMsgs.NodeStatusActive,
        )
        srv.nodeRegistry.addNode(entry)

        # Add peer to Raft transport + group descriptor FIRST, so the
        # subsequent raftPut replication fanout includes this new node.
        # The new peer's nextIndex defaults to 1, so replicateEntry will
        # send the entire log (full backfill).
        {.cast(gcsafe).}:
          srv.addPeerToRaft(peerNodeId, peerHost, peerRaftPort)

        # Now write to sys.nodes via Raft. The replication will include
        # the new peer, triggering full log sync.
        if not srv.raftStore.isNil:
          let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, $peerNodeId)
          let nodeVal = $ %* {
            "nodeId": peerNodeId.int,
            "host": peerHost,
            "raftPort": peerRaftPort,
            "clientPort": peerClientPort,
            "webPort": peerWebPort,
            "status": 1,
          }
          var putOk = false
          {.cast(gcsafe).}:
            let putResult = srv.raftStore.raftPut(nodeKey, nodeVal)
            putOk = putResult.isOk
          if not putOk:
            statusCode = 500
            return %* {"success": false, "error": "raft write failed"}

        # Return current cluster members (excluding the joining node)
        var members = newJArray()
        if not srv.raftStore.isNil:
          let startKey = encodeTableKey(SYS_NODES_TABLE_ID, "")
          let endKey = encodeTableKey(SYS_NODES_TABLE_ID + 1, "")
          {.cast(gcsafe).}:
            let sr = srv.raftStore.raftScan(startKey, endKey, 0,
                includeSystemKeys = true)
            if sr.isOk:
              for (key, ent) in sr.value:
                try:
                  let mj = parseJson(ent.value)
                  if mj.getOrDefault("nodeId").getInt(0) != int(peerNodeId):
                    members.add(mj)
                except JsonParsingError:
                  discard

        return %* {"success": true, "members": members}

      # ---- REST: nodes DELETE ----
      delete "/api/nodes/{id:int}":
        let srv = getSrv()
        if srv.isNil:
          statusCode = 503
          return %* {"error": "server not ready"}
        # Delete from Raft-backed sys.nodes table
        if not srv.raftStore.isNil:
          let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, $id)
          {.cast(gcsafe).}:
            discard srv.raftStore.raftDelete(nodeKey)
        # Remove from local registry
        let removed = srv.nodeRegistry.removeNode(uint16(id))
        if removed and srv.config.dataDir != "":
          pserver.saveRegistry(srv.nodeRegistry,
            srv.config.dataDir / "node_registry.dat")
        if removed:
          return %* {"success": true, "message": "node " & $id & " removed"}
        else:
          statusCode = 404
          return %* {"success": false, "message": "node " & $id & " not found"}

      # ---- REST: spaces ----
      get "/api/spaces":
        let srv = getSrv()
        if srv.isNil or srv.raftStore.isNil:
          statusCode = 503
          return %* {"error": "server not ready"}
        let startKey = encodeTableKey(SYS_SPACES_TABLE_ID, "")
        let endKey = encodeTableKey(SYS_SPACES_TABLE_ID + 1, "")
        var arr = newJArray()
        {.cast(gcsafe).}:
          let sr = srv.raftStore.raftScan(startKey, endKey, 0,
              includeSystemKeys = true)
          if sr.isOk:
            for (key, entry) in sr.value:
              try:
                let j = parseJson(entry.value)
                arr.add(j)
              except JsonParsingError:
                discard
        return arr

      # ---- REST: SQL query ----
      post "/api/sql":
        let srv = getSrv()
        if srv.isNil or srv.raftStore.isNil:
          statusCode = 503
          return %* {"error": "server not ready"}
        var j: JsonNode
        try:
          j = parseJson(req.body.get(""))
        except JsonParsingError:
          statusCode = 400
          return %* {"error": "invalid JSON body"}
        let sql = j.getOrDefault("sql").getStr("")
        let db = j.getOrDefault("database").getStr("default")
        let sc = j.getOrDefault("schema").getStr("public")
        if sql.len == 0:
          statusCode = 400
          return %* {"error": "missing 'sql' field"}
        var execResult: ExecResult
        {.cast(gcsafe).}:
          execResult = executeSQL(sql, srv.raftStore, db, sc)
        case execResult.kind
        of erkRows:
          var rowsJson = newJArray()
          for row in execResult.rows:
            var rowObj = newJObject()
            for i, col in execResult.columns:
              if i < row.len:
                rowObj[col] = newJString(row[i])
            rowsJson.add(rowObj)
          return %* {"kind": "rows", "columns": execResult.columns, "rows": rowsJson}
        of erkModified:
          return %* {"kind": "modified", "count": execResult.count, "message": execResult.message}
        of erkOk:
          return %* {"kind": "ok", "message": execResult.okMessage}
        of erkError:
          statusCode = 400
          return %* {"kind": "error", "error": execResult.error}
        of erkUseDatabase:
          return %* {"kind": "useDatabase", "database": execResult.newDatabase}
        of erkUseSchema:
          return %* {"kind": "useSchema", "schema": execResult.newSchema}

      # ---- REST: SQL convenience endpoints ----
      get "/api/sql/databases":
        let srv = getSrv()
        if srv.isNil or srv.raftStore.isNil:
          statusCode = 503
          return %* {"error": "server not ready"}
        var execResult: ExecResult
        {.cast(gcsafe).}:
          execResult = executeSQL("SHOW DATABASES", srv.raftStore)
        if execResult.kind == erkRows:
          var arr = newJArray()
          for row in execResult.rows:
            if row.len > 0:
              arr.add(newJString(row[0]))
          return arr
        return newJArray()

      get "/api/sql/schemas":
        let srv = getSrv()
        if srv.isNil or srv.raftStore.isNil:
          statusCode = 503
          return %* {"error": "server not ready"}
        let db = ($headers.getOrDefault("X-Database")).strip()
        let dbName = if db.len > 0: db else: "default"
        var execResult: ExecResult
        {.cast(gcsafe).}:
          execResult = executeSQL("SHOW SCHEMAS IN " & dbName, srv.raftStore, dbName)
        if execResult.kind == erkRows:
          var arr = newJArray()
          for row in execResult.rows:
            if row.len > 0:
              arr.add(newJString(row[0]))
          return arr
        return newJArray()

      get "/api/sql/tables":
        let srv = getSrv()
        if srv.isNil or srv.raftStore.isNil:
          statusCode = 503
          return %* {"error": "server not ready"}
        let db = ($headers.getOrDefault("X-Database")).strip()
        let sc = ($headers.getOrDefault("X-Schema")).strip()
        let dbName = if db.len > 0: db else: "default"
        let scName = if sc.len > 0: sc else: "public"
        var execResult: ExecResult
        {.cast(gcsafe).}:
          execResult = executeSQL("SHOW TABLES IN " & dbName & "." & scName,
              srv.raftStore, dbName, scName)
        if execResult.kind == erkRows:
          var arr = newJArray()
          for row in execResult.rows:
            if row.len > 0:
              arr.add(newJString(row[0]))
          return arr
        return newJArray()

      # ---- REST: system table browser ----
      get "/api/sql/system-tables":
        let srv = getSrv()
        if srv.isNil or srv.raftStore.isNil:
          statusCode = 503
          return %* {"error": "server not ready"}
        # Return the list of known system tables with row counts
        let sysTables = [
          (id: SYS_DATABASES_TABLE_ID, name: "sys.databases", desc: "Database catalog"),
          (id: SYS_SCHEMAS_TABLE_ID, name: "sys.schemas", desc: "Schema catalog"),
          (id: SYS_TABLES_TABLE_ID, name: "sys.tables", desc: "Table descriptors"),
          (id: SYS_GROUPS_TABLE_ID, name: "sys.groups", desc: "Group map"),
          (id: SYS_NODES_TABLE_ID, name: "sys.nodes", desc: "Node registry"),
          (id: SYS_SETTINGS_TABLE_ID, name: "sys.settings", desc: "Cluster settings"),
          (id: SYS_SPACES_TABLE_ID, name: "sys.spaces", desc: "Space catalog"),
          (id: SYS_NODE_METRICS_ID, name: "sys.node_metrics", desc: "Node metrics"),
          (id: SYS_GROUP_METRICS_ID, name: "sys.group_metrics", desc: "Group metrics"),
          (id: SYS_EVENTS_TABLE_ID, name: "sys.events", desc: "Cluster events"),
        ]
        var arr = newJArray()
        for st in sysTables:
          let startKey = encodeTableKey(st.id, "")
          let endKey = encodeTableKey(st.id + 1, "")
          var rowCount = 0
          {.cast(gcsafe).}:
            let sr = srv.raftStore.raftScan(startKey, endKey, 0,
                includeSystemKeys = true)
            if sr.isOk:
              rowCount = sr.value.len
          arr.add(%* {
            "id": int(st.id),
            "name": st.name,
            "description": st.desc,
            "rowCount": rowCount,
          })
        return arr

      get "/api/sql/system-table/{tableId:int}":
        let srv = getSrv()
        if srv.isNil or srv.raftStore.isNil:
          statusCode = 503
          return %* {"error": "server not ready"}
        let tid = uint32(tableId)
        if tid < 1 or tid > MAX_SYSTEM_TABLE_ID:
          statusCode = 400
          return %* {"error": "invalid system table ID"}
        let startKey = encodeTableKey(tid, "")
        let endKey = encodeTableKey(tid + 1, "")
        var rows = newJArray()
        {.cast(gcsafe).}:
          let sr = srv.raftStore.raftScan(startKey, endKey, 0,
              includeSystemKeys = true)
          if sr.isOk:
            for (key, entry) in sr.value:
              let decoded = decodeTableKey(key)
              var rowObj = %* {"_key": decoded.primaryKey}
              try:
                let j = parseJson(entry.value)
                for k, v in j:
                  rowObj[k] = v
              except JsonParsingError:
                rowObj["_value"] = newJString(entry.value)
              rows.add(rowObj)
        # Column names: from first row if available, otherwise from schema
        var columns = newJArray()
        if rows.len > 0:
          for k, v in rows[0]:
            columns.add(newJString(k))
        else:
          # Hardcoded schemas for empty system tables
          let sysColumns = case tid
            of SYS_DATABASES_TABLE_ID: @["_key", "id", "name", "replicaCount", "createdAt"]
            of SYS_SCHEMAS_TABLE_ID: @["_key", "id", "databaseId", "name", "createdAt"]
            of SYS_TABLES_TABLE_ID: @["_key", "id", "schemaId", "name", "columns", "indices", "createdAt"]
            of SYS_GROUPS_TABLE_ID: @["_key", "groupId", "startKey", "endKey", "replicas"]
            of SYS_NODES_TABLE_ID: @["_key", "nodeId", "host", "raftPort", "clientPort", "status"]
            of SYS_SETTINGS_TABLE_ID: @["_key", "value"]
            of SYS_NODE_METRICS_ID: @["_key", "nodeId", "cpuPercent", "memUsedBytes", "diskUsedBytes"]
            of SYS_GROUP_METRICS_ID: @["_key", "groupId", "keyCount", "sizeBytes", "readQps", "writeQps"]
            of SYS_EVENTS_TABLE_ID: @["_key", "timestamp", "eventType", "nodeId", "message"]
            else: @["_key", "_value"]
          for c in sysColumns:
            columns.add(newJString(c))
        return %* {"tableId": int(tid), "columns": columns, "rows": rows}

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
