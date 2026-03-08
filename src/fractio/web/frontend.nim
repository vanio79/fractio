# Fractio Web Dashboard — Frontend
#
# Compiled to JavaScript with: nim js -d:release -o:static/app.js frontend.nim
#
# Uses std/dom for DOM manipulation and importjs pragmas for fetch().
# Three panels: Dashboard, Nodes, Metrics.  Auto-refreshes every 5 seconds.

import std/dom
import std/jsffi
import std/asyncjs

# ---------------------------------------------------------------------------
# JS interop
# ---------------------------------------------------------------------------

proc fetchJson(url: cstring): Future[JsObject]
    {.importjs: "fetch(#).then(function(r){return r.json()})", async.}

proc fetchWithOpts(url: cstring, opts: JsObject): Future[JsObject]
    {.importjs: "fetch(#, #).then(function(r){return r.json()})", async.}

proc jsStringify(o: JsObject): cstring
    {.importjs: "JSON.stringify(#)".}

proc setIntervalMs(fn: proc(), ms: int)
    {.importjs: "setInterval(#, #)".}

proc numFmt(n: float): cstring
    {.importjs: "Number(#).toLocaleString()".}

proc jsGet(obj: JsObject, field: cstring): JsObject
    {.importjs: "#[#]".}

proc jsParseInt(s: cstring): int
    {.importjs: "parseInt(#, 10)".}

# ---------------------------------------------------------------------------
# DOM helpers
# ---------------------------------------------------------------------------

proc el(id: string): Element = document.getElementById(id)

proc clearEl(e: Element) = e.innerHTML = ""

proc setHtml(e: Element, h: string) =
  if not e.isNil: e.innerHTML = h

proc addCls(e: Element, cls: string) =
  if not e.isNil: e.classList.add(cls)

proc rmCls(e: Element, cls: string) =
  if not e.isNil: e.classList.remove(cls)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

var currentTab = "dashboard"
var lastInfo: JsObject
var lastHealth: JsObject
var lastMetrics: JsObject
var lastNodes: JsObject

# ---------------------------------------------------------------------------
# Tab switching
# ---------------------------------------------------------------------------

proc showTab(name: string) =
  currentTab = name
  for tab in ["dashboard", "nodes", "metrics"]:
    let panel = el("panel-" & tab)
    let btn = el("tab-" & tab)
    if not panel.isNil:
      panel.style.display = if tab == name: "block" else: "none"
    if tab == name: addCls(btn, "tab-active")
    else: rmCls(btn, "tab-active")

# ---------------------------------------------------------------------------
# String helpers
# ---------------------------------------------------------------------------

proc roleStr(r: int): string =
  case r
  of 1: "Leader"
  of 2: "Follower"
  of 3: "Candidate"
  else: "Unknown"

proc uptimeStr(secs: int): string =
  let h = secs div 3600
  let m = (secs mod 3600) div 60
  let s = secs mod 60
  if h > 0: $h & "h " & $m & "m"
  elif m > 0: $m & "m " & $s & "s"
  else: $s & "s"

proc healthStr(s: int): string =
  case s
  of 0: "OK"
  of 1: "DEGRADED"
  of 2: "CRITICAL"
  else: "UNKNOWN"

proc healthCls(s: int): string =
  case s
  of 0: "badge-ok"
  of 1: "badge-degraded"
  of 2: "badge-critical"
  else: "badge-unknown"

proc nodeStatusStr(s: int): string =
  case s
  of 1: "active"
  of 2: "draining"
  of 3: "down"
  else: "unknown"

proc nodeStatusCls(s: int): string =
  case s
  of 1: "status-active"
  of 2: "status-draining"
  of 3: "status-down"
  else: "status-unknown"

# ---------------------------------------------------------------------------
# Dashboard panel
# ---------------------------------------------------------------------------

proc updateDashboard() =
  if lastInfo.isNil or lastHealth.isNil: return

  let nodeId = lastInfo.nodeId.to(int)
  let version = $lastInfo.version.to(cstring)
  let uptime = lastInfo.uptimeSecs.to(int)
  let role = lastInfo.role.to(int)
  let shards = lastInfo.shardCount.to(int)
  let clients = lastInfo.clientCount.to(int)
  let clusterName = $lastInfo.clusterName.to(cstring)
  let healthStatus = lastHealth.status.to(int)
  let replicas = lastHealth.replicaCount.to(int)
  let healthyReplicas = lastHealth.healthyReplicas.to(int)

  let hbadge = el("health-badge")
  if not hbadge.isNil:
    hbadge.innerHTML = cstring(healthStr(healthStatus))
    hbadge.className = cstring("badge " & healthCls(healthStatus))

  setHtml(el("stat-nodeid"), $nodeId)
  setHtml(el("stat-role"), roleStr(role))
  setHtml(el("stat-uptime"), uptimeStr(uptime))
  setHtml(el("stat-clients"), $clients)
  setHtml(el("stat-shards"), $shards)
  setHtml(el("stat-version"), version)
  setHtml(el("stat-cluster"), clusterName)
  setHtml(el("stat-replicas"), $healthyReplicas & " / " & $replicas)

# ---------------------------------------------------------------------------
# Nodes panel
# ---------------------------------------------------------------------------

proc refreshNodesTable() {.async.} =
  lastNodes = await fetchJson("/api/nodes")
  let tbody = el("nodes-table-body")
  if tbody.isNil: return
  clearEl(tbody)

  let arr = lastNodes.to(seq[JsObject])
  for node in arr:
    let nodeId = node.nodeId.to(int)
    let host = $node.host.to(cstring)
    let raftPort = node.raftPort.to(int)
    let clientPort = node.clientPort.to(int)
    let status = node.status.to(int)

    let tr = document.createElement("tr")
    tr.innerHTML = cstring(
      "<td>" & $nodeId & "</td>" &
      "<td>" & host & "</td>" &
      "<td>" & $raftPort & "</td>" &
      "<td>" & $clientPort & "</td>" &
      "<td><span class=\"status-pill " & nodeStatusCls(status) & "\">" &
        nodeStatusStr(status) & "</span></td>" &
      "<td><button class=\"btn-remove\" onclick=\"_removeNode(" &
        $nodeId & ")\">Remove</button></td>"
    )
    tbody.appendChild(tr)

  let countEl = el("nodes-count")
  if not countEl.isNil:
    let s = if arr.len != 1: "s" else: ""
    countEl.innerHTML = cstring($arr.len & " node" & s & " registered")

proc removeNodeById(nodeId: int) {.async.} =
  let opts = newJsObject()
  opts.method = cstring"DELETE"
  discard await fetchWithOpts(cstring("/api/nodes/" & $nodeId), opts)
  await refreshNodesTable()

proc joinNodeSubmit() {.async.} =
  let idEl = el("join-id")
  let hostEl = el("join-host")
  let raftEl = el("join-raft")
  let clientEl = el("join-client")
  let msgEl = el("join-msg")

  if idEl.isNil or hostEl.isNil or raftEl.isNil or clientEl.isNil: return

  let body = newJsObject()
  body.nodeId = jsParseInt(idEl.value)
  body.host = hostEl.value
  body.raftPort = jsParseInt(raftEl.value)
  body.clientPort = jsParseInt(clientEl.value)

  let headers = newJsObject()
  headers[cstring"Content-Type"] = cstring"application/json"

  let opts = newJsObject()
  opts.method = cstring"POST"
  opts.headers = headers
  opts.body = jsStringify(body)

  let resp = await fetchWithOpts("/api/nodes", opts)
  let success = resp.success.to(bool)
  let message = $resp.message.to(cstring)

  if not msgEl.isNil:
    msgEl.innerHTML = cstring(message)
    msgEl.className = if success: "form-msg ok" else: "form-msg err"

  if success:
    idEl.value = ""
    hostEl.value = ""
    raftEl.value = ""
    clientEl.value = ""
    await refreshNodesTable()

# ---------------------------------------------------------------------------
# Metrics panel
# ---------------------------------------------------------------------------

proc updateMetrics() =
  if lastMetrics.isNil: return

  let fields = [
    ("requests-total", cstring"requestsTotal"),
    ("requests-ok", cstring"requestsOK"),
    ("requests-err", cstring"requestsErr"),
    ("bytes-in", cstring"bytesIn"),
    ("bytes-out", cstring"bytesOut"),
    ("kv-gets", cstring"kvGets"),
    ("kv-puts", cstring"kvPuts"),
    ("kv-deletes", cstring"kvDeletes"),
    ("active-txns", cstring"activeTxns"),
    ("committed-txns", cstring"committedTxns"),
    ("aborted-txns", cstring"abortedTxns"),
  ]
  for (elemId, field) in fields:
    let e = el("metric-" & elemId)
    if not e.isNil:
      let v = jsGet(lastMetrics, field).to(float)
      e.innerHTML = numFmt(v)

# ---------------------------------------------------------------------------
# Main refresh
# ---------------------------------------------------------------------------

proc doRefresh() {.async.} =
  try:
    lastInfo = await fetchJson("/api/info")
    lastHealth = await fetchJson("/api/health")
    lastMetrics = await fetchJson("/api/metrics")
    updateDashboard()
    updateMetrics()
    if currentTab == "nodes":
      await refreshNodesTable()
    else:
      lastNodes = await fetchJson("/api/nodes")
      let countEl = el("nodes-count")
      if not countEl.isNil:
        let arr = lastNodes.to(seq[JsObject])
        let s = if arr.len != 1: "s" else: ""
        countEl.innerHTML = cstring($arr.len & " node" & s & " registered")
  except:
    discard

# ---------------------------------------------------------------------------
# Global JS bindings for onclick handlers in innerHTML
# ---------------------------------------------------------------------------

proc removeNodeExport(nodeId: int) {.exportc: "_removeNode".} =
  discard removeNodeById(nodeId)

proc joinNodeExport() {.exportc: "_joinNode".} =
  discard joinNodeSubmit()

proc switchTabExport(name: cstring) {.exportc: "switchTab".} =
  showTab($name)
  if currentTab == "nodes":
    discard refreshNodesTable()

# ---------------------------------------------------------------------------
# Build initial HTML
# ---------------------------------------------------------------------------

proc buildUI() =
  document.body.innerHTML = """
<div class="app">
  <header>
    <div class="logo">&#x2B22; FRACTIO</div>
    <nav>
      <button id="tab-dashboard" class="tab-btn tab-active" onclick="switchTab('dashboard')">Dashboard</button>
      <button id="tab-nodes"     class="tab-btn"            onclick="switchTab('nodes')">Nodes</button>
      <button id="tab-metrics"   class="tab-btn"            onclick="switchTab('metrics')">Metrics</button>
    </nav>
    <div id="health-badge" class="badge badge-unknown">...</div>
  </header>

  <main>
    <div id="panel-dashboard" class="panel">
      <h2>Cluster Overview</h2>
      <div class="stats-grid">
        <div class="stat-card"><div class="stat-label">Node ID</div>
          <div id="stat-nodeid" class="stat-value">&#8212;</div></div>
        <div class="stat-card"><div class="stat-label">Role</div>
          <div id="stat-role" class="stat-value">&#8212;</div></div>
        <div class="stat-card"><div class="stat-label">Uptime</div>
          <div id="stat-uptime" class="stat-value">&#8212;</div></div>
        <div class="stat-card"><div class="stat-label">Active Clients</div>
          <div id="stat-clients" class="stat-value">&#8212;</div></div>
        <div class="stat-card"><div class="stat-label">Shards</div>
          <div id="stat-shards" class="stat-value">&#8212;</div></div>
        <div class="stat-card"><div class="stat-label">Version</div>
          <div id="stat-version" class="stat-value">&#8212;</div></div>
        <div class="stat-card"><div class="stat-label">Cluster</div>
          <div id="stat-cluster" class="stat-value">&#8212;</div></div>
        <div class="stat-card"><div class="stat-label">Healthy Replicas</div>
          <div id="stat-replicas" class="stat-value">&#8212;</div></div>
      </div>
    </div>

    <div id="panel-nodes" class="panel" style="display:none">
      <div class="panel-header">
        <h2>Cluster Nodes</h2>
        <span id="nodes-count" class="count-badge">&#8212;</span>
      </div>
      <div class="table-wrap">
        <table class="data-table">
          <thead><tr>
            <th>ID</th><th>Host</th><th>Raft Port</th>
            <th>Client Port</th><th>Status</th><th>Action</th>
          </tr></thead>
          <tbody id="nodes-table-body"></tbody>
        </table>
      </div>
      <div class="form-section">
        <h3>Join Node</h3>
        <div class="form-row">
          <input id="join-id"     type="number" placeholder="Node ID" min="1" max="65535">
          <input id="join-host"   type="text"   placeholder="Host (e.g. 10.0.0.2)">
          <input id="join-raft"   type="number" placeholder="Raft port">
          <input id="join-client" type="number" placeholder="Client port">
          <button class="btn-primary" onclick="_joinNode()">Join</button>
        </div>
        <div id="join-msg" class="form-msg"></div>
      </div>
    </div>

    <div id="panel-metrics" class="panel" style="display:none">
      <h2>Server Metrics</h2>
      <div class="metrics-grid">
        <div class="metrics-section">
          <h3>Requests</h3>
          <table class="metrics-table">
            <tr><td>Total</td>    <td id="metric-requests-total">&#8212;</td></tr>
            <tr><td>OK</td>       <td id="metric-requests-ok">&#8212;</td></tr>
            <tr><td>Errors</td>   <td id="metric-requests-err">&#8212;</td></tr>
          </table>
        </div>
        <div class="metrics-section">
          <h3>Network</h3>
          <table class="metrics-table">
            <tr><td>Bytes In</td>  <td id="metric-bytes-in">&#8212;</td></tr>
            <tr><td>Bytes Out</td> <td id="metric-bytes-out">&#8212;</td></tr>
          </table>
        </div>
        <div class="metrics-section">
          <h3>KV Operations</h3>
          <table class="metrics-table">
            <tr><td>Gets</td>    <td id="metric-kv-gets">&#8212;</td></tr>
            <tr><td>Puts</td>    <td id="metric-kv-puts">&#8212;</td></tr>
            <tr><td>Deletes</td> <td id="metric-kv-deletes">&#8212;</td></tr>
          </table>
        </div>
        <div class="metrics-section">
          <h3>Transactions</h3>
          <table class="metrics-table">
            <tr><td>Active</td>    <td id="metric-active-txns">&#8212;</td></tr>
            <tr><td>Committed</td> <td id="metric-committed-txns">&#8212;</td></tr>
            <tr><td>Aborted</td>   <td id="metric-aborted-txns">&#8212;</td></tr>
          </table>
        </div>
      </div>
    </div>
  </main>

  <footer><span>Fractio Management Console &middot; Auto-refresh every 5s</span></footer>
</div>
"""

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

proc main() {.async.} =
  buildUI()
  showTab("dashboard")
  await doRefresh()
  setIntervalMs(proc() = discard doRefresh(), 5000)

when isMainModule:
  discard main()
