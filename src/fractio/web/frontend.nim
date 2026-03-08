# Fractio Web Dashboard — Frontend
#
# Compiled to JavaScript with: nim js -d:release -o:static/app.js frontend.nim
#
# Uses std/dom for DOM manipulation and importjs pragmas for fetch().
# Three panels: Dashboard, Nodes, Metrics.  Auto-refreshes every 5 seconds.
# Uses Shoelace web components (loaded via CDN in the HTML shell).

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

# Listen for Shoelace sl-tab-show event on a tab-group element
proc onSlTabShow(tabGroup: Element, fn: proc(e: JsObject))
    {.importjs: "#.addEventListener('sl-tab-show', function(e){#(e)})".}

# Read e.detail.name from a Shoelace tab-show CustomEvent
proc jsDetailName(e: JsObject): cstring
    {.importjs: "#.detail.name".}

# ---------------------------------------------------------------------------
# DOM helpers
# ---------------------------------------------------------------------------

proc el(id: string): Element = document.getElementById(id)
proc clearEl(e: Element) = e.innerHTML = ""
proc setHtml(e: Element, h: string) =
  if not e.isNil: e.innerHTML = h

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

var currentTab = "dashboard"
var lastInfo:    JsObject
var lastHealth:  JsObject
var lastMetrics: JsObject
var lastNodes:   JsObject

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

proc healthVariant(s: int): string =
  case s
  of 0: "success"
  of 1: "warning"
  of 2: "danger"
  else: "neutral"

proc nodeStatusStr(s: int): string =
  case s
  of 1: "active"
  of 2: "draining"
  of 3: "down"
  else: "unknown"

proc nodeStatusVariant(s: int): string =
  case s
  of 1: "success"
  of 2: "warning"
  of 3: "danger"
  else: "neutral"

# ---------------------------------------------------------------------------
# Dashboard panel
# ---------------------------------------------------------------------------

proc updateDashboard() =
  if lastInfo.isNil or lastHealth.isNil: return

  let nodeId          = lastInfo.nodeId.to(int)
  let version         = $lastInfo.version.to(cstring)
  let uptime          = lastInfo.uptimeSecs.to(int)
  let role            = lastInfo.role.to(int)
  let shards          = lastInfo.shardCount.to(int)
  let clients         = lastInfo.clientCount.to(int)
  let clusterName     = $lastInfo.clusterName.to(cstring)
  let healthStatus    = lastHealth.status.to(int)
  let replicas        = lastHealth.replicaCount.to(int)
  let healthyReplicas = lastHealth.healthyReplicas.to(int)

  let hbadge = el("health-badge")
  if not hbadge.isNil:
    hbadge.innerHTML = cstring(healthStr(healthStatus))
    hbadge.setAttribute("variant", cstring(healthVariant(healthStatus)))

  setHtml(el("stat-nodeid"),   $nodeId)
  setHtml(el("stat-role"),     roleStr(role))
  setHtml(el("stat-uptime"),   uptimeStr(uptime))
  setHtml(el("stat-clients"),  $clients)
  setHtml(el("stat-shards"),   $shards)
  setHtml(el("stat-version"),  version)
  setHtml(el("stat-cluster"),  clusterName)
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
    let nodeId     = node.nodeId.to(int)
    let host       = $node.host.to(cstring)
    let raftPort   = node.raftPort.to(int)
    let clientPort = node.clientPort.to(int)
    let status     = node.status.to(int)

    let tr = document.createElement("tr")
    tr.innerHTML = cstring(
      "<td>" & $nodeId & "</td>" &
      "<td>" & host & "</td>" &
      "<td>" & $raftPort & "</td>" &
      "<td>" & $clientPort & "</td>" &
      "<td><sl-badge variant=\"" & nodeStatusVariant(status) & "\" pill>" &
        nodeStatusStr(status) & "</sl-badge></td>" &
      "<td><sl-button variant=\"danger\" size=\"small\" onclick=\"_removeNode(" &
        $nodeId & ")\">Remove</sl-button></td>"
    )
    tbody.appendChild(tr)

  let countEl = el("nodes-count")
  if not countEl.isNil:
    let s = if arr.len != 1: "s" else: ""
    countEl.innerHTML = cstring($arr.len & " node" & s)

proc removeNodeById(nodeId: int) {.async.} =
  let opts = newJsObject()
  opts.method = cstring"DELETE"
  discard await fetchWithOpts(cstring("/api/nodes/" & $nodeId), opts)
  await refreshNodesTable()

proc joinNodeSubmit() {.async.} =
  let idEl     = el("join-id")
  let hostEl   = el("join-host")
  let raftEl   = el("join-raft")
  let clientEl = el("join-client")
  let msgEl    = el("join-msg")

  if idEl.isNil or hostEl.isNil or raftEl.isNil or clientEl.isNil: return

  let body = newJsObject()
  body.nodeId     = jsParseInt(idEl.value)
  body.host       = hostEl.value
  body.raftPort   = jsParseInt(raftEl.value)
  body.clientPort = jsParseInt(clientEl.value)

  let headers = newJsObject()
  headers[cstring"Content-Type"] = cstring"application/json"

  let opts = newJsObject()
  opts.method  = cstring"POST"
  opts.headers = headers
  opts.body    = jsStringify(body)

  let resp    = await fetchWithOpts("/api/nodes", opts)
  let success = resp.success.to(bool)
  let message = $resp.message.to(cstring)

  if not msgEl.isNil:
    msgEl.innerHTML = cstring(message)
    msgEl.className = if success: "form-msg ok" else: "form-msg err"

  if success:
    idEl.value     = ""
    hostEl.value   = ""
    raftEl.value   = ""
    clientEl.value = ""
    await refreshNodesTable()

# ---------------------------------------------------------------------------
# Metrics panel
# ---------------------------------------------------------------------------

proc updateMetrics() =
  if lastMetrics.isNil: return
  let fields = [
    ("requests-total",  cstring"requestsTotal"),
    ("requests-ok",     cstring"requestsOK"),
    ("requests-err",    cstring"requestsErr"),
    ("bytes-in",        cstring"bytesIn"),
    ("bytes-out",       cstring"bytesOut"),
    ("kv-gets",         cstring"kvGets"),
    ("kv-puts",         cstring"kvPuts"),
    ("kv-deletes",      cstring"kvDeletes"),
    ("active-txns",     cstring"activeTxns"),
    ("committed-txns",  cstring"committedTxns"),
    ("aborted-txns",    cstring"abortedTxns"),
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
    lastInfo    = await fetchJson("/api/info")
    lastHealth  = await fetchJson("/api/health")
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
        countEl.innerHTML = cstring($arr.len & " node" & s)
  except:
    discard

# ---------------------------------------------------------------------------
# Global JS bindings for onclick handlers injected into innerHTML
# ---------------------------------------------------------------------------

proc removeNodeExport(nodeId: int) {.exportc: "_removeNode".} =
  discard removeNodeById(nodeId)

proc joinNodeExport() {.exportc: "_joinNode".} =
  discard joinNodeSubmit()

# ---------------------------------------------------------------------------
# Build initial HTML (Shoelace web components)
# ---------------------------------------------------------------------------

proc buildUI() =
  document.body.innerHTML = """
<div class="app">
  <header>
    <div class="logo">&#x2B22; FRACTIO</div>
    <div style="flex:1"></div>
    <sl-badge id="health-badge" variant="neutral" pill>&#8230;</sl-badge>
  </header>

  <main>
    <sl-tab-group id="main-tabs">
      <sl-tab slot="nav" panel="dashboard">Dashboard</sl-tab>
      <sl-tab slot="nav" panel="nodes">Nodes</sl-tab>
      <sl-tab slot="nav" panel="metrics">Metrics</sl-tab>

      <sl-tab-panel name="dashboard" style="--padding:1.25rem 0 0 0">
        <div class="stats-grid">
          <sl-card>
            <div class="stat-label">Node ID</div>
            <div id="stat-nodeid" class="stat-value">&#8212;</div>
          </sl-card>
          <sl-card>
            <div class="stat-label">Role</div>
            <div id="stat-role" class="stat-value">&#8212;</div>
          </sl-card>
          <sl-card>
            <div class="stat-label">Uptime</div>
            <div id="stat-uptime" class="stat-value">&#8212;</div>
          </sl-card>
          <sl-card>
            <div class="stat-label">Active Clients</div>
            <div id="stat-clients" class="stat-value">&#8212;</div>
          </sl-card>
          <sl-card>
            <div class="stat-label">Shards</div>
            <div id="stat-shards" class="stat-value">&#8212;</div>
          </sl-card>
          <sl-card>
            <div class="stat-label">Version</div>
            <div id="stat-version" class="stat-value">&#8212;</div>
          </sl-card>
          <sl-card>
            <div class="stat-label">Cluster</div>
            <div id="stat-cluster" class="stat-value">&#8212;</div>
          </sl-card>
          <sl-card>
            <div class="stat-label">Healthy Replicas</div>
            <div id="stat-replicas" class="stat-value">&#8212;</div>
          </sl-card>
        </div>
      </sl-tab-panel>

      <sl-tab-panel name="nodes" style="--padding:1.25rem 0 0 0">
        <div class="panel-header">
          <h2>Cluster Nodes</h2>
          <sl-badge id="nodes-count" variant="neutral">0 nodes</sl-badge>
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
        <sl-card>
          <strong slot="header">Join Node</strong>
          <div class="form-row">
            <sl-input id="join-id"     type="number" placeholder="Node ID"     size="small" style="width:130px"></sl-input>
            <sl-input id="join-host"   type="text"   placeholder="Host"        size="small" style="width:190px"></sl-input>
            <sl-input id="join-raft"   type="number" placeholder="Raft port"   size="small" style="width:130px"></sl-input>
            <sl-input id="join-client" type="number" placeholder="Client port" size="small" style="width:130px"></sl-input>
            <sl-button variant="primary" onclick="_joinNode()">Join</sl-button>
          </div>
          <div id="join-msg" class="form-msg"></div>
        </sl-card>
      </sl-tab-panel>

      <sl-tab-panel name="metrics" style="--padding:1.25rem 0 0 0">
        <div class="metrics-grid">
          <sl-card>
            <strong slot="header">Requests</strong>
            <table class="metrics-table">
              <tr><td>Total</td>  <td id="metric-requests-total">&#8212;</td></tr>
              <tr><td>OK</td>     <td id="metric-requests-ok">&#8212;</td></tr>
              <tr><td>Errors</td> <td id="metric-requests-err">&#8212;</td></tr>
            </table>
          </sl-card>
          <sl-card>
            <strong slot="header">Network</strong>
            <table class="metrics-table">
              <tr><td>Bytes In</td>  <td id="metric-bytes-in">&#8212;</td></tr>
              <tr><td>Bytes Out</td> <td id="metric-bytes-out">&#8212;</td></tr>
            </table>
          </sl-card>
          <sl-card>
            <strong slot="header">KV Operations</strong>
            <table class="metrics-table">
              <tr><td>Gets</td>    <td id="metric-kv-gets">&#8212;</td></tr>
              <tr><td>Puts</td>    <td id="metric-kv-puts">&#8212;</td></tr>
              <tr><td>Deletes</td> <td id="metric-kv-deletes">&#8212;</td></tr>
            </table>
          </sl-card>
          <sl-card>
            <strong slot="header">Transactions</strong>
            <table class="metrics-table">
              <tr><td>Active</td>    <td id="metric-active-txns">&#8212;</td></tr>
              <tr><td>Committed</td> <td id="metric-committed-txns">&#8212;</td></tr>
              <tr><td>Aborted</td>   <td id="metric-aborted-txns">&#8212;</td></tr>
            </table>
          </sl-card>
        </div>
      </sl-tab-panel>
    </sl-tab-group>
  </main>

  <footer>Fractio Management Console &middot; Auto-refresh every 5s</footer>
</div>
"""

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

proc main() {.async.} =
  buildUI()
  currentTab = "dashboard"
  # Track active tab via Shoelace's sl-tab-show event (fired when tab changes)
  let tabGroup = el("main-tabs")
  onSlTabShow(tabGroup, proc(e: JsObject) =
    let name = $jsDetailName(e)
    currentTab = name
    if name == "nodes":
      discard refreshNodesTable()
  )
  await doRefresh()
  setIntervalMs(proc() = discard doRefresh(), 5000)

when isMainModule:
  discard main()
