# Fractio Web Dashboard — HappyX SPA frontend
#
# Compiled to JavaScript with:
#   nim js -d:release -o:src/fractio/web/static/app.js src/fractio/web/frontend.nim
#
# True SPA: hash-based routing via HappyX appRoutes.
# Routes: /#/, /#/nodes, /#/metrics.

import happyx
import std/[jsffi, asyncjs]

# ---------------------------------------------------------------------------
# JS interop
# ---------------------------------------------------------------------------

proc fetchJson(url: cstring): Future[JsObject]
    {.importjs: "fetch(#).then(r=>r.json())", async.}

proc fetchDelete(url: cstring): Future[JsObject]
    {.importjs: "fetch(#,{method:'DELETE'}).then(r=>r.json())", async.}

proc fetchPost(url: cstring, body: cstring): Future[JsObject]
    {.importjs: "fetch(#,{method:'POST',headers:{'Content-Type':'application/json'},body:#}).then(r=>r.json())", async.}

proc jsStringify(o: JsObject): cstring
    {.importjs: "JSON.stringify(#)".}

proc numFmt(n: float): cstring
    {.importjs: "Number(#).toLocaleString()".}

proc jsParseInt(s: cstring): int
    {.importjs: "parseInt(#,10)".}

proc getInputVal(id: cstring): cstring
    {.importjs: "(document.getElementById(#)||{value:''}).value".}

proc clearInput(id: cstring)
    {.importjs: "(function(i){var e=document.getElementById(i);if(e)e.value='';})(#)".}

proc jsSetInterval(fn: proc(), ms: int)
    {.importjs: "setInterval(#,#)".}

# Safe field accessors — coerce missing/null fields without BigInt crash
proc safeInt(obj: JsObject, field: cstring): int
    {.importjs: "Number(#[#]??0)".}

proc safeFloat(obj: JsObject, field: cstring): float
    {.importjs: "Number(#[#]??0)".}

proc safeStr(obj: JsObject, field: cstring): cstring
    {.importjs: "String(#[#]??'')".}

# Returns integer field as a string — avoids Nim $int -> BigInt conversion
proc safeIntStr(obj: JsObject, field: cstring): cstring
    {.importjs: "String(Number(#[#]??0))".}

proc jsLen(obj: JsObject): cstring
    {.importjs: "String((#)?.length??0)".}

# ---------------------------------------------------------------------------
# Global reactive state
# ---------------------------------------------------------------------------

var
  gInfo:    State[JsObject] = remember newJsObject()
  gHealth:  State[JsObject] = remember newJsObject()
  gMetrics: State[JsObject] = remember newJsObject()
  gNodes:   State[JsObject] = remember newJsObject()
  gMsg:     State[string]   = remember ""
  gMsgOk:   State[bool]     = remember false

# ---------------------------------------------------------------------------
# Helpers
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

proc healthColor(s: int): string =
  case s
  of 0: "#1a7f37"
  of 1: "#b45309"
  of 2: "#c41010"
  else: "#888"

proc statusStr(s: int): string =
  case s
  of 1: "active"
  of 2: "draining"
  of 3: "down"
  else: "unknown"

proc statusColor(s: int): string =
  case s
  of 1: "#1a7f37"
  of 2: "#b45309"
  of 3: "#c41010"
  else: "#888"

# ---------------------------------------------------------------------------
# Data fetch
# ---------------------------------------------------------------------------

proc doRefresh() {.async.} =
  try:
    gInfo.set(await fetchJson("/api/info"))
    gHealth.set(await fetchJson("/api/health"))
    gMetrics.set(await fetchJson("/api/metrics"))
    gNodes.set(await fetchJson("/api/nodes"))
  except:
    discard

proc doRemoveNode(nodeId: int) {.async.} =
  discard await fetchDelete(cstring("/api/nodes/" & $nodeId))
  gNodes.set(await fetchJson("/api/nodes"))

proc doJoinNode() {.async.} =
  let body = newJsObject()
  body.nodeId     = jsParseInt(getInputVal("join-id"))
  body.host       = getInputVal("join-host")
  body.raftPort   = jsParseInt(getInputVal("join-raft"))
  body.clientPort = jsParseInt(getInputVal("join-client"))
  let resp = await fetchPost("/api/nodes", jsStringify(body))
  gMsgOk.set(safeInt(resp, "success") != 0)
  gMsg.set($safeStr(resp, "message"))
  if gMsgOk:
    clearInput("join-id")
    clearInput("join-host")
    clearInput("join-raft")
    clearInput("join-client")
    gNodes.set(await fetchJson("/api/nodes"))

# ---------------------------------------------------------------------------
# Components
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

appRoutes "app":

  # ---- Dashboard ----
  "/":
    tDiv(style = "display:flex;flex-direction:column;min-height:100vh"):
      tHeader(style = "display:flex;align-items:center;gap:1rem;padding:0 1.75rem;height:60px;background:#e81c1c;box-shadow:0 2px 8px rgba(0,0,0,.18);position:sticky;top:0;z-index:100"):
        tDiv(style = "font-size:1.1rem;font-weight:800;color:#fff;letter-spacing:.1em"):
          "⬡ FRACTIO"
        tDiv(style = "flex:1")
        let hs = healthStr(safeInt(gHealth.get(), "status"))
        let hc = healthColor(safeInt(gHealth.get(), "status"))
        tSpan(style = "background:{hc};color:#fff;padding:.25rem .75rem;border-radius:999px;font-size:.8rem;font-weight:700"):
          "{hs}"
      tNav(style = "background:#111;display:flex;padding:0 1.25rem"):
        tA(href = "/#/",        style = "color:#fff;text-decoration:none;padding:.6rem 1rem;font-size:.82rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em;border-bottom:2px solid #e81c1c"): "Dashboard"
        tA(href = "/#/nodes",   style = "color:#aaa;text-decoration:none;padding:.6rem 1rem;font-size:.82rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em"): "Nodes"
        tA(href = "/#/metrics", style = "color:#aaa;text-decoration:none;padding:.6rem 1rem;font-size:.82rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em"): "Metrics"
      tMain(style = "flex:1;padding:1.75rem;max-width:1260px;width:100%"):
        let nid  = $safeIntStr(gInfo.get(), "nodeId")
        let role = roleStr(safeInt(gInfo.get(), "role"))
        let upt  = uptimeStr(safeInt(gInfo.get(), "uptimeSecs"))
        let cli  = $safeIntStr(gInfo.get(), "clientCount")
        let shd  = $safeIntStr(gInfo.get(), "shardCount")
        let ver  = $safeStr(gInfo.get(), "version")
        let cln  = $safeStr(gInfo.get(), "clusterName")
        let rep  = $safeIntStr(gHealth.get(), "healthyReplicas") & " / " & $safeIntStr(gHealth.get(), "replicaCount")
        tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:1rem"):
          for (cardLbl, cardVal) in [("Node ID", nid), ("Role", role), ("Uptime", upt),
                                     ("Active Clients", cli), ("Shards", shd),
                                     ("Version", ver), ("Cluster", cln),
                                     ("Healthy Replicas", rep)]:
            tDiv(style = "background:#fff;border-top:3px solid #e81c1c;border-radius:6px;padding:1rem;box-shadow:0 1px 4px rgba(0,0,0,.07);text-align:center"):
              tDiv(style = "font-size:.68rem;color:#666;text-transform:uppercase;letter-spacing:.07em;margin-bottom:.5rem;font-weight:600"):
                "{cardLbl}"
              tDiv(style = "font-size:1.5rem;font-weight:700;color:#e81c1c"):
                "{cardVal}"
      tFooter(style = "padding:.75rem 1.75rem;background:#111;color:#888;font-size:.75rem;text-align:center"):
        "Fractio Management Console · Auto-refresh every 5s"

  # ---- Nodes ----
  "/nodes":
    tDiv(style = "display:flex;flex-direction:column;min-height:100vh"):
      tHeader(style = "display:flex;align-items:center;gap:1rem;padding:0 1.75rem;height:60px;background:#e81c1c;box-shadow:0 2px 8px rgba(0,0,0,.18);position:sticky;top:0;z-index:100"):
        tDiv(style = "font-size:1.1rem;font-weight:800;color:#fff;letter-spacing:.1em"): "⬡ FRACTIO"
        tDiv(style = "flex:1")
        let hs2 = healthStr(safeInt(gHealth.get(), "status"))
        let hc2 = healthColor(safeInt(gHealth.get(), "status"))
        tSpan(style = "background:{hc2};color:#fff;padding:.25rem .75rem;border-radius:999px;font-size:.8rem;font-weight:700"): "{hs2}"
      tNav(style = "background:#111;display:flex;padding:0 1.25rem"):
        tA(href = "/#/",        style = "color:#aaa;text-decoration:none;padding:.6rem 1rem;font-size:.82rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em"): "Dashboard"
        tA(href = "/#/nodes",   style = "color:#fff;text-decoration:none;padding:.6rem 1rem;font-size:.82rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em;border-bottom:2px solid #e81c1c"): "Nodes"
        tA(href = "/#/metrics", style = "color:#aaa;text-decoration:none;padding:.6rem 1rem;font-size:.82rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em"): "Metrics"
      tMain(style = "flex:1;padding:1.75rem;max-width:1260px;width:100%"):
        let arr = to(gNodes.get(), seq[JsObject])
        let arrLen = arr.len
        let nodeCount = $jsLen(gNodes.get()) & (if arrLen != 1: " nodes" else: " node")
        tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:1rem"):
          tH2(style = "font-size:1.05rem;font-weight:700;color:#111;margin:0"): "Cluster Nodes"
          tSpan(style = "background:#eee;color:#444;padding:.2rem .6rem;border-radius:999px;font-size:.8rem"):
            "{nodeCount}"
        tDiv(style = "overflow-x:auto;margin-bottom:1.25rem"):
          tTable(style = "width:100%;border-collapse:collapse;font-size:.875rem;background:#fff;border:1px solid #e0e0e0;border-radius:6px;overflow:hidden"):
            tThead:
              tTr:
                for h in ["ID","Host","Raft Port","Client Port","Status","Action"]:
                  tTh(style = "background:#111;color:#fff;padding:.55rem .85rem;text-align:left;font-size:.7rem;text-transform:uppercase;letter-spacing:.07em;font-weight:600"):
                    "{h}"
            tTbody:
              for node in arr:
                let nid   = safeInt(node, "nodeId")
                let nhost = $safeStr(node, "host")
                let nrp   = $safeIntStr(node, "raftPort")
                let ncp   = $safeIntStr(node, "clientPort")
                let nst   = safeInt(node, "status")
                let nsc   = statusColor(nst)
                let nss   = statusStr(nst)
                let nidStr = $safeIntStr(node, "nodeId")
                tTr:
                  tTd(style = "padding:.55rem .85rem;border-bottom:1px solid #eee"): "{nidStr}"
                  tTd(style = "padding:.55rem .85rem;border-bottom:1px solid #eee"): "{nhost}"
                  tTd(style = "padding:.55rem .85rem;border-bottom:1px solid #eee"): "{nrp}"
                  tTd(style = "padding:.55rem .85rem;border-bottom:1px solid #eee"): "{ncp}"
                  tTd(style = "padding:.55rem .85rem;border-bottom:1px solid #eee"):
                    tSpan(style = "color:{nsc};font-weight:600"): "{nss}"
                  tTd(style = "padding:.55rem .85rem;border-bottom:1px solid #eee"):
                    tButton(style = "background:#e81c1c;color:#fff;border:none;padding:.3rem .75rem;border-radius:4px;cursor:pointer;font-size:.8rem"):
                      "Remove"
                      @click:
                        discard doRemoveNode(nid)
        tDiv(style = "background:#fff;border-radius:6px;border:1px solid #e0e0e0;padding:1rem"):
          tStrong: "Join Node"
          tDiv(style = "display:flex;gap:.5rem;flex-wrap:wrap;align-items:flex-end;margin:.75rem 0 .5rem"):
            tInput(id = "join-id",     `type` = "number", placeholder = "Node ID",     style = "width:130px;padding:.4rem .6rem;border:1px solid #ccc;border-radius:4px")
            tInput(id = "join-host",   `type` = "text",   placeholder = "Host",        style = "width:190px;padding:.4rem .6rem;border:1px solid #ccc;border-radius:4px")
            tInput(id = "join-raft",   `type` = "number", placeholder = "Raft port",   style = "width:130px;padding:.4rem .6rem;border:1px solid #ccc;border-radius:4px")
            tInput(id = "join-client", `type` = "number", placeholder = "Client port", style = "width:130px;padding:.4rem .6rem;border:1px solid #ccc;border-radius:4px")
            tButton(style = "background:#e81c1c;color:#fff;border:none;padding:.45rem 1.1rem;border-radius:4px;cursor:pointer;font-weight:600"):
              "Join"
              @click:
                discard doJoinNode()
          if gMsg != "":
            let mc = if gMsgOk: "#1a7f37" else: "#c41010"
            let mt = $gMsg
            tDiv(style = "font-size:.82rem;color:{mc}"): "{mt}"
      tFooter(style = "padding:.75rem 1.75rem;background:#111;color:#888;font-size:.75rem;text-align:center"):
        "Fractio Management Console · Auto-refresh every 5s"

  # ---- Metrics ----
  "/metrics":
    tDiv(style = "display:flex;flex-direction:column;min-height:100vh"):
      tHeader(style = "display:flex;align-items:center;gap:1rem;padding:0 1.75rem;height:60px;background:#e81c1c;box-shadow:0 2px 8px rgba(0,0,0,.18);position:sticky;top:0;z-index:100"):
        tDiv(style = "font-size:1.1rem;font-weight:800;color:#fff;letter-spacing:.1em"): "⬡ FRACTIO"
        tDiv(style = "flex:1")
        let hs3 = healthStr(safeInt(gHealth.get(), "status"))
        let hc3 = healthColor(safeInt(gHealth.get(), "status"))
        tSpan(style = "background:{hc3};color:#fff;padding:.25rem .75rem;border-radius:999px;font-size:.8rem;font-weight:700"): "{hs3}"
      tNav(style = "background:#111;display:flex;padding:0 1.25rem"):
        tA(href = "/#/",        style = "color:#aaa;text-decoration:none;padding:.6rem 1rem;font-size:.82rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em"): "Dashboard"
        tA(href = "/#/nodes",   style = "color:#aaa;text-decoration:none;padding:.6rem 1rem;font-size:.82rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em"): "Nodes"
        tA(href = "/#/metrics", style = "color:#fff;text-decoration:none;padding:.6rem 1rem;font-size:.82rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em;border-bottom:2px solid #e81c1c"): "Metrics"
      tMain(style = "flex:1;padding:1.75rem;max-width:1260px;width:100%"):
        tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:1rem"):
          tDiv(style = "background:#fff;border-radius:6px;border:1px solid #e0e0e0;padding:1rem"):
            tStrong: "Requests"
            tTable(style = "width:100%;font-size:.875rem;border-collapse:collapse;margin-top:.5rem"):
              for (lbl, fld) in [("Total","requestsTotal"),("OK","requestsOK"),("Errors","requestsErr")]:
                let v = $numFmt(safeFloat(gMetrics.get(), cstring(fld)))
                tTr:
                  tTd(style = "padding:.35rem 0;color:#444"): "{lbl}"
                  tTd(style = "text-align:right;font-family:monospace;color:#e81c1c;font-weight:600"): "{v}"
          tDiv(style = "background:#fff;border-radius:6px;border:1px solid #e0e0e0;padding:1rem"):
            tStrong: "Network"
            tTable(style = "width:100%;font-size:.875rem;border-collapse:collapse;margin-top:.5rem"):
              for (lbl, fld) in [("Bytes In","bytesIn"),("Bytes Out","bytesOut")]:
                let v = $numFmt(safeFloat(gMetrics.get(), cstring(fld)))
                tTr:
                  tTd(style = "padding:.35rem 0;color:#444"): "{lbl}"
                  tTd(style = "text-align:right;font-family:monospace;color:#e81c1c;font-weight:600"): "{v}"
          tDiv(style = "background:#fff;border-radius:6px;border:1px solid #e0e0e0;padding:1rem"):
            tStrong: "KV Operations"
            tTable(style = "width:100%;font-size:.875rem;border-collapse:collapse;margin-top:.5rem"):
              for (lbl, fld) in [("Gets","kvGets"),("Puts","kvPuts"),("Deletes","kvDeletes")]:
                let v = $numFmt(safeFloat(gMetrics.get(), cstring(fld)))
                tTr:
                  tTd(style = "padding:.35rem 0;color:#444"): "{lbl}"
                  tTd(style = "text-align:right;font-family:monospace;color:#e81c1c;font-weight:600"): "{v}"
          tDiv(style = "background:#fff;border-radius:6px;border:1px solid #e0e0e0;padding:1rem"):
            tStrong: "Transactions"
            tTable(style = "width:100%;font-size:.875rem;border-collapse:collapse;margin-top:.5rem"):
              for (lbl, fld) in [("Active","activeTxns"),("Committed","committedTxns"),("Aborted","abortedTxns")]:
                let v = $numFmt(safeFloat(gMetrics.get(), cstring(fld)))
                tTr:
                  tTd(style = "padding:.35rem 0;color:#444"): "{lbl}"
                  tTd(style = "text-align:right;font-family:monospace;color:#e81c1c;font-weight:600"): "{v}"
      tFooter(style = "padding:.75rem 1.75rem;background:#111;color:#888;font-size:.75rem;text-align:center"):
        "Fractio Management Console · Auto-refresh every 5s"

# ---------------------------------------------------------------------------
# Boot
# ---------------------------------------------------------------------------

when isMainModule:
  discard doRefresh()
  jsSetInterval(proc() = discard doRefresh(), 5000)
