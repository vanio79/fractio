# Fractio Web Dashboard — HappyX SPA frontend
#
# Compiled to JavaScript with:
#   nim js -d:release -o:src/fractio/web/static/app.js src/fractio/web/frontend.nim
#   npx terser src/fractio/web/static/app.js --compress --mangle -o src/fractio/web/static/app.js
#
# True SPA: hash-based routing via HappyX appRoutes.
# Routes: /#/, /#/nodes, /#/metrics, /#/clock.

import happyx
import std/jsffi

import ./js_interop
import ./state
import ./helpers
import ./data
import ./layout

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

appRoutes "app":

  # ---- Dashboard ----
  "/":
    let hs = healthStr(safeInt(gHealth.get(), "status"))
    let hc = healthColor(safeInt(gHealth.get(), "status"))
    tDiv(style = "display:flex;flex-direction:column;min-height:100vh"):
      tHeader(style = "display:flex;align-items:center;gap:1rem;padding:0 1.75rem;height:60px;background:#e81c1c;box-shadow:0 2px 8px rgba(0,0,0,.18);position:sticky;top:0;z-index:100"):
        tDiv(style = "font-size:1.1rem;font-weight:800;color:#fff;letter-spacing:.1em"):
          "⬡ FRACTIO"
        tDiv(style = "flex:1")
        tSpan(style = "background:{hc};color:#fff;padding:.25rem .75rem;border-radius:999px;font-size:.8rem;font-weight:700"):
          "{hs}"
      tNav(style = "background:#2d2d2d;display:flex;padding:0 1.25rem"):
        tA(href = "/#/",        style = "{navStyle(true)}"):   "Dashboard"
        tA(href = "/#/nodes",   style = "{navStyle(false)}"): "Nodes"
        tA(href = "/#/metrics", style = "{navStyle(false)}"): "Metrics"
        tA(href = "/#/clock",   style = "{navStyle(false)}"): "Clock"
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
      tFooter(style = "padding:.75rem 1.75rem;background:#2d2d2d;color:#999;font-size:.75rem;text-align:center"):
        "Fractio Management Console · Auto-refresh every 5s"

  # ---- Nodes ----
  "/nodes":
    let hs2 = healthStr(safeInt(gHealth.get(), "status"))
    let hc2 = healthColor(safeInt(gHealth.get(), "status"))
    tDiv(style = "display:flex;flex-direction:column;min-height:100vh"):
      tHeader(style = "display:flex;align-items:center;gap:1rem;padding:0 1.75rem;height:60px;background:#e81c1c;box-shadow:0 2px 8px rgba(0,0,0,.18);position:sticky;top:0;z-index:100"):
        tDiv(style = "font-size:1.1rem;font-weight:800;color:#fff;letter-spacing:.1em"): "⬡ FRACTIO"
        tDiv(style = "flex:1")
        tSpan(style = "background:{hc2};color:#fff;padding:.25rem .75rem;border-radius:999px;font-size:.8rem;font-weight:700"): "{hs2}"
      tNav(style = "background:#2d2d2d;display:flex;padding:0 1.25rem"):
        tA(href = "/#/",        style = "{navStyle(false)}"): "Dashboard"
        tA(href = "/#/nodes",   style = "{navStyle(true)}"):  "Nodes"
        tA(href = "/#/metrics", style = "{navStyle(false)}"): "Metrics"
        tA(href = "/#/clock",   style = "{navStyle(false)}"): "Clock"
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
                  tTh(style = "background:#3a3a3a;color:#fff;padding:.55rem .85rem;text-align:left;font-size:.7rem;text-transform:uppercase;letter-spacing:.07em;font-weight:600"):
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
      tFooter(style = "padding:.75rem 1.75rem;background:#2d2d2d;color:#999;font-size:.75rem;text-align:center"):
        "Fractio Management Console · Auto-refresh every 5s"

  # ---- Metrics ----
  "/metrics":
    let hs3 = healthStr(safeInt(gHealth.get(), "status"))
    let hc3 = healthColor(safeInt(gHealth.get(), "status"))
    tDiv(style = "display:flex;flex-direction:column;min-height:100vh"):
      tHeader(style = "display:flex;align-items:center;gap:1rem;padding:0 1.75rem;height:60px;background:#e81c1c;box-shadow:0 2px 8px rgba(0,0,0,.18);position:sticky;top:0;z-index:100"):
        tDiv(style = "font-size:1.1rem;font-weight:800;color:#fff;letter-spacing:.1em"): "⬡ FRACTIO"
        tDiv(style = "flex:1")
        tSpan(style = "background:{hc3};color:#fff;padding:.25rem .75rem;border-radius:999px;font-size:.8rem;font-weight:700"): "{hs3}"
      tNav(style = "background:#2d2d2d;display:flex;padding:0 1.25rem"):
        tA(href = "/#/",        style = "{navStyle(false)}"): "Dashboard"
        tA(href = "/#/nodes",   style = "{navStyle(false)}"): "Nodes"
        tA(href = "/#/metrics", style = "{navStyle(true)}"):  "Metrics"
        tA(href = "/#/clock",   style = "{navStyle(false)}"): "Clock"
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
      tFooter(style = "padding:.75rem 1.75rem;background:#2d2d2d;color:#999;font-size:.75rem;text-align:center"):
        "Fractio Management Console · Auto-refresh every 5s"

  # ---- Clock drift ----
  "/clock":
    let hs4 = healthStr(safeInt(gHealth.get(), "status"))
    let hc4 = healthColor(safeInt(gHealth.get(), "status"))
    tDiv(style = "display:flex;flex-direction:column;min-height:100vh"):
      tHeader(style = "display:flex;align-items:center;gap:1rem;padding:0 1.75rem;height:60px;background:#e81c1c;box-shadow:0 2px 8px rgba(0,0,0,.18);position:sticky;top:0;z-index:100"):
        tDiv(style = "font-size:1.1rem;font-weight:800;color:#fff;letter-spacing:.1em"): "⬡ FRACTIO"
        tDiv(style = "flex:1")
        tSpan(style = "background:{hc4};color:#fff;padding:.25rem .75rem;border-radius:999px;font-size:.8rem;font-weight:700"): "{hs4}"
      tNav(style = "background:#2d2d2d;display:flex;padding:0 1.25rem"):
        tA(href = "/#/",        style = "{navStyle(false)}"): "Dashboard"
        tA(href = "/#/nodes",   style = "{navStyle(false)}"): "Nodes"
        tA(href = "/#/metrics", style = "{navStyle(false)}"): "Metrics"
        tA(href = "/#/clock",   style = "{navStyle(true)}"):  "Clock"
      tMain(style = "flex:1;padding:1.75rem;max-width:1260px;width:100%"):

        # Title row — WS status badge is populated by injectClockDom()
        tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:1.25rem"):
          tH2(style = "font-size:1.05rem;font-weight:700;color:#111;margin:0"):
            "SharedTimer Clock Drift"
          tDiv(id = "clock-ws-status")

        # Chart card
        tDiv(style = "background:#fff;border:1px solid #e0e0e0;border-radius:8px;padding:1rem 1rem .5rem;margin-bottom:1.25rem;box-shadow:0 1px 4px rgba(0,0,0,.07)"):
          tDiv(style = "display:flex;justify-content:space-between;margin-bottom:.25rem"):
            tSpan(style = "font-size:.65rem;color:#999;font-family:monospace"): "+25 ms"
            tSpan(style = "font-size:.65rem;color:#666;font-family:monospace"): "clock offset"
            tSpan(style = "font-size:.65rem;color:#999;font-family:monospace"): "−25 ms"

          # SVG container — injected by injectClockDom(), never touched by HappyX
          tDiv(id = "drift-chart", style = "width:100%;min-height:120px")

          # X-axis legend
          tDiv(style = "display:flex;justify-content:space-between;margin-top:.35rem"):
            tSpan(style = "font-size:.65rem;color:#999;font-family:monospace"): "−2 min"
            tSpan(style = "font-size:.65rem;color:#999;font-family:monospace"): "now"

        # Stats row — values updated by injectClockDom()
        tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:1rem;margin-bottom:1.25rem"):
          tDiv(style = "background:#fff;border-top:3px solid #e81c1c;border-radius:6px;padding:1rem;box-shadow:0 1px 4px rgba(0,0,0,.07)"):
            tDiv(style = "font-size:.68rem;color:#666;text-transform:uppercase;letter-spacing:.07em;margin-bottom:.5rem;font-weight:600"):
              "Latest Offset"
            tDiv(id = "clock-last-offset")
          tDiv(style = "background:#fff;border-top:3px solid #e81c1c;border-radius:6px;padding:1rem;box-shadow:0 1px 4px rgba(0,0,0,.07)"):
            tDiv(style = "font-size:.68rem;color:#666;text-transform:uppercase;letter-spacing:.07em;margin-bottom:.5rem;font-weight:600"):
              "Samples"
            tDiv(id = "clock-sample-count")
          tDiv(style = "background:#fff;border-top:3px solid #e81c1c;border-radius:6px;padding:1rem;box-shadow:0 1px 4px rgba(0,0,0,.07)"):
            tDiv(style = "font-size:.68rem;color:#666;text-transform:uppercase;letter-spacing:.07em;margin-bottom:.5rem;font-weight:600"):
              "Threshold"
            tDiv(style = "font-size:1.2rem;font-weight:700;color:#e81c1c;font-family:monospace"):
              "±10 ms"

        # Legend
        tDiv(style = "font-size:.78rem;color:#888"):
          "━ Clock offset   ╌ ±10 ms threshold   ┄ 0 ms baseline   · Updates every 1 s"

      tFooter(style = "padding:.75rem 1.75rem;background:#2d2d2d;color:#999;font-size:.75rem;text-align:center"):
        "Fractio Management Console · SharedTimer drift stream"

# ---------------------------------------------------------------------------
# Boot
# ---------------------------------------------------------------------------

when isMainModule:
  discard doRefresh()
  jsSetInterval(proc() = discard doRefresh(), 5000)
  connectDriftWs()
  # Initial DOM injection in case WS hasn't fired yet when page first loads
  jsSetTimeout(proc() = injectClockDom(), 100)
