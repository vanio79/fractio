# Fractio Web Dashboard — HappyX SPA frontend
#
# Compiled to JavaScript with:
#   nim js -d:release -o:src/fractio/web/static/app.js src/fractio/web/frontend.nim
#   npx terser src/fractio/web/static/app.js --compress --mangle -o src/fractio/web/static/app.js
#
# True SPA: hash-based routing via HappyX appRoutes.
# Routes: /#/, /#/nodes, /#/metrics, /#/clock, /#/data/...

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
    tDiv(style = shellStyle):
      tHeader(style = headerStyle):
        tDiv(style = logoStyle): "⬡ FRACTIO"
        tDiv(style = "flex:1")
        tSpan(style = "{badgeStyle(hc)}"): "{hs}"
      tNav(style = navBarStyle):
        for (href, label) in navItems:
          tA(href = href, style = "{navStyle(label == \"Dashboard\")}"): "{label}"
      tMain(style = mainStyle):
        let nid  = $safeIntStr(gInfo.get(), "nodeId")
        let role = roleStr(safeInt(gInfo.get(), "role"))
        let upt  = uptimeStr(safeInt(gInfo.get(), "uptimeSecs"))
        let cli  = $safeIntStr(gInfo.get(), "clientCount")
        let shd  = $safeIntStr(gInfo.get(), "shardCount")
        let ver  = $safeStr(gInfo.get(), "version")
        let cln  = $safeStr(gInfo.get(), "clusterName")
        let rep  = $safeIntStr(gHealth.get(), "healthyReplicas") & " / " & $safeIntStr(gHealth.get(), "replicaCount")
        tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:1rem;margin-bottom:1.5rem"):
          for (cardLbl, cardVal) in [("Node ID", nid), ("Role", role), ("Uptime", upt),
                                     ("Active Clients", cli), ("Shards", shd),
                                     ("Version", ver), ("Cluster", cln),
                                     ("Healthy Replicas", rep)]:
            tDiv(style = "background:#fff;border-top:3px solid #e81c1c;border-radius:6px;padding:1rem;box-shadow:0 1px 4px rgba(0,0,0,.07);text-align:center"):
              tDiv(style = "font-size:.68rem;color:#666;text-transform:uppercase;letter-spacing:.07em;margin-bottom:.5rem;font-weight:600"):
                "{cardLbl}"
              tDiv(style = "font-size:1.5rem;font-weight:700;color:#e81c1c"):
                "{cardVal}"

        # ---- Nodes list (clickable, expandable with storage details) ----
        let arr = to(gNodes.get(), seq[JsObject])
        let arrLen = arr.len
        let nodeCount = $jsLen(gNodes.get()) & (if arrLen != 1: " nodes" else: " node")
        let expanded = gExpandedNodes.get()
        tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:.75rem"):
          tH2(style = "font-size:1.05rem;font-weight:700;color:#111;margin:0"): "Nodes"
          tSpan(style = "background:#eee;color:#444;padding:.2rem .6rem;border-radius:999px;font-size:.8rem"):
            "{nodeCount}"
        tDiv(style = "display:flex;flex-direction:column;gap:0;margin-bottom:1.25rem"):
          for node in arr:
            let nid   = safeInt(node, "nodeId")
            let nhost = $safeStr(node, "host")
            let nrp   = $safeIntStr(node, "raftPort")
            let ncp   = $safeIntStr(node, "clientPort")
            let nrole = $safeStr(node, "role")
            let nalive = safeBool(node, "alive")
            let nidStr = $safeIntStr(node, "nodeId")
            let isExpanded = nid in expanded
            let chevron = if isExpanded: "▾" else: "▸"
            let rc = case nrole
              of "leader": "#1a7f37"
              of "follower": "#2563eb"
              else: "#888"
            let sc = if nalive: "#1a7f37" else: "#c41010"
            let ss = if nalive: "alive" else: "unreachable"
            let borderBot = if isExpanded: "none" else: "1px solid #e0e0e0"
            let borderRad = if isExpanded: "6px 6px 0 0" else: "6px"
            # Node header row (clickable)
            tDiv(id = "node-row-" & nidStr, style = "background:#fff;border:1px solid #e0e0e0;border-bottom:{borderBot};border-radius:{borderRad};padding:.65rem 1rem;cursor:pointer;display:flex;align-items:center;gap:.75rem"):
              tSpan(style = "color:#888;font-size:.85rem;width:1rem;text-align:center"): "{chevron}"
              tSpan(style = "font-weight:700;font-size:.9rem;min-width:2.5rem"): "{nidStr}"
              tSpan(style = "color:#555;font-size:.85rem"): "{nhost}"
              tSpan(style = "color:#999;font-size:.8rem"): ":{nrp}"
              tDiv(style = "flex:1")
              tSpan(style = "color:{rc};font-weight:600;font-size:.82rem"): "{nrole}"
              tSpan(style = "color:{sc};font-weight:600;font-size:.82rem;margin-left:.5rem"): "{ss}"
            # Expanded storage detail
            if isExpanded:
              tDiv(style = "background:#fafafa;border:1px solid #e0e0e0;border-top:none;border-radius:0 0 6px 6px;padding:1rem"):
                tDiv(style = "font-size:.82rem;font-weight:600;color:#444;margin-bottom:.65rem"): "Storage"
                let nf = node.numFiles
                let nfLen = jsArrayLen(nf)
                if nfLen > 0:
                  # Table with Level, Files, Size columns
                  tTable(style = "width:100%;max-width:420px;border-collapse:collapse;font-size:.82rem"):
                    tThead:
                      tTr:
                        for h in ["Level", "Files", "Size (MB)"]:
                          tTh(style = "background:#3a3a3a;color:#fff;padding:.4rem .7rem;text-align:left;font-size:.68rem;text-transform:uppercase;letter-spacing:.06em;font-weight:600"):
                            "{h}"
                    tTbody:
                      for lvl in 0 ..< nfLen:
                        let fc = jsParseInt(jsArrayGet(nf, lvl))
                        let fcStr = $fc
                        let lvlStr = $lvl
                        let sizeMB = $safeFloat(node.levelSizes, cstring($lvl))
                        # Color gradient: L0=#e81c1c (red) → L6=#2563eb (blue)
                        let r = int(float(232) + float(37 - 232) * float(lvl) / 6.0)
                        let g = int(float(28) + float(99 - 28) * float(lvl) / 6.0)
                        let b = int(float(28) + float(235 - 28) * float(lvl) / 6.0)
                        let barColor = "rgb(" & $r & "," & $g & "," & $b & ")"
                        let rowBg = if fc > 0: "#fff" else: "transparent"
                        tTr:
                          tTd(style = "padding:.35rem .7rem;border-bottom:1px solid #eee;background:{rowBg}"):
                            tSpan(style = "display:inline-block;width:8px;height:8px;border-radius:2px;background:{barColor};margin-right:.4rem")
                            tSpan: "L{lvlStr}"
                          tTd(style = "padding:.35rem .7rem;border-bottom:1px solid #eee;font-family:monospace;background:{rowBg}"): "{fcStr}"
                          tTd(style = "padding:.35rem .7rem;border-bottom:1px solid #eee;font-family:monospace;background:{rowBg}"): "{sizeMB}"
                else:
                  tDiv(style = "color:#999;font-size:.82rem"): "No storage data"

        # ---- Spaces list (clickable, expandable with group details) ----
        let spacesArr = gSpaces.get()
        let spacesLenStr = jsLen(spacesArr)
        let spacesLen = jsParseInt(spacesLenStr)
        let expandedSpaces = gExpandedSpaces.get()
        let spaceCount = $spacesLenStr & (if spacesLen != 1: " spaces" else: " space")
        tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:.75rem"):
          tH2(style = "font-size:1.05rem;font-weight:700;color:#111;margin:0"): "Spaces"
          tSpan(style = "background:#eee;color:#444;padding:.2rem .6rem;border-radius:999px;font-size:.8rem"):
            "{spaceCount}"
        if spacesLen == 0 and loadedSpaces:
          tDiv(style = "color:#888;font-size:.85rem;padding:1rem;margin-bottom:1.25rem"): "No spaces found."
        elif spacesLen == 0:
          tDiv(style = "color:#888;font-size:.85rem;padding:1rem;margin-bottom:1.25rem"): "Loading spaces..."
        else:
          tDiv(style = "display:flex;flex-direction:column;gap:0;margin-bottom:1.25rem"):
            for si in 0 ..< spacesLen:
              let sp = spacesArr[si]
              let sid = safeInt(sp, "spaceId")
              let sidStr = $safeIntStr(sp, "spaceId")
              let sname = $safeStr(sp, "name")
              let srep = safeInt(sp, "replicas")
              let srepStr = if srep == 0: "ALL" else: $safeIntStr(sp, "replicas")
              let sgc = $safeIntStr(sp, "groupCount")
              let isRebalancing = safeBool(sp, "rebalancing")
              let isSpaceExpanded = sid in expandedSpaces
              let schevron = if isSpaceExpanded: "▾" else: "▸"
              let sborderBot = if isSpaceExpanded: "none" else: "1px solid #e0e0e0"
              let sborderRad = if isSpaceExpanded: "6px 6px 0 0" else: "6px"
              # Space header row (clickable)
              tDiv(id = "space-row-" & sidStr, style = "background:#fff;border:1px solid #e0e0e0;border-bottom:{sborderBot};border-radius:{sborderRad};padding:.65rem 1rem;cursor:pointer;display:flex;align-items:center;gap:.75rem"):
                tSpan(style = "color:#888;font-size:.85rem;width:1rem;text-align:center"): "{schevron}"
                tSpan(style = "font-weight:700;font-size:.9rem"): "{sname}"
                tSpan(style = "background:#eee;color:#444;padding:.15rem .5rem;border-radius:999px;font-size:.75rem"):
                  "{srepStr} replicas"
                tSpan(style = "background:#eee;color:#444;padding:.15rem .5rem;border-radius:999px;font-size:.75rem"):
                  "{sgc} groups"
                if isRebalancing:
                  tSpan(style = "background:#b45309;color:#fff;padding:.15rem .5rem;border-radius:999px;font-size:.75rem;font-weight:600"):
                    "rebalancing"
              # Expanded group details
              if isSpaceExpanded:
                tDiv(style = "background:#fafafa;border:1px solid #e0e0e0;border-top:none;border-radius:0 0 6px 6px;padding:1rem"):
                  # Current groups
                  let groups = sp.groups
                  let groupsLen = jsArrayLen(groups)
                  tDiv(style = "font-size:.82rem;font-weight:600;color:#444;margin-bottom:.65rem"): "Current Groups"
                  if groupsLen > 0:
                    tDiv(style = "display:flex;flex-direction:column;gap:.5rem"):
                      for gi in 0 ..< groupsLen:
                        let grp = groups[gi]
                        let gid = $safeIntStr(grp, "groupId")
                        let members = grp.members
                        let membersLen = jsArrayLen(members)
                        tDiv(style = "background:#fff;border:1px solid #e8e8e8;border-radius:4px;padding:.5rem .75rem"):
                          tDiv(style = "display:flex;align-items:center;gap:.5rem;flex-wrap:wrap"):
                            tSpan(style = "font-weight:600;font-size:.82rem;color:#555"): "Group {gid}"
                            for mi in 0 ..< membersLen:
                              let member = members[mi]
                              let mnid = $safeIntStr(member, "nodeId")
                              let mrole = $safeStr(member, "role")
                              let mcolor = if mrole == "leader": "#1a7f37" else: "#2563eb"
                              tSpan(style = "display:inline-flex;align-items:center;gap:.25rem;font-size:.78rem"):
                                tSpan(style = "color:#666"): "n{mnid}"
                                tSpan(style = "color:{mcolor};font-weight:600;font-size:.72rem;background:#f0f0f0;padding:.1rem .4rem;border-radius:999px"):
                                  "{mrole}"
                  else:
                    tDiv(style = "color:#999;font-size:.82rem"): "No group data"
                  # Old groups (only when rebalancing)
                  if isRebalancing:
                    let oldGroups = sp.oldGroups
                    let oldGroupsLen = jsArrayLen(oldGroups)
                    tDiv(style = "font-size:.82rem;font-weight:600;color:#444;margin-top:1rem;margin-bottom:.65rem"): "Old Groups (rebalancing)"
                    if oldGroupsLen > 0:
                      tDiv(style = "display:flex;flex-direction:column;gap:.5rem"):
                        for gi in 0 ..< oldGroupsLen:
                          let grp = oldGroups[gi]
                          let gid = $safeIntStr(grp, "groupId")
                          let members = grp.members
                          let membersLen = jsArrayLen(members)
                          tDiv(style = "background:#fff;border:1px solid #e8e8e8;border-left:3px solid #b45309;border-radius:4px;padding:.5rem .75rem"):
                            tDiv(style = "display:flex;align-items:center;gap:.5rem;flex-wrap:wrap"):
                              tSpan(style = "font-weight:600;font-size:.82rem;color:#555"): "Group {gid}"
                              for mi in 0 ..< membersLen:
                                let member = members[mi]
                                let mnid = $safeIntStr(member, "nodeId")
                                let mrole = $safeStr(member, "role")
                                tSpan(style = "display:inline-flex;align-items:center;gap:.25rem;font-size:.78rem"):
                                  tSpan(style = "color:#666"): "n{mnid}"
                                  tSpan(style = "color:#888;font-weight:600;font-size:.72rem;background:#f0f0f0;padding:.1rem .4rem;border-radius:999px"):
                                    "{mrole}"
                    else:
                      tDiv(style = "color:#999;font-size:.82rem"): "No old group data"

      tFooter(style = footerStyle):
        "Fractio Management Console · Auto-refresh every 5s"

  # ---- Nodes (Join form) ----
  "/nodes":
    let hs2 = healthStr(safeInt(gHealth.get(), "status"))
    let hc2 = healthColor(safeInt(gHealth.get(), "status"))
    tDiv(style = shellStyle):
      tHeader(style = headerStyle):
        tDiv(style = logoStyle): "⬡ FRACTIO"
        tDiv(style = "flex:1")
        tSpan(style = "{badgeStyle(hc2)}"): "{hs2}"
      tNav(style = navBarStyle):
        for (href, label) in navItems:
          tA(href = href, style = "{navStyle(label == \"Nodes\")}"): "{label}"
      tMain(style = mainStyle):
        tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:1rem"):
          tH2(style = "font-size:1.05rem;font-weight:700;color:#111;margin:0"): "Cluster Nodes"
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
      tFooter(style = footerStyle):
        "Fractio Management Console · Auto-refresh every 5s"

  # ---- Metrics ----
  "/metrics":
    let hs3 = healthStr(safeInt(gHealth.get(), "status"))
    let hc3 = healthColor(safeInt(gHealth.get(), "status"))
    tDiv(style = shellStyle):
      tHeader(style = headerStyle):
        tDiv(style = logoStyle): "⬡ FRACTIO"
        tDiv(style = "flex:1")
        tSpan(style = "{badgeStyle(hc3)}"): "{hs3}"
      tNav(style = navBarStyle):
        for (href, label) in navItems:
          tA(href = href, style = "{navStyle(label == \"Metrics\")}"): "{label}"
      tMain(style = mainStyle):
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
      tFooter(style = footerStyle):
        "Fractio Management Console · Auto-refresh every 5s"

  # ---- Clock drift ----
  "/clock":
    let hs4 = healthStr(safeInt(gHealth.get(), "status"))
    let hc4 = healthColor(safeInt(gHealth.get(), "status"))
    tDiv(style = shellStyle):
      tHeader(style = headerStyle):
        tDiv(style = logoStyle): "⬡ FRACTIO"
        tDiv(style = "flex:1")
        tSpan(style = "{badgeStyle(hc4)}"): "{hs4}"
      tNav(style = navBarStyle):
        for (href, label) in navItems:
          tA(href = href, style = "{navStyle(label == \"Clock\")}"): "{label}"
      tMain(style = mainStyle):

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

      tFooter(style = footerStyle):
        "Fractio Management Console · SharedTimer drift stream"

  # ---- Storage ----
  "/storage":
    let hs6 = healthStr(safeInt(gHealth.get(), "status"))
    let hc6 = healthColor(safeInt(gHealth.get(), "status"))
    tDiv(style = shellStyle):
      tHeader(style = headerStyle):
        tDiv(style = logoStyle): "⬡ FRACTIO"
        tDiv(style = "flex:1")
        tSpan(style = "{badgeStyle(hc6)}"): "{hs6}"
      tNav(style = navBarStyle):
        for (href, label) in navItems:
          tA(href = href, style = "{navStyle(label == \"Storage\")}"): "{label}"
      tMain(style = mainStyle):
        let storagePath = $safeStr(gStorage.get(), "path")
        tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:1rem"):
          tH2(style = "font-size:1.05rem;font-weight:700;color:#111;margin:0"): "Storage"
          if storagePath.len > 0:
            tSpan(style = "background:#eee;color:#444;padding:.2rem .6rem;border-radius:999px;font-size:.8rem"):
              "{storagePath}"

        # Per-level file counts
        let numFiles = gStorage.get().numFiles
        let numFilesLen = jsArrayLen(numFiles)
        if numFilesLen > 0:
          tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(120px,1fr));gap:1rem;margin-bottom:1.25rem"):
            for lvl in 0 ..< numFilesLen:
              let fc = $jsArrayGet(numFiles, lvl)
              let lvlStr = $lvl
              tDiv(style = "background:#fff;border-top:3px solid #e81c1c;border-radius:6px;padding:1rem;box-shadow:0 1px 4px rgba(0,0,0,.07);text-align:center"):
                tDiv(style = "font-size:.68rem;color:#666;text-transform:uppercase;letter-spacing:.07em;margin-bottom:.5rem;font-weight:600"):
                  "Level {lvlStr}"
                tDiv(style = "font-size:1.5rem;font-weight:700;color:#e81c1c"):
                  "{fc} files"

        # LevelDB stats table
        let statsText = $safeStr(gStorage.get(), "stats")
        if statsText.len > 0:
          tDiv(style = "background:#fff;border-radius:6px;border:1px solid #e0e0e0;padding:1rem;margin-bottom:1.25rem"):
            tStrong: "LevelDB Compaction Stats"
            tPre(style = "margin-top:.75rem;font-size:.82rem;font-family:'SF Mono','Fira Mono',monospace;color:#222;overflow-x:auto;white-space:pre;line-height:1.5"):
              "{statsText}"
        else:
          tDiv(style = "color:#888;font-size:.85rem;padding:1rem"): "Waiting for storage data..."

      tFooter(style = footerStyle):
        "Fractio Management Console · Storage"

  # ===========================================================================
  # Data Browser — URL-routed
  # ===========================================================================

  # ---- /data — database list ----
  "/data":
    let hsD = healthStr(safeInt(gHealth.get(), "status") + triggerLoadDatabases())
    let hcD = healthColor(safeInt(gHealth.get(), "status"))
    tDiv(style = shellStyle):
      tHeader(style = headerStyle):
        tDiv(style = logoStyle): "⬡ FRACTIO"
        tDiv(style = "flex:1")
        tSpan(style = "{badgeStyle(hcD)}"): "{hsD}"
      tNav(style = navBarStyle):
        for (href, label) in navItems:
          tA(href = href, style = "{navStyle(label == \"Data\")}"): "{label}"
      tMain(style = mainStyle):
        # Breadcrumb
        tDiv(style = "display:flex;align-items:center;gap:.35rem;margin-bottom:1.25rem;font-size:.85rem;color:#666"):
          tSpan(style = "font-weight:600;color:#111"): "Databases"

        tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:.75rem"):
          # Virtual "sys" database
          tA(href = "/#/data/sys", style = "text-decoration:none;color:inherit"):
            tDiv(style = "background:#fff;border:1px solid #e0e0e0;border-radius:6px;padding:.85rem 1rem;transition:border-color .15s,box-shadow .15s"):
              tDiv(style = "font-size:.65rem;color:#999;text-transform:uppercase;letter-spacing:.07em;margin-bottom:.25rem;font-weight:600"):
                "SYSTEM DATABASE"
              tDiv(style = "font-size:.95rem;font-weight:600;color:#111"): "sys"
              tDiv(style = "font-size:.75rem;color:#888"): "System tables (nodes, groups, settings, ...)"

          # User databases
          let dbs = gDatabases.get()
          for d in dbs:
            tA(href = "/#/data/" & d, style = "text-decoration:none;color:inherit"):
              tDiv(style = "background:#fff;border:1px solid #e0e0e0;border-radius:6px;padding:.85rem 1rem;transition:border-color .15s,box-shadow .15s"):
                tDiv(style = "font-size:.65rem;color:#999;text-transform:uppercase;letter-spacing:.07em;margin-bottom:.25rem;font-weight:600"):
                  "DATABASE"
                tDiv(style = "font-size:.95rem;font-weight:600;color:#111"): "{d}"

      tFooter(style = footerStyle):
        "Fractio Management Console · Data Browser"

  # ---- /data/{db} — schema list ----
  "/data/{db}":
    let hsD2 = healthStr(safeInt(gHealth.get(), "status") + (if db != "sys": triggerLoadSchemas(db) else: 0))
    let hcD2 = healthColor(safeInt(gHealth.get(), "status"))
    tDiv(style = shellStyle):
      tHeader(style = headerStyle):
        tDiv(style = logoStyle): "⬡ FRACTIO"
        tDiv(style = "flex:1")
        tSpan(style = "{badgeStyle(hcD2)}"): "{hsD2}"
      tNav(style = navBarStyle):
        for (href, label) in navItems:
          tA(href = href, style = "{navStyle(label == \"Data\")}"): "{label}"
      tMain(style = mainStyle):
        # Breadcrumb
        tDiv(style = "display:flex;align-items:center;gap:.35rem;margin-bottom:1.25rem;font-size:.85rem;color:#666"):
          tA(href = "/#/data", style = "color:#e81c1c;font-weight:600;text-decoration:none"): "Databases"
          tSpan: " / "
          tSpan(style = "font-weight:600;color:#111"): "{db}"

        if db == "sys":
          tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:.75rem"):
            tA(href = "/#/data/sys/default", style = "text-decoration:none;color:inherit"):
              tDiv(style = "background:#fff;border:1px solid #e0e0e0;border-radius:6px;padding:.85rem 1rem;transition:border-color .15s,box-shadow .15s"):
                tDiv(style = "font-size:.65rem;color:#999;text-transform:uppercase;letter-spacing:.07em;margin-bottom:.25rem;font-weight:600"):
                  "SCHEMA"
                tDiv(style = "font-size:.95rem;font-weight:600;color:#111"): "default"
        else:
          let schemas = gSchemas.get()
          if schemas.len == 0 and loadedSchemasKey != db:
            tDiv(style = "color:#888;font-size:.85rem;padding:1rem"): "Loading schemas..."
          elif schemas.len == 0:
            tDiv(style = "color:#888;font-size:.85rem;padding:1rem"): "No schemas found."
          else:
            tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:.75rem"):
              for s in schemas:
                tA(href = "/#/data/" & db & "/" & s, style = "text-decoration:none;color:inherit"):
                  tDiv(style = "background:#fff;border:1px solid #e0e0e0;border-radius:6px;padding:.85rem 1rem;transition:border-color .15s,box-shadow .15s"):
                    tDiv(style = "font-size:.65rem;color:#999;text-transform:uppercase;letter-spacing:.07em;margin-bottom:.25rem;font-weight:600"):
                      "SCHEMA"
                    tDiv(style = "font-size:.95rem;font-weight:600;color:#111"): "{s}"

      tFooter(style = footerStyle):
        "Fractio Management Console · Data Browser"

  # ---- /data/{db}/{schema} — table list ----
  "/data/{db}/{schema}":
    let hsD3 = healthStr(safeInt(gHealth.get(), "status") + (if db == "sys": triggerLoadSystemTables() else: triggerLoadTables(db, schema)))
    let hcD3 = healthColor(safeInt(gHealth.get(), "status"))
    tDiv(style = shellStyle):
      tHeader(style = headerStyle):
        tDiv(style = logoStyle): "⬡ FRACTIO"
        tDiv(style = "flex:1")
        tSpan(style = "{badgeStyle(hcD3)}"): "{hsD3}"
      tNav(style = navBarStyle):
        for (href, label) in navItems:
          tA(href = href, style = "{navStyle(label == \"Data\")}"): "{label}"
      tMain(style = mainStyle):
        # Breadcrumb
        tDiv(style = "display:flex;align-items:center;gap:.35rem;margin-bottom:1.25rem;font-size:.85rem;color:#666"):
          tA(href = "/#/data", style = "color:#e81c1c;font-weight:600;text-decoration:none"): "Databases"
          tSpan: " / "
          tA(href = "/#/data/" & db, style = "color:#e81c1c;font-weight:600;text-decoration:none"): "{db}"
          tSpan: " / "
          tSpan(style = "font-weight:600;color:#111"): "{schema}"

        if db == "sys":
          # System tables
          let stArr = gSysTables.get()
          let stLen = jsArrayLen(stArr)
          if stLen > 0:
            tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:.75rem"):
              for si in 0 ..< stLen:
                let st = stArr[si]
                let stName = $safeStr(st, "name")
                let stDesc = $safeStr(st, "description")
                let stId = safeInt(st, "id")
                let stRows = $safeIntStr(st, "rowCount")
                tA(href = "/#/data/sys/default/" & stName, style = "text-decoration:none;color:inherit"):
                  tDiv(style = "background:#fff;border:1px solid #e0e0e0;border-left:3px solid #e81c1c;border-radius:6px;padding:.85rem 1rem;transition:border-color .15s,box-shadow .15s"):
                    tDiv(style = "font-size:.65rem;color:#999;text-transform:uppercase;letter-spacing:.07em;margin-bottom:.25rem;font-weight:600"):
                      "SYSTEM TABLE · ID {stId}"
                    tDiv(style = "font-size:.95rem;font-weight:600;color:#111"):
                      "{stName}"
                    tDiv(style = "font-size:.75rem;color:#888"):
                      "{stDesc} · {stRows} rows"
          elif loadedSysTables:
            tDiv(style = "color:#888;font-size:.85rem;padding:.5rem"): "No system tables found."
          else:
            tDiv(style = "color:#888;font-size:.85rem;padding:.5rem"): "Loading system tables..."
        else:
          # User tables
          let tables = gTables.get()
          let tablesKey = db & "." & schema
          if tables.len == 0 and loadedTablesKey != tablesKey:
            tDiv(style = "color:#888;font-size:.85rem;padding:1rem"): "Loading tables..."
          elif tables.len == 0:
            tDiv(style = "color:#888;font-size:.85rem;padding:1rem"): "No tables found."
          else:
            tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:.75rem"):
              for t in tables:
                tA(href = "/#/data/" & db & "/" & schema & "/" & t, style = "text-decoration:none;color:inherit"):
                  tDiv(style = "background:#fff;border:1px solid #e0e0e0;border-radius:6px;padding:.85rem 1rem;transition:border-color .15s,box-shadow .15s"):
                    tDiv(style = "font-size:.65rem;color:#999;text-transform:uppercase;letter-spacing:.07em;margin-bottom:.25rem;font-weight:600"):
                      "TABLE"
                    tDiv(style = "font-size:.95rem;font-weight:600;color:#111"): "{t}"

      tFooter(style = footerStyle):
        "Fractio Management Console · Data Browser"

  # ---- /data/{db}/{schema}/{table} — table rows ----
  "/data/{db}/{schema}/{table}":
    let hsD4 = healthStr(safeInt(gHealth.get(), "status") + (block:
      if db == "sys":
        let stId = sysTableIdByName(table)
        if stId < 0: triggerLoadSystemTables()
        else: triggerLoadSystemTableData(stId, table)
      else:
        triggerLoadTableData(db, schema, table)))
    let hcD4 = healthColor(safeInt(gHealth.get(), "status"))
    tDiv(style = shellStyle):
      tHeader(style = headerStyle):
        tDiv(style = logoStyle): "⬡ FRACTIO"
        tDiv(style = "flex:1")
        tSpan(style = "{badgeStyle(hcD4)}"): "{hsD4}"
      tNav(style = navBarStyle):
        for (href, label) in navItems:
          tA(href = href, style = "{navStyle(label == \"Data\")}"): "{label}"
      tMain(style = mainStyle):
        # Breadcrumb
        tDiv(style = "display:flex;align-items:center;gap:.35rem;margin-bottom:1.25rem;font-size:.85rem;color:#666"):
          tA(href = "/#/data", style = "color:#e81c1c;font-weight:600;text-decoration:none"): "Databases"
          tSpan: " / "
          tA(href = "/#/data/" & db, style = "color:#e81c1c;font-weight:600;text-decoration:none"): "{db}"
          tSpan: " / "
          tA(href = "/#/data/" & db & "/" & schema, style = "color:#e81c1c;font-weight:600;text-decoration:none"): "{schema}"
          tSpan: " / "
          tSpan(style = "font-weight:600;color:#111"): "{table}"

        tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:1rem"):
          tH2(style = "font-size:1.05rem;font-weight:700;color:#111;margin:0"): "{table}"

        if db == "sys":
          # System table rows
          let stId = sysTableIdByName(table)
          if stId < 0:
            tDiv(style = "color:#888;font-size:.85rem"): "Loading..."
          else:
            let std = gSysTableData.get()
            let sysCols = std.columns
            let numCols = jsArrayLen(sysCols)
            let sysRows = std.rows
            let sysRowLen = jsArrayLen(sysRows)
            if numCols == 0 and loadedSysTableDataKey != table:
              tDiv(style = "color:#888;font-size:.85rem"): "Loading system table data..."
            else:
              tDiv(style = "margin-bottom:.75rem;font-size:.82rem;color:#888"):
                "{sysRowLen} row(s)"
              tDiv(style = "overflow-x:auto"):
                tTable(style = "width:100%;border-collapse:collapse;font-size:.875rem;background:#fff;border:1px solid #e0e0e0;border-radius:6px;overflow:hidden"):
                  tThead:
                    tTr:
                      for ci in 0 ..< numCols:
                        let colName = $jsArrayGet(sysCols, ci)
                        tTh(style = "background:#3a3a3a;color:#fff;padding:.55rem .85rem;text-align:left;font-size:.7rem;text-transform:uppercase;letter-spacing:.07em;font-weight:600"):
                          "{colName}"
                  tTbody:
                    for ri in 0 ..< sysRowLen:
                      let row = sysRows[ri]
                      tTr:
                        for ci in 0 ..< numCols:
                          let colKey = jsArrayGet(sysCols, ci)
                          let cellVal = $jsObjField(row, colKey)
                          tTd(style = "padding:.55rem .85rem;border-bottom:1px solid #eee;font-family:monospace;font-size:.82rem"):
                            "{cellVal}"
        else:
          # User table rows
          let td = gTableData.get()
          let tdKind = $safeStr(td, "kind")
          if tdKind.len == 0:
            tDiv(style = "color:#888;font-size:.85rem"): "Loading table data..."
          elif tdKind == "rows":
            let cols = td.columns
            let colLen = safeInt(cols, "length")
            let dataRows = td.rows
            let rowLen = safeInt(dataRows, "length")
            tDiv(style = "margin-bottom:.75rem;font-size:.82rem;color:#888"):
              "{rowLen} row(s)"
            tDiv(style = "overflow-x:auto"):
              tTable(style = "width:100%;border-collapse:collapse;font-size:.875rem;background:#fff;border:1px solid #e0e0e0;border-radius:6px;overflow:hidden"):
                tThead:
                  tTr:
                    for ci in 0 ..< colLen:
                      let colName = $safeStr(cols, cstring($ci))
                      tTh(style = "background:#3a3a3a;color:#fff;padding:.55rem .85rem;text-align:left;font-size:.7rem;text-transform:uppercase;letter-spacing:.07em;font-weight:600"):
                        "{colName}"
                tTbody:
                  for ri in 0 ..< rowLen:
                    let row = dataRows[ri]
                    tTr:
                      for ci in 0 ..< colLen:
                        let cn = $safeStr(cols, cstring($ci))
                        let cellVal = $safeStr(row, cstring(cn))
                        tTd(style = "padding:.55rem .85rem;border-bottom:1px solid #eee;font-family:monospace;font-size:.82rem"):
                          "{cellVal}"
          elif tdKind == "error":
            let errMsg = $safeStr(td, "error")
            tDiv(style = "color:#c41010;font-size:.85rem"): "{errMsg}"
          else:
            tDiv(style = "color:#888;font-size:.85rem"): "Loading table data..."

      tFooter(style = footerStyle):
        "Fractio Management Console · Data Browser"

# ---------------------------------------------------------------------------
# Boot
# ---------------------------------------------------------------------------

when isMainModule:
  installLinkInterceptor()
  installNodeClickHandler(proc(nid: int) = toggleNodeExpanded(nid))
  installSpaceClickHandler(proc(sid: int) = toggleSpaceExpanded(sid))
  discard doRefresh()
  jsSetInterval(proc() = discard doRefresh(), 5000)
  connectDriftWs()
  # Initial DOM injection in case WS hasn't fired yet when page first loads
  jsSetTimeout(proc() = injectClockDom(), 100)
