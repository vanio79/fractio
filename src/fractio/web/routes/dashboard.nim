# Fractio Web Dashboard - Dashboard Route
#
# Main dashboard view with stats, nodes, and spaces.

import happyx
import std/jsffi
import ../styles
import ../store
import ../js_interop
import ../utils/helpers
import ../components/[stat_card, node_row, space_row, header, footer, toast, modal]

# Helper to safely get role string from JsObject (handles both string and numeric roles)
proc safeRoleStr*(obj: JsObject): cstring =
  {.emit: """
  var v = `obj`['role'];
  if (typeof v === 'string') {
    var s = v.toLowerCase();
    `result` = (s === 'leader' ? 'Leader' : s === 'follower' ? 'Follower' : s === 'candidate' ? 'Candidate' : 'Unknown');
  } else {
    var n = Number(v) || 0;
    `result` = (n === 1 ? 'Leader' : n === 2 ? 'Follower' : n === 3 ? 'Candidate' : 'Unknown');
  }
  """.}

# Dashboard route handler
proc renderDashboard(): string =
  let dark = gDarkMode.get()
  let health = gHealth.get()
  let info = gInfo.get()
  let hs = healthStr(safeInt(health, "status"))
  let hc = healthColor(safeInt(health, "status"))
  let nid = $safeIntStr(info, "nodeId")
  let role = safeRoleStr(info)
  let upt = uptimeStr(safeInt(info, "uptimeSecs"))
  let cli = $safeIntStr(info, "clientCount")
  let shd = $safeIntStr(info, "shardCount")
  let ver = $safeStr(info, "version")
  let cln = $safeStr(info, "clusterName")
  let rep = $safeIntStr(health, "healthyReplicas") & " / " & $safeIntStr(health, "replicaCount")

  buildHtml:
    tDiv(style = shellStyle(dark)):
      AppHeader()
      tMain(style = mainStyle(dark)):
        # Stat cards
        tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:1rem;margin-bottom:1.5rem"):
          StatCard(label = "Node ID", value = nid)
          StatCard(label = "Role", value = role)
          StatCard(label = "Uptime", value = upt)
          StatCard(label = "Active Clients", value = cli)
          StatCard(label = "Shards", value = shd)
          StatCard(label = "Version", value = ver)
          StatCard(label = "Cluster", value = cln)
          StatCard(label = "Healthy Replicas", value = rep)

        # Nodes section
        NodeList()

        # Spaces section
        SpaceList()

      AppFooter()
      ToastContainer()
      GlobalModal()

# Export for use in frontend.nim
export renderDashboard
