# Fractio Web Dashboard - Dashboard Stats Component
#
# Reactive component for displaying top-level statistics.

import happyx
import std/jsffi
import ../styles
import ../store
import ../js_interop
import ../utils/helpers

component DashboardStats:
  `html`:
    let dark = gDarkMode.get()
    let health = gHealth.get()
    let info = gInfo.get()
    let nid = $safeIntStr(info, "nodeId")
    let role = $safeRoleStr(info)
    let upt = uptimeStr(safeInt(info, "uptimeSecs"))
    let cli = $safeIntStr(info, "clientCount")
    let shd = $safeIntStr(info, "shardCount")
    let ver = $safeStr(info, "version")
    let cln = $safeStr(info, "clusterName")
    let rep = $safeIntStr(health, "healthyReplicas") & " / " & $safeIntStr(
        health, "replicaCount")

    tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:1rem;margin-bottom:1.5rem"):
      tDiv(style = statCardStyle(dark)):
        tDiv(style = labelStyle(dark)):
          "Node ID"
        tDiv(style = valueStyle(dark)):
          {nid}
      tDiv(style = statCardStyle(dark)):
        tDiv(style = labelStyle(dark)):
          "Role"
        tDiv(style = valueStyle(dark)):
          {role}
      tDiv(style = statCardStyle(dark)):
        tDiv(style = labelStyle(dark)):
          "Uptime"
        tDiv(style = valueStyle(dark)):
          {upt}
      tDiv(style = statCardStyle(dark)):
        tDiv(style = labelStyle(dark)):
          "Active Clients"
        tDiv(style = valueStyle(dark)):
          {cli}
      tDiv(style = statCardStyle(dark)):
        tDiv(style = labelStyle(dark)):
          "Shards"
        tDiv(style = valueStyle(dark)):
          {shd}
      tDiv(style = statCardStyle(dark)):
        tDiv(style = labelStyle(dark)):
          "Version"
        tDiv(style = valueStyle(dark)):
          {ver}
      tDiv(style = statCardStyle(dark)):
        tDiv(style = labelStyle(dark)):
          "Cluster"
        tDiv(style = valueStyle(dark)):
          {cln}
      tDiv(style = statCardStyle(dark)):
        tDiv(style = labelStyle(dark)):
          "Healthy Replicas"
        tDiv(style = valueStyle(dark)):
          {rep}
