# Fractio Web Dashboard - Nodes Route
#
# Node management view with join/remove functionality.

import happyx
import std/jsffi
import ../styles
import ../store
import ../js_interop
import ../api
import ../components/[header, footer, toast, modal, node_row]

mount "/nodes" -> NodesRoute:
  "/":
    let dark = gDarkMode.get()
    let hs = healthStr(safeInt(gHealth.get(), "status"))
    let hc = healthColor(safeInt(gHealth.get(), "status"))

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tNav(style = navBarStyle(dark)):
        for (href, label) in navItems:
          let isActive = label == "Nodes"
          tA(href = href, style = navStyle(isActive, dark)):
            label
      tMain(style = mainStyle(dark)):
        # Header
        tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:1rem"):
          tH2(style = "font-size:1.05rem;font-weight:700;color:" & (
              if dark: DarkText else: "#111") & ";margin:0"):
            "Cluster Nodes"

        # Join node form
        tDiv(style = cardStyle(dark) & ";margin-bottom:1.25rem"):
          tStrong(style = "color:" & (if dark: DarkText else: "#111")):
            "Join New Node"
          tDiv(style = "display:flex;gap:.5rem;flex-wrap:wrap;align-items:flex-end;margin:.75rem 0 .5rem"):
            tSlInput(
              id = "join-id",
              type = "number",
              label = "Node ID",
              size = "small",
              style = "width:130px"
            )
            tSlInput(
              id = "join-host",
              label = "Host",
              size = "small",
              style = "width:190px"
            )
            tSlInput(
              id = "join-raft",
              type = "number",
              label = "Raft Port",
              size = "small",
              style = "width:130px"
            )
            tSlInput(
              id = "join-client",
              type = "number",
              label = "Client Port",
              size = "small",
              style = "width:130px"
            )
            tSlInput(
              id = "join-web",
              type = "number",
              label = "Web Port",
              size = "small",
              style = "width:130px"
            )
            tSlButton(
              variant = "primary",
              size = "small",
              @click = proc() =
              let nodeId = jsParseInt(getInputVal("join-id"))
              let host = getInputVal("join-host")
              let raftPort = jsParseInt(getInputVal("join-raft"))
              let clientPort = jsParseInt(getInputVal("join-client"))
              let webPort = jsParseInt(getInputVal("join-web"))
              if nodeId > 0 and host.len > 0:
                discard clusterJoin(nodeId, $host, raftPort, clientPort, webPort)
            ):
              "Join Node"

          # Message display
          if gMsg.get().len > 0:
            let mc = if gMsgOk.get(): SuccessColor else: DangerColor
            tDiv(style = "font-size:.82rem;color:" & mc):
              gMsg.get()

        # Node list
        NodeList()

        # Rebalance button
        tDiv(style = "margin-top:1rem"):
          tSlButton(
            variant = "warning",
            size = "small",
            @click = proc() =
            showModal(
              "Trigger Rebalance",
              "Are you sure you want to trigger a space rebalance? This may redistribute data across nodes.",
              proc() = discard triggerRebalance(),
              "Rebalance",
              "Cancel",
              true
            )
          ):
            "Trigger Space Rebalance"

      AppFooter()
      ToastContainer()
      GlobalModal()
