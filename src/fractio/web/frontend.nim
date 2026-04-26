# Fractio Web Dashboard — HappyX SPA frontend
#
# Compiled to JavaScript with:
#   nim js -d:release -o:src/fractio/web/static/app.js src/fractio/web/frontend.nim
#   npx terser src/fractio/web/static/app.js --compress --mangle -o src/fractio/web/static/app.min.js
#
# This file is the main entry point with route definitions.

import happyx

import ./styles
import ./store
import ./api
import ./js_interop
import ./dom
import ./utils/helpers
import ./components/[stat_card, node_row, space_row, header, footer, toast,
    modal, dashboard_stats, data_browser]
# Note: routes/*.nim use mount-based routing which conflicts with appRoutes.
# They need to be integrated differently or the routes here need API trigger calls.

# =============================================================================
# Helper procs for JsObject field access
# =============================================================================

proc getField(obj: JsObject, field: cstring): JsObject =
  {.emit: "return `obj`[`field`];".}

proc getIntField(obj: JsObject, field: cstring): int =
  {.emit: "return `obj`[`field`] || 0;".}

proc getStrField(obj: JsObject, field: cstring): cstring =
  {.emit: "return `obj`[`field`] || '';".}

proc getObjLen(obj: JsObject): int =
  {.emit: "return `obj`.length || 0;".}

proc getObjElem(obj: JsObject, idx: int): JsObject =
  {.emit: "return `obj`[`idx`];".}

proc titleStyle(dark: bool): string =
  "font-size:1.05rem;font-weight:700;color:" & (if dark: DarkText else: "#111")

proc sectionStyle(): string =
  "margin-bottom:1.5rem"

# =============================================================================
# App Initialization
# =============================================================================

proc initApp*() =
  # Inject theme CSS
  injectThemeCss()

  # Install link interceptor for hash-based routing
  installLinkInterceptor()

  # Initial data refresh
  discard refreshAll()

  # Start auto-refresh
  startAutoRefresh(5000)

  # Connect WebSocket for clock drift
  connectDriftWs()

  # Initial DOM injection for clock chart
  js_interop.jsSetTimeout(proc() = injectClockDom(), 100)

# =============================================================================
# Main Application Routes
# =============================================================================

appRoutes "app":
  "/":
    let dark = gDarkMode.get()

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tMain(style = mainStyle(dark)):
        # Stat cards (reactive component)
        DashboardStats()

        # Nodes section
        NodeList()

        # Spaces section
        SpaceList()

      AppFooter()
      ToastContainer()
      GlobalModal()

  "/nodes":
    let dark = gDarkMode.get()
    tDiv(style = shellStyle(dark)):
      AppHeader()
      tMain(style = mainStyle(dark)):
        NodeList()
      AppFooter()
      ToastContainer()

  "/metrics":
    let dark = gDarkMode.get()
    let metrics = gMetrics.get()
    let titleSt = titleStyle(dark)

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tMain(style = mainStyle(dark)):
        tH2(style = titleSt):
          "Metrics"
        tDiv(style = cardStyle(dark)):
          let mLen = getObjLen(metrics)
          if mLen > 0:
            tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:1rem"):
              for i in 0 ..< mLen:
                let m = getObjElem(metrics, i)
                let mName = $getStrField(m, "name")
                let mValue = $getStrField(m, "value")
                StatCard(label = mName, value = mValue)
          else:
            tDiv(style = "text-align:center;padding:2rem;color:" & (
                if dark: DarkTextMuted else: "#666")):
              "No metrics available"
      AppFooter()

  "/clock":
    let dark = gDarkMode.get()
    let titleSt = titleStyle(dark)

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tMain(style = mainStyle(dark)):
        tH2(style = titleSt):
          "Clock Drift Monitor"
        tDiv(style = cardStyle(dark)):
          tDiv(style = "display:grid;grid-template-columns:repeat(3,1fr);gap:1rem;margin-bottom:1rem"):
            tDiv:
              tDiv(style = labelStyle(dark)):
                "WebSocket Status"
              tDiv(id = "clock-ws-status", style = valueStyle(dark)):
                "connecting..."
            tDiv:
              tDiv(style = labelStyle(dark)):
                "Last Offset"
              tDiv(id = "clock-last-offset", style = valueStyle(dark)):
                "—"
            tDiv:
              tDiv(style = labelStyle(dark)):
                "Sample Count"
              tDiv(id = "clock-sample-count", style = valueStyle(dark)):
                "0"
          tDiv(id = "drift-chart"):
            ""
      AppFooter()

  "/storage":
    let dark = gDarkMode.get()
    let storage = gStorage.get()
    let titleSt = titleStyle(dark)

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tMain(style = mainStyle(dark)):
        tH2(style = titleSt):
          "Storage (LevelDB Stats)"
        tDiv(style = cardStyle(dark)):
          let sLen = getObjLen(storage)
          if sLen > 0:
            tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:1rem"):
              for i in 0 ..< sLen:
                let s = getObjElem(storage, i)
                let sName = $getStrField(s, "name")
                let sValue = $getStrField(s, "value")
                StatCard(label = sName, value = sValue)
          else:
            tDiv(style = "text-align:center;padding:2rem;color:" & (
                if dark: DarkTextMuted else: "#666")):
              "No storage stats available"
      AppFooter()

  "/data":
    let dark = gDarkMode.get()
    let titleSt = titleStyle(dark)

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tMain(style = mainStyle(dark)):
        tH2(style = titleSt):
          "Data Browser"

        # Data browser component with selectors + table grid
        DataBrowser()
      AppFooter()

  "/sql":
    let dark = gDarkMode.get()
    let titleSt = titleStyle(dark)
    let sqlQuery = gSqlQuery.get()
    let sqlResult = gSqlResult.get()
    let currentDb = gCurrentDatabase.get()
    let currentSch = gCurrentSchema.get()
    let dbVal = if currentDb.len > 0: currentDb else: "default"
    let schVal = if currentSch.len > 0: currentSch else: "public"

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tMain(style = mainStyle(dark)):
        tH2(style = titleSt):
          "SQL Editor"

        # Database/Schema selector
        tDiv(style = cardStyle(dark) & ";margin-bottom:1rem"):
          tDiv(style = "display:flex;gap:1rem;align-items:center"):
            tSlSelect(label = "Database", size = "small", value = dbVal):
              tSlOption(value = "default"):
                "default"
            tSlSelect(label = "Schema", size = "small", value = schVal):
              tSlOption(value = "public"):
                "public"

        # SQL query input
        tDiv(style = cardStyle(dark) & ";margin-bottom:1rem"):
          tSlTextarea(label = "SQL Query", placeholder = "Enter SQL query...",
              size = "small", rows = "5"):
            if sqlQuery.len > 0:
              {sqlQuery}

        # Execute button
        tDiv(style = "margin-bottom:1rem"):
          tSlButton(variant = "primary", size = "small"):
            "Execute Query"

        # Results section
        tDiv(style = cardStyle(dark)):
          tH3(style = "font-size:0.9rem;font-weight:600;margin-bottom:0.5rem"):
            "Results"
          let rLen = getObjLen(sqlResult)
          if rLen > 0:
            tDiv(style = "overflow-x:auto"):
              "Query results displayed here"
          else:
            tDiv(style = "text-align:center;padding:1rem;color:" & (
                if dark: DarkTextMuted else: "#666")):
              "No query executed yet"
      AppFooter()

  "/settings":
    let dark = gDarkMode.get()
    let titleSt = titleStyle(dark)

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tMain(style = mainStyle(dark)):
        tH2(style = titleSt):
          "Settings"

        tDiv(style = cardStyle(dark)):
          # Theme setting
          tDiv(style = "display:flex;align-items:center;gap:1rem;margin-bottom:1rem"):
            tDiv(style = labelStyle(dark)):
              "Theme Mode"
            tSlSwitch(checked = dark):
              if dark: "Dark Mode" else: "Light Mode"
            tSlButton(variant = "default", size = "small"):
              "Toggle Theme"
              @click:
                discard toggleDarkMode()

          # Auto-refresh setting
          tDiv(style = "display:flex;align-items:center;gap:1rem;margin-bottom:1rem"):
            tDiv(style = labelStyle(dark)):
              "Auto-Refresh Interval"
            tSlSelect(size = "small"):
              tSlOption(value = "1000"):
                "1 second"
              tSlOption(value = "5000", selected = true):
                "5 seconds"
              tSlOption(value = "10000"):
                "10 seconds"
              tSlOption(value = "30000"):
                "30 seconds"

          # Info display
          tDiv(style = "margin-top:1rem;padding-top:1rem;border-top:1px solid " &
              (if dark: DarkBorder else: LightBorder)):
            tH3(style = "font-size:0.9rem;font-weight:600;margin-bottom:0.5rem"):
              "System Information"
            let info = gInfo.get()
            let infoText = "Version: " & $safeStr(info, "version") &
                " | Node ID: " & $safeIntStr(info, "nodeId") &
                " | Role: " & $safeRoleStr(info)
            tDiv(style = "font-size:0.8rem;color:" & (
                if dark: DarkTextMuted else: "#666")):
              {infoText}
      AppFooter()

  notfound:
    let dark = gDarkMode.get()
    tDiv(style = shellStyle(dark)):
      AppHeader()
      tMain(style = mainStyle(dark)):
        tDiv(style = "display:flex;flex-direction:column;align-items:center;justify-content:center;height:50vh;gap:1rem"):
          tDiv(style = "font-size:5rem;font-weight:700;color:" & PrimaryColor):
            "404"
          tDiv(style = "font-size:1.2rem;color:" & (
              if dark: DarkText else: "#111")):
            "Page not found"
          tA(href = "/#/", style = "color:" & PrimaryColor &
              ";font-weight:600;text-decoration:none"):
            "Return to Dashboard"
      AppFooter()

# =============================================================================
# Boot
# =============================================================================

when isMainModule:
  initApp()
