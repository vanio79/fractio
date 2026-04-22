# Fractio Web Dashboard - Clock Route
#
# Clock drift visualization via WebSocket stream.

import happyx
import std/jsffi
import ../styles
import ../store
import ../js_interop
import ../chart
import ../dom
import ../components/[header, footer, toast, modal]

mount "/clock" -> ClockRoute:
  "/":
    let dark = gDarkMode.get()
    let hs = healthStr(safeInt(gHealth.get(), "status"))
    let hc = healthColor(safeInt(gHealth.get(), "status"))

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tNav(style = navBarStyle(dark)):
        for (href, label) in navItems:
          let isActive = label == "Clock"
          tA(href = href, style = navStyle(isActive, dark)):
            label
      tMain(style = mainStyle(dark)):
        # Title row
        tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:1.25rem"):
          tH2(style = "font-size:1.05rem;font-weight:700;color:" & (
              if dark: DarkText else: "#111") & ";margin:0"):
            "SharedTimer Clock Drift"
          tDiv(id = "clock-ws-status")

        # Chart card
        tDiv(style = cardStyle(dark) & ";padding:1rem 1rem .5rem;margin-bottom:1.25rem"):
          tDiv(style = "display:flex;justify-content:space-between;margin-bottom:.25rem"):
            tSpan(style = "font-size:.65rem;color:#999;font-family:monospace"):
              "+25 ms"
            tSpan(style = "font-size:.65rem;color:#666;font-family:monospace"):
              "clock offset"
            tSpan(style = "font-size:.65rem;color:#999;font-family:monospace"):
              "-25 ms"

          # SVG container
          tDiv(id = "drift-chart", style = "width:100%;min-height:120px")

          # X-axis legend
          tDiv(style = "display:flex;justify-content:space-between;margin-top:.35rem"):
            tSpan(style = "font-size:.65rem;color:#999;font-family:monospace"):
              "-2 min"
            tSpan(style = "font-size:.65rem;color:#999;font-family:monospace"):
              "now"

        # Stats row
        tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:1rem;margin-bottom:1.25rem"):
          tDiv(style = statCardStyle(dark)):
            tDiv(style = labelStyle(dark)):
              "Latest Offset"
            tDiv(id = "clock-last-offset")
          tDiv(style = statCardStyle(dark)):
            tDiv(style = labelStyle(dark)):
              "Samples"
            tDiv(id = "clock-sample-count")
          tDiv(style = statCardStyle(dark)):
            tDiv(style = labelStyle(dark)):
              "Threshold"
            tDiv(style = "font-size:1.2rem;font-weight:700;color:#e81c1c;font-family:monospace"):
              "±10 ms"

        # Legend
        tDiv(style = "font-size:.78rem;color:" & (
            if dark: DarkTextMuted else: "#888")):
          "━ Clock offset   ╌ ±10 ms threshold   ┄ 0 ms baseline   · Updates every 1 s"

      AppFooter()
      ToastContainer()
      GlobalModal()
