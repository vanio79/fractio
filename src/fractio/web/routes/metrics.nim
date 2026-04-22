# Fractio Web Dashboard - Metrics Route
#
# Server metrics view with real-time charts.

import happyx
import std/jsffi
import ../styles
import ../store
import ../js_interop
import ../components/[header, footer, toast, modal, metrics_chart]

mount "/metrics" -> MetricsRoute:
  "/":
    let dark = gDarkMode.get()
    let hs = healthStr(safeInt(gHealth.get(), "status"))
    let hc = healthColor(safeInt(gHealth.get(), "status"))

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tNav(style = navBarStyle(dark)):
        for (href, label) in navItems:
          let isActive = label == "Metrics"
          tA(href = href, style = navStyle(isActive, dark)):
            label
      tMain(style = mainStyle(dark)):
        MetricsPageContent()

      AppFooter()
      ToastContainer()
      GlobalModal()
