# Fractio Web Dashboard - Settings Route
#
# Application settings including theme, refresh interval, and cluster configuration.

import happyx
import std/jsffi
import ../styles
import ../store
import ../js_interop
import ../api
import ../components/[header, footer, toast, modal]

var gRefreshInterval* {.global.}: State[int] = remember 5000

mount "/settings" -> SettingsRoute:
  "/":
    let dark = gDarkMode.get()

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tNav(style = navBarStyle(dark)):
        for (href, label) in navItems:
          let isActive = label == "Settings"
          tA(href = href, style = navStyle(isActive, dark)):
            label
      tMain(style = mainStyle(dark)):
        # Title
        tH2(style = "font-size:1.05rem;font-weight:700;color:" & (
            if dark: DarkText else: "#111") & ";margin:0;margin-bottom:1rem"):
          "Settings"

        # Settings cards
        tDiv(style = "display:flex;flex-direction:column;gap:1rem"):
          # Theme settings
          tDiv(style = cardStyle(dark)):
            tStrong(style = "font-size:.9rem;color:" & (
                if dark: DarkText else: "#111")):
              "Appearance"
            tDiv(style = "margin-top:.75rem"):
              tDiv(style = "display:flex;align-items:center;gap:.5rem"):
                tSlSwitch(
                  checked = dark,
                  @sl-change = toggleDarkMode()
                ):
                  "Dark Mode"

          # Refresh interval
          tDiv(style = cardStyle(dark)):
            tStrong(style = "font-size:.9rem;color:" & (
                if dark: DarkText else: "#111")):
              "Auto-Refresh"
            tDiv(style = "margin-top:.75rem"):
              tSlSelect(
                label = "Refresh Interval",
                value = $gRefreshInterval.get(),
                @sl-change = proc(ev: JsObject) =
                let val = parseInt($safeStr(ev, "value"))
                gRefreshInterval.set(val)
              ):
                tSlOption(value = "1000"): "1 second"
                tSlOption(value = "5000"): "5 seconds"
                tSlOption(value = "10000"): "10 seconds"
                tSlOption(value = "30000"): "30 seconds"
                tSlOption(value = "0"): "Disabled"

          # Cluster info
          tDiv(style = cardStyle(dark)):
            tStrong(style = "font-size:.9rem;color:" & (
                if dark: DarkText else: "#111")):
              "Cluster Information"
            tDiv(style = "margin-top:.75rem;display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:.5rem;font-size:.85rem"):
              tDiv:
                tSpan(style = "color:" & (if dark: DarkTextMuted else: "#666")):
                  "Cluster Name: "
                tSpan(style = "font-weight:600;color:" & (
                    if dark: DarkText else: "#111")):
                  $safeStr(gInfo.get(), "clusterName")
              tDiv:
                tSpan(style = "color:" & (if dark: DarkTextMuted else: "#666")):
                  "Node ID: "
                tSpan(style = "font-weight:600;color:" & (
                    if dark: DarkText else: "#111")):
                  $safeIntStr(gInfo.get(), "nodeId")
              tDiv:
                tSpan(style = "color:" & (if dark: DarkTextMuted else: "#666")):
                  "Version: "
                tSpan(style = "font-weight:600;color:" & (
                    if dark: DarkText else: "#111")):
                  $safeStr(gInfo.get(), "version")
              tDiv:
                tSpan(style = "color:" & (if dark: DarkTextMuted else: "#666")):
                  "Role: "
                tSpan(style = "font-weight:600;color:" & (
                    if dark: DarkText else: "#111")):
                  $safeStr(gInfo.get(), "role")

          # Saved queries management
          tDiv(style = cardStyle(dark)):
            tStrong(style = "font-size:.9rem;color:" & (
                if dark: DarkText else: "#111")):
              "Query History"
            tDiv(style = "margin-top:.75rem"):
              let hist = gSqlHistory.get()
              if hist.len > 0:
                tDiv(style = "font-size:.82rem;color:" & (
                    if dark: DarkTextMuted else: "#666")):
                  $hist.len & " queries in history"
                tSlButton(
                  variant = "danger",
                  size = "small",
                  style = "margin-top:.5rem",
                  @click = proc() =
                  gSqlHistory.set(newSeq[string]())
                  showSuccess("Query history cleared")
                ):
                  "Clear History"
              else:
                tDiv(style = "font-size:.82rem;color:" & (
                    if dark: DarkTextMuted else: "#888")):
                  "No query history"

      AppFooter()
      ToastContainer()
      GlobalModal()
