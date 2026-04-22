# Fractio Web Dashboard - Header Component
#
# Application header with logo, navigation, and theme toggle.

import happyx
import std/strutils
import ../styles
import ../store
import ../js_interop
import ../utils/helpers

# Helper to get active route from hash
proc getActiveRoute(): string =
  var routeName = "Dashboard"
  {.emit: """
  var hash = window.location.hash || "#/";
  var parts = hash.split('/');
  if (parts.length > 1 && parts[1] !== '') {
    `routeName` = parts[1].charAt(0).toUpperCase() + parts[1].slice(1);
  }
  """.}
  return routeName

# Helper to check if route is active
proc isRouteActive(href: cstring, activeRoute: string): bool =
  let h = $href
  let arLower = activeRoute.toLowerAscii()
  if activeRoute == "Dashboard" and h == "/#/":
    return true
  if arLower.len > 0 and h.startsWith("/#/" & arLower):
    return true
  return false

component HeaderNav:
  activeRoute: string

  `html`:
    let dark = gDarkMode.get()
    let active = self.activeRoute.get()
    tNav(style = navBarStyle(dark)):
      for (href, label) in navItems:
        let isActive = label == active or isRouteActive(href, active)
        tA(href = href, style = navStyle(isActive, dark)):
          {label}

component ThemeToggle:
  `html`:
    let dark = gDarkMode.get()
    let iconName = if dark: "sun" else: "moon"
    let iconLabel = if dark: "Switch to light mode" else: "Switch to dark mode"
    tSlIconButton(
      name = iconName,
      label = iconLabel,
      style = "color:#fff"
    ):
      @click:
        discard toggleDarkMode()

component Header:
  activeRoute: string = "Dashboard"
  showHealthBadge: bool = true

  `html`:
    let dark = gDarkMode.get()
    let health = gHealth.get()
    let hs = healthStr(safeInt(health, "status"))
    let hc = healthColor(safeInt(health, "status"))
    let showBadge = self.showHealthBadge.get()
    let logoStr = logoStyle()

    tHeader(style = headerStyle(dark)):
      tDiv(style = logoStr):
        "⬡ FRACTIO"
      tDiv(style = "flex:1")
      if showBadge:
        tSpan(style = badgeStyle(hc)):
          {hs}
      ThemeToggle()

component AppHeader:
  `html`:
    let dark = gDarkMode.get()
    let health = gHealth.get()
    let hs = healthStr(safeInt(health, "status"))
    let hc = healthColor(safeInt(health, "status"))
    let logoStr = logoStyle()
    let activeRoute = getActiveRoute()

    tHeader(style = headerStyle(dark)):
      tDiv(style = logoStr):
        "⬡ FRACTIO"
      tDiv(style = "flex:1")
      tSpan(style = badgeStyle(hc)):
        {hs}
      ThemeToggle()

    HeaderNav(activeRoute = activeRoute)
