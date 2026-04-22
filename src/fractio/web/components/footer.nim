# Fractio Web Dashboard - Footer Component
#
# Application footer with branding and version info.

import happyx
import ../styles
import ../store
import ../js_interop

component Footer:
  autoRefreshInterval: int = 5000

  `html`:
    let dark = gDarkMode.get()
    let info = gInfo.get()
    let version = $safeStr(info, "version")
    let interval = self.autoRefreshInterval.get()
    let intervalSec = interval / 1000
    let versionText = if version.len > 0: "· v" & version else: ""
    let refreshText = "· Auto-refresh every " & $intervalSec & "s"

    tFooter(style = footerStyle(dark)):
      tDiv(style = "display:flex;align-items:center;gap:.5rem"):
        tSpan:
          "Fractio Management Console"
        if version.len > 0:
          tSpan:
            {versionText}
        tSpan:
          {refreshText}

component AppFooter:
  `html`:
    let dark = gDarkMode.get()

    tFooter(style = footerStyle(dark)):
      "Fractio Management Console · Auto-refresh every 5s"
