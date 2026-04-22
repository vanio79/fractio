# Fractio Web Dashboard - Storage Route
#
# Storage engine view with LevelDB stats and per-level details.

import happyx
import std/jsffi
import ../styles
import ../store
import ../js_interop
import ../components/[header, footer, toast, modal, stat_card]

mount "/storage" -> StorageRoute:
  "/":
    let dark = gDarkMode.get()
    let hs = healthStr(safeInt(gHealth.get(), "status"))
    let hc = healthColor(safeInt(gHealth.get(), "status"))

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tNav(style = navBarStyle(dark)):
        for (href, label) in navItems:
          let isActive = label == "Storage"
          tA(href = href, style = navStyle(isActive, dark)):
            label
      tMain(style = mainStyle(dark)):
        let storagePath = $safeStr(gStorage.get(), "path")

        # Header
        tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:1rem"):
          tH2(style = "font-size:1.05rem;font-weight:700;color:" & (
              if dark: DarkText else: "#111") & ";margin:0"):
            "Storage"
          if storagePath.len > 0:
            tSlTag(variant = "neutral", size = "small"):
              storagePath

        # Per-level file counts
        let numFiles = gStorage.get().numFiles
        let numFilesLen = jsArrayLen(numFiles)
        let levelSizes = gStorage.get().levelSizes

        if numFilesLen > 0:
          tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(120px,1fr));gap:1rem;margin-bottom:1.25rem"):
            for lvl in 0 ..< numFilesLen:
              let fc = $jsArrayGet(numFiles, lvl)
              let lvlStr = $lvl
              let sizeMB = if not levelSizes.isNil: $safeFloat(levelSizes,
                  cstring($lvl)) else: "0"

              # Color gradient: L0=red -> L6=blue
              let r = int(float(232) + float(37 - 232) * float(lvl) / 6.0)
              let g = int(float(28) + float(99 - 28) * float(lvl) / 6.0)
              let b = int(float(28) + float(235 - 28) * float(lvl) / 6.0)
              let levelColor = "rgb(" & $r & "," & $g & "," & $b & ")"

              tDiv(style = statCardStyle(dark) & ";border-top-color:" & levelColor):
                tDiv(style = labelStyle(dark)):
                  "Level " & lvlStr
                tDiv(style = valueStyle(dark) & ";color:" & levelColor):
                  fc & " files"
                tDiv(style = "font-size:.75rem;color:" & (
                    if dark: DarkTextMuted else: "#666") &
                    ";margin-top:.25rem"):
                  sizeMB & " MB"

        # LevelDB stats table
        let statsText = $safeStr(gStorage.get(), "stats")
        if statsText.len > 0:
          tDiv(style = cardStyle(dark)):
            tStrong(style = "color:" & (if dark: DarkText else: "#111")):
              "LevelDB Compaction Stats"
            tPre(style = "margin-top:.75rem;font-size:.82rem;font-family:'SF Mono','Fira Mono',monospace;color:" &
                (if dark: DarkText else: "#222") &
                ";overflow-x:auto;white-space:pre;line-height:1.5"):
              statsText
        else:
          tDiv(style = "color:" & (if dark: DarkTextMuted else: "#888") &
              ";font-size:.85rem;padding:1rem"):
            "Waiting for storage data..."

      AppFooter()
      ToastContainer()
      GlobalModal()
