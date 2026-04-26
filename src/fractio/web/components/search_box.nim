# Fractio Web Dashboard - Search Box Component
#
# Placeholder search box - search functionality to be added later.

import happyx
import std/jsffi
import ../store
import ../styles
import ../js_interop

# =============================================================================
# Simple Search placeholder
# =============================================================================

component TableSearch:
  db: string = ""
  schema: string = ""
  table: string = ""

  html:
    let p = gTablePagination.get()
    let dark = gDarkMode.get()
    let rowsText = $p.totalRows & " rows"

    tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:1rem;flex-wrap:wrap"):
      # Row count
      tSpan(style = "font-size:.85rem;color:" & (if dark: "#888" else: "#666")):
        {rowsText}
