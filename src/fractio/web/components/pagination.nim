# Fractio Web Dashboard - Pagination Component
#
# HTML-based pagination controls for data tables.

import happyx
import std/jsffi
import ../store
import ../styles
import ../js_interop

# =============================================================================
# Style helpers
# =============================================================================

proc btnNorm(dark: bool): string =
  "padding:.25rem .5rem;border-radius:4px;cursor:pointer;background:" &
  (if dark: DarkCardBg else: "#f5f5f5") & ";border:1px solid " &
  (if dark: DarkBorder else: "#ddd") & ";color:" & (
      if dark: DarkText else: "#333")

proc btnAct(): string =
  "padding:.25rem .5rem;border-radius:4px;font-weight:600;background:" &
  PrimaryColor & ";color:white;border:none"

proc btnDis(dark: bool): string =
  "padding:.25rem .5rem;border-radius:4px;opacity:.5;cursor:not-allowed;background:" &
  (if dark: DarkCardBg else: "#f5f5f5") & ";border:1px solid " &
  (if dark: DarkBorder else: "#ddd") & ";color:" & (
      if dark: "#888" else: "#aaa")

# =============================================================================
# Simple Pagination (uses global state)
# =============================================================================

component PaginationSimple:
  html:
    let p = gTablePagination.get()
    let pages = p.totalPages()
    let dark = gDarkMode.get()
    let prevDis = p.page <= 1
    let nextDis = p.page >= pages
    let rowsText = $p.totalRows & " rows"

    tDiv(style = "display:flex;align-items:center;gap:.5rem;margin-bottom:1rem"):
      # Previous button
      if prevDis:
        tButton(style = btnDis(dark)):
          "←"
      else:
        tButton(style = btnNorm(dark)):
          "←"
          @click:
            discard prevPage()

      # Page 1
      if p.page == 1:
        tButton(style = btnAct()):
          "1"
      else:
        tButton(style = btnNorm(dark)):
          "1"
          @click:
            discard setPage(1)

      # Page 2 (if exists)
      if pages >= 2:
        if p.page == 2:
          tButton(style = btnAct()):
            "2"
        else:
          tButton(style = btnNorm(dark)):
            "2"
            @click:
              discard setPage(2)

      # Page 3 (if exists)
      if pages >= 3:
        if p.page == 3:
          tButton(style = btnAct()):
            "3"
        else:
          tButton(style = btnNorm(dark)):
            "3"
            @click:
              discard setPage(3)

      # Page 4 (if exists)
      if pages >= 4:
        if p.page == 4:
          tButton(style = btnAct()):
            "4"
        else:
          tButton(style = btnNorm(dark)):
            "4"
            @click:
              discard setPage(4)

      # Page 5 (if exists)
      if pages >= 5:
        if p.page == 5:
          tButton(style = btnAct()):
            "5"
        else:
          tButton(style = btnNorm(dark)):
            "5"
            @click:
              discard setPage(5)

      # Ellipsis and last page if more than 5 pages
      if pages > 5:
        tSpan(style = "color:" & (if dark: "#888" else: "#666")):
          "... "
        let lastPageStr = $pages
        if p.page == pages:
          tButton(style = btnAct()):
            {lastPageStr}
        else:
          tButton(style = btnNorm(dark)):
            {lastPageStr}
            @click:
              discard setPage(pages)

      # Next button
      if nextDis:
        tButton(style = btnDis(dark)):
          "→"
      else:
        tButton(style = btnNorm(dark)):
          "→"
          @click:
            discard nextPage()

      tSpan(style = "font-size:.82rem;color:" & (if dark: "#888" else: "#666") &
          ";margin-left:.5rem"):
        {rowsText}
