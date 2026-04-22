# Fractio Web Dashboard - Pagination Component
#
# Shoelace-based pagination controls for data tables.

import happyx
import std/jsffi
import ../store
import ../styles
import ../js_interop

component PaginationControls:
  currentPage: int
  totalPages: int
  totalRows: int
  pageSize: int
  onPageChange: proc(page: int) = nil
  onPageSizeChange: proc(size: int) = nil
  
  html:
    let dark = gDarkMode.get()
    tDiv(style = "display:flex;align-items:center;gap:.75rem;flex-wrap:wrap;margin-bottom:1rem"):
      # Page size selector
      tSlSelect(
        label = "Rows per page",
        size = "small",
        style = "width:120px",
        @sl-change = proc(ev: JsObject) =
          let val = safeInt(ev, "value")
          if self.onPageSizeChange != nil:
            self.onPageSizeChange(val)
      ):
        tSlOption(value = "25"): "25"
        tSlOption(value = "50", selected = self.pageSize == 50): "50"
        tSlOption(value = "100"): "100"
        tSlOption(value = "200"): "200"
      
      # Page navigation
      tSlButtonGroup:
        tSlIconButton(
          name = "chevron-left",
          label = "Previous",
          disabled = self.currentPage <= 1,
          @click = proc() =
            if self.onPageChange != nil and self.currentPage > 1:
              self.onPageChange(self.currentPage - 1)
        )
        tSlButton(
          style = "font-size:.85rem"
        ):
          "Page " & $self.currentPage & " of " & $self.totalPages
        tSlIconButton(
          name = "chevron-right",
          label = "Next",
          disabled = self.currentPage >= self.totalPages,
          @click = proc() =
            if self.onPageChange != nil and self.currentPage < self.totalPages:
              self.onPageChange(self.currentPage + 1)
        )
      
      # Row count info
      tSpan(style = "font-size:.82rem;color:" & (if dark: "#888" else: "#666")):
        $self.totalRows & " total rows"

component PaginationSimple:
  html:
    let p = gTablePagination.get()
    let pages = p.totalPages()
    
    tDiv(style = "display:flex;align-items:center;gap:.5rem;margin-bottom:1rem"):
      tSlButtonGroup:
        tSlIconButton(
          name = "chevron-left",
          label = "Previous",
          disabled = p.page <= 1
        ):
          @click:
            discard prevPage()
        for i in 1 .. min(pages, 5):
          let isCurrent = i == p.page
          tSlButton(
            variant = if isCurrent: "primary" else: "default",
            size = "small"
          ):
            $i
            @click:
              discard setPage(i)
        if pages > 5:
          tSlButton(size = "small"): "..."
          tSlButton(
            variant = if p.page == pages: "primary" else: "default",
            size = "small"
          ):
            $pages
            @click:
              discard setPage(pages)
        tSlIconButton(
          name = "chevron-right",
          label = "Next",
          disabled = p.page >= pages
        ):
          @click:
            discard nextPage()
      
      tSpan(style = "font-size:.82rem;color:#666"):
        $p.totalRows & " rows"