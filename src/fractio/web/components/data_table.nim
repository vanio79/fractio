# Fractio Web Dashboard - Data Table Component
#
# A reusable, sortable, paginated data table component with export functionality.

import happyx
import std/[jsffi, sequtils]
import ../styles
import ../store
import ../js_interop
import ./pagination
import ./search_box
import ./loading_spinner

type ColumnDef* = object
  name*: string
  key*: cstring
  sortable*: bool
  align*: string # "left", "center", "right"

component DataTable:
  columns: JsObject # Array of column names
  rows: JsObject # Array of row objects
  sortable: bool = true
  paginated: bool = true
  searchable: bool = true
  exportable: bool = true
  pageSize: int = 50
  maxHeight: string = "500px"
  db: string = ""
  schema: string = ""
  table: string = ""

  sortColumn: string = ""
  sortAsc: bool = true

  html:
    let dark = gDarkMode.get()
    let colLen = jsArrayLen(self.columns)
    let rowLen = jsArrayLen(self.rows)
    let p = gTablePagination.get()

    # Table wrapper
    tDiv(style = cardStyle(dark) & ";overflow:hidden"):
      # Toolbar
      if self.searchable or self.exportable:
        tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:.75rem;flex-wrap:wrap;justify-content:space-between"):
          # Search
          if self.searchable:
            tSlInput(
              placeholder = "Search...",
              size = "small",
              clearable = true,
              style = "width:200px",
              @sl-input = proc(ev: JsObject) =
              setSearchQuery($safeStr(ev, "value"))
            )

          # Export buttons
          if self.exportable and rowLen > 0:
            tSlButtonGroup:
              tSlButton(
                size = "small",
                @click = downloadJsonExport(self.rows, self.table & ".json")
              ):
                "Export JSON"
              tSlButton(
                size = "small",
                @click = downloadCsvExport(self.columns, self.rows, self.table & ".csv")
              ):
                "Export CSV"

      # Row count
      tDiv(style = "font-size:.82rem;color:" & (if dark: "#888" else: "#666") &
          ";margin-bottom:.5rem"):
        $rowLen & " rows"

      # Table container with scroll
      tDiv(style = "overflow-x:auto;overflow-y:auto;max-height:" &
          self.maxHeight):
        tTable(style = "width:100%;border-collapse:collapse;font-size:.875rem;background:" &
            (if dark: DarkCardBg else: LightCardBg)):
          tThead:
            tTr:
              for ci in 0 ..< colLen:
                let colName = $jsArrayGet(self.columns, ci)
                let isActive = self.sortColumn == colName
                let sortIcon = if isActive: (
                  if self.sortAsc: "↑" else: "↓") else: ""
                tTh(
                  style = tableHeaderStyle(dark) & ";cursor:" & (
                      if self.sortable: "pointer" else: "default"),
                  @click = proc() =
                  if self.sortable:
                    if self.sortColumn == colName:
                      self.sortAsc = not self.sortAsc
                    else:
                      self.sortColumn = colName
                      self.sortAsc = true
                ):
                  colName & " " & sortIcon
          tTbody:
            if rowLen == 0:
              tTr:
                tTd(
                  colspan = $colLen,
                  style = tableCellStyle(dark) & ";text-align:center;color:#888;padding:2rem"
                ):
                  "No data"
            else:
              for ri in 0 ..< rowLen:
                let row = self.rows[ri]
                tTr(style = if dark: "" else: "", @mouseover = proc() = discard,
                    @mouseout = proc() = discard):
                  for ci in 0 ..< colLen:
                    let colKey = jsArrayGet(self.columns, ci)
                    let cellVal = $jsObjField(row, colKey)
                    tTd(style = tableCellStyle(dark)):
                      cellVal

      # Pagination
      if self.paginated:
        tDiv(style = "margin-top:.75rem"):
          PaginationSimple()

component DataTableSimple:
  columns: JsObject
  rows: JsObject

  html:
    let dark = gDarkMode.get()
    let colLen = jsArrayLen(self.columns)
    let rowLen = jsArrayLen(self.rows)

    tDiv(style = "overflow-x:auto"):
      tTable(style = "width:100%;border-collapse:collapse;font-size:.875rem;background:" &
          (if dark: DarkCardBg else: LightCardBg) & ";border:1px solid " & (
          if dark: DarkBorder else: LightBorder) & ";border-radius:6px"):
        tThead:
          tTr:
            for ci in 0 ..< colLen:
              let colName = $jsArrayGet(self.columns, ci)
              tTh(style = tableHeaderStyle(dark)):
                colName
        tTbody:
          for ri in 0 ..< rowLen:
            let row = self.rows[ri]
            tTr:
              for ci in 0 ..< colLen:
                let colKey = jsArrayGet(self.columns, ci)
                let cellVal = $jsObjField(row, colKey)
                tTd(style = tableCellStyle(dark)):
                  cellVal

component SystemTableViewer:
  tableId: int
  tableName: string

  html:
    let dark = gDarkMode.get()
    let std = gSysTableData.get()
    let sysCols = std.columns
    let sysRows = std.rows
    let colLen = jsArrayLen(sysCols)
    let rowLen = jsArrayLen(sysRows)

    if colLen == 0 and loadedSysTableDataKey != self.tableName:
      tDiv(style = "color:#888;font-size:.85rem;padding:1rem"):
        tSlSpinner(style = "font-size:1rem;margin-right:.5rem")
        "Loading system table data..."
    else:
      # Toolbar with export
      tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:.75rem;flex-wrap:wrap;justify-content:space-between"):
        tDiv(style = "font-size:.82rem;color:" & (
            if dark: "#888" else: "#666")):
          $rowLen & " rows"

        if rowLen > 0:
          tSlButtonGroup:
            tSlButton(
              size = "small",
              @click = downloadJsonExport(sysRows, self.tableName & ".json")
            ):
              "Export JSON"
            tSlButton(
              size = "small",
              @click = downloadCsvExport(sysCols, sysRows, self.tableName & ".csv")
            ):
              "Export CSV"

      tDiv(style = "overflow-x:auto"):
        tTable(style = "width:100%;border-collapse:collapse;font-size:.875rem;background:" &
            (if dark: DarkCardBg else: LightCardBg)):
          tThead:
            tTr:
              for ci in 0 ..< colLen:
                let colName = $jsArrayGet(sysCols, ci)
                tTh(style = tableHeaderStyle(dark)):
                  colName
          tTbody:
            for ri in 0 ..< rowLen:
              let row = sysRows[ri]
              tTr:
                for ci in 0 ..< colLen:
                  let colKey = jsArrayGet(sysCols, ci)
                  let cellVal = $jsObjField(row, colKey)
                  tTd(style = tableCellStyle(dark)):
                    cellVal
