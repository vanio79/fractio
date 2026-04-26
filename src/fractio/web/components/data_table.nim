# Fractio Web Dashboard - Data Table Component
#
# A reusable, sortable, paginated data table component with export functionality.

import happyx
import std/[jsffi, sequtils]
import ../styles
import ../store
import ../api
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
    let cols = self.columns.get()
    let rows = self.rows.get()
    let colLen = jsArrayLen(cols)
    let rowLen = jsArrayLen(rows)
    let tblName = self.table.get()
    let p = gTablePagination.get()
    let sortCol = self.sortColumn.get()
    let sortAscVal = self.sortAsc.get()
    let maxH = self.maxHeight.get()
    let sortableVal = self.sortable.get()
    let searchableVal = self.searchable.get()
    let exportableVal = self.exportable.get()
    let paginatedVal = self.paginated.get()

    # Table wrapper
    tDiv(style = cardStyle(dark) & ";overflow:hidden"):
      # Toolbar
      if searchableVal or exportableVal:
        tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:.75rem;flex-wrap:wrap;justify-content:space-between"):
          # Export buttons
          if exportableVal and rowLen > 0:
            let jsonFile = tblName & ".json"
            let csvFile = tblName & ".csv"
            tSlButtonGroup:
              tSlButton(
                size = "small",
                @click = proc() = discard downloadJsonExport(rows, jsonFile)
              ):
                "Export JSON"
              tSlButton(
                size = "small",
                @click = proc() = discard downloadCsvExport(cols, rows, csvFile)
              ):
                "Export CSV"

      # Row count
      let rowsText = $rowLen & " rows"
      tDiv(style = "font-size:.82rem;color:" & (if dark: "#888" else: "#666") &
          ";margin-bottom:.5rem"):
        {rowsText}

      # Table container with scroll
      tDiv(style = "overflow-x:auto;overflow-y:auto;max-height:" & maxH):
        tTable(style = "width:100%;border-collapse:collapse;font-size:.875rem;background:" &
            (if dark: DarkCardBg else: LightCardBg)):
          tThead:
            tTr:
              for ci in 0 ..< colLen:
                let colName = $jsArrayGet(cols, ci)
                let isActive = sortCol == colName
                let sortIcon = if isActive: (
                  if sortAscVal: "↑" else: "↓") else: ""
                let headerText = colName & " " & sortIcon
                let cursorStyle = if sortableVal: "pointer" else: "default"
                tTh(
                  style = tableHeaderStyle(dark) & ";cursor:" & cursorStyle,
                  @click = proc() =
                  if sortableVal:
                    if sortCol == colName:
                      self.sortAsc.set(not sortAscVal)
                    else:
                      self.sortColumn.set(colName)
                      self.sortAsc.set(true)
                ):
                  {headerText}
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
                let row = rows[ri]
                tTr(style = if dark: "" else: "", @mouseover = proc() = discard,
                    @mouseout = proc() = discard):
                  for ci in 0 ..< colLen:
                    let colKey = jsArrayGet(cols, ci)
                    let cellVal = $jsObjField(row, colKey)
                    tTd(style = tableCellStyle(dark)):
                      cellVal

      # Pagination
      if paginatedVal:
        tDiv(style = "margin-top:.75rem"):
          PaginationSimple()

component DataTableSimple:
  columns: JsObject
  rows: JsObject

  html:
    let dark = gDarkMode.get()
    let cols = self.columns.get()
    let rows = self.rows.get()
    let colLen = jsArrayLen(cols)
    let rowLen = jsArrayLen(rows)

    tDiv(style = "overflow-x:auto"):
      tTable(style = "width:100%;border-collapse:collapse;font-size:.875rem;background:" &
          (if dark: DarkCardBg else: LightCardBg) & ";border:1px solid " & (
          if dark: DarkBorder else: LightBorder) & ";border-radius:6px"):
        tThead:
          tTr:
            for ci in 0 ..< colLen:
              let colName = $jsArrayGet(cols, ci)
              tTh(style = tableHeaderStyle(dark)):
                colName
        tTbody:
          for ri in 0 ..< rowLen:
            let row = rows[ri]
            tTr:
              for ci in 0 ..< colLen:
                let colKey = jsArrayGet(cols, ci)
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
    let tblName = self.tableName.get()

    if colLen == 0 and loadedSysTableDataKey != tblName:
      tDiv(style = "color:#888;font-size:.85rem;padding:1rem"):
        tSlSpinner(style = "font-size:1rem;margin-right:.5rem")
        "Loading system table data..."
    else:
      # Toolbar with export
      let rowsText = $rowLen & " rows"
      let jsonFile = tblName & ".json"
      let csvFile = tblName & ".csv"
      tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:.75rem;flex-wrap:wrap;justify-content:space-between"):
        tDiv(style = "font-size:.82rem;color:" & (
            if dark: "#888" else: "#666")):
          {rowsText}

        if rowLen > 0:
          tSlButtonGroup:
            tSlButton(
              size = "small",
              @click = downloadJsonExport(sysRows, jsonFile)
            ):
              "Export JSON"
            tSlButton(
              size = "small",
              @click = downloadCsvExport(sysCols, sysRows, csvFile)
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
