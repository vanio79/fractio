# Fractio Web Dashboard - Data Browser Route
#
# Hierarchical data browser for databases, schemas, tables, and rows.

import happyx
import std/jsffi
import ../styles
import ../store
import ../js_interop
import ../api
import ../components/[header, footer, toast, modal, breadcrumb, data_table, stat_card]

mount "/data" -> DataRoute:
  # Database list
  "/":
    let dark = gDarkMode.get()
    discard triggerLoadDatabases()

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tNav(style = navBarStyle(dark)):
        for (href, label) in navItems:
          let isActive = label == "Data"
          tA(href = href, style = navStyle(isActive, dark)):
            label
      tMain(style = mainStyle(dark)):
        # Breadcrumb
        tDiv(style = "display:flex;align-items:center;gap:.35rem;margin-bottom:1.25rem;font-size:.85rem;color:" &
            (if dark: DarkTextMuted else: "#666")):
          tSpan(style = "font-weight:600;color:" & (
              if dark: DarkText else: "#111")):
            "Databases"

        # Grid of databases
        tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:.75rem"):
          # Virtual "sys" database
          tA(href = "/#/data/sys", style = "text-decoration:none;color:inherit"):
            tDiv(style = "background:" & (
                if dark: DarkCardBg else: LightCardBg) & ";border:1px solid " &
                (if dark: DarkBorder else: LightBorder) &
                ";border-radius:6px;padding:.85rem 1rem;transition:border-color .15s,box-shadow .15s;border-left:3px solid " & PrimaryColor):
              tDiv(style = "font-size:.65rem;color:" & (
                  if dark: DarkTextMuted else: "#999") &
                  ";text-transform:uppercase;letter-spacing:.07em;margin-bottom:.25rem;font-weight:600"):
                "SYSTEM DATABASE"
              tDiv(style = "font-size:.95rem;font-weight:600;color:" & (
                  if dark: DarkText else: "#111")):
                "sys"
              tDiv(style = "font-size:.75rem;color:" & (
                  if dark: DarkTextMuted else: "#888")):
                "System tables (nodes, groups, settings, ...)"

          # User databases
          let dbs = gDatabases.get()
          for d in dbs:
            tA(href = "/#/data/" & d, style = "text-decoration:none;color:inherit"):
              tDiv(style = "background:" & (
                  if dark: DarkCardBg else: LightCardBg) &
                  ";border:1px solid " & (
                  if dark: DarkBorder else: LightBorder) &
                  ";border-radius:6px;padding:.85rem 1rem;transition:border-color .15s,box-shadow .15s"):
                tDiv(style = "font-size:.65rem;color:" & (
                    if dark: DarkTextMuted else: "#999") &
                    ";text-transform:uppercase;letter-spacing:.07em;margin-bottom:.25rem;font-weight:600"):
                  "DATABASE"
                tDiv(style = "font-size:.95rem;font-weight:600;color:" & (
                    if dark: DarkText else: "#111")):
                  d

      AppFooter()
      ToastContainer()
      GlobalModal()

  # Schema list
  "/data/{db}":
    let dark = gDarkMode.get()
    if db != "sys":
      discard triggerLoadSchemas(db)

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tNav(style = navBarStyle(dark)):
        for (href, label) in navItems:
          let isActive = label == "Data"
          tA(href = href, style = navStyle(isActive, dark)):
            label
      tMain(style = mainStyle(dark)):
        # Breadcrumb
        tDiv(style = "display:flex;align-items:center;gap:.35rem;margin-bottom:1.25rem;font-size:.85rem;color:" &
            (if dark: DarkTextMuted else: "#666")):
          tA(href = "/#/data", style = "color:#e81c1c;font-weight:600;text-decoration:none"):
            "Databases"
          tSpan: " / "
          tSpan(style = "font-weight:600;color:" & (
              if dark: DarkText else: "#111")):
            db

        # Schema grid
        tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:.75rem"):
          if db == "sys":
            # System database has a default schema
            tA(href = "/#/data/sys/default",
                style = "text-decoration:none;color:inherit"):
              tDiv(style = "background:" & (
                  if dark: DarkCardBg else: LightCardBg) &
                  ";border:1px solid " & (
                  if dark: DarkBorder else: LightBorder) &
                  ";border-radius:6px;padding:.85rem 1rem;transition:border-color .15s,box-shadow .15s"):
                tDiv(style = "font-size:.65rem;color:" & (
                    if dark: DarkTextMuted else: "#999") &
                    ";text-transform:uppercase;letter-spacing:.07em;margin-bottom:.25rem;font-weight:600"):
                  "SCHEMA"
                tDiv(style = "font-size:.95rem;font-weight:600;color:" & (
                    if dark: DarkText else: "#111")):
                  "default"
          else:
            # User schemas
            let schemas = gSchemas.get()
            if schemas.len == 0 and loadedSchemasKey != db:
              tDiv(style = "color:" & (if dark: DarkTextMuted else: "#888") &
                  ";font-size:.85rem;padding:1rem"):
                "Loading schemas..."
            elif schemas.len == 0:
              tDiv(style = "color:" & (if dark: DarkTextMuted else: "#888") &
                  ";font-size:.85rem;padding:1rem"):
                "No schemas found."
            else:
              for s in schemas:
                tA(href = "/#/data/" & db & "/" & s,
                    style = "text-decoration:none;color:inherit"):
                  tDiv(style = "background:" & (
                      if dark: DarkCardBg else: LightCardBg) &
                      ";border:1px solid " & (
                      if dark: DarkBorder else: LightBorder) &
                      ";border-radius:6px;padding:.85rem 1rem;transition:border-color .15s,box-shadow .15s"):
                    tDiv(style = "font-size:.65rem;color:" & (
                        if dark: DarkTextMuted else: "#999") &
                        ";text-transform:uppercase;letter-spacing:.07em;margin-bottom:.25rem;font-weight:600"):
                      "SCHEMA"
                    tDiv(style = "font-size:.95rem;font-weight:600;color:" & (
                        if dark: DarkText else: "#111")):
                      s

      AppFooter()
      ToastContainer()
      GlobalModal()

  # Table list
  "/data/{db}/{schema}":
    let dark = gDarkMode.get()
    if db == "sys":
      discard triggerLoadSystemTables()
    else:
      discard triggerLoadTables(db, schema)

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tNav(style = navBarStyle(dark)):
        for (href, label) in navItems:
          let isActive = label == "Data"
          tA(href = href, style = navStyle(isActive, dark)):
            label
      tMain(style = mainStyle(dark)):
        # Breadcrumb
        tDiv(style = "display:flex;align-items:center;gap:.35rem;margin-bottom:1.25rem;font-size:.85rem;color:" &
            (if dark: DarkTextMuted else: "#666")):
          tA(href = "/#/data", style = "color:#e81c1c;font-weight:600;text-decoration:none"):
            "Databases"
          tSpan: " / "
          tA(href = "/#/data/" & db, style = "color:#e81c1c;font-weight:600;text-decoration:none"):
            db
          tSpan: " / "
          tSpan(style = "font-weight:600;color:" & (
              if dark: DarkText else: "#111")):
            schema

        # Table grid
        tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:.75rem"):
          if db == "sys":
            # System tables
            let stArr = gSysTables.get()
            let stLen = jsArrayLen(stArr)
            if stLen > 0:
              for si in 0 ..< stLen:
                let st = stArr[si]
                let stName = $safeStr(st, "name")
                let stDesc = $safeStr(st, "description")
                let stId = safeInt(st, "id")
                let stRows = $safeIntStr(st, "rowCount")
                tA(href = "/#/data/sys/default/" & stName,
                    style = "text-decoration:none;color:inherit"):
                  tDiv(style = "background:" & (
                      if dark: DarkCardBg else: LightCardBg) &
                      ";border:1px solid " & (
                      if dark: DarkBorder else: LightBorder) &
                      ";border-left:3px solid " & PrimaryColor &
                      ";border-radius:6px;padding:.85rem 1rem;transition:border-color .15s,box-shadow .15s"):
                    tDiv(style = "font-size:.65rem;color:" & (
                        if dark: DarkTextMuted else: "#999") &
                        ";text-transform:uppercase;letter-spacing:.07em;margin-bottom:.25rem;font-weight:600"):
                      "SYSTEM TABLE · ID " & $stId
                    tDiv(style = "font-size:.95rem;font-weight:600;color:" & (
                        if dark: DarkText else: "#111")):
                      stName
                    tDiv(style = "font-size:.75rem;color:" & (
                        if dark: DarkTextMuted else: "#888")):
                      stDesc & " · " & stRows & " rows"
            elif loadedSysTables:
              tDiv(style = "color:" & (if dark: DarkTextMuted else: "#888") &
                  ";font-size:.85rem;padding:.5rem"):
                "No system tables found."
            else:
              tDiv(style = "color:" & (if dark: DarkTextMuted else: "#888") &
                  ";font-size:.85rem;padding:.5rem"):
                "Loading system tables..."
          else:
            # User tables
            let tables = gTables.get()
            let tablesKey = db & "." & schema
            if tables.len == 0 and loadedTablesKey != tablesKey:
              tDiv(style = "color:" & (if dark: DarkTextMuted else: "#888") &
                  ";font-size:.85rem;padding:1rem"):
                "Loading tables..."
            elif tables.len == 0:
              tDiv(style = "color:" & (if dark: DarkTextMuted else: "#888") &
                  ";font-size:.85rem;padding:1rem"):
                "No tables found."
            else:
              for t in tables:
                tA(href = "/#/data/" & db & "/" & schema & "/" & t,
                    style = "text-decoration:none;color:inherit"):
                  tDiv(style = "background:" & (
                      if dark: DarkCardBg else: LightCardBg) &
                      ";border:1px solid " & (
                      if dark: DarkBorder else: LightBorder) &
                      ";border-radius:6px;padding:.85rem 1rem;transition:border-color .15s,box-shadow .15s"):
                    tDiv(style = "font-size:.65rem;color:" & (
                        if dark: DarkTextMuted else: "#999") &
                        ";text-transform:uppercase;letter-spacing:.07em;margin-bottom:.25rem;font-weight:600"):
                      "TABLE"
                    tDiv(style = "font-size:.95rem;font-weight:600;color:" & (
                        if dark: DarkText else: "#111")):
                      t

      AppFooter()
      ToastContainer()
      GlobalModal()

  # Table rows view
  "/data/{db}/{schema}/{table}":
    let dark = gDarkMode.get()
    if db == "sys":
      let stId = sysTableIdByName(table)
      if stId >= 0:
        discard triggerLoadSystemTableData(stId, table)
      else:
        discard triggerLoadSystemTables()
    else:
      discard triggerLoadTableData(db, schema, table)

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tNav(style = navBarStyle(dark)):
        for (href, label) in navItems:
          let isActive = label == "Data"
          tA(href = href, style = navStyle(isActive, dark)):
            label
      tMain(style = mainStyle(dark)):
        # Breadcrumb
        tDiv(style = "display:flex;align-items:center;gap:.35rem;margin-bottom:1.25rem;font-size:.85rem;color:" &
            (if dark: DarkTextMuted else: "#666")):
          tA(href = "/#/data", style = "color:#e81c1c;font-weight:600;text-decoration:none"):
            "Databases"
          tSpan: " / "
          tA(href = "/#/data/" & db, style = "color:#e81c1c;font-weight:600;text-decoration:none"):
            db
          tSpan: " / "
          tA(href = "/#/data/" & db & "/" & schema,
              style = "color:#e81c1c;font-weight:600;text-decoration:none"):
            schema
          tSpan: " / "
          tSpan(style = "font-weight:600;color:" & (
              if dark: DarkText else: "#111")):
            table

        # Table header
        tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:1rem"):
          tH2(style = "font-size:1.05rem;font-weight:700;color:" & (
              if dark: DarkText else: "#111") & ";margin:0"):
            table

        # Table data
        if db == "sys":
          # System table
          let stId = sysTableIdByName(table)
          if stId < 0:
            tDiv(style = "color:" & (if dark: DarkTextMuted else: "#888") &
                ";font-size:.85rem"):
              tSlSpinner(style = "font-size:1rem;margin-right:.5rem")
              "Loading..."
          else:
            SystemTableViewer(tableId = stId, tableName = table)
        else:
          # User table
          let td = gTableData.get()
          let tdKind = $safeStr(td, "kind")
          if tdKind.len == 0:
            tDiv(style = "color:" & (if dark: DarkTextMuted else: "#888") &
                ";font-size:.85rem"):
              tSlSpinner(style = "font-size:1rem;margin-right:.5rem")
              "Loading table data..."
          elif tdKind == "rows":
            let cols = td.columns
            let rows = td.rows
            DataTable(
              columns = cols,
              rows = rows,
              sortable = true,
              paginated = true,
              searchable = true,
              exportable = true,
              db = db,
              schema = schema,
              table = table
            )
          elif tdKind == "error":
            tDiv(style = "color:" & DangerColor &
                ";font-size:.85rem;padding:.5rem"):
              $safeStr(td, "error")
          else:
            tDiv(style = "color:" & (if dark: DarkTextMuted else: "#888") &
                ";font-size:.85rem"):
              "Loading table data..."

      AppFooter()
      ToastContainer()
      GlobalModal()
