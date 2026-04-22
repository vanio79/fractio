# Fractio Web Dashboard - SQL Route
#
# SQL query editor with Monaco integration.

import happyx
import std/jsffi
import ../styles
import ../store
import ../js_interop
import ../api
import ../components/[header, footer, toast, modal, sql_editor]

mount "/sql" -> SqlRoute:
  "/":
    let dark = gDarkMode.get()
    let db = gCurrentDatabase.get()
    let sc = gCurrentSchema.get()

    tDiv(style = shellStyle(dark)):
      AppHeader()
      tNav(style = navBarStyle(dark)):
        for (href, label) in navItems:
          let isActive = label == "SQL"
          tA(href = href, style = navStyle(isActive, dark)):
            label
      tMain(style = mainStyle(dark)):
        # Title
        tH2(style = "font-size:1.05rem;font-weight:700;color:" & (
            if dark: DarkText else: "#111") & ";margin:0;margin-bottom:1rem"):
          "SQL Query Editor"

        # SQL editor
        SqlEditor(
          database = db,
          schema = sc,
          onExecute = proc(sql: string) =
          discard executeSql(sql, db, sc)
        )

        # Results section
        tH3(style = "font-size:.95rem;font-weight:600;color:" & (
            if dark: DarkText else: "#111") & ";margin:1rem 0 .5rem"):
          "Query Results"

        SqlResultViewer()

      AppFooter()
      ToastContainer()
      GlobalModal()
