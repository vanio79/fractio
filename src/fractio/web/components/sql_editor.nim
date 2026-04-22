# Fractio Web Dashboard - SQL Editor Component
#
# Monaco-based SQL query editor with history and saved queries.

import happyx
import std/[jsffi, sequtils]
import ../styles
import ../store
import ../js_interop
import ./data_table
import ./loading_spinner

# Monaco Editor integration
proc initMonacoEditor*(id: cstring, value: cstring) =
  {.emit: """
  if (window.monaco && window.monaco.editor) {
    var editor = monaco.editor.create(document.getElementById(id), {
      value: value,
      language: 'sql',
      theme: 'vs-dark',
      automaticLayout: true,
      minimap: { enabled: false },
      fontSize: 14,
      lineNumbers: 'on',
      scrollBeyondLastLine: false,
      wordWrap: 'on',
      folding: true,
      tabSize: 2
    });
    `gMonacoEditor` = editor;
  }
  """.}

var gMonacoEditor: JsObject = nil

proc getMonacoValue(): cstring =
  {.emit: """
  if (`gMonacoEditor`) {
    `result` = `gMonacoEditor`.getValue();
  } else {
    `result` = '';
  }
  """.}
  result

proc setMonacoValue*(value: cstring) =
  {.emit: """
  if (`gMonacoEditor`) {
    `gMonacoEditor`.setValue(value);
  }
  """.}

proc loadMonacoLibrary*() =
  {.emit: """
  if (!window.monacoLoaded) {
    window.monacoLoaded = true;
    var script = document.createElement('script');
    script.src = 'https://cdn.jsdelivr.net/npm/monaco-editor@0.45.0/min/vs/loader.js';
    script.onload = function() {
      require.config({ paths: { 'vs': 'https://cdn.jsdelivr.net/npm/monaco-editor@0.45.0/min/vs' }});
      require(['vs/editor/editor.main'], function() {
        window.monaco = monaco;
        // Dispatch event when loaded
        window.dispatchEvent(new Event('monaco-loaded'));
      });
    };
    document.head.appendChild(script);
  }
  """.}
  result

component SqlEditor:
  initialQuery: string = ""
  database: string = "default"
  schema: string = "public"
  onExecute: proc(sql: string) = nil
  
  editorId: string = "monaco-editor"
  monacoLoaded: bool = false
  
  created:
    loadMonacoLibrary()
    # Listen for monaco load event
    {.emit: """
    window.addEventListener('monaco-loaded', function() {
      `self`.`monacoLoaded` = true;
      initMonacoEditor('monaco-editor', `self`.`initialQuery` || 'SELECT * FROM sys.nodes LIMIT 10;');
    });
    """.}
  
  html:
    let dark = gDarkMode.get()
    let loadingSql = isLoading("sqlQuery")
    let hist = gSqlHistory.get()
    
    tDiv(style = cardStyle(dark) & ";padding:1rem;margin-bottom:1rem"):
      # Toolbar
      tDiv(style = "display:flex;align-items:center;gap:.75rem;flex-wrap:wrap;margin-bottom:.75rem"):
        # Database/Schema selectors
        tSlSelect(
          label = "Database",
          size = "small",
          value = self.database,
          style = "width:140px",
          @sl-change = proc(ev: JsObject) =
            gCurrentDatabase.set($safeStr(ev, "value"))
        ):
          tSlOption(value = "default"): "default"
          tSlOption(value = "sys"): "sys"
        
        tSlSelect(
          label = "Schema",
          size = "small",
          value = self.schema,
          style = "width:140px",
          @sl-change = proc(ev: JsObject) =
            gCurrentSchema.set($safeStr(ev, "value"))
        ):
          tSlOption(value = "public"): "public"
          tSlOption(value = "default"): "default"
        
        # Execute button
        tSlButton(
          variant = "primary",
          size = "medium",
          loading = loadingSql,
          style = "margin-left:.5rem",
          @click = proc() =
            let sql = getMonacoValue()
            if sql.len > 0 and self.onExecute != nil:
              self.onExecute($sql)
        ):
          "Execute"
        
        # Clear button
        tSlButton(
          variant = "default",
          size = "medium",
          @click = proc() =
            setMonacoValue("")
        ):
          "Clear"
      
      # Editor container
      tDiv(
        id = self.editorId,
        style = "height:300px;border:1px solid " & (if dark: DarkBorder else: LightBorder) & ";border-radius:4px;overflow:hidden"
      ):
        if not self.monacoLoaded:
          tDiv(style = "height:100%;display:flex;justify-content:center;align-items:center;color:#888"):
            tSlSpinner(style = "font-size:2rem;margin-right:.5rem")
            "Loading Monaco Editor..."
      
      # Query history
      if hist.len > 0:
        tDiv(style = "margin-top:.75rem"):
          tDiv(style = "font-size:.75rem;color:" & (if dark: DarkTextMuted else: "#666") & ";margin-bottom:.25rem;font-weight:600"):
            "Recent Queries"
          tSlDropdown(style = "width:100%"):
            tSlButton(slot = "trigger", caret = true, size = "small"):
              "Load from history..."
            tSlMenu:
              for i in countdown(hist.len - 1, max(0, hist.len - 10)):
                let q = hist[i]
                let shortQ = if q.len > 50: q[0..50] & "..." else: q
                tSlMenuItem(
                  @click = proc() =
                    setMonacoValue(cstring(q))
                ):
                  shortQ

component SqlResultViewer:
  html:
    let dark = gDarkMode.get()
    let result = gSqlResult.get()
    let kind = $safeStr(result, "kind")
    
    if kind.len == 0:
      tDiv(style = "color:" & (if dark: DarkTextMuted else: "#888") & ";font-size:.85rem;padding:1rem;text-align:center"):
        "Execute a query to see results"
    elif kind == "rows":
      let cols = result.columns
      let rows = result.rows
      DataTableSimple(columns = cols, rows = rows)
    elif kind == "modified":
      tDiv(style = cardStyle(dark)):
        tDiv(style = "display:flex;align-items:center;gap:.5rem"):
          tSlIcon(name = "check-circle", style = "color:" & SuccessColor)
          tSpan(style = "font-weight:600;color:" & SuccessColor):
            "Modified " & $safeInt(result, "count") & " rows"
        if safeStr(result, "message").len > 0:
          tDiv(style = "font-size:.85rem;color:" & (if dark: DarkTextMuted else: "#666") & ";margin-top:.25rem"):
            $safeStr(result, "message")
    elif kind == "ok":
      tDiv(style = cardStyle(dark)):
        tDiv(style = "display:flex;align-items:center;gap:.5rem"):
          tSlIcon(name = "check-circle", style = "color:" & SuccessColor)
          tSpan(style = "font-weight:600;color:" & SuccessColor):
            "Success"
        if safeStr(result, "okMessage").len > 0:
          tDiv(style = "font-size:.85rem;color:" & (if dark: DarkTextMuted else: "#666") & ";margin-top:.25rem"):
            $safeStr(result, "okMessage")
    elif kind == "error":
      tDiv(style = cardStyle(dark)):
        tDiv(style = "display:flex;align-items:center;gap:.5rem"):
          tSlIcon(name = "x-circle", style = "color:" & DangerColor)
          tSpan(style = "font-weight:600;color:" & DangerColor):
            "Error"
        tDiv(style = "font-size:.85rem;color:" & (if dark: DarkText else: "#c41010") & ";margin-top:.25rem;font-family:monospace"):
          $safeStr(result, "error")
    elif kind == "useDatabase":
      tDiv(style = cardStyle(dark)):
        tDiv(style = "display:flex;align-items:center;gap:.5rem"):
          tSlIcon(name = "database", style = "color:" & InfoColor)
          tSpan(style = "font-weight:600"):
            "Switched to database: " & $safeStr(result, "newDatabase")
    elif kind == "useSchema":
      tDiv(style = cardStyle(dark)):
        tDiv(style = "display:flex;align-items:center;gap:.5rem"):
          tSlIcon(name = "layers", style = "color:" & InfoColor)
          tSpan(style = "font-weight:600"):
            "Switched to schema: " & $safeStr(result, "newSchema")
    else:
      tDiv(style = "color:" & (if dark: DarkTextMuted else: "#888") & ";font-size:.85rem"):
        "Unknown result type"

component SavedQueries:
  html:
    let dark = gDarkMode.get()
    let saved = gSavedQueries.get()
    
    if saved.len > 0:
      tDiv(style = "margin-top:.75rem"):
        tDiv(style = "font-size:.75rem;color:" & (if dark: DarkTextMuted else: "#666") & ";margin-bottom:.25rem;font-weight:600"):
          "Saved Queries"
        for (name, sql) in saved:
          tDiv(style = "display:flex;align-items:center;gap:.5rem;margin-bottom:.25rem"):
            tSlButton(
              size = "small",
              variant = "default",
              @click = proc() =
                setMonacoValue(cstring(sql))
            ):
              name
            tSlIconButton(
              name = "trash",
              size = "small",
              label = "Delete"
            )

component SqlEditorPage:
  html:
    let dark = gDarkMode.get()
    let db = gCurrentDatabase.get()
    let sc = gCurrentSchema.get()
    
    tDiv(style = shellStyle(dark)):
      AppHeader()
      tMain(style = mainStyle(dark)):
        tH2(style = "font-size:1.05rem;font-weight:700;color:" & (if dark: DarkText else: "#111") & ";margin:0;margin-bottom:1rem"):
          "SQL Query Editor"
        
        SqlEditor(
          database = db,
          schema = sc,
          onExecute = proc(sql: string) =
            {.emit: """
            discard executeSql(`sql`, `db`, `sc`);
            """.}
        )
        
        tH3(style = "font-size:.95rem;font-weight:600;color:" & (if dark: DarkText else: "#111") & ";margin:1rem 0 .5rem"):
          "Query Results"
        
        SqlResultViewer()
      
      AppFooter()