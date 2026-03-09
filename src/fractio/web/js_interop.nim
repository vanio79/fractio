# JS FFI bindings for the HappyX SPA frontend.
# All importjs procs live here to keep frontend.nim slim.

import std/[jsffi, asyncjs]

proc fetchJson*(url: cstring): Future[JsObject]
    {.importjs: "fetch(#).then(r=>r.json())", async.}

proc fetchDelete*(url: cstring): Future[JsObject]
    {.importjs: "fetch(#,{method:'DELETE'}).then(r=>r.json())", async.}

proc fetchPost*(url: cstring, body: cstring): Future[JsObject]
    {.importjs: "fetch(#,{method:'POST',headers:{'Content-Type':'application/json'},body:#}).then(r=>r.json())", async.}

proc jsStringify*(o: JsObject): cstring
    {.importjs: "JSON.stringify(#)".}

proc numFmt*(n: float): cstring
    {.importjs: "Number(#).toLocaleString()".}

proc jsParseInt*(s: cstring): int
    {.importjs: "parseInt(#,10)".}

proc getInputVal*(id: cstring): cstring
    {.importjs: "(document.getElementById(#)||{value:''}).value".}

proc clearInput*(id: cstring)
    {.importjs: "(function(i){var e=document.getElementById(i);if(e)e.value='';})(#)".}

proc jsSetInterval*(fn: proc(), ms: int)
    {.importjs: "setInterval(#,#)".}

# Safe field accessors — coerce missing/null fields without BigInt crash
proc safeInt*(obj: JsObject, field: cstring): int
    {.importjs: "Number(#[#]??0)".}

proc safeFloat*(obj: JsObject, field: cstring): float
    {.importjs: "Number(#[#]??0)".}

proc safeStr*(obj: JsObject, field: cstring): cstring
    {.importjs: "String(#[#]??'')".}

# Returns integer field as a string — avoids Nim $int -> BigInt conversion
proc safeIntStr*(obj: JsObject, field: cstring): cstring
    {.importjs: "String(Number(#[#]??0))".}

proc jsLen*(obj: JsObject): cstring
    {.importjs: "String((#)?.length??0)".}

# WebSocket native JS interop
proc jsParseJsonStr*(s: cstring): JsObject
    {.importjs: "JSON.parse(#)".}

proc jsWsNew*(url: cstring): JsObject
    {.importjs: "new WebSocket(#)".}

proc jsWsOnMessage*(ws: JsObject, cb: proc(ev: JsObject))
    {.importjs: "#.onmessage = #".}

proc jsWsOnClose*(ws: JsObject, cb: proc())
    {.importjs: "#.onclose = #".}

proc jsWsOnOpen*(ws: JsObject, cb: proc())
    {.importjs: "#.onopen = #".}

proc jsEvData*(ev: JsObject): cstring
    {.importjs: "#.data".}

proc jsLocation*(): cstring
    {.importjs: "(function(){return window.location.host;})()".}

proc jsSetInnerHtml*(id: cstring, html: cstring)
    {.importjs: "(function(i,h){var e=document.getElementById(i);if(e)e.innerHTML=h;})(#,#)".}

proc jsSetTimeout*(fn: proc(), ms: int)
    {.importjs: "setTimeout(#,#)".}

# Safe float→integer-string for SVG attributes (avoids BigInt crash)
proc istrJs*(v: float): cstring {.importjs: "String(Math.round(#))".}
proc istr*(v: float): string = $istrJs(v)
