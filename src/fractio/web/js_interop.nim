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

proc safeBool*(obj: JsObject, field: cstring): bool
    {.importjs: "(#[#]===true)".}

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

proc jsObjectKeys*(o: JsObject): JsObject
    {.importjs: "Object.keys(#)".}

proc jsArrayLen*(a: JsObject): int
    {.importjs: "((#)||[]).length".}

proc jsArrayGet*(a: JsObject, i: int): cstring
    {.importjs: "String(#[#]||'')".}

proc jsObjField*(o: JsObject, key: cstring): cstring
    {.importjs: "String(#[#]??'')".}

proc jsSetInnerHtml*(id: cstring, html: cstring)
    {.importjs: "(function(i,h){var e=document.getElementById(i);if(e)e.innerHTML=h;})(#,#)".}

proc jsSetTimeout*(fn: proc(), ms: int)
    {.importjs: "setTimeout(#,#)".}

# Capture-safe callback helpers — wrap a value so the closure captures by value
proc jsCaptureClickInt*(el: JsObject, val: int, cb: proc(v: int))
    {.importjs: "(function(e,v,f){e.addEventListener('click',function(){f(v);})})(#,#,#)".}

proc jsCaptureClickIntStr*(el: JsObject, id: int, name: cstring, cb: proc(id: int, name: cstring))
    {.importjs: "(function(e,i,n,f){e.addEventListener('click',function(){f(i,n);})})(#,#,#,#)".}

# Safe float→integer-string for SVG attributes (avoids BigInt crash)
proc istrJs*(v: float): cstring {.importjs: "String(Math.round(#))".}
proc istr*(v: float): string = $istrJs(v)

# Delegated click handler for expandable node rows (class="node-row").
# Reads the node ID from the second child span's textContent.
proc installNodeClickHandler*(cb: proc(nid: int)) =
  {.emit: """
  document.addEventListener('click', function(e) {
    var el = e.target;
    while (el && el !== document.body) {
      if (el.id && el.id.substring(0, 9) === 'node-row-') {
        var nid = parseInt(el.id.substring(9), 10);
        if (!isNaN(nid)) { `cb`(nid); return; }
      }
      el = el.parentElement;
    }
  });
  """.}

# Intercept clicks on internal hash links (/#/...) to use HappyX's route()
# instead of default browser navigation.  This avoids double history entries
# (one from the href change, one from HappyX's hashchange → pushState).
proc installLinkInterceptor*() =
  ## Fix back/forward buttons for HappyX hash-mode routing.
  ##
  ## Two problems:
  ## 1. Clicking <a href="/#/..."> changes hash (1 entry) + hashchange
  ##    handler calls rt() → pushState (2nd entry).
  ## 2. Browser back/forward changes hash → hashchange → rt() → pushState
  ##    creates a NEW forward entry instead of traversing history.
  ##
  ## Fix 1: Intercept link clicks, preventDefault, call rt() directly.
  ## Fix 2: Patch rt() to use replaceState during popstate-triggered
  ##    hash changes (back/forward navigation).
  {.emit: """
  document.addEventListener('click', function(e) {
    var a = e.target;
    while (a && a.tagName !== 'A') a = a.parentElement;
    if (!a) return;
    var h = a.getAttribute('href');
    if (!h || h.length < 3 || h.charAt(0) !== '/' || h.charAt(1) !== '#' || h.charAt(2) !== '/') return;
    e.preventDefault();
    rt(h.substr(2));
  }, true);
  var _isPopping = false;
  window.addEventListener('popstate', function() {
    _isPopping = true;
    setTimeout(function() { _isPopping = false; }, 0);
  });
  var _origPush = History.prototype.pushState;
  History.prototype.pushState = function(state, title, url) {
    if (_isPopping) {
      return History.prototype.replaceState.call(this, state, title, url);
    }
    return _origPush.call(this, state, title, url);
  };
  """.}
