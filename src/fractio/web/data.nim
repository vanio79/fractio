# Data fetching and WebSocket drift connection.

import happyx
import std/[jsffi, asyncjs]
import ./js_interop
import ./chart
import ./state

var gDriftWs* {.global.}: JsObject = nil

proc doRefresh*() {.async.} =
  try:
    gInfo.set(await fetchJson("/api/info"))
    gHealth.set(await fetchJson("/api/health"))
    gMetrics.set(await fetchJson("/api/metrics"))
    gNodes.set(await fetchJson("/api/nodes"))
    # Re-inject clock DOM after HappyX re-renders wipe #drift-chart
    jsSetTimeout(proc() = injectClockDom(), 0)
  except:
    discard

proc doRemoveNode*(nodeId: int) {.async.} =
  discard await fetchDelete(cstring("/api/nodes/" & $nodeId))
  gNodes.set(await fetchJson("/api/nodes"))

proc doJoinNode*() {.async.} =
  let body = newJsObject()
  body.nodeId     = jsParseInt(getInputVal("join-id"))
  body.host       = getInputVal("join-host")
  body.raftPort   = jsParseInt(getInputVal("join-raft"))
  body.clientPort = jsParseInt(getInputVal("join-client"))
  let resp = await fetchPost("/api/nodes", jsStringify(body))
  gMsgOk.set(safeInt(resp, "success") != 0)
  gMsg.set($safeStr(resp, "message"))
  if gMsgOk:
    clearInput("join-id")
    clearInput("join-host")
    clearInput("join-raft")
    clearInput("join-client")
    gNodes.set(await fetchJson("/api/nodes"))

proc connectDriftWs*() =
  let host = jsLocation()
  let url  = cstring("ws://") & host & cstring("/ws/drift")
  let ws   = jsWsNew(url)
  gDriftWs = ws

  jsWsOnOpen(ws, proc() =
    gDriftWsStr = "live"
    injectClockDom()
  )

  jsWsOnMessage(ws, proc(ev: JsObject) =
    let data = jsEvData(ev)
    try:
      let msg = jsParseJsonStr(data)
      let offsetUs = safeFloat(msg, "offsetUs")
      gDriftSamples.add(offsetUs)
      if gDriftSamples.len > MaxSamples:
        gDriftSamples.delete(0)
      let signChar = if offsetUs >= 0.0: "+" else: ""
      gDriftLastStr = $signChar & $int(offsetUs) & " µs"
      injectClockDom()
    except:
      discard
  )

  jsWsOnClose(ws, proc() =
    gDriftWsStr = "reconnecting…"
    injectClockDom()
    jsSetTimeout(proc() = connectDriftWs(), 2000)
  )
