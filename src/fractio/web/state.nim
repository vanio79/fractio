# Global reactive state vars and clock drift globals.

import happyx
import std/jsffi
import ./js_interop
import ./chart

var
  gInfo*:    State[JsObject] = remember newJsObject()
  gHealth*:  State[JsObject] = remember newJsObject()
  gMetrics*: State[JsObject] = remember newJsObject()
  gNodes*:   State[JsObject] = remember newJsObject()
  gMsg*:     State[string]   = remember ""
  gMsgOk*:   State[bool]     = remember false

  # Clock drift: plain globals — updated directly via jsSetInnerHtml,
  # NOT via State, so they don't trigger HappyX re-renders.
  gDriftSamples*: seq[float] = @[]
  gDriftLastStr*: string     = "—"
  gDriftWsStr*:   string     = "connecting…"

proc injectClockDom*() =
  ## Update all clock DOM nodes directly without triggering any HappyX re-render.
  let wsstColor = if gDriftWsStr == "live": "#1a7f37" else: "#b45309"
  jsSetInnerHtml("clock-ws-status", cstring(
    "<span style=\"font-size:.75rem;color:" & wsstColor &
    ";font-weight:600;background:#f0f0f0;padding:.2rem .6rem;border-radius:999px\">" &
    gDriftWsStr & "</span>"))
  jsSetInnerHtml("clock-last-offset", cstring(
    "<div style=\"font-size:1.2rem;font-weight:700;color:#e81c1c;font-family:monospace\">" &
    gDriftLastStr & "</div>"))
  jsSetInnerHtml("clock-sample-count", cstring(
    "<div style=\"font-size:1.2rem;font-weight:700;color:#e81c1c;font-family:monospace\">" &
    $gDriftSamples.len & " / " & $MaxSamples & "</div>"))
  jsSetInnerHtml("drift-chart", cstring(buildLineChartSvg(driftChartCfg, gDriftSamples)))
