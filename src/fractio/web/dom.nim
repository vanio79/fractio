# Fractio Web Dashboard - DOM Injection Utilities
#
# This module provides DOM injection functions that update the DOM directly
# without triggering HappyX re-renders. Used for clock drift and other
# real-time updates.

import ./js_interop
import ./styles
import ./chart
import ./store

const MaxSamples = chart.MaxSamples # Alias to avoid ambiguity

# =============================================================================
# Clock Drift DOM Injection
# =============================================================================

proc injectClockDom*() =
  ## Update all clock DOM nodes directly without triggering HappyX re-render.
  ## This uses raw JS DOM manipulation to avoid HappyX's re-render cycle.
  let wsstColor = if gDriftWsStr == "live": SuccessColor else: WarningColor
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
  jsSetInnerHtml("drift-chart", cstring(buildLineChartSvg(driftChartCfg,
      gDriftSamples)))
