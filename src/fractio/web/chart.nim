# Reusable SVG line chart types and builder procs.

import ./js_interop

type
  ChartThreshold* = object
    value*: float           # Y-axis data value for the line
    color*: string          # stroke color
    dashed*: bool           # dashed or solid
    bandTo*: float          # if != value, fill a band between value..bandTo
    bandColor*: string      # band fill color
    bandOpacity*: float     # band fill-opacity

  LineChartConfig* = object
    width*, height*: float
    padX*, padY*: float
    yMin*, yMax*: float      # data range (symmetric: -yMax..+yMax)
    lineColor*: string       # polyline stroke
    lineWidth*: float
    bgColor*: string         # SVG background rect fill
    borderColor*: string     # SVG border rect stroke
    gridColor*: string       # zero-line color
    axisColor*: string       # Y-axis line color
    thresholds*: seq[ChartThreshold]

const MaxSamples* = 120  # 2 minutes @ 1Hz

let driftChartCfg* = LineChartConfig(
  width: 600.0, height: 120.0, padX: 4.0, padY: 10.0,
  yMin: -25_000.0, yMax: 25_000.0,
  lineColor: "#e81c1c", lineWidth: 2.0,
  bgColor: "#fafafa", borderColor: "#e0e0e0",
  gridColor: "#ccc", axisColor: "#ddd",
  thresholds: @[
    ChartThreshold(value: 10_000.0, color: "#c41010", dashed: true,
                   bandTo: -10_000.0, bandColor: "#e81c1c", bandOpacity: 0.06),
  ],
)

proc chartY*(cfg: LineChartConfig, value: float): float =
  ## Map a data value to pixel Y coordinate.
  let usH = cfg.height - cfg.padY * 2.0
  let vc  = max(cfg.yMin, min(cfg.yMax, value))
  cfg.padY + usH * (0.5 - vc / (cfg.yMax - cfg.yMin))

proc buildPolyline*(cfg: LineChartConfig, samples: seq[float]): string =
  ## Build SVG polyline points string from samples.
  if samples.len < 2: return ""
  let n   = samples.len
  let usW = cfg.width - cfg.padX * 2.0
  var pts = ""
  for i, v in samples:
    let x = cfg.padX + usW * float(i) / float(n - 1)
    let y = chartY(cfg, v)
    if pts.len > 0: pts &= " "
    pts &= istr(x) & "," & istr(y)
  pts

proc buildLineChartSvg*(cfg: LineChartConfig, samples: seq[float]): string =
  ## Build a complete SVG string for a line chart.
  let yZero = chartY(cfg, 0.0)
  let pts   = buildPolyline(cfg, samples)
  let w = istr(cfg.width)
  let h = istr(cfg.height)

  # Open SVG
  result = "<svg viewBox=\"0 0 " & w & " " & h &
    "\" width=\"100%\" style=\"display:block;overflow:visible\" xmlns=\"http://www.w3.org/2000/svg\">"

  # Background
  result &= "<rect width=\"" & w & "\" height=\"" & h &
    "\" fill=\"" & cfg.bgColor & "\" rx=\"4\"/>"

  # Threshold bands and lines
  for th in cfg.thresholds:
    if th.bandTo != th.value:
      let yTop = chartY(cfg, th.value)
      let yBot = chartY(cfg, th.bandTo)
      let bandH = yBot - yTop
      result &= "<rect x=\"0\" y=\"" & istr(yTop) & "\" width=\"" & w &
        "\" height=\"" & istr(bandH) & "\" fill=\"" & th.bandColor &
        "\" fill-opacity=\"" & $th.bandOpacity & "\"/>"
    # Threshold lines
    let yTh = chartY(cfg, th.value)
    let dash = if th.dashed: " stroke-dasharray=\"4,4\"" else: ""
    result &= "<line x1=\"0\" y1=\"" & istr(yTh) & "\" x2=\"" & w &
      "\" y2=\"" & istr(yTh) & "\" stroke=\"" & th.color &
      "\" stroke-width=\"1\"" & dash & " opacity=\"0.7\"/>"
    if th.bandTo != th.value:
      let yTh2 = chartY(cfg, th.bandTo)
      result &= "<line x1=\"0\" y1=\"" & istr(yTh2) & "\" x2=\"" & w &
        "\" y2=\"" & istr(yTh2) & "\" stroke=\"" & th.color &
        "\" stroke-width=\"1\"" & dash & " opacity=\"0.7\"/>"

  # Zero / grid line
  result &= "<line x1=\"0\" y1=\"" & istr(yZero) & "\" x2=\"" & w &
    "\" y2=\"" & istr(yZero) & "\" stroke=\"" & cfg.gridColor &
    "\" stroke-width=\"1\" stroke-dasharray=\"2,6\" opacity=\"0.4\"/>"

  # Data polyline
  if pts.len > 0:
    result &= "<polyline points=\"" & pts & "\" fill=\"none\" stroke=\"" &
      cfg.lineColor & "\" stroke-width=\"" & istr(cfg.lineWidth) &
      "\" stroke-linejoin=\"round\" stroke-linecap=\"round\"/>"

  # Y-axis
  result &= "<line x1=\"0\" y1=\"0\" x2=\"0\" y2=\"" & h &
    "\" stroke=\"" & cfg.axisColor & "\" stroke-width=\"1\"/>"

  # Border
  result &= "<rect width=\"" & w & "\" height=\"" & h &
    "\" fill=\"none\" stroke=\"" & cfg.borderColor & "\" stroke-width=\"1\" rx=\"4\"/>"

  result &= "</svg>"
