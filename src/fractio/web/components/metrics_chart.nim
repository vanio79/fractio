# Fractio Web Dashboard - Metrics Chart Component
#
# Real-time metrics visualization using Chart.js.

import happyx
import std/[jsffi, sequtils]
import ../styles
import ../store
import ../js_interop

# Chart.js integration
proc loadChartJsLibrary*() =
  {.emit: """
  if (!window.chartJsLoaded) {
    window.chartJsLoaded = true;
    var script = document.createElement('script');
    script.src = 'https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js';
    script.onload = function() {
      window.dispatchEvent(new Event('chartjs-loaded'));
    };
    document.head.appendChild(script);
  }
  """.}

var gMetricsChart: JsObject = nil
var gMetricsDataPoints: seq[JsObject] = @[]

proc initMetricsChart*(id: cstring) =
  {.emit: """
  if (window.Chart) {
    var ctx = document.getElementById(id);
    if (ctx) {
      `gMetricsChart` = new Chart(ctx, {
        type: 'line',
        data: {
          labels: [],
          datasets: [{
            label: 'Requests/sec',
            data: [],
            borderColor: '#e81c1c',
            backgroundColor: 'rgba(232, 28, 28, 0.1)',
            fill: true,
            tension: 0.4
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            y: {
              beginAtZero: true
            }
          },
          plugins: {
            legend: {
              display: false
            }
          }
        }
      });
    }
  }
  """.}
  result

proc updateMetricsChart*(requestsTotal: float, requestsOK: float,
    requestsErr: float) =
  {.emit: """
  if (`gMetricsChart`) {
    var now = new Date().toLocaleTimeString();
    `gMetricsChart`.data.labels.push(now);
    `gMetricsChart`.data.datasets[0].data.push(`requestsOK` - (`gMetricsChart`.data.datasets[0].data[`gMetricsChart`.data.datasets[0].data.length - 1] || 0));
    
    // Keep last 60 points
    if (`gMetricsChart`.data.labels.length > 60) {
      `gMetricsChart`.data.labels.shift();
      `gMetricsChart`.data.datasets[0].data.shift();
    }
    `gMetricsChart`.update('none');
  }
  """.}

component MetricsChart:
  chartId: string = "metrics-chart"
  height: string = "200px"
  chartLoaded: bool = false

  created:
    loadChartJsLibrary()
    {.emit: """
    window.addEventListener('chartjs-loaded', function() {
      `self`.`chartLoaded` = true;
      initMonacoEditor('metrics-chart');
    });
    """.}

  html:
    let dark = gDarkMode.get()
    let m = gMetrics.get()
    let requestsTotal = safeFloat(m, "requestsTotal")
    let requestsOK = safeFloat(m, "requestsOK")
    let requestsErr = safeFloat(m, "requestsErr")
    let bytesIn = safeFloat(m, "bytesIn")
    let bytesOut = safeFloat(m, "bytesOut")
    let kvGets = safeFloat(m, "kvGets")
    let kvPuts = safeFloat(m, "kvPuts")
    let kvDeletes = safeFloat(m, "kvDeletes")

    # Update chart on render
    if self.chartLoaded:
      updateMetricsChart(requestsTotal, requestsOK, requestsErr)

    tDiv(style = cardStyle(dark) & ";padding:1rem"):
      tDiv(style = "height:" & self.height):
        tCanvas(id = self.chartId, style = "width:100%;height:100%")
        if not self.chartLoaded:
          tDiv(style = "height:100%;display:flex;justify-content:center;align-items:center;color:#888"):
            "Loading chart..."

component MetricsTable:
  title: string
  items: seq[(string, string)]

  html:
    let dark = gDarkMode.get()

    tDiv(style = cardStyle(dark)):
      tStrong(style = "font-size:.85rem;color:" & (
          if dark: DarkText else: "#111")):
        self.title
      tTable(style = "width:100%;font-size:.875rem;border-collapse:collapse;margin-top:.5rem"):
        for (lbl, val) in self.items:
          tTr:
            tTd(style = "padding:.35rem 0;color:" & (
                if dark: DarkTextMuted else: "#444")):
              lbl
            tTd(style = "text-align:right;font-family:monospace;color:" &
                PrimaryColor & ";font-weight:600"):
              val

component MetricsPageContent:
  html:
    let dark = gDarkMode.get()
    let m = gMetrics.get()

    # Format values
    let total = $numFmt(safeFloat(m, "requestsTotal"))
    let ok = $numFmt(safeFloat(m, "requestsOK"))
    let err = $numFmt(safeFloat(m, "requestsErr"))
    let bytesIn = $numFmt(safeFloat(m, "bytesIn"))
    let bytesOut = $numFmt(safeFloat(m, "bytesOut"))
    let gets = $numFmt(safeFloat(m, "kvGets"))
    let puts = $numFmt(safeFloat(m, "kvPuts"))
    let dels = $numFmt(safeFloat(m, "kvDeletes"))
    let activeTxns = $numFmt(safeFloat(m, "activeTxns"))
    let committedTxns = $numFmt(safeFloat(m, "committedTxns"))
    let abortedTxns = $numFmt(safeFloat(m, "abortedTxns"))

    tDiv(style = "display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:1rem"):
      MetricsTable(title = "Requests", items = @[
        ("Total", total),
        ("OK", ok),
        ("Errors", err)
      ])

      MetricsTable(title = "Network", items = @[
        ("Bytes In", bytesIn),
        ("Bytes Out", bytesOut)
      ])

      MetricsTable(title = "KV Operations", items = @[
        ("Gets", gets),
        ("Puts", puts),
        ("Deletes", dels)
      ])

      MetricsTable(title = "Transactions", items = @[
        ("Active", activeTxns),
        ("Committed", committedTxns),
        ("Aborted", abortedTxns)
      ])

    # Real-time chart
    tDiv(style = "margin-top:1.5rem"):
      tH3(style = "font-size:.95rem;font-weight:600;color:" & (
          if dark: DarkText else: "#111") & ";margin:0 .5rem"):
        "Request Rate (per second)"
      MetricsChart(height = "250px")
