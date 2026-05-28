## Query timing instrumentation for Fractio performance analysis.
##
## Provides a per-query timing context that records timestamps at key stages
## of query execution. Timings are logged at debug level when complete.
##
## Usage:
##   let timer = newQueryTimer()
##   timer.stamp("scan_start")
##   ... do work ...
##   timer.stamp("scan_end")
##   timer.log()  # prints breakdown to debug log

import std/[monotimes, strformat, tables, strutils]
import fractio/utils/logging

type
  QueryTimer* = ref object
    ## Accumulates named timestamps for a single query execution.
    stamps*: OrderedTable[string, float] ## name -> elapsed ms from start
    startTicks*: int64                   ## monotonic ticks at creation
    lastTicks*: int64                    ## monotonic ticks at last stamp

proc newQueryTimer*(): QueryTimer =
  ## Create a new query timer. The first stamp is at creation time.
  let now = getMonotime()
  result = QueryTimer(
    stamps: initOrderedTable[string, float](),
    startTicks: now.ticks,
    lastTicks: now.ticks,
  )

proc stamp*(timer: QueryTimer, name: string) =
  ## Record a named timestamp. The value stored is milliseconds since start.
  let now = getMonotime()
  let elapsed = (now.ticks.float - timer.startTicks.float) / 1_000_000.0
  timer.stamps[name] = elapsed
  timer.lastTicks = now.ticks

proc stampDelta*(timer: QueryTimer, name: string) =
  ## Record the time since the last stamp (not since start).
  ## Stores the delta value (ms since last stamp), not absolute from start.
  let now = getMonotime()
  let delta = (now.ticks.float - timer.lastTicks.float) / 1_000_000.0
  timer.stamps[name] = delta
  timer.lastTicks = now.ticks

proc reset*(timer: QueryTimer) =
  ## Reset the timer, clearing all stamps and re-zeroing start time.
  let now = getMonotime()
  timer.stamps.clear()
  timer.startTicks = now.ticks
  timer.lastTicks = now.ticks

proc log*(timer: QueryTimer, query: string = "") =
  ## Log all recorded timestamps as a formatted string.
  var parts: seq[string] = @[]
  for name, ms in timer.stamps.pairs:
    parts.add(&"{name}={ms:.2f}ms")
  let queryInfo = if query.len > 0: &" query={query}" else: ""
  debug(&"[query_timer]{queryInfo} {parts.join(\" \")}")

proc totalMs*(timer: QueryTimer): float =
  ## Return total elapsed time from start to the last stamp, in milliseconds.
  if timer.stamps.len == 0:
    return 0.0
  var last = 0.0
  for ms in timer.stamps.values:
    if ms > last: last = ms
  result = last

proc formatBreakdown*(timer: QueryTimer): string {.raises: [].} =
  ## Return a human-readable breakdown showing delta between consecutive stamps.
  var parts: seq[string] = @[]
  var prevMs = 0.0
  var lastMs = 0.0
  for name, ms in timer.stamps.pairs:
    let delta = ms - prevMs
    parts.add(name & "=" & formatFloat(delta, ffDecimal, 2) & "ms")
    prevMs = ms
    lastMs = ms
  parts.add("total=" & formatFloat(lastMs, ffDecimal, 2) & "ms")
  result = parts.join(" ")
