## WallClockTimeProvider - uses wall-clock time (subject to NTP adjustments)

import ./timeprovider
import ./types
import std/times

type
  WallClockTimeProvider* = ref object of TimeProvider
    ## Time provider based on wall-clock time (may jump forwards/backwards).

method now*(self: WallClockTimeProvider): Timestamp =
  let t = getTime()
  result = toUnix(t) * 1_000_000_000 + nanosecond(t).int64
