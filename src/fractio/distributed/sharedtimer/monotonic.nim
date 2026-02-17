## MonotonicTimeProvider - uses monotonic clock (ideal for measuring intervals)

import ./timeprovider
import ./types
import std/times

type
  MonotonicTimeProvider* = ref object of TimeProvider
    ## Time provider based on monotonic clock (not subject to NTP adjustments).

method now*(self: MonotonicTimeProvider): Timestamp =
  let t = getTime()
  result = toUnix(t) * 1_000_000_000 + nanosecond(t).int64
