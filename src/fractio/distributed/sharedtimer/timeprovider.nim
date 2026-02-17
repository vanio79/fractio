## Abstract TimeProvider base class
## Provides a common interface for time sources.

import ./types

type
  TimeProvider* = ref object of RootObj
    ## Base class for time sources. Can be extended for monotonic, wall-clock, or mock time.

method now*(self: TimeProvider): Timestamp {.base, gcsafe.} =
  ## Get current time in nanoseconds.
  result = 0
