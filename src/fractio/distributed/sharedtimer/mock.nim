## MockTimeProvider - deterministic time for testing

import ./timeprovider
import ./types

type
  MockTimeProvider* = ref object of TimeProvider
    currentTime*: Timestamp
      ## The time to return on each call to now().

method now*(self: MockTimeProvider): Timestamp =
  result = self.currentTime

proc setTime*(self: MockTimeProvider, t: Timestamp) =
  ## Set the current time for this mock provider.
  self.currentTime = t
