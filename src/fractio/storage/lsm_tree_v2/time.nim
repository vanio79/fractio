# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Time Utilities
##
## Time-related utilities.

import std/[times]

proc unixTimestamp*(): int64 =
  ## Get current Unix timestamp
  getTime().toUnix()

proc monotonicNow*(): int64 =
  ## Get monotonic time in nanoseconds
  epochTime().int64 * 1_000_000_000

when isMainModule:
  echo "Unix timestamp: ", unixTimestamp()
