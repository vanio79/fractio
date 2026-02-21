# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Write Delay/Throttling Implementation
##
## Implements write stall mechanisms to prevent:
## - Unbounded L0 growth (slows reads)
## - OOM from too many sealed memtables
##
## Threshold behavior:
## - l0Runs < 20: No throttling
## - l0Runs 20-29: Increasing CPU busy-wait (throttle)
## - l0Runs >= 30: Sleep-based halt (see checkWriteHalt)

const
  StepSize* = 10_000
  Threshold* = 20         # Start throttling at 20 L0 tables
  HaltThreshold* = 30     # Halt writes at 30 L0 tables
  MaxSealedMemtables* = 4 # Halt if 4+ sealed memtables

proc performWriteStall*(l0Runs: int) =
  ## Performs CPU-based write throttling when L0 runs exceed threshold.
  ##
  ## This uses a busy-wait approach with increasing delay based on
  ## how far above the threshold we are. The delay grows linearly
  ## with the number of L0 tables above threshold.
  if l0Runs >= Threshold and l0Runs < HaltThreshold:
    let d = l0Runs - Threshold
    for i in 0 ..< (d * StepSize):
      # Busy-wait that doesn't get optimized away
      # Using discard to prevent compiler optimization
      discard i
