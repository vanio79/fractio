# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import std/atomics

# Ephemeral, runtime stats
type
  Stats* = object
    # Active compaction counter
    activeCompactionCount*: Atomic[int]

    # Time spent in compactions (in microseconds)
    timeCompacting*: Atomic[uint64]

    # Number of completed compactions
    compactionsCompleted*: Atomic[int]

# Constructor
proc newStats*(): Stats =
  result = Stats()
  result.activeCompactionCount.store(0, moRelaxed)
  result.timeCompacting.store(0, moRelaxed)
  result.compactionsCompleted.store(0, moRelaxed)
