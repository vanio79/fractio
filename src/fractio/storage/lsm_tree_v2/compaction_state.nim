# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Compaction State
##
## Compaction state management.

import std/[sets]
import types

# Hidden set for compaction
type
  HiddenSet* = ref object
    ## Tracks hidden entries during compaction
    hiddenKeys*: HashSet[pointer] # Would be InternalKey

proc newHiddenSet*(): HiddenSet =
  HiddenSet(hiddenKeys: initHashSet[pointer]())

proc insert*(hs: HiddenSet, key: pointer) =
  hs.hiddenKeys.incl(key)

proc contains*(hs: HiddenSet, key: pointer): bool =
  hs.hiddenKeys.contains(key)

# Compaction state
type
  CompactionState* = ref object
    ## Current state of compaction
    isRunning*: bool
    inputLevel*: int
    outputLevel*: int
    hiddenSet*: HiddenSet

proc newCompactionState*(): CompactionState =
  CompactionState(
    isRunning: false,
    inputLevel: 0,
    outputLevel: 0,
    hiddenSet: newHiddenSet()
  )
