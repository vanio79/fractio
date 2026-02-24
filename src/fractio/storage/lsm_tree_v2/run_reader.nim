# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Run Reader
##
## Reads through a disjoint run (collection of tables).

import std/[options, sequtils]
import types
import version
import error

type
  RunReader* = ref object
    ## Reads through a disjoint run
    run*: seq[pointer]         # Would be Arc[Run[Table]]
    lo*: int
    hi*: int
    loReader*: Option[pointer] # Would be BoxedIterator
    hiReader*: Option[pointer]

proc newRunReader*(run: seq[pointer], rangeStart, rangeEnd: Slice): Option[RunReader] =
  if run.len == 0:
    return none(RunReader)

  let lo = 0
  let hi = run.len - 1

  # Create readers for first and last table
  some(RunReader(
    run: run,
    lo: lo,
    hi: hi,
    loReader: nil,
    hiReader: nil
  ))

proc next*(r: RunReader): Option[LsmResult[InternalValue]] =
  ## Get next item in forward order
  # Would iterate through tables in the run
  none(LsmResult[InternalValue])

proc nextBack*(r: RunReader): Option[LsmResult[InternalValue]] =
  ## Get next item in reverse order
  none(LsmResult[InternalValue])

proc hasNext*(r: RunReader): bool =
  false

proc hasPrevious*(r: RunReader): bool =
  false

# Range overlap calculation
proc rangeOverlapIndexes*(run: seq[pointer], startKey, endKey: Slice): Option[(
    int, int)] =
  ## Find overlapping table indexes for a range
  if run.len == 0:
    return none((int, int))

  # Simplified - return full range
  some((0, run.len - 1))
