# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Run Scanner
##
## Scans through a disjoint run - optimized for compaction.

import std/[options]
import types
import version
import error
import table_ext

type
  RunScanner* = ref object
    ## Scans through a disjoint run for compaction
    tables*: seq[pointer] # Would be Run[Table]
    lo*: int
    hi*: int
    loScanner*: Option[TableScanner]

proc culled*(run: seq[pointer], lo, hi: Option[int]): LsmResult[RunScanner] =
  let loIdx = lo.get(0)
  let hiIdx = hi.get(run.len - 1)

  # Would create scanner for first table
  ok(RunScanner(
    tables: run,
    lo: loIdx,
    hi: hiIdx,
    loScanner: nil
  ))

proc next*(s: RunScanner): Option[LsmResult[InternalValue]] =
  ## Get next item from scanner
  none(LsmResult[InternalValue])
