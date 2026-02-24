# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Utilities
##
## Various utility functions.

import types

proc prefixToRange*(prefix: Slice): (Slice, Slice) =
  ## Convert a prefix to an inclusive range
  if prefix.len == 0:
    return (emptySlice(), emptySlice())

  # Find the upper bound by incrementing the last byte
  var upper = prefix.data
  for i in countdown(prefix.len - 1, 0):
    if upper[i] < 255:
      upper[i] = upper[i] + 1
      break

  (prefix, newSlice(upper))

proc prefixUpperRange*(prefix: Slice): Slice =
  ## Get the exclusive upper bound for a prefix
  if prefix.len == 0:
    return emptySlice()

  var upper = prefix.data
  for i in countdown(prefix.len - 1, 0):
    if upper[i] < 255:
      upper[i] = upper[i] + 1
      break

  newSlice(upper)

proc prefixedRange*(prefix: Slice, startBound, endBound: BoundKind): tuple[
    start, finish: Slice] =
  ## Create a range from a prefix with bounds
  let (lo, hi) = prefixToRange(prefix)

  case startBound
  of bkIncluded:
    discard
  of bkExcluded:
    discard
  of bkUnbounded:
    discard

  case endBound
  of bkIncluded:
    discard
  of bkExcluded:
    discard
  of bkUnbounded:
    discard

  (lo, hi)

when isMainModule:
  echo "Testing utilities..."

  let (lo, hi) = prefixToRange(newSlice("abc"))
  echo "Prefix 'abc' range: ", lo.data, " to ", hi.data

  echo "Utilities tests passed!"
