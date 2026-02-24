# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Key Range
##
## Represents a range of keys for queries and compaction.

import types

type
  BoundKind* = enum
    bkIncluded
    bkExcluded
    bkUnbounded

  Bound*[T] = object
    case kind*: BoundKind
    of bkIncluded, bkExcluded:
      value*: T
    of bkUnbounded:
      discard

  KeyRange* = object
    ## A key range [min, max] inclusive on both sides
    minKey*: Slice
    maxKey*: Slice

proc newKeyRange*(minKey, maxKey: Slice): KeyRange =
  KeyRange(minKey: minKey, maxKey: maxKey)

proc emptyKeyRange*(): KeyRange =
  KeyRange(minKey: emptySlice(), maxKey: emptySlice())

proc minKey*(kr: KeyRange): Slice = kr.minKey
proc maxKey*(kr: KeyRange): Slice = kr.maxKey

proc containsKey*(kr: KeyRange, key: Slice): bool =
  key >= kr.minKey and key <= kr.maxKey

proc containsRange*(kr, other: KeyRange): bool =
  kr.minKey <= other.minKey and kr.maxKey >= other.maxKey

proc overlapsWith*(kr, other: KeyRange): bool =
  kr.maxKey >= other.minKey and kr.minKey <= other.maxKey

proc aggregate*(ranges: seq[KeyRange]): KeyRange =
  if ranges.len == 0:
    return emptyKeyRange()

  var minK = ranges[0].minKey
  var maxK = ranges[0].maxKey

  for kr in ranges:
    if kr.minKey < minK:
      minK = kr.minKey
    if kr.maxKey > maxK:
      maxK = kr.maxKey

  KeyRange(minKey: minK, maxKey: maxK)

# Bound helpers
proc included*[T](value: T): Bound[T] =
  Bound[T](kind: bkIncluded, value: value)

proc excluded*[T](value: T): Bound[T] =
  Bound[T](kind: bkExcluded, value: value)

proc unbounded*[T](): Bound[T] =
  Bound[T](kind: bkUnbounded)

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing key range..."

  let kr = newKeyRange(newSlice("a"), newSlice("z"))
  echo "Contains 'm': ", kr.containsKey(newSlice("m"))
  echo "Contains 'a': ", kr.containsKey(newSlice("a"))
  echo "Contains 'zz': ", kr.containsKey(newSlice("zz"))

  let kr2 = newKeyRange(newSlice("b"), newSlice("y"))
  echo "Overlaps: ", kr.overlapsWith(kr2)

  echo "Key range tests passed!"
