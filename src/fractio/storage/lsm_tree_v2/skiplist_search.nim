# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License

## Zero-copy search operations for InternalKey skip lists
## Uses SearchKey with borrowed KeySlice for zero-copy lookups

import std/atomics
import types
import crossbeam_skiplist

const
  HEIGHT_BITS = 5
  MAX_HEIGHT = 1 shl HEIGHT_BITS

type
  PositionInternal* = object
    found*: SkipListNode[InternalKey, string]
    left*: array[MAX_HEIGHT, SkipListNode[InternalKey, string]]
    right*: array[MAX_HEIGHT, SkipListNode[InternalKey, string]]

proc getTower(node: SkipListNode[InternalKey, string]): ptr UncheckedArray[
    Atomic[SkipListNode[InternalKey, string]]] {.inline.} =
  cast[ptr UncheckedArray[Atomic[SkipListNode[InternalKey, string]]]](
    cast[uint](node) + uint(sizeof(SkipListNodeObj[InternalKey, string]))
  )

proc searchPositionWithSearchKey(s: SkipList[InternalKey, string],
    key: SearchKey, result: var PositionInternal) {.inline.} =
  ## Search for SearchKey in InternalKey skip list - ZERO COPY

  let maxH = load(s.maxHeight, moRelaxed)

  for i in 0 ..< MAX_HEIGHT:
    result.left[i] = s.head
    result.right[i] = nil

  result.found = nil

  var pred = s.head
  var predTower = pred.getTower()
  var level = maxH - 1

  while level >= 0:
    var curr = load(predTower[level], moRelaxed)

    # Move forward while curr.key < search key (uses InternalKey < SearchKey)
    while curr != nil and curr.key < key:
      pred = curr
      predTower = pred.getTower()
      curr = load(predTower[level], moRelaxed)

    result.left[level] = pred
    result.right[level] = curr

    if curr != nil and key == curr.key and result.found == nil:
      result.found = curr

    if level == 0:
      break
    level.dec()

proc rangeFromWithSearchKey*(s: SkipList[InternalKey, string],
    startKey: SearchKey): RangeIter[InternalKey, string] =
  ## Create iterator over key range [startKey, +infinity) using SearchKey
  var pos: PositionInternal
  searchPositionWithSearchKey(s, startKey, pos)
  var start = pos.found

  if start == nil or start.key < startKey:
    start = pos.right[0]

  RangeIter[InternalKey, string](list: s, current: start,
      endKey: default(InternalKey), endInclusive: false)
