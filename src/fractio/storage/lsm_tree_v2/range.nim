# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Range Operations
##
## Range queries and prefix operations.

import std/[options]
import types

proc seqnoFilter*(itemSeqno, seqno: SeqNo): bool =
  ## Filter items by sequence number
  itemSeqno < seqno

proc prefixUpperRange*(prefix: Slice): BoundKind =
  ## Calculate the prefix's upper range
  if prefix.len == 0:
    return bkUnbounded

  # Find the first byte we can increment
  for i in countdown(prefix.len - 1, 0):
    if prefix[i] != '\255':
      return bkExcluded # Would return upper bound

  bkUnbounded

proc prefixToRange*(prefix: Slice): tuple[start, finish: BoundKind] =
  ## Convert a prefix to range bounds
  if prefix.len == 0:
    return (bkUnbounded, bkUnbounded)

  (bkIncluded, prefixUpperRange(prefix))

# Iter state for range iteration
type
  IterState* = ref object
    ## The iter state references memtables used while range is open
    version*: pointer # Would be SuperVersion
    ephemeral*: bool

proc newIterState*(): IterState =
  IterState(version: nil, ephemeral: false)
