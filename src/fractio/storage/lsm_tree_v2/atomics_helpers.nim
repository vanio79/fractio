# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Atomic Helpers for LSM Tree
##
## Provides atomic operations that match Rust's atomics behavior

import std/atomics
import fractio/storage/lsm_tree_v2/types

proc atomicMaxSeqNo*(a: var Atomic[SeqNo], val: SeqNo): bool =
  ## Atomic max for SeqNo - returns true if updated
  ## Uses CAS loop which is the standard pattern in Nim
  var current = load(a, moRelaxed)
  while true:
    if val <= current:
      return false
    # Try to update if current hasn't changed
    if compareExchange(a, current, val, moRelaxed, moRelaxed):
      return true
    # CAS failed, current was updated, retry
    current = load(a, moRelaxed)

proc fetchAddSeqNo*(a: var Atomic[SeqNo], val: int64,
    order: MemoryOrder = moSequentiallyConsistent): SeqNo =
  ## Atomic fetch-add for SeqNo using CAS loop
  ## Returns the old value
  var current = load(a, order)
  while true:
    let newVal = SeqNo(int64(current) + val)
    if compareExchange(a, current, newVal, order, order):
      return current
    # CAS failed, retry with updated current
