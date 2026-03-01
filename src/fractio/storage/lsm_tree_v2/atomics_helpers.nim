# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Atomic Helpers for LSM Tree
##
## Provides atomic operations that match Rust's atomics behavior

import std/atomics
import types

template atomicMaxSeqNo*(a: var Atomic[SeqNo], val: SeqNo): bool =
  ## Atomic max for SeqNo - returns true if updated
  ## Uses CAS loop which is the standard pattern in Nim
  block:
    var current = load(a, moRelaxed)
    var result = false
    while true:
      if val <= current:
        break
      # Try to update if current hasn't changed
      if compareExchange(a, current, val, moRelaxed, moRelaxed):
        result = true
        break
      # CAS failed, current was updated, retry
      current = load(a, moRelaxed)
    result

template atomicMaxSeqNoAcqRel*(a: var Atomic[SeqNo], val: SeqNo): bool =
  ## Atomic max for SeqNo with Acquire ordering - matches Rust's fetch_max
  ## Returns true if updated
  block:
    var current = load(a, moAcquire)
    var result = false
    while true:
      if val <= current:
        break
      # Try to update if current hasn't changed
      if compareExchange(a, current, val, moAcquire, moAcquire):
        result = true
        break
      # CAS failed, current was updated, retry
      current = load(a, moAcquire)
    result

template fetchAddSeqNo*(a: var Atomic[SeqNo], val: int64,
    order: MemoryOrder = moSequentiallyConsistent): SeqNo =
  ## Atomic fetch-add for SeqNo using CAS loop
  ## Returns the old value
  block:
    var current = load(a, order)
    var result: SeqNo
    while true:
      let newVal = SeqNo(int64(current) + val)
      if compareExchange(a, current, newVal, order, order):
        result = current
        break
      # CAS failed, retry with updated current
    result
