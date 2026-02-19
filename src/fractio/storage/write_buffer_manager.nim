# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import std/atomics

# Keeps track of the size of the database's write buffer
type
  WriteBufferManager* = ref object
    value*: Atomic[uint64]

# Constructor
proc newWriteBufferManager*(): WriteBufferManager =
  WriteBufferManager(value: Atomic[uint64](0))

# Get current value
proc get*(manager: WriteBufferManager): uint64 =
  manager.value.load(moAcquire)

# Adds some bytes to the write buffer counter.
# Returns the counter *after* incrementing.
proc allocate*(manager: WriteBufferManager, n: uint64): uint64 =
  let before = manager.value.fetchAdd(n, moAcqRel)
  before + n

# Frees some bytes from the write buffer counter.
# Returns the counter *after* decrementing.
proc free*(manager: WriteBufferManager, n: uint64): uint64 =
  while true:
    let now = manager.value.load(moAcquire)
    let subbed = max(0, now - n)

    if manager.value.compareExchange(now, subbed, moSeqCst, moSeqCst):
      return subbed
