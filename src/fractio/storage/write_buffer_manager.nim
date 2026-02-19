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
  result = WriteBufferManager()
  result.value.store(0'u64, moRelaxed)

# Get current value
proc get*(manager: WriteBufferManager): uint64 =
  manager.value.load(moAcquire)

# Adds some bytes to the write buffer counter.
# Returns the counter *after* incrementing.
proc allocate*(manager: WriteBufferManager, n: uint64): uint64 =
  let before = manager.value.fetchAdd(n, moRelaxed)
  before + n

# Frees some bytes from the write buffer counter.
# Returns the counter *after* decrementing.
proc free*(manager: WriteBufferManager, n: uint64): uint64 =
  while true:
    var current = manager.value.load(moAcquire)
    let subbed = if current > n: current - n else: 0'u64

    if manager.value.compareExchange(current, subbed, moSequentiallyConsistent,
        moSequentiallyConsistent):
      return subbed
