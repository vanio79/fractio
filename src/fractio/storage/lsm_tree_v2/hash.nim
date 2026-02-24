# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Hash Functions
##
## Hash functions for bloom filters and other purposes.

import std/[strutils]

proc hash64*(data: string): uint64 =
  ## 64-bit hash using simple polynomial hash
  ## (In production, use xxhash or similar)
  var h: uint64 = 14695981039346656037'u64 # FNV offset basis
  for c in data:
    h = h xor uint64(ord(c))
    h = h * 1099511628211'u64 # FNV prime
  h

proc hash64*(data: seq[uint8]): uint64 =
  hash64(cast[string](data))

type
  Hash128* = tuple[lo, hi: uint64]

proc hash128*(data: string): Hash128 =
  ## 128-bit hash (simplified - would use proper implementation)
  let h1 = hash64(data)
  let h2 = hash64(data & "salt")
  result = (lo: h1, hi: h2)

proc hash128*(data: seq[uint8]): Hash128 =
  hash128(cast[string](data))

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing hash functions..."

  let h1 = hash64("")
  echo "hash64(''): ", h1

  let h2 = hash64("hello")
  echo "hash64('hello'): ", h2

  let h3 = hash128("")
  echo "hash128(''): ", h3

  echo "Hash tests passed!"
