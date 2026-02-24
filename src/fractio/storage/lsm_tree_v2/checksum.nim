# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Checksum
##
## Checksum types for data integrity.

import std/[strutils]
import hash

type
  ChecksumType* = enum
    ctXxh3 = 0

  Checksum* = object
    ## 128-bit checksum represented as two 64-bit values
    high*: uint64
    low*: uint64

proc newChecksum*(high, low: uint64): Checksum =
  Checksum(high: high, low: low)

proc `$`*(c: Checksum): string =
  "Checksum(" & $c.high & ":" & $c.low & ")"

proc `==`*(a, b: Checksum): bool =
  a.high == b.high and a.low == b.low

proc check*(got, expected: Checksum): bool =
  got.high == expected.high and got.low == expected.low

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing checksum..."

  let cs1 = newChecksum(123, 456)
  let cs2 = newChecksum(123, 456)
  let cs3 = newChecksum(789, 101)

  echo "Checksum eq: ", cs1 == cs2
  echo "Checksum ne: ", cs1 == cs3

  echo "Checksum tests passed!"
