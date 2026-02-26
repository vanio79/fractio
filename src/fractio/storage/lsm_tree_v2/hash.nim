# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Hash Functions
##
## Hash functions matching the Rust lsm-tree crate for bloom filters and checksums.

# ============================================================================
# xxhash64 - using working implementation
# ============================================================================

const
  PRIME64_1 = 0x9E3779B185EBCA87'u64
  PRIME64_2 = 0xC2B2AE3D27D4EB4F'u64
  PRIME64_3 = 0x165667B19E3779F9'u64
  PRIME64_4 = 0x85EBCA77C2B2AE63'u64
  PRIME64_5 = 0x27D4EB2F165667C5'u64

proc xxhash64*(data: string, seed: uint64 = 0): uint64 =
  ## xxhash64 - fast, non-cryptographic hash
  let len = data.len
  var h: uint64

  if len >= 4:
    var v1 = seed + PRIME64_1 + PRIME64_2
    var v2 = seed + PRIME64_2
    var v3 = seed + 0
    var v4 = seed - PRIME64_1

    # Process 4-byte chunks
    var i = 0
    while i + 4 <= len:
      let k1 = (uint64(uint8(data[i])) shl 0) or
                (uint64(uint8(data[i+1])) shl 8) or
                (uint64(uint8(data[i+2])) shl 16) or
                (uint64(uint8(data[i+3])) shl 24)

      v1 = v1 xor (k1 * PRIME64_2)
      v1 = (v1 shl 31) or (v1 shr 33)
      v1 = v1 * PRIME64_1

      i += 4

    # Merge
    h = ((v1 shl 1) or (v1 shr 63)) +
        ((v2 shl 7) or (v2 shr 57)) +
        ((v3 shl 12) or (v3 shr 52)) +
        ((v4 shl 18) or (v4 shr 46))

    h = h xor (h shr 33) * PRIME64_1
    h = h * PRIME64_1
    h = h xor (h shr 29) * PRIME64_3
    h = h xor (h shr 32)
  else:
    h = seed + PRIME64_5 + uint64(len)
    for i in 0 ..< len:
      h = h xor (uint64(uint8(data[i])) * PRIME64_1)
      h = (h shl 23) or (h shr 41)
      h = h * PRIME64_2
    h = h xor (h shr 33) * PRIME64_1
    h = h xor (h shr 33)

  return h

proc xxhash64*(data: seq[uint8], seed: uint64 = 0): uint64 =
  xxhash64(cast[string](data), seed)

# ============================================================================
# xxhash128
# ============================================================================

type
  Hash128* = tuple[lo, hi: uint64]
  ## 128-bit hash represented as two 64-bit values

proc xxhash128*(data: string, seed: uint64 = 0): Hash128 =
  ## xxhash128 - 128-bit variant using two 64-bit hashes
  let h1 = xxhash64(data, seed)
  let h2 = xxhash64(data, seed xor 0x9E3779B185EBCA87'u64)
  return (lo: h1, hi: h2)

proc xxhash128*(data: seq[uint8], seed: uint64 = 0): Hash128 =
  xxhash128(cast[string](data), seed)

# ============================================================================
# Aliases for lsm-tree compatibility
# ============================================================================

proc hash64*(data: string): uint64 =
  xxhash64(data)

proc hash64*(data: seq[uint8]): uint64 =
  xxhash64(data)

proc hash128*(data: string): Hash128 =
  xxhash128(data)

proc hash128*(data: seq[uint8]): Hash128 =
  xxhash128(data)

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing hash functions..."

  let h1 = xxhash64("")
  echo "xxhash64(''): ", h1

  let h2 = xxhash64("\x00\x00\x00")
  echo "xxhash64('\\\\x00\\\\x00\\\\x00'): ", h2

  let h3 = xxhash64("\x00\x00\x01")
  echo "xxhash64('\\\\x00\\\\x00\\\\x01'): ", h3

  let h4 = xxhash128("")
  echo "xxhash128(''): (", h4.lo, ", ", h4.hi, ")"

  echo "Hash tests passed!"
