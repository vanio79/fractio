# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License

## Memory Arena for LSM Tree keys
##
## Allocates a large chunk of memory upfront and provides fast bump allocation.
## This eliminates per-string GC tracking overhead and improves cache locality.
##
## Usage:
##   var arena = newKeyArena(1024 * 1024)  # 1MB arena
##   let key = arena.copyKey("mykey")      # Copies into arena, returns borrowed slice

import std/algorithm

type
  KeyArena* = ref object
    ## A memory arena for storing keys
    ## All keys are stored in a single contiguous buffer
    ## Keys borrowed from the arena are just ptr + len (zero-copy)
    buffer*: seq[byte]
    pos*: int # Current allocation position

  KeySlice* = object
    ## A borrowed slice into a KeyArena
    ## Zero-copy, just a pointer and length
    data*: ptr UncheckedArray[byte]
    len*: int

proc newKeyArena*(initialSize: int = 1024 * 1024): KeyArena =
  ## Create a new arena with the given initial size
  result = KeyArena()
  result.buffer = newSeq[byte](initialSize)
  result.pos = 0

proc grow(arena: var KeyArena, needed: int) =
  ## Grow the arena buffer if needed
  let newSize = max(arena.buffer.len * 2, arena.pos + needed)
  var newBuffer = newSeq[byte](newSize)
  if arena.pos > 0:
    copyMem(newBuffer[0].addr, arena.buffer[0].unsafeAddr, arena.pos)
  arena.buffer = newBuffer

proc copyKey*(arena: var KeyArena, key: string): KeySlice {.inline.} =
  ## Copy a string key into the arena, return a borrowed slice
  ## This is fast - just a bump allocation
  let keyLen = key.len

  # Grow if needed
  if arena.pos + keyLen > arena.buffer.len:
    arena.grow(keyLen)

  # Copy key data
  if keyLen > 0:
    copyMem(arena.buffer[arena.pos].addr, key[0].unsafeAddr, keyLen)

  # Create result slice pointing into arena
  result = KeySlice(
    data: cast[ptr UncheckedArray[byte]](arena.buffer[arena.pos].unsafeAddr),
    len: keyLen
  )

  # Bump position
  arena.pos += keyLen

proc copyKey*(arena: var KeyArena, key: KeySlice): KeySlice {.inline.} =
  ## Copy a KeySlice into the arena
  if key.len == 0:
    result = KeySlice(data: nil, len: 0)
    return

  # Grow if needed
  if arena.pos + key.len > arena.buffer.len:
    arena.grow(key.len)

  # Copy key data
  copyMem(arena.buffer[arena.pos].addr, key.data, key.len)

  # Create result slice
  result = KeySlice(
    data: cast[ptr UncheckedArray[byte]](arena.buffer[arena.pos].unsafeAddr),
    len: key.len
  )

  arena.pos += key.len

# ============================================================================
# KeySlice Comparison - uses memcmp
# ============================================================================

proc memcmp(a, b: pointer, len: csize_t): cint {.importc, header: "<string.h>".}

proc cmp*(a, b: KeySlice): int {.inline.} =
  let minLen = min(a.len, b.len)
  if minLen > 0:
    let cmpResult = memcmp(a.data, b.data, csize_t(minLen))
    if cmpResult != 0:
      return cmpResult.int
  result = a.len - b.len

proc `==`*(a, b: KeySlice): bool {.inline.} =
  if a.len != b.len:
    return false
  if a.len == 0:
    return true
  result = memcmp(a.data, b.data, csize_t(a.len)) == 0

proc `<`*(a, b: KeySlice): bool {.inline.} =
  let minLen = min(a.len, b.len)
  if minLen > 0:
    let cmpResult = memcmp(a.data, b.data, csize_t(minLen))
    if cmpResult < 0:
      return true
    if cmpResult > 0:
      return false
  result = a.len < b.len

proc `<=`*(a, b: KeySlice): bool {.inline.} =
  a == b or a < b

# KeySlice vs string comparison (for lookups)
proc `==`*(a: KeySlice, b: string): bool {.inline.} =
  if a.len != b.len:
    return false
  if a.len == 0:
    return true
  result = memcmp(a.data, b[0].unsafeAddr, csize_t(a.len)) == 0

proc `<`*(a: KeySlice, b: string): bool {.inline.} =
  let minLen = min(a.len, b.len)
  if minLen > 0:
    let cmpResult = memcmp(a.data, b[0].unsafeAddr, csize_t(minLen))
    if cmpResult < 0:
      return true
    if cmpResult > 0:
      return false
  result = a.len < b.len

proc `==`*(a: string, b: KeySlice): bool {.inline.} = b == a
proc `<`*(a: string, b: KeySlice): bool {.inline.} = b < a

# ============================================================================
# KeySlice Utilities
# ============================================================================

proc `[]`*(s: KeySlice, i: int): byte {.inline.} =
  ## Index into the slice
  s.data[i]

proc charAt*(s: KeySlice, i: int): char {.inline.} =
  ## Get character at index (for string interop)
  cast[char](s.data[i])

proc slice*(s: KeySlice, start: int, endex: int): KeySlice {.inline.} =
  ## Get a slice of the KeySlice [start, endex)
  let sliceLen = endex - start
  if sliceLen > 0:
    result = KeySlice(
      data: cast[ptr UncheckedArray[byte]](s.data[start].addr),
      len: sliceLen
    )
  else:
    result = KeySlice(data: nil, len: 0)

proc sliceToString*(s: KeySlice, start: int, endex: int): string {.inline.} =
  ## Get a slice as a string (copies data)
  let sliceLen = endex - start
  result = newString(sliceLen)
  if sliceLen > 0:
    copyMem(result[0].addr, s.data[start].addr, sliceLen)

import std/hashes

proc toString*(s: KeySlice): string {.inline.} =
  result = newString(s.len)
  if s.len > 0:
    copyMem(result[0].addr, s.data, s.len)

proc toKeySlice*(s: string): KeySlice {.inline.} =
  ## Create a borrowed KeySlice from a string
  ## WARNING: The string must outlive the KeySlice!
  if s.len > 0:
    result = KeySlice(
      data: cast[ptr UncheckedArray[byte]](s[0].unsafeAddr),
      len: s.len
    )
  else:
    result = KeySlice(data: nil, len: 0)

proc hash*(s: KeySlice): Hash =
  var h: Hash = 0
  for i in 0 ..< s.len:
    h = h !& int(s.data[i])
  result = !$h

proc usedBytes*(arena: KeyArena): int {.inline.} =
  arena.pos

proc capacity*(arena: KeyArena): int {.inline.} =
  arena.buffer.len
