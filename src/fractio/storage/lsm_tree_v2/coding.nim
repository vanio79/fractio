# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Binary Encoding/Decoding
##
## Traits for serializing and deserializing types.

import std/[streams]
import error

# ============================================================================
# Encode Trait
# ============================================================================

type
  Encode*[T] = concept e
    ## Concept for encoding types
    e.encodeInto(Stream)

# ============================================================================
# Decode Trait
# ============================================================================

type
  Decode*[T] = concept d
    ## Concept for decoding types
    d.decodeFrom(Stream)

# ============================================================================
# Common Encodings
# ============================================================================

proc encodeVarint*(stream: Stream, value: uint64) =
  ## Encode unsigned 64-bit integer as varint
  var v = value
  while true:
    var b = (v and 0x7F).uint8
    v = v shr 7
    if v != 0:
      b = b or 0x80
    stream.write(b)
    if v == 0:
      break

proc encodeVarint*(value: uint64): string =
  ## Encode unsigned 64-bit integer as varint, return as string
  var v = value
  var result = ""
  while true:
    var b = (v and 0x7F).uint8
    v = v shr 7
    if v != 0:
      b = b or 0x80
    result.add(b.char)
    if v == 0:
      break
  result

proc decodeVarint*(stream: Stream): uint64 =
  ## Decode varint to unsigned 64-bit integer
  var result: uint64 = 0
  var shift = 0
  while true:
    let b = stream.readChar().uint8
    result = result or ((b and 0x7F) shl shift)
    if (b and 0x80) == 0:
      break
    inc shift
  result

proc encodeFixed32*(stream: Stream, value: uint32) =
  ## Encode 32-bit unsigned integer as 4 bytes
  for i in 0 ..< 4:
    stream.write(((value shr (i * 8)) and 0xFF).uint8)

proc encodeFixed32*(value: uint32): string =
  ## Encode 32-bit unsigned integer as 4 bytes, return as string
  var result = ""
  for i in 0 ..< 4:
    result.add(((value shr (i * 8)) and 0xFF).uint8.char)
  result

proc decodeFixed32*(stream: Stream): uint32 =
  ## Decode 32-bit unsigned integer
  var result: uint32 = 0
  for i in 0 ..< 4:
    result = result or (uint32(stream.readChar().uint8) shl (i * 8))
  result

proc decodeFixed32*(data: string, pos: int): uint32 =
  ## Decode 32-bit unsigned integer from string at position
  var result: uint32 = 0
  for i in 0 ..< 4:
    result = result or (uint32(data[pos + i].uint8) shl (i * 8))
  result

proc encodeFixed64*(stream: Stream, value: uint64) =
  ## Encode 64-bit unsigned integer as 8 bytes
  for i in 0 ..< 8:
    stream.write(((value shr (i * 8)) and 0xFF).uint8)

proc encodeFixed64*(value: uint64): string =
  ## Encode 64-bit unsigned integer as 8 bytes, return as string
  var result = ""
  for i in 0 ..< 8:
    result.add(((value shr (i * 8)) and 0xFF).uint8.char)
  result

proc decodeFixed64*(stream: Stream): uint64 =
  ## Decode 64-bit unsigned integer
  var result: uint64 = 0
  for i in 0 ..< 8:
    result = result or (uint64(stream.readChar().uint8) shl (i * 8))
  result

proc decodeFixed64*(data: string, pos: int): uint64 =
  ## Decode 64-bit unsigned integer from string at position
  var result: uint64 = 0
  for i in 0 ..< 8:
    result = result or (uint64(data[pos + i].uint8) shl (i * 8))
  result

# ============================================================================
# Varint Size Calculation
# ============================================================================

proc varintSize*(value: uint64 | uint32 | int): int =
  ## Calculate the number of bytes needed to encode a value as varint
  var v = uint64(value)
  if v < 0x80'u64:
    return 1
  elif v < 0x4000'u64:
    return 2
  elif v < 0x200000'u64:
    return 3
  elif v < 0x10000000'u64:
    return 4
  elif v < 0x80000000'u64:
    return 5
  elif v < 0x400000000'u64:
    return 6
  elif v < 0x2000000000'u64:
    return 7
  elif v < 0x10000000000'u64:
    return 8
  elif v < 0x80000000000'u64:
    return 9
  elif v < 0x400000000000'u64:
    return 10
  else:
    return 11

proc decodeVarintFromString*(data: string, pos: int): tuple[value: uint64, newPos: int] =
  ## Decode varint from string at given position
  var result: uint64 = 0
  var shift = 0
  var i = pos
  while i < data.len:
    let b = data[i].uint8
    result = result or ((b and 0x7F) shl shift)
    if (b and 0x80) == 0:
      break
    inc shift
    inc i
  return (result, i + 1)

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing coding..."

  # Test varint encoding
  let ms = newMemoryStream()
  ms.encodeVarint(300)
  ms.setPosition(0)
  let decoded = ms.decodeVarint()
  echo "Varint 300: ", decoded

  echo "Coding tests passed!"
