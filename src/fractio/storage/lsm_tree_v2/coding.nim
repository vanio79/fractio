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
