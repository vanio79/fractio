# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[error, keyspace/config]
import std/[streams, endians]

# Forward declarations
type
  CompressionPolicy* = object
    compressionTypes*: seq[CompressionType]

# Implementation of EncodeConfig for CompressionPolicy
proc encode*(policy: CompressionPolicy): string =
  var stream = newStringStream()

  # Write length (limited to 255 entries)
  let len = min(255, policy.compressionTypes.len)
  stream.write.uint8(uint8(len))

  # Write each compression type
  for i in 0..<len:
    # In a full implementation, this would encode the compression type
    # For now, we'll just write the enum value
    stream.write.uint8(uint8(policy.compressionTypes[i]))

  return stream.data

# Implementation of DecodeConfig for CompressionPolicy
proc decode*(bytes: string): StorageResult[CompressionPolicy] =
  var stream = newStringStream(bytes)

  # Read length
  let len = stream.read.uint8()

  # Read compression types
  var compressionTypes: seq[CompressionType] = @[]
  for i in 0..<len:
    let compressionTypeByte = stream.read.uint8()
    # In a full implementation, this would decode the compression type
    # For now, we'll just convert the byte to CompressionType
    let compressionType = case compressionTypeByte
      of 0: ctNone
      of 1: ctLz4
      of 2: ctSnappy
      else: ctNone # Default fallback

    compressionTypes.add(compressionType)

  return ok(CompressionPolicy(compressionTypes: compressionTypes))
