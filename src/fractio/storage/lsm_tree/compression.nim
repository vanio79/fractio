# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Compression Module
##
## Provides compression support for SSTable blocks using zippy (zlib/deflate).

import zippy
import std/streams

# ============================================================================
# Compression Types
# ============================================================================

type
  CompressionType* = enum
    ctNone = 0 ## No compression
    ctZlib = 1 ## Zlib/deflate compression
    ctLz4 = 2  ## LZ4 compression (not implemented, placeholder for future)

  CompressionError* = object of CatchableError

# ============================================================================
# Compression Functions
# ============================================================================

proc compress*(data: string, compressionType: CompressionType): string =
  ## Compress data using the specified compression type.
  ## Returns the original data if compression doesn't reduce size.
  case compressionType
  of ctNone:
    return data
  of ctZlib:
    let compressed = zippy.compress(data, level = BestSpeed,
        dataFormat = dfZlib)
    # Only use compressed data if it's actually smaller
    if compressed.len < data.len:
      return compressed
    else:
      return data
  of ctLz4:
    # Not implemented yet
    return data

proc decompress*(data: string, compressionType: CompressionType): string =
  ## Decompress data using the specified compression type.
  ## Returns the original data if no compression was used.
  case compressionType
  of ctNone:
    return data
  of ctZlib:
    try:
      return zippy.uncompress(data)
    except ZippyError as e:
      raise newException(CompressionError, "Failed to decompress: " & e.msg)
  of ctLz4:
    # Not implemented yet
    return data

proc compressBlock*(data: string, compressionType: CompressionType): tuple[
    data: string, compressed: bool] =
  ## Compress a block of data.
  ## Returns (compressed_data, was_compressed).
  if compressionType == ctNone:
    return (data: data, compressed: false)

  let compressed = zippy.compress(data, level = BestSpeed, dataFormat = dfZlib)

  # Only use compression if it saves space
  if compressed.len < data.len - 4: # Account for compression type byte + overhead
    return (data: compressed, compressed: true)
  else:
    return (data: data, compressed: false)

proc decompressBlock*(data: string, compressionType: CompressionType): string {.gcsafe.} =
  ## Decompress a block of data.
  ## The compression type indicates whether the data is compressed.
  if compressionType == ctNone:
    return data

  try:
    return zippy.uncompress(data)
  except ZippyError as e:
    raise newException(CompressionError, "Failed to decompress block: " & e.msg)

# ============================================================================
# Serialization Helpers
# ============================================================================

proc writeCompressionType*(stream: Stream, ct: CompressionType) =
  ## Write compression type to stream.
  stream.write(uint8(ct))

proc readCompressionType*(stream: Stream): CompressionType =
  ## Read compression type from stream.
  let b = stream.readUInt8()
  case b
  of 0: return ctNone
  of 1: return ctZlib
  of 2: return ctLz4
  else: return ctNone

# ============================================================================
# Statistics
# ============================================================================

type
  CompressionStats* = ref object
    bytesCompressed*: uint64
    bytesWritten*: uint64
    blocksCompressed*: uint64
    blocksUncompressed*: uint64

proc newCompressionStats*(): CompressionStats =
  CompressionStats(
    bytesCompressed: 0,
    bytesWritten: 0,
    blocksCompressed: 0,
    blocksUncompressed: 0
  )

proc compressionRatio*(stats: CompressionStats): float =
  ## Get the compression ratio (0.0 to 1.0, lower is better).
  if stats.bytesCompressed == 0:
    return 1.0
  return float(stats.bytesWritten) / float(stats.bytesCompressed)

proc recordCompression*(stats: CompressionStats, originalSize: int,
    compressedSize: int, wasCompressed: bool) =
  ## Record a compression operation.
  stats.bytesCompressed += uint64(originalSize)
  stats.bytesWritten += uint64(compressedSize)
  if wasCompressed:
    inc stats.blocksCompressed
  else:
    inc stats.blocksUncompressed
