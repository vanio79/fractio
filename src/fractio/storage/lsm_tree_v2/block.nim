# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Block
##
## SSTable block structures with hash index, compression, and checksum.

import std/[streams, options]
import types
import error
import hash
import coding

# ============================================================================
# Block Type
# ============================================================================

type
  BlockKind* = enum
    bkData = 0
    bkIndex = 1
    bkFilter = 2
    bkMeta = 3

# ============================================================================
# Compression Type (extended)
# ============================================================================

type
  CompressionType* = enum
    ctNone = 0
    ctLz4 = 1

# ============================================================================
# Block Header
# ============================================================================

type
  BlockHeader* = ref object
    blockType*: BlockKind
    checksum*: uint64 ## 128-bit checksum stored as u64 (low bits)
    dataLength*: uint32
    uncompressedLength*: uint32

proc serializedLen*(h: BlockHeader): int = 20

# ============================================================================
# Block Decoder
# ============================================================================

type
  BlockDecoder* = ref object
    data*: string
    header*: BlockHeader

proc newBlockDecoder*(data: string): BlockDecoder =
  BlockDecoder(data: data, header: nil)

# ============================================================================
# Block Encoder
# ============================================================================

type
  BlockEncoder* = ref object
    buffer*: string
    blockType*: BlockKind
    compression*: CompressionType

proc newBlockEncoder*(blockType: BlockKind): BlockEncoder =
  BlockEncoder(buffer: "", blockType: blockType, compression: ctNone)

# ============================================================================
# Block Trailer
# ============================================================================

type
  BlockTrailer* = ref object
    compression*: CompressionType
    checksum*: uint32

const
  BlockTrailerSize* = 5
  TrailerStartMarker* = 0x02'u8

# ============================================================================
# Binary Index
# ============================================================================

type
  BinaryIndex* = ref object
    entries*: seq[tuple[key: types.Slice, offset: uint32]]

# ============================================================================
# Hash Index - matches Rust lsm-tree implementation
# ============================================================================

const
  HashIndexMarkerFree*: uint8 = 254     # MARKER_FREE = u8::MAX - 1
  HashIndexMarkerConflict*: uint8 = 255 # MARKER_CONFLICT = u8::MAX

type
  HashIndexBuilder* = ref object
    buckets*: seq[uint8]
    bucketCount*: uint32

  HashIndexReader* = ref object
    data*: string
    offset*: uint32
    len*: uint32

proc newHashIndexBuilder*(bucketCount: uint32): HashIndexBuilder =
  var buckets = newSeq[uint8](bucketCount.int)
  for i in 0 ..< buckets.len:
    buckets[i] = HashIndexMarkerFree
  HashIndexBuilder(
    buckets: buckets,
    bucketCount: bucketCount
  )

proc calculateBucketPosition*(key: string, bucketCount: uint32): int =
  ## Calculate bucket position for a key using hash
  let h = xxhash64(key)
  int(h mod uint64(bucketCount))

proc set*(b: HashIndexBuilder, key: string, binaryIndexPos: uint8): bool =
  ## Map a key to binary index position. Returns true if set successfully.
  if b.bucketCount == 0:
    return false

  let pos = calculateBucketPosition(key, b.bucketCount)
  let curr = b.buckets[pos]

  if curr == HashIndexMarkerConflict:
    return false
  elif curr == HashIndexMarkerFree:
    b.buckets[pos] = binaryIndexPos
    return true
  elif curr == binaryIndexPos:
    return true
  else:
    b.buckets[pos] = HashIndexMarkerConflict
    return false

proc intoInner*(b: HashIndexBuilder): string =
  ## Convert builder to raw bytes
  result = newString(b.buckets.len)
  for i in 0 ..< b.buckets.len:
    result[i] = chr(b.buckets[i])

proc newHashIndexReader*(data: string, offset, len: uint32): HashIndexReader =
  HashIndexReader(data: data, offset: offset, len: len)

proc bucketCount*(r: HashIndexReader): int = int(r.len)

proc get*(r: HashIndexReader, key: string): uint8 =
  ## Get binary index position for key, or marker if not found/conflicted
  if r.len == 0:
    return HashIndexMarkerFree

  let pos = calculateBucketPosition(key, r.len)
  if pos < r.data.len:
    return uint8(r.data[r.offset.int + pos])
  return HashIndexMarkerFree

proc conflictCount*(r: HashIndexReader): int =
  ## Count number of conflicts in hash index
  var count = 0
  for i in 0 ..< int(r.len):
    if uint8(r.data[r.offset.int + i]) == HashIndexMarkerConflict:
      inc count
  return count

# ============================================================================
# Block Offset
# ============================================================================

type
  BlockOffset* = uint64

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing block structures..."

  # Test hash index builder
  var hashIdx = newHashIndexBuilder(100)
  echo "Hash index with 100 buckets created"

  discard hashIdx.set("a", 5)
  discard hashIdx.set("b", 8)
  discard hashIdx.set("c", 10)

  let hashBytes = hashIdx.intoInner()
  echo "Hash index bytes: ", hashBytes.len

  # Test hash index reader
  var reader = newHashIndexReader(hashBytes, 0, 100)
  echo "Reader bucket count: ", reader.bucketCount()
  echo "Conflict count: ", reader.conflictCount()
  echo "Get 'a': ", reader.get("a")
  echo "Get 'b': ", reader.get("b")
  echo "Get 'd': ", reader.get("d")

  echo "Block tests passed!"
