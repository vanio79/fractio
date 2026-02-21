# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## SSTable Block Implementation
##
## Implements data blocks for SSTables with prefix compression.
## Optional hash index for fast point lookups.

import ./types
import fractio/storage/lsm_tree/types
import fractio/storage/error
import std/[streams, strutils, algorithm, endians, hashes]

# Default block size
const DEFAULT_BLOCK_SIZE* = 4096

# Restart interval - every N entries, we store a full key
const DEFAULT_RESTART_INTERVAL* = 16

# Default hash index utilization ratio
const DEFAULT_HASH_UTIL_RATIO* = 0.75

# Maximum number of restart intervals supported by hash index
const MAX_HASH_RESTARTS* = 253

# Create a new data block
proc newDataBlock*(): DataBlock {.gcsafe.} =
  DataBlock(
    entries: @[],
    size: 0,
    restartPoints: @[],
    hashIndex: nil
  )

# Create a new index block
proc newIndexBlock*(): IndexBlock {.gcsafe.} =
  IndexBlock(entries: @[])

# Create a new block hash index
proc newBlockHashIndex*(numBuckets: uint32): BlockHashIndex =
  result = BlockHashIndex(
    buckets: newSeq[uint8](numBuckets),
    numBuckets: numBuckets
  )
  # Initialize all buckets as empty
  for i in 0..<numBuckets:
    result.buckets[i] = HASH_INDEX_EMPTY

# Calculate shared prefix length between two strings
proc sharedPrefixLen(a: string, b: string): int =
  let minLen = min(a.len, b.len)
  for i in 0 ..< minLen:
    if a[i] != b[i]:
      return i
  return minLen

# Simple hash function for block hash index
proc hashKey*(key: string): uint32 =
  ## Simple hash function for keys.
  ## Uses FNV-1a variant for good distribution.
  var hash: uint32 = 2166136261'u32
  for c in key:
    hash = hash xor uint32(c)
    hash = hash * 16777619'u32
  return hash

# Build hash index from entries
proc buildHashIndex*(dataBlock: DataBlock,
    utilRatio: float = DEFAULT_HASH_UTIL_RATIO) =
  ## Builds a hash index for the data block.
  ##
  ## Parameters:
  ##   utilRatio: Target utilization ratio (entries/buckets), default 0.75
  ##              Lower ratio = less collisions, more space overhead

  # Check if we can use hash index (max 253 restart intervals)
  let numRestarts = dataBlock.restartPoints.len
  if numRestarts > MAX_HASH_RESTARTS or numRestarts == 0:
    dataBlock.hashIndex = nil
    return

  # Calculate number of buckets
  let numBuckets = uint32(float(numRestarts) / utilRatio + 0.5)
  if numBuckets == 0:
    dataBlock.hashIndex = nil
    return

  dataBlock.hashIndex = newBlockHashIndex(numBuckets)

  # Build hash index: for each entry, compute hash and store restart interval index
  for i, entry in dataBlock.entries:
    # Find which restart interval this entry belongs to
    let restartIdx = i div DEFAULT_RESTART_INTERVAL
    let hash = hashKey(entry.key)
    let bucketIdx = hash mod dataBlock.hashIndex.numBuckets
    let bucket = addr dataBlock.hashIndex.buckets[bucketIdx]

    if bucket[] == HASH_INDEX_EMPTY:
      # Empty bucket - store restart index
      bucket[] = uint8(restartIdx)
    elif bucket[] != uint8(restartIdx):
      # Collision - different restart interval, mark as collision
      bucket[] = HASH_INDEX_COLLISION

# Add entry to data block with prefix compression
proc add*(dataBlock: DataBlock, key: string, value: string,
          seqno: uint64, valueType: ValueType): bool =
  # Check if block is full
  let entrySize = 4 + key.len + 4 + value.len + 8 + 1 # Rough estimate
  if dataBlock.size > 0 and dataBlock.size + uint32(entrySize) > DEFAULT_BLOCK_SIZE:
    return false

  # Add restart point every RESTART_INTERVAL entries
  if dataBlock.entries.len mod DEFAULT_RESTART_INTERVAL == 0:
    dataBlock.restartPoints.add(dataBlock.size)

  dataBlock.entries.add(BlockEntry(
    key: key,
    value: value,
    seqno: seqno,
    valueType: uint8(valueType)
  ))

  dataBlock.size += uint32(entrySize)
  return true

# Add entry to index block
proc add*(indexBlock: IndexBlock, key: string, handle: BlockHandle) =
  indexBlock.entries.add(IndexEntry(key: key, handle: handle))

# Serialize data block to stream
proc serialize*(dataBlock: DataBlock, stream: Stream): StorageResult[void] =
  var prevKey = ""

  for i, entry in dataBlock.entries:
    # Calculate shared prefix
    let sharedLen = if i mod DEFAULT_RESTART_INTERVAL == 0:
      0
    else:
      sharedPrefixLen(prevKey, entry.key)

    let unsharedLen = entry.key.len - sharedLen

    # Write shared length (varint - simplified to u32)
    var sharedLenLe: uint32 = uint32(sharedLen)
    littleEndian32(addr sharedLenLe, addr sharedLenLe)
    stream.write(sharedLenLe)

    # Write unshared length (varint - simplified to u32)
    var unsharedLenLe: uint32 = uint32(unsharedLen)
    littleEndian32(addr unsharedLenLe, addr unsharedLenLe)
    stream.write(unsharedLenLe)

    # Write value length
    var valueLenLe: uint32 = uint32(entry.value.len)
    littleEndian32(addr valueLenLe, addr valueLenLe)
    stream.write(valueLenLe)

    # Write sequence number and value type (combined)
    var seqnoType = entry.seqno shl 8 or uint64(entry.valueType)
    littleEndian64(addr seqnoType, addr seqnoType)
    stream.write(seqnoType)

    # Write unshared key bytes
    if unsharedLen > 0:
      stream.write(entry.key[sharedLen ..< entry.key.len])

    # Write value
    if entry.value.len > 0:
      stream.write(entry.value)

    prevKey = entry.key

  # Write restart points (BEFORE numRestarts)
  # Format: entries | restart_points | numRestarts
  for rp in dataBlock.restartPoints:
    var rpLe = rp
    littleEndian32(addr rpLe, addr rpLe)
    stream.write(rpLe)

  # Write numRestarts at the very end
  var numRestartsLe: uint32 = uint32(dataBlock.restartPoints.len)
  littleEndian32(addr numRestartsLe, addr numRestartsLe)
  stream.write(numRestartsLe)

  return okVoid

# Serialize data block with hash index to stream
proc serializeWithHashIndex*(dataBlock: DataBlock, stream: Stream,
                            utilRatio: float = DEFAULT_HASH_UTIL_RATIO): StorageResult[void] =
  ## Serializes data block with optional hash index.
  ## Format: entries | restart_points | numRestarts | [hash_index | hash_buckets | numBuckets]

  # First serialize without hash index
  let baseResult = dataBlock.serialize(stream)
  if baseResult.isErr:
    return baseResult

  # Build hash index
  dataBlock.buildHashIndex(utilRatio)

  # Write hash index if available
  if dataBlock.hashIndex != nil:
    # Write hash index flag (1 = has hash index)
    stream.write(uint8(1))

    # Write hash buckets
    for bucket in dataBlock.hashIndex.buckets:
      stream.write(bucket)

    # Write number of buckets
    var numBucketsLe = dataBlock.hashIndex.numBuckets
    littleEndian32(addr numBucketsLe, addr numBucketsLe)
    stream.write(numBucketsLe)
  else:
    # No hash index (0 = no hash index)
    stream.write(uint8(0))

  return okVoid

# Serialize index block to stream
proc serialize*(indexBlock: IndexBlock, stream: Stream): StorageResult[void] =
  for entry in indexBlock.entries:
    # Write key length
    var keyLenLe: uint32 = uint32(entry.key.len)
    littleEndian32(addr keyLenLe, addr keyLenLe)
    stream.write(keyLenLe)

    # Write key
    stream.write(entry.key)

    # Write block handle (offset + size + uncompressedSize)
    var offsetLe = entry.handle.offset
    littleEndian64(addr offsetLe, addr offsetLe)
    stream.write(offsetLe)

    var sizeLe = entry.handle.size
    littleEndian32(addr sizeLe, addr sizeLe)
    stream.write(sizeLe)

    var uncompressedSizeLe = entry.handle.uncompressedSize
    littleEndian32(addr uncompressedSizeLe, addr uncompressedSizeLe)
    stream.write(uncompressedSizeLe)

  return okVoid

# Get number of entries
proc len*(dataBlock: DataBlock): int =
  dataBlock.entries.len

# Check if block is empty
proc isEmpty*(dataBlock: DataBlock): bool =
  dataBlock.entries.len == 0

# Get all entries (for iteration)
proc getEntries*(dataBlock: DataBlock): seq[BlockEntry] =
  dataBlock.entries

# Find entry using hash index (returns restart interval index or -1)
proc findWithHashIndex*(dataBlock: DataBlock, key: string): int =
  ## Uses hash index to find the restart interval that may contain the key.
  ## Returns -1 if key definitely not in block (hash miss on empty bucket).
  ## Returns restart interval index to search.

  if dataBlock.hashIndex == nil:
    return 0 # Fall back to searching from start

  let hash = hashKey(key)
  let bucketIdx = hash mod dataBlock.hashIndex.numBuckets
  let bucket = dataBlock.hashIndex.buckets[bucketIdx]

  if bucket == HASH_INDEX_EMPTY:
    return -1 # Key definitely not in block
  elif bucket == HASH_INDEX_COLLISION:
    return 0 # Collision - need full search
  else:
    return int(bucket) # Return restart interval index
