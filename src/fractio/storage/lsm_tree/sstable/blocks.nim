# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## SSTable Block Implementation
##
## Implements data blocks for SSTables with prefix compression.

import ./types
import fractio/storage/lsm_tree/types
import fractio/storage/error
import std/[streams, strutils, algorithm, endians]

# Default block size
const DEFAULT_BLOCK_SIZE* = 4096

# Restart interval - every N entries, we store a full key
const DEFAULT_RESTART_INTERVAL* = 16

# Create a new data block
proc newDataBlock*(): DataBlock {.gcsafe.} =
  DataBlock(
    entries: @[],
    size: 0,
    restartPoints: @[]
  )

# Create a new index block
proc newIndexBlock*(): IndexBlock {.gcsafe.} =
  IndexBlock(entries: @[])

# Calculate shared prefix length between two strings
proc sharedPrefixLen(a: string, b: string): int =
  let minLen = min(a.len, b.len)
  for i in 0 ..< minLen:
    if a[i] != b[i]:
      return i
  return minLen

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
