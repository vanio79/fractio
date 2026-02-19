# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## SSTable Reader Implementation
## 
## Reads SSTable files for querying data.

import ./types
import ./blocks
import fractio/storage/error
import fractio/storage/types
import fractio/storage/lsm_tree/types
import std/[streams, os, strutils, endians, options, algorithm]

# SSTable reader
type
  SsTableReader* = ref object
    path*: string
    stream*: FileStream
    indexBlock*: IndexBlock
    footer*: SsTableFooter
    smallestKey*: string
    largestKey*: string
    seqnoRange*: (uint64, uint64)

# Block cache entry
type
  CachedBlock* = ref object
    data*: seq[byte]
    entries*: seq[BlockEntry]

# Read footer from SSTable file
proc readFooter(stream: Stream): StorageResult[SsTableFooter] =
  # Seek to end - footer size
  
  var footer = SsTableFooter()
  
  # Read index handle
  var offsetLe: uint64
  var sizeLe: uint32
  
  offsetLe = s.readUInt64()
  littleEndian64(addr footer.indexHandle.offset, addr offsetLe)
  
  sizeLe = s.readUInt32()
  littleEndian32(addr footer.indexHandle.size, addr sizeLe)
  
  # Read magic
  for i in 0 ..< 8:
    footer.magic[i] = s.readUInt8()
  
  # Verify magic
  if footer.magic != SSTABLE_MAGIC:
    return err[SsTableFooter, StorageError](StorageError(
      kind: seInvalidVersion, invalidVersion: none(FormatVersion)))
  
  # Read version
  var versionLe: uint32 = s.readUInt32()
  littleEndian32(addr footer.version, addr versionLe)
  
  # Read checksum
  footer.checksum = s.readUInt32()
  
  return ok[SsTableFooter, StorageError](footer)

# Read index block from file
proc readIndexBlock(stream: Stream, handle: BlockHandle): StorageResult[IndexBlock] =
  
  var indexBlock = newIndexBlock()
  
  # Read block type
  let blockType = BlockType(s.readUInt8())
  if blockType != btIndex:
    return err[IndexBlock, StorageError](StorageError(
      kind: seIo, ioError: "Expected index block"))
  
  let endPos = int(h.offset) + int(h.size)
  
  while stream.getPosition() < endPos:
    var entry = IndexEntry()
    
    # Read key length
    var keyLenLe: uint32 = s.readUInt32()
    littleEndian32(addr keyLenLe, addr keyLenLe)
    
    # Read key
    if keyLenLe > 0:
      entry.key = newString(int(keyLenLe))
      discard s.readData(addr entry.key[0], int(keyLenLe))
    
    # Read block handle
    var offsetLe: uint64 = s.readUInt64()
    littleEndian64(addr entry.h.offset, addr offsetLe)
    
    var sizeLe: uint32 = s.readUInt32()
    littleEndian32(addr entry.h.size, addr sizeLe)
    
    indexBlock.entries.add(entry)
  
  return ok[IndexBlock, StorageError](indexBlock)

# Read data block from file
proc readDataBlock(s: Stream, h: BlockHandle): StorageResult[DataBlock] =
  
  var dataBlock = newDataBlock()
  
  # Read block type
  let blockType = BlockType(s.readUInt8())
  if blockType != btData:
    return err[DataBlock, StorageError](StorageError(
      kind: seIo, ioError: "Expected data block"))
  
  let endPos = int(h.offset) + int(h.size)
  var prevKey = ""
  var entryIdx = 0
  
  # Read entries until we hit restart points
  while stream.getPosition() < endPos - 8:  # Leave room for restart points
    var entry = BlockEntry()
    
    # Read shared length
    var sharedLenLe: uint32 = s.readUInt32()
    littleEndian32(addr sharedLenLe, addr sharedLenLe)
    
    # Read unshared length
    var unsharedLenLe: uint32 = s.readUInt32()
    littleEndian32(addr unsharedLenLe, addr unsharedLenLe)
    
    # Read value length
    var valueLenLe: uint32 = s.readUInt32()
    littleEndian32(addr valueLenLe, addr valueLenLe)
    
    # Read seqno and type
    var seqnoType: uint64 = s.readUInt64()
    littleEndian64(addr seqnoType, addr seqnoType)
    entry.seqno = seqnoType shr 8
    entry.valueType = uint8(seqnoType and 0xFF)
    
    # Reconstruct key
    if sharedLenLe > 0:
      entry.key = prevKey[0 ..< int(sharedLenLe)]
    if unsharedLenLe > 0:
      var unshared = newString(int(unsharedLenLe))
      discard s.readData(addr unshared[0], int(unsharedLenLe))
      entry.key &= unshared
    
    # Read value
    if valueLenLe > 0:
      entry.value = newString(int(valueLenLe))
      discard s.readData(addr entry.value[0], int(valueLenLe))
    
    dataBlock.entries.add(entry)
    prevKey = entry.key
    entryIdx += 1
    
    # Safety check to prevent infinite loop
    if entryIdx > 100000:
      break
  
  return ok[DataBlock, StorageError](dataBlock)

# Open SSTable for reading
proc openSsTable*(path: string): StorageResult[SsTableReader] =
  let stream = newFileStream(path, fmRead)
  if stream == nil:
    return err[SsTableReader, StorageError](StorageError(
      kind: seIo, ioError: "Failed to open SSTable file: " & path))
  
  
  # Read footer
  let footerResult = readFooter(stream)
  if footerResult.isErr:
    stream.close()
    return err[SsTableReader, StorageError](footerResult.error)
  
  let footer = footerResult.value
  
  # Read index block
  let indexResult = readIndexBlock(stream, footer.indexHandle)
  if indexResult.isErr:
    stream.close()
    return err[SsTableReader, StorageError](indexResult.error)
  
  result = SsTableReader(
    path: path,
    stream: stream,
    indexBlock: indexResult.value,
    footer: footer
  )
  
  # Get key range from index
  if result.indexBlock.entries.len > 0:
    result.smallestKey = result.indexBlock.entries[0].key
    result.largestKey = result.indexBlock.entries[^1].key

# Close SSTable reader
proc close*(reader: SsTableReader) =
  if reader.stream != nil:
    reader.stream.close()
    reader.stream = nil

# Get value for key from SSTable
proc get*(reader: SsTableReader, key: string): Option[string] =
  # Binary search in index block to find the right data block
  var handle: BlockHandle
  
  # Find last index entry where entry.key <= key
  var found = false
  for entry in reader.indexBlock.entries:
    if entry.key <= key:
      handle = entry.handle
      found = true
    else:
      break
  
  if not found:
    return none(string)
  
  # Read data block
  let blockResult = readDataBlock(reader.stream, handle)
  if blockResult.isErr:
    return none(string)
  
  let dataBlock = blockResult.value
  
  # Binary search in data block
  var lo = 0
  var hi = dataBlock.entries.len - 1
  
  while lo <= hi:
    let mid = (lo + hi) div 2
    let entry = dataBlock.entries[mid]
    
    if entry.key == key:
      if entry.valueType == uint8(vtTombstone) or 
         entry.valueType == uint8(vtWeakTombstone):
        return none(string)
      return some(entry.value)
    elif entry.key < key:
      lo = mid + 1
    else:
      hi = mid - 1
  
  return none(string)

# Check if key might exist (uses index only)
proc mightContain*(reader: SsTableReader, key: string): bool =
  if reader.smallestKey.len == 0:
    return false
  
  # Check key range
  if key < reader.smallestKey or key > reader.largestKey:
    return false
  
  return true

# Get key range
proc getKeyRange*(reader: SsTableReader): (string, string) =
  (reader.smallestKey, reader.largestKey)

# Get number of data blocks
proc numDataBlocks*(reader: SsTableReader): int =
  reader.indexBlock.entries.len
