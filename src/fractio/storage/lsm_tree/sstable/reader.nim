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
import fractio/storage/lsm_tree/block_cache
import fractio/storage/lsm_tree/bloom_filter
import fractio/storage/lsm_tree/compression
import std/[streams, os, strutils, endians, options]

type
  SsTableReader* = ref object
    path*: string
    stream*: FileStream
    indexBlock*: IndexBlock
    topLevelIndex*: TopLevelIndex # For partitioned index
    footer*: SsTableFooter
    bloomFilter*: BloomFilter     # Bloom filter for quick rejection
    smallestKey*: string
    largestKey*: string
    seqnoRange*: (uint64, uint64)
    sstableId*: uint64            # ID of this SSTable
    blockCache*: BlockCache       # Optional block cache (may be nil)
    indexMode*: IndexMode         # Full or partitioned index mode

proc readFooter(strm: Stream, fileSize: int64): StorageResult[SsTableFooter] =
  ## Read the SSTable footer.
  ## Supports v1 (32 bytes), v2 (44 bytes), and v3 (53 bytes) footer formats.

  # First, read the last 4 bytes to get footer size
  strm.setPosition(int(fileSize - 4))
  var footerSizeLe: uint32 = strm.readUInt32()
  var footerSize: uint32
  littleEndian32(addr footerSize, addr footerSizeLe)

  var footer = SsTableFooter()

  if footerSize == 53:
    # Version 3 footer (includes index mode)
    strm.setPosition(int(fileSize - 53))

    var offsetLe: uint64 = strm.readUInt64()
    littleEndian64(addr footer.indexHandle.offset, addr offsetLe)

    var sizeLe: uint32 = strm.readUInt32()
    littleEndian32(addr footer.indexHandle.size, addr sizeLe)

    var uncompressedLe: uint32 = strm.readUInt32()
    littleEndian32(addr footer.indexHandle.uncompressedSize,
        addr uncompressedLe)

    offsetLe = strm.readUInt64()
    littleEndian64(addr footer.filterHandle.offset, addr offsetLe)

    sizeLe = strm.readUInt32()
    littleEndian32(addr footer.filterHandle.size, addr sizeLe)

    uncompressedLe = strm.readUInt32()
    littleEndian32(addr footer.filterHandle.uncompressedSize,
        addr uncompressedLe)

    for i in 0 ..< 8:
      footer.magic[i] = strm.readUInt8()

    var versionLe: uint32 = strm.readUInt32()
    littleEndian32(addr footer.version, addr versionLe)

    # Read index mode
    footer.indexMode = IndexMode(strm.readUInt8())

    footer.checksum = strm.readUInt32()

  elif footerSize == 44:
    # Version 2 footer (includes bloom filter)
    strm.setPosition(int(fileSize - 44))

    var offsetLe: uint64 = strm.readUInt64()
    littleEndian64(addr footer.indexHandle.offset, addr offsetLe)

    var sizeLe: uint32 = strm.readUInt32()
    littleEndian32(addr footer.indexHandle.size, addr sizeLe)

    offsetLe = strm.readUInt64()
    littleEndian64(addr footer.filterHandle.offset, addr offsetLe)

    sizeLe = strm.readUInt32()
    littleEndian32(addr footer.filterHandle.size, addr sizeLe)

    for i in 0 ..< 8:
      footer.magic[i] = strm.readUInt8()

    var versionLe: uint32 = strm.readUInt32()
    littleEndian32(addr footer.version, addr versionLe)

    footer.checksum = strm.readUInt32()
    footer.indexMode = imFull # v2 always uses full index

  elif footerSize == 32:
    # Version 1 footer (no bloom filter)
    strm.setPosition(int(fileSize - 32))

    var offsetLe: uint64 = strm.readUInt64()
    littleEndian64(addr footer.indexHandle.offset, addr offsetLe)

    var sizeLe: uint32 = strm.readUInt32()
    littleEndian32(addr footer.indexHandle.size, addr sizeLe)

    for i in 0 ..< 8:
      footer.magic[i] = strm.readUInt8()

    var versionLe: uint32 = strm.readUInt32()
    littleEndian32(addr footer.version, addr versionLe)

    footer.checksum = strm.readUInt32()

    # No filter in v1
    footer.filterHandle = BlockHandle(offset: 0, size: 0)
    footer.indexMode = imFull # v1 always uses full index
  else:
    return err[SsTableFooter, StorageError](StorageError(
      kind: seInvalidVersion, invalidVersion: none(FormatVersion)))

  if footer.magic != SSTABLE_MAGIC:
    return err[SsTableFooter, StorageError](StorageError(
      kind: seInvalidVersion, invalidVersion: none(FormatVersion)))

  return ok[SsTableFooter, StorageError](footer)

proc readIndexBlock(strm: Stream, hdl: BlockHandle): StorageResult[IndexBlock] =
  # Position at start of index block data (AFTER block type byte)
  strm.setPosition(int(hdl.offset))

  var idxBlock = newIndexBlock()

  let endPos = int(hdl.offset) + int(hdl.size)

  while strm.getPosition() + 20 <= endPos: # Min entry size: 4 + key + 8 + 4 + 4
    var entry = IndexEntry()

    var keyLenLe: uint32 = strm.readUInt32()
    var keyLen: uint32
    littleEndian32(addr keyLen, addr keyLenLe)

    if keyLen > 0 and keyLen < 10000: # Sanity check
      entry.key = newString(int(keyLen))
      discard strm.readData(addr entry.key[0], int(keyLen))
    else:
      break

    var offsetLe: uint64 = strm.readUInt64()
    littleEndian64(addr entry.handle.offset, addr offsetLe)

    var sizeLe: uint32 = strm.readUInt32()
    littleEndian32(addr entry.handle.size, addr sizeLe)

    # Read uncompressed size (may be 0 for v1 format or uncompressed blocks)
    if strm.getPosition() + 4 <= endPos:
      var uncompressedSizeLe: uint32 = strm.readUInt32()
      littleEndian32(addr entry.handle.uncompressedSize,
          addr uncompressedSizeLe)
    else:
      entry.handle.uncompressedSize = 0

    idxBlock.entries.add(entry)

  return ok[IndexBlock, StorageError](idxBlock)

# Forward declaration
proc parseDataBlock*(data: string): StorageResult[DataBlock]

proc readDataBlock*(strm: Stream, hdl: BlockHandle): StorageResult[DataBlock] =
  ## Read a data block from the stream.
  ## Handles both compressed and uncompressed blocks.
  ## The handle.offset points AFTER the compression flag byte.

  # Use cast(gcsafe) to allow memory allocation
  # ORC handles thread-local GC correctly
  {.cast(gcsafe).}:
    # Determine if compressed based on uncompressedSize > 0
    let isCompressed = hdl.uncompressedSize > 0

    var blockData: string

    if isCompressed:
      # Read compressed data
      let compressedSize = int(hdl.size)
      blockData = newString(compressedSize)
      strm.setPosition(int(hdl.offset))
      discard strm.readData(addr blockData[0], compressedSize)

      # Decompress
      try:
        blockData = decompressBlock(blockData, ctZlib)
      except CompressionError as e:
        return err[DataBlock, StorageError](StorageError(
          kind: seIo, ioError: "Failed to decompress block: " & e.msg))
    else:
      # Read uncompressed data directly
      let dataSize = int(hdl.size)
      blockData = newString(dataSize)
      strm.setPosition(int(hdl.offset))
      discard strm.readData(addr blockData[0], dataSize)

    # Parse the block data
    return parseDataBlock(blockData)

proc parseDataBlockImpl(data: string): DataBlock =
  ## Internal implementation that allocates memory.
  result = DataBlock(entries: @[], size: 0, restartPoints: @[])

  if data.len < 4:
    return

  # Read numRestarts from the last 4 bytes
  var numRestartsLe: uint32
  copyMem(addr numRestartsLe, addr data[data.len - 4], 4)
  var numRestarts: uint32
  littleEndian32(addr numRestarts, addr numRestartsLe)

  # Calculate where entries end (before restart points)
  let entriesEndPos = data.len - 4 - int(numRestarts * 4)

  if entriesEndPos < 0:
    return

  var pos = 0
  var prevKey = ""
  var entryCount = 0

  while pos < entriesEndPos:
    # Check if we have enough bytes for a minimal entry
    if pos + 20 > entriesEndPos:
      break

    var entry = BlockEntry()

    var sharedLenLe: uint32
    copyMem(addr sharedLenLe, addr data[pos], 4)
    pos += 4
    var sharedLen: uint32
    littleEndian32(addr sharedLen, addr sharedLenLe)

    var unsharedLenLe: uint32
    copyMem(addr unsharedLenLe, addr data[pos], 4)
    pos += 4
    var unsharedLen: uint32
    littleEndian32(addr unsharedLen, addr unsharedLenLe)

    var valueLenLe: uint32
    copyMem(addr valueLenLe, addr data[pos], 4)
    pos += 4
    var valueLen: uint32
    littleEndian32(addr valueLen, addr valueLenLe)

    var seqnoTypeLe: uint64
    copyMem(addr seqnoTypeLe, addr data[pos], 8)
    pos += 8
    var seqnoType: uint64
    littleEndian64(addr seqnoType, addr seqnoTypeLe)
    entry.seqno = seqnoType shr 8
    entry.valueType = uint8(seqnoType and 0xFF)

    if sharedLen > 0:
      entry.key = prevKey[0 ..< int(sharedLen)]
    if unsharedLen > 0:
      var unshared = newString(int(unsharedLen))
      copyMem(addr unshared[0], addr data[pos], int(unsharedLen))
      pos += int(unsharedLen)
      entry.key &= unshared

    if valueLen > 0:
      entry.value = newString(int(valueLen))
      copyMem(addr entry.value[0], addr data[pos], int(valueLen))
      pos += int(valueLen)

    result.entries.add(entry)
    prevKey = entry.key
    entryCount += 1

    if entryCount > 100000:
      break

proc parseDataBlock*(data: string): StorageResult[DataBlock] =
  ## Parse a DataBlock from raw bytes.
  ## Used by the block cache to avoid re-reading from disk.
  {.cast(gcsafe).}:
    var dataBlk = parseDataBlockImpl(data)
    return ok[DataBlock, StorageError](dataBlk)

proc readRawBlock*(strm: Stream, hdl: BlockHandle): StorageResult[string] =
  ## Read raw block data from disk (for caching).
  ## The handle.offset points AFTER the block type byte, so we read directly.
  strm.setPosition(int(hdl.offset))
  let size = int(hdl.size)
  var data = newString(size)
  if size > 0:
    discard strm.readData(addr data[0], size)
  return ok[string, StorageError](data)

proc readDataBlockWithCache*(reader: SsTableReader,
    hdl: BlockHandle): StorageResult[DataBlock] =
  ## Read a data block, using the block cache if available.
  let isCompressed = hdl.uncompressedSize > 0

  if reader.blockCache != nil:
    let cacheKey = BlockKey(sstableId: reader.sstableId,
        blockOffset: hdl.offset)
    let cached = reader.blockCache.get(cacheKey)
    if cached.isSome:
      # Cache hit - decompress if needed, then parse
      var dataToParse = cached.get
      if isCompressed:
        try:
          dataToParse = decompressBlock(dataToParse, ctZlib)
        except CompressionError as e:
          return err[DataBlock, StorageError](StorageError(
            kind: seIo, ioError: "Failed to decompress cached block: " & e.msg))
      return parseDataBlock(dataToParse)

  # Cache miss - read from disk
  let rawResult = readRawBlock(reader.stream, hdl)
  if rawResult.isErr:
    return err[DataBlock, StorageError](rawResult.error)

  let rawData = rawResult.value

  # Store in cache (store compressed data)
  if reader.blockCache != nil:
    let cacheKey = BlockKey(sstableId: reader.sstableId,
        blockOffset: hdl.offset)
    reader.blockCache.put(cacheKey, rawData)

  # Decompress if needed, then parse
  var dataToParse = rawData
  if isCompressed:
    try:
      dataToParse = decompressBlock(dataToParse, ctZlib)
    except CompressionError as e:
      return err[DataBlock, StorageError](StorageError(
        kind: seIo, ioError: "Failed to decompress block: " & e.msg))

  # Parse the data
  return parseDataBlock(dataToParse)

proc readBloomFilter*(strm: Stream, hdl: BlockHandle): StorageResult[BloomFilter] =
  ## Read a bloom filter from disk.
  ## The handle.offset points AFTER the block type byte (consistent with other blocks).
  if hdl.size == 0:
    return ok[BloomFilter, StorageError](nil)

  strm.setPosition(int(hdl.offset))

  # Read the bloom filter (handle already points after block type byte)
  try:
    result = ok[BloomFilter, StorageError](deserializeBloomFilter(strm))
  except IOError:
    return err[BloomFilter, StorageError](StorageError(
      kind: seIo, ioError: "Failed to read bloom filter"))

proc readTopLevelIndex*(strm: Stream, hdl: BlockHandle): StorageResult[
    TopLevelIndex] =
  ## Read a top level index from disk.
  ## The handle.offset points AFTER the block type byte.
  var tli = TopLevelIndex(entries: @[], isPartitioned: true)

  strm.setPosition(int(hdl.offset))
  let endPos = int(hdl.offset) + int(hdl.size)

  # Read number of entries
  if strm.getPosition() + 4 > endPos:
    return ok[TopLevelIndex, StorageError](tli)

  var numEntriesLe: uint32 = strm.readUInt32()
  var numEntries: uint32
  littleEndian32(addr numEntries, addr numEntriesLe)

  for i in 0 ..< int(numEntries):
    if strm.getPosition() + 4 > endPos:
      break

    var entry = TopLevelIndexEntry()

    # Read key length
    var keyLenLe: uint32 = strm.readUInt32()
    var keyLen: uint32
    littleEndian32(addr keyLen, addr keyLenLe)

    if keyLen > 0 and keyLen < 10000:
      entry.key = newString(int(keyLen))
      discard strm.readData(addr entry.key[0], int(keyLen))

    # Read handle
    var offsetLe: uint64 = strm.readUInt64()
    littleEndian64(addr entry.handle.offset, addr offsetLe)

    var sizeLe: uint32 = strm.readUInt32()
    littleEndian32(addr entry.handle.size, addr sizeLe)

    var uncompressedLe: uint32 = strm.readUInt32()
    littleEndian32(addr entry.handle.uncompressedSize, addr uncompressedLe)

    tli.entries.add(entry)

  return ok[TopLevelIndex, StorageError](tli)

proc findIndexBlockForPartitioned*(reader: SsTableReader,
    key: string): StorageResult[IndexBlock] =
  ## Find and read the index block that may contain the key.
  ## Used for partitioned index mode.
  if reader.topLevelIndex.entries.len == 0:
    return ok[IndexBlock, StorageError](newIndexBlock())

  # Find the TLI entry that may contain the key
  var targetHandle: BlockHandle
  var found = false

  for entry in reader.topLevelIndex.entries:
    if entry.key >= key:
      targetHandle = entry.handle
      found = true
      break
    targetHandle = entry.handle
    found = true

  if not found:
    return ok[IndexBlock, StorageError](newIndexBlock())

  # Read the index block
  return readIndexBlock(reader.stream, targetHandle)

proc openSsTable*(path: string, sstableId: uint64 = 0,
                  cache: BlockCache = nil): StorageResult[SsTableReader] =
  ## Open an SSTable file for reading.
  ##
  ## Parameters:
  ##   path: Path to the SSTable file
  ##   sstableId: ID of the SSTable (used for cache key)
  ##   cache: Optional block cache for caching data blocks
  let strm = newFileStream(path, fmRead)
  if strm == nil:
    return err[SsTableReader, StorageError](StorageError(
      kind: seIo, ioError: "Failed to open SSTable file: " & path))

  # Get file size
  let fileSize = getFileSize(path)

  let footerResult = readFooter(strm, fileSize)
  if footerResult.isErr:
    strm.close()
    return err[SsTableReader, StorageError](footerResult.error)

  let footer = footerResult.value

  # Read bloom filter if present
  var bloomFilter: BloomFilter = nil
  if footer.filterHandle.size > 0:
    let filterResult = readBloomFilter(strm, footer.filterHandle)
    if filterResult.isOk:
      bloomFilter = filterResult.value

  var reader = SsTableReader(
    path: path,
    stream: strm,
    indexBlock: newIndexBlock(),
    topLevelIndex: TopLevelIndex(entries: @[], isPartitioned: false),
    footer: footer,
    bloomFilter: bloomFilter,
    sstableId: sstableId,
    blockCache: cache,
    indexMode: footer.indexMode
  )

  if footer.indexMode == imPartitioned:
    # Read top level index
    let tliResult = readTopLevelIndex(strm, footer.indexHandle)
    if tliResult.isErr:
      strm.close()
      return err[SsTableReader, StorageError](tliResult.error)

    reader.topLevelIndex = tliResult.value

    # Get key range from TLI
    if reader.topLevelIndex.entries.len > 0:
      reader.largestKey = reader.topLevelIndex.entries[^1].key

      # Read first index block to get smallest key
      let firstIdxResult = readIndexBlock(strm, reader.topLevelIndex.entries[0].handle)
      if firstIdxResult.isOk and firstIdxResult.value.entries.len > 0:
        let firstHandle = firstIdxResult.value.entries[0].handle
        let blockResult = readDataBlock(strm, firstHandle)
        if blockResult.isOk and blockResult.value.entries.len > 0:
          reader.smallestKey = blockResult.value.entries[0].key
        else:
          reader.smallestKey = firstIdxResult.value.entries[0].key
  else:
    # Read full index block
    let indexResult = readIndexBlock(strm, footer.indexHandle)
    if indexResult.isErr:
      strm.close()
      return err[SsTableReader, StorageError](indexResult.error)

    reader.indexBlock = indexResult.value

    # Get key range from index block
    if reader.indexBlock.entries.len > 0:
      reader.largestKey = reader.indexBlock.entries[^1].key

      let firstHandle = reader.indexBlock.entries[0].handle
      let blockResult = readDataBlock(strm, firstHandle)
      if blockResult.isOk and blockResult.value.entries.len > 0:
        reader.smallestKey = blockResult.value.entries[0].key
      else:
        reader.smallestKey = reader.indexBlock.entries[0].key

  return ok[SsTableReader, StorageError](reader)

proc close*(reader: SsTableReader) =
  if reader.stream != nil:
    reader.stream.close()
    reader.stream = nil

# Result type for get operations that include value type
type
  SsTableGetResult* = object
    value*: string
    valueType*: uint8 # Raw value type byte (cast to ValueType when needed)
    seqno*: uint64
    found*: bool

proc getEntry*(reader: SsTableReader, key: string): SsTableGetResult =
  ## Get a value and its metadata from the SSTable.
  ## Returns the value, value type, and sequence number.
  result = SsTableGetResult(found: false)

  # Check bloom filter first for quick rejection
  if reader.bloomFilter != nil:
    if not reader.bloomFilter.mayContain(key):
      return

  var handle: BlockHandle
  var found = false

  # Get the appropriate index block
  var idxBlock: IndexBlock
  if reader.indexMode == imPartitioned:
    # Find the index block that may contain the key
    let idxResult = reader.findIndexBlockForPartitioned(key)
    if idxResult.isErr:
      return
    idxBlock = idxResult.value
  else:
    idxBlock = reader.indexBlock

  # Find the data block that might contain the key
  for entry in idxBlock.entries:
    if entry.key >= key:
      handle = entry.handle
      found = true
      break
    handle = entry.handle
    found = true

  if not found:
    return

  # Use cached read if block cache is available
  let blockResult = if reader.blockCache != nil:
    reader.readDataBlockWithCache(handle)
  else:
    readDataBlock(reader.stream, handle)

  if blockResult.isErr:
    return

  let dataBlk = blockResult.value
  var lo = 0
  var hi = dataBlk.entries.len - 1

  while lo <= hi:
    let mid = (lo + hi) div 2
    let entry = dataBlk.entries[mid]

    if entry.key == key:
      result.value = entry.value
      result.valueType = entry.valueType # Keep as uint8
      result.seqno = entry.seqno
      result.found = true
      return
    elif entry.key < key:
      lo = mid + 1
    else:
      hi = mid - 1

proc get*(reader: SsTableReader, key: string): Option[string] =
  # Check bloom filter first for quick rejection
  if reader.bloomFilter != nil:
    if not reader.bloomFilter.mayContain(key):
      return none(string)

  var handle: BlockHandle
  var found = false

  # Get the appropriate index block
  var idxBlock: IndexBlock
  if reader.indexMode == imPartitioned:
    # Find the index block that may contain the key
    let idxResult = reader.findIndexBlockForPartitioned(key)
    if idxResult.isErr:
      return none(string)
    idxBlock = idxResult.value
  else:
    idxBlock = reader.indexBlock

  # Find the data block that might contain the key
  # The index block stores the largest key in each data block
  # We want the first block where the largest key >= our search key
  for entry in idxBlock.entries:
    if entry.key >= key:
      handle = entry.handle
      found = true
      break
    # Keep track of the last block we've seen that could contain our key
    handle = entry.handle
    found = true

  if not found:
    return none(string)

  # Use cached read if block cache is available
  let blockResult = if reader.blockCache != nil:
    reader.readDataBlockWithCache(handle)
  else:
    readDataBlock(reader.stream, handle)

  if blockResult.isErr:
    return none(string)

  let dataBlk = blockResult.value
  var lo = 0
  var hi = dataBlk.entries.len - 1

  while lo <= hi:
    let mid = (lo + hi) div 2
    let entry = dataBlk.entries[mid]

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

proc mightContain*(reader: SsTableReader, key: string): bool =
  ## Check if the key might be in this SSTable.
  ## Uses bloom filter for fast rejection, then key range check.

  # First check key range
  if reader.smallestKey.len == 0:
    return false
  if key < reader.smallestKey or key > reader.largestKey:
    return false

  # Then check bloom filter if available
  if reader.bloomFilter != nil:
    return reader.bloomFilter.mayContain(key)

  return true

proc getKeyRange*(reader: SsTableReader): (string, string) =
  (reader.smallestKey, reader.largestKey)

proc numDataBlocks*(reader: SsTableReader): int =
  ## Returns the number of data blocks in the SSTable.
  ## For partitioned index, this is an estimate based on TLI entries.
  if reader.indexMode == imPartitioned:
    # For partitioned index, estimate based on TLI entries * avg entries per index block
    return reader.topLevelIndex.entries.len * INDEX_BLOCK_ENTRIES
  else:
    return reader.indexBlock.entries.len

proc numIndexBlocks*(reader: SsTableReader): int =
  ## Returns the number of index blocks (1 for full, N for partitioned).
  if reader.indexMode == imPartitioned:
    return reader.topLevelIndex.entries.len
  else:
    return 1

proc isPartitioned*(reader: SsTableReader): bool =
  ## Returns true if this SSTable uses partitioned index.
  reader.indexMode == imPartitioned
