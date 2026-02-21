# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## SSTable Writer Implementation

import ./types
import ./blocks
import fractio/storage/error
import fractio/storage/lsm_tree/types
import fractio/storage/lsm_tree/memtable
import fractio/storage/lsm_tree/bloom_filter
import fractio/storage/lsm_tree/compression
import std/[streams, os, strutils, endians, hashes]

type
  SsTableWriter* = ref object
    path: string
    stream: Stream
    dataBlock: DataBlock
    indexBlock: IndexBlock
    bloomFilter: BloomFilter
    lastKey: string
    blockOffset: uint64
    blockSize: uint32
    numEntries: uint64
    firstKey: string
    smallestSeqno: uint64
    largestSeqno: uint64
    expectedKeys: int                        # For bloom filter sizing
    compression: compression.CompressionType # Compression type for data blocks
    compressionStats: CompressionStats
    usePartitionedIndex: bool                # Whether to use partitioned index
    indexBlockEntries: seq[IndexBlock]       # Partitioned index blocks
    topLevelIndex: TopLevelIndex             # Top level index for partitioned mode

proc newSsTableWriter*(path: string, expectedKeys: int = 1000,
                      compression: compression.CompressionType = compression.ctZlib,
                      usePartitionedIndex: bool = false): StorageResult[
    SsTableWriter] =
  let stream = newFileStream(path, fmWrite)
  if stream == nil:
    return err[SsTableWriter, StorageError](StorageError(
      kind: seIo, ioError: "Failed to create SSTable file: " & path))

  var writer: SsTableWriter
  new(writer)
  writer.path = path
  writer.stream = stream
  writer.dataBlock = newDataBlock()
  writer.indexBlock = newIndexBlock()
  writer.bloomFilter = newBloomFilter(expectedKeys, 0.01) # 1% false positive rate
  writer.blockOffset = 0'u64
  writer.blockSize = 4096'u32
  writer.numEntries = 0'u64
  writer.expectedKeys = expectedKeys
  writer.compression = compression
  writer.compressionStats = newCompressionStats()
  writer.smallestSeqno = high(uint64)
  writer.largestSeqno = 0'u64
  writer.usePartitionedIndex = usePartitionedIndex
  writer.indexBlockEntries = @[]
  writer.topLevelIndex = TopLevelIndex(entries: @[],
      isPartitioned: usePartitionedIndex)

  return ok[SsTableWriter, StorageError](writer)

proc flushBlock*(writer: SsTableWriter): StorageResult[void] =
  if writer.dataBlock.isEmpty:
    return okVoid

  # Serialize data block to a memory stream first
  var memStream = newStringStream()
  let serializeResult = writer.dataBlock.serialize(memStream)
  if serializeResult.isErr:
    return err[void, StorageError](serializeResult.error)

  let uncompressedData = memStream.data
  let uncompressedSize = int(uncompressedData.len)

  # Compress if enabled
  var (dataToWrite, wasCompressed) = compressBlock(uncompressedData,
      writer.compression)
  writer.compressionStats.recordCompression(uncompressedSize, dataToWrite.len,
      wasCompressed)

  # Write block type
  writer.stream.write(uint8(btData))

  # Write compression flag
  writer.stream.write(uint8(if wasCompressed: 1 else: 0))

  # Get start offset for handle (after block type and compression flag)
  let startOffset = writer.stream.getPosition()

  # Write the (possibly compressed) data
  writer.stream.writeData(addr dataToWrite[0], dataToWrite.len)

  let endOffset = writer.stream.getPosition()

  # Create block handle and add to index
  var handle = BlockHandle(
    offset: uint64(startOffset),
    size: uint32(endOffset - startOffset),
    uncompressedSize: if wasCompressed: uint32(uncompressedSize) else: 0
  )
  writer.indexBlock.add(writer.lastKey, handle)

  # Reset data block
  writer.dataBlock = newDataBlock()
  writer.blockOffset = uint64(writer.stream.getPosition())

  return okVoid

proc add*(writer: SsTableWriter, key: string, value: string,
          seqno: uint64, valueType: ValueType): StorageResult[void] =
  if writer.firstKey.len == 0:
    writer.firstKey = key
  writer.lastKey = key

  if seqno < writer.smallestSeqno:
    writer.smallestSeqno = seqno
  if seqno > writer.largestSeqno:
    writer.largestSeqno = seqno

  # Add key to bloom filter (only for regular values, not tombstones)
  if valueType == vtValue:
    writer.bloomFilter.add(key)

  if not writer.dataBlock.add(key, value, seqno, valueType):
    let flushResult = writer.flushBlock()
    if flushResult.isErr:
      return flushResult
    discard writer.dataBlock.add(key, value, seqno, valueType)

  writer.numEntries += 1
  return okVoid

proc add*(writer: SsTableWriter, entry: MemtableEntry): StorageResult[void] =
  return writer.add(entry.key, entry.value, entry.seqno, entry.valueType)

# Helper to write an index block and return its handle
proc writeIndexBlock(writer: SsTableWriter,
    idxBlock: IndexBlock): StorageResult[BlockHandle] =
  writer.stream.write(uint8(btIndex))
  let startOffset = writer.stream.getPosition()
  let serializeResult = idxBlock.serialize(writer.stream)
  if serializeResult.isErr:
    return err[BlockHandle, StorageError](serializeResult.error)
  let endOffset = writer.stream.getPosition()
  # Note: Index blocks are not compressed, so uncompressedSize is 0
  return ok[BlockHandle, StorageError](BlockHandle(
    offset: uint64(startOffset),
    size: uint32(endOffset - startOffset),
    uncompressedSize: 0
  ))

# Helper to write top level index block
proc writeTopLevelIndex(writer: SsTableWriter,
    tli: TopLevelIndex): StorageResult[BlockHandle] =
  writer.stream.write(uint8(btTopLevelIndex))
  let startOffset = writer.stream.getPosition()

  # Write number of entries
  var numEntriesLe = uint32(tli.entries.len)
  littleEndian32(addr numEntriesLe, addr numEntriesLe)
  writer.stream.write(numEntriesLe)

  # Write each entry
  for entry in tli.entries:
    # Write key length
    var keyLenLe = uint32(entry.key.len)
    littleEndian32(addr keyLenLe, addr keyLenLe)
    writer.stream.write(keyLenLe)

    # Write key
    writer.stream.write(entry.key)

    # Write handle
    var offsetLe = entry.handle.offset
    littleEndian64(addr offsetLe, addr offsetLe)
    writer.stream.write(offsetLe)

    var sizeLe = entry.handle.size
    littleEndian32(addr sizeLe, addr sizeLe)
    writer.stream.write(sizeLe)

    var uncompressedLe = entry.handle.uncompressedSize
    littleEndian32(addr uncompressedLe, addr uncompressedLe)
    writer.stream.write(uncompressedLe)

  let endOffset = writer.stream.getPosition()
  return ok[BlockHandle, StorageError](BlockHandle(
    offset: uint64(startOffset),
    size: uint32(endOffset - startOffset)
  ))

proc finish*(writer: SsTableWriter): StorageResult[SsTable] =
  # Flush remaining data
  let flushResult = writer.flushBlock()
  if flushResult.isErr:
    return err[SsTable, StorageError](flushResult.error)

  # Write bloom filter block
  writer.stream.write(uint8(btFilter))
  let filterStartOffset = writer.stream.getPosition()
  writer.bloomFilter.serialize(writer.stream)
  let filterEndOffset = writer.stream.getPosition()

  let filterHandle = BlockHandle(
    offset: uint64(filterStartOffset),
    size: uint32(filterEndOffset - filterStartOffset)
  )

  # Determine if we should use partitioned index
  let numIndexEntries = writer.indexBlock.entries.len
  let usePartitioned = writer.usePartitionedIndex or
                       numIndexEntries >= MIN_INDEX_ENTRIES_FOR_PARTITION

  var indexHandle: BlockHandle
  var indexMode: IndexMode

  if usePartitioned and numIndexEntries > 0:
    # Partition the index into multiple blocks
    indexMode = imPartitioned

    # Split index entries into blocks of INDEX_BLOCK_ENTRIES each
    var tli = TopLevelIndex(entries: @[], isPartitioned: true)
    var currentBlock = newIndexBlock()
    var count = 0

    for entry in writer.indexBlock.entries:
      currentBlock.entries.add(entry)
      count += 1

      if count >= INDEX_BLOCK_ENTRIES:
        # Write this index block
        let handleResult = writer.writeIndexBlock(currentBlock)
        if handleResult.isErr:
          return err[SsTable, StorageError](handleResult.error)

        # Add to top level index (use last key of this block)
        tli.entries.add(TopLevelIndexEntry(
          key: currentBlock.entries[^1].key,
          handle: handleResult.value
        ))

        # Start new block
        currentBlock = newIndexBlock()
        count = 0

    # Write remaining entries
    if currentBlock.entries.len > 0:
      let handleResult = writer.writeIndexBlock(currentBlock)
      if handleResult.isErr:
        return err[SsTable, StorageError](handleResult.error)

      tli.entries.add(TopLevelIndexEntry(
        key: currentBlock.entries[^1].key,
        handle: handleResult.value
      ))

    # Write top level index
    let tliResult = writer.writeTopLevelIndex(tli)
    if tliResult.isErr:
      return err[SsTable, StorageError](tliResult.error)

    indexHandle = tliResult.value
  else:
    # Use full (single-level) index
    indexMode = imFull

    writer.stream.write(uint8(btIndex))
    let indexStartOffset = writer.stream.getPosition()

    let indexSerializeResult = writer.indexBlock.serialize(writer.stream)
    if indexSerializeResult.isErr:
      return err[SsTable, StorageError](indexSerializeResult.error)

    let indexEndOffset = writer.stream.getPosition()

    indexHandle = BlockHandle(
      offset: uint64(indexStartOffset),
      size: uint32(indexEndOffset - indexStartOffset)
    )

  # Write footer (48 bytes for v3 with index mode)
  # - indexHandle (offset: 8, size: 4, uncompressedSize: 4) = 16 bytes
  # - filterHandle (offset: 8, size: 4, uncompressedSize: 4) = 16 bytes
  # - magic (8 bytes)
  # - version (4 bytes)
  # - indexMode (1 byte)
  # - checksum (4 bytes)
  # - footerSize (4 bytes)
  # Total = 53 bytes

  # Index handle
  var indexOffsetLe = indexHandle.offset
  littleEndian64(addr indexOffsetLe, addr indexOffsetLe)
  writer.stream.write(indexOffsetLe)

  var indexSizeLe = indexHandle.size
  littleEndian32(addr indexSizeLe, addr indexSizeLe)
  writer.stream.write(indexSizeLe)

  var indexUncompressedLe = indexHandle.uncompressedSize
  littleEndian32(addr indexUncompressedLe, addr indexUncompressedLe)
  writer.stream.write(indexUncompressedLe)

  # Filter handle
  var filterOffsetLe = filterHandle.offset
  littleEndian64(addr filterOffsetLe, addr filterOffsetLe)
  writer.stream.write(filterOffsetLe)

  var filterSizeLe = filterHandle.size
  littleEndian32(addr filterSizeLe, addr filterSizeLe)
  writer.stream.write(filterSizeLe)

  var filterUncompressedLe = filterHandle.uncompressedSize
  littleEndian32(addr filterUncompressedLe, addr filterUncompressedLe)
  writer.stream.write(filterUncompressedLe)

  # Magic
  writer.stream.writeData(addr SSTABLE_MAGIC[0], 8)

  # Version (3 = partitioned index support)
  var versionLe: uint32 = 3
  littleEndian32(addr versionLe, addr versionLe)
  writer.stream.write(versionLe)

  # Index mode
  writer.stream.write(uint8(indexMode))

  # Checksum (placeholder)
  var checksumLe: uint32 = 0
  writer.stream.write(checksumLe)

  # Footer size
  var footerSizeLe: uint32 = 53
  littleEndian32(addr footerSizeLe, addr footerSizeLe)
  writer.stream.write(footerSizeLe)

  writer.stream.flush()
  writer.stream.close()

  let fileSize = getFileSize(writer.path)

  return ok[SsTable, StorageError](SsTable(
    id: 0,
    path: writer.path,
    size: uint64(fileSize),
    level: 0,
    smallestKey: writer.firstKey,
    largestKey: writer.lastKey,
    seqnoRange: (writer.smallestSeqno, writer.largestSeqno)
  ))

proc writeMemtable*(path: string, memtable: Memtable,
                    expectedKeys: int = 0): StorageResult[SsTable] =
  ## Write a memtable to an SSTable file.
  ##
  ## Parameters:
  ##   path: Output file path
  ##   memtable: The memtable to write
  ##   expectedKeys: Expected number of keys for bloom filter sizing (0 = use memtable size)
  let keyCount = if expectedKeys > 0: expectedKeys else: memtable.len

  let writerResult = newSsTableWriter(path, keyCount)
  if writerResult.isErr:
    return err[SsTable, StorageError](writerResult.error)

  let writer = writerResult.value

  for entry in memtable.getSortedEntries():
    let addResult = writer.add(entry)
    if addResult.isErr:
      return err[SsTable, StorageError](addResult.error)

  return writer.finish()
