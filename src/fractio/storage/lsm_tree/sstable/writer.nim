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
    expectedKeys: int # For bloom filter sizing

proc newSsTableWriter*(path: string, expectedKeys: int = 1000): StorageResult[
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
  writer.smallestSeqno = high(uint64)
  writer.largestSeqno = 0'u64

  return ok[SsTableWriter, StorageError](writer)

proc flushBlock*(writer: SsTableWriter): StorageResult[void] =
  if writer.dataBlock.isEmpty:
    return okVoid

  # Write block type
  writer.stream.write(uint8(btData))

  # Get start offset for handle
  let startOffset = writer.stream.getPosition()

  # Serialize data block
  let serializeResult = writer.dataBlock.serialize(writer.stream)
  if serializeResult.isErr:
    return err[void, StorageError](serializeResult.error)

  let endOffset = writer.stream.getPosition()

  # Create block handle and add to index
  let handle = BlockHandle(offset: uint64(startOffset), size: uint32(endOffset - startOffset))
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

  # Write index block
  # Write block type first
  writer.stream.write(uint8(btIndex))

  # Get start offset AFTER block type byte (consistent with data block)
  let indexStartOffset = writer.stream.getPosition()

  let indexSerializeResult = writer.indexBlock.serialize(writer.stream)
  if indexSerializeResult.isErr:
    return err[SsTable, StorageError](indexSerializeResult.error)

  let indexEndOffset = writer.stream.getPosition()

  let indexHandle = BlockHandle(
    offset: uint64(indexStartOffset),
    size: uint32(indexEndOffset - indexStartOffset)
  )

  # Write footer (48 bytes total)
  # - indexHandle (offset: 8, size: 4) = 12 bytes
  # - filterHandle (offset: 8, size: 4) = 12 bytes
  # - magic (8 bytes)
  # - version (4 bytes)
  # - checksum (4 bytes)
  # - footerSize (4 bytes)
  # Total = 44 bytes

  # Index handle
  var indexOffsetLe = indexHandle.offset
  littleEndian64(addr indexOffsetLe, addr indexOffsetLe)
  writer.stream.write(indexOffsetLe)

  var indexSizeLe = indexHandle.size
  littleEndian32(addr indexSizeLe, addr indexSizeLe)
  writer.stream.write(indexSizeLe)

  # Filter handle
  var filterOffsetLe = filterHandle.offset
  littleEndian64(addr filterOffsetLe, addr filterOffsetLe)
  writer.stream.write(filterOffsetLe)

  var filterSizeLe = filterHandle.size
  littleEndian32(addr filterSizeLe, addr filterSizeLe)
  writer.stream.write(filterSizeLe)

  # Magic
  writer.stream.writeData(addr SSTABLE_MAGIC[0], 8)

  # Version
  var versionLe: uint32 = 2 # Version 2 includes bloom filter
  littleEndian32(addr versionLe, addr versionLe)
  writer.stream.write(versionLe)

  # Checksum (placeholder)
  var checksumLe: uint32 = 0
  writer.stream.write(checksumLe)

  # Footer size
  var footerSizeLe: uint32 = 44
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
