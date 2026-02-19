# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## SSTable Writer Implementation

import ./types
import ./blocks
import fractio/storage/error
import fractio/storage/lsm_tree/types
import fractio/storage/lsm_tree/memtable
import std/[streams, os, strutils, endians, hashes]

type
  SsTableWriter* = ref object
    path: string
    stream: Stream
    dataBlock: DataBlock
    indexBlock: IndexBlock
    lastKey: string
    blockOffset: uint64
    blockSize: uint32
    numEntries: uint64
    firstKey: string
    smallestSeqno: uint64
    largestSeqno: uint64

proc newSsTableWriter*(path: string): StorageResult[SsTableWriter] =
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
  writer.blockOffset = 0'u64
  writer.blockSize = 4096'u32
  writer.numEntries = 0'u64
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
  
  # Write index block
  let indexStartOffset = writer.stream.getPosition()
  writer.stream.write(uint8(btIndex))
  
  let indexSerializeResult = writer.indexBlock.serialize(writer.stream)
  if indexSerializeResult.isErr:
    return err[SsTable, StorageError](indexSerializeResult.error)
    
  let indexEndOffset = writer.stream.getPosition()
  
  let indexHandle = BlockHandle(
    offset: uint64(indexStartOffset),
    size: uint32(indexEndOffset - indexStartOffset)
  )
  
  # Write footer
  var offsetLe = indexHandle.offset
  littleEndian64(addr offsetLe, addr offsetLe)
  writer.stream.write(offsetLe)
  
  var sizeLe = indexHandle.size
  littleEndian32(addr sizeLe, addr sizeLe)
  writer.stream.write(sizeLe)
  
  writer.stream.writeData(addr SSTABLE_MAGIC[0], 8)
  
  var versionLe: uint32 = 1
  littleEndian32(addr versionLe, addr versionLe)
  writer.stream.write(versionLe)
  
  var checksumLe: uint32 = 0
  writer.stream.write(checksumLe)
  
  var footerSizeLe: uint32 = 32
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

proc writeMemtable*(path: string, memtable: Memtable): StorageResult[SsTable] =
  let writerResult = newSsTableWriter(path)
  if writerResult.isErr:
    return err[SsTable, StorageError](writerResult.error)
  
  let writer = writerResult.value
  
  for entry in memtable.getSortedEntries():
    let addResult = writer.add(entry)
    if addResult.isErr:
      return err[SsTable, StorageError](addResult.error)
  
  return writer.finish()
