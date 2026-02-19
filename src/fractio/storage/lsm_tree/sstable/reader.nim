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
import std/[streams, os, strutils, endians, options]

type
  SsTableReader* = ref object
    path*: string
    stream*: FileStream
    indexBlock*: IndexBlock
    footer*: SsTableFooter
    smallestKey*: string
    largestKey*: string
    seqnoRange*: (uint64, uint64)

proc readFooter(strm: Stream): StorageResult[SsTableFooter] =
  strm.setPosition(-32, spEnd)

  var footer = SsTableFooter()
  var offsetLe: uint64
  var sizeLe: uint32

  offsetLe = strm.readUInt64()
  littleEndian64(addr footer.indexHandle.offset, addr offsetLe)

  sizeLe = strm.readUInt32()
  littleEndian32(addr footer.indexHandle.size, addr sizeLe)

  for i in 0 ..< 8:
    footer.magic[i] = strm.readUInt8()

  if footer.magic != SSTABLE_MAGIC:
    return err[SsTableFooter, StorageError](StorageError(
      kind: seInvalidVersion, invalidVersion: none(FormatVersion)))

  var versionLe: uint32 = strm.readUInt32()
  littleEndian32(addr footer.version, addr versionLe)

  footer.checksum = strm.readUInt32()

  return ok[SsTableFooter, StorageError](footer)

proc readIndexBlock(strm: Stream, hdl: BlockHandle): StorageResult[IndexBlock] =
  strm.setPosition(int(hdl.offset))

  var idxBlock = newIndexBlock()

  let blockType = BlockType(strm.readUInt8())
  if blockType != btIndex:
    return err[IndexBlock, StorageError](StorageError(
      kind: seIo, ioError: "Expected index block"))

  let endPos = int(hdl.offset) + int(hdl.size)

  while strm.getPosition() < endPos:
    var entry = IndexEntry()

    var keyLenLe: uint32 = strm.readUInt32()
    var keyLen: uint32
    littleEndian32(addr keyLen, addr keyLenLe)

    if keyLen > 0:
      entry.key = newString(int(keyLen))
      discard strm.readData(addr entry.key[0], int(keyLen))

    var offsetLe: uint64 = strm.readUInt64()
    littleEndian64(addr entry.handle.offset, addr offsetLe)

    var sizeLe: uint32 = strm.readUInt32()
    littleEndian32(addr entry.handle.size, addr sizeLe)

    idxBlock.entries.add(entry)

  return ok[IndexBlock, StorageError](idxBlock)

proc readDataBlock(strm: Stream, hdl: BlockHandle): StorageResult[DataBlock] =
  strm.setPosition(int(hdl.offset))

  var dataBlk = newDataBlock()

  let blockType = BlockType(strm.readUInt8())
  if blockType != btData:
    return err[DataBlock, StorageError](StorageError(
      kind: seIo, ioError: "Expected data block"))

  let endPos = int(hdl.offset) + int(hdl.size)
  var prevKey = ""
  var entryCount = 0

  while strm.getPosition() < endPos - 8:
    var entry = BlockEntry()

    var sharedLenLe: uint32 = strm.readUInt32()
    var sharedLen: uint32
    littleEndian32(addr sharedLen, addr sharedLenLe)

    var unsharedLenLe: uint32 = strm.readUInt32()
    var unsharedLen: uint32
    littleEndian32(addr unsharedLen, addr unsharedLenLe)

    var valueLenLe: uint32 = strm.readUInt32()
    var valueLen: uint32
    littleEndian32(addr valueLen, addr valueLenLe)

    var seqnoType: uint64 = strm.readUInt64()
    littleEndian64(addr seqnoType, addr seqnoType)
    entry.seqno = seqnoType shr 8
    entry.valueType = uint8(seqnoType and 0xFF)

    if sharedLen > 0:
      entry.key = prevKey[0 ..< int(sharedLen)]
    if unsharedLen > 0:
      var unshared = newString(int(unsharedLen))
      discard strm.readData(addr unshared[0], int(unsharedLen))
      entry.key &= unshared

    if valueLen > 0:
      entry.value = newString(int(valueLen))
      discard strm.readData(addr entry.value[0], int(valueLen))

    dataBlk.entries.add(entry)
    prevKey = entry.key
    entryCount += 1

    if entryCount > 100000:
      break

  return ok[DataBlock, StorageError](dataBlk)

proc openSsTable*(path: string): StorageResult[SsTableReader] =
  let strm = newFileStream(path, fmRead)
  if strm == nil:
    return err[SsTableReader, StorageError](StorageError(
      kind: seIo, ioError: "Failed to open SSTable file: " & path))


  let footerResult = readFooter(strm)
  if footerResult.isErr:
    strm.close()
    return err[SsTableReader, StorageError](footerResult.error)

  let footer = footerResult.value

  let indexResult = readIndexBlock(strm, footer.indexHandle)
  if indexResult.isErr:
    strm.close()
    return err[SsTableReader, StorageError](indexResult.error)

  result = SsTableReader(
    path: path,
    stream: strm,
    indexBlock: indexResult.value,
    footer: footer
  )

  if result.indexBlock.entries.len > 0:
    result.smallestKey = result.indexBlock.entries[0].key
    result.largestKey = result.indexBlock.entries[^1].key

proc close*(reader: SsTableReader) =
  if reader.stream != nil:
    reader.stream.close()
    reader.stream = nil

proc get*(reader: SsTableReader, key: string): Option[string] =
  var handle: BlockHandle
  var found = false

  for entry in reader.indexBlock.entries:
    if entry.key <= key:
      handle = entry.handle
      found = true
    else:
      break

  if not found:
    return none(string)

  let blockResult = readDataBlock(reader.stream, handle)
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
  if reader.smallestKey.len == 0:
    return false
  if key < reader.smallestKey or key > reader.largestKey:
    return false
  return true

proc getKeyRange*(reader: SsTableReader): (string, string) =
  (reader.smallestKey, reader.largestKey)

proc numDataBlocks*(reader: SsTableReader): int =
  reader.indexBlock.entries.len
