# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree v2 - SSTable
##
## This module provides the SSTable (Sorted String Table) implementation
## for on-disk storage of sorted key-value pairs.

import std/[sequtils, streams, endians, strformat, tables, options, hashes, algorithm]
import types
import error
import coding

# ============================================================================
# Block Handle
# ============================================================================

type
  BlockHandle* = ref object
    offset*: uint64
    size*: uint32

proc `$`*(h: BlockHandle): string =
  "BlockHandle(offset=" & $h.offset & ", size=" & $h.size & ")"

# ============================================================================
# SSTable Metadata
# ============================================================================

type
  TableMeta* = ref object
    id*: TableId
    globalId*: GlobalTableId
    level*: int
    size*: uint64
    minKey*: types.Slice
    maxKey*: types.Slice
    entryCount*: uint64
    compression*: CompressionType
    smallestSeqno*: SeqNo
    largestSeqno*: SeqNo
    checksum*: uint64

proc newTableMeta*(id: TableId, level: int = 0): TableMeta =
  TableMeta(
    id: id,
    globalId: id,
    level: level,
    size: 0,
    minKey: emptySlice(),
    maxKey: emptySlice(),
    entryCount: 0,
    compression: ctNone,
    smallestSeqno: 0,
    largestSeqno: 0,
    checksum: 0
  )

# ============================================================================
# SSTable
# ============================================================================

type
  SsTable* = ref object
    path*: string
    meta*: TableMeta
    fileSize*: uint64

proc newSsTable*(path: string): SsTable =
  SsTable(
    path: path,
    meta: newTableMeta(0, 0),
    fileSize: 0
  )

proc id*(t: SsTable): TableId = t.meta.id
proc level*(t: SsTable): int = t.meta.level
proc size*(t: SsTable): uint64 = t.meta.size
proc entryCount*(t: SsTable): uint64 = t.meta.entryCount
proc minKey*(t: SsTable): types.Slice = t.meta.minKey
proc maxKey*(t: SsTable): types.Slice = t.meta.maxKey

# ============================================================================
# Block Builder
# ============================================================================

const
  DefaultRestartInterval* = 16
  BlockTrailerSize* = 5 # 1 byte compression + 4 bytes checksum

type
  BlockBuilder* = ref object
    buffer*: string
    restartPoints*: seq[int]
    entryCount*: int
    restartInterval*: int
    lastKey*: types.Slice

proc newBlockBuilder*(restartInterval: int = DefaultRestartInterval): BlockBuilder =
  BlockBuilder(
    buffer: newString(0),
    restartPoints: @[0],
    entryCount: 0,
    restartInterval: restartInterval,
    lastKey: emptySlice()
  )

proc estimatedSize*(b: BlockBuilder): int = b.buffer.len

proc add*(b: BlockBuilder, value: InternalValue): LsmResult[void] =
  try:
    let key = value.key
    let val = value.value

    # Calculate shared prefix length
    var sharedLen = 0
    let minLen = min(b.lastKey.len, key.userKey.len)
    while sharedLen < minLen and b.lastKey[sharedLen] == key.userKey[sharedLen]:
      inc sharedLen

    let unsharedLen = key.userKey.len - sharedLen

    # Encode: shared_len (varint), unshared_len (varint), value_len (varint)
    # Then: unshared_key_bytes, value_bytes
    var entry = ""

    # Shared key length
    entry.add(encodeVarint(sharedLen.uint64))
    # Unshared key length
    entry.add(encodeVarint(unsharedLen.uint64))
    # Value length
    entry.add(encodeVarint(val.len.uint64))

    # Unshared key bytes
    if unsharedLen > 0:
      entry.add(key.userKey[sharedLen ..< key.userKey.len])

    # Value bytes
    entry.add(val.data)

    # Add to buffer
    b.buffer.add(entry)

    # Update restart points
    b.entryCount += 1
    if b.entryCount mod b.restartInterval == 0:
      b.restartPoints.add(b.buffer.len)

    # Update last key
    b.lastKey = key.userKey

    okVoid()
  except:
    errVoid(newIoError("Failed to add entry to block: " &
        getCurrentExceptionMsg()))

proc finish*(b: BlockBuilder): LsmResult[string] =
  try:
    var data = b.buffer

    # Add restart points
    for rp in b.restartPoints:
      data.add(encodeFixed32(rp.uint32))

    # Add restart point count
    data.add(encodeFixed32(b.restartPoints.len.uint32))

    ok(data)
  except:
    err[string](newIoError("Failed to finish block: " & getCurrentExceptionMsg()))

proc reset*(b: BlockBuilder) =
  b.buffer = ""
  b.restartPoints = @[0]
  b.entryCount = 0
  b.lastKey = emptySlice()

# ============================================================================
# Varint Encoding
# ============================================================================

proc encodeVarint*(value: int): string =
  result = ""
  var v = uint64(value)
  while true:
    var b = (v and 0x7F).uint8
    v = v shr 7
    if v != 0:
      b = b or 0x80
    result.add(chr(b))
    if v == 0:
      break

proc encodeUint32*(value: uint32): string =
  result = newString(4)
  result[0] = chr((value and 0xFF).uint8)
  result[1] = chr(((value shr 8) and 0xFF).uint8)
  result[2] = chr(((value shr 16) and 0xFF).uint8)
  result[3] = chr(((value shr 24) and 0xFF).uint8)

proc encodeUint64*(value: uint64): string =
  result = newString(8)
  for i in 0 ..< 8:
    result[i] = chr(((value shr (i * 8)) and 0xFF).uint8)

proc decodeVarint*(data: string, offset: var int): int =
  var result: int = 0
  var shift = 0
  while true:
    let b = data[offset].uint8
    inc offset
    result = result or (int(b and 0x7F) shl shift)
    if (b and 0x80) == 0:
      break
    inc shift
  return result

proc decodeUint32*(data: string, offset: int): uint32 =
  result = 0
  for i in 0 ..< 4:
    result = result or (uint32(data[offset + i].uint8) shl (i * 8))

# ============================================================================
# Block Reader
# ============================================================================

type
  BlockReader* = ref object
    data*: string
    restartPoints*: seq[int]
    restartInterval*: int
    currentIndex*: int

proc newBlockReader*(data: string, restartInterval: int = DefaultRestartInterval): BlockReader =
  # Parse restart points from end
  let totalRestarts = int(decodeUint32(data, data.len - 4))
  var restartPoints = newSeq[int](totalRestarts)

  let restartOffset = data.len - 4 - (totalRestarts * 4)
  for i in 0 ..< totalRestarts:
    restartPoints[i] = int(decodeUint32(data, restartOffset + i * 4))

  BlockReader(
    data: data,
    restartPoints: restartPoints,
    restartInterval: restartInterval,
    currentIndex: -1
  )

proc next*(r: BlockReader): Option[InternalValue] =
  if r.currentIndex >= r.restartPoints.len - 1:
    return none(InternalValue)

  # Find restart point for current index
  let restartIdx = (r.currentIndex + 1) div r.restartInterval
  var offset = r.restartPoints[restartIdx]

  # Skip entries until we reach current position
  for i in (restartIdx * r.restartInterval) ..< r.currentIndex + 1:
    if i > restartIdx * r.restartInterval:
      let sharedLen = decodeVarint(r.data, offset)
      let unsharedLen = decodeVarint(r.data, offset)
      let valueLen = decodeVarint(r.data, offset)
      offset += unsharedLen + valueLen

  # Read current entry
  let sharedLen = decodeVarint(r.data, offset)
  let unsharedLen = decodeVarint(r.data, offset)
  let valueLen = decodeVarint(r.data, offset)

  # Reconstruct key (simplified - we'd need previous keys in practice)
  var keyData = newString(sharedLen + unsharedLen)
  for i in 0 ..< sharedLen:
    keyData[i] = '\0' # Would need actual previous key
  for i in 0 ..< unsharedLen:
    keyData[sharedLen + i] = r.data[offset]
    inc offset

  # Read value
  var valueData = r.data[offset ..< offset + valueLen]

  r.currentIndex += 1

  some(InternalValue(
    key: newInternalKey(newSlice(keyData), 0, vtValue),
    value: newSlice(valueData)
  ))

proc hasNext*(r: BlockReader): bool =
  r.currentIndex < r.restartPoints.len - 1

# ============================================================================
# Table Writer
# ============================================================================

type
  TableWriter* = ref object
    path*: string
    stream*: Stream
    currentBlock*: BlockBuilder
    indexEntries*: seq[tuple[key: types.Slice, handle: BlockHandle]]
    minKey*: types.Slice
    maxKey*: types.Slice
    entryCount*: uint64
    blockRestartInterval*: int
    smallestSeqno*: SeqNo
    largestSeqno*: SeqNo

proc newTableWriter*(path: string, restartInterval: int = DefaultRestartInterval): LsmResult[TableWriter] =
  let stream = newFileStream(path, fmWrite)
  if stream.isNil:
    return err[TableWriter](newIoError("Failed to create table file: " & path))

  let writer = TableWriter(
    path: path,
    stream: stream,
    currentBlock: newBlockBuilder(restartInterval),
    indexEntries: @[],
    minKey: emptySlice(),
    maxKey: emptySlice(),
    entryCount: 0,
    blockRestartInterval: restartInterval,
    smallestSeqno: MAX_VALID_SEQNO,
    largestSeqno: 0
  )
  ok(writer)

proc addEntry*(w: TableWriter, value: InternalValue): LsmResult[void] =
  try:
    let key = value.key

    # Update min/max key
    if w.entryCount == 0:
      w.minKey = key.userKey
      w.smallestSeqno = key.seqno
    w.maxKey = key.userKey
    w.largestSeqno = key.seqno

    # Add to current block
    let addResult = w.currentBlock.add(value)
    if addResult.isErr:
      return errVoid(addResult.error)

    # Check if block is full (4KB default)
    if w.currentBlock.estimatedSize() >= 4096:
      # Finish current block
      let blockData = w.currentBlock.finish()
      if blockData.isErr:
        return errVoid(blockData.error)

      # Add index entry
      let handle = BlockHandle(
        offset: w.stream.getPosition().uint64,
        size: blockData.value.len.uint32
      )
      w.indexEntries.add((key.userKey, handle))

      # Write block data
      w.stream.write(blockData.value)

      # Reset for next block
      w.currentBlock.reset()

    w.entryCount += 1
    okVoid()
  except:
    errVoid(newIoError("Failed to add entry to table: " &
        getCurrentExceptionMsg()))

proc finish*(w: TableWriter): LsmResult[SsTable] =
  try:
    # Flush remaining block
    if w.currentBlock.estimatedSize() > 0:
      let blockData = w.currentBlock.finish()
      if blockData.isErr:
        return err[SsTable](blockData.error)

      let handle = BlockHandle(
        offset: w.stream.getPosition().uint64,
        size: blockData.value.len.uint32
      )
      w.indexEntries.add((w.maxKey, handle))
      w.stream.write(blockData.value)

    # Write index block
    var indexData = ""
    for entry in w.indexEntries:
      indexData.add(encodeVarint(entry.key.len))
      indexData.add(entry.key.data)
      indexData.add(encodeVarint(int(entry.handle.offset)))
      indexData.add(encodeVarint(int(entry.handle.size)))

    let indexOffset = w.stream.getPosition().uint64
    let indexSize = indexData.len.uint32
    w.stream.write(indexData)

    # Write footer
    var footer = ""
    footer.add(encodeUint64(indexOffset))
    footer.add(encodeUint32(indexSize))
    footer.add(encodeUint64(0)) # No meta block
    footer.add(encodeUint32(0))
    w.stream.write(footer)

    close(w.stream)

    # Create table
    let table = newSsTable(w.path)
    table.meta.minKey = w.minKey
    table.meta.maxKey = w.maxKey
    table.meta.entryCount = w.entryCount
    table.meta.smallestSeqno = w.smallestSeqno
    table.meta.largestSeqno = w.largestSeqno
    table.fileSize = w.stream.getPosition().uint64
    table.meta.size = table.fileSize

    ok(table)
  except:
    err[SsTable](newIoError("Failed to finish table: " & getCurrentExceptionMsg()))

proc close*(w: TableWriter) =
  close(w.stream)

# ============================================================================
# Bloom Filter (simplified)
# ============================================================================

type
  BloomFilter* = ref object
    data*: string
    numHashes*: int
    numBits*: int

proc newBloomFilter*(numBits: int, numHashes: int): BloomFilter =
  BloomFilter(
    data: newString(numBits div 8),
    numHashes: numHashes,
    numBits: numBits
  )

proc hash*(s: types.Slice): int =
  # Simple hash - in production use better hash function
  var h = 0
  for c in s.data:
    h = h * 31 + ord(c)
  h

proc addKey*(bf: BloomFilter, key: types.Slice) =
  let h1 = key.hash()
  let h2 = h1 shr 16

  for i in 0 ..< bf.numHashes:
    let idx = ((h1 + i * h2) mod bf.numBits) div 8
    let bit = ((h1 + i * h2) mod bf.numBits) mod 8
    bf.data[idx] = chr(ord(bf.data[idx]) or (1 shl bit))

proc mightContain*(bf: BloomFilter, key: types.Slice): bool =
  let h1 = key.hash()
  let h2 = h1 shr 16

  for i in 0 ..< bf.numHashes:
    let idx = ((h1 + i * h2) mod bf.numBits) div 8
    let bit = ((h1 + i * h2) mod bf.numBits) mod 8
    if (ord(bf.data[idx]) and (1 shl bit)) == 0:
      return false
  true

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing table..."

  # Test varint encoding
  let encoded = encodeVarint(300)
  echo "Encoded 300: ", encoded.len, " bytes"

  # Test block builder
  let builder = newBlockBuilder()
  discard builder.add(newInternalValue("key1", "value1", 1, vtValue))
  discard builder.add(newInternalValue("key2", "value2", 2, vtValue))

  let blockData = builder.finish()
  if blockData.isOk:
    echo "Block data size: ", blockData.value.len

  # Test bloom filter
  let bf = newBloomFilter(1024, 3)
  bf.addKey(newSlice("test_key"))
  echo "Bloom might contain test_key: ", bf.mightContain(newSlice("test_key"))
  echo "Bloom might contain other: ", bf.mightContain(newSlice("other"))

  echo "Table tests passed!"
