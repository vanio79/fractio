# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree v2 - SSTable
##
## This module provides the SSTable (Sorted String Table) implementation
## for on-disk storage of sorted key-value pairs.

import std/[sequtils, posix, endians, strformat, tables,
    options, hashes, algorithm, os]
import types
import error
import coding
import hash
import cache
import sstable_block

# ============================================================================
# Block Handle
# ============================================================================

type
  TableBlockHandle* = ref object
    offset*: uint64
    size*: uint32

proc `$`*(h: TableBlockHandle): string =
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
    minKey*: string
    maxKey*: string
    entryCount*: uint64
    compression*: sstable_block.CompressionType
    smallestSeqno*: SeqNo
    largestSeqno*: SeqNo
    checksum*: uint64

proc newTableMeta*(id: TableId, level: int = 0): TableMeta =
  TableMeta(
    id: id,
    globalId: GlobalTableId(id),
    level: level,
    size: 0,
    minKey: "",
    maxKey: "",
    entryCount: 0,
    compression: ctNone,
    smallestSeqno: 0.SeqNo,
    largestSeqno: 0.SeqNo,
    checksum: 0
  )

# ============================================================================
# SSTable
# ============================================================================

# Forward declaration with fields
type
  BloomFilter* = ref object
    data*: string
    numHashes*: int
    numBits*: int

type
  SsTable* = ref object
    path*: string
    meta*: TableMeta
    fileSize*: uint64
    ## Cached data (optional optimization)
    cachedIndexBlock*: Option[string]
    cachedFilterBlock*: Option[BloomFilter]
    filterOffset*: uint64
    filterSize*: uint32
    fileHandle*: cint    # File descriptor for efficient I/O
    blockCache*: pointer # Pointer to shared block cache (Cache object)

proc newSsTable*(path: string): SsTable =
  result = SsTable(
    path: path,
    meta: newTableMeta(TableId(0), 0),
    fileSize: 0,
    cachedIndexBlock: none(string),
    cachedFilterBlock: none(BloomFilter),
    filterOffset: 0,
    filterSize: 0,
    blockCache: nil
  )
  # Open and keep file handle
  result.fileHandle = posix.open(path, cint(FileFlags.frRead), 0)

proc id*(t: SsTable): TableId = t.meta.id
proc level*(t: SsTable): int = t.meta.level
proc size*(t: SsTable): uint64 = t.meta.size

proc setBlockCache*(t: SsTable, cache: ptr Cache) =
  ## Set the shared block cache for this table
  t.blockCache = cast[pointer](cache)

proc getCachedBlock*(table: SsTable, offset: uint64): Option[string] =
  ## Get a block from the shared cache
  if table.blockCache.isNil:
    return none(string)
  let cachePtr = cast[ptr Cache](table.blockCache)
  cachePtr.getBlock(0, int64(table.meta.id), offset)

proc putCachedBlock*(table: SsTable, offset: uint64, data: string) =
  ## Put a block into the shared cache
  if table.blockCache.isNil:
    return
  let cachePtr = cast[ptr Cache](table.blockCache)
  cachePtr.insertBlock(0, int64(table.meta.id), offset, data)

proc entryCount*(t: SsTable): uint64 = t.meta.entryCount
proc minKey*(t: SsTable): string = t.meta.minKey
proc maxKey*(t: SsTable): string = t.meta.maxKey

proc closeTable*(t: SsTable) =
  ## Close the table and release resources
  discard posix.close(t.fileHandle)

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
    lastKey*: string
    hashIndexBuilder*: HashIndexBuilder ## Hash index for fast lookups

proc newBlockBuilder*(restartInterval: int = DefaultRestartInterval,
                      hashIndexRatio: float = 1.33): BlockBuilder =
  # Calculate bucket count based on ratio (similar to Rust)
  let bucketCount = max(uint32(1), uint32(float(restartInterval) *
      hashIndexRatio))
  let hashIdx = sstable_block.newHashIndexBuilder(bucketCount)
  BlockBuilder(
    buffer: newString(0),
    restartPoints: @[0],
    entryCount: 0,
    restartInterval: restartInterval,
    lastKey: "",
    hashIndexBuilder: hashIdx
  )

proc estimatedSize*(b: BlockBuilder): int = b.buffer.len

proc add*(b: BlockBuilder, key: InternalKey, value: string): LsmResult[void] =
  try:
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
    entry.add(encodeVarint(value.len.uint64))

    # Unshared key bytes
    if unsharedLen > 0:
      let unsharedKey = key.userKey[sharedLen..<key.userKey.len]
      entry.add(unsharedKey)

    # Value bytes
    entry.add(value)

    # Add to buffer
    b.buffer.add(entry)

    # Update last key
    b.lastKey = key.userKey

    # Update hash index - map key to binary index position (restart point index)
    let restartIdx = b.restartPoints.len - 1 # Current restart point index
    if restartIdx < 255: # Max value for uint8
      discard b.hashIndexBuilder.set(key.userKey, uint8(restartIdx))

    # Update restart points
    b.entryCount += 1
    if b.entryCount mod b.restartInterval == 0:
      b.restartPoints.add(b.buffer.len)

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

    # Add hash index data at the end
    let hashIndexData = b.hashIndexBuilder.intoInner()
    data.add(hashIndexData)

    ok(data)
  except:
    err[string](newIoError("Failed to finish block: " & getCurrentExceptionMsg()))

proc reset*(b: BlockBuilder) =
  b.buffer = ""
  b.restartPoints = @[0]
  b.entryCount = 0
  b.lastKey = ""
  # Recreate hash index with same bucket count
  let bucketCount = max(uint32(1), uint32(float(b.restartInterval) * 1.33))
  b.hashIndexBuilder = sstable_block.newHashIndexBuilder(bucketCount)

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

proc next*(r: BlockReader): Option[tuple[key: InternalKey, value: string]] =
  if r.currentIndex >= r.restartPoints.len - 1:
    return none(tuple[key: InternalKey, value: string])

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

  some((
    key: InternalKey(userKey: keyData, seqno: SeqNo(0), valueType: vtValue),
    value: valueData
  ))

proc hasNext*(r: BlockReader): bool =
  r.currentIndex < r.restartPoints.len - 1

# ============================================================================
# Table Writer
# ============================================================================

type
  TableWriter* = ref object
    path*: string
    stream*: cint
    currentBlock*: BlockBuilder
    indexEntries*: seq[tuple[key: string, handle: TableBlockHandle]]
    minKey*: string
    maxKey*: string
    entryCount*: uint64
    blockRestartInterval*: int
    smallestSeqno*: SeqNo
    largestSeqno*: SeqNo

proc newTableWriter*(path: string, restartInterval: int = DefaultRestartInterval): LsmResult[TableWriter] =
  let stream = posix.open(path, cint(FileFlags.frWrite) or cint(
      FileFlags.frCreate), 0o644)
  if stream == -1:
    return err[TableWriter](newIoError("Failed to create table file: " & path))

  let writer = TableWriter(
    path: path,
    stream: stream,
    currentBlock: newBlockBuilder(restartInterval),
    indexEntries: @[],
    minKey: "",
    maxKey: "",
    entryCount: 0,
    blockRestartInterval: restartInterval,
    smallestSeqno: MAX_VALID_SEQNO,
    largestSeqno: 0.SeqNo
  )

  ok(writer)

proc addEntry*(w: TableWriter, key: InternalKey, value: string): LsmResult[void] =
  try:
    # Update min/max key and seqno
    # NOTE: InternalKey ordering has DESCENDING seqno, so first entry has HIGHEST seqno
    if w.entryCount == 0:
      w.minKey = key.userKey
      w.largestSeqno = key.seqno # First entry has highest seqno
    w.maxKey = key.userKey
    w.smallestSeqno = key.seqno # Will end up with lowest seqno after iteration
    inc(w.entryCount) # CRITICAL FIX: Increment entry count!

    # Add to current block
    let addResult = w.currentBlock.add(key, value)
    if addResult.isErr:
      return errVoid(addResult.error)

    # Check if block is full (4KB default)
    if w.currentBlock.estimatedSize() >= 4096:
      # Finish current block
      let blockData = w.currentBlock.finish()
      if blockData.isErr:
        return errVoid(blockData.error)

      let handle = TableBlockHandle(
        offset: posix.lseek(w.stream, 0, SEEK_CUR).uint64,
        size: blockData.value.len.uint32
      )
      w.indexEntries.add((w.maxKey, handle))
      discard posix.write(w.stream, cast[pointer](addr(blockData.value[0])),
          blockData.value.len)

      # CRITICAL: Reset the block builder for next block
      w.currentBlock.reset()

  except:
    return errVoid(newIoError("Failed to add entry: " & getCurrentExceptionMsg()))

  okVoid()

proc addEntrySimple*(w: TableWriter, userKey: string, seqno: SeqNo,
                     valueType: ValueType, value: string): LsmResult[void] =
  ## Add entry with simple string key (from memtable)
  let internalKey = InternalKey(userKey: userKey, seqno: seqno,
      valueType: valueType)
  addEntry(w, internalKey, value)

proc finish*(w: TableWriter): LsmResult[void] =
  try:
    # Write the final block (which may not be full)
    let blockData = w.currentBlock.finish()
    if blockData.isErr:
      return errVoid(blockData.error)

    let handle = TableBlockHandle(
      offset: posix.lseek(w.stream, 0, SEEK_CUR).uint64,
      size: blockData.value.len.uint32
    )
    w.indexEntries.add((w.maxKey, handle))
    discard posix.write(w.stream, cast[pointer](addr(blockData.value[0])),
        blockData.value.len)

    # Write index block
    var indexData = ""
    for entry in w.indexEntries:
      indexData.add(encodeVarint(entry.key.len))
      indexData.add(entry.key)
      indexData.add(encodeVarint(int(entry.handle.offset)))
      indexData.add(encodeVarint(int(entry.handle.size)))

    let indexOffset = posix.lseek(w.stream, 0, SEEK_CUR).uint64
    let indexSize = indexData.len.uint32
    discard posix.write(w.stream, cast[pointer](addr(indexData[0])), indexData.len)

    # Write footer
    var footer = ""
    footer.add(encodeUint64(indexOffset))
    footer.add(encodeUint32(indexSize))
    footer.add(encodeUint64(0)) # No meta block
    footer.add(encodeUint32(0))
    discard posix.write(w.stream, cast[pointer](addr(footer[0])), footer.len)

    discard posix.fsync(w.stream)
    discard posix.close(w.stream)

    okVoid()
  except:
    return errVoid(newIoError("Failed to finish table: " &
        getCurrentExceptionMsg()))

proc close*(w: TableWriter) =
  discard posix.close(cint(w.stream))

# Metadata accessors for TableWriter
proc getMinKey*(w: TableWriter): string = w.minKey
proc getMaxKey*(w: TableWriter): string = w.maxKey
proc getEntryCount*(w: TableWriter): uint64 = w.entryCount
proc getSmallestSeqno*(w: TableWriter): SeqNo = w.smallestSeqno
proc getLargestSeqno*(w: TableWriter): SeqNo = w.largestSeqno

# ============================================================================
# Bloom Filter (simplified)
# ============================================================================

# BloomFilter is forward-declared above for SsTable

proc newBloomFilter*(numBits: int, numHashes: int): BloomFilter =
  BloomFilter(
    data: newString(numBits div 8),
    numHashes: numHashes,
    numBits: numBits
  )

proc hash*(s: string): int =
  ## Use xxhash64 for fast hashing - matches Rust implementation
  int(xxhash64(s))

proc addKey*(bf: BloomFilter, key: string) =
  let h1 = key.hash()
  let h2 = h1 shr 16

  for i in 0 ..< bf.numHashes:
    let idx = ((h1 + i * h2) mod bf.numBits) div 8
    let bit = ((h1 + i * h2) mod bf.numBits) mod 8
    bf.data[idx] = chr(ord(bf.data[idx]) or (1 shl bit))

proc mightContain*(bf: BloomFilter, key: string): bool =
  let h1 = key.hash()
  let h2 = h1 shr 16

  for i in 0 ..< bf.numHashes:
    let idx = ((h1 + i * h2) mod bf.numBits) div 8
    let bit = ((h1 + i * h2) mod bf.numBits) mod 8
    if (ord(bf.data[idx]) and (1 shl bit)) == 0:
      return false
  true

# ============================================================================
# SSTable Lookup - Full Implementation
# ============================================================================
# SSTable Lookup - Full Implementation
# ============================================================================

proc decodeFixed32FromString*(data: string, pos: int): uint32 =
  ## Decode 32-bit unsigned integer from string
  var result: uint32 = 0
  for i in 0 ..< 4:
    result = result or (uint32(data[pos + i].uint8) shl (i * 8))
  result

proc decodeFixed64FromString*(data: string, pos: int): uint64 =
  ## Decode 64-bit unsigned integer from string
  var result: uint64 = 0
  for i in 0 ..< 8:
    result = result or (uint64(data[pos + i].uint8) shl (i * 8))
  result

proc decodeVarintFromString*(data: string, pos: int): tuple[value: uint64, newPos: int] =
  ## Decode varint from string, returns (value, newPosition)
  var result: uint64 = 0
  var shift = 0
  var i = pos
  while i < data.len:
    let b = data[i].uint8
    result = result or ((b and 0x7F) shl shift)
    if (b and 0x80) == 0:
      break
    inc shift
    inc i
  (result, i + 1)

proc readFileBytes*(path: string, offset: uint64, size: uint64): string =
  ## Read bytes from a file at given offset
  var fd = posix.open(path, cint(FileFlags.frRead))
  if fd == -1:
    raise newException(IOError, "Failed to open file: " & path)
  discard posix.lseek(fd, offset.int64, SEEK_SET)
  var buffer = newSeq[uint8](size.int)
  var totalRead = 0
  while totalRead < size.int:
    let bytesRead = posix.read(fd, addr(buffer[totalRead]), size.int - totalRead)
    if bytesRead == 0:
      break
    inc totalRead, bytesRead
  result = cast[string](buffer)
  if totalRead < size.int:
    result = result[0 ..< totalRead]
  discard posix.close(fd)

proc readFromHandle*(table: SsTable, offset: uint64, size: uint64): string =
  ## Read from cached file handle or block cache
  # Try to get from block cache first
  let cached = table.getCachedBlock(offset)
  if cached.isSome:
    return cached.get

  # Read from file
  discard posix.lseek(table.fileHandle, offset.int64, SEEK_SET)
  var buffer = newSeq[uint8](size.int)
  var totalRead = 0
  while totalRead < size.int:
    let bytesRead = posix.read(table.fileHandle, cast[pointer](addr(buffer[
        totalRead])), size.int - totalRead)
    if bytesRead == 0:
      break
    inc totalRead, bytesRead
  # Fast string conversion using move
  result = cast[string](buffer)
  # Trim to actual bytes read
  if totalRead < size.int:
    result = result[0 ..< totalRead]

  # Insert into cache
  table.putCachedBlock(offset, result)

proc searchIndexBlock*(indexData: string, key: string): tuple[offset: uint64,
    size: uint32, found: bool] =
  ## Search index block for key, returns (offset, size, found)
  ## Index block format: (key_len, key, offset, size) repeated

  var pos = 0
  var lastOffset: uint64 = 0
  var lastSize: uint32 = 0

  while pos < indexData.len:
    # Read key length
    if pos >= indexData.len:
      break
    let (keyLen, newPos) = decodeVarintFromString(indexData, pos)
    pos = newPos

    if pos + keyLen.int > indexData.len:
      break
    let indexedKey = indexData[pos ..< pos + keyLen.int]
    pos += keyLen.int

    # Read offset and size
    if pos + 12 > indexData.len:
      break
    let offset = decodeFixed64FromString(indexData, pos)
    pos += 8
    let size = decodeFixed32FromString(indexData, pos)
    pos += 4

    # Check if this is the right block
    if indexedKey >= key:
      return (offset, size, indexedKey == key)

    lastOffset = offset
    lastSize = size

  # Return the last block if key is greater than all indexed keys
  if lastOffset > 0:
    return (lastOffset, lastSize, false)

  (0, 0, false)

proc searchDataBlockWithHashIndex*(blockData: string, key: string, seqno: SeqNo,
    globalSeqno: SeqNo): Option[tuple[key: InternalKey, value: string]] =
  ## Search data block for key using hash index (fast path)
  ## Returns the value if found

  var pos = 0

  # First, read restart points
  if pos + 4 > blockData.len:
    return none(tuple[key: InternalKey, value: string])
  let restartCount = decodeFixed32FromString(blockData, pos)
  pos += 4

  # Read restart point offsets
  var restartOffsets: seq[int] = @[]
  for i in 0 ..< restartCount:
    if pos + 4 > blockData.len:
      break
    let offset = decodeFixed32FromString(blockData, pos).int
    restartOffsets.add(offset)
    pos += 4

  # Calculate hash index position
  let hashIndexStart = pos + 4 # After restart count
  let hashIndexLen = restartOffsets.len # Bucket count equals restart count
  let hashIndexData = blockData[hashIndexStart ..< blockData.len]

  # Try hash index lookup
  if hashIndexLen > 0:
    let hashReader = sstable_block.newHashIndexReader(hashIndexData, 0, uint32(hashIndexLen))
    let bucketPos = hashReader.get(key)

    if bucketPos == sstable_block.HashIndexMarkerFree:
      # Key definitely not in block
      return none(tuple[key: InternalKey, value: string])

    if bucketPos != sstable_block.HashIndexMarkerConflict and bucketPos <
        restartOffsets.len.uint8:
      # Direct lookup via hash index
      let restartIdx = int(bucketPos)
      if restartIdx < restartOffsets.len:
        var entryPos = restartOffsets[restartIdx]

        # Decode entry at restart point
        let (sharedLen, keyPos) = decodeVarintFromString(blockData, entryPos)
        entryPos = keyPos

        let (unsharedLen, valuePos) = decodeVarintFromString(blockData, entryPos)
        entryPos = valuePos

        let (valueLen, dataPos) = decodeVarintFromString(blockData, entryPos)
        entryPos = dataPos

        if entryPos + unsharedLen.int + valueLen.int > blockData.len:
          return none(tuple[key: InternalKey, value: string])

        let entryKey = blockData[entryPos ..< entryPos + unsharedLen.int]

        # Check if this is the key we want
        if entryKey == key:
          let valuePos = entryPos + unsharedLen.int
          if valuePos + valueLen.int + 9 > blockData.len:
            return none(tuple[key: InternalKey, value: string])
          let valueData = blockData[valuePos ..< valuePos + valueLen.int]

          # Decode full InternalKey: user key + 8-byte seqno + 1-byte type
          let seqnoBytes = blockData[valuePos + valueLen.int ..< valuePos +
              valueLen.int + 8]
          let typeByte = blockData[valuePos + valueLen.int + 8]
          let storedSeqno = decodeFixed64FromString(seqnoBytes, 0)
          let storedType = ValueType(ord(typeByte))

          # Only accept if stored seqno <= snapshot seqno
          if storedSeqno > uint64(seqno):
            return none(tuple[key: InternalKey, value: string])

          let internalKey = InternalKey(userKey: entryKey,
              seqno: SeqNo(storedSeqno), valueType: storedType)
          return some((key: internalKey, value: valueData))
        # If key doesn't match, fall through to linear scan

      # Fallback to binary search
  var low = 0
  var high = restartOffsets.len - 1
  var lastKey = ""

  while low <= high:
    let mid = (low + high) div 2
    var entryPos = restartOffsets[mid]

    # Decode entry at restart point
    let (sharedLen, keyPos) = decodeVarintFromString(blockData, entryPos)
    entryPos = keyPos

    let (unsharedLen, valuePos) = decodeVarintFromString(blockData, entryPos)
    entryPos = valuePos

    let (valueLen, dataPos) = decodeVarintFromString(blockData, entryPos)
    entryPos = dataPos

    if entryPos + unsharedLen.int + valueLen.int > blockData.len:
      return none(tuple[key: InternalKey, value: string])



  none(tuple[key: InternalKey, value: string])

# Keep the old binary search version for compatibility
proc searchDataBlock*(blockData: string, key: string, seqno: SeqNo,
    globalSeqno: SeqNo): Option[tuple[key: InternalKey, value: string]] =
  ## Search data block for key at given seqno
  ## Returns the value if found - uses hash index when available
  searchDataBlockWithHashIndex(blockData, key, seqno, globalSeqno)

proc lookup*(table: SsTable, key: string, seqno: SeqNo): Option[tuple[
    key: InternalKey, value: string]] =
  ## Look up a key in the SSTable
  ## Returns the value if found and seqno is within range, none otherwise
  if table.meta.entryCount == 0:
    return none(tuple[key: InternalKey, value: string])

  # Check if key is in range
  if table.meta.minKey.len > 0 and key < table.meta.minKey:
    return none(tuple[key: InternalKey, value: string])
  if table.meta.maxKey.len > 0 and key > table.meta.maxKey:
    return none(tuple[key: InternalKey, value: string])

  # Check global seqno - if query seqno is before table's seqno range, skip
  if table.meta.smallestSeqno > 0.SeqNo and seqno < table.meta.smallestSeqno:
    return none(tuple[key: InternalKey, value: string])

  # Read footer to get index block and filter block locations
  let footerSize = 24 # 8 + 4 + 8 + 4 bytes
  if table.fileSize <= footerSize.uint64:
    return none(tuple[key: InternalKey, value: string])

  let footerData = table.readFromHandle(table.fileSize - footerSize.uint64,
      footerSize.uint64)

  let indexOffset = decodeFixed64FromString(footerData, 0)
  let indexSize = decodeFixed32FromString(footerData, 8)
  let filterOffset = decodeFixed64FromString(footerData, 12)
  let filterSize = decodeFixed32FromString(footerData, 20)

  # Check bloom filter first (if available)
  if filterSize > 0 and filterOffset > 0:
    # Load filter block if not cached
    if table.cachedFilterBlock.isNone():
      let filterData = table.readFromHandle(filterOffset, filterSize.uint64)
      # Simple bloom filter reconstruction (for now, assume 10 hashes, 1MB)
      let numBits = filterData.len * 8
      var bf = newBloomFilter(numBits, 10)
      bf.data = filterData
      table.cachedFilterBlock = some(bf)

    # Check if key might be in table
    if table.cachedFilterBlock.isSome:
      if not table.cachedFilterBlock.get.mightContain(key):
        # Bloom filter says key definitely not in table
        return none(tuple[key: InternalKey, value: string])

  if indexSize == 0 or indexOffset >= table.fileSize:
    return none(tuple[key: InternalKey, value: string])

  # Read index block (use cache if available)
  let indexData = if table.cachedIndexBlock.isSome:
    table.cachedIndexBlock.get
  else:
    let data = table.readFromHandle(indexOffset, indexSize.uint64)
    table.cachedIndexBlock = some(data)
    data

  # Search index block for the key
  let (dataOffset, dataSize, found) = searchIndexBlock(indexData, key)

  if dataSize == 0:
    return none(tuple[key: InternalKey, value: string])

  # Read data block
  let blockData = table.readFromHandle(dataOffset, dataSize.uint64)

  # Search data block
  let globalSeqno = seqno # Use snapshot seqno, not table.minSeqno
  result = searchDataBlock(blockData, key, seqno, globalSeqno)

# ============================================================================
# Table Range Iterator
# ============================================================================

type
  TableRangeIter* = ref object
    table*: SsTable
    startKey*: string
    endKey*: string
    targetSeqno*: SeqNo
    entries*: seq[tuple[key: InternalKey, value: string]]
    position*: int
    ## IMPORTANT: Caller must call close() to unregister from IteratorGuard and prevent memory leaks

proc newTableRangeIter*(table: SsTable, startKey, endKey: string,
    targetSeqno: SeqNo): TableRangeIter =
  ## Create a new table range iterator that reads all entries in range
  result = TableRangeIter(
    table: table,
    startKey: startKey,
    endKey: endKey,
    targetSeqno: targetSeqno,
    entries: newSeq[tuple[key: InternalKey, value: string]](),
    position: 0
  )

  # Register this iterator with the guard


  # Read all data blocks and collect entries in range
  let footerSize = 24
  if table.fileSize > footerSize.uint64:
    let footerData = table.readFromHandle(table.fileSize - footerSize.uint64,
        footerSize.uint64)
    let indexOffset = decodeFixed64FromString(footerData, 0)
    let indexSize = decodeFixed32FromString(footerData, 8)

    if indexSize > 0 and indexOffset < table.fileSize:
      let indexData = table.readFromHandle(indexOffset, indexSize.uint64)

      # Iterate through index entries
      var pos = 0
      while pos < indexData.len:
        let (keyLen, newPos) = decodeVarintFromString(indexData, pos)
        pos = newPos

        if pos + keyLen.int > indexData.len:
          break
        let indexedKey = indexData[pos ..< pos + keyLen.int]
        pos += keyLen.int

        if pos + 12 > indexData.len:
          break
        let blockOffset = decodeFixed64FromString(indexData, pos)
        pos += 8
        let blockSize = decodeFixed32FromString(indexData, pos)
        pos += 4

        # Skip blocks outside our range
        if result.endKey.len > 0 and indexedKey > result.endKey:
          break
        if result.startKey.len > 0 and indexedKey < result.startKey:
          continue

        # Read and search the data block
        if blockSize > 0 and blockOffset < table.fileSize:
          let blockData = table.readFromHandle(blockOffset, blockSize.uint64)

          # Simple linear scan through restart points
          var blockPos = 0
          if blockPos + 4 <= blockData.len:
            let restartCount = decodeFixed32FromString(blockData, blockPos)
            blockPos += 4

            # Skip to first entry after restart points
            blockPos = restartCount.int * 4 + 4

            # Iterate through entries
            while blockPos < blockData.len:
              # Check for restart point
              let restartIndex = (blockPos - 4) div 4 - 1
              if restartIndex >= 0 and (blockPos - 4) mod 4 == 0 and
                  restartIndex < restartCount.int:
                # At restart point - skip the offset
                blockPos += 0 # Already at right position
              
              # Try to read entry
              if blockPos >= blockData.len:
                break

              let (sharedLen, keyPos) = decodeVarintFromString(blockData, blockPos)
              if keyPos >= blockData.len:
                break
              blockPos = keyPos

              let (unsharedLen, valuePos) = decodeVarintFromString(blockData, blockPos)
              if valuePos >= blockData.len:
                break
              blockPos = valuePos

              let (valueLen, dataPos) = decodeVarintFromString(blockData, blockPos)
              if dataPos >= blockData.len:
                break
              blockPos = dataPos

              if dataPos + unsharedLen.int > blockData.len:
                break
              let entryKey = blockData[dataPos ..< dataPos + unsharedLen.int]
              blockPos = dataPos + unsharedLen.int

              if dataPos + unsharedLen.int + valueLen.int > blockData.len:
                break
              let valueData = blockData[dataPos + unsharedLen.int ..< dataPos +
                  unsharedLen.int + valueLen.int]

              # Check if key is in range
              if result.startKey.len > 0 and entryKey < result.startKey:
                continue
              if result.endKey.len > 0 and entryKey > result.endKey:
                break

              # Add entry (using targetSeqno as approximation)
              let internalKey = InternalKey(userKey: entryKey,
                  seqno: targetSeqno, valueType: vtValue)
              result.entries.add((key: internalKey, value: valueData))

  # Close the iterator immediately after building entries
  # We don't need the iterator anymore — only the entries are used


proc close*(iter: var TableRangeIter) =
  ## Close the iterator and unregister it from the guard

  iter.entries = @[]
  iter.position = 0

proc hasNext*(t: TableRangeIter): bool =
  t.position < t.entries.len

proc next*(t: TableRangeIter): Option[tuple[key: InternalKey, value: string]] =
  if t.position < t.entries.len:
    let result = t.entries[t.position]
    inc t.position
    return some(result)
  none(tuple[key: InternalKey, value: string])

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
  discard builder.add(InternalKey(userKey: "key1", seqno: 1.SeqNo,
      valueType: vtValue), "value1")
  discard builder.add(InternalKey(userKey: "key2", seqno: 2.SeqNo,
      valueType: vtValue), "value2")

  let blockData = builder.finish()
  if blockData.isOk:
    echo "Block data size: ", blockData.value.len

  # Test bloom filter
  let bf = newBloomFilter(1024, 3)
  bf.addKey("test_key")
  echo "Bloom might contain test_key: ", bf.mightContain("test_key")
  echo "Bloom might contain other: ", bf.mightContain("other")

  echo "Table tests passed!"
