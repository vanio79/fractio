# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Block
##
## SSTable block structures with hash index, compression, and checksum.

import std/[streams, options]
import types
import error
import hash
import coding

# ============================================================================
# Block Type
# ============================================================================

type
  BlockKind* = enum
    bkData = 0
    bkIndex = 1
    bkFilter = 2
    bkMeta = 3

# ============================================================================
# Compression Type (extended)
# ============================================================================

type
  CompressionType* = enum
    ctNone = 0
    ctLz4 = 1

# ============================================================================
# Block Header
# ============================================================================

type
  BlockHeader* = ref object
    blockType*: BlockKind
    checksum*: uint64 ## 128-bit checksum stored as u64 (low bits)
    dataLength*: uint32
    uncompressedLength*: uint32

proc serializedLen*(h: BlockHeader): int = 20

# ============================================================================
# Block Decoder
# ============================================================================

type
  BlockDecoder* = ref object
    data*: string
    header*: BlockHeader

proc newBlockDecoder*(data: string): BlockDecoder =
  BlockDecoder(data: data, header: nil)

# ============================================================================
# Block Encoder
# ============================================================================

type
  BlockEncoder* = ref object
    buffer*: string
    blockType*: BlockKind
    compression*: CompressionType

proc newBlockEncoder*(blockType: BlockKind): BlockEncoder =
  BlockEncoder(buffer: "", blockType: blockType, compression: ctNone)

# ============================================================================
# Block Trailer
# ============================================================================

type
  BlockTrailer* = ref object
    compression*: CompressionType
    checksum*: uint32

const
  BlockTrailerSize* = 5
  TrailerStartMarker* = 0x02'u8

# ============================================================================
# Binary Index
# ============================================================================

type
  BinaryIndex* = ref object
    entries*: seq[tuple[key: string, offset: uint32]]

# ============================================================================
# Hash Index - matches Rust lsm-tree implementation
# ============================================================================

const
  HashIndexMarkerFree*: uint8 = 254     # MARKER_FREE = u8::MAX - 1
  HashIndexMarkerConflict*: uint8 = 255 # MARKER_CONFLICT = u8::MAX

type
  HashIndexBuilder* = ref object
    buckets*: seq[uint8]
    bucketCount*: uint32

  HashIndexReader* = ref object
    data*: string
    offset*: uint32
    len*: uint32

proc newHashIndexBuilder*(bucketCount: uint32): HashIndexBuilder =
  var buckets = newSeq[uint8](bucketCount.int)
  for i in 0 ..< buckets.len:
    buckets[i] = HashIndexMarkerFree
  HashIndexBuilder(
    buckets: buckets,
    bucketCount: bucketCount
  )

proc calculateBucketPosition*(key: string, bucketCount: uint32): int =
  ## Calculate bucket position for a key using hash
  let h = xxhash64(key)
  int(h mod uint64(bucketCount))

proc set*(b: HashIndexBuilder, key: string, binaryIndexPos: uint8): bool =
  ## Map a key to binary index position. Returns true if set successfully.
  if b.bucketCount == 0:
    return false

  let pos = calculateBucketPosition(key, b.bucketCount)
  let curr = b.buckets[pos]

  if curr == HashIndexMarkerConflict:
    return false
  elif curr == HashIndexMarkerFree:
    b.buckets[pos] = binaryIndexPos
    return true
  elif curr == binaryIndexPos:
    return true
  else:
    b.buckets[pos] = HashIndexMarkerConflict
    return false

proc intoInner*(b: HashIndexBuilder): string =
  ## Convert builder to raw bytes
  result = newString(b.buckets.len)
  for i in 0 ..< b.buckets.len:
    result[i] = chr(b.buckets[i])

proc newHashIndexReader*(data: string, offset, len: uint32): HashIndexReader =
  HashIndexReader(data: data, offset: offset, len: len)

proc bucketCount*(r: HashIndexReader): int = int(r.len)

proc get*(r: HashIndexReader, key: string): uint8 =
  ## Get binary index position for key, or marker if not found/conflicted
  if r.len == 0:
    return HashIndexMarkerFree

  let pos = calculateBucketPosition(key, r.len)
  if pos < r.data.len:
    return uint8(r.data[r.offset.int + pos])
  return HashIndexMarkerFree

proc conflictCount*(r: HashIndexReader): int =
  ## Count number of conflicts in hash index
  var count = 0
  for i in 0 ..< int(r.len):
    if uint8(r.data[r.offset.int + i]) == HashIndexMarkerConflict:
      inc count
  return count

# ============================================================================
# Block Offset
# ============================================================================

type
  BlockOffset* = uint64

# ============================================================================
# Block Handle (matches Rust lsm-tree)
# ============================================================================

type
  BlockHandle* = ref object
    offset*: BlockOffset
    size*: uint32

proc newBlockHandle*(offset: BlockOffset, size: uint32): BlockHandle =
  BlockHandle(offset: offset, size: size)

# ============================================================================
# Keyed Block Handle - Block handle with key metadata
# ============================================================================

type
  KeyedBlockHandle* = ref object
    endKey*: string ## Last key in the block
    seqno*: SeqNo   ## Sequence number
    blockHandle*: BlockHandle

proc newKeyedBlockHandle*(endKey: string, seqno: SeqNo,
                          offset: BlockOffset, size: uint32): KeyedBlockHandle =
  KeyedBlockHandle(
    endKey: endKey,
    seqno: seqno,
    blockHandle: newBlockHandle(offset, size)
  )

proc offset*(k: KeyedBlockHandle): BlockOffset = k.blockHandle.offset
proc size*(k: KeyedBlockHandle): uint32 = k.blockHandle.size

proc shift*(k: KeyedBlockHandle, base: BlockOffset) =
  ## Shift the offset by adding base offset
  k.blockHandle.offset = k.blockHandle.offset + base

# ============================================================================
# Index Block - encodes/decodes KeyedBlockHandle sequences
# ============================================================================

proc encodeIndexBlock*(handles: seq[KeyedBlockHandle]): string =
  ## Encode a sequence of KeyedBlockHandle into index block format
  result = ""
  for h in handles:
    # Encode key length (varint)
    result.add(encodeVarint(uint64(h.endKey.len)))
    # Encode key data
    if h.endKey.len > 0:
      result.add(h.endKey)
    # Encode seqno (varint)
    result.add(encodeVarint(uint64(h.seqno)))
    # Encode offset (varint)
    result.add(encodeVarint(h.blockHandle.offset))
    # Encode size (varint)
    result.add(encodeVarint(uint64(h.blockHandle.size)))

proc decodeIndexBlock*(data: string): seq[KeyedBlockHandle] =
  ## Decode index block data into KeyedBlockHandle sequence
  result = @[]
  var pos = 0

  while pos < data.len:
    # Read key length
    if pos >= data.len:
      break
    let (keyLen, newPos) = decodeVarintFromString(data, pos)
    pos = newPos

    # Read key data
    var keyData = ""
    if keyLen > 0:
      if pos + keyLen.int > data.len:
        break
      keyData = data[pos ..< pos + keyLen.int]
      pos += keyLen.int

    # Read seqno
    if pos >= data.len:
      break
    let (seqno, seqnoPos) = decodeVarintFromString(data, pos)
    pos = seqnoPos

    # Read offset
    if pos >= data.len:
      break
    let (offset, offsetPos) = decodeVarintFromString(data, pos)
    pos = offsetPos

    # Read size
    if pos >= data.len:
      break
    let (size, sizePos) = decodeVarintFromString(data, pos)
    pos = sizePos

    # Create handle
    result.add(newKeyedBlockHandle(keyData, SeqNo(seqno), BlockOffset(offset), size.uint32))

# ============================================================================
# Partitioned Index Writer
# ============================================================================

const
  DefaultPartitionSize*: uint32 = 4096

type
  PartitionedIndexWriter* = ref object
    relativeFilePos*: uint64
    compression*: CompressionType
    tliHandles*: seq[KeyedBlockHandle]       # Top Level Index entries
    dataBlockHandles*: seq[KeyedBlockHandle] # Current partition entries
    bufferSize*: uint32                      # Current partition size
    partitionSize*: uint32                   # Threshold to cut partition
    indexBlockCount*: int
    blockBuffer*: string                     # Current partition being built
    finalWriteBuffer*: string                # All completed partitions

proc newPartitionedIndexWriter*(): PartitionedIndexWriter =
  PartitionedIndexWriter(
    relativeFilePos: 0,
    bufferSize: 0,
    indexBlockCount: 0,
    partitionSize: DefaultPartitionSize,
    compression: ctNone,
    tliHandles: @[],
    dataBlockHandles: @[],
    blockBuffer: "",
    finalWriteBuffer: ""
  )

proc usePartitionSize*(w: PartitionedIndexWriter,
    size: uint32): PartitionedIndexWriter =
  w.partitionSize = size
  w

proc useCompression*(w: PartitionedIndexWriter,
    compression: CompressionType): PartitionedIndexWriter =
  w.compression = compression
  w

proc cutIndexBlock*(w: PartitionedIndexWriter): LsmResult[void] =
  ## Cut the current index partition and start a new one
  try:
    # Encode current data block handles into index block
    let indexData = encodeIndexBlock(w.dataBlockHandles)

    # Calculate bytes written (block header + data)
    let headerLen = 20 # block header size
    let bytesWritten = (headerLen + indexData.len).uint32

    # Get the last key (for TLI entry)
    if w.dataBlockHandles.len == 0:
      return okVoid()

    let last = w.dataBlockHandles[^1]

    # Create TLI entry
    let tliEntry = newKeyedBlockHandle(
      last.endKey,
      last.seqno,
      BlockOffset(w.relativeFilePos),
      bytesWritten
    )
    w.tliHandles.add(tliEntry)

    # Add to final buffer
    w.finalWriteBuffer.add(w.blockBuffer)
    w.finalWriteBuffer.add(indexData)

    # Update metadata
    w.indexBlockCount += 1
    w.relativeFilePos += uint64(bytesWritten)

    # Clear buffer for next partition
    w.dataBlockHandles = @[]
    w.bufferSize = 0
    w.blockBuffer = ""

    okVoid()
  except:
    errVoid(newIoError("Failed to cut index block: " & getCurrentExceptionMsg()))

proc registerDataBlock*(w: PartitionedIndexWriter,
                        endKey: string, seqno: SeqNo,
                        offset: BlockOffset, size: uint32): LsmResult[void] =
  ## Register a data block with the index writer
  try:
    let handle = newKeyedBlockHandle(endKey, seqno, offset, size)

    # Calculate size contribution
    # key_len + key + seqno + offset + size (all varints)
    let keyLenSize = varintSize(endKey.len)
    let seqnoSize = varintSize(uint64(seqno))
    let offsetSize = varintSize(offset)
    let sizeSize = varintSize(size)
    let handleSize = keyLenSize + endKey.len + seqnoSize + offsetSize + sizeSize

    w.bufferSize += handleSize.uint32
    w.dataBlockHandles.add(handle)

    # Cut partition if threshold exceeded
    if w.bufferSize >= w.partitionSize:
      let cutResult = w.cutIndexBlock()
      if cutResult.isErr:
        return errVoid(cutResult.error)

    okVoid()
  except:
    errVoid(newIoError("Failed to register data block: " &
        getCurrentExceptionMsg()))

proc finishIndex*(w: PartitionedIndexWriter): LsmResult[tuple[indexData: string,
    indexBlockCount: int, tliHandles: seq[KeyedBlockHandle]]] =
  ## Finish writing index and return final data + metadata
  try:
    # Cut remaining partition if any
    if w.dataBlockHandles.len > 0:
      let cutResult = w.cutIndexBlock()
      if cutResult.isErr:
        return err[tuple[indexData: string, indexBlockCount: int,
            tliHandles: seq[KeyedBlockHandle]]](cutResult.error)

    # Build final index data = partitions + TLI
    var indexData = w.finalWriteBuffer

    # Encode TLI
    let tliData = encodeIndexBlock(w.tliHandles)
    indexData.add(tliData)

    ok((indexData: indexData, indexBlockCount: w.indexBlockCount,
        tliHandles: w.tliHandles))
  except:
    err[tuple[indexData: string, indexBlockCount: int, tliHandles: seq[
        KeyedBlockHandle]]](
      newIoError("Failed to finish index: " & getCurrentExceptionMsg()))

# ============================================================================
# Index Block Reader (for partitioned index lookup)
# ============================================================================

type
  IndexBlockReader* = ref object
    data*: string
    entries*: seq[KeyedBlockHandle]

proc newIndexBlockReader*(data: string): IndexBlockReader =
  let entries = decodeIndexBlock(data)
  IndexBlockReader(data: data, entries: entries)

proc numEntries*(r: IndexBlockReader): int = r.entries.len

proc findBlock*(r: IndexBlockReader, key: string): tuple[offset: BlockOffset,
    size: uint32] =
  ## Find the block handle for a key using binary search
  if r.entries.len == 0:
    return (0.BlockOffset, 0.uint32)

  var low = 0
  var high = r.entries.len - 1
  var resultOffset = r.entries[high].offset
  var resultSize = r.entries[high].size

  while low <= high:
    let mid = (low + high) div 2
    let entryKey = r.entries[mid].endKey

    if entryKey >= key:
      resultOffset = r.entries[mid].offset
      resultSize = r.entries[mid].size
      if entryKey == key:
        break
      high = mid - 1
    else:
      low = mid + 1

  return (resultOffset, resultSize)

proc entry*(r: IndexBlockReader, i: int): KeyedBlockHandle =
  r.entries[i]

# ============================================================================
# Top Level Index (for partitioned index)
# ============================================================================

type
  TopLevelIndex* = ref object
    entries*: seq[KeyedBlockHandle]

proc newTopLevelIndex*(entries: seq[KeyedBlockHandle]): TopLevelIndex =
  TopLevelIndex(entries: entries)

proc numPartitions*(t: TopLevelIndex): int = t.entries.len

proc findPartition*(t: TopLevelIndex, key: string): tuple[
    handle: KeyedBlockHandle, found: bool] =
  ## Find the index partition for a key
  if t.entries.len == 0:
    return (nil, false)

  var low = 0
  var high = t.entries.len - 1
  var resultHandle = t.entries[high]
  var found = false

  while low <= high:
    let mid = (low + high) div 2
    let entryKey = t.entries[mid].endKey

    if entryKey >= key:
      resultHandle = t.entries[mid]
      if entryKey == key:
        found = true
        break
      high = mid - 1
    else:
      low = mid + 1

  return (resultHandle, found)

proc partition*(t: TopLevelIndex, i: int): KeyedBlockHandle =
  t.entries[i]

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing block structures..."

  # Test hash index builder
  var hashIdx = newHashIndexBuilder(100)
  echo "Hash index with 100 buckets created"

  discard hashIdx.set("a", 5)
  discard hashIdx.set("b", 8)
  discard hashIdx.set("c", 10)

  let hashBytes = hashIdx.intoInner()
  echo "Hash index bytes: ", hashBytes.len

  # Test hash index reader
  var reader = newHashIndexReader(hashBytes, 0, 100)
  echo "Reader bucket count: ", reader.bucketCount()
  echo "Conflict count: ", reader.conflictCount()
  echo "Get 'a': ", reader.get("a")
  echo "Get 'b': ", reader.get("b")
  echo "Get 'd': ", reader.get("d")

  echo "Block tests passed!"
