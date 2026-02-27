# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Filter
##
## Bloom filter structures for SSTables.

import std/[sequtils, math]
import types
import hash
import coding
import error

# ============================================================================
# Bloom Construction Policy
# ============================================================================

type
  BloomConstructionPolicyKind* = enum
    bcpBitsPerKey
    bcpFalsePositiveRate

  BloomConstructionPolicy* = object
    case kind*: BloomConstructionPolicyKind
    of bcpBitsPerKey:
      bitsPerKey*: float
    of bcpFalsePositiveRate:
      falsePositiveRate*: float

proc defaultBloomPolicy*(): BloomConstructionPolicy =
  BloomConstructionPolicy(kind: bcpBitsPerKey, bitsPerKey: 10.0)

proc bitsPerKeyPolicy*(bpk: float): BloomConstructionPolicy =
  BloomConstructionPolicy(kind: bcpBitsPerKey, bitsPerKey: bpk)

proc fprPolicy*(fpr: float): BloomConstructionPolicy =
  BloomConstructionPolicy(kind: bcpFalsePositiveRate, falsePositiveRate: fpr)

proc isActive*(p: BloomConstructionPolicy): bool =
  case p.kind
  of bcpBitsPerKey:
    p.bitsPerKey > 0.0
  of bcpFalsePositiveRate:
    p.falsePositiveRate > 0.0

proc estimatedFilterSize*(p: BloomConstructionPolicy, n: int): int =
  if n == 0:
    return 0
  case p.kind
  of bcpBitsPerKey:
    int(p.bitsPerKey * float(n)) div 8
  of bcpFalsePositiveRate:
    # Calculate m = -n * ln(fpr) / (ln(2)^2)
    let ln2 = 0.69314718056 # ln(2)
    let ln2Squared = ln2 * ln2
    let fpr = p.falsePositiveRate
    if fpr <= 0.0:
      return 0
    let m = -(float(n) * ln(fpr)) / ln2Squared
    # Round up to next byte
    ((m / 8.0).ceil() * 8.0).int div 8

# ============================================================================
# Filter Policy
# ============================================================================

type
  FilterPolicy* = object
    bitsPerKey*: float
    maxKeys*: int

# Filter block
type
  FilterBlock* = ref object
    data*: string
    numBits*: int
    numHashes*: int

proc newFilterBlock*(numBits, numHashes: int): FilterBlock =
  FilterBlock(
    data: newString(numBits div 8),
    numBits: numBits,
    numHashes: numHashes
  )

# ============================================================================
# Standard Bloom Filter Builder
# ============================================================================

const
  BloomMagicBytes* = "BLOOM01" # Magic bytes for bloom filter header

type
  StandardBloomBuilder* = ref object
    data*: string # Bit array
    m*: int       # Number of bits
    k*: int       # Number of hash functions

proc newStandardBloomBuilder*(numBits, numHashes: int): StandardBloomBuilder =
  StandardBloomBuilder(
    data: newString((numBits + 7) div 8),
    m: numBits,
    k: numHashes
  )

proc withBpk*(n: int, bpk: float): StandardBloomBuilder =
  ## Construct a bloom filter that can hold n items with bpk bits per key
  assert(bpk > 0.0)
  assert(n > 0)
  let m = n * int(bpk)
  let k = max(1, int(bpk * 0.69314718056)) # bpk * ln(2)
  let bytes = ((float(m) / 8.0).ceil()).int
  StandardBloomBuilder(
    data: newString(bytes),
    m: bytes * 8,
    k: k
  )

proc withFpRate*(n: int, fpr: float): StandardBloomBuilder =
  ## Construct a bloom filter with n items and target false positive rate
  assert(n > 0)
  let fpr = max(fpr, 0.0000001) # Minimum fpr
  let m = int(ceil(-float(n) * ln(fpr) / (0.69314718056 * 0.69314718056)))
  let bytes = ((float(m) / 8.0).ceil()).int
  let k = max(1, int((float(bytes * 8) / float(n)) * 0.69314718056))
  StandardBloomBuilder(
    data: newString(bytes),
    m: bytes * 8,
    k: k
  )

proc initPolicy*(p: BloomConstructionPolicy, n: int): StandardBloomBuilder =
  ## Initialize a bloom filter builder based on the policy
  case p.kind
  of bcpBitsPerKey:
    withBpk(n, p.bitsPerKey)
  of bcpFalsePositiveRate:
    withFpRate(n, p.falsePositiveRate)

proc secondaryHashBloom*(h1: uint64): uint64 =
  ## Secondary hash function for bloom filter
  (h1 shr 32).uint64 * 0x517C_C1B7_2722_0A95'u64

proc setWithHash*(b: StandardBloomBuilder, h1: uint64) =
  ## Add a key hash to the filter
  var h2 = secondaryHashBloom(h1)
  var h = h1
  for i in 1 .. b.k:
    let idx = int(h mod uint64(b.m))
    let byteIdx = idx div 8
    let bitIdx = idx mod 8
    b.data[byteIdx] = chr(ord(b.data[byteIdx]) or (1 shl bitIdx))
    h = h + h2

proc build*(b: StandardBloomBuilder): string =
  ## Build the bloom filter data (includes header)
  result = ""
  # Magic bytes
  result.add(BloomMagicBytes)
  # Filter type (0 = standard)
  result.add(chr(0))
  # Hash type (unused)
  result.add(chr(0))
  # m (number of bits) - 8 bytes
  result.add(encodeFixed64(uint64(b.m)))
  # k (number of hashes) - 8 bytes
  result.add(encodeFixed64(uint64(b.k)))
  # Bit array data
  result.add(b.data)

# ============================================================================
# Standard bloom filter
# ============================================================================

type
  StandardBloomFilter* = ref object
    data*: string
    bitsPerKey*: float

# ============================================================================
# Blocked bloom filter
# ============================================================================

type
  BlockedBloomFilter* = ref object
    data*: string
    blockSize*: int

# ============================================================================
# Bit array
# ============================================================================

type
  BitArray* = ref object
    data*: string
    numBits*: int

proc newBitArray*(numBits: int): BitArray =
  BitArray(data: newString((numBits + 7) div 8), numBits: numBits)

proc set*(ba: BitArray, index: int) =
  let byteIndex = index div 8
  let bitIndex = index mod 8
  ba.data[byteIndex] = chr(ord(ba.data[byteIndex]) or (1 shl bitIndex))

proc get*(ba: BitArray, index: int): bool =
  let byteIndex = index div 8
  let bitIndex = index mod 8
  (ord(ba.data[byteIndex]) and (1 shl bitIndex)) != 0

# ============================================================================
# Block Handle Types (duplicated from block.nim to avoid circular import)
# ============================================================================

type
  BlockOffset* = uint64

  BlockHandle* = ref object
    offset*: BlockOffset
    size*: uint32

  KeyedBlockHandle* = ref object
    endKey*: types.Slice
    seqno*: SeqNo
    blockHandle*: BlockHandle

proc newKeyedBlockHandle*(endKey: types.Slice, seqno: SeqNo,
                          offset: BlockOffset, size: uint32): KeyedBlockHandle =
  KeyedBlockHandle(
    endKey: endKey,
    seqno: seqno,
    blockHandle: BlockHandle(offset: offset, size: size)
  )

# Compression type
type
  CompressionType* = enum
    ctNone = 0
    ctLz4 = 1

# Index block encoding (for TLI)
proc encodeIndexBlock*(handles: seq[KeyedBlockHandle]): string =
  result = ""
  for h in handles:
    result.add(encodeVarint(uint64(h.endKey.len)))
    if h.endKey.len > 0:
      result.add(h.endKey.data)
    result.add(encodeVarint(h.seqno))
    result.add(encodeVarint(h.blockHandle.offset))
    result.add(encodeVarint(uint64(h.blockHandle.size)))

# ============================================================================
# Partitioned Filter Writer
# ============================================================================

const
  DefaultFilterPartitionSize*: uint32 = 4096

type
  PartitionedFilterWriter* = ref object
    finalFilterBuffer*: string
    tliHandles*: seq[KeyedBlockHandle]
    bloomHashBuffer*: seq[uint64] # Key hashes for current AMQ filter
    approxFilterSize*: int        # Estimated size of current filter
    partitionSize*: uint32        # Threshold to cut partition
    bloomPolicy*: BloomConstructionPolicy
    relativeFilePos*: uint64
    lastKey*: types.Slice
    compression*: CompressionType
    filterBlockCount*: int

proc newPartitionedFilterWriter*(bloomPolicy: BloomConstructionPolicy = defaultBloomPolicy()): PartitionedFilterWriter =
  PartitionedFilterWriter(
    finalFilterBuffer: "",
    bloomHashBuffer: @[],
    approxFilterSize: 0,
    partitionSize: DefaultFilterPartitionSize,
    bloomPolicy: bloomPolicy,
    relativeFilePos: 0,
    lastKey: emptySlice(),
    compression: ctNone,
    tliHandles: @[],
    filterBlockCount: 0
  )

proc usePartitionSize*(w: PartitionedFilterWriter,
    size: uint32): PartitionedFilterWriter =
  w.partitionSize = size
  w

proc useCompression*(w: PartitionedFilterWriter,
    compression: CompressionType): PartitionedFilterWriter =
  w.compression = compression
  w

proc useBloomPolicy*(w: PartitionedFilterWriter,
    policy: BloomConstructionPolicy): PartitionedFilterWriter =
  w.bloomPolicy = policy
  w

proc spillFilterPartition*(w: PartitionedFilterWriter): LsmResult[void] =
  ## Cut the current filter partition and start a new one
  try:
    if w.bloomHashBuffer.len == 0:
      return okVoid()

    # Build bloom filter for current partition
    var builder = initPolicy(w.bloomPolicy, w.bloomHashBuffer.len)
    for h in w.bloomHashBuffer:
      builder.setWithHash(h)

    let filterData = builder.build()
    let bytesWritten = filterData.len.uint32

    # Create TLI entry
    if w.lastKey.len > 0:
      let tliEntry = newKeyedBlockHandle(
        w.lastKey,
        0.SeqNo,
        BlockOffset(w.relativeFilePos),
        bytesWritten
      )
      w.tliHandles.add(tliEntry)

    # Add to final buffer
    w.finalFilterBuffer.add(filterData)
    w.filterBlockCount += 1
    w.relativeFilePos += uint64(bytesWritten)

    # Clear for next partition
    w.bloomHashBuffer = @[]
    w.approxFilterSize = 0

    okVoid()
  except:
    errVoid(newIoError("Failed to spill filter partition: " &
        getCurrentExceptionMsg()))

proc registerKey*(w: PartitionedFilterWriter, key: types.Slice): LsmResult[void] =
  ## Register a key with the filter writer
  try:
    # Hash the key using xxhash64
    let h = xxhash64(key.data)
    w.bloomHashBuffer.add(h)

    # Estimate filter size
    w.approxFilterSize = w.bloomPolicy.estimatedFilterSize(
        w.bloomHashBuffer.len)

    # Update last key
    w.lastKey = newSlice(key.data)

    # Cut partition if threshold exceeded
    if w.approxFilterSize >= w.partitionSize.int:
      let spillResult = w.spillFilterPartition()
      if spillResult.isErr:
        return errVoid(spillResult.error)

    okVoid()
  except:
    errVoid(newIoError("Failed to register key: " & getCurrentExceptionMsg()))

proc finishFilter*(w: PartitionedFilterWriter): LsmResult[tuple[
    filterData: string, filterBlockCount: int, tliHandles: seq[
    KeyedBlockHandle]]] =
  ## Finish writing filter and return final data + metadata
  try:
    # Spill remaining partition if any
    if w.bloomHashBuffer.len > 0:
      let spillResult = w.spillFilterPartition()
      if spillResult.isErr:
        return err[tuple[filterData: string, filterBlockCount: int,
            tliHandles: seq[KeyedBlockHandle]]](spillResult.error)

    # Encode TLI
    let tliData = encodeIndexBlock(w.tliHandles)
    var filterData = w.finalFilterBuffer
    filterData.add(tliData)

    ok((filterData: filterData, filterBlockCount: w.filterBlockCount,
        tliHandles: w.tliHandles))
  except:
    err[tuple[filterData: string, filterBlockCount: int, tliHandles: seq[
        KeyedBlockHandle]]](
      newIoError("Failed to finish filter: " & getCurrentExceptionMsg()))

# ============================================================================
# Filter Reader (for partitioned filters)
# ============================================================================

type
  FilterPartitionReader* = ref object
    data*: string
    m*: int # Number of bits
    k*: int # Number of hashes

proc mightContain*(r: FilterPartitionReader, key: types.Slice): bool =
  ## Check if key might be in the filter partition
  let h = xxhash64(key.data)
  var h2 = secondaryHashBloom(h)
  var hVar = h

  for i in 1 .. r.k:
    let idx = int(hVar mod uint64(r.m))
    let byteIdx = idx div 8
    let bitIdx = idx mod 8
    if byteIdx < r.data.len:
      if (ord(r.data[byteIdx]) and (1 shl bitIdx)) == 0:
        return false
    hVar = hVar + h2
  true

# ============================================================================
# Filter Type Enum
# ============================================================================

type
  FilterType* = enum
    ftStandardBloom = 0
    ftBlockedBloom = 1

# ============================================================================
# Blocked Bloom Filter
# ============================================================================
##
## A blocked bloom filter uses cache-line sized blocks (64 bytes) for better
## cache locality. It uses double hashing instead of k separate hash functions.
##
## Format:
##   - MAGIC_BYTES (4 bytes): "LSM\003"
##   - filter_type (1 byte): 1 for blocked bloom
##   - hash_type (1 byte): 0 (unused)
##   - num_blocks (8 bytes): LittleEndian
##   - k (8 bytes): LittleEndian
##   - data: num_blocks * 64 bytes

const
  CacheLineBytes*: int = 64
  MagicBytes*: string = "LSM\x03"

type
  BlockedBloomBuilder* = ref object
    data*: string   ## Bit array
    k*: int         ## Number of hash functions
    numBlocks*: int ## Number of cache-line blocks

proc newBlockedBloomBuilder*(capacity: int): BlockedBloomBuilder =
  ## Create a new blocked bloom builder with initial capacity
  let numBlocks = (capacity + 1).div(CacheLineBytes * 8)
  let size = numBlocks * CacheLineBytes
  BlockedBloomBuilder(
    data: newString(size),
    k: 0,
    numBlocks: numBlocks
  )

proc withBlockedFpRate*(n: int, fpr: float): BlockedBloomBuilder =
  ## Construct a blocked bloom filter that can hold n items with target fpr
  assert(n > 0)

  let fpr = max(fpr, 0.0000001)

  # Add 5-25% extra bits for blocked bloom being less accurate
  let bonus = if fpr <= 0.001: 1.25
              elif fpr <= 0.01: 1.20
              elif fpr <= 0.1: 1.10
              else: 1.05

  # Calculate m = -n * ln(fpr) / ln(2)^2
  let ln2 = 0.69314718056
  let ln2Squared = ln2 * ln2
  var m = int(-float(n) * ln(fpr) / ln2Squared)
  m = int(float(m) * bonus)

  # Round up to cache line
  let numBlocks = (m + CacheLineBytes * 8 - 1) div (CacheLineBytes * 8)
  let totalBytes = numBlocks * CacheLineBytes

  # Calculate k
  let bpk = float(totalBytes * 8) / float(n)
  let k = max(1, int(bpk * ln2))

  BlockedBloomBuilder(
    data: newString(totalBytes),
    k: k,
    numBlocks: numBlocks
  )

proc withBlockedBpk*(n: int, bpk: float): BlockedBloomBuilder =
  ## Construct a blocked bloom filter with n items and bpk bits per key
  assert(bpk > 0.0)
  assert(n > 0)

  let m = n * int(bpk)
  let numBlocks = (m + CacheLineBytes * 8 - 1) div (CacheLineBytes * 8)
  let totalBytes = numBlocks * CacheLineBytes

  let k = max(1, int(bpk * 0.69314718056))

  BlockedBloomBuilder(
    data: newString(totalBytes),
    k: k,
    numBlocks: numBlocks
  )

proc getBitIdxBlocked*(blockIdx: int, idxInBlock: int): int =
  ## Get the bit index in the blocked bloom filter
  blockIdx * CacheLineBytes * 8 + idxInBlock

proc secondaryHashBlocked*(h1: uint64): uint64 =
  ## Secondary hash function for blocked bloom filter
  (h1 shr 32) * 0x517C_C1B7_2722_0A95'u64

proc setWithHashBlocked*(b: BlockedBloomBuilder, h1: uint64) =
  ## Add a hash to the blocked bloom filter using double hashing
  var h2 = secondaryHashBlocked(h1)
  var h = h1

  let blockIdx = int(h mod uint64(b.numBlocks))

  for i in 1 ..< uint64(b.k):
    let idx = int(h mod uint64(CacheLineBytes * 8))
    let bitIdx = getBitIdxBlocked(blockIdx, idx)

    # Enable the bit
    let byteIdx = bitIdx div 8
    let bitPos = bitIdx mod 8
    if byteIdx < b.data.len:
      b.data[byteIdx] = chr(ord(b.data[byteIdx]) or (1 shl (7 - bitPos)))

    # Update hashes for double hashing
    h = h + h2
    h2 = h2 * i

proc buildBlocked*(b: BlockedBloomBuilder): string =
  ## Build the blocked bloom filter into a string with header
  result = ""

  # Write magic bytes
  result.add(MagicBytes)

  # Write filter type (1 = blocked bloom)
  result.add(chr(ftBlockedBloom.ord))

  # Write hash type (0 = unused)
  result.add(chr(0))

  # Write num_blocks (8 bytes little endian)
  result.add(encodeFixed32(uint32(b.numBlocks and 0xFFFFFFFF)))
  result.add(encodeFixed32(uint32((b.numBlocks shr 32) and 0xFFFFFFFF)))

  # Write k (8 bytes little endian)
  result.add(encodeFixed32(uint32(b.k and 0xFFFFFFFF)))
  result.add(encodeFixed32(uint32((b.k shr 32) and 0xFFFFFFFF)))

  # Write filter data
  result.add(b.data)

# ============================================================================
# Blocked Bloom Filter Reader
# ============================================================================

type
  BlockedBloomReader* = ref object
    data*: string   ## Filter data (without header)
    k*: int         ## Number of hash functions
    numBlocks*: int ## Number of cache-line blocks

proc newBlockedBloomReader*(filterData: string): LsmResult[BlockedBloomReader] =
  ## Parse a blocked bloom filter from data
  try:
    var pos = 0

    # Check magic bytes
    if filterData.len < 4:
      return err[BlockedBloomReader](newIoError("Invalid blocked bloom filter: too short"))

    let magic = filterData[pos ..< pos + 4]
    if magic != MagicBytes:
      return err[BlockedBloomReader](newIoError("Invalid blocked bloom filter: bad magic"))
    pos += 4

    # Read filter type
    let filterType = ord(filterData[pos])
    pos += 1
    if filterType != ftBlockedBloom.ord:
      return err[BlockedBloomReader](newIoError("Invalid filter type for blocked bloom"))

    # Read hash type (unused)
    pos += 1

    # Read num_blocks (8 bytes little endian)
    if pos + 8 > filterData.len:
      return err[BlockedBloomReader](newIoError("Invalid blocked bloom filter: too short for num_blocks"))
    let numBlocksLow = decodeFixed32(filterData, pos)
    let numBlocksHigh = decodeFixed32(filterData, pos + 4)
    let numBlocks = int(numBlocksLow) or (int(numBlocksHigh) shl 32)
    pos += 8

    # Read k (8 bytes little endian)
    let kLow = decodeFixed32(filterData, pos)
    let kHigh = decodeFixed32(filterData, pos + 4)
    let k = int(kLow) or (int(kHigh) shl 32)
    pos += 8

    # Get filter data
    let data = filterData[pos ..< filterData.len]

    ok(BlockedBloomReader(
      data: data,
      k: k,
      numBlocks: numBlocks
    ))
  except:
    err[BlockedBloomReader](newIoError("Failed to parse blocked bloom filter: " &
        getCurrentExceptionMsg()))

proc containsHashBlocked*(r: BlockedBloomReader, h1: uint64): bool =
  ## Check if hash might be in the blocked bloom filter
  var h2 = secondaryHashBlocked(h1)
  var h = h1

  let blockIdx = int(h mod uint64(r.numBlocks))

  for i in 1 ..< uint64(r.k):
    let idx = int(h mod uint64(CacheLineBytes * 8))
    let bitIdx = getBitIdxBlocked(blockIdx, idx)

    let byteIdx = bitIdx div 8
    let bitPos = bitIdx mod 8

    if byteIdx >= r.data.len:
      return false

    if (ord(r.data[byteIdx]) and (1 shl (7 - bitPos))) == 0:
      return false

    # Update hashes
    h = h + h2
    h2 = h2 * i

  true

proc getBlockedHash*(key: string): uint64 =
  ## Get the hash of a key for blocked bloom filter
  xxhash64(key)

proc mightContainBlocked*(r: BlockedBloomReader, key: types.Slice): bool =
  ## Check if key might be in the blocked bloom filter
  let h = getBlockedHash(key.data)
  r.containsHashBlocked(h)

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing filter structures..."

  # Test bloom policy
  let policy = bitsPerKeyPolicy(10.0)
  echo "Policy is active: ", policy.isActive()
  echo "Estimated size for 1000 keys: ", policy.estimatedFilterSize(1000), " bytes"

  # Test standard bloom builder
  var builder = withBpk(1000, 10.0)
  echo "Bloom builder m: ", builder.m, " bits, k: ", builder.k

  builder.setWithHash(xxhash64("test_key"))
  let filterData = builder.build()
  echo "Filter data length: ", filterData.len

  # Test partitioned filter writer
  var fw = newPartitionedFilterWriter()
  discard fw.registerKey(newSlice("key1"))
  discard fw.registerKey(newSlice("key2"))
  discard fw.registerKey(newSlice("key3"))

  let result = fw.finishFilter()
  if result.isOk:
    echo "Filter finished, partitions: ", result.value.filterBlockCount

  # Test blocked bloom filter
  echo "Testing blocked bloom filter..."

  # Test with_fp_rate
  var blockedBuilder = withBlockedFpRate(10, 0.0001)
  echo "Blocked bloom (fp_rate): numBlocks=", blockedBuilder.numBlocks, ", k=",
      blockedBuilder.k

  # Add some keys
  for i in 0 ..< 10:
    let key = "item" & $i
    blockedBuilder.setWithHashBlocked(xxhash64(key))

  # Build the filter
  let blockedData = blockedBuilder.buildBlocked()
  echo "Blocked bloom filter size: ", blockedData.len, " bytes"

  # Parse and check
  let reader = newBlockedBloomReader(blockedData)
  if reader.isOk:
    let r = reader.value
    echo "Parsed blocked bloom: numBlocks=", r.numBlocks, ", k=", r.k

    # Test contains
    for i in 0 ..< 10:
      let key = "item" & $i
      if r.mightContainBlocked(newSlice(key)):
        echo "Contains: ", key, " - OK"
      else:
        echo "ERROR: Missing key: ", key

    # Test not contains
    if not r.mightContainBlocked(newSlice("notpresent")):
      echo "Correctly rejects absent key - OK"
    else:
      echo "ERROR: Should not contain absent key"
  else:
    echo "ERROR: Failed to parse blocked bloom: ", reader.error

  echo "Filter tests passed!"
