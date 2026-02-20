# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Bloom Filter Implementation
##
## A probabilistic data structure for testing set membership.
## Used in SSTables to quickly reject keys that definitely don't exist.

import std/[hashes, streams, endians, math]

# ============================================================================
# Constants
# ============================================================================

const
  LN2* = 0.6931471805599453
  LN2_SQUARED* = 0.4804530139182014

# ============================================================================
# Bloom Filter Type
# ============================================================================

type
  BloomFilter* = ref object
    ## A bloom filter for probabilistic set membership testing.
    ## Can produce false positives but never false negatives.
    bits*: seq[uint64] # Bit array stored as uint64 words
    numBits*: uint64 # Total number of bits
    numHashes*: int # Number of hash functions
    numKeys*: int # Number of keys added

# ============================================================================
# Hash Functions
# ============================================================================

proc murmurHash3(data: string, seed: uint32): uint32 =
  ## Simplified MurmurHash3 x86 32-bit
  var h = seed
  let len = data.len

  # Process 4-byte chunks
  var i = 0
  while i + 4 <= len:
    var k: uint32
    copyMem(addr k, addr data[i], 4)

    # Little endian to native
    when cpuEndian == littleEndian:
      discard
    else:
      var tmp: uint32
      littleEndian32(addr tmp, addr k)
      k = tmp

    k = k * 0xcc9e2d51'u32
    k = (k shl 15) or (k shr 17) # ROL 15
    k = k * 0x1b873593'u32

    h = h xor k
    h = (h shl 13) or (h shr 19) # ROL 13
    h = h * 5 + 0xe6546b64'u32

    i += 4

  # Process remaining bytes
  var k: uint32 = 0
  var remaining = len mod 4
  if remaining > 0:
    for j in 0 ..< remaining:
      k = k or (uint32(data[len - remaining + j]) shl (j * 8))

    k = k * 0xcc9e2d51'u32
    k = (k shl 15) or (k shr 17)
    k = k * 0x1b873593'u32
    h = h xor k

  # Finalization
  h = h xor uint32(len)
  h = h xor (h shr 16)
  h = h * 0x85ebca6b'u32
  h = h xor (h shr 13)
  h = h * 0xc2b2ae35'u32
  h = h xor (h shr 16)

  return h

proc hashKey(filter: BloomFilter, key: string, hashIndex: int): uint64 =
  ## Compute the hash for a key at a given hash index.
  ## Uses double hashing to simulate multiple hash functions.
  let h1 = murmurHash3(key, 0)
  let h2 = murmurHash3(key, uint32(h1))

  # Use formula: h(i) = h1 + i * h2
  let combined = uint64(h1) + uint64(hashIndex) * uint64(h2)
  return combined mod filter.numBits

# ============================================================================
# Constructor
# ============================================================================

proc optimalNumHashes*(expectedKeys: int, bitsPerKey: int): int =
  ## Calculate optimal number of hash functions.
  ## k = (m/n) * ln(2) where m=bits, n=keys
  let k = int(float(bitsPerKey) * LN2)
  return max(1, min(k, 30))

proc optimalNumBits*(expectedKeys: int, falsePositiveRate: float): uint64 =
  ## Calculate optimal number of bits.
  ## m = -n * ln(p) / (ln(2)^2)
  if expectedKeys <= 0:
    return 64'u64
  let m = -float(expectedKeys) * ln(falsePositiveRate) / LN2_SQUARED
  return max(64'u64, uint64(m))

proc newBloomFilter*(expectedKeys: int, falsePositiveRate: float = 0.01): BloomFilter =
  ## Create a new bloom filter optimized for the expected number of keys.
  let numBits = optimalNumBits(expectedKeys, falsePositiveRate)
  let numWords = int((numBits + 63) div 64)
  let effectiveKeys = max(1, expectedKeys)
  let bitsPerKey = int(numBits div uint64(effectiveKeys))
  let numHashes = optimalNumHashes(expectedKeys, bitsPerKey)

  result = BloomFilter(
    bits: newSeq[uint64](numWords),
    numBits: uint64(numWords * 64),
    numHashes: numHashes,
    numKeys: 0
  )

proc newBloomFilterWithBits*(numBits: uint64, numHashes: int): BloomFilter =
  ## Create a bloom filter with specific parameters.
  let numWords = int((numBits + 63) div 64)

  result = BloomFilter(
    bits: newSeq[uint64](numWords),
    numBits: uint64(numWords * 64),
    numHashes: numHashes,
    numKeys: 0
  )

# ============================================================================
# Core Operations
# ============================================================================

proc setBit(filter: BloomFilter, index: uint64) =
  ## Set a bit in the filter.
  let wordIndex = int(index div 64)
  let bitIndex = index mod 64
  filter.bits[wordIndex] = filter.bits[wordIndex] or (1'u64 shl bitIndex)

proc testBit(filter: BloomFilter, index: uint64): bool =
  ## Test if a bit is set in the filter.
  let wordIndex = int(index div 64)
  let bitIndex = index mod 64
  return (filter.bits[wordIndex] and (1'u64 shl bitIndex)) != 0

proc add*(filter: BloomFilter, key: string) =
  ## Add a key to the bloom filter.
  for i in 0 ..< filter.numHashes:
    let index = hashKey(filter, key, i)
    filter.setBit(index)
  inc filter.numKeys

proc mayContain*(filter: BloomFilter, key: string): bool =
  ## Check if the key might be in the set.
  ## Returns true if the key might be present (could be false positive).
  ## Returns false if the key is definitely not present.
  for i in 0 ..< filter.numHashes:
    let index = hashKey(filter, key, i)
    if not filter.testBit(index):
      return false
  return true

proc clear*(filter: BloomFilter) =
  ## Clear all bits in the filter.
  for i in 0 ..< filter.bits.len:
    filter.bits[i] = 0
  filter.numKeys = 0

# ============================================================================
# Serialization
# ============================================================================

proc serialize*(filter: BloomFilter, stream: Stream) =
  ## Serialize the bloom filter to a stream.
  ## Format:
  ##   - numBits (4 bytes, little endian)
  ##   - numHashes (4 bytes, little endian)
  ##   - numKeys (4 bytes, little endian)
  ##   - bits (numBits/8 bytes)

  var numBitsLe = uint32(filter.numBits)
  littleEndian32(addr numBitsLe, addr numBitsLe)
  stream.write(numBitsLe)

  var numHashesLe = uint32(filter.numHashes)
  littleEndian32(addr numHashesLe, addr numHashesLe)
  stream.write(numHashesLe)

  var numKeysLe = uint32(filter.numKeys)
  littleEndian32(addr numKeysLe, addr numKeysLe)
  stream.write(numKeysLe)

  # Write bits as bytes
  for word in filter.bits:
    var wordLe = word
    littleEndian64(addr wordLe, addr wordLe)
    stream.write(wordLe)

proc deserializeBloomFilter*(stream: Stream): BloomFilter =
  ## Deserialize a bloom filter from a stream.
  var numBitsLe: uint32 = stream.readUInt32()
  var numBits: uint32
  littleEndian32(addr numBits, addr numBitsLe)

  var numHashesLe: uint32 = stream.readUInt32()
  var numHashes: uint32
  littleEndian32(addr numHashes, addr numHashesLe)

  var numKeysLe: uint32 = stream.readUInt32()
  var numKeys: uint32
  littleEndian32(addr numKeys, addr numKeysLe)

  let numWords = int((uint64(numBits) + 63) div 64)
  var bits = newSeq[uint64](numWords)

  for i in 0 ..< numWords:
    var wordLe: uint64 = stream.readUInt64()
    littleEndian64(addr bits[i], addr wordLe)

  result = BloomFilter(
    bits: bits,
    numBits: uint64(numBits),
    numHashes: int(numHashes),
    numKeys: int(numKeys)
  )

proc serializedSize*(filter: BloomFilter): int =
  ## Get the serialized size of the bloom filter in bytes.
  result = 12 + filter.bits.len * 8

# ============================================================================
# Statistics
# ============================================================================

proc countBitsSet*(filter: BloomFilter): int =
  ## Count the number of bits set in the filter.
  for word in filter.bits:
    # Count set bits in word (popcount)
    var w = word
    while w != 0:
      w = w and (w - 1)
      inc result

proc estimatedFalsePositiveRate*(filter: BloomFilter): float =
  ## Estimate the false positive rate based on the current state.
  ## Uses the formula: p = (1 - e^(-kn/m))^k
  if filter.numKeys == 0:
    return 0.0

  let n = float(filter.numKeys)
  let m = float(filter.numBits)
  let k = float(filter.numHashes)

  let exponent = -k * n / m
  let inner = 1.0 - exp(exponent)
  result = pow(inner, k)

proc fillRatio*(filter: BloomFilter): float =
  ## Get the ratio of set bits to total bits.
  let setBits = float(filter.countBitsSet())
  let totalBits = float(filter.numBits)
  return setBits / totalBits
