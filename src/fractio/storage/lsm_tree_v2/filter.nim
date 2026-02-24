# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Filter
##
## Bloom filter structures for SSTables.

import std/[sequtils]
import types
import hash

# Filter policy
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

# Standard bloom filter
type
  StandardBloomFilter* = ref object
    data*: string
    bitsPerKey*: float

# Blocked bloom filter
type
  BlockedBloomFilter* = ref object
    data*: string
    blockSize*: int

# Bit array
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
