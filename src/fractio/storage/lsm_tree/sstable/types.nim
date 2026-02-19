# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## SSTable Block Types
##
## Blocks are the basic unit of storage in SSTables.

import std/[streams, tables]

# Block header size
const BLOCK_HEADER_SIZE* = 8

# Block type
type
  BlockType* = enum
    btData = 1   ## Data block (key-value pairs)
    btIndex = 2  ## Index block (pointers to data blocks)
    btFilter = 3 ## Filter block (bloom filter)
    btFooter = 4 ## Footer (file metadata)

# Block header
type
  BlockHeader* = object
    checksum*: uint32 ## CRC32 checksum of block data
    blockType*: BlockType
    compressed*: bool ## Whether block is compressed
    padding*: uint8   ## Reserved

# Block handle - pointer to a block in file
type
  BlockHandle* = object
    offset*: uint64 ## Offset in file
    size*: uint32   ## Size of block

# Data block entry
type
  BlockEntry* = object
    key*: string
    value*: string
    seqno*: uint64
    valueType*: uint8

# Data block
type
  DataBlock* = ref object
    entries*: seq[BlockEntry]
    size*: uint32
    restartPoints*: seq[uint32] # Restart points for binary search

# Index block entry
type
  IndexEntry* = object
    key*: string         # Last key in the data block
    handle*: BlockHandle # Pointer to data block

# Index block
type
  IndexBlock* = ref object
    entries*: seq[IndexEntry]

# Bloom filter
type
  BloomFilter* = ref object
    data*: seq[byte]
    numKeys*: uint32
    numBits*: uint32
    numHashes*: uint8

# SSTable footer
type
  SsTableFooter* = object
    magic*: array[8, byte]
    version*: uint32
    indexHandle*: BlockHandle
    filterHandle*: BlockHandle
    metaIndexHandle*: BlockHandle
    checksum*: uint32

const SSTABLE_MAGIC* = [byte('S'), byte('S'), byte('T'), byte('B'),
                        byte('L'), byte('K'), byte('V'), byte('1')]
