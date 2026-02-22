# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## SSTable Block Types
##
## Blocks are the basic unit of storage in SSTables.

import std/streams

# Block header size
const BLOCK_HEADER_SIZE* = 8

# Hash index special values
const HASH_INDEX_EMPTY* = 255'u8
const HASH_INDEX_COLLISION* = 254'u8

# Index partitioning threshold - levels >= this use partitioned index
const PARTITIONED_INDEX_THRESHOLD* = 2

# Target entries per index block for partitioned index
const INDEX_BLOCK_ENTRIES* = 64

# Minimum index entries to trigger partitioning
# This is the number of data blocks, not key-value entries
const MIN_INDEX_ENTRIES_FOR_PARTITION* = 8

# Minimum data blocks to trigger filter partitioning
const MIN_FILTER_ENTRIES_FOR_PARTITION * = 16

# Target data blocks per filter partition
const FILTER_BLOCK_ENTRIES* = 128

# Block type
type
  BlockType* = enum
    btData = 1          ## Data block (key-value pairs)
    btIndex = 2         ## Index block (pointers to data blocks)
    btFilter = 3        ## Filter block (bloom filter)
    btFooter = 4        ## Footer (file metadata)
    btTopLevelIndex = 5 ## Top level index block (for partitioned index)
    btTopLevelFilter = 6 ## Top level filter index block (for partitioned filters)

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
    size*: uint32   ## Size of block (compressed size if compressed)
    uncompressedSize*: uint32 ## Original size before compression (0 if not compressed)

# Data block entry
type
  BlockEntry* = object
    key*: string
    value*: string
    seqno*: uint64
    valueType*: uint8

# Block hash index for fast point lookups
type
  BlockHashIndex* = ref object
    ## Hash index for fast point lookups within a data block.
    ## Maps key hashes to restart interval indices.
    buckets*: seq[uint8] # Array of restart indices (or EMPTY/COLLISION)
    numBuckets*: uint32

# Data block
type
  DataBlock* = ref object
    entries*: seq[BlockEntry]
    size*: uint32
    restartPoints*: seq[uint32] # Restart points for binary search
    hashIndex*: BlockHashIndex  # Optional hash index for fast lookups

# Index block entry
type
  IndexEntry* = object
    key*: string         # Last key in the data block
    handle*: BlockHandle # Pointer to data block

# Index block (single-level or partitioned level-2)
type
  IndexBlock* = ref object
    entries*: seq[IndexEntry]

# Top Level Index entry - points to an index block
type
  TopLevelIndexEntry* = object
    key*: string         # Largest key in the index block
    handle*: BlockHandle # Pointer to index block

# Top Level Index (TLI) - always in memory, very small
type
  TopLevelIndex* = ref object
    ## Top-level index for partitioned block index.
    ## Always loaded in memory, contains pointers to index blocks.
    ## Each entry points to an index block that contains data block handles.
    entries*: seq[TopLevelIndexEntry]
    isPartitioned*: bool # True if this SSTable uses partitioned index

# Index mode
type
  IndexMode* = enum
    imFull        ## Full index - single level, all entries in memory
    imPartitioned ## Partitioned index - two level, TLI in memory

# Filter mode
type
  FilterMode* = enum
    fmFull        ## Full filter - single bloom filter for entire SSTable
    fmPartitioned ## Partitioned filter - multiple bloom filters, TLI in memory

# Top Level Filter Index entry - points to a filter block
type
  TopLevelFilterEntry* = object
    firstKey*: string    # First key in the data blocks covered by this filter
    lastKey*: string     # Last key in the data blocks covered by this filter
    handle*: BlockHandle # Pointer to filter block

# Top Level Filter Index - always in memory for partitioned filters
type
  TopLevelFilterIndex* = ref object
    ## Top-level index for partitioned bloom filters.
    ## Always loaded in memory, contains pointers to filter blocks.
    ## Each entry points to a bloom filter that covers a range of data blocks.
    entries*: seq[TopLevelFilterEntry]
    isPartitioned*: bool # True if this SSTable uses partitioned filters

# SSTable footer
type
  SsTableFooter* = object
    magic*: array[8, byte]
    version*: uint32
    indexHandle*: BlockHandle # Points to index block (full) or TLI (partitioned)
    filterHandle*: BlockHandle # Points to bloom filter block or TLI (partitioned)
    metaIndexHandle*: BlockHandle
    checksum*: uint32
    indexMode*: IndexMode     # v3+: Index mode (full or partitioned)
    filterMode*: FilterMode   # v4+: Filter mode (full or partitioned)

const SSTABLE_MAGIC* = [byte('S'), byte('S'), byte('T'), byte('B'),
                        byte('L'), byte('K'), byte('V'), byte('1')]
