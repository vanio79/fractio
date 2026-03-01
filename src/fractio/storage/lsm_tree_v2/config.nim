# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree v2 - Configuration
##
## This module provides configuration options for the LSM tree.

import std/[strutils, options]
import types
import error

# ============================================================================
# Tree Type
# ============================================================================

type
  TreeType* = enum
    ttStandard = 0 ## Standard LSM-tree
    ttBlob = 1     ## Key-value separated LSM-tree

# ============================================================================
# Policies
# ============================================================================

type
  BlockSizePolicy* = object
    size*: uint32

proc newBlockSizePolicy*(size: uint32): BlockSizePolicy =
  BlockSizePolicy(size: size)

type
  RestartIntervalPolicy* = object
    interval*: int

proc newRestartIntervalPolicy*(interval: int): RestartIntervalPolicy =
  RestartIntervalPolicy(interval: interval)

type
  CompressionPolicy* = object
    compression*: CompressionType

proc newCompressionPolicy*(compression: CompressionType): CompressionPolicy =
  CompressionPolicy(compression: compression)

type
  FilterPolicy* = object
    bitsPerKey*: float

proc newBloomFilterPolicy*(bitsPerKey: float): FilterPolicy =
  FilterPolicy(bitsPerKey: bitsPerKey)

type
  HashRatioPolicy* = object
    hashRatio*: float

proc newHashRatioPolicy*(ratio: float): HashRatioPolicy =
  HashRatioPolicy(hashRatio: ratio)

type
  PinningPolicy* = object
    enabled*: bool

proc newPinningPolicy*(enabled: bool): PinningPolicy =
  PinningPolicy(enabled: enabled)

# ============================================================================
# KV Separation Options
# ============================================================================

type
  KvSeparationOptions* = ref object
    compression*: CompressionType
    fileTargetSize*: uint64
    separationThreshold*: uint32
    stalenessThreshold*: float
    ageCutoff*: float

proc newDefaultKvSeparationOptions*(): KvSeparationOptions =
  KvSeparationOptions(
    compression: ctNone,
    fileTargetSize: 64 * 1024 * 1024, # 64 MiB
    separationThreshold: 1024, # 1 KiB
    stalenessThreshold: 0.25,
    ageCutoff: 0.25
  )

# ============================================================================
# Main Config
# ============================================================================

type
  Config* = ref object
    path*: string
    treeType*: TreeType

    # Block sizes
    dataBlockSize*: BlockSizePolicy
    indexBlockSize*: BlockSizePolicy

    # Restart intervals
    dataBlockRestartInterval*: RestartIntervalPolicy
    indexBlockRestartInterval*: RestartIntervalPolicy

    # Compression
    dataBlockCompression*: CompressionPolicy
    indexBlockCompression*: CompressionPolicy

    # Bloom filter
    filterPolicy*: FilterPolicy

    # Hash ratio
    dataBlockHashRatio*: HashRatioPolicy

    # Pinning
    filterBlockPinning*: PinningPolicy
    indexBlockPinning*: PinningPolicy

    # Level config
    levelCount*: int
    level0FileNum*: int
    levelSizeMultiplier*: int

    # Size config
    maxMemtableSize*: uint64
    targetTableSize*: uint64

    # Compaction
    maxConcurrentCompactions*: int

    # Cache
    cacheSize*: int

    # KV separation
    kvSeparation*: Option[KvSeparationOptions]

    # WAL - set to false for in-memory only (matches Rust lsm-tree behavior)
    walEnabled*: bool

proc newDefaultConfig*(path: string): Config =
  Config(
    path: path,
    treeType: ttStandard,
    dataBlockSize: newBlockSizePolicy(4096),
    indexBlockSize: newBlockSizePolicy(4096),
    dataBlockRestartInterval: newRestartIntervalPolicy(16),
    indexBlockRestartInterval: newRestartIntervalPolicy(16),
    dataBlockCompression: newCompressionPolicy(ctNone),
    indexBlockCompression: newCompressionPolicy(ctNone),
    filterPolicy: newBloomFilterPolicy(10.0),
    dataBlockHashRatio: newHashRatioPolicy(0.75),
    filterBlockPinning: newPinningPolicy(false),
    indexBlockPinning: newPinningPolicy(false),
    levelCount: 7,
    level0FileNum: 4,
    levelSizeMultiplier: 10,
    maxMemtableSize: 64 * 1024 * 1024, # 64 MiB
    targetTableSize: 64 * 1024 * 1024, # 64 MiB
    maxConcurrentCompactions: 4,
    cacheSize: 16 * 1024 * 1024, # 16 MiB
    kvSeparation: none(KvSeparationOptions),
    walEnabled: true # WAL enabled by default for durability
  )

# ============================================================================
# Config Builder
# ============================================================================

type
  ConfigBuilder* = ref object
    config*: Config

proc newConfigBuilder*(path: string): ConfigBuilder =
  ConfigBuilder(config: newDefaultConfig(path))

proc withTreeType*(b: ConfigBuilder, treeType: TreeType): ConfigBuilder =
  b.config.treeType = treeType
  b

proc withMemtableSize*(b: ConfigBuilder, size: uint64): ConfigBuilder =
  b.config.maxMemtableSize = size
  b

proc withTableSize*(b: ConfigBuilder, size: uint64): ConfigBuilder =
  b.config.targetTableSize = size
  b

proc withBlockSize*(b: ConfigBuilder, size: uint32): ConfigBuilder =
  b.config.dataBlockSize = newBlockSizePolicy(size)
  b

proc withCompression*(b: ConfigBuilder, compression: CompressionType): ConfigBuilder =
  b.config.dataBlockCompression = newCompressionPolicy(compression)
  b.config.indexBlockCompression = newCompressionPolicy(compression)
  b

proc withBloomFilter*(b: ConfigBuilder, bitsPerKey: float): ConfigBuilder =
  b.config.filterPolicy = newBloomFilterPolicy(bitsPerKey)
  b

proc withLevels*(b: ConfigBuilder, count: int): ConfigBuilder =
  b.config.levelCount = count
  b

proc withKvSeparation*(b: ConfigBuilder,
    opts: KvSeparationOptions): ConfigBuilder =
  b.config.kvSeparation = some(opts)
  b

proc withWalEnabled*(b: ConfigBuilder, enabled: bool): ConfigBuilder =
  b.config.walEnabled = enabled
  b

proc build*(b: ConfigBuilder): Config =
  b.config

# ============================================================================
# Constants
# ============================================================================

const
  DefaultDataBlockSize* = 4096
  DefaultIndexBlockSize* = 4096
  DefaultRestartInterval* = 16
  DefaultMaxMemtableSize* = 64 * 1024 * 1024 # 64 MiB
  DefaultTargetTableSize* = 64 * 1024 * 1024 # 64 MiB
  DefaultLevelCount* = 7
  DefaultLevel0FileNum* = 4

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing config..."

  let config = newDefaultConfig("/tmp/test_lsm")
  echo "Path: ", config.path
  echo "Level count: ", config.levelCount
  echo "Memtable size: ", config.maxMemtableSize

  # Test builder
  let builder = newConfigBuilder("/tmp/test")
    .withTreeType(ttStandard)
    .withMemtableSize(128 * 1024 * 1024)
    .withBlockSize(8192)
    .withCompression(ctLz4)
    .withBloomFilter(12.0)

  let built = builder.build()
  echo "Built memtable size: ", built.maxMemtableSize
  echo "Built block size: ", built.dataBlockSize.size

  echo "Config tests passed!"
