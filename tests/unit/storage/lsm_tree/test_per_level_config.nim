# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for per-level configuration support

import unittest
import fractio/storage/lsm_tree/lsm_tree
import fractio/storage/lsm_tree/types
import fractio/storage/keyspace/options
import fractio/storage/types as storage_types
import std/[tempfiles, os]

suite "Per-Level Configuration Tests":
  setup:
    let tempDir = createTempDir("perlevel_test_", "")
    let dbPath = tempDir / "db"

  teardown:
    try:
      removeDir(tempDir)
    except OSError:
      discard

  test "Default config has per-level settings":
    let config = newConfig(dbPath)

    check config.blockSizes.len == DefaultLevelCount
    check config.restartIntervals.len == DefaultLevelCount
    check config.compressionTypes.len == DefaultLevelCount
    check config.bloomFpr.len == DefaultLevelCount

    # All levels should have default block size
    for bs in config.blockSizes:
      check bs == uint32(DefaultBlockSize)

    # Last level should have bloom filter disabled
    check config.bloomFpr[^1] == 0.0

  test "Get block size for level":
    let config = newConfig(dbPath)

    # Should return the configured size for each level
    for level in 0 ..< config.levelCount:
      check config.getBlockSize(level) == uint32(DefaultBlockSize)

    # Should fall back to last level for out-of-range
    check config.getBlockSize(100) == uint32(DefaultBlockSize)

  test "Get restart interval for level":
    let config = newConfig(dbPath)

    for level in 0 ..< config.levelCount:
      check config.getRestartInterval(level) == DefaultRestartInterval

  test "Get compression type for level":
    let config = newConfig(dbPath)

    for level in 0 ..< config.levelCount:
      check config.getCompressionType(level) == storage_types.ctNone

  test "Get bloom filter FPR for level":
    let config = newConfig(dbPath)

    # First levels should have default FPR
    for level in 0 ..< config.levelCount - 1:
      check config.getBloomFpr(level) == DefaultBloomFpr

    # Last level should have bloom disabled
    check config.getBloomFpr(config.levelCount - 1) == 0.0

  test "Bloom filter enabled check":
    let config = newConfig(dbPath)

    # First levels should have bloom enabled
    for level in 0 ..< config.levelCount - 1:
      check config.isBloomFilterEnabled(level)

    # Last level should have bloom disabled
    check not config.isBloomFilterEnabled(config.levelCount - 1)

  test "Custom per-level block sizes":
    var config = newConfig(dbPath)
    config.blockSizes = @[4096'u32, 8192'u32, 16384'u32]

    check config.getBlockSize(0) == 4096'u32
    check config.getBlockSize(1) == 8192'u32
    check config.getBlockSize(2) == 16384'u32
    # Out of range should fall back to last configured
    check config.getBlockSize(5) == 16384'u32

  test "Custom per-level compression":
    var config = newConfig(dbPath)
    config.compressionTypes = @[storage_types.ctNone, storage_types.ctLz4,
                                storage_types.ctLz4, storage_types.ctSnappy]

    check config.getCompressionType(0) == storage_types.ctNone
    check config.getCompressionType(1) == storage_types.ctLz4
    check config.getCompressionType(2) == storage_types.ctLz4
    check config.getCompressionType(3) == storage_types.ctSnappy
    # Out of range should fall back
    check config.getCompressionType(10) == storage_types.ctSnappy

  test "Custom per-level bloom FPR":
    var config = newConfig(dbPath)
    config.bloomFpr = @[0.01, 0.02, 0.05, 0.0]

    check config.getBloomFpr(0) == 0.01
    check config.getBloomFpr(1) == 0.02
    check config.getBloomFpr(2) == 0.05
    check config.getBloomFpr(3) == 0.0
    check not config.isBloomFilterEnabled(3)

  test "Create config from CreateOptions with per-level settings":
    var opts = defaultCreateOptions()

    # Set per-level block sizes
    opts.dataBlockSizePolicy = BlockSizePolicy(sizes: @[4096'u32, 8192'u32, 16384'u32])

    # Set per-level compression
    opts.dataBlockCompressionPolicy = CompressionPolicy(
      compressionTypes: @[storage_types.ctNone, storage_types.ctLz4]
    )

    # Set per-level restart intervals
    opts.dataBlockRestartIntervalPolicy = RestartIntervalPolicy(intervals: @[10,
        16, 32])

    let config = newConfigFromOptions(dbPath, opts)

    # Verify block sizes
    check config.getBlockSize(0) == 4096'u32
    check config.getBlockSize(1) == 8192'u32
    check config.getBlockSize(2) == 16384'u32

    # Verify compression
    check config.getCompressionType(0) == storage_types.ctNone
    check config.getCompressionType(1) == storage_types.ctLz4
    # Should fall back to last configured
    check config.getCompressionType(5) == storage_types.ctLz4

    # Verify restart intervals
    check config.getRestartInterval(0) == 10
    check config.getRestartInterval(1) == 16
    check config.getRestartInterval(2) == 32

  test "Helper functions for policies":
    # Test uniformBlockSize helper
    let uniformPolicy = uniformBlockSize(8192'u32)
    check uniformPolicy.sizes == @[8192'u32]

    # Test perLevelBlockSizes helper
    let perLevelPolicy = perLevelBlockSizes(4096'u32, 8192'u32, 16384'u32)
    check perLevelPolicy.sizes == @[4096'u32, 8192'u32, 16384'u32]

    # Test perLevelCompression helper
    let compressionPolicy = perLevelCompression(
      storage_types.ctNone, storage_types.ctLz4, storage_types.ctSnappy
    )
    check compressionPolicy.compressionTypes ==
      @[storage_types.ctNone, storage_types.ctLz4, storage_types.ctSnappy]

  test "Config with strategy preserves per-level settings":
    let config = newConfigWithStrategy(dbPath, defaultLeveled())

    check config.blockSizes.len == DefaultLevelCount
    check config.compressionTypes.len == DefaultLevelCount
    check config.compactionStrategy.kind == cskLeveled
