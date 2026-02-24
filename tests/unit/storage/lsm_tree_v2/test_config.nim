# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for LSM Tree v2 - Config

import std/unittest
import fractio/storage/lsm_tree_v2/config
import fractio/storage/lsm_tree_v2/error as lsmerror

suite "config":
  test "newDefaultConfig":
    let cfg = newDefaultConfig("/tmp/test")
    check cfg.path == "/tmp/test"
    check cfg.treeType == ttStandard
    check cfg.levelCount == 7
    check cfg.level0FileNum == 4
    check cfg.levelSizeMultiplier == 10
    check cfg.maxMemtableSize == 64'u64 * 1024'u64 * 1024'u64
    check cfg.targetTableSize == 64'u64 * 1024'u64 * 1024'u64

  test "config builder basic":
    let cfg = newConfigBuilder("/tmp/test")
      .withTreeType(ttBlob)
      .withMemtableSize(128'u64 * 1024'u64 * 1024'u64)
      .withTableSize(128'u64 * 1024'u64 * 1024'u64)
      .build()

    check cfg.path == "/tmp/test"
    check cfg.treeType == ttBlob
    check cfg.maxMemtableSize == 128'u64 * 1024'u64 * 1024'u64
    check cfg.targetTableSize == 128'u64 * 1024'u64 * 1024'u64

  test "config builder with block size":
    let cfg = newConfigBuilder("/tmp/test")
      .withBlockSize(8192)
      .build()

    check cfg.dataBlockSize.size == 8192

  test "config builder with compression":
    let cfg = newConfigBuilder("/tmp/test")
      .withCompression(lsmerror.ctLz4)
      .build()

    check cfg.dataBlockCompression.compression == lsmerror.ctLz4
    check cfg.indexBlockCompression.compression == lsmerror.ctLz4

  test "config builder with bloom filter":
    let cfg = newConfigBuilder("/tmp/test")
      .withBloomFilter(12.0)
      .build()

    check cfg.filterPolicy.bitsPerKey == 12.0

  test "config builder with levels":
    let cfg = newConfigBuilder("/tmp/test")
      .withLevels(10)
      .build()

    check cfg.levelCount == 10

  test "config constants":
    check DefaultDataBlockSize == 4096
    check DefaultIndexBlockSize == 4096
    check DefaultRestartInterval == 16
    check DefaultMaxMemtableSize == 64'u64 * 1024'u64 * 1024'u64
    check DefaultTargetTableSize == 64'u64 * 1024'u64 * 1024'u64
    check DefaultLevelCount == 7
    check DefaultLevel0FileNum == 4
