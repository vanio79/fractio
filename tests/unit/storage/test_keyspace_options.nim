# Unit tests for keyspace options module
# Tests for CreateOptions functionality

import unittest
import fractio/storage/keyspace/options
import fractio/storage/types
import std/[options, strutils]

suite "Keyspace Options Unit Tests":

  test "Default CreateOptions":
    let options = defaultCreateOptions()
    check options.manualJournalPersist == false
    let expectedMemtable: uint64 = 64 * 1024 * 1024         # 64 MiB
    check options.maxMemtableSize == expectedMemtable
    check options.dataBlockHashRatioPolicy.ratio == 0.0
    check options.dataBlockSizePolicy.sizes.len == 1 # Has default size
    check options.expectPointReadHits == false
    check options.levelCount == 7

  test "CreateOptions builder methods":
    var options = defaultCreateOptions()

    # Test maxMemtableSize
    let memtableSize: uint64 = 128 * 1024 * 1024
    options = options.maxMemtableSize(memtableSize)
    check options.maxMemtableSize == memtableSize

    # Test manualJournalPersist
    options = options.manualJournalPersist(true)
    check options.manualJournalPersist == true

    # Test expectPointReadHits
    options = options.expectPointReadHits(true)
    check options.expectPointReadHits == true

  test "Compression policy":
    let options = defaultCreateOptions()
    check options.dataBlockCompressionPolicy.compressionTypes.len > 0

    # Test with specific compression
    var customOptions = defaultCreateOptions()
    customOptions = customOptions.dataBlockCompressionPolicy(
      CompressionPolicy(compressionTypes: @[ctLz4])
    )
    check customOptions.dataBlockCompressionPolicy.compressionTypes[0] == ctLz4

  test "Configuration policies":
    var options = defaultCreateOptions()

    # Test data block size policy
    options = options.dataBlockSizePolicy(BlockSizePolicy(sizes: @[8192'u32])) # 8 KiB
    check options.dataBlockSizePolicy.sizes == @[8192'u32]

    # Test restart interval policy
    options = options.dataBlockRestartIntervalPolicy(
      RestartIntervalPolicy(intervals: @[5, 10])
    )
    check options.dataBlockRestartIntervalPolicy.intervals == @[5, 10]

    # Test hash ratio policy
    options = options.dataBlockHashRatioPolicy(
      HashRatioPolicy(ratio: 0.5)
    )
    check options.dataBlockHashRatioPolicy.ratio == 0.5

  test "KvSeparationOptions":
    var options = defaultCreateOptions()

    # Test with KV separation using KvSeparationOptions from keyspace/options
    var kvOpts: KvSeparationOptions
    kvOpts.ageCutoff = 0.5
    kvOpts.compression = ctLz4
    kvOpts.fileTargetSize = 1024 * 1024
    kvOpts.separationThreshold = 1024
    kvOpts.stalenessThreshold = 0.8

    options = options.withKvSeparation(some(kvOpts))
    check options.kvSeparationOpts.isSome
    let kvSome = options.kvSeparationOpts.get()
    check kvSome.compression == ctLz4
    let expectedTarget: uint64 = 1024 * 1024
    check kvSome.fileTargetSize == expectedTarget

  test "Encode/Decode config keys":
    let keyspaceId: uint64 = 123
    let key = encodeConfigKey(keyspaceId, "test_config")
    check key.len > 0
    check "test_config" in key
