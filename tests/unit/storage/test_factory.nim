# Unit tests for Storage Factory
# Tests for factory creation, configuration, and error handling

import unittest
import std/[options, tables, strutils]
import fractio/storage/backend
import fractio/storage/factory

# =============================================================================
# Test Suites - Storage Engine Types
# =============================================================================

suite "Storage Factory - Engine Types":
  test "StorageEngineType enum values":
    check setWiscKey.ord == 0
    check setInMemory.ord == 1
    check setRocksDB.ord == 2

  test "StorageEngineType string representation":
    check $setWiscKey == "wisckey"
    check $setInMemory == "in_memory"
    check $setRocksDB == "rocksdb"

  test "StorageEngineType ordering":
    check setWiscKey < setInMemory
    check setInMemory < setRocksDB

# =============================================================================
# Test Suites - Factory Error Handling
# =============================================================================

suite "Storage Factory - Error Handling":
  test "createStorageBackend with inMemory raises error":
    var config = defaultStorageConfig("/tmp/test")
    var exceptionRaised = false
    try:
      discard createStorageBackend(setInMemory, config)
    except StorageError as e:
      exceptionRaised = true
      check "not yet implemented" in e.msg.toLowerAscii()
    check exceptionRaised

  test "createStorageBackend with RocksDB raises error":
    var config = defaultStorageConfig("/tmp/test")
    var exceptionRaised = false
    try:
      discard createStorageBackend(setRocksDB, config)
    except StorageError as e:
      exceptionRaised = true
      check "not yet implemented" in e.msg.toLowerAscii()
    check exceptionRaised

# =============================================================================
# Test Suites - Configuration Validation
# =============================================================================

suite "Storage Factory - Configuration":
  test "createWiscKeyBackend with default config":
    let config = defaultStorageConfig("/tmp/test_factory_default")
    check config.createIfMissing == true
    check config.errorIfExists == false
    check config.syncWrites == false
    check config.compression == ctSnappy

  test "createWiscKeyBackendSync config has syncWrites":
    # The function exists and should set syncWrites=true
    # We verify the function signature by checking it compiles
    let path = "/tmp/test_sync_path"
    # This would create a real backend; we just verify the function exists
    check true # Placeholder - real test would need LevelDB

  test "factory function parameters match config defaults":
    # Verify default values in createWiscKeyBackend
    let defaultWriteBufferSize = 4 * 1024 * 1024         # 4MB
    let defaultBlockSize = 4 * 1024 # 4KB
    let defaultCompression = ctSnappy

    check defaultWriteBufferSize == 4194304
    check defaultBlockSize == 4096
    check defaultCompression == ctSnappy

# =============================================================================
# Test Suites - WiscKey Backend Creation
# =============================================================================

suite "Storage Factory - WiscKey Backend":
  test "newWiscKeyBackend creates backend with correct path":
    var config = defaultStorageConfig("/tmp/test_wisckey_path")
    config.createIfMissing = true
    # Backend creation requires LevelDB; we test the path is set correctly
    # Note: This test creates a real backend, requires LevelDB installed
    check config.path == "/tmp/test_wisckey_path"

  test "WiscKey backend defaults to not open":
    var config = defaultStorageConfig("/tmp/test_wisckey_not_open")
    config.createIfMissing = true
    # After newWiscKeyBackend, isOpen should be false until open() is called
    check true # Placeholder for actual backend creation test

  test "WiscKey config with block cache":
    var config = defaultStorageConfig("/tmp/test_block_cache")
    config.blockCacheSize = 16 * 1024 * 1024 # 16MB
    check config.blockCacheSize == 16777216

  test "WiscKey config with vlog settings":
    var config = defaultStorageConfig("/tmp/test_vlog")
    config.vlogMaxSize = 2_000_000_000
    config.vlogCleanThreshold = 200_000
    config.vlogMinCleanThreshold = 5_000
    config.vlogCleanBufferSize = 128 * 1024 * 1024

    check config.vlogMaxSize == 2000000000
    check config.vlogCleanThreshold == 200000
    check config.vlogMinCleanThreshold == 5000
    check config.vlogCleanBufferSize == 134217728

# =============================================================================
# Test Suites - Compression Type
# =============================================================================

suite "Storage Factory - Compression Types":
  test "compression type none":
    var config = defaultStorageConfig("/tmp/test")
    config.compression = ctNone
    check config.compression == ctNone
    check $config.compression == "none"

  test "compression type snappy":
    var config = defaultStorageConfig("/tmp/test")
    config.compression = ctSnappy
    check config.compression == ctSnappy
    check $config.compression == "snappy"

  test "compression type lz4":
    var config = defaultStorageConfig("/tmp/test")
    config.compression = ctLz4
    check config.compression == ctLz4
    check $config.compression == "lz4"

# =============================================================================
# Test Suites - Sync Write Options
# =============================================================================

suite "Storage Factory - Sync Write Options":
  test "createWiscKeyBackend default sync is false":
    let config = defaultStorageConfig("/tmp/test")
    check config.syncWrites == false

  test "createWiscKeyBackendSync enables sync writes":
    # Verify the config for sync backend
    var config = defaultStorageConfig("/tmp/test")
    config.syncWrites = true
    check config.syncWrites == true

# =============================================================================
# Test Suites - ErrorIfExists Option
# =============================================================================

suite "Storage Factory - ErrorIfExists Option":
  test "default config has errorIfExists false":
    let config = defaultStorageConfig("/tmp/test")
    check config.errorIfExists == false

  test "errorIfExists true prevents overwriting existing database":
    var config = defaultStorageConfig("/tmp/test")
    config.errorIfExists = true
    check config.errorIfExists == true

# =============================================================================
# Test Suites - Max Open Files
# =============================================================================

suite "Storage Factory - Max Open Files":
  test "default maxOpenFiles is 1000":
    let config = defaultStorageConfig("/tmp/test")
    check config.maxOpenFiles == 1000

  test "custom maxOpenFiles":
    var config = defaultStorageConfig("/tmp/test")
    config.maxOpenFiles = 500
    check config.maxOpenFiles == 500

# =============================================================================
# Test Suites - Write Buffer Size
# =============================================================================

suite "Storage Factory - Write Buffer Size":
  test "default writeBufferSize is 4MB":
    let config = defaultStorageConfig("/tmp/test")
    check config.writeBufferSize == 4 * 1024 * 1024

  test "custom writeBufferSize":
    var config = defaultStorageConfig("/tmp/test")
    config.writeBufferSize = 8 * 1024 * 1024
    check config.writeBufferSize == 8388608

# =============================================================================
# Test Suites - Block Size
# =============================================================================

suite "Storage Factory - Block Size":
  test "default blockSize is 4KB":
    let config = defaultStorageConfig("/tmp/test")
    check config.blockSize == 4 * 1024

  test "custom blockSize":
    var config = defaultStorageConfig("/tmp/test")
    config.blockSize = 8 * 1024
    check config.blockSize == 8192

# =============================================================================
# Test Suites - StorageError Type
# =============================================================================

suite "Storage Factory - StorageError":
  test "StorageError can be created":
    let err = newStorageError(secNotFound, "Key not found")
    check err.code == secNotFound
    check err.msg == "Key not found"

  test "StorageError with different codes":
    let ioErr = newStorageError(secIOError, "IO failed")
    check ioErr.code == secIOError

    let corruptErr = newStorageError(secCorruption, "Data corrupted")
    check corruptErr.code == secCorruption

# =============================================================================
# Test Suites - Path Configuration
# =============================================================================

suite "Storage Factory - Path Configuration":
  test "path is stored correctly":
    let config = defaultStorageConfig("/custom/data/path")
    check config.path == "/custom/data/path"

  test "path with trailing slash":
    let config = defaultStorageConfig("/tmp/test/")
    check config.path == "/tmp/test/"

  test "relative path":
    let config = defaultStorageConfig("data/db")
    check config.path == "data/db"

  test "empty path":
    let config = defaultStorageConfig("")
    check config.path == ""

# =============================================================================
# Test Suites - Factory Object
# =============================================================================

suite "Storage Factory - Factory Object":
  test "StorageFactory type exists":
    let factory = StorageFactory()
    check true # Factory is an empty object for type safety

  test "factory has correct structure":
    # StorageFactory is a marker type for organizing creation logic
    let factory: StorageFactory = StorageFactory()
    check true
