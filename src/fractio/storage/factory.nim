# Storage Factory
# Creates storage backends based on configuration

import backend
import wisckey_backend

type
  StorageEngineType* = enum
    setWiscKey = "wisckey"
    setInMemory = "in_memory"
    setRocksDB = "rocksdb" # Future

  StorageFactory* = object
    ## Factory for creating storage backends

proc createStorageBackend*(engineType: StorageEngineType,
                          config: StorageConfig): StorageBackend =
  ## Create a storage backend based on the specified type
  case engineType
  of setWiscKey:
    let backend = newWiscKeyBackend(config)
    if backend.open(config):
      return backend
    else:
      raise newException(StorageError, "Failed to open WiscKey backend")
  of setInMemory:
    raise newException(StorageError, "InMemory backend not yet implemented")
  of setRocksDB:
    raise newException(StorageError, "RocksDB backend not yet implemented")

proc createWiscKeyBackend*(path: string,
                          createIfMissing: bool = true,
                          writeBufferSize: int = 4 * 1024 * 1024,
                          blockSize: int = 4 * 1024,
                          compression: CompressionType = ctSnappy): StorageBackend =
  ## Convenience function to create a WiscKey backend with default settings
  var config = defaultStorageConfig(path)
  config.createIfMissing = createIfMissing
  config.writeBufferSize = writeBufferSize
  config.blockSize = blockSize
  config.compression = compression
  return createStorageBackend(setWiscKey, config)

proc createWiscKeyBackendSync*(path: string,
                             createIfMissing: bool = true,
                             writeBufferSize: int = 4 * 1024 * 1024,
                             blockSize: int = 4 * 1024,
                             compression: CompressionType = ctSnappy): StorageBackend =
  ## Convenience function to create a WiscKey backend with SYNC writes
  ## This ensures data is persisted to disk before returning
  var config = defaultStorageConfig(path)
  config.createIfMissing = createIfMissing
  config.writeBufferSize = writeBufferSize
  config.blockSize = blockSize
  config.compression = compression
  config.syncWrites = true # Enable sync writes for durability
  return createStorageBackend(setWiscKey, config)
