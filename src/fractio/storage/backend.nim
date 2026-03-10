# Storage Backend Interface
# Defines the contract for all storage engines in Fractio
# This allows for pluggable storage backends (WiscKey, LSM-Tree, etc.)

import std/[options, hashes]

# Forward declarations
type
  StorageBackend* = ref object of RootObj
    ## Base class for all storage backends

  StorageConfig* = object
    ## Configuration for storage backends
    path*: string
    maxOpenFiles*: int
    writeBufferSize*: int
    blockSize*: int
    compression*: CompressionType
    createIfMissing*: bool
    errorIfExists*: bool
    syncWrites*: bool
      ## If true, writes are synced to disk before returning (default: false)
    blockCacheSize*: int
      ## LevelDB block cache size in bytes; 0 = LevelDB default (8 MB)

    # WiscKey-specific options (key-value separation)
    vlogMaxSize*: int64
      ## Maximum size of vlog file (default: 1GB)
    vlogCleanThreshold*: int64
      ## Number of garbage records to trigger vlog GC (default: 100000)
    vlogMinCleanThreshold*: int64
      ## Minimum garbage records for manual cleanup (default: 1000)
    vlogCleanBufferSize*: int64
      ## Write buffer size for vlog GC (default: 64MB)

  StorageStats* = object
    ## Statistics from storage operations
    reads*: int64
    writes*: int64
    bytesRead*: int64
    bytesWritten*: int64
    compactions*: int64
    cacheHits*: int64
    cacheMisses*: int64

  CompressionType* = enum
    ctNone = "none"
    ctSnappy = "snappy"
    ctLz4 = "lz4"

  StorageError* = object of CatchableError
    ## Base error for storage operations
    code*: StorageErrorCode

  StorageErrorCode* = enum
    secNotFound
    secCorruption
    secIOError
    secNotSupported
    secAlreadyExists
    secInvalidArgument
    secOutOfMemory

  KeyValuePair* = tuple[key: string, value: string]
    ## A key-value pair returned by iterators

  StorageIterator* = ref object of RootObj
    ## Iterator interface for traversing storage
    backend*: StorageBackend

# Storage backend interface methods
method open*(backend: StorageBackend, config: StorageConfig): bool {.base.} =
  ## Open the storage backend with given configuration
  ## Returns true if successful
  discard

method close*(backend: StorageBackend) {.base.} =
  ## Close the storage backend and release resources
  discard

method isOpen*(backend: StorageBackend): bool {.base.} =
  ## Check if the storage backend is open
  result = false

method put*(backend: StorageBackend, key: string,
    value: string): bool {.base, gcsafe.} =
  ## Put a key-value pair into storage
  ## Returns true if successful
  discard false

method get*(backend: StorageBackend, key: string): Option[string] {.base, gcsafe.} =
  ## Get a value by key
  ## Returns some(value) if found, none if not found
  result = none(string)

method delete*(backend: StorageBackend, key: string): bool {.base, gcsafe.} =
  ## Delete a key from storage
  ## Returns true if the key was found and deleted
  discard false

method exists*(backend: StorageBackend, key: string): bool {.base, gcsafe.} =
  ## Check if a key exists in storage
  discard false

method writeBatch*(backend: StorageBackend, pairs: seq[KeyValuePair],
                   deletes: seq[string]): bool {.base, gcsafe.} =
  ## Write multiple key-value pairs and deletions atomically with sync.
  discard false

method writeBatchNoSync*(backend: StorageBackend, pairs: seq[KeyValuePair],
                         deletes: seq[string]): bool {.base, gcsafe.} =
  ## Write multiple key-value pairs and deletions atomically WITHOUT fsync.
  ## Use for staging data (e.g. transactional intents) where durability is
  ## not required until an explicit sync write (commit) follows.
  discard false

method newIterator*(backend: StorageBackend): StorageIterator {.base.} =
  ## Create a new iterator for traversing storage
  result = StorageIterator(backend: backend)

method seekToFirst*(iter: StorageIterator): bool {.base.} =
  ## Position iterator at first key
  discard false

method seekToLast*(iter: StorageIterator): bool {.base.} =
  ## Position iterator at last key
  discard false

method seek*(iter: StorageIterator, key: string): bool {.base.} =
  ## Position iterator at key or first key >= key
  discard false

method next*(iter: StorageIterator): bool {.base.} =
  ## Move iterator to next key
  discard false

method prev*(iter: StorageIterator): bool {.base.} =
  ## Move iterator to previous key
  discard false

method valid*(iter: StorageIterator): bool {.base.} =
  ## Check if iterator is at a valid position
  discard false

method key*(iter: StorageIterator): string {.base.} =
  ## Get current key (requires valid() == true)
  result = ""

method value*(iter: StorageIterator): string {.base.} =
  ## Get current value (requires valid() == true)
  result = ""

method destroy*(iter: StorageIterator) {.base.} =
  ## Destroy the iterator and free resources
  discard

method compactRange*(backend: StorageBackend, startKey: Option[string] = none(string),
                     endKey: Option[string] = none(string)) {.base.} =
  ## Compact storage in the given key range
  discard

method getStats*(backend: StorageBackend): StorageStats {.base.} =
  ## Get storage statistics
  result = StorageStats()

method approximateSize*(backend: StorageBackend, startKey: string,
                        endKey: string): int64 {.base.} =
  ## Get approximate size of storage in bytes for the given range
  result = 0

method flush*(backend: StorageBackend): bool {.base.} =
  ## Flush any pending writes to storage
  discard false

method destroy*(backend: StorageBackend): bool {.base.} =
  ## Destroy the storage and all its data
  discard false

# Helper procs
proc newStorageError*(code: StorageErrorCode, message: string): StorageError =
  result = StorageError(code: code, msg: message)

proc toHash*(key: string): Hash =
  result = hash(key)

# Default implementations for StorageConfig
proc defaultStorageConfig*(path: string): StorageConfig =
  result = StorageConfig(
    path: path,
    maxOpenFiles: 1000,
    writeBufferSize: 4 * 1024 * 1024, # 4MB
    blockSize: 4 * 1024, # 4KB
    compression: ctSnappy,
    createIfMissing: true,
    errorIfExists: false,
    syncWrites: false # Async by default for performance
  )
