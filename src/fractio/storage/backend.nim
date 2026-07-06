# Storage Backend Interface
# Defines the contract for all storage engines in Fractio
# This allows for pluggable storage backends (WiscKey, LSM-Tree, etc.)

import std/[options, hashes, locks, deques, atomics]

# Forward declarations
type
  StorageBackend* = ref object of RootObj
    ## Base class for all storage backends

  StreamResultSet* = ref object of RootObj
    ## Streaming result set for range scans.
    ## Uses a background thread to read ahead and buffer results.
    ## Thread-safe: consumers can call next() while prefetch thread fills buffer.

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
    maxFileSize*: int
      ## LevelDB max SST file size in bytes; 0 = LevelDB default (2 MB).
      ## Larger files mean fewer L0 SSTs for the same data volume, which
      ## reduces L0 compaction backlog. With 100K+ rows and many small
      ## tombstone writes, the default 2 MB causes L0 to accumulate
      ## dozens of files, each held in RSS.
    l0CompactionTrigger*: int
      ## Number of L0 files that triggers a manual compaction. 0 = disabled.
      ## The fork's LevelDB C API does not expose level0_slowdown_writes_trigger,
      ## so we monitor L0 file count via leveldb_property_value("num-files-at-level0")
      ## and call c_leveldb_compact_range() to compact L0->L1 when threshold is hit.

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

  # ============================================================================
  # Streaming ResultSet Types
  # ============================================================================

  StreamConfig* = object
    ## Configuration for streaming result sets
    bufferSize*: int
      ## Number of key-value pairs to buffer (default: 1000)
    prefetchThreshold*: int
      ## Number of buffered items remaining before triggering prefetch (default: 100)

  StreamState* = enum
    ssIdle      ## Stream not started
    ssReading   ## Stream actively reading (prefetch thread running)
    ssExhausted ## Stream has read all data
    ssError     ## Stream encountered error
    ssClosed    ## Stream explicitly closed

  StreamError* = object of CatchableError
    ## Error during streaming operation
    code*: StreamErrorCode

  StreamErrorCode* = enum
    secStreamClosed    ## Stream was closed
    secStreamExhausted ## No more data available
    secPrefetchError   ## Background prefetch failed
    secInvalidState    ## Invalid operation for current state

  PrefetchWorkerArgs* = tuple
    ## Arguments passed to prefetch worker thread
    resultSet: StreamResultSet
    backend: StorageBackend
    startKey: string
    endKey: string
    limit: int

const
  DEFAULT_STREAM_BUFFER_SIZE* = 1000
  DEFAULT_PREFETCH_THRESHOLD* = 100

# Storage backend interface methods
method open*(backend: StorageBackend, config: StorageConfig): bool {.base.} =
  ## Open the storage backend with given configuration
  ## Returns true if successful
  discard

method close*(backend: StorageBackend) {.base.} =
  ## Close the storage backend and release resources
  discard

method isOpen*(backend: StorageBackend): bool {.base, gcsafe.} =
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

method newIterator*(backend: StorageBackend): StorageIterator {.base, gcsafe.} =
  ## Create a new iterator for traversing storage
  result = StorageIterator(backend: backend)

method seekToFirst*(iter: StorageIterator): bool {.base, gcsafe.} =
  ## Position iterator at first key
  discard false

method seekToLast*(iter: StorageIterator): bool {.base, gcsafe.} =
  ## Position iterator at last key
  discard false

method seek*(iter: StorageIterator, key: string): bool {.base, gcsafe.} =
  ## Position iterator at key or first key >= key
  discard false

method next*(iter: StorageIterator): bool {.base, gcsafe.} =
  ## Move iterator to next key
  discard false

method prev*(iter: StorageIterator): bool {.base, gcsafe.} =
  ## Move iterator to previous key
  discard false

method valid*(iter: StorageIterator): bool {.base, gcsafe.} =
  ## Check if iterator is at a valid position
  discard false

method key*(iter: StorageIterator): string {.base, gcsafe.} =
  ## Get current key (requires valid() == true)
  result = ""

method value*(iter: StorageIterator): string {.base, gcsafe.} =
  ## Get current value (requires valid() == true)
  result = ""

method destroy*(iter: StorageIterator) {.base, gcsafe.} =
  ## Destroy the iterator and free resources
  discard

method compactRange*(backend: StorageBackend, startKey: Option[string] = none(string),
                     endKey: Option[string] = none(string)) {.base.} =
  ## Compact storage in the given key range
  discard

method getL0FileCount*(backend: StorageBackend): int {.base, gcsafe.} =
  ## Return the number of files currently in L0 (the freshly-flushed
  ## memtable level). Returns 0 on error or if the backend does not
  ## expose this metric.
  result = 0

method maybeTriggerL0Compaction*(backend: StorageBackend,
    threshold: int): bool {.base, gcsafe.} =
  ## If the L0 file count is >= threshold, force a full compaction.
  ## Returns true if compaction was triggered. The default is a no-op.
  result = false

method getMemtableSize*(backend: StorageBackend): int64 {.base, gcsafe.} =
  ## Return the current memtable size in bytes. Returns 0 on error or
  ## if the backend does not expose this metric.
  result = 0'i64

method getTotalSizeBytes*(backend: StorageBackend): int64 {.base, gcsafe.} =
  ## Return the total size of all persisted data (SST files) on disk in bytes.
  ## This is the cold-storage footprint; combined with memtable/block cache,
  ## it gives the full storage size. Returns 0 on error.
  result = 0'i64

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

# ============================================================================
# Streaming ResultSet Implementation
# ============================================================================

# Internal data for StreamResultSet (shared across threads)
type
  StreamSharedData* = object
    ## Thread-safe shared data between consumer and prefetch thread
    buffer*: Deque[KeyValuePair]
      ## Ring buffer of prefetched key-value pairs
    bufferLock*: Lock
      ## Lock protecting buffer access
    state*: Atomic[StreamState]
      ## Current stream state
    error*: Atomic[string]
      ## Error message if state == ssError
    totalRead*: Atomic[int]
      ## Total number of items read so far
    consumerPos*: Atomic[int]
      ## Current position of consumer (for progress tracking)

proc newStreamError*(code: StreamErrorCode, message: string): StreamError =
  result = StreamError(code: code, msg: message)

# StreamResultSet methods (base class)
method init*(rs: StreamResultSet, backend: StorageBackend, startKey: string,
            endKey: string, limit: int = 0,
            config: StreamConfig = StreamConfig()): bool {.base, gcsafe.} =
  ## Initialize the stream for reading from the given range.
  ## Must be called before next() or close().
  ## Returns true if initialization successful.
  discard false

method next*(rs: StreamResultSet): Option[KeyValuePair] {.base, gcsafe.} =
  ## Get the next key-value pair from the stream.
  ## Returns some(pair) if available, none() if exhausted or closed.
  ## Thread-safe: blocks if buffer empty but prefetch still running.
  result = none(KeyValuePair)

method hasNext*(rs: StreamResultSet): bool {.base, gcsafe.} =
  ## Check if more data is available without consuming it.
  ## Returns true if buffer has items or prefetch thread is still running.
  discard false

method close*(rs: StreamResultSet) {.base, gcsafe.} =
  ## Close the stream and stop the prefetch thread.
  ## Must be called to release resources.
  discard

method getState*(rs: StreamResultSet): StreamState {.base, gcsafe.} =
  ## Get current stream state.
  discard ssIdle

method getTotalRead*(rs: StreamResultSet): int {.base, gcsafe.} =
  ## Get total number of items read by the stream.
  discard 0

method getError*(rs: StreamResultSet): Option[string] {.base, gcsafe.} =
  ## Get error message if stream is in error state.
  discard none(string)

# Helper procs for default StreamConfig
proc defaultStreamConfig*(): StreamConfig =
  result = StreamConfig(
    bufferSize: DEFAULT_STREAM_BUFFER_SIZE,
    prefetchThreshold: DEFAULT_PREFETCH_THRESHOLD
  )

proc smallStreamConfig*(): StreamConfig =
  ## Smaller buffer for testing or limited memory scenarios
  result = StreamConfig(
    bufferSize: 100,
    prefetchThreshold: 20
  )

proc largeStreamConfig*(): StreamConfig =
  ## Larger buffer for high-throughput scenarios
  result = StreamConfig(
    bufferSize: 5000,
    prefetchThreshold: 500
  )
