# WiscKey Storage Backend Implementation
# Implements the StorageBackend interface using WiscKey (LSM-Tree with value log separation)

import std/[options, locks, atomics, typedthreads, deques, os]
import backend

# WiscKey C bindings - cross-platform linking
const
  LevelDbLib = when defined(macosx): "libleveldb.dylib" else: "libleveldb.so"

when defined(macosx):
  {.passL: "-L/opt/homebrew/lib -lleveldb -Wl,-rpath,/opt/homebrew/lib".}
  {.passC: "-I/opt/homebrew/include".}
else:
  {.passL: "/usr/local/lib/libleveldb.a".}
  {.passC: "-I/usr/local/include".}

type
  WiscKeyBackend* {.acyclic.} = ref object of StorageBackend
    ## WiscKey storage backend.
    ## mu guards isOpen, db, and all options pointers against concurrent
    ## close vs put/get/writeBatch races (TOCTOU → nil ptr → SIGSEGV).
    mu*: Lock
    db*: pointer # leveldb_t*
    options*: pointer # leveldb_options_t*
    readOptions*: pointer # leveldb_readoptions_t*
    writeOptions*: pointer # leveldb_writeoptions_t* (sync=configured)
    noSyncWriteOptions*: pointer # leveldb_writeoptions_t* (sync=false, always)
    blockCache*: pointer # leveldb_cache_t* (LRU block cache, nil if default)
    path*: string
    isOpen*: bool
    syncWrites*: bool # Whether to sync writes to disk

  WiscKeyIterator* = ref object of StorageIterator
    ## WiscKey iterator
    iter*: pointer # leveldb_iterator_t*
    backendRef*: WiscKeyBackend

  WiscKeyStreamResultSet* = ref object of StreamResultSet
    ## Streaming result set implementation for WiscKey backend.
    ## Uses a background thread to prefetch data from LevelDB.
    backend*: WiscKeyBackend
    sharedData*: ptr StreamSharedData
    prefetchThread*: Thread[PrefetchWorkerArgs]
    config*: StreamConfig
    startKey*: string
    endKey*: string
    limit*: int

# Forward declarations for C functions
proc c_leveldb_open(options: pointer, name: cstring,
    err: ptr cstring): pointer {.
  importc: "leveldb_open", dynlib: LevelDbLib.}
proc c_leveldb_close(db: pointer) {.
  importc: "leveldb_close", dynlib: LevelDbLib.}
proc c_leveldb_put(db, writeOptions: pointer; key: cstring, keylen: csize_t;
    val: cstring, vallen: csize_t; err: ptr cstring) {.
  importc: "leveldb_put", dynlib: LevelDbLib.}
proc c_leveldb_get(db, readOptions: pointer; key: cstring, keylen: csize_t;
    vallen: ptr csize_t; err: ptr cstring): cstring {.
  importc: "leveldb_get", dynlib: LevelDbLib.}
proc c_leveldb_delete(db, writeOptions: pointer; key: cstring, keylen: csize_t;
    err: ptr cstring) {.
  importc: "leveldb_delete", dynlib: LevelDbLib.}
proc c_leveldb_create_iterator(db, readOptions: pointer): pointer {.
  importc: "leveldb_create_iterator", dynlib: LevelDbLib.}
proc c_leveldb_iter_destroy(iter: pointer) {.
  importc: "leveldb_iter_destroy", dynlib: LevelDbLib.}
proc c_leveldb_iter_valid(iter: pointer): uint8 {.
  importc: "leveldb_iter_valid", dynlib: LevelDbLib.}
proc c_leveldb_iter_seek_to_first(iter: pointer) {.
  importc: "leveldb_iter_seek_to_first", dynlib: LevelDbLib.}
proc c_leveldb_iter_seek_to_last(iter: pointer) {.
  importc: "leveldb_iter_seek_to_last", dynlib: LevelDbLib.}
proc c_leveldb_iter_seek(iter: pointer; key: cstring, keylen: csize_t) {.
  importc: "leveldb_iter_seek", dynlib: LevelDbLib.}
proc c_leveldb_iter_next(iter: pointer) {.
  importc: "leveldb_iter_next", dynlib: LevelDbLib.}
proc c_leveldb_iter_prev(iter: pointer) {.
  importc: "leveldb_iter_prev", dynlib: LevelDbLib.}
proc c_leveldb_iter_key(iter: pointer; keylen: ptr csize_t): cstring {.
  importc: "leveldb_iter_key", dynlib: LevelDbLib.}
proc c_leveldb_iter_value(iter: pointer; vallen: ptr csize_t): cstring {.
  importc: "leveldb_iter_value", dynlib: LevelDbLib.}
proc c_leveldb_iter_get_error(iter: pointer; err: ptr cstring) {.
  importc: "leveldb_iter_get_error", dynlib: LevelDbLib.}
proc c_leveldb_write(db, writeOptions, batch: pointer; err: ptr cstring) {.
  importc: "leveldb_write", dynlib: LevelDbLib.}
proc c_leveldb_compact_range(db: pointer; startKey: cstring,
    startKeyLen: csize_t; limitKey: cstring, limitKeyLen: csize_t) {.
  importc: "leveldb_compact_range", dynlib: LevelDbLib.}
proc c_leveldb_approximate_sizes(db: pointer; numRanges: cint;
    startKeys, startKeyLens, limitKeys, limitKeyLens: pointer;
    sizes: ptr uint64) {.
  importc: "leveldb_approximate_sizes", dynlib: LevelDbLib.}
proc c_leveldb_destroy_db(options: pointer; name: cstring; err: ptr cstring) {.
  importc: "leveldb_destroy_db", dynlib: LevelDbLib.}
proc c_leveldb_free(p: pointer) {.
  importc: "leveldb_free", dynlib: LevelDbLib.}
proc c_leveldb_property_value(db: pointer, name: cstring): cstring {.
  importc: "leveldb_property_value", dynlib: LevelDbLib.}
proc c_leveldb_options_create(): pointer {.
  importc: "leveldb_options_create", dynlib: LevelDbLib.}
proc c_leveldb_options_destroy(options: pointer) {.
  importc: "leveldb_options_destroy", dynlib: LevelDbLib.}
proc c_leveldb_options_set_create_if_missing(options: pointer; value: uint8) {.
  importc: "leveldb_options_set_create_if_missing", dynlib: LevelDbLib.}
proc c_leveldb_options_set_error_if_exists(options: pointer; value: uint8) {.
  importc: "leveldb_options_set_error_if_exists", dynlib: LevelDbLib.}
proc c_leveldb_options_set_write_buffer_size(options: pointer; size: csize_t) {.
  importc: "leveldb_options_set_write_buffer_size", dynlib: LevelDbLib.}
proc c_leveldb_options_set_max_open_files(options: pointer; maxFiles: cint) {.
  importc: "leveldb_options_set_max_open_files", dynlib: LevelDbLib.}
proc c_leveldb_options_set_block_size(options: pointer; size: csize_t) {.
  importc: "leveldb_options_set_block_size", dynlib: LevelDbLib.}
proc c_leveldb_options_set_compression(options: pointer; compression: cint) {.
  importc: "leveldb_options_set_compression", dynlib: LevelDbLib.}
proc c_leveldb_cache_create_lru(capacity: csize_t): pointer {.
  importc: "leveldb_cache_create_lru", dynlib: LevelDbLib.}
proc c_leveldb_cache_destroy(cache: pointer) {.
  importc: "leveldb_cache_destroy", dynlib: LevelDbLib.}
proc c_leveldb_options_set_cache(options, cache: pointer) {.
  importc: "leveldb_options_set_cache", dynlib: LevelDbLib.}
proc c_leveldb_readoptions_create(): pointer {.
  importc: "leveldb_readoptions_create", dynlib: LevelDbLib.}
proc c_leveldb_readoptions_destroy(options: pointer) {.
  importc: "leveldb_readoptions_destroy", dynlib: LevelDbLib.}
proc c_leveldb_writeoptions_create(): pointer {.
  importc: "leveldb_writeoptions_create", dynlib: LevelDbLib.}
proc c_leveldb_writeoptions_destroy(options: pointer) {.
  importc: "leveldb_writeoptions_destroy", dynlib: LevelDbLib.}
proc c_leveldb_writeoptions_set_sync(options: pointer; value: uint8) {.
  importc: "leveldb_writeoptions_set_sync", dynlib: LevelDbLib.}
proc c_leveldb_writebatch_create(): pointer {.
  importc: "leveldb_writebatch_create", dynlib: LevelDbLib.}
proc c_leveldb_writebatch_destroy(batch: pointer) {.
  importc: "leveldb_writebatch_destroy", dynlib: LevelDbLib.}
proc c_leveldb_writebatch_clear(batch: pointer) {.
  importc: "leveldb_writebatch_clear", dynlib: LevelDbLib.}
proc c_leveldb_writebatch_put(batch: pointer; key: cstring, keylen: csize_t;
    val: cstring, vallen: csize_t) {.
  importc: "leveldb_writebatch_put", dynlib: LevelDbLib.}
proc c_leveldb_writebatch_delete(batch: pointer; key: cstring,
    keylen: csize_t) {.
  importc: "leveldb_writebatch_delete", dynlib: LevelDbLib.}

proc newWiscKeyBackend*(config: StorageConfig): WiscKeyBackend =
  ## Create a new WiscKey backend
  new(result)
  initLock(result.mu)
  result.path = config.path
  result.isOpen = false

proc openWiscKey*(backend: WiscKeyBackend, config: StorageConfig): bool =
  ## Open the WiscKey database
  if backend.isOpen:
    return true

  # Always create fresh options for each open
  if backend.options != nil:
    c_leveldb_options_destroy(backend.options)
  if backend.readOptions != nil:
    c_leveldb_readoptions_destroy(backend.readOptions)
  if backend.writeOptions != nil:
    c_leveldb_writeoptions_destroy(backend.writeOptions)
  if backend.noSyncWriteOptions != nil:
    c_leveldb_writeoptions_destroy(backend.noSyncWriteOptions)

  # Create options
  backend.options = c_leveldb_options_create()
  c_leveldb_options_set_create_if_missing(backend.options, uint8(
    config.createIfMissing))
  c_leveldb_options_set_error_if_exists(backend.options, uint8(
    config.errorIfExists))
  c_leveldb_options_set_write_buffer_size(backend.options, csize_t(
    config.writeBufferSize))
  c_leveldb_options_set_max_open_files(backend.options, cint(
    config.maxOpenFiles))
  c_leveldb_options_set_block_size(backend.options, csize_t(config.blockSize))

  # Block cache
  if backend.blockCache != nil:
    c_leveldb_cache_destroy(backend.blockCache)
    backend.blockCache = nil
  if config.blockCacheSize > 0:
    backend.blockCache = c_leveldb_cache_create_lru(csize_t(
        config.blockCacheSize))
    c_leveldb_options_set_cache(backend.options, backend.blockCache)

  # Set compression
  case config.compression
  of ctSnappy:
    c_leveldb_options_set_compression(backend.options, 1) # leveldb_snappy_compression
  else:
    c_leveldb_options_set_compression(backend.options, 0) # leveldb_no_compression

  # Create read/write options
  backend.readOptions = c_leveldb_readoptions_create()
  backend.writeOptions = c_leveldb_writeoptions_create()
  backend.syncWrites = config.syncWrites

  # Set sync mode if configured
  if config.syncWrites:
    c_leveldb_writeoptions_set_sync(backend.writeOptions, 1.uint8)

  # Always-async write options (sync=false) used for non-durable staging writes
  backend.noSyncWriteOptions = c_leveldb_writeoptions_create()
  c_leveldb_writeoptions_set_sync(backend.noSyncWriteOptions, 0.uint8)

  # Open database
  var errPtr: cstring
  backend.db = c_leveldb_open(backend.options, config.path.cstring, addr errPtr)

  if errPtr != nil:
    # Copy error message before freeing
    var errMsg = $errPtr
    c_leveldb_free(errPtr)
    echo "LevelDB open error: ", errMsg
    return false

  backend.isOpen = true
  return true

method open*(backend: WiscKeyBackend, config: StorageConfig): bool =
  return openWiscKey(backend, config)

method close*(backend: WiscKeyBackend) =
  acquire(backend.mu)
  defer: release(backend.mu)
  if not backend.isOpen:
    return

  if backend.db != nil:
    # Force compaction to flush memtable to SSTable
    # This ensures all data is persisted before closing
    c_leveldb_compact_range(backend.db, nil, 0, nil, 0)
    c_leveldb_close(backend.db)
    backend.db = nil

  # Destroy options FIRST (they hold references to the cache)
  # The cache reference count is decremented when options are destroyed
  if backend.readOptions != nil:
    c_leveldb_readoptions_destroy(backend.readOptions)
    backend.readOptions = nil
  if backend.writeOptions != nil:
    c_leveldb_writeoptions_destroy(backend.writeOptions)
    backend.writeOptions = nil
  if backend.noSyncWriteOptions != nil:
    c_leveldb_writeoptions_destroy(backend.noSyncWriteOptions)
    backend.noSyncWriteOptions = nil
  if backend.options != nil:
    c_leveldb_options_destroy(backend.options)
    backend.options = nil

  # NOW destroy block cache (after options are gone)
  # This ensures proper reference counting
  if backend.blockCache != nil:
    c_leveldb_cache_destroy(backend.blockCache)
    backend.blockCache = nil

  backend.isOpen = false

method isOpen*(backend: WiscKeyBackend): bool =
  acquire(backend.mu)
  defer: release(backend.mu)
  return backend.isOpen

proc checkError(backend: WiscKeyBackend, errPtr: cstring): bool =
  if errPtr != nil:
    c_leveldb_free(errPtr)
    return true
  return false

method put*(backend: WiscKeyBackend, key: string, value: string): bool =
  acquire(backend.mu)
  defer: release(backend.mu)
  if not backend.isOpen:
    return false

  var errPtr: cstring
  c_leveldb_put(backend.db, backend.writeOptions, key.cstring, key.len.csize_t,
               value.cstring, value.len.csize_t, addr errPtr)

  return not checkError(backend, errPtr)

method get*(backend: WiscKeyBackend, key: string): Option[string] =
  acquire(backend.mu)
  defer: release(backend.mu)
  if not backend.isOpen:
    return none(string)

  var errPtr: cstring
  var vallen: csize_t
  let val = c_leveldb_get(backend.db, backend.readOptions, key.cstring, key.len.csize_t,
                          addr vallen, addr errPtr)

  if errPtr != nil:
    c_leveldb_free(errPtr)
    return none(string)

  if val == nil:
    return none(string)

  # Create a string from the buffer with the correct length
  # Use copyMem to handle binary data including empty strings
  var resultVal = newString(vallen)
  if vallen > 0:
    copyMem(resultVal[0].addr, val, vallen)
  c_leveldb_free(val)
  return some(resultVal)

method delete*(backend: WiscKeyBackend, key: string): bool =
  acquire(backend.mu)
  defer: release(backend.mu)
  if not backend.isOpen:
    return false

  var errPtr: cstring
  c_leveldb_delete(backend.db, backend.writeOptions, key.cstring,
      key.len.csize_t, addr errPtr)

  return not checkError(backend, errPtr)

method exists*(backend: WiscKeyBackend, key: string): bool =
  # get() acquires mu internally; do not acquire here to avoid recursive lock
  result = backend.get(key).isSome

method writeBatch*(backend: WiscKeyBackend, pairs: seq[KeyValuePair],
                  deletes: seq[string]): bool =
  acquire(backend.mu)
  defer: release(backend.mu)
  if not backend.isOpen:
    return false

  var errPtr: cstring
  let batch = c_leveldb_writebatch_create()

  for pair in pairs:
    c_leveldb_writebatch_put(batch, pair.key.cstring, pair.key.len.csize_t,
                             pair.value.cstring, pair.value.len.csize_t)

  for key in deletes:
    c_leveldb_writebatch_delete(batch, key.cstring, key.len.csize_t)

  c_leveldb_write(backend.db, backend.writeOptions, batch, addr errPtr)
  c_leveldb_writebatch_destroy(batch)

  return not checkError(backend, errPtr)

method writeBatchNoSync*(backend: WiscKeyBackend, pairs: seq[KeyValuePair],
                         deletes: seq[string]): bool =
  ## Atomically write pairs/deletes WITHOUT fdatasync.
  ## Safe for transactional intents: data lands in LevelDB's memtable
  ## immediately (readable via get()) but is not fdatasync'd until the
  ## subsequent commit write (which goes through writeBatch with sync=true).
  acquire(backend.mu)
  defer: release(backend.mu)
  if not backend.isOpen:
    return false

  var errPtr: cstring
  let batch = c_leveldb_writebatch_create()

  for pair in pairs:
    c_leveldb_writebatch_put(batch, pair.key.cstring, pair.key.len.csize_t,
                             pair.value.cstring, pair.value.len.csize_t)

  for key in deletes:
    c_leveldb_writebatch_delete(batch, key.cstring, key.len.csize_t)

  c_leveldb_write(backend.db, backend.noSyncWriteOptions, batch, addr errPtr)
  c_leveldb_writebatch_destroy(batch)

  return not checkError(backend, errPtr)

method newIterator*(backend: WiscKeyBackend): StorageIterator =
  if not backend.isOpen:
    return nil

  let iter = c_leveldb_create_iterator(backend.db, backend.readOptions)
  result = WiscKeyIterator(iter: iter, backendRef: backend)

# WiscKeyIterator methods - these use the pointer directly without type checking
# since we know the concrete type here
proc seekToFirstWiscKey*(iter: WiscKeyIterator): bool =
  c_leveldb_iter_seek_to_first(iter.iter)
  return c_leveldb_iter_valid(iter.iter) != 0

proc seekToLastWiscKey*(iter: WiscKeyIterator): bool =
  c_leveldb_iter_seek_to_last(iter.iter)
  return c_leveldb_iter_valid(iter.iter) != 0

proc seekWiscKey*(iter: WiscKeyIterator, key: string): bool =
  c_leveldb_iter_seek(iter.iter, key.cstring, key.len.csize_t)
  return c_leveldb_iter_valid(iter.iter) != 0

proc nextWiscKey*(iter: WiscKeyIterator): bool =
  c_leveldb_iter_next(iter.iter)
  return c_leveldb_iter_valid(iter.iter) != 0

proc prevWiscKey*(iter: WiscKeyIterator): bool =
  c_leveldb_iter_prev(iter.iter)
  return c_leveldb_iter_valid(iter.iter) != 0

proc validWiscKey*(iter: WiscKeyIterator): bool =
  return c_leveldb_iter_valid(iter.iter) != 0

proc keyWiscKey*(iter: WiscKeyIterator): string =
  var keylen: csize_t
  let keyC = c_leveldb_iter_key(iter.iter, addr keylen)
  if keyC != nil:
    var result = newString(keylen)
    if keylen > 0:
      copyMem(result[0].addr, keyC, keylen)
    return result
  return ""

proc valueWiscKey*(iter: WiscKeyIterator): string =
  var vallen: csize_t
  let valC = c_leveldb_iter_value(iter.iter, addr vallen)
  if valC != nil:
    var result = newString(vallen)
    if vallen > 0:
      copyMem(result[0].addr, valC, vallen)
    return result
  return ""

# Wrapper procs that use the concrete WiscKeyIterator methods
proc seekToFirstIter*(iter: StorageIterator): bool =
  if iter of WiscKeyIterator:
    let witer = WiscKeyIterator(iter)
    return seekToFirstWiscKey(witer)
  return false

proc seekToLastIter*(iter: StorageIterator): bool =
  if iter of WiscKeyIterator:
    let witer = WiscKeyIterator(iter)
    return seekToLastWiscKey(witer)
  return false

proc seekIter*(iter: StorageIterator, key: string): bool =
  if iter of WiscKeyIterator:
    let witer = WiscKeyIterator(iter)
    return seekWiscKey(witer, key)
  return false

proc nextIter*(iter: StorageIterator): bool =
  if iter of WiscKeyIterator:
    let witer = WiscKeyIterator(iter)
    return nextWiscKey(witer)
  return false

proc prevIter*(iter: StorageIterator): bool =
  if iter of WiscKeyIterator:
    let witer = WiscKeyIterator(iter)
    return prevWiscKey(witer)
  return false

proc validIter*(iter: StorageIterator): bool =
  if iter of WiscKeyIterator:
    let witer = WiscKeyIterator(iter)
    return validWiscKey(witer)
  return false

proc keyIter*(iter: StorageIterator): string =
  if iter of WiscKeyIterator:
    let witer = WiscKeyIterator(iter)
    return keyWiscKey(witer)
  return ""

proc valueIter*(iter: StorageIterator): string =
  if iter of WiscKeyIterator:
    let witer = WiscKeyIterator(iter)
    return valueWiscKey(witer)
  return ""

proc destroyIter*(iter: StorageIterator) =
  if iter of WiscKeyIterator:
    let witer = WiscKeyIterator(iter)
    if witer.iter != nil:
      c_leveldb_iter_destroy(witer.iter)
      witer.iter = nil

proc scan*(backend: WiscKeyBackend, startKey, endKey: string,
           limit: int = 0): seq[KeyValuePair] =
  ## High-level range scan: collect key-value pairs in [startKey, endKey).
  ## If endKey is empty, scans to the end of the database.
  ## If limit > 0, returns at most `limit` pairs.
  acquire(backend.mu)
  defer: release(backend.mu)
  if not backend.isOpen:
    return @[]

  let iter = c_leveldb_create_iterator(backend.db, backend.readOptions)
  defer: c_leveldb_iter_destroy(iter)

  if startKey.len > 0:
    c_leveldb_iter_seek(iter, startKey.cstring, startKey.len.csize_t)
  else:
    c_leveldb_iter_seek_to_first(iter)

  while c_leveldb_iter_valid(iter) != 0:
    var keylen: csize_t
    let keyC = c_leveldb_iter_key(iter, addr keylen)
    if keyC == nil: break
    var k = newString(keylen)
    if keylen > 0:
      copyMem(k[0].addr, keyC, keylen)

    # Check upper bound
    if endKey.len > 0 and k >= endKey:
      break

    var vallen: csize_t
    let valC = c_leveldb_iter_value(iter, addr vallen)
    var v = newString(vallen)
    if vallen > 0 and valC != nil:
      copyMem(v[0].addr, valC, vallen)

    result.add((key: k, value: v))
    if limit > 0 and result.len >= limit:
      break

    c_leveldb_iter_next(iter)

method compactRange*(backend: WiscKeyBackend, startKey: Option[string] = none(string),
                   endKey: Option[string] = none(string)) =
  if not backend.isOpen:
    return

  # Note: leveldb_compact_range takes size_t for key lengths
  # Passing nil for both means compact entire database
  c_leveldb_compact_range(backend.db, nil, 0, nil, 0)

method getStats*(backend: WiscKeyBackend): StorageStats =
  # WiscKey doesn't expose detailed stats via API, return basic stats
  return StorageStats()

method approximateSize*(backend: WiscKeyBackend, startKey: string,
                       endKey: string): int64 =
  if not backend.isOpen:
    return 0

  # Approximate size - simplified implementation
  # Note: Full implementation requires properly handling string to C string conversion
  return 0

method flush*(backend: WiscKeyBackend): bool =
  # LevelDB writes are always synchronous at some level
  # This is a no-op for LevelDB
  return backend.isOpen

proc getProperty*(backend: WiscKeyBackend, name: string): string =
  ## Query a LevelDB property (e.g. "leveldb.stats", "leveldb.num-files-at-level<N>").
  acquire(backend.mu)
  defer: release(backend.mu)
  if not backend.isOpen or backend.db == nil:
    return ""
  let val = c_leveldb_property_value(backend.db, name.cstring)
  if val == nil:
    return ""
  result = $val
  c_leveldb_free(val)

method destroy*(backend: WiscKeyBackend): bool =
  # First, close the database to flush any pending writes
  if backend.isOpen:
    backend.close()

  # Now destroy the database files
  # We need fresh options for destroy
  let destroyOptions = c_leveldb_options_create()

  var errPtr: cstring
  c_leveldb_destroy_db(destroyOptions, backend.path.cstring, addr errPtr)
  c_leveldb_options_destroy(destroyOptions)

  if errPtr != nil:
    c_leveldb_free(errPtr)
    return false

  return true

# ============================================================================
# Streaming Scan Implementation
# ============================================================================

# Prefetch worker thread proc - runs in background reading from LevelDB
proc prefetchWorker(args: PrefetchWorkerArgs) {.thread.} =
  ## Background thread that reads key-value pairs from LevelDB and fills buffer.
  ## Thread-safe: uses locks to protect buffer and atomics for state.
  let rs = args.resultSet
  let wkRs = cast[WiscKeyStreamResultSet](rs)
  let backend = wkRs.backend
  let shared = wkRs.sharedData
  let config = wkRs.config

  # Early exit if stream already closed
  if shared.state.load(moRelaxed) == ssClosed:
    return

  # Acquire backend lock for iterator creation
  acquire(backend.mu)
  if not backend.isOpen:
    release(backend.mu)
    shared.state.store(ssError, moRelaxed)
    shared.error.store("backend not open", moRelaxed)
    return

  let iter = c_leveldb_create_iterator(backend.db, backend.readOptions)
  release(backend.mu)

  if iter == nil:
    shared.state.store(ssError, moRelaxed)
    shared.error.store("failed to create iterator", moRelaxed)
    return

  # Position iterator
  if args.startKey.len > 0:
    c_leveldb_iter_seek(iter, args.startKey.cstring, args.startKey.len.csize_t)
  else:
    c_leveldb_iter_seek_to_first(iter)

  # Read loop - fill buffer until exhausted or closed
  var itemsRead = 0
  while true:
    # Check if we should stop
    let currentState = shared.state.load(moRelaxed)
    if currentState == ssClosed:
      break

    if args.limit > 0 and itemsRead >= args.limit:
      break

    # Check iterator validity
    if c_leveldb_iter_valid(iter) == 0:
      break

    # Read current key
    var keylen: csize_t
    let keyC = c_leveldb_iter_key(iter, addr keylen)
    if keyC == nil:
      break

    var k = newString(keylen)
    if keylen > 0:
      copyMem(k[0].addr, keyC, keylen)

    # Check upper bound
    if args.endKey.len > 0 and k >= args.endKey:
      break

    # Read current value
    var vallen: csize_t
    let valC = c_leveldb_iter_value(iter, addr vallen)
    var v = newString(vallen)
    if vallen > 0 and valC != nil:
      copyMem(v[0].addr, valC, vallen)

    # Add to buffer (thread-safe)
    acquire(shared.bufferLock)
    shared.buffer.addLast((key: k, value: v))
    let bufferLen = shared.buffer.len
    release(shared.bufferLock)

    inc itemsRead
    shared.totalRead.store(itemsRead, moRelaxed)

    # Pause if buffer is full - wait for consumer to drain
    while bufferLen >= config.bufferSize and
          shared.state.load(moRelaxed) != ssClosed:
      # Small sleep to avoid busy waiting
      os.sleep(10)
      acquire(shared.bufferLock)
      let currentLen = shared.buffer.len
      release(shared.bufferLock)
      if currentLen < config.bufferSize:
        break

    # Advance iterator
    c_leveldb_iter_next(iter)

  # Cleanup iterator
  c_leveldb_iter_destroy(iter)

  # Mark stream as exhausted (or error if closed during reading)
  let finalState = shared.state.load(moRelaxed)
  if finalState != ssClosed and finalState != ssError:
    shared.state.store(ssExhausted, moRelaxed)

# StreamResultSet implementation for WiscKey

method init*(rs: WiscKeyStreamResultSet, backend: StorageBackend,
            startKey: string, endKey: string, limit: int = 0,
            config: StreamConfig = StreamConfig()): bool =
  ## Initialize the stream for reading from WiscKey backend.
  if backend of WiscKeyBackend:
    rs.backend = WiscKeyBackend(backend)
  else:
    return false

  rs.startKey = startKey
  rs.endKey = endKey
  rs.limit = limit
  rs.config = config

  # Allocate shared data
  rs.sharedData = create(StreamSharedData)
  rs.sharedData.buffer = initDeque[KeyValuePair]()
  initLock(rs.sharedData.bufferLock)
  rs.sharedData.state.store(ssIdle, moRelaxed)
  rs.sharedData.error.store("", moRelaxed)
  rs.sharedData.totalRead.store(0, moRelaxed)
  rs.sharedData.consumerPos.store(0, moRelaxed)

  # Start prefetch thread
  rs.sharedData.state.store(ssReading, moRelaxed)
  let args: PrefetchWorkerArgs = (
    resultSet: rs,
    backend: rs.backend,
    startKey: startKey,
    endKey: endKey,
    limit: limit
  )
  createThread(rs.prefetchThread, prefetchWorker, args)

  return true

method next*(rs: WiscKeyStreamResultSet): Option[KeyValuePair] =
  ## Get the next key-value pair from the stream.
  ## Returns none() if exhausted or closed.
  let shared = rs.sharedData

  # Check state
  let currentState = shared.state.load(moRelaxed)
  if currentState == ssClosed:
    return none(KeyValuePair)

  if currentState == ssError:
    return none(KeyValuePair)

  # Try to get from buffer
  acquire(shared.bufferLock)
  if shared.buffer.len > 0:
    let kv = shared.buffer.popFirst()
    let consumerPos = shared.consumerPos.load(moRelaxed) + 1
    shared.consumerPos.store(consumerPos, moRelaxed)
    release(shared.bufferLock)
    return some(kv)
  release(shared.bufferLock)

  # Buffer empty - check if stream exhausted
  if currentState == ssExhausted:
    return none(KeyValuePair)

  # Wait briefly for prefetch to fill buffer
  for i in 0 ..< 10:
    os.sleep(1)
    acquire(shared.bufferLock)
    if shared.buffer.len > 0:
      let kv = shared.buffer.popFirst()
      let consumerPos = shared.consumerPos.load(moRelaxed) + 1
      shared.consumerPos.store(consumerPos, moRelaxed)
      release(shared.bufferLock)
      return some(kv)
    let state = shared.state.load(moRelaxed)
    release(shared.bufferLock)
    if state == ssExhausted or state == ssClosed:
      break

  # Still empty after wait
  return none(KeyValuePair)

method hasNext*(rs: WiscKeyStreamResultSet): bool =
  ## Check if more data is available.
  let shared = rs.sharedData

  let currentState = shared.state.load(moRelaxed)
  if currentState == ssClosed or currentState == ssError:
    return false

  # Check buffer
  acquire(shared.bufferLock)
  let bufferLen = shared.buffer.len
  release(shared.bufferLock)

  if bufferLen > 0:
    return true

  # Buffer empty but still reading
  if currentState == ssReading:
    return true

  # Exhausted and empty
  return false

method close*(rs: WiscKeyStreamResultSet) =
  ## Close the stream and stop the prefetch thread.
  let shared = rs.sharedData

  if shared == nil:
    return

  # Signal thread to stop
  shared.state.store(ssClosed, moRelaxed)

  # Wait for thread to finish
  joinThread(rs.prefetchThread)

  # Cleanup shared data
  deinitLock(shared.bufferLock)
  dealloc(shared)
  rs.sharedData = nil

method getState*(rs: WiscKeyStreamResultSet): StreamState =
  if rs.sharedData == nil:
    return ssIdle
  rs.sharedData.state.load(moRelaxed)

method getTotalRead*(rs: WiscKeyStreamResultSet): int =
  if rs.sharedData == nil:
    return 0
  rs.sharedData.totalRead.load(moRelaxed)

method getError*(rs: WiscKeyStreamResultSet): Option[string] =
  if rs.sharedData == nil:
    return none(string)
  let err = rs.sharedData.error.load(moRelaxed)
  if err.len > 0:
    return some(err)
  return none(string)

# Factory proc for creating streaming result set
proc newWiscKeyStreamResultSet*(backend: WiscKeyBackend, startKey: string,
                               endKey: string, limit: int = 0,
                               config: StreamConfig = defaultStreamConfig()): WiscKeyStreamResultSet =
  ## Create a new streaming result set for WiscKey backend.
  ## Automatically starts the prefetch thread.
  new(result)
  discard result.init(backend, startKey, endKey, limit, config)

# Streaming scan method
proc streamScan*(backend: WiscKeyBackend, startKey: string, endKey: string,
               limit: int = 0,
               config: StreamConfig = defaultStreamConfig()): StreamResultSet =
  ## Streaming range scan: returns a StreamResultSet for lazy iteration.
  ## Uses a background thread to prefetch data into a buffer.
  ## Consumer can read from buffer while prefetch thread continues reading.
  result = newWiscKeyStreamResultSet(backend, startKey, endKey, limit, config)
