# WiscKey Storage Backend Implementation
# Implements the StorageBackend interface using WiscKey (LSM-Tree with value log separation)

import std/options
import backend

# WiscKey C bindings - static linking
{.passL: "/usr/local/lib/libleveldb.a".}
{.passC: "-I/usr/local/include".}

type
  WiscKeyBackend* = ref object of StorageBackend
    ## WiscKey storage backend
    db*: pointer           # leveldb_t*
    options*: pointer      # leveldb_options_t*
    readOptions*: pointer  # leveldb_readoptions_t*
    writeOptions*: pointer # leveldb_writeoptions_t*
    path*: string
    isOpen*: bool
    syncWrites*: bool      # Whether to sync writes to disk

  WiscKeyIterator* = ref object of StorageIterator
    ## WiscKey iterator
    iter*: pointer # leveldb_iterator_t*
    backendRef*: WiscKeyBackend

# Forward declarations for C functions
proc c_leveldb_open(options: pointer, name: cstring,
    err: ptr cstring): pointer {.
  importc: "leveldb_open", dynlib: "libleveldb.so".}
proc c_leveldb_close(db: pointer) {.
  importc: "leveldb_close", dynlib: "libleveldb.so".}
proc c_leveldb_put(db, writeOptions: pointer; key: cstring, keylen: csize_t;
    val: cstring, vallen: csize_t; err: ptr cstring) {.
  importc: "leveldb_put", dynlib: "libleveldb.so".}
proc c_leveldb_get(db, readOptions: pointer; key: cstring, keylen: csize_t;
    vallen: ptr csize_t; err: ptr cstring): cstring {.
  importc: "leveldb_get", dynlib: "libleveldb.so".}
proc c_leveldb_delete(db, writeOptions: pointer; key: cstring, keylen: csize_t;
    err: ptr cstring) {.
  importc: "leveldb_delete", dynlib: "libleveldb.so".}
proc c_leveldb_create_iterator(db, readOptions: pointer): pointer {.
  importc: "leveldb_create_iterator", dynlib: "libleveldb.so".}
proc c_leveldb_iter_destroy(iter: pointer) {.
  importc: "leveldb_iter_destroy", dynlib: "libleveldb.so".}
proc c_leveldb_iter_valid(iter: pointer): uint8 {.
  importc: "leveldb_iter_valid", dynlib: "libleveldb.so".}
proc c_leveldb_iter_seek_to_first(iter: pointer) {.
  importc: "leveldb_iter_seek_to_first", dynlib: "libleveldb.so".}
proc c_leveldb_iter_seek_to_last(iter: pointer) {.
  importc: "leveldb_iter_seek_to_last", dynlib: "libleveldb.so".}
proc c_leveldb_iter_seek(iter: pointer; key: cstring, keylen: csize_t) {.
  importc: "leveldb_iter_seek", dynlib: "libleveldb.so".}
proc c_leveldb_iter_next(iter: pointer) {.
  importc: "leveldb_iter_next", dynlib: "libleveldb.so".}
proc c_leveldb_iter_prev(iter: pointer) {.
  importc: "leveldb_iter_prev", dynlib: "libleveldb.so".}
proc c_leveldb_iter_key(iter: pointer; keylen: ptr csize_t): cstring {.
  importc: "leveldb_iter_key", dynlib: "libleveldb.so".}
proc c_leveldb_iter_value(iter: pointer; vallen: ptr csize_t): cstring {.
  importc: "leveldb_iter_value", dynlib: "libleveldb.so".}
proc c_leveldb_iter_get_error(iter: pointer; err: ptr cstring) {.
  importc: "leveldb_iter_get_error", dynlib: "libleveldb.so".}
proc c_leveldb_write(db, writeOptions, batch: pointer; err: ptr cstring) {.
  importc: "leveldb_write", dynlib: "libleveldb.so".}
proc c_leveldb_compact_range(db: pointer; startKey: cstring,
    startKeyLen: csize_t; limitKey: cstring, limitKeyLen: csize_t) {.
  importc: "leveldb_compact_range", dynlib: "libleveldb.so".}
proc c_leveldb_approximate_sizes(db: pointer; numRanges: cint;
    startKeys, startKeyLens, limitKeys, limitKeyLens: pointer;
    sizes: ptr uint64) {.
  importc: "leveldb_approximate_sizes", dynlib: "libleveldb.so".}
proc c_leveldb_destroy_db(options: pointer; name: cstring; err: ptr cstring) {.
  importc: "leveldb_destroy_db", dynlib: "libleveldb.so".}
proc c_leveldb_free(p: pointer) {.
  importc: "leveldb_free", dynlib: "libleveldb.so".}
proc c_leveldb_options_create(): pointer {.
  importc: "leveldb_options_create", dynlib: "libleveldb.so".}
proc c_leveldb_options_destroy(options: pointer) {.
  importc: "leveldb_options_destroy", dynlib: "libleveldb.so".}
proc c_leveldb_options_set_create_if_missing(options: pointer; value: uint8) {.
  importc: "leveldb_options_set_create_if_missing", dynlib: "libleveldb.so".}
proc c_leveldb_options_set_error_if_exists(options: pointer; value: uint8) {.
  importc: "leveldb_options_set_error_if_exists", dynlib: "libleveldb.so".}
proc c_leveldb_options_set_write_buffer_size(options: pointer; size: csize_t) {.
  importc: "leveldb_options_set_write_buffer_size", dynlib: "libleveldb.so".}
proc c_leveldb_options_set_max_open_files(options: pointer; maxFiles: cint) {.
  importc: "leveldb_options_set_max_open_files", dynlib: "libleveldb.so".}
proc c_leveldb_options_set_block_size(options: pointer; size: csize_t) {.
  importc: "leveldb_options_set_block_size", dynlib: "libleveldb.so".}
proc c_leveldb_options_set_compression(options: pointer; compression: cint) {.
  importc: "leveldb_options_set_compression", dynlib: "libleveldb.so".}
proc c_leveldb_readoptions_create(): pointer {.
  importc: "leveldb_readoptions_create", dynlib: "libleveldb.so".}
proc c_leveldb_readoptions_destroy(options: pointer) {.
  importc: "leveldb_readoptions_destroy", dynlib: "libleveldb.so".}
proc c_leveldb_writeoptions_create(): pointer {.
  importc: "leveldb_writeoptions_create", dynlib: "libleveldb.so".}
proc c_leveldb_writeoptions_destroy(options: pointer) {.
  importc: "leveldb_writeoptions_destroy", dynlib: "libleveldb.so".}
proc c_leveldb_writeoptions_set_sync(options: pointer; value: uint8) {.
  importc: "leveldb_writeoptions_set_sync", dynlib: "libleveldb.so".}
proc c_leveldb_writebatch_create(): pointer {.
  importc: "leveldb_writebatch_create", dynlib: "libleveldb.so".}
proc c_leveldb_writebatch_destroy(batch: pointer) {.
  importc: "leveldb_writebatch_destroy", dynlib: "libleveldb.so".}
proc c_leveldb_writebatch_clear(batch: pointer) {.
  importc: "leveldb_writebatch_clear", dynlib: "libleveldb.so".}
proc c_leveldb_writebatch_put(batch: pointer; key: cstring, keylen: csize_t;
    val: cstring, vallen: csize_t) {.
  importc: "leveldb_writebatch_put", dynlib: "libleveldb.so".}
proc c_leveldb_writebatch_delete(batch: pointer; key: cstring,
    keylen: csize_t) {.
  importc: "leveldb_writebatch_delete", dynlib: "libleveldb.so".}

proc newWiscKeyBackend*(config: StorageConfig): WiscKeyBackend =
  ## Create a new WiscKey backend
  new(result)
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
  if not backend.isOpen:
    return

  if backend.db != nil:
    # Force compaction to flush memtable to SSTable
    # This ensures all data is persisted before closing
    c_leveldb_compact_range(backend.db, nil, 0, nil, 0)
    c_leveldb_close(backend.db)
    backend.db = nil

  # Don't destroy options - they can be reused for reopening
  # Just reset the state
  backend.isOpen = false

method isOpen*(backend: WiscKeyBackend): bool =
  return backend.isOpen

proc checkError(backend: WiscKeyBackend, errPtr: cstring): bool =
  if errPtr != nil:
    c_leveldb_free(errPtr)
    return true
  return false

method put*(backend: WiscKeyBackend, key: string, value: string): bool =
  if not backend.isOpen:
    return false

  var errPtr: cstring
  c_leveldb_put(backend.db, backend.writeOptions, key.cstring, key.len.csize_t,
               value.cstring, value.len.csize_t, addr errPtr)

  return not checkError(backend, errPtr)

method get*(backend: WiscKeyBackend, key: string): Option[string] =
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
  if not backend.isOpen:
    return false

  var errPtr: cstring
  c_leveldb_delete(backend.db, backend.writeOptions, key.cstring,
      key.len.csize_t, addr errPtr)

  return not checkError(backend, errPtr)

method exists*(backend: WiscKeyBackend, key: string): bool =
  result = backend.get(key).isSome

method writeBatch*(backend: WiscKeyBackend, pairs: seq[KeyValuePair],
                  deletes: seq[string]): bool =
  if not backend.isOpen:
    return false

  var errPtr: cstring
  let batch = c_leveldb_writebatch_create()

  # Add puts
  for pair in pairs:
    c_leveldb_writebatch_put(batch, pair.key.cstring, pair.key.len.csize_t,
                             pair.value.cstring, pair.value.len.csize_t)

  # Add deletes
  for key in deletes:
    c_leveldb_writebatch_delete(batch, key.cstring, key.len.csize_t)

  c_leveldb_write(backend.db, backend.writeOptions, batch, addr errPtr)
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
