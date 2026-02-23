# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Table Reader Cache
##
## Thread-safe LRU cache for open SSTable readers.
## Avoids repeated file opens during reads while maintaining thread safety.

import ./sstable/reader
import ./block_cache
import fractio/storage/error
import std/[tables, locks, lists, times, hashes]

type
  CacheEntry = object
    reader: SsTableReader
    lastAccess: int64 # Monotonic counter for LRU

  TableReaderCacheObj* = object
    entries*: Table[string, CacheEntry]
    maxSize*: int
    lock*: Lock
    accessCounter*: int64 # Monotonic counter for LRU ordering

  TableReaderCache* = ptr TableReaderCacheObj
    ## Thread-safe LRU cache for open SSTable readers.

proc newTableReaderCache*(maxSize: int = 64): TableReaderCache =
  ## Creates a new table reader cache.
  ## maxSize is the maximum number of open readers to keep.
  var cache = create(TableReaderCacheObj)
  cache.entries = initTable[string, CacheEntry]()
  cache.maxSize = maxSize
  cache.accessCounter = 0
  initLock(cache.lock)
  result = cache

proc free*(cache: TableReaderCache) =
  ## Frees the cache and all resources.
  ## Note: We don't close readers to avoid use-after-free.
  ## Readers may still be in use by other threads.
  if cache != nil:
    acquire(cache.lock)
    # Just clear entries without closing readers
    cache.entries.clear()
    release(cache.lock)
    deinitLock(cache.lock)
    dealloc(cache)

proc get*(cache: TableReaderCache, path: string, sstableId: uint64,
          blockCache: BlockCache): StorageResult[SsTableReader] =
  ## Gets a reader from the cache, or opens a new one if not cached.
  ## Thread-safe.
  if cache == nil:
    # No cache, open directly
    return openSsTable(path, sstableId, blockCache)

  acquire(cache.lock)
  defer: release(cache.lock)

  # Check cache hit
  if path in cache.entries:
    var entry = cache.entries[path]
    # Update access time
    inc(cache.accessCounter)
    entry.lastAccess = cache.accessCounter
    cache.entries[path] = entry
    return ok[SsTableReader, StorageError](entry.reader)

  # Cache miss - open a new reader
  let readerResult = openSsTable(path, sstableId, blockCache)
  if readerResult.isErr:
    return readerResult

  let reader = readerResult.value

  # Evict LRU entries if at capacity
  while cache.entries.len >= cache.maxSize:
    # Find the least recently used entry
    var lruPath = ""
    var lruTime = high(int64)
    for p, e in cache.entries:
      if e.lastAccess < lruTime:
        lruTime = e.lastAccess
        lruPath = p

    if lruPath.len > 0:
      # Just remove from cache, don't close the reader
      # The reader might be in use by another thread
      cache.entries.del(lruPath)
    else:
      break

  # Add to cache
  inc(cache.accessCounter)
  cache.entries[path] = CacheEntry(
    reader: reader,
    lastAccess: cache.accessCounter
  )

  return ok[SsTableReader, StorageError](reader)

proc invalidate*(cache: TableReaderCache, path: string) =
  ## Removes a reader from the cache.
  ## Note: We don't close the reader here to avoid use-after-free.
  ## The reader may still be in use by another thread. It will be
  ## closed when the cache is destroyed or when it's eventually evicted.
  ## Thread-safe.
  if cache == nil:
    return

  acquire(cache.lock)
  defer: release(cache.lock)

  if path in cache.entries:
    # Just remove from cache, don't close the reader
    # The reader might be in use by another thread
    cache.entries.del(path)

proc clear*(cache: TableReaderCache) =
  ## Clears the cache.
  ## Note: We don't close the readers to avoid use-after-free.
  ## Readers may still be in use by other threads.
  ## Thread-safe.
  if cache == nil:
    return

  acquire(cache.lock)
  defer: release(cache.lock)

  # Just clear the cache entries without closing readers
  # Readers will be closed when they're no longer referenced
  cache.entries.clear()

proc close*(cache: TableReaderCache) =
  ## Closes the cache and frees all resources.
  if cache != nil:
    cache.clear()
    deinitLock(cache.lock)
    dealloc(cache)
