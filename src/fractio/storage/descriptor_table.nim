# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Descriptor Table - File Handle Cache
##
## Caches file descriptors (handles) to SSTables and blob files
## to avoid expensive fopen syscalls on repeated reads.
##
## The descriptor table is shared across all keyspaces in a database
## and enforces a global limit on open file handles.

import ./error
import std/[tables, streams, locks, atomics, os, deques]

type
  FileKind* = enum
    fkSsTable ## SSTable file
    fkBlob    ## Blob file

  FileKey* = object
    ## Unique identifier for a file in the cache
    case kind*: FileKind
    of fkSsTable:
      tableId*: uint64
      level*: int
    of fkBlob:
      blobId*: uint64

  CacheEntry* = ref object
    ## Entry in the descriptor table cache
    path*: string
    stream*: FileStream
    accessCount*: uint64
    inUse*: bool # Whether the file is currently being read

  DescriptorTableInner* = ref object
    ## Inner state of the descriptor table
    maxFiles*: int
    entries*: Table[string, CacheEntry] # path -> entry
    lruOrder*: Deque[string]            # LRU order (front = most recent, back = least)
    lock*: Lock
    hitCount*: Atomic[uint64]
    missCount*: Atomic[uint64]
    evictionCount*: Atomic[uint64]

  DescriptorTable* = ref object
    ## Thread-safe cache for open file handles.
    ##
    ## Usage:
    ##   let table = newDescriptorTable(128)
    ##   let stream = table.getStream("/path/to/file.sst")
    ##   # use stream...
    ##   table.releaseStream("/path/to/file.sst")
    inner*: DescriptorTableInner

# Forward declaration
proc evictIfNeeded(table: DescriptorTable)

# Create a new descriptor table
proc newDescriptorTable*(maxFiles: int = 64): DescriptorTable =
  ## Creates a new descriptor table with the given max open files limit.
  ##
  ## Parameters:
  ##   maxFiles: Maximum number of file handles to keep open (default 64)
  let inner = DescriptorTableInner(
    maxFiles: maxFiles,
    entries: initTable[string, CacheEntry](),
    lruOrder: initDeque[string]()
  )
  initLock(inner.lock)
  inner.hitCount.store(0, moRelaxed)
  inner.missCount.store(0, moRelaxed)
  inner.evictionCount.store(0, moRelaxed)

  result = DescriptorTable(inner: inner)

# Move path to front of LRU (most recently used)
proc touch(table: DescriptorTable, path: string) =
  ## Moves a path to the front of the LRU queue.
  ## Must be called with lock held.
  var found = false
  var newOrder: Deque[string] = initDeque[string]()

  # Add target to front first
  newOrder.addFirst(path)

  # Add all other entries in order
  for p in table.inner.lruOrder:
    if p != path:
      newOrder.addLast(p)

  table.inner.lruOrder = newOrder

# Get or open a file stream
proc getStream*(table: DescriptorTable, path: string): StorageResult[FileStream] =
  ## Gets a file stream from the cache, or opens it if not cached.
  ##
  ## IMPORTANT: After using the stream, call releaseStream() to allow eviction.
  ##
  ## Parameters:
  ##   path: Path to the file
  ##
  ## Returns: The file stream, or an error if the file cannot be opened.
  table.inner.lock.acquire()
  defer: table.inner.lock.release()

  # Check if already in cache
  if path in table.inner.entries:
    let entry = table.inner.entries[path]
    entry.accessCount += 1
    entry.inUse = true
    discard table.inner.hitCount.fetchAdd(1, moRelaxed)

    # Move to front of LRU
    table.touch(path)

    return ok[FileStream, StorageError](entry.stream)

  # Cache miss
  discard table.inner.missCount.fetchAdd(1, moRelaxed)

  # Check if file exists
  if not fileExists(path):
    return err[FileStream, StorageError](StorageError(
      kind: seIo,
      ioError: "File not found: " & path
    ))

  # Evict entries if at capacity
  evictIfNeeded(table)

  # Open new file
  var stream = newFileStream(path, fmRead)
  if stream == nil:
    return err[FileStream, StorageError](StorageError(
      kind: seIo,
      ioError: "Failed to open file: " & path
    ))

  # Create cache entry
  let entry = CacheEntry(
    path: path,
    stream: stream,
    accessCount: 1,
    inUse: true
  )

  table.inner.entries[path] = entry
  table.inner.lruOrder.addFirst(path)

  return ok[FileStream, StorageError](stream)

# Release a stream (allow eviction)
proc releaseStream*(table: DescriptorTable, path: string) =
  ## Releases a file stream, allowing it to be evicted from the cache.
  ##
  ## Call this when you're done reading from a stream obtained via getStream().
  table.inner.lock.acquire()
  defer: table.inner.lock.release()

  if path in table.inner.entries:
    table.inner.entries[path].inUse = false

# Evict entries if at capacity
proc evictIfNeeded(table: DescriptorTable) =
  ## Evicts least recently used entries until under capacity.
  ## Must be called with lock held.

  while table.inner.entries.len >= table.inner.maxFiles and
        table.inner.lruOrder.len > 0:
    # Find oldest entry that's not in use (from back of queue)
    var evicted = false

    for i in 0..<table.inner.lruOrder.len:
      let path = table.inner.lruOrder[table.inner.lruOrder.len - 1 - i]
      if path in table.inner.entries:
        let entry = table.inner.entries[path]
        if not entry.inUse:
          # Close and remove
          try:
            entry.stream.close()
          except:
            discard

          table.inner.entries.del(path)

          # Remove from LRU queue
          var newOrder: Deque[string] = initDeque[string]()
          for j in 0..<(table.inner.lruOrder.len - 1 - i):
            newOrder.addLast(table.inner.lruOrder[j])
          for j in (table.inner.lruOrder.len - i)..<(table.inner.lruOrder.len):
            newOrder.addLast(table.inner.lruOrder[j])
          table.inner.lruOrder = newOrder

          discard table.inner.evictionCount.fetchAdd(1, moRelaxed)
          evicted = true
          break

    if not evicted:
      # All entries are in use, can't evict
      break

# Remove a specific file from the cache
proc remove*(table: DescriptorTable, path: string) =
  ## Removes a file from the cache immediately.
  ##
  ## Use this when a file is deleted.
  table.inner.lock.acquire()
  defer: table.inner.lock.release()

  if path in table.inner.entries:
    let entry = table.inner.entries[path]
    try:
      entry.stream.close()
    except:
      discard
    table.inner.entries.del(path)

    # Remove from LRU queue
    var newOrder: Deque[string] = initDeque[string]()
    for p in table.inner.lruOrder:
      if p != path:
        newOrder.addLast(p)
    table.inner.lruOrder = newOrder

# Check if a file is in the cache
proc contains*(table: DescriptorTable, path: string): bool =
  ## Returns true if the file is currently cached.
  table.inner.lock.acquire()
  defer: table.inner.lock.release()
  result = path in table.inner.entries

# Get cache statistics
proc stats*(table: DescriptorTable): tuple[size: int, hits: uint64,
    misses: uint64, evictions: uint64] =
  ## Returns cache statistics.
  table.inner.lock.acquire()
  defer: table.inner.lock.release()
  result = (
    size: table.inner.entries.len,
    hits: table.inner.hitCount.load(moRelaxed),
    misses: table.inner.missCount.load(moRelaxed),
    evictions: table.inner.evictionCount.load(moRelaxed)
  )

# Get current size
proc len*(table: DescriptorTable): int =
  ## Returns the number of files currently in the cache.
  table.inner.lock.acquire()
  defer: table.inner.lock.release()
  result = table.inner.entries.len

# Clear the cache
proc clear*(table: DescriptorTable) =
  ## Closes all files and clears the cache.
  table.inner.lock.acquire()
  defer: table.inner.lock.release()

  for entry in table.inner.entries.values:
    try:
      entry.stream.close()
    except:
      discard

  table.inner.entries.clear()
  table.inner.lruOrder = initDeque[string]()

# Close the descriptor table
proc close*(table: DescriptorTable) =
  ## Closes all open files and releases resources.
  table.clear()
  deinitLock(table.inner.lock)

# Get max files limit
proc maxFiles*(table: DescriptorTable): int =
  ## Returns the maximum number of files allowed in the cache.
  table.inner.maxFiles

# Prefetch a file into the cache
proc prefetch*(table: DescriptorTable, path: string): bool =
  ## Prefetches a file into the cache without returning the stream.
  ##
  ## Returns true if the file was successfully cached.
  let result = table.getStream(path)
  if result.isOk:
    table.releaseStream(path)
    return true
  return false

# Get hit rate
proc hitRate*(table: DescriptorTable): float64 =
  ## Returns the cache hit rate (0.0 to 1.0).
  let hits = table.inner.hitCount.load(moRelaxed)
  let misses = table.inner.missCount.load(moRelaxed)
  let total = hits + misses
  if total == 0:
    return 0.0
  return float64(hits) / float64(total)
