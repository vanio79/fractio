# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Block Cache
##
## LRU cache for SSTable data blocks to avoid repeated disk reads.
## Provides significant performance improvement for point lookups and scans.

import std/[locks, times, tables, hashes, streams, options]

# ============================================================================
# Cache Key
# ============================================================================

type
  BlockKey* = object
    ## Unique identifier for a block in the cache
    sstableId*: uint64   # ID of the SSTable
    blockOffset*: uint64 # Offset of the block within the SSTable

proc hash*(key: BlockKey): Hash =
  ## Hash function for BlockKey
  var h: Hash = 0
  h = h !& hash(key.sstableId)
  h = h !& hash(key.blockOffset)
  result = !$h

proc `==`*(a, b: BlockKey): bool =
  a.sstableId == b.sstableId and a.blockOffset == b.blockOffset

# ============================================================================
# Cache Entry
# ============================================================================

type
  CacheEntry* = ref object
    ## Entry in the block cache
    key*: BlockKey
    data*: string      # Raw block data
    size*: int         # Size in bytes
    lastAccess*: float # Timestamp of last access
    accessCount*: int  # Number of times accessed
    prev*: CacheEntry  # For LRU list (older)
    next*: CacheEntry  # For LRU list (newer)

# ============================================================================
# Block Cache
# ============================================================================

type
  BlockCache* = ref object
    ## LRU cache for SSTable blocks
    capacity*: uint64  # Maximum bytes to cache
    usedBytes*: uint64 # Currently cached bytes
    entries*: Table[BlockKey, CacheEntry]
    lock*: Lock
    head*: CacheEntry  # Most recently used
    tail*: CacheEntry  # Least recently used
    hits*: uint64      # Cache hit count
    misses*: uint64    # Cache miss count

# ============================================================================
# Constructor
# ============================================================================

proc newBlockCache*(capacity: uint64 = 256 * 1024 * 1024): BlockCache =
  ## Create a new block cache with specified capacity (default: 256 MiB)
  result = BlockCache(
    capacity: capacity,
    usedBytes: 0,
    entries: initTable[BlockKey, CacheEntry](),
    hits: 0,
    misses: 0
  )
  initLock(result.lock)

# ============================================================================
# LRU List Management
# ============================================================================

proc removeFromList(cache: BlockCache, entry: CacheEntry) =
  ## Remove an entry from the LRU list
  if entry.prev != nil:
    entry.prev.next = entry.next
  else:
    cache.head = entry.next

  if entry.next != nil:
    entry.next.prev = entry.prev
  else:
    cache.tail = entry.prev

  entry.prev = nil
  entry.next = nil

proc addToFront(cache: BlockCache, entry: CacheEntry) =
  ## Add an entry to the front of the LRU list (most recently used)
  entry.next = cache.head
  entry.prev = nil

  if cache.head != nil:
    cache.head.prev = entry

  cache.head = entry

  if cache.tail == nil:
    cache.tail = entry

proc moveToToFront(cache: BlockCache, entry: CacheEntry) =
  ## Move an entry to the front of the LRU list
  if entry == cache.head:
    return # Already at front

  cache.removeFromList(entry)
  cache.addToFront(entry)

# ============================================================================
# Eviction
# ============================================================================

proc evictOne(cache: BlockCache): bool =
  ## Evict the least recently used entry. Returns true if something was evicted.
  if cache.tail == nil:
    return false

  let entry = cache.tail

  # Remove from LRU list
  cache.removeFromList(entry)

  # Remove from hash table
  cache.entries.del(entry.key)

  # Update size
  cache.usedBytes -= uint64(entry.size)

  return true

proc evictIfNeeded(cache: BlockCache, requiredBytes: uint64) =
  ## Evict entries until we have enough space
  while cache.usedBytes + requiredBytes > cache.capacity and cache.tail != nil:
    discard cache.evictOne()

# ============================================================================
# Public API
# ============================================================================

proc get*(cache: BlockCache, key: BlockKey): Option[string] =
  ## Get a block from the cache. Returns none if not found.
  cache.lock.acquire()
  defer: cache.lock.release()

  if key in cache.entries:
    let entry = cache.entries[key]

    # Update access stats
    entry.lastAccess = epochTime()
    inc entry.accessCount

    # Move to front of LRU list
    cache.moveToToFront(entry)

    # Record hit
    inc cache.hits

    return some(entry.data)

  # Record miss
  inc cache.misses
  return none(string)

proc put*(cache: BlockCache, key: BlockKey, data: string): void =
  ## Add a block to the cache
  let size = uint64(data.len)

  # Don't cache if block is larger than capacity
  if size > cache.capacity:
    return

  cache.lock.acquire()
  defer: cache.lock.release()

  # Check if already in cache
  if key in cache.entries:
    let existing = cache.entries[key]

    # Update data and move to front
    cache.usedBytes -= uint64(existing.size)
    existing.data = data
    existing.size = int(size)
    existing.lastAccess = epochTime()
    inc existing.accessCount
    cache.usedBytes += size
    cache.moveToToFront(existing)
    return

  # Evict if needed
  cache.evictIfNeeded(size)

  # Create new entry
  let entry = CacheEntry(
    key: key,
    data: data,
    size: int(size),
    lastAccess: epochTime(),
    accessCount: 1
  )

  # Add to cache
  cache.entries[key] = entry
  cache.usedBytes += size
  cache.addToFront(entry)

proc contains*(cache: BlockCache, key: BlockKey): bool =
  ## Check if a block is in the cache
  cache.lock.acquire()
  defer: cache.lock.release()
  result = key in cache.entries

proc remove*(cache: BlockCache, key: BlockKey): bool =
  ## Remove a block from the cache. Returns true if it was present.
  cache.lock.acquire()
  defer: cache.lock.release()

  if key in cache.entries:
    let entry = cache.entries[key]
    cache.removeFromList(entry)
    cache.entries.del(key)
    cache.usedBytes -= uint64(entry.size)
    return true

  return false

proc clear*(cache: BlockCache) =
  ## Clear all entries from the cache
  cache.lock.acquire()
  defer: cache.lock.release()

  cache.entries.clear()
  cache.usedBytes = 0
  cache.head = nil
  cache.tail = nil
  cache.hits = 0
  cache.misses = 0

# ============================================================================
# Statistics
# ============================================================================

proc size*(cache: BlockCache): uint64 =
  ## Get the current size of cached data in bytes
  cache.lock.acquire()
  defer: cache.lock.release()
  result = cache.usedBytes

proc count*(cache: BlockCache): int =
  ## Get the number of cached blocks
  cache.lock.acquire()
  defer: cache.lock.release()
  result = cache.entries.len

proc hitRate*(cache: BlockCache): float =
  ## Get the cache hit rate (0.0 to 1.0)
  cache.lock.acquire()
  defer: cache.lock.release()

  let total = cache.hits + cache.misses
  if total == 0:
    return 0.0
  result = float(cache.hits) / float(total)

proc stats*(cache: BlockCache): tuple[hits, misses: uint64, hitRate: float,
                                       size: uint64, count: int] =
  ## Get cache statistics
  cache.lock.acquire()
  defer: cache.lock.release()

  result.hits = cache.hits
  result.misses = cache.misses

  # Calculate hit rate inline to avoid deadlock from calling hitRate()
  let total = cache.hits + cache.misses
  if total == 0:
    result.hitRate = 0.0
  else:
    result.hitRate = float(cache.hits) / float(total)

  result.size = cache.usedBytes
  result.count = cache.entries.len

# ============================================================================
# Invalidate SSTable
# ============================================================================

proc invalidateSsTable*(cache: BlockCache, sstableId: uint64) =
  ## Remove all blocks belonging to an SSTable from the cache
  ## Called when an SSTable is compacted or deleted
  cache.lock.acquire()
  defer: cache.lock.release()

  var keysToRemove: seq[BlockKey] = @[]

  for key, entry in cache.entries:
    if key.sstableId == sstableId:
      keysToRemove.add(key)

  for key in keysToRemove:
    let entry = cache.entries[key]
    cache.removeFromList(entry)
    cache.usedBytes -= uint64(entry.size)
    cache.entries.del(key)

# ============================================================================
# Destructor
# ============================================================================

proc destroy*(cache: BlockCache) =
  ## Clean up cache resources
  cache.clear()
  deinitLock(cache.lock)
