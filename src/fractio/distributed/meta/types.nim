# Meta Types for Distribution Layer
#
# This module defines types for meta ranges - the two-level index structure
# used to locate data ranges in the cluster.
#
# Meta Range Structure:
# - meta1: /sys/meta1/<key> -> RangeDescriptor (points to meta2 range)
# - meta2: /sys/meta2/<key> -> RangeDescriptor (points to data range)

import std/locks
import std/json
import std/options
import std/strutils
import std/tables
import fractio/distributed/range/types

# ============================================================================
# Constants
# ============================================================================

const
  META1_KEY_PREFIX* = "/sys/meta1/"
    ## Prefix for meta1 keys

  META2_KEY_PREFIX* = "/sys/meta2/"
    ## Prefix for meta2 keys

  META1_RANGE_ID* = RangeID(1)
    ## Special RangeID for the meta1 range

  META2_RANGE_ID_START* = RangeID(2)
    ## Starting RangeID for meta2 ranges

  DEFAULT_CACHE_TTL_NS* = 60_000_000_000'i64
    ## Default cache TTL: 60 seconds in nanoseconds

  MAX_CACHE_ENTRIES* = 10000
    ## Maximum number of entries in the range cache

# ============================================================================
# Meta Key Encoding
# ============================================================================

proc encodeMeta1Key*(key: seq[byte]): string =
  ## Encode a key for meta1 lookup
  ## Format: /sys/meta1/<key>
  result = META1_KEY_PREFIX
  for b in key:
    result.add(char(b))

proc encodeMeta2Key*(key: seq[byte]): string =
  ## Encode a key for meta2 lookup
  ## Format: /sys/meta2/<key>
  result = META2_KEY_PREFIX
  for b in key:
    result.add(char(b))

proc decodeMetaKey*(encoded: string): seq[byte] =
  ## Decode a meta key back to the original key bytes
  ## Strips the prefix and returns the raw key bytes
  var prefix: string
  if encoded.startsWith(META1_KEY_PREFIX):
    prefix = META1_KEY_PREFIX
  elif encoded.startsWith(META2_KEY_PREFIX):
    prefix = META2_KEY_PREFIX
  else:
    raise newException(ValueError, "Invalid meta key format: " & encoded)

  result = newSeq[byte](encoded.len - prefix.len)
  for i in prefix.len..<encoded.len:
    result[i - prefix.len] = byte(encoded[i])

proc isMeta1Key*(key: string): bool =
  ## Check if a key is a meta1 key
  key.startsWith(META1_KEY_PREFIX)

proc isMeta2Key*(key: string): bool =
  ## Check if a key is a meta2 key
  key.startsWith(META2_KEY_PREFIX)

proc isMetaKey*(key: string): bool =
  ## Check if a key is any meta key
  isMeta1Key(key) or isMeta2Key(key)

# ============================================================================
# Cache Entry
# ============================================================================

type
  CacheEntry* = ref object
    ## A cached range descriptor with metadata
    descriptor*: RangeDescriptor
    cachedAtNs*: int64
      ## When this entry was cached (monotonic nanoseconds)
    expiresAtNs*: int64
      ## When this entry expires
    accessCount*: int64
      ## Number of times this entry has been accessed
    lastAccessNs*: int64
      ## Last access time

proc newCacheEntry*(desc: RangeDescriptor, nowNs, ttlNs: int64): CacheEntry =
  ## Create a new cache entry
  new(result)
  result.descriptor = desc
  result.cachedAtNs = nowNs
  result.expiresAtNs = nowNs + ttlNs
  result.accessCount = 0
  result.lastAccessNs = nowNs

proc isExpired*(entry: CacheEntry, nowNs: int64): bool =
  ## Check if this cache entry has expired
  nowNs >= entry.expiresAtNs

proc touch*(entry: CacheEntry, nowNs: int64) =
  ## Mark this entry as accessed
  inc entry.accessCount
  entry.lastAccessNs = nowNs

proc ageNs*(entry: CacheEntry, nowNs: int64): int64 =
  ## Get the age of this cache entry
  nowNs - entry.cachedAtNs

proc timeUntilExpiryNs*(entry: CacheEntry, nowNs: int64): int64 =
  ## Get time until this entry expires (negative if expired)
  entry.expiresAtNs - nowNs

# ============================================================================
# Range Cache
# ============================================================================

type
  RangeCache* = ref object
    ## Cache of range descriptors for fast lookup
    ## Uses two-level caching:
    ## 1. Direct cache by RangeID
    ## 2. Key-based cache for meta2 ranges

    # Direct cache by RangeID
    byRangeId*: Table[RangeID, CacheEntry]

    # Key-based cache for data ranges (maps startKey -> descriptor)
    byStartKey*: Table[string, CacheEntry]

    # Meta2 range cache (maps key prefix -> meta2 RangeDescriptor)
    meta2Cache*: Table[string, CacheEntry]

    # Configuration
    ttlNs*: int64
    maxEntries*: int

    # Statistics
    hits*: int64
    misses*: int64
    evictions*: int64

    # Thread safety
    cacheLock*: Lock

proc newRangeCache*(ttlNs: int64 = DEFAULT_CACHE_TTL_NS,
                    maxEntries: int = MAX_CACHE_ENTRIES): RangeCache =
  ## Create a new range cache
  new(result)
  result.byRangeId = initTable[RangeID, CacheEntry]()
  result.byStartKey = initTable[string, CacheEntry]()
  result.meta2Cache = initTable[string, CacheEntry]()
  result.ttlNs = ttlNs
  result.maxEntries = maxEntries
  result.hits = 0
  result.misses = 0
  result.evictions = 0
  initLock(result.cacheLock)

proc destroy*(cache: RangeCache) =
  ## Clean up cache resources
  deinitLock(cache.cacheLock)

proc get*(cache: RangeCache, rangeId: RangeID, nowNs: int64): Option[
    RangeDescriptor] =
  ## Get a cached range descriptor by RangeID
  ## Returns none if not found or expired
  withLock cache.cacheLock:
    if cache.byRangeId.contains(rangeId):
      let entry = cache.byRangeId[rangeId]
      if not entry.isExpired(nowNs):
        entry.touch(nowNs)
        inc cache.hits
        return some(entry.descriptor)
      else:
        # Remove expired entry
        cache.byRangeId.del(rangeId)
        inc cache.evictions

    inc cache.misses
    return none(RangeDescriptor)

proc getByKey*(cache: RangeCache, key: seq[byte], nowNs: int64): Option[
    RangeDescriptor] =
  ## Get a cached range descriptor by key
  ## Finds the range containing this key
  withLock cache.cacheLock:
    # Convert key to string for lookup
    var keyStr = newString(key.len)
    for i, b in key:
      keyStr[i] = char(b)

    # Look for exact match first
    if cache.byStartKey.contains(keyStr):
      let entry = cache.byStartKey[keyStr]
      if not entry.isExpired(nowNs):
        entry.touch(nowNs)
        inc cache.hits
        return some(entry.descriptor)

    # Look for containing range
    for startKey, entry in cache.byStartKey:
      if not entry.isExpired(nowNs):
        let desc = entry.descriptor
        if key >= desc.startKey and key < desc.endKey:
          entry.touch(nowNs)
          inc cache.hits
          return some(entry.descriptor)

    inc cache.misses
    return none(RangeDescriptor)

proc put*(cache: RangeCache, desc: RangeDescriptor, nowNs: int64) =
  ## Add a range descriptor to the cache
  withLock cache.cacheLock:
    let entry = newCacheEntry(desc, nowNs, cache.ttlNs)

    # Add by RangeID
    cache.byRangeId[desc.rangeId] = entry

    # Add by start key
    var startKeyStr = newString(desc.startKey.len)
    for i, b in desc.startKey:
      startKeyStr[i] = char(b)
    cache.byStartKey[startKeyStr] = entry

    # Evict if over limit
    while cache.byRangeId.len > cache.maxEntries:
      # Simple eviction: remove oldest entries
      # In production, would use LRU
      var oldestKey: RangeID
      var oldestTime = int64.high
      for rangeId, e in cache.byRangeId:
        if e.cachedAtNs < oldestTime:
          oldestTime = e.cachedAtNs
          oldestKey = rangeId

      if cache.byRangeId.contains(oldestKey):
        let oldEntry = cache.byRangeId[oldestKey]
        cache.byRangeId.del(oldestKey)

        # Also remove from byStartKey
        var oldStartKey = newString(oldEntry.descriptor.startKey.len)
        for i, b in oldEntry.descriptor.startKey:
          oldStartKey[i] = char(b)
        cache.byStartKey.del(oldStartKey)

        inc cache.evictions

proc putMeta2*(cache: RangeCache, key: seq[byte], desc: RangeDescriptor,
    nowNs: int64) =
  ## Add a meta2 range descriptor to the cache
  withLock cache.cacheLock:
    let entry = newCacheEntry(desc, nowNs, cache.ttlNs)
    var keyStr = newString(key.len)
    for i, b in key:
      keyStr[i] = char(b)
    cache.meta2Cache[keyStr] = entry

    # Evict if over limit
    while cache.meta2Cache.len > cache.maxEntries:
      var oldestKey = ""
      var oldestTime = int64.high
      for k, e in cache.meta2Cache:
        if e.cachedAtNs < oldestTime:
          oldestTime = e.cachedAtNs
          oldestKey = k
      cache.meta2Cache.del(oldestKey)
      inc cache.evictions

proc getMeta2*(cache: RangeCache, key: seq[byte], nowNs: int64): Option[
    RangeDescriptor] =
  ## Get a cached meta2 range descriptor
  withLock cache.cacheLock:
    var keyStr = newString(key.len)
    for i, b in key:
      keyStr[i] = char(b)

    if cache.meta2Cache.contains(keyStr):
      let entry = cache.meta2Cache[keyStr]
      if not entry.isExpired(nowNs):
        entry.touch(nowNs)
        inc cache.hits
        return some(entry.descriptor)
      else:
        cache.meta2Cache.del(keyStr)
        inc cache.evictions

    inc cache.misses
    return none(RangeDescriptor)

proc invalidate*(cache: RangeCache, rangeId: RangeID) =
  ## Invalidate a cached range descriptor
  withLock cache.cacheLock:
    if cache.byRangeId.contains(rangeId):
      let entry = cache.byRangeId[rangeId]
      let desc = entry.descriptor
      var startKey = newString(desc.startKey.len)
      for i, b in desc.startKey:
        startKey[i] = char(b)
      cache.byStartKey.del(startKey)
      cache.byRangeId.del(rangeId)

proc invalidateAll*(cache: RangeCache) =
  ## Invalidate all cached entries
  withLock cache.cacheLock:
    cache.byRangeId.clear()
    cache.byStartKey.clear()
    cache.meta2Cache.clear()

proc stats*(cache: RangeCache): tuple[hits, misses, evictions: int64, size: int] =
  ## Get cache statistics
  withLock cache.cacheLock:
    result.hits = cache.hits
    result.misses = cache.misses
    result.evictions = cache.evictions
    result.size = cache.byRangeId.len

proc hitRate*(cache: RangeCache): float64 =
  ## Calculate cache hit rate
  withLock cache.cacheLock:
    let total = cache.hits + cache.misses
    if total > 0:
      result = float64(cache.hits) / float64(total)
    else:
      result = 0.0

# ============================================================================
# Node Descriptor
# ============================================================================

type
  NodeDescriptor* = ref object
    ## Describes a node in the cluster
    nodeId*: NodeID
    address*: string
      ## Network address (host:port)
    locality*: seq[tuple[key, value: string]]
      ## Locality tags (e.g., region, zone)
    isAlive*: bool
      ## Whether the node is considered alive
    lastHeartbeatNs*: int64
      ## Last heartbeat received

proc newNodeDescriptor*(nodeId: NodeID, address: string): NodeDescriptor =
  ## Create a new node descriptor
  new(result)
  result.nodeId = nodeId
  result.address = address
  result.isAlive = true
  result.lastHeartbeatNs = 0

proc toJson*(desc: NodeDescriptor): JsonNode =
  ## Serialize NodeDescriptor to JSON
  var localityJson = newJArray()
  for loc in desc.locality:
    localityJson.add(%*{"key": loc.key, "value": loc.value})

  result = %*{
    "nodeId": desc.nodeId.uint32,
    "address": desc.address,
    "locality": localityJson,
    "isAlive": desc.isAlive,
    "lastHeartbeatNs": desc.lastHeartbeatNs
  }

proc parseNodeDescriptor*(json: JsonNode): NodeDescriptor =
  ## Parse NodeDescriptor from JSON
  new(result)
  result.nodeId = NodeID(json["nodeId"].getInt())
  result.address = json["address"].getStr()

  for locJson in json["locality"]:
    result.locality.add((locJson["key"].getStr(), locJson["value"].getStr()))

  result.isAlive = json["isAlive"].getBool()
  result.lastHeartbeatNs = json["lastHeartbeatNs"].getInt()

proc `$`*(desc: NodeDescriptor): string =
  ## String representation
  result = "NodeDescriptor(" & $desc.nodeId & ", " & desc.address & ")"

# ============================================================================
# Leaseholder Info
# ============================================================================

type
  LeaseholderInfo* = object
    ## Information about the current leaseholder for a range
    rangeId*: RangeID
    leaseholder*: NodeID
    leaseExpirationNs*: int64
    epoch*: uint64

proc newLeaseholderInfo*(rangeId: RangeID, leaseholder: NodeID,
                         expirationNs: int64,
                             epoch: uint64 = 0): LeaseholderInfo =
  ## Create new leaseholder info
  result = LeaseholderInfo(
    rangeId: rangeId,
    leaseholder: leaseholder,
    leaseExpirationNs: expirationNs,
    epoch: epoch
  )

proc isValid*(info: LeaseholderInfo, nowNs: int64): bool =
  ## Check if lease is still valid
  nowNs < info.leaseExpirationNs

proc toJson*(info: LeaseholderInfo): JsonNode =
  ## Serialize to JSON
  result = %*{
    "rangeId": info.rangeId.uint64,
    "leaseholder": info.leaseholder.uint32,
    "leaseExpirationNs": info.leaseExpirationNs,
    "epoch": info.epoch
  }

proc parseLeaseholderInfo*(json: JsonNode): LeaseholderInfo =
  ## Parse from JSON
  result = LeaseholderInfo(
    rangeId: RangeID(json["rangeId"].getInt()),
    leaseholder: NodeID(json["leaseholder"].getInt()),
    leaseExpirationNs: json["leaseExpirationNs"].getInt(),
    epoch: uint64(json["epoch"].getInt())
  )
