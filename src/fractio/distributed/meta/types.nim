# Meta Types for Distribution Layer
#
# This module defines types for meta ranges - the two-level index structure
# used to locate data ranges in the cluster.
#
# Meta Range Structure:
# - meta1: /sys/meta1/<key> -> GroupDescriptor (points to meta2 range)
# - meta2: /sys/meta2/<key> -> GroupDescriptor (points to data range)

import std/locks
import std/options
import std/strutils
import std/tables
import fractio/distributed/raft/group_types
import fractio/distributed/meta/system_tables
import fractio/utils/binary

# ============================================================================
# Constants
# ============================================================================

const
  META1_KEY_PREFIX* = "/sys/meta1/"
    ## Prefix for meta1 keys

  META2_KEY_PREFIX* = "/sys/meta2/"
    ## Prefix for meta2 keys

  DEFAULT_CACHE_TTL_NS* = 60_000_000_000'i64
    ## Default cache TTL: 60 seconds in nanoseconds

  MAX_CACHE_ENTRIES* = 10000
    ## Maximum number of entries in the range cache

# Runtime constants - these alias the system_tables values
proc META1_RANGE_ID*(): GroupID =
  ## Special GroupID for the meta1 range (same as META_GROUP_ID)
  META_GROUP_ID

proc META2_RANGE_ID_START*(): GroupID =
  ## Starting GroupID for meta2 ranges (same as DATA_GROUP_START_ID)
  DATA_GROUP_START_ID

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
    descriptor*: GroupDescriptor
    cachedAtNs*: int64
      ## When this entry was cached (monotonic nanoseconds)
    expiresAtNs*: int64
      ## When this entry expires
    accessCount*: int64
      ## Number of times this entry has been accessed
    lastAccessNs*: int64
      ## Last access time

proc newCacheEntry*(desc: GroupDescriptor, nowNs, ttlNs: int64): CacheEntry =
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
  GroupCache* = ref object
    ## Cache of group descriptors for fast lookup
    ## Uses two-level caching:
    ## 1. Direct cache by GroupID
    ## 2. Key-based cache for meta2 groups

    # Direct cache by GroupID
    byGroupId*: Table[GroupID, CacheEntry]

    # Meta2 group cache (maps key prefix -> meta2 GroupDescriptor)
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

proc newGroupCache*(ttlNs: int64 = DEFAULT_CACHE_TTL_NS,
                    maxEntries: int = MAX_CACHE_ENTRIES): GroupCache =
  ## Create a new group cache
  new(result)
  result.byGroupId = initTable[GroupID, CacheEntry]()
  result.meta2Cache = initTable[string, CacheEntry]()
  result.ttlNs = ttlNs
  result.maxEntries = maxEntries
  result.hits = 0
  result.misses = 0
  result.evictions = 0
  initLock(result.cacheLock)

proc destroy*(cache: GroupCache) =
  ## Clean up cache resources
  deinitLock(cache.cacheLock)

proc get*(cache: GroupCache, groupId: GroupID, nowNs: int64): Option[
    GroupDescriptor] =
  ## Get a cached range descriptor by GroupID
  ## Returns none if not found or expired
  withLock cache.cacheLock:
    if cache.byGroupId.contains(groupId):
      let entry = cache.byGroupId[groupId]
      if not entry.isExpired(nowNs):
        entry.touch(nowNs)
        inc cache.hits
        return some(entry.descriptor)
      else:
        # Remove expired entry
        cache.byGroupId.del(groupId)
        inc cache.evictions

    inc cache.misses
    return none(GroupDescriptor)

proc put*(cache: GroupCache, desc: GroupDescriptor, nowNs: int64) =
  ## Add a group descriptor to the cache
  withLock cache.cacheLock:
    let entry = newCacheEntry(desc, nowNs, cache.ttlNs)

    # Add by GroupID
    cache.byGroupId[desc.groupId] = entry

    # Evict if over limit
    while cache.byGroupId.len > cache.maxEntries:
      var oldestKey: GroupID
      var oldestTime = int64.high
      for groupId, e in cache.byGroupId:
        if e.cachedAtNs < oldestTime:
          oldestTime = e.cachedAtNs
          oldestKey = groupId

      if cache.byGroupId.contains(oldestKey):
        cache.byGroupId.del(oldestKey)
        inc cache.evictions

proc putMeta2*(cache: GroupCache, key: seq[byte], desc: GroupDescriptor,
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

proc getMeta2*(cache: GroupCache, key: seq[byte], nowNs: int64): Option[
    GroupDescriptor] =
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
    return none(GroupDescriptor)

proc invalidate*(cache: GroupCache, groupId: GroupID) =
  ## Invalidate a cached group descriptor
  withLock cache.cacheLock:
    if cache.byGroupId.contains(groupId):
      cache.byGroupId.del(groupId)

proc invalidateAll*(cache: GroupCache) =
  ## Invalidate all cached entries
  withLock cache.cacheLock:
    cache.byGroupId.clear()
    cache.meta2Cache.clear()

proc stats*(cache: GroupCache): tuple[hits, misses, evictions: int64, size: int] =
  ## Get cache statistics
  withLock cache.cacheLock:
    result.hits = cache.hits
    result.misses = cache.misses
    result.evictions = cache.evictions
    result.size = cache.byGroupId.len

proc hitRate*(cache: GroupCache): float64 =
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

# =============================================================================
# NodeDescriptor Binary Serialization
# =============================================================================

const
  NODE_DESC_MAGIC* = [0x4E'u8, 0x44'u8, 0x53'u8] # "NDS" - Node Descriptor
  NODE_DESC_VERSION* = 0x01'u8

proc encodeNodeDescriptor*(desc: NodeDescriptor): string =
  ## Encode a NodeDescriptor to binary format.
  ##
  ## Binary format (little-endian):
  ## - Magic: 3 bytes ("NDS")
  ## - Version: 1 byte
  ## - NodeId: 4 bytes (uint32)
  ## - IsAlive: 1 byte (uint8 boolean)
  ## - LastHeartbeatNs: 8 bytes (int64)
  ## - Address: length-prefixed string
  ## - Locality count: 4 bytes (uint32)
  ## - Locality entries: each is 2 length-prefixed strings (key, value)
  var w = initBinaryWriter()
  w.writeBytes(NODE_DESC_MAGIC)
  w.writeU8(NODE_DESC_VERSION)
  w.writeU32(desc.nodeId.uint32)
  w.writeU8(if desc.isAlive: 1'u8 else: 0'u8)
  w.writeI64(desc.lastHeartbeatNs)
  w.writeString(desc.address)
  w.writeU32(uint32(desc.locality.len))
  for loc in desc.locality:
    w.writeString(loc.key)
    w.writeString(loc.value)
  w.finish()

proc decodeNodeDescriptor*(data: string): NodeDescriptor =
  ## Decode binary data to a NodeDescriptor.
  ## Raises ValueError if data is invalid.
  if data.len < 17: # Minimum: 3 + 1 + 4 + 1 + 8 = 17
    raise newException(ValueError, "NodeDescriptor: data too small")
  var r = initBinaryReader(data)

  # Verify magic header
  let magic0 = r.readU8()
  let magic1 = r.readU8()
  let magic2 = r.readU8()
  if magic0 != NODE_DESC_MAGIC[0] or magic1 != NODE_DESC_MAGIC[1] or
     magic2 != NODE_DESC_MAGIC[2]:
    raise newException(ValueError, "NodeDescriptor: invalid magic header")

  # Verify version
  let version = r.readU8()
  if version != NODE_DESC_VERSION:
    raise newException(ValueError, "NodeDescriptor: unsupported version " & $version)

  new(result)
  result.nodeId = NodeID(r.readU32())
  result.isAlive = r.readU8() == 1
  result.lastHeartbeatNs = r.readI64()
  result.address = r.readString()
  let locCount = int(r.readU32())
  result.locality = newSeq[tuple[key, value: string]](locCount)
  for i in 0..<locCount:
    result.locality[i] = (r.readString(), r.readString())

proc `$`*(desc: NodeDescriptor): string =
  ## String representation
  result = "NodeDescriptor(" & $desc.nodeId & ", " & desc.address & ")"

# ============================================================================
# Leaseholder Info
# ============================================================================

type
  LeaseholderInfo* = object
    ## Information about the current leaseholder for a range
    groupId*: GroupID
    leaseholder*: NodeID
    leaseExpirationNs*: int64
    epoch*: uint64

proc newLeaseholderInfo*(groupId: GroupID, leaseholder: NodeID,
                         expirationNs: int64,
                             epoch: uint64 = 0): LeaseholderInfo =
  ## Create new leaseholder info
  result = LeaseholderInfo(
    groupId: groupId,
    leaseholder: leaseholder,
    leaseExpirationNs: expirationNs,
    epoch: epoch
  )

proc isValid*(info: LeaseholderInfo, nowNs: int64): bool =
  ## Check if lease is still valid
  nowNs < info.leaseExpirationNs

# =============================================================================
# LeaseholderInfo Binary Serialization
# =============================================================================

const
  LEASEHOLDER_INFO_MAGIC* = [0x4C'u8, 0x48'u8, 0x49'u8] # "LHI" - LeaseHolder Info
  LEASEHOLDER_INFO_VERSION* = 0x01'u8

proc encodeLeaseholderInfo*(info: LeaseholderInfo): string =
  ## Encode a LeaseholderInfo to binary format.
  ##
  ## Binary format (little-endian):
  ## - Magic: 3 bytes ("LHI")
  ## - Version: 1 byte
  ## - GroupId: 16 bytes (ULID)
  ## - Leaseholder: 4 bytes (uint32)
  ## - LeaseExpirationNs: 8 bytes (int64)
  ## - Epoch: 8 bytes (uint64)
  ## Total: 40 bytes fixed
  var w = initBinaryWriter()
  w.writeBytes(LEASEHOLDER_INFO_MAGIC)
  w.writeU8(LEASEHOLDER_INFO_VERSION)
  w.writeBytes(groupIDToBytes(info.groupId))
  w.writeU32(info.leaseholder.uint32)
  w.writeI64(info.leaseExpirationNs)
  w.writeU64(info.epoch)
  w.finish()

proc decodeLeaseholderInfo*(data: string): LeaseholderInfo =
  ## Decode binary data to a LeaseholderInfo.
  ## Raises ValueError if data is invalid.
  if data.len < 40:
    raise newException(ValueError, "LeaseholderInfo: data too small")
  var r = initBinaryReader(data)

  # Verify magic header
  let magic0 = r.readU8()
  let magic1 = r.readU8()
  let magic2 = r.readU8()
  if magic0 != LEASEHOLDER_INFO_MAGIC[0] or magic1 != LEASEHOLDER_INFO_MAGIC[1] or
     magic2 != LEASEHOLDER_INFO_MAGIC[2]:
    raise newException(ValueError, "LeaseholderInfo: invalid magic header")

  # Verify version
  let version = r.readU8()
  if version != LEASEHOLDER_INFO_VERSION:
    raise newException(ValueError, "LeaseholderInfo: unsupported version " & $version)

  let groupIdBytes = r.readFixedString(16)
  result.groupId = groupIDFromBytes(groupIdBytes)
  result.leaseholder = NodeID(r.readU32())
  result.leaseExpirationNs = r.readI64()
  result.epoch = r.readU64()
