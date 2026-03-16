# Range Lookup Protocol
#
# This module implements the range lookup protocol for locating data ranges.
# Uses a two-level index structure (meta1 -> meta2 -> data range).

import std/options
import std/tables
import std/locks

import fractio/distributed/raft/group_types
import fractio/distributed/meta/types

# ============================================================================
# Constants
# ============================================================================

const
  LOOKUP_TIMEOUT_NS* = 5_000_000_000'i64
    ## Default lookup timeout: 5 seconds

  MAX_LOOKUP_RETRIES* = 3
    ## Maximum number of lookup retries

  META1_START_KEY* = @[byte(0)]
    ## Start key for meta1 range (covers entire keyspace)

  META1_END_KEY*: seq[byte] = @[]
    ## End key for meta1 range (empty = unbounded)

# ============================================================================
# Lookup Error Types
# ============================================================================

type
  LookupError* = object of CatchableError
    ## Base error for lookup operations

  GroupNotFoundError* = object of LookupError
    ## Range not found in meta ranges

  MetaGroupUnavailableError* = object of LookupError
    ## Meta range is unavailable

  LeaseholderNotFoundError* = object of LookupError
    ## No leaseholder found for range

# ============================================================================
# Lookup Request/Response
# ============================================================================

type
  LookupRequest* = object
    ## Request to look up a range
    key*: seq[byte]
      ## The key to look up
    readAtNs*: int64
      ## Timestamp of the lookup
    maxStalenessNs*: int64
      ## Maximum staleness allowed for cached data

  LookupResponse* = object
    ## Response from a range lookup
    descriptor*: GroupDescriptor
      ## The range descriptor
    leaseholder*: NodeID
      ## Current leaseholder (if known)
    found*: bool
      ## Whether the range was found

proc newLookupRequest*(key: seq[byte], readAtNs: int64,
                       maxStalenessNs: int64 = 0): LookupRequest =
  ## Create a new lookup request
  result = LookupRequest(
    key: key,
    readAtNs: readAtNs,
    maxStalenessNs: maxStalenessNs
  )

proc newLookupResponse*(desc: GroupDescriptor,
                        leaseholder: NodeID = NodeID(0)): LookupResponse =
  ## Create a successful lookup response
  result = LookupResponse(
    descriptor: desc,
    leaseholder: leaseholder,
    found: true
  )

proc notFoundResponse*(): LookupResponse =
  ## Create a not-found response
  result = LookupResponse(
    descriptor: nil,
    leaseholder: NodeID(0),
    found: false
  )

# ============================================================================
# Range Lookup Protocol
# ============================================================================

type
  GroupLookup* = ref object
    ## Handles range lookups using the two-level meta index
    cache*: GroupCache
    meta1Descriptor*: Option[GroupDescriptor]
    meta2Descriptors*: Table[GroupID, GroupDescriptor]
    lock*: Lock

proc newGroupLookup*(cache: GroupCache): GroupLookup =
  ## Create a new range lookup handler
  new(result)
  result.cache = cache
  result.meta2Descriptors = initTable[GroupID, GroupDescriptor]()
  initLock(result.lock)

proc destroy*(lookup: GroupLookup) =
  ## Clean up resources
  deinitLock(lookup.lock)

proc setMeta1Descriptor*(lookup: GroupLookup, desc: GroupDescriptor) =
  ## Set the meta1 range descriptor
  withLock lookup.lock:
    lookup.meta1Descriptor = some(desc)

proc setMeta2Descriptor*(lookup: GroupLookup, groupId: GroupID,
                         desc: GroupDescriptor) =
  ## Set a meta2 range descriptor
  withLock lookup.lock:
    lookup.meta2Descriptors[groupId] = desc

proc findContainingGroup*(lookup: GroupLookup, key: seq[byte],
                          nowNs: int64): Option[GroupDescriptor] =
  ## Find the group for a key using cached descriptors.
  ## With hash-based spaces, key-range containment is not used;
  ## routing is done via resolveGroupId in raft_store.
  return none(GroupDescriptor)

proc lookupMeta1*(lookup: GroupLookup, key: seq[byte],
                  nowNs: int64): Option[GroupDescriptor] =
  ## Look up which meta2 range contains a key
  ## Returns the meta2 range descriptor

  withLock lookup.lock:
    if lookup.meta1Descriptor.isSome:
      # With hash-based spaces, meta2 lookup by key range is unused.
      # Routing is done via resolveGroupId in raft_store.
      discard

  return none(GroupDescriptor)

proc lookupMeta2*(lookup: GroupLookup, meta2RangeId: GroupID,
                  key: seq[byte], nowNs: int64): Option[GroupDescriptor] =
  ## Look up which data range contains a key in a meta2 range
  ## Returns the data range descriptor

  # Check cache first
  let cached = lookup.cache.getMeta2(key, nowNs)
  if cached.isSome:
    return cached

  # Check if we have the meta2 range descriptor
  withLock lookup.lock:
    if lookup.meta2Descriptors.contains(meta2RangeId):
      # In production, this would query the meta2 range via RPC
      # For now, we return the cached range if it contains the key
      # This is a simplified implementation

      return none(GroupDescriptor)

  return none(GroupDescriptor)

proc fullLookup*(lookup: GroupLookup, key: seq[byte],
                 nowNs: int64): LookupResponse =
  ## Perform a full lookup through the meta range hierarchy
  ## 1. Check cache
  ## 2. Look up meta2 range in meta1
  ## 3. Look up data range in meta2
  ## 4. Cache result

  # Step 1: Check cache (key-range lookup removed; hash-based routing
  # is handled by resolveGroupId in raft_store)
  discard

  # Step 2: Find meta2 range
  let meta2Opt = lookup.lookupMeta1(key, nowNs)
  if meta2Opt.isNone:
    return notFoundResponse()

  let meta2 = meta2Opt.get

  # Step 3: Find data range in meta2
  let dataOpt = lookup.lookupMeta2(meta2.groupId, key, nowNs)
  if dataOpt.isNone:
    return notFoundResponse()

  let data = dataOpt.get

  # Step 4: Cache and return
  lookup.cache.put(data, nowNs)

  return newLookupResponse(data)

proc getLeaseholder*(lookup: GroupLookup, groupId: GroupID,
                     nowNs: int64): Option[NodeID] =
  ## Get the current leaseholder for a range
  ## Returns the first voter replica (simplified)

  let descOpt = lookup.cache.get(groupId, nowNs)
  if descOpt.isSome:
    let desc = descOpt.get
    let voters = desc.getVoters()
    if voters.len > 0:
      return some(voters[0].nodeId)

  return none(NodeID)

proc updateDescriptor*(lookup: GroupLookup, desc: GroupDescriptor,
                       nowNs: int64) =
  ## Update a range descriptor in the cache
  lookup.cache.put(desc, nowNs)

proc invalidateGroup*(lookup: GroupLookup, groupId: GroupID) =
  ## Invalidate a cached range descriptor
  lookup.cache.invalidate(groupId)

# ============================================================================
# Key Range Utilities
# ============================================================================

proc keyInRange*(key: seq[byte], startKey, endKey: seq[byte]): bool =
  ## Check if a key is within a range
  result = key >= startKey
  if endKey.len > 0:
    result = result and key < endKey

proc rangesOverlap*(start1, end1, start2, end2: seq[byte]): bool =
  ## Check if two ranges overlap
  if end1.len == 0 and end2.len == 0:
    return true
  if end1.len == 0:
    return start1 < end2
  if end2.len == 0:
    return start2 < end1
  return start1 < end2 and start2 < end1

proc compareRanges*(start1, end1, start2, end2: seq[byte]): int =
  ## Compare two ranges
  ## Returns: -1 if range1 < range2, 0 if equal, 1 if range1 > range2
  if start1 < start2:
    return -1
  if start1 > start2:
    return 1
  if end1 < end2:
    return -1
  if end1 > end2:
    return 1
  return 0

proc splitKey*(startKey, endKey: seq[byte]): seq[byte] =
  ## Calculate a split key for a range
  ## Returns the midpoint key

  if endKey.len == 0:
    # Unbounded range - append a byte
    result = newSeq[byte](startKey.len + 1)
    for i, b in startKey:
      result[i] = b
    result[startKey.len] = byte(128) # Midpoint
    return

  # Find midpoint
  let maxLen = max(startKey.len, endKey.len)
  result = newSeq[byte](maxLen)

  var carry = 0
  for i in 0..<maxLen:
    let a = if i < startKey.len: int(startKey[i]) else: 0
    let b = if i < endKey.len: int(endKey[i]) else: 255
    let sum = a + b + carry
    result[i] = byte(sum div 2)
    carry = (sum mod 2) * 256

  # If there's a carry, we need an extra byte
  if carry > 0:
    result.add(byte(128))

proc nextKey*(key: seq[byte]): seq[byte] =
  ## Get the next key in lexicographic order
  result = newSeq[byte](key.len)
  for i, b in key:
    result[i] = b

  # Increment from the end
  for i in countdown(result.len - 1, 0):
    if result[i] < 255:
      result[i] = result[i] + 1
      return result
    result[i] = 0

  # All bytes were 255, prepend a 0
  result = newSeq[byte](key.len + 1)
  result[0] = 0
  for i, b in key:
    result[i + 1] = b

proc prevKey*(key: seq[byte]): seq[byte] =
  ## Get the previous key in lexicographic order
  if key.len == 0:
    return @[]

  result = newSeq[byte](key.len)
  for i, b in key:
    result[i] = b

  # Decrement from the end
  for i in countdown(result.len - 1, 0):
    if result[i] > 0:
      result[i] = result[i] - 1
      return result
    result[i] = 255

  # All bytes were 0, remove the first byte
  if result.len > 1:
    result = result[1..^1]
  else:
    result = @[]
