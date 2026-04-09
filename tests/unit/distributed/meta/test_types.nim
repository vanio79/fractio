# Unit tests for meta types module
#
# Tests for:
# - Meta key encoding/decoding
# - Cache entry lifecycle
# - Range cache operations
# - Node descriptor serialization
# - Leaseholder info

import std/unittest
import std/options

import fractio/distributed/meta/types
import fractio/distributed/meta/system_tables
import fractio/distributed/raft/group_types

suite "Meta Key Encoding":
  test "encode meta1 key":
    let key = @[byte(1), byte(2), byte(3)]
    let encoded = encodeMeta1Key(key)
    check encoded == "/sys/meta1/\x01\x02\x03"

  test "encode meta2 key":
    let key = @[byte(4), byte(5), byte(6)]
    let encoded = encodeMeta2Key(key)
    check encoded == "/sys/meta2/\x04\x05\x06"

  test "decode meta1 key":
    let key = @[byte(1), byte(2), byte(3)]
    let encoded = encodeMeta1Key(key)
    let decoded = decodeMetaKey(encoded)
    check decoded == key

  test "decode meta2 key":
    let key = @[byte(4), byte(5), byte(6)]
    let encoded = encodeMeta2Key(key)
    let decoded = decodeMetaKey(encoded)
    check decoded == key

  test "is meta1 key":
    check isMeta1Key("/sys/meta1/test")
    check not isMeta1Key("/sys/meta2/test")
    check not isMeta1Key("/other/test")

  test "is meta2 key":
    check isMeta2Key("/sys/meta2/test")
    check not isMeta2Key("/sys/meta1/test")
    check not isMeta2Key("/other/test")

  test "is meta key":
    check isMetaKey("/sys/meta1/test")
    check isMetaKey("/sys/meta2/test")
    check not isMetaKey("/other/test")

  test "decode invalid key raises error":
    expect ValueError:
      discard decodeMetaKey("/invalid/key")

suite "Cache Entry":
  test "create cache entry":
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check entry.descriptor.groupId == META_GROUP_ID
    check entry.cachedAtNs == 1000
    check entry.expiresAtNs == 61000
    check entry.accessCount == 0

  test "is expired":
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check not entry.isExpired(50000) # Not expired
    check entry.isExpired(61000) # Expired

  test "touch updates access":
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    entry.touch(5000)
    check entry.accessCount == 1
    check entry.lastAccessNs == 5000
    entry.touch(10000)
    check entry.accessCount == 2
    check entry.lastAccessNs == 10000

  test "age calculation":
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check entry.ageNs(5000) == 4000

  test "time until expiry":
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check entry.timeUntilExpiryNs(5000) == 56000
    check entry.timeUntilExpiryNs(61000) == 0 # At expiry time

suite "Range Cache":
  test "create range cache":
    let cache = newGroupCache()
    check cache.ttlNs == DEFAULT_CACHE_TTL_NS
    check cache.maxEntries == MAX_CACHE_ENTRIES
    check cache.hits == 0
    check cache.misses == 0
    cache.destroy()

  test "create with custom settings":
    let cache = newGroupCache(ttlNs = 30000'i64, maxEntries = 100)
    check cache.ttlNs == 30000
    check cache.maxEntries == 100
    cache.destroy()

  test "put and get by range ID":
    let cache = newGroupCache(ttlNs = 60000'i64)
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    cache.put(desc, 1000)

    let result = cache.get(META_GROUP_ID, 5000)
    check result.isSome
    check result.get.groupId == META_GROUP_ID

    let stats = cache.stats()
    check stats.hits == 1
    check stats.size == 1
    cache.destroy()

  test "get non-existent range":
    let cache = newGroupCache()
    let gid = genGroupID()
    let result = cache.get(gid, 1000)
    check result.isNone

    let stats = cache.stats()
    check stats.misses == 1
    cache.destroy()

  test "get expired entry":
    let cache = newGroupCache(ttlNs = 1000'i64)
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    cache.put(desc, 0)

    # Entry should be expired
    let result = cache.get(META_GROUP_ID, 2000)
    check result.isNone

    let stats = cache.stats()
    check stats.evictions == 1
    cache.destroy()

  test "invalidate group":
    let cache = newGroupCache(ttlNs = 60000'i64)
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    cache.put(desc, 1000)

    cache.invalidate(META_GROUP_ID)

    let result = cache.get(META_GROUP_ID, 5000)
    check result.isNone
    cache.destroy()

  test "invalidate all":
    let cache = newGroupCache(ttlNs = 60000'i64)
    var gids: seq[GroupID] = @[]
    for i in 1..5:
      let gid = genGroupID()
      gids.add(gid)
      let desc = newGroupDescriptor(
        gid,
        @[newReplicaDescriptor(NodeID(1), ReplicaID(i))]
      )
      cache.put(desc, 1000)

    check cache.stats().size == 5

    cache.invalidateAll()

    check cache.stats().size == 0
    cache.destroy()

  test "hit rate calculation":
    let cache = newGroupCache(ttlNs = 60000'i64)
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    cache.put(desc, 1000)

    # 3 hits
    discard cache.get(META_GROUP_ID, 2000)
    discard cache.get(META_GROUP_ID, 3000)
    discard cache.get(META_GROUP_ID, 4000)

    # 2 misses
    discard cache.get(DATA_GROUP_START_ID, 5000)
    let gid = genGroupID()
    discard cache.get(gid, 6000)

    check cache.hitRate() == 0.6 # 3/5
    cache.destroy()

suite "Meta2 Cache":
  test "put and get meta2":
    let cache = newGroupCache(ttlNs = 60000'i64)
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )

    let key = @[byte(50)]
    cache.putMeta2(key, desc, 1000)

    let result = cache.getMeta2(key, 5000)
    check result.isSome
    check result.get.groupId == META_GROUP_ID
    cache.destroy()

  test "get non-existent meta2":
    let cache = newGroupCache()
    let key = @[byte(50)]
    let result = cache.getMeta2(key, 1000)
    check result.isNone
    cache.destroy()

suite "Node Descriptor":
  test "create node descriptor":
    let desc = newNodeDescriptor(NodeID(1), "localhost:8080")
    check desc.nodeId == NodeID(1)
    check desc.address == "localhost:8080"
    check desc.isAlive
    check desc.locality.len == 0

  test "binary serialization roundtrip":
    var desc = newNodeDescriptor(NodeID(1), "localhost:8080")
    desc.locality.add(("region", "us-west"))
    desc.locality.add(("zone", "a"))

    let encoded = encodeNodeDescriptor(desc)
    let decoded = decodeNodeDescriptor(encoded)

    check decoded.nodeId == NodeID(1)
    check decoded.address == "localhost:8080"
    check decoded.isAlive
    check decoded.locality.len == 2
    check decoded.locality[0] == ("region", "us-west")
    check decoded.locality[1] == ("zone", "a")

  test "binary serialization with heartbeat":
    var desc = newNodeDescriptor(NodeID(42), "10.0.0.1:9000")
    desc.lastHeartbeatNs = 12345678
    desc.isAlive = false

    let encoded = encodeNodeDescriptor(desc)
    let decoded = decodeNodeDescriptor(encoded)

    check decoded.nodeId == NodeID(42)
    check decoded.address == "10.0.0.1:9000"
    check not decoded.isAlive
    check decoded.lastHeartbeatNs == 12345678

  test "string representation":
    let desc = newNodeDescriptor(NodeID(1), "localhost:8080")
    check $desc == "NodeDescriptor(n1, localhost:8080)"

suite "Leaseholder Info":
  test "create leaseholder info":
    let info = newLeaseholderInfo(META_GROUP_ID, NodeID(1), 100000'i64)
    check info.groupId == META_GROUP_ID
    check info.leaseholder == NodeID(1)
    check info.leaseExpirationNs == 100000
    check info.epoch == 0

  test "create with epoch":
    let info = newLeaseholderInfo(META_GROUP_ID, NodeID(1), 100000'i64, 5)
    check info.epoch == 5

  test "is valid":
    let info = newLeaseholderInfo(META_GROUP_ID, NodeID(1), 100000'i64)
    check info.isValid(50000) # Not expired
    check not info.isValid(100000) # Expired
    check not info.isValid(150000) # Expired

  test "binary serialization roundtrip":
    let info = newLeaseholderInfo(META_GROUP_ID, NodeID(1), 100000'i64, 5)
    let encoded = encodeLeaseholderInfo(info)
    let decoded = decodeLeaseholderInfo(encoded)
    check decoded.groupId == META_GROUP_ID
    check decoded.leaseholder == NodeID(1)
    check decoded.leaseExpirationNs == 100000
    check decoded.epoch == 5

  test "binary serialization fixed size":
    let info = newLeaseholderInfo(genGroupID(), NodeID(42), 999999'i64, 100)
    let encoded = encodeLeaseholderInfo(info)
    # Fixed size: 3 (magic) + 1 (version) + 16 (groupId) + 4 (leaseholder) + 8 (expiration) + 8 (epoch) = 40
    check encoded.len == 40

suite "Constants":
  test "meta1 key prefix":
    check META1_KEY_PREFIX == "/sys/meta1/"

  test "meta2 key prefix":
    check META2_KEY_PREFIX == "/sys/meta2/"

  test "meta1 range ID":
    check META1_RANGE_ID() == META_GROUP_ID

  test "meta2 range ID start":
    check META2_RANGE_ID_START() == DATA_GROUP_START_ID

  test "default cache TTL":
    check DEFAULT_CACHE_TTL_NS == 60_000_000_000

  test "max cache entries":
    check MAX_CACHE_ENTRIES == 10000
