# Unit tests for meta types module
#
# Tests for:
# - Meta key encoding/decoding
# - Cache entry lifecycle
# - Range cache operations
# - Node descriptor serialization
# - Leaseholder info
# - Cache eviction
# - Error conditions

import std/[unittest, options, tables]

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

  test "encode empty key":
    let key: seq[byte] = @[]
    let encoded1 = encodeMeta1Key(key)
    let encoded2 = encodeMeta2Key(key)
    check encoded1 == "/sys/meta1/"
    check encoded2 == "/sys/meta2/"

  test "encode key with special bytes":
    let key = @[byte(0), byte(255), byte(128)]
    let encoded = encodeMeta1Key(key)
    check encoded == "/sys/meta1/\x00\xFF\x80"

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

  test "decode preserves byte values":
    let key = @[byte(0), byte(127), byte(128), byte(255)]
    let encoded = encodeMeta1Key(key)
    let decoded = decodeMetaKey(encoded)
    check decoded.len == 4
    check decoded[0] == 0
    check decoded[1] == 127
    check decoded[2] == 128
    check decoded[3] == 255

  test "is meta1 key":
    check isMeta1Key("/sys/meta1/test")
    check not isMeta1Key("/sys/meta2/test")
    check not isMeta1Key("/other/test")
    check not isMeta1Key("")

  test "is meta2 key":
    check isMeta2Key("/sys/meta2/test")
    check not isMeta2Key("/sys/meta1/test")
    check not isMeta2Key("/other/test")
    check not isMeta2Key("")

  test "is meta key":
    check isMetaKey("/sys/meta1/test")
    check isMetaKey("/sys/meta2/test")
    check not isMetaKey("/other/test")
    check not isMetaKey("")

  test "decode invalid key raises error - no prefix":
    expect ValueError:
      discard decodeMetaKey("/invalid/key")

  test "decode invalid key raises error - wrong prefix":
    expect ValueError:
      discard decodeMetaKey("/sys/other/test")

  test "decode invalid key raises error - empty":
    expect ValueError:
      discard decodeMetaKey("")

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
    check entry.lastAccessNs == 1000

  test "create with zero TTL":
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 0'i64)
    check entry.expiresAtNs == 1000
    check entry.isExpired(1000)
    check entry.isExpired(1001)

  test "create with large TTL":
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 0'i64, int64.high)
    check not entry.isExpired(1000000000)
    check not entry.isExpired(int64.high - 1)
    check entry.isExpired(int64.high)

  test "is expired - not expired":
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check not entry.isExpired(50000)
    check not entry.isExpired(60999)

  test "is expired - at expiry":
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check entry.isExpired(61000)

  test "is expired - past expiry":
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check entry.isExpired(150000)

  test "touch updates access count":
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check entry.accessCount == 0
    entry.touch(5000)
    check entry.accessCount == 1
    entry.touch(10000)
    check entry.accessCount == 2
    entry.touch(15000)
    check entry.accessCount == 3

  test "touch updates last access time":
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check entry.lastAccessNs == 1000
    entry.touch(5000)
    check entry.lastAccessNs == 5000
    entry.touch(10000)
    check entry.lastAccessNs == 10000

  test "age calculation":
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check entry.ageNs(1000) == 0
    check entry.ageNs(5000) == 4000
    check entry.ageNs(61000) == 60000

  test "time until expiry - positive":
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check entry.timeUntilExpiryNs(5000) == 56000
    check entry.timeUntilExpiryNs(60999) == 1

  test "time until expiry - zero":
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check entry.timeUntilExpiryNs(61000) == 0

  test "time until expiry - negative":
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check entry.timeUntilExpiryNs(70000) == -9000

suite "Range Cache":
  test "create range cache":
    let cache = newGroupCache()
    check cache.ttlNs == DEFAULT_CACHE_TTL_NS
    check cache.maxEntries == MAX_CACHE_ENTRIES
    check cache.hits == 0
    check cache.misses == 0
    check cache.evictions == 0
    check len(cache.byGroupId) == 0
    check len(cache.meta2Cache) == 0
    cache.destroy()

  test "create with custom settings":
    let cache = newGroupCache(ttlNs = 30000'i64, maxEntries = 100)
    check cache.ttlNs == 30000
    check cache.maxEntries == 100
    cache.destroy()

  test "create with minimum settings":
    let cache = newGroupCache(ttlNs = 1'i64, maxEntries = 1)
    check cache.ttlNs == 1
    check cache.maxEntries == 1
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

  test "put multiple descriptors":
    let cache = newGroupCache(ttlNs = 60000'i64)
    for i in 1..5:
      let gid = genGroupID()
      let desc = newGroupDescriptor(
        gid,
        @[newReplicaDescriptor(NodeID(i), ReplicaID(i))]
      )
      cache.put(desc, 1000)

    check cache.stats().size == 5
    cache.destroy()

  test "put replaces existing":
    let cache = newGroupCache(ttlNs = 60000'i64)
    let gid = genGroupID()
    let desc1 = newGroupDescriptor(
      gid,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    desc1.generation = 1
    cache.put(desc1, 1000)

    let desc2 = newGroupDescriptor(
      gid,
      @[newReplicaDescriptor(NodeID(2), ReplicaID(2))]
    )
    desc2.generation = 2
    cache.put(desc2, 2000)

    let result = cache.get(gid, 5000)
    check result.isSome
    check result.get.generation == 2
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

    let result = cache.get(META_GROUP_ID, 2000)
    check result.isNone

    let stats = cache.stats()
    check stats.evictions == 1
    check stats.misses == 1
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

  test "invalidate non-existent group does nothing":
    let cache = newGroupCache(ttlNs = 60000'i64)
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    cache.put(desc, 1000)

    let gid = genGroupID()
    cache.invalidate(gid)

    check cache.stats().size == 1
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

    for gid in gids:
      check cache.get(gid, 5000).isNone
    cache.destroy()

  test "hit rate calculation - no operations":
    let cache = newGroupCache()
    check cache.hitRate() == 0.0
    cache.destroy()

  test "hit rate calculation - all hits":
    let cache = newGroupCache(ttlNs = 60000'i64)
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    cache.put(desc, 1000)

    discard cache.get(META_GROUP_ID, 2000)
    discard cache.get(META_GROUP_ID, 3000)
    discard cache.get(META_GROUP_ID, 4000)

    check cache.hitRate() == 1.0
    cache.destroy()

  test "hit rate calculation - mixed":
    let cache = newGroupCache(ttlNs = 60000'i64)
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    cache.put(desc, 1000)

    discard cache.get(META_GROUP_ID, 2000)
    discard cache.get(META_GROUP_ID, 3000)
    discard cache.get(META_GROUP_ID, 4000)
    discard cache.get(DATA_GROUP_START_ID, 5000)
    let gid = genGroupID()
    discard cache.get(gid, 6000)

    check cache.hitRate() == 0.6
    cache.destroy()

suite "Range Cache Eviction":
  test "eviction when over limit":
    let cache = newGroupCache(ttlNs = 60000'i64, maxEntries = 3)
    for i in 1..5:
      let gid = genGroupID()
      let desc = newGroupDescriptor(
        gid,
        @[newReplicaDescriptor(NodeID(i), ReplicaID(i))]
      )
      cache.put(desc, i * 1000)

    check cache.stats().size <= 3
    check cache.stats().evictions > 0
    cache.destroy()

  test "eviction removes oldest":
    let cache = newGroupCache(ttlNs = 60000'i64, maxEntries = 2)
    let gid1 = genGroupID()
    let gid2 = genGroupID()
    let gid3 = genGroupID()

    let desc1 = newGroupDescriptor(gid1, @[newReplicaDescriptor(NodeID(1),
        ReplicaID(1))])
    let desc2 = newGroupDescriptor(gid2, @[newReplicaDescriptor(NodeID(2),
        ReplicaID(2))])
    let desc3 = newGroupDescriptor(gid3, @[newReplicaDescriptor(NodeID(3),
        ReplicaID(3))])

    cache.put(desc1, 1000)
    cache.put(desc2, 2000)
    cache.put(desc3, 3000)

    check cache.get(gid3, 5000).isSome
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

  test "get expired meta2":
    let cache = newGroupCache(ttlNs = 1000'i64)
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let key = @[byte(50)]
    cache.putMeta2(key, desc, 0)

    let result = cache.getMeta2(key, 2000)
    check result.isNone
    check cache.stats().evictions == 1
    cache.destroy()

  test "different keys have different entries":
    let cache = newGroupCache(ttlNs = 60000'i64)
    let desc1 = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let desc2 = newGroupDescriptor(
      DATA_GROUP_START_ID,
      @[newReplicaDescriptor(NodeID(2), ReplicaID(2))]
    )

    let key1 = @[byte(10)]
    let key2 = @[byte(20)]
    cache.putMeta2(key1, desc1, 1000)
    cache.putMeta2(key2, desc2, 1000)

    let result1 = cache.getMeta2(key1, 5000)
    let result2 = cache.getMeta2(key2, 5000)
    check result1.isSome
    check result1.get.groupId == META_GROUP_ID
    check result2.isSome
    check result2.get.groupId == DATA_GROUP_START_ID
    cache.destroy()

  test "meta2 eviction when over limit":
    let cache = newGroupCache(ttlNs = 60000'i64, maxEntries = 3)
    for i in 1..5:
      let desc = newGroupDescriptor(
        genGroupID(),
        @[newReplicaDescriptor(NodeID(i), ReplicaID(i))]
      )
      cache.putMeta2(@[byte(i)], desc, i * 1000)

    check cache.stats().evictions >= 2
    cache.destroy()

suite "Node Descriptor":
  test "create node descriptor":
    let desc = newNodeDescriptor(NodeID(1), "localhost:8080")
    check desc.nodeId == NodeID(1)
    check desc.address == "localhost:8080"
    check desc.isAlive
    check desc.locality.len == 0
    check desc.lastHeartbeatNs == 0

  test "create with locality":
    var desc = newNodeDescriptor(NodeID(1), "localhost:8080")
    desc.locality.add(("region", "us-west"))
    desc.locality.add(("zone", "a"))
    desc.locality.add(("rack", "rack1"))
    check desc.locality.len == 3
    check desc.locality[0] == ("region", "us-west")
    check desc.locality[1] == ("zone", "a")
    check desc.locality[2] == ("rack", "rack1")

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

  test "binary serialization empty locality":
    let desc = newNodeDescriptor(NodeID(1), "localhost:8080")
    let encoded = encodeNodeDescriptor(desc)
    let decoded = decodeNodeDescriptor(encoded)
    check decoded.locality.len == 0

  test "binary serialization max NodeID":
    var desc = newNodeDescriptor(NodeID(uint32.high), "localhost:8080")
    desc.lastHeartbeatNs = int64.high
    let encoded = encodeNodeDescriptor(desc)
    let decoded = decodeNodeDescriptor(encoded)
    check decoded.nodeId == NodeID(uint32.high)
    check decoded.lastHeartbeatNs == int64.high

  test "binary serialization preserves magic header":
    let desc = newNodeDescriptor(NodeID(1), "localhost:8080")
    let encoded = encodeNodeDescriptor(desc)
    check encoded[0] == char(NODE_DESC_MAGIC[0])
    check encoded[1] == char(NODE_DESC_MAGIC[1])
    check encoded[2] == char(NODE_DESC_MAGIC[2])
    check encoded[3] == char(NODE_DESC_VERSION)

  test "decode invalid magic raises error":
    let badData = "XXX" & char(0x01) & "test"
    expect ValueError:
      discard decodeNodeDescriptor(badData)

  test "decode wrong version raises error":
    var badData = ""
    badData.add(char(NODE_DESC_MAGIC[0]))
    badData.add(char(NODE_DESC_MAGIC[1]))
    badData.add(char(NODE_DESC_MAGIC[2]))
    badData.add(char(0x02)) # Wrong version
    expect ValueError:
      discard decodeNodeDescriptor(badData)

  test "decode too small raises error":
    let badData = "tiny"
    expect ValueError:
      discard decodeNodeDescriptor(badData)

  test "string representation":
    let desc = newNodeDescriptor(NodeID(1), "localhost:8080")
    check $desc == "NodeDescriptor(n1, localhost:8080)"

  test "string representation with different address":
    let desc = newNodeDescriptor(NodeID(100), "192.168.1.1:9999")
    check $desc == "NodeDescriptor(n100, 192.168.1.1:9999)"

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

  test "create with max epoch":
    let info = newLeaseholderInfo(META_GROUP_ID, NodeID(1), 100000'i64, uint64.high)
    check info.epoch == uint64.high

  test "is valid - not expired":
    let info = newLeaseholderInfo(META_GROUP_ID, NodeID(1), 100000'i64)
    check info.isValid(50000)
    check info.isValid(99999)

  test "is valid - at expiry":
    let info = newLeaseholderInfo(META_GROUP_ID, NodeID(1), 100000'i64)
    check not info.isValid(100000)

  test "is valid - past expiry":
    let info = newLeaseholderInfo(META_GROUP_ID, NodeID(1), 100000'i64)
    check not info.isValid(150000)

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
    check encoded.len == 40

  test "binary serialization preserves magic header":
    let info = newLeaseholderInfo(META_GROUP_ID, NodeID(1), 100000'i64, 5)
    let encoded = encodeLeaseholderInfo(info)
    check encoded[0] == char(LEASEHOLDER_INFO_MAGIC[0])
    check encoded[1] == char(LEASEHOLDER_INFO_MAGIC[1])
    check encoded[2] == char(LEASEHOLDER_INFO_MAGIC[2])
    check encoded[3] == char(LEASEHOLDER_INFO_VERSION)

  test "decode invalid magic raises error":
    let badData = "XXX" & newString(37)
    expect ValueError:
      discard decodeLeaseholderInfo(badData)

  test "decode wrong version raises error":
    var badData = ""
    badData.add(char(LEASEHOLDER_INFO_MAGIC[0]))
    badData.add(char(LEASEHOLDER_INFO_MAGIC[1]))
    badData.add(char(LEASEHOLDER_INFO_MAGIC[2]))
    badData.add(char(0x02))
    badData.add(newString(36))
    expect ValueError:
      discard decodeLeaseholderInfo(badData)

  test "decode too small raises error":
    let badData = "tiny"
    expect ValueError:
      discard decodeLeaseholderInfo(badData)

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

  test "node descriptor magic":
    check NODE_DESC_MAGIC == [0x4E'u8, 0x44'u8, 0x53'u8]

  test "node descriptor version":
    check NODE_DESC_VERSION == 0x01'u8

  test "leaseholder info magic":
    check LEASEHOLDER_INFO_MAGIC == [0x4C'u8, 0x48'u8, 0x49'u8]

  test "leaseholder info version":
    check LEASEHOLDER_INFO_VERSION == 0x01'u8

suite "Thread Safety - Cache Operations":
  test "concurrent put operations":
    let cache = newGroupCache(ttlNs = 60000'i64, maxEntries = 100)
    for i in 1..10:
      let gid = genGroupID()
      let desc = newGroupDescriptor(gid, @[newReplicaDescriptor(NodeID(i),
          ReplicaID(i))])
      cache.put(desc, i * 1000)
    check cache.stats().size == 10
    cache.destroy()

  test "concurrent get operations":
    let cache = newGroupCache(ttlNs = 60000'i64)
    let desc = newGroupDescriptor(
      META_GROUP_ID,
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    cache.put(desc, 1000)
    for i in 1..10:
      let result = cache.get(META_GROUP_ID, i * 1000)
      check result.isSome
    let stats = cache.stats()
    check stats.hits == 10
    cache.destroy()

  test "concurrent invalidate":
    let cache = newGroupCache(ttlNs = 60000'i64)
    var gids: seq[GroupID] = @[]
    for i in 1..5:
      let gid = genGroupID()
      gids.add(gid)
      let desc = newGroupDescriptor(gid, @[newReplicaDescriptor(NodeID(i),
          ReplicaID(i))])
      cache.put(desc, 1000)
    for gid in gids:
      cache.invalidate(gid)
    check cache.stats().size == 0
    cache.destroy()
