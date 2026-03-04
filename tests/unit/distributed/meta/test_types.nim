# Unit tests for meta types module
#
# Tests for:
# - Meta key encoding/decoding
# - Cache entry lifecycle
# - Range cache operations
# - Node descriptor serialization
# - Leaseholder info

import std/unittest
import std/json
import std/options

import fractio/distributed/meta/types
import fractio/distributed/range/types

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
    let desc = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check entry.descriptor.rangeId == RangeID(1)
    check entry.cachedAtNs == 1000
    check entry.expiresAtNs == 61000
    check entry.accessCount == 0

  test "is expired":
    let desc = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check not entry.isExpired(50000) # Not expired
    check entry.isExpired(61000) # Expired

  test "touch updates access":
    let desc = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
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
    let desc = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check entry.ageNs(5000) == 4000

  test "time until expiry":
    let desc = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    let entry = newCacheEntry(desc, 1000'i64, 60000'i64)
    check entry.timeUntilExpiryNs(5000) == 56000
    check entry.timeUntilExpiryNs(61000) == 0 # At expiry time

suite "Range Cache":
  test "create range cache":
    let cache = newRangeCache()
    check cache.ttlNs == DEFAULT_CACHE_TTL_NS
    check cache.maxEntries == MAX_CACHE_ENTRIES
    check cache.hits == 0
    check cache.misses == 0
    cache.destroy()

  test "create with custom settings":
    let cache = newRangeCache(ttlNs = 30000'i64, maxEntries = 100)
    check cache.ttlNs == 30000
    check cache.maxEntries == 100
    cache.destroy()

  test "put and get by range ID":
    let cache = newRangeCache(ttlNs = 60000'i64)
    let desc = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    cache.put(desc, 1000)

    let result = cache.get(RangeID(1), 5000)
    check result.isSome
    check result.get.rangeId == RangeID(1)

    let stats = cache.stats()
    check stats.hits == 1
    check stats.size == 1
    cache.destroy()

  test "get non-existent range":
    let cache = newRangeCache()
    let result = cache.get(RangeID(999), 1000)
    check result.isNone

    let stats = cache.stats()
    check stats.misses == 1
    cache.destroy()

  test "get expired entry":
    let cache = newRangeCache(ttlNs = 1000'i64)
    let desc = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    cache.put(desc, 0)

    # Entry should be expired
    let result = cache.get(RangeID(1), 2000)
    check result.isNone

    let stats = cache.stats()
    check stats.evictions == 1
    cache.destroy()

  test "get by key":
    let cache = newRangeCache(ttlNs = 60000'i64)
    let desc = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    cache.put(desc, 1000)

    # Key within range
    let key = @[byte(50)]
    let result = cache.getByKey(key, 5000)
    check result.isSome
    check result.get.rangeId == RangeID(1)
    cache.destroy()

  test "get by key not in range":
    let cache = newRangeCache(ttlNs = 60000'i64)
    let desc = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    cache.put(desc, 1000)

    # Key outside range
    let key = @[byte(150)]
    let result = cache.getByKey(key, 5000)
    check result.isNone
    cache.destroy()

  test "invalidate range":
    let cache = newRangeCache(ttlNs = 60000'i64)
    let desc = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    cache.put(desc, 1000)

    cache.invalidate(RangeID(1))

    let result = cache.get(RangeID(1), 5000)
    check result.isNone
    cache.destroy()

  test "invalidate all":
    let cache = newRangeCache(ttlNs = 60000'i64)
    for i in 1..5:
      let desc = newRangeDescriptor(
        RangeID(i),
        @[byte(i * 10)],
        @[byte((i + 1) * 10)],
        @[newReplicaDescriptor(NodeID(1), ReplicaID(i))]
      )
      cache.put(desc, 1000)

    check cache.stats().size == 5

    cache.invalidateAll()

    check cache.stats().size == 0
    cache.destroy()

  test "hit rate calculation":
    let cache = newRangeCache(ttlNs = 60000'i64)
    let desc = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    cache.put(desc, 1000)

    # 3 hits
    discard cache.get(RangeID(1), 2000)
    discard cache.get(RangeID(1), 3000)
    discard cache.get(RangeID(1), 4000)

    # 2 misses
    discard cache.get(RangeID(2), 5000)
    discard cache.get(RangeID(3), 6000)

    check cache.hitRate() == 0.6 # 3/5
    cache.destroy()

suite "Meta2 Cache":
  test "put and get meta2":
    let cache = newRangeCache(ttlNs = 60000'i64)
    let desc = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )

    let key = @[byte(50)]
    cache.putMeta2(key, desc, 1000)

    let result = cache.getMeta2(key, 5000)
    check result.isSome
    check result.get.rangeId == RangeID(1)
    cache.destroy()

  test "get non-existent meta2":
    let cache = newRangeCache()
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

  test "serialize to JSON":
    var desc = newNodeDescriptor(NodeID(1), "localhost:8080")
    desc.locality.add(("region", "us-west"))
    desc.locality.add(("zone", "a"))

    let json = desc.toJson()
    check json["nodeId"].getInt() == 1
    check json["address"].getStr() == "localhost:8080"
    check json["isAlive"].getBool()
    check json["locality"].len == 2

  test "parse from JSON":
    let json = %*{
      "nodeId": 1,
      "address": "localhost:8080",
      "locality": [{"key": "region", "value": "us-west"}],
      "isAlive": true,
      "lastHeartbeatNs": 12345
    }

    let desc = parseNodeDescriptor(json)
    check desc.nodeId == NodeID(1)
    check desc.address == "localhost:8080"
    check desc.locality.len == 1
    check desc.locality[0] == ("region", "us-west")
    check desc.isAlive
    check desc.lastHeartbeatNs == 12345

  test "string representation":
    let desc = newNodeDescriptor(NodeID(1), "localhost:8080")
    check $desc == "NodeDescriptor(n1, localhost:8080)"

suite "Leaseholder Info":
  test "create leaseholder info":
    let info = newLeaseholderInfo(RangeID(1), NodeID(1), 100000'i64)
    check info.rangeId == RangeID(1)
    check info.leaseholder == NodeID(1)
    check info.leaseExpirationNs == 100000
    check info.epoch == 0

  test "create with epoch":
    let info = newLeaseholderInfo(RangeID(1), NodeID(1), 100000'i64, 5)
    check info.epoch == 5

  test "is valid":
    let info = newLeaseholderInfo(RangeID(1), NodeID(1), 100000'i64)
    check info.isValid(50000) # Not expired
    check not info.isValid(100000) # Expired
    check not info.isValid(150000) # Expired

  test "serialize to JSON":
    let info = newLeaseholderInfo(RangeID(1), NodeID(1), 100000'i64, 5)
    let json = info.toJson()
    check json["rangeId"].getInt() == 1
    check json["leaseholder"].getInt() == 1
    check json["leaseExpirationNs"].getInt() == 100000
    check json["epoch"].getInt() == 5

  test "parse from JSON":
    let json = %*{
      "rangeId": 1,
      "leaseholder": 1,
      "leaseExpirationNs": 100000,
      "epoch": 5
    }

    let info = parseLeaseholderInfo(json)
    check info.rangeId == RangeID(1)
    check info.leaseholder == NodeID(1)
    check info.leaseExpirationNs == 100000
    check info.epoch == 5

suite "Constants":
  test "meta1 key prefix":
    check META1_KEY_PREFIX == "/sys/meta1/"

  test "meta2 key prefix":
    check META2_KEY_PREFIX == "/sys/meta2/"

  test "meta1 range ID":
    check META1_RANGE_ID == RangeID(1)

  test "meta2 range ID start":
    check META2_RANGE_ID_START == RangeID(2)

  test "default cache TTL":
    check DEFAULT_CACHE_TTL_NS == 60_000_000_000

  test "max cache entries":
    check MAX_CACHE_ENTRIES == 10000
