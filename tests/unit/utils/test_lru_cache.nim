# Unit tests for fractio/utils/lru_cache.nim
# Covers: basic put/get, eviction order, capacity edges, del, clear, iteration.

import std/[unittest, options, strformat]
import fractio/utils/lru_cache

suite "LruCache: basic operations":

  test "empty cache has no keys":
    let c = initLruCache[string, int](10)
    check c.len == 0
    check c.hasKey("a") == false
    check c.get("a").isNone

  test "put then get returns the value":
    let c = initLruCache[string, int](10)
    c.put("a", 1)
    check c.len == 1
    check c.hasKey("a")
    check c.get("a") == some(1)

  test "put overwrites existing value":
    let c = initLruCache[string, int](10)
    c.put("a", 1)
    c.put("a", 2)
    check c.len == 1
    check c.get("a") == some(2)

  test "multiple keys coexist":
    let c = initLruCache[string, int](10)
    c.put("a", 1)
    c.put("b", 2)
    c.put("c", 3)
    check c.len == 3
    check c.get("a") == some(1)
    check c.get("b") == some(2)
    check c.get("c") == some(3)

suite "LruCache: eviction":

  test "evicting LRU when capacity exceeded":
    let c = initLruCache[string, int](3)
    c.put("a", 1)
    c.put("b", 2)
    c.put("c", 3)
    check c.len == 3
    c.put("d", 4) # Should evict "a" (oldest)
    check c.len == 3
    check not c.hasKey("a")
    check c.hasKey("b")
    check c.hasKey("c")
    check c.hasKey("d")

  test "get refreshes recency":
    let c = initLruCache[string, int](3)
    c.put("a", 1)
    c.put("b", 2)
    c.put("c", 3)
    discard c.get("a") # "a" is now most-recently-used
    c.put("d", 4) # Should evict "b" (now LRU)
    check c.hasKey("a")
    check not c.hasKey("b")
    check c.hasKey("c")
    check c.hasKey("d")

  test "put on existing key refreshes recency":
    let c = initLruCache[string, int](3)
    c.put("a", 1)
    c.put("b", 2)
    c.put("c", 3)
    c.put("a", 10) # Update "a", refresh recency
    c.put("d", 4) # Should evict "b"
    check c.hasKey("a")
    check not c.hasKey("b")
    check c.hasKey("c")
    check c.hasKey("d")
    check c.get("a") == some(10)

  test "capacity of 1 always evicts":
    let c = initLruCache[string, int](1)
    c.put("a", 1)
    check c.len == 1
    c.put("b", 2)
    check c.len == 1
    check not c.hasKey("a")
    check c.hasKey("b")

  test "capacity of 0 treated as 1":
    # The initLruCache caps capacity at min 1 to avoid degenerate behavior.
    let c = initLruCache[string, int](0)
    c.put("a", 1)
    c.put("b", 2)
    check c.len == 1
    check not c.hasKey("a")
    check c.hasKey("b")

suite "LruCache: del":

  test "del removes existing key":
    let c = initLruCache[string, int](10)
    c.put("a", 1)
    c.put("b", 2)
    let removed = c.del("a")
    check removed == true
    check c.len == 1
    check not c.hasKey("a")
    check c.hasKey("b")

  test "del returns false for missing key":
    let c = initLruCache[string, int](10)
    c.put("a", 1)
    let removed = c.del("z")
    check removed == false
    check c.len == 1

  test "del on empty cache is a no-op":
    let c = initLruCache[string, int](10)
    let removed = c.del("z")
    check removed == false
    check c.len == 0

suite "LruCache: clear":

  test "clear removes all entries":
    let c = initLruCache[string, int](10)
    c.put("a", 1)
    c.put("b", 2)
    c.put("c", 3)
    check c.len == 3
    c.clear()
    check c.len == 0
    check not c.hasKey("a")
    check not c.hasKey("b")
    check not c.hasKey("c")

  test "clear then put works":
    let c = initLruCache[string, int](10)
    for i in 0 ..< 5: c.put(&"k{i}", i)
    c.clear()
    c.put("x", 42)
    check c.len == 1
    check c.get("x") == some(42)

suite "LruCache: iteration":

  test "iter returns MRU → LRU order":
    let c = initLruCache[string, int](10)
    c.put("a", 1)
    c.put("b", 2)
    c.put("c", 3)
    var keys: seq[string] = @[]
    for k, _ in c.items:
      keys.add(k)
    check keys == @["c", "b", "a"]

  test "iter after get reflects updated order":
    let c = initLruCache[string, int](10)
    c.put("a", 1)
    c.put("b", 2)
    c.put("c", 3)
    discard c.get("a") # "a" is now MRU
    var keys: seq[string] = @[]
    for k, _ in c.items:
      keys.add(k)
    check keys == @["a", "c", "b"]

suite "LruCache: stress test":

  test "1000 puts with capacity 100 evicts oldest":
    let c = initLruCache[int, int](100)
    for i in 0 ..< 1000:
      c.put(i, i * 10)
    check c.len == 100
    # Most recent 100 keys (900..999) should be present
    for i in 900 ..< 1000:
      check c.hasKey(i)
    # Oldest 900 keys (0..899) should be evicted
    for i in 0 ..< 899:
      check not c.hasKey(i)
