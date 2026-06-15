## Generic LRU (Least-Recently-Used) cache with bounded size.
##
## Used by the protocol layer to cap unbounded maps like `keyVersions`
## and `commitIndex` so they can't grow without bound and exhaust memory.
##
## Bounded by **number of entries** (not bytes) — for protocol maps the
## per-entry size is roughly constant (key + value) so entry count is
## a good proxy. For per-byte budgeting, swap in a different LRU
## implementation.
##
## Implementation: hash table for O(1) lookup + doubly-linked list of
## node ids for O(1) move-to-front. `keyAt` maps node id → key (a parallel
## array to the doubly-linked list).
##
## Thread-safety: NOT thread-safe by itself. Wrap with a lock at the
## callsite (the existing protocol maps already use a Lock).

import std/[tables, options]

type
  LruNode = object
    prev: int   ## node id of previous entry (MRU direction); -1 = head's prev
    next: int   ## node id of next entry (LRU direction); -1 = tail's next
    inUse: bool ## true if this node id is currently linked in the list

  LruCache*[K, V] = ref object
    ## Generic LRU cache with O(1) get/put/del.
    capacity*: int
    map*: Table[K, V]          ## key → value
    keyAt*: seq[K]             ## node id → key (parallel to nodes)
    recencyIdx*: Table[K, int] ## key → node id
    nodes*: seq[LruNode]       ## doubly-linked list pointers
    head*: int                 ## node id of MRU end
    tail*: int                 ## node id of LRU end (-1 if empty)
    freeList*: seq[int]        ## recycled node ids available for reuse

proc initLruCache*[K, V](capacity: int): LruCache[K, V] =
  ## Create a new LRU cache with the given maximum entry count.
  ## When capacity is exceeded, the least-recently-used entry is evicted.
  new(result)
  result.capacity = max(capacity, 1)
  result.map = initTable[K, V]()
  result.keyAt = @[]
  result.recencyIdx = initTable[K, int]()
  result.nodes = @[]
  result.head = -1
  result.tail = -1
  result.freeList = @[]

proc len*[K, V](c: LruCache[K, V]): int {.inline, raises: [].} =
  c.map.len

proc hasKey*[K, V](c: LruCache[K, V], key: K): bool {.inline, raises: [].} =
  c.map.hasKey(key)

proc allocNodeId[K, V](c: LruCache[K, V]): int =
  ## Allocate a new node id, reusing from freeList if possible.
  if c.freeList.len > 0:
    result = c.freeList.pop()
  else:
    result = c.nodes.len
    c.nodes.add(LruNode())
    c.keyAt.add(default(K)) ## placeholder, will be overwritten
  c.nodes[result] = LruNode(prev: -1, next: -1, inUse: true)

proc unlinkNode[K, V](c: LruCache[K, V], id: int) =
  ## Remove a node from the doubly-linked list. O(1).
  let n = c.nodes[id]
  if n.prev >= 0:
    c.nodes[n.prev].next = n.next
  else:
    c.head = n.next
  if n.next >= 0:
    c.nodes[n.next].prev = n.prev
  else:
    c.tail = n.prev
  c.nodes[id].inUse = false

proc linkAtHead[K, V](c: LruCache[K, V], id: int) =
  ## Insert a node at the head (MRU position). O(1).
  if c.head >= 0:
    c.nodes[c.head].prev = id
  c.nodes[id].next = c.head
  c.nodes[id].prev = -1
  c.nodes[id].inUse = true
  c.head = id
  if c.tail < 0:
    c.tail = id

proc get*[K, V](c: LruCache[K, V], key: K): Option[V] =
  ## Get a value, marking it as most-recently-used.
  ## Returns none() if not present.
  ## O(1) amortized.
  if not c.map.hasKey(key): return none(V)
  let v = c.map.getOrDefault(key)
  let id = c.recencyIdx.getOrDefault(key, -1)
  if id >= 0 and c.nodes[id].inUse:
    # Move to head (MRU)
    c.unlinkNode(id)
    c.linkAtHead(id)
  return some(v)

proc put*[K, V](c: LruCache[K, V], key: K, value: V) =
  ## Insert or update a value. Marks it as most-recently-used.
  ## If the cache is at capacity, the LRU entry is evicted.
  ## O(1) amortized.
  if c.map.hasKey(key):
    # Update existing value
    c.map[key] = value
    # Move to head
    let id = c.recencyIdx.getOrDefault(key, -1)
    if id >= 0 and c.nodes[id].inUse:
      c.unlinkNode(id)
      c.linkAtHead(id)
  else:
    # Insert new entry
    c.map[key] = value
    let id = c.allocNodeId()
    c.keyAt[id] = key
    c.recencyIdx[key] = id
    c.linkAtHead(id)
    # Evict LRU entries if over capacity
    while c.map.len > c.capacity:
      if c.tail < 0: break
      let evictId = c.tail
      let evictKey = c.keyAt[evictId]
      c.unlinkNode(evictId)
      c.recencyIdx.del(evictKey)
      c.map.del(evictKey)
      c.freeList.add(evictId)

proc del*[K, V](c: LruCache[K, V], key: K): bool =
  ## Remove a key. Returns true if it was present.
  if not c.map.hasKey(key): return false
  c.map.del(key)
  let id = c.recencyIdx.getOrDefault(key, -1)
  if id >= 0 and c.nodes[id].inUse:
    c.unlinkNode(id)
    c.recencyIdx.del(key)
    c.freeList.add(id)
  return true

proc clear*[K, V](c: LruCache[K, V]) =
  c.map.clear()
  c.recencyIdx.clear()
  for i in 0 ..< c.nodes.len:
    c.nodes[i] = LruNode()
  c.freeList.setLen(0)
  c.head = -1
  c.tail = -1
  # Note: keyAt is preserved (still indexed by node id) — entries are
  # just stale. The `inUse` flag prevents reading them.

iterator items*[K, V](c: LruCache[K, V]): (K, V) =
  ## Iterate over entries in MRU → LRU order.
  var id = c.head
  while id >= 0:
    if c.map.hasKey(c.keyAt[id]):
      yield (c.keyAt[id], c.map[c.keyAt[id]])
    id = c.nodes[id].next
