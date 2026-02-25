# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Skip List Implementation
##
## A probabilistic data structure that provides O(log n) expected time
## for search, insertion, and deletion operations. Supports range queries.
##
## This implementation mirrors Rust's crossbeam_skiplist::SkipMap behavior.

import std/[random, math, options]

const
  MAX_LEVEL* = 32 # Maximum level for skiplist (supports ~4 billion elements)
  P* = 0.5        # Probability factor for level promotion

type
  SkipListNode*[K, V] = ref object
    ## A node in the skip list
    key*: K
    value*: V
    forward*: seq[SkipListNode[K, V]] # Forward pointers at each level

  SkipList*[K, V] = ref object
    ## Skip list data structure
    ## Supports ordered key-value storage with range iteration
    level*: int # Current maximum level
    header*: SkipListNode[K, V] # Header node (sentinel)
    length*: int # Number of elements
    rng*: Rand # Random number generator for level generation

proc newSkipListNode*[K, V](key: K, value: V, level: int): SkipListNode[K, V] =
  SkipListNode[K, V](
    key: key,
    value: value,
    forward: newSeq[SkipListNode[K, V]](level)
  )

proc newSkipList*[K, V](): SkipList[K, V] =
  ## Creates a new empty skip list
  let rng = initRand(0)
  SkipList[K, V](
    level: 0,
    header: newSkipListNode[K, V](default(K), default(V), MAX_LEVEL),
    length: 0,
    rng: rng
  )

proc randomLevel*(s: SkipList): int =
  ## Generates a random level for a new node
  var lvl = 1
  while lvl < MAX_LEVEL and s.rng.rand(1.0) < P:
    lvl += 1
  return lvl

proc search*[K, V](s: SkipList[K, V], key: K): tuple[update: seq[SkipListNode[K,
    V]], node: SkipListNode[K, V]] =
  ## Searches for a key and returns the update path and target node
  var update: seq[SkipListNode[K, V]] = newSeq[SkipListNode[K, V]](MAX_LEVEL)
  var x = s.header

  # Search from the top level down
  for i in countdown(s.level - 1, 0):
    while x.forward[i] != nil and x.forward[i].key < key:
      x = x.forward[i]
    update[i] = x

  result.update = update
  result.node = x.forward[0]

proc insert*[K, V](s: SkipList[K, V], key: K, value: V): tuple[
    node: SkipListNode[K, V], replaced: bool] =
  ## Inserts a key-value pair into the skip list
  ## Returns (node, replaced) where replaced is true if key already existed
  let searchResult = s.search(key)
  var update = searchResult.update
  var node = searchResult.node

  if node != nil and node.key == key:
    # Key exists, replace value
    node.value = value
    return (node, true)

  # Key doesn't exist, insert new node
  let lvl = s.randomLevel()
  if lvl > s.level:
    # Update header pointers for new levels
    for i in s.level ..< lvl:
      update[i] = s.header
    s.level = lvl

  let newNode = newSkipListNode(key, value, lvl)
  for i in 0 ..< lvl:
    newNode.forward[i] = update[i].forward[i]
    update[i].forward[i] = newNode

  s.length += 1
  return (newNode, false)

proc get*[K, V](s: SkipList[K, V], key: K): Option[V] =
  ## Gets the value for a key
  let searchResult = s.search(key)
  let node = searchResult.node

  if node != nil and node.key == key:
    return some(node.value)
  return none(V)

proc contains*[K, V](s: SkipList[K, V], key: K): bool =
  ## Checks if a key exists
  s.get(key).isSome

proc delete*[K, V](s: SkipList[K, V], key: K): bool =
  ## Deletes a key from the skip list
  let searchResult = s.search(key)
  let update = searchResult.update
  let node = searchResult.node

  if node == nil or node.key != key:
    return false

  for i in 0 ..< s.level:
    if update[i].forward[i] != node:
      continue
    update[i].forward[i] = node.forward[i]

  # Update level if top levels are empty
  while s.level > 0 and s.header.forward[s.level - 1] == nil:
    s.level -= 1

  s.length -= 1
  return true

proc len*[K, V](s: SkipList[K, V]): int =
  ## Returns the number of elements
  s.length

proc isEmpty*[K, V](s: SkipList[K, V]): bool =
  ## Returns true if the skip list is empty
  s.length == 0

# ============================================================================
# Range Iteration
# ============================================================================

type
  SkipListRangeIter*[K, V] = ref object
    ## Iterator for range queries on skip list
    list*: SkipList[K, V]
    current*: SkipListNode[K, V]
    endKey*: K
    endInclusive*: bool

proc newRangeIter*[K, V](s: SkipList[K, V], startKey: K, endKey: K,
    endInclusive: bool = false): SkipListRangeIter[K, V] =
  ## Creates a range iterator starting from startKey
  var x = s.header

  # Search for the first node >= startKey
  for i in countdown(s.level - 1, 0):
    while x.forward[i] != nil and x.forward[i].key < startKey:
      x = x.forward[i]

  SkipListRangeIter[K, V](
    list: s,
    current: x.forward[0],
    endKey: endKey,
    endInclusive: endInclusive
  )

proc newRangeIterFrom*[K, V](s: SkipList[K, V], startKey: K): SkipListRangeIter[K, V] =
  ## Creates a range iterator starting from startKey (unbounded end)
  var x = s.header

  # Search for the first node >= startKey
  for i in countdown(s.level - 1, 0):
    while x.forward[i] != nil and x.forward[i].key < startKey:
      x = x.forward[i]

  SkipListRangeIter[K, V](
    list: s,
    current: x.forward[0],
    endKey: default(K),
    endInclusive: true
  )

proc hasNext*[K, V](r: SkipListRangeIter[K, V]): bool =
  ## Checks if there are more elements in the range
  if r.current == nil:
    return false

  if r.endInclusive:
    return r.current.key <= r.endKey
  else:
    return r.current.key < r.endKey

proc next*[K, V](r: SkipListRangeIter[K, V]): tuple[key: K, value: V] =
  ## Returns the next element in the range
  let result = (r.current.key, r.current.value)
  r.current = r.current.forward[0]
  result

# ============================================================================
# Full Iteration
# ============================================================================

type
  SkipListIter*[K, V] = ref object
    ## Full iterator over skip list
    current*: SkipListNode[K, V]

proc iter*[K, V](s: SkipList[K, V]): SkipListIter[K, V] =
  SkipListIter[K, V](current: s.header.forward[0])

proc hasNext*[K, V](i: SkipListIter[K, V]): bool =
  i.current != nil

proc next*[K, V](i: SkipListIter[K, V]): tuple[key: K, value: V] =
  let result = (i.current.key, i.current.value)
  i.current = i.current.forward[0]
  result

proc all*[K, V](s: SkipList[K, V]): SkipListIter[K, V] =
  ## Returns an iterator over all elements (uses full iterator for unbounded iteration)
  SkipListIter[K, V](current: s.header.forward[0])

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing SkipList..."

  let s = newSkipList[string, string]()

  # Test insert
  discard s.insert("key1", "value1")
  discard s.insert("key2", "value2")
  discard s.insert("key3", "value3")

  assert s.len() == 3
  assert s.get("key1").get() == "value1"
  assert s.get("key2").get() == "value2"
  assert s.get("key3").get() == "value3"
  assert s.get("key4").isNone()

  # Test contains
  assert s.contains("key1")
  assert not s.contains("key4")

  # Test range iteration
  var count = 0
  let rangeIter = s.newRangeIter("key1", "key3", false)
  while rangeIter.hasNext():
    discard rangeIter.next()
    count += 1
  assert count == 2 # key1, key2 (key3 is exclusive)

  # Test full iteration
  count = 0
  let fullIter = s.iter()
  while fullIter.hasNext():
    discard fullIter.next()
    count += 1
  assert count == 3

  # Test delete
  assert s.delete("key2")
  assert not s.delete("key4")
  assert s.len() == 2
  assert s.get("key2").isNone()

  # Test insert with replacement
  discard s.insert("key1", "new_value1")
  assert s.get("key1").get() == "new_value1"
  assert s.len() == 2 # Length should not increase

  echo "SkipList tests passed!"
