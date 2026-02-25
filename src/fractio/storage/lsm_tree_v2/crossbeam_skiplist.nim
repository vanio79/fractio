# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## High-performance port of crossbeam-skiplist to Nim
##
## Key optimizations from Rust crossbeam-skiplist:
## - Xorshift RNG with countTrailingZeroBits for O(1) random height
## - Tight search loops with minimal branching
## - Cache-friendly memory layout
## - Optimized pointer chasing patterns
##
## This implementation focuses on matching Rust's single-threaded performance

import std/[atomics, options, bitops]

const
  HEIGHT_BITS = 5
  MAX_HEIGHT = 1 shl HEIGHT_BITS ## 32 levels max
  HEIGHT_MASK = MAX_HEIGHT - 1

# ============================================================================
# Types
# ============================================================================

type
  ## Node structure with embedded tower
  ## Tower is stored inline after the fixed fields for cache efficiency
  SkipListNodeObj[K, V] = object
    key: K
    value: V
    height: int ## Cached height (avoids bit ops in hot path)
    next: ptr UncheckedArray[SkipListNodeObj[K, V]] ## Tower of pointers

  SkipListNode[K, V] = ptr SkipListNodeObj[K, V]

  SkipList*[K, V] = ref object
    head: SkipListNode[K, V] ## Head node with MAX_HEIGHT tower
    len: Atomic[int]
    maxHeight: Atomic[int]
    seed: Atomic[uint]

  Entry*[K, V] = ref object
    list: SkipList[K, V]
    node: SkipListNode[K, V]

  Iter*[K, V] = ref object
    list: SkipList[K, V]
    current: SkipListNode[K, V]

  RangeIter*[K, V] = ref object
    list: SkipList[K, V]
    current: SkipListNode[K, V]
    endKey: K
    endInclusive: bool

# ============================================================================
# Node Allocation
# ============================================================================

proc allocNode[K, V](h: int): SkipListNode[K, V] {.inline.} =
  ## Allocate node with embedded tower of given height
  ## Tower is allocated inline with the node for cache efficiency
  let size = sizeof(SkipListNodeObj[K, V]) + h * sizeof(pointer)
  result = cast[SkipListNode[K, V]](alloc0(size))
  result.height = h
  ## Tower starts at the next field
  result.next = cast[ptr UncheckedArray[SkipListNodeObj[K, V]]](
    cast[ByteAddress](result) + sizeof(SkipListNodeObj[K, V])
  )

proc deallocNode[K, V](node: SkipListNode[K, V]) {.inline.} =
  ## Free a node
  dealloc(cast[pointer](node))

# ============================================================================
# SkipList Creation
# ============================================================================

proc newSkipList*[K, V](): SkipList[K, V] =
  ## Create a new empty skip list
  result = SkipList[K, V]()

  ## Allocate head with full MAX_HEIGHT tower
  result.head = allocNode[K, V](MAX_HEIGHT)

  ## Initialize all tower slots to nil
  for i in 0 ..< MAX_HEIGHT:
    cast[ptr UncheckedArray[pointer]](result.head.next)[i] = nil

  store(result.maxHeight, 1, moRelaxed)
  store(result.seed, 1u, moRelaxed)
  store(result.len, 0, moRelaxed)

# ============================================================================
# Random Height Generation (Xorshift + countTrailingZeroBits)
# ============================================================================

proc randomHeight[K, V](s: SkipList[K, V]): int {.inline.} =
  ## Generate random height using Xorshift RNG
  ## Uses countTrailingZeroBits for O(1) height calculation
  ## This is the KEY optimization from Rust crossbeam-skiplist

  ## Xorshift RNG from "Xorshift RNGs" by George Marsaglia
  var num = load(s.seed, moRelaxed)
  num = num xor (num shl 13)
  num = num xor (num shr 17)
  num = num xor (num shl 5)
  store(s.seed, num, moRelaxed)

  ## Use trailing zeros to determine height (O(1) vs O(height) loop)
  var height = min(MAX_HEIGHT, countTrailingZeroBits(num) + 1)

  ## Match Rust's optimization: decrease height if much larger than current towers
  let maxH = load(s.maxHeight, moRelaxed)
  while height >= 4 and height > maxH:
    ## Check if the level above current max is empty
    let checkLevel = height - 2
    if checkLevel >= 0 and checkLevel < MAX_HEIGHT:
      if cast[ptr UncheckedArray[pointer]](s.head.next)[checkLevel] == nil:
        height -= 1
      else:
        break
    else:
      break

  ## Track max height using compare-and-swap (same as Rust)
  var curMax = maxH
  while height > curMax:
    if compareExchange(s.maxHeight, curMax, height, moRelaxed, moRelaxed):
      break
    curMax = load(s.maxHeight, moRelaxed)

  height

# ============================================================================
# Search Operations
# ============================================================================

type
  SearchResult[K, V] = object
    update: array[MAX_HEIGHT, SkipListNode[K, V]]
    node: SkipListNode[K, V]
    found: bool

proc search[K, V](s: SkipList[K, V], key: K): SearchResult[K, V] {.inline.} =
  ## Search for a key in the skip list
  ## Tight loop matching Rust's implementation

  let maxH = load(s.maxHeight, moRelaxed)

  ## Initialize all update slots to head
  for i in 0 ..< MAX_HEIGHT:
    result.update[i] = s.head

  var x = s.head
  var level = maxH - 1

  ## Search from top level down
  while level >= 0:
    ## Get next at this level
    let next = cast[ptr UncheckedArray[SkipListNode[K, V]]](x.next)[level]

    if next != nil and next.key < key:
      ## Keep moving forward at this level
      x = next
      continue

    ## Record predecessor at this level
    result.update[level] = x

    ## Move down
    if level == 0:
      break
    level -= 1

  result.node = cast[ptr UncheckedArray[SkipListNode[K, V]]](x.next)[0]
  result.found = result.node != nil and result.node.key == key

# ============================================================================
# Core Operations
# ============================================================================

proc get*[K, V](s: SkipList[K, V], key: K): Option[V] {.inline.} =
  ## Get value for key
  let res = s.search(key)
  if res.found:
    some(res.node.value)
  else:
    none(V)

proc contains*[K, V](s: SkipList[K, V], key: K): bool {.inline.} =
  ## Check if key exists
  s.get(key).isSome

proc len*[K, V](s: SkipList[K, V]): int {.inline.} =
  ## Get number of elements
  load(s.len, moRelaxed)

proc isEmpty*[K, V](s: SkipList[K, V]): bool {.inline.} =
  ## Check if empty
  s.len() == 0

# ============================================================================
# Insertion
# ============================================================================

proc insert*[K, V](s: SkipList[K, V], key: K, value: V): Entry[K, V] =
  ## Insert key-value pair into skip list

  ## First, search for the key
  var searchRes = s.search(key)

  ## If key exists, update value and return
  if searchRes.found:
    searchRes.node.value = value
    return Entry[K, V](list: s, node: searchRes.node)

  ## Generate random height for new node
  let height = s.randomHeight()

  ## Allocate and initialize new node
  let newNode = allocNode[K, V](height)
  newNode.key = key
  newNode.value = value

  ## Initialize tower to nil
  for i in 0 ..< height:
    cast[ptr UncheckedArray[pointer]](newNode.next)[i] = nil

  ## Link into list level by level
  let update = searchRes.update

  ## Level 0 insertion
  cast[ptr UncheckedArray[SkipListNode[K, V]]](newNode.next)[0] =
    cast[ptr UncheckedArray[SkipListNode[K, V]]](update[0].next)[0]
  cast[ptr UncheckedArray[SkipListNode[K, V]]](update[0].next)[0] = newNode

  ## Build upper levels
  for level in 1 ..< height:
    cast[ptr UncheckedArray[SkipListNode[K, V]]](newNode.next)[level] =
      cast[ptr UncheckedArray[SkipListNode[K, V]]](update[level].next)[level]
    cast[ptr UncheckedArray[SkipListNode[K, V]]](update[level].next)[
        level] = newNode

  ## Increment length
  discard fetchAdd(s.len, 1, moRelaxed)

  Entry[K, V](list: s, node: newNode)

# ============================================================================
# Removal
# ============================================================================

proc remove*[K, V](s: SkipList[K, V], key: K): bool =
  ## Remove key from skip list

  let searchRes = s.search(key)

  if not searchRes.found:
    return false

  let node = searchRes.node
  let height = node.height

  ## Unlink from all levels
  for level in 0 ..< height:
    cast[ptr UncheckedArray[SkipListNode[K, V]]](searchRes.update[level].next)[level] =
      cast[ptr UncheckedArray[SkipListNode[K, V]]](node.next)[level]

  ## Update max height if needed
  var maxH = load(s.maxHeight, moRelaxed)
  while maxH > 1 and cast[ptr UncheckedArray[pointer]](s.head.next)[maxH - 1] == nil:
    discard compareExchange(s.maxHeight, maxH, maxH - 1, moRelaxed, moRelaxed)
    maxH = load(s.maxHeight, moRelaxed)

  ## Decrement length
  discard fetchSub(s.len, 1, moRelaxed)

  ## Free the node
  deallocNode(node)

  true

# ============================================================================
# Iteration
# ============================================================================

proc iter*[K, V](s: SkipList[K, V]): Iter[K, V] {.inline.} =
  ## Create iterator over all entries
  Iter[K, V](list: s, current: cast[ptr UncheckedArray[SkipListNode[K, V]]](
      s.head.next)[0])

proc hasNext*[K, V](it: Iter[K, V]): bool {.inline.} =
  ## Check if more elements available
  it.current != nil

proc next*[K, V](it: Iter[K, V]): tuple[key: K, value: V] {.inline.} =
  ## Get next element
  let node = it.current
  result = (node.key, node.value)
  it.current = cast[ptr UncheckedArray[SkipListNode[K, V]]](node.next)[0]

# ============================================================================
# Range Iteration
# ============================================================================

proc range*[K, V](s: SkipList[K, V], startKey: K, endKey: K,
                  endInclusive: bool = false): RangeIter[K, V] =
  ## Create iterator over key range [startKey, endKey)
  let searchRes = s.search(startKey)
  var start = searchRes.node

  ## If startKey not found, use the next node
  if start == nil or start.key < startKey:
    if searchRes.update[0] != nil:
      start = cast[ptr UncheckedArray[SkipListNode[K, V]]](searchRes.update[
          0].next)[0]

  RangeIter[K, V](list: s, current: start, endKey: endKey,
                  endInclusive: endInclusive)

proc hasNext*[K, V](r: RangeIter[K, V]): bool {.inline.} =
  ## Check if more elements in range
  if r.current == nil:
    return false

  let cmp = r.current.key
  if r.endInclusive:
    cmp <= r.endKey
  else:
    cmp < r.endKey

proc next*[K, V](r: RangeIter[K, V]): tuple[key: K, value: V] {.inline.} =
  ## Get next element in range
  let node = r.current
  result = (node.key, node.value)
  r.current = cast[ptr UncheckedArray[SkipListNode[K, V]]](node.next)[0]

# ============================================================================
# Entry Accessors
# ============================================================================

proc key*[K, V](e: Entry[K, V]): K {.inline.} = e.node.key
proc value*[K, V](e: Entry[K, V]): V {.inline.} = e.node.value

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing crossbeam-skiplist..."

  var s = newSkipList[string, string]()

  ## Test insert
  discard s.insert("key1", "value1")
  discard s.insert("key2", "value2")
  discard s.insert("key3", "value3")

  assert s.len() == 3
  assert s.get("key1").get() == "value1"
  assert s.get("key2").get() == "value2"
  assert s.get("key3").get() == "value3"
  assert s.get("key4").isNone()

  ## Test contains
  assert s.contains("key1")
  assert not s.contains("key4")

  ## Test range iteration
  var count = 0
  let rangeIter = s.range("key1", "key3", false)
  while rangeIter.hasNext():
    discard rangeIter.next()
    count += 1
  assert count == 2

  ## Test full iteration
  count = 0
  let fullIter = s.iter()
  while fullIter.hasNext():
    discard fullIter.next()
    count += 1
  assert count == 3

  ## Test delete
  assert s.remove("key2")
  assert not s.remove("key4")
  assert s.len() == 2
  assert s.get("key2").isNone()

  ## Test insert with replacement
  discard s.insert("key1", "new_value1")
  assert s.get("key1").get() == "new_value1"
  assert s.len() == 2

  echo "crossbeam-skiplist tests passed!"
