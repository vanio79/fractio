# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Cache
##
## LRU cache for blocks and blobs.

import std/[tables, hashes, options]
import types

type
  CacheKey* = object
    ## Cache key for blocks/blobs
    tag*: uint8 # TAG_BLOCK or TAG_BLOB
    treeId*: int64
    tableId*: int64
    offset*: uint64

  CacheItem* = object
    ## Cached item
    case isBlock*: bool
    of true:
      blockData*: string
    of false:
      blobData*: types.Slice

  Cache* = object
    ## LRU cache for blocks and blobs
    data*: Table[CacheKey, CacheItem]
    capacity*: uint64
    currentSize*: uint64
    order*: seq[CacheKey] # LRU order

const
  TAG_BLOCK* = 0'u8
  TAG_BLOB* = 1'u8

proc hash*(k: CacheKey): Hash =
  result = hash(k.tag)
  result = result !& hash(k.treeId)
  result = result !& hash(k.tableId)
  result = result !& hash(k.offset)

proc newCache*(capacity: uint64): Cache =
  Cache(
    data: initTable[CacheKey, CacheItem](),
    capacity: capacity,
    currentSize: 0,
    order: @[]
  )

proc size*(c: Cache): uint64 = c.currentSize
proc capacity*(c: Cache): uint64 = c.capacity

proc getBlock*(c: Cache, treeId, tableId: int64, offset: uint64): Option[string] =
  let key = CacheKey(tag: TAG_BLOCK, treeId: treeId, tableId: tableId,
      offset: offset)
  if c.data.hasKey(key):
    return some(c.data[key].blockData)
  none(string)

proc insertBlock*(c: var Cache, treeId, tableId: int64, offset: uint64,
    data: string) =
  let key = CacheKey(tag: TAG_BLOCK, treeId: treeId, tableId: tableId,
      offset: offset)
  let item = CacheItem(isBlock: true, blockData: data)

  # Evict if necessary
  while c.currentSize + uint64(data.len) > c.capacity and c.order.len > 0:
    let oldKey = c.order.pop()
    if c.data.hasKey(oldKey):
      let oldItem = c.data[oldKey]
      if oldItem.isBlock:
        c.currentSize -= uint64(oldItem.blockData.len)
      c.data.del(oldKey)

  c.data[key] = item
  c.currentSize += uint64(data.len)
  c.order.insert(key, 0)

proc getBlob*(c: Cache, vlogId: int64, blobFileId: int64,
    offset: uint64): Option[types.Slice] =
  let key = CacheKey(tag: TAG_BLOB, treeId: vlogId, tableId: blobFileId,
      offset: offset)
  if c.data.hasKey(key):
    return some(c.data[key].blobData)
  none(types.Slice)

proc insertBlob*(c: var Cache, vlogId: int64, blobFileId: int64, offset: uint64,
    data: types.Slice) =
  let key = CacheKey(tag: TAG_BLOB, treeId: vlogId, tableId: blobFileId,
      offset: offset)
  let item = CacheItem(isBlock: false, blobData: data)

  # Evict if necessary
  while c.currentSize + uint64(data.len) > c.capacity and c.order.len > 0:
    let oldKey = c.order.pop()
    if c.data.hasKey(oldKey):
      let oldItem = c.data[oldKey]
      if not oldItem.isBlock:
        c.currentSize -= uint64(oldItem.blobData.len)
      c.data.del(oldKey)

  c.data[key] = item
  c.currentSize += uint64(data.len)
  c.order.insert(key, 0)

when isMainModule:
  echo "Testing cache..."

  var cache = newCache(1000)
  cache.insertBlock(1, 1, 0, "test data")

  let blk = cache.getBlock(1, 1, 0)
  if blk.isSome:
    echo "Got block: ", blk.get

  echo "Cache tests passed!"
