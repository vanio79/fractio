# Copyright (c) 2025-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Descriptor Table
##
## Caches file descriptors to tables and blob files.

import std/[os, hashes, tables]

type
  DescriptorKey* = object
    tag*: uint8
    treeId*: int64
    tableId*: int64

  DescriptorTable* = ref object
    ## Caches file descriptors
    data*: Table[DescriptorKey, File]
    capacity*: int

proc hash*(k: DescriptorKey): Hash =
  result = !& hash(k.tag)
  result = !& result.hash(k.treeId)
  result = !& result.hash(k.tableId)

proc newDescriptorTable*(capacity: int): DescriptorTable =
  DescriptorTable(
    data: initTable[DescriptorKey, File](),
    capacity: capacity
  )

proc len*(dt: DescriptorTable): int = dt.data.len

proc accessForTable*(dt: DescriptorTable, treeId, tableId: int64): Option[File] =
  let key = DescriptorKey(tag: 0, treeId: treeId, tableId: tableId)
  if dt.data.hasKey(key):
    return some(dt.data[key])
  none(File)

proc insertForTable*(dt: var DescriptorTable, treeId, tableId: int64, file: File) =
  let key = DescriptorKey(tag: 0, treeId: treeId, tableId: tableId)
  dt.data[key] = file

proc accessForBlobFile*(dt: DescriptorTable, treeId, tableId: int64): Option[File] =
  let key = DescriptorKey(tag: 1, treeId: treeId, tableId: tableId)
  if dt.data.hasKey(key):
    return some(dt.data[key])
  none(File)

proc insertForBlobFile*(dt: var DescriptorTable, treeId, tableId: int64, file: File) =
  let key = DescriptorKey(tag: 1, treeId: treeId, tableId: tableId)
  dt.data[key] = file

proc removeForTable*(dt: var DescriptorTable, treeId, tableId: int64) =
  let key = DescriptorKey(tag: 0, treeId: treeId, tableId: tableId)
  dt.data.del(key)

proc removeForBlobFile*(dt: var DescriptorTable, treeId, tableId: int64) =
  let key = DescriptorKey(tag: 1, treeId: treeId, tableId: tableId)
  dt.data.del(key)
