# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tree Extensions
##
## Additional tree types and structures.

import std/[options]
import types

# Tree inner (internal state)
type
  TreeInner* = ref object
    ## Internal tree state
    id*: TreeId
    memtableIdCounter*: int64
    tableIdCounter*: int64
    blobFileIdCounter*: int64

# Sealed memtable reference
type
  SealedMemtableRef* = ref object
    ## Reference to a sealed memtable
    id*: MemtableId
    size*: uint64

# Tree ingestion
type
  TreeIngest* = ref object
    ## Handles data ingestion into tree
    discard

# Tree operations
type
  TreeStats* = ref object
    ## Tree statistics
    tableCount*: int
    memtableCount*: int
    totalSize*: uint64

proc newTreeStats*(): TreeStats =
  TreeStats(
    tableCount: 0,
    memtableCount: 0,
    totalSize: 0
  )

# Abstract tree (trait placeholder)
type
  AbstractTree* = ref object of RootObj
    ## Abstract tree interface
    discard

# Any tree (polymorphic tree)
type
  AnyTree* = ref object
    ## Polymorphic tree type
    case isStandard*: bool
    of true:
      tree*: pointer # Would be Tree
    of false:
      blobTree*: pointer # Would be BlobTree
