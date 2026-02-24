# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Table Extensions
##
## Additional table types and structures.

import std/[tables]
import types

# Table ID
type
  TableId* = int64

# Block offset
type
  BlockOffset* = uint64

# Table regions
type
  TableRegion* = ref object
    offset*: uint64
    size*: uint32

# Table metadata
type
  TableMetadata* = ref object
    id*: TableId
    globalId*: GlobalTableId
    level*: int
    size*: uint64
    minKey*: Slice
    maxKey*: Slice
    entryCount*: uint64

# Block index types
type
  BlockIndexImplKind* = enum
    biiFull     ## Fully loaded index
    biiTwoLevel ## Partitioned/indexed index
    biiVolatile ## Cached but not pinned

  BlockIndex* = ref object
    kind*: BlockIndexImplKind

# Table scanner for compactions
type
  TableScanner* = ref object
    ## Scanner for compactions
    table*: pointer
    currentBlock*: int

# Multi-writer support
type
  WriterState* = object
    ## Writer state for multi-writer
    bytesWritten*: uint64
    entriesWritten*: uint64
