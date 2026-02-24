# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Version Management
##
## Version and super version management for MVCC.

import std/[sequtils, tables]
import types

# Run - a collection of tables at the same level
type
  Run* = ref object
    ## A run is a collection of tables at the same level
    level*: int
    tables*: seq[pointer] # Would be Table references

# Version - snapshot of the tree
type
  Version* = ref object
    ## A version is a snapshot of the tree
    id*: int64
    memtableSize*: uint64
    runs*: seq[Run]
    blobFiles*: Table[int64, pointer] # blob_id -> BlobFile

# Super version - contains current state
type
  SuperVersions* = ref object
    ## Super version containing current tree state
    current*: Version
    activeMemtable*: pointer       # Would be Memtable
    sealedMemtables*: seq[pointer] # Would be Memtable refs

proc newSuperVersions*(): SuperVersions =
  SuperVersions(
    current: nil,
    activeMemtable: nil,
    sealedMemtables: @[]
  )

# Run management
type
  RunList* = ref object
    ## List of runs at each level
    levels*: seq[seq[Run]]

proc newRunList*(numLevels: int): RunList =
  RunList(levels: newSeqWith(numLevels, newSeq[Run]()))

# Blob file list
type
  BlobFileList* = ref object
    files*: Table[int64, pointer] # file_id -> BlobFile

# Version optimize
type
  VersionOptimize* = ref object
    ## Version optimization
    discard

# Version persist
type
  VersionPersist* = ref object
    ## Version persistence
    discard
