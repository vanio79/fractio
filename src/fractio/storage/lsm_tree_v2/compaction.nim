# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Compaction
##
## Compaction strategies for LSM tree.

import std/[options]
import types

# Compaction strategy trait (simplified)
type
  CompactionStrategy* = ref object of RootObj
    ## Base compaction strategy

  LeveledCompaction* = ref object of CompactionStrategy
    ## Level-based compaction
    maxLevel*: int
    sizeRatio*: float

  TieredCompaction* = ref object of CompactionStrategy
    ## Tier-based compaction
    numTiers*: int

  FifoCompaction* = ref object of CompactionStrategy
    ## FIFO compaction - oldest tables are dropped first
    maxTables*: int

proc newLeveledCompaction*(maxLevel: int = 7,
    sizeRatio: float = 10.0): LeveledCompaction =
  LeveledCompaction(maxLevel: maxLevel, sizeRatio: sizeRatio)

proc newTieredCompaction*(numTiers: int = 3): TieredCompaction =
  TieredCompaction(numTiers: numTiers)

proc newFifoCompaction*(maxTables: int = 10): FifoCompaction =
  FifoCompaction(maxTables: maxTables)

# Compaction state
type
  CompactionInput* = ref object
    ## Input to a compaction
    level*: int
    tables*: seq[pointer] # Would be Table references

  CompactionOutput* = ref object
    ## Output of a compaction
    level*: int
    tables*: seq[pointer] # Would be Table references

# Drop range operation
type
  DropRange* = ref object
    ## Represents a drop range operation
    startKey*: types.Slice
    endKey*: types.Slice

# Compaction stream (for reading during compaction)
type
  CompactionStream* = ref object
    ## Iterator for compaction
    discard

# Maintenance operations
type
  MaintenanceKind* = enum
    mkCompaction
    mkFlush

  Maintenance* = ref object
    kind*: MaintenanceKind
    priority*: int
