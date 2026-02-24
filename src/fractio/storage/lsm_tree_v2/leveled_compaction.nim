# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Leveled Compaction
##
## Level-based compaction strategy.

import std/[options]
import types

# Level configuration
type
  LeveledConfig* = ref object
    maxLevel*: int
    sizeRatio*: float
    maxBytesForLevel*: uint64

proc newLeveledConfig*(maxLevel: int = 7,
    sizeRatio: float = 10.0): LeveledConfig =
  LeveledConfig(
    maxLevel: maxLevel,
    sizeRatio: sizeRatio,
    maxBytesForLevel: 0
  )

# Compaction picking
type
  CompactionPicker* = ref object
    config*: LeveledConfig

proc pickCompaction*(cp: CompactionPicker, level: int): Option[pointer] =
  ## Pick tables for compaction
  none(pointer)

# Level sizes
type
  LevelSize* = ref object
    level*: int
    size*: uint64
    tableCount*: int

# Leveled compaction picker
type
  LeveledCompactionPicker* = ref object of CompactionPicker
    levelSizes*: seq[LevelSize]

proc newLeveledCompactionPicker*(config: LeveledConfig): LeveledCompactionPicker =
  LeveledCompactionPicker(
    config: config,
    levelSizes: @[]
  )
