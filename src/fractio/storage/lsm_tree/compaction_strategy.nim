# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Compaction Strategy
##
## Defines compaction strategies for LSM tree:
## - Leveled: Classic LSM compaction (L0 -> L1 -> L2 -> ...)
## - Tiered: Multiple runs per level, merge when threshold exceeded
## - FIFO: Delete oldest tables when size limit exceeded

import fractio/storage/lsm_tree/types
import std/[options, strutils]

# Compaction strategy types
type
  CompactionStrategyKind* = enum
    cskLeveled ## Classic leveled compaction
    cskTiered  ## Tiered compaction (size-tiered)
    cskFifo    ## FIFO compaction (time-based expiry)

  # Leveled compaction options
  LeveledOptions* = object
    ## Options for leveled compaction
    targetSize*: uint64   ## Target size for each level (default: 64 MiB)
    levelMultiplier*: int ## Size multiplier between levels (default: 10)

  # Tiered compaction options
  TieredOptions* = object
    ## Options for tiered compaction
    maxTablesPerLevel*: int ## Max tables before compaction triggers (default: 4)
    targetSize*: uint64     ## Target size for output tables

  # FIFO compaction options
  FifoOptions* = object
    ## Options for FIFO compaction
    maxSize*: uint64            ## Max total size before compaction (default: 1 GiB)
    ttlSeconds*: Option[uint64] ## Optional time-to-live for entries

  # Compaction strategy
  CompactionStrategy* = object
    ## Compaction strategy configuration
    case kind*: CompactionStrategyKind
    of cskLeveled:
      leveled*: LeveledOptions
    of cskTiered:
      tiered*: TieredOptions
    of cskFifo:
      fifo*: FifoOptions

# Default constructors
proc defaultLeveled*(): CompactionStrategy =
  ## Create default leveled compaction strategy
  result = CompactionStrategy(kind: cskLeveled)
  result.leveled = LeveledOptions(
    targetSize: 64 * 1024 * 1024, # 64 MiB
    levelMultiplier: 10
  )

proc defaultTiered*(): CompactionStrategy =
  ## Create default tiered compaction strategy
  result = CompactionStrategy(kind: cskTiered)
  result.tiered = TieredOptions(
    maxTablesPerLevel: 4,
    targetSize: 64 * 1024 * 1024 # 64 MiB
  )

proc defaultFifo*(): CompactionStrategy =
  ## Create default FIFO compaction strategy
  result = CompactionStrategy(kind: cskFifo)
  result.fifo = FifoOptions(
    maxSize: 1024 * 1024 * 1024, # 1 GiB
    ttlSeconds: none(uint64)
  )

proc newFifo*(maxSize: uint64, ttlSeconds: Option[uint64]): CompactionStrategy =
  ## Create FIFO compaction strategy with custom settings
  result = CompactionStrategy(kind: cskFifo)
  result.fifo = FifoOptions(maxSize: maxSize, ttlSeconds: ttlSeconds)

proc newLeveled*(targetSize: uint64, levelMultiplier: int): CompactionStrategy =
  ## Create leveled compaction strategy with custom settings
  result = CompactionStrategy(kind: cskLeveled)
  result.leveled = LeveledOptions(
    targetSize: targetSize,
    levelMultiplier: levelMultiplier
  )

proc newTiered*(maxTablesPerLevel: int, targetSize: uint64): CompactionStrategy =
  ## Create tiered compaction strategy with custom settings
  result = CompactionStrategy(kind: cskTiered)
  result.tiered = TieredOptions(
    maxTablesPerLevel: maxTablesPerLevel,
    targetSize: targetSize
  )

# Get target table size for a strategy
proc getTargetTableSize*(strategy: CompactionStrategy): uint64 =
  ## Get the target table size for compaction output
  case strategy.kind
  of cskLeveled:
    return strategy.leveled.targetSize
  of cskTiered:
    return strategy.tiered.targetSize
  of cskFifo:
    return 64 * 1024 * 1024 # Default for FIFO

# Get the strategy name
proc getName*(strategy: CompactionStrategy): string =
  ## Get the name of the compaction strategy
  case strategy.kind
  of cskLeveled:
    return "leveled"
  of cskTiered:
    return "tiered"
  of cskFifo:
    return "fifo"

# Check if FIFO should trigger based on total size
proc shouldCompactFifo*(strategy: CompactionStrategy, totalSize: uint64): bool =
  ## Check if FIFO compaction should trigger
  if strategy.kind != cskFifo:
    return false
  return totalSize >= strategy.fifo.maxSize

# Check if leveled should trigger based on L0 count
proc shouldCompactLeveled*(strategy: CompactionStrategy, l0Count: int): bool =
  ## Check if leveled compaction should trigger
  if strategy.kind != cskLeveled:
    return false
  # Trigger when L0 has >= 4 tables
  return l0Count >= 4

# Check if tiered should trigger based on table count per level
proc shouldCompactTiered*(strategy: CompactionStrategy, tableCount: int): bool =
  ## Check if tiered compaction should trigger
  if strategy.kind != cskTiered:
    return false
  return tableCount >= strategy.tiered.maxTablesPerLevel

# Check if compaction should run based on strategy
proc shouldCompact*(strategy: CompactionStrategy, l0Count: int,
                    totalSize: uint64, level: int): bool =
  ## Check if compaction should run based on the strategy
  case strategy.kind
  of cskLeveled:
    return strategy.shouldCompactLeveled(l0Count)
  of cskTiered:
    return strategy.shouldCompactTiered(l0Count)
  of cskFifo:
    return strategy.shouldCompactFifo(totalSize)
