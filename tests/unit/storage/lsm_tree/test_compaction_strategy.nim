# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for compaction strategies

import unittest
import fractio/storage/lsm_tree/compaction_strategy
import std/options

suite "Compaction Strategy Tests":
  test "Default leveled strategy":
    let strategy = defaultLeveled()
    check strategy.kind == cskLeveled
    check strategy.leveled.targetSize == 64 * 1024 * 1024'u64
    check strategy.leveled.levelMultiplier == 10
    check strategy.getTargetTableSize() == 64 * 1024 * 1024'u64
    check strategy.getName() == "leveled"

  test "Default tiered strategy":
    let strategy = defaultTiered()
    check strategy.kind == cskTiered
    check strategy.tiered.maxTablesPerLevel == 4
    check strategy.tiered.targetSize == 64 * 1024 * 1024'u64
    check strategy.getTargetTableSize() == 64 * 1024 * 1024'u64
    check strategy.getName() == "tiered"

  test "Default FIFO strategy":
    let strategy = defaultFifo()
    check strategy.kind == cskFifo
    check strategy.fifo.maxSize == 1024 * 1024 * 1024'u64
    check strategy.fifo.ttlSeconds.isNone
    check strategy.getName() == "fifo"

  test "Custom leveled strategy":
    let strategy = newLeveled(128 * 1024 * 1024, 8)
    check strategy.kind == cskLeveled
    check strategy.leveled.targetSize == 128 * 1024 * 1024'u64
    check strategy.leveled.levelMultiplier == 8

  test "Custom tiered strategy":
    let strategy = newTiered(8, 32 * 1024 * 1024)
    check strategy.kind == cskTiered
    check strategy.tiered.maxTablesPerLevel == 8
    check strategy.tiered.targetSize == 32 * 1024 * 1024'u64

  test "Custom FIFO strategy with TTL":
    let strategy = newFifo(512 * 1024 * 1024, some(3600'u64))
    check strategy.kind == cskFifo
    check strategy.fifo.maxSize == 512 * 1024 * 1024'u64
    check strategy.fifo.ttlSeconds.isSome
    check strategy.fifo.ttlSeconds.unsafeGet() == 3600'u64

  test "shouldCompactLeveled":
    let strategy = defaultLeveled()
    check not strategy.shouldCompactLeveled(0)
    check not strategy.shouldCompactLeveled(1)
    check not strategy.shouldCompactLeveled(2)
    check not strategy.shouldCompactLeveled(3)
    check strategy.shouldCompactLeveled(4)
    check strategy.shouldCompactLeveled(10)

  test "shouldCompactTiered":
    let strategy = defaultTiered()
    check not strategy.shouldCompactTiered(0)
    check not strategy.shouldCompactTiered(1)
    check not strategy.shouldCompactTiered(2)
    check not strategy.shouldCompactTiered(3)
    check strategy.shouldCompactTiered(4)
    check strategy.shouldCompactTiered(10)

  test "shouldCompactFifo":
    let strategy = defaultFifo()
    let maxSize = 1024 * 1024 * 1024'u64 # 1 GiB
    check not strategy.shouldCompactFifo(0)
    check not strategy.shouldCompactFifo(maxSize - 1)
    check strategy.shouldCompactFifo(maxSize)
    check strategy.shouldCompactFifo(maxSize * 2)

  test "shouldCompact dispatch for leveled":
    let strategy = defaultLeveled()
    check strategy.shouldCompact(4, 0, 0) # L0 count = 4
    check not strategy.shouldCompact(3, 0, 0)

  test "shouldCompact dispatch for tiered":
    let strategy = defaultTiered()
    check strategy.shouldCompact(4, 0, 0) # table count = 4
    check not strategy.shouldCompact(3, 0, 0)

  test "shouldCompact dispatch for FIFO":
    let strategy = defaultFifo()
    let maxSize = 1024 * 1024 * 1024'u64
    check strategy.shouldCompact(0, maxSize, 0)
    check not strategy.shouldCompact(0, maxSize - 1, 0)

  test "getTargetTableSize for different strategies":
    let leveled = defaultLeveled()
    check leveled.getTargetTableSize() == 64 * 1024 * 1024'u64

    let tiered = newTiered(4, 128 * 1024 * 1024)
    check tiered.getTargetTableSize() == 128 * 1024 * 1024'u64

    let fifo = defaultFifo()
    check fifo.getTargetTableSize() == 64 * 1024 * 1024'u64 # Default for FIFO

  test "Strategy kind matching":
    var strategy = defaultLeveled()
    check strategy.kind == cskLeveled
    check strategy.getName() == "leveled"

    strategy = defaultTiered()
    check strategy.kind == cskTiered
    check strategy.getName() == "tiered"

    strategy = defaultFifo()
    check strategy.kind == cskFifo
    check strategy.getName() == "fifo"
