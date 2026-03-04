# Unit tests for stats module
# Tests for Stats functionality

import unittest
import fractio/storage/stats
import std/atomics

suite "Stats Unit Tests":

  test "Stats creation":
    var stats = newStats()
    check stats.activeCompactionCount.load(moRelaxed) == 0
    check stats.timeCompacting.load(moRelaxed) == 0
    check stats.compactionsCompleted.load(moRelaxed) == 0

  test "Stats atomic operations":
    var stats = newStats()

    # Test active compaction counter
    discard stats.activeCompactionCount.fetchAdd(1, moRelaxed)
    check stats.activeCompactionCount.load(moRelaxed) == 1

    discard stats.activeCompactionCount.fetchAdd(2, moRelaxed)
    check stats.activeCompactionCount.load(moRelaxed) == 3

    discard stats.activeCompactionCount.fetchSub(1, moRelaxed)
    check stats.activeCompactionCount.load(moRelaxed) == 2

  test "Stats time compacting":
    var stats = newStats()

    # Test time compacting counter
    discard stats.timeCompacting.fetchAdd(1000, moRelaxed)
    check stats.timeCompacting.load(moRelaxed) == 1000

    discard stats.timeCompacting.fetchAdd(500, moRelaxed)
    check stats.timeCompacting.load(moRelaxed) == 1500

  test "Stats compactions completed":
    var stats = newStats()

    # Test compactions completed counter
    discard stats.compactionsCompleted.fetchAdd(1, moRelaxed)
    check stats.compactionsCompleted.load(moRelaxed) == 1

    discard stats.compactionsCompleted.fetchAdd(3, moRelaxed)
    check stats.compactionsCompleted.load(moRelaxed) == 4
