# Unit tests for compaction worker module
# Tests for compaction worker functionality

import unittest
import std/atomics
import fractio/storage/[compaction/worker, snapshot_tracker, stats]
import fractio/storage/keyspace as ks
import fractio/storage/lsm_tree/[types as lsm_types, lsm_tree]
import fractio/storage/supervisor
import fractio/storage/write_buffer_manager
import std/os

suite "Compaction Worker Unit Tests":

  test "Compaction worker with nil keyspace":
    let snapshotTracker = newSnapshotTracker(newSequenceNumberCounter())
    var stats = newStats()

    # Run compaction on nil keyspace - should succeed without doing anything
    let result = run(nil, snapshotTracker, stats)
    check result.isOk

  test "Compaction worker stats tracking":
    let snapshotTracker = newSnapshotTracker(newSequenceNumberCounter())
    var stats = newStats()

    # Check initial stats
    check stats.activeCompactionCount.load(moRelaxed) == 0
    check stats.compactionsCompleted.load(moRelaxed) == 0

    # Stats are updated when compaction runs on a real keyspace
    # This is tested in integration tests
