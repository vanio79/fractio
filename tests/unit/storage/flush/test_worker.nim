# Unit tests for flush worker module
# Tests for flush worker functionality

import unittest
import fractio/storage/[flush/worker, snapshot_tracker, write_buffer_manager, stats]
import fractio/storage/keyspace as ks
import fractio/storage/lsm_tree/[types as lsm_types, lsm_tree]
import fractio/storage/supervisor
import std/os

const TestPath = "tmp/test_flush_worker"

suite "Flush Worker Unit Tests":

  test "Flush worker with nil keyspace":
    let writeBufferManager = newWriteBufferManager()
    let snapshotTracker = newSnapshotTracker(newSequenceNumberCounter())
    var stats = newStats()

    # Run flush with nil keyspace - should succeed without doing anything
    let result = run(nil, writeBufferManager, snapshotTracker, stats)
    check result.isOk

  test "Flush worker write buffer tracking":
    let writeBufferManager = newWriteBufferManager()
    let snapshotTracker = newSnapshotTracker(newSequenceNumberCounter())
    var stats = newStats()

    # Allocate some bytes in write buffer
    discard writeBufferManager.allocate(1024)
    check writeBufferManager.get() == 1024

    # Free some bytes (simulating what happens after flush)
    discard writeBufferManager.free(512)
    check writeBufferManager.get() == 512
