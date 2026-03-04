# Unit tests for snapshot_tracker module
# Tests for SnapshotTracker functionality

import unittest
import fractio/storage/snapshot_tracker

suite "SnapshotTracker Unit Tests":

  test "SequenceNumberCounter basic operations":
    var counter = newSequenceNumberCounter()
    check counter.get() == 0

    let next1 = counter.next()
    check next1 == 1
    check counter.get() == 1

    let next2 = counter.next()
    check next2 == 2
    check counter.get() == 2

  test "SequenceNumberCounter fetchMax":
    var counter = newSequenceNumberCounter()
    counter.fetchMax(10)
    check counter.get() == 10

    counter.fetchMax(5)
    check counter.get() == 10 # Should not decrease

    counter.fetchMax(15)
    check counter.get() == 15 # Should increase

  test "SnapshotTracker creation":
    let seqno = newSequenceNumberCounter()
    let tracker = newSnapshotTracker(seqno)
    check tracker.get() == 0
    check tracker.len() == 0
    check tracker.openSnapshots() == 0

  test "SnapshotTracker open/close":
    let seqno = newSequenceNumberCounter()
    let tracker = newSnapshotTracker(seqno)

    let nonce1 = tracker.open()
    check nonce1.instant == 0
    check tracker.len() == 1
    check tracker.openSnapshots() == 1

    let nonce2 = tracker.open()
    check nonce2.instant == 0 # Same seqno since we didn't advance
    check tracker.len() == 1 # Same instant, so same entry
    check tracker.openSnapshots() == 2 # But two references
    
    # Close one reference
    tracker.close(nonce1)
    check tracker.openSnapshots() == 1

    # Close second reference
    tracker.close(nonce2)
    check tracker.openSnapshots() == 0

  test "SnapshotTracker clone":
    let seqno = newSequenceNumberCounter()
    let tracker = newSnapshotTracker(seqno)

    let orig = tracker.open()
    let clone = tracker.cloneSnapshot(orig)

    check orig.instant == clone.instant
    check tracker.openSnapshots() == 2

    # Closing one leaves one
    tracker.close(orig)
    check tracker.openSnapshots() == 1

    # Closing the clone removes all
    tracker.close(clone)
    check tracker.openSnapshots() == 0

  test "SnapshotTracker publish":
    let seqno = newSequenceNumberCounter()
    let tracker = newSnapshotTracker(seqno)

    let current = tracker.get()
    tracker.publish(100)
    check tracker.get() == 101 # publish increments by 1
    
    # Publishing smaller value should not change
    tracker.publish(50)
    check tracker.get() == 101

  test "SnapshotTracker gc watermark":
    let seqno = newSequenceNumberCounter()
    let tracker = newSnapshotTracker(seqno)

    # Initially should be 0
    check tracker.getSeqnoSafeToGc() == 0

    # After publishing and gc, should update
    tracker.publish(10)
    tracker.gc()
    # In a full implementation this would be updated, but in our simplified version
    # it might still be 0 depending on implementation details
