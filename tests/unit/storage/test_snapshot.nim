# Unit tests for snapshot module
# Tests for Snapshot functionality

import unittest
import std/strutils
import fractio/storage/[snapshot, snapshot_nonce, snapshot_tracker]

suite "Snapshot Unit Tests":

  test "SnapshotNonce creation":
    let tracker = newSnapshotTracker(newSequenceNumberCounter())
    let nonce = newSnapshotNonce(123, tracker)
    check nonce.instant == 123

  test "SnapshotNonce clone":
    let tracker = newSnapshotTracker(newSequenceNumberCounter())
    let orig = newSnapshotNonce(456, tracker)
    let cloneResult = orig.clone()
    check orig.instant == cloneResult.instant
    # In a full implementation, we would check that the tracker reference is handled correctly

  test "Snapshot creation":
    let tracker = newSnapshotTracker(newSequenceNumberCounter())
    let nonce = newSnapshotNonce(789, tracker)
    let snapshot = newSnapshot(nonce)
    check snapshot.nonce.instant == 789

  test "Snapshot sequence number":
    let tracker = newSnapshotTracker(newSequenceNumberCounter())
    let nonce = newSnapshotNonce(999, tracker)
    let snapshot = newSnapshot(nonce)
    check snapshot.seqno() == 999

  test "Snapshot nonce debug":
    let tracker = newSnapshotTracker(newSequenceNumberCounter())
    let nonce = newSnapshotNonce(123, tracker)
    let nonceStr = $nonce
    check "SnapshotNonce" in nonceStr
    check "123" in nonceStr
