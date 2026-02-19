# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/types

# Forward declaration
type
  SnapshotTracker* = object

# Holds a snapshot instant and automatically frees it from the snapshot tracker when destroyed
type
  SnapshotNonce* = ref object
    instant*: SeqNo
    tracker*: SnapshotTracker

# Debug representation
proc `==`(a, b: SnapshotNonce): bool =
  a.instant == b.instant

proc `$`*(nonce: SnapshotNonce): string =
  "SnapshotNonce(" & $nonce.instant & ")"

# Constructor
proc newSnapshotNonce*(seqno: SeqNo, tracker: SnapshotTracker): SnapshotNonce =
  SnapshotNonce(instant: seqno, tracker: tracker)

# Clone implementation (in Nim, this is handled by the assignment operator for ref objects)
# But we need to manually handle the tracker cloning
proc clone*(nonce: SnapshotNonce): SnapshotNonce =
  # In the full implementation, this would call tracker.cloneSnapshot(nonce)
  # For now, we just create a new reference
  newSnapshotNonce(nonce.instant, nonce.tracker)

# Destructor - in Nim we use =destroy
proc `=destroy`*(nonce: var SnapshotNonce) =
  # In the full implementation, this would call tracker.close(nonce)
  # For now, we just clean up the object
  discard
