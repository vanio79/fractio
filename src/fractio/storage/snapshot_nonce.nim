# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

# This module re-exports SnapshotNonce from snapshot_tracker to maintain
# backward compatibility with existing imports

import fractio/storage/snapshot_tracker
export snapshot_tracker.SnapshotNonce
export snapshot_tracker.newSnapshotNonce

# Clone a snapshot nonce
proc clone*(nonce: SnapshotNonce): SnapshotNonce =
  cloneSnapshot(nonce.tracker, nonce)

# String representation
proc `$`*(nonce: SnapshotNonce): string =
  "SnapshotNonce(" & $nonce.instant & ")"
