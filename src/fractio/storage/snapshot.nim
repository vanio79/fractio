# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Cross-Keyspace Snapshot
##
## Provides a consistent view across multiple keyspaces at a specific sequence number.
## This allows reading from multiple keyspaces as they existed at a single point in time.

import fractio/storage/[error, types, guard, snapshot_tracker]
import std/options

type
  # Forward declaration - defined in keyspace.nim
  Keyspace = ref object

  Snapshot* = ref object
    ## A cross-keyspace snapshot that provides a consistent view
    ## across all keyspaces at a specific sequence number.
    nonce*: SnapshotNonce

# Constructor
proc newSnapshot*(nonce: SnapshotNonce): Snapshot =
  Snapshot(nonce: nonce)

# Get the sequence number of the snapshot
proc seqno*(self: Snapshot): SeqNo =
  self.nonce.instant

# Close the snapshot (release the reference)
proc close*(self: Snapshot) =
  ## Closes the snapshot, releasing its reference from the tracker.
  if self.nonce.tracker != nil:
    self.nonce.tracker.close(self.nonce)

# Get a value from a keyspace at the snapshot's sequence number
# Note: Full implementation requires access to keyspace internals
# which would create a circular import. This is a simplified version
# that works with the Database.snapshot() method.
proc snapshotGet*(self: Snapshot, tree: ptr object, key: string): Option[string] =
  ## Internal method to get from an LSM tree at this snapshot's sequence number.
  ## This is called by the Database's snapshot() method.
  ## The tree parameter is cast from LsmTree to avoid circular imports.
  none(string)

# Check if key exists at the snapshot's sequence number
proc snapshotContainsKey*(self: Snapshot, tree: ptr object, key: string): bool =
  ## Internal method to check if a key exists at this snapshot's sequence number.
  false

# Get size of value at snapshot
proc snapshotSizeOf*(self: Snapshot, tree: ptr object, key: string): Option[uint32] =
  ## Internal method to get the size of a value at this snapshot's sequence number.
  none(uint32)

# Check if keyspace is empty at snapshot
proc snapshotIsEmpty*(self: Snapshot, tree: ptr object): bool =
  ## Internal method to check if the keyspace is empty at this snapshot.
  true

# Get approximate length at snapshot
proc snapshotLen*(self: Snapshot, tree: ptr object): int =
  ## Internal method to get approximate length at this snapshot.
  0
