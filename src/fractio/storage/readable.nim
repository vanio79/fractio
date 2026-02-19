# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[error, types, guard]

# Forward declarations for types that will be defined later
type
  Keyspace* = object
  Iter* = object

# Readable snapshot trait
# Can be used to pass a write transaction into a function that expects a snapshot.

type
  Readable* = concept self
    # Retrieves an item from the snapshot.
    proc get*(self, keyspace: Keyspace, key: string): StorageResult[Option[UserValue]]

    # Returns `true` if the snapshot contains the specified key.
    proc containsKey*(self, keyspace: Keyspace, key: string): StorageResult[bool]

    # Returns the first key-value pair in the snapshot.
    # The key in this pair is the minimum key in the snapshot.
    proc firstKeyValue*(self, keyspace: Keyspace): Option[Guard]

    # Returns the last key-value pair in the snapshot.
    # The key in this pair is the maximum key in the snapshot.
    proc lastKeyValue*(self, keyspace: Keyspace): Option[Guard]

    # Retrieves the size of an item from the snapshot.
    proc sizeOf*(self, keyspace: Keyspace, key: string): StorageResult[Option[uint32]]

    # Returns `true` if the partition is empty.
    proc isEmpty*(self, keyspace: Keyspace): StorageResult[bool]

    # Iterates over the snapshot.
    proc iter*(self, keyspace: Keyspace): Iter

    # Scans the entire keyspace, returning the number of items.
    proc len*(self, keyspace: Keyspace): StorageResult[int]

    # Iterates over a range of the snapshot.
    proc range*(self, keyspace: Keyspace, startKey: string,
        endKey: string): Iter

    # Iterates over a prefixed set of the snapshot.
    proc prefix*(self, keyspace: Keyspace, prefix: string): Iter

# Default implementation for isEmpty (similar to Rust)
proc isEmpty*(self: Readable, keyspace: Keyspace): StorageResult[bool] =
  # This is a placeholder implementation
  # In the full implementation, this would check if firstKeyValue returns None
  return ok(true)

# Default implementation for len (similar to Rust)
proc len*(self: Readable, keyspace: Keyspace): StorageResult[int] =
  # This is a placeholder implementation
  # In the full implementation, this would iterate and count items
  return ok(0)
