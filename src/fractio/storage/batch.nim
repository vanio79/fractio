# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[error, types, batch/item, keyspace, journal]
import std/[sets, atomics]

# Forward declarations
type
  Database* = object
  PersistMode* = enum
    pmBuffer
    pmSyncData
    pmSyncAll

# An atomic write batch
# Allows atomically writing across keyspaces inside the Database
type
  WriteBatch* = object
    data*: seq[Item]
    db*: Database
    durability*: Option[PersistMode]

# Initializes a new write batch
proc newWriteBatch*(db: Database): WriteBatch =
  WriteBatch(
    data: newSeq[Item](),
    db: db,
    durability: none(PersistMode)
  )

# Initializes a new write batch with preallocated capacity
proc withCapacity*(db: Database, capacity: int): WriteBatch =
  WriteBatch(
    data: newSeq[Item](0, capacity),
    db: db,
    durability: none(PersistMode)
  )

# Gets the number of batched items
proc len*(batch: WriteBatch): int =
  batch.data.len

# Returns true if there are no batched items
proc isEmpty*(batch: WriteBatch): bool =
  batch.len == 0

# Sets the durability level
proc durability*(batch: WriteBatch, mode: Option[PersistMode]): WriteBatch =
  var result = batch
  result.durability = mode
  return result

# Inserts a key-value pair into the batch
proc insert*(batch: var WriteBatch, keyspace: Keyspace, key: UserKey,
    value: UserValue) =
  batch.data.add(newItem(keyspace, key, value, vtValue))

# Removes a key-value pair
proc remove*(batch: var WriteBatch, keyspace: Keyspace, key: UserKey) =
  batch.data.add(newItem(keyspace, key, "", vtTombstone))

# Adds a weak tombstone marker for a key
proc removeWeak*(batch: var WriteBatch, keyspace: Keyspace, key: UserKey) =
  batch.data.add(newItem(keyspace, key, "", vtWeakTombstone))

# Commits the batch to the Database atomically
proc commit*(batch: WriteBatch): StorageResult[void] =
  if batch.isEmpty():
    return ok()

  logTrace("batch: Acquiring journal writer")

  # In a full implementation, this would get the journal writer
  # For now, we'll skip this

  # Check if database is poisoned
  # In a full implementation, this would check the atomic boolean
  # For now, we'll assume it's not poisoned

  # Get batch sequence number
  # In a full implementation, this would get the next sequence number
  # For now, we'll use a placeholder

  # Write batch to journal
  # In a full implementation, this would write the batch
  # For now, we'll skip this

  # Persist if durability is set
  # In a full implementation, this would persist based on durability setting
  # For now, we'll skip this

  # Track keyspaces that might need stalling
  var keyspacesWithPossibleStall: HashSet[Keyspace] = initHashSet[Keyspace]()

  # Apply batch to memtables
  var batchSize: uint64 = 0

  logTrace("Applying batch (size=" & $batch.data.len & ") to memtable(s)")

  # In a full implementation, this would apply the batch to memtables
  # For now, we'll simulate this

  # Update snapshot tracker
  # In a full implementation, this would publish the batch sequence number
  # For now, we'll skip this

  # Add batch size to write buffer
  # In a full implementation, this would update the write buffer size
  # For now, we'll skip this

  # Check for write stalls
  for keyspace in keyspacesWithPossibleStall:
    # In a full implementation, this would check for write stalls
    # For now, we'll skip this
    discard

  return ok()
