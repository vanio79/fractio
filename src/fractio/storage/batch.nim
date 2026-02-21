# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Write Batch Implementation
##
## An atomic write batch allows atomically writing across keyspaces.

import fractio/storage/[types, batch/item]
import fractio/storage/keyspace as ks
import std/options

# Persist mode for durability control
type
  PersistMode* = enum
    pmBuffer   ## Buffer in memory, no sync
    pmSyncData ## Sync data to disk
    pmSyncAll  ## Sync data and metadata to disk

# Forward declaration - actual type is in db.nim
type
  Database* = ref object

# An atomic write batch
type
  WriteBatch* = ref object
    data*: seq[Item]
    db*: Database
    durability*: Option[PersistMode]

# Creates a new write batch
proc newWriteBatch*(db: Database): WriteBatch =
  ## Creates a new write batch for the given database.
  result = WriteBatch(
    data: @[],
    db: db,
    durability: none(PersistMode)
  )

# Creates a new write batch with preallocated capacity
proc withCapacity*(db: Database, capacity: int): WriteBatch =
  ## Creates a new write batch with preallocated capacity.
  result = WriteBatch(
    data: newSeqOfCap[Item](capacity),
    db: db,
    durability: none(PersistMode)
  )

# Gets the number of batched items
proc len*(batch: WriteBatch): int =
  ## Returns the number of items in the batch.
  batch.data.len

# Returns true if there are no batched items
proc isEmpty*(batch: WriteBatch): bool =
  ## Returns true if the batch is empty.
  batch.len == 0

# Sets the durability level
proc setDurability*(batch: WriteBatch, mode: PersistMode) =
  ## Sets the durability level for the batch.
  batch.durability = some(mode)

# Inserts a key-value pair into the batch
proc insert*(batch: WriteBatch, keyspace: ks.Keyspace, key: UserKey,
    value: UserValue) =
  ## Inserts a key-value pair into the batch.
  batch.data.add(newItem(keyspace, key, value, vtValue))

# Removes a key-value pair
proc remove*(batch: WriteBatch, keyspace: ks.Keyspace, key: UserKey) =
  ## Removes a key from the batch (adds a tombstone).
  batch.data.add(newItem(keyspace, key, "", vtTombstone))

# Adds a weak tombstone marker for a key
proc removeWeak*(batch: WriteBatch, keyspace: ks.Keyspace, key: UserKey) =
  ## Adds a weak tombstone marker for a key.
  batch.data.add(newItem(keyspace, key, "", vtWeakTombstone))

# Clears the batch
proc clear*(batch: WriteBatch) =
  ## Clears all items from the batch.
  batch.data.setLen(0)

# Gets the total size of the batch in bytes
proc size*(batch: WriteBatch): uint64 =
  ## Calculates the approximate size of the batch in bytes.
  for item in batch.data:
    result += uint64(item.key.len + item.value.len)
