# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[error, types, keyspace, ingestion]
import std/[tables, atomics, locks]

# Forward declarations
type
  AnyTree* = object
  Keyspaces* = Table[string, Keyspace]
  InternalKeyspaceId* = uint64
  SequenceNumberCounter* = object
    value: Atomic[SeqNo]

proc encodeConfigKey*(keyspaceId: InternalKeyspaceId, name: string): UserKey =
  var key: seq[byte] = @[]
  key.add(byte('c'))

  # Add keyspace ID as big endian bytes
  let idBytes = toBigEndianBytes(keyspaceId)
  key.add(idBytes)

  # Add name bytes
  for b in name:
    key.add(byte(b))

  # Convert to UserKey (string in our implementation)
  return cast[string](key)

# Helper to convert uint64 to big endian bytes
proc toBigEndianBytes*(value: uint64): array[8, byte] =
  result[0] = byte((value shr 56) and 0xFF)
  result[1] = byte((value shr 48) and 0xFF)
  result[2] = byte((value shr 40) and 0xFF)
  result[3] = byte((value shr 32) and 0xFF)
  result[4] = byte((value shr 24) and 0xFF)
  result[5] = byte((value shr 16) and 0xFF)
  result[6] = byte((value shr 8) and 0xFF)
  result[7] = byte(value and 0xFF)

# The meta keyspace keeps mappings of keyspace names to their internal IDs and configurations
# The meta keyspace is always keyspace #0
type
  MetaKeyspace* = object
    inner*: AnyTree
    keyspaces*: ptr RwLock[Keyspaces]
    seqnoGenerator*: SequenceNumberCounter
    visibleSeqno*: SequenceNumberCounter

# Constructor
proc newMetaKeyspace*(inner: AnyTree, keyspaces: ptr RwLock[Keyspaces],
                      seqnoGenerator: SequenceNumberCounter,
                      visibleSeqno: SequenceNumberCounter): MetaKeyspace =
  MetaKeyspace(
    inner: inner,
    keyspaces: keyspaces,
    seqnoGenerator: seqnoGenerator,
    visibleSeqno: visibleSeqno
  )

# Get key-value for config
proc getKvForConfig*(meta: MetaKeyspace, keyspaceId: InternalKeyspaceId,
                     name: string): StorageResult[Option[UserValue]] =
  let key = encodeConfigKey(keyspaceId, name)
  # In a full implementation, this would get the value from the tree
  # For now, we'll return none
  return ok(none(UserValue))

# Maintenance
proc maintenance*(meta: MetaKeyspace): StorageResult[void] =
  # In a full implementation, this would perform compaction
  # For now, we'll return success
  return ok()

# Create keyspace
proc createKeyspace*(meta: MetaKeyspace, keyspaceId: InternalKeyspaceId, name: string,
                     keyspace: Keyspace,
                         keyspaces: var Keyspaces): StorageResult[void] =
  # In a full implementation, this would:
  # 1. Encode keyspace config to key-value pairs
  # 2. Add ID->name mapping
  # 3. Sort KVs
  # 4. Ingest into meta tree
  # 5. Insert into keyspaces map
  # 6. Run maintenance

  # For now, we'll just insert into the keyspaces map
  keyspaces[name] = keyspace

  # Run maintenance (ignore errors)
  discard meta.maintenance()

  return ok()

# Remove keyspace
proc removeKeyspace*(meta: MetaKeyspace, name: string): StorageResult[void] =
  # In a full implementation, this would:
  # 1. Acquire keyspaces lock
  # 2. Get keyspace by name
  # 3. Generate next sequence number
  # 4. Create ingestion
  # 5. Remove config KVs and ID->name mapping
  # 6. Finish ingestion
  # 7. Update visible seqno
  # 8. Remove from keyspaces map
  # 9. Run maintenance

  # For now, we'll just remove from the keyspaces map
  var lock: RwLock[Keyspaces]
  # In a full implementation, we would acquire the lock
  # keyspaces.remove(name)

  # Run maintenance (ignore errors)
  discard meta.maintenance()

  return ok()

# Resolve ID to name
proc resolveId*(meta: MetaKeyspace, id: InternalKeyspaceId): StorageResult[
    Option[string]] =
  # In a full implementation, this would:
  # 1. Create key for ID->name mapping
  # 2. Get value from tree
  # 3. Convert to string

  # For now, we'll return none
  return ok(none(string))

# Check if keyspace exists
proc keyspaceExists*(meta: MetaKeyspace, name: string): bool =
  # In a full implementation, this would check if keyspace exists in the map
  # For now, we'll return false
  return false
