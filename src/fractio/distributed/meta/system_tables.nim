# System Table Definitions for Fractio
#
# Defines the key encoding scheme for table records stored in the KV layer.
# All table records use the format: /t/<tableID>/<primaryKey>
#
# - tableID is a uint32 encoded as a zero-padded 10-digit decimal string
#   for correct lexicographic ordering (e.g., 0000000001)
# - System tables use low tableID values (1-99)
# - User tables start at tableID = 100
#
# Replication tiers:
#   Tier 1 (Meta Range, Range 1): system tables 1-6, replicated on ALL nodes
#   Tier 2 (Standard RF=3):       metrics/events tables 10+, user tables 100+
#   Tier 3 (Node-local):          /sys/liveness/*, /raft/*, not replicated

import std/strutils
import fractio/core/types
import fractio/distributed/raft/group_types

# ============================================================================
# System Table IDs
# ============================================================================

# Special ULID for META_GROUP_ID - all zeros except last byte = 1
# This is a well-known ULID for the meta group
proc metaGroupULID(): ULID =
  for i in 0..<15:
    result.data[i] = 0'u8
  result.data[15] = 1'u8

proc dataGroupStartULID(): ULID =
  for i in 0..<15:
    result.data[i] = 0'u8
  result.data[15] = 2'u8

var META_GROUP_ID* = GroupID(metaGroupULID())
  ## The meta range is always Range 1, replicated on every node
  ## Uses a well-known ULID (00000000000000000000000001)

var DATA_GROUP_START_ID* = GroupID(dataGroupStartULID())
  ## First data range ID - ULID with last byte = 2

# ============================================================================
# System Table IDs
# ============================================================================

const
  SYS_DATABASES_TABLE_ID* = 1'u32
    ## Database catalog: /t/0000000001/<dbName>
    ## Value: JSON {id, name, owner, createdAt}

  SYS_SCHEMAS_TABLE_ID* = 2'u32
    ## Schema catalog: /t/0000000002/<dbId>/<schemaName>
    ## Value: JSON {id, dbId, name, createdAt}

  SYS_TABLES_TABLE_ID* = 3'u32
    ## Table descriptors: /t/0000000003/<schemaId>/<tableName>
    ## Value: JSON {id, schemaId, name, columns, indices, createdAt}

  SYS_GROUPS_TABLE_ID* = 4'u32
    ## Authoritative range map: /t/0000000004/<groupId>
    ## Value: JSON-encoded GroupDescriptor

  SYS_NODES_TABLE_ID* = 5'u32
    ## Cluster node registry: /t/0000000005/<nodeId>
    ## Value: JSON {nodeId, host, raftPort, clientPort, status, ...}

  SYS_SETTINGS_TABLE_ID* = 6'u32
    ## Cluster-wide configuration: /t/0000000006/<settingKey>
    ## Value: string

  SYS_SPACES_TABLE_ID* = 7'u32
    ## Space catalog: /t/0000000007/<spaceId>
    ## Value: JSON {spaceId, name, replicas, groupCount, groupIds, createdAt}

  SYS_NODE_METRICS_ID* = 10'u32
    ## Per-node performance metrics: /t/0000000010/<nodeId>/<metricName>
    ## Value: numeric string

  SYS_GROUP_METRICS_ID* = 11'u32
    ## Per-range stats: /t/0000000011/<groupId>/<metricName>
    ## Value: numeric string

  SYS_EVENTS_TABLE_ID* = 12'u32
    ## Cluster event log: /t/0000000012/<timestampNs>/<seqNo>
    ## Value: JSON event payload

  FIRST_USER_TABLE_ID* = 100'u32
    ## User tables start at this ID

  MAX_SYSTEM_TABLE_ID* = 99'u32
    ## System tables occupy IDs 1..99

  # Tier 1 system tables (replicated on all nodes via meta range)
  MAX_META_GROUP_TABLE_ID* = 7'u32
    ## Tables 1-7 live in the meta range (Range 1)

# ============================================================================
# Key encoding constants
# ============================================================================

const
  TABLE_KEY_PREFIX* = "/t/"
    ## All table keys start with this prefix

  TABLE_ID_WIDTH* = 10
    ## Width of zero-padded tableID in the key

  # Pre-computed boundary key: one past the last meta range table
  # /t/0000000008 — first key NOT in the meta range
  META_GROUP_END_KEY* = "/t/0000000008"
    ## Exclusive upper bound for meta range table keys

# ============================================================================
# Key Encoding / Decoding
# ============================================================================

const
  ULID_KEY_WIDTH* = 26
    ## Width of ULID string in key (26 characters)

proc formatTableId*(tableId: uint32): string {.inline.} =
  ## Format a tableID as a zero-padded 10-digit decimal string.
  ## e.g., 1 → "0000000001", 100 → "0000000100"
  result = align($tableId, TABLE_ID_WIDTH, '0')

proc formatTableIdUlid*(tableId: ULID): string {.inline.} =
  ## Format a table ULID as a 26-character string for use in keys.
  result = $tableId

proc encodeTableKey*(tableId: uint32, primaryKey: string): string =
  ## Encode a table record key: /t/<tableID>/<primaryKey>
  result = TABLE_KEY_PREFIX & formatTableId(tableId) & "/" & primaryKey

proc encodeTableKeyUlid*(tableId: ULID, primaryKey: string): string =
  ## Encode a table record key with ULID: /t/<ulid>/<primaryKey>
  result = TABLE_KEY_PREFIX & formatTableIdUlid(tableId) & "/" & primaryKey

proc decodeTableKey*(key: string): tuple[tableId: uint32, primaryKey: string] =
  ## Decode a table key back to its components.
  ## Raises ValueError if the key format is invalid.
  if not key.startsWith(TABLE_KEY_PREFIX):
    raise newException(ValueError, "Not a table key: " & key)

  let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
  if afterPrefix.len < TABLE_ID_WIDTH + 1: # tableId + "/"
    raise newException(ValueError, "Table key too short: " & key)

  let tableIdStr = afterPrefix[0 ..< TABLE_ID_WIDTH]
  let tableId = parseUInt(tableIdStr)
  if tableId > uint32.high.uint:
    raise newException(ValueError, "Table ID overflow: " & tableIdStr)

  # Skip the "/" after tableId
  let primaryKey = afterPrefix[TABLE_ID_WIDTH + 1 .. ^1]

  result = (tableId: uint32(tableId), primaryKey: primaryKey)

proc decodeTableKeyUlid*(key: string): tuple[tableId: ULID,
    primaryKey: string] =
  ## Decode a table key with ULID back to its components.
  ## Raises ValueError if the key format is invalid.
  if not key.startsWith(TABLE_KEY_PREFIX):
    raise newException(ValueError, "Not a table key: " & key)

  let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
  if afterPrefix.len < ULID_KEY_WIDTH + 1: # ulid + "/"
    raise newException(ValueError, "Table key too short for ULID: " & key)

  let ulidStr = afterPrefix[0 ..< ULID_KEY_WIDTH]
  let tableId = ulidFromString(ulidStr)

  # Skip the "/" after ulid
  let primaryKey = afterPrefix[ULID_KEY_WIDTH + 1 .. ^1]

  result = (tableId: tableId, primaryKey: primaryKey)

proc isUlidTableKey*(key: string): bool =
  ## Check if a key has a ULID table ID (26-char width instead of 10).
  ## ULID table keys start with /t/ and have a 26-character ID portion.
  if not key.startsWith(TABLE_KEY_PREFIX):
    return false
  let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
  # ULID keys are 26 chars + "/" + rest
  # Check if the separator is at position 26
  if afterPrefix.len > ULID_KEY_WIDTH and afterPrefix[ULID_KEY_WIDTH] == '/':
    # Check if first 26 chars are valid ULID characters
    for i in 0..<ULID_KEY_WIDTH:
      let c = afterPrefix[i]
      if c notin {'0'..'9', 'A'..'Z'}:
        return false
    return true
  return false

# ============================================================================
# Key classification
# ============================================================================

proc isTableKey*(key: string): bool {.inline.} =
  ## Check if a key is a table key (starts with /t/)
  key.startsWith(TABLE_KEY_PREFIX)

proc isSystemKey*(key: string): bool =
  ## Check if a key belongs to a system table (tableID 1-99).
  ## Returns false for non-table keys.
  if not key.startsWith(TABLE_KEY_PREFIX):
    return false
  let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
  if afterPrefix.len < TABLE_ID_WIDTH:
    return false
  try:
    let tableId = parseUInt(afterPrefix[0 ..< TABLE_ID_WIDTH])
    result = tableId >= 1 and tableId <= MAX_SYSTEM_TABLE_ID
  except ValueError:
    result = false

proc isMetaGroupKey*(key: string): bool =
  ## Check if a key belongs to the meta range (tableID 1-6 or /sys/meta*).
  ## These keys are replicated on ALL nodes via Range 1.
  if key.startsWith("/sys/meta1/") or key.startsWith("/sys/meta2/"):
    return true
  if not key.startsWith(TABLE_KEY_PREFIX):
    return false
  let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
  if afterPrefix.len < TABLE_ID_WIDTH:
    return false
  try:
    let tableId = parseUInt(afterPrefix[0 ..< TABLE_ID_WIDTH])
    result = tableId >= 1 and tableId <= MAX_META_GROUP_TABLE_ID
  except ValueError:
    result = false

proc isUserTableKey*(key: string): bool =
  ## Check if a key belongs to a user table (tableID >= 100).
  if not key.startsWith(TABLE_KEY_PREFIX):
    return false
  let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
  if afterPrefix.len < TABLE_ID_WIDTH:
    return false
  try:
    let tableId = parseUInt(afterPrefix[0 ..< TABLE_ID_WIDTH])
    result = tableId >= FIRST_USER_TABLE_ID
  except ValueError:
    result = false

proc tableIdFromKey*(key: string): uint32 =
  ## Extract the tableID from a table key. Raises ValueError for non-table keys.
  let (tableId, _) = decodeTableKey(key)
  result = tableId

# ============================================================================
# User table key helpers (data rows and secondary index entries)
# ============================================================================

proc encodeDataRowKey*(tableId: uint32, primaryKey: string): string =
  ## Encode a data row key: /t/<tableID>/d/<primaryKey>
  encodeTableKey(tableId, "d/" & primaryKey)

proc encodeIndexKey*(tableId: uint32, indexId: uint32,
                     indexKey: string, primaryKey: string): string =
  ## Encode a secondary index entry key:
  ## /t/<tableID>/i/<indexID>/<indexKey>/<primaryKey>
  encodeTableKey(tableId, "i/" & formatTableId(indexId) & "/" &
                 indexKey & "/" & primaryKey)

proc encodeSpaceKey*(spaceId: ULID): string =
  ## Encode a space catalog key: /t/<SYS_SPACES_TABLE_ID>/<spaceId>
  encodeTableKey(SYS_SPACES_TABLE_ID, $spaceId)
