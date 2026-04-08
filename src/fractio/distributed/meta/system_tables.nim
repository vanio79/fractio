# System Table Definitions for Fractio
#
# Defines the key encoding scheme for table records stored in the KV layer.
# All table records use the format: /t/<tableId>/<primaryKey>
#
# - tableId is now a ULID (TableId = distinct ULID) encoded as 26-char string
# - System tables use well-known ULIDs with embedded system table numbers
# - User tables use genTableId() for globally unique, sortable IDs
#
# Replication tiers:
#   Tier 1 (Meta Range): system tables 1-7, replicated on ALL nodes
#   Tier 2 (Standard RF=3): metrics/events tables 10+, user tables
#   Tier 3 (Node-local): /sys/liveness/*, /raft/*, not replicated

import std/strutils
import fractio/core/types
import fractio/distributed/raft/group_types

# ============================================================================
# Well-known ULID Generation for System Tables
# ============================================================================

proc systemTableULID*(sysTableNum: uint8): ULID =
  ## Create a well-known ULID for a system table.
  ## System table ULIDs have:
  ## - Timestamp bytes (0-5): all zeros (earliest possible timestamp)
  ## - Randomness bytes (6-14): all zeros
  ## - Last byte (15): the system table number
  ## This ensures system tables have deterministic, globally-known ULIDs
  ## that sort before any user-generated ULIDs.
  for i in 0..<15:
    result.data[i] = 0'u8
  result.data[15] = sysTableNum

# ============================================================================
# System Table IDs (Well-known ULIDs)
# ============================================================================

const
  # System table numbers (embedded in last byte of ULID)
  SYS_DATABASES_TABLE_NUM* = 1'u8
  SYS_SCHEMAS_TABLE_NUM* = 2'u8
  SYS_TABLES_TABLE_NUM* = 3'u8
  SYS_GROUPS_TABLE_NUM* = 4'u8
  SYS_NODES_TABLE_NUM* = 5'u8
  SYS_SETTINGS_TABLE_NUM* = 6'u8
  SYS_SPACES_TABLE_NUM* = 7'u8
  SYS_NODE_METRICS_NUM* = 10'u8
  SYS_GROUP_METRICS_NUM* = 11'u8
  SYS_EVENTS_TABLE_NUM* = 12'u8

# System table IDs as well-known TableId (ULID-based)
let
  SYS_DATABASES_TABLE_ID* = TableId(systemTableULID(SYS_DATABASES_TABLE_NUM))
    ## Database catalog: /t/<SYS_DATABASES_TABLE_ID>/<dbName>
    ## Value: JSON {id, name, owner, createdAt}

  SYS_SCHEMAS_TABLE_ID* = TableId(systemTableULID(SYS_SCHEMAS_TABLE_NUM))
    ## Schema catalog: /t/<SYS_SCHEMAS_TABLE_ID>/<dbId>/<schemaName>
    ## Value: JSON {id, dbId, name, createdAt}

  SYS_TABLES_TABLE_ID* = TableId(systemTableULID(SYS_TABLES_TABLE_NUM))
    ## Table descriptors: /t/<SYS_TABLES_TABLE_ID>/<schemaId>/<tableName>
    ## Value: JSON {id, schemaId, name, columns, indices, createdAt}

  SYS_GROUPS_TABLE_ID* = TableId(systemTableULID(SYS_GROUPS_TABLE_NUM))
    ## Authoritative range map: /t/<SYS_GROUPS_TABLE_ID>/<groupId>
    ## Value: JSON-encoded GroupDescriptor

  SYS_NODES_TABLE_ID* = TableId(systemTableULID(SYS_NODES_TABLE_NUM))
    ## Cluster node registry: /t/<SYS_NODES_TABLE_ID>/<nodeId>
    ## Value: JSON {nodeId, host, raftPort, clientPort, status, ...}

  SYS_SETTINGS_TABLE_ID* = TableId(systemTableULID(SYS_SETTINGS_TABLE_NUM))
    ## Cluster-wide configuration: /t/<SYS_SETTINGS_TABLE_ID>/<settingKey>
    ## Value: string

  SYS_SPACES_TABLE_ID* = TableId(systemTableULID(SYS_SPACES_TABLE_NUM))
    ## Space catalog: /t/<SYS_SPACES_TABLE_ID>/<spaceId>
    ## Value: JSON {spaceId, name, replicas, groupCount, groupIds, createdAt}

  SYS_NODE_METRICS_ID* = TableId(systemTableULID(SYS_NODE_METRICS_NUM))
    ## Per-node performance metrics: /t/<SYS_NODE_METRICS_ID>/<nodeId>/<metricName>
    ## Value: numeric string

  SYS_GROUP_METRICS_ID* = TableId(systemTableULID(SYS_GROUP_METRICS_NUM))
    ## Per-range stats: /t/<SYS_GROUP_METRICS_ID>/<groupId>/<metricName>
    ## Value: numeric string

  SYS_EVENTS_TABLE_ID* = TableId(systemTableULID(SYS_EVENTS_TABLE_NUM))
    ## Cluster event log: /t/<SYS_EVENTS_TABLE_ID>/<timestampNs>/<seqNo>
    ## Value: JSON event payload

# ============================================================================
# Constants for table classification
# ============================================================================

const
  MAX_SYSTEM_TABLE_NUM* = 99'u8
    ## System tables use numbers 1-99

  MAX_META_GROUP_TABLE_NUM* = 7'u8
    ## Tables 1-7 live in the meta range (Range 1)

# ============================================================================
# Key encoding constants
# ============================================================================

const
  TABLE_KEY_PREFIX* = "/t/"
    ## All table keys start with this prefix

  TABLE_ID_WIDTH* = 26
    ## Width of ULID string in key (26 characters)

# ============================================================================
# Helper functions for scan boundaries
# ============================================================================

proc nextSystemTableId*(tableId: TableId): TableId =
  ## Get the "next" system table ID for scan end bounds.
  ## For system tables (well-known ULIDs), this increments the last byte.
  ## For user tables, returns a placeholder since we can't predict the next ULID.
  var ulid = ULID(tableId)
  # Check if it's a system table (bytes 0-14 are zero)
  var isSys = true
  for i in 0..<15:
    if ulid.data[i] != 0'u8:
      isSys = false
      break
  if isSys and ulid.data[15] < 255'u8:
    ulid.data[15] = ulid.data[15] + 1'u8
    result = TableId(ulid)
  else:
    # For user tables or max system table, return a high ULID
    # This ensures the scan includes all possible keys
    var res: ULID
    for i in 0..<16:
      res.data[i] = 0xFF'u8
    result = TableId(res)

proc makeScanEndKey*(tableId: TableId): string =
  ## Create an end key for scanning a table's catalog records.
  ## Returns "/t/<nextTableId>/<empty>" to scan all keys for the given table.
  TABLE_KEY_PREFIX & $(nextSystemTableId(tableId)) & "/"

# ============================================================================
# Meta Group ID (unchanged)
# ============================================================================

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

# Pre-computed boundary key: one past the last meta range table
# System table 8 would be 00000000000000000000000008
let META_GROUP_END_KEY* = TABLE_KEY_PREFIX & "00000000000000000000000008/"
  ## Exclusive upper bound for meta range table keys

# ============================================================================
# Key Encoding / Decoding
# ============================================================================

proc formatTableId*(tableId: TableId): string {.inline.} =
  ## Format a TableId as a 26-character ULID string for use in keys.
  result = $tableId

proc encodeTableKey*(tableId: TableId, primaryKey: string): string =
  ## Encode a table record key: /t/<tableId>/<primaryKey>
  result = TABLE_KEY_PREFIX & formatTableId(tableId) & "/" & primaryKey

proc decodeTableKey*(key: string): tuple[tableId: TableId, primaryKey: string] =
  ## Decode a table key back to its components.
  ## Raises ValueError if the key format is invalid.
  if not key.startsWith(TABLE_KEY_PREFIX):
    raise newException(ValueError, "Not a table key: " & key)

  let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
  if afterPrefix.len < TABLE_ID_WIDTH + 1: # ulid + "/"
    raise newException(ValueError, "Table key too short: " & key)

  let tableIdStr = afterPrefix[0 ..< TABLE_ID_WIDTH]
  let tableId = tableIdFromString(tableIdStr)

  # Skip the "/" after tableId
  let primaryKey = afterPrefix[TABLE_ID_WIDTH + 1 .. ^1]

  result = (tableId: tableId, primaryKey: primaryKey)

# ============================================================================
# Key classification
# ============================================================================

proc isTableKey*(key: string): bool {.inline.} =
  ## Check if a key is a table key (starts with /t/)
  key.startsWith(TABLE_KEY_PREFIX)

proc isSystemTableId*(tableId: TableId): bool =
  ## Check if a TableId is a system table (well-known ULID pattern).
  ## System table ULIDs have all zeros in bytes 0-14 and table num in byte 15.
  var ulid = ULID(tableId)
  for i in 0..<15:
    if ulid.data[i] != 0'u8:
      return false
  let tableNum = ulid.data[15]
  result = tableNum >= 1'u8 and tableNum <= MAX_SYSTEM_TABLE_NUM

proc isMetaGroupTableId*(tableId: TableId): bool =
  ## Check if a TableId belongs to the meta range (tables 1-7).
  ## These keys are replicated on ALL nodes via Range 1.
  var ulid = ULID(tableId)
  for i in 0..<15:
    if ulid.data[i] != 0'u8:
      return false
  let tableNum = ulid.data[15]
  result = tableNum >= 1'u8 and tableNum <= MAX_META_GROUP_TABLE_NUM

proc isUserTableId*(tableId: TableId): bool =
  ## Check if a TableId is a user table (not a system table).
  ## User tables have non-zero bytes in the ULID (generated timestamps).
  not isSystemTableId(tableId)

proc isSystemKey*(key: string): bool =
  ## Check if a key belongs to a system table.
  ## Returns false for non-table keys.
  if not key.startsWith(TABLE_KEY_PREFIX):
    return false
  try:
    let (tableId, _) = decodeTableKey(key)
    result = isSystemTableId(tableId)
  except ValueError:
    result = false

proc isMetaGroupKey*(key: string): bool =
  ## Check if a key belongs to the meta range (system tables 1-7).
  ## These keys are replicated on ALL nodes via Range 1.
  if not key.startsWith(TABLE_KEY_PREFIX):
    return false
  try:
    let (tableId, _) = decodeTableKey(key)
    result = isMetaGroupTableId(tableId)
  except ValueError:
    result = false

proc isUserTableKey*(key: string): bool =
  ## Check if a key belongs to a user table.
  if not key.startsWith(TABLE_KEY_PREFIX):
    return false
  try:
    let (tableId, _) = decodeTableKey(key)
    result = isUserTableId(tableId)
  except ValueError:
    result = false

proc tableIdFromKey*(key: string): TableId =
  ## Extract the TableId from a table key. Raises ValueError for non-table keys.
  let (tableId, _) = decodeTableKey(key)
  result = tableId

proc systemTableNumFromId*(tableId: TableId): uint8 =
  ## Extract the system table number from a well-known system TableId.
  ## Returns 0 for user tables (non-system).
  var ulid = ULID(tableId)
  for i in 0..<15:
    if ulid.data[i] != 0'u8:
      return 0'u8 # Not a system table
  result = ulid.data[15]

# ============================================================================
# User table key helpers (data rows and secondary index entries)
# ============================================================================

proc encodeDataRowKey*(tableId: TableId, primaryKey: string): string =
  ## Encode a data row key: /t/<tableId>/d/<primaryKey>
  encodeTableKey(tableId, "d/" & primaryKey)

proc makeDataRowScanEndKey*(tableId: TableId): string =
  ## Create an end key for scanning all data rows of a table.
  ## Returns "/t/<tableId>/e/" where "e" > "d" (data prefix).
  ## This scans all keys matching /t/<tableId>/d/... (data rows).
  encodeTableKey(tableId, "e/")

proc encodeIndexKey*(tableId: TableId, indexId: TableId,
                     indexKey: string, primaryKey: string): string =
  ## Encode a secondary index entry key:
  ## /t/<tableId>/i/<indexId>/<indexKey>/<primaryKey>
  encodeTableKey(tableId, "i/" & formatTableId(indexId) & "/" &
                 indexKey & "/" & primaryKey)

proc encodeSpaceKey*(spaceId: SpaceID): string =
  ## Encode a space catalog key: /t/<SYS_SPACES_TABLE_ID>/<spaceId>
  encodeTableKey(SYS_SPACES_TABLE_ID, $spaceId)

# ============================================================================
# Legacy uint32 compatibility (for migration)
# ============================================================================

proc formatTableIdLegacy*(tableId: uint32): string {.inline.} =
  ## Legacy format for uint32 tableId (zero-padded 10-digit decimal).
  ## DEPRECATED: Use formatTableId(TableId) instead.
  result = align($tableId, 10, '0')

proc encodeTableKeyLegacy*(tableId: uint32, primaryKey: string): string =
  ## Legacy encoding for uint32 tableId.
  ## DEPRECATED: Use encodeTableKey(TableId) instead.
  result = TABLE_KEY_PREFIX & formatTableIdLegacy(tableId) & "/" & primaryKey

proc decodeTableKeyLegacy*(key: string): tuple[tableId: uint32,
    primaryKey: string] =
  ## Legacy decoder for uint32 tableId keys.
  ## DEPRECATED: Use decodeTableKey() instead.
  if not key.startsWith(TABLE_KEY_PREFIX):
    raise newException(ValueError, "Not a table key: " & key)

  let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
  if afterPrefix.len < 10 + 1: # old width + "/"
    raise newException(ValueError, "Table key too short: " & key)

  let tableIdStr = afterPrefix[0 ..< 10]
  let tableId = parseUInt(tableIdStr)
  if tableId > uint32.high.uint:
    raise newException(ValueError, "Table ID overflow: " & tableIdStr)

  let primaryKey = afterPrefix[11 .. ^1]
  result = (tableId: uint32(tableId), primaryKey: primaryKey)

proc isLegacyTableKey*(key: string): bool =
  ## Check if a key uses legacy 10-digit uint32 format.
  ## Returns false for ULID-format keys or non-table keys.
  if not key.startsWith(TABLE_KEY_PREFIX):
    return false
  let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
  # Legacy keys have 10 digits + "/"
  if afterPrefix.len > 10 and afterPrefix[10] == '/':
    # Check if first 10 chars are digits
    for i in 0..<10:
      let c = afterPrefix[i]
      if c notin {'0'..'9'}:
        return false
    return true
  return false
