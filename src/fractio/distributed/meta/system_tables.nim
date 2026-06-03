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
#   Tier 1 (Meta Group): system tables 1-7, replicated on ALL nodes
#   Tier 2 (Standard RF=3): metrics/events tables 10+, user tables
#   Tier 3 (Node-local): /sys/liveness/*, /raft/*, not replicated

import std/[strutils, options]
import fractio/core/types
import fractio/distributed/meta/system_schemas
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
  SYS_COLUMNS_TABLE_NUM* = 8'u8
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
    ## Authoritative group map: /t/<SYS_GROUPS_TABLE_ID>/<groupId>
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

  SYS_COLUMNS_TABLE_ID* = TableId(systemTableULID(SYS_COLUMNS_TABLE_NUM))
    ## Column descriptors: /t/<SYS_COLUMNS_TABLE_ID>/<tableId>/<ordinal>
    ## Value: binary ColumnRecord

  SYS_NODE_METRICS_ID* = TableId(systemTableULID(SYS_NODE_METRICS_NUM))
    ## Per-node performance metrics: /t/<SYS_NODE_METRICS_ID>/<nodeId>/<metricName>
    ## Value: numeric string

  SYS_GROUP_METRICS_ID* = TableId(systemTableULID(SYS_GROUP_METRICS_NUM))
    ## Per-group stats: /t/<SYS_GROUP_METRICS_ID>/<groupId>/<metricName>
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

  MAX_META_GROUP_TABLE_NUM* = 8'u8
    ## Tables 1-8 live in the meta group (Group 1)

# ============================================================================
# System Table Registry
# ============================================================================
#
# Single source of truth for all system table metadata. Every system table
# is defined here with its ID, name, columns, and description. This registry
# is used by the planner, executor, dashboard, and bootstrap code so that
# system tables can be treated like normal user tables — queried via SQL,
# listed in SHOW TABLES, and introspected from sys.tables.
#
# When a new system table is added:
#   1. Add a SYS_*_TABLE_NUM and SYS_*_TABLE_ID constant above
#   2. Add an entry to SYSTEM_TABLES_REGISTRY below
#   3. Add a decodeSystemTableRecord case in executor.nim
#   4. Add a binary record type in system_schemas.nim if needed

type
  SysColDef* = object
    ## Column definition for system table registry.
    ## Self-contained (no dependency on planner types).
    name*: string
    dataType*: DataType ## Uses core/types.DataType (dtInt, dtString, etc.)
    maxLen*: int ## 0 = unspecified, use default
    primaryKey*: bool
    notNull*: bool

  SysPrimaryKeySpec* = object
    ## Primary key spec for system tables (simplified).
    columns*: seq[tuple[name: string, dataType: ColumnDataType, maxLen: int]]

  SystemTableInfo* = object
    ## Complete metadata for a system table.
    ## Used as the single source of truth for system table definitions.
    tableNum*: uint8 ## Well-known system table number (1-99)
    tableId*: TableId ## Well-known TableId (ULID with tableNum in last byte)
    name*: string ## Unqualified table name (e.g. "databases")
    schema*: string ## Schema name (always "sys" for system tables)
    database*: string ## Database name (always "sys" for system tables)
    description*: string ## Human-readable description
    columns*: seq[SysColDef] ## Column definitions
    primaryKey*: seq[string] ## Primary key column names
    pkSpec*: SysPrimaryKeySpec ## Primary key spec for binary encoding
    keyEncoding*: TableKeyEncoding ## Always tkeSystemTable for system tables

let
  SYSTEM_TABLES_REGISTRY* = [
    SystemTableInfo(
      tableNum: SYS_DATABASES_TABLE_NUM,
      tableId: SYS_DATABASES_TABLE_ID,
      name: "databases",
      schema: "sys",
      database: "sys",
      description: "Database catalog",
      columns: @[
        SysColDef(name: "_key", dataType: dtString, maxLen: 64,
            primaryKey: true, notNull: true),
        SysColDef(name: "name", dataType: dtString),
        SysColDef(name: "createdAt", dataType: dtDateTime)
    ],
    primaryKey: @["_key"],
    pkSpec: SysPrimaryKeySpec(columns: @[(name: "_key", dataType: cdtString,
        maxLen: 64)]),
    keyEncoding: tkeSystemTable
  ),
    SystemTableInfo(
      tableNum: SYS_SCHEMAS_TABLE_NUM,
      tableId: SYS_SCHEMAS_TABLE_ID,
      name: "schemas",
      schema: "sys",
      database: "sys",
      description: "Schema catalog",
      columns: @[
        SysColDef(name: "_key", dataType: dtString, maxLen: 64,
            primaryKey: true, notNull: true),
        SysColDef(name: "name", dataType: dtString),
        SysColDef(name: "database", dataType: dtString),
        SysColDef(name: "createdAt", dataType: dtDateTime)
    ],
    primaryKey: @["_key"],
    pkSpec: SysPrimaryKeySpec(columns: @[(name: "_key", dataType: cdtString,
        maxLen: 64)]),
    keyEncoding: tkeSystemTable
  ),
    SystemTableInfo(
      tableNum: SYS_TABLES_TABLE_NUM,
      tableId: SYS_TABLES_TABLE_ID,
      name: "tables",
      schema: "sys",
      database: "sys",
      description: "Table descriptors",
      columns: @[
        SysColDef(name: "_key", dataType: dtString, maxLen: 64,
            primaryKey: true, notNull: true),
        SysColDef(name: "tableId", dataType: dtULID),
        SysColDef(name: "name", dataType: dtString),
        SysColDef(name: "schema", dataType: dtString),
        SysColDef(name: "database", dataType: dtString),
        SysColDef(name: "spaceId", dataType: dtULID),
        SysColDef(name: "primaryKey", dataType: dtString),
        SysColDef(name: "columns", dataType: dtBytes)
    ],
    primaryKey: @["_key"],
    pkSpec: SysPrimaryKeySpec(columns: @[(name: "_key", dataType: cdtString,
        maxLen: 64)]),
    keyEncoding: tkeSystemTable
  ),
    SystemTableInfo(
      tableNum: SYS_GROUPS_TABLE_NUM,
      tableId: SYS_GROUPS_TABLE_ID,
      name: "groups",
      schema: "sys",
      database: "sys",
      description: "Raft group metadata",
      columns: @[
        SysColDef(name: "_key", dataType: dtString, maxLen: 64,
            primaryKey: true, notNull: true),
        SysColDef(name: "groupId", dataType: dtULID),
        SysColDef(name: "spaceId", dataType: dtULID),
        SysColDef(name: "preferredLeader", dataType: dtInt),
        SysColDef(name: "leader", dataType: dtInt),
        SysColDef(name: "replicas", dataType: dtBytes)
    ],
    primaryKey: @["_key"],
    pkSpec: SysPrimaryKeySpec(columns: @[(name: "_key", dataType: cdtString,
        maxLen: 64)]),
    keyEncoding: tkeSystemTable
  ),
    SystemTableInfo(
      tableNum: SYS_NODES_TABLE_NUM,
      tableId: SYS_NODES_TABLE_ID,
      name: "nodes",
      schema: "sys",
      database: "sys",
      description: "Cluster node registry",
      columns: @[
        SysColDef(name: "_key", dataType: dtString, maxLen: 64,
            primaryKey: true, notNull: true),
        SysColDef(name: "nodeId", dataType: dtInt),
        SysColDef(name: "host", dataType: dtString),
        SysColDef(name: "raftPort", dataType: dtInt),
        SysColDef(name: "clientPort", dataType: dtInt),
        SysColDef(name: "webPort", dataType: dtInt),
        SysColDef(name: "status", dataType: dtInt)
    ],
    primaryKey: @["_key"],
    pkSpec: SysPrimaryKeySpec(columns: @[(name: "_key", dataType: cdtString,
        maxLen: 64)]),
    keyEncoding: tkeSystemTable
  ),
    SystemTableInfo(
      tableNum: SYS_SETTINGS_TABLE_NUM,
      tableId: SYS_SETTINGS_TABLE_ID,
      name: "settings",
      schema: "sys",
      database: "sys",
      description: "Cluster configuration",
      columns: @[
        SysColDef(name: "_key", dataType: dtString, maxLen: 64,
            primaryKey: true, notNull: true),
        SysColDef(name: "value", dataType: dtString)
    ],
    primaryKey: @["_key"],
    pkSpec: SysPrimaryKeySpec(columns: @[(name: "_key", dataType: cdtString,
        maxLen: 64)]),
    keyEncoding: tkeSystemTable
  ),
    SystemTableInfo(
      tableNum: SYS_SPACES_TABLE_NUM,
      tableId: SYS_SPACES_TABLE_ID,
      name: "spaces",
      schema: "sys",
      database: "sys",
      description: "Space catalog",
      columns: @[
        SysColDef(name: "_key", dataType: dtString, maxLen: 64,
            primaryKey: true, notNull: true),
        SysColDef(name: "spaceId", dataType: dtULID),
        SysColDef(name: "name", dataType: dtString),
        SysColDef(name: "replicas", dataType: dtInt),
        SysColDef(name: "groupCount", dataType: dtInt),
        SysColDef(name: "groupIds", dataType: dtBytes),
        SysColDef(name: "oldGroupIds", dataType: dtBytes),
        SysColDef(name: "rebalancing", dataType: dtBool),
        SysColDef(name: "createdAt", dataType: dtDateTime)
    ],
    primaryKey: @["_key"],
    pkSpec: SysPrimaryKeySpec(columns: @[(name: "_key", dataType: cdtString,
        maxLen: 64)]),
    keyEncoding: tkeSystemTable
  ),
    SystemTableInfo(
      tableNum: SYS_COLUMNS_TABLE_NUM,
      tableId: SYS_COLUMNS_TABLE_ID,
      name: "columns",
      schema: "sys",
      database: "sys",
      description: "Column descriptors",
      columns: @[
        SysColDef(name: "_key", dataType: dtString, maxLen: 64,
            primaryKey: true, notNull: true),
        SysColDef(name: "tableId", dataType: dtULID),
        SysColDef(name: "name", dataType: dtString),
        SysColDef(name: "ordinal", dataType: dtInt),
        SysColDef(name: "dataType", dataType: dtString),
        SysColDef(name: "maxLen", dataType: dtInt),
        SysColDef(name: "flags", dataType: dtInt)
    ],
    primaryKey: @["_key"],
    pkSpec: SysPrimaryKeySpec(columns: @[(name: "_key", dataType: cdtString,
        maxLen: 64)]),
    keyEncoding: tkeSystemTable
  ),
    SystemTableInfo(
      tableNum: SYS_NODE_METRICS_NUM,
      tableId: SYS_NODE_METRICS_ID,
      name: "node_metrics",
      schema: "sys",
      database: "sys",
      description: "Per-node performance metrics",
      columns: @[
        SysColDef(name: "_key", dataType: dtString, maxLen: 64,
            primaryKey: true, notNull: true),
        SysColDef(name: "nodeId", dataType: dtInt),
        SysColDef(name: "cpuPercent", dataType: dtFloat),
        SysColDef(name: "memUsedBytes", dataType: dtInt),
        SysColDef(name: "diskUsedBytes", dataType: dtInt)
    ],
    primaryKey: @["_key"],
    pkSpec: SysPrimaryKeySpec(columns: @[(name: "_key", dataType: cdtString,
        maxLen: 64)]),
    keyEncoding: tkeSystemTable
  ),
    SystemTableInfo(
      tableNum: SYS_GROUP_METRICS_NUM,
      tableId: SYS_GROUP_METRICS_ID,
      name: "group_metrics",
      schema: "sys",
      database: "sys",
      description: "Per-group performance metrics",
      columns: @[
        SysColDef(name: "_key", dataType: dtString, maxLen: 64,
            primaryKey: true, notNull: true),
        SysColDef(name: "groupId", dataType: dtULID),
        SysColDef(name: "keyCount", dataType: dtInt),
        SysColDef(name: "sizeBytes", dataType: dtInt),
        SysColDef(name: "readQps", dataType: dtFloat),
        SysColDef(name: "writeQps", dataType: dtFloat)
    ],
    primaryKey: @["_key"],
    pkSpec: SysPrimaryKeySpec(columns: @[(name: "_key", dataType: cdtString,
        maxLen: 64)]),
    keyEncoding: tkeSystemTable
  ),
    SystemTableInfo(
      tableNum: SYS_EVENTS_TABLE_NUM,
      tableId: SYS_EVENTS_TABLE_ID,
      name: "events",
      schema: "sys",
      database: "sys",
      description: "Cluster event log",
      columns: @[
        SysColDef(name: "_key", dataType: dtString, maxLen: 64,
            primaryKey: true, notNull: true),
        SysColDef(name: "timestamp", dataType: dtDateTime),
        SysColDef(name: "eventType", dataType: dtString),
        SysColDef(name: "nodeId", dataType: dtInt),
        SysColDef(name: "message", dataType: dtString)
    ],
    primaryKey: @["_key"],
    pkSpec: SysPrimaryKeySpec(columns: @[(name: "_key", dataType: cdtString, maxLen: 64)])
  )
  ]

proc getSystemTableInfoByName*(name: string): Option[SystemTableInfo] =
  ## Look up a SystemTableInfo by its unqualified table name (e.g. "databases").
  let lowerName = name.toLowerAscii()
  for info in SYSTEM_TABLES_REGISTRY:
    if info.name == lowerName:
      return some(info)
  none(SystemTableInfo)

proc getSystemTableInfoById*(tableId: TableId): Option[SystemTableInfo] =
  ## Look up a SystemTableInfo by its well-known TableId.
  for info in SYSTEM_TABLES_REGISTRY:
    if info.tableId == tableId:
      return some(info)
  none(SystemTableInfo)

proc dataTypeToColumnDataType*(dt: DataType): ColumnDataType =
  ## Convert core DataType to binary format ColumnDataType.
  ## Defined here (not in planner) to avoid circular imports.
  case dt
  of dtInt: cdtInt
  of dtFloat: cdtFloat
  of dtString: cdtString
  of dtBool: cdtBool
  of dtBytes: cdtBytes
  of dtDate: cdtDate
  of dtDateTime: cdtDateTime
  of dtULID: cdtULID

proc systemTableInfoToTableRecord*(info: SystemTableInfo): TableRecord =
  ## Convert a SystemTableInfo to a TableRecord for storing in sys.tables.
  ## Column definitions are stored in sys.columns separately for normalisation.
  TableRecord(
    tableId: info.tableId,
    name: info.name,
    schema: info.schema,
    database: info.database,
    spaceId: zeroSpaceID(),
    primaryKey: info.primaryKey,
    keyEncoding: info.keyEncoding
  )

proc systemTableInfoToColumnRecords*(info: SystemTableInfo): seq[ColumnRecord] =
  ## Convert a SystemTableInfo's column definitions to ColumnRecords.
  ## These are stored in sys.columns keyed by (tableId, ordinal).
  result = @[]
  var ordinal = 0
  for sysCol in info.columns:
    var flags: uint8 = 0
    if sysCol.primaryKey: flags = flags or 0x01
    if sysCol.notNull: flags = flags or 0x02
    result.add(ColumnRecord(
      tableId: info.tableId,
      name: sysCol.name,
      ordinal: int32(ordinal),
      dataType: dataTypeToColumnDataType(sysCol.dataType),
      maxLen: uint16(sysCol.maxLen),
      flags: flags
    ))
    inc ordinal

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
  ## The meta group is always Group 1, replicated on every node
  ## Uses a well-known ULID (00000000000000000000000001)

var DATA_GROUP_START_ID* = GroupID(dataGroupStartULID())
  ## First data group ID - ULID with last byte = 2

# Pre-computed boundary key: one past the last meta group table
# System table 8 would be 00000000000000000000000008
let META_GROUP_END_KEY* = TABLE_KEY_PREFIX & "00000000000000000000000008/"
  ## Exclusive upper bound for meta group table keys

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
  ## Check if a TableId belongs to the meta group (tables 1-7).
  ## These keys are replicated on ALL nodes via Group 1.
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
  ## Check if a key belongs to the meta group (system tables 1-7).
  ## These keys are replicated on ALL nodes via Group 1.
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
#
# Key format (all data rows include the group ID):
#   Data row:  /t/<tableId>/d/<groupId>/<primaryKey>
#   Index:     /t/<tableId>/i/<groupId>/<indexId>/<indexKey>/<primaryKey>
#
# The group ID in the key enables per-group range scans:
#   - Scan bounds for group G: /t/<tableId>/d/<groupId>/  ..  /t/<tableId>/d/<groupId>{
#   - Table-wide end key: /t/<tableId>/e/  (covers all groups)
#
# Scan bound keys (start/end of ranges) omit the groupId since they span
# all groups. Use encodeDataRowScanBound() for these delimiters.

const
  DATA_ROW_PREFIX* = "d/"
    ## Prefix for data row keys within a table key
  INDEX_PREFIX* = "i/"
    ## Prefix for index keys within a table key
  GROUP_ID_WIDTH* = 26
    ## Width of GroupID ULID string in key (26 characters, same as TableId)

proc encodeDataRowKey*(tableId: TableId, groupId: GroupID,
                       primaryKey: string): string =
  ## Encode a data row key: /t/<tableId>/d/<groupId>/<primaryKey>
  ##
  ## The groupId enables per-group range scans in the KV store.
  ## Each group's data is stored in a contiguous key range, so a scan
  ## for a specific group only reads that group's keys.
  encodeTableKey(tableId, DATA_ROW_PREFIX & $groupId & "/" & primaryKey)

proc encodeDataRowScanBound*(tableId: TableId, primaryKey: string): string =
  ## Encode a data row scan bound key WITHOUT group ID.
  ## Format: /t/<tableId>/d/<primaryKey>
  ##
  ## Used only for scan range delimiters (start/end keys) where the groupId
  ## is not included because these bounds span all groups. For actual data
  ## storage keys, use encodeDataRowKey(tableId, groupId, primaryKey).
  ##
  ## For per-group scan bounds, use makeGroupDataRowScanBounds() or
  ## narrowScanBoundsToGroup() instead.
  encodeTableKey(tableId, DATA_ROW_PREFIX & primaryKey)

proc makeGroupDataRowScanBounds*(tableId: TableId,
                                 groupId: GroupID): tuple[
                                     startKey: string, endKey: string] =
  ## Create scan bounds (start, end) for all data rows of a specific group.
  ##
  ## Start key: /t/<tableId>/d/<groupId>/
  ## End key:   /t/<tableId>/d/<groupId>0
  ##
  ## The "0" character is lexicographically before any ULID character
  ## (which uses Crockford base32: 0-9, A-Z excluding I, L, O, U).
  ## Wait — '0' is actually a valid ULID character. We need a character
  ## that sorts AFTER all valid ULID characters. The highest ULID char is
  ## 'Z'. So we append '\xFF' or use a character beyond 'Z'.
  ## Actually, the ULID alphabet is "0123456789ABCDEFGHJKMNPQRSTVWXYZ".
  ## The highest character is 'Z'. Any character after 'Z' in ASCII works.
  ## We use '~' (0x7E) which sorts after 'Z' and is safe in LevelDB keys.
  ##
  ## Actually, the simplest approach: append a character that sorts after
  ## all 26-char ULID strings. Since ULID uses Crockford base32 (chars
  ## 0-9, A-Z excluding I/L/O/U), the max char is 'Z'. We append a
  ## single character that sorts after 'Z' — we use '{' (0x7B).
  ##
  ## But even simpler: the end key just needs to be one past the last
  ## possible key with this groupId prefix. We use the groupId string
  ## plus '{' which sorts after 'Z':
  ##   End: /t/<tableId>/d/<groupId>{  (exclusive bound)
  ##
  ## Since all primary keys start after the "/" following groupId, and
  ## no primary key can contain '{', this correctly bounds the range.
  let startKey = encodeTableKey(tableId, DATA_ROW_PREFIX & $groupId & "/")
  let endKey = encodeTableKey(tableId, DATA_ROW_PREFIX & $groupId & "{")
  result = (startKey: startKey, endKey: endKey)

proc makeDataRowScanEndKey*(tableId: TableId): string =
  ## Create an end key for scanning all data rows of a table (across all groups).
  ## Returns "/t/<tableId>/e/" where "e" > "d" (data prefix).
  ## This scans all keys matching /t/<tableId>/d/... (data rows from all groups).
  encodeTableKey(tableId, "e/")

proc decodeDataRowKey*(key: string): tuple[tableId: TableId,
    groupId: GroupID, primaryKey: string] =
  ## Decode a data row key with group ID back to its components.
  ##
  ## Key format: /t/<tableId>/d/<groupId>/<primaryKey>
  ## Where <groupId> is a 26-character ULID string.
  ##
  ## Raises ValueError if the key does not contain a valid group ID.
  if not key.startsWith(TABLE_KEY_PREFIX):
    raise newException(ValueError, "Not a table key: " & key)

  let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
  if afterPrefix.len < TABLE_ID_WIDTH + 1:
    raise newException(ValueError, "Data row key too short: " & key)

  let tableIdStr = afterPrefix[0 ..< TABLE_ID_WIDTH]
  let tableId = tableIdFromString(tableIdStr)

  # Skip the "/" after tableId
  let rest = afterPrefix[TABLE_ID_WIDTH + 1 .. ^1]

  # Check if this starts with "d/" (data row prefix)
  if not rest.startsWith(DATA_ROW_PREFIX):
    raise newException(ValueError, "Not a data row key: " & key)

  let afterDataPrefix = rest[DATA_ROW_PREFIX.len .. ^1]

  # Parse the group ID (26-char ULID string followed by "/")
  if afterDataPrefix.len < GROUP_ID_WIDTH + 1 or
     afterDataPrefix[GROUP_ID_WIDTH] != '/':
    raise newException(ValueError,
        "Data row key missing group ID: " & key)

  let groupIdStr = afterDataPrefix[0 ..< GROUP_ID_WIDTH]
  let groupId = parseGroupID(groupIdStr)
  let pk = afterDataPrefix[GROUP_ID_WIDTH + 1 .. ^1]
  result = (tableId: tableId, groupId: groupId, primaryKey: pk)

proc primaryKeyFromDataRowKey*(key: string): string =
  ## Extract the primary key portion from a stored data row key.
  ## This strips the /t/<tableId>/d/<groupId>/ prefix, returning only
  ## the binary-encoded primary key bytes. Used by the k-way merge to
  ## compare keys across groups by PK order rather than by groupId.
  ##
  ## Key format: /t/<26 tableId>/d/<26 groupId>/<pk>
  ## PK starts at offset: TABLE_KEY_PREFIX.len + TABLE_ID_WIDTH + 1 + DATA_ROW_PREFIX.len + GROUP_ID_WIDTH + 1
  ##
  ## Returns the full key unchanged if it's not a data row key
  ## (for non-data-table scans like system tables).
  const PK_OFFSET = TABLE_KEY_PREFIX.len + TABLE_ID_WIDTH + 1 +
                     DATA_ROW_PREFIX.len + GROUP_ID_WIDTH + 1
  if key.len >= PK_OFFSET and key.startsWith(TABLE_KEY_PREFIX):
    # Check if the prefix after tableId is "/d/" (data row separator + data prefix)
    # Position: TABLE_KEY_PREFIX.len + TABLE_ID_WIDTH is the "/" before "d/"
    let sepStart = TABLE_KEY_PREFIX.len + TABLE_ID_WIDTH
    if key.len > sepStart + 2 and key[sepStart .. sepStart + 2] == "/d/":
      return key[PK_OFFSET .. ^1]
  # Not a data row key — return full key for default string comparison
  key

proc isDataRowKey*(key: string): bool =
  ## Check if a key is a data row key (has /d/ prefix after tableId).
  ## Returns false for system table keys, index keys, and non-table keys.
  if not key.startsWith(TABLE_KEY_PREFIX):
    return false
  try:
    let (_, primaryKey) = decodeTableKey(key)
    result = primaryKey.startsWith(DATA_ROW_PREFIX)
  except ValueError:
    return false

proc extractGroupIdFromDataRowKey*(key: string): GroupID =
  ## Extract the GroupID from a data row key.
  ## All data row keys must contain a group ID (format: /t/<tableId>/d/<groupId>/<pk>).
  ## Returns ZeroGroupID() for non-data-row keys.
  ## Raises ValueError for data row keys with invalid format.
  if not isDataRowKey(key):
    return ZeroGroupID()
  let (_, groupId, _) = decodeDataRowKey(key)
  groupId

proc addGroupIdToKey*(key: string, groupId: GroupID): string =
  ## Add a group ID to a data row scan-bound key.
  ##
  ## Transforms /t/<tableId>/d/<pk> → /t/<tableId>/d/<groupId>/<pk>.
  ## This is used by the client layer to convert planner-generated scan-bound
  ## keys (without groupId) into the canonical stored key format (with groupId).
  ##
  ## If the key already contains the target groupId, returns it unchanged.
  ## If the key is not a data row key, returns it unchanged.
  ##
  ## To avoid the old false-positive bug (decodeDataRowKey misreading scan-bound
  ## keys whose pk happened to look like a ULID + '/'), we check whether the
  ## embedded prefix matches the *specific* groupId we are about to add.
  ## A collision is impossible in practice because groupIds are 128-bit random
  ## values; a scan-bound pk would need to start with exactly that value.
  if not isDataRowKey(key):
    return key

  try:
    let (tableId, primaryKey) = decodeTableKey(key)
    let groupIdStr = $groupId

    # If the key is already a stored-format key with this groupId,
    # return it unchanged (idempotent).
    let expectedPrefix = DATA_ROW_PREFIX & groupIdStr & "/"
    if primaryKey.startsWith(expectedPrefix):
      return key

    # It's a scan-bound key (no groupId) — strip "d/" and add groupId
    let barePk = if primaryKey.startsWith(DATA_ROW_PREFIX):
                   primaryKey[DATA_ROW_PREFIX.len .. ^1]
                 else:
                   primaryKey
    return encodeDataRowKey(tableId, groupId, barePk)
  except ValueError:
    return key

proc rewriteGroupIdInKey*(key: string, newGroupId: GroupID): string =
  ## Replace the group ID in a stored data row key with a new group ID.
  ##
  ## Handles three key types:
  ##   Plain data key:  /t/<tableId>/d/<oldGid>/<pk>
  ##   Version key:     /t/<tableId>/d/<oldGid>/<pk>\x00\x00<8 bytes timestamp>
  ##   Intent key:      /t/<tableId>/d/<oldGid>/<pk>\x00\x01<16 bytes txnId>
  ##
  ## For all three, the group ID portion is replaced while preserving the
  ## rest of the key (PK, MVCC suffix). If the key is not a data row key
  ## or does not contain a group ID, returns the key unchanged.
  if not key.startsWith(TABLE_KEY_PREFIX):
    return key

  try:
    let (tableId, primaryKey) = decodeTableKey(key)
    if not primaryKey.startsWith(DATA_ROW_PREFIX):
      return key

    let afterDataPrefix = primaryKey[DATA_ROW_PREFIX.len .. ^1]

    # Check if the key already has a group ID (26-char ULID + "/")
    if afterDataPrefix.len < GROUP_ID_WIDTH + 1 or
       afterDataPrefix[GROUP_ID_WIDTH] != '/':
      # No group ID present — this is a scan-bound key, just add
      return addGroupIdToKey(key, newGroupId)

    let rest = afterDataPrefix[GROUP_ID_WIDTH + 1 .. ^1]
    # `rest` is the bare PK, possibly with MVCC suffix (\x00\x00<ts> or \x00\x01<txnId>)

    # Rebuild the key with the new group ID
    let newGroupIdStr = $newGroupId
    let newPrimaryKey = DATA_ROW_PREFIX & newGroupIdStr & "/" & rest
    return encodeTableKey(tableId, newPrimaryKey)
  except ValueError:
    return key

proc narrowScanBoundsToGroup*(startKey, endKey: string,
    tableId: TableId, groupId: GroupID): tuple[startKey: string,
        endKey: string] =
  ## Narrow table-wide scan bounds to a specific group's key range.
  ##
  ## Given a start/end key range for a table, intersect it with the group's
  ## data row prefix range. This enables per-group range scans that avoid
  ## reading data from other groups.
  ##
  ## The planner generates scan bounds WITHOUT the groupId (e.g.
  ## `/t/<tableId>/d/<binaryPk>`). Stored keys INCLUDE the groupId (e.g.
  ## `/t/<tableId>/d/<groupId>/<binaryPk>`). If we intersect the planner
  ## bounds directly with group bounds, binary PK bytes (e.g. \x01 for small
  ## ints) sort BEFORE the ASCII `0` of the ULID groupId, producing
  ## `startKey > endKey` and an empty scan.
  ##
  ## Fix: first convert planner bounds to group-aware format using
  ## `addGroupIdToKey`, THEN intersect with the group range.
  let (groupStart, groupEnd) = makeGroupDataRowScanBounds(tableId, groupId)

  # Convert planner bounds (no groupId) to group-aware stored-key format
  let plannerStart = if startKey.len > 0: addGroupIdToKey(startKey,
      groupId) else: ""
  let plannerEnd = if endKey.len > 0: addGroupIdToKey(endKey, groupId) else: ""

  # Intersect: max of starts, min of ends
  if plannerStart.len > 0 and plannerStart > groupStart:
    result.startKey = plannerStart
  else:
    result.startKey = groupStart

  if plannerEnd.len > 0 and plannerEnd < groupEnd:
    result.endKey = plannerEnd
  else:
    result.endKey = groupEnd

proc encodeIndexKey*(tableId: TableId, groupId: GroupID, indexId: TableId,
                     indexKey: string, primaryKey: string): string =
  ## Encode a secondary index entry key:
  ## /t/<tableId>/i/<groupId>/<indexId>/<indexKey>/<primaryKey>
  ##
  ## The groupId enables per-group index scans.
  encodeTableKey(tableId, INDEX_PREFIX & $groupId & "/" &
                 formatTableId(indexId) & "/" & indexKey & "/" & primaryKey)

proc encodeColumnKey*(tableId: TableId, ordinal: int): string =
  ## Encode a column record key: /t/<SYS_COLUMNS_TABLE_ID>/<tableId>/<ordinal>
  ## This stores column metadata separately from sys.tables for normalisation.
  encodeTableKey(SYS_COLUMNS_TABLE_ID, $(tableId) & "/" & $ordinal)

proc decodeColumnKey*(key: string): tuple[tableId: TableId, ordinal: int] =
  ## Decode a column key back to its components.
  ## Raises ValueError if the key format is invalid.
  if not key.startsWith(TABLE_KEY_PREFIX):
    raise newException(ValueError, "Not a table key: " & key)

  let afterPrefix = key[TABLE_KEY_PREFIX.len .. ^1]
  if afterPrefix.len < TABLE_ID_WIDTH + 1:
    raise newException(ValueError, "Column key too short: " & key)

  let tableIdStr = afterPrefix[0 ..< TABLE_ID_WIDTH]
  let tableId = tableIdFromString(tableIdStr)

  let rest = afterPrefix[TABLE_ID_WIDTH + 1 .. ^1]
  let sepIdx = rest.find('/')
  if sepIdx < 0:
    raise newException(ValueError, "Column key missing ordinal: " & key)

  let tableId2Str = rest[0 ..< sepIdx]
  let tableId2 = tableIdFromString(tableId2Str)
  let ordinalStr = rest[sepIdx + 1 .. ^1]
  let ordinal = parseInt(ordinalStr)

  result = (tableId: tableId2, ordinal: ordinal)

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
