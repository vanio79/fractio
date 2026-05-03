# System Table Binary Schemas for Fractio
#
# Defines typed record structures for all system tables.
# These are serialized using the binary.nim primitives.
#
# All variable-length fields use length-prefixed encoding.
# Multi-field records use a header + trailer pattern for efficiency.

import std/[times, strutils, json]
import fractio/utils/binary
import fractio/core/types
import fractio/distributed/raft/group_types

# =============================================================================
# Constants
# =============================================================================

const
  MAX_NAME_LEN* = 64          # Max length for names (database, schema, table)
  MAX_HOST_LEN* = 64          # Max length for hostnames
  MAX_COLUMN_NAME_LEN* = 32   # Max length for column names
  MAX_COLUMNS_PER_TABLE* = 64 # Reasonable limit for columns

# =============================================================================
# Database Record (sys.databases)
# =============================================================================

type
  DatabaseRecord* = object
    ## Record stored in SYS_DATABASES_TABLE
    ## Key: /t/0000000001/<dbName>
    name*: string # Database name (length-prefixed)
    createdAtNs*: int64 # Unix nanoseconds

proc encode*(rec: DatabaseRecord): string =
  ## Encode a DatabaseRecord to binary
  var w = initBinaryWriter()
  w.writeString(rec.name)
  w.writeI64(rec.createdAtNs)
  w.finish()

proc decodeDatabaseRecord*(data: string): DatabaseRecord =
  ## Decode binary data to a DatabaseRecord
  var r = initBinaryReader(data)
  result.name = r.readString()
  result.createdAtNs = r.readI64()

# =============================================================================
# Schema Record (sys.schemas)
# =============================================================================

type
  SchemaRecord* = object
    ## Record stored in SYS_SCHEMAS_TABLE
    ## Key: /t/0000000002/<databaseName>.<schemaName>
    name*: string # Schema name
    database*: string # Parent database name
    createdAtNs*: int64 # Unix nanoseconds

proc encode*(rec: SchemaRecord): string =
  var w = initBinaryWriter()
  w.writeString(rec.name)
  w.writeString(rec.database)
  w.writeI64(rec.createdAtNs)
  w.finish()

proc decodeSchemaRecord*(data: string): SchemaRecord =
  var r = initBinaryReader(data)
  result.name = r.readString()
  result.database = r.readString()
  result.createdAtNs = r.readI64()

# =============================================================================
# Column Definition (for TableRecord)
# =============================================================================

type
  ColumnDataType* = enum
    cdtInt = 0
    cdtFloat = 1
    cdtString = 2
    cdtBool = 3
    cdtBytes = 4
    cdtDate = 5
    cdtDateTime = 6
    cdtULID = 7

  ColumnFlags* = enum
    cfPrimaryKey
    cfNotNull
    cfUnique

  ColumnDefBin* = object
    ## Binary-encoded column definition
    name*: string
    dataType*: ColumnDataType
    maxLen*: uint16 ## Max length for VARCHAR/bytes types (used for PK encoding)
    flags*: uint8   # Bitfield: bit 0 = primaryKey, bit 1 = notNull, bit 2 = unique

proc encodeColumnDef*(col: ColumnDefBin, w: var BinaryWriter) =
  w.writeString(col.name)
  w.writeU8(uint8(col.dataType))
  w.writeU16(col.maxLen)
  w.writeU8(col.flags)

proc decodeColumnDef*(r: var BinaryReader): ColumnDefBin =
  result.name = r.readString()
  result.dataType = ColumnDataType(r.readU8())
  result.maxLen = r.readU16()
  result.flags = r.readU8()

# =============================================================================
# Table Record (sys.tables)
# =============================================================================

type
  TableRecord* = object
    ## Record stored in SYS_TABLES_TABLE
    ## Key: /t/<SYS_TABLES_TABLE_ID>/<database>.<schema>.<tableName>
    tableId*: TableId ## ULID-based table ID for globally unique, sortable IDs
    name*: string
    schema*: string
    database*: string
    spaceId*: SpaceID ## Space this table belongs to
    primaryKey*: seq[string] ## Column names forming the primary key
    columns*: seq[ColumnDefBin]

proc encode*(rec: TableRecord): string =
  var w = initBinaryWriter()
  w.writeBytes(tableIdToBytes(rec.tableId))
  w.writeString(rec.name)
  w.writeString(rec.schema)
  w.writeString(rec.database)
  w.writeBytes(spaceIDToBytes(rec.spaceId))
  # Primary key columns
  w.writeU32(uint32(rec.primaryKey.len))
  for pk in rec.primaryKey:
    w.writeString(pk)
  # Columns
  w.writeU32(uint32(rec.columns.len))
  for col in rec.columns:
    encodeColumnDef(col, w)
  w.finish()

proc decodeTableRecord*(data: string): TableRecord =
  var r = initBinaryReader(data)
  result.tableId = tableIdFromBytes(r.readFixedString(ULID_SIZE))
  result.name = r.readString()
  result.schema = r.readString()
  result.database = r.readString()
  result.spaceId = spaceIDFromBytes(r.readFixedString(ULID_SIZE))
  # Primary key columns
  let pkCount = int(r.readU32())
  result.primaryKey = newSeq[string](pkCount)
  for i in 0..<pkCount:
    result.primaryKey[i] = r.readString()
  # Columns
  let colCount = int(r.readU32())
  result.columns = newSeq[ColumnDefBin](colCount)
  for i in 0..<colCount:
    result.columns[i] = decodeColumnDef(r)

# =============================================================================
# Group Replica (for GroupRecord)
# =============================================================================

type
  ReplicaType* = enum
    rtVoter = 0
    rtLearner = 1

  GroupReplicaBin* = object
    nodeId*: uint32
    replicaType*: ReplicaType

proc encodeGroupReplica*(rep: GroupReplicaBin, w: var BinaryWriter) =
  w.writeU32(rep.nodeId)
  w.writeU8(uint8(rep.replicaType))

proc decodeGroupReplica*(r: var BinaryReader): GroupReplicaBin =
  result.nodeId = r.readU32()
  let typeByte = r.readU8()
  # Handle both our enum values and NuRaft's srv_config flags
  # Our values: rtVoter=0, rtLearner=1
  # NuRaft flags: LEARNER_FLAG=0x1, NEW_JOINER_FLAG=0x2
  # Any unknown value defaults to rtVoter for safety
  if typeByte <= 1:
    result.replicaType = ReplicaType(typeByte)
  else:
    # Unknown value - default to voter (most common case)
    # This handles corrupted data or version mismatches gracefully
    result.replicaType = rtVoter

# =============================================================================
# Group Record (sys.groups)
# =============================================================================

type
  GroupRecord* = object
    ## Record stored in SYS_GROUPS_TABLE
    ## Key: /t/0000000004/<groupId>
    groupId*: ULID
    spaceId*: SpaceID
    preferredLeader*: uint32
    leader*: uint32 # Current leader (0 = unknown)
    replicas*: seq[GroupReplicaBin]

proc encode*(rec: GroupRecord): string =
  var w = initBinaryWriter()
  w.writeBytes(ulidToBytes(rec.groupId))
  w.writeBytes(spaceIDToBytes(rec.spaceId))
  w.writeU32(rec.preferredLeader)
  w.writeU32(rec.leader)
  # Replicas
  w.writeU32(uint32(rec.replicas.len))
  for rep in rec.replicas:
    encodeGroupReplica(rep, w)
  w.finish()

proc decodeGroupRecord*(data: string): GroupRecord =
  var r = initBinaryReader(data)
  result.groupId = ulidFromBytes(r.readFixedString(ULID_SIZE))
  result.spaceId = spaceIDFromBytes(r.readFixedString(ULID_SIZE))
  result.preferredLeader = r.readU32()
  result.leader = r.readU32()
  # Replicas
  let repCount = int(r.readU32())
  result.replicas = newSeq[GroupReplicaBin](repCount)
  for i in 0..<repCount:
    result.replicas[i] = decodeGroupReplica(r)

# =============================================================================
# Node Record (sys.nodes)
# =============================================================================

type
  NodeStatus* = enum
    nsUnknown = 0
    nsAlive = 1
    nsDraining = 2
    nsDecommissioned = 3

  NodeRecord* = object
    ## Record stored in SYS_NODES_TABLE
    ## Key: /t/0000000005/<nodeId>
    nodeId*: uint32
    host*: string
    raftPort*: uint16
    clientPort*: uint16
    webPort*: uint16
    status*: NodeStatus

proc encode*(rec: NodeRecord): string =
  var w = initBinaryWriter()
  w.writeU32(rec.nodeId)
  w.writeString(rec.host)
  w.writeU16(rec.raftPort)
  w.writeU16(rec.clientPort)
  w.writeU16(rec.webPort)
  w.writeU8(uint8(rec.status))
  w.finish()

proc decodeNodeRecord*(data: string): NodeRecord =
  var r = initBinaryReader(data)
  result.nodeId = r.readU32()
  result.host = r.readString()
  result.raftPort = r.readU16()
  result.clientPort = r.readU16()
  result.webPort = r.readU16()
  result.status = NodeStatus(r.readU8())

proc stripMVCCHeader*(data: string): tuple[payload: string, isDeleted: bool] =
  ## Internal helper to strip MVCC header from a value.
  ## MVCC format: <MAGIC (4 bytes)><8 bytes timestamp><16 bytes txn_id ULID><1 byte delete flag><payload>

  const MVCC_HEADER_SIZE = 29 # 4 (magic) + 8 (ts) + 16 (txn ULID) + 1 (del)
  const MVCC_MAGIC = "MVCC"

  if data.len >= MVCC_HEADER_SIZE and data.startsWith(MVCC_MAGIC):
    let isDeleted = data[28] == '1'
    return (data[MVCC_HEADER_SIZE..^1], isDeleted)

  return (data, false)


proc decodeNodeRecordFromMVCC*(data: string): tuple[record: NodeRecord,
    isDeleted: bool] =
  let (payload, isDeleted) = stripMVCCHeader(data)
  if isDeleted: return (NodeRecord(), true)
  if payload.len < 10: return (NodeRecord(), false)
  result.record = decodeNodeRecord(payload)
  result.isDeleted = false

# =============================================================================
# Space Record (sys.spaces)
# =============================================================================

type
  SpaceRecord* = object
    ## Record stored in SYS_SPACES_TABLE
    ## Key: /t/0000000007/<spaceId>
    spaceId*: SpaceID
    name*: string
    replicas*: int32 # 0 = ALL nodes
    groupCount*: int32
    groupIds*: seq[GroupID]
    oldGroupIds*: seq[GroupID] # Used during rebalancing
    rebalancing*: bool
    rebalanceWorker*: int32 # nodeId of the migrating worker
    rebalanceHeartbeat*: int64 # unix epoch seconds of last worker heartbeat
    rebalanceCursor*: string # last key migrated (resume point)
    createdAtNs*: int64

proc encode*(rec: SpaceRecord): string =
  var w = initBinaryWriter()
  w.writeBytes(spaceIDToBytes(rec.spaceId))
  w.writeString(rec.name)
  w.writeI32(rec.replicas)
  w.writeI32(rec.groupCount)
  # groupIds
  w.writeU32(uint32(rec.groupIds.len))
  for gid in rec.groupIds:
    w.writeBytes(groupIDToBytes(gid))
  # oldGroupIds
  w.writeU32(uint32(rec.oldGroupIds.len))
  for gid in rec.oldGroupIds:
    w.writeBytes(groupIDToBytes(gid))
  # flags
  var flags: uint8 = 0
  if rec.rebalancing:
    flags = flags or 0x01
  w.writeU8(flags)
  # rebalance tracking
  w.writeI32(rec.rebalanceWorker)
  w.writeI64(rec.rebalanceHeartbeat)
  w.writeString(rec.rebalanceCursor)
  w.writeI64(rec.createdAtNs)
  w.finish()

proc decodeSpaceRecord*(data: string): SpaceRecord =
  var r = initBinaryReader(data)
  result.spaceId = spaceIDFromBytes(r.readFixedString(ULID_SIZE))
  result.name = r.readString()
  result.replicas = r.readI32()
  result.groupCount = r.readI32()
  # groupIds
  let gidCount = int(r.readU32())
  result.groupIds = newSeq[GroupID](gidCount)
  for i in 0..<gidCount:
    result.groupIds[i] = groupIDFromBytes(r.readFixedString(ULID_SIZE))
  # oldGroupIds
  let oldGidCount = int(r.readU32())
  result.oldGroupIds = newSeq[GroupID](oldGidCount)
  for i in 0..<oldGidCount:
    result.oldGroupIds[i] = groupIDFromBytes(r.readFixedString(ULID_SIZE))
  # flags
  let flags = r.readU8()
  result.rebalancing = (flags and 0x01) != 0
  # rebalance tracking
  result.rebalanceWorker = r.readI32()
  result.rebalanceHeartbeat = r.readI64()
  result.rebalanceCursor = r.readString()
  result.createdAtNs = r.readI64()

# =============================================================================
# Setting Record (sys.settings)
# =============================================================================

type
  SettingRecord* = object
    ## Record stored in SYS_SETTINGS_TABLE
    ## Key: /t/0000000006/<settingKey>
    value*: string

proc encode*(rec: SettingRecord): string =
  var w = initBinaryWriter()
  w.writeString(rec.value)
  w.finish()

proc decodeSettingRecord*(data: string): SettingRecord =
  var r = initBinaryReader(data)
  result.value = r.readString()

# =============================================================================
# Helper: Current timestamp
# =============================================================================

proc nowNs*(): int64 =
  ## Get current time as Unix nanoseconds
  let t = getTime()
  let secs = t.toUnix()
  let nanos = t.nanosecond()
  result = secs * 1_000_000_000'i64 + nanos

# =============================================================================
# Conversion helpers: Binary -> JSON (for dashboard API)
# =============================================================================

import std/json

proc toJson*(rec: DatabaseRecord): JsonNode =
  result = %*{
    "name": rec.name,
    "createdAt": $fromUnix(rec.createdAtNs div 1_000_000_000)
  }

proc toJson*(rec: SchemaRecord): JsonNode =
  result = %*{
    "name": rec.name,
    "database": rec.database,
    "createdAt": $fromUnix(rec.createdAtNs div 1_000_000_000)
  }

proc toJson*(rec: TableRecord): JsonNode =
  var columns = newJArray()
  for col in rec.columns:
    var dt: string
    case col.dataType
    of cdtInt: dt = "INT"
    of cdtFloat: dt = "FLOAT"
    of cdtString: dt = "TEXT"
    of cdtBool: dt = "BOOL"
    of cdtBytes: dt = "BLOB"
    of cdtDate: dt = "DATE"
    of cdtDateTime: dt = "DATETIME"
    of cdtULID: dt = "ULID"
    columns.add(%*{
      "name": col.name,
      "type": dt,
      "primaryKey": (col.flags and 0x01) != 0,
      "notNull": (col.flags and 0x02) != 0
    })
  var pkArr = newJArray()
  for pk in rec.primaryKey:
    pkArr.add(%pk)
  result = %*{
    "tableId": $(rec.tableId),
    "name": rec.name,
    "schema": rec.schema,
    "database": rec.database,
    "spaceId": $(rec.spaceId),
    "primaryKey": pkArr,
    "columns": columns
  }

proc toJson*(rec: GroupRecord): JsonNode =
  var replicas = newJArray()
  for rep in rec.replicas:
    replicas.add(%*{
      "nodeId": rep.nodeId,
      "type": if rep.replicaType == rtVoter: "voter" else: "learner"
    })
  result = %*{
    "groupId": $(rec.groupId),
    "spaceId": $(rec.spaceId),
    "preferredLeader": rec.preferredLeader,
    "leader": rec.leader,
    "replicas": replicas
  }

proc toJson*(rec: NodeRecord): JsonNode =
  var status: string
  case rec.status
  of nsUnknown: status = "unknown"
  of nsAlive: status = "alive"
  of nsDraining: status = "draining"
  of nsDecommissioned: status = "decommissioned"
  result = %*{
    "nodeId": rec.nodeId,
    "host": rec.host,
    "raftPort": rec.raftPort,
    "clientPort": rec.clientPort,
    "status": status
  }

proc toJson*(rec: SettingRecord): JsonNode =
  result = %*{
    "value": rec.value
  }

proc toJson*(rec: SpaceRecord): JsonNode =
  var groupIds = newJArray()
  for gid in rec.groupIds:
    groupIds.add(%($gid))
  var oldGroupIds = newJArray()
  for gid in rec.oldGroupIds:
    oldGroupIds.add(%($gid))
  result = %*{
    "spaceId": $(rec.spaceId),
    "name": rec.name,
    "replicas": rec.replicas,
    "groupCount": rec.groupCount,
    "groupIds": groupIds,
    "oldGroupIds": oldGroupIds,
    "rebalancing": rec.rebalancing,
    "rebalanceWorker": rec.rebalanceWorker,
    "rebalanceHeartbeat": rec.rebalanceHeartbeat,
    "rebalanceCursor": rec.rebalanceCursor
  }

# =============================================================================
# MVCC-aware Decoders
# =============================================================================


proc decodeGroupRecordFromMVCC*(data: string): tuple[record: GroupRecord,
    isDeleted: bool] =
  let (payload, isDeleted) = stripMVCCHeader(data)
  if isDeleted: return (GroupRecord(), true)
  if payload.len < 8: return (GroupRecord(), false)
  try:
    return (decodeGroupRecord(payload), false)
  except CatchableError as e:
    raise e

proc decodeTableRecordFromMVCC*(data: string): tuple[record: TableRecord,
    isDeleted: bool] =
  let (payload, isDeleted) = stripMVCCHeader(data)
  if isDeleted: return (TableRecord(), true)
  if payload.len < 4: return (TableRecord(), false)
  try:
    return (decodeTableRecord(payload), false)
  except CatchableError as e:
    raise e

proc decodeDatabaseRecordFromMVCC*(data: string): tuple[record: DatabaseRecord,
    isDeleted: bool] =
  let (payload, isDeleted) = stripMVCCHeader(data)
  if isDeleted: return (DatabaseRecord(), true)
  try:
    return (decodeDatabaseRecord(payload), false)
  except CatchableError as e:
    raise e

proc decodeSpaceRecordFromMVCC*(data: string): tuple[record: SpaceRecord,
    isDeleted: bool] =
  let (payload, isDeleted) = stripMVCCHeader(data)
  if isDeleted: return (SpaceRecord(), true)
  try:
    return (decodeSpaceRecord(payload), false)
  except CatchableError as e:
    raise e

proc decodeSchemaRecordFromMVCC*(data: string): tuple[record: SchemaRecord,
    isDeleted: bool] =
  let (payload, isDeleted) = stripMVCCHeader(data)
  if isDeleted: return (SchemaRecord(), true)
  if payload.len < 4: return (SchemaRecord(), false)
  try:
    return (decodeSchemaRecord(payload), false)
  except CatchableError as e:
    raise e
