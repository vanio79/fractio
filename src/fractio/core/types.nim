# Core types for Fractio distributed database
# Thread-safe by design - immutable structs and atomic operations

import tables
import sets
import times

type
  # Basic data types
  DataType* = enum
    dtInt, dtFloat, dtString, dtBool, dtDate, dtDateTime, dtBytes

  Timestamp* = int64
    ## Nanosecond-precision Unix timestamp

  Constraint* = object
    nullable*: bool
    unique*: bool
    primaryKey*: bool
    defaultValue*: ValueRef

  ValueRef* = ref object
    case kind*: DataType
    of dtInt:
      intValue*: int64
    of dtFloat:
      floatValue*: float64
    of dtString:
      strValue*: string
    of dtBool:
      boolValue*: bool
    of dtDate:
      dateValue*: int64
    of dtDateTime:
      datetimeValue*: int64
    of dtBytes:
      bytesValue*: seq[uint8]

  ColumnDef* = object
    name*: string
    dataType*: DataType
    constraints*: Constraint
    isShardKey*: bool

  RowID* = distinct int64

  Row* = ref object
    id*: RowID
    values*: seq[ValueRef]
    createdAt*: int64
    updatedAt*: int64
    version*: int64

  Table* = ref object
    name*: string
    columns*: seq[ColumnDef]
    rows*: seq[Row]
    indexes*: TableIndexes
    mutex*: pointer
    version*: int64

  TableIndexes* = object
    columnIndices*: tables.Table[string, int]

  Schema* = ref object
    tables*: tables.Table[string, Table]
    mutex*: pointer
    version*: int64

  TransactionID* = distinct int64

  Transaction* = ref object
    id*: TransactionID
    timestamp*: int64
    status*: TransactionStatus
    readSnapshot*: int64
    mutatedTables*: HashSet[string]
    mutex*: pointer

  TransactionStatus* = enum
    tsActive, tsCommitted, tsAborted

  ShardID* = distinct int64

  Shard* = ref object
    id*: ShardID
    rangeStart*: uint64
    rangeEnd*: uint64
    replicas*: seq[ReplicaInfo]
    primaryReplica*: int
    table*: string

  ReplicaInfo* = object
    nodeId*: string
    address*: string
    port*: uint16
    lastSeen*: int64

  NodeID* = distinct string

  NodeInfo* = object
    id*: NodeID
    address*: string
    port*: uint16
    role*: NodeRole
    capacity*: int
    used*: int
    load*: int

  NodeRole* = enum
    nrCoordinator, nrPrimary, nrSecondary, nrClient

# Helper templates for safe type conversions
template int64Value*(v: ValueRef): int64 =
  case v.kind
  of dtInt: v.intValue
  else: 0

template float64Value*(v: ValueRef): float64 =
  case v.kind
  of dtFloat: v.floatValue
  else: 0.0

template stringValue*(v: ValueRef): string =
  case v.kind
  of dtString: v.strValue
  else: ""

template boolValue*(v: ValueRef): bool =
  case v.kind
  of dtBool: v.boolValue
  else: false

# Constructors for ValueRef
proc newValueRef*(i: int64): ValueRef =
  result = ValueRef(kind: dtInt, intValue: i)

proc newValueRef*(f: float64): ValueRef =
  result = ValueRef(kind: dtFloat, floatValue: f)

proc newValueRef*(s: string): ValueRef =
  result = ValueRef(kind: dtString, strValue: s)

proc newValueRef*(b: bool): ValueRef =
  result = ValueRef(kind: dtBool, boolValue: b)

proc newValueRef*(bytes: seq[uint8]): ValueRef =
  result = ValueRef(kind: dtBytes, bytesValue: bytes)

proc newRow*(id: RowID = RowID(0)): Row =
  Row(id: id, values: @[], createdAt: getTime().toUnix * 1000,
       updatedAt: getTime().toUnix * 1000, version: 1)

proc `==`*(a, b: TransactionID): bool = a.int64 == b.int64
proc `!=`*(a, b: TransactionID): bool = not (a == b)

# Transaction ID generation (legacy - will be replaced by P2PTimeSynchronizer)
# Row ID generation
proc genRowID*(): RowID =
  RowID(getTime().toUnix * 1000000 + (getTime().toUnix*1000000 mod 1000000).int64)
