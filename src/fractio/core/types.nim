# Core types for Fractio distributed database
# Thread-safe by design - immutable structs and atomic operations

type
  DataType* = enum
    dtInt, dtFloat, dtString, dtBool, dtDate, dtDateTime, dtBytes

  Constraint* = object
    nullable*: bool
    unique*: bool
    primaryKey*: bool
    defaultValue*: ValueRef # stores a default value

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
      dateValue*: int64     # Unix timestamp in milliseconds
    of dtDateTime:
      datetimeValue*: int64 # Unix timestamp in nanoseconds
    of dtBytes:
      bytesValue*: seq[uint8]

  ColumnDef* = object
    name*: string
    dataType*: DataType
    constraints*: Constraint
    # For sharding: columns that participate in shard key
    isShardKey*: bool

  RowID* = distinct int64

  Row* = ref object
    id*: RowID
    values*: seq[ValueRef]
    createdAt*: int64 # timestamp
    updatedAt*: int64 # timestamp
    version*: int64   # MVCC version

  Table* = ref object
    name*: string
    columns*: seq[ColumnDef]
    rows*: seq[Row]
    indexes*: TableIndexes
    mutex*: pointer # Thread-safe mutation lock (pthread_mutex_t in C)
    version*: int64 # Schema version for compatibility checks

  TableIndexes* = object
    # Maps column name to index of values in rows
    columnIndices*: Table[string, int]

  Schema* = ref object
    tables*: Table[string, Table]
    mutex*: pointer # Schema-level lock
    version*: int64 # Global schema version

  TransactionID* = distinct int64

  Transaction* = ref object
    id*: TransactionID
    timestamp*: int64    # MVCC timestamp
    status*: TransactionStatus
    readSnapshot*: int64 # Snapshot for snapshot isolation
    mutatedTables*: HashSet[string]
    mutex*: pointer      # Transaction-local lock

  TransactionStatus* = enum
    tsActive, tsCommitted, tsAborted

  ShardID* = distinct int64

  Shard* = ref object
    id*: ShardID
    rangeStart*: uint64         # Inclusive start of shard key range
    rangeEnd*: uint64           # Exclusive end of shard key range
    replicas*: seq[ReplicaInfo] # Primary + secondary replicas
    primaryReplica*: int        # Index into replicas array
    table*: string              # Table this shard belongs to

  ReplicaInfo* = object
    nodeId*: string
    address*: string
    port*: uint16
    lastSeen*: int64 # Timestamp of last heartbeat

  NodeID* = distinct string

  NodeInfo* = object
    id*: NodeID
    address*: string
    port*: uint16
    role*: NodeRole # Primary, secondary, or coordinator
    capacity*: int  # Total storage capacity in bytes
    used*: int      # Used storage in bytes
    load*: int      # Current load (active connections)

  NodeRole* = enum
    nrCoordinator, nrPrimary, nrSecondary, nrClient

  Query* = ref object
    sql*: string
    ast*: SQLElement # Parsed AST
    params*: seq[ValueRef]
    executionPlan*: ExecutionPlan

  SQLElement* = ref object
    case kind*: SQLNodeKind
    of sqlnSelect:
      selectColumns*: seq[ColumnRef]
      fromTable*: string
      whereClause*: ConditionRef
      joinClauses*: seq[JoinClause]
    of sqlnInsert:
      insertTable*: string
      insertColumns*: seq[string]
      insertValues*: seq[ValueRef]
    of sqlnUpdate:
      updateTable*: string
      setClauses*: seq[SetClause]
      whereClause*: ConditionRef
    of sqlnDelete:
      deleteTable*: string
      whereClause*: ConditionRef
    of sqlnCreateTable:
      createTable*: string
      createColumns*: seq[ColumnDef]
    of sqlnDropTable:
      dropTable*: string

  ColumnRef* = object
    table*: string
    column*: string

  ConditionRef* = ref object
    left*: ConditionOperand
    op*: ConditionOp
    right*: ConditionOperand
    # For composite conditions
    next*: ConditionRef
    combineOp*: LogicOp # AND or OR

  ConditionOperand* = ref object
    case kind*: OperandKind
    of opkColumn:
      columnName*: string
    of opkValue:
      value*: ValueRef
    of opkSubquery:
      subquery*: Query

  ConditionOp* = enum
    copEqual, copNotEqual, copGreater, copLess, copGreaterEq, copLessEq,
      copLike, copIn

  LogicOp* = enum
    lopAnd, lopOr

  SetClause* = object
    column*: string
    value*: ValueRef

  JoinClause* = object
    table*: string
    condition*: ConditionRef
    joinType*: JoinType # Inner, Left, Right, Full

  JoinType* = enum
    jtInner, jtLeft, jtRight, jtFull

  ExecutionPlan* = ref object
    steps*: seq[ExecutionStep]
    estimatedCost*: float64

  ExecutionStep* = ref object
    case kind*: StepKind
    of stepScan:
      scanTable*: string
      scanShard*: Option[ShardID]
      scanIndex*: Option[string]
    of stepFilter:
      filterCondition*: ConditionRef
    of stepProject:
      projectColumns*: seq[ColumnRef]
    of stepJoin:
      joinTable*: string
      joinCondition*: ConditionRef
      joinType*: JoinType
    of stepAggregate:
      aggregateFunc*: AggregateFunc
      groupBy*: seq[ColumnRef]

  StepKind* = enum
    stepScan, stepFilter, stepProject, stepJoin, stepAggregate

  AggregateFunc* = enum
    afCount, afSum, afAvg, afMin, afMax

  SQLNodeKind* = enum
    sqlnSelect, sqlnInsert, sqlnUpdate, sqlnDelete, sqlnCreateTable, sqlnDropTable

  OpResult* = ref object
    case kind*: ResultKind
    of rkSuccess:
      rows*: seq[Row]
      rowCount*: int
      lastInsertId*: RowID
    of rkError:
      errorMessage*: string
      errorCode*: int

  ResultKind* = enum
    rkSuccess, rkError

  # Global timestamp provider for MVCC
  TimestampProvider* = object
    lastTimestamp*: atomic[int64]
    nodeId*: NodeID

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

# Transaction ID generation
proc genTransactionID*(): TransactionID =
  var tp: TimestampProvider
  let ts = atomicInc(tp.lastTimestamp)
  TransactionID(ts)

# Row ID generation (could be from distributed ID generator)
proc genRowID*(): RowID =
  RowID(getTime().toUnix * 1000000 + (getTime().toUnix*1000000 mod 1000000).int64)
