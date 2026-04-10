# Dependency Injection Interfaces for Fractio
# Using Nim concepts for duck-typed interfaces (compile-time polymorphism)
# Thread-safe by design - immutable or atomic operations

import std/[options, tables]
import fractio/core/types
import fractio/core/errors

# Import GroupID from distributed layer (NodeID is in core/types)
from fractio/distributed/raft/group_types import GroupID

export options, tables

# =============================================================================
# Log Level (shared across all loggers)
# =============================================================================

type
  LogLevel* = enum
    llDebug, llInfo, llWarn, llError

# =============================================================================
# Service Lifecycle
# =============================================================================

type
  ServiceLifecycle* = enum
    ## Service instantiation strategy
    slSingleton ## One instance for application lifetime
    slScoped    ## One instance per scope (request, transaction)
    slTransient ## New instance every resolution

# =============================================================================
# Execution Result (for SQL executor)
# =============================================================================
# Note: This uses a simplified representation for mock testing.
# The real executor uses ExecResult with seq[seq[string]] rows.

type
  ExecutionResultKind* = enum
    erkRows, erkModified, erkEmpty, erkError

  ExecutionResult* = object
    kind*: ExecutionResultKind
    rows*: seq[seq[string]] # Each row is column values as strings
    count*: int64
    error*: Option[FractioError]

# =============================================================================
# Error Types
# =============================================================================

type
  KVStoreError* = object of FractioError
  TransactionManagerError* = object of FractioError
  BackendError* = object of FractioError
  ConnectionError* = object of FractioError
  SqlExecutorError* = object of FractioError

# =============================================================================
# Concepts (Compile-time Interface Constraints)
# =============================================================================
# These concepts are used for compile-time type checking.
# They cannot be stored as concrete types - use refs for runtime storage.

type
  TimeProviderConcept* = concept tp
    ## Provides timestamps for deterministic testing
    ## This matches the DI mock interface (nowNs, nowUs, nowMs)
    proc nowNs*(tp: tp): int64
    proc nowUs*(tp: tp): int64
    proc nowMs*(tp: tp): int64
    proc advance*(tp: tp, deltaNs: int64)

  SharedTimerTimeProviderConcept* = concept tp
    ## Matches sharedtimer.TimeProvider interface (now() method)
    proc now*(tp: tp): int64

  TimestampProviderConcept* = concept tp
    ## Matches core/timestamp_provider.TimestampProvider interface
    proc now*(tp: tp): int64
    proc acquireStartTimestamp*(tp: tp): int64
    proc acquireCommitTimestamp*(tp: tp, minTimestamp: int64): int64

  LogProviderConcept* = concept lp
    ## Pluggable logging interface
    proc log*(lp: lp, level: LogLevel, msg: string, fields: Table[string, string])
    proc debug*(lp: lp, msg: string, fields: Table[string, string])
    proc info*(lp: lp, msg: string, fields: Table[string, string])
    proc warn*(lp: lp, msg: string, fields: Table[string, string])
    proc error*(lp: lp, msg: string, fields: Table[string, string])
    proc setMinLevel*(lp: lp, level: LogLevel)
    proc shouldLog*(lp: lp, level: LogLevel): bool

  KVStoreConcept* = concept ks
    ## Key-value store abstraction
    proc get*(ks: ks, key: string): Option[string]
    proc put*(ks: ks, key: string, value: string): bool
    proc delete*(ks: ks, key: string): bool
    proc scan*(ks: ks, prefix: string, limit: uint32): seq[(string, string)]
    proc close*(ks: ks)
    proc exists*(ks: ks, key: string): bool

  TransactionManagerConcept* = concept tm
    ## Transaction management interface
    proc begin*(tm: tm): TransactionID
    proc commit*(tm: tm, txnId: TransactionID): bool
    proc rollback*(tm: tm, txnId: TransactionID): bool
    proc getStatus*(tm: tm, txnId: TransactionID): TransactionStatus
    proc getActiveCount*(tm: tm): int
    proc getOldestSnapshot*(tm: tm): int64

  StorageBackendConcept* = concept b
    ## Matches storage/backend.StorageBackend interface
    proc open*(b: b, config: RootRef): bool
    proc close*(b: b)
    proc isOpen*(b: b): bool
    proc put*(b: b, key: string, value: string): bool
    proc get*(b: b, key: string): Option[string]
    proc delete*(b: b, key: string): bool
    proc exists*(b: b, key: string): bool
    proc writeBatch*(b: b, pairs: seq[tuple[key: string, value: string]],
        deletes: seq[string]): bool
    proc flush*(b: b): bool
    proc destroy*(b: b): bool

  BackendConcept* = concept b
    ## Simplified storage backend abstraction for testing
    proc get*(b: b, key: string): Option[string]
    proc put*(b: b, key: string, value: string): bool
    proc delete*(b: b, key: string): bool
    proc scan*(b: b, prefix: string, limit: uint32): seq[(string, string)]
    proc flush*(b: b): bool
    proc compact*(b: b): bool
    proc close*(b: b)
    proc stats*(b: b): Table[string, int64]

  ConnectionHandleConcept* = concept ch
    ## Connection handle abstraction
    proc send*(ch: ch, data: seq[uint8]): bool
    proc recv*(ch: ch, maxSize: int): seq[uint8]
    proc close*(ch: ch): bool
    proc isConnected*(ch: ch): bool
    proc remoteAddress*(ch: ch): string

  ConnectionManagerConcept* = concept cm
    ## Connection pool manager interface
    proc acquire*(cm: cm, address: string, port: uint16): RootRef
    proc release*(cm: cm, conn: RootRef)
    proc closeAll*(cm: cm)
    proc poolSize*(cm: cm): int
    proc activeCount*(cm: cm): int

  # ==========================================================================
  # Distributed Layer Concepts (Phase 4)
  # ==========================================================================

  RaftCoordinatorConcept* = concept rc
    ## Raft coordinator interface for managing multiple Raft groups
    ## Matches NuRaftCoordinator interface
    proc start*(rc: rc)
    proc stop*(rc: rc)
    proc hasGroup*(rc: rc, groupId: GroupID): bool
    proc getLeader*(rc: rc, groupId: GroupID): int32
    proc isLeader*(rc: rc, groupId: GroupID): bool
    proc isRunning*(rc: rc): bool

  RaftTransportConcept* = concept rt
    ## Raft transport abstraction for sending/receiving Raft messages
    proc send*(rt: rt, targetNodeId: NodeID, data: seq[uint8]): bool
    proc startServer*(rt: rt)
    proc stopServer*(rt: rt)
    proc isServerRunning*(rt: rt): bool

  RaftStateMachineConcept* = concept sm
    ## Raft state machine interface
    proc apply*(sm: sm, data: seq[uint8]): bool
    proc getLastAppliedIndex*(sm: sm): int64
    proc snapshot*(sm: sm): seq[uint8]

  RaftLogConcept* = concept rl
    ## Raft log storage interface
    proc append*(rl: rl, term: int64, data: seq[uint8]): int64
    proc get*(rl: rl, index: int64): Option[seq[uint8]]
    proc truncate*(rl: rl, index: int64): bool
    proc getLastIndex*(rl: rl): int64
    proc getLastTerm*(rl: rl): int64

  SpaceManagerConcept* = concept sm
    ## Space management interface for distributed tables
    proc createSpace*(sm: sm, spaceName: string): GroupID
    proc dropSpace*(sm: sm, spaceId: GroupID): bool
    proc getSpaceInfo*(sm: sm, spaceId: GroupID): Option[RootRef]
    proc listSpaces*(sm: sm): seq[GroupID]

  NetworkTransportConcept* = concept nt
    ## Network transport interface for TCP/UDP communication
    proc connect*(nt: nt, host: string, port: uint16): bool
    proc disconnect*(nt: nt)
    proc isConnected*(nt: nt): bool
    proc send*(nt: nt, data: seq[uint8]): bool
    proc recv*(nt: nt, timeoutMs: int): Option[seq[uint8]]

  # ==========================================================================
  # Protocol Layer Concepts (Phase 5)
  # ==========================================================================

  ProtocolServerConcept* = concept ps
    ## Protocol server interface for testing server behavior
    ## Matches key lifecycle methods from ProtocolServer
    ## Note: registerHandler uses a complex proc signature that is difficult
    ## to express in Nim concepts; it's tracked separately via registerHandlerCallCount
    proc start*(ps: ps)
    proc stop*(ps: ps)
    proc isRunning*(ps: ps): bool
    proc clientCount*(ps: ps): int

  ProtocolClientConcept* = concept pc
    ## Protocol client interface for testing client behavior
    ## Matches key methods from ProtocolClient
    proc connect*(pc: pc): bool
    proc disconnect*(pc: pc)
    proc isConnected*(pc: pc): bool
    proc ping*(pc: pc): bool
    proc kvGet*(pc: pc, key: string): Option[string]
    proc kvPut*(pc: pc, key: string, value: string): bool

  # ==========================================================================
  # SQL Layer Concepts (Phase 6)
  # ==========================================================================

  SqlExecutorConcept* = concept se
    ## SQL execution interface for testing
    ## Simplified interface that takes SQL strings directly
    ## Note: Real executor takes Plan + FractioClient; this is for mock testing
    proc execute*(se: se, sql: string): ExecutionResult
    proc executeInTxn*(se: se, sql: string,
        txnId: TransactionID): ExecutionResult
    proc reset*(se: se)

  SqlPlannerConcept* = concept sp
    ## SQL planning interface for testing
    ## Simplified interface that takes SQL strings directly
    ## Returns a plan identifier for mock testing
    ## Note: Real planner takes Stmt + FractioClient; this is for mock testing
    proc planSql*(sp: sp, sql: string): int64
    proc planSqlWithDb*(sp: sp, sql: string, database: string,
        schema: string): int64
    proc reset*(sp: sp)

# =============================================================================
# Generic Service Proc (for type-safe operations)
# =============================================================================

proc isTimeProvider*(tp: TimeProviderConcept): bool = true
proc isSharedTimerTimeProvider*(tp: SharedTimerTimeProviderConcept): bool = true
proc isTimestampProvider*(tp: TimestampProviderConcept): bool = true
proc isLogProvider*(lp: LogProviderConcept): bool = true
proc isKVStore*(ks: KVStoreConcept): bool = true
proc isTransactionManager*(tm: TransactionManagerConcept): bool = true
proc isStorageBackend*(b: StorageBackendConcept): bool = true
proc isBackend*(b: BackendConcept): bool = true
proc isConnectionHandle*(ch: ConnectionHandleConcept): bool = true
proc isConnectionManager*(cm: ConnectionManagerConcept): bool = true

# Distributed layer helper procs
proc isRaftCoordinator*(rc: RaftCoordinatorConcept): bool = true
proc isRaftTransport*(rt: RaftTransportConcept): bool = true
proc isRaftStateMachine*(sm: RaftStateMachineConcept): bool = true
proc isRaftLog*(rl: RaftLogConcept): bool = true
proc isSpaceManager*(sm: SpaceManagerConcept): bool = true
proc isNetworkTransport*(nt: NetworkTransportConcept): bool = true

# Protocol layer helper procs
proc isProtocolServer*(ps: ProtocolServerConcept): bool = true
proc isProtocolClient*(pc: ProtocolClientConcept): bool = true

# SQL layer helper procs (Phase 6)
proc isSqlExecutor*(se: SqlExecutorConcept): bool = true
proc isSqlPlanner*(sp: SqlPlannerConcept): bool = true
