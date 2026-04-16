# Raft Types and Core Definitions (declarative)
#
# This file contains ONLY type definitions and constants (no executable code).
# Excluded from coverage reporting since type definitions cannot be unit tested.
# The encode/decode procs are in types.nim which imports this file.

# =============================================================================
# Binary Serialization Constants
# =============================================================================

const
  LOG_ENTRY_MAGIC* = [0x52'u8, 0x45'u8] # "RE" - Raft Entry binary marker
  LOG_ENTRY_VERSION* = 0x01'u8          # Current binary format version

# =============================================================================
# Raft Server Roles
# =============================================================================

type
  ServerRole* = enum
    SR_LEADER
    SR_CANDIDATE
    SR_FOLLOWER

# Raft Node State
type
  RaftNodeState* = object
    role*: ServerRole
    currentTerm*: int64
    votedFor*: int32
    leaderId*: int32
    commitIndex*: int64
    lastApplied*: int64

# Raft Configuration
type
  RaftConfig* = object
    ## Configuration for Raft node
    serverId*: int32
    endpoint*: string
    electionTimeout*: int   # ms
    heartbeatInterval*: int # ms
    logStoragePath*: string # WiscKey path
    snapshotEnabled*: bool
    snapshotDistance*: int  # Log distance between snapshots
    maxAppendSize*: int     # Max entries per append RPC

# =============================================================================
# Raft Log Entry
# =============================================================================

type
  LogEntryType* = enum
    LET_NORMAL
    LET_CONFIG_CHANGE
    LET_NO_OP

  LogEntry* = object
    term*: int64
    entryType*: LogEntryType
    data*: string

# =============================================================================
# Raft Log Store Interface
# =============================================================================
type
  RaftLogStore* = ref object of RootObj
    ## Abstract log store for Raft

  RaftError* = object of CatchableError
    ## Raft-specific errors

# Raft State Machine Interface
type
  StateMachine* = ref object of RootObj
    ## Base class for user-defined state machines

  RaftNode* = ref object of RootObj
    ## High-level Raft node for managing consensus
    serverId*: int32
    endpoint*: string
    config*: RaftConfig
    nodeState*: RaftNodeState
    logStore*: RaftLogStore
    stateMachine*: StateMachine
    initialized*: bool
    isLeader*: bool
    leaderId*: int32
    commitIndex*: int64
    lastApplied*: int64

type
  RPCType* = enum
    RPC_APPEND_ENTRIES
    RPC_REQUEST_VOTE
    RPC_CLIENT_REQUEST

  RaftRPC* = object
    ## Raft RPC message
    rpcType*: RPCType
    term*: int64
    leaderId*: int32
    prevLogIndex*: int64
    prevLogTerm*: int64
    entries*: seq[LogEntry]
    leaderCommit*: int64
    success*: bool
    data*: string
