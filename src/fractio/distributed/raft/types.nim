# Raft Types and Core Definitions

import fractio/utils/binary

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

proc encodeLogEntry*(entry: LogEntry): string =
  ## Encode a LogEntry to binary format.
  ##
  ## Binary format (little-endian):
  ## - Magic: 2 bytes (0x52 0x45 = "RE")
  ## - Version: 1 byte (0x01)
  ## - Term: 8 bytes (int64)
  ## - EntryType: 1 byte (uint8 ordinal)
  ## - Data: length-prefixed (u32 len + bytes)
  ##
  ## Total minimum: 16 bytes (empty data)
  var w = initBinaryWriter()
  w.writeBytes(LOG_ENTRY_MAGIC)
  w.writeU8(LOG_ENTRY_VERSION)
  w.writeI64(entry.term)
  w.writeU8(uint8(ord(entry.entryType)))
  w.writeString(entry.data)
  w.finish()

proc decodeLogEntry*(data: string): LogEntry =
  ## Decode binary data to a LogEntry.
  ## Raises ValueError if data is invalid or not binary format.
  var r = initBinaryReader(data)

  # Verify magic header
  if r.remaining < 3:
    raise newException(ValueError, "LogEntry: data too small for header")
  let magic0 = r.readU8()
  let magic1 = r.readU8()
  if magic0 != LOG_ENTRY_MAGIC[0] or magic1 != LOG_ENTRY_MAGIC[1]:
    raise newException(ValueError, "LogEntry: invalid magic header")

  # Verify version
  let version = r.readU8()
  if version != LOG_ENTRY_VERSION:
    raise newException(ValueError, "LogEntry: unsupported version " & $version)

  # Read fields
  result.term = r.readI64()
  result.entryType = LogEntryType(int(r.readU8()))
  result.data = r.readString()

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

method close*(store: RaftLogStore) {.base.} =
  ## Close the log store (base implementation does nothing)
  discard
