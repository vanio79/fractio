# Network Types - Message types and wire protocol definitions
# TCP-based network communication for distributed Fractio

import ../../core/types
from ../raft/group_types import GroupID, groupIDToULID, groupIDFromULID,
    ZeroGroupID, `$`

type
  # ==========================================================================
  # Message Header - Common header for all protocol messages
  # ==========================================================================

  MessageHeader* = object
    ## Header for all network messages
    messageType*: uint16  # Type of message (RaftMessageType or ClientMessageType)
    messageId*: uint64    # Unique ID for request/response correlation
    sourceNodeId*: NodeID # Sender node ID
    targetNodeId*: NodeID # Target node ID
    term*: uint64         # Raft term (0 for non-Raft messages)
    timestamp*: uint64    # HLC timestamp for ordering
    groupId*: GroupID     # Raft group ID (for multiplexing) - serialized as 16-byte ULID

  # ==========================================================================
  # Raft Message Types - Used for consensus (Port + 0)
  # ==========================================================================

  RaftMessageType* = enum
    ## Raft protocol message types
    rmtRequestVote = 1
    rmtRequestVoteResponse = 2
    rmtAppendEntries = 3
    rmtAppendEntriesResponse = 4
    rmtInstallSnapshot = 5
    rmtInstallSnapshotResponse = 6
    rmtTimeoutNow = 7 # For leadership transfer
    rmtReadIndex = 8  # For linearizable reads
    rmtReadIndexResponse = 9

  RequestVoteMsg* = object
    ## Raft RequestVote RPC
    header*: MessageHeader
    candidateId*: NodeID
    lastLogIndex*: uint64
    lastLogTerm*: uint64

  RequestVoteResponseMsg* = object
    ## Response to RequestVote
    header*: MessageHeader
    voteGranted*: bool
    term*: uint64

  AppendEntriesMsg* = object
    ## Raft AppendEntries RPC
    header*: MessageHeader
    leaderId*: NodeID
    prevLogIndex*: uint64
    prevLogTerm*: uint64
    commitIndex*: uint64
    numEntries*: uint32
    entriesData*: string # Serialized log entries

  AppendEntriesResponseMsg* = object
    ## Response to AppendEntries
    header*: MessageHeader
    success*: bool
    term*: uint64
    matchIndex*: uint64
    rejectHint*: uint64 # For fast log matching

  InstallSnapshotMsg* = object
    ## Raft InstallSnapshot RPC
    header*: MessageHeader
    leaderId*: NodeID
    lastIncludedIndex*: uint64
    lastIncludedTerm*: uint64
    offset*: uint64
    done*: bool
    data*: string

  InstallSnapshotResponseMsg* = object
    ## Response to InstallSnapshot
    header*: MessageHeader
    term*: uint64
    offset*: uint64

  TimeoutNowMsg* = object
    ## Request immediate election (leadership transfer)
    header*: MessageHeader

  ReadIndexMsg* = object
    ## Request for linearizable read
    header*: MessageHeader
    readRequestId*: uint64

  ReadIndexResponseMsg* = object
    ## Response to ReadIndex
    header*: MessageHeader
    readRequestId*: uint64
    index*: uint64

  # ==========================================================================
  # Client Message Types - Used for KV operations (Port + 1)
  # ==========================================================================

  ClientMessageType* = enum
    ## Client protocol message types
    cmtBatchRequest = 100
    cmtBatchResponse = 101
    cmtScanRequest = 102
    cmtScanResponse = 103
    cmtTxnPrepare = 104
    cmtTxnPrepareResponse = 105
    cmtTxnCommit = 106
    cmtTxnCommitResponse = 107
    cmtTxnRollback = 108
    cmtTxnRollbackResponse = 109
    cmtHeartbeat = 110
    cmtHeartbeatResponse = 111
    cmtError = 112

  RequestKind* = enum
    ## Types of requests in a batch
    rkGet
    rkPut
    rkDelete
    rkScan

  KVRequest* = object
    ## Single KV request
    case kind*: RequestKind
    of rkGet:
      getKey*: string
      getTimestamp*: uint64 # Read at specific timestamp
    of rkPut:
      putKey*: string
      putValue*: string
    of rkDelete:
      deleteKey*: string
    of rkScan:
      scanStartKey*: string
      scanEndKey*: string
      scanLimit*: uint32
      scanTimestamp*: uint64

  KVResponse* = object
    ## Response to a single KV request
    success*: bool
    errorMessage*: string
    case kind*: RequestKind
    of rkGet:
      getValue*: string
      getValueTimestamp*: uint64
      getFound*: bool
    of rkPut:
      putCommitTimestamp*: uint64
    of rkDelete:
      deleteCommitTimestamp*: uint64
    of rkScan:
      scanKeyValues*: seq[tuple[key: string, value: string]]
      scanHasMore*: bool

  BatchRequestMsg* = object
    ## Batch of KV requests
    header*: MessageHeader
    requestId*: uint64
    groupId*: uint32
    transactionId*: uint64 # 0 if not in transaction
    requests*: seq[KVRequest]

  BatchResponseMsg* = object
    ## Response to batch request
    header*: MessageHeader
    requestId*: uint64
    success*: bool
    errorMessage*: string
    responses*: seq[KVResponse]

  ScanRequestMsg* = object
    ## Dedicated scan request for large results
    header*: MessageHeader
    requestId*: uint64
    groupId*: uint32
    startKey*: string
    endKey*: string
    limit*: uint32
    timestamp*: uint64

  ScanResponseMsg* = object
    ## Response to scan request
    header*: MessageHeader
    requestId*: uint64
    success*: bool
    errorMessage*: string
    keyValues*: seq[tuple[key: string, value: string]]
    hasMore*: bool
    continuationToken*: string

  # ==========================================================================
  # 2PC Message Types - Used for distributed transactions
  # ==========================================================================

  TxnPrepareMsg* = object
    ## Prepare phase of 2PC
    header*: MessageHeader
    txnId*: uint64
    coordinatorId*: NodeID
    participantIds*: seq[NodeID]
    timestamp*: uint64

  TxnPrepareResponseMsg* = object
    ## Response to prepare
    header*: MessageHeader
    txnId*: uint64
    vote*: bool # true = commit, false = abort
    errorMessage*: string

  TxnCommitMsg* = object
    ## Commit phase of 2PC
    header*: MessageHeader
    txnId*: uint64
    commitTimestamp*: uint64

  TxnCommitResponseMsg* = object
    ## Response to commit
    header*: MessageHeader
    txnId*: uint64
    success*: bool
    errorMessage*: string

  TxnRollbackMsg* = object
    ## Rollback transaction
    header*: MessageHeader
    txnId*: uint64

  TxnRollbackResponseMsg* = object
    ## Response to rollback
    header*: MessageHeader
    txnId*: uint64
    success*: bool

  HeartbeatMsg* = object
    ## Connection heartbeat
    header*: MessageHeader
    ping*: bool

  HeartbeatResponseMsg* = object
    ## Response to heartbeat
    header*: MessageHeader
    pong*: bool

  ErrorMsg* = object
    ## Generic error response
    header*: MessageHeader
    errorCode*: uint32
    errorMessage*: string

  # ==========================================================================
  # Admin Message Types - Used for metrics and admin (Port + 2)
  # ==========================================================================

  AdminMessageType* = enum
    ## Admin protocol message types
    amtMetrics = 200
    amtMetricsResponse = 201
    amtHealth = 202
    amtHealthResponse = 203
    amtConfig = 204
    amtConfigResponse = 205
    amtNodeInfo = 206
    amtNodeInfoResponse = 207

  MetricsRequestMsg* = object
    ## Request node metrics
    header*: MessageHeader

  MetricsResponseMsg* = object
    ## Node metrics response
    header*: MessageHeader
    metricsJson*: string

  HealthRequestMsg* = object
    ## Health check request
    header*: MessageHeader

  HealthResponseMsg* = object
    ## Health check response
    header*: MessageHeader
    healthy*: bool
    details*: string

  ConfigRequestMsg* = object
    ## Request node configuration
    header*: MessageHeader

  ConfigResponseMsg* = object
    ## Node configuration response
    header*: MessageHeader
    configJson*: string

  NodeInfoRequestMsg* = object
    ## Request node information
    header*: MessageHeader
    targetNodeId*: NodeID

  NodeInfoResponseMsg* = object
    ## Node information response
    header*: MessageHeader
    nodeId*: NodeID
    raftAddr*: string
    clientAddr*: string
    adminAddr*: string
    isHealthy*: bool
    uptime*: uint64

  # ==========================================================================
  # Wire Frame - Low-level message framing
  # ==========================================================================

  FrameHeader* = object
    ## Frame header for TCP messages
    payloadLen*: uint32 # Length of payload
    checksum*: uint32   # CRC32 checksum of payload

  Frame* = object
    ## Complete wire frame
    header*: FrameHeader
    payload*: string

  # ==========================================================================
  # Connection Types
  # ==========================================================================

  ConnectionState* = enum
    ## State of a connection
    csIdle
    csConnecting
    csConnected
    csFailed
    csClosed

  # ==========================================================================
  # Error Types
  # ==========================================================================

  NetworkErrorCode* = enum
    ## Network error codes
    necConnectionRefused
    necConnectionReset
    necTimeout
    necInvalidMessage
    necChecksumMismatch
    necUnknownNode
    necNodeUnhealthy
    necBufferOverflow
    necSocketError
    necUnknownMessageType
    necSerializationError

  NetworkError* = object of CatchableError
    ## Network error
    code*: NetworkErrorCode

# ==========================================================================
# Constants
# ==========================================================================

const
  # Frame sizes
  FRAME_HEADER_SIZE* = 8               # 4 bytes len + 4 bytes checksum
  MESSAGE_HEADER_SIZE* = 48 # Size of MessageHeader when encoded (32 + 16 bytes for GroupID ULID)

  # Message size limits
  MAX_MESSAGE_SIZE* = 16 * 1024 * 1024 # 16MB max message
  MAX_FRAME_SIZE* = MAX_MESSAGE_SIZE + FRAME_HEADER_SIZE

  # Timeouts (milliseconds)
  DEFAULT_CONNECT_TIMEOUT_MS* = 5000
  DEFAULT_READ_TIMEOUT_MS* = 30000
  DEFAULT_WRITE_TIMEOUT_MS* = 30000

  # Connection pooling
  DEFAULT_MAX_CONNECTIONS_PER_NODE* = 4
  DEFAULT_IDLE_TIMEOUT_MS* = 60000

  # Health checking
  DEFAULT_HEALTH_CHECK_INTERVAL_MS* = 1000
  DEFAULT_FAILURE_THRESHOLD* = 3
  DEFAULT_RECOVERY_THRESHOLD* = 2

# ==========================================================================
# Helper Procs
# ==========================================================================

proc newMessageHeader*(msgType: uint16, msgId: uint64,
                       source, target: NodeID,
                           term: uint64 = 0,
                           groupId: GroupID = ZeroGroupID()): MessageHeader =
  ## Create a new message header
  result.messageType = msgType
  result.messageId = msgId
  result.sourceNodeId = source
  result.targetNodeId = target
  result.term = term
  result.timestamp = 0 # Set by sender
  result.groupId = groupId

proc newNetworkError*(code: NetworkErrorCode, msg: string): NetworkError =
  ## Create a new network error
  result = NetworkError(
    code: code,
    msg: msg
  )

proc `$`*(header: MessageHeader): string =
  ## String representation of message header
  result = "MessageHeader(type=" & $header.messageType &
           ", id=" & $header.messageId &
           ", src=" & string(header.sourceNodeId) &
           ", dst=" & string(header.targetNodeId) &
           ", term=" & $header.term &
           ", groupId=" & $header.groupId & ")"
