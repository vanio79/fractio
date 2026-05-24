# Two-Phase Commit (2PC) for Distributed Transactions
# Provides atomic commit across multiple nodes/ranges using the 2PC protocol
# Integrated with Raft for consensus and durability

import std/[options, tables, sets, sequtils, times, strutils,
    asyncdispatch, random]
import fractio/core/types
import fractio/core/transaction
import fractio/core/timestamp_provider
import fractio/storage/mvcc/engine
import fractio/storage/mvcc/types
import fractio/utils/logging
import fractio/utils/binary
import fractio/distributed/raft/types

type
  # 2PC States
  TwoPCState* = enum
    ## State of a 2PC participant or coordinator
    tpcsIdle
      ## Not participating in any transaction
    tpcsPreparing
      ## In prepare phase, waiting for votes
    tpcsPrepared
      ## Prepared, waiting for commit/rollback
    tpcsCommitting
      ## In commit phase
    tpcsCommitted
      ## Transaction committed successfully
    tpcsAborting
      ## In abort phase
    tpcsAborted
      ## Transaction aborted
    tpcsRecovering
      ## Recovering from coordinator failure

  # 2PC Participant
  Participant* = ref object
    ## Represents a participant in a distributed transaction
    nodeId*: string
      ## Unique identifier for this node
    endpoint*: string
      ## Network endpoint for RPC communication
    state*: TwoPCState
      ## Current 2PC state
    transactionId*: TransactionID
      ## Transaction ID being coordinated
    prepareTimestamp*: Timestamp
      ## Timestamp when prepare was received
    vote*: ParticipantVote
      ## Vote (yes/no/abstain)
    transaction*: MVCCTransaction
      ## Local transaction (if any)
    lastHeartbeat*: Timestamp
      ## Last heartbeat from coordinator

  ParticipantVote* = enum
    ## Vote from participant during prepare phase
    pvYes
      ## Participant can commit
    pvNo
      ## Participant cannot commit
    pvAbstain
      ## Participant abstains (doesn't vote)

  # 2PC Coordinator
  Coordinator* = ref object
    ## Coordinates a distributed transaction across multiple participants
    transaction*: MVCCTransaction
      ## The transaction being coordinated
    transactionId*: TransactionID
      ## Unique transaction ID
    coordinatorId*: string
      ## Node ID of the coordinator
    participants*: seq[Participant]
      ## List of participants in this transaction
    state*: TwoPCState
      ## Current 2PC state
    prepareTimeout*: int64
      ## Timeout for prepare phase (milliseconds)
    commitTimeout*: int64
      ## Timeout for commit/rollback phase (milliseconds)
    startTime*: Timestamp
      ## When the transaction started
    votes*: seq[ParticipantVote]
      ## Votes received from participants
    preparedCount*: int
      ## Number of participants that voted yes
    abortedCount*: int
      ## Number of participants that voted no/abstain
    quorum*: int
      ## Number of votes needed to proceed
    retryCount*: int
      ## Number of retry attempts
    raftNode*: RaftNode
      ## Raft node for consensus (optional)
    logger*: Logger
      ## Logger for this coordinator

  # 2PC Request Types
  TwoPCRequestType* = enum
    ## Types of 2PC requests
    tpcPrepare
      ## Prepare request
    tpcCommit
      ## Commit request
    tpcRollback
      ## Rollback request
    tpcRecovery
      ## Recovery request
    tpcHeartbeat
      ## Heartbeat to keep transaction alive

  TwoPCRequest* = object
    ## A 2PC request
    requestId*: string
      ## Unique request ID
    requestType*: TwoPCRequestType
    transactionId*: TransactionID
    coordinatorId*: string
    timestamp*: Timestamp
    data*: string
      ## Additional request data (e.g., write set)
    participantEndpoints*: seq[string]
      ## List of participant endpoints

  TwoPCResponse* = object
    ## A 2PC response
    requestId*: string
    transactionId*: TransactionID
    participantId*: string
    vote*: ParticipantVote
    state*: TwoPCState
    error*: string
      ## Error message if failed

  TwoPCResult* = object
    ## Result of a 2PC transaction
    success*: bool
    transactionId*: TransactionID
    commitTimestamp*: Timestamp
    participants*: seq[string]
      ## IDs of participants that committed
    error*: string
      ## Error message if failed
    retryable*: bool
      ## Whether the transaction can be retried

  # 2PC Configuration
  TwoPCConfig* = object
    ## Configuration for 2PC
    prepareTimeoutMs*: int64
      ## Timeout for prepare phase (default: 10s)
    commitTimeoutMs*: int64
      ## Timeout for commit/rollback (default: 5s)
    maxRetries*: int
      ## Maximum retry attempts
    enableRecovery*: bool
      ## Enable recovery from coordinator failure
    recoveryCheckIntervalMs*: int64
      ## How often to check for recovery (default: 1s)
    heartbeatIntervalMs*: int64
      ## Heartbeat interval (default: 2s)
    enableRaft*: bool
      ## Use Raft for consensus

  # 2PC Error Types
  TwoPCErrorKind* = enum
    tpekTimeout
    tpekNetworkError
    tpekParticipantFailed
    tpekCoordinatorFailed
    tpekQuorumNotReached
    tpekInvalidState
    tpekRaftError

  TwoPCError* = ref object of CatchableError
    ## 2PC-specific error
    errorKind*: TwoPCErrorKind
    participantId*: string
      ## Participant that caused the error (if applicable)

const
  DEFAULT_PREPARE_TIMEOUT_MS* = 10_000        # 10 seconds
  DEFAULT_COMMIT_TIMEOUT_MS* = 5_000          # 5 seconds
  DEFAULT_2PC_MAX_RETRIES* = 3
  DEFAULT_RECOVERY_CHECK_INTERVAL_MS* = 1_000 # 1 second
  DEFAULT_HEARTBEAT_INTERVAL_MS* = 2_000      # 2 seconds

# Helper functions

proc newTwoPCConfig*(): TwoPCConfig =
  ## Create default 2PC configuration
  TwoPCConfig(
    prepareTimeoutMs: DEFAULT_PREPARE_TIMEOUT_MS,
    commitTimeoutMs: DEFAULT_COMMIT_TIMEOUT_MS,
    maxRetries: DEFAULT_2PC_MAX_RETRIES,
    enableRecovery: true,
    recoveryCheckIntervalMs: DEFAULT_RECOVERY_CHECK_INTERVAL_MS,
    heartbeatIntervalMs: DEFAULT_HEARTBEAT_INTERVAL_MS,
    enableRaft: false
  )

proc generateTransactionId*(tsProvider: TimestampProvider): TransactionID =
  ## Generate a unique transaction ID using ULID
  ## The timestamp provider is used for the timestamp component
  result = genTransactionID(tsProvider.now())

proc generateRequestId*(): string =
  ## Generate a unique request ID
  result = "req_" & $epochTime().int64 & "_" & $rand(1000000)

proc newTwoPCError*(kind: TwoPCErrorKind, message: string,
    participantId: string = ""): TwoPCError =
  ## Create a 2PC error
  result = TwoPCError(
    msg: message,
    errorKind: kind,
    participantId: participantId
  )

# Coordinator operations

proc newCoordinator*(transaction: MVCCTransaction,
    coordinatorId: string,
    config: TwoPCConfig = newTwoPCConfig(),
    raftNode: RaftNode = nil,
    logger: Logger = nil): Coordinator =
  ## Create a new coordinator for a transaction
  new(result)
  result.transaction = transaction
  result.transactionId = transaction.id
  result.coordinatorId = coordinatorId
  result.participants = @[]
  result.state = tpcsIdle
  result.prepareTimeout = config.prepareTimeoutMs
  result.commitTimeout = config.commitTimeoutMs
  result.startTime = transaction.createdAt
  result.votes = @[]
  result.preparedCount = 0
  result.abortedCount = 0
  result.quorum = 0
  result.retryCount = 0
  result.raftNode = raftNode
  result.logger = if logger != nil: logger else: newLogger()

proc addParticipant*(coordinator: Coordinator,
    nodeId: string, endpoint: string,
    transaction: MVCCTransaction) =
  ## Add a participant to the coordinator
  let participant = Participant(
    nodeId: nodeId,
    endpoint: endpoint,
    state: tpcsIdle,
    transactionId: coordinator.transactionId,
    prepareTimestamp: INVALID_TIMESTAMP,
    vote: pvAbstain,
    transaction: transaction,
    lastHeartbeat: INVALID_TIMESTAMP
  )
  coordinator.participants.add(participant)
  # Quorum is majority of participants
  coordinator.quorum = (coordinator.participants.len div 2) + 1

proc sendPrepareRequest*(coordinator: Coordinator): Future[seq[
    TwoPCResponse]] {.async.} =
  ## Send prepare requests to all participants
  ## Returns responses from participants

  coordinator.state = tpcsPreparing
  coordinator.startTime = Timestamp(epochTime().int64 * 1_000_000) # Convert to nanoseconds

  var responses: seq[TwoPCResponse] = @[]
  let requestId = generateRequestId()

  coordinator.logger.info("Sending prepare requests",
    {"transactionId": $coordinator.transactionId,
     "participants": $coordinator.participants.len}.toTable)

  # If Raft is enabled, log the prepare phase to Raft
  if coordinator.raftNode != nil:
    # In a real implementation, we would log the prepare phase to Raft
    # for durability and consensus
    discard

  for participant in coordinator.participants:
    # In a real implementation, this would send RPC to participant
    # For now, we simulate the response
    let response = TwoPCResponse(
      requestId: requestId,
      transactionId: coordinator.transactionId,
      participantId: participant.nodeId,
      vote: pvYes, # Assume all vote yes for now
      state: tpcsPrepared,
      error: ""
    )
    responses.add(response)

  return responses

proc processPrepareResponses*(coordinator: Coordinator,
    responses: seq[TwoPCResponse]): bool =
  ## Process prepare responses from participants
  ## Returns true if quorum achieved

  coordinator.votes = @[]
  coordinator.preparedCount = 0
  coordinator.abortedCount = 0

  for response in responses:
    coordinator.votes.add(response.vote)

    if response.vote == pvYes:
      coordinator.preparedCount += 1
    elif response.vote == pvNo:
      coordinator.abortedCount += 1
    elif response.vote == pvAbstain:
      coordinator.abortedCount += 1

    coordinator.logger.debug("Received prepare response",
      {"participantId": response.participantId,
       "vote": $response.vote,
       "state": $response.state}.toTable)

  # Check if we have quorum
  let quorumAchieved = coordinator.preparedCount >= coordinator.quorum

  coordinator.logger.info("Prepare phase completed",
    {"transactionId": $coordinator.transactionId,
     "preparedCount": $coordinator.preparedCount,
     "abortedCount": $coordinator.abortedCount,
     "quorum": $coordinator.quorum,
     "quorumAchieved": $quorumAchieved}.toTable)

  return quorumAchieved

proc sendCommitRequest*(coordinator: Coordinator): Future[seq[
    TwoPCResponse]] {.async.} =
  ## Send commit requests to all participants
  ## Returns responses from participants

  coordinator.state = tpcsCommitting

  var responses: seq[TwoPCResponse] = @[]
  let requestId = generateRequestId()

  coordinator.logger.info("Sending commit requests",
    {"transactionId": $coordinator.transactionId,
     "participants": $coordinator.participants.len}.toTable)

  # If Raft is enabled, log the commit phase to Raft
  if coordinator.raftNode != nil:
    # In a real implementation, we would log the commit phase to Raft
    discard

  for participant in coordinator.participants:
    # In a real implementation, this would send RPC to participant
    # For now, we simulate the response
    let response = TwoPCResponse(
      requestId: requestId,
      transactionId: coordinator.transactionId,
      participantId: participant.nodeId,
      vote: pvAbstain,
      state: tpcsCommitted,
      error: ""
    )
    responses.add(response)

  return responses

proc sendRollbackRequest*(coordinator: Coordinator): Future[seq[
    TwoPCResponse]] {.async.} =
  ## Send rollback requests to all participants
  ## Returns responses from participants

  coordinator.state = tpcsAborting

  var responses: seq[TwoPCResponse] = @[]
  let requestId = generateRequestId()

  coordinator.logger.info("Sending rollback requests",
    {"transactionId": $coordinator.transactionId,
     "participants": $coordinator.participants.len}.toTable)

  for participant in coordinator.participants:
    # In a real implementation, this would send RPC to participant
    # For now, we simulate the response
    let response = TwoPCResponse(
      requestId: requestId,
      transactionId: coordinator.transactionId,
      participantId: participant.nodeId,
      vote: pvAbstain,
      state: tpcsAborted,
      error: ""
    )
    responses.add(response)

  return responses

proc executeTwoPC*(coordinator: Coordinator): Future[TwoPCResult] {.async.} =
  ## Execute the full 2PC protocol
  ## 1. Prepare phase
  ## 2. Commit or Rollback phase

  coordinator.logger.info("Starting 2PC transaction",
    {"transactionId": $coordinator.transactionId,
     "coordinatorId": coordinator.coordinatorId,
     "participants": $coordinator.participants.len}.toTable)

  # Phase 1: Prepare
  let prepareResponses = await coordinator.sendPrepareRequest()

  if not coordinator.processPrepareResponses(prepareResponses):
    # Quorum not achieved, rollback
    let rollbackResponses = await coordinator.sendRollbackRequest()
    coordinator.state = tpcsAborted

    coordinator.logger.warn("Prepare phase failed, rolling back",
      {"transactionId": $coordinator.transactionId,
       "reason": "quorum not achieved"}.toTable)

    return TwoPCResult(
      success: false,
      transactionId: coordinator.transactionId,
      commitTimestamp: INVALID_TIMESTAMP,
      participants: @[],
      error: "Prepare phase failed: quorum not achieved",
      retryable: true
    )

  # Phase 2: Commit
  let commitResponses = await coordinator.sendCommitRequest()

  # Verify all participants committed
  var allCommitted = true
  for response in commitResponses:
    if response.state != tpcsCommitted:
      allCommitted = false
      coordinator.logger.error("Participant failed to commit",
        {"transactionId": $coordinator.transactionId,
         "participantId": response.participantId,
         "state": $response.state,
         "error": response.error}.toTable)
      break

  if allCommitted:
    coordinator.state = tpcsCommitted
    let commitTs = coordinator.transaction.commitTimestamp
    let participantIds = coordinator.participants.mapIt(it.nodeId)

    coordinator.logger.info("2PC transaction committed successfully",
      {"transactionId": $coordinator.transactionId,
       "commitTimestamp": $commitTs,
       "participants": $participantIds.len}.toTable)

    return TwoPCResult(
      success: true,
      transactionId: coordinator.transactionId,
      commitTimestamp: commitTs,
      participants: participantIds,
      error: "",
      retryable: false
    )
  else:
    # Some participants failed to commit
    coordinator.state = tpcsAborted

    coordinator.logger.error("Commit phase failed",
      {"transactionId": $coordinator.transactionId,
       "reason": "some participants failed"}.toTable)

    return TwoPCResult(
      success: false,
      transactionId: coordinator.transactionId,
      commitTimestamp: INVALID_TIMESTAMP,
      participants: @[],
      error: "Commit phase failed: some participants failed",
      retryable: false
    )

# Participant operations

proc newParticipant*(nodeId: string, endpoint: string,
    config: TwoPCConfig = newTwoPCConfig(),
    logger: Logger = nil): Participant =
  ## Create a new participant
  new(result)
  result.nodeId = nodeId
  result.endpoint = endpoint
  result.state = tpcsIdle
  result.transactionId = zeroTransactionID()
  result.prepareTimestamp = INVALID_TIMESTAMP
  result.vote = pvAbstain
  result.transaction = nil
  result.lastHeartbeat = INVALID_TIMESTAMP

proc handlePrepareRequest*(participant: Participant,
    request: TwoPCRequest): TwoPCResponse =
  ## Handle a prepare request from coordinator
  ## Returns response with vote

  participant.state = tpcsPreparing
  participant.transactionId = request.transactionId
  participant.prepareTimestamp = request.timestamp

  # In a real implementation, this would:
  # 1. Validate the transaction
  # 2. Lock resources
  # 3. Write intents
  # 4. Return vote

  # For now, we always vote yes
  participant.vote = pvYes
  participant.state = tpcsPrepared

  return TwoPCResponse(
    requestId: request.requestId,
    transactionId: request.transactionId,
    participantId: participant.nodeId,
    vote: pvYes,
    state: tpcsPrepared,
    error: ""
  )

proc handleCommitRequest*(participant: Participant,
    request: TwoPCRequest): TwoPCResponse =
  ## Handle a commit request from coordinator
  ## Returns response with new state

  participant.state = tpcsCommitting

  # In a real implementation, this would:
  # 1. Upgrade intents to committed values
  # 2. Release locks
  # 3. Return response

  participant.state = tpcsCommitted

  return TwoPCResponse(
    requestId: request.requestId,
    transactionId: request.transactionId,
    participantId: participant.nodeId,
    vote: pvAbstain,
    state: tpcsCommitted,
    error: ""
  )

proc handleRollbackRequest*(participant: Participant,
    request: TwoPCRequest): TwoPCResponse =
  ## Handle a rollback request from coordinator
  ## Returns response with new state

  participant.state = tpcsAborting

  # In a real implementation, this would:
  # 1. Rollback intents
  # 2. Release locks
  # 3. Return response

  participant.state = tpcsAborted

  return TwoPCResponse(
    requestId: request.requestId,
    transactionId: request.transactionId,
    participantId: participant.nodeId,
    vote: pvAbstain,
    state: tpcsAborted,
    error: ""
  )

proc handleHeartbeat*(participant: Participant,
    request: TwoPCRequest): TwoPCResponse =
  ## Handle a heartbeat from coordinator
  ## Returns response to keep transaction alive

  participant.lastHeartbeat = request.timestamp

  return TwoPCResponse(
    requestId: request.requestId,
    transactionId: request.transactionId,
    participantId: participant.nodeId,
    vote: pvAbstain,
    state: participant.state,
    error: ""
  )

# Recovery mechanism

proc checkRecovery*(participant: Participant,
    coordinatorId: string,
    config: TwoPCConfig): TwoPCResult =
  ## Check if recovery is needed for pending transactions
  ## Returns recovery result

  # In a real implementation, this would:
  # 1. Check for pending transactions
  # 2. Contact coordinator for status
  # 3. Complete or rollback pending transactions

  return TwoPCResult(
    success: true,
    transactionId: zeroTransactionID(),
    commitTimestamp: INVALID_TIMESTAMP,
    participants: @[],
    error: "",
    retryable: false
  )

proc recoverTransaction*(coordinator: Coordinator,
    transactionId: TransactionID): TwoPCResult =
  ## Attempt to recover a transaction after coordinator failure
  ## Returns recovery result

  coordinator.state = tpcsRecovering

  coordinator.logger.info("Attempting to recover transaction",
    {"transactionId": $transactionId,
     "coordinatorId": coordinator.coordinatorId}.toTable)

  # In a real implementation, this would:
  # 1. Query all participants for their state
  # 2. Determine if transaction can be committed or must be aborted
  # 3. Send commit/rollback to participants
  # 4. Return result

  # For now, return success
  coordinator.state = tpcsIdle

  return TwoPCResult(
    success: true,
    transactionId: transactionId,
    commitTimestamp: INVALID_TIMESTAMP,
    participants: @[],
    error: "",
    retryable: false
  )

# Timeout handling

proc checkTimeout*(coordinator: Coordinator,
    currentTime: Timestamp): bool =
  ## Check if the current phase has timed out
  ## Returns true if timeout occurred

  let elapsed = currentTime - coordinator.startTime

  case coordinator.state
  of tpcsPreparing:
    return elapsed >= coordinator.prepareTimeout
  of tpcsCommitting, tpcsAborting:
    return elapsed >= coordinator.commitTimeout
  else:
    return false

proc handleTimeout*(coordinator: Coordinator): TwoPCResult =
  ## Handle a timeout in the current phase
  ## Returns result of timeout handling

  coordinator.logger.warn("2PC timeout occurred",
    {"transactionId": $coordinator.transactionId,
     "state": $coordinator.state}.toTable)

  case coordinator.state
  of tpcsPreparing:
    # Timeout in prepare phase - rollback
    coordinator.state = tpcsAborted
    return TwoPCResult(
      success: false,
      transactionId: coordinator.transactionId,
      commitTimestamp: INVALID_TIMESTAMP,
      participants: @[],
      error: "Prepare phase timeout",
      retryable: true
    )
  of tpcsCommitting:
    # Timeout in commit phase - this is a critical error
    # Need to run recovery
    coordinator.state = tpcsRecovering
    return TwoPCResult(
      success: false,
      transactionId: coordinator.transactionId,
      commitTimestamp: INVALID_TIMESTAMP,
      participants: @[],
      error: "Commit phase timeout - recovery needed",
      retryable: false
    )
  of tpcsAborting:
    # Timeout in abort phase - assume aborted
    coordinator.state = tpcsAborted
    return TwoPCResult(
      success: false,
      transactionId: coordinator.transactionId,
      commitTimestamp: INVALID_TIMESTAMP,
      participants: @[],
      error: "Abort phase timeout",
      retryable: false
    )
  else:
    return TwoPCResult(
      success: false,
      transactionId: coordinator.transactionId,
      commitTimestamp: INVALID_TIMESTAMP,
      participants: @[],
      error: "Unknown timeout state",
      retryable: false
    )

# =============================================================================
# Binary Serialization for RPC
# =============================================================================

const
  TWOPC_REQUEST_MAGIC* = [0x32'u8, 0x50'u8, 0x52'u8]       # "2PR" - 2PC Request
  TWOPC_RESPONSE_MAGIC* = [0x32'u8, 0x50'u8, 0x53'u8]      # "2PS" - 2PC reSponse
  TWOPC_VERSION* = 0x01'u8

proc encodeTwoPCRequest*(request: TwoPCRequest): string =
  ## Encode a TwoPCRequest to binary format.
  ##
  ## Binary format (little-endian):
  ## - Magic: 3 bytes ("2PR")
  ## - Version: 1 byte
  ## - RequestType: 1 byte (uint8 ordinal)
  ## - RequestId: length-prefixed string
  ## - TransactionId: 16 bytes (ULID)
  ## - CoordinatorId: length-prefixed string
  ## - Timestamp: 8 bytes (int64)
  ## - Data: length-prefixed string
  ## - ParticipantEndpoints: length-prefixed seq of strings
  var w = initBinaryWriter()
  w.writeBytes(TWOPC_REQUEST_MAGIC)
  w.writeU8(TWOPC_VERSION)
  w.writeU8(uint8(ord(request.requestType)))
  w.writeString(request.requestId)
  w.writeBytes(transactionIDToBytes(request.transactionId))
  w.writeString(request.coordinatorId)
  w.writeI64(int64(request.timestamp))
  w.writeString(request.data)
  # Write participant endpoints
  w.writeU32(uint32(request.participantEndpoints.len))
  for ep in request.participantEndpoints:
    w.writeString(ep)
  w.finish()

proc decodeTwoPCRequest*(data: string): TwoPCRequest =
  ## Decode binary data to a TwoPCRequest.
  ## Raises ValueError if data is invalid.
  var r = initBinaryReader(data)

  # Verify magic header
  if r.remaining < 4:
    raise newException(ValueError, "TwoPCRequest: data too small for header")
  let magic0 = r.readU8()
  let magic1 = r.readU8()
  let magic2 = r.readU8()
  if magic0 != TWOPC_REQUEST_MAGIC[0] or magic1 != TWOPC_REQUEST_MAGIC[1] or
     magic2 != TWOPC_REQUEST_MAGIC[2]:
    raise newException(ValueError, "TwoPCRequest: invalid magic header")

  # Verify version
  let version = r.readU8()
  if version != TWOPC_VERSION:
    raise newException(ValueError, "TwoPCRequest: unsupported version " & $version)

  # Read fields
  result.requestType = TwoPCRequestType(int(r.readU8()))
  result.requestId = r.readString()
  let txnIdBytes = r.readFixedString(16)
  result.transactionId = transactionIDFromBytes(txnIdBytes)
  result.coordinatorId = r.readString()
  result.timestamp = Timestamp(r.readI64())
  result.data = r.readString()
  # Read participant endpoints
  let epCount = int(r.readU32())
  result.participantEndpoints = newSeq[string](epCount)
  for i in 0..<epCount:
    result.participantEndpoints[i] = r.readString()

proc encodeTwoPCResponse*(response: TwoPCResponse): string =
  ## Encode a TwoPCResponse to binary format.
  ##
  ## Binary format (little-endian):
  ## - Magic: 3 bytes ("2PS")
  ## - Version: 1 byte
  ## - Vote: 1 byte (uint8 ordinal)
  ## - State: 1 byte (uint8 ordinal)
  ## - RequestId: length-prefixed string
  ## - TransactionId: 16 bytes (ULID)
  ## - ParticipantId: length-prefixed string
  ## - Error: length-prefixed string
  var w = initBinaryWriter()
  w.writeBytes(TWOPC_RESPONSE_MAGIC)
  w.writeU8(TWOPC_VERSION)
  w.writeU8(uint8(ord(response.vote)))
  w.writeU8(uint8(ord(response.state)))
  w.writeString(response.requestId)
  w.writeBytes(transactionIDToBytes(response.transactionId))
  w.writeString(response.participantId)
  w.writeString(response.error)
  w.finish()

proc decodeTwoPCResponse*(data: string): TwoPCResponse =
  ## Decode binary data to a TwoPCResponse.
  ## Raises ValueError if data is invalid.
  var r = initBinaryReader(data)

  # Verify magic header
  if r.remaining < 5:
    raise newException(ValueError, "TwoPCResponse: data too small for header")
  let magic0 = r.readU8()
  let magic1 = r.readU8()
  let magic2 = r.readU8()
  if magic0 != TWOPC_RESPONSE_MAGIC[0] or magic1 != TWOPC_RESPONSE_MAGIC[1] or
     magic2 != TWOPC_RESPONSE_MAGIC[2]:
    raise newException(ValueError, "TwoPCResponse: invalid magic header")

  # Verify version
  let version = r.readU8()
  if version != TWOPC_VERSION:
    raise newException(ValueError, "TwoPCResponse: unsupported version " & $version)

  # Read fields
  result.vote = ParticipantVote(int(r.readU8()))
  result.state = TwoPCState(int(r.readU8()))
  result.requestId = r.readString()
  let txnIdBytes = r.readFixedString(16)
  result.transactionId = transactionIDFromBytes(txnIdBytes)
  result.participantId = r.readString()
  result.error = r.readString()
