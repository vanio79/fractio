# Unit tests for Two-Phase Commit (2PC) module

import unittest
import std/[times, options, strutils]
import fractio/core/types
import fractio/core/transaction
import fractio/core/timestamp_provider
import fractio/core/two_phase_commit
import fractio/storage/mvcc/types
import fractio/storage/mvcc/engine

# Constants
const
  INVALID_TIMESTAMP* = Timestamp(0)
  MAX_TIMESTAMP* = high(Timestamp)
  DEFAULT_PRIORITY* = 500
  DEFAULT_MAX_OFFSET_NS* = 100_000_000 # 100ms

suite "Two-Phase Commit - Configuration":
  test "default configuration":
    let config = newTwoPCConfig()

    check config.prepareTimeoutMs == DEFAULT_PREPARE_TIMEOUT_MS
    check config.commitTimeoutMs == DEFAULT_COMMIT_TIMEOUT_MS
    check config.maxRetries == DEFAULT_2PC_MAX_RETRIES
    check config.enableRecovery == true
    check config.recoveryCheckIntervalMs == DEFAULT_RECOVERY_CHECK_INTERVAL_MS
    check config.heartbeatIntervalMs == DEFAULT_HEARTBEAT_INTERVAL_MS
    check config.enableRaft == false

suite "Two-Phase Commit - Transaction ID Generation":
  test "generate unique transaction IDs":
    # Note: generateTransactionId requires a SharedTimer which we can't easily mock
    # So we'll just test that TransactionID type works correctly
    let txnId1 = genTransactionIDLocal()
    let txnId2 = genTransactionIDLocal()

    # TransactionID is a distinct ULID, check it's not zero
    check txnId1 != zeroTransactionID()
    check txnId2 != zeroTransactionID()
    check txnId1 != txnId2

  test "transaction IDs are unique":
    let txnId1 = genTransactionIDLocal()
    let txnId2 = genTransactionIDLocal()

    check txnId1 != txnId2

suite "Two-Phase Commit - Request ID Generation":
  test "generate unique request IDs":
    let reqId1 = generateRequestId()
    let reqId2 = generateRequestId()

    check reqId1 != ""
    check reqId2 != ""
    check reqId1 != reqId2
    check reqId1.startsWith("req_")
    check reqId2.startsWith("req_")

  test "request IDs contain timestamp":
    let reqId = generateRequestId()
    let parts = reqId.split('_')
    check parts.len >= 2
    # Second part should be a timestamp
    let timestamp = parseInt(parts[1])
    check timestamp > 0

suite "Two-Phase Commit - Coordinator":
  test "create coordinator":
    var txn = MVCCTransaction(
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let coord = newCoordinator(txn, "node1")

    check coord.transaction == txn
    check coord.transactionId == txn.id
    check coord.coordinatorId == "node1"
    check coord.state == tpcsIdle
    check coord.participants.len == 0
    check coord.retryCount == 0

  test "add participants to coordinator":
    var txn = MVCCTransaction(
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let coord = newCoordinator(txn, "node1")

    coord.addParticipant("node2", "127.0.0.1:8000", nil)
    coord.addParticipant("node3", "127.0.0.1:8001", nil)

    check coord.participants.len == 2
    check coord.quorum == 2 # Majority of 2 is 2

    coord.addParticipant("node4", "127.0.0.1:8002", nil)
    check coord.participants.len == 3
    check coord.quorum == 2 # Majority of 3 is 2

  test "quorum calculation":
    var txn = MVCCTransaction(
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let coord = newCoordinator(txn, "node1")

    # 1 participant
    coord.addParticipant("node2", "127.0.0.1:8000", nil)
    check coord.quorum == 1

    # 2 participants
    coord.addParticipant("node3", "127.0.0.1:8001", nil)
    check coord.quorum == 2

    # 3 participants
    coord.addParticipant("node4", "127.0.0.1:8002", nil)
    check coord.quorum == 2

    # 4 participants
    coord.addParticipant("node5", "127.0.0.1:8003", nil)
    check coord.quorum == 3

    # 5 participants
    coord.addParticipant("node6", "127.0.0.1:8004", nil)
    check coord.quorum == 3

suite "Two-Phase Commit - Participant":
  test "create participant":
    let participant = newParticipant("node1", "127.0.0.1:8000")

    check participant.nodeId == "node1"
    check participant.endpoint == "127.0.0.1:8000"
    check participant.state == tpcsIdle
    check participant.vote == pvAbstain
    check participant.transactionId == zeroTransactionID()

  test "handle prepare request":
    let participant = newParticipant("node1", "127.0.0.1:8000")

    let request = TwoPCRequest(
      requestId: "req_123",
      requestType: tpcPrepare,
      transactionId: genTransactionIDLocal(),
      coordinatorId: "coord1",
      timestamp: Timestamp(1000),
      data: "test data",
      participantEndpoints: @["node1:8000", "node2:8001"]
    )

    let response = participant.handlePrepareRequest(request)

    check response.requestId == request.requestId
    check response.transactionId == request.transactionId
    check response.participantId == "node1"
    check response.vote == pvYes
    check response.state == tpcsPrepared
    check participant.state == tpcsPrepared

  test "handle commit request":
    let participant = newParticipant("node1", "127.0.0.1:8000")

    let request = TwoPCRequest(
      requestId: "req_123",
      requestType: tpcCommit,
      transactionId: genTransactionIDLocal(),
      coordinatorId: "coord1",
      timestamp: Timestamp(1000),
      data: "test data",
      participantEndpoints: @[]
    )

    let response = participant.handleCommitRequest(request)

    check response.requestId == request.requestId
    check response.state == tpcsCommitted
    check participant.state == tpcsCommitted

  test "handle rollback request":
    let participant = newParticipant("node1", "127.0.0.1:8000")

    let request = TwoPCRequest(
      requestId: "req_123",
      requestType: tpcRollback,
      transactionId: genTransactionIDLocal(),
      coordinatorId: "coord1",
      timestamp: Timestamp(1000),
      data: "test data",
      participantEndpoints: @[]
    )

    let response = participant.handleRollbackRequest(request)

    check response.requestId == request.requestId
    check response.state == tpcsAborted
    check participant.state == tpcsAborted

  test "handle heartbeat":
    let participant = newParticipant("node1", "127.0.0.1:8000")

    let request = TwoPCRequest(
      requestId: "req_123",
      requestType: tpcHeartbeat,
      transactionId: genTransactionIDLocal(),
      coordinatorId: "coord1",
      timestamp: Timestamp(1000),
      data: "test data",
      participantEndpoints: @[]
    )

    let response = participant.handleHeartbeat(request)

    check response.requestId == request.requestId
    check participant.lastHeartbeat == Timestamp(1000)

suite "Two-Phase Commit - Response Processing":
  test "process prepare responses with quorum":
    var txn = MVCCTransaction(
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let coord = newCoordinator(txn, "node1")
    coord.addParticipant("node2", "127.0.0.1:8000", nil)
    coord.addParticipant("node3", "127.0.0.1:8001", nil)

    let responses = @[
      TwoPCResponse(
        requestId: "req_1",
        transactionId: genTransactionIDLocal(),
        participantId: "node2",
        vote: pvYes,
        state: tpcsPrepared,
        error: ""
      ),
      TwoPCResponse(
        requestId: "req_1",
        transactionId: genTransactionIDLocal(),
        participantId: "node3",
        vote: pvYes,
        state: tpcsPrepared,
        error: ""
      )
    ]

    let quorumAchieved = coord.processPrepareResponses(responses)

    check quorumAchieved == true
    check coord.preparedCount == 2
    check coord.abortedCount == 0
    check coord.votes.len == 2

  test "process prepare responses without quorum":
    var txn = MVCCTransaction(
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let coord = newCoordinator(txn, "node1")
    coord.addParticipant("node2", "127.0.0.1:8000", nil)
    coord.addParticipant("node3", "127.0.0.1:8001", nil)
    coord.addParticipant("node4", "127.0.0.1:8002", nil)

    let responses = @[
      TwoPCResponse(
        requestId: "req_1",
        transactionId: genTransactionIDLocal(),
        participantId: "node2",
        vote: pvYes,
        state: tpcsPrepared,
        error: ""
      ),
      TwoPCResponse(
        requestId: "req_1",
        transactionId: genTransactionIDLocal(),
        participantId: "node3",
        vote: pvNo,
        state: tpcsIdle,
        error: "Conflict detected"
      ),
      TwoPCResponse(
        requestId: "req_1",
        transactionId: genTransactionIDLocal(),
        participantId: "node4",
        vote: pvNo,
        state: tpcsIdle,
        error: "Conflict detected"
      )
    ]

    let quorumAchieved = coord.processPrepareResponses(responses)

    check quorumAchieved == false
    check coord.preparedCount == 1
    check coord.abortedCount == 2
    check coord.votes.len == 3

suite "Two-Phase Commit - Timeout Handling":
  test "check timeout in prepare phase":
    var txn = MVCCTransaction(
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let coord = newCoordinator(txn, "node1")
    coord.state = tpcsPreparing
    coord.startTime = Timestamp(100)
    coord.prepareTimeout = 10_000 # 10 seconds

    # Not timed out yet
    check coord.checkTimeout(Timestamp(100 + 5_000)) == false

    # Timed out
    check coord.checkTimeout(Timestamp(100 + 10_000)) == true
    check coord.checkTimeout(Timestamp(100 + 15_000)) == true

  test "check timeout in commit phase":
    var txn = MVCCTransaction(
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let coord = newCoordinator(txn, "node1")
    coord.state = tpcsCommitting
    coord.startTime = Timestamp(100)
    coord.commitTimeout = 5_000 # 5 seconds

    # Not timed out yet
    check coord.checkTimeout(Timestamp(100 + 3_000)) == false

    # Timed out
    check coord.checkTimeout(Timestamp(100 + 5_000)) == true

  test "handle timeout in prepare phase":
    var txn = MVCCTransaction(
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let coord = newCoordinator(txn, "node1")
    coord.state = tpcsPreparing

    let result = coord.handleTimeout()

    check result.success == false
    check result.error == "Prepare phase timeout"
    check result.retryable == true
    check coord.state == tpcsAborted

  test "handle timeout in commit phase":
    var txn = MVCCTransaction(
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let coord = newCoordinator(txn, "node1")
    coord.state = tpcsCommitting

    let result = coord.handleTimeout()

    check result.success == false
    check result.error == "Commit phase timeout - recovery needed"
    check result.retryable == false
    check coord.state == tpcsRecovering

suite "Two-Phase Commit - Recovery":
  test "recover transaction":
    var txn = MVCCTransaction(
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let coord = newCoordinator(txn, "node1")
    let result = coord.recoverTransaction(genTransactionIDLocal())

    check result.success == true
    check coord.state == tpcsIdle

  test "participant check recovery":
    let participant = newParticipant("node1", "127.0.0.1:8000")
    let config = newTwoPCConfig()

    let result = participant.checkRecovery("coord1", config)

    check result.success == true

suite "Two-Phase Commit - Serialization":
  test "serialize and deserialize request":
    let request = TwoPCRequest(
      requestId: "req_123",
      requestType: tpcPrepare,
      transactionId: genTransactionIDLocal(),
      coordinatorId: "coord1",
      timestamp: Timestamp(1000),
      data: "test data",
      participantEndpoints: @["node1:8000", "node2:8001"]
    )

    let encoded = encodeTwoPCRequest(request)
    let decoded = decodeTwoPCRequest(encoded)

    check decoded.requestId == request.requestId
    check decoded.requestType == request.requestType
    check decoded.transactionId == request.transactionId
    check decoded.coordinatorId == request.coordinatorId
    check decoded.timestamp == request.timestamp
    check decoded.data == request.data
    check decoded.participantEndpoints == request.participantEndpoints

  test "serialize and deserialize response":
    let response = TwoPCResponse(
      requestId: "req_123",
      transactionId: genTransactionIDLocal(),
      participantId: "node1",
      vote: pvYes,
      state: tpcsPrepared,
      error: ""
    )

    let encoded = encodeTwoPCResponse(response)
    let decoded = decodeTwoPCResponse(encoded)

    check decoded.requestId == response.requestId
    check decoded.transactionId == response.transactionId
    check decoded.participantId == response.participantId
    check decoded.vote == response.vote
    check decoded.state == response.state
    check decoded.error == response.error

suite "Two-Phase Commit - Error Handling":
  test "create 2PC error":
    let err = newTwoPCError(tpekTimeout, "Prepare phase timeout")

    check err.msg == "Prepare phase timeout"
    check err.errorKind == tpekTimeout
    check err.participantId == ""

  test "create 2PC error with participant":
    let err = newTwoPCError(tpekParticipantFailed,
      "Participant failed", "node1")

    check err.msg == "Participant failed"
    check err.errorKind == tpekParticipantFailed
    check err.participantId == "node1"

suite "Two-Phase Commit - Result Types":
  test "create successful result":
    let txnId = genTransactionIDLocal()
    let result = TwoPCResult(
      success: true,
      transactionId: txnId,
      commitTimestamp: Timestamp(1000),
      participants: @["node1", "node2", "node3"],
      error: "",
      retryable: false
    )

    check result.success == true
    check result.transactionId == txnId
    check result.commitTimestamp == Timestamp(1000)
    check result.participants.len == 3
    check result.error == ""
    check result.retryable == false

  test "create failed result with retry":
    let txnId = genTransactionIDLocal()
    let result = TwoPCResult(
      success: false,
      transactionId: txnId,
      commitTimestamp: INVALID_TIMESTAMP,
      participants: @[],
      error: "Prepare phase failed",
      retryable: true
    )

    check result.success == false
    check result.error == "Prepare phase failed"
    check result.retryable == true

  test "create failed result without retry":
    let txnId = genTransactionIDLocal()
    let result = TwoPCResult(
      success: false,
      transactionId: txnId,
      commitTimestamp: INVALID_TIMESTAMP,
      participants: @[],
      error: "Commit phase failed",
      retryable: false
    )

    check result.success == false
    check result.error == "Commit phase failed"
    check result.retryable == false

suite "Two-Phase Commit - State Transitions":
  test "coordinator state transitions":
    var txn = MVCCTransaction(
      id: genTransactionIDLocal(),
      status: TXN_PENDING,
      startTimestamp: Timestamp(100),
      commitTimestamp: INVALID_TIMESTAMP,
      priority: DEFAULT_PRIORITY,
      maxTimestamp: MAX_TIMESTAMP,
      deadline: MAX_TIMESTAMP,
      createdAt: Timestamp(100),
      writeSet: WriteSet(entries: @[]),
      readSet: ReadSet(keys: @[], timestamps: @[]),
      lockedKeys: 0,
      epoch: 0
    )

    let coord = newCoordinator(txn, "node1")

    # Initial state
    check coord.state == tpcsIdle

    # After prepare
    coord.state = tpcsPreparing
    check coord.state == tpcsPreparing

    # After prepared
    coord.state = tpcsPrepared
    check coord.state == tpcsPrepared

    # After commit
    coord.state = tpcsCommitting
    check coord.state == tpcsCommitting

    # After committed
    coord.state = tpcsCommitted
    check coord.state == tpcsCommitted

  test "participant state transitions":
    let participant = newParticipant("node1", "127.0.0.1:8000")

    # Initial state
    check participant.state == tpcsIdle

    # After prepare
    participant.state = tpcsPreparing
    check participant.state == tpcsPreparing

    # After prepared
    participant.state = tpcsPrepared
    check participant.state == tpcsPrepared

    # After commit
    participant.state = tpcsCommitting
    check participant.state == tpcsCommitting

    # After committed
    participant.state = tpcsCommitted
    check participant.state == tpcsCommitted

suite "Two-Phase Commit - Vote Types":
  test "vote enum values":
    let yesVote = pvYes
    let noVote = pvNo
    let abstainVote = pvAbstain

    check yesVote == pvYes
    check noVote == pvNo
    check abstainVote == pvAbstain

suite "Two-Phase Commit - Request Types":
  test "request type enum values":
    let prepare = tpcPrepare
    let commit = tpcCommit
    let rollback = tpcRollback
    let recovery = tpcRecovery
    let heartbeat = tpcHeartbeat

    check prepare == tpcPrepare
    check commit == tpcCommit
    check rollback == tpcRollback
    check recovery == tpcRecovery
    check heartbeat == tpcHeartbeat
