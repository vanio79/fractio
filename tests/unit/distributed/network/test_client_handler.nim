# Unit tests for client_handler.nim

import unittest
import std/[tables, atomics]

import fractio/distributed/network/types
import fractio/distributed/network/config
import fractio/distributed/network/serialization
import fractio/distributed/network/connection_manager
import fractio/distributed/network/client_handler
import fractio/distributed/network/raft_transport
import fractio/core/types as coretypes

suite "Client Handler Tests":

  test "Create client handler":
    let netConfig = newNetworkConfig(toNodeID(1), 9000)
    let connManager = newConnectionManager(netConfig)
    let handler = newClientHandler(connManager)

    check handler.connManager != nil
    check handler.requestsHandled.load() == 0
    check handler.batchesHandled.load() == 0

    handler.close()
    connManager.close()

  test "Register batch handler":
    let netConfig = newNetworkConfig(toNodeID(1), 9001)
    let connManager = newConnectionManager(netConfig)
    let handler = newClientHandler(connManager)

    var called = false
    handler.registerBatchHandler(proc(msg: BatchRequestMsg): BatchResponseMsg {.gcsafe.} =
      called = true
      result.header = newMessageHeader(uint16(cmtBatchResponse), 1, toNodeID(1),
          toNodeID(2))
      result.requestId = msg.requestId
      result.success = true
    )

    check handler.batchHandler != nil

    handler.close()
    connManager.close()

  test "Register scan handler":
    let netConfig = newNetworkConfig(toNodeID(1), 9002)
    let connManager = newConnectionManager(netConfig)
    let handler = newClientHandler(connManager)

    handler.registerScanHandler(proc(msg: ScanRequestMsg): ScanResponseMsg {.gcsafe.} =
      result.header = newMessageHeader(uint16(cmtScanResponse), 1, toNodeID(1),
          toNodeID(2))
      result.requestId = msg.requestId
      result.success = true
    )

    check handler.scanHandler != nil

    handler.close()
    connManager.close()

  test "Register transaction handlers":
    let netConfig = newNetworkConfig(toNodeID(1), 9003)
    let connManager = newConnectionManager(netConfig)
    let handler = newClientHandler(connManager)

    handler.registerTxnPrepareHandler(proc(
        msg: TxnPrepareMsg): TxnPrepareResponseMsg {.gcsafe.} =
      result.header = newMessageHeader(uint16(cmtTxnPrepareResponse), 1,
          toNodeID(1), toNodeID(2))
      result.txnId = msg.txnId
      result.vote = true
    )

    handler.registerTxnCommitHandler(proc(
        msg: TxnCommitMsg): TxnCommitResponseMsg {.gcsafe.} =
      result.header = newMessageHeader(uint16(cmtTxnCommitResponse), 1,
          toNodeID(1), toNodeID(2))
      result.txnId = msg.txnId
      result.success = true
    )

    handler.registerTxnRollbackHandler(proc(
        msg: TxnRollbackMsg): TxnRollbackResponseMsg {.gcsafe.} =
      result.header = newMessageHeader(uint16(cmtTxnRollbackResponse), 1,
          toNodeID(1), toNodeID(2))
      result.txnId = msg.txnId
      result.success = true
    )

    check handler.txnPrepareHandler != nil
    check handler.txnCommitHandler != nil
    check handler.txnRollbackHandler != nil

    handler.close()
    connManager.close()

  test "Handle batch request with no handler":
    let netConfig = newNetworkConfig(toNodeID(1), 9004)
    let connManager = newConnectionManager(netConfig)
    let handler = newClientHandler(connManager)

    # Create a batch request
    var msg: BatchRequestMsg
    msg.header = newMessageHeader(uint16(cmtBatchRequest), 1, toNodeID(2),
        toNodeID(1))
    msg.requestId = 123
    msg.rangeId = 1

    let encoded = encodeBatchRequestMsg(msg)
    let response = handler.handleBatchRequest(encoded)

    let respMsg = decodeBatchResponseMsg(response)
    check respMsg.success == false
    check respMsg.errorMessage == "No batch handler registered"
    check handler.requestsHandled.load() == 1
    check handler.batchesHandled.load() == 1

    handler.close()
    connManager.close()

  test "Handle batch request with handler":
    let netConfig = newNetworkConfig(toNodeID(1), 9005)
    let connManager = newConnectionManager(netConfig)
    let handler = newClientHandler(connManager)

    handler.registerBatchHandler(proc(msg: BatchRequestMsg): BatchResponseMsg {.gcsafe.} =
      result.header = newMessageHeader(uint16(cmtBatchResponse), msg.header.messageId,
                                       msg.header.targetNodeId,
                                           msg.header.sourceNodeId)
      result.requestId = msg.requestId
      result.success = true
      result.responses = @[]
    )

    var msg: BatchRequestMsg
    msg.header = newMessageHeader(uint16(cmtBatchRequest), 1, toNodeID(2),
        toNodeID(1))
    msg.requestId = 456
    msg.rangeId = 1

    let encoded = encodeBatchRequestMsg(msg)
    let response = handler.handleBatchRequest(encoded)

    let respMsg = decodeBatchResponseMsg(response)
    check respMsg.success == true
    check respMsg.requestId == 456

    handler.close()
    connManager.close()

  test "Handle scan request":
    let netConfig = newNetworkConfig(toNodeID(1), 9006)
    let connManager = newConnectionManager(netConfig)
    let handler = newClientHandler(connManager)

    handler.registerScanHandler(proc(msg: ScanRequestMsg): ScanResponseMsg {.gcsafe.} =
      result.header = newMessageHeader(uint16(cmtScanResponse), msg.header.messageId,
                                       msg.header.targetNodeId,
                                           msg.header.sourceNodeId)
      result.requestId = msg.requestId
      result.success = true
      result.keyValues = @[(key: "k1", value: "v1"), (key: "k2", value: "v2")]
      result.hasMore = false
    )

    var msg: ScanRequestMsg
    msg.header = newMessageHeader(uint16(cmtScanRequest), 1, toNodeID(2),
        toNodeID(1))
    msg.requestId = 789
    msg.rangeId = 1
    msg.startKey = "a"
    msg.endKey = "z"
    msg.limit = 100

    let encoded = encodeScanRequestMsg(msg)
    let response = handler.handleScanRequest(encoded)

    let respMsg = decodeScanResponseMsg(response)
    check respMsg.success == true
    check respMsg.keyValues.len == 2
    check handler.scansHandled.load() == 1

    handler.close()
    connManager.close()

  test "Handle heartbeat":
    let netConfig = newNetworkConfig(toNodeID(1), 9007)
    let connManager = newConnectionManager(netConfig)
    let handler = newClientHandler(connManager)

    var msg: HeartbeatMsg
    msg.header = newMessageHeader(uint16(cmtHeartbeat), 1, toNodeID(2),
        toNodeID(1))
    msg.ping = true

    let encoded = encodeHeartbeatMsg(msg)
    let response = handler.handleHeartbeat(encoded)

    let respMsg = decodeHeartbeatResponseMsg(response)
    check respMsg.pong == true

    handler.close()
    connManager.close()

  test "Handle transaction prepare":
    let netConfig = newNetworkConfig(toNodeID(1), 9008)
    let connManager = newConnectionManager(netConfig)
    let handler = newClientHandler(connManager)

    handler.registerTxnPrepareHandler(proc(
        msg: TxnPrepareMsg): TxnPrepareResponseMsg {.gcsafe.} =
      result.header = newMessageHeader(uint16(cmtTxnPrepareResponse), msg.header.messageId,
                                       msg.header.targetNodeId,
                                           msg.header.sourceNodeId)
      result.txnId = msg.txnId
      result.vote = true
    )

    var msg: TxnPrepareMsg
    msg.header = newMessageHeader(uint16(cmtTxnPrepare), 1, toNodeID(2),
        toNodeID(1))
    msg.txnId = 12345

    let encoded = encodeTxnPrepareMsg(msg)
    let response = handler.handleTxnPrepare(encoded)

    let respMsg = decodeTxnPrepareResponseMsg(response)
    check respMsg.vote == true
    check respMsg.txnId == 12345
    check handler.txnsHandled.load() == 1

    handler.close()
    connManager.close()

  test "Handle transaction commit":
    let netConfig = newNetworkConfig(toNodeID(1), 9009)
    let connManager = newConnectionManager(netConfig)
    let handler = newClientHandler(connManager)

    handler.registerTxnCommitHandler(proc(
        msg: TxnCommitMsg): TxnCommitResponseMsg {.gcsafe.} =
      result.header = newMessageHeader(uint16(cmtTxnCommitResponse), msg.header.messageId,
                                       msg.header.targetNodeId,
                                           msg.header.sourceNodeId)
      result.txnId = msg.txnId
      result.success = true
    )

    var msg: TxnCommitMsg
    msg.header = newMessageHeader(uint16(cmtTxnCommit), 1, toNodeID(2),
        toNodeID(1))
    msg.txnId = 12345
    msg.commitTimestamp = 100000

    let encoded = encodeTxnCommitMsg(msg)
    let response = handler.handleTxnCommit(encoded)

    let respMsg = decodeTxnCommitResponseMsg(response)
    check respMsg.success == true
    check respMsg.txnId == 12345

    handler.close()
    connManager.close()

  test "Handle transaction rollback":
    let netConfig = newNetworkConfig(toNodeID(1), 9010)
    let connManager = newConnectionManager(netConfig)
    let handler = newClientHandler(connManager)

    handler.registerTxnRollbackHandler(proc(
        msg: TxnRollbackMsg): TxnRollbackResponseMsg {.gcsafe.} =
      result.header = newMessageHeader(uint16(cmtTxnRollbackResponse), msg.header.messageId,
                                       msg.header.targetNodeId,
                                           msg.header.sourceNodeId)
      result.txnId = msg.txnId
      result.success = true
    )

    var msg: TxnRollbackMsg
    msg.header = newMessageHeader(uint16(cmtTxnRollback), 1, toNodeID(2),
        toNodeID(1))
    msg.txnId = 12345

    let encoded = encodeTxnRollbackMsg(msg)
    let response = handler.handleTxnRollback(encoded)

    let respMsg = decodeTxnRollbackResponseMsg(response)
    check respMsg.success == true
    check respMsg.txnId == 12345

    handler.close()
    connManager.close()

  test "Get handler statistics":
    let netConfig = newNetworkConfig(toNodeID(1), 9011)
    let connManager = newConnectionManager(netConfig)
    let handler = newClientHandler(connManager)

    # Handle some messages
    discard handler.handleHeartbeat(encodeHeartbeatMsg(HeartbeatMsg(
      header: newMessageHeader(uint16(cmtHeartbeat), 1, toNodeID(2), toNodeID(1)),
      ping: true
    )))

    discard handler.handleBatchRequest(encodeBatchRequestMsg(BatchRequestMsg(
      header: newMessageHeader(uint16(cmtBatchRequest), 2, toNodeID(2),
          toNodeID(1)),
      requestId: 1
    )))

    let stats = handler.getStats()
    check stats.requestsHandled == 2
    check stats.batchesHandled == 1

    handler.close()
    connManager.close()
