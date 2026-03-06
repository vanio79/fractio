# Client Handler - Handles incoming client requests
# Part of the network transport layer for distributed Fractio

import std/[tables, locks, options, atomics]
import ./types
import ./serialization
import ./connection_manager
import ./tcp_transport
import ../../core/types as coretypes
import ../../utils/logging

# =============================================================================
# Handler Types
# =============================================================================

type
  BatchHandler* = proc(msg: BatchRequestMsg): BatchResponseMsg {.closure, gcsafe.}
    ## Handler for batch requests

  ScanHandler* = proc(msg: ScanRequestMsg): ScanResponseMsg {.closure, gcsafe.}
    ## Handler for scan requests

  TxnPrepareHandler* = proc(msg: TxnPrepareMsg): TxnPrepareResponseMsg {.closure, gcsafe.}
    ## Handler for transaction prepare

  TxnCommitHandler* = proc(msg: TxnCommitMsg): TxnCommitResponseMsg {.closure, gcsafe.}
    ## Handler for transaction commit

  TxnRollbackHandler* = proc(msg: TxnRollbackMsg): TxnRollbackResponseMsg {.closure, gcsafe.}
    ## Handler for transaction rollback

  ClientHandler* = ref object
    ## Handles incoming client protocol messages
    connManager*: ConnectionManager

    # Message handlers
    batchHandler*: BatchHandler
    scanHandler*: ScanHandler
    txnPrepareHandler*: TxnPrepareHandler
    txnCommitHandler*: TxnCommitHandler
    txnRollbackHandler*: TxnRollbackHandler

    # Statistics
    requestsHandled*: Atomic[int64]
    batchesHandled*: Atomic[int64]
    scansHandled*: Atomic[int64]
    txnsHandled*: Atomic[int64]
    errorsHandled*: Atomic[int64]

    # State
    running*: Atomic[bool]
    lock*: Lock

# =============================================================================
# Client Handler Implementation
# =============================================================================

proc newClientHandler*(connManager: ConnectionManager): ClientHandler =
  ## Create a new client handler
  result = ClientHandler(
    connManager: connManager,
    running: Atomic[bool]()
  )
  result.requestsHandled.store(0)
  result.batchesHandled.store(0)
  result.scansHandled.store(0)
  result.txnsHandled.store(0)
  result.errorsHandled.store(0)
  initLock(result.lock)

proc close*(handler: ClientHandler) =
  ## Close the client handler
  handler.running.store(false)
  deinitLock(handler.lock)

# =============================================================================
# Handler Registration
# =============================================================================

proc registerBatchHandler*(handler: ClientHandler, h: BatchHandler) =
  ## Register a batch request handler
  withLock handler.lock:
    handler.batchHandler = h

proc registerScanHandler*(handler: ClientHandler, h: ScanHandler) =
  ## Register a scan request handler
  withLock handler.lock:
    handler.scanHandler = h

proc registerTxnPrepareHandler*(handler: ClientHandler, h: TxnPrepareHandler) =
  ## Register a transaction prepare handler
  withLock handler.lock:
    handler.txnPrepareHandler = h

proc registerTxnCommitHandler*(handler: ClientHandler, h: TxnCommitHandler) =
  ## Register a transaction commit handler
  withLock handler.lock:
    handler.txnCommitHandler = h

proc registerTxnRollbackHandler*(handler: ClientHandler,
    h: TxnRollbackHandler) =
  ## Register a transaction rollback handler
  withLock handler.lock:
    handler.txnRollbackHandler = h

# =============================================================================
# Default Handlers
# =============================================================================

proc defaultBatchHandler(msg: BatchRequestMsg): BatchResponseMsg {.gcsafe.} =
  ## Default batch handler returns error
  result.header = newMessageHeader(
    uint16(cmtBatchResponse),
    msg.header.messageId,
    msg.header.targetNodeId,
    msg.header.sourceNodeId
  )
  result.requestId = msg.requestId
  result.success = false
  result.errorMessage = "No batch handler registered"
  result.responses = @[]

proc defaultScanHandler(msg: ScanRequestMsg): ScanResponseMsg {.gcsafe.} =
  ## Default scan handler returns error
  result.header = newMessageHeader(
    uint16(cmtScanResponse),
    msg.header.messageId,
    msg.header.targetNodeId,
    msg.header.sourceNodeId
  )
  result.requestId = msg.requestId
  result.success = false
  result.errorMessage = "No scan handler registered"
  result.keyValues = @[]
  result.hasMore = false

proc defaultTxnPrepareHandler(msg: TxnPrepareMsg): TxnPrepareResponseMsg {.gcsafe.} =
  ## Default transaction prepare handler returns abort
  result.header = newMessageHeader(
    uint16(cmtTxnPrepareResponse),
    msg.header.messageId,
    msg.header.targetNodeId,
    msg.header.sourceNodeId
  )
  result.txnId = msg.txnId
  result.vote = false
  result.errorMessage = "No transaction prepare handler registered"

proc defaultTxnCommitHandler(msg: TxnCommitMsg): TxnCommitResponseMsg {.gcsafe.} =
  ## Default transaction commit handler returns error
  result.header = newMessageHeader(
    uint16(cmtTxnCommitResponse),
    msg.header.messageId,
    msg.header.targetNodeId,
    msg.header.sourceNodeId
  )
  result.txnId = msg.txnId
  result.success = false
  result.errorMessage = "No transaction commit handler registered"

proc defaultTxnRollbackHandler(msg: TxnRollbackMsg): TxnRollbackResponseMsg {.gcsafe.} =
  ## Default transaction rollback handler returns success
  result.header = newMessageHeader(
    uint16(cmtTxnRollbackResponse),
    msg.header.messageId,
    msg.header.targetNodeId,
    msg.header.sourceNodeId
  )
  result.txnId = msg.txnId
  result.success = true

# =============================================================================
# Message Handling
# =============================================================================

proc handleBatchRequest*(handler: ClientHandler,
    data: string): string {.gcsafe.} =
  ## Handle a batch request message
  discard handler.requestsHandled.fetchAdd(1)
  discard handler.batchesHandled.fetchAdd(1)

  let msg = decodeBatchRequestMsg(data)

  var h: BatchHandler
  withLock handler.lock:
    h = if handler.batchHandler != nil: handler.batchHandler else: defaultBatchHandler

  let response = h(msg)
  result = encodeBatchResponseMsg(response)

proc handleScanRequest*(handler: ClientHandler,
    data: string): string {.gcsafe.} =
  ## Handle a scan request message
  discard handler.requestsHandled.fetchAdd(1)
  discard handler.scansHandled.fetchAdd(1)

  let msg = decodeScanRequestMsg(data)

  var h: ScanHandler
  withLock handler.lock:
    h = if handler.scanHandler != nil: handler.scanHandler else: defaultScanHandler

  let response = h(msg)
  result = encodeScanResponseMsg(response)

proc handleTxnPrepare*(handler: ClientHandler,
    data: string): string {.gcsafe.} =
  ## Handle a transaction prepare message
  discard handler.requestsHandled.fetchAdd(1)
  discard handler.txnsHandled.fetchAdd(1)

  let msg = decodeTxnPrepareMsg(data)

  var h: TxnPrepareHandler
  withLock handler.lock:
    h = if handler.txnPrepareHandler != nil: handler.txnPrepareHandler else: defaultTxnPrepareHandler

  let response = h(msg)
  result = encodeTxnPrepareResponseMsg(response)

proc handleTxnCommit*(handler: ClientHandler, data: string): string {.gcsafe.} =
  ## Handle a transaction commit message
  discard handler.requestsHandled.fetchAdd(1)
  discard handler.txnsHandled.fetchAdd(1)

  let msg = decodeTxnCommitMsg(data)

  var h: TxnCommitHandler
  withLock handler.lock:
    h = if handler.txnCommitHandler != nil: handler.txnCommitHandler else: defaultTxnCommitHandler

  let response = h(msg)
  result = encodeTxnCommitResponseMsg(response)

proc handleTxnRollback*(handler: ClientHandler,
    data: string): string {.gcsafe.} =
  ## Handle a transaction rollback message
  discard handler.requestsHandled.fetchAdd(1)
  discard handler.txnsHandled.fetchAdd(1)

  let msg = decodeTxnRollbackMsg(data)

  var h: TxnRollbackHandler
  withLock handler.lock:
    h = if handler.txnRollbackHandler !=
        nil: handler.txnRollbackHandler else: defaultTxnRollbackHandler

  let response = h(msg)
  result = encodeTxnRollbackResponseMsg(response)

proc handleHeartbeat*(handler: ClientHandler, data: string): string {.gcsafe.} =
  ## Handle a heartbeat message
  discard handler.requestsHandled.fetchAdd(1)

  let msg = decodeHeartbeatMsg(data)

  var response: HeartbeatResponseMsg
  response.header = newMessageHeader(
    uint16(cmtHeartbeatResponse),
    msg.header.messageId,
    msg.header.targetNodeId,
    msg.header.sourceNodeId
  )
  response.pong = true

  result = encodeHeartbeatResponseMsg(response)

# =============================================================================
# Setup
# =============================================================================

proc setupHandlers*(handler: ClientHandler) =
  ## Setup handlers with connection manager
  ## Registers all client message handlers with the client transport

  # Register handlers with the client transport
  handler.connManager.clientTransport.registerHandler(
    uint16(cmtBatchRequest),
    proc(data: string): string {.gcsafe.} = handler.handleBatchRequest(data)
  )

  handler.connManager.clientTransport.registerHandler(
    uint16(cmtScanRequest),
    proc(data: string): string {.gcsafe.} = handler.handleScanRequest(data)
  )

  handler.connManager.clientTransport.registerHandler(
    uint16(cmtTxnPrepare),
    proc(data: string): string {.gcsafe.} = handler.handleTxnPrepare(data)
  )

  handler.connManager.clientTransport.registerHandler(
    uint16(cmtTxnCommit),
    proc(data: string): string {.gcsafe.} = handler.handleTxnCommit(data)
  )

  handler.connManager.clientTransport.registerHandler(
    uint16(cmtTxnRollback),
    proc(data: string): string {.gcsafe.} = handler.handleTxnRollback(data)
  )

  handler.connManager.clientTransport.registerHandler(
    uint16(cmtHeartbeat),
    proc(data: string): string {.gcsafe.} = handler.handleHeartbeat(data)
  )

  handler.running.store(true)

  var fields = initTable[string, string]()
  info("Client handler started", fields)

# =============================================================================
# Statistics
# =============================================================================

type
  ClientHandlerStats* = object
    ## Statistics for the client handler
    requestsHandled*: int64
    batchesHandled*: int64
    scansHandled*: int64
    txnsHandled*: int64
    errorsHandled*: int64

proc getStats*(handler: ClientHandler): ClientHandlerStats =
  ## Get handler statistics
  result.requestsHandled = handler.requestsHandled.load()
  result.batchesHandled = handler.batchesHandled.load()
  result.scansHandled = handler.scansHandled.load()
  result.txnsHandled = handler.txnsHandled.load()
  result.errorsHandled = handler.errorsHandled.load()
