# Transaction message encoding/decoding for the Fractio wire protocol.
#
# Implements BeginTxn (0x0200), CommitTxn (0x0201), RollbackTxn (0x0202),
# and TxnStatus (0x0203).
#
# Wire formats use ULID-based TransactionID (16 bytes).
# All encode procs emit a 2-byte MessageType prefix followed by fields.
# The caller wraps the result in a Frame via frame.encodeFrame.

import ../types
import ../codec
import ../../core/types as coreTypes

# ---------------------------------------------------------------------------
# Status constants (shared across Commit, Rollback, TxnStatus responses)
# ---------------------------------------------------------------------------

const
  # Commit response statuses
  TxnCommitOK* = 0x00'u8         ## successfully committed
  TxnCommitConflict* = 0x01'u8   ## aborted — write conflict
  TxnCommitTimeout* = 0x02'u8    ## aborted — timed out
  TxnCommitNotFound* = 0x03'u8   ## no such transaction

  # Rollback response statuses
  TxnRollbackOK* = 0x00'u8       ## successfully rolled back
  TxnRollbackNotFound* = 0x01'u8 ## no such transaction

  # TxnStatus response statuses  (also used internally by TransactionManager)
  TxnStatusActive* = 0x00'u8     ## transaction is still open
  TxnStatusCommitted* = 0x01'u8  ## committed
  TxnStatusAborted* = 0x02'u8    ## aborted / rolled back
  TxnStatusNotFound* = 0x03'u8   ## no such transaction

  # BeginTxn flags
  TxnFlagReadOnly* = 0x01'u8
  TxnFlagSerializable* = 0x02'u8

# ---------------------------------------------------------------------------
# BeginTxn  (0x0200)
#
# Request:
#   Flags   (1 byte)
#   Timeout (4 bytes uint32 BE): milliseconds; 0 = server default
#
# Response:
#   TxnId         (16 bytes ULID)
#   ReadTimestamp (8 bytes uint64): MVCC snapshot timestamp
# ---------------------------------------------------------------------------

type
  BeginTxnRequest* = object
    flags*: uint8
    timeoutMs*: uint32

  BeginTxnResponse* = object
    txnId*: TransactionID
    readTimestamp*: uint64

proc encodeBeginTxnRequest*(req: BeginTxnRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtBeginTxn))
  buf.writeUint8(req.flags)
  buf.writeUint32BE(req.timeoutMs)
  buf

proc decodeBeginTxnRequest*(payload: string): Result[BeginTxnRequest,
    ProtocolError] =
  var pos = 2 # skip MessageType
  var req: BeginTxnRequest

  let flagsR = readUint8(payload, pos)
  if flagsR.isErr: return peErr(flagsR.error)
  req.flags = flagsR.value

  let toR = readUint32BE(payload, pos)
  if toR.isErr: return peErr(toR.error)
  req.timeoutMs = toR.value

  peOk(req)

proc encodeBeginTxnResponse*(resp: BeginTxnResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtBeginTxn))
  buf.add(transactionIDToBytes(resp.txnId))
  buf.writeUint64BE(resp.readTimestamp)
  buf

proc decodeBeginTxnResponse*(payload: string): Result[BeginTxnResponse,
    ProtocolError] =
  var pos = 2
  var resp: BeginTxnResponse

  # Read 16-byte ULID
  if pos + 16 > payload.len:
    return peErr(newProtocolError(peBoundsOverflow,
        "BeginTxnResponse: txnId truncated"))
  let txnBytes = payload[pos ..< pos + 16]
  resp.txnId = transactionIDFromBytes(txnBytes)
  pos += 16

  let tsR = readUint64BE(payload, pos)
  if tsR.isErr: return peErr(tsR.error)
  resp.readTimestamp = tsR.value

  peOk(resp)

# ---------------------------------------------------------------------------
# TxnKeepalive  (0x0204)
#
# Sent by the client to confirm that a transaction is still alive.
# The client should send this every 1 second if the transaction has not
# performed any KV operations in the last second.
#
# Request:
#   TxnId   (16 bytes ULID)
#
# Response:
#   Status  (1 byte): 0x00 = OK, 0x01 = Not Found
# ---------------------------------------------------------------------------

const
  TxnKeepaliveOK* = 0x00'u8
  TxnKeepaliveNotFound* = 0x01'u8

type
  TxnKeepaliveRequest* = object
    txnId*: TransactionID

  TxnKeepaliveResponse* = object
    status*: uint8

proc encodeTxnKeepaliveRequest*(req: TxnKeepaliveRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtTxnKeepalive))
  buf.add(transactionIDToBytes(req.txnId))
  buf

proc decodeTxnKeepaliveRequest*(payload: string): Result[TxnKeepaliveRequest,
    ProtocolError] =
  var pos = 2
  if pos + 16 > payload.len:
    return peErr(newProtocolError(peBoundsOverflow,
        "TxnKeepaliveRequest: txnId truncated"))
  let txnBytes = payload[pos ..< pos + 16]
  peOk(TxnKeepaliveRequest(txnId: transactionIDFromBytes(txnBytes)))

proc encodeTxnKeepaliveResponse*(resp: TxnKeepaliveResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtTxnKeepalive))
  buf.writeUint8(resp.status)
  buf

proc decodeTxnKeepaliveResponse*(payload: string): Result[TxnKeepaliveResponse,
    ProtocolError] =
  var pos = 2
  var resp: TxnKeepaliveResponse

  let statusR = readUint8(payload, pos)
  if statusR.isErr: return peErr(statusR.error)
  resp.status = statusR.value

  peOk(resp)

# ---------------------------------------------------------------------------
# CommitTxn  (0x0201)
#
# Request:
#   TxnId (16 bytes ULID)
#
# Response:
#   Status          (1 byte): TxnCommit*
#   CommitTimestamp (8 bytes uint64): valid only when Status == TxnCommitOK
# ---------------------------------------------------------------------------

type
  CommitTxnRequest* = object
    txnId*: TransactionID

  CommitTxnResponse* = object
    status*: uint8
    commitTimestamp*: uint64

proc encodeCommitTxnRequest*(req: CommitTxnRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtCommitTxn))
  buf.add(transactionIDToBytes(req.txnId))
  buf

proc decodeCommitTxnRequest*(payload: string): Result[CommitTxnRequest,
    ProtocolError] =
  var pos = 2
  # Read 16-byte ULID
  if pos + 16 > payload.len:
    return peErr(newProtocolError(peBoundsOverflow,
        "CommitTxnRequest: txnId truncated"))
  let txnBytes = payload[pos ..< pos + 16]
  peOk(CommitTxnRequest(txnId: transactionIDFromBytes(txnBytes)))

proc encodeCommitTxnResponse*(resp: CommitTxnResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtCommitTxn))
  buf.writeUint8(resp.status)
  buf.writeUint64BE(resp.commitTimestamp)
  buf

proc decodeCommitTxnResponse*(payload: string): Result[CommitTxnResponse,
    ProtocolError] =
  var pos = 2
  var resp: CommitTxnResponse

  let statusR = readUint8(payload, pos)
  if statusR.isErr: return peErr(statusR.error)
  resp.status = statusR.value

  let tsR = readUint64BE(payload, pos)
  if tsR.isErr: return peErr(tsR.error)
  resp.commitTimestamp = tsR.value

  peOk(resp)

# ---------------------------------------------------------------------------
# RollbackTxn  (0x0202)
#
# Request:
#   TxnId (16 bytes ULID)
#
# Response:
#   Status (1 byte): TxnRollback*
# ---------------------------------------------------------------------------

type
  RollbackTxnRequest* = object
    txnId*: TransactionID

  RollbackTxnResponse* = object
    status*: uint8

proc encodeRollbackTxnRequest*(req: RollbackTxnRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtRollbackTxn))
  buf.add(transactionIDToBytes(req.txnId))
  buf

proc decodeRollbackTxnRequest*(payload: string): Result[RollbackTxnRequest,
    ProtocolError] =
  var pos = 2
  # Read 16-byte ULID
  if pos + 16 > payload.len:
    return peErr(newProtocolError(peBoundsOverflow,
        "RollbackTxnRequest: txnId truncated"))
  let txnBytes = payload[pos ..< pos + 16]
  peOk(RollbackTxnRequest(txnId: transactionIDFromBytes(txnBytes)))

proc encodeRollbackTxnResponse*(resp: RollbackTxnResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtRollbackTxn))
  buf.writeUint8(resp.status)
  buf

proc decodeRollbackTxnResponse*(payload: string): Result[RollbackTxnResponse,
    ProtocolError] =
  var pos = 2
  let r = readUint8(payload, pos)
  if r.isErr: return peErr(r.error)
  peOk(RollbackTxnResponse(status: r.value))

# ---------------------------------------------------------------------------
# TxnStatus  (0x0203)
#
# Request:
#   TxnId (16 bytes ULID)
#
# Response:
#   Status          (1 byte): TxnStatus*
#   CommitTimestamp (8 bytes uint64): valid only when Status == TxnStatusCommitted
# ---------------------------------------------------------------------------

type
  TxnStatusRequest* = object
    txnId*: TransactionID

  TxnStatusResponse* = object
    status*: uint8
    commitTimestamp*: uint64

proc encodeTxnStatusRequest*(req: TxnStatusRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtTxnStatus))
  buf.add(transactionIDToBytes(req.txnId))
  buf

proc decodeTxnStatusRequest*(payload: string): Result[TxnStatusRequest,
    ProtocolError] =
  var pos = 2
  # Read 16-byte ULID
  if pos + 16 > payload.len:
    return peErr(newProtocolError(peBoundsOverflow,
        "TxnStatusRequest: txnId truncated"))
  let txnBytes = payload[pos ..< pos + 16]
  peOk(TxnStatusRequest(txnId: transactionIDFromBytes(txnBytes)))

proc encodeTxnStatusResponse*(resp: TxnStatusResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtTxnStatus))
  buf.writeUint8(resp.status)
  buf.writeUint64BE(resp.commitTimestamp)
  buf

proc decodeTxnStatusResponse*(payload: string): Result[TxnStatusResponse,
    ProtocolError] =
  var pos = 2
  var resp: TxnStatusResponse

  let statusR = readUint8(payload, pos)
  if statusR.isErr: return peErr(statusR.error)
  resp.status = statusR.value

  let tsR = readUint64BE(payload, pos)
  if tsR.isErr: return peErr(tsR.error)
  resp.commitTimestamp = tsR.value

  peOk(resp)
