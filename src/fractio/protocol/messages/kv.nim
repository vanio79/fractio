# KV operation message encoding/decoding for the Fractio wire protocol.
#
# Implements Get, Put, Delete, Batch, and Scan messages.
# Every encode proc produces a complete payload with a 2-byte MessageType prefix.
# The caller wraps the payload in a Frame via frame.encodeFrame.
#
# Wire format changes for ULID-based IDs:
# - GroupID: 16-byte ULIDs. When the GroupRouted flag is set,
#   the groupId is appended as 16 raw bytes (no length prefix).
# - TransactionID: 16-byte ULIDs appended after flags.

import std/[options, strutils]
import ../types
import ../codec
import ../../core/types
import ../../distributed/raft/group_types
import ../../sql/data_row

# ---------------------------------------------------------------------------
# Wire-encoded Filter Expressions (forward declarations)
# ---------------------------------------------------------------------------
# These types are used in GetRequest and ScanRequest for server-side filtering.
# Full definitions are in the Wire-encoded Filter Expressions section below.

type
  WireExprKind* = enum
    wekLiteral = 0
    wekColumn = 1
    wekBinOp = 2
    wekUnaryOp = 3
    wekIsNull = 4
    wekBetween = 5
    wekLike = 6

  WireBinOp* = enum
    wboEq = 0, wboNeq = 1, wboLt = 2, wboLte = 3, wboGt = 4, wboGte = 5
    wboAnd = 6, wboOr = 7

  WireUnaryOp* = enum
    wuoNot = 0, wuoNeg = 1

  WireDataType* = enum
    wdtInt = 0, wdtFloat = 1, wdtString = 2, wdtBool = 3, wdtNull = 4

  WireFilterExpr* = ref object
    ## Wire-encoded filter expression for server-side filtering.
    ## Full definition in Wire-encoded Filter Expressions section below.
    case kind*: WireExprKind
    of wekLiteral:
      litDataType*: WireDataType
      litIntVal*: int64
      litFloatVal*: float64
      litStringVal*: string
      litBoolVal*: bool
    of wekColumn:
      colName*: string
    of wekBinOp:
      binOpKind*: WireBinOp
      binLeft*: WireFilterExpr
      binRight*: WireFilterExpr
    of wekUnaryOp:
      unaryOpKind*: WireUnaryOp
      unaryExpr*: WireFilterExpr
    of wekIsNull:
      isNullExpr*: WireFilterExpr
      isNullNot*: bool
    of wekBetween:
      betweenExpr*: WireFilterExpr
      betweenLo*: WireFilterExpr
      betweenHi*: WireFilterExpr
      betweenNot*: bool
    of wekLike:
      likeExpr*: WireFilterExpr
      likePattern*: WireFilterExpr
      likeNot*: bool

# Forward declarations for encode/decode functions (defined later in file)
proc encodeWireFilterExpr*(expr: WireFilterExpr, buf: var string) {.raises: [], gcsafe.}
proc decodeWireFilterExpr*(payload: string, pos: var int): Result[
    WireFilterExpr, ProtocolError] {.raises: [], gcsafe.}

# ---------------------------------------------------------------------------
# Get  (0x0100)
#
# Request:
#   Flags (1 byte):    bit0=IncludeTimestamp  bit1=IncludeVersion
#                      bit4=GroupRouted        bit5=HasFilter
#   TxnId (16 bytes ULID)
#   ReadTimestamp (8 bytes, 0 for latest)
#   Key (uint32-prefixed)
#   GroupId (16 bytes, if GroupRouted flag set)
#   Filter (nested WireFilterExpr, if HasFilter flag set)
#
# Response (found):
#   Flags (1 byte):    bit0=Found  bit1=HasTimestamp  bit2=HasVersion
#   Timestamp (8 bytes, if bit1 set)
#   Version (8 bytes, if bit2 set)
#   Value (uint32-prefixed)
#
# Response (not found):
#   Flags (1 byte): 0x00
# ---------------------------------------------------------------------------

const
  GetFlagIncludeTimestamp* = 0x01'u8
  GetFlagIncludeVersion* = 0x02'u8
  GetFlagGroupRouted* = 0x10'u8 ## groupId appended after key
  GetFlagHasFilter* = 0x20'u8   ## serialized filter appended for server-side filtering

  GetRespFlagFound* = 0x01'u8
  GetRespFlagHasTimestamp* = 0x02'u8
  GetRespFlagHasVersion* = 0x04'u8

type
  GetRequest* = object
    flags*: uint8
    txnId*: TransactionID
    readTimestamp*: uint64
    key*: string
    groupId*: GroupID ## non-zero when GroupRouted flag is set
    filter*: Option[WireFilterExpr] ## serialized filter for server-side filtering (PointGet optimization)

  GetResponse* = object
    found*: bool
    timestamp*: uint64 ## valid when found and HasTimestamp was requested
    version*: uint64   ## valid when found and HasVersion was requested
    hasTimestamp*: bool
    hasVersion*: bool
    value*: string

proc encodeGetRequest*(req: GetRequest): string {.raises: [], gcsafe.} =
  var buf = ""
  buf.writeUint16BE(uint16(mtGet))
  var flags = req.flags
  if req.groupId != ZeroGroupID(): flags = flags or GetFlagGroupRouted
  if req.filter.isSome: flags = flags or GetFlagHasFilter
  buf.writeUint8(flags)
  buf.add(transactionIDToBytes(req.txnId))
  buf.writeUint64BE(req.readTimestamp)
  buf.writeBytes(req.key)
  if req.groupId != ZeroGroupID():
    buf.add(ulidToBytes(groupIDToULID(req.groupId)))
  if req.filter.isSome:
    encodeWireFilterExpr(req.filter.get(), buf)
  buf

proc decodeGetRequest*(payload: string): Result[GetRequest,
    ProtocolError] {.raises: [], gcsafe.} =
  var pos = 2 # skip MessageType
  var req: GetRequest
  let flagsR = readUint8(payload, pos)
  if flagsR.isErr: return peErr(flagsR.error)
  req.flags = flagsR.value

  # Read 16-byte ULID for txnId
  if pos + ULID_SIZE > payload.len:
    return peErr(newProtocolError(peBoundsOverflow,
        "payload too short for txnId ULID"))
  let txnBytes = payload[pos ..< pos + ULID_SIZE]
  req.txnId = transactionIDFromBytes(txnBytes)
  pos += ULID_SIZE

  let tsR = readUint64BE(payload, pos)
  if tsR.isErr: return peErr(tsR.error)
  req.readTimestamp = tsR.value

  let keyR = readBytes(payload, pos)
  if keyR.isErr: return peErr(keyR.error)
  req.key = keyR.value

  if (req.flags and GetFlagGroupRouted) != 0:
    if pos + ULID_SIZE > payload.len:
      return peErr(newProtocolError(peBoundsOverflow,
          "payload too short for groupId ULID"))
    let ulidBytes = payload[pos ..< pos + ULID_SIZE]
    req.groupId = GroupID(ulidFromBytes(ulidBytes))
    pos += ULID_SIZE

  if (req.flags and GetFlagHasFilter) != 0:
    let filterR = decodeWireFilterExpr(payload, pos)
    if filterR.isErr: return peErr(filterR.error)
    req.filter = some(filterR.value)

  peOk(req)

proc encodeGetResponse*(resp: GetResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtGet))
  if not resp.found:
    buf.writeUint8(0x00'u8)
    return buf
  var flags = GetRespFlagFound
  if resp.hasTimestamp: flags = flags or GetRespFlagHasTimestamp
  if resp.hasVersion: flags = flags or GetRespFlagHasVersion
  buf.writeUint8(flags)
  if resp.hasTimestamp: buf.writeUint64BE(resp.timestamp)
  if resp.hasVersion: buf.writeUint64BE(resp.version)
  buf.writeBytes(resp.value)
  buf

proc decodeGetResponse*(payload: string): Result[GetResponse, ProtocolError] =
  var pos = 2 # skip MessageType
  var resp: GetResponse
  let flagsR = readUint8(payload, pos)
  if flagsR.isErr: return peErr(flagsR.error)
  let flags = flagsR.value

  resp.found = (flags and GetRespFlagFound) != 0
  resp.hasTimestamp = (flags and GetRespFlagHasTimestamp) != 0
  resp.hasVersion = (flags and GetRespFlagHasVersion) != 0

  if not resp.found:
    return peOk(resp)

  if resp.hasTimestamp:
    let r = readUint64BE(payload, pos)
    if r.isErr: return peErr(r.error)
    resp.timestamp = r.value

  if resp.hasVersion:
    let r = readUint64BE(payload, pos)
    if r.isErr: return peErr(r.error)
    resp.version = r.value

  let valR = readBytes(payload, pos)
  if valR.isErr: return peErr(valR.error)
  resp.value = valR.value

  peOk(resp)

# ---------------------------------------------------------------------------
# Put  (0x0101)
#
# Request:
#   Flags (1 byte):  bit0=ReturnPrev  bit1=SyncWrite  bit2=CAS
#   TxnId (16 bytes ULID)
#   ExpectedVersion (8 bytes, for CAS; 0 otherwise)
#   Key (uint32-prefixed)
#   Value (uint32-prefixed)
#   GroupId (16 bytes, if GroupRouted flag set)
#
# Response:
#   Status (1 byte):  0x00=OK  0x01=CASFailed  0x02=TxnAborted
#   Timestamp (8 bytes)
#   Version (8 bytes)
#   PreviousValue (uint32-prefixed, present only if ReturnPrev was set and
#                  previous value existed; length 0 means absent)
# ---------------------------------------------------------------------------

const
  PutFlagReturnPrev* = 0x01'u8
  PutFlagSyncWrite* = 0x02'u8
  PutFlagCAS* = 0x04'u8
  PutFlagGroupRouted* = 0x10'u8    ## groupId appended after value
  PutFlagMigrationWrite* = 0x20'u8 ## migration write: skip routing validation

  PutStatusOK* = 0x00'u8
  PutStatusCASFailed* = 0x01'u8
  PutStatusTxnAborted* = 0x02'u8

type
  PutRequest* = object
    flags*: uint8
    txnId*: TransactionID
    expectedVersion*: uint64
    key*: string
    value*: string
    groupId*: GroupID ## non-zero when GroupRouted flag is set

  PutResponse* = object
    status*: uint8
    timestamp*: uint64
    version*: uint64
    hasPreviousValue*: bool
    previousValue*: string


proc encodeRawPutRequest*(req: PutRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtRawPut))
  var flags = req.flags
  if req.groupId != ZeroGroupID(): flags = flags or PutFlagGroupRouted
  buf.writeUint8(flags)
  buf.add(transactionIDToBytes(req.txnId))
  buf.writeUint64BE(req.expectedVersion)
  buf.writeBytes(req.key)
  buf.writeBytes(req.value)
  if req.groupId != ZeroGroupID():
    buf.add(ulidToBytes(groupIDToULID(req.groupId)))
  buf

proc encodePutRequest*(req: PutRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtPut))
  var flags = req.flags
  if req.groupId != ZeroGroupID(): flags = flags or PutFlagGroupRouted
  buf.writeUint8(flags)
  buf.add(transactionIDToBytes(req.txnId))
  buf.writeUint64BE(req.expectedVersion)
  buf.writeBytes(req.key)
  buf.writeBytes(req.value)
  if req.groupId != ZeroGroupID():
    buf.add(ulidToBytes(groupIDToULID(req.groupId)))
  buf

proc decodePutRequest*(payload: string): Result[PutRequest, ProtocolError] =
  var pos = 2
  var req: PutRequest
  let flagsR = readUint8(payload, pos)
  if flagsR.isErr: return peErr(flagsR.error)
  req.flags = flagsR.value

  # Read 16-byte ULID for txnId
  if pos + ULID_SIZE > payload.len:
    return peErr(newProtocolError(peBoundsOverflow,
        "payload too short for txnId ULID"))
  let txnBytes = payload[pos ..< pos + ULID_SIZE]
  req.txnId = transactionIDFromBytes(txnBytes)
  pos += ULID_SIZE

  let evR = readUint64BE(payload, pos)
  if evR.isErr: return peErr(evR.error)
  req.expectedVersion = evR.value

  let keyR = readBytes(payload, pos)
  if keyR.isErr: return peErr(keyR.error)
  req.key = keyR.value

  let valR = readBytes(payload, pos)
  if valR.isErr: return peErr(valR.error)
  req.value = valR.value

  if (req.flags and PutFlagGroupRouted) != 0:
    if pos + ULID_SIZE > payload.len:
      return peErr(newProtocolError(peBoundsOverflow,
          "payload too short for groupId ULID"))
    let ulidBytes = payload[pos ..< pos + ULID_SIZE]
    req.groupId = GroupID(ulidFromBytes(ulidBytes))

  peOk(req)

proc encodePutResponse*(resp: PutResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtPut))
  buf.writeUint8(resp.status)
  buf.writeUint64BE(resp.timestamp)
  buf.writeUint64BE(resp.version)
  # PreviousValue: always write the uint32-prefixed field; length 0 = absent
  if resp.hasPreviousValue:
    buf.writeBytes(resp.previousValue)
  else:
    buf.writeUint32BE(0'u32)
  buf

proc decodePutResponse*(payload: string): Result[PutResponse, ProtocolError] =
  var pos = 2
  var resp: PutResponse
  let statusR = readUint8(payload, pos)
  if statusR.isErr: return peErr(statusR.error)
  resp.status = statusR.value

  let tsR = readUint64BE(payload, pos)
  if tsR.isErr: return peErr(tsR.error)
  resp.timestamp = tsR.value

  let verR = readUint64BE(payload, pos)
  if verR.isErr: return peErr(verR.error)
  resp.version = verR.value

  let prevR = readBytes(payload, pos)
  if prevR.isErr: return peErr(prevR.error)
  if prevR.value.len > 0:
    resp.hasPreviousValue = true
    resp.previousValue = prevR.value

  peOk(resp)

# ---------------------------------------------------------------------------
# Delete  (0x0102)
#
# Request:
#   Flags (1 byte):  bit0=ReturnPrev  bit1=SyncWrite  bit2=OnlyIfExists
#   TxnId (16 bytes ULID)
#   Key (uint32-prefixed)
#   GroupId (16 bytes, if GroupRouted flag set)
#
# Response:
#   Status (1 byte):  0x00=Deleted  0x01=NotFound  0x02=TxnAborted
#   PreviousValue (uint32-prefixed, 0-length if absent)
# ---------------------------------------------------------------------------

const
  DelFlagReturnPrev* = 0x01'u8
  DelFlagSyncWrite* = 0x02'u8
  DelFlagOnlyIfExists* = 0x04'u8
  DelFlagGroupRouted* = 0x10'u8 ## groupId appended after key

  DelStatusDeleted* = 0x00'u8
  DelStatusNotFound* = 0x01'u8
  DelStatusTxnAborted* = 0x02'u8

type
  DeleteRequest* = object
    flags*: uint8
    txnId*: TransactionID
    key*: string
    groupId*: GroupID ## non-zero when GroupRouted flag is set

  DeleteResponse* = object
    status*: uint8
    hasPreviousValue*: bool
    previousValue*: string

proc encodeDeleteRequest*(req: DeleteRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtDelete))
  var flags = req.flags
  if req.groupId != ZeroGroupID(): flags = flags or DelFlagGroupRouted
  buf.writeUint8(flags)
  buf.add(transactionIDToBytes(req.txnId))
  buf.writeBytes(req.key)
  if req.groupId != ZeroGroupID():
    buf.add(ulidToBytes(groupIDToULID(req.groupId)))
  buf

proc decodeDeleteRequest*(payload: string): Result[DeleteRequest,
    ProtocolError] =
  var pos = 2
  var req: DeleteRequest
  let flagsR = readUint8(payload, pos)
  if flagsR.isErr: return peErr(flagsR.error)
  req.flags = flagsR.value

  # Read 16-byte ULID for txnId
  if pos + ULID_SIZE > payload.len:
    return peErr(newProtocolError(peBoundsOverflow,
        "payload too short for txnId ULID"))
  let txnBytes = payload[pos ..< pos + ULID_SIZE]
  req.txnId = transactionIDFromBytes(txnBytes)
  pos += ULID_SIZE

  let keyR = readBytes(payload, pos)
  if keyR.isErr: return peErr(keyR.error)
  req.key = keyR.value

  if (req.flags and DelFlagGroupRouted) != 0:
    if pos + ULID_SIZE > payload.len:
      return peErr(newProtocolError(peBoundsOverflow,
          "payload too short for groupId ULID"))
    let ulidBytes = payload[pos ..< pos + ULID_SIZE]
    req.groupId = GroupID(ulidFromBytes(ulidBytes))

  peOk(req)

proc encodeDeleteResponse*(resp: DeleteResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtDelete))
  buf.writeUint8(resp.status)
  if resp.hasPreviousValue:
    buf.writeBytes(resp.previousValue)
  else:
    buf.writeUint32BE(0'u32)
  buf

proc decodeDeleteResponse*(payload: string): Result[DeleteResponse,
    ProtocolError] =
  var pos = 2
  var resp: DeleteResponse
  let statusR = readUint8(payload, pos)
  if statusR.isErr: return peErr(statusR.error)
  resp.status = statusR.value

  let prevR = readBytes(payload, pos)
  if prevR.isErr: return peErr(prevR.error)
  if prevR.value.len > 0:
    resp.hasPreviousValue = true
    resp.previousValue = prevR.value

  peOk(resp)

# ---------------------------------------------------------------------------
# Batch  (0x0103)
#
# A single-shard atomic batch.  Each BatchOp carries an opcode (Get/Put/Delete),
# per-op flags, and the serialised op payload (key-only for Get/Delete, key+value
# for Put).
#
# Request:
#   Flags (1 byte):  bit0=AllOrNothing  bit1=ContinueOnError
#   TxnId (16 bytes ULID)
#   OpCount (4 bytes)
#   For each op:
#     OpType (1 byte):  0x00=Get  0x01=Put  0x02=Delete
#     OpFlags (1 byte)
#     OpData (uint32-prefixed)
#
# Response:
#   Status (1 byte):  0x00=AllOK  0x01=PartialFailure  0x02=AllFailed
#   ResultCount (4 bytes)
#   For each result:
#     Status (1 byte)
#     ResultData (uint32-prefixed)
# ---------------------------------------------------------------------------

const
  BatchFlagAllOrNothing* = 0x01'u8
  BatchFlagContinueOnErr* = 0x02'u8

  BatchOpGet* = 0x00'u8
  BatchOpPut* = 0x01'u8
  BatchOpDelete* = 0x02'u8

  BatchStatusAllOK* = 0x00'u8
  BatchStatusPartialFailure* = 0x01'u8
  BatchStatusAllFailed* = 0x02'u8

type
  BatchOp* = object
    kind*: uint8  ## BatchOpGet / BatchOpPut / BatchOpDelete
    flags*: uint8
    data*: string ## serialised op payload

  BatchOpResult* = object
    status*: uint8
    data*: string ## serialised result payload (may be empty)

  BatchRequest* = object
    flags*: uint8
    txnId*: TransactionID
    operations*: seq[BatchOp]

  BatchResponse* = object
    status*: uint8
    results*: seq[BatchOpResult]

proc encodeBatchRequest*(req: BatchRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtBatch))
  buf.writeUint8(req.flags)
  buf.add(transactionIDToBytes(req.txnId))
  buf.writeUint32BE(uint32(req.operations.len))
  for op in req.operations:
    buf.writeUint8(op.kind)
    buf.writeUint8(op.flags)
    buf.writeBytes(op.data)
  buf

proc decodeBatchRequest*(payload: string): Result[BatchRequest, ProtocolError] =
  var pos = 2
  var req: BatchRequest

  let flagsR = readUint8(payload, pos)
  if flagsR.isErr: return peErr(flagsR.error)
  req.flags = flagsR.value

  # Read 16-byte ULID for txnId
  if pos + ULID_SIZE > payload.len:
    return peErr(newProtocolError(peBoundsOverflow,
        "payload too short for txnId ULID"))
  let txnBytes = payload[pos ..< pos + ULID_SIZE]
  req.txnId = transactionIDFromBytes(txnBytes)
  pos += ULID_SIZE

  let cntR = readUint32BE(payload, pos)
  if cntR.isErr: return peErr(cntR.error)
  let count = int(cntR.value)
  if count > MAX_BATCH_OPS:
    return peErr(newProtocolError(peInvalidFrame,
      "batch op count exceeds MAX_BATCH_OPS"))

  req.operations = newSeq[BatchOp](count)
  for i in 0 ..< count:
    let kindR = readUint8(payload, pos)
    if kindR.isErr: return peErr(kindR.error)
    req.operations[i].kind = kindR.value

    let opFlagsR = readUint8(payload, pos)
    if opFlagsR.isErr: return peErr(opFlagsR.error)
    req.operations[i].flags = opFlagsR.value

    let dataR = readBytes(payload, pos)
    if dataR.isErr: return peErr(dataR.error)
    req.operations[i].data = dataR.value

  peOk(req)

proc encodeBatchResponse*(resp: BatchResponse): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtBatch))
  buf.writeUint8(resp.status)
  buf.writeUint32BE(uint32(resp.results.len))
  for r in resp.results:
    buf.writeUint8(r.status)
    buf.writeBytes(r.data)
  buf

proc decodeBatchResponse*(payload: string): Result[BatchResponse,
    ProtocolError] =
  var pos = 2
  var resp: BatchResponse
  let statusR = readUint8(payload, pos)
  if statusR.isErr: return peErr(statusR.error)
  resp.status = statusR.value

  let cntR = readUint32BE(payload, pos)
  if cntR.isErr: return peErr(cntR.error)
  let count = int(cntR.value)

  resp.results = newSeq[BatchOpResult](count)
  for i in 0 ..< count:
    let rsR = readUint8(payload, pos)
    if rsR.isErr: return peErr(rsR.error)
    resp.results[i].status = rsR.value

    let dataR = readBytes(payload, pos)
    if dataR.isErr: return peErr(dataR.error)
    resp.results[i].data = dataR.value

  peOk(resp)

# ---------------------------------------------------------------------------
# Scan  (0x0104)
#
# Request:
#   Flags (1 byte):  bit0=IncludeTimestamp  bit1=IncludeVersion
#                    bit2=KeysOnly          bit3=Reverse
#                    bit4=GroupRouted       bit5=Streaming
#   TxnId (16 bytes ULID)
#   ReadTimestamp (8 bytes)
#   StartKey (uint32-prefixed, 0 length = beginning of keyspace)
#   EndKey (uint32-prefixed, 0 length = end of keyspace)
#   Limit (4 bytes, 0 = no limit)
#   ChunkSize (4 bytes, if Streaming flag set - items per frame)
#
# Response frame (one or more frames for streaming):
#   Flags (1 byte):  bit0=HasMore  bit1=EndOfScan
#   Count (4 bytes): number of KV pairs in this frame
#   For each pair:
#     Key (uint32-prefixed)
#     Value (uint32-prefixed, empty if KeysOnly)
#     Timestamp (8 bytes, if IncludeTimestamp)
#     Version (8 bytes, if IncludeVersion)
# ---------------------------------------------------------------------------

const
  ScanFlagIncludeTimestamp* = 0x01'u8
  ScanFlagIncludeVersion* = 0x02'u8
  ScanFlagKeysOnly* = 0x04'u8
  ScanFlagReverse* = 0x08'u8
  ScanFlagGroupRouted* = 0x10'u8  ## groupId appended for routing filter
  ScanFlagStreaming* = 0x20'u8    ## streaming scan - multiple frames
  ScanFlagHasFilter* = 0x40'u8    ## serialized filter appended for server-side filtering
  ScanFlagHasColumns* = 0x80'u8 ## serialized column-name list appended for server-side column projection

  ScanRespFlagHasMore* = 0x01'u8
  ScanRespFlagEndOfScan* = 0x02'u8

  DEFAULT_SCAN_CHUNK_SIZE* = 1000 ## Items per streaming frame

# ---------------------------------------------------------------------------
# Wire-encoded Filter Expressions
# ---------------------------------------------------------------------------
#
# Filter expressions are serialized and sent to the server for server-side
# filtering during scans. This reduces network traffic by filtering rows
# before they're sent back to the client.
#
# Wire format:
#   ExprKind (1 byte): literal, column, binOp, unaryOp, isNull, between, like
#   For literal:
#     DataType (1 byte): int, float, string, bool
#     Value: int(8 bytes), float(8 bytes), string(uint32+len), bool(1 byte)
#   For column:
#     ColName (uint32-prefixed)
#   For binOp:
#     BinOpKind (1 byte): eq, neq, lt, lte, gt, gte, and, or
#     LeftExpr (nested)
#     RightExpr (nested)
#   For unaryOp:
#     UnaryOpKind (1 byte): not, neg
#     Expr (nested)
#   For isNull:
#     IsNot (1 byte): 0=is null, 1=is not null
#     Expr (nested)
#   For between:
#     IsNot (1 byte): 0=between, 1=not between
#     Expr (nested)
#     LoExpr (nested)
#     HiExpr (nested)
#   For like:
#     IsNot (1 byte): 0=like, 1=not like
#     Expr (nested)
#     Pattern (nested literal)
#

const
  ScanFilterExprKind* = 0x40'u8 ## Filter expression follows scan request

# ---------------------------------------------------------------------------
# WireFilterExpr encoding/decoding
# ---------------------------------------------------------------------------

proc encodeWireFilterExpr*(expr: WireFilterExpr, buf: var string) {.raises: [], gcsafe.} =
  ## Encode a WireFilterExpr to the buffer. Uses nested encoding for
  ## sub-expressions.
  buf.writeUint8(uint8(expr.kind))
  case expr.kind
  of wekLiteral:
    buf.writeUint8(uint8(expr.litDataType))
    case expr.litDataType
    of wdtInt:
      buf.writeInt64BE(expr.litIntVal)
    of wdtFloat:
      # Encode float as 8 bytes (IEEE 754 binary64)
      var floatBytes: array[8, uint8]
      var f = expr.litFloatVal
      {.cast(uncheckedAssign).}:
        for i in 0 ..< 8:
          floatBytes[i] = uint8((cast[uint64](f) shr ((7 - i) * 8)) and 0xFF)
      for b in floatBytes:
        buf.writeUint8(b)
    of wdtString:
      buf.writeBytes(expr.litStringVal)
    of wdtBool:
      buf.writeUint8(if expr.litBoolVal: 1'u8 else: 0'u8)
    of wdtNull:
      discard # No additional data for null
  of wekColumn:
    buf.writeBytes(expr.colName)
  of wekBinOp:
    buf.writeUint8(uint8(expr.binOpKind))
    encodeWireFilterExpr(expr.binLeft, buf)
    encodeWireFilterExpr(expr.binRight, buf)
  of wekUnaryOp:
    buf.writeUint8(uint8(expr.unaryOpKind))
    encodeWireFilterExpr(expr.unaryExpr, buf)
  of wekIsNull:
    buf.writeUint8(if expr.isNullNot: 1'u8 else: 0'u8)
    encodeWireFilterExpr(expr.isNullExpr, buf)
  of wekBetween:
    buf.writeUint8(if expr.betweenNot: 1'u8 else: 0'u8)
    encodeWireFilterExpr(expr.betweenExpr, buf)
    encodeWireFilterExpr(expr.betweenLo, buf)
    encodeWireFilterExpr(expr.betweenHi, buf)
  of wekLike:
    buf.writeUint8(if expr.likeNot: 1'u8 else: 0'u8)
    encodeWireFilterExpr(expr.likeExpr, buf)
    encodeWireFilterExpr(expr.likePattern, buf)

proc encodeWireFilterExpr*(expr: WireFilterExpr): string {.raises: [], gcsafe.} =
  ## Encode a WireFilterExpr to a standalone string buffer.
  result = ""
  encodeWireFilterExpr(expr, result)

proc decodeWireFilterExpr*(payload: string, pos: var int): Result[
    WireFilterExpr, ProtocolError] {.raises: [], gcsafe.} =
  ## Decode a WireFilterExpr from the payload at the given position.
  ## Advances pos as bytes are read.
  ## Creates new objects for each case variant to avoid case transition issues.

  let kindR = readUint8(payload, pos)
  if kindR.isErr: return peErr(kindR.error)
  let kind = WireExprKind(kindR.value)

  case kind
  of wekLiteral:
    let dtR = readUint8(payload, pos)
    if dtR.isErr: return peErr(dtR.error)
    let litDataType = WireDataType(dtR.value)

    case litDataType
    of wdtInt:
      let valR = readInt64BE(payload, pos)
      if valR.isErr: return peErr(valR.error)
      peOk(WireFilterExpr(kind: wekLiteral, litDataType: wdtInt,
          litIntVal: valR.value))
    of wdtFloat:
      let valR = readUint64BE(payload, pos)
      if valR.isErr: return peErr(valR.error)
      peOk(WireFilterExpr(kind: wekLiteral, litDataType: wdtFloat,
          litFloatVal: cast[float64](valR.value)))
    of wdtString:
      let valR = readBytes(payload, pos)
      if valR.isErr: return peErr(valR.error)
      peOk(WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
          litStringVal: valR.value))
    of wdtBool:
      let valR = readUint8(payload, pos)
      if valR.isErr: return peErr(valR.error)
      peOk(WireFilterExpr(kind: wekLiteral, litDataType: wdtBool,
          litBoolVal: valR.value == 1))
    of wdtNull:
      peOk(WireFilterExpr(kind: wekLiteral, litDataType: wdtNull))

  of wekColumn:
    let nameR = readBytes(payload, pos)
    if nameR.isErr: return peErr(nameR.error)
    peOk(WireFilterExpr(kind: wekColumn, colName: nameR.value))

  of wekBinOp:
    let opR = readUint8(payload, pos)
    if opR.isErr: return peErr(opR.error)
    let binOpKind = WireBinOp(opR.value)

    let leftR = decodeWireFilterExpr(payload, pos)
    if leftR.isErr: return peErr(leftR.error)

    let rightR = decodeWireFilterExpr(payload, pos)
    if rightR.isErr: return peErr(rightR.error)

    peOk(WireFilterExpr(kind: wekBinOp, binOpKind: binOpKind,
        binLeft: leftR.value, binRight: rightR.value))

  of wekUnaryOp:
    let opR = readUint8(payload, pos)
    if opR.isErr: return peErr(opR.error)
    let unaryOpKind = WireUnaryOp(opR.value)

    let innerR = decodeWireFilterExpr(payload, pos)
    if innerR.isErr: return peErr(innerR.error)

    peOk(WireFilterExpr(kind: wekUnaryOp, unaryOpKind: unaryOpKind,
        unaryExpr: innerR.value))

  of wekIsNull:
    let notR = readUint8(payload, pos)
    if notR.isErr: return peErr(notR.error)
    let isNullNot = notR.value == 1

    let innerR = decodeWireFilterExpr(payload, pos)
    if innerR.isErr: return peErr(innerR.error)

    peOk(WireFilterExpr(kind: wekIsNull, isNullExpr: innerR.value,
        isNullNot: isNullNot))

  of wekBetween:
    let notR = readUint8(payload, pos)
    if notR.isErr: return peErr(notR.error)
    let betweenNot = notR.value == 1

    let exprR = decodeWireFilterExpr(payload, pos)
    if exprR.isErr: return peErr(exprR.error)

    let loR = decodeWireFilterExpr(payload, pos)
    if loR.isErr: return peErr(loR.error)

    let hiR = decodeWireFilterExpr(payload, pos)
    if hiR.isErr: return peErr(hiR.error)

    peOk(WireFilterExpr(kind: wekBetween, betweenExpr: exprR.value,
        betweenLo: loR.value, betweenHi: hiR.value, betweenNot: betweenNot))

  of wekLike:
    let notR = readUint8(payload, pos)
    if notR.isErr: return peErr(notR.error)
    let likeNot = notR.value == 1

    let exprR = decodeWireFilterExpr(payload, pos)
    if exprR.isErr: return peErr(exprR.error)

    let patR = decodeWireFilterExpr(payload, pos)
    if patR.isErr: return peErr(patR.error)

    peOk(WireFilterExpr(kind: wekLike, likeExpr: exprR.value,
        likePattern: patR.value, likeNot: likeNot))

# ---------------------------------------------------------------------------
# WireFilterExpr evaluation (server-side)
# ---------------------------------------------------------------------------

proc evalWireFilterExprValue(expr: WireFilterExpr, row: DataRow): DataRowValue =
  ## Evaluate a WireFilterExpr against a DataRow, returning a DataRowValue.
  ## Used for server-side filter evaluation during scans.
  case expr.kind
  of wekLiteral:
    case expr.litDataType
    of wdtInt: newRowValue(expr.litIntVal)
    of wdtFloat: newRowValue(expr.litFloatVal)
    of wdtString: newRowValue(expr.litStringVal)
    of wdtBool: newRowValue(expr.litBoolVal)
    of wdtNull: newRowValue()

  of wekColumn:
    if row.hasColumn(expr.colName):
      row[expr.colName]
    else:
      newRowValue()

  of wekBinOp:
    let left = evalWireFilterExprValue(expr.binLeft, row)
    let right = evalWireFilterExprValue(expr.binRight, row)

    case expr.binOpKind
    of wboEq: newRowValue(left == right)
    of wboNeq: newRowValue(left != right)
    of wboLt: newRowValue(left < right)
    of wboLte: newRowValue(left <= right)
    of wboGt: newRowValue(left > right)
    of wboGte: newRowValue(left >= right)
    of wboAnd:
      let leftBool = left.kind == drvkBool and left.boolVal
      let rightBool = right.kind == drvkBool and right.boolVal
      newRowValue(leftBool and rightBool)
    of wboOr:
      let leftBool = left.kind == drvkBool and left.boolVal
      let rightBool = right.kind == drvkBool and right.boolVal
      newRowValue(leftBool or rightBool)

  of wekUnaryOp:
    let inner = evalWireFilterExprValue(expr.unaryExpr, row)
    case expr.unaryOpKind
    of wuoNot:
      let innerBool = inner.kind == drvkBool and inner.boolVal
      newRowValue(not innerBool)
    of wuoNeg:
      case inner.kind
      of drvkInt: newRowValue(-inner.intVal)
      of drvkFloat: newRowValue(-inner.floatVal)
      else: newRowValue()

  of wekIsNull:
    let inner = evalWireFilterExprValue(expr.isNullExpr, row)
    let isNull = inner.kind == drvkNull
    newRowValue(if expr.isNullNot: not isNull else: isNull)

  of wekBetween:
    let val = evalWireFilterExprValue(expr.betweenExpr, row)
    let lo = evalWireFilterExprValue(expr.betweenLo, row)
    let hi = evalWireFilterExprValue(expr.betweenHi, row)
    var inRange = val >= lo and val <= hi
    newRowValue(if expr.betweenNot: not inRange else: inRange)

  of wekLike:
    # Simple LIKE: handle % wildcard at start/end
    let val = evalWireFilterExprValue(expr.likeExpr, row)
    let pat = evalWireFilterExprValue(expr.likePattern, row)
    if val.kind == drvkString and pat.kind == drvkString:
      let s = val.strVal
      let p = pat.strVal
      var matches = false
      if p.startsWith("%") and p.endsWith("%"):
        matches = p[1..^2] in s
      elif p.startsWith("%"):
        matches = s.endsWith(p[1..^1])
      elif p.endsWith("%"):
        matches = s.startsWith(p[0..^2])
      else:
        matches = s == p
      newRowValue(if expr.likeNot: not matches else: matches)
    else:
      newRowValue()

proc matchesWireFilter*(filter: Option[WireFilterExpr], row: DataRow): bool =
  ## Check if a DataRow passes the server-side wire filter.
  ## Returns true if no filter, or if the row matches the filter.
  if filter.isNone:
    return true
  let result = evalWireFilterExprValue(filter.get(), row)
  result.kind == drvkBool and result.boolVal

proc matchesWireFilterWithDecodedValue*(filter: Option[WireFilterExpr],
    value: string): bool =
  ## Check if a value (binary-encoded DataRow) passes the wire filter.
  ## Returns true if no filter or if decoding/filter evaluation succeeds.
  if filter.isNone:
    return true
  try:
    let row = decodeDataRow(value)
    return matchesWireFilter(filter, row)
  except ValueError:
    # If decoding fails, pass the row through (let client filter)
    return true

type
  ScanRequest* = object
    flags*: uint8
    txnId*: TransactionID
    readTimestamp*: uint64
    startKey*: string ## empty = beginning of keyspace
    endKey*: string   ## empty = end of keyspace
    limit*: uint32    ## 0 = no limit
    groupId*: GroupID ## non-zero when GroupRouted flag is set - for server-side routing filter
    chunkSize*: uint32 ## items per frame for streaming (0 = DEFAULT_SCAN_CHUNK_SIZE)
    filter*: Option[WireFilterExpr] ## serialized filter for server-side filtering
    columns*: Option[seq[string]] ## column names to project (server-side projection)

  ScanPair* = object
    key*: string
    value*: string ## empty when ScanFlagKeysOnly is set
    timestamp*: uint64
    version*: uint64

  ScanResponseFrame* = object
    ## One streaming chunk of Scan results.
    respFlags*: uint8 ## ScanRespFlag* bits
    pairs*: seq[ScanPair]
    ## Mirror of the request flags so encode/decode know which optional
    ## fields are present in each pair.
    reqFlags*: uint8

  StreamingScanResult* = object
    ## Result of a streaming scan operation - tracks state across frames
    streamId*: uint32   ## Unique stream identifier
    hasMore*: bool      ## More frames available
    exhausted*: bool    ## All data received
    error*: Option[ProtocolError]
    totalReceived*: int ## Total pairs received across all frames

proc encodeScanRequest*(req: ScanRequest): string =
  var buf = ""
  var flags = req.flags
  if req.groupId != ZeroGroupID():
    flags = flags or ScanFlagGroupRouted
  if req.filter.isSome:
    flags = flags or ScanFlagHasFilter
  if req.columns.isSome and req.columns.get().len > 0:
    flags = flags or ScanFlagHasColumns
  buf.writeUint16BE(uint16(mtScan))
  buf.writeUint8(flags)
  buf.add(transactionIDToBytes(req.txnId))
  buf.writeUint64BE(req.readTimestamp)
  buf.writeBytes(req.startKey)
  buf.writeBytes(req.endKey)
  buf.writeUint32BE(req.limit)
  if req.groupId != ZeroGroupID():
    buf.add(ulidToBytes(groupIDToULID(req.groupId)))
  # Include chunkSize for streaming scans
  if (flags and ScanFlagStreaming) != 0:
    buf.writeUint32BE(req.chunkSize)
  # Include filter for server-side filtering
  if req.filter.isSome:
    encodeWireFilterExpr(req.filter.get(), buf)
  # Include column projection list (server-side projection of DataRow columns)
  if req.columns.isSome and req.columns.get().len > 0:
    let cols = req.columns.get()
    buf.writeUint32BE(uint32(cols.len))
    for c in cols:
      buf.writeBytes(c)
  buf

proc decodeScanRequest*(payload: string): Result[ScanRequest, ProtocolError] =
  var pos = 2
  var req: ScanRequest
  let flagsR = readUint8(payload, pos)
  if flagsR.isErr: return peErr(flagsR.error)
  req.flags = flagsR.value

  # Read 16-byte ULID for txnId
  if pos + ULID_SIZE > payload.len:
    return peErr(newProtocolError(peBoundsOverflow,
        "payload too short for txnId ULID"))
  let txnBytes = payload[pos ..< pos + ULID_SIZE]
  req.txnId = transactionIDFromBytes(txnBytes)
  pos += ULID_SIZE

  let tsR = readUint64BE(payload, pos)
  if tsR.isErr: return peErr(tsR.error)
  req.readTimestamp = tsR.value

  let skR = readBytes(payload, pos)
  if skR.isErr: return peErr(skR.error)
  req.startKey = skR.value

  let ekR = readBytes(payload, pos)
  if ekR.isErr: return peErr(ekR.error)
  req.endKey = ekR.value

  let limR = readUint32BE(payload, pos)
  if limR.isErr: return peErr(limR.error)
  req.limit = limR.value

  # Read groupId if GroupRouted flag is set
  if (req.flags and ScanFlagGroupRouted) != 0:
    if pos + ULID_SIZE > payload.len:
      return peErr(newProtocolError(peBoundsOverflow,
          "payload too short for groupId ULID"))
    let gidBytes = payload[pos ..< pos + ULID_SIZE]
    req.groupId = GroupID(ulidFromBytes(gidBytes))
    pos += ULID_SIZE

  # Read chunkSize if Streaming flag is set
  if (req.flags and ScanFlagStreaming) != 0:
    let chunkR = readUint32BE(payload, pos)
    if chunkR.isErr: return peErr(chunkR.error)
    req.chunkSize = chunkR.value
    if req.chunkSize == 0:
      req.chunkSize = DEFAULT_SCAN_CHUNK_SIZE

  # Read filter if HasFilter flag is set
  if (req.flags and ScanFlagHasFilter) != 0:
    let filterR = decodeWireFilterExpr(payload, pos)
    if filterR.isErr: return peErr(filterR.error)
    req.filter = some(filterR.value)

  # Read column projection list if HasColumns flag is set
  if (req.flags and ScanFlagHasColumns) != 0:
    let cntR = readUint32BE(payload, pos)
    if cntR.isErr: return peErr(cntR.error)
    let ncols = int(cntR.value)
    var cols = newSeq[string](ncols)
    for i in 0 ..< ncols:
      let cR = readBytes(payload, pos)
      if cR.isErr: return peErr(cR.error)
      cols[i] = cR.value
    req.columns = some(cols)

  peOk(req)

proc encodeScanResponseFrame*(rf: ScanResponseFrame): string =
  ## `rf.reqFlags` must mirror the original ScanRequest.flags so this proc
  ## knows whether to emit Timestamp / Version fields per pair.
  var buf = ""
  buf.writeUint16BE(uint16(mtScan))
  buf.writeUint8(rf.respFlags)
  buf.writeUint32BE(uint32(rf.pairs.len))
  let inclTs = (rf.reqFlags and ScanFlagIncludeTimestamp) != 0
  let inclVer = (rf.reqFlags and ScanFlagIncludeVersion) != 0
  let keysOnly = (rf.reqFlags and ScanFlagKeysOnly) != 0
  for p in rf.pairs:
    buf.writeBytes(p.key)
    if keysOnly:
      buf.writeUint32BE(0'u32) # empty value placeholder
    else:
      buf.writeBytes(p.value)
    if inclTs: buf.writeUint64BE(p.timestamp)
    if inclVer: buf.writeUint64BE(p.version)
  buf

proc decodeScanResponseFrame*(payload: string,
    reqFlags: uint8): Result[ScanResponseFrame, ProtocolError] =
  var pos = 2
  var rf: ScanResponseFrame
  rf.reqFlags = reqFlags

  let rFlagsR = readUint8(payload, pos)
  if rFlagsR.isErr: return peErr(rFlagsR.error)
  rf.respFlags = rFlagsR.value

  let cntR = readUint32BE(payload, pos)
  if cntR.isErr: return peErr(cntR.error)
  let count = int(cntR.value)

  let inclTs = (reqFlags and ScanFlagIncludeTimestamp) != 0
  let inclVer = (reqFlags and ScanFlagIncludeVersion) != 0

  rf.pairs = newSeq[ScanPair](count)
  for i in 0 ..< count:
    let kR = readBytes(payload, pos)
    if kR.isErr: return peErr(kR.error)
    rf.pairs[i].key = kR.value

    let vR = readBytes(payload, pos)
    if vR.isErr: return peErr(vR.error)
    rf.pairs[i].value = vR.value

    if inclTs:
      let r = readUint64BE(payload, pos)
      if r.isErr: return peErr(r.error)
      rf.pairs[i].timestamp = r.value

    if inclVer:
      let r = readUint64BE(payload, pos)
      if r.isErr: return peErr(r.error)
      rf.pairs[i].version = r.value

  peOk(rf)
