# KV operation message encoding/decoding for the Fractio wire protocol.
#
# Implements Get, Put, Delete, Batch, and Scan messages.
# Every encode proc produces a complete payload with a 2-byte MessageType prefix.
# The caller wraps the payload in a Frame via frame.encodeFrame.
#
# Wire formats match protocol_design.md §4.2 exactly.

import ../types
import ../codec

# ---------------------------------------------------------------------------
# Get  (0x0100)
#
# Request:
#   Flags (1 byte):    bit0=IncludeTimestamp  bit1=IncludeVersion
#   TxnId (8 bytes)
#   ReadTimestamp (8 bytes, 0 for latest)
#   Key (uint32-prefixed)
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
  GetFlagGroupRouted* = 0x10'u8  ## groupId appended after key

  GetRespFlagFound* = 0x01'u8
  GetRespFlagHasTimestamp* = 0x02'u8
  GetRespFlagHasVersion* = 0x04'u8

type
  GetRequest* = object
    flags*: uint8
    txnId*: uint64
    readTimestamp*: uint64
    key*: string
    groupId*: uint64  ## non-zero when GroupRouted flag is set

  GetResponse* = object
    found*: bool
    timestamp*: uint64 ## valid when found and HasTimestamp was requested
    version*: uint64   ## valid when found and HasVersion was requested
    hasTimestamp*: bool
    hasVersion*: bool
    value*: string

proc encodeGetRequest*(req: GetRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtGet))
  var flags = req.flags
  if req.groupId != 0: flags = flags or GetFlagGroupRouted
  buf.writeUint8(flags)
  buf.writeUint64BE(req.txnId)
  buf.writeUint64BE(req.readTimestamp)
  buf.writeBytes(req.key)
  if req.groupId != 0:
    buf.writeUint64BE(req.groupId)
  buf

proc decodeGetRequest*(payload: string): Result[GetRequest, ProtocolError] =
  var pos = 2 # skip MessageType
  var req: GetRequest
  let flagsR = readUint8(payload, pos)
  if flagsR.isErr: return peErr(flagsR.error)
  req.flags = flagsR.value

  let txnR = readUint64BE(payload, pos)
  if txnR.isErr: return peErr(txnR.error)
  req.txnId = txnR.value

  let tsR = readUint64BE(payload, pos)
  if tsR.isErr: return peErr(tsR.error)
  req.readTimestamp = tsR.value

  let keyR = readBytes(payload, pos)
  if keyR.isErr: return peErr(keyR.error)
  req.key = keyR.value

  if (req.flags and GetFlagGroupRouted) != 0:
    let gidR = readUint64BE(payload, pos)
    if gidR.isErr: return peErr(gidR.error)
    req.groupId = gidR.value

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
#   TxnId (8 bytes)
#   ExpectedVersion (8 bytes, for CAS; 0 otherwise)
#   Key (uint32-prefixed)
#   Value (uint32-prefixed)
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
  PutFlagGroupRouted* = 0x10'u8  ## groupId appended after value

  PutStatusOK* = 0x00'u8
  PutStatusCASFailed* = 0x01'u8
  PutStatusTxnAborted* = 0x02'u8

type
  PutRequest* = object
    flags*: uint8
    txnId*: uint64
    expectedVersion*: uint64
    key*: string
    value*: string
    groupId*: uint64  ## non-zero when GroupRouted flag is set

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
  if req.groupId != 0: flags = flags or PutFlagGroupRouted
  buf.writeUint8(flags)
  buf.writeUint64BE(req.txnId)
  buf.writeUint64BE(req.expectedVersion)
  buf.writeBytes(req.key)
  buf.writeBytes(req.value)
  if req.groupId != 0:
    buf.writeUint64BE(req.groupId)
  buf
proc encodePutRequest*(req: PutRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtPut))
  var flags = req.flags
  if req.groupId != 0: flags = flags or PutFlagGroupRouted
  buf.writeUint8(flags)
  buf.writeUint64BE(req.txnId)
  buf.writeUint64BE(req.expectedVersion)
  buf.writeBytes(req.key)
  buf.writeBytes(req.value)
  if req.groupId != 0:
    buf.writeUint64BE(req.groupId)
  buf

proc decodePutRequest*(payload: string): Result[PutRequest, ProtocolError] =
  var pos = 2
  var req: PutRequest
  let flagsR = readUint8(payload, pos)
  if flagsR.isErr: return peErr(flagsR.error)
  req.flags = flagsR.value

  let txnR = readUint64BE(payload, pos)
  if txnR.isErr: return peErr(txnR.error)
  req.txnId = txnR.value

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
    let gidR = readUint64BE(payload, pos)
    if gidR.isErr: return peErr(gidR.error)
    req.groupId = gidR.value

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
#   TxnId (8 bytes)
#   Key (uint32-prefixed)
#
# Response:
#   Status (1 byte):  0x00=Deleted  0x01=NotFound  0x02=TxnAborted
#   PreviousValue (uint32-prefixed, 0-length if absent)
# ---------------------------------------------------------------------------

const
  DelFlagReturnPrev* = 0x01'u8
  DelFlagSyncWrite* = 0x02'u8
  DelFlagOnlyIfExists* = 0x04'u8
  DelFlagGroupRouted* = 0x10'u8  ## groupId appended after key

  DelStatusDeleted* = 0x00'u8
  DelStatusNotFound* = 0x01'u8
  DelStatusTxnAborted* = 0x02'u8

type
  DeleteRequest* = object
    flags*: uint8
    txnId*: uint64
    key*: string
    groupId*: uint64  ## non-zero when GroupRouted flag is set

  DeleteResponse* = object
    status*: uint8
    hasPreviousValue*: bool
    previousValue*: string

proc encodeDeleteRequest*(req: DeleteRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtDelete))
  var flags = req.flags
  if req.groupId != 0: flags = flags or DelFlagGroupRouted
  buf.writeUint8(flags)
  buf.writeUint64BE(req.txnId)
  buf.writeBytes(req.key)
  if req.groupId != 0:
    buf.writeUint64BE(req.groupId)
  buf

proc decodeDeleteRequest*(payload: string): Result[DeleteRequest,
    ProtocolError] =
  var pos = 2
  var req: DeleteRequest
  let flagsR = readUint8(payload, pos)
  if flagsR.isErr: return peErr(flagsR.error)
  req.flags = flagsR.value

  let txnR = readUint64BE(payload, pos)
  if txnR.isErr: return peErr(txnR.error)
  req.txnId = txnR.value

  let keyR = readBytes(payload, pos)
  if keyR.isErr: return peErr(keyR.error)
  req.key = keyR.value

  if (req.flags and DelFlagGroupRouted) != 0:
    let gidR = readUint64BE(payload, pos)
    if gidR.isErr: return peErr(gidR.error)
    req.groupId = gidR.value

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
#   TxnId (8 bytes)
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
    txnId*: uint64
    operations*: seq[BatchOp]

  BatchResponse* = object
    status*: uint8
    results*: seq[BatchOpResult]

proc encodeBatchRequest*(req: BatchRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtBatch))
  buf.writeUint8(req.flags)
  buf.writeUint64BE(req.txnId)
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

  let txnR = readUint64BE(payload, pos)
  if txnR.isErr: return peErr(txnR.error)
  req.txnId = txnR.value

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
#   TxnId (8 bytes)
#   ReadTimestamp (8 bytes)
#   StartKey (uint32-prefixed, 0 length = beginning of keyspace)
#   EndKey (uint32-prefixed, 0 length = end of keyspace)
#   Limit (4 bytes, 0 = no limit)
#
# Response frame (one or more frames):
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

  ScanRespFlagHasMore* = 0x01'u8
  ScanRespFlagEndOfScan* = 0x02'u8

type
  ScanRequest* = object
    flags*: uint8
    txnId*: uint64
    readTimestamp*: uint64
    startKey*: string ## empty = beginning of keyspace
    endKey*: string   ## empty = end of keyspace
    limit*: uint32    ## 0 = no limit

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

proc encodeScanRequest*(req: ScanRequest): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtScan))
  buf.writeUint8(req.flags)
  buf.writeUint64BE(req.txnId)
  buf.writeUint64BE(req.readTimestamp)
  buf.writeBytes(req.startKey)
  buf.writeBytes(req.endKey)
  buf.writeUint32BE(req.limit)
  buf

proc decodeScanRequest*(payload: string): Result[ScanRequest, ProtocolError] =
  var pos = 2
  var req: ScanRequest
  let flagsR = readUint8(payload, pos)
  if flagsR.isErr: return peErr(flagsR.error)
  req.flags = flagsR.value

  let txnR = readUint64BE(payload, pos)
  if txnR.isErr: return peErr(txnR.error)
  req.txnId = txnR.value

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
