# Unit tests for fractio/protocol/messages/txn.nim
# Tests BeginTxn, CommitTxn, RollbackTxn, TxnStatus encoding/decoding

import std/unittest
import fractio/protocol/messages/txn
import fractio/protocol/types
import fractio/protocol/codec
import fractio/core/types

suite "BeginTxn Messages":

  test "encodeBeginTxnRequest basic":
    let req = BeginTxnRequest(flags: 0x00'u8, timeoutMs: 0'u32)
    let encoded = encodeBeginTxnRequest(req)
    check encoded.len == 7 # 2 byte type + 1 byte flags + 4 byte timeout
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtBeginTxn)
    let flags = readUint8(encoded, pos)
    check flags.isOk
    check flags.value == 0x00'u8
    let timeout = readUint32BE(encoded, pos)
    check timeout.isOk
    check timeout.value == 0'u32

  test "encodeBeginTxnRequest with flags":
    let req = BeginTxnRequest(flags: TxnFlagReadOnly, timeoutMs: 5000'u32)
    let encoded = encodeBeginTxnRequest(req)
    check encoded.len == 7
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check flags.value == TxnFlagReadOnly
    let timeout = readUint32BE(encoded, pos)
    check timeout.value == 5000'u32

  test "encodeBeginTxnRequest serializable":
    let req = BeginTxnRequest(flags: TxnFlagSerializable, timeoutMs: 10000'u32)
    let encoded = encodeBeginTxnRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check flags.value == TxnFlagSerializable

  test "encodeBeginTxnRequest combined flags":
    let req = BeginTxnRequest(flags: TxnFlagReadOnly or TxnFlagSerializable,
        timeoutMs: 30000'u32)
    let encoded = encodeBeginTxnRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check flags.value == (TxnFlagReadOnly or TxnFlagSerializable)

  test "decodeBeginTxnRequest valid":
    let req = BeginTxnRequest(flags: 0x03'u8, timeoutMs: 15000'u32)
    let encoded = encodeBeginTxnRequest(req)
    let decoded = decodeBeginTxnRequest(encoded)
    check decoded.isOk
    check decoded.value.flags == 0x03'u8
    check decoded.value.timeoutMs == 15000'u32

  test "decodeBeginTxnRequest truncated flags":
    let invalid = "\x02\x00" # Just message type + partial
    let decoded = decodeBeginTxnRequest(invalid)
    check decoded.isErr

  test "decodeBeginTxnRequest truncated timeout":
    let invalid = "\x02\x00\x01" # Message type + flags, no timeout
    let decoded = decodeBeginTxnRequest(invalid)
    check decoded.isErr

  test "encodeBeginTxnResponse":
    let txnId = genULIDLocal()
    let resp = BeginTxnResponse(txnId: TransactionID(txnId),
        readTimestamp: 123456789'u64)
    let encoded = encodeBeginTxnResponse(resp)
    check encoded.len == 26 # 2 byte type + 16 byte ULID + 8 byte timestamp

  test "decodeBeginTxnResponse valid":
    let txnId = genULIDLocal()
    let resp = BeginTxnResponse(txnId: TransactionID(txnId),
        readTimestamp: 987654321'u64)
    let encoded = encodeBeginTxnResponse(resp)
    let decoded = decodeBeginTxnResponse(encoded)
    check decoded.isOk
    check decoded.value.readTimestamp == 987654321'u64

  test "decodeBeginTxnResponse truncated txnId":
    let invalid = "\x02\x00" # Just message type, no txnId
    let decoded = decodeBeginTxnResponse(invalid)
    check decoded.isErr

  test "decodeBeginTxnResponse truncated timestamp":
    let txnId = genULIDLocal()
    let txnBytes = ulidToBytes(txnId)
    let invalid = "\x02\x00" & txnBytes # Message type + txnId, no timestamp
    let decoded = decodeBeginTxnResponse(invalid)
    check decoded.isErr

  test "BeginTxn roundtrip":
    let req = BeginTxnRequest(flags: TxnFlagReadOnly, timeoutMs: 60000'u32)
    let encoded = encodeBeginTxnRequest(req)
    let decoded = decodeBeginTxnRequest(encoded)
    check decoded.isOk
    check decoded.value.flags == req.flags
    check decoded.value.timeoutMs == req.timeoutMs

suite "CommitTxn Messages":

  test "encodeCommitTxnRequest":
    let txnId = genULIDLocal()
    let req = CommitTxnRequest(txnId: TransactionID(txnId))
    let encoded = encodeCommitTxnRequest(req)
    check encoded.len == 18 # 2 byte type + 16 byte ULID

  test "decodeCommitTxnRequest valid":
    let txnId = genULIDLocal()
    let req = CommitTxnRequest(txnId: TransactionID(txnId))
    let encoded = encodeCommitTxnRequest(req)
    let decoded = decodeCommitTxnRequest(encoded)
    check decoded.isOk

  test "decodeCommitTxnRequest truncated":
    let invalid = "\x02\x01" # Just message type
    let decoded = decodeCommitTxnRequest(invalid)
    check decoded.isErr

  test "encodeCommitTxnResponse OK":
    let resp = CommitTxnResponse(status: TxnCommitOK,
        commitTimestamp: 111111111'u64)
    let encoded = encodeCommitTxnResponse(resp)
    check encoded.len == 11 # 2 byte type + 1 byte status + 8 byte timestamp

  test "encodeCommitTxnResponse Conflict":
    let resp = CommitTxnResponse(status: TxnCommitConflict,
        commitTimestamp: 0'u64)
    let encoded = encodeCommitTxnResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let status = readUint8(encoded, pos)
    check status.value == TxnCommitConflict

  test "encodeCommitTxnResponse Timeout":
    let resp = CommitTxnResponse(status: TxnCommitTimeout,
        commitTimestamp: 0'u64)
    let encoded = encodeCommitTxnResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let status = readUint8(encoded, pos)
    check status.value == TxnCommitTimeout

  test "encodeCommitTxnResponse NotFound":
    let resp = CommitTxnResponse(status: TxnCommitNotFound,
        commitTimestamp: 0'u64)
    let encoded = encodeCommitTxnResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let status = readUint8(encoded, pos)
    check status.value == TxnCommitNotFound

  test "decodeCommitTxnResponse OK":
    let resp = CommitTxnResponse(status: TxnCommitOK,
        commitTimestamp: 222222222'u64)
    let encoded = encodeCommitTxnResponse(resp)
    let decoded = decodeCommitTxnResponse(encoded)
    check decoded.isOk
    check decoded.value.status == TxnCommitOK
    check decoded.value.commitTimestamp == 222222222'u64

  test "decodeCommitTxnResponse Conflict":
    let resp = CommitTxnResponse(status: TxnCommitConflict,
        commitTimestamp: 0'u64)
    let encoded = encodeCommitTxnResponse(resp)
    let decoded = decodeCommitTxnResponse(encoded)
    check decoded.isOk
    check decoded.value.status == TxnCommitConflict

  test "decodeCommitTxnResponse truncated status":
    let invalid = "\x02\x01" # Just message type
    let decoded = decodeCommitTxnResponse(invalid)
    check decoded.isErr

  test "decodeCommitTxnResponse truncated timestamp":
    let invalid = "\x02\x01\x00" # Message type + status, no timestamp
    let decoded = decodeCommitTxnResponse(invalid)
    check decoded.isErr

  test "CommitTxn request roundtrip":
    let txnId = genULIDLocal()
    let req = CommitTxnRequest(txnId: TransactionID(txnId))
    let encoded = encodeCommitTxnRequest(req)
    let decoded = decodeCommitTxnRequest(encoded)
    check decoded.isOk

  test "CommitTxn response roundtrip":
    for status in [TxnCommitOK, TxnCommitConflict, TxnCommitTimeout,
        TxnCommitNotFound]:
      let resp = CommitTxnResponse(status: status, commitTimestamp: if status ==
          TxnCommitOK: 12345'u64 else: 0'u64)
      let encoded = encodeCommitTxnResponse(resp)
      let decoded = decodeCommitTxnResponse(encoded)
      check decoded.isOk
      check decoded.value.status == status

suite "RollbackTxn Messages":

  test "encodeRollbackTxnRequest":
    let txnId = genULIDLocal()
    let req = RollbackTxnRequest(txnId: TransactionID(txnId))
    let encoded = encodeRollbackTxnRequest(req)
    check encoded.len == 18 # 2 byte type + 16 byte ULID

  test "decodeRollbackTxnRequest valid":
    let txnId = genULIDLocal()
    let req = RollbackTxnRequest(txnId: TransactionID(txnId))
    let encoded = encodeRollbackTxnRequest(req)
    let decoded = decodeRollbackTxnRequest(encoded)
    check decoded.isOk

  test "decodeRollbackTxnRequest truncated":
    let invalid = "\x02\x02" # Just message type
    let decoded = decodeRollbackTxnRequest(invalid)
    check decoded.isErr

  test "encodeRollbackTxnResponse OK":
    let resp = RollbackTxnResponse(status: TxnRollbackOK)
    let encoded = encodeRollbackTxnResponse(resp)
    check encoded.len == 3 # 2 byte type + 1 byte status

  test "encodeRollbackTxnResponse NotFound":
    let resp = RollbackTxnResponse(status: TxnRollbackNotFound)
    let encoded = encodeRollbackTxnResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let status = readUint8(encoded, pos)
    check status.value == TxnRollbackNotFound

  test "decodeRollbackTxnResponse OK":
    let resp = RollbackTxnResponse(status: TxnRollbackOK)
    let encoded = encodeRollbackTxnResponse(resp)
    let decoded = decodeRollbackTxnResponse(encoded)
    check decoded.isOk
    check decoded.value.status == TxnRollbackOK

  test "decodeRollbackTxnResponse NotFound":
    let resp = RollbackTxnResponse(status: TxnRollbackNotFound)
    let encoded = encodeRollbackTxnResponse(resp)
    let decoded = decodeRollbackTxnResponse(encoded)
    check decoded.isOk
    check decoded.value.status == TxnRollbackNotFound

  test "decodeRollbackTxnResponse truncated":
    let invalid = "\x02\x02" # Just message type
    let decoded = decodeRollbackTxnResponse(invalid)
    check decoded.isErr

  test "RollbackTxn request roundtrip":
    let txnId = genULIDLocal()
    let req = RollbackTxnRequest(txnId: TransactionID(txnId))
    let encoded = encodeRollbackTxnRequest(req)
    let decoded = decodeRollbackTxnRequest(encoded)
    check decoded.isOk

  test "RollbackTxn response roundtrip":
    for status in [TxnRollbackOK, TxnRollbackNotFound]:
      let resp = RollbackTxnResponse(status: status)
      let encoded = encodeRollbackTxnResponse(resp)
      let decoded = decodeRollbackTxnResponse(encoded)
      check decoded.isOk
      check decoded.value.status == status

suite "TxnStatus Messages":

  test "encodeTxnStatusRequest":
    let txnId = genULIDLocal()
    let req = TxnStatusRequest(txnId: TransactionID(txnId))
    let encoded = encodeTxnStatusRequest(req)
    check encoded.len == 18 # 2 byte type + 16 byte ULID

  test "decodeTxnStatusRequest valid":
    let txnId = genULIDLocal()
    let req = TxnStatusRequest(txnId: TransactionID(txnId))
    let encoded = encodeTxnStatusRequest(req)
    let decoded = decodeTxnStatusRequest(encoded)
    check decoded.isOk

  test "decodeTxnStatusRequest truncated":
    let invalid = "\x02\x03" # Just message type
    let decoded = decodeTxnStatusRequest(invalid)
    check decoded.isErr

  test "encodeTxnStatusResponse Active":
    let resp = TxnStatusResponse(status: TxnStatusActive,
        commitTimestamp: 0'u64)
    let encoded = encodeTxnStatusResponse(resp)
    check encoded.len == 11 # 2 byte type + 1 byte status + 8 byte timestamp

  test "encodeTxnStatusResponse Committed":
    let resp = TxnStatusResponse(status: TxnStatusCommitted,
        commitTimestamp: 333333333'u64)
    let encoded = encodeTxnStatusResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let status = readUint8(encoded, pos)
    check status.value == TxnStatusCommitted
    let ts = readUint64BE(encoded, pos)
    check ts.value == 333333333'u64

  test "encodeTxnStatusResponse Aborted":
    let resp = TxnStatusResponse(status: TxnStatusAborted,
        commitTimestamp: 0'u64)
    let encoded = encodeTxnStatusResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let status = readUint8(encoded, pos)
    check status.value == TxnStatusAborted

  test "encodeTxnStatusResponse NotFound":
    let resp = TxnStatusResponse(status: TxnStatusNotFound,
        commitTimestamp: 0'u64)
    let encoded = encodeTxnStatusResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let status = readUint8(encoded, pos)
    check status.value == TxnStatusNotFound

  test "decodeTxnStatusResponse Active":
    let resp = TxnStatusResponse(status: TxnStatusActive,
        commitTimestamp: 0'u64)
    let encoded = encodeTxnStatusResponse(resp)
    let decoded = decodeTxnStatusResponse(encoded)
    check decoded.isOk
    check decoded.value.status == TxnStatusActive
    check decoded.value.commitTimestamp == 0'u64

  test "decodeTxnStatusResponse Committed":
    let resp = TxnStatusResponse(status: TxnStatusCommitted,
        commitTimestamp: 444444444'u64)
    let encoded = encodeTxnStatusResponse(resp)
    let decoded = decodeTxnStatusResponse(encoded)
    check decoded.isOk
    check decoded.value.status == TxnStatusCommitted
    check decoded.value.commitTimestamp == 444444444'u64

  test "decodeTxnStatusResponse truncated status":
    let invalid = "\x02\x03" # Just message type
    let decoded = decodeTxnStatusResponse(invalid)
    check decoded.isErr

  test "decodeTxnStatusResponse truncated timestamp":
    let invalid = "\x02\x03\x00" # Message type + status, no timestamp
    let decoded = decodeTxnStatusResponse(invalid)
    check decoded.isErr

  test "TxnStatus request roundtrip":
    let txnId = genULIDLocal()
    let req = TxnStatusRequest(txnId: TransactionID(txnId))
    let encoded = encodeTxnStatusRequest(req)
    let decoded = decodeTxnStatusRequest(encoded)
    check decoded.isOk

  test "TxnStatus response roundtrip":
    for status in [TxnStatusActive, TxnStatusCommitted, TxnStatusAborted,
        TxnStatusNotFound]:
      let ts = if status == TxnStatusCommitted: 12345'u64 else: 0'u64
      let resp = TxnStatusResponse(status: status, commitTimestamp: ts)
      let encoded = encodeTxnStatusResponse(resp)
      let decoded = decodeTxnStatusResponse(encoded)
      check decoded.isOk
      check decoded.value.status == status
      check decoded.value.commitTimestamp == ts

suite "Transaction Constants":

  test "TxnCommitOK value":
    check TxnCommitOK == 0x00'u8

  test "TxnCommitConflict value":
    check TxnCommitConflict == 0x01'u8

  test "TxnCommitTimeout value":
    check TxnCommitTimeout == 0x02'u8

  test "TxnCommitNotFound value":
    check TxnCommitNotFound == 0x03'u8

  test "TxnRollbackOK value":
    check TxnRollbackOK == 0x00'u8

  test "TxnRollbackNotFound value":
    check TxnRollbackNotFound == 0x01'u8

  test "TxnStatusActive value":
    check TxnStatusActive == 0x00'u8

  test "TxnStatusCommitted value":
    check TxnStatusCommitted == 0x01'u8

  test "TxnStatusAborted value":
    check TxnStatusAborted == 0x02'u8

  test "TxnStatusNotFound value":
    check TxnStatusNotFound == 0x03'u8

  test "TxnFlagReadOnly value":
    check TxnFlagReadOnly == 0x01'u8

  test "TxnFlagSerializable value":
    check TxnFlagSerializable == 0x02'u8
