# Unit tests for fractio/protocol/messages/txn.nim
# Tests BeginTxn, CommitTxn, RollbackTxn, TxnStatus encoding/decoding

import std/unittest
import fractio/protocol/messages/txn
import fractio/protocol/types
import fractio/protocol/codec
import fractio/core/types

suite "BeginTxnRequest/BeginTxnResponse":

  proc makeTxnId(): TransactionID =
    genTransactionIDLocal()

  test "encodeBeginTxnRequest basic":
    let req = BeginTxnRequest(
      flags: 0'u8,
      timeoutMs: 0'u32
    )
    let encoded = encodeBeginTxnRequest(req)
    check encoded.len == 7
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtBeginTxn)

  test "encodeBeginTxnRequest with flags":
    let req = BeginTxnRequest(
      flags: TxnFlagReadOnly or TxnFlagSerializable,
      timeoutMs: 5000'u32
    )
    let encoded = encodeBeginTxnRequest(req)
    check encoded.len == 7

  test "encodeBeginTxnRequest with timeout":
    let req = BeginTxnRequest(
      flags: 0'u8,
      timeoutMs: 30000'u32
    )
    let encoded = encodeBeginTxnRequest(req)
    check encoded.len == 7

  test "decodeBeginTxnRequest roundtrip":
    let req = BeginTxnRequest(
      flags: TxnFlagReadOnly,
      timeoutMs: 10000'u32
    )
    let encoded = encodeBeginTxnRequest(req)
    let decoded = decodeBeginTxnRequest(encoded)
    check decoded.isOk
    check decoded.value.flags == req.flags
    check decoded.value.timeoutMs == req.timeoutMs

  test "decodeBeginTxnRequest truncated":
    let truncated = "\x02\x00"
    let decoded = decodeBeginTxnRequest(truncated)
    check decoded.isErr

  test "encodeBeginTxnResponse":
    let txnId = makeTxnId()
    let resp = BeginTxnResponse(
      txnId: txnId,
      readTimestamp: 123456789'u64
    )
    let encoded = encodeBeginTxnResponse(resp)
    check encoded.len == 26

  test "decodeBeginTxnResponse roundtrip":
    let txnId = makeTxnId()
    let resp = BeginTxnResponse(
      txnId: txnId,
      readTimestamp: 999999'u64
    )
    let encoded = encodeBeginTxnResponse(resp)
    let decoded = decodeBeginTxnResponse(encoded)
    check decoded.isOk
    check decoded.value.txnId == resp.txnId
    check decoded.value.readTimestamp == resp.readTimestamp

  test "decodeBeginTxnResponse truncated":
    let truncated = "\x02\x00\x01\x02\x03"
    let decoded = decodeBeginTxnResponse(truncated)
    check decoded.isErr

suite "CommitTxnRequest/CommitTxnResponse":

  proc makeTxnId(): TransactionID =
    genTransactionIDLocal()

  test "encodeCommitTxnRequest":
    let txnId = makeTxnId()
    let req = CommitTxnRequest(txnId: txnId)
    let encoded = encodeCommitTxnRequest(req)
    check encoded.len == 18
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtCommitTxn)

  test "decodeCommitTxnRequest roundtrip":
    let txnId = makeTxnId()
    let req = CommitTxnRequest(txnId: txnId)
    let encoded = encodeCommitTxnRequest(req)
    let decoded = decodeCommitTxnRequest(encoded)
    check decoded.isOk
    check decoded.value.txnId == req.txnId

  test "decodeCommitTxnRequest truncated":
    let truncated = "\x02\x01\x01\x02"
    let decoded = decodeCommitTxnRequest(truncated)
    check decoded.isErr

  test "encodeCommitTxnResponse OK":
    let resp = CommitTxnResponse(
      status: TxnCommitOK,
      commitTimestamp: 1000000'u64
    )
    let encoded = encodeCommitTxnResponse(resp)
    check encoded.len == 11

  test "encodeCommitTxnResponse Conflict":
    let resp = CommitTxnResponse(
      status: TxnCommitConflict,
      commitTimestamp: 0'u64
    )
    let encoded = encodeCommitTxnResponse(resp)
    check encoded.len == 11

  test "encodeCommitTxnResponse Timeout":
    let resp = CommitTxnResponse(
      status: TxnCommitTimeout,
      commitTimestamp: 0'u64
    )
    let encoded = encodeCommitTxnResponse(resp)
    check encoded.len == 11

  test "encodeCommitTxnResponse NotFound":
    let resp = CommitTxnResponse(
      status: TxnCommitNotFound,
      commitTimestamp: 0'u64
    )
    let encoded = encodeCommitTxnResponse(resp)
    check encoded.len == 11

  test "decodeCommitTxnResponse roundtrip OK":
    let resp = CommitTxnResponse(
      status: TxnCommitOK,
      commitTimestamp: 55555'u64
    )
    let encoded = encodeCommitTxnResponse(resp)
    let decoded = decodeCommitTxnResponse(encoded)
    check decoded.isOk
    check decoded.value.status == resp.status
    check decoded.value.commitTimestamp == resp.commitTimestamp

  test "decodeCommitTxnResponse roundtrip Conflict":
    let resp = CommitTxnResponse(
      status: TxnCommitConflict,
      commitTimestamp: 0'u64
    )
    let encoded = encodeCommitTxnResponse(resp)
    let decoded = decodeCommitTxnResponse(encoded)
    check decoded.isOk
    check decoded.value.status == TxnCommitConflict

  test "decodeCommitTxnResponse truncated":
    let truncated = "\x02\x01"
    let decoded = decodeCommitTxnResponse(truncated)
    check decoded.isErr

suite "RollbackTxnRequest/RollbackTxnResponse":

  proc makeTxnId(): TransactionID =
    genTransactionIDLocal()

  test "encodeRollbackTxnRequest":
    let txnId = makeTxnId()
    let req = RollbackTxnRequest(txnId: txnId)
    let encoded = encodeRollbackTxnRequest(req)
    check encoded.len == 18
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtRollbackTxn)

  test "decodeRollbackTxnRequest roundtrip":
    let txnId = makeTxnId()
    let req = RollbackTxnRequest(txnId: txnId)
    let encoded = encodeRollbackTxnRequest(req)
    let decoded = decodeRollbackTxnRequest(encoded)
    check decoded.isOk
    check decoded.value.txnId == req.txnId

  test "decodeRollbackTxnRequest truncated":
    let truncated = "\x02\x02\x01\x02"
    let decoded = decodeRollbackTxnRequest(truncated)
    check decoded.isErr

  test "encodeRollbackTxnResponse OK":
    let resp = RollbackTxnResponse(status: TxnRollbackOK)
    let encoded = encodeRollbackTxnResponse(resp)
    check encoded.len == 3

  test "encodeRollbackTxnResponse NotFound":
    let resp = RollbackTxnResponse(status: TxnRollbackNotFound)
    let encoded = encodeRollbackTxnResponse(resp)
    check encoded.len == 3

  test "decodeRollbackTxnResponse roundtrip OK":
    let resp = RollbackTxnResponse(status: TxnRollbackOK)
    let encoded = encodeRollbackTxnResponse(resp)
    let decoded = decodeRollbackTxnResponse(encoded)
    check decoded.isOk
    check decoded.value.status == TxnRollbackOK

  test "decodeRollbackTxnResponse roundtrip NotFound":
    let resp = RollbackTxnResponse(status: TxnRollbackNotFound)
    let encoded = encodeRollbackTxnResponse(resp)
    let decoded = decodeRollbackTxnResponse(encoded)
    check decoded.isOk
    check decoded.value.status == TxnRollbackNotFound

  test "decodeRollbackTxnResponse truncated":
    let truncated = "\x02\x02"
    let decoded = decodeRollbackTxnResponse(truncated)
    check decoded.isErr

suite "TxnStatusRequest/TxnStatusResponse":

  proc makeTxnId(): TransactionID =
    genTransactionIDLocal()

  test "encodeTxnStatusRequest":
    let txnId = makeTxnId()
    let req = TxnStatusRequest(txnId: txnId)
    let encoded = encodeTxnStatusRequest(req)
    check encoded.len == 18
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtTxnStatus)

  test "decodeTxnStatusRequest roundtrip":
    let txnId = makeTxnId()
    let req = TxnStatusRequest(txnId: txnId)
    let encoded = encodeTxnStatusRequest(req)
    let decoded = decodeTxnStatusRequest(encoded)
    check decoded.isOk
    check decoded.value.txnId == req.txnId

  test "decodeTxnStatusRequest truncated":
    let truncated = "\x02\x03\x01\x02"
    let decoded = decodeTxnStatusRequest(truncated)
    check decoded.isErr

  test "encodeTxnStatusResponse Active":
    let resp = TxnStatusResponse(
      status: TxnStatusActive,
      commitTimestamp: 0'u64
    )
    let encoded = encodeTxnStatusResponse(resp)
    check encoded.len == 11

  test "encodeTxnStatusResponse Committed":
    let resp = TxnStatusResponse(
      status: TxnStatusCommitted,
      commitTimestamp: 100000'u64
    )
    let encoded = encodeTxnStatusResponse(resp)
    check encoded.len == 11

  test "encodeTxnStatusResponse Aborted":
    let resp = TxnStatusResponse(
      status: TxnStatusAborted,
      commitTimestamp: 0'u64
    )
    let encoded = encodeTxnStatusResponse(resp)
    check encoded.len == 11

  test "encodeTxnStatusResponse NotFound":
    let resp = TxnStatusResponse(
      status: TxnStatusNotFound,
      commitTimestamp: 0'u64
    )
    let encoded = encodeTxnStatusResponse(resp)
    check encoded.len == 11

  test "decodeTxnStatusResponse roundtrip Active":
    let resp = TxnStatusResponse(
      status: TxnStatusActive,
      commitTimestamp: 0'u64
    )
    let encoded = encodeTxnStatusResponse(resp)
    let decoded = decodeTxnStatusResponse(encoded)
    check decoded.isOk
    check decoded.value.status == TxnStatusActive
    check decoded.value.commitTimestamp == 0'u64

  test "decodeTxnStatusResponse roundtrip Committed":
    let resp = TxnStatusResponse(
      status: TxnStatusCommitted,
      commitTimestamp: 777777'u64
    )
    let encoded = encodeTxnStatusResponse(resp)
    let decoded = decodeTxnStatusResponse(encoded)
    check decoded.isOk
    check decoded.value.status == TxnStatusCommitted
    check decoded.value.commitTimestamp == 777777'u64

  test "decodeTxnStatusResponse roundtrip Aborted":
    let resp = TxnStatusResponse(
      status: TxnStatusAborted,
      commitTimestamp: 0'u64
    )
    let encoded = encodeTxnStatusResponse(resp)
    let decoded = decodeTxnStatusResponse(encoded)
    check decoded.isOk
    check decoded.value.status == TxnStatusAborted

  test "decodeTxnStatusResponse truncated":
    let truncated = "\x02\x03"
    let decoded = decodeTxnStatusResponse(truncated)
    check decoded.isErr

suite "Transaction Constants":

  test "Commit status values":
    check TxnCommitOK == 0x00'u8
    check TxnCommitConflict == 0x01'u8
    check TxnCommitTimeout == 0x02'u8
    check TxnCommitNotFound == 0x03'u8

  test "Rollback status values":
    check TxnRollbackOK == 0x00'u8
    check TxnRollbackNotFound == 0x01'u8

  test "Txn status values":
    check TxnStatusActive == 0x00'u8
    check TxnStatusCommitted == 0x01'u8
    check TxnStatusAborted == 0x02'u8
    check TxnStatusNotFound == 0x03'u8

  test "BeginTxn flag values":
    check TxnFlagReadOnly == 0x01'u8
    check TxnFlagSerializable == 0x02'u8

suite "Transaction Message Roundtrip Integration":

  proc makeTxnId(): TransactionID =
    genTransactionIDLocal()

  test "Full transaction lifecycle encode/decode":
    let beginReq = BeginTxnRequest(flags: 0'u8, timeoutMs: 10000'u32)
    let beginEncoded = encodeBeginTxnRequest(beginReq)
    let beginDecoded = decodeBeginTxnRequest(beginEncoded)
    check beginDecoded.isOk

    let txnId = makeTxnId()
    let beginResp = BeginTxnResponse(txnId: txnId, readTimestamp: 5000'u64)
    let beginRespEncoded = encodeBeginTxnResponse(beginResp)
    let beginRespDecoded = decodeBeginTxnResponse(beginRespEncoded)
    check beginRespDecoded.isOk
    check beginRespDecoded.value.txnId == txnId

    let commitReq = CommitTxnRequest(txnId: txnId)
    let commitEncoded = encodeCommitTxnRequest(commitReq)
    let commitDecoded = decodeCommitTxnRequest(commitEncoded)
    check commitDecoded.isOk
    check commitDecoded.value.txnId == txnId

    let commitResp = CommitTxnResponse(status: TxnCommitOK,
        commitTimestamp: 6000'u64)
    let commitRespEncoded = encodeCommitTxnResponse(commitResp)
    let commitRespDecoded = decodeCommitTxnResponse(commitRespEncoded)
    check commitRespDecoded.isOk
    check commitRespDecoded.value.status == TxnCommitOK

  test "Rollback lifecycle encode/decode":
    let txnId = makeTxnId()

    let rollbackReq = RollbackTxnRequest(txnId: txnId)
    let rollbackEncoded = encodeRollbackTxnRequest(rollbackReq)
    let rollbackDecoded = decodeRollbackTxnRequest(rollbackEncoded)
    check rollbackDecoded.isOk
    check rollbackDecoded.value.txnId == txnId

    let rollbackResp = RollbackTxnResponse(status: TxnRollbackOK)
    let rollbackRespEncoded = encodeRollbackTxnResponse(rollbackResp)
    let rollbackRespDecoded = decodeRollbackTxnResponse(rollbackRespEncoded)
    check rollbackRespDecoded.isOk
    check rollbackRespDecoded.value.status == TxnRollbackOK

  test "Status query encode/decode":
    let txnId = makeTxnId()

    let statusReq = TxnStatusRequest(txnId: txnId)
    let statusEncoded = encodeTxnStatusRequest(statusReq)
    let statusDecoded = decodeTxnStatusRequest(statusEncoded)
    check statusDecoded.isOk
    check statusDecoded.value.txnId == txnId

    let statusResp = TxnStatusResponse(status: TxnStatusActive,
        commitTimestamp: 0'u64)
    let statusRespEncoded = encodeTxnStatusResponse(statusResp)
    let statusRespDecoded = decodeTxnStatusResponse(statusRespEncoded)
    check statusRespDecoded.isOk
    check statusRespDecoded.value.status == TxnStatusActive
