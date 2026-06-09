# Unit tests for fractio/protocol/messages/kv.nim
# Tests Get, Put, Delete, Batch, Scan encoding/decoding

import std/[unittest, strutils]
import fractio/protocol/messages/kv
import fractio/protocol/types
import fractio/protocol/codec
import fractio/core/types
import fractio/distributed/raft/group_types

suite "GetRequest/GetResponse":

  proc makeTxnId(): TransactionID =
    genTransactionIDLocal()

  test "encodeGetRequest basic":
    let txnId = makeTxnId()
    let req = GetRequest(
      flags: 0'u8,
      txnId: txnId,
      readTimestamp: 1000'u64,
      key: "test_key"
    )
    let encoded = encodeGetRequest(req)
    check encoded.len > 2
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtGet)

  test "encodeGetRequest with IncludeTimestamp flag":
    let txnId = makeTxnId()
    let req = GetRequest(
      flags: GetFlagIncludeTimestamp,
      txnId: txnId,
      readTimestamp: 5000'u64,
      key: "key_ts"
    )
    let encoded = encodeGetRequest(req)
    check encoded.len > 2

  test "encodeGetRequest with GroupRouted":
    let txnId = makeTxnId()
    let groupId = genGroupIDLocal()
    let req = GetRequest(
      flags: 0'u8,
      txnId: txnId,
      readTimestamp: 100'u64,
      key: "routed_key",
      groupId: groupId
    )
    let encoded = encodeGetRequest(req)
    check encoded.len > 18

  test "decodeGetRequest roundtrip":
    let txnId = makeTxnId()
    let req = GetRequest(
      flags: GetFlagIncludeTimestamp or GetFlagIncludeVersion,
      txnId: txnId,
      readTimestamp: 12345'u64,
      key: "roundtrip_key"
    )
    let encoded = encodeGetRequest(req)
    let decoded = decodeGetRequest(encoded)
    check decoded.isOk
    check decoded.value.flags == req.flags
    check decoded.value.txnId == req.txnId
    check decoded.value.readTimestamp == req.readTimestamp
    check decoded.value.key == req.key
    check decoded.value.groupId == ZeroGroupID()

  test "decodeGetRequest with groupId roundtrip":
    let txnId = makeTxnId()
    let groupId = genGroupIDLocal()
    let req = GetRequest(
      flags: GetFlagGroupRouted,
      txnId: txnId,
      readTimestamp: 999'u64,
      key: "gkey",
      groupId: groupId
    )
    let encoded = encodeGetRequest(req)
    let decoded = decodeGetRequest(encoded)
    check decoded.isOk
    check decoded.value.groupId == groupId

  test "decodeGetRequest truncated payload":
    let truncated = "\x01\x00\x01"
    let decoded = decodeGetRequest(truncated)
    check decoded.isErr

  test "encodeGetResponse found with value":
    let resp = GetResponse(
      found: true,
      timestamp: 1000'u64,
      version: 5'u64,
      hasTimestamp: true,
      hasVersion: true,
      value: "test_value"
    )
    let encoded = encodeGetResponse(resp)
    check encoded.len > 2

  test "encodeGetResponse not found":
    let resp = GetResponse(found: false)
    let encoded = encodeGetResponse(resp)
    check encoded.len == 3

  test "decodeGetResponse found roundtrip":
    let resp = GetResponse(
      found: true,
      timestamp: 1111'u64,
      version: 42'u64,
      hasTimestamp: true,
      hasVersion: true,
      value: "hello"
    )
    let encoded = encodeGetResponse(resp)
    let decoded = decodeGetResponse(encoded)
    check decoded.isOk
    check decoded.value.found == true
    check decoded.value.timestamp == resp.timestamp
    check decoded.value.version == resp.version
    check decoded.value.value == resp.value

  test "decodeGetResponse not found roundtrip":
    let resp = GetResponse(found: false)
    let encoded = encodeGetResponse(resp)
    let decoded = decodeGetResponse(encoded)
    check decoded.isOk
    check decoded.value.found == false
    check decoded.value.value == ""

  test "decodeGetResponse truncated":
    let truncated = "\x01\x00"
    let decoded = decodeGetResponse(truncated)
    check decoded.isErr

suite "PutRequest/PutResponse":

  proc makeTxnId(): TransactionID =
    genTransactionIDLocal()

  test "encodePutRequest basic":
    let txnId = makeTxnId()
    let req = PutRequest(
      flags: 0'u8,
      txnId: txnId,
      expectedVersion: 0'u64,
      key: "put_key",
      value: "put_value"
    )
    let encoded = encodePutRequest(req)
    check encoded.len > 2
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtPut)

  test "encodeRawPutRequest":
    let txnId = makeTxnId()
    let req = PutRequest(
      flags: PutFlagSyncWrite,
      txnId: txnId,
      expectedVersion: 0'u64,
      key: "raw_key",
      value: "raw_value"
    )
    let encoded = encodeRawPutRequest(req)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtRawPut)

  test "encodePutRequest with CAS flag":
    let txnId = makeTxnId()
    let req = PutRequest(
      flags: PutFlagCAS,
      txnId: txnId,
      expectedVersion: 10'u64,
      key: "cas_key",
      value: "cas_value"
    )
    let encoded = encodePutRequest(req)
    check encoded.len > 2

  test "encodePutRequest with groupId":
    let txnId = makeTxnId()
    let groupId = genGroupIDLocal()
    let req = PutRequest(
      flags: 0'u8,
      txnId: txnId,
      expectedVersion: 0'u64,
      key: "gput_key",
      value: "gput_value",
      groupId: groupId
    )
    let encoded = encodePutRequest(req)
    check encoded.len > 18

  test "decodePutRequest roundtrip":
    let txnId = makeTxnId()
    let req = PutRequest(
      flags: PutFlagReturnPrev or PutFlagSyncWrite,
      txnId: txnId,
      expectedVersion: 99'u64,
      key: "roundtrip_put",
      value: "value123"
    )
    let encoded = encodePutRequest(req)
    let decoded = decodePutRequest(encoded)
    check decoded.isOk
    check decoded.value.flags == req.flags
    check decoded.value.txnId == req.txnId
    check decoded.value.expectedVersion == req.expectedVersion
    check decoded.value.key == req.key
    check decoded.value.value == req.value

  test "decodePutRequest with groupId roundtrip":
    let txnId = makeTxnId()
    let groupId = genGroupIDLocal()
    let req = PutRequest(
      flags: PutFlagGroupRouted,
      txnId: txnId,
      expectedVersion: 0'u64,
      key: "gpkey",
      value: "gpval",
      groupId: groupId
    )
    let encoded = encodePutRequest(req)
    let decoded = decodePutRequest(encoded)
    check decoded.isOk
    check decoded.value.groupId == groupId

  test "decodePutRequest truncated":
    let truncated = "\x01\x01\x01"
    let decoded = decodePutRequest(truncated)
    check decoded.isErr

  test "encodePutResponse OK":
    let resp = PutResponse(
      status: PutStatusOK,
      timestamp: 1000'u64,
      version: 1'u64,
      hasPreviousValue: false
    )
    let encoded = encodePutResponse(resp)
    check encoded.len > 2

  test "encodePutResponse with previous value":
    let resp = PutResponse(
      status: PutStatusOK,
      timestamp: 2000'u64,
      version: 5'u64,
      hasPreviousValue: true,
      previousValue: "old_value"
    )
    let encoded = encodePutResponse(resp)
    check encoded.len > 10

  test "encodePutResponse CASFailed":
    let resp = PutResponse(
      status: PutStatusCASFailed,
      timestamp: 0'u64,
      version: 0'u64,
      hasPreviousValue: false
    )
    let encoded = encodePutResponse(resp)
    check encoded.len > 2

  test "decodePutResponse roundtrip":
    let resp = PutResponse(
      status: PutStatusOK,
      timestamp: 5555'u64,
      version: 77'u64,
      hasPreviousValue: true,
      previousValue: "prev_val"
    )
    let encoded = encodePutResponse(resp)
    let decoded = decodePutResponse(encoded)
    check decoded.isOk
    check decoded.value.status == resp.status
    check decoded.value.timestamp == resp.timestamp
    check decoded.value.version == resp.version
    check decoded.value.hasPreviousValue == true
    check decoded.value.previousValue == resp.previousValue

  test "decodePutResponse no previous value":
    let resp = PutResponse(
      status: PutStatusOK,
      timestamp: 100'u64,
      version: 1'u64,
      hasPreviousValue: false
    )
    let encoded = encodePutResponse(resp)
    let decoded = decodePutResponse(encoded)
    check decoded.isOk
    check decoded.value.hasPreviousValue == false

  test "decodePutResponse truncated":
    let truncated = "\x01\x01"
    let decoded = decodePutResponse(truncated)
    check decoded.isErr

suite "DeleteRequest/DeleteResponse":

  proc makeTxnId(): TransactionID =
    genTransactionIDLocal()

  test "encodeDeleteRequest basic":
    let txnId = makeTxnId()
    let req = DeleteRequest(
      flags: 0'u8,
      txnId: txnId,
      key: "del_key"
    )
    let encoded = encodeDeleteRequest(req)
    check encoded.len > 2
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtDelete)

  test "encodeDeleteRequest with flags":
    let txnId = makeTxnId()
    let req = DeleteRequest(
      flags: DelFlagReturnPrev or DelFlagSyncWrite,
      txnId: txnId,
      key: "del_flagged"
    )
    let encoded = encodeDeleteRequest(req)
    check encoded.len > 2

  test "encodeDeleteRequest with groupId":
    let txnId = makeTxnId()
    let groupId = genGroupIDLocal()
    let req = DeleteRequest(
      flags: DelFlagGroupRouted,
      txnId: txnId,
      key: "gdel_key",
      groupId: groupId
    )
    let encoded = encodeDeleteRequest(req)
    check encoded.len > 18

  test "decodeDeleteRequest roundtrip":
    let txnId = makeTxnId()
    let req = DeleteRequest(
      flags: DelFlagOnlyIfExists,
      txnId: txnId,
      key: "roundtrip_del"
    )
    let encoded = encodeDeleteRequest(req)
    let decoded = decodeDeleteRequest(encoded)
    check decoded.isOk
    check decoded.value.flags == req.flags
    check decoded.value.txnId == req.txnId
    check decoded.value.key == req.key

  test "decodeDeleteRequest with groupId roundtrip":
    let txnId = makeTxnId()
    let groupId = genGroupIDLocal()
    let req = DeleteRequest(
      flags: DelFlagGroupRouted,
      txnId: txnId,
      key: "gdel",
      groupId: groupId
    )
    let encoded = encodeDeleteRequest(req)
    let decoded = decodeDeleteRequest(encoded)
    check decoded.isOk
    check decoded.value.groupId == groupId

  test "decodeDeleteRequest truncated":
    let truncated = "\x01\x02\x01"
    let decoded = decodeDeleteRequest(truncated)
    check decoded.isErr

  test "encodeDeleteResponse Deleted":
    let resp = DeleteResponse(
      status: DelStatusDeleted,
      hasPreviousValue: false
    )
    let encoded = encodeDeleteResponse(resp)
    check encoded.len == 7

  test "encodeDeleteResponse with previous value":
    let resp = DeleteResponse(
      status: DelStatusDeleted,
      hasPreviousValue: true,
      previousValue: "deleted_value"
    )
    let encoded = encodeDeleteResponse(resp)
    check encoded.len > 7

  test "encodeDeleteResponse NotFound":
    let resp = DeleteResponse(
      status: DelStatusNotFound,
      hasPreviousValue: false
    )
    let encoded = encodeDeleteResponse(resp)
    check encoded.len == 7

  test "decodeDeleteResponse roundtrip with prev":
    let resp = DeleteResponse(
      status: DelStatusDeleted,
      hasPreviousValue: true,
      previousValue: "prev_del"
    )
    let encoded = encodeDeleteResponse(resp)
    let decoded = decodeDeleteResponse(encoded)
    check decoded.isOk
    check decoded.value.status == resp.status
    check decoded.value.hasPreviousValue == true
    check decoded.value.previousValue == resp.previousValue

  test "decodeDeleteResponse roundtrip no prev":
    let resp = DeleteResponse(
      status: DelStatusNotFound,
      hasPreviousValue: false
    )
    let encoded = encodeDeleteResponse(resp)
    let decoded = decodeDeleteResponse(encoded)
    check decoded.isOk
    check decoded.value.status == resp.status
    check decoded.value.hasPreviousValue == false

  test "decodeDeleteResponse truncated":
    let truncated = "\x01\x02"
    let decoded = decodeDeleteResponse(truncated)
    check decoded.isErr

suite "BatchRequest/BatchResponse":

  proc makeTxnId(): TransactionID =
    genTransactionIDLocal()

  test "encodeBatchRequest empty":
    let txnId = makeTxnId()
    let req = BatchRequest(
      flags: 0'u8,
      txnId: txnId,
      operations: @[]
    )
    let encoded = encodeBatchRequest(req)
    check encoded.len > 2
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtBatch)

  test "encodeBatchRequest with single Get":
    let txnId = makeTxnId()
    let req = BatchRequest(
      flags: BatchFlagAllOrNothing,
      txnId: txnId,
      operations: @[BatchOp(kind: BatchOpGet, flags: 0'u8, data: "key1")]
    )
    let encoded = encodeBatchRequest(req)
    check encoded.len > 6

  test "encodeBatchRequest with mixed ops":
    let txnId = makeTxnId()
    let req = BatchRequest(
      flags: BatchFlagContinueOnErr,
      txnId: txnId,
      operations: @[
        BatchOp(kind: BatchOpGet, flags: 0'u8, data: "get_key"),
        BatchOp(kind: BatchOpPut, flags: 0'u8, data: "put_data"),
        BatchOp(kind: BatchOpDelete, flags: 0'u8, data: "del_key")
      ]
    )
    let encoded = encodeBatchRequest(req)
    check encoded.len > 6

  test "decodeBatchRequest roundtrip empty":
    let txnId = makeTxnId()
    let req = BatchRequest(
      flags: 0'u8,
      txnId: txnId,
      operations: @[]
    )
    let encoded = encodeBatchRequest(req)
    let decoded = decodeBatchRequest(encoded)
    check decoded.isOk
    check decoded.value.flags == req.flags
    check decoded.value.txnId == req.txnId
    check decoded.value.operations.len == 0

  test "decodeBatchRequest roundtrip with ops":
    let txnId = makeTxnId()
    let req = BatchRequest(
      flags: BatchFlagAllOrNothing,
      txnId: txnId,
      operations: @[
        BatchOp(kind: BatchOpGet, flags: 1'u8, data: "k1"),
        BatchOp(kind: BatchOpPut, flags: 2'u8, data: "k2v2")
      ]
    )
    let encoded = encodeBatchRequest(req)
    let decoded = decodeBatchRequest(encoded)
    check decoded.isOk
    check decoded.value.operations.len == 2
    check decoded.value.operations[0].kind == BatchOpGet
    check decoded.value.operations[0].data == "k1"
    check decoded.value.operations[1].kind == BatchOpPut
    check decoded.value.operations[1].data == "k2v2"

  test "decodeBatchRequest truncated":
    let truncated = "\x01\x03\x01"
    let decoded = decodeBatchRequest(truncated)
    check decoded.isErr

  test "encodeBatchResponse AllOK":
    let resp = BatchResponse(
      status: BatchStatusAllOK,
      results: @[]
    )
    let encoded = encodeBatchResponse(resp)
    check encoded.len == 7

  test "encodeBatchResponse with results":
    let resp = BatchResponse(
      status: BatchStatusPartialFailure,
      results: @[
        BatchOpResult(status: 0'u8, data: "r1"),
        BatchOpResult(status: 1'u8, data: "")
      ]
    )
    let encoded = encodeBatchResponse(resp)
    check encoded.len > 7

  test "decodeBatchResponse roundtrip":
    let resp = BatchResponse(
      status: BatchStatusAllOK,
      results: @[
        BatchOpResult(status: 0'u8, data: "result_data"),
        BatchOpResult(status: 1'u8, data: "")
      ]
    )
    let encoded = encodeBatchResponse(resp)
    let decoded = decodeBatchResponse(encoded)
    check decoded.isOk
    check decoded.value.status == resp.status
    check decoded.value.results.len == 2
    check decoded.value.results[0].data == "result_data"

  test "decodeBatchResponse truncated":
    let truncated = "\x01\x03\x01"
    let decoded = decodeBatchResponse(truncated)
    check decoded.isErr

suite "ScanRequest/ScanResponseFrame":

  proc makeTxnId(): TransactionID =
    genTransactionIDLocal()

  test "encodeScanRequest basic":
    let txnId = makeTxnId()
    let req = ScanRequest(
      flags: 0'u8,
      txnId: txnId,
      readTimestamp: 1000'u64,
      startKey: "",
      endKey: "",
      limit: 100'u32
    )
    let encoded = encodeScanRequest(req)
    check encoded.len > 2
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtScan)

  test "encodeScanRequest with keys":
    let txnId = makeTxnId()
    let req = ScanRequest(
      flags: ScanFlagIncludeTimestamp,
      txnId: txnId,
      readTimestamp: 5000'u64,
      startKey: "start",
      endKey: "end",
      limit: 50'u32
    )
    let encoded = encodeScanRequest(req)
    check encoded.len > 10

  test "encodeScanRequest with groupId":
    let txnId = makeTxnId()
    let groupId = genGroupIDLocal()
    let req = ScanRequest(
      flags: ScanFlagGroupRouted,
      txnId: txnId,
      readTimestamp: 100'u64,
      startKey: "a",
      endKey: "z",
      limit: 0'u32,
      groupId: groupId
    )
    let encoded = encodeScanRequest(req)
    check encoded.len > 18

  test "decodeScanRequest roundtrip":
    let txnId = makeTxnId()
    let req = ScanRequest(
      flags: ScanFlagIncludeVersion or ScanFlagKeysOnly,
      txnId: txnId,
      readTimestamp: 9999'u64,
      startKey: "from",
      endKey: "to",
      limit: 25'u32
    )
    let encoded = encodeScanRequest(req)
    let decoded = decodeScanRequest(encoded)
    check decoded.isOk
    check decoded.value.flags == req.flags
    check decoded.value.txnId == req.txnId
    check decoded.value.readTimestamp == req.readTimestamp
    check decoded.value.startKey == req.startKey
    check decoded.value.endKey == req.endKey
    check decoded.value.limit == req.limit

  test "decodeScanRequest with groupId roundtrip":
    let txnId = makeTxnId()
    let groupId = genGroupIDLocal()
    let req = ScanRequest(
      flags: ScanFlagGroupRouted,
      txnId: txnId,
      readTimestamp: 1'u64,
      startKey: "",
      endKey: "",
      limit: 0'u32,
      groupId: groupId
    )
    let encoded = encodeScanRequest(req)
    let decoded = decodeScanRequest(encoded)
    check decoded.isOk
    check decoded.value.groupId == groupId

  test "decodeScanRequest truncated":
    let truncated = "\x01\x04\x01"
    let decoded = decodeScanRequest(truncated)
    check decoded.isErr

  test "encodeScanResponseFrame empty":
    let rf = ScanResponseFrame(
      respFlags: ScanRespFlagEndOfScan,
      pairs: @[],
      reqFlags: 0'u8
    )
    let encoded = encodeScanResponseFrame(rf)
    check encoded.len == 7

  test "encodeScanResponseFrame with pairs":
    let rf = ScanResponseFrame(
      respFlags: ScanRespFlagHasMore,
      pairs: @[
        ScanPair(key: "k1", value: "v1", timestamp: 0'u64, version: 0'u64),
        ScanPair(key: "k2", value: "v2", timestamp: 0'u64, version: 0'u64)
      ],
      reqFlags: 0'u8
    )
    let encoded = encodeScanResponseFrame(rf)
    check encoded.len > 7

  test "encodeScanResponseFrame with timestamp and version":
    let rf = ScanResponseFrame(
      respFlags: 0'u8,
      pairs: @[
        ScanPair(key: "tskey", value: "tsval", timestamp: 1000'u64,
            version: 5'u64)
      ],
      reqFlags: ScanFlagIncludeTimestamp or ScanFlagIncludeVersion
    )
    let encoded = encodeScanResponseFrame(rf)
    check encoded.len > 15

  test "encodeScanResponseFrame keys only":
    let rf = ScanResponseFrame(
      respFlags: ScanRespFlagEndOfScan,
      pairs: @[
        ScanPair(key: "keyonly1", value: "", timestamp: 0'u64, version: 0'u64),
        ScanPair(key: "keyonly2", value: "", timestamp: 0'u64, version: 0'u64)
      ],
      reqFlags: ScanFlagKeysOnly
    )
    let encoded = encodeScanResponseFrame(rf)
    check encoded.len > 7

  test "decodeScanResponseFrame roundtrip":
    let rf = ScanResponseFrame(
      respFlags: ScanRespFlagHasMore or ScanRespFlagEndOfScan,
      pairs: @[
        ScanPair(key: "rk1", value: "rv1", timestamp: 0'u64, version: 0'u64)
      ],
      reqFlags: 0'u8
    )
    let encoded = encodeScanResponseFrame(rf)
    let decoded = decodeScanResponseFrame(encoded, 0'u8)
    check decoded.isOk
    check decoded.value.respFlags == rf.respFlags
    check decoded.value.pairs.len == 1
    check decoded.value.pairs[0].key == "rk1"
    check decoded.value.pairs[0].value == "rv1"

  test "decodeScanResponseFrame with ts and ver":
    let rf = ScanResponseFrame(
      respFlags: 0'u8,
      pairs: @[
        ScanPair(key: "tsvkey", value: "tsvval", timestamp: 12345'u64,
            version: 99'u64)
      ],
      reqFlags: ScanFlagIncludeTimestamp or ScanFlagIncludeVersion
    )
    let encoded = encodeScanResponseFrame(rf)
    let decoded = decodeScanResponseFrame(encoded, ScanFlagIncludeTimestamp or ScanFlagIncludeVersion)
    check decoded.isOk
    check decoded.value.pairs[0].timestamp == 12345'u64
    check decoded.value.pairs[0].version == 99'u64

  test "decodeScanResponseFrame truncated":
    let truncated = "\x01\x04\x01"
    let decoded = decodeScanResponseFrame(truncated, 0'u8)
    check decoded.isErr

suite "KV Constants":

  test "Get flag values":
    check GetFlagIncludeTimestamp == 0x01'u8
    check GetFlagIncludeVersion == 0x02'u8
    check GetFlagGroupRouted == 0x10'u8

  test "Get response flag values":
    check GetRespFlagFound == 0x01'u8
    check GetRespFlagHasTimestamp == 0x02'u8
    check GetRespFlagHasVersion == 0x04'u8

  test "Put flag values":
    check PutFlagReturnPrev == 0x01'u8
    check PutFlagSyncWrite == 0x02'u8
    check PutFlagCAS == 0x04'u8
    check PutFlagGroupRouted == 0x10'u8

  test "Put status values":
    check PutStatusOK == 0x00'u8
    check PutStatusCASFailed == 0x01'u8
    check PutStatusTxnAborted == 0x02'u8

  test "Delete flag values":
    check DelFlagReturnPrev == 0x01'u8
    check DelFlagSyncWrite == 0x02'u8
    check DelFlagOnlyIfExists == 0x04'u8
    check DelFlagGroupRouted == 0x10'u8

  test "Delete status values":
    check DelStatusDeleted == 0x00'u8
    check DelStatusNotFound == 0x01'u8
    check DelStatusTxnAborted == 0x02'u8

  test "Batch flag values":
    check BatchFlagAllOrNothing == 0x01'u8
    check BatchFlagContinueOnErr == 0x02'u8

  test "Batch op values":
    check BatchOpGet == 0x00'u8
    check BatchOpPut == 0x01'u8
    check BatchOpDelete == 0x02'u8

  test "Batch status values":
    check BatchStatusAllOK == 0x00'u8
    check BatchStatusPartialFailure == 0x01'u8
    check BatchStatusAllFailed == 0x02'u8

  test "Scan flag values":
    check ScanFlagIncludeTimestamp == 0x01'u16
    check ScanFlagIncludeVersion == 0x02'u16
    check ScanFlagKeysOnly == 0x04'u16
    check ScanFlagReverse == 0x08'u16
    check ScanFlagGroupRouted == 0x10'u16

  test "Scan response flag values":
    check ScanRespFlagHasMore == 0x01'u8
    check ScanRespFlagEndOfScan == 0x02'u8
