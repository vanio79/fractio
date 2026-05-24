# Unit tests for fractio/protocol/messages/kv.nim
# Tests Get, Put, Delete, Batch, Scan encoding/decoding

import std/[unittest, options]
import fractio/protocol/messages/kv
import fractio/protocol/types
import fractio/protocol/codec
import fractio/core/types
import fractio/distributed/raft/group_types
import fractio/sql/data_row # for DataRow type in filter evaluation tests

suite "Get Request Messages":

  test "encodeGetRequest basic":
    let txnId = genULIDLocal()
    let req = GetRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      readTimestamp: 0'u64,
      key: "test_key",
      groupId: ZeroGroupID()
    )
    let encoded = encodeGetRequest(req)
    check encoded.len > 2 # Has message type + fields
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtGet)

  test "encodeGetRequest with IncludeTimestamp flag":
    let txnId = genULIDLocal()
    let req = GetRequest(
      flags: GetFlagIncludeTimestamp,
      txnId: TransactionID(txnId),
      readTimestamp: 1000'u64,
      key: "key1",
      groupId: ZeroGroupID()
    )
    let encoded = encodeGetRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check flags.value == GetFlagIncludeTimestamp

  test "encodeGetRequest with IncludeVersion flag":
    let txnId = genULIDLocal()
    let req = GetRequest(
      flags: GetFlagIncludeVersion,
      txnId: TransactionID(txnId),
      readTimestamp: 0'u64,
      key: "key2",
      groupId: ZeroGroupID()
    )
    let encoded = encodeGetRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check flags.value == GetFlagIncludeVersion

  test "encodeGetRequest with GroupRouted flag":
    let txnId = genULIDLocal()
    let groupId = groupIDFromInt(1)
    let req = GetRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      readTimestamp: 0'u64,
      key: "routed_key",
      groupId: groupId
    )
    let encoded = encodeGetRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check (flags.value and GetFlagGroupRouted) != 0'u8

  test "encodeGetRequest combined flags":
    let txnId = genULIDLocal()
    let req = GetRequest(
      flags: GetFlagIncludeTimestamp or GetFlagIncludeVersion,
      txnId: TransactionID(txnId),
      readTimestamp: 5000'u64,
      key: "key3",
      groupId: ZeroGroupID()
    )
    let encoded = encodeGetRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check flags.value == (GetFlagIncludeTimestamp or GetFlagIncludeVersion)

  test "encodeGetRequest empty key":
    let txnId = genULIDLocal()
    let req = GetRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      readTimestamp: 0'u64,
      key: "",
      groupId: ZeroGroupID()
    )
    let encoded = encodeGetRequest(req)
    let decoded = decodeGetRequest(encoded)
    check decoded.isOk
    check decoded.value.key == ""

  test "encodeGetRequest binary key":
    let txnId = genULIDLocal()
    let req = GetRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      readTimestamp: 0'u64,
      key: "\x00\x01\x02\xff",
      groupId: ZeroGroupID()
    )
    let encoded = encodeGetRequest(req)
    let decoded = decodeGetRequest(encoded)
    check decoded.isOk
    check decoded.value.key == "\x00\x01\x02\xff"

  test "decodeGetRequest valid":
    let txnId = genULIDLocal()
    let req = GetRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      readTimestamp: 12345'u64,
      key: "test_key",
      groupId: ZeroGroupID()
    )
    let encoded = encodeGetRequest(req)
    let decoded = decodeGetRequest(encoded)
    check decoded.isOk
    check decoded.value.key == "test_key"
    check decoded.value.readTimestamp == 12345'u64

  test "decodeGetRequest with groupId":
    let txnId = genULIDLocal()
    let groupId = groupIDFromInt(42)
    let req = GetRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      readTimestamp: 0'u64,
      key: "grouped_key",
      groupId: groupId
    )
    let encoded = encodeGetRequest(req)
    let decoded = decodeGetRequest(encoded)
    check decoded.isOk
    check decoded.value.groupId == groupId

  test "decodeGetRequest truncated flags":
    let invalid = "\x01\x00" # Just message type
    let decoded = decodeGetRequest(invalid)
    check decoded.isErr

  test "decodeGetRequest truncated txnId":
    let invalid = "\x01\x00\x00" # Message type + flags, no txnId
    let decoded = decodeGetRequest(invalid)
    check decoded.isErr

  test "decodeGetRequest truncated key":
    let txnId = genULIDLocal()
    let txnBytes = ulidToBytes(txnId)
    let invalid = "\x01\x00\x00" & txnBytes &
        "\x00\x00\x00\x00\x00\x00\x00\x00" # no key length
    let decoded = decodeGetRequest(invalid)
    check decoded.isErr

  test "encodeGetRequest with filter":
    let txnId = genULIDLocal()
    let filterExpr = WireFilterExpr(
      kind: wekBinOp,
      binOpKind: wboEq,
      binLeft: WireFilterExpr(kind: wekColumn, colName: "id"),
      binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt, litIntVal: 1)
    )
    let req = GetRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      readTimestamp: 0'u64,
      key: "filter_key",
      groupId: ZeroGroupID(),
      filter: some(filterExpr)
    )
    let encoded = encodeGetRequest(req)
    var pos = 2
    let flagsR = readUint8(encoded, pos)
    check (flagsR.value and GetFlagHasFilter) != 0'u8

  test "decodeGetRequest with filter roundtrip":
    let txnId = genULIDLocal()
    let filterExpr = WireFilterExpr(
      kind: wekBinOp,
      binOpKind: wboEq,
      binLeft: WireFilterExpr(kind: wekColumn, colName: "status"),
      binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
          litStringVal: "active")
    )
    let req = GetRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      readTimestamp: 0'u64,
      key: "user:123",
      groupId: ZeroGroupID(),
      filter: some(filterExpr)
    )
    let encoded = encodeGetRequest(req)
    let decoded = decodeGetRequest(encoded)
    check decoded.isOk
    check decoded.value.filter.isSome
    let decodedFilter = decoded.value.filter.get()
    check decodedFilter.kind == wekBinOp
    check decodedFilter.binOpKind == wboEq
    check decodedFilter.binLeft.colName == "status"
    check decodedFilter.binRight.litDataType == wdtString
    check decodedFilter.binRight.litStringVal == "active"

  test "decodeGetRequest with filter and groupId":
    let txnId = genULIDLocal()
    let groupId = groupIDFromInt(5)
    let filterExpr = WireFilterExpr(
      kind: wekBinOp,
      binOpKind: wboGt,
      binLeft: WireFilterExpr(kind: wekColumn, colName: "age"),
      binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt, litIntVal: 18)
    )
    let req = GetRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      readTimestamp: 0'u64,
      key: "person:456",
      groupId: groupId,
      filter: some(filterExpr)
    )
    let encoded = encodeGetRequest(req)
    let decoded = decodeGetRequest(encoded)
    check decoded.isOk
    check decoded.value.groupId == groupId
    check decoded.value.filter.isSome
    check decoded.value.filter.get().binOpKind == wboGt

  test "GetFlagHasFilter value":
    check GetFlagHasFilter == 0x20'u8

suite "Get Response Messages":

  test "encodeGetResponse not found":
    let resp = GetResponse(found: false)
    let encoded = encodeGetResponse(resp)
    check encoded.len == 3 # 2 byte type + 1 byte flags
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtGet)
    let flags = readUint8(encoded, pos)
    check flags.value == 0x00'u8

  test "encodeGetResponse found":
    let resp = GetResponse(
      found: true,
      value: "test_value",
      hasTimestamp: false,
      hasVersion: false
    )
    let encoded = encodeGetResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check (flags.value and GetRespFlagFound) != 0'u8

  test "encodeGetResponse with timestamp":
    let resp = GetResponse(
      found: true,
      value: "value1",
      timestamp: 1000'u64,
      hasTimestamp: true,
      hasVersion: false
    )
    let encoded = encodeGetResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check (flags.value and GetRespFlagHasTimestamp) != 0'u8
    let ts = readUint64BE(encoded, pos)
    check ts.value == 1000'u64

  test "encodeGetResponse with version":
    let resp = GetResponse(
      found: true,
      value: "value2",
      version: 5'u64,
      hasTimestamp: false,
      hasVersion: true
    )
    let encoded = encodeGetResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check (flags.value and GetRespFlagHasVersion) != 0'u8
    let ver = readUint64BE(encoded, pos)
    check ver.value == 5'u64

  test "encodeGetResponse with both timestamp and version":
    let resp = GetResponse(
      found: true,
      value: "value3",
      timestamp: 2000'u64,
      version: 10'u64,
      hasTimestamp: true,
      hasVersion: true
    )
    let encoded = encodeGetResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check (flags.value and GetRespFlagHasTimestamp) != 0'u8
    check (flags.value and GetRespFlagHasVersion) != 0'u8

  test "encodeGetResponse empty value":
    let resp = GetResponse(
      found: true,
      value: "",
      hasTimestamp: false,
      hasVersion: false
    )
    let encoded = encodeGetResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    discard readUint8(encoded, pos)
    let val = readBytes(encoded, pos)
    check val.value == ""

  test "encodeGetResponse binary value":
    let resp = GetResponse(
      found: true,
      value: "\x00\xff\xfe",
      hasTimestamp: false,
      hasVersion: false
    )
    let encoded = encodeGetResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    discard readUint8(encoded, pos)
    let val = readBytes(encoded, pos)
    check val.value == "\x00\xff\xfe"

  test "decodeGetResponse not found":
    let resp = GetResponse(found: false)
    let encoded = encodeGetResponse(resp)
    let decoded = decodeGetResponse(encoded)
    check decoded.isOk
    check decoded.value.found == false
    check decoded.value.value == ""

  test "decodeGetResponse found":
    let resp = GetResponse(
      found: true,
      value: "decoded_value",
      hasTimestamp: false,
      hasVersion: false
    )
    let encoded = encodeGetResponse(resp)
    let decoded = decodeGetResponse(encoded)
    check decoded.isOk
    check decoded.value.found == true
    check decoded.value.value == "decoded_value"

  test "decodeGetResponse with timestamp":
    let resp = GetResponse(
      found: true,
      value: "val",
      timestamp: 3000'u64,
      hasTimestamp: true,
      hasVersion: false
    )
    let encoded = encodeGetResponse(resp)
    let decoded = decodeGetResponse(encoded)
    check decoded.isOk
    check decoded.value.timestamp == 3000'u64

  test "decodeGetResponse with version":
    let resp = GetResponse(
      found: true,
      value: "val",
      version: 15'u64,
      hasTimestamp: false,
      hasVersion: true
    )
    let encoded = encodeGetResponse(resp)
    let decoded = decodeGetResponse(encoded)
    check decoded.isOk
    check decoded.value.version == 15'u64

  test "decodeGetResponse truncated flags":
    let invalid = "\x01\x00" # Just message type
    let decoded = decodeGetResponse(invalid)
    check decoded.isErr

suite "Put Request Messages":

  test "encodePutRequest basic":
    let txnId = genULIDLocal()
    let req = PutRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      expectedVersion: 0'u64,
      key: "put_key",
      value: "put_value",
      groupId: ZeroGroupID()
    )
    let encoded = encodePutRequest(req)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtPut)

  test "encodePutRequest with ReturnPrev flag":
    let txnId = genULIDLocal()
    let req = PutRequest(
      flags: PutFlagReturnPrev,
      txnId: TransactionID(txnId),
      expectedVersion: 0'u64,
      key: "key1",
      value: "val1",
      groupId: ZeroGroupID()
    )
    let encoded = encodePutRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check (flags.value and PutFlagReturnPrev) != 0'u8

  test "encodePutRequest with SyncWrite flag":
    let txnId = genULIDLocal()
    let req = PutRequest(
      flags: PutFlagSyncWrite,
      txnId: TransactionID(txnId),
      expectedVersion: 0'u64,
      key: "key2",
      value: "val2",
      groupId: ZeroGroupID()
    )
    let encoded = encodePutRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check (flags.value and PutFlagSyncWrite) != 0'u8

  test "encodePutRequest with CAS flag":
    let txnId = genULIDLocal()
    let req = PutRequest(
      flags: PutFlagCAS,
      txnId: TransactionID(txnId),
      expectedVersion: 5'u64,
      key: "cas_key",
      value: "new_val",
      groupId: ZeroGroupID()
    )
    let encoded = encodePutRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    discard readUint8(encoded, pos)
    discard readUint64BE(encoded, pos) # txn bytes
    discard readUint64BE(encoded, pos) # skip txn bytes from pos
    let ev = readUint64BE(encoded, pos)
    check ev.value == 5'u64

  test "encodePutRequest with groupId":
    let txnId = genULIDLocal()
    let groupId = groupIDFromInt(7)
    let req = PutRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      expectedVersion: 0'u64,
      key: "g_key",
      value: "g_val",
      groupId: groupId
    )
    let encoded = encodePutRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check (flags.value and PutFlagGroupRouted) != 0'u8

  test "encodePutRequest combined flags":
    let txnId = genULIDLocal()
    let req = PutRequest(
      flags: PutFlagReturnPrev or PutFlagSyncWrite or PutFlagCAS,
      txnId: TransactionID(txnId),
      expectedVersion: 3'u64,
      key: "combined",
      value: "data",
      groupId: ZeroGroupID()
    )
    let encoded = encodePutRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check flags.value == (PutFlagReturnPrev or PutFlagSyncWrite or PutFlagCAS)

  test "encodePutRequest empty key and value":
    let txnId = genULIDLocal()
    let req = PutRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      expectedVersion: 0'u64,
      key: "",
      value: "",
      groupId: ZeroGroupID()
    )
    let encoded = encodePutRequest(req)
    let decoded = decodePutRequest(encoded)
    check decoded.isOk
    check decoded.value.key == ""
    check decoded.value.value == ""

  test "decodePutRequest valid":
    let txnId = genULIDLocal()
    let req = PutRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      expectedVersion: 0'u64,
      key: "decode_key",
      value: "decode_value",
      groupId: ZeroGroupID()
    )
    let encoded = encodePutRequest(req)
    let decoded = decodePutRequest(encoded)
    check decoded.isOk
    check decoded.value.key == "decode_key"
    check decoded.value.value == "decode_value"

  test "decodePutRequest with groupId":
    let txnId = genULIDLocal()
    let groupId = groupIDFromInt(99)
    let req = PutRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      expectedVersion: 0'u64,
      key: "key",
      value: "val",
      groupId: groupId
    )
    let encoded = encodePutRequest(req)
    let decoded = decodePutRequest(encoded)
    check decoded.isOk
    check decoded.value.groupId == groupId

  test "decodePutRequest truncated":
    let invalid = "\x01\x01" # Just message type
    let decoded = decodePutRequest(invalid)
    check decoded.isErr

  test "encodeRawPutRequest":
    let txnId = genULIDLocal()
    let req = PutRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      expectedVersion: 0'u64,
      key: "raw_key",
      value: "raw_value",
      groupId: ZeroGroupID()
    )
    let encoded = encodeRawPutRequest(req)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtRawPut)

suite "Put Response Messages":

  test "encodePutResponse OK":
    let resp = PutResponse(
      status: PutStatusOK,
      timestamp: 1000'u64,
      version: 1'u64,
      hasPreviousValue: false,
      previousValue: ""
    )
    let encoded = encodePutResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let status = readUint8(encoded, pos)
    check status.value == PutStatusOK

  test "encodePutResponse CASFailed":
    let resp = PutResponse(
      status: PutStatusCASFailed,
      timestamp: 0'u64,
      version: 0'u64,
      hasPreviousValue: false,
      previousValue: ""
    )
    let encoded = encodePutResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let status = readUint8(encoded, pos)
    check status.value == PutStatusCASFailed

  test "encodePutResponse TxnAborted":
    let resp = PutResponse(
      status: PutStatusTxnAborted,
      timestamp: 0'u64,
      version: 0'u64,
      hasPreviousValue: false,
      previousValue: ""
    )
    let encoded = encodePutResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let status = readUint8(encoded, pos)
    check status.value == PutStatusTxnAborted

  test "encodePutResponse with previous value":
    let resp = PutResponse(
      status: PutStatusOK,
      timestamp: 2000'u64,
      version: 2'u64,
      hasPreviousValue: true,
      previousValue: "old_value"
    )
    let encoded = encodePutResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    discard readUint8(encoded, pos)
    discard readUint64BE(encoded, pos)
    discard readUint64BE(encoded, pos)
    let prev = readBytes(encoded, pos)
    check prev.value == "old_value"

  test "encodePutResponse empty previous value":
    let resp = PutResponse(
      status: PutStatusOK,
      timestamp: 3000'u64,
      version: 3'u64,
      hasPreviousValue: false,
      previousValue: ""
    )
    let encoded = encodePutResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    discard readUint8(encoded, pos)
    discard readUint64BE(encoded, pos)
    discard readUint64BE(encoded, pos)
    let prev = readBytes(encoded, pos)
    check prev.value == ""

  test "decodePutResponse OK":
    let resp = PutResponse(
      status: PutStatusOK,
      timestamp: 4000'u64,
      version: 4'u64,
      hasPreviousValue: false,
      previousValue: ""
    )
    let encoded = encodePutResponse(resp)
    let decoded = decodePutResponse(encoded)
    check decoded.isOk
    check decoded.value.status == PutStatusOK
    check decoded.value.timestamp == 4000'u64
    check decoded.value.version == 4'u64

  test "decodePutResponse with previous":
    let resp = PutResponse(
      status: PutStatusOK,
      timestamp: 5000'u64,
      version: 5'u64,
      hasPreviousValue: true,
      previousValue: "prev_val"
    )
    let encoded = encodePutResponse(resp)
    let decoded = decodePutResponse(encoded)
    check decoded.isOk
    check decoded.value.hasPreviousValue == true
    check decoded.value.previousValue == "prev_val"

  test "decodePutResponse truncated":
    let invalid = "\x01\x01" # Just message type
    let decoded = decodePutResponse(invalid)
    check decoded.isErr

suite "Delete Request Messages":

  test "encodeDeleteRequest basic":
    let txnId = genULIDLocal()
    let req = DeleteRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      key: "del_key",
      groupId: ZeroGroupID()
    )
    let encoded = encodeDeleteRequest(req)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtDelete)

  test "encodeDeleteRequest with ReturnPrev flag":
    let txnId = genULIDLocal()
    let req = DeleteRequest(
      flags: DelFlagReturnPrev,
      txnId: TransactionID(txnId),
      key: "key1",
      groupId: ZeroGroupID()
    )
    let encoded = encodeDeleteRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check (flags.value and DelFlagReturnPrev) != 0'u8

  test "encodeDeleteRequest with SyncWrite flag":
    let txnId = genULIDLocal()
    let req = DeleteRequest(
      flags: DelFlagSyncWrite,
      txnId: TransactionID(txnId),
      key: "key2",
      groupId: ZeroGroupID()
    )
    let encoded = encodeDeleteRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check (flags.value and DelFlagSyncWrite) != 0'u8

  test "encodeDeleteRequest with OnlyIfExists flag":
    let txnId = genULIDLocal()
    let req = DeleteRequest(
      flags: DelFlagOnlyIfExists,
      txnId: TransactionID(txnId),
      key: "key3",
      groupId: ZeroGroupID()
    )
    let encoded = encodeDeleteRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check (flags.value and DelFlagOnlyIfExists) != 0'u8

  test "encodeDeleteRequest with groupId":
    let txnId = genULIDLocal()
    let groupId = groupIDFromInt(11)
    let req = DeleteRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      key: "group_del",
      groupId: groupId
    )
    let encoded = encodeDeleteRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check (flags.value and DelFlagGroupRouted) != 0'u8

  test "decodeDeleteRequest valid":
    let txnId = genULIDLocal()
    let req = DeleteRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      key: "valid_del",
      groupId: ZeroGroupID()
    )
    let encoded = encodeDeleteRequest(req)
    let decoded = decodeDeleteRequest(encoded)
    check decoded.isOk
    check decoded.value.key == "valid_del"

  test "decodeDeleteRequest with groupId":
    let txnId = genULIDLocal()
    let groupId = groupIDFromInt(22)
    let req = DeleteRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      key: "key",
      groupId: groupId
    )
    let encoded = encodeDeleteRequest(req)
    let decoded = decodeDeleteRequest(encoded)
    check decoded.isOk
    check decoded.value.groupId == groupId

  test "decodeDeleteRequest truncated":
    let invalid = "\x01\x02" # Just message type
    let decoded = decodeDeleteRequest(invalid)
    check decoded.isErr

suite "Delete Response Messages":

  test "encodeDeleteResponse Deleted":
    let resp = DeleteResponse(
      status: DelStatusDeleted,
      hasPreviousValue: false,
      previousValue: ""
    )
    let encoded = encodeDeleteResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let status = readUint8(encoded, pos)
    check status.value == DelStatusDeleted

  test "encodeDeleteResponse NotFound":
    let resp = DeleteResponse(
      status: DelStatusNotFound,
      hasPreviousValue: false,
      previousValue: ""
    )
    let encoded = encodeDeleteResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let status = readUint8(encoded, pos)
    check status.value == DelStatusNotFound

  test "encodeDeleteResponse TxnAborted":
    let resp = DeleteResponse(
      status: DelStatusTxnAborted,
      hasPreviousValue: false,
      previousValue: ""
    )
    let encoded = encodeDeleteResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let status = readUint8(encoded, pos)
    check status.value == DelStatusTxnAborted

  test "encodeDeleteResponse with previous":
    let resp = DeleteResponse(
      status: DelStatusDeleted,
      hasPreviousValue: true,
      previousValue: "deleted_val"
    )
    let encoded = encodeDeleteResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    discard readUint8(encoded, pos)
    let prev = readBytes(encoded, pos)
    check prev.value == "deleted_val"

  test "decodeDeleteResponse Deleted":
    let resp = DeleteResponse(
      status: DelStatusDeleted,
      hasPreviousValue: false,
      previousValue: ""
    )
    let encoded = encodeDeleteResponse(resp)
    let decoded = decodeDeleteResponse(encoded)
    check decoded.isOk
    check decoded.value.status == DelStatusDeleted

  test "decodeDeleteResponse with previous":
    let resp = DeleteResponse(
      status: DelStatusDeleted,
      hasPreviousValue: true,
      previousValue: "old"
    )
    let encoded = encodeDeleteResponse(resp)
    let decoded = decodeDeleteResponse(encoded)
    check decoded.isOk
    check decoded.value.hasPreviousValue == true
    check decoded.value.previousValue == "old"

  test "decodeDeleteResponse truncated":
    let invalid = "\x01\x02" # Just message type
    let decoded = decodeDeleteResponse(invalid)
    check decoded.isErr

suite "Batch Messages":

  test "encodeBatchRequest empty":
    let txnId = genULIDLocal()
    let req = BatchRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      operations: @[]
    )
    let encoded = encodeBatchRequest(req)
    let decoded = decodeBatchRequest(encoded)
    check decoded.isOk
    check decoded.value.operations.len == 0

  test "encodeBatchRequest single Get":
    let txnId = genULIDLocal()
    let req = BatchRequest(
      flags: BatchFlagAllOrNothing,
      txnId: TransactionID(txnId),
      operations: @[BatchOp(kind: BatchOpGet, flags: 0x00'u8, data: "key_data")]
    )
    let encoded = encodeBatchRequest(req)
    let decoded = decodeBatchRequest(encoded)
    check decoded.isOk
    check decoded.value.operations.len == 1
    check decoded.value.operations[0].kind == BatchOpGet
    check decoded.value.operations[0].data == "key_data"

  test "encodeBatchRequest single Put":
    let txnId = genULIDLocal()
    let req = BatchRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      operations: @[BatchOp(kind: BatchOpPut, flags: 0x00'u8, data: "put_data")]
    )
    let encoded = encodeBatchRequest(req)
    let decoded = decodeBatchRequest(encoded)
    check decoded.isOk
    check decoded.value.operations.len == 1
    check decoded.value.operations[0].kind == BatchOpPut
    check decoded.value.operations[0].data == "put_data"

  test "encodeBatchRequest single Delete":
    let txnId = genULIDLocal()
    let req = BatchRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      operations: @[BatchOp(kind: BatchOpDelete, flags: 0x00'u8,
          data: "del_data")]
    )
    let encoded = encodeBatchRequest(req)
    let decoded = decodeBatchRequest(encoded)
    check decoded.isOk
    check decoded.value.operations.len == 1
    check decoded.value.operations[0].kind == BatchOpDelete
    check decoded.value.operations[0].data == "del_data"

  test "encodeBatchRequest multiple operations":
    let txnId = genULIDLocal()
    let req = BatchRequest(
      flags: BatchFlagContinueOnErr,
      txnId: TransactionID(txnId),
      operations: @[
        BatchOp(kind: BatchOpGet, flags: 0x01'u8, data: "get1"),
        BatchOp(kind: BatchOpPut, flags: 0x02'u8, data: "put1"),
        BatchOp(kind: BatchOpDelete, flags: 0x00'u8, data: "del1")
      ]
    )
    let encoded = encodeBatchRequest(req)
    let decoded = decodeBatchRequest(encoded)
    check decoded.isOk
    check decoded.value.operations.len == 3
    check decoded.value.flags == BatchFlagContinueOnErr

  test "decodeBatchRequest valid":
    let txnId = genULIDLocal()
    let req = BatchRequest(
      flags: BatchFlagAllOrNothing,
      txnId: TransactionID(txnId),
      operations: @[
        BatchOp(kind: BatchOpGet, flags: 0x00'u8, data: "op1"),
        BatchOp(kind: BatchOpPut, flags: 0x00'u8, data: "op2")
      ]
    )
    let encoded = encodeBatchRequest(req)
    let decoded = decodeBatchRequest(encoded)
    check decoded.isOk
    check decoded.value.flags == BatchFlagAllOrNothing
    check decoded.value.operations.len == 2

  test "decodeBatchRequest empty":
    let txnId = genULIDLocal()
    let req = BatchRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      operations: @[]
    )
    let encoded = encodeBatchRequest(req)
    let decoded = decodeBatchRequest(encoded)
    check decoded.isOk
    check decoded.value.operations.len == 0

  test "decodeBatchRequest truncated":
    let invalid = "\x01\x03" # Just message type
    let decoded = decodeBatchRequest(invalid)
    check decoded.isErr

  test "encodeBatchResponse AllOK":
    let resp = BatchResponse(
      status: BatchStatusAllOK,
      results: @[]
    )
    let encoded = encodeBatchResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let status = readUint8(encoded, pos)
    check status.value == BatchStatusAllOK

  test "encodeBatchResponse PartialFailure":
    let resp = BatchResponse(
      status: BatchStatusPartialFailure,
      results: @[BatchOpResult(status: 0x01'u8, data: "err1")]
    )
    let encoded = encodeBatchResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let status = readUint8(encoded, pos)
    check status.value == BatchStatusPartialFailure

  test "encodeBatchResponse AllFailed":
    let resp = BatchResponse(
      status: BatchStatusAllFailed,
      results: @[]
    )
    let encoded = encodeBatchResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let status = readUint8(encoded, pos)
    check status.value == BatchStatusAllFailed

  test "encodeBatchResponse with results":
    let resp = BatchResponse(
      status: BatchStatusAllOK,
      results: @[
        BatchOpResult(status: 0x00'u8, data: "res1"),
        BatchOpResult(status: 0x00'u8, data: "res2")
      ]
    )
    let encoded = encodeBatchResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    discard readUint8(encoded, pos)
    let count = readUint32BE(encoded, pos)
    check count.value == 2'u32

  test "decodeBatchResponse AllOK":
    let resp = BatchResponse(
      status: BatchStatusAllOK,
      results: @[]
    )
    let encoded = encodeBatchResponse(resp)
    let decoded = decodeBatchResponse(encoded)
    check decoded.isOk
    check decoded.value.status == BatchStatusAllOK

  test "decodeBatchResponse with results":
    let resp = BatchResponse(
      status: BatchStatusPartialFailure,
      results: @[
        BatchOpResult(status: 0x00'u8, data: "ok"),
        BatchOpResult(status: 0x01'u8, data: "fail")
      ]
    )
    let encoded = encodeBatchResponse(resp)
    let decoded = decodeBatchResponse(encoded)
    check decoded.isOk
    check decoded.value.results.len == 2

  test "decodeBatchResponse truncated":
    let invalid = "\x01\x03" # Just message type
    let decoded = decodeBatchResponse(invalid)
    check decoded.isErr

suite "Scan Request Messages":

  test "encodeScanRequest basic":
    let txnId = genULIDLocal()
    let req = ScanRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      readTimestamp: 0'u64,
      startKey: "",
      endKey: "",
      limit: 0'u32,
      groupId: ZeroGroupID()
    )
    let encoded = encodeScanRequest(req)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtScan)

  test "encodeScanRequest with flags":
    let txnId = genULIDLocal()
    let req = ScanRequest(
      flags: ScanFlagIncludeTimestamp or ScanFlagIncludeVersion,
      txnId: TransactionID(txnId),
      readTimestamp: 1000'u64,
      startKey: "start",
      endKey: "end",
      limit: 100'u32,
      groupId: ZeroGroupID()
    )
    let encoded = encodeScanRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check flags.value == (ScanFlagIncludeTimestamp or ScanFlagIncludeVersion)

  test "encodeScanRequest KeysOnly":
    let txnId = genULIDLocal()
    let req = ScanRequest(
      flags: ScanFlagKeysOnly,
      txnId: TransactionID(txnId),
      readTimestamp: 0'u64,
      startKey: "",
      endKey: "",
      limit: 50'u32,
      groupId: ZeroGroupID()
    )
    let encoded = encodeScanRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check (flags.value and ScanFlagKeysOnly) != 0'u8

  test "encodeScanRequest Reverse":
    let txnId = genULIDLocal()
    let req = ScanRequest(
      flags: ScanFlagReverse,
      txnId: TransactionID(txnId),
      readTimestamp: 0'u64,
      startKey: "",
      endKey: "",
      limit: 0'u32,
      groupId: ZeroGroupID()
    )
    let encoded = encodeScanRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check (flags.value and ScanFlagReverse) != 0'u8

  test "encodeScanRequest with groupId":
    let txnId = genULIDLocal()
    let groupId = groupIDFromInt(33)
    let req = ScanRequest(
      flags: ScanFlagGroupRouted,
      txnId: TransactionID(txnId),
      readTimestamp: 0'u64,
      startKey: "",
      endKey: "",
      limit: 0'u32,
      groupId: groupId
    )
    let encoded = encodeScanRequest(req)
    let decoded = decodeScanRequest(encoded)
    check decoded.isOk
    check decoded.value.groupId == groupId

  test "encodeScanRequest with range":
    let txnId = genULIDLocal()
    let req = ScanRequest(
      flags: 0x00'u8,
      txnId: TransactionID(txnId),
      readTimestamp: 2000'u64,
      startKey: "a",
      endKey: "z",
      limit: 1000'u32,
      groupId: ZeroGroupID()
    )
    let encoded = encodeScanRequest(req)
    let decoded = decodeScanRequest(encoded)
    check decoded.isOk
    check decoded.value.startKey == "a"
    check decoded.value.endKey == "z"
    check decoded.value.limit == 1000'u32

  test "decodeScanRequest valid":
    let txnId = genULIDLocal()
    let req = ScanRequest(
      flags: ScanFlagKeysOnly,
      txnId: TransactionID(txnId),
      readTimestamp: 3000'u64,
      startKey: "from",
      endKey: "to",
      limit: 200'u32,
      groupId: ZeroGroupID()
    )
    let encoded = encodeScanRequest(req)
    let decoded = decodeScanRequest(encoded)
    check decoded.isOk
    check decoded.value.startKey == "from"
    check decoded.value.endKey == "to"
    check decoded.value.limit == 200'u32

  test "decodeScanRequest with groupId":
    let txnId = genULIDLocal()
    let groupId = groupIDFromInt(44)
    let req = ScanRequest(
      flags: ScanFlagGroupRouted,
      txnId: TransactionID(txnId),
      readTimestamp: 0'u64,
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
    let invalid = "\x01\x04" # Just message type
    let decoded = decodeScanRequest(invalid)
    check decoded.isErr

suite "Scan Response Frame Messages":

  test "encodeScanResponseFrame empty":
    let rf = ScanResponseFrame(
      respFlags: ScanRespFlagEndOfScan,
      pairs: @[],
      reqFlags: 0x00'u8
    )
    let encoded = encodeScanResponseFrame(rf)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check (flags.value and ScanRespFlagEndOfScan) != 0'u8
    let count = readUint32BE(encoded, pos)
    check count.value == 0'u32

  test "encodeScanResponseFrame single pair":
    let rf = ScanResponseFrame(
      respFlags: ScanRespFlagHasMore,
      pairs: @[ScanPair(key: "k1", value: "v1", timestamp: 0'u64,
          version: 0'u64)],
      reqFlags: 0x00'u8
    )
    let encoded = encodeScanResponseFrame(rf)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check (flags.value and ScanRespFlagHasMore) != 0'u8
    let count = readUint32BE(encoded, pos)
    check count.value == 1'u32

  test "encodeScanResponseFrame multiple pairs":
    let rf = ScanResponseFrame(
      respFlags: 0x00'u8,
      pairs: @[
        ScanPair(key: "k1", value: "v1", timestamp: 0'u64, version: 0'u64),
        ScanPair(key: "k2", value: "v2", timestamp: 0'u64, version: 0'u64),
        ScanPair(key: "k3", value: "v3", timestamp: 0'u64, version: 0'u64)
      ],
      reqFlags: 0x00'u8
    )
    let encoded = encodeScanResponseFrame(rf)
    let decoded = decodeScanResponseFrame(encoded, 0x00'u8)
    check decoded.isOk
    check decoded.value.pairs.len == 3
    check decoded.value.pairs[0].key == "k1"
    check decoded.value.pairs[1].key == "k2"
    check decoded.value.pairs[2].key == "k3"

  test "encodeScanResponseFrame with timestamp":
    let rf = ScanResponseFrame(
      respFlags: 0x00'u8,
      pairs: @[ScanPair(key: "k", value: "v", timestamp: 1000'u64,
          version: 0'u64)],
      reqFlags: ScanFlagIncludeTimestamp
    )
    let encoded = encodeScanResponseFrame(rf)
    var pos = 0
    discard readUint16BE(encoded, pos)
    discard readUint8(encoded, pos)
    discard readUint32BE(encoded, pos)
    discard readBytes(encoded, pos) # key
    discard readBytes(encoded, pos) # value
    let ts = readUint64BE(encoded, pos)
    check ts.value == 1000'u64

  test "encodeScanResponseFrame with version":
    let rf = ScanResponseFrame(
      respFlags: 0x00'u8,
      pairs: @[ScanPair(key: "k", value: "v", timestamp: 0'u64,
          version: 5'u64)],
      reqFlags: ScanFlagIncludeVersion
    )
    let encoded = encodeScanResponseFrame(rf)
    var pos = 0
    discard readUint16BE(encoded, pos)
    discard readUint8(encoded, pos)
    discard readUint32BE(encoded, pos)
    discard readBytes(encoded, pos) # key
    discard readBytes(encoded, pos) # value
    let ver = readUint64BE(encoded, pos)
    check ver.value == 5'u64

  test "encodeScanResponseFrame KeysOnly":
    let rf = ScanResponseFrame(
      respFlags: 0x00'u8,
      pairs: @[ScanPair(key: "k", value: "", timestamp: 0'u64, version: 0'u64)],
      reqFlags: ScanFlagKeysOnly
    )
    let encoded = encodeScanResponseFrame(rf)
    var pos = 0
    discard readUint16BE(encoded, pos)
    discard readUint8(encoded, pos)
    discard readUint32BE(encoded, pos)
    discard readBytes(encoded, pos) # key
    let val = readBytes(encoded, pos)
    check val.value == ""

  test "decodeScanResponseFrame empty":
    let rf = ScanResponseFrame(
      respFlags: ScanRespFlagEndOfScan,
      pairs: @[],
      reqFlags: 0x00'u8
    )
    let encoded = encodeScanResponseFrame(rf)
    let decoded = decodeScanResponseFrame(encoded, 0x00'u8)
    check decoded.isOk
    check decoded.value.pairs.len == 0
    check (decoded.value.respFlags and ScanRespFlagEndOfScan) != 0'u8

  test "decodeScanResponseFrame with pairs":
    let rf = ScanResponseFrame(
      respFlags: ScanRespFlagHasMore,
      pairs: @[
        ScanPair(key: "k1", value: "v1", timestamp: 0'u64, version: 0'u64),
        ScanPair(key: "k2", value: "v2", timestamp: 0'u64, version: 0'u64)
      ],
      reqFlags: 0x00'u8
    )
    let encoded = encodeScanResponseFrame(rf)
    let decoded = decodeScanResponseFrame(encoded, 0x00'u8)
    check decoded.isOk
    check decoded.value.pairs.len == 2
    check decoded.value.pairs[0].key == "k1"
    check decoded.value.pairs[0].value == "v1"

  test "decodeScanResponseFrame with timestamp":
    let rf = ScanResponseFrame(
      respFlags: 0x00'u8,
      pairs: @[ScanPair(key: "k", value: "v", timestamp: 2000'u64,
          version: 0'u64)],
      reqFlags: ScanFlagIncludeTimestamp
    )
    let encoded = encodeScanResponseFrame(rf)
    let decoded = decodeScanResponseFrame(encoded, ScanFlagIncludeTimestamp)
    check decoded.isOk
    check decoded.value.pairs[0].timestamp == 2000'u64

  test "decodeScanResponseFrame with version":
    let rf = ScanResponseFrame(
      respFlags: 0x00'u8,
      pairs: @[ScanPair(key: "k", value: "v", timestamp: 0'u64,
          version: 10'u64)],
      reqFlags: ScanFlagIncludeVersion
    )
    let encoded = encodeScanResponseFrame(rf)
    let decoded = decodeScanResponseFrame(encoded, ScanFlagIncludeVersion)
    check decoded.isOk
    check decoded.value.pairs[0].version == 10'u64

  test "decodeScanResponseFrame truncated":
    let invalid = "\x01\x04" # Just message type
    let decoded = decodeScanResponseFrame(invalid, 0x00'u8)
    check decoded.isErr

suite "KV Constants":

  test "GetFlagIncludeTimestamp value":
    check GetFlagIncludeTimestamp == 0x01'u8

  test "GetFlagIncludeVersion value":
    check GetFlagIncludeVersion == 0x02'u8

  test "GetFlagGroupRouted value":
    check GetFlagGroupRouted == 0x10'u8

  test "GetRespFlagFound value":
    check GetRespFlagFound == 0x01'u8

  test "GetRespFlagHasTimestamp value":
    check GetRespFlagHasTimestamp == 0x02'u8

  test "GetRespFlagHasVersion value":
    check GetRespFlagHasVersion == 0x04'u8

  test "PutFlagReturnPrev value":
    check PutFlagReturnPrev == 0x01'u8

  test "PutFlagSyncWrite value":
    check PutFlagSyncWrite == 0x02'u8

  test "PutFlagCAS value":
    check PutFlagCAS == 0x04'u8

  test "PutFlagGroupRouted value":
    check PutFlagGroupRouted == 0x10'u8

  test "PutStatusOK value":
    check PutStatusOK == 0x00'u8

  test "PutStatusCASFailed value":
    check PutStatusCASFailed == 0x01'u8

  test "PutStatusTxnAborted value":
    check PutStatusTxnAborted == 0x02'u8

  test "DelFlagReturnPrev value":
    check DelFlagReturnPrev == 0x01'u8

  test "DelFlagSyncWrite value":
    check DelFlagSyncWrite == 0x02'u8

  test "DelFlagOnlyIfExists value":
    check DelFlagOnlyIfExists == 0x04'u8

  test "DelFlagGroupRouted value":
    check DelFlagGroupRouted == 0x10'u8

  test "DelStatusDeleted value":
    check DelStatusDeleted == 0x00'u8

  test "DelStatusNotFound value":
    check DelStatusNotFound == 0x01'u8

  test "DelStatusTxnAborted value":
    check DelStatusTxnAborted == 0x02'u8

  test "BatchFlagAllOrNothing value":
    check BatchFlagAllOrNothing == 0x01'u8

  test "BatchFlagContinueOnErr value":
    check BatchFlagContinueOnErr == 0x02'u8

  test "BatchOpGet value":
    check BatchOpGet == 0x00'u8

  test "BatchOpPut value":
    check BatchOpPut == 0x01'u8

  test "BatchOpDelete value":
    check BatchOpDelete == 0x02'u8

  test "BatchStatusAllOK value":
    check BatchStatusAllOK == 0x00'u8

  test "BatchStatusPartialFailure value":
    check BatchStatusPartialFailure == 0x01'u8

  test "BatchStatusAllFailed value":
    check BatchStatusAllFailed == 0x02'u8

  test "ScanFlagIncludeTimestamp value":
    check ScanFlagIncludeTimestamp == 0x01'u8

  test "ScanFlagIncludeVersion value":
    check ScanFlagIncludeVersion == 0x02'u8

  test "ScanFlagKeysOnly value":
    check ScanFlagKeysOnly == 0x04'u8

  test "ScanFlagReverse value":
    check ScanFlagReverse == 0x08'u8

  test "ScanFlagGroupRouted value":
    check ScanFlagGroupRouted == 0x10'u8

  test "ScanRespFlagHasMore value":
    check ScanRespFlagHasMore == 0x01'u8

  test "ScanRespFlagEndOfScan value":
    check ScanRespFlagEndOfScan == 0x02'u8

suite "WireFilterExpr encode/decode":

  test "encode literal int":
    let expr = WireFilterExpr(kind: wekLiteral, litDataType: wdtInt,
        litIntVal: 42'i64)
    let encoded = encodeWireFilterExpr(expr)
    check encoded.len == 10 # 1 byte kind + 1 byte dataType + 8 bytes int
    var pos = 0
    let kindR = readUint8(encoded, pos)
    check kindR.value == uint8(wekLiteral)
    let dtR = readUint8(encoded, pos)
    check dtR.value == uint8(wdtInt)
    let valR = readInt64BE(encoded, pos)
    check valR.value == 42'i64

  test "encode literal float":
    let expr = WireFilterExpr(kind: wekLiteral, litDataType: wdtFloat,
        litFloatVal: 3.14)
    let encoded = encodeWireFilterExpr(expr)
    check encoded.len == 10 # 1 byte kind + 1 byte dataType + 8 bytes float
    var pos = 0
    let kindR = readUint8(encoded, pos)
    check kindR.value == uint8(wekLiteral)
    let dtR = readUint8(encoded, pos)
    check dtR.value == uint8(wdtFloat)

  test "encode literal string":
    let expr = WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
        litStringVal: "hello")
    let encoded = encodeWireFilterExpr(expr)
    check encoded.len == 11 # 1 byte kind + 1 byte dataType + 4 byte len + 5 bytes
    var pos = 0
    let kindR = readUint8(encoded, pos)
    check kindR.value == uint8(wekLiteral)
    let dtR = readUint8(encoded, pos)
    check dtR.value == uint8(wdtString)
    let valR = readBytes(encoded, pos)
    check valR.value == "hello"

  test "encode literal bool true":
    let expr = WireFilterExpr(kind: wekLiteral, litDataType: wdtBool,
        litBoolVal: true)
    let encoded = encodeWireFilterExpr(expr)
    check encoded.len == 3 # 1 byte kind + 1 byte dataType + 1 byte bool
    var pos = 0
    let kindR = readUint8(encoded, pos)
    check kindR.value == uint8(wekLiteral)
    let dtR = readUint8(encoded, pos)
    check dtR.value == uint8(wdtBool)
    let valR = readUint8(encoded, pos)
    check valR.value == 1'u8

  test "encode literal bool false":
    let expr = WireFilterExpr(kind: wekLiteral, litDataType: wdtBool,
        litBoolVal: false)
    let encoded = encodeWireFilterExpr(expr)
    check encoded.len == 3
    var pos = 0
    discard readUint8(encoded, pos) # kind
    discard readUint8(encoded, pos) # dataType
    let valR = readUint8(encoded, pos)
    check valR.value == 0'u8

  test "encode literal null":
    let expr = WireFilterExpr(kind: wekLiteral, litDataType: wdtNull)
    let encoded = encodeWireFilterExpr(expr)
    check encoded.len == 2 # 1 byte kind + 1 byte dataType

  test "encode column reference":
    let expr = WireFilterExpr(kind: wekColumn, colName: "id")
    let encoded = encodeWireFilterExpr(expr)
    check encoded.len == 7 # 1 byte kind + 4 byte len + 2 bytes "id"
    var pos = 0
    let kindR = readUint8(encoded, pos)
    check kindR.value == uint8(wekColumn)
    let nameR = readBytes(encoded, pos)
    check nameR.value == "id"

  test "encode binary op eq":
    let left = WireFilterExpr(kind: wekColumn, colName: "id")
    let right = WireFilterExpr(kind: wekLiteral, litDataType: wdtInt, litIntVal: 1)
    let expr = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq, binLeft: left,
        binRight: right)
    let encoded = encodeWireFilterExpr(expr)
    var pos = 0
    let kindR = readUint8(encoded, pos)
    check kindR.value == uint8(wekBinOp)
    let opR = readUint8(encoded, pos)
    check opR.value == uint8(wboEq)

  test "encode AND expression":
    let left = WireFilterExpr(kind: wekLiteral, litDataType: wdtBool,
        litBoolVal: true)
    let right = WireFilterExpr(kind: wekLiteral, litDataType: wdtBool,
        litBoolVal: false)
    let expr = WireFilterExpr(kind: wekBinOp, binOpKind: wboAnd, binLeft: left,
        binRight: right)
    let encoded = encodeWireFilterExpr(expr)
    var pos = 0
    let kindR = readUint8(encoded, pos)
    check kindR.value == uint8(wekBinOp)
    let opR = readUint8(encoded, pos)
    check opR.value == uint8(wboAnd)

  test "encode NOT expression":
    let inner = WireFilterExpr(kind: wekLiteral, litDataType: wdtBool,
        litBoolVal: true)
    let expr = WireFilterExpr(kind: wekUnaryOp, unaryOpKind: wuoNot,
        unaryExpr: inner)
    let encoded = encodeWireFilterExpr(expr)
    var pos = 0
    let kindR = readUint8(encoded, pos)
    check kindR.value == uint8(wekUnaryOp)
    let opR = readUint8(encoded, pos)
    check opR.value == uint8(wuoNot)

  test "encode IS NULL expression":
    let inner = WireFilterExpr(kind: wekColumn, colName: "nullable")
    let expr = WireFilterExpr(kind: wekIsNull, isNullExpr: inner,
        isNullNot: false)
    let encoded = encodeWireFilterExpr(expr)
    var pos = 0
    let kindR = readUint8(encoded, pos)
    check kindR.value == uint8(wekIsNull)
    let notR = readUint8(encoded, pos)
    check notR.value == 0'u8

  test "encode IS NOT NULL expression":
    let inner = WireFilterExpr(kind: wekColumn, colName: "nullable")
    let expr = WireFilterExpr(kind: wekIsNull, isNullExpr: inner,
        isNullNot: true)
    let encoded = encodeWireFilterExpr(expr)
    var pos = 0
    discard readUint8(encoded, pos) # kind
    let notR = readUint8(encoded, pos)
    check notR.value == 1'u8

  test "encode BETWEEN expression":
    let exprCol = WireFilterExpr(kind: wekColumn, colName: "age")
    let lo = WireFilterExpr(kind: wekLiteral, litDataType: wdtInt, litIntVal: 18)
    let hi = WireFilterExpr(kind: wekLiteral, litDataType: wdtInt, litIntVal: 65)
    let expr = WireFilterExpr(kind: wekBetween, betweenExpr: exprCol,
        betweenLo: lo, betweenHi: hi, betweenNot: false)
    let encoded = encodeWireFilterExpr(expr)
    var pos = 0
    let kindR = readUint8(encoded, pos)
    check kindR.value == uint8(wekBetween)

  test "encode LIKE expression":
    let exprCol = WireFilterExpr(kind: wekColumn, colName: "name")
    let pattern = WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
        litStringVal: "%test%")
    let expr = WireFilterExpr(kind: wekLike, likeExpr: exprCol,
        likePattern: pattern, likeNot: false)
    let encoded = encodeWireFilterExpr(expr)
    var pos = 0
    let kindR = readUint8(encoded, pos)
    check kindR.value == uint8(wekLike)

  test "decode literal int roundtrip":
    let expr = WireFilterExpr(kind: wekLiteral, litDataType: wdtInt,
        litIntVal: 42'i64)
    let encoded = encodeWireFilterExpr(expr)
    var pos = 0
    let decodedR = decodeWireFilterExpr(encoded, pos)
    check decodedR.isOk
    let decoded = decodedR.value
    check decoded.kind == wekLiteral
    check decoded.litDataType == wdtInt
    check decoded.litIntVal == 42'i64

  test "decode literal string roundtrip":
    let expr = WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
        litStringVal: "hello")
    let encoded = encodeWireFilterExpr(expr)
    var pos = 0
    let decodedR = decodeWireFilterExpr(encoded, pos)
    check decodedR.isOk
    let decoded = decodedR.value
    check decoded.kind == wekLiteral
    check decoded.litDataType == wdtString
    check decoded.litStringVal == "hello"

  test "decode column reference roundtrip":
    let expr = WireFilterExpr(kind: wekColumn, colName: "id")
    let encoded = encodeWireFilterExpr(expr)
    var pos = 0
    let decodedR = decodeWireFilterExpr(encoded, pos)
    check decodedR.isOk
    let decoded = decodedR.value
    check decoded.kind == wekColumn
    check decoded.colName == "id"

  test "decode binary op eq roundtrip":
    let left = WireFilterExpr(kind: wekColumn, colName: "id")
    let right = WireFilterExpr(kind: wekLiteral, litDataType: wdtInt, litIntVal: 1)
    let expr = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq, binLeft: left,
        binRight: right)
    let encoded = encodeWireFilterExpr(expr)
    var pos = 0
    let decodedR = decodeWireFilterExpr(encoded, pos)
    check decodedR.isOk
    let decoded = decodedR.value
    check decoded.kind == wekBinOp
    check decoded.binOpKind == wboEq
    check decoded.binLeft.kind == wekColumn
    check decoded.binLeft.colName == "id"
    check decoded.binRight.kind == wekLiteral
    check decoded.binRight.litIntVal == 1'i64

  test "decode nested AND/OR roundtrip":
    # (a > 5) AND (b < 10)
    let left = WireFilterExpr(
      kind: wekBinOp,
      binOpKind: wboGt,
      binLeft: WireFilterExpr(kind: wekColumn, colName: "a"),
      binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt, litIntVal: 5)
    )
    let right = WireFilterExpr(
      kind: wekBinOp,
      binOpKind: wboLt,
      binLeft: WireFilterExpr(kind: wekColumn, colName: "b"),
      binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt, litIntVal: 10)
    )
    let expr = WireFilterExpr(kind: wekBinOp, binOpKind: wboAnd, binLeft: left,
        binRight: right)
    let encoded = encodeWireFilterExpr(expr)
    var pos = 0
    let decodedR = decodeWireFilterExpr(encoded, pos)
    check decodedR.isOk
    let decoded = decodedR.value
    check decoded.kind == wekBinOp
    check decoded.binOpKind == wboAnd
    check decoded.binLeft.kind == wekBinOp
    check decoded.binLeft.binOpKind == wboGt
    check decoded.binRight.kind == wekBinOp
    check decoded.binRight.binOpKind == wboLt

  test "decode IS NULL roundtrip":
    let inner = WireFilterExpr(kind: wekColumn, colName: "nullable")
    let expr = WireFilterExpr(kind: wekIsNull, isNullExpr: inner,
        isNullNot: false)
    let encoded = encodeWireFilterExpr(expr)
    var pos = 0
    let decodedR = decodeWireFilterExpr(encoded, pos)
    check decodedR.isOk
    let decoded = decodedR.value
    check decoded.kind == wekIsNull
    check decoded.isNullNot == false
    check decoded.isNullExpr.kind == wekColumn

  test "decode BETWEEN roundtrip":
    let exprCol = WireFilterExpr(kind: wekColumn, colName: "age")
    let lo = WireFilterExpr(kind: wekLiteral, litDataType: wdtInt, litIntVal: 18)
    let hi = WireFilterExpr(kind: wekLiteral, litDataType: wdtInt, litIntVal: 65)
    let expr = WireFilterExpr(kind: wekBetween, betweenExpr: exprCol,
        betweenLo: lo, betweenHi: hi, betweenNot: false)
    let encoded = encodeWireFilterExpr(expr)
    var pos = 0
    let decodedR = decodeWireFilterExpr(encoded, pos)
    check decodedR.isOk
    let decoded = decodedR.value
    check decoded.kind == wekBetween
    check decoded.betweenNot == false
    check decoded.betweenExpr.colName == "age"
    check decoded.betweenLo.litIntVal == 18'i64
    check decoded.betweenHi.litIntVal == 65'i64

suite "ScanRequest with filter":

  test "ScanRequest without filter":
    let txnId = genULIDLocal()
    let req = ScanRequest(
      flags: ScanFlagStreaming,
      txnId: TransactionID(txnId),
      readTimestamp: 0'u64,
      startKey: "",
      endKey: "",
      limit: 100'u32,
      groupId: ZeroGroupID(),
      chunkSize: 50'u32,
      filter: none(WireFilterExpr)
    )
    let encoded = encodeScanRequest(req)
    let decodedR = decodeScanRequest(encoded)
    check decodedR.isOk
    let decoded = decodedR.value
    check decoded.filter.isNone

  test "ScanRequest with filter":
    let txnId = genULIDLocal()
    let filterExpr = WireFilterExpr(
      kind: wekBinOp,
      binOpKind: wboEq,
      binLeft: WireFilterExpr(kind: wekColumn, colName: "id"),
      binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt, litIntVal: 1)
    )
    let req = ScanRequest(
      flags: ScanFlagStreaming,
      txnId: TransactionID(txnId),
      readTimestamp: 0'u64,
      startKey: "",
      endKey: "",
      limit: 100'u32,
      groupId: ZeroGroupID(),
      chunkSize: 50'u32,
      filter: some(filterExpr)
    )
    let encoded = encodeScanRequest(req)
    # Check HasFilter flag is set
    var pos = 2
    let flagsR = readUint8(encoded, pos)
    check (flagsR.value and ScanFlagHasFilter) != 0

    let decodedR = decodeScanRequest(encoded)
    check decodedR.isOk
    let decoded = decodedR.value
    check decoded.filter.isSome
    let decodedFilter = decoded.filter.get()
    check decodedFilter.kind == wekBinOp
    check decodedFilter.binOpKind == wboEq
    check decodedFilter.binLeft.colName == "id"
    check decodedFilter.binRight.litIntVal == 1'i64

  test "ScanFlagHasFilter value":
    check ScanFlagHasFilter == 0x40'u8

suite "WireFilterExpr evaluation (matchesWireFilter)":

  test "matchesWireFilter with no filter returns true":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "id", value: newRowValue(1'i64)))
    row.columns.add(DataRowColumn(name: "name", value: newRowValue("test")))
    check matchesWireFilter(none(WireFilterExpr), row) == true

  test "matchesWireFilter literal int equality":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "id", value: newRowValue(42'i64)))
    row.columns.add(DataRowColumn(name: "status", value: newRowValue("active")))
    let filter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "id"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt,
            litIntVal: 42))
    check matchesWireFilter(some(filter), row) == true

  test "matchesWireFilter literal int inequality":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "id", value: newRowValue(10'i64)))
    row.columns.add(DataRowColumn(name: "status", value: newRowValue("active")))
    let filter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "id"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt,
            litIntVal: 42))
    check matchesWireFilter(some(filter), row) == false

  test "matchesWireFilter literal string equality":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "id", value: newRowValue(1'i64)))
    row.columns.add(DataRowColumn(name: "status", value: newRowValue("active")))
    let filter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "status"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
            litStringVal: "active"))
    check matchesWireFilter(some(filter), row) == true

  test "matchesWireFilter literal string inequality":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "id", value: newRowValue(1'i64)))
    row.columns.add(DataRowColumn(name: "status", value: newRowValue("inactive")))
    let filter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "status"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
            litStringVal: "active"))
    check matchesWireFilter(some(filter), row) == false

  test "matchesWireFilter greater than":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "age", value: newRowValue(25'i64)))
    let filter = WireFilterExpr(kind: wekBinOp, binOpKind: wboGt,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "age"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt,
            litIntVal: 18))
    check matchesWireFilter(some(filter), row) == true

  test "matchesWireFilter less than":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "age", value: newRowValue(15'i64)))
    let filter = WireFilterExpr(kind: wekBinOp, binOpKind: wboLt,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "age"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt,
            litIntVal: 18))
    check matchesWireFilter(some(filter), row) == true

  test "matchesWireFilter AND expression true":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "id", value: newRowValue(42'i64)))
    row.columns.add(DataRowColumn(name: "status", value: newRowValue("active")))
    let leftFilter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "id"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt,
            litIntVal: 42))
    let rightFilter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "status"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
            litStringVal: "active"))
    let filter = WireFilterExpr(kind: wekBinOp, binOpKind: wboAnd,
        binLeft: leftFilter, binRight: rightFilter)
    check matchesWireFilter(some(filter), row) == true

  test "matchesWireFilter AND expression false (left fails)":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "id", value: newRowValue(10'i64)))
    row.columns.add(DataRowColumn(name: "status", value: newRowValue("active")))
    let leftFilter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "id"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt,
            litIntVal: 42))
    let rightFilter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "status"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
            litStringVal: "active"))
    let filter = WireFilterExpr(kind: wekBinOp, binOpKind: wboAnd,
        binLeft: leftFilter, binRight: rightFilter)
    check matchesWireFilter(some(filter), row) == false

  test "matchesWireFilter AND expression false (right fails)":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "id", value: newRowValue(42'i64)))
    row.columns.add(DataRowColumn(name: "status", value: newRowValue("inactive")))
    let leftFilter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "id"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt,
            litIntVal: 42))
    let rightFilter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "status"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
            litStringVal: "active"))
    let filter = WireFilterExpr(kind: wekBinOp, binOpKind: wboAnd,
        binLeft: leftFilter, binRight: rightFilter)
    check matchesWireFilter(some(filter), row) == false

  test "matchesWireFilter OR expression true (left true)":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "status", value: newRowValue("active")))
    let leftFilter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "status"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
            litStringVal: "active"))
    let rightFilter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "status"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
            litStringVal: "pending"))
    let filter = WireFilterExpr(kind: wekBinOp, binOpKind: wboOr,
        binLeft: leftFilter, binRight: rightFilter)
    check matchesWireFilter(some(filter), row) == true

  test "matchesWireFilter OR expression true (right true)":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "status", value: newRowValue("pending")))
    let leftFilter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "status"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
            litStringVal: "active"))
    let rightFilter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "status"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
            litStringVal: "pending"))
    let filter = WireFilterExpr(kind: wekBinOp, binOpKind: wboOr,
        binLeft: leftFilter, binRight: rightFilter)
    check matchesWireFilter(some(filter), row) == true

  test "matchesWireFilter OR expression false":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "status", value: newRowValue("inactive")))
    let leftFilter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "status"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
            litStringVal: "active"))
    let rightFilter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "status"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
            litStringVal: "pending"))
    let filter = WireFilterExpr(kind: wekBinOp, binOpKind: wboOr,
        binLeft: leftFilter, binRight: rightFilter)
    check matchesWireFilter(some(filter), row) == false

  test "matchesWireFilter NOT expression":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "status", value: newRowValue("inactive")))
    let innerFilter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "status"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtString,
            litStringVal: "active"))
    let filter = WireFilterExpr(kind: wekUnaryOp, unaryOpKind: wuoNot,
        unaryExpr: innerFilter)
    check matchesWireFilter(some(filter), row) == true

  test "matchesWireFilter IS NULL true":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "id", value: newRowValue(1'i64)))
    # no "deleted_at" column
    let filter = WireFilterExpr(kind: wekIsNull, isNullNot: false,
        isNullExpr: WireFilterExpr(kind: wekColumn, colName: "deleted_at"))
    check matchesWireFilter(some(filter), row) == true

  test "matchesWireFilter IS NULL false":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "id", value: newRowValue(1'i64)))
    row.columns.add(DataRowColumn(name: "deleted_at", value: newRowValue("2024-01-01")))
    let filter = WireFilterExpr(kind: wekIsNull, isNullNot: false,
        isNullExpr: WireFilterExpr(kind: wekColumn, colName: "deleted_at"))
    check matchesWireFilter(some(filter), row) == false

  test "matchesFilter IS NOT NULL true":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "id", value: newRowValue(1'i64)))
    row.columns.add(DataRowColumn(name: "deleted_at", value: newRowValue("2024-01-01")))
    let filter = WireFilterExpr(kind: wekIsNull, isNullNot: true,
        isNullExpr: WireFilterExpr(kind: wekColumn, colName: "deleted_at"))
    check matchesWireFilter(some(filter), row) == true

  test "matchesFilter IS NOT NULL false":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "id", value: newRowValue(1'i64)))
    # no "deleted_at" column
    let filter = WireFilterExpr(kind: wekIsNull, isNullNot: true,
        isNullExpr: WireFilterExpr(kind: wekColumn, colName: "deleted_at"))
    check matchesWireFilter(some(filter), row) == false

  test "matchesWireFilter BETWEEN true":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "age", value: newRowValue(30'i64)))
    let filter = WireFilterExpr(kind: wekBetween, betweenNot: false,
        betweenExpr: WireFilterExpr(kind: wekColumn, colName: "age"),
        betweenLo: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt,
            litIntVal: 18),
        betweenHi: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt,
            litIntVal: 65))
    check matchesWireFilter(some(filter), row) == true

  test "matchesWireFilter BETWEEN false (below range)":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "age", value: newRowValue(10'i64)))
    let filter = WireFilterExpr(kind: wekBetween, betweenNot: false,
        betweenExpr: WireFilterExpr(kind: wekColumn, colName: "age"),
        betweenLo: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt,
            litIntVal: 18),
        betweenHi: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt,
            litIntVal: 65))
    check matchesWireFilter(some(filter), row) == false

  test "matchesWireFilter BETWEEN false (above range)":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "age", value: newRowValue(70'i64)))
    let filter = WireFilterExpr(kind: wekBetween, betweenNot: false,
        betweenExpr: WireFilterExpr(kind: wekColumn, colName: "age"),
        betweenLo: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt,
            litIntVal: 18),
        betweenHi: WireFilterExpr(kind: wekLiteral, litDataType: wdtInt,
            litIntVal: 65))
    check matchesWireFilter(some(filter), row) == false

  test "matchesWireFilter bool literal true":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "active", value: newRowValue(true)))
    let filter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "active"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtBool,
            litBoolVal: true))
    check matchesWireFilter(some(filter), row) == true

  test "matchesWireFilter bool literal false":
    var row = newDataRow()
    row.columns.add(DataRowColumn(name: "active", value: newRowValue(false)))
    let filter = WireFilterExpr(kind: wekBinOp, binOpKind: wboEq,
        binLeft: WireFilterExpr(kind: wekColumn, colName: "active"),
        binRight: WireFilterExpr(kind: wekLiteral, litDataType: wdtBool,
            litBoolVal: false))
    check matchesWireFilter(some(filter), row) == true
