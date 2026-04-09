# Unit tests for TwoPCRequest/TwoPCResponse binary serialization

import unittest
import std/strutils
import fractio/core/types
import fractio/core/two_phase_commit
import fractio/utils/binary

suite "TwoPCRequest Binary Serialization":
  test "encode minimal TwoPCRequest":
    let txnId = genTransactionID()
    let req = TwoPCRequest(
      requestId: "req_001",
      requestType: tpcPrepare,
      transactionId: txnId,
      coordinatorId: "coord_1",
      timestamp: Timestamp(1000000),
      data: "",
      participantEndpoints: @[]
    )
    let encoded = encodeTwoPCRequest(req)
    # Minimum: 3 (magic) + 1 (version) + 1 (type) + str(reqId) + 16 (txnId) + str(coordId) + 8 (ts) + str(data) + 4 (ep count)
    check encoded.len > 25
    check encoded[0] == '2'
    check encoded[1] == 'P'
    check encoded[2] == 'R'

  test "encode TwoPCRequest with data and endpoints":
    let txnId = genTransactionID()
    let req = TwoPCRequest(
      requestId: "req_002",
      requestType: tpcCommit,
      transactionId: txnId,
      coordinatorId: "coord_1",
      timestamp: Timestamp(2000000),
      data: "some transaction data",
      participantEndpoints: @["node1:8080", "node2:8080", "node3:8080"]
    )
    let encoded = encodeTwoPCRequest(req)
    check encoded.len > 30

  test "decode TwoPCRequest roundtrip":
    let txnId = genTransactionID()
    let req = TwoPCRequest(
      requestId: "req_003",
      requestType: tpcRollback,
      transactionId: txnId,
      coordinatorId: "coord_2",
      timestamp: Timestamp(3000000),
      data: "rollback data with \"quotes\"",
      participantEndpoints: @["endpoint1", "endpoint2"]
    )
    let encoded = encodeTwoPCRequest(req)
    let decoded = decodeTwoPCRequest(encoded)
    check decoded.requestId == req.requestId
    check decoded.requestType == req.requestType
    check decoded.transactionId == req.transactionId
    check decoded.coordinatorId == req.coordinatorId
    check decoded.timestamp == req.timestamp
    check decoded.data == req.data
    check decoded.participantEndpoints == req.participantEndpoints

  test "decode all request types":
    for rt in [tpcPrepare, tpcCommit, tpcRollback, tpcRecovery, tpcHeartbeat]:
      let req = TwoPCRequest(
        requestId: "req_rt",
        requestType: rt,
        transactionId: genTransactionID(),
        coordinatorId: "coord",
        timestamp: Timestamp(0),
        data: "",
        participantEndpoints: @[]
      )
      let encoded = encodeTwoPCRequest(req)
      let decoded = decodeTwoPCRequest(encoded)
      check decoded.requestType == rt

  test "decode rejects invalid magic":
    let badData = "XXX\x01\x00" & "\x00".repeat(20)
    expect ValueError:
      discard decodeTwoPCRequest(badData)

  test "decode rejects unsupported version":
    var w = initBinaryWriter()
    w.writeBytes(TWOPC_REQUEST_MAGIC)
    w.writeU8(99'u8) # Invalid version
    w.writeU8(0'u8)
    w.writeString("req")
    w.writeBytes("\x00".repeat(16)) # txnId
    w.writeString("coord")
    w.writeI64(0'i64)
    w.writeString("")
    w.writeU32(0'u32)
    let encoded = w.finish()
    expect ValueError:
      discard decodeTwoPCRequest(encoded)

suite "TwoPCResponse Binary Serialization":
  test "encode minimal TwoPCResponse":
    let txnId = genTransactionID()
    let resp = TwoPCResponse(
      requestId: "req_001",
      transactionId: txnId,
      participantId: "part_1",
      vote: pvYes,
      state: tpcsPrepared,
      error: ""
    )
    let encoded = encodeTwoPCResponse(resp)
    check encoded.len > 10
    check encoded[0] == '2'
    check encoded[1] == 'P'
    check encoded[2] == 'S'

  test "encode TwoPCResponse with error":
    let txnId = genTransactionID()
    let resp = TwoPCResponse(
      requestId: "req_002",
      transactionId: txnId,
      participantId: "part_2",
      vote: pvNo,
      state: tpcsAborted,
      error: "Transaction timeout"
    )
    let encoded = encodeTwoPCResponse(resp)
    check encoded.len > 20

  test "decode TwoPCResponse roundtrip":
    let txnId = genTransactionID()
    let resp = TwoPCResponse(
      requestId: "req_003",
      transactionId: txnId,
      participantId: "part_3",
      vote: pvAbstain,
      state: tpcsCommitted,
      error: ""
    )
    let encoded = encodeTwoPCResponse(resp)
    let decoded = decodeTwoPCResponse(encoded)
    check decoded.requestId == resp.requestId
    check decoded.transactionId == resp.transactionId
    check decoded.participantId == resp.participantId
    check decoded.vote == resp.vote
    check decoded.state == resp.state
    check decoded.error == resp.error

  test "decode all vote types":
    for v in [pvYes, pvNo, pvAbstain]:
      let resp = TwoPCResponse(
        requestId: "req_vote",
        transactionId: genTransactionID(),
        participantId: "part",
        vote: v,
        state: tpcsPrepared,
        error: ""
      )
      let encoded = encodeTwoPCResponse(resp)
      let decoded = decodeTwoPCResponse(encoded)
      check decoded.vote == v

  test "decode all 2PC states":
    for s in [tpcsIdle, tpcsPreparing, tpcsPrepared, tpcsCommitting,
              tpcsCommitted, tpcsAborting, tpcsAborted, tpcsRecovering]:
      let resp = TwoPCResponse(
        requestId: "req_state",
        transactionId: genTransactionID(),
        participantId: "part",
        vote: pvYes,
        state: s,
        error: ""
      )
      let encoded = encodeTwoPCResponse(resp)
      let decoded = decodeTwoPCResponse(encoded)
      check decoded.state == s

  test "decode response rejects invalid magic":
    let badData = "XXX\x01\x00\x00" & "\x00".repeat(20)
    expect ValueError:
      discard decodeTwoPCResponse(badData)

  test "decode response rejects unsupported version":
    var w = initBinaryWriter()
    w.writeBytes(TWOPC_RESPONSE_MAGIC)
    w.writeU8(99'u8) # Invalid version
    w.writeU8(0'u8)
    w.writeU8(0'u8)
    w.writeString("req")
    w.writeBytes("\x00".repeat(16)) # txnId
    w.writeString("part")
    w.writeString("")
    let encoded = w.finish()
    expect ValueError:
      discard decodeTwoPCResponse(encoded)

suite "TwoPC Binary Serialization - Edge Cases":
  test "large data field":
    let largeData = "x".repeat(50000)
    let req = TwoPCRequest(
      requestId: "req_large",
      requestType: tpcCommit,
      transactionId: genTransactionID(),
      coordinatorId: "coord",
      timestamp: Timestamp(0),
      data: largeData,
      participantEndpoints: @[]
    )
    let encoded = encodeTwoPCRequest(req)
    let decoded = decodeTwoPCRequest(encoded)
    check decoded.data.len == 50000
    check decoded.data == largeData

  test "many participant endpoints":
    var endpoints: seq[string] = @[]
    for i in 0..<100:
      endpoints.add("node" & $i & ":8080")
    let req = TwoPCRequest(
      requestId: "req_many",
      requestType: tpcPrepare,
      transactionId: genTransactionID(),
      coordinatorId: "coord",
      timestamp: Timestamp(0),
      data: "",
      participantEndpoints: endpoints
    )
    let encoded = encodeTwoPCRequest(req)
    let decoded = decodeTwoPCRequest(encoded)
    check decoded.participantEndpoints.len == 100
    check decoded.participantEndpoints[0] == "node0:8080"
    check decoded.participantEndpoints[99] == "node99:8080"

  test "special characters in strings":
    let req = TwoPCRequest(
      requestId: "req_special",
      requestType: tpcCommit,
      transactionId: genTransactionID(),
      coordinatorId: "coord with \"quotes\" and \\backslash\\",
      timestamp: Timestamp(0),
      data: "data\nwith\nnewlines\tand\ttabs",
      participantEndpoints: @["endpoint:with:colons"]
    )
    let encoded = encodeTwoPCRequest(req)
    let decoded = decodeTwoPCRequest(encoded)
    check decoded.coordinatorId == req.coordinatorId
    check decoded.data == req.data
    check decoded.participantEndpoints[0] == "endpoint:with:colons"
