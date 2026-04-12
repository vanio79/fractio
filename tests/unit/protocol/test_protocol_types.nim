# Unit tests for fractio/protocol/types.nim
# Tests protocol version, error codes, message types, and result types

import std/[unittest, strutils]
import fractio/protocol/types

suite "ProtocolVersion":

  test "PROTOCOL_VERSION_1 is defined":
    check PROTOCOL_VERSION_1 == ProtocolVersion(0x0001)

  test "ProtocolVersion equality":
    check ProtocolVersion(0x0001) == ProtocolVersion(0x0001)
    check ProtocolVersion(0x0001) != ProtocolVersion(0x0002)

  test "ProtocolVersion string representation":
    check $PROTOCOL_VERSION_1 == "0x0001"
    check $ProtocolVersion(0x1234) == "0x1234"
    check $ProtocolVersion(0xFFFF) == "0xFFFF"

suite "Magic Bytes":

  test "PROTOCOL_MAGIC is defined":
    check PROTOCOL_MAGIC == "FRC1"
    check PROTOCOL_MAGIC.len == 4

suite "Server Feature Flags":

  test "All feature flags defined":
    check FeatTLS == 1'u32 shl 0
    check FeatCompression == 1'u32 shl 1
    check FeatPipelining == 1'u32 shl 2
    check FeatAsync == 1'u32 shl 3
    check FeatTransactions == 1'u32 shl 4
    check FeatSQL == 1'u32 shl 5
    check FeatGraph == 1'u32 shl 6
    check FeatVector == 1'u32 shl 7
    check FeatRedirect == 1'u32 shl 8
    check FeatProxy == 1'u32 shl 9

  test "Feature flags can be combined":
    let features = FeatTLS or FeatCompression or FeatTransactions
    check (features and FeatTLS) != 0
    check (features and FeatCompression) != 0
    check (features and FeatTransactions) != 0
    check (features and FeatSQL) == 0

  test "Feature flags are unique":
    let allFeatures = [
      FeatTLS, FeatCompression, FeatPipelining, FeatAsync,
      FeatTransactions, FeatSQL, FeatGraph, FeatVector,
      FeatRedirect, FeatProxy
    ]
    for i in 0..<allFeatures.len:
      for j in (i+1)..<allFeatures.len:
        check (allFeatures[i] and allFeatures[j]) == 0

suite "Frame Flags":

  test "All frame flags defined":
    check FlagCompressed == 1'u16 shl 0
    check FlagRequiresAck == 1'u16 shl 1
    check FlagIsResponse == 1'u16 shl 2
    check FlagIsError == 1'u16 shl 3
    check FlagEndOfStream == 1'u16 shl 4

  test "Frame flags can be combined":
    let flags = FlagCompressed or FlagIsResponse
    check (flags and FlagCompressed) != 0
    check (flags and FlagIsResponse) != 0
    check (flags and FlagRequiresAck) == 0

suite "MessageType":

  test "Core/Control message types":
    check mtPing.ord == 0x0001
    check mtEcho.ord == 0x0002
    check mtClose.ord == 0x0003
    check mtCancelStream.ord == 0x0004

  test "KV Operations message types":
    check mtGet.ord == 0x0100
    check mtPut.ord == 0x0101
    check mtDelete.ord == 0x0102
    check mtBatch.ord == 0x0103
    check mtScan.ord == 0x0104
    check mtRawPut.ord == 0x0105

  test "Transactions message types":
    check mtBeginTxn.ord == 0x0200
    check mtCommitTxn.ord == 0x0201
    check mtRollbackTxn.ord == 0x0202
    check mtTxnStatus.ord == 0x0203

  test "Admin/Metrics message types":
    check mtServerInfo.ord == 0x0700
    check mtMetrics.ord == 0x0701
    check mtHealth.ord == 0x0702

  test "Cluster Admin message types":
    check mtJoinNode.ord == 0x0703
    check mtRemoveNode.ord == 0x0704
    check mtListNodes.ord == 0x0705
    check mtRebalanceStatus.ord == 0x0706
    check mtDrainNode.ord == 0x0707

  test "Space Management message types":
    check mtCreateSpace.ord == 0x0708
    check mtDropSpace.ord == 0x0709

  test "Group Creation message types":
    check mtCreateGroup.ord == 0x070A
    check mtJoinGroup.ord == 0x070B

suite "AuthMethod":

  test "All auth methods defined":
    check amNone.ord == 0x00
    check amPassword.ord == 0x01
    check amToken.ord == 0x02
    check amTLS.ord == 0x03

suite "Wire-level Error Codes":

  test "Protocol errors":
    check ErrOK == 0x00000000'u32
    check ErrProtocol == 0x00000001'u32
    check ErrVersion == 0x00000002'u32
    check ErrAuthRequired == 0x00000003'u32
    check ErrAuthFailed == 0x00000004'u32

  test "KV errors":
    check ErrNotFound == 0x01000001'u32
    check ErrAlreadyExists == 0x01000002'u32

  test "Transaction errors":
    check ErrTxnAborted == 0x02000001'u32
    check ErrTxnTimeout == 0x02000002'u32
    check ErrTxnConflict == 0x02000003'u32
    check ErrTxnNotFound == 0x02000004'u32

  test "System errors":
    check ErrNotLeader == 0x07000001'u32
    check ErrClusterDown == 0x07000002'u32
    check ErrOverloaded == 0x07000003'u32
    check ErrInternal == 0x07000004'u32
    check ErrBadRouting == 0x07000005'u32

  test "Error categories":
    check ErrCatProtocol == 0x00'u8
    check ErrCatKV == 0x01'u8
    check ErrCatTransaction == 0x02'u8
    check ErrCatSQL == 0x03'u8
    check ErrCatGraph == 0x04'u8
    check ErrCatVector == 0x05'u8
    check ErrCatAuth == 0x06'u8
    check ErrCatSystem == 0x07'u8

suite "LeaderRedirect":

  test "LeaderRedirect no leader known":
    let r = LeaderRedirect(leaderId: 0)
    check $r == "no leader known"

  test "LeaderRedirect with leader info":
    let r = LeaderRedirect(
      leaderId: 1,
      leaderHost: "127.0.0.1",
      leaderClientPort: 9001
    )
    check $r == "node 1 at 127.0.0.1:9001"

  test "LeaderRedirect different ports":
    let r = LeaderRedirect(
      leaderId: 5,
      leaderHost: "192.168.1.100",
      leaderClientPort: 8080
    )
    check $r == "node 5 at 192.168.1.100:8080"

suite "isNotLeaderError helper":

  test "isNotLeaderError detects 'not leader'":
    check isNotLeaderError("not leader") == true
    check isNotLeaderError("NOT LEADER") == true
    check isNotLeaderError("you are not leader") == true

  test "isNotLeaderError detects 'not the leader'":
    check isNotLeaderError("not the leader") == true
    check isNotLeaderError("NOT THE LEADER") == true

  test "isNotLeaderError detects wire error code":
    check isNotLeaderError("server error 0x07000001") == true
    check isNotLeaderError("0x07000001") == true

  test "isNotLeaderError detects NuRaft code":
    check isNotLeaderError("code -3") == true

  test "isNotLeaderError detects group not found":
    check isNotLeaderError("group not found") == true
    check isNotLeaderError("GROUP NOT FOUND") == true

  test "isNotLeaderError returns false for other errors":
    check isNotLeaderError("timeout") == false
    check isNotLeaderError("connection refused") == false
    check isNotLeaderError("internal error") == false

suite "ProtocolError":

  test "newProtocolError basic":
    let err = newProtocolError(peTimeout, "Operation timed out")
    check err.kind == peTimeout
    check err.msg == "Operation timed out"

  test "newProtocolError with redirect":
    let redirect = LeaderRedirect(leaderId: 1, leaderHost: "127.0.0.1",
        leaderClientPort: 9001)
    let err = newProtocolError(peNotLeader, "Not the leader", redirect)
    check err.kind == peNotLeader
    check err.leaderRedirect.leaderId == 1

  test "ProtocolError string representation":
    let err = newProtocolError(peInvalidFrame, "Bad frame")
    check "ProtocolError[peInvalidFrame]" in $err
    check "Bad frame" in $err

  test "ProtocolError string with redirect":
    let redirect = LeaderRedirect(leaderId: 1, leaderHost: "127.0.0.1",
        leaderClientPort: 9001)
    let err = newProtocolError(peNotLeader, "Not leader", redirect)
    check "redirect to" in $err

  test "isNotLeader on ProtocolError":
    let err1 = newProtocolError(peNotLeader, "Not leader")
    check err1.isNotLeader() == true

    let err2 = newProtocolError(peGroupNotFound, "Group missing")
    check err2.isNotLeader() == true

    let err3 = newProtocolError(peTimeout, "Timeout")
    check err3.isNotLeader() == false

  test "All ProtocolErrorKind values":
    check peInvalidFrame.ord == 0
    check peChecksumMismatch.ord == 1
    check peFrameTooLarge.ord == 2
    check peUnknownMessageType.ord == 3
    check peVersionMismatch.ord == 4
    check peAuthFailed.ord == 5
    check peNotLeader.ord == 6
    check peGroupNotFound.ord == 7
    check peTimeout.ord == 8
    check peBoundsOverflow.ord == 9
    check peInternal.ord == 10

suite "Result[T, E]":

  test "ok creates successful result":
    var r: Result[int, ProtocolError]
    r = Result[int, ProtocolError](isOk: true, val: 42)
    check r.isOk == true
    check r.isErr() == false
    check r.value() == 42

  test "Result with error":
    let err = newProtocolError(peTimeout, "Timed out")
    let r: Result[int, ProtocolError] = Result[int, ProtocolError](
      isOk: false,
      err: err
    )
    check r.isOk == false
    check r.isErr() == true
    check r.error().kind == peTimeout

  test "Result with string value":
    var r: Result[string, ProtocolError]
    r = Result[string, ProtocolError](isOk: true, val: "hello")
    check r.value() == "hello"

  test "Result with bool value":
    var r: Result[bool, ProtocolError]
    r = Result[bool, ProtocolError](isOk: true, val: true)
    check r.value() == true

suite "PResult (Void Result)":

  test "pOk creates successful PResult":
    let r = pOk()
    check r.isOk == true
    check r.isOk() == true
    check r.isErr() == false

  test "pErr creates error PResult":
    let err = newProtocolError(peInternal, "Internal error")
    let r = pErr(err)
    check r.isOk == false
    check r.isErr() == true
    check r.error().kind == peInternal

  test "PResult error retrieval":
    let err = newProtocolError(peAuthFailed, "Auth failed")
    let r = pErr(err)
    check r.error().msg == "Auth failed"

suite "TxnState":

  test "All transaction states defined":
    check tsActive.ord == 0
    check tsCommitted.ord == 1
    check tsAborted.ord == 2
    check tsTimedOut.ord == 3

suite "CommitResult":

  test "CommitResult successful":
    let r = CommitResult(
      committed: true,
      commitTimestamp: 1000'u64,
      conflictKey: ""
    )
    check r.committed == true
    check r.commitTimestamp == 1000'u64
    check r.conflictKey == ""

  test "CommitResult with conflict":
    let r = CommitResult(
      committed: false,
      commitTimestamp: 0'u64,
      conflictKey: "conflicting-key"
    )
    check r.committed == false
    check r.conflictKey == "conflicting-key"

suite "Limits":

  test "MAX_KEY_BYTES":
    check MAX_KEY_BYTES == 4 * 1024

  test "MAX_VALUE_BYTES":
    check MAX_VALUE_BYTES == 64 * 1024 * 1024

  test "MAX_BATCH_OPS":
    check MAX_BATCH_OPS == 10_000
