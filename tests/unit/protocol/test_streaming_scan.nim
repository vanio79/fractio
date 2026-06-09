# Unit tests for streaming scan protocol
# Tests ScanFlagStreaming, chunkSize, ScanResponseFrame, and StreamingScanClient

import unittest
import std/options
import std/atomics
import fractio/protocol/messages/kv as kvMsgs
import fractio/protocol/types
import fractio/protocol/client
import fractio/core/types
import fractio/distributed/raft/group_types

suite "Scan Streaming Flags":
  test "ScanFlagStreaming is defined":
    check kvMsgs.ScanFlagStreaming == 0x20'u16

  test "Scan streaming flag can combine with other flags":
    let flags = kvMsgs.ScanFlagStreaming or kvMsgs.ScanFlagIncludeTimestamp
    check (flags and kvMsgs.ScanFlagStreaming) != 0
    check (flags and kvMsgs.ScanFlagIncludeTimestamp) != 0

  test "Scan response flags":
    check kvMsgs.ScanRespFlagHasMore == 0x01'u8
    check kvMsgs.ScanRespFlagEndOfScan == 0x02'u8

  test "DEFAULT_SCAN_CHUNK_SIZE":
    check kvMsgs.DEFAULT_SCAN_CHUNK_SIZE == 1000

suite "ScanRequest with Streaming":
  test "ScanRequest with chunkSize":
    let req = kvMsgs.ScanRequest(
      flags: kvMsgs.ScanFlagStreaming,
      txnId: zeroTransactionID(),
      readTimestamp: 0,
      startKey: "key_",
      endKey: "key_z",
      limit: 100,
      groupId: ZeroGroupID(),
      chunkSize: 500
    )
    check req.chunkSize == 500

  test "ScanRequest streaming encode includes chunkSize":
    let req = kvMsgs.ScanRequest(
      flags: kvMsgs.ScanFlagStreaming,
      txnId: zeroTransactionID(),
      readTimestamp: 0,
      startKey: "a",
      endKey: "z",
      limit: 10,
      groupId: ZeroGroupID(),
      chunkSize: 100
    )
    let encoded = kvMsgs.encodeScanRequest(req)
    # Check that the encoded payload has streaming flag set
    # and chunkSize is included (encoded length will be larger)
    check encoded.len > 20 # Minimum without streaming/chunkSize

  test "ScanRequest streaming decode":
    let req = kvMsgs.ScanRequest(
      flags: kvMsgs.ScanFlagStreaming or kvMsgs.ScanFlagKeysOnly,
      txnId: zeroTransactionID(),
      readTimestamp: 1000,
      startKey: "start",
      endKey: "end",
      limit: 50,
      groupId: ZeroGroupID(),
      chunkSize: 200
    )
    let encoded = kvMsgs.encodeScanRequest(req)
    let decodedR = kvMsgs.decodeScanRequest(encoded)
    check decodedR.isOk
    let decoded = decodedR.value
    check decoded.flags == req.flags
    check decoded.chunkSize == req.chunkSize
    check decoded.startKey == req.startKey
    check decoded.endKey == req.endKey

  test "ScanRequest streaming decode uses default chunkSize if zero":
    let req = kvMsgs.ScanRequest(
      flags: kvMsgs.ScanFlagStreaming,
      txnId: zeroTransactionID(),
      readTimestamp: 0,
      startKey: "",
      endKey: "",
      limit: 0,
      groupId: ZeroGroupID(),
      chunkSize: 0 # Should become DEFAULT_SCAN_CHUNK_SIZE after decode
    )
    let encoded = kvMsgs.encodeScanRequest(req)
    let decodedR = kvMsgs.decodeScanRequest(encoded)
    check decodedR.isOk
    check decodedR.value.chunkSize == uint32(kvMsgs.DEFAULT_SCAN_CHUNK_SIZE)

suite "ScanResponseFrame":
  test "ScanResponseFrame with hasMore flag":
    let frame = kvMsgs.ScanResponseFrame(
      respFlags: kvMsgs.ScanRespFlagHasMore,
      pairs: @[
        kvMsgs.ScanPair(key: "key1", value: "val1", timestamp: 100, version: 1),
        kvMsgs.ScanPair(key: "key2", value: "val2", timestamp: 100, version: 2)
      ],
      reqFlags: 0
    )
    check (frame.respFlags and kvMsgs.ScanRespFlagHasMore) != 0
    check frame.pairs.len == 2

  test "ScanResponseFrame with endOfScan flag":
    let frame = kvMsgs.ScanResponseFrame(
      respFlags: kvMsgs.ScanRespFlagEndOfScan,
      pairs: @[],
      reqFlags: 0
    )
    check (frame.respFlags and kvMsgs.ScanRespFlagEndOfScan) != 0
    check frame.pairs.len == 0

  test "ScanResponseFrame encode/decode basic":
    let frame = kvMsgs.ScanResponseFrame(
      respFlags: kvMsgs.ScanRespFlagHasMore,
      pairs: @[
        kvMsgs.ScanPair(key: "key_a", value: "value_a", timestamp: 0,
            version: 0),
        kvMsgs.ScanPair(key: "key_b", value: "value_b", timestamp: 0, version: 0)
      ],
      reqFlags: 0
    )
    let encoded = kvMsgs.encodeScanResponseFrame(frame)
    let decodedR = kvMsgs.decodeScanResponseFrame(encoded, 0)
    check decodedR.isOk
    let decoded = decodedR.value
    check decoded.respFlags == frame.respFlags
    check decoded.pairs.len == 2
    check decoded.pairs[0].key == "key_a"
    check decoded.pairs[0].value == "value_a"

  test "ScanResponseFrame encode/decode with timestamp":
    let frame = kvMsgs.ScanResponseFrame(
      respFlags: 0,
      pairs: @[
        kvMsgs.ScanPair(key: "key1", value: "val1", timestamp: 12345, version: 0)
      ],
      reqFlags: kvMsgs.ScanFlagIncludeTimestamp
    )
    let encoded = kvMsgs.encodeScanResponseFrame(frame)
    let decodedR = kvMsgs.decodeScanResponseFrame(encoded,
        kvMsgs.ScanFlagIncludeTimestamp)
    check decodedR.isOk
    check decodedR.value.pairs[0].timestamp == 12345

  test "ScanResponseFrame encode/decode with version":
    let frame = kvMsgs.ScanResponseFrame(
      respFlags: 0,
      pairs: @[
        kvMsgs.ScanPair(key: "key1", value: "val1", timestamp: 0, version: 999)
      ],
      reqFlags: kvMsgs.ScanFlagIncludeVersion
    )
    let encoded = kvMsgs.encodeScanResponseFrame(frame)
    let decodedR = kvMsgs.decodeScanResponseFrame(encoded,
        kvMsgs.ScanFlagIncludeVersion)
    check decodedR.isOk
    check decodedR.value.pairs[0].version == 999

  test "ScanResponseFrame encode/decode keys only":
    let frame = kvMsgs.ScanResponseFrame(
      respFlags: 0,
      pairs: @[
        kvMsgs.ScanPair(key: "key1", value: "", timestamp: 0, version: 0),
        kvMsgs.ScanPair(key: "key2", value: "", timestamp: 0, version: 0)
      ],
      reqFlags: kvMsgs.ScanFlagKeysOnly
    )
    let encoded = kvMsgs.encodeScanResponseFrame(frame)
    let decodedR = kvMsgs.decodeScanResponseFrame(encoded,
        kvMsgs.ScanFlagKeysOnly)
    check decodedR.isOk
    # Values should be empty for keysOnly
    check decodedR.value.pairs[0].value == ""
    check decodedR.value.pairs[1].value == ""

suite "StreamingScanResult":
  test "StreamingScanResult default":
    let result = kvMsgs.StreamingScanResult(
      streamId: 1,
      hasMore: false,
      exhausted: false,
      error: none(ProtocolError),
      totalReceived: 0
    )
    check result.streamId == 1
    check result.hasMore == false
    check result.exhausted == false
    check result.error.isNone

  test "StreamingScanResult with error":
    let err = newProtocolError(peTimeout, "Timeout during scan")
    let result = kvMsgs.StreamingScanResult(
      streamId: 5,
      hasMore: false,
      exhausted: true,
      error: some(err),
      totalReceived: 50
    )
    check result.error.isSome
    check result.error.get.kind == peTimeout
    check result.totalReceived == 50

suite "ScanPair":
  test "ScanPair basic":
    let pair = kvMsgs.ScanPair(
      key: "test_key",
      value: "test_value",
      timestamp: 1000,
      version: 5
    )
    check pair.key == "test_key"
    check pair.value == "test_value"
    check pair.timestamp == 1000
    check pair.version == 5

  test "ScanPair empty value":
    let pair = kvMsgs.ScanPair(
      key: "key_only",
      value: "",
      timestamp: 0,
      version: 0
    )
    check pair.key == "key_only"
    check pair.value == ""

# ---------------------------------------------------------------------------
# Regression: closeStream must clean up streams properly.
# New behavior: closeStream drains remaining frames instead of disconnecting,
# preserving the TCP connection for reuse. Disconnect only happens on error.
# ---------------------------------------------------------------------------

suite "closeStream handles stream cleanup":

  test "closeStream on exhausted stream preserves connection":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    client.connected.store(true) # simulate a live connection
    let ss = newStreamingScanClient(client)
    ss.exhausted = true
    ss.closeStream()
    check ss.exhausted == true
    check client.connected.load() == true # connection preserved

  test "closeStream on stream with no more data preserves connection":
    # When hasMore=false, there's nothing to drain and no reason to disconnect
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    client.connected.store(true)
    let ss = newStreamingScanClient(client)
    ss.hasMore = false
    ss.closeStream()
    check ss.exhausted == true
    check client.connected.load() == true # connection preserved

  test "closeStream on nil client is safe":
    let ss = newStreamingScanClient(nil)
    ss.closeStream()
    check ss.exhausted == true

  test "closeStream on disconnected client is safe":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    client.connected.store(false) # already disconnected
    let ss = newStreamingScanClient(client)
    ss.hasMore = true # has more data, but client is disconnected
    ss.closeStream()
    check ss.exhausted == true
    check client.connected.load() == false # still disconnected
