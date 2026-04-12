# Unit tests for fractio/protocol/raft_txn.nim
# Tests coordinator record encoding/decoding and constants

import std/[unittest, strutils]
import fractio/protocol/raft_txn

suite "Coordinator State Constants":

  test "CoordStatePrepared":
    check CoordStatePrepared == "PREPARED"

  test "CoordStateCommitting":
    check CoordStateCommitting == "COMMITTING"

  test "CoordStateAborting":
    check CoordStateAborting == "ABORTING"

suite "encodeCoordRecord":

  test "encode PREPARED record":
    let keys = @["key1", "key2"]
    let data = encodeCoordRecord(123'u64, CoordStatePrepared, 1000'u64, keys)
    check data == "PREPARED:123:1000:key1,key2"

  test "encode COMMITTING record":
    let keys = @["keyA"]
    let data = encodeCoordRecord(456'u64, CoordStateCommitting, 5000'u64, keys)
    check data == "COMMITTING:456:5000:keyA"

  test "encode ABORTING record":
    let keys = @["keyX", "keyY", "keyZ"]
    let data = encodeCoordRecord(789'u64, CoordStateAborting, 0'u64, keys)
    check data == "ABORTING:789:0:keyX,keyY,keyZ"

  test "encode with empty keys":
    let keys: seq[string] = @[]
    let data = encodeCoordRecord(1'u64, CoordStatePrepared, 100'u64, keys)
    check data == "PREPARED:1:100:"

  test "encode with single key":
    let keys = @["single-key"]
    let data = encodeCoordRecord(99'u64, CoordStateCommitting, 999'u64, keys)
    check data == "COMMITTING:99:999:single-key"

  test "encode with large txnId":
    let keys = @["k"]
    let data = encodeCoordRecord(uint64.high, CoordStatePrepared, 1'u64, keys)
    check "PREPARED:" in data
    check $uint64.high in data

  test "encode with large commitTs":
    let keys = @["k"]
    let data = encodeCoordRecord(1'u64, CoordStateCommitting, uint64.high, keys)
    check "COMMITTING:" in data
    check $uint64.high in data

  test "encode with special characters in keys":
    # Keys with dashes work fine, colons would interfere with format
    let keys = @["key-with-dashes", "another-key"]
    let data = encodeCoordRecord(1'u64, CoordStatePrepared, 100'u64, keys)
    check "key-with-dashes" in data
    check "another-key" in data

suite "decodeCoordRecord":

  test "decode PREPARED record":
    let data = "PREPARED:123:1000:key1,key2"
    let (state, txnId, commitTs, keys) = decodeCoordRecord(data)
    check state == "PREPARED"
    check txnId == 123'u64
    check commitTs == 1000'u64
    check keys == @["key1", "key2"]

  test "decode COMMITTING record":
    let data = "COMMITTING:456:5000:keyA"
    let (state, txnId, commitTs, keys) = decodeCoordRecord(data)
    check state == "COMMITTING"
    check txnId == 456'u64
    check commitTs == 5000'u64
    check keys == @["keyA"]

  test "decode ABORTING record":
    let data = "ABORTING:789:0:keyX,keyY,keyZ"
    let (state, txnId, commitTs, keys) = decodeCoordRecord(data)
    check state == "ABORTING"
    check txnId == 789'u64
    check commitTs == 0'u64
    check keys == @["keyX", "keyY", "keyZ"]

  test "decode with empty keys":
    let data = "PREPARED:1:100:"
    let (state, txnId, commitTs, keys) = decodeCoordRecord(data)
    check state == "PREPARED"
    check txnId == 1'u64
    check commitTs == 100'u64
    check keys.len == 0

  test "decode with single key":
    let data = "COMMITTING:99:999:single-key"
    let (state, txnId, commitTs, keys) = decodeCoordRecord(data)
    check state == "COMMITTING"
    check keys == @["single-key"]

  test "decode invalid - too few parts":
    let data = "PREPARED:123"
    let (state, txnId, commitTs, keys) = decodeCoordRecord(data)
    check state == ""
    check txnId == 0'u64
    check commitTs == 0'u64
    check keys.len == 0

  test "decode invalid - empty string":
    let data = ""
    let (state, txnId, commitTs, keys) = decodeCoordRecord(data)
    check state == ""
    check txnId == 0'u64
    check commitTs == 0'u64
    check keys.len == 0

  test "decode invalid - malformed number":
    let data = "PREPARED:abc:1000:key1"
    let (state, txnId, commitTs, keys) = decodeCoordRecord(data)
    # Should return empty/zero values on parse error
    check state == ""
    check txnId == 0'u64
    check commitTs == 0'u64
    check keys.len == 0

  test "decode with special characters in keys":
    # Note: keys with colons in the encoding format will be split by the
    # colons in the format, so "key:with:colons" becomes "key" only
    # The format uses ':' as separator: STATE:txnId:commitTs:keys
    let data = "PREPARED:1:100:key-with-dashes"
    let (state, txnId, commitTs, keys) = decodeCoordRecord(data)
    check state == "PREPARED"
    check txnId == 1'u64
    check commitTs == 100'u64
    check keys == @["key-with-dashes"]

suite "encode/decode Roundtrip":

  test "roundtrip PREPARED with multiple keys":
    let originalKeys = @["key1", "key2", "key3"]
    let encoded = encodeCoordRecord(100'u64, CoordStatePrepared, 5000'u64, originalKeys)
    let (state, txnId, commitTs, keys) = decodeCoordRecord(encoded)
    check state == CoordStatePrepared
    check txnId == 100'u64
    check commitTs == 5000'u64
    check keys == originalKeys

  test "roundtrip COMMITTING":
    let originalKeys = @["abc", "def"]
    let encoded = encodeCoordRecord(42'u64, CoordStateCommitting, 99999'u64, originalKeys)
    let (state, txnId, commitTs, keys) = decodeCoordRecord(encoded)
    check state == CoordStateCommitting
    check txnId == 42'u64
    check commitTs == 99999'u64
    check keys == originalKeys

  test "roundtrip ABORTING":
    let originalKeys = @["x", "y", "z"]
    let encoded = encodeCoordRecord(0'u64, CoordStateAborting, 0'u64, originalKeys)
    let (state, txnId, commitTs, keys) = decodeCoordRecord(encoded)
    check state == CoordStateAborting
    check txnId == 0'u64
    check commitTs == 0'u64
    check keys == originalKeys

  test "roundtrip with empty keys":
    let originalKeys: seq[string] = @[]
    let encoded = encodeCoordRecord(1'u64, CoordStatePrepared, 1'u64, originalKeys)
    let (state, txnId, commitTs, keys) = decodeCoordRecord(encoded)
    check keys.len == 0

  test "roundtrip with very long keys":
    let originalKeys = @[repeat("a", 1000), repeat("b", 500)]
    let encoded = encodeCoordRecord(12345'u64, CoordStateCommitting, 54321'u64, originalKeys)
    let (state, txnId, commitTs, keys) = decodeCoordRecord(encoded)
    check keys[0].len == 1000
    check keys[1].len == 500

suite "Edge Cases":

  test "keys with commas are split":
    let data = "PREPARED:1:100:a,b,c"
    let (state, txnId, commitTs, keys) = decodeCoordRecord(data)
    check keys.len == 3
    check keys == @["a", "b", "c"]

  test "txnId zero is valid":
    let encoded = encodeCoordRecord(0'u64, CoordStatePrepared, 100'u64, @["key"])
    let (state, txnId, commitTs, keys) = decodeCoordRecord(encoded)
    check txnId == 0'u64

  test "commitTs zero is valid":
    let encoded = encodeCoordRecord(100'u64, CoordStateAborting, 0'u64, @["key"])
    let (state, txnId, commitTs, keys) = decodeCoordRecord(encoded)
    check commitTs == 0'u64

  test "large number of keys":
    var originalKeys: seq[string] = @[]
    for i in 1..100:
      originalKeys.add("key" & $i)
    let encoded = encodeCoordRecord(1'u64, CoordStatePrepared, 100'u64, originalKeys)
    let (state, txnId, commitTs, keys) = decodeCoordRecord(encoded)
    check keys.len == 100
