# Unit tests for LogEntry binary serialization

import unittest
import std/strutils
import fractio/distributed/raft/types
import fractio/utils/binary

suite "LogEntry Binary Serialization":
  test "encode empty LogEntry":
    let entry = LogEntry(
      term: 1'i64,
      entryType: LET_NORMAL,
      data: ""
    )
    let encoded = encodeLogEntry(entry)
    # Minimum size: 2 (magic) + 1 (version) + 8 (term) + 1 (type) + 4 (data length) = 16 bytes
    check encoded.len == 16
    check encoded[0] == 'R'
    check encoded[1] == 'E'
    check encoded[2] == char(LOG_ENTRY_VERSION)

  test "encode LogEntry with data":
    let entry = LogEntry(
      term: 42'i64,
      entryType: LET_CONFIG_CHANGE,
      data: "test data payload"
    )
    let encoded = encodeLogEntry(entry)
    check encoded.len > 16
    check encoded[0] == 'R'
    check encoded[1] == 'E'

  test "decode LogEntry roundtrip":
    let entry = LogEntry(
      term: 999'i64,
      entryType: LET_NO_OP,
      data: "hello world"
    )
    let encoded = encodeLogEntry(entry)
    let decoded = decodeLogEntry(encoded)
    check decoded.term == entry.term
    check decoded.entryType == entry.entryType
    check decoded.data == entry.data

  test "decode LogEntry with special characters in data":
    let entry = LogEntry(
      term: 1'i64,
      entryType: LET_NORMAL,
      data: "data with \"quotes\" and \\backslash\\"
    )
    let encoded = encodeLogEntry(entry)
    let decoded = decodeLogEntry(encoded)
    check decoded.data == entry.data
    check decoded.term == entry.term

  test "decode all entry types":
    for et in [LET_NORMAL, LET_CONFIG_CHANGE, LET_NO_OP]:
      let entry = LogEntry(term: 1'i64, entryType: et, data: "test")
      let encoded = encodeLogEntry(entry)
      let decoded = decodeLogEntry(encoded)
      check decoded.entryType == et

  test "decode rejects invalid magic":
    let badData = "XX\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    expect ValueError:
      discard decodeLogEntry(badData)

  test "decode rejects unsupported version":
    # Valid magic but version 99
    var w = initBinaryWriter()
    w.writeBytes(LOG_ENTRY_MAGIC)
    w.writeU8(99'u8) # Invalid version
    w.writeI64(1'i64)
    w.writeU8(0'u8)
    w.writeString("")
    let encoded = w.finish()
    expect ValueError:
      discard decodeLogEntry(encoded)

  test "decode rejects truncated data":
    let entry = LogEntry(term: 1'i64, entryType: LET_NORMAL, data: "test")
    let encoded = encodeLogEntry(entry)
    # Truncate to just magic bytes
    let truncated = encoded[0..1]
    expect ValueError:
      discard decodeLogEntry(truncated)

  test "large data payload":
    let largeData = "x".repeat(10000)
    let entry = LogEntry(term: 1'i64, entryType: LET_NORMAL, data: largeData)
    let encoded = encodeLogEntry(entry)
    let decoded = decodeLogEntry(encoded)
    check decoded.data.len == 10000
    check decoded.data == largeData
