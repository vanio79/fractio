# Tests for streaming resultset functionality
# Covers MockStreamResultSet, StreamConfig, and streaming patterns

import unittest
import std/[options, deques, strutils]
import fractio/storage/backend
import fractio/core/kv_interface # for consumeStream
import fractio/di/mocks

suite "StreamResultSet Types":
  test "StreamConfig defaults":
    let config = defaultStreamConfig()
    check config.bufferSize == DEFAULT_STREAM_BUFFER_SIZE
    check config.prefetchThreshold == DEFAULT_PREFETCH_THRESHOLD

  test "StreamConfig small":
    let config = smallStreamConfig()
    check config.bufferSize == 100
    check config.prefetchThreshold == 20

  test "StreamConfig large":
    let config = largeStreamConfig()
    check config.bufferSize == 5000
    check config.prefetchThreshold == 500

  test "StreamState enum values":
    check ssIdle == StreamState.ssIdle
    check ssReading == StreamState.ssReading
    check ssExhausted == StreamState.ssExhausted
    check ssError == StreamState.ssError
    check ssClosed == StreamState.ssClosed

suite "MockStreamResultSet":
  test "newMockStreamResultSet creates empty stream":
    let rs = newMockStreamResultSet(@[])
    check rs.getState() == ssReading
    check rs.hasNext() == false
    check rs.getTotalRead() == 0

  test "newMockStreamResultSet with data":
    let data = @[
      ("key1", "value1"),
      ("key2", "value2"),
      ("key3", "value3")
    ]
    let rs = newMockStreamResultSet(data)
    check rs.getState() == ssReading
    check rs.hasNext() == true
    check rs.getTotalRead() == 0

  test "next returns items in order":
    let data = @[
      ("key1", "value1"),
      ("key2", "value2"),
      ("key3", "value3")
    ]
    let rs = newMockStreamResultSet(data)

    let first = rs.next()
    check first.isSome
    check first.get.key == "key1"
    check first.get.value == "value1"
    check rs.getTotalRead() == 1

    let second = rs.next()
    check second.isSome
    check second.get.key == "key2"
    check rs.getTotalRead() == 2

    let third = rs.next()
    check third.isSome
    check third.get.key == "key3"
    check rs.getTotalRead() == 3

  test "next returns none when exhausted":
    let data = @[("key1", "value1")]
    let rs = newMockStreamResultSet(data)

    discard rs.next() # consume one
    check rs.hasNext() == false

    let empty = rs.next()
    check empty.isNone
    check rs.getState() == ssExhausted

  test "close stops iteration":
    let data = @[
      ("key1", "value1"),
      ("key2", "value2")
    ]
    let rs = newMockStreamResultSet(data)

    discard rs.next()
    rs.close()

    check rs.getState() == ssClosed
    check rs.hasNext() == false
    let empty = rs.next()
    check empty.isNone

  test "hasNext reflects buffer state":
    let data = @[
      ("key1", "value1"),
      ("key2", "value2")
    ]
    let rs = newMockStreamResultSet(data)

    check rs.hasNext() == true
    discard rs.next()
    check rs.hasNext() == true
    discard rs.next()
    check rs.hasNext() == false

  test "getError returns none for normal streams":
    let data = @[("key1", "value1")]
    let rs = newMockStreamResultSet(data)
    check rs.getError().isNone

  test "consumeStream collects all results":
    let data = @[
      ("key1", "value1"),
      ("key2", "value2"),
      ("key3", "value3")
    ]
    let rs = newMockStreamResultSet(data)

    let allData = consumeStream(rs)
    check allData.len == 3
    check allData[0].key == "key1"
    check allData[1].key == "key2"
    check allData[2].key == "key3"

    rs.close()

suite "MockKVStore Streaming":
  test "mockStreamScan creates stream from MockKVStore":
    let store = newMockKVStore()
    discard store.put("prefix_a", "value_a")
    discard store.put("prefix_b", "value_b")
    discard store.put("prefix_c", "value_c")
    discard store.put("other_x", "value_x")

    let rs = store.mockStreamScan("prefix_", 10)
    check rs.hasNext() == true

    var count = 0
    while rs.hasNext():
      let kv = rs.next()
      if kv.isSome:
        check kv.get.key.startsWith("prefix_")
        inc count

    check count == 3
    rs.close()

  test "mockStreamScan respects limit":
    let store = newMockKVStore()
    discard store.put("key_1", "v1")
    discard store.put("key_2", "v2")
    discard store.put("key_3", "v3")
    discard store.put("key_4", "v4")

    let rs = store.mockStreamScan("key_", 2)

    let first = rs.next()
    check first.isSome

    let second = rs.next()
    check second.isSome

    # After 2 items, stream should stop (mock immediately stops)
    check rs.getTotalRead() == 2
    rs.close()

suite "MockBackend Streaming":
  test "streamScan creates stream from MockBackend":
    let backend = newMockBackend()
    discard backend.put("prefix_a", "value_a")
    discard backend.put("prefix_b", "value_b")
    discard backend.put("other_x", "value_x")

    let rs = backend.streamScan("prefix_", 10)
    check rs.hasNext() == true

    var count = 0
    while rs.hasNext():
      let kv = rs.next()
      if kv.isSome:
        check kv.get.key.startsWith("prefix_")
        inc count

    check count == 2
    rs.close()
