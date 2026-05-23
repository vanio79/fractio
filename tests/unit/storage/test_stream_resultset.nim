# Tests for streaming resultset functionality
# Covers MockStreamResultSet, StreamConfig, and streaming patterns

import unittest
import std/[options, deques, strutils]
import fractio/storage/backend
import fractio/core/kv_interface # for consumeStream, KVStreamResult
import fractio/core/mock_kv # for MockKVStore (KVStore subclass with streamScan)
import fractio/protocol/mvcc_store # for ScanChunk
import fractio/di/mocks # for MockStreamResultSet, MockBackend, newMockKVStore (mocks version)

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
    let store = mocks.newMockKVStore()
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
    let store = mocks.newMockKVStore()
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

suite "MockKVStore streamScan (KVStore interface)":
  test "streamScan returns all keys in range":
    let store = mock_kv.newMockKVStore()
    discard store.put("key_a", "val_a")
    discard store.put("key_b", "val_b")
    discard store.put("key_c", "val_c")
    discard store.put("other_x", "val_x")

    # Use endKey to limit the range (empty endKey = no upper bound)
    let res = store.streamScan("key_a", "key_z", 0)
    check res.isOk
    var count = 0
    while res.stream.hasNext():
      let kv = res.stream.next()
      if kv.isSome:
        inc count
    check count == 3
    res.stream.close()

  test "streamScan respects limit":
    let store = mock_kv.newMockKVStore()
    discard store.put("k1", "v1")
    discard store.put("k2", "v2")
    discard store.put("k3", "v3")
    discard store.put("k4", "v4")

    let res = store.streamScan("", "", 2)
    check res.isOk
    var count = 0
    while res.stream.hasNext():
      let kv = res.stream.next()
      if kv.isSome:
        inc count
    check count == 2
    res.stream.close()

  test "streamScan with empty store returns no results":
    let store = mock_kv.newMockKVStore()
    let res = store.streamScan("", "", 0)
    check res.isOk
    check res.stream.hasNext() == false
    res.stream.close()

  test "streamScan returns sorted keys":
    let store = mock_kv.newMockKVStore()
    discard store.put("z_key", "val_z")
    discard store.put("a_key", "val_a")
    discard store.put("m_key", "val_m")

    let res = store.streamScan("", "", 0)
    check res.isOk
    var keys: seq[string] = @[]
    while res.stream.hasNext():
      let kv = res.stream.next()
      if kv.isSome:
        keys.add(kv.get.key)
    check keys == @["a_key", "m_key", "z_key"]
    res.stream.close()

suite "ScanChunk type":
  test "ScanChunk holds pairs and hasMore flag":
    let chunk = ScanChunk(
      pairs: @[(key: "k1", value: "v1"), (key: "k2", value: "v2")],
      hasMore: true
    )
    check chunk.pairs.len == 2
    check chunk.hasMore == true
    check chunk.pairs[0].key == "k1"
    check chunk.pairs[1].key == "k2"

  test "ScanChunk final chunk has hasMore=false":
    let chunk = ScanChunk(
      pairs: @[(key: "k3", value: "v3")],
      hasMore: false
    )
    check chunk.pairs.len == 1
    check chunk.hasMore == false

  test "ScanChunk empty chunk signals end":
    let chunk = ScanChunk(pairs: @[], hasMore: false)
    check chunk.pairs.len == 0
    check chunk.hasMore == false
