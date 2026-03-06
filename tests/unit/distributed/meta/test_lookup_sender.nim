# Unit tests for sender and lookup modules
#
# Tests for:
# - Range lookup protocol
# - DistSender request routing
# - Retry logic
# - Request splitting

import std/unittest
import std/options
import std/strutils

import fractio/distributed/range/types
import fractio/distributed/meta/types
import fractio/distributed/meta/lookup
import fractio/distributed/sender

suite "Range Lookup":
  test "create range lookup":
    let cache = newRangeCache()
    let lookup = newRangeLookup(cache)
    lookup.destroy()
    cache.destroy()

  test "set meta1 descriptor":
    let cache = newRangeCache()
    let lookup = newRangeLookup(cache)

    let meta1 = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )

    lookup.setMeta1Descriptor(meta1)
    lookup.destroy()
    cache.destroy()

  test "set meta2 descriptor":
    let cache = newRangeCache()
    let lookup = newRangeLookup(cache)

    let meta2 = newRangeDescriptor(
      RangeID(2),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )

    lookup.setMeta2Descriptor(RangeID(2), meta2)
    lookup.destroy()
    cache.destroy()

  test "find containing range from cache":
    let cache = newRangeCache(ttlNs = 60000'i64)
    let lookup = newRangeLookup(cache)

    let desc = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )

    cache.put(desc, 1000)

    let found = lookup.findContainingRange(@[byte(50)], 5000)
    check found.isSome
    check found.get.rangeId == RangeID(1)

    lookup.destroy()
    cache.destroy()

  test "get leaseholder":
    let cache = newRangeCache(ttlNs = 60000'i64)
    let lookup = newRangeLookup(cache)

    let desc = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[
        newReplicaDescriptor(NodeID(1), ReplicaID(1)),
        newReplicaDescriptor(NodeID(2), ReplicaID(2)),
        newReplicaDescriptor(NodeID(3), ReplicaID(3))
      ]
    )

    cache.put(desc, 1000)

    let leaseholder = lookup.getLeaseholder(RangeID(1), 5000)
    check leaseholder.isSome
    check leaseholder.get == NodeID(1) # First voter

    lookup.destroy()
    cache.destroy()

  test "update and invalidate descriptor":
    let cache = newRangeCache(ttlNs = 60000'i64)
    let lookup = newRangeLookup(cache)

    let desc1 = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )

    lookup.updateDescriptor(desc1, 1000)

    check lookup.getLeaseholder(RangeID(1), 5000).isSome

    lookup.invalidateRange(RangeID(1))

    check lookup.getLeaseholder(RangeID(1), 5000).isNone

    lookup.destroy()
    cache.destroy()

suite "Key Range Utilities":
  test "key in range":
    check keyInRange(@[byte(50)], @[byte(0)], @[byte(100)])
    check not keyInRange(@[byte(150)], @[byte(0)], @[byte(100)])
    check keyInRange(@[byte(0)], @[byte(0)], @[byte(100)]) # Start is inclusive
    check not keyInRange(@[byte(100)], @[byte(0)], @[byte(100)]) # End is exclusive

  test "key in unbounded range":
    check keyInRange(@[byte(50)], @[byte(0)], @[]) # Unbounded end

  test "ranges overlap":
    check rangesOverlap(@[byte(0)], @[byte(100)], @[byte(50)], @[byte(150)])
    check not rangesOverlap(@[byte(0)], @[byte(50)], @[byte(50)], @[byte(
        100)]) # Adjacent, not overlapping
    check rangesOverlap(@[byte(0)], @[], @[byte(50)], @[byte(100)]) # Unbounded overlaps

  test "compare ranges":
    check compareRanges(@[byte(0)], @[byte(100)], @[byte(50)], @[byte(150)]) == -1
    check compareRanges(@[byte(50)], @[byte(150)], @[byte(0)], @[byte(100)]) == 1
    check compareRanges(@[byte(0)], @[byte(100)], @[byte(0)], @[byte(100)]) == 0

  test "split key":
    let split = splitKey(@[byte(0)], @[byte(100)])
    check split.len > 0
    check split > @[byte(0)]
    check split < @[byte(100)]

  test "next key":
    let next = nextKey(@[byte(50)])
    check next > @[byte(50)]
    check next == @[byte(51)]

  test "next key with overflow":
    # When incrementing [255], we get [0, 255] (prepend 0, keep original)
    let next = nextKey(@[byte(255)])
    check next == @[byte(0), byte(255)]

  test "prev key":
    let prev = prevKey(@[byte(50)])
    check prev < @[byte(50)]
    check prev == @[byte(49)]

  test "prev key with underflow":
    # When decrementing [0, 1], we get [0, 0]
    let prev = prevKey(@[byte(0), byte(1)])
    check prev == @[byte(0), byte(0)]

suite "Request Types":
  test "create get request":
    let req = newGetRequest(@[byte(1), byte(2), byte(3)])
    check req.kind == rkGet
    check req.getKey == @[byte(1), byte(2), byte(3)]

  test "create put request":
    let req = newPutRequest(@[byte(1)], @[byte(2)])
    check req.kind == rkPut
    check req.putKey == @[byte(1)]
    check req.putValue == @[byte(2)]

  test "create delete request":
    let req = newDeleteRequest(@[byte(1)])
    check req.kind == rkDelete
    check req.deleteKey == @[byte(1)]

  test "create scan request":
    let req = newScanRequest(@[byte(0)], @[byte(100)], 10)
    check req.kind == rkScan
    check req.scanStart == @[byte(0)]
    check req.scanEnd == @[byte(100)]
    check req.scanLimit == 10

  test "create batch request":
    let reqs = @[
      newGetRequest(@[byte(1)]),
      newPutRequest(@[byte(2)], @[byte(3)])
    ]
    let batch = newBatchRequest(reqs, 1000'i64, 1)
    check batch.requests.len == 2
    check batch.timestampNs == 1000
    check batch.priority == 1

suite "Response Types":
  test "create get response":
    let resp = newGetResponse(some(@[byte(1)]))
    check resp.kind == rkGet
    check resp.getValue.isSome
    check resp.getValue.get == @[byte(1)]

  test "create get response not found":
    let resp = newGetResponse(none(seq[byte]))
    check resp.kind == rkGet
    check not resp.getValue.isSome

  test "create put response":
    let resp = newPutResponse(true)
    check resp.kind == rkPut
    check resp.putSuccess

  test "create delete response":
    let resp = newDeleteResponse(true)
    check resp.kind == rkDelete
    check resp.deleteSuccess

  test "create scan response":
    let keys = @[@[byte(1)], @[byte(2)]]
    let values = @[some(@[byte(10)]), none(seq[byte])]
    let resp = newScanResponse(keys, values)
    check resp.kind == rkScan
    check resp.scanKeys.len == 2
    check resp.scanValues.len == 2

suite "DistSender":
  test "create dist sender":
    let cache = newRangeCache()
    let lookup = newRangeLookup(cache)

    proc mockSend(req: RangeRequest): RangeResponse =
      result = RangeResponse(rangeId: req.rangeId, responses: @[])

    let sender = newDistSender(lookup, mockSend)
    sender.destroy()
    lookup.destroy()
    cache.destroy()

  test "calculate backoff":
    let cache = newRangeCache()
    let lookup = newRangeLookup(cache)

    proc mockSend(req: RangeRequest): RangeResponse =
      result = RangeResponse(rangeId: req.rangeId, responses: @[])

    let sender = newDistSender(lookup, mockSend)

    # First attempt: base delay
    check sender.calculateBackoff(0) == DEFAULT_RETRY_BASE_NS

    # Second attempt: 2x base
    check sender.calculateBackoff(1) == DEFAULT_RETRY_BASE_NS * 2

    # Third attempt: 4x base
    check sender.calculateBackoff(2) == DEFAULT_RETRY_BASE_NS * 4

    sender.destroy()
    lookup.destroy()
    cache.destroy()

  test "should retry on not leader":
    let cache = newRangeCache()
    let lookup = newRangeLookup(cache)

    proc mockSend(req: RangeRequest): RangeResponse =
      result = RangeResponse(rangeId: req.rangeId, responses: @[])

    let sender = newDistSender(lookup, mockSend)

    let err = newNotLeaderError(RangeID(1), NodeID(2))
    check sender.shouldRetry(err, 0)
    check sender.shouldRetry(err, 4)
    check not sender.shouldRetry(err, 5) # Max retries

    sender.destroy()
    lookup.destroy()
    cache.destroy()

  test "should retry on range unavailable":
    let cache = newRangeCache()
    let lookup = newRangeLookup(cache)

    proc mockSend(req: RangeRequest): RangeResponse =
      result = RangeResponse(rangeId: req.rangeId, responses: @[])

    let sender = newDistSender(lookup, mockSend)

    let err = newRangeUnavailableError(RangeID(1))
    check sender.shouldRetry(err, 0)
    check not sender.shouldRetry(err, 3) # Fewer retries for unavailable

    sender.destroy()
    lookup.destroy()
    cache.destroy()

  test "send with mock callback":
    let cache = newRangeCache(ttlNs = 60000'i64)
    let lookup = newRangeLookup(cache)

    # Set up a range
    let desc = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    cache.put(desc, 1000)

    var sendCalled = false
    proc mockSend(req: RangeRequest): RangeResponse =
      sendCalled = true
      result = RangeResponse(
        rangeId: req.rangeId,
        responses: @[newGetResponse(some(@[byte(42)]))]
      )

    let sender = newDistSender(lookup, mockSend)

    let batch = newBatchRequest(@[newGetRequest(@[byte(50)])], 1000'i64)
    let resp = sender.send(batch, 5000)

    check sendCalled
    check resp.responses.len == 1

    sender.destroy()
    lookup.destroy()
    cache.destroy()

  test "sender statistics":
    let cache = newRangeCache(ttlNs = 60000'i64)
    let lookup = newRangeLookup(cache)

    # Set up a range
    let desc = newRangeDescriptor(
      RangeID(1),
      @[byte(0)],
      @[byte(100)],
      @[newReplicaDescriptor(NodeID(1), ReplicaID(1))]
    )
    cache.put(desc, 1000)

    proc mockSend(req: RangeRequest): RangeResponse =
      result = RangeResponse(
        rangeId: req.rangeId,
        responses: @[newGetResponse(some(@[byte(42)]))]
      )

    let sender = newDistSender(lookup, mockSend)

    # Send a few requests
    for i in 0..<5:
      let batch = newBatchRequest(@[newGetRequest(@[byte(50)])], 1000'i64)
      discard sender.send(batch, 5000)

    let stats = sender.getStats()
    check stats.sendsAttempted == 5
    check stats.sendsSucceeded == 5
    check stats.successRate == 1.0

    sender.destroy()
    lookup.destroy()
    cache.destroy()

suite "Error Types":
  test "not leader error":
    let err = newNotLeaderError(RangeID(1), NodeID(2))
    check err.rangeId == RangeID(1)
    check err.leaderHint == NodeID(2)
    check "Not leader" in err.msg

  test "range unavailable error":
    let err = newRangeUnavailableError(RangeID(1))
    check err.rangeId == RangeID(1)
    check "unavailable" in err.msg

  test "send timeout error":
    let err = newSendTimeoutError(RangeID(1))
    check err.rangeId == RangeID(1)
    check "timed out" in err.msg

suite "Constants":
  test "default retry settings":
    check DEFAULT_MAX_RETRIES == 5
    check DEFAULT_RETRY_BASE_NS == 100_000_000
    check DEFAULT_RETRY_MAX_NS == 10_000_000_000
    check DEFAULT_SEND_TIMEOUT_NS == 30_000_000_000
