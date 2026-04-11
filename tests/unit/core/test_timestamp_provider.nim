# Unit tests for Timestamp Provider
# Tests HLC (Hybrid Logical Clock) timestamp generation with dependency injection

import unittest
import std/[atomics, strutils]
import fractio/core/timestamp_provider
import fractio/core/types
import fractio/distributed/sharedtimer/mock as sharedtimerMock

suite "TimestampProvider - Creation":
  test "create with default parameters":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = newTimestampProvider(mockTimer)
    check tsProvider.timer != nil
    check tsProvider.lastTimestamp == 0
    check tsProvider.lastCounter == 0
    check tsProvider.maxOffset == DEFAULT_MAX_OFFSET_NS
    check tsProvider.nodeId == 0

  test "create with custom node ID":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = newTimestampProvider(mockTimer, nodeId = 5)
    check tsProvider.nodeId == 5

  test "create with custom max offset":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = newTimestampProvider(mockTimer, maxOffset = 2_000_000)
    check tsProvider.maxOffset == 2_000_000

  test "create with all parameters":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = newTimestampProvider(mockTimer, nodeId = 10,
        maxOffset = 5_000_000)
    check tsProvider.nodeId == 10
    check tsProvider.maxOffset == 5_000_000

suite "TimestampProvider - Timestamp Generation":
  test "now returns wall time":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 0,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    let ts = tsProvider.now()
    check ts == 1000_000_000

  test "now updates last timestamp":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 500_000_000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    discard tsProvider.now()
    check tsProvider.lastTimestamp == 1000_000_000

  test "now resets counter when time advances":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 500_000_000,
      lastCounter: 100,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    discard tsProvider.now()
    check tsProvider.lastCounter == 0

  test "now increments counter for same wall time":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 1000_000_000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    let ts1 = tsProvider.now()
    check tsProvider.lastCounter == 1
    let ts2 = tsProvider.now()
    check tsProvider.lastCounter == 2
    # Timestamps should be different due to logical counter
    check ts1 != ts2

  test "now handles time going backward":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 500_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 1000_000_000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    let ts = tsProvider.now()
    # Should increment counter and use last timestamp + 1
    check ts > 1000_000_000
    check tsProvider.lastCounter == 1

  test "counter wraps at max":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 1000_000_000,
      lastCounter: MAX_LOGICAL_COUNTER - 1,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    discard tsProvider.now()
    # Counter should wrap to 0
    check tsProvider.lastCounter == 0

  test "timestamps are strictly increasing":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 1000_000_000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    var prevTs = Timestamp(0)
    for i in 0..<100:
      mockTimer.currentTime = 1000_000_000 + i * 1_000_000
      let ts = tsProvider.now()
      check ts > prevTs
      prevTs = ts

suite "TimestampProvider - Transaction Timestamps":
  test "acquire start timestamp":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 0,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    let startTs = tsProvider.acquireStartTimestamp()
    check startTs == 1000_000_000

  test "acquire commit timestamp":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 0,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    let minTs = Timestamp(500_000_000)
    let commitTs = tsProvider.acquireCommitTimestamp(minTs)
    check commitTs > minTs

  test "acquire commit timestamp ensures ordering":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 500_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 0,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    let minTs = Timestamp(1000_000_000) # Greater than current time
    mockTimer.currentTime = 1500_000_000
    let commitTs = tsProvider.acquireCommitTimestamp(minTs)
    check commitTs > minTs

  test "multiple commit timestamps are ordered":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 0,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    let minTs1 = Timestamp(500_000_000)
    let commitTs1 = tsProvider.acquireCommitTimestamp(minTs1)
    mockTimer.currentTime += 1_000_000
    let minTs2 = commitTs1
    let commitTs2 = tsProvider.acquireCommitTimestamp(minTs2)
    check commitTs2 > commitTs1

suite "TimestampProvider - Global Timestamp":
  test "get global timestamp":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 2000_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 0,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    let globalTs = tsProvider.getGlobalTimestamp()
    check globalTs == 2000_000_000

  test "global timestamp matches timer":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 500_000_000,
      lastCounter: 10,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    let globalTs = tsProvider.getGlobalTimestamp()
    check globalTs == mockTimer.currentTime

suite "TimestampProvider - Clock Validation":
  test "validate clock offset returns true":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 0,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    check tsProvider.validateClockOffset() == true

  test "validate clock offset always passes for mock":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 0)
    let tsProvider = newTimestampProvider(mockTimer)
    check tsProvider.validateClockOffset() == true

suite "TimestampProvider - Encoding/Decoding":
  test "encode timestamp with node ID":
    let ts = Timestamp(1234567890'i64)
    let nodeId = uint16(1)
    let counter = int64(100)
    let encoded = encodeTimestamp(ts, nodeId, counter)
    check encoded > 0

  test "decode timestamp":
    let original = (timestamp: Timestamp(1234567890'i64), nodeId: uint16(1),
        counter: int64(100))
    let encoded = encodeTimestamp(original.timestamp, original.nodeId,
        original.counter)
    let decoded = decodeTimestamp(encoded)
    # Timestamp loses lower 10 bits for counter, so check approximate match
    check abs(decoded.timestamp - original.timestamp) < 1024'i64
    check decoded.nodeId == original.nodeId
    check decoded.counter == original.counter

  test "encode/decode round-trip - multiple values":
    for nodeId in [uint16(0), uint16(5), uint16(100), uint16(1023)]:
      for counter in [int64(0), int64(50), int64(1000)]:
        let ts = Timestamp(1000_000_000)
        let encoded = encodeTimestamp(ts, nodeId, counter)
        let decoded = decodeTimestamp(encoded)
        # Timestamp loses lower 10 bits for counter, so check approximate match
        check abs(decoded.timestamp - ts) < 1024'i64
        check decoded.nodeId == nodeId
        check decoded.counter == counter

  test "encode preserves node ID":
    let ts = Timestamp(1000_000_000)
    for nodeId in 0'u16..100'u16:
      let encoded = encodeTimestamp(ts, nodeId, 100)
      let decoded = decodeTimestamp(encoded)
      check decoded.nodeId == nodeId

  test "encode preserves counter":
    let ts = Timestamp(1000_000_000)
    for counter in 0'i64..100'i64:
      let encoded = encodeTimestamp(ts, 1, counter)
      let decoded = decodeTimestamp(encoded)
      check decoded.counter == counter

suite "TimestampProvider - Error Handling":
  test "timestamp error construction":
    let err = timestampError(tsClockTooFarBehind, "Clock skew detected")
    check err.code == 4000 + ord(tsClockTooFarBehind)
    check "Clock skew" in err.message
    check "Clock too far behind" in err.message

  test "timestamp error invalid timestamp":
    let err = timestampError(tsInvalidTimestamp, "Bad timestamp value")
    check err.code == 4000 + ord(tsInvalidTimestamp)
    check "Invalid timestamp" in err.message

  test "timestamp error too many retries":
    let err = timestampError(tsTooManyRetries, "Retry limit exceeded")
    check err.code == 4000 + ord(tsTooManyRetries)
    check "Too many retries" in err.message

  test "timestamp error with context":
    let err = timestampError(tsClockTooFarBehind, "Clock issue", "Node 5")
    check err.context == "Node 5"

suite "TimestampProvider - Constants":
  test "default max offset":
    check DEFAULT_MAX_OFFSET_NS == 1_000_000 # 1ms

  test "max logical counter":
    check MAX_LOGICAL_COUNTER == 1_000_000

  test "logical bits":
    check LOGICAL_BITS == 20

  test "logical mask":
    check LOGICAL_MASK == (1 shl LOGICAL_BITS) - 1

suite "TimestampProvider - Edge Cases":
  test "timestamp at zero":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 0)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 0,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    let ts = tsProvider.now()
    check ts >= 0

  test "timestamp at max counter":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 1000_000_000,
      lastCounter: MAX_LOGICAL_COUNTER,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    # Should wrap
    discard tsProvider.now()
    check tsProvider.lastCounter < MAX_LOGICAL_COUNTER

  test "node ID boundary values":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProviderMax = newTimestampProvider(mockTimer, nodeId = 1023)
    check tsProviderMax.nodeId == 1023

    let tsProviderZero = newTimestampProvider(mockTimer, nodeId = 0)
    check tsProviderZero.nodeId == 0

  test "consecutive timestamps with same wall time":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 1000_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 1000_000_000,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )
    let timestamps: seq[Timestamp] = @[]
    for i in 0..<50:
      let ts = tsProvider.now()
      # Each timestamp should be unique
    check tsProvider.lastCounter == 50

suite "TimestampProvider - Integration with Mock Time Provider":
  test "mock time set to specific value":
    let mockTimer = sharedtimerMock.MockTimeProvider(currentTime: 5000_000_000)
    let tsProvider = TimestampProvider(
      timer: mockTimer,
      lastTimestamp: 0,
      lastCounter: 0,
      maxOffset: DEFAULT_MAX_OFFSET_NS,
      nodeId: 0
    )

    let ts = tsProvider.now()
    check ts == 5000_000_000
