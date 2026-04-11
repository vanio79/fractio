# Unit tests for fractio/distributed/sharedtimer/mock.nim
# Tests MockTimeProvider for deterministic testing

import std/unittest
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/timeprovider
import fractio/core/types

suite "MockTimeProvider - Construction":

  test "create mock time provider":
    var mock: MockTimeProvider
    new(mock)
    check mock != nil

  test "default currentTime is zero":
    var mock: MockTimeProvider
    new(mock)
    check mock.currentTime == 0

  test "currentTime can be set at construction":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 1000_000_000
    check mock.currentTime == 1000_000_000

suite "MockTimeProvider - now()":

  test "now returns currentTime":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 1000_000_000
    let ts = mock.now()
    check ts == 1000_000_000

  test "now returns same value multiple times":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 5000_000_000
    let ts1 = mock.now()
    let ts2 = mock.now()
    let ts3 = mock.now()
    check ts1 == ts2
    check ts2 == ts3
    check ts1 == 5000_000_000

  test "now returns zero when currentTime is zero":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 0
    check mock.now() == 0

  test "now returns negative timestamp":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = -1000
    check mock.now() == -1000

  test "now returns max timestamp":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 0x7FFFFFFFFFFFFFFF
    check mock.now() == 0x7FFFFFFFFFFFFFFF

suite "MockTimeProvider - setTime()":

  test "setTime changes currentTime":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 0
    mock.setTime(1000_000_000)
    check mock.currentTime == 1000_000_000

  test "setTime affects now()":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 0
    mock.setTime(2000_000_000)
    check mock.now() == 2000_000_000

  test "setTime can advance time":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 1000
    check mock.now() == 1000
    mock.setTime(2000)
    check mock.now() == 2000
    mock.setTime(3000)
    check mock.now() == 3000

  test "setTime can go backwards":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 5000
    mock.setTime(1000)
    check mock.now() == 1000

  test "setTime preserves exact value":
    var mock: MockTimeProvider
    new(mock)
    let exactValue: Timestamp = 1234567890123456789
    mock.setTime(exactValue)
    check mock.currentTime == exactValue
    check mock.now() == exactValue

suite "MockTimeProvider - Inheritance":

  test "MockTimeProvider is TimeProvider":
    var mock: MockTimeProvider
    new(mock)
    check mock of TimeProvider

  test "can be used as TimeProvider reference":
    var mock: MockTimeProvider
    new(mock)
    var provider: TimeProvider = mock
    check provider != nil
    check provider.now() == 0

  test "now() polymorphic dispatch":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 1000
    var provider: TimeProvider = mock
    check provider.now() == 1000

suite "MockTimeProvider - Determinism":

  test "time does not change without setTime":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 1000_000_000
    let before = mock.now()
    let after = mock.now()
    check before == after

  test "repeated calls are deterministic":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 42
    for i in 1..100:
      check mock.now() == 42

suite "MockTimeProvider - Edge Cases":

  test "large positive timestamp":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 9223372036854775807
    check mock.now() == 9223372036854775807

  test "large negative timestamp":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = -9223372036854775807
    check mock.now() == -9223372036854775807

  test "typical nanosecond timestamp":
    var mock: MockTimeProvider
    new(mock)
    let typicalNs = 1700000000000000000'i64
    mock.currentTime = typicalNs
    check mock.now() == typicalNs

suite "MockTimeProvider - Thread Safety":

  test "now() is gcsafe":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 1000
    {.cast(gcsafe).}:
      let ts = mock.now()
    check ts == 1000

suite "MockTimeProvider - Time Sequence Simulation":

  test "simulate advancing time":
    var mock: MockTimeProvider
    new(mock)
    var timestamps: seq[Timestamp] = @[]
    for i in 1..10:
      mock.setTime(i * 1_000_000_000'i64)
      timestamps.add(mock.now())

    for i in 0..<9:
      check timestamps[i] < timestamps[i+1]

  test "simulate clock with offset":
    var mock: MockTimeProvider
    new(mock)
    let baseTime = 1000_000_000'i64
    let offset = 50_000_000'i64

    mock.currentTime = baseTime
    let localTime = mock.now()
    mock.currentTime = baseTime + offset
    let peerTime = mock.now()

    check peerTime - localTime == offset

suite "MockTimeProvider - Integration Patterns":

  test "reset time for each test":
    var mock: MockTimeProvider
    new(mock)

    mock.setTime(0)
    check mock.now() == 0

    mock.setTime(1000)
    check mock.now() == 1000

    mock.setTime(0)
    check mock.now() == 0

  test "time precision":
    var mock: MockTimeProvider
    new(mock)
    let preciseNs = 1'i64
    mock.currentTime = preciseNs
    check mock.now() == 1

    mock.currentTime = 999
    check mock.now() == 999
