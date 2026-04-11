# Unit tests for Distributed SharedTimer Components
# Tests P2P time synchronization with mock implementations

import unittest
import fractio/distributed/sharedtimer/types
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/wallclock
import fractio/distributed/sharedtimer/monotonic

suite "SharedTimer - Mock Time Provider":
  test "create mock time provider":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 1000_000_000
    check mock.currentTime == 1000_000_000

  test "now returns current time":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 1000_000_000
    let ts = mock.now()
    check ts == 1000_000_000

  test "set time":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 0
    mock.setTime(5000_000_000)
    check mock.currentTime == 5000_000_000

suite "SharedTimer - WallClock":
  test "wallclock returns positive time":
    var clock: WallClockTimeProvider
    new(clock)
    let ts = clock.now()
    check ts > 0

  test "wallclock returns nanoseconds":
    var clock: WallClockTimeProvider
    new(clock)
    let ts = clock.now()
    # Wallclock should return nanoseconds (very large number)
    check ts > 1_000_000_000'i64

  test "wallclock time increases":
    var clock: WallClockTimeProvider
    new(clock)
    let ts1 = clock.now()
    # Even without sleep, multiple calls should return different values
    # (though might be same due to precision)
    let ts2 = clock.now()
    check ts2 >= ts1

suite "SharedTimer - Monotonic Clock":
  test "monotonic returns positive time":
    var clock: MonotonicTimeProvider
    new(clock)
    let ts = clock.now()
    check ts > 0

  test "monotonic is monotonic":
    var clock: MonotonicTimeProvider
    new(clock)
    let ts1 = clock.now()
    let ts2 = clock.now()
    check ts2 >= ts1

suite "SharedTimer - Time Provider Interface":
  test "mock implements TimeProvider":
    var mock: MockTimeProvider
    new(mock)
    mock.currentTime = 1000_000_000
    # Interface method works
    let ts = mock.now()
    check ts == 1000_000_000

  test "wallclock implements TimeProvider":
    var clock: WallClockTimeProvider
    new(clock)
    let ts = clock.now()
    check ts > 0

  test "monotonic implements TimeProvider":
    var clock: MonotonicTimeProvider
    new(clock)
    let ts = clock.now()
    check ts > 0

suite "SharedTimer - ClockOffset":
  test "create clock offset":
    let offset = ClockOffset(
      offset: 100.0,
      delay: 50.0,
      peerId: "peer1",
      confidence: 0.95,
      lastUpdate: 1000_000_000
    )
    check offset.offset == 100.0
    check offset.delay == 50.0
    check offset.peerId == "peer1"
    check offset.confidence == 0.95
    check offset.lastUpdate == 1000_000_000

  test "clock offset default values":
    let offset = ClockOffset()
    check offset.offset == 0.0
    check offset.delay == 0.0
    check offset.peerId == ""
    check offset.confidence == 0.0
    check offset.lastUpdate == 0

suite "SharedTimer - PeerConfig":
  test "create peer config":
    let peer = PeerConfig(
      peerId: "node1",
      address: "192.168.1.1",
      port: 8080,
      weight: 1.0
    )
    check peer.peerId == "node1"
    check peer.address == "192.168.1.1"
    check peer.port == 8080
    check peer.weight == 1.0

  test "peer config default weight":
    let peer = PeerConfig(peerId: "node2")
    check peer.weight == 0.0
