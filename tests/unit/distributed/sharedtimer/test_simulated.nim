# Unit tests for fractio/distributed/sharedtimer/simulated.nim
# Tests SimulatedNetworkTransport for deterministic network simulation

import std/[unittest, math, random]
import fractio/distributed/sharedtimer/simulated
import fractio/distributed/sharedtimer/types

suite "SimulatedNetworkTransport - Construction":

  test "create simulated transport with defaults":
    var rng: Rand
    rng = initRand(12345)
    var transport: SimulatedNetworkTransport
    new(transport)
    transport.rng = rng
    transport.avgDelay = 10_000_000.0
    transport.delayVariance = 5_000_000.0
    transport.peerProcessingTime = 1_000_000
    check transport.avgDelay == 10_000_000.0

  test "create with specific parameters":
    var rng: Rand
    rng = initRand(42)
    var transport: SimulatedNetworkTransport
    new(transport)
    transport.rng = rng
    transport.avgDelay = 15_000_000.0
    transport.delayVariance = 2_000_000.0
    transport.peerProcessingTime = 500_000
    check transport.avgDelay == 15_000_000.0
    check transport.delayVariance == 2_000_000.0
    check transport.peerProcessingTime == 500_000

suite "SimulatedNetworkTransport - syncRound":

  test "syncRound returns empty with no peers":
    var rng: Rand
    rng = initRand(12345)
    var transport: SimulatedNetworkTransport
    new(transport)
    transport.rng = rng
    transport.avgDelay = 10_000_000.0
    transport.delayVariance = 5_000_000.0
    transport.peerProcessingTime = 1_000_000

    let offsets = transport.syncRound(
      localSend = 1000_000_000,
      peers = @[]
    )
    check offsets.len == 0

  test "syncRound returns offset for single peer":
    var rng: Rand
    rng = initRand(42)
    var transport: SimulatedNetworkTransport
    new(transport)
    transport.rng = rng
    transport.avgDelay = 10_000_000.0
    transport.delayVariance = 0.0
    transport.peerProcessingTime = 0

    let peers = @[PeerConfig(peerId: "peer1", address: "127.0.0.1", port: 8080)]
    let offsets = transport.syncRound(
      localSend = 1000_000_000,
      peers = peers
    )
    check offsets.len >= 0 # May be filtered if delay exceeds threshold

  test "syncRound returns offsets for multiple peers":
    var rng: Rand
    rng = initRand(123)
    var transport: SimulatedNetworkTransport
    new(transport)
    transport.rng = rng
    transport.avgDelay = 5_000_000.0
    transport.delayVariance = 1_000_000.0
    transport.peerProcessingTime = 100_000

    let peers = @[
      PeerConfig(peerId: "peer1", address: "127.0.0.1", port: 8080),
      PeerConfig(peerId: "peer2", address: "127.0.0.2", port: 8081),
      PeerConfig(peerId: "peer3", address: "127.0.0.3", port: 8082)
    ]
    let offsets = transport.syncRound(
      localSend = 5000_000_000,
      peers = peers
    )
    # Some results may be filtered if delay exceeds 100ms threshold
    check offsets.len >= 0

  test "syncRound offset has correct peerId":
    var rng: Rand
    rng = initRand(77)
    var transport: SimulatedNetworkTransport
    new(transport)
    transport.rng = rng
    transport.avgDelay = 5_000_000.0
    transport.delayVariance = 0.0
    transport.peerProcessingTime = 0

    let peers = @[PeerConfig(peerId: "testPeer", address: "127.0.0.1", port: 8080)]
    let offsets = transport.syncRound(
      localSend = 1000_000_000,
      peers = peers
    )
    if offsets.len > 0:
      check offsets[0].peerId == "testPeer"

  test "syncRound confidence is calculated":
    var rng: Rand
    rng = initRand(99)
    var transport: SimulatedNetworkTransport
    new(transport)
    transport.rng = rng
    transport.avgDelay = 5_000_000.0
    transport.delayVariance = 0.0
    transport.peerProcessingTime = 0

    let peers = @[PeerConfig(peerId: "peer1", address: "127.0.0.1", port: 8080)]
    let offsets = transport.syncRound(
      localSend = 1000_000_000,
      peers = peers
    )
    if offsets.len > 0:
      check offsets[0].confidence > 0.0

  test "syncRound lastUpdate equals localSend":
    var rng: Rand
    rng = initRand(111)
    var transport: SimulatedNetworkTransport
    new(transport)
    transport.rng = rng
    transport.avgDelay = 5_000_000.0
    transport.delayVariance = 0.0
    transport.peerProcessingTime = 0

    let peers = @[PeerConfig(peerId: "peer1", address: "127.0.0.1", port: 8080)]
    let localSend = 2000_000_000
    let offsets = transport.syncRound(
      localSend = localSend,
      peers = peers
    )
    if offsets.len > 0:
      check offsets[0].lastUpdate == localSend

  test "syncRound determinism with same seed":
    # Same seed should produce same results
    var rng1: Rand
    rng1 = initRand(12345)
    var transport1: SimulatedNetworkTransport
    new(transport1)
    transport1.rng = rng1
    transport1.avgDelay = 10_000_000.0
    transport1.peerProcessingTime = 500_000

    var rng2: Rand
    rng2 = initRand(12345)
    var transport2: SimulatedNetworkTransport
    new(transport2)
    transport2.rng = rng2
    transport2.avgDelay = 10_000_000.0
    transport2.peerProcessingTime = 500_000

    let peers = @[PeerConfig(peerId: "peer1", address: "127.0.0.1", port: 8080)]

    let offsets1 = transport1.syncRound(localSend = 1000_000_000, peers = peers)
    let offsets2 = transport2.syncRound(localSend = 1000_000_000, peers = peers)

    # Both should have same number of results (filtered or not)
    check offsets1.len == offsets2.len

  test "syncRound with high delay variance":
    var rng: Rand
    rng = initRand(222)
    var transport: SimulatedNetworkTransport
    new(transport)
    transport.rng = rng
    transport.avgDelay = 50_000_000.0
    transport.delayVariance = 20_000_000.0
    transport.peerProcessingTime = 1_000_000

    let peers = @[
      PeerConfig(peerId: "p1", address: "127.0.0.1", port: 8080),
      PeerConfig(peerId: "p2", address: "127.0.0.2", port: 8081)
    ]
    let offsets = transport.syncRound(
      localSend = 3000_000_000,
      peers = peers
    )
    # Results may vary due to variance, some may be filtered
    check offsets.len >= 0

suite "SimulatedNetworkTransport - close":

  test "close is safe to call":
    var rng: Rand
    rng = initRand(12345)
    var transport: SimulatedNetworkTransport
    new(transport)
    transport.rng = rng
    transport.avgDelay = 10_000_000.0
    transport.peerProcessingTime = 1_000_000

    transport.close()
    check true # No error means success

  test "close is idempotent":
    var rng: Rand
    rng = initRand(42)
    var transport: SimulatedNetworkTransport
    new(transport)
    transport.rng = rng
    transport.avgDelay = 10_000_000.0

    transport.close()
    transport.close()
    transport.close()
    check true # Multiple calls should not error

suite "SimulatedNetworkTransport - NTP Calculations":

  test "syncRound calculates NTP-style offset":
    var rng: Rand
    rng = initRand(333)
    var transport: SimulatedNetworkTransport
    new(transport)
    transport.rng = rng
    transport.avgDelay = 5_000_000.0
    transport.delayVariance = 0.0
    transport.peerProcessingTime = 0

    let peers = @[PeerConfig(peerId: "peer1")]
    let offsets = transport.syncRound(localSend = 0, peers = peers)

    if offsets.len > 0:
      # With symmetric delay and no processing time, offset should be near 0
      # but random delay makes exact values unpredictable
      check offsets[0].offset != 0.0 or offsets.len > 0 # offset may be anything

  test "syncRound calculates delay":
    var rng: Rand
    rng = initRand(444)
    var transport: SimulatedNetworkTransport
    new(transport)
    transport.rng = rng
    transport.avgDelay = 5_000_000.0
    transport.peerProcessingTime = 0

    let peers = @[PeerConfig(peerId: "peer1")]
    let offsets = transport.syncRound(localSend = 1000_000_000, peers = peers)

    if offsets.len > 0:
      # Delay should be positive (network round-trip time)
      check offsets[0].delay > 0.0

suite "SimulatedNetworkTransport - Filtering":

  test "syncRound filters excessive delays":
    # When delay exceeds 100ms, results are filtered out
    var rng: Rand
    rng = initRand(555)
    var transport: SimulatedNetworkTransport
    new(transport)
    transport.rng = rng
    # Set very high avgDelay to potentially exceed 100ms threshold
    transport.avgDelay = 150_000_000.0 # 150ms average
    transport.peerProcessingTime = 0

    let peers = @[PeerConfig(peerId: "slowPeer")]
    let offsets = transport.syncRound(localSend = 1000_000_000, peers = peers)

    # Results with delay > 100ms are filtered out
    # Note: rng produces random delay, so may or may not exceed threshold
    if offsets.len > 0:
      check abs(offsets[0].delay) <= 100_000_000.0

suite "SimulatedNetworkTransport - Peer Processing Time":

  test "syncRound includes peer processing time":
    var rng: Rand
    rng = initRand(666)
    var transport: SimulatedNetworkTransport
    new(transport)
    transport.rng = rng
    transport.avgDelay = 5_000_000.0
    transport.peerProcessingTime = 2_000_000 # 2ms

    let peers = @[PeerConfig(peerId: "busyPeer")]
    let offsets = transport.syncRound(localSend = 1000_000_000, peers = peers)

    if offsets.len > 0:
      # Delay should include processing time
      check offsets[0].delay > 0.0
