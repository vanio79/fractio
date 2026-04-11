# Unit tests for fractio/distributed/sharedtimer/types.nim
# Tests ClockOffset and PeerConfig types

import std/[unittest, strutils]
import fractio/distributed/sharedtimer/types
import fractio/core/types

suite "ClockOffset":

  test "default ClockOffset":
    let offset = ClockOffset()
    check offset.offset == 0.0
    check offset.delay == 0.0
    check offset.peerId == ""
    check offset.confidence == 0.0
    check offset.lastUpdate == 0

  test "ClockOffset with values":
    let offset = ClockOffset(
      offset: 1000.0, # 1 microsecond ahead
      delay: 5000.0,  # 5 microseconds RTT
      peerId: "node-2",
      confidence: 0.95,
      lastUpdate: 123456789
    )
    check offset.offset == 1000.0
    check offset.delay == 5000.0
    check offset.peerId == "node-2"
    check offset.confidence == 0.95
    check offset.lastUpdate == 123456789

  test "positive offset means peer ahead":
    let offset = ClockOffset(offset: 100.0)
    check offset.offset > 0.0

  test "negative offset means peer behind":
    let offset = ClockOffset(offset: -50.0)
    check offset.offset < 0.0

  test "confidence range 0-1":
    for confidence in [0.0, 0.5, 1.0]:
      let offset = ClockOffset(confidence: confidence)
      check offset.confidence >= 0.0
      check offset.confidence <= 1.0

  test "delay reflects RTT":
    let offset = ClockOffset(delay: 10_000.0) # 10 microseconds
    check offset.delay > 0.0

suite "PeerConfig":

  test "default PeerConfig":
    let config = PeerConfig()
    check config.peerId == ""
    check config.address == ""
    check config.port == 0
    check config.weight == 0.0

  test "PeerConfig with values":
    let config = PeerConfig(
      peerId: "peer-1",
      address: "192.168.1.100",
      port: 9000'u16,
      weight: 0.8
    )
    check config.peerId == "peer-1"
    check config.address == "192.168.1.100"
    check config.port == 9000
    check config.weight == 0.8

  test "PeerConfig weight 0.0 to 1.0":
    for w in [0.0, 0.25, 0.5, 0.75, 1.0]:
      let config = PeerConfig(weight: w)
      check config.weight >= 0.0
      check config.weight <= 1.0

  test "PeerConfig with localhost":
    let config = PeerConfig(
      peerId: "local",
      address: "127.0.0.1",
      port: 8080'u16
    )
    check config.address == "127.0.0.1"

  test "PeerConfig typical setup":
    let config = PeerConfig(
      peerId: "node-3",
      address: "10.0.0.3",
      port: 5000'u16,
      weight: 1.0
    )
    check config.peerId == "node-3"
    check config.address.contains("10.0.0")
    check config.port > 0

suite "Timestamp Integration":

  test "ClockOffset uses core Timestamp":
    let now: Timestamp = 1000000
    let offset = ClockOffset(lastUpdate: now)
    check offset.lastUpdate == now

  test "Timestamp comparisons":
    let earlier: Timestamp = 1000
    let later: Timestamp = 2000
    check earlier < later
    check later > earlier

suite "Multiple ClockOffsets":

  test "different peers have different offsets":
    let offsets = [
      ClockOffset(offset: 100.0, peerId: "peer1"),
      ClockOffset(offset: -50.0, peerId: "peer2"),
      ClockOffset(offset: 0.0, peerId: "peer3")
    ]
    check offsets[0].offset > offsets[1].offset
    check offsets[2].offset == 0.0

  test "sorted by confidence":
    let offsets = [
      ClockOffset(confidence: 0.5, peerId: "low"),
      ClockOffset(confidence: 0.9, peerId: "high"),
      ClockOffset(confidence: 0.7, peerId: "medium")
    ]
    # Manual sort check
    var maxConf = 0.0
    for o in offsets:
      if o.confidence > maxConf:
        maxConf = o.confidence
    check maxConf == 0.9

suite "Peer Weight Distribution":

  test "equal weights":
    let peers = [
      PeerConfig(peerId: "p1", weight: 0.5),
      PeerConfig(peerId: "p2", weight: 0.5)
    ]
    check peers[0].weight == peers[1].weight

  test "unequal weights for trust levels":
    let peers = [
      PeerConfig(peerId: "primary", weight: 1.0),
      PeerConfig(peerId: "backup", weight: 0.5),
      PeerConfig(peerId: "observer", weight: 0.1)
    ]
    check peers[0].weight > peers[1].weight
    check peers[1].weight > peers[2].weight
