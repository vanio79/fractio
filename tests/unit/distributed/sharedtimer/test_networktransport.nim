# Unit tests for fractio/distributed/sharedtimer/networktransport.nim
# Tests abstract NetworkTransport base class

import std/unittest
import fractio/distributed/sharedtimer/networktransport
import fractio/distributed/sharedtimer/types

type StartTestTransport = ref object of NetworkTransport
  started: bool

method start(self: StartTestTransport) =
  self.started = true

type SyncRoundTestTransport = ref object of NetworkTransport
  offsets: seq[ClockOffset]

method syncRound(self: SyncRoundTestTransport, localSend: Timestamp,
                 peers: seq[PeerConfig]): seq[ClockOffset] =
  result = self.offsets

type CloseTestTransport = ref object of NetworkTransport
  closed: bool

method close(self: CloseTestTransport) =
  self.closed = true

type CallCountTransport = ref object of NetworkTransport
  callCount: int

method start(self: CallCountTransport) =
  inc self.callCount

suite "NetworkTransport - Construction":

  test "create base transport":
    var transport: NetworkTransport
    new(transport)
    check transport != nil

suite "NetworkTransport - Default Methods":

  test "default start does nothing":
    var transport: NetworkTransport
    new(transport)
    transport.start()
    check true

  test "default syncRound returns empty":
    var transport: NetworkTransport
    new(transport)
    let result = transport.syncRound(1000_000_000, @[])
    check result.len == 0

  test "default close does nothing":
    var transport: NetworkTransport
    new(transport)
    transport.close()
    check true

suite "NetworkTransport - syncRound Interface":

  test "syncRound with peers parameter":
    var transport: NetworkTransport
    new(transport)
    let peers = @[PeerConfig(peerId: "p1", address: "127.0.0.1", port: 8080)]
    let result = transport.syncRound(1000_000_000, peers)
    check result.len == 0

  test "syncRound with timestamp parameter":
    var transport: NetworkTransport
    new(transport)
    let ts: Timestamp = 1234567890
    let result = transport.syncRound(ts, @[])
    check result.len == 0

  test "syncRound returns seq[ClockOffset]":
    var transport: NetworkTransport
    new(transport)
    let result: seq[ClockOffset] = transport.syncRound(0, @[])
    check result is seq[ClockOffset]

suite "NetworkTransport - Method Signatures":

  test "start is gcsafe":
    var transport: NetworkTransport
    new(transport)
    {.cast(gcsafe).}:
      transport.start()
    check true

  test "syncRound is gcsafe":
    var transport: NetworkTransport
    new(transport)
    {.cast(gcsafe).}:
      discard transport.syncRound(0, @[])
    check true

  test "close is gcsafe":
    var transport: NetworkTransport
    new(transport)
    {.cast(gcsafe).}:
      transport.close()
    check true

suite "NetworkTransport - Inheritance":

  test "derived type can override start":
    var transport: StartTestTransport
    new(transport)
    transport.started = false
    transport.start()
    check transport.started

  test "derived type can override syncRound":
    var transport: SyncRoundTestTransport
    new(transport)
    transport.offsets = @[ClockOffset(offset: 100.0, peerId: "test")]
    let result = transport.syncRound(0, @[])
    check result.len == 1
    check result[0].offset == 100.0

  test "derived type can override close":
    var transport: CloseTestTransport
    new(transport)
    transport.closed = false
    transport.close()
    check transport.closed

suite "NetworkTransport - Polymorphism":

  test "derived type is NetworkTransport":
    var transport: StartTestTransport
    new(transport)
    check transport of NetworkTransport

  test "base reference can hold derived":
    var derived: StartTestTransport
    new(derived)
    derived.started = false
    var base: NetworkTransport = derived
    check base of StartTestTransport

  test "method dispatch works polymorphically":
    var derived: CallCountTransport
    new(derived)
    derived.callCount = 0
    var base: NetworkTransport = derived
    base.start()
    check derived.callCount == 1

suite "NetworkTransport - Empty Peer List":

  test "syncRound handles empty peers":
    var transport: NetworkTransport
    new(transport)
    let result = transport.syncRound(1000, @[])
    check result.len == 0

suite "NetworkTransport - Timestamp Handling":

  test "syncRound accepts zero timestamp":
    var transport: NetworkTransport
    new(transport)
    let result = transport.syncRound(0, @[])
    check result.len == 0

  test "syncRound accepts large timestamp":
    var transport: NetworkTransport
    new(transport)
    let ts: Timestamp = 0x7FFFFFFFFFFFFFFF
    let result = transport.syncRound(ts, @[])
    check result.len == 0

  test "syncRound accepts negative timestamp":
    var transport: NetworkTransport
    new(transport)
    let ts: Timestamp = -1
    let result = transport.syncRound(ts, @[])
    check result.len == 0

suite "NetworkTransport - PeerConfig Handling":

  test "syncRound accepts multiple peers":
    var transport: NetworkTransport
    new(transport)
    let peers = @[
      PeerConfig(peerId: "p1", address: "10.0.0.1", port: 1000),
      PeerConfig(peerId: "p2", address: "10.0.0.2", port: 2000),
      PeerConfig(peerId: "p3", address: "10.0.0.3", port: 3000)
    ]
    let result = transport.syncRound(0, peers)
    check result.len == 0

  test "syncRound preserves peer order":
    var transport: NetworkTransport
    new(transport)
    let peers = @[
      PeerConfig(peerId: "first"),
      PeerConfig(peerId: "second"),
      PeerConfig(peerId: "third")
    ]
    discard transport.syncRound(0, peers)
    check peers[0].peerId == "first"
    check peers[1].peerId == "second"
    check peers[2].peerId == "third"
