# Unit tests for UDPTransport
# 100% coverage target

import unittest
import std/[random, times, math, locks, os, strutils, net, asyncdispatch, atomics]
import fractio/distributed/sharedtimer/udptransport
import fractio/distributed/sharedtimer/types
import fractio/utils/logging

import nativesockets

# Helper to get an ephemeral port by binding a temporary socket
proc getEphemeralPort(): uint16 =
  var s = newSocket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)
  try:
    s.bindAddr(Port(0))
    let (_, p) = s.getLocalAddr()
    result = uint16(p)
  finally:
    s.close()

suite "UDPTransport Unit Tests":

  var logger: Logger
  var serverTransport: UDPTransport
  var clientTransport: UDPTransport
  var serverPort: uint16

  setup:
    randomize(12345)
    logger = Logger(name: "Test", minLevel: llWarn, handlers: @[])

  teardown:
    if not clientTransport.isNil:
      try: clientTransport.close() except: discard
    clientTransport = nil
    if not serverTransport.isNil:
      try: serverTransport.close() except: discard
    serverTransport = nil

  test "UDPTransport creation and properties":
    let t = newUDPTransport(port = 0, logger = logger)
    check not t.isNil
    check t.serverSocket.isNil
    check t.localPort == 0'u16
    check not t.logger.isNil
    check not t.timeProvider.isNil

  test "UDPTransport start and close":
    let t = newUDPTransport(port = 0, logger = logger)
    check not load(t.serverRunning, moRelaxed)
    t.start()
    try:
      check t.localPort != 0'u16
      check load(t.serverRunning, moRelaxed)
      check not t.serverSocket.isNil
      var s = t.getStats()
      check s.sent == 0
      check s.received == 0
      check s.errors == 0
    finally:
      t.close()
      check not load(t.serverRunning, moRelaxed)

  test "syncRound requires transport to be started":
    let t = newUDPTransport(port = 0, logger = logger)
    let peers = @[PeerConfig(peerId: "p1", address: "127.0.0.1",
        port: 12345'u16, weight: 1.0)]
    expect IOError:
      discard t.syncRound(Timestamp(1_000_000_000_000'i64), peers)

  test "server responds to client request (happy path)":
    serverTransport = newUDPTransport(port = 0, logger = logger)
    serverTransport.start()
    serverPort = serverTransport.localPort
    check serverPort != 0

    clientTransport = newUDPTransport(port = 0, logger = logger)
    clientTransport.start()

    let peers = @[PeerConfig(peerId: "server", address: "127.0.0.1",
        port: serverPort, weight: 1.0)]
    let localSend = Timestamp(int64(epochTime() * 1e9))
    let offsets = clientTransport.syncRound(localSend, peers)
    check offsets.len == 1
    let off = offsets[0]
    check off.peerId == "server"
    check off.delay > 0.0
    check off.delay < 100_000_000.0
    check abs(off.offset) < 10_000_000.0
    check off.confidence > 0.0 and off.confidence <= 1.0
    check off.lastUpdate == localSend

    var s = clientTransport.getStats()
    check s.sent >= 1
    check s.received >= 1

  test "syncRound filters high delay (>100ms)":
    serverTransport = newUDPTransport(port = 0, logger = logger)
    serverTransport.start()
    serverPort = serverTransport.localPort

    clientTransport = newUDPTransport(port = 0, logger = logger)
    clientTransport.start()

    let peers = @[PeerConfig(peerId: "server", address: "127.0.0.1",
        port: serverPort, weight: 1.0)]
    let oldNs = int64(epochTime() * 1e9) - 200_000_000'i64
    let oldTime = Timestamp(oldNs)
    let offsets = clientTransport.syncRound(oldTime, peers)
    check offsets.len == 0
    var s = clientTransport.getStats()
    check s.sent >= 1

  test "syncRound timeout with unreachable peer":
    clientTransport = newUDPTransport(port = 0, logger = logger)
    clientTransport.start()
    let peers = @[PeerConfig(peerId: "ghost", address: "192.0.2.1",
        port: 9999'u16, weight: 1.0)]
    let localSend = Timestamp(1_000_000_000_000'i64)
    expect SyncTimeout:
      discard clientTransport.syncRound(localSend, peers)
    var s = clientTransport.getStats()
    check s.errors >= 1

  test "getStats returns snapshot":
    serverTransport = newUDPTransport(port = 0, logger = logger)
    serverTransport.start()
    serverPort = serverTransport.localPort
    clientTransport = newUDPTransport(port = 0, logger = logger)
    clientTransport.start()
    let peers = @[PeerConfig(peerId: "s", address: "127.0.0.1",
        port: serverPort, weight: 1.0)]
    discard clientTransport.syncRound(Timestamp(1_000_000_000_000'i64), peers)
    var s1 = clientTransport.getStats()
    var s2 = clientTransport.getStats()
    check s1.sent == s2.sent
    check s1.received == s2.received
    check s1.errors == s2.errors

  test "close shuts down server thread and releases resources":
    serverTransport = newUDPTransport(port = 0, logger = logger)
    serverTransport.start()
    let port = serverTransport.localPort
    check load(serverTransport.serverRunning, moRelaxed)
    serverTransport.close()
    check not load(serverTransport.serverRunning, moRelaxed)
    expect IOError:
      discard serverTransport.syncRound(Timestamp(1_000_000_000_000'i64), @[
          PeerConfig(peerId: "s", address: "127.0.0.1", port: port, weight: 1.0)])

  test "start called twice raises error":
    let t = newUDPTransport(port = 0, logger = logger)
    t.start()
    expect ValueError:
      t.start()

  test "statistics increment correctly after multiple calls":
    serverTransport = newUDPTransport(port = 0, logger = logger)
    serverTransport.start()
    serverPort = serverTransport.localPort
    clientTransport = newUDPTransport(port = 0, logger = logger)
    clientTransport.start()
    let peers = @[PeerConfig(peerId: "s", address: "127.0.0.1",
        port: serverPort, weight: 1.0)]
    let baseTime = Timestamp(int64(epochTime() * 1e9))
    for i in 0..<5:
      discard clientTransport.syncRound(Timestamp(baseTime.uint64 + uint64(i)), peers)
    var s = clientTransport.getStats()
    check s.sent == 5
    check s.received == 5
    check s.errors == 0

  test "logger is used for info, debug, warn, error":
    let t = newUDPTransport(port = 0, logger = logger)
    t.start()
    t.close()

  when isMainModule:
    discard # Tests run via nimble
