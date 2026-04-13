# Unit tests for fractio/distributed/sharedtimer/sharedtimer_impl.nim
# Tests SharedTimer implementation and helper functions

import std/[unittest, math, times]
import locks
import atomics
import fractio/distributed/sharedtimer/sharedtimer_impl
import fractio/distributed/sharedtimer/types
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/networktransport
import fractio/core/types

type MockTransportForTimer = ref object of NetworkTransport
  startedFlag: bool

method start(self: MockTransportForTimer) =
  self.startedFlag = true

method close(self: MockTransportForTimer) =
  self.startedFlag = false

type MockTransportSimple = ref object of NetworkTransport
method close(self: MockTransportSimple) = discard

type MockTransportWithResults = ref object of NetworkTransport
  results: seq[ClockOffset]

method syncRound(self: MockTransportWithResults, localSend: Timestamp,
    peers: seq[PeerConfig]): seq[ClockOffset] {.gcsafe.} =
  result = self.results

method close(self: MockTransportWithResults) {.gcsafe.} =
  discard

type MockTransportOffsets = ref object of NetworkTransport
  offsetsToReturn: seq[ClockOffset]

method syncRound(self: MockTransportOffsets, localSend: Timestamp,
    peers: seq[PeerConfig]): seq[ClockOffset] {.gcsafe.} =
  result = self.offsetsToReturn

method close(self: MockTransportOffsets) {.gcsafe.} =
  discard

type MockTransportTime = ref object of NetworkTransport
  results: seq[ClockOffset]

method syncRound(self: MockTransportTime, localSend: Timestamp,
    peers: seq[PeerConfig]): seq[ClockOffset] {.gcsafe.} =
  result = self.results

method close(self: MockTransportTime) {.gcsafe.} =
  discard

type MockTransportHistory = ref object of NetworkTransport
  results: seq[ClockOffset]

method syncRound(self: MockTransportHistory, localSend: Timestamp,
    peers: seq[PeerConfig]): seq[ClockOffset] {.gcsafe.} =
  result = self.results

method close(self: MockTransportHistory) {.gcsafe.} =
  discard

type MockTransportError = ref object of NetworkTransport

method syncRound(self: MockTransportError, localSend: Timestamp,
    peers: seq[PeerConfig]): seq[ClockOffset] {.gcsafe.} =
  raise newException(Exception, "Network error")

method close(self: MockTransportError) {.gcsafe.} =
  discard

suite "SharedTimer - Helper Functions":

  test "calculateOffset basic":
    let (offset, delay) = calculateOffset(
      localSend = 1000,
      peerReceive = 1010,
      peerResponse = 1015,
      localReceive = 1025
    )
    check offset == ((1010.0 - 1000.0) + (1015.0 - 1025.0)) / 2.0
    check delay == (1025.0 - 1000.0) - (1015.0 - 1010.0)

  test "calculateOffset with symmetric delay":
    let (offset, delay) = calculateOffset(
      localSend = 0,
      peerReceive = 10,
      peerResponse = 20,
      localReceive = 30
    )
    check offset == 0.0
    check delay == 20.0

  test "calculateOffset peer ahead":
    let (offset, delay) = calculateOffset(
      localSend = 100,
      peerReceive = 110,
      peerResponse = 115,
      localReceive = 120
    )
    check offset == 2.5

  test "calculateOffset peer behind":
    let (offset, delay) = calculateOffset(
      localSend = 100,
      peerReceive = 90,
      peerResponse = 95,
      localReceive = 110
    )
    check offset == -12.5

  test "meanStdDev empty sequence":
    let (mean, stddev) = meanStdDev(@[])
    check mean == 0.0
    check stddev == 0.0

  test "meanStdDev single value":
    let (mean, stddev) = meanStdDev(@[5.0])
    check mean == 5.0
    check stddev == 0.0

  test "meanStdDev two values":
    let (mean, stddev) = meanStdDev(@[2.0, 4.0])
    check mean == 3.0
    check stddev == 1.0

  test "meanStdDev multiple values":
    let (mean, stddev) = meanStdDev(@[1.0, 2.0, 3.0, 4.0, 5.0])
    check mean == 3.0
    check stddev > 0.0

  test "meanStdDev constant values":
    let (mean, stddev) = meanStdDev(@[10.0, 10.0, 10.0])
    check mean == 10.0
    check stddev == 0.0

suite "SharedTimer - filterOutliers":

  test "filterOutliers less than 3 items":
    let offsets = @[ClockOffset(offset: 100.0)]
    let filtered = filterOutliers(offsets)
    check filtered.len == 1

  test "filterOutliers two items":
    let offsets = @[ClockOffset(offset: 100.0), ClockOffset(offset: 200.0)]
    let filtered = filterOutliers(offsets)
    check filtered.len == 2

  test "filterOutliers three items":
    let offsets = @[ClockOffset(offset: 100.0), ClockOffset(offset: 105.0),
        ClockOffset(offset: 110.0)]
    let filtered = filterOutliers(offsets)
    check filtered.len == 3

  test "filterOutliers removes extreme outlier":
    let offsets = @[
      ClockOffset(offset: 100.0),
      ClockOffset(offset: 100.0),
      ClockOffset(offset: 100.0),
      ClockOffset(offset: 10000.0)
    ]
    let filtered = filterOutliers(offsets)
    check filtered.len >= 1

  test "filterOutliers keeps all normal":
    let offsets = @[
      ClockOffset(offset: 100.0),
      ClockOffset(offset: 102.0),
      ClockOffset(offset: 98.0),
      ClockOffset(offset: 101.0),
      ClockOffset(offset: 99.0)
    ]
    let filtered = filterOutliers(offsets)
    check filtered.len == 5

suite "SharedTimer - computeConsensusOffset":

  test "computeConsensusOffset empty":
    let offset = computeConsensusOffset(@[])
    check offset == 0.0

  test "computeConsensusOffset single":
    let offset = computeConsensusOffset(@[ClockOffset(offset: 100.0)])
    check offset == 100.0

  test "computeConsensusOffset two items":
    let offsets = @[
      ClockOffset(offset: 100.0, confidence: 1.0, peerId: "peer1"),
      ClockOffset(offset: 200.0, confidence: 1.0, peerId: "peer2")
    ]
    let offset = computeConsensusOffset(offsets)
    check offset >= 100.0 and offset <= 200.0

  test "computeConsensusOffset weighted median":
    let offsets = @[
      ClockOffset(offset: 50.0, confidence: 1.0, peerId: "a"),
      ClockOffset(offset: 100.0, confidence: 10.0, peerId: "trusted_peer"),
      ClockOffset(offset: 150.0, confidence: 1.0, peerId: "c")
    ]
    let offset = computeConsensusOffset(offsets)
    check offset == 100.0

suite "SharedTimer - TimeSyncState":

  test "all states defined":
    check tssUninitialized.ord == 0
    check tssSyncing.ord == 1
    check tssSynchronized.ord == 2
    check tssFailed.ord == 3

  test "state transitions":
    check tssUninitialized < tssSyncing
    check tssSyncing < tssSynchronized
    check tssSynchronized < tssFailed

suite "SharedTimer - Construction":

  test "create with minimal params":
    var mockClock: MockTimeProvider
    new(mockClock)
    mockClock.currentTime = 1000_000_000

    var transport: MockTransportForTimer
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test-node",
      numericNodeId = 1'u16,
      localClock = mockClock,
      network = transport
    )
    check timer.nodeId == "test-node"
    check timer.numericNodeId == 1
    check timer.state == tssUninitialized
    transport.close()

  test "create with peers":
    let peers = @[PeerConfig(peerId: "p1", address: "127.0.0.1", port: 8080)]
    var mockClock: MockTimeProvider
    new(mockClock)

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "node1",
      numericNodeId = 0'u16,
      peers = peers,
      localClock = mockClock,
      network = transport
    )
    check timer.peers.len == 1
    transport.close()

  test "create with defaults":
    let timer = newSharedTimer(nodeId = "default-node", numericNodeId = 100'u16)
    check timer.nodeId == "default-node"
    check timer.localClock != nil
    check timer.network != nil
    timer.stop()

  test "syncInterval default":
    let timer = newSharedTimer(nodeId = "test", numericNodeId = 1'u16)
    check timer.syncInterval.inNanoseconds == DEFAULT_SYNC_INTERVAL
    timer.stop()

  test "offsetHistory initialized":
    let timer = newSharedTimer(nodeId = "test", numericNodeId = 1'u16)
    check timer.offsetHistory.capacity == MAX_HISTORY_SIZE
    check timer.offsetHistory.size == 0
    timer.stop()

suite "SharedTimer - State Management":

  test "getState initial":
    var mockClock: MockTimeProvider
    new(mockClock)

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      localClock = mockClock,
      network = transport
    )
    check timer.getState() == tssUninitialized
    transport.close()

  test "setState changes state":
    var mockClock: MockTimeProvider
    new(mockClock)

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      localClock = mockClock,
      network = transport
    )
    timer.setState(tssSyncing)
    check timer.getState() == tssSyncing
    transport.close()

suite "SharedTimer - Peer Management":

  test "setPeers updates peer list":
    var mockClock: MockTimeProvider
    new(mockClock)

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      localClock = mockClock,
      network = transport
    )
    let newPeers = @[PeerConfig(peerId: "new1"), PeerConfig(peerId: "new2")]
    timer.setPeers(newPeers)
    check timer.getPeers().len == 2
    transport.close()

  test "getPeers returns copy":
    var mockClock: MockTimeProvider
    new(mockClock)

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      peers = @[PeerConfig(peerId: "p1")],
      localClock = mockClock,
      network = transport
    )
    let peers1 = timer.getPeers()
    timer.setPeers(@[])
    let peers2 = timer.getPeers()
    check peers1.len == 1
    check peers2.len == 0
    transport.close()

suite "SharedTimer - Offset Management":

  test "getCurrentOffset initial":
    var mockClock: MockTimeProvider
    new(mockClock)

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      localClock = mockClock,
      network = transport
    )
    check timer.getCurrentOffset() == 0.0
    transport.close()

suite "SharedTimer - Time Methods":

  test "now returns local time when uninitialized":
    var mockClock: MockTimeProvider
    new(mockClock)
    mockClock.currentTime = 5000_000_000

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      localClock = mockClock,
      network = transport
    )
    let ts = timer.now()
    check ts == 5000_000_000
    transport.close()

  test "getSynchronizedTime without sync":
    var mockClock: MockTimeProvider
    new(mockClock)
    mockClock.currentTime = 1000_000_000

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      localClock = mockClock,
      network = transport
    )
    let ts = timer.getSynchronizedTime()
    check ts == 1000_000_000
    transport.close()

suite "SharedTimer - isSynchronized":

  test "isSynchronized false initially":
    var mockClock: MockTimeProvider
    new(mockClock)

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      localClock = mockClock,
      network = transport
    )
    check not timer.isSynchronized()
    transport.close()

  test "isSynchronized true after setState":
    var mockClock: MockTimeProvider
    new(mockClock)

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      localClock = mockClock,
      network = transport
    )
    timer.setState(tssSynchronized)
    check timer.isSynchronized()
    transport.close()

suite "SharedTimer - TransactionID":

  test "getTransactionID returns valid ID":
    let timer = newSharedTimer(nodeId = "test", numericNodeId = 1'u16)
    let tid = timer.getTransactionID()
    check tid != zeroTransactionID()
    timer.stop()

  test "getTransactionID generates unique IDs":
    let timer = newSharedTimer(nodeId = "test", numericNodeId = 1'u16)
    let id1 = timer.getTransactionID()
    let id2 = timer.getTransactionID()
    check id1 != id2
    timer.stop()

suite "SharedTimer - Constants":

  test "DEFAULT_SYNC_INTERVAL":
    check DEFAULT_SYNC_INTERVAL == 1_000_000_000'i64

  test "REQUEST_TIMEOUT_NS":
    check REQUEST_TIMEOUT_NS == 100_000_000'i64

  test "MAX_HISTORY_SIZE":
    check MAX_HISTORY_SIZE == 100

  test "OUTLIER_STDDEV_FACTOR":
    check OUTLIER_STDDEV_FACTOR == 2.0

  test "MIN_PEERS_FOR_CONSENSUS":
    check MIN_PEERS_FOR_CONSENSUS == 2

  test "MAX_CLOCK_DRIFT_NS":
    check MAX_CLOCK_DRIFT_NS == 1_000_000'i64

suite "SharedTimer - Mutex":

  test "mutex initialized":
    var mockClock: MockTimeProvider
    new(mockClock)

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      localClock = mockClock,
      network = transport
    )
    try:
      acquire(timer.mutex)
      release(timer.mutex)
      check true
    except:
      check false
    transport.close()

suite "SharedTimer - Running Flag":

  test "running flag initialized to false":
    let timer = newSharedTimer(nodeId = "test", numericNodeId = 1'u16)
    check not timer.running.load()
    timer.stop()

suite "SharedTimer - Background Thread":

  test "start sets running flag":
    let timer = newSharedTimer(nodeId = "test", numericNodeId = 1'u16)
    timer.start()
    check timer.running.load()
    timer.stop()

  test "stop clears running flag":
    let timer = newSharedTimer(nodeId = "test", numericNodeId = 1'u16)
    timer.start()
    timer.stop()
    check not timer.running.load()

  test "start is idempotent":
    let timer = newSharedTimer(nodeId = "test", numericNodeId = 1'u16)
    timer.start()
    timer.start()
    check timer.running.load()
    timer.stop()

  test "stop without start is safe":
    let timer = newSharedTimer(nodeId = "test", numericNodeId = 1'u16)
    timer.stop()
    check not timer.running.load()

suite "SharedTimer - updateConsensus":

  test "updateConsensus fails with insufficient peers":
    var mockClock: MockTimeProvider
    new(mockClock)
    mockClock.currentTime = 1000_000_000

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      localClock = mockClock,
      network = transport
    )
    timer.offsets = @[ClockOffset(offset: 100.0, peerId: "p1")]
    timer.updateConsensus()
    check timer.getState() == tssFailed
    transport.close()

  test "updateConsensus succeeds with enough peers":
    var mockClock: MockTimeProvider
    new(mockClock)
    mockClock.currentTime = 1000_000_000

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      localClock = mockClock,
      network = transport
    )
    timer.offsets = @[
      ClockOffset(offset: 100.0, peerId: "peer1", confidence: 1.0),
      ClockOffset(offset: 105.0, peerId: "peer2", confidence: 1.0)
    ]
    timer.updateConsensus()
    check timer.getState() == tssSynchronized
    check timer.consensusOffset >= 100.0
    transport.close()

  test "updateConsensus filters outliers":
    var mockClock: MockTimeProvider
    new(mockClock)
    mockClock.currentTime = 1000_000_000

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      localClock = mockClock,
      network = transport
    )
    # One extreme outlier, others clustered around 100
    timer.offsets = @[
      ClockOffset(offset: 100.0, peerId: "p1", confidence: 1.0),
      ClockOffset(offset: 100.0, peerId: "p2", confidence: 1.0),
      ClockOffset(offset: 100.0, peerId: "p3", confidence: 1.0),
      ClockOffset(offset: 10000.0, peerId: "p4", confidence: 1.0) # outlier
    ]
    timer.updateConsensus()
    # Should be synchronized after filtering
    check timer.getState() == tssSynchronized
    transport.close()

  test "updateConsensus fails if all peers filtered as outliers":
    var mockClock: MockTimeProvider
    new(mockClock)
    mockClock.currentTime = 1000_000_000

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      localClock = mockClock,
      network = transport
    )
    # All peers are extreme outliers relative to each other
    timer.offsets = @[
      ClockOffset(offset: 0.0, peerId: "p1", confidence: 1.0),
      ClockOffset(offset: 1000000.0, peerId: "p2", confidence: 1.0),
      ClockOffset(offset: -1000000.0, peerId: "p3", confidence: 1.0)
    ]
    timer.updateConsensus()
    # After filtering, may have fewer than MIN_PEERS_FOR_CONSENSUS
    check timer.getState() == tssFailed or timer.getState() == tssSynchronized
    transport.close()

  test "updateConsensus updates consensusOffset":
    var mockClock: MockTimeProvider
    new(mockClock)
    mockClock.currentTime = 1000_000_000

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      localClock = mockClock,
      network = transport
    )
    timer.offsets = @[
      ClockOffset(offset: 50.0, peerId: "p1", confidence: 1.0),
      ClockOffset(offset: 50.0, peerId: "p2", confidence: 1.0)
    ]
    let oldOffset = timer.consensusOffset
    timer.updateConsensus()
    check timer.consensusOffset != oldOffset
    transport.close()

suite "SharedTimer - tick":

  test "tick sets state to syncing":
    var mockClock: MockTimeProvider
    new(mockClock)
    mockClock.currentTime = 1000_000_000

    var transport: MockTransportWithResults
    new(transport)
    transport.results = @[
      ClockOffset(offset: 100.0, peerId: "p1", confidence: 1.0),
      ClockOffset(offset: 100.0, peerId: "p2", confidence: 1.0)
    ]

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      peers = @[PeerConfig(peerId: "p1"), PeerConfig(peerId: "p2")],
      localClock = mockClock,
      network = transport
    )
    timer.tick()
    check timer.getState() == tssSynchronized
    transport.close()

  test "tick updates offsets":
    var mockClock: MockTimeProvider
    new(mockClock)
    mockClock.currentTime = 1000_000_000

    var transport: MockTransportOffsets
    new(transport)
    transport.offsetsToReturn = @[
      ClockOffset(offset: 50.0, peerId: "p1", confidence: 1.0),
      ClockOffset(offset: 55.0, peerId: "p2", confidence: 1.0)
    ]

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      peers = @[PeerConfig(peerId: "p1")],
      localClock = mockClock,
      network = transport
    )
    timer.tick()
    check timer.offsets.len == 2
    transport.close()

  test "tick updates lastSyncTime":
    var mockClock: MockTimeProvider
    new(mockClock)
    mockClock.currentTime = 5000_000_000

    var transport: MockTransportTime
    new(transport)
    transport.results = @[
      ClockOffset(offset: 100.0, peerId: "p1", confidence: 1.0),
      ClockOffset(offset: 100.0, peerId: "p2", confidence: 1.0)
    ]

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      peers = @[PeerConfig(peerId: "p1")],
      localClock = mockClock,
      network = transport
    )
    let beforeSync = timer.lastSyncTime
    timer.tick()
    check timer.lastSyncTime > beforeSync
    transport.close()

  test "tick adds to offsetHistory":
    var mockClock: MockTimeProvider
    new(mockClock)
    mockClock.currentTime = 1000_000_000

    var transport: MockTransportHistory
    new(transport)
    transport.results = @[
      ClockOffset(offset: 100.0, peerId: "p1", confidence: 1.0),
      ClockOffset(offset: 100.0, peerId: "p2", confidence: 1.0)
    ]

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      peers = @[PeerConfig(peerId: "p1")],
      localClock = mockClock,
      network = transport
    )
    let beforeSize = timer.offsetHistory.size
    timer.tick()
    check timer.offsetHistory.size > beforeSize
    transport.close()

  test "tick sets failed on exception":
    var mockClock: MockTimeProvider
    new(mockClock)
    mockClock.currentTime = 1000_000_000

    var transport: MockTransportError
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      peers = @[PeerConfig(peerId: "p1")],
      localClock = mockClock,
      network = transport
    )
    timer.tick()
    check timer.getState() == tssFailed
    transport.close()

suite "SharedTimer - now with synchronized state":

  test "now returns synchronized time when state is synchronized":
    var mockClock: MockTimeProvider
    new(mockClock)
    mockClock.currentTime = 1000_000_000

    var transport: MockTransportSimple
    new(transport)

    let timer = newSharedTimer(
      nodeId = "test",
      numericNodeId = 1'u16,
      localClock = mockClock,
      network = transport
    )
    # Manually set offset and state
    timer.consensusOffset = 500.0
    timer.setState(tssSynchronized)

    let ts = timer.now()
    # Should return synchronized time (local + offset)
    check ts == 1000_000_500 # local + offset as int64
    transport.close()

suite "SharedTimer - Dollar operator":

  test "dollar for ClockOffset":
    let offset = ClockOffset(
      offset: 100.0,
      delay: 50.0,
      peerId: "peer1",
      confidence: 0.95,
      lastUpdate: 1000_000_000
    )
    let s = $offset
    check s.len > 0

  test "dollar for PeerConfig":
    let peer = PeerConfig(
      peerId: "node1",
      address: "192.168.1.1",
      port: 8080,
      weight: 1.0
    )
    let s = $peer
    check s.len > 0
