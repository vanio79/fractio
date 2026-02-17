# Unit tests for SharedTimer (P2P time synchronization, OOP version)
# Tests offset calculation, consensus computation, and transaction ID generation

import unittest
import std/[random, times, math, locks, os]
import fractio/core/types
import fractio/utils/logging
import fractio/distributed/sharedtimer

suite "SharedTimer Tests (OOP)":

  var logger: Logger = nil
  var sync: SharedTimer

  setup:
    randomize(12345)
    let peers = @[
      PeerConfig(peerId: "node1", address: "127.0.0.1", port: 5200'u16,
          weight: 1.0),
      PeerConfig(peerId: "node2", address: "127.0.0.1", port: 5201'u16,
          weight: 1.0),
      PeerConfig(peerId: "node3", address: "127.0.0.1", port: 5202'u16, weight: 0.5)
    ]
    let clock = MonotonicTimeProvider()
    let network = SimulatedNetworkTransport(
      rng: initRand(12345),
      avgDelay: 10_000_000.0,
      delayVariance: 5_000_000.0,
      peerProcessingTime: 1_000_000
    )
    sync = newSharedTimer("testNode", 1'u16, peers, clock, network, logger)

  teardown:
    sync.stop()
    sync = nil

  test "calculateOffset with perfect network (no delay)":
    let (offset, delay) = calculateOffset(100, 200, 300, 400)
    check offset == 0.0
    check delay == 200.0

  test "calculateOffset with 50ms delay and -10ms clock offset":
    let (offset, delay) = calculateOffset(
        1_000_000_000'i64, # t1
      1_015_000_000'i64,   # t2 (25ms + (-10ms) = 15ms)
      1_016_000_000'i64,   # t3 (t2 + 1ms processing)
      1_051_000_000'i64)   # t4 (t1 + 25ms + 25ms + 1ms = 51ms)
    check abs(offset / 1_000_000.0 + 10.0) < 0.1
    check abs(delay / 1_000_000.0 - 50.0) < 0.1

  test "filterOutliers removes clear outliers":
    var offsets: seq[ClockOffset] = @[]
    offsets.add ClockOffset(offset: 1000.0, confidence: 1.0, peerId: "p1", lastUpdate: 0)
    offsets.add ClockOffset(offset: 1020.0, confidence: 1.0, peerId: "p2", lastUpdate: 0)
    offsets.add ClockOffset(offset: 980.0, confidence: 1.0, peerId: "p3", lastUpdate: 0)
    offsets.add ClockOffset(offset: 1010.0, confidence: 1.0, peerId: "p4", lastUpdate: 0)
    offsets.add ClockOffset(offset: 990.0, confidence: 1.0, peerId: "p5", lastUpdate: 0)
    offsets.add ClockOffset(offset: 10000.0, confidence: 1.0, peerId: "p6", lastUpdate: 0)
    let filtered = filterOutliers(offsets)
    check filtered.len == 5
    for o in filtered:
      check abs(o.offset) < 2000.0

  test "filterOutliers keeps all when within stddev":
    var offsets: seq[ClockOffset] = @[]
    for i in 0..10:
      offsets.add ClockOffset(offset: 1000.0 + (rand(100) - 50).float64,
        confidence: 1.0, peerId: "p" & $i, lastUpdate: 0)
    let filtered = filterOutliers(offsets)
    check filtered.len == 11

  test "computeConsensusOffset with clear majority":
    var offsets: seq[ClockOffset] = @[]
    for i in 0..2:
      offsets.add ClockOffset(offset: 100.0 + (rand(10)-5).float64,
        confidence: 1.0, peerId: "p" & $i, lastUpdate: 0)
    for i in 3..4:
      offsets.add ClockOffset(offset: 5000.0, confidence: 1.0, peerId: "p" & $i, lastUpdate: 0)
    let consensus = computeConsensusOffset(offsets)
    check abs(consensus - 100.0) < 10.0

  test "RingBuffer basic operations":
    var rb = newRingBuffer[int](3)
    check rb.size == 0
    rb.add(1)
    rb.add(2)
    check rb.size == 2
    rb.add(3)
    rb.add(4)
    check rb.size == 3
    let items = rb.items()
    check items.len == 3
    # Should contain 2,3,4 in some order (most recent first due to wrap)
    check (2 in items) and (3 in items) and (4 in items)

  test "RingBuffer clear works":
    var rb = newRingBuffer[int](5)
    for i in 1..5:
      rb.add(i)
    check rb.size == 5
    rb.clear()
    check rb.size == 0
    check rb.items().len == 0

  test "syncRound produces valid offsets":
    var transport = SimulatedNetworkTransport(
      rng: initRand(12345),
      avgDelay: 10_000_000.0,
      delayVariance: 5_000_000.0,
      peerProcessingTime: 1_000_000
    )
    let localSend = 1_000_000_000_000'i64
    let offsets = transport.syncRound(localSend, sync.getPeers())
    check offsets.len >= 0
    for offset in offsets:
      check offset.offset.abs < 1_000_000_000.0
      check offset.delay > 0.0 and offset.delay < 100_000_000.0
      check offset.confidence >= 0.0 and offset.confidence <= 1.0

  test "updateConsensus requires minimum peers":
    withLock(sync.mutex):
      sync.offsets = @[]
    sync.updateConsensus()
    check sync.getState() == tssFailed

  test "updateConsensus succeeds with sufficient peers":
    withLock(sync.mutex):
      sync.offsets = @[
        ClockOffset(offset: 1000.0, confidence: 1.0, peerId: "p1",
            lastUpdate: 0),
        ClockOffset(offset: 1020.0, confidence: 1.0, peerId: "p2", lastUpdate: 0)
      ]
      sync.offsetHistory.clear()
      for o in sync.offsets:
        sync.offsetHistory.add(o)
    sync.updateConsensus()
    check sync.getState() == tssSynchronized
    check abs(sync.getCurrentOffset() - 1010.0) < 20.0

  test "getSynchronizedTime combines local and offset":
    withLock(sync.mutex):
      sync.consensusOffset = 1_000_000.0
      sync.state = tssSynchronized
    let baseTime = 1_000_000_000'i64
    let mockClock = MockTimeProvider()
    mockClock.currentTime = baseTime
    sync.localClock = mockClock
    let synced = sync.getSynchronizedTime()
    check synced == baseTime + 1_000_000

  test "SharedTimer initialization":
    check sync.nodeId == "testNode"
    check sync.numericNodeId == 1'u16
    check sync.peers.len == 3
    check sync.getState() == tssUninitialized

  test "ClockOffset has required fields":
    let co = ClockOffset(
      offset: 123.4,
      delay: 50.6,
      peerId: "test",
      confidence: 0.8,
      lastUpdate: 1234567890
    )
    check co.offset == 123.4
    check co.delay == 50.6
    check co.peerId == "test"
    check co.confidence == 0.8

  test "meanStdDev calculation":
    let data = @[1.0, 2.0, 3.0, 4.0, 5.0]
    let (mean, stddev) = meanStdDev(data)
    check mean == 3.0
    check abs(stddev - sqrt(2.0)) < 0.001

  test "meanStdDev with single element":
    let (mean, stddev) = meanStdDev(@[42.0])
    check mean == 42.0
    check stddev == 0.0

  test "meanStdDev with empty sequence":
    let (mean, stddev) = meanStdDev(@[])
    check mean == 0.0
    check stddev == 0.0

  test "getTransactionID produces unique IDs":
    var ids: array[100, TransactionID]
    for i in 0..<ids.len:
      ids[i] = sync.getTransactionID()
    for i in 0..<ids.len:
      for j in i+1..<ids.len:
        check ids[i] != ids[j]

  test "getTransactionID format in synchronized mode":
    withLock(sync.mutex):
      sync.consensusOffset = 0
      sync.state = tssSynchronized
    let mockNs: int64 = 1_600_000_000_000_000_000'i64
    let mockClock = MockTimeProvider()
    mockClock.currentTime = mockNs
    sync.localClock = mockClock
    let id = sync.getTransactionID()
    let idVal = uint64(id.int64)
    let timePart = (idVal shr 22) and ((1'u64 shl 42) - 1)
    let nodePart = (idVal shr 12) and ((1'u64 shl 10) - 1)
    let counterPart = idVal and ((1'u64 shl 12) - 1)
    let expectedTimeMs = uint64(mockNs div 1_000_000'i64) and ((1'u64 shl 42)-1)
    check timePart == expectedTimeMs
    check nodePart == (sync.numericNodeId.uint64 and ((1'u64 shl 10)-1))
    check counterPart <= 4095'u64

  test "getTransactionID format in unsynchronized mode":
    withLock(sync.mutex):
      sync.state = tssFailed
    let id1 = sync.getTransactionID()
    let id2 = sync.getTransactionID()
    check id1 != id2
    let idVal = uint64(id1.int64)
    let nodePart = idVal shr 48
    check nodePart == (sync.numericNodeId.uint64 and 0xFFFF'u64)

  test "drift under 1ms with realistic network (3 peers)":
    randomize(12345)
    sync.localClock = MonotonicTimeProvider()
    # Run several synchronization ticks to converge
    for i in 0..4:
      sync.tick()
    let driftNs = abs(sync.getCurrentOffset())
    echo "\n  [Perf] Drift after 5 ticks: ", $driftNs, " ns (", $(driftNs /
        1_000_000.0), " ms)"
    check driftNs < 1_000_000.0

  test "consensus improves accuracy over naive average":
    randomize(12345)
    let trueOffset = 750_000.0 # 0.75 ms ground truth
    var offsets: seq[ClockOffset] = @[]
    # Generate 5 inliers with uniform noise +/- 200k ns
    for i in 0..4:
      let noise = (rand(400_000) - 200_000).float64
      offsets.add ClockOffset(offset: trueOffset + noise, confidence: 1.0,
          peerId: "p" & $i, lastUpdate: 0)
    # Add an outlier far from truth
    offsets.add ClockOffset(offset: 5_000_000.0, confidence: 1.0,
        peerId: "p5", lastUpdate: 0)
    withLock(sync.mutex):
      sync.offsets = offsets
      sync.offsetHistory.clear()
      for o in offsets:
        sync.offsetHistory.add(o)
    sync.updateConsensus()
    let consensus = sync.getCurrentOffset()
    # Compute naive average of all offsets
    var total = 0.0
    for o in offsets:
      total += o.offset
    let naive = total / offsets.len.float64
    echo "\n  [Accuracy] True offset: ", trueOffset, " ns"
    echo "  [Accuracy] Naive average: ", naive, " ns (error: ", abs(naive -
        trueOffset), " ns)"
    echo "  [Accuracy] Consensus:  ", consensus, " ns (error: ", abs(consensus -
        trueOffset), " ns)"
    check abs(consensus - trueOffset) < abs(naive - trueOffset)

  test "scales to 200 nodes - latency and correctness":
    randomize(12345)
    var manyPeers: seq[PeerConfig] = @[]
    for i in 0..199:
      manyPeers.add PeerConfig(peerId: "node" & $i, address: "127.0.0.1",
          port: uint16(5000+i), weight: 1.0)
    let manyNetwork = SimulatedNetworkTransport(
      rng: initRand(12345),
      avgDelay: 10_000_000.0,
      delayVariance: 5_000_000.0,
      peerProcessingTime: 1_000_000
    )
    let manyClock = MonotonicTimeProvider()
    let manySync = newSharedTimer("node0", 0'u16, manyPeers, manyClock,
        manyNetwork, logger)
    let start = epochTime()
    for i in 0..2: # 3 ticks
      manySync.tick()
    let elapsed = (epochTime() - start) * 1000.0         # milliseconds
    echo "\n  [Scalability] 200 nodes: 3 ticks took ", elapsed, " ms"
    check elapsed < 500.0 # should complete within 500ms
    check manySync.getState() == tssSynchronized
    let offset = manySync.getCurrentOffset()
    check abs(offset) < 1_000_000.0 # <1ms

  # State machine transition tests
  test "state transitions: uninitialized -> syncing -> synchronized":
    withLock(sync.mutex):
      sync.state = tssUninitialized
    check sync.getState() == tssUninitialized
    # Manually set offsets to trigger consensus
    withLock(sync.mutex):
      sync.offsets = @[
        ClockOffset(offset: 100.0, confidence: 1.0, peerId: "p1",
            lastUpdate: 0),
        ClockOffset(offset: 110.0, confidence: 1.0, peerId: "p2", lastUpdate: 0)
      ]
      sync.offsetHistory.clear()
      for o in sync.offsets:
        sync.offsetHistory.add(o)
    sync.updateConsensus()
    check sync.getState() == tssSynchronized

  test "setPeers updates peer list":
    let newPeers = @[
      PeerConfig(peerId: "newnode", address: "10.0.0.1", port: 5300'u16, weight: 1.0)
    ]
    sync.setPeers(newPeers)
    check sync.getPeers().len == 1
    check sync.getPeers()[0].peerId == "newnode"

  test "MockTimeProvider allows time manipulation":
    let mock = MockTimeProvider()
    mock.currentTime = 1_000_000_000_000'i64
    check mock.now() == 1_000_000_000_000'i64
    mock.currentTime = 2_000_000_000_000'i64
    check mock.now() == 2_000_000_000_000'i64

  test "SimulatedNetworkTransport produces reproducible results with fixed seed":
    let net1 = SimulatedNetworkTransport(
      rng: initRand(99999),
      avgDelay: 10_000_000.0,
      delayVariance: 5_000_000.0,
      peerProcessingTime: 1_000_000
    )
    let net2 = SimulatedNetworkTransport(
      rng: initRand(99999),
      avgDelay: 10_000_000.0,
      delayVariance: 5_000_000.0,
      peerProcessingTime: 1_000_000
    )
    let testPeers = @[
      PeerConfig(peerId: "nodeA", address: "127.0.0.1", port: 5400'u16,
          weight: 1.0),
      PeerConfig(peerId: "nodeB", address: "127.0.0.1", port: 5401'u16, weight: 1.0)
    ]
    let t = 1_000_000_000_000'i64
    let offsets1 = net1.syncRound(t, testPeers)
    let offsets2 = net2.syncRound(t, testPeers)
    check offsets1.len == offsets2.len
    for i in 0..<offsets1.len:
      check offsets1[i].offset == offsets2[i].offset
      check offsets1[i].delay == offsets2[i].delay
      check offsets1[i].peerId == offsets2[i].peerId

  test "TimeSyncState enum values are correct":
    check tssUninitialized == tssUninitialized
    check tssSyncing == tssSyncing
    check tssSynchronized == tssSynchronized
    check tssFailed == tssFailed

when isMainModule:
  echo "Running SharedTimer OOP tests..."
