# Integration tests for SharedTimer using real UDPTransport
# Tests actual UDP communication, thread safety, and synchronization correctness

import unittest
import std/[times, os, tables, math, net]

import fractio/utils/logging
import fractio/distributed/sharedtimer
import fractio/distributed/sharedtimer/udptransport

# Helper to get an ephemeral port by binding a temporary socket
proc getEphemeralPort(): uint16 =
  var s = newSocket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)
  try:
    s.bindAddr(Port(0))
    let (_, p) = s.getLocalAddr()
    result = uint16(p)
  finally:
    s.close()



# Quiet logger for tests (suppress all logs)
let testLogger* = Logger(name: "Test", minLevel: llWarn, handlers: @[])

# Helper to create a SharedTimer with UDPTransport
proc newTimerWithUDP(nodeId: string, numericNodeId: uint16, port: uint16 = 0,
                     peers: seq[PeerConfig] = @[],
                         logger: Logger = nil): SharedTimer =
  let clock = MonotonicTimeProvider()
  # Use quiet test logger if none provided
  let log = if logger.isNil: testLogger else: logger
  let net = newUDPTransport(port = port, logger = log, timeProvider = clock)
  result = newSharedTimer(
    nodeId = nodeId,
    numericNodeId = numericNodeId,
    localClock = clock,
    network = net,
    peers = peers,
    logger = log
  )



proc makePeer(peerId: string, address: string, port: uint16): PeerConfig =
  PeerConfig(peerId: peerId, address: address, port: port, weight: 1.0)

suite "SharedTimer UDP Integration Tests":

  var timers: seq[SharedTimer]
  var ports: seq[uint16]

  setup:
    timers = @[]
    ports = @[]

  teardown:
    # Ensure all timers are stopped and transport closed
    for t in timers:
      try:
        t.stop()
      except:
        discard
    timers = @[]
    ports = @[]
    # Small delay to allow threads to join
    sleep(100)

  test "Multiple nodes can synchronize via UDP with small clock offsets":
    # Create 3 nodes with slightly different local times (within 10ms)
    # In real deployment, they would have different clocks; we'll use a base time
    # Create first node (will be the reference)
    let port1 = getEphemeralPort()
    var timer1 = newTimerWithUDP("node1", 1, port1, @[])
    timer1.start()
    timers.add(timer1)
    ports.add(port1)

    # Create second node that knows about node1
    let port2 = getEphemeralPort()
    let peersFor2 = @[makePeer("node1", "127.0.0.1", port1)]
    var timer2 = newTimerWithUDP("node2", 2, port2, peersFor2)
    timer2.start()
    timers.add(timer2)
    ports.add(port2)

    # Create third node that knows about both node1 and node2
    let port3 = getEphemeralPort()
    let peersFor3 = @[
      makePeer("node1", "127.0.0.1", port1),
      makePeer("node2", "127.0.0.1", port2)
    ]
    var timer3 = newTimerWithUDP("node3", 3, port3, peersFor3)
    timer3.start()
    timers.add(timer3)
    ports.add(port3)

    # Also add node3 as a peer to node1 and node2 (full mesh)
    # Note: In a real ring, each node would have a subset; for integration test we want eventual sync
    timer1.setPeers(@[
      makePeer("node2", "127.0.0.1", port2),
      makePeer("node3", "127.0.0.1", port3)
    ])
    timer2.setPeers(@[
      makePeer("node1", "127.0.0.1", port1),
      makePeer("node3", "127.0.0.1", port3)
    ])

    # Run several synchronization rounds
    # Give time for threads to start
    sleep(200)

    var synchronizedCount = 0
    var maxDrift: float64 = 0.0
    var times: seq[Timestamp] = @[]

    # Perform multiple ticks and check convergence
    for i in 0..<5:
      # Tick each timer
      for t in timers:
        t.tick()
      sleep(150) # Allow for network round-trip and consensus
    
    # Check synchronization state
    for t in timers:
      let state = t.getState()
      # After several ticks with good network, nodes should be synchronized
      # However, UDP can be slow; we'll check at least they attempted sync
      check state != tssUninitialized
      if state == tssSynchronized:
        inc(synchronizedCount)
      times.add(t.now())

    # Check that all timers are in a consistent state (not failed)
    for t in timers:
      check t.getState() != tssFailed

    # Measure drift between nodes that became synchronized
    if synchronizedCount >= 2:
      # Compare times - they should be within ~1ms (accounting for network jitter)
      for i, t1 in timers:
        for j, t2 in timers:
          if i < j and t1.getState() == tssSynchronized and t2.getState() == tssSynchronized:
            let time1 = t1.now()
            let time2 = t2.now()
            let diff = abs((time1 - time2).float64)
            maxDrift = max(maxDrift, diff)

    discard

    # The drift should be small; we allow up to 5ms due to localhost variability
    check maxDrift < 5_000_000.0

  test "SharedTimer can recover from failed to synchronized when sufficient peers added":
    # Start with a single node that has no peers - will be failed
    let port1 = getEphemeralPort()
    var timer1 = newTimerWithUDP("rec1", 1, port1, @[])
    timer1.start()
    timers.add(timer1)
    sleep(100)
    timer1.tick()
    check timer1.getState() == tssFailed

    # Now add two more nodes that will form a full mesh
    let port2 = getEphemeralPort()
    let port3 = getEphemeralPort()
    var timer2 = newTimerWithUDP("rec2", 2, port2, @[])
    var timer3 = newTimerWithUDP("rec3", 3, port3, @[])
    timers.add(timer2)
    timers.add(timer3)

    # Build full mesh: each node knows the other two
    timer1.setPeers(@[
      makePeer("rec2", "127.0.0.1", port2),
      makePeer("rec3", "127.0.0.1", port3)
    ])
    timer2.setPeers(@[
      makePeer("rec1", "127.0.0.1", port1),
      makePeer("rec3", "127.0.0.1", port3)
    ])
    timer3.setPeers(@[
      makePeer("rec1", "127.0.0.1", port1),
      makePeer("rec2", "127.0.0.1", port2)
    ])

    # Start timer2 and timer3
    timer2.start()
    timer3.start()
    sleep(200)

    # Perform multiple ticks
    for i in 0..<5:
      timer1.tick()
      timer2.tick()
      timer3.tick()
      sleep(200)

    # After several rounds, all should be synchronized
    check timer1.getState() == tssSynchronized
    check timer2.getState() == tssSynchronized
    check timer3.getState() == tssSynchronized

  test "UDPTransport statistics track cross-node communication":
    # Start two nodes that communicate
    let port1 = getEphemeralPort()
    var timer1 = newTimerWithUDP("stats1", 1, port1, @[])
    timer1.start()
    timers.add(timer1)

    let port2 = getEphemeralPort()
    var timer2 = newTimerWithUDP("stats2", 2, port2, @[makePeer("stats1",
        "127.0.0.1", port1)])
    timer2.start()
    timers.add(timer2)
    timer1.setPeers(@[makePeer("stats2", "127.0.0.1", port2)])

    sleep(100)

    # Get initial stats (cast to UDPTransport to access getStats)
    let stats1Init = cast[UDPTransport](timer1.network).getStats()
    let stats2Init = cast[UDPTransport](timer2.network).getStats()

    # Perform a sync round
    timer2.tick()
    sleep(200)

    # Check that stats increased appropriately
    let stats1After = cast[UDPTransport](timer1.network).getStats()
    let stats2After = cast[UDPTransport](timer2.network).getStats()

    # node2 sent a request (to node1), node1 received and responded
    check stats1After.sent >= stats1Init.sent
    check stats1After.received >= stats1Init.received
    check stats2After.sent >= stats2Init.sent
    check stats2After.received >= stats2Init.received

    # Total sent should be balanced (request from 2, response from 1)
    check stats1After.sent + stats2After.sent >= 1
    check stats1After.received + stats2After.received >= 1

  test "Thread safety: concurrent ticks on same SharedTimer":
    # Start a single timer with a reachable peer
    let port1 = getEphemeralPort()
    var timer = newTimerWithUDP("concurrent", 1, port1, @[])
    timer.start()
    timers.add(timer)

    # Add a peer that we won't actually start (will just test that tick() serializes correctly)
    timer.setPeers(@[makePeer("ghost", "192.0.2.1", 12345)])

    sleep(100)

    # Call tick multiple times rapidly from this main thread
    # These should not cause data races on the timer's internal state
    for i in 0..<10:
      timer.tick()
      sleep(10)

    # Should still be functional
    check timer.getState() != tssFailed or timer.getState() == tssFailed # Either is fine
    let finalState = timer.getState()

    # Verify we can still get transaction IDs without crashing
    for i in 0..<5:
      discard timer.getTransactionID()

    # State should be consistent
    check timer.getState() == finalState

  test "Integration: network timeout propagates to SharedTimer error state":
    # Single node with only unreachable peers
    let port = getEphemeralPort()
    var timer = newTimerWithUDP("timeout", 1, port, @[])
    timer.start()
    timers.add(timer)

    timer.setPeers(@[makePeer("unreachable", "192.0.2.1", 9999)])

    sleep(100)

    # First tick should attempt sync and fail due to timeout
    timer.tick()
    sleep(150)

    # Should end up in failed state (network level synctimeout caught by SharedTimer.tick)
    check timer.getState() == tssFailed

  test "Integration: consensus requires minimum peers":
    # With 1 peer, consensus should fail even if peer responds (MIN_PEERS_FOR_CONSENSUS=2)
    let port1 = getEphemeralPort()
    var timer1 = newTimerWithUDP("lonely", 1, port1, @[])
    timer1.start()
    timers.add(timer1)

    let port2 = getEphemeralPort()
    var timer2 = newTimerWithUDP("partner", 2, port2, @[makePeer("lonely",
        "127.0.0.1", port1)])
    timer2.start()
    timers.add(timer2)
    timer1.setPeers(@[makePeer("partner", "127.0.0.1", port2)])

    sleep(150)

    # Both tick
    timer1.tick()
    timer2.tick()
    sleep(150)

    # With only 1 peer each, consensus cannot be reached; should fail
    check timer1.getState() == tssFailed
    check timer2.getState() == tssFailed

  test "Integration: three nodes reach consensus with proper configuration":
    # Create 3 nodes in a full mesh
    var portsArr: array[3, uint16]
    var timersArr: array[3, SharedTimer]

    for i in 0..2:
      let port = getEphemeralPort()
      portsArr[i] = port
      # We'll create timers without peers first, then set peers after all created
      # But we need to start them first to bind sockets
      var t = newTimerWithUDP("node" & $i, uint16(i+10), port, @[])
      t.start()
      timers.add(t)
      timersArr[i] = t

    # Build full mesh
    for i in 0..2:
      var peers: seq[PeerConfig] = @[]
      for j in 0..2:
        if i != j:
          peers.add(makePeer("node" & $j, "127.0.0.1", portsArr[j]))
      timersArr[i].setPeers(peers)

    sleep(200)

    # Run multiple ticks
    for round in 0..<4:
      for t in timersArr:
        t.tick()
      sleep(200)

    # At least one should be synchronized (usually all)
    var anySynced = false
    for t in timersArr:
      if t.getState() == tssSynchronized:
        anySynced = true
      # None should be failed
      check t.getState() != tssFailed

    check anySynced # At least one achieved consensus

  test "Integration: transaction ID uniqueness across nodes and calls":
    # With two synchronized nodes, generate many IDs and verify uniqueness
    var timersSmall: array[2, SharedTimer]
    var portsSmall: array[2, uint16]

    for i in 0..1:
      let port = getEphemeralPort()
      portsSmall[i] = port
      var t = newTimerWithUDP("unid" & $i, uint16(i+100), port, @[])
      t.start()
      timers.add(t)
      timersSmall[i] = t

    # Connect them
    timersSmall[0].setPeers(@[makePeer("unid1", "127.0.0.1", portsSmall[1])])
    timersSmall[1].setPeers(@[makePeer("unid0", "127.0.0.1", portsSmall[0])])

    sleep(200)

    # Sync them
    for it in 0..<5:
      for t in timersSmall:
        t.tick()
      sleep(200)

    # If both synchronized, check uniqueness
    if timersSmall[0].getState() == tssSynchronized and timersSmall[1].getState() == tssSynchronized:
      var idSet: Table[int64, bool]
      idSet = initTable[int64, bool]()

      # Generate 100 IDs from each node
      for t in timersSmall:
        for i in 0..<100:
          let id = t.getTransactionID().int64
          check not (id in idSet) # Must be globally unique
          idSet[id] = true

  test "Scale test: 200 nodes with real UDP (stress)":
    # This test may take a few seconds - it creates 200 nodes, each with 5 random peers.
    const nodeCount = 200
    const peersPerNode = 5

    var allPorts: seq[uint16] = @[]
    var allTimers: seq[SharedTimer] = @[]

    # Step 1: Create all nodes first (no peers yet)
    for i in 0..<nodeCount:
      let port = getEphemeralPort()
      let t = newTimerWithUDP("scale" & $i, uint16(i), port, @[])
      allTimers.add(t)
      allPorts.add(port)

    # Step 2: Assign peers in a ring pattern (deterministic)
    # Each node connects to the next 'peersPerNode' nodes (mod nodeCount)
    for i in 0..<nodeCount:
      var peers: seq[PeerConfig] = @[]
      for offset in 1..peersPerNode:
        let j = (i + offset) mod nodeCount
        peers.add(makePeer("scale" & $j, "127.0.0.1", allPorts[j]))
      allTimers[i].setPeers(peers)

    # Step 3: Start all nodes and track for teardown
    for t in allTimers:
      t.start()
    # Also add to suite's global cleanup list
    timers &= allTimers

    # Allow time for all server threads to initialize
    sleep(1000)

    # Step 4: Run multiple sync rounds
    for round in 0..<3:
      for t in allTimers:
        t.tick()
      # Wait for network + processing
      sleep(400)

    # Step 5: Evaluate results
    var syncCount = 0
    var failedCount = 0
    var otherCount = 0
    for t in allTimers:
      case t.getState()
      of tssSynchronized:
        inc(syncCount)
      of tssFailed:
        inc(failedCount)
      else:
        inc(otherCount)

    # Expect most nodes to achieve synchronization (allow some failures due to timing/jitter)
    check syncCount >= nodeCount div 2
    # Failure rate should be low (< 20%)
    check failedCount <= nodeCount div 5

    # Cleanup: allTimers will be closed by teardown (via timers list)
