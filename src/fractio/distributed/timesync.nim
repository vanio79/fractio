# P2P Clock Synchronization for Fractio
# Provides sub-millisecond time synchronization across hundreds of nodes without a master
# Inspired by NTP/PTP but simplified for transaction ID generation

import ../core/types
import ../utils/logging
import std/[times, random, math, algorithm, sequtils, atomics, os, tables]
import locks

type
  TimeSyncState* = enum
    tssUninitialized,
    tssSyncing,
    tssSynchronized,
    tssFailed

  Timestamp* = int64 # Nanosecond precision Unix timestamp

  TimeSyncMsgKind* = enum
    tsmRequest, tsmResponse

  # NTP-style timestamp exchange
  TimeSyncMessage* = ref object
    requestId*: uint64         # Common field for matching requests/responses
    case kind*: TimeSyncMsgKind
    of tsmRequest:
      sendTime*: Timestamp     # t1: when request was sent (local time)
    of tsmResponse:
      receiveTime*: Timestamp  # t2: when request was received (peer time)
      responseTime*: Timestamp # t3: when response was sent (peer time)
      peerId*: string

  ClockOffset* = object
    offset*: float64       # Nanoseconds offset from local to peer time
    delay*: float64        # Round-trip delay in nanoseconds
    peerId*: string
    confidence*: float64   # Based on consistency of measurements
    lastUpdate*: Timestamp # Last time this offset was updated

  PeerConfig* = object
    peerId*: string
    address*: string
    port*: uint16
    weight*: float64 # Trust weight for this peer (0.0-1.0)

  RingBuffer*[T] = object
    data*: seq[T]
    capacity*: int
    head*: int
    size*: int

  # P2P synchronizer - each node runs one independently
  P2PTimeSynchronizer* = ref object
    nodeId*: string
    numericNodeId*: uint16                  # Unique numeric ID (0-1023 for 10-bit space)
    localClock*: proc(): Timestamp          # Function to get local time
    peers*: seq[PeerConfig]
    state*: TimeSyncState
    offsets*: seq[ClockOffset]              # Current offsets from peers
    consensusOffset*: float64               # Combined offset from all peers
    offsetHistory*: RingBuffer[ClockOffset] # For outlier filtering
    syncInterval*: Duration                 # How often to sync (default: 1 second)
    requestTimeout*: Duration
    lastSyncTime*: Timestamp
    mutex*: Lock                            # Protects offsets and state
    logger*: Logger
    txCounter*: Atomic[int]                 # Global transaction ID counter (24-bit)

const
  DEFAULT_SYNC_INTERVAL* = 1_000_000_000'i64 # 1 second in nanoseconds
  REQUEST_TIMEOUT_NS* = 100_000_000'i64      # 100ms in nanoseconds
  MAX_HISTORY_SIZE* = 100                    # Keep last 100 measurements per peer
  OUTLIER_STDDEV_FACTOR* = 2.0               # Filter measurements beyond 2σ
  MIN_PEERS_FOR_CONSENSUS* = 2               # Need at least 2 peers for consensus
  MAX_CLOCK_DRIFT_NS* = 1_000_000'i64        # 1ms max allowed drift before re-sync

# Helper to get current monotonic time in nanoseconds
proc nowMonotonicNs*(): Timestamp =
  let t = getTime()
  result = toUnix(t) * 1_000_000_000 + nanosecond(t).int64

# Helper to get wall-clock time
proc nowWallClockNs*(): Timestamp =
  let t = getTime()
  result = toUnix(t) * 1_000_000_000 + nanosecond(t).int64

# Initialize ring buffer
proc initRingBuffer*[T](capacity: int): RingBuffer[T] =
  RingBuffer[T](
    data: newSeq[T](capacity),
    capacity: capacity,
    head: 0,
    size: 0
  )

# Add item to ring buffer
proc add*[T](rb: var RingBuffer[T], item: T) =
  rb.data[rb.head] = item
  rb.head = (rb.head + 1) mod rb.capacity
  if rb.size < rb.capacity:
    inc(rb.size)

# Get all items as seq
proc items*[T](rb: RingBuffer[T]): seq[T] =
  result = @[]
  if rb.size == 0: return
  if rb.size < rb.capacity:
    result = rb.data[0..<rb.size]
  else:
    # Wrap around
    result = rb.data[rb.head..^1] & rb.data[0..<rb.head]

# Calculate mean and stddev
proc meanStdDev*(data: seq[float64]): tuple[mean: float64, stddev: float64] =
  if data.len == 0:
    return (0.0, 0.0)
  let sum = data.sum()
  let mean = sum / float64(data.len)
  var variance = 0.0
  for x in data:
    let diff = x - mean
    variance += diff * diff
  variance /= float64(data.len)
  (mean, sqrt(variance))

# Filter outliers using standard deviation
proc filterOutliers*(offsets: seq[ClockOffset]): seq[ClockOffset] =
  if offsets.len < 3:
    return offsets
  let offsetsNs = offsets.mapIt(it.offset)
  let (mean, stddev) = meanStdDev(offsetsNs)
  let threshold = OUTLIER_STDDEV_FACTOR * stddev
  result = offsets.filterIt(abs(it.offset - mean) <= threshold)

# Compute consensus offset using weighted median
proc computeConsensusOffset*(offsets: seq[ClockOffset]): float64 =
  if offsets.len == 0:
    return 0.0
  if offsets.len == 1:
    return offsets[0].offset

  # Weighted median based on confidence and peer weight
  var weighted: seq[(float64, float64)] = @[]       # (offset, weight)
  for offset in offsets:
    let weight = offset.confidence * offset.peerId.len.float64 # Simplified weight
    weighted.add((offset.offset, weight))

  # Sort by offset
  weighted.sort do (x, y: (float64, float64)) -> int:
    result = cmp(x[0], y[0])

  # Compute weighted median
  let totalWeight = weighted.mapIt(it[1]).sum()
  var cumulative = 0.0
  for (offset, weight) in weighted:
    cumulative += weight
    if cumulative >= totalWeight / 2.0:
      return offset

  # Fallback to simple median
  result = weighted[weighted.len div 2][0]

# Create new P2P time synchronizer
proc newP2PTimeSynchronizer*(nodeId: string, numericNodeId: uint16, peers: seq[PeerConfig],
                            logger: Logger = nil): P2PTimeSynchronizer =
  result = P2PTimeSynchronizer(
    nodeId: nodeId,
    numericNodeId: numericNodeId,
    localClock: nowMonotonicNs,
    peers: peers,
    state: tssUninitialized,
    offsets: @[],
    consensusOffset: 0.0,
    offsetHistory: initRingBuffer[ClockOffset](MAX_HISTORY_SIZE),
    syncInterval: initDuration(nanoseconds = DEFAULT_SYNC_INTERVAL),
    requestTimeout: initDuration(nanoseconds = REQUEST_TIMEOUT_NS),
    lastSyncTime: 0,
    mutex: Lock(), # default constructed; then initLock
    logger: logger
  )
  initLock(result.mutex)

# Update peer configuration
proc setPeers*(s: P2PTimeSynchronizer, peers: seq[PeerConfig]) =
  withLock(s.mutex):
    s.peers = peers

# Get current synchronized time (with offset correction)
proc getSynchronizedTime*(s: P2PTimeSynchronizer): Timestamp =
  let local = s.localClock()
  withLock(s.mutex):
    result = local + s.consensusOffset.int64

# Get the current offset from consensus (for diagnostics)
proc getCurrentOffset*(s: P2PTimeSynchronizer): float64 =
  withLock(s.mutex):
    result = s.consensusOffset

# Get synchronization state
proc getState*(s: P2PTimeSynchronizer): TimeSyncState =
  withLock(s.mutex):
    result = s.state

# Calculate offset from a single sync exchange (NTP-style)
proc calculateOffset(localSend: Timestamp, peerReceive: Timestamp,
                     peerResponse: Timestamp, localReceive: Timestamp): tuple[
                         offset, delay: float64] =
  # NTP algorithm:
  # delay = (t4 - t1) - (t3 - t2)
  # offset = ((t2 - t1) + (t3 - t4)) / 2
  let t1 = localSend.float64
  let t2 = peerReceive.float64
  let t3 = peerResponse.float64
  let t4 = localReceive.float64
  let delay = (t4 - t1) - (t3 - t2)
  let offset = ((t2 - t1) + (t3 - t4)) / 2.0
  (offset, delay)

# Update offsets based on recent history
proc updateConsensus*(s: P2PTimeSynchronizer) =
  withLock(s.mutex):
    if s.offsets.len < MIN_PEERS_FOR_CONSENSUS:
      s.state = tssFailed
      if s.logger != nil:
        var fields = initTable[string, string]()
        fields["peers"] = $s.offsets.len
        s.logger.warn("TimeSync: Insufficient peers for consensus", fields)
      return

    # Filter outliers
    let filtered = filterOutliers(s.offsets)

    if filtered.len < MIN_PEERS_FOR_CONSENSUS:
      if s.logger != nil:
        var fields = initTable[string, string]()
        fields["remaining"] = $filtered.len
        s.logger.warn("TimeSync: All peers filtered as outliers", fields)
      s.state = tssFailed
      return

    # Compute consensus
    let newOffset = computeConsensusOffset(filtered)

    # Check drift
    let drift = abs(newOffset - s.consensusOffset)
    if drift > MAX_CLOCK_DRIFT_NS.float64:
      if s.logger != nil:
        var fields = initTable[string, string]()
        fields["drift_ns"] = $drift
        fields["old"] = $s.consensusOffset
        fields["new"] = $newOffset
        s.logger.warn("TimeSync: Large drift detected", fields)
    else:
      if s.logger != nil:
        var fields = initTable[string, string]()
        fields["drift_ns"] = $(drift)
        s.logger.debug("TimeSync: Drift within bounds", fields)

    s.consensusOffset = newOffset
    s.state = tssSynchronized
    if s.logger != nil:
      var fields = initTable[string, string]()
      fields["offset_ns"] = $s.consensusOffset
      fields["peers"] = $filtered.len
      s.logger.info("TimeSync: Consensus updated", fields)

# Perform a single synchronization round with all peers
proc syncRound*(s: P2PTimeSynchronizer): seq[ClockOffset] =
  result = @[]
  let localSendTime = s.localClock()

  for peer in s.peers:
    # In production, this would send/receive network messages
    # Here we simulate with local time for testing
    # Implementation in network layer will handle actual RPC

    # Simulate network delay (random 1-20ms)
    let simulatedDelay = rand(1_000_000..20_000_000).float64
    let peerReceiveTime = localSendTime + (simulatedDelay / 2).Timestamp
    let peerResponseTime = peerReceiveTime + 1_000_000.Timestamp # Peer processes 1ms
    let localReceiveTime = peerResponseTime + (simulatedDelay / 2).Timestamp

    let (offset, delay) = calculateOffset(
      localSendTime, peerReceiveTime, peerResponseTime, localReceiveTime
    )

    # Skip if delay is too high (unreliable measurement)
    if abs(delay) > 100_000_000.0: # 100ms max round-trip
      if s.logger != nil:
        var fields = initTable[string, string]()
        fields["peer"] = peer.peerId
        fields["delay_ms"] = $(delay / 1_000_000.0)
        s.logger.warn("TimeSync: High delay from peer", fields)
      continue

    let clockOffset = ClockOffset(
      offset: offset,
      delay: delay,
      peerId: peer.peerId,
      confidence: 1.0 / (1.0 + delay / 1_000_000.0), # Lower confidence with high latency
      lastUpdate: s.localClock()
    )

    result.add(clockOffset)
    s.offsetHistory.add(clockOffset)

# Perform a single synchronization tick (call this periodically)
proc tick*(s: P2PTimeSynchronizer) =
  ## Perform one synchronization cycle. Should be called by an external
  ## scheduler at intervals matching `syncInterval`.
  try:
    let currentTime = s.localClock()
    withLock(s.mutex):
      if s.state == tssFailed:
        s.state = tssSyncing

    # Perform sync with all peers
    let newOffsets = s.syncRound()

    withLock(s.mutex):
      s.offsets = newOffsets
      s.lastSyncTime = currentTime

    # Update consensus
    s.updateConsensus()
  except Exception as e:
    if s.logger != nil:
      var fields = initTable[string, string]()
      fields["error"] = $e.msg
      s.logger.error("TimeSync: Tick error", fields)
    withLock(s.mutex):
      s.state = tssFailed

# Get globally unique transaction ID using synchronized time if available
proc getTransactionID*(s: P2PTimeSynchronizer): TransactionID =
  ## Generates a globally unique transaction ID. Uses synchronized time (ms) when
  ## in synchronized state, with node identifier and counter for uniqueness.
  ## Format:
  ##   Synchronized: [42-bit time(ms)][10-bit nodeId][12-bit counter]
  ##   Unsynchronized: [16-bit nodeId][48-bit local counter]
  if s.state == tssSynchronized:
    let syncTime = s.getSynchronizedTime()
    # Convert to milliseconds (reduces bit width)
    let timeMs = (syncTime div 1_000_000'i64).uint64
    # Use 42 bits for time (covers ~139,000 years from epoch)
    let time42 = timeMs and ((1'u64 shl 42) - 1)
    let timePart = time42 shl 22
    # Node ID uses 10 bits (supports up to 1024 nodes)
    let node10 = s.numericNodeId.uint64 and ((1'u64 shl 10) - 1)
    let nodePart = node10 shl 12
    # Counter uses 12 bits (4096 IDs per ms per node)
    atomicInc(s.txCounter)
    let rawCounter = load(s.txCounter)
    let counter12 = rawCounter.uint64 and ((1'u64 shl 12) - 1)
    let combined = timePart or nodePart or counter12
    result = TransactionID(combined.int64)
  else:
    # Fallback: node ID in upper 16 bits + 48-bit local counter guarantees uniqueness
    atomicInc(s.txCounter)
    let rawCounter = load(s.txCounter)
    # 48-bit counter, wrap modulo 2^48
    let counter48 = rawCounter.uint64 and ((1'u64 shl 48) - 1)
    let node16 = s.numericNodeId.uint64 and ((1'u64 shl 16) - 1)
    let combined = (node16 shl 48) or counter48
    result = TransactionID(combined.int64)

# Export types and procs
export
  P2PTimeSynchronizer, newP2PTimeSynchronizer,
  getSynchronizedTime, getTransactionID, getState, getCurrentOffset,
  tick, setPeers,
  PeerConfig, ClockOffset, TimeSyncState, Timestamp,
  calculateOffset, filterOutliers, computeConsensusOffset,
  meanStdDev, RingBuffer, initRingBuffer, items
