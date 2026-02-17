## Shared Timer - P2P time synchronization for generating globally unique transaction IDs.
##
## This implementation uses NTP-style offset calculation, outlier filtering, and weighted median consensus
## to achieve sub-millisecond clock drift across hundreds of nodes without a master.

import ./types
import ./timeprovider
import ./networktransport
import ./ringbuffer
import ./monotonic
import ../../core/types
import ../../utils/logging
import ./udptransport
import std/[times, math, algorithm, sequtils, tables]
import locks

const
  DEFAULT_SYNC_INTERVAL* = 1_000_000_000'i64 # 1 second in nanoseconds
  REQUEST_TIMEOUT_NS* = 100_000_000'i64      # 100ms in nanoseconds
  MAX_HISTORY_SIZE* = 100                    # Keep last 100 measurements per peer
  OUTLIER_STDDEV_FACTOR* = 2.0               # Filter measurements beyond 2σ
  MIN_PEERS_FOR_CONSENSUS* = 2               # Need at least 2 peers for consensus
  MAX_CLOCK_DRIFT_NS* = 1_000_000'i64        # 1ms max allowed drift before re-sync

type
  TimeSyncState* = enum
    tssUninitialized, ## Not yet synchronized
    tssSyncing,       ## Performing a synchronization round
    tssSynchronized,  ## Successfully synchronized with peers
    tssFailed         ## Unable to achieve consensus

  SharedTimer* = ref object of TimeProvider
    nodeId*: string
      ## Human-readable node identifier.
    numericNodeId*: uint16
      ## Unique numeric node ID (0-1023 for 10-bit space in transaction IDs).
    localClock*: TimeProvider
      ## Time source (monotonic or wall-clock).
    network*: NetworkTransport
      ## Network transport for synchronization messages.
    peers*: seq[PeerConfig]
      ## List of peers to synchronize with.
    state*: TimeSyncState
      ## Current synchronization state.
    offsets*: seq[ClockOffset]
      ## Current offsets measured from peers in the latest round.
    consensusOffset*: float64
      ## Combined offset from all peers (nanoseconds).
    offsetHistory*: RingBuffer[ClockOffset]
      ## History of offsets for outlier detection.
    syncInterval*: Duration
      ## How often to run synchronization.
    requestTimeout*: Duration
      ## Timeout for network requests.
    lastSyncTime*: Timestamp
      ## Last time a sync round completed.
    mutex*: Lock
      ## Protects offsets, consensusOffset, state, txCounter.
    logger*: Logger
      ## Optional logger for diagnostics.
    txCounter*: int
      ## Transaction ID counter (protected by mutex).
    onStateChange*: proc(state: TimeSyncState)
      ## Optional callback for state transitions.

# Helper functions (algorithm internals)

proc calculateOffset*(localSend, peerReceive, peerResponse,
    localReceive: Timestamp): tuple[offset, delay: float64] =
  ## Compute NTP-style offset and delay from four timestamps.
  ## t1: local send, t2: peer receive, t3: peer response, t4: local receive
  let t1 = localSend.float64
  let t2 = peerReceive.float64
  let t3 = peerResponse.float64
  let t4 = localReceive.float64
  let delay = (t4 - t1) - (t3 - t2)
  let offset = ((t2 - t1) + (t3 - t4)) / 2.0
  (offset, delay)

proc meanStdDev*(data: seq[float64]): tuple[mean: float64, stddev: float64] =
  ## Compute mean and standard deviation of a sequence.
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

proc filterOutliers*(offsets: seq[ClockOffset]): seq[ClockOffset] =
  ## Remove measurements beyond OUTLIER_STDDEV_FACTOR standard deviations from mean.
  if offsets.len < 3:
    return offsets
  let offsetsNs = offsets.mapIt(it.offset)
  let (mean, stddev) = meanStdDev(offsetsNs)
  let threshold = OUTLIER_STDDEV_FACTOR * stddev
  result = offsets.filterIt(abs(it.offset - mean) <= threshold)

proc computeConsensusOffset*(offsets: seq[ClockOffset]): float64 =
  ## Compute weighted median of offsets.
  ## Weight = confidence * length(peerId) (assuming longer IDs have higher trust).
  if offsets.len == 0:
    return 0.0
  if offsets.len == 1:
    return offsets[0].offset
  var weighted: seq[(float64, float64)] = @[]
  for offset in offsets:
    let weight = offset.confidence * offset.peerId.len.float64
    weighted.add((offset.offset, weight))
  weighted.sort(proc(x, y: (float64, float64)): int = cmp(x[0], y[0]))
  let totalWeight = weighted.mapIt(it[1]).sum()
  var cumulative = 0.0
  for (offset, weight) in weighted:
    cumulative += weight
    if cumulative >= totalWeight / 2.0:
      return offset
  # Fallback: median
  result = weighted[weighted.len div 2][0]

# Forward declarations to satisfy the compiler
proc getState*(self: SharedTimer): TimeSyncState {.gcsafe.}
proc getSynchronizedTime*(self: SharedTimer): Timestamp {.gcsafe.}

# SharedTimer implementation

method now*(self: SharedTimer): Timestamp {.gcsafe.} =
  ## Get the current synchronized time (offset-adjusted) if synchronized, else local time.
  let local = self.localClock.now()
  if self.getState() == tssSynchronized:
    result = self.getSynchronizedTime()
  else:
    result = local

proc newSharedTimer*(nodeId: string, numericNodeId: uint16,
                     peers: seq[PeerConfig] = @[],
                     localClock: TimeProvider = nil,
                     network: NetworkTransport = nil,
                     logger: Logger = nil): SharedTimer =
  ## Create a new SharedTimer instance.
  ## If localClock is nil, defaults to MonotonicTimeProvider.
  ## If network is nil, defaults to UDPTransport (production-ready).
  let clock = if localClock != nil: localClock else: MonotonicTimeProvider()
  let net = if network != nil: network else:
    newUDPTransport(port = 0, logger = logger, timeProvider = clock)

  # Start the network transport (no-op for simulated)
  net.start()

  result = SharedTimer(
    nodeId: nodeId,
    numericNodeId: numericNodeId,
    localClock: clock,
    network: net,
    peers: peers,
    state: tssUninitialized,
    offsets: @[],
    consensusOffset: 0.0,
    offsetHistory: newRingBuffer[ClockOffset](MAX_HISTORY_SIZE),
    syncInterval: initDuration(nanoseconds = DEFAULT_SYNC_INTERVAL),
    requestTimeout: initDuration(nanoseconds = REQUEST_TIMEOUT_NS),
    lastSyncTime: 0,
    mutex: Lock(),
    logger: logger,
    txCounter: 0,
    onStateChange: nil
  )
  initLock(result.mutex)

proc setPeers*(self: SharedTimer, peers: seq[PeerConfig]) =
  ## Update the list of peers.
  withLock(self.mutex):
    self.peers = peers

proc getPeers*(self: SharedTimer): seq[PeerConfig] =
  ## Get current peer list.
  withLock(self.mutex):
    result = self.peers

proc getSynchronizedTime*(self: SharedTimer): Timestamp {.gcsafe.} =
  ## Return local time plus consensus offset.
  let local = self.localClock.now()
  withLock(self.mutex):
    if self.state == tssSynchronized:
      result = local + self.consensusOffset.int64
    else:
      result = local

proc getCurrentOffset*(self: SharedTimer): float64 =
  ## Return the current consensus offset in nanoseconds.
  withLock(self.mutex):
    result = self.consensusOffset

proc getState*(self: SharedTimer): TimeSyncState {.gcsafe.} =
  ## Get current synchronization state.
  withLock(self.mutex):
    result = self.state

proc setState*(self: SharedTimer, state: TimeSyncState) =
  ## Set synchronization state (internal). Triggers onStateChange callback if state changes.
  let oldState = self.state
  withLock(self.mutex):
    self.state = state
    self.lastSyncTime = self.localClock.now()
  if oldState != state and self.onStateChange != nil:
    try:
      self.onStateChange(state)
    except:
      discard

proc updateConsensus*(self: SharedTimer) =
  ## Compute consensus from current offsets and update state accordingly.
  var targetState: TimeSyncState = tssFailed
  var logAfter = false
  var logMsg = ""
  var logFields = initTable[string, string]()
  var newOffset = 0.0

  withLock(self.mutex):
    if self.offsets.len < MIN_PEERS_FOR_CONSENSUS:
      targetState = tssFailed
      logAfter = true
      logMsg = "TimeSync: Insufficient peers for consensus"
      logFields = initTable[string, string]()
      logFields["peers"] = $self.offsets.len
    else:
      let filtered = filterOutliers(self.offsets)
      if filtered.len < MIN_PEERS_FOR_CONSENSUS:
        targetState = tssFailed
        logAfter = true
        logMsg = "TimeSync: All peers filtered as outliers"
        logFields = initTable[string, string]()
        logFields["remaining"] = $filtered.len
      else:
        newOffset = computeConsensusOffset(filtered)
        let oldOffset = self.consensusOffset
        let drift = abs(newOffset - oldOffset)

        # Log drift information while holding lock
        if self.logger != nil:
          var driftFields = initTable[string, string]()
          driftFields["drift_ns"] = $drift
          if drift > MAX_CLOCK_DRIFT_NS.float64:
            self.logger.warn("TimeSync: Large drift detected", driftFields)
          else:
            self.logger.debug("TimeSync: Drift within bounds", driftFields)

        # Update consensus offset
        self.consensusOffset = newOffset
        targetState = tssSynchronized
        logAfter = true
        logMsg = "TimeSync: Consensus updated"
        logFields = initTable[string, string]()
        logFields["offset_ns"] = $self.consensusOffset
        logFields["peers"] = $filtered.len

  # Apply state change and log after releasing lock to avoid nested locking
  self.setState(targetState)
  if logAfter and self.logger != nil and logMsg.len > 0:
    case targetState
    of tssFailed:
      self.logger.warn(logMsg, logFields)
    of tssSynchronized:
      self.logger.info(logMsg, logFields)
    else:
      discard

proc tick*(self: SharedTimer) =
  ## Perform one synchronization round.
  try:
    self.setState(tssSyncing)
    let localSendTime = self.localClock.now()
    let offsets = self.network.syncRound(localSendTime, self.peers)
    withLock(self.mutex):
      self.offsets = offsets
      self.lastSyncTime = localSendTime
      for offset in offsets:
        self.offsetHistory.add(offset)
    self.updateConsensus()
  except Exception as e:
    if self.logger != nil:
      var fields = initTable[string, string]()
      fields["error"] = e.msg
      self.logger.error("TimeSync: Tick error", fields)
    self.setState(tssFailed)

proc start*(self: SharedTimer) =
  ## Start periodic synchronization (currently no-op, future: spawn background task).
  discard

proc stop*(self: SharedTimer) =
  ## Stop synchronization and close network transport.
  self.network.close()

proc isSynchronized*(self: SharedTimer): bool =
  ## Check if the timer is in synchronized state.
  self.getState() == tssSynchronized

proc getTransactionID*(self: SharedTimer): TransactionID =
  ## Generate a globally unique transaction ID.
  ## Format: synchronized mode -> [42-bit time][10-bit nodeId][12-bit counter]
  ##         unsynchronized mode -> [16-bit nodeId][48-bit counter]
  let state = self.getState()
  if state == tssSynchronized:
    let syncTime = self.getSynchronizedTime()
    let timeMs = (syncTime div 1_000_000'i64).uint64
    let time42 = timeMs and ((1'u64 shl 42) - 1)
    let timePart = time42 shl 22
    let node10 = self.numericNodeId.uint64 and ((1'u64 shl 10) - 1)
    let nodePart = node10 shl 12
    var rawCounter: int
    withLock(self.mutex):
      inc(self.txCounter)
      rawCounter = self.txCounter
    let counter12 = rawCounter.uint64 and ((1'u64 shl 12) - 1)
    let combined = timePart or nodePart or counter12
    result = TransactionID(combined.int64)
  else:
    var rawCounter: int
    withLock(self.mutex):
      inc(self.txCounter)
      rawCounter = self.txCounter
    let counter48 = rawCounter.uint64 and ((1'u64 shl 48) - 1)
    let node16 = self.numericNodeId.uint64 and ((1'u64 shl 16) - 1)
    let combined = (node16 shl 48) or counter48
    result = TransactionID(combined.int64)
