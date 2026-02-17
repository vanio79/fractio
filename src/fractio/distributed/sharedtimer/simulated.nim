## SimulatedNetworkTransport - deterministic network simulation for testing

import ./networktransport
import ./types
import std/random

type
  SimulatedNetworkTransport* = ref object of NetworkTransport
    rng*: Rand
      ## Random number generator for deterministic simulation.
    avgDelay*: float64
      ## Average round-trip delay in nanoseconds.
    delayVariance*: float64
      ## Maximum deviation from average delay in nanoseconds.
    peerProcessingTime*: Timestamp
      ## Time (nanoseconds) peer takes to process request.

method syncRound*(self: SimulatedNetworkTransport, localSend: Timestamp,
    peers: seq[PeerConfig]): seq[ClockOffset] =
  result = @[]
  for peer in peers:
    # Simulate network delay with random variation
    let simulatedDelay = self.rng.rand(1_000_000..20_000_000).float64
    let peerReceiveTime = localSend + (simulatedDelay / 2).Timestamp
    let peerResponseTime = peerReceiveTime + self.peerProcessingTime
    let localReceiveTime = peerResponseTime + (simulatedDelay / 2).Timestamp

    # NTP offset calculation
    let t1 = localSend.float64
    let t2 = peerReceiveTime.float64
    let t3 = peerResponseTime.float64
    let t4 = localReceiveTime.float64
    let delay = (t4 - t1) - (t3 - t2)
    let offset = ((t2 - t1) + (t3 - t4)) / 2.0

    # Filter out measurements with excessive delay (>100ms)
    if abs(delay) > 100_000_000.0:
      continue

    result.add(ClockOffset(
      offset: offset,
      delay: delay,
      peerId: peer.peerId,
      confidence: 1.0 / (1.0 + delay / 1_000_000.0),
      lastUpdate: localSend
    ))

method close*(self: SimulatedNetworkTransport) =
  discard
