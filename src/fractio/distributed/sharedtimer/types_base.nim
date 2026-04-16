## Shared Timer Types and Constants
## Provides fundamental data structures used by the shared timer system.

import ../../core/types
export Timestamp

type
  ClockOffset* = object
    offset*: float64
      ## Nanoseconds offset from local to peer time
    delay*: float64
      ## Round-trip delay in nanoseconds
    peerId*: string
      ## Identifier of the peer
    confidence*: float64
      ## Confidence based on consistency of measurements
    lastUpdate*: Timestamp
      ## Last time this offset was updated

  PeerConfig* = object
    peerId*: string
      ## Unique peer identifier
    address*: string
      ## Network address
    port*: uint16
      ## Port number
    weight*: float64
      ## Trust weight for consensus (0.0-1.0)
