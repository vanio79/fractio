## Abstract NetworkTransport base class
## Provides network communication abstraction for time synchronization.

import ./types

type
  NetworkTransport* = ref object of RootObj
    ## Base class for network transports.

method syncRound*(self: NetworkTransport, localSend: Timestamp, peers: seq[
    PeerConfig]): seq[ClockOffset] {.base.} =
  ## Perform one synchronization round and return measured offsets.
  result = @[]

method close*(self: NetworkTransport) {.base.} =
  ## Close the transport and free resources.
  discard
