## Abstract NetworkTransport base class
## Provides network communication abstraction for time synchronization.

import ./types

type
  NetworkTransport* = ref object of RootObj
    ## Base class for network transports.

method start*(self: NetworkTransport) {.base, gcsafe.} =
  ## Start the transport (if needed). Default implementation does nothing.
  discard

method syncRound*(self: NetworkTransport, localSend: Timestamp, peers: seq[
    PeerConfig]): seq[ClockOffset] {.base, gcsafe.} =
  ## Perform one synchronization round and return measured offsets.
  result = @[]

method close*(self: NetworkTransport) {.base, gcsafe.} =
  ## Close the transport and free resources.
  discard
