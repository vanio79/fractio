# UDP Transport for P2P Time Synchronization
# Production-ready with mixed byte ordering support via BOM.

import std/[net, typedthreads, os, tables, nativesockets]
import locks
import std/atomics

import ./networktransport
import ./types
import ./timeprovider
import ./monotonic
import ../../utils/logging
import ../../network/packetcodec

type
  SyncTimeout* = ref object of CatchableError

const
  DEFAULT_REQUEST_TIMEOUT_MS* = 100 # milliseconds
  MAX_PACKET_SIZE* = 65507          # Max UDP payload

type
  UDPTransport* = ref object of NetworkTransport
    ## UDP-based network transport with automatic endianness detection.
    ## Thread-safe: multiple syncRound calls are serialized by internal mutex.
    serverSocket*: Socket
    serverThread*: Thread[UDPTransport]
    serverRunning*: Atomic[bool]
    localPort*: uint16
    logger*: Logger
    timeProvider*: TimeProvider
    mutex*: Lock ## Serializes syncRound calls
    stats*: tuple[sent: Atomic[int], received: Atomic[int], errors: Atomic[int]]
    codec*: PacketCodec

proc newUDPTransport*(port: uint16 = 123'u16, logger: Logger = nil,
                      timeProvider: TimeProvider = MonotonicTimeProvider()): UDPTransport =
  ## Create a new UDP transport bound to the given port.
  ## If port is 0, an ephemeral port is chosen.
  let log = if logger.isNil: newLogger("UDPTransport") else: logger
  result = UDPTransport(
    localPort: port,
    logger: log,
    timeProvider: timeProvider,
    serverRunning: Atomic[bool](),
    mutex: Lock(),
    stats: (sent: Atomic[int](), received: Atomic[int](), errors: Atomic[int]())
  )
  initLock(result.mutex)
  store(result.serverRunning, false)
  result.codec = newPacketCodec()

proc serverMain*(transport: UDPTransport) {.thread, gcsafe.} =
  ## Main loop for the UDP server thread.
  var startFields = initTable[string, string]()
  startFields["port"] = $transport.localPort
  transport.logger.debug("UDPTransport server thread started", startFields)

  var buffer = newString(MAX_PACKET_SIZE)
  var clientAddr: string
  var clientPort: Port

  while load(transport.serverRunning, moRelaxed):
    try:
      # Use select to avoid permanent block and allow graceful shutdown
      var readfds = @[getFd(transport.serverSocket)]
      let ready = selectRead(readfds, 100) # 100ms poll
      if ready <= 0:
        # Timeout or error; just loop again to check serverRunning
        continue

      # Socket is ready, receive the request
      let n = transport.serverSocket.recvFrom(buffer, MAX_PACKET_SIZE,
          clientAddr, clientPort)
      if n <= 0:
        continue

      # Convert string (char seq) to uint8 seq for decoding
      var bytes = newSeq[uint8](n)
      for i in 0..<n:
        bytes[i] = uint8(buffer[i])

      try:
        let (bom, req) = transport.codec.decodeRequest(bytes)
        discard req.t1 # Not used; only BOM needed for response encoding
        let t2 = transport.timeProvider.now()
        let t3 = transport.timeProvider.now()
        let response = transport.codec.encodeResponse(t2, t3, bom)
        # Send using low-level sendto to avoid overload ambiguity
        let clientIp = parseIpAddress(clientAddr)
        var sa: Sockaddr_storage
        var sl: SockLen
        toSockAddr(clientIp, clientPort, sa, sl)
        let fd = getFd(transport.serverSocket)
        discard sendto(fd, cast[pointer](addr(response[0])), response.len.cint,
                       0'i32, cast[ptr SockAddr](addr(sa)), sl)
        discard fetchAdd(transport.stats.received, 1, moRelaxed)
        discard fetchAdd(transport.stats.sent, 1, moRelaxed)
      except ValueError:
        var invalidFields = initTable[string, string]()
        invalidFields["clientAddr"] = clientAddr
        invalidFields["clientPort"] = $clientPort
        invalidFields["error"] = getCurrentExceptionMsg()
        transport.logger.debug("Invalid request", invalidFields)
      except:
        var errorFields = initTable[string, string]()
        errorFields["clientAddr"] = clientAddr
        errorFields["clientPort"] = $clientPort
        errorFields["error"] = getCurrentExceptionMsg()
        transport.logger.error("Server error", errorFields)
        # Break the loop on any other error (including socket errors)
        break
    except:
      if load(transport.serverRunning, moRelaxed):
        var loopErrorFields = initTable[string, string]()
        loopErrorFields["error"] = getCurrentExceptionMsg()
        transport.logger.error("Server loop error", loopErrorFields)
      break

  transport.logger.debug("UDPTransport server thread exiting")

method start*(self: UDPTransport) =
  ## Start the UDP server.
  if load(self.serverRunning, moRelaxed):
    raise newException(ValueError, "Transport already started")

  # Create and bind server socket
  self.serverSocket = newSocket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)
  self.serverSocket.setSockOpt(OptReuseAddr, true)
  try:
    self.serverSocket.bindAddr(Port(self.localPort))
  except:
    self.serverSocket.close()
    raise
  let (_, actualPort) = self.serverSocket.getLocalAddr()
  self.localPort = uint16(actualPort)

  # Start server thread
  createThread(self.serverThread, serverMain, self)
  store(self.serverRunning, true)

  var fields = initTable[string, string]()
  fields["port"] = $self.localPort
  self.logger.info("UDPTransport started", fields)

method syncRound*(self: UDPTransport, localSend: Timestamp, peers: seq[
    PeerConfig]): seq[ClockOffset] =
  ## Perform one synchronization round with the given peers.
  ## Sends a sync request to each peer and collects offsets.
  ## Thread-safe: multiple calls are serialized by internal mutex.
  result = @[]

  withLock self.mutex:
    if self.serverSocket.isNil or not load(self.serverRunning, moRelaxed):
      raise newException(IOError, "Transport not started or already closed")

    var anyResponse = false

    for peer in peers:
      var sock: Socket
      try:
        sock = newSocket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)

        # Prepare and send request using low-level sendto to avoid overload ambiguity
        let request = self.codec.encodeRequest(localSend)
        let fd = getFd(sock)
        let peerIp = parseIpAddress(peer.address)
        var sa: Sockaddr_storage
        var sl: SockLen
        toSockAddr(peerIp, Port(peer.port), sa, sl)
        discard sendto(fd, cast[pointer](addr(request[0])), request.len.cint, 0'i32,
                      cast[ptr SockAddr](addr(sa)), sl)
        discard fetchAdd(self.stats.sent, 1, moRelaxed)

        # Wait for response with timeout using selectRead
        var readfds = @[getFd(sock)]
        let ready = selectRead(readfds, DEFAULT_REQUEST_TIMEOUT_MS)
        if ready <= 0:
          if ready == 0:
            raise SyncTimeout(msg: "recv timeout")
          else:
            raiseOsError(osLastError())

        # Receive response
        var buffer: array[RESPONSE_PACKET_SIZE, uint8]
        let n = recv(fd, addr(buffer[0]), RESPONSE_PACKET_SIZE, 0'i32)
        if n != RESPONSE_PACKET_SIZE:
          var warnFields = initTable[string, string]()
          warnFields["peer"] = peer.peerId
          warnFields["size"] = $n
          self.logger.warn("Invalid response size", warnFields)
          discard fetchAdd(self.stats.errors, 1, moRelaxed)
          continue

        let (_, resp) = self.codec.decodeResponse(buffer[0..n-1])
        anyResponse = true
        let t4 = self.timeProvider.now()

        # Compute NTP offset and delay
        let t1f = localSend.float64
        let t2f = resp.t2.float64
        let t3f = resp.t3.float64
        let t4f = t4.float64
        let delay = (t4f - t1f) - (t3f - t2f)
        let offset = ((t2f - t1f) + (t3f - t4f)) / 2.0

        # Filter high-delay measurements (>100ms)
        if abs(delay) > 100_000_000.0:
          var debugFields = initTable[string, string]()
          debugFields["peer"] = peer.peerId
          debugFields["delay"] = $delay
          self.logger.debug("Peer delay too high", debugFields)
          continue

        discard fetchAdd(self.stats.received, 1, moRelaxed)
        result.add(ClockOffset(
          offset: offset,
          delay: delay,
          peerId: peer.peerId,
          confidence: 1.0 / (1.0 + delay / 1_000_000.0),
          lastUpdate: localSend
        ))

      except SyncTimeout:
        var timeoutFields = initTable[string, string]()
        timeoutFields["peer"] = peer.peerId
        self.logger.debug("Sync request timeout", timeoutFields)
        discard fetchAdd(self.stats.errors, 1, moRelaxed)
      except:
        var errFields = initTable[string, string]()
        errFields["peer"] = peer.peerId
        errFields["error"] = getCurrentExceptionMsg()
        self.logger.error("Error syncing with peer", errFields)
        discard fetchAdd(self.stats.errors, 1, moRelaxed)
      finally:
        if not sock.isNil:
          try: sock.close() except: discard

    # After processing all peers, if no valid offsets were collected and no peer responded at all, raise SyncTimeout
    if result.len == 0 and not anyResponse:
      raise SyncTimeout(msg: "No peers responded in sync round")

method getStats*(self: UDPTransport): tuple[sent: int, received: int,
    errors: int] {.base.} =
  ## Get current statistics (snapshot).
  result = (
    sent: load(self.stats.sent, moRelaxed),
    received: load(self.stats.received, moRelaxed),
    errors: load(self.stats.errors, moRelaxed)
  )

method close*(self: UDPTransport) =
  ## Stop the server and close all resources.
  if not load(self.serverRunning, moRelaxed):
    return # Already closed

  # Signal server thread to exit
  store(self.serverRunning, false)

  # Close server socket to unblock recv
  if not self.serverSocket.isNil:
    try:
      self.serverSocket.close()
    except:
      discard

  # Wait for server thread (must exist if serverRunning was true)
  joinThread(self.serverThread)

  let stats = self.getStats()
  var closeFields = initTable[string, string]()
  closeFields["sent"] = $stats.sent
  closeFields["received"] = $stats.received
  closeFields["errors"] = $stats.errors
  self.logger.info("UDPTransport closed", closeFields)
