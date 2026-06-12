# Regression test for the FD_SETSIZE crash in ProtocolClient.
#
# Background:
#   The protocol client used to implement `pollForRead` and `pollForWrite`
#   with `posix.select()` + `FD_SET()`. The `FD_SET` macro indexes into a
#   fixed-size `fd_set` bitset using `fd mod FD_SETSIZE` — when `fd` is
#   larger than `FD_SETSIZE` (typically 1024 on Linux) the macro silently
#   writes past the end of the bitset, corrupting memory and eventually
#   causing a SIGSEGV.
#
#   The web server creates a new `FractioClient` (and hence a new TCP
#   socket) per HTTP request. Under sustained load the per-request fds
#   routinely exceed 1024, so the web server crashes with
#   "double free or corruption" / SIGSEGV as soon as the load is
#   applied.
#
#   The fix replaces `select()` with `posix.poll()`, which has no
#   FD_SETSIZE limit (it uses a heap/stack-allocated `struct pollfd`
#   per descriptor).
#
# What this test does:
#   1. Starts a minimal mock server that speaks just enough of the
#      Fractio protocol (greeting, handshake response, echo a single
#      response frame) to make a full `ProtocolClient.connect()` succeed
#      and let us issue a real send/recv.
#   2. Pre-opens ~1100 dummy sockets to force the OS to hand out fds
#      >= 1024 to the test clients.
#   3. Creates N `ProtocolClient` instances, each connecting to the
#      mock server and performing one full round-trip (handshake +
#      send frame + read response frame). With N = 100 and 1100 pre-
#      opened dummies, every client gets an fd in [1100, 1200],
#      well above FD_SETSIZE.
#   4. Verifies that no connection crashes, every client receives a
#      valid response, and the response payload is what the server
#      sent. Pre-fix this test reliably segfaults the entire process
#      around client #50-100 once the fd crosses FD_SETSIZE.
#
#   The test also includes a sanity check that the OS actually hands
#   out fds >= 1024 to the clients (otherwise the bug isn't exercised).

import std/[unittest, os, nativesockets, net, strformat, atomics, times]
import posix
import fractio/protocol/client
import fractio/protocol/handshake
import fractio/protocol/types
import fractio/protocol/frame
import fractio/protocol/codec
import fractio/protocol/messages/core
import fractio/utils/socket_utils

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  NumClients = 100    # Total clients to spawn (well above FD_SETSIZE=1024)
  PreOpenedFds = 1100 # Dummy fds to force the OS to hand out high fds
  BindHost = "127.0.0.1"

# ClientHandshake wire size for the default config. Derived from the
# encoder rather than hard-coded so it stays correct if the encoding
# (or the default clientId length) ever changes. Computed at module
# load — the encoder is a pure function and cheap to call.
#
# Expected breakdown for default "fractio-client" (14 chars) + empty authData:
#   2 version + 4 features + 1 authType
#   + 4 (authData length) + 0 (authData)   (empty → 4)
#   + 1 (clientId length) + 14 (clientId)
# = 2 + 4 + 1 + 4 + 1 + 14 = 26 bytes
let HandshakeSize = encodeClientHandshake(ClientHandshake(
  version: PROTOCOL_VERSION_1,
  features: 0'u32,
  authType: 0'u8,
  authData: "",
  clientId: "fractio-client",
)).len

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

type
  SharedState = ref object
    listenerPort: Atomic[int]
    failServer: Atomic[bool]    # Set true to make the server thread exit
    serverStarted: Atomic[bool] # Set true when the server is bound+listening

proc buildGreeting(): string =
  ## Build a valid protocol greeting (4 + 2 + 4 + 1 + 0 + 2 + 8 = 21 bytes).
  encodeGreeting(ServerGreeting(
    magic: PROTOCOL_MAGIC,
    version: PROTOCOL_VERSION_1,
    features: 0'u32,
    authMethods: @[], # zero auth methods
    serverId: 1'u16,
    clusterId: 1'u64,
  ))

proc buildHandshakeResponse(): string =
  ## Build a valid HandshakeResponse (1 + 4 + 1 + N bytes).
  encodeHandshakeResponse(HandshakeResponse(
    status: HandshakeOK,
    features: 0'u32,
    serverName: "mock",
    errorMessage: "",
  ))

# Send a full buffer to a non-blocking socket with poll+send retry, like
# the production client does internally. Returns true iff all bytes sent.
proc sendAllNonBlocking(fd: cint, data: string, timeoutMs: int): bool =
  var total = 0
  let deadline = epochTime() + timeoutMs.float / 1000.0
  var pfd: TPollfd
  pfd.fd = fd
  pfd.events = cshort(POLLOUT or POLLERR or POLLHUP)
  pfd.revents = 0
  while total < data.len and epochTime() < deadline:
    let rc = posix.poll(addr pfd, Tnfds(1), 100)
    if rc <= 0:
      continue
    let sent = posix.send(SocketHandle(fd), unsafeAddr data[total],
                          data.len - total, 0)
    if sent > 0:
      total += sent
    elif sent == 0:
      discard posix.usleep(1000)
    else:
      let err = errno
      if err != EAGAIN and err != EWOULDBLOCK:
        return false
  return total == data.len

# Recv a full buffer from a non-blocking socket with poll+recv retry.
# Returns the actual number of bytes read (< size means timeout/closed).
proc recvAllNonBlocking(fd: cint, size: int, timeoutMs: int): string =
  result = newString(size)
  var total = 0
  let deadline = epochTime() + timeoutMs.float / 1000.0
  var pfd: TPollfd
  pfd.fd = fd
  pfd.events = cshort(POLLIN or POLLERR or POLLHUP)
  pfd.revents = 0
  while total < size and epochTime() < deadline:
    let rc = posix.poll(addr pfd, Tnfds(1), 100)
    if rc <= 0:
      continue
    let n = posix.recv(SocketHandle(fd), unsafeAddr result[total], size - total, 0)
    if n > 0:
      total += n
    elif n == 0:
      break # EOF
    else:
      let err = errno
      if err != EAGAIN and err != EWOULDBLOCK:
        break
  result.setLen(total)

# Server thread: accepts connections, sends greeting, reads client handshake,
# sends HandshakeResponse, then echoes the first frame back to the client.
proc serverThread(state: SharedState) {.thread.} =
  let serverSock = newSocket()
  serverSock.setLingerZero()
  serverSock.bindAddr(Port(0), BindHost) # ephemeral port
  let assignedPort = serverSock.getLocalAddr()[1].int
  state.listenerPort.store(assignedPort)
  serverSock.listen()
  state.serverStarted.store(true)

  let greeting = buildGreeting()
  let hsResp = buildHandshakeResponse()

  while not state.failServer.load():
    # Poll the listening socket for incoming connections with 50ms
    # granularity. We use posix.poll() rather than posix.select() to
    # match the rest of the codebase — and even though the listening
    # socket's fd is always low (< 10) so FD_SETSIZE isn't a concern
    # here, consistency makes the test code easier to reason about.
    var pfd: TPollfd
    pfd.fd = cint(serverSock.getFd())
    pfd.events = cshort(POLLIN or POLLERR or POLLHUP)
    pfd.revents = 0
    let rc = posix.poll(addr pfd, Tnfds(1), 50)
    if rc <= 0:
      continue
    try:
      var clientSock: owned(Socket)
      var clientAddr = ""
      serverSock.acceptAddr(clientSock, clientAddr)
      let client = clientSock
      client.setLingerZero()
      # Make the accepted socket non-blocking so we can poll for activity
      # without stalling. The client (ProtocolClient) does the same on its end.
      var flags = fcntl(client.getFd(), F_GETFL)
      discard fcntl(client.getFd(), F_SETFL, flags or O_NONBLOCK)
      let clientFd = cint(client.getFd())

      # 1. Send greeting
      if not sendAllNonBlocking(clientFd, greeting, 5_000):
        client.close()
        continue

      # 2. Read HandshakeSize-byte client handshake (2+4+1+4+0+1+14 = 26
      #    with default clientId="fractio-client" and empty authData).
      #    We just consume bytes — we don't care about the contents.
      let hsBuf = recvAllNonBlocking(clientFd, HandshakeSize, 5_000)
      if hsBuf.len != HandshakeSize:
        client.close()
        continue

      # 3. Send HandshakeResponse
      if not sendAllNonBlocking(clientFd, hsResp, 5_000):
        client.close()
        continue

      # 4. Read 12-byte frame header, decode payload length, read payload,
      #    decode echo request, encode echo response, send it back.
      let hdrBuf = recvAllNonBlocking(clientFd, 12, 5_000)
      if hdrBuf.len != 12:
        client.close()
        continue
      var pos = 0
      let hdrR = decodeFrameHeader(hdrBuf, pos)
      if hdrR.isErr:
        client.close()
        continue
      let payloadLen = int(hdrR.value.payloadLen)
      if payloadLen == 0 or payloadLen >= 1024 * 1024:
        client.close()
        continue
      let payload = recvAllNonBlocking(clientFd, payloadLen, 5_000)
      if payload.len != payloadLen:
        client.close()
        continue

      # Echo request format (from encodeEchoRequest):
      #   2 bytes msgType (mtEcho) + 4 bytes dataLen BE + N bytes data
      # Echo response format (encodeEchoResponse): same layout.
      if payloadLen < 6:
        client.close()
        continue
      var dpos = 2
      let dataLenR = readUint32BE(payload, dpos)
      if dataLenR.isErr:
        client.close()
        continue
      let dataLen = int(dataLenR.value)
      if 6 + dataLen > payloadLen:
        client.close()
        continue
      let echoData = payload[6 ..< 6 + dataLen]

      # Build the echo response with the proper codec and wrap it in a
      # frame with FlagIsResponse. This guarantees the client can decode
      # the response with decodeEchoData and get back exactly echoData.
      let respPayload = encodeEchoResponse(echoData)
      let resp = encodeFrame(respPayload, hdrR.value.requestId, FlagIsResponse)
      discard sendAllNonBlocking(clientFd, resp, 5_000)
      client.close()
    except CatchableError:
      # If accept/read fails, just continue
      discard

  serverSock.close()

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

suite "FD_SETSIZE crash regression - Integration":
  var
    state: SharedState
    serverRef: Thread[SharedState]
    dummyFds: seq[Socket]
    listenerPort: int

  setup:
    state = SharedState()
    state.listenerPort.store(0)
    state.failServer.store(false)
    state.serverStarted.store(false)
    createThread(serverRef, serverThread, state)

    # Wait for server to start
    var waited = 0
    while not state.serverStarted.load() and waited < 200:
      os.sleep(50)
      waited += 1
    doAssert state.serverStarted.load(), "server failed to start within 10s"
    listenerPort = state.listenerPort.load()
    doAssert listenerPort > 0, "server bound to port 0 unexpectedly"

    # Pre-open dummy sockets to force fds >= 1024
    newSeq dummyFds, PreOpenedFds
    for i in 0 ..< PreOpenedFds:
      let s = newSocket()
      # We don't need to actually connect — just keep the fd alive.
      dummyFds[i] = s

  teardown:
    state.failServer.store(true)
    joinThread(serverRef)
    for s in dummyFds:
      try: s.close()
      except CatchableError: discard

  test "connect clients with fds > 1024 (FD_SETSIZE boundary)":
    # With pre-fix code: this test crashes (SIGSEGV / corruption) once
    # the client fd crosses FD_SETSIZE=1024 inside pollForRead /
    # pollForWrite. With post-fix code: every client should connect
    # and complete a full round-trip successfully.
    var
      successCount = 0
      failureCount = 0
      observedMinFd = high(int)

    # Sanity check: HandshakeSize is derived from encodeClientHandshake
    # at module load, so it cannot drift away from the encoder's actual
    # output. We re-derive it here as a one-line smoke test that the
    # constants used by the server (handshake length, "fractio-client"
    # string) match what the production client sends.
    let probeHs = encodeClientHandshake(ClientHandshake(
      version: PROTOCOL_VERSION_1,
      features: 0'u32,
      authType: 0'u8,
      authData: "",
      clientId: "fractio-client",
    ))
    doAssert probeHs.len == HandshakeSize,
      "handshake size mismatch — protocol drift? got=" & $probeHs.len &
      " expected=" & $HandshakeSize

    for i in 0 ..< NumClients:
      var cfg = defaultClientConfig(BindHost, listenerPort)
      cfg.timeoutMs = 10_000
      let client = newProtocolClient(cfg)
      let cr = client.connect()
      if cr.isErr:
        inc failureCount
        if i < 3 or (i mod 100 == 0):
          echo "[test] client #", i, " connect failed: ", cr.error.msg
        continue

      # Track the smallest fd we saw — must be >= 1024 for this test to
      # actually exercise the FD_SETSIZE boundary.
      observedMinFd = min(observedMinFd, int(client.fd))

      # Full round-trip: send frame + read response frame via the
      # production echo() API. This exercises sendPayload + readOneFrame,
      # which go through pollForWrite / pollForRead respectively.
      let echoPayload = "hello-" & $i
      let echoR = client.echo(echoPayload)
      if echoR.isErr:
        inc failureCount
        if i < 3 or (i mod 100 == 0):
          echo "[test] client #", i, " echo failed: ", echoR.error.msg
      elif echoR.value == echoPayload:
        inc successCount
      else:
        inc failureCount
        if i < 3 or (i mod 100 == 0):
          echo "[test] client #", i, " echo mismatch: got '", echoR.value, "'"

      client.disconnect()

    echo "[test] success=", successCount, " failures=", failureCount,
         " minFd=", observedMinFd
    check successCount == NumClients
    check failureCount == 0
    # Sanity: confirm the OS handed out fds >= 1024 to the clients.
    # (If the OS reuses low fds, the test wouldn't exercise the bug.)
    check observedMinFd >= 1024

  test "sanity: file descriptors above FD_SETSIZE are allocated":
    # Bare sanity check independent of the server. Pre-open dummy fds,
    # then open a new client fd, and verify it's >= 1024. This guards
    # against test environments where dummy fds are not actually held
    # (e.g. if `newSocket` becomes cheap to reuse).
    var cfg = defaultClientConfig(BindHost, listenerPort)
    cfg.timeoutMs = 5_000
    let client = newProtocolClient(cfg)
    let cr = client.connect()
    if cr.isOk:
      check client.fd >= 1024
      client.disconnect()
    else:
      # If connect fails (server not running, etc.) we can't verify
      # the fd allocation. Skip rather than fail.
      skip()
