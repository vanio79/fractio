# Socket utility functions for Fractio
# Provides non-blocking socket operations with poll() polling.
#
# Why poll() and not select():
#   select(2) uses a fixed-size bitmask (fd_set) with a hard limit of
#   FD_SETSIZE (typically 1024) descriptors. On Linux, once any fd
#   passed to select() is >= FD_SETSIZE, the kernel aborts the process
#   with SIGABRT ("bit out of range 0 - FD_SETSIZE on fd_set"). This
#   crashed the server under sustained load when many client conns
#   pushed fds past 1024 during stream sends.
#
#   poll(2) takes an array of pollfd structures, so it scales with
#   whatever array we allocate — there is no FD_SETSIZE cap.
#   Behaviourally it is a drop-in replacement for the single-fd cases
#   we use here (read and write readiness with a timeout).
#
# See: protocol/client.nim pollForRead/pollForWrite for the original
# poll()-based implementation we mirror here.

import std/net
import std/posix
import std/nativesockets

# =============================================================================
# Socket Options Helpers
# =============================================================================

proc setLingerZero*(sock: Socket) =
  ## Sets SO_LINGER with a zero timeout on the socket.
  ## This causes the socket to be closed immediately with a RST
  ## instead of a normal FIN handshake, avoiding the TIME_WAIT state.
  var ling: TLinger
  ling.l_onoff = 1
  ling.l_linger = 0

  if setsockopt(sock.getFd(), SOL_SOCKET, SO_LINGER, addr ling, sizeof(
      ling).SockLen) < 0:
    # We don't raise here to avoid crashing on socket option failures,
    # but in a real system we might want to log this.
    discard

proc setReuseAddr*(sock: Socket, enabled: bool = true) =
  ## Sets SO_REUSEADDR on the socket.
  sock.setSockOpt(OptReuseAddr, enabled)

# =============================================================================
# Non-blocking socket helpers (fcntl)
# =============================================================================

proc setSocketNonBlocking*(fd: cint): bool {.gcsafe, raises: [].} =
  ## Set socket to non-blocking mode using fcntl.
  ## Returns true if successful.
  let flags = fcntl(fd, F_GETFL)
  if flags == -1:
    return false
  let rc = fcntl(fd, F_SETFL, flags or O_NONBLOCK)
  rc != -1

proc setSocketBlocking*(fd: cint): bool {.gcsafe, raises: [].} =
  ## Set socket to blocking mode using fcntl.
  ## Returns true if successful.
  let flags = fcntl(fd, F_GETFL)
  if flags == -1:
    return false
  let rc = fcntl(fd, F_SETFL, flags and (not O_NONBLOCK))
  rc != -1

# =============================================================================
# poll() helpers with timeout (replaces the old select()+FD_SET versions)
# =============================================================================
#
# Semantics (unchanged from the previous select()-based API):
#   - timeoutMs > 0   -> wait at most timeoutMs milliseconds
#   - timeoutMs <= 0  -> block indefinitely (mapped to poll() with -1)
#   - EINTR is retried in a tight loop so callers don't have to
#   - On timeout (poll() returns 0) or error (poll() returns -1) we
#     return false; on ready (poll() returns 1) we return true iff
#     the relevant event bit is set in revents. POLLERR and POLLHUP
#     are always treated as "ready" because they signal a closed or
#     failing socket that the caller must observe.

proc pollForRead*(fd: cint, timeoutMs: int): bool {.gcsafe, raises: [].} =
  ## Poll socket for read readiness using poll() with timeout.
  ## Returns true if data is available (or POLLERR/POLLHUP), false on
  ## timeout or error. Safe for any fd value (no FD_SETSIZE limit).
  let waitMs: cint = if timeoutMs <= 0: -1 else: timeoutMs.cint
  var pfd: TPollfd
  pfd.fd = fd
  pfd.events = cshort(POLLIN or POLLERR or POLLHUP)
  pfd.revents = 0

  var attempts = 0
  const maxAttempts = 16
  while true:
    let rc = posix.poll(addr pfd, Tnfds(1), waitMs)
    if rc > 0:
      # Ready. revents may include POLLERR/POLLHUP — treat as "woken".
      return (pfd.revents and cshort(POLLIN or POLLERR or POLLHUP)) != 0
    if rc == 0:
      # Timeout.
      return false
    # rc < 0 — EINTR is the only retry-worthy error here.
    let err = errno
    if err == EINTR:
      inc attempts
      if attempts >= maxAttempts: return false
      continue
    # Any other error (EFAULT, EINVAL, ENOMEM, ...).
    return false

proc pollForWrite*(fd: cint, timeoutMs: int): bool {.gcsafe, raises: [].} =
  ## Poll socket for write readiness using poll() with timeout.
  ## Returns true if socket is writable (or POLLERR/POLLHUP), false on
  ## timeout or error. Safe for any fd value (no FD_SETSIZE limit).
  let waitMs: cint = if timeoutMs <= 0: -1 else: timeoutMs.cint
  var pfd: TPollfd
  pfd.fd = fd
  pfd.events = cshort(POLLOUT or POLLERR or POLLHUP)
  pfd.revents = 0

  var attempts = 0
  const maxAttempts = 16
  while true:
    let rc = posix.poll(addr pfd, Tnfds(1), waitMs)
    if rc > 0:
      # Ready. revents may include POLLERR/POLLHUP — treat as "woken".
      return (pfd.revents and cshort(POLLOUT or POLLERR or POLLHUP)) != 0
    if rc == 0:
      # Timeout.
      return false
    # rc < 0 — EINTR is the only retry-worthy error here.
    let err = errno
    if err == EINTR:
      inc attempts
      if attempts >= maxAttempts: return false
      continue
    # Any other error (EFAULT, EINVAL, ENOMEM, ...).
    return false

# =============================================================================
# Non-blocking recv with poll polling
# =============================================================================

proc recvExactNonBlocking*(fd: cint, buf: var string, size: int,
                           timeoutMs: int): int {.gcsafe, raises: [].} =
  ## Read exactly `size` bytes using non-blocking recv with poll polling.
  ## Returns the number of bytes actually read (< size means timeout/error/closed).
  buf.setLen(size)
  var total = 0
  var retries = 0
  const maxRetries = 100 # Safety limit to prevent infinite loops
  let sockFd = SocketHandle(fd)

  while total < size and retries < maxRetries:
    # Poll for read readiness
    if not pollForRead(fd, timeoutMs):
      # Timeout or error
      buf.setLen(total)
      return total

    # Socket is ready - attempt recv
    let got = posix.recv(sockFd, addr buf[total], size - total, 0)

    if got > 0:
      total += got
      retries = 0 # Reset retry count on successful read
    elif got == 0:
      # Connection closed by peer
      buf.setLen(total)
      return total
    else:
      # got < 0 - check errno
      let err = errno
      if err == EAGAIN or err == EWOULDBLOCK:
        # Shouldn't happen since we polled, but handle it
        inc retries
        # Small yield to prevent CPU spinning
        discard posix.usleep(1000) # 1ms
      else:
        # Real error (EPIPE, ECONNRESET, etc.)
        buf.setLen(total)
        return total

  buf.setLen(total)
  total

proc recvNNonBlocking*(fd: cint, n: int, timeoutMs: int): string {.gcsafe,
    raises: [].} =
  ## Read exactly n bytes using non-blocking recv; returns shorter string on timeout/EOF/error.
  result = newString(n)
  let got = recvExactNonBlocking(fd, result, n, timeoutMs)
  result.setLen(got)

# =============================================================================
# Non-blocking send with poll polling
# =============================================================================

proc sendNonBlocking*(fd: cint, data: string, timeoutMs: int): int {.gcsafe,
    raises: [].} =
  ## Send data using non-blocking socket with poll polling.
  ## Returns number of bytes sent (< data.len means timeout/error).
  if data.len == 0:
    return 0

  var total = 0
  var retries = 0
  const maxRetries = 100
  let sockFd = SocketHandle(fd)

  while total < data.len and retries < maxRetries:
    # Poll for write readiness
    if not pollForWrite(fd, timeoutMs):
      # Timeout or error
      return total

    # Socket is ready - attempt send
    let sent = posix.send(sockFd, addr data[total], data.len - total, 0)

    if sent > 0:
      total += sent
      retries = 0
    elif sent == 0:
      # Shouldn't happen, but treat as error
      return total
    else:
      # sent < 0 - check errno
      let err = errno
      if err == EAGAIN or err == EWOULDBLOCK:
        inc retries
        discard posix.usleep(1000)
      else:
        # Real error
        return total

  total
