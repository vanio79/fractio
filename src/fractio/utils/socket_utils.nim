# Socket utility functions for Fractio
# Provides non-blocking socket operations with select() polling.

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
# Select polling helpers with timeout
# =============================================================================

proc pollForRead*(fd: cint, timeoutMs: int): bool {.gcsafe, raises: [].} =
  ## Poll socket for read readiness using select() with timeout.
  ## Returns true if data is available, false on timeout or error.
  ## timeoutMs: milliseconds to wait (0 or negative = wait indefinitely)
  if timeoutMs <= 0:
    # No timeout - wait indefinitely (dangerous, but allow for edge cases)
    var readSet: TFdSet
    posix.FD_ZERO(readSet)
    posix.FD_SET(fd, readSet)
    let rc = posix.select(fd + 1, addr readSet, nil, nil, nil)
    return rc > 0

  var tv: Timeval
  tv.tv_sec = Time(timeoutMs div 1000)
  tv.tv_usec = Suseconds((timeoutMs mod 1000) * 1000)

  var readSet: TFdSet
  posix.FD_ZERO(readSet)
  posix.FD_SET(fd, readSet)

  let rc = posix.select(fd + 1, addr readSet, nil, nil, addr tv)
  return rc > 0 and posix.FD_ISSET(fd, readSet) != 0

proc pollForWrite*(fd: cint, timeoutMs: int): bool {.gcsafe, raises: [].} =
  ## Poll socket for write readiness using select() with timeout.
  ## Returns true if socket is writable, false on timeout or error.
  ## timeoutMs: milliseconds to wait (0 or negative = wait indefinitely)
  if timeoutMs <= 0:
    var writeSet: TFdSet
    posix.FD_ZERO(writeSet)
    posix.FD_SET(fd, writeSet)
    let rc = posix.select(fd + 1, nil, addr writeSet, nil, nil)
    return rc > 0

  var tv: Timeval
  tv.tv_sec = Time(timeoutMs div 1000)
  tv.tv_usec = Suseconds((timeoutMs mod 1000) * 1000)

  var writeSet: TFdSet
  posix.FD_ZERO(writeSet)
  posix.FD_SET(fd, writeSet)

  let rc = posix.select(fd + 1, nil, addr writeSet, nil, addr tv)
  return rc > 0 and posix.FD_ISSET(fd, writeSet) != 0

# =============================================================================
# Non-blocking recv with select polling
# =============================================================================

proc recvExactNonBlocking*(fd: cint, buf: var string, size: int,
                           timeoutMs: int): int {.gcsafe, raises: [].} =
  ## Read exactly `size` bytes using non-blocking recv with select polling.
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
# Non-blocking send with select polling
# =============================================================================

proc sendNonBlocking*(fd: cint, data: string, timeoutMs: int): int {.gcsafe,
    raises: [].} =
  ## Send data using non-blocking socket with select polling.
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
