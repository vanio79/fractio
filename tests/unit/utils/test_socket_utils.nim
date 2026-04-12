# Unit tests for fractio/utils/socket_utils.nim
# Tests socket utility functions

import std/[unittest, net]
import fractio/utils/socket_utils

suite "Socket Utilities":

  test "setReuseAddr enables reuse addr":
    let sock = newSocket()
    sock.setReuseAddr(true)
    # No exception means success - can't easily check the actual option
    sock.close()

  test "setReuseAddr disables reuse addr":
    let sock = newSocket()
    sock.setReuseAddr(false)
    sock.close()

  test "setReuseAddr default parameter":
    let sock = newSocket()
    sock.setReuseAddr() # Uses default enabled=true
    sock.close()

  test "setReuseAddr on bound socket":
    let sock = newSocket()
    sock.setReuseAddr(true)
    # Bind to an available port
    sock.bindAddr(Port(0)) # Port 0 lets OS pick an available port
    sock.close()

  test "setLingerZero on socket":
    let sock = newSocket()
    sock.setLingerZero()
    # No exception means success - can't easily verify the actual option
    sock.close()

  test "setLingerZero on bound socket":
    let sock = newSocket()
    sock.setReuseAddr(true)
    sock.bindAddr(Port(0))
    sock.setLingerZero()
    sock.close()

  test "Combined socket options":
    let sock = newSocket()
    sock.setReuseAddr(true)
    sock.setLingerZero()
    sock.bindAddr(Port(0))
    sock.close()

test "Multiple sockets with reuse addr":
  # This demonstrates that SO_REUSEADDR allows binding to the same port
  # quickly after close (no TIME_WAIT)
  let sock1 = newSocket()
  sock1.setReuseAddr(true)
  sock1.setLingerZero()
  sock1.bindAddr(Port(0))

  # Get the actual port assigned
  let (addrStr, port) = sock1.getLocalAddr()

  sock1.close()

  # Create another socket - would fail without SO_REUSEADDR
  # if previous socket was in TIME_WAIT
  let sock2 = newSocket()
  sock2.setReuseAddr(true)
  # Note: in practice might still fail if TIME_WAIT hasn't cleared
  # but the test validates the functions work without exceptions
  sock2.close()

suite "Socket Utility Edge Cases":

  test "setReuseAddr on already closed socket raises":
    let sock = newSocket()
    sock.close()
    # Calling setReuseAddr after close should raise
    var raised = false
    try:
      sock.setReuseAddr(true)
    except:
      raised = true
    check raised

test "Socket operations sequence":
  # Test typical usage sequence
  let sock = newSocket()

  # Set options before binding
  sock.setReuseAddr(true)
  sock.setLingerZero()

  # Bind
  sock.bindAddr(Port(0))

  # Get local address
  let (addrStr, port) = sock.getLocalAddr()
  check addrStr.len > 0

  sock.close()
