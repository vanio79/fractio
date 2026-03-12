import std/net
import std/posix

proc setLingerZero*(sock: Socket) =
  ## Sets SO_LINGER with a zero timeout on the socket.
  ## This causes the socket to be closed immediately with a RST
  ## instead of a normal FIN handshake, avoiding the TIME_WAIT state.
  var ling: TLinger
  ling.l_onoff = 1
  ling.l_linger = 0
  
  if setsockopt(sock.getFd(), SOL_SOCKET, SO_LINGER, addr ling, sizeof(ling).SockLen) < 0:
    # We don't raise here to avoid crashing on socket option failures,
    # but in a real system we might want to log this.
    discard

proc setReuseAddr*(sock: Socket, enabled: bool = true) =
  ## Sets SO_REUSEADDR on the socket.
  sock.setSockOpt(OptReuseAddr, enabled)
