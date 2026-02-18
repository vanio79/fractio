## UDP Transport for Raft consensus
## Production-ready, thread-safe network transport using UDP.
## Handles message serialization, delivery, and error recovery.

import std/[net, nativesockets, typedthreads, os, atomics, tables, monotimes]
import posix
import ../../core/errors
import ../../utils/logging
import ./types

const
  MAX_UDP_PACKET_SIZE* = 65507 ## Maximum safe UDP payload (bytes)
  DEFAULT_LOG_TAG* = "RaftUDPTransport"

type
  RaftMessageHandler* = proc(sender: NodeId, msg: RaftMessage,
      now: uint64) {.raises: [FractioError], gcsafe.}

  RaftUDPTransport* = ref object of RaftTransport
    nodeId*: NodeId
    bindAddr*: NodeAddress
    peerAddrs*: Table[NodeId, NodeAddress]
    handler*: RaftMessageHandler
    socket*: Socket
    serverThread*: Thread[RaftUDPTransport]
    serverRunning*: Atomic[bool]
    logger*: Logger
    addrToNodeId*: Table[(string, uint16), NodeId]

# Helper: construct NodeAddress
proc newNodeAddress*(host: string, port: uint16): NodeAddress =
  NodeAddress(host: host, port: port)

# Construct a new UDP transport.
# - nodeId: this node's Raft ID
# - bindAddr: local address to bind (host, port). Use "" and 0 for any.
# - peerAddrs: mapping from peer NodeIds to their network addresses
# - handler: called for each incoming message with (sender, message, now)
# - logger: optional logger (creates default if nil)
proc newRaftUDPTransport*(nodeId: NodeId, bindAddr: NodeAddress,
    peerAddrs: Table[NodeId, NodeAddress], handler: RaftMessageHandler,
    logger: Logger = nil): RaftUDPTransport =
  if handler.isNil:
    raise newException(ValueError, "handler must be provided")
  result = RaftUDPTransport(
    nodeId: nodeId,
    bindAddr: bindAddr,
    peerAddrs: peerAddrs,
    handler: handler,
    logger: if logger.isNil: newLogger(DEFAULT_LOG_TAG) else: logger,
    serverRunning: Atomic[bool]()
  )
  store(result.serverRunning, false)
  # Build reverse lookup: (host, port) -> NodeId
  result.addrToNodeId = initTable[(string, uint16), NodeId]()
  for nid, addr in peerAddrs:
    result.addrToNodeId[(addr.host, addr.port)] = nid

# Forward declarations for serialization procs (used in send/receive)
proc encodeRaftMessage*(msg: RaftMessage): seq[byte] {.gcsafe.}
proc decodeRaftMessage*(data: openArray[byte]): RaftMessage {.raises: [
    ValueError], gcsafe.}

# Start listening for incoming messages.
# Raises ValueError if already started, or OSError/InheritFromException on bind failure.
proc receiveLoop(self: RaftUDPTransport) {.thread, gcsafe.}
method start*(self: RaftUDPTransport) =
  if load(self.serverRunning, moRelaxed):
    raise newException(ValueError, "Transport already started")
  self.socket = newSocket(net.AF_INET, net.SOCK_DGRAM, net.IPPROTO_UDP)
  self.socket.setSockOpt(OptReuseAddr, true)
  try:
    let bindHost = if self.bindAddr.host.len ==
        0: "0.0.0.0" else: self.bindAddr.host
    self.socket.bindAddr(Port(self.bindAddr.port), bindHost)
    # Retrieve actual bound port (in case we passed 0)
    let (_, actualPort) = self.socket.getLocalAddr()
    self.bindAddr.port = uint16(actualPort)
  except:
    self.socket.close()
    raise
  # Mark as running before starting thread to avoid race
  store(self.serverRunning, true)
  # Launch receiver thread
  createThread(self.serverThread, receiveLoop, self)
  var fields = initTable[string, string]()
  fields["port"] = $self.bindAddr.port
  fields["nodeId"] = $self.nodeId
  self.logger.info("RaftUDPTransport started", fields)

# Send a message to the destination node.
# Looks up the network address for dest, serializes, and transmits via UDP.
# Raises FractioError if dest address is unknown or send fails.
method send*(self: RaftUDPTransport, dest: NodeId, msg: RaftMessage) {.raises: [
    FractioError].} =
  let peerAddr = self.peerAddrs.getOrDefault(dest)
  if peerAddr.host == "":
    raise newError(fekConfig, "No network address configured for node " & $dest)
  var data: seq[byte]
  try:
    data = encodeRaftMessage(msg)
  except Exception as e:
    raise newError(fekNetwork, "Failed to encode Raft message: " & e.msg)
  if data.len == 0:
    return # nothing to send
  try:
    # Resolve destination IP address
    let ip = parseIpAddress(peerAddr.host)
    var sa: Sockaddr_storage
    var sl: SockLen
    toSockAddr(ip, Port(peerAddr.port), sa, sl)
    # Get native socket handle
    let fd = getFd(self.socket)
    # Send the data using low-level sendto
    let bytesSent = sendto(fd, cast[pointer](unsafeAddr data[0]), data.len.cint, 0'i32,
      cast[ptr SockAddr](addr sa), sl)
    if bytesSent < 0:
      raise newError(fekNetwork, "Failed to send Raft message to " &
          peerAddr.host & ":" & $peerAddr.port)
  except Exception as e:
    raise newError(fekNetwork, "Network send failed to " & peerAddr.host & ":" &
        $peerAddr.port & ": " & e.msg)

# Stop the transport: close socket, wait for receiver thread, log summary.
method close*(self: RaftUDPTransport) =
  if not load(self.serverRunning, moRelaxed):
    return
  store(self.serverRunning, false)
  # Shutdown the socket to unblock recvFrom
  let fd = getFd(self.socket)
  discard posix.shutdown(fd, posix.SHUT_RD)
  self.socket.close()
  joinThread(self.serverThread)
  var closeFields = initTable[string, string]()
  closeFields["nodeId"] = $self.nodeId
  self.logger.info("RaftUDPTransport closed", closeFields)

proc getMonotonicTime*(): uint64 {.gcsafe.}

# Receiver thread main loop.
proc receiveLoop(self: RaftUDPTransport) {.thread, gcsafe.} =
  var buffer = newString(MAX_UDP_PACKET_SIZE)
  while load(self.serverRunning, moRelaxed):
    try:
      var clientAddr: string
      var clientPort: Port
      let n = self.socket.recvFrom(buffer, MAX_UDP_PACKET_SIZE, clientAddr, clientPort)
      if n <= 0:
        continue
      # Convert string buffer to seq[byte] for decoding
      var data = newSeq[uint8](n)
      for i in 0..<n:
        data[i] = uint8(buffer[i])
      # Decode the RaftMessage
      let msg = decodeRaftMessage(data)
      # Map (addr, port) to NodeId
      let sender = self.addrToNodeId.getOrDefault((clientAddr, uint16(
          clientPort)), NodeId(0))
      if sender == NodeId(0):
        var warnFields = initTable[string, string]()
        warnFields["addr"] = clientAddr
        warnFields["port"] = $clientPort
        self.logger.warn("Received message from unknown peer (not in peerAddrs)", warnFields)
        continue
      # Capture current monotonic time
      let now = getMonotonicTime()
      # Deliver to the registered handler
      self.handler(sender, msg, now)
    except FractioError as e:
      var errFields = initTable[string, string]()
      errFields["error"] = e.msg
      errFields["nodeId"] = $self.nodeId
      self.logger.error("Failed to process Raft message", errFields)
    except OSError as e:
      # If shutting down, allow loop to exit without further action
      if not load(self.serverRunning, moRelaxed):
        continue
      # Ignore timeout errors (non-fatal during normal operation)
      if e.errorCode == posix.EAGAIN or e.errorCode == posix.EWOULDBLOCK:
        continue
      var errFields = initTable[string, string]()
      errFields["error"] = e.msg
      errFields["nodeId"] = $self.nodeId
      self.logger.error("Socket recv error", errFields)
    except Exception as e:
      var errFields = initTable[string, string]()
      errFields["error"] = e.msg
      errFields["nodeId"] = $self.nodeId
      self.logger.error("Unexpected error in receive loop", errFields)

# --- Serialization (big-endian / network byte order) ---

func toBytesBE(x: uint64): array[8, uint8] =
  result[0] = uint8((x shr 56) and 0xFF)
  result[1] = uint8((x shr 48) and 0xFF)
  result[2] = uint8((x shr 40) and 0xFF)
  result[3] = uint8((x shr 32) and 0xFF)
  result[4] = uint8((x shr 24) and 0xFF)
  result[5] = uint8((x shr 16) and 0xFF)
  result[6] = uint8((x shr 8) and 0xFF)
  result[7] = uint8(x and 0xFF)

func fromBytesBE(a: openArray[uint8]): uint64 =
  result = uint64(a[0]) shl 56 or uint64(a[1]) shl 48 or uint64(a[2]) shl 40 or uint64(a[3]) shl 32 or
          uint64(a[4]) shl 24 or uint64(a[5]) shl 16 or uint64(a[6]) shl 8 or
              uint64(a[7])

func toBytesBE32(x: uint32): array[4, uint8] =
  result[0] = uint8((x shr 24) and 0xFF)
  result[1] = uint8((x shr 16) and 0xFF)
  result[2] = uint8((x shr 8) and 0xFF)
  result[3] = uint8(x and 0xFF)

func fromBytesBE32(a: openArray[uint8]): uint32 =
  result = uint32(a[0]) shl 24 or uint32(a[1]) shl 16 or uint32(a[2]) shl 8 or
      uint32(a[3])

proc encodeRaftMessage*(msg: RaftMessage): seq[byte] {.gcsafe.} =
  ## Serialize a RaftMessage to a byte sequence (big-endian).
  ## Used internally by the transport.
  case msg.kind
  of rmRequestVote:
    let m = msg.requestVote
    result = newSeq[byte](1 + 8*4)
    result[0] = byte(rmRequestVote.ord)
    # term
    let termBE = toBytesBE(m.term)
    copyMem(addr result[1], unsafeAddr termBE[0], 8)
    # candidateId
    let cidBE = toBytesBE(uint64(m.candidateId))
    copyMem(addr result[1+8], unsafeAddr cidBE[0], 8)
    # lastLogIndex
    let idxBE = toBytesBE(m.lastLogIndex)
    copyMem(addr result[1+16], unsafeAddr idxBE[0], 8)
    # lastLogTerm
    let term2BE = toBytesBE(m.lastLogTerm)
    copyMem(addr result[1+24], unsafeAddr term2BE[0], 8)
  of rmRequestVoteReply:
    let m = msg.requestVoteReply
    result = newSeq[byte](1 + 8 + 1)
    result[0] = byte(rmRequestVoteReply.ord)
    let termBE = toBytesBE(m.term)
    copyMem(addr result[1], unsafeAddr termBE[0], 8)
    result[9] = if m.voteGranted: 1'u8 else: 0'u8
  of rmAppendEntries:
    let m = msg.appendEntries
    # Pre-serialize entries
    var entriesData = newSeq[byte]()
    let count = uint64(m.entries.len)
    let countBE = toBytesBE(count)
    entriesData.add(countBE)
    for e in m.entries:
      let termBE = toBytesBE(e.term)
      entriesData.add(termBE)
      let idxBE = toBytesBE(e.index)
      entriesData.add(idxBE)
      # command kind
      entriesData.add(byte(e.command.kind.ord))
      # command data length and data
      let dataLen = uint64(e.command.data.len)
      let dataLenBE = toBytesBE(dataLen)
      entriesData.add(dataLenBE)
      if dataLen > 0:
        entriesData.add(e.command.data)
      # checksum
      let chkBE = toBytesBE32(e.checksum)
      entriesData.add(chkBE)
    let totalLen = 1 + 40 + entriesData.len
    result = newSeq[byte](totalLen)
    result[0] = byte(rmAppendEntries.ord)
    var offset = 1
    template putU64(x: uint64) =
      let be = toBytesBE(x)
      copyMem(addr result[offset], unsafeAddr be[0], 8)
      offset += 8
    putU64(m.term)
    putU64(uint64(m.leaderId))
    putU64(m.prevLogIndex)
    putU64(m.prevLogTerm)
    # Serialize entries data (count + serialized entries)
    if entriesData.len > 0:
      copyMem(addr result[offset], unsafeAddr entriesData[0], entriesData.len)
      offset += entriesData.len
    # leaderCommit follows entries
    putU64(m.leaderCommit)
  of rmAppendEntriesReply:
    let m = msg.appendEntriesReply
    result = newSeq[byte](1 + 8 + 1 + 8 + 8)
    result[0] = byte(rmAppendEntriesReply.ord)
    var offset = 1
    let termBE = toBytesBE(m.term)
    copyMem(addr result[offset], unsafeAddr termBE[0], 8)
    offset += 8
    result[offset] = if m.success: 1'u8 else: 0'u8
    inc offset
    let conflictTermBE = toBytesBE(m.conflictTerm)
    copyMem(addr result[offset], unsafeAddr conflictTermBE[0], 8)
    offset += 8
    let conflictIdxBE = toBytesBE(m.conflictIndex)
    copyMem(addr result[offset], unsafeAddr conflictIdxBE[0], 8)
  of rmInstallSnapshot:
    let m = msg.installSnapshot
    let dataLen = uint64(m.data.len)
    let totalLen = 1 + 8*4 + 8 + m.data.len
    result = newSeq[byte](totalLen)
    result[0] = byte(rmInstallSnapshot.ord)
    var offset = 1
    template putU64(x: uint64) =
      let be = toBytesBE(x)
      copyMem(addr result[offset], unsafeAddr be[0], 8)
      offset += 8
    putU64(m.term)
    putU64(uint64(m.leaderId))
    putU64(m.lastIncludedIndex)
    putU64(m.lastIncludedTerm)
    let dataLenBE = toBytesBE(dataLen)
    copyMem(addr result[offset], unsafeAddr dataLenBE[0], 8)
    offset += 8
    if m.data.len > 0:
      copyMem(addr result[offset], unsafeAddr m.data[0], m.data.len)

proc decodeRaftMessage*(data: openArray[byte]): RaftMessage {.raises: [
    ValueError], gcsafe.} =
  ## Deserialize a byte sequence into a RaftMessage.
  ## Raises ValueError on malformed input.
  if data.len == 0:
    raise newException(ValueError, "empty Raft message data")
  let kind = RaftMessageKind(data[0])
  var i = 1
  template readU64(): uint64 =
    if i + 7 >= data.len:
      raise newException(ValueError, "truncated uint64 in Raft message")
    let arr = data[i..i+7]
    i += 8
    fromBytesBE(arr)
  template readU32(): uint32 =
    if i + 3 >= data.len:
      raise newException(ValueError, "truncated uint32 in Raft message")
    let arr = data[i..i+3]
    i += 4
    fromBytesBE32(arr)
  template readByte(): uint8 =
    if i >= data.len:
      raise newException(ValueError, "truncated byte in Raft message")
    let v = data[i]
    inc i
    v
  template readBool(): bool =
    readByte() == 1'u8
  template readSeqByte(dataLen: uint64): seq[byte] =
    if i + int(dataLen) > data.len:
      raise newException(ValueError, "truncated data sequence in Raft message")
    let start = i
    i += int(dataLen)
    var s = newSeq[byte](dataLen.int)
    for j in 0..<dataLen.int:
      s[j] = data[start+j]
    s
  case kind
  of rmRequestVote:
    result = RaftMessage(kind: rmRequestVote)
    result.requestVote.term = readU64()
    result.requestVote.candidateId = NodeId(readU64())
    result.requestVote.lastLogIndex = readU64()
    result.requestVote.lastLogTerm = readU64()
  of rmRequestVoteReply:
    result = RaftMessage(kind: rmRequestVoteReply)
    result.requestVoteReply.term = readU64()
    result.requestVoteReply.voteGranted = readBool()
  of rmAppendEntries:
    result = RaftMessage(kind: rmAppendEntries)
    result.appendEntries.term = readU64()
    result.appendEntries.leaderId = NodeId(readU64())
    result.appendEntries.prevLogIndex = readU64()
    result.appendEntries.prevLogTerm = readU64()
    let count = readU64()
    if count > 0:
      var entries = newSeq[RaftEntry](count.int)
      for idx in 0..<count.int:
        var entry = RaftEntry()
        entry.term = readU64()
        entry.index = readU64()
        # command
        let cmdKindOrd = readByte().int
        if cmdKindOrd < 0 or cmdKindOrd > ord(high(RaftCommandKind)):
          raise newException(ValueError, "invalid RaftCommandKind value")
        entry.command = RaftCommand(kind: RaftCommandKind(cmdKindOrd))
        let cmdDataLen = readU64()
        entry.command.data = readSeqByte(cmdDataLen)
        entry.checksum = readU32()
        entries[idx] = entry
      result.appendEntries.entries = entries
    result.appendEntries.leaderCommit = readU64()
  of rmAppendEntriesReply:
    result = RaftMessage(kind: rmAppendEntriesReply)
    result.appendEntriesReply.term = readU64()
    result.appendEntriesReply.success = readBool()
    result.appendEntriesReply.conflictTerm = readU64()
    result.appendEntriesReply.conflictIndex = readU64()
  of rmInstallSnapshot:
    result = RaftMessage(kind: rmInstallSnapshot)
    result.installSnapshot.term = readU64()
    result.installSnapshot.leaderId = NodeId(readU64())
    result.installSnapshot.lastIncludedIndex = readU64()
    result.installSnapshot.lastIncludedTerm = readU64()
    let dataLen = readU64()
    result.installSnapshot.data = readSeqByte(dataLen)
  if i != data.len:
    raise newException(ValueError, "trailing bytes after Raft message")

proc getMonotonicTime*(): uint64 {.gcsafe.} =
  ## Return current monotonic time in nanoseconds.
  ## Suitable for timeouts and message timestamps.
  result = uint64(getMonoTime().ticks)
