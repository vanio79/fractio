# Raft Transport - Adapts network transport for Raft consensus
# Part of the network transport layer for distributed Fractio

import std/[tables, options, locks, strutils]
import ./types
import ./serialization
import ./connection_manager
import ../raft/types as raft_types
import ../../core/types as coretypes
import ../../utils/logging

# =============================================================================
# Conversion Helpers
# =============================================================================

proc toNodeID*(serverId: int32): NodeID =
  ## Convert Raft server ID (int32) to NodeID
  result = NodeID("raft_" & $serverId)

proc toServerId*(nodeId: NodeID): int32 =
  ## Convert NodeID to Raft server ID (int32)
  let s = string(nodeId)
  if s.startsWith("raft_"):
    try:
      result = int32(parseInt(s[5..^1]))
    except:
      result = -1
  else:
    result = -1

# =============================================================================
# Raft Message Adapters
# =============================================================================

proc encodeLogEntries*(entries: seq[raft_types.LogEntry]): string =
  ## Encode log entries for transmission
  var w = newBinaryWriter()
  w.writeUint32BE(uint32(entries.len))
  for entry in entries:
    w.writeUint64BE(uint64(entry.term))
    w.writeUint8(uint8(entry.entryType))
    w.writeString(entry.data)
  result = w.getString()

proc decodeLogEntries*(data: string): seq[raft_types.LogEntry] =
  ## Decode log entries from transmission
  var r = newBinaryReader(data)
  let count = r.readUint32BE()
  for i in 0..<count:
    var entry: raft_types.LogEntry
    entry.term = int64(r.readUint64BE())
    entry.entryType = raft_types.LogEntryType(r.readUint8())
    entry.data = r.readString()
    result.add(entry)

# =============================================================================
# Raft Transport
# =============================================================================

type
  RaftTransport* = ref object
    ## Transport layer for Raft consensus
    connManager*: ConnectionManager
    nodeId*: NodeID
    serverId*: int32
    handlers*: tables.Table[uint16, proc(data: string): string {.gcsafe.}]
    handlersLock*: Lock

proc newRaftTransport*(connManager: ConnectionManager,
                       serverId: int32): RaftTransport =
  ## Create a new Raft transport
  result = RaftTransport(
    connManager: connManager,
    nodeId: toNodeID(serverId),
    serverId: serverId,
    handlers: tables.initTable[uint16, proc(data: string): string {.gcsafe.}]()
  )
  initLock(result.handlersLock)

proc close*(rt: RaftTransport) =
  ## Close the Raft transport
  deinitLock(rt.handlersLock)

# =============================================================================
# Message Handlers
# =============================================================================

proc registerHandler*(rt: RaftTransport, msgType: uint16,
                      handler: proc(data: string): string {.gcsafe.}) =
  ## Register a message handler
  withLock rt.handlersLock:
    rt.handlers[msgType] = handler

proc getHandler*(rt: RaftTransport, msgType: uint16): Option[proc(
    data: string): string {.gcsafe.}] =
  ## Get a message handler
  withLock rt.handlersLock:
    if msgType in rt.handlers:
      return some(rt.handlers[msgType])
  return none(proc(data: string): string {.gcsafe.})

# =============================================================================
# RequestVote
# =============================================================================

proc sendRequestVote*(rt: RaftTransport, targetServerId: int32,
                      term: uint64, candidateId: int32,
                      lastLogIndex: uint64, lastLogTerm: uint64): Option[
                          RequestVoteResponseMsg] =
  ## Send a RequestVote RPC to a target server
  let targetNodeId = toNodeID(targetServerId)

  var msg: RequestVoteMsg
  msg.header = newMessageHeader(uint16(rmtRequestVote), 0'u64, rt.nodeId,
      targetNodeId, term)
  msg.candidateId = toNodeID(candidateId)
  msg.lastLogIndex = lastLogIndex
  msg.lastLogTerm = lastLogTerm

  let payload = encodeRequestVoteMsg(msg)

  let responseOpt = rt.connManager.sendRaftMessageWithResponse(
    targetNodeId, payload, rt.connManager.config.tcpReadTimeoutMs)

  if responseOpt.isSome:
    result = some(decodeRequestVoteResponseMsg(responseOpt.get()))

proc handleRequestVote*(rt: RaftTransport, data: string): string {.gcsafe.} =
  ## Handle incoming RequestVote message
  let msg = decodeRequestVoteMsg(data)
  let handlerOpt = rt.getHandler(uint16(rmtRequestVote))

  if handlerOpt.isSome:
    return handlerOpt.get()(data)

  # Default response - deny vote
  var resp: RequestVoteResponseMsg
  resp.header = newMessageHeader(uint16(rmtRequestVoteResponse), msg.header.messageId,
                                  msg.header.targetNodeId,
                                      msg.header.sourceNodeId, 0'u64)
  resp.voteGranted = false
  resp.term = 0'u64
  result = encodeRequestVoteResponseMsg(resp)

# =============================================================================
# AppendEntries
# =============================================================================

proc sendAppendEntries*(rt: RaftTransport, targetServerId: int32,
                        term: uint64, leaderId: int32,
                        prevLogIndex: uint64, prevLogTerm: uint64,
                        commitIndex: uint64,
                        entries: seq[raft_types.LogEntry]): Option[
                            AppendEntriesResponseMsg] =
  ## Send an AppendEntries RPC to a target server
  let targetNodeId = toNodeID(targetServerId)

  var msg: AppendEntriesMsg
  msg.header = newMessageHeader(uint16(rmtAppendEntries), 0'u64, rt.nodeId,
      targetNodeId, term)
  msg.leaderId = toNodeID(leaderId)
  msg.prevLogIndex = prevLogIndex
  msg.prevLogTerm = prevLogTerm
  msg.commitIndex = commitIndex
  msg.numEntries = uint32(entries.len)
  msg.entriesData = encodeLogEntries(entries)

  let payload = encodeAppendEntriesMsg(msg)

  let responseOpt = rt.connManager.sendRaftMessageWithResponse(
    targetNodeId, payload, rt.connManager.config.tcpReadTimeoutMs)

  if responseOpt.isSome:
    result = some(decodeAppendEntriesResponseMsg(responseOpt.get()))

proc handleAppendEntries*(rt: RaftTransport, data: string): string {.gcsafe.} =
  ## Handle incoming AppendEntries message
  let msg = decodeAppendEntriesMsg(data)
  let handlerOpt = rt.getHandler(uint16(rmtAppendEntries))

  if handlerOpt.isSome:
    return handlerOpt.get()(data)

  # Default response - success
  var resp: AppendEntriesResponseMsg
  resp.header = newMessageHeader(uint16(rmtAppendEntriesResponse), msg.header.messageId,
                                  msg.header.targetNodeId,
                                      msg.header.sourceNodeId, 0'u64)
  resp.success = true
  resp.term = 0'u64
  resp.matchIndex = msg.prevLogIndex + uint64(msg.numEntries)
  resp.rejectHint = 0'u64
  result = encodeAppendEntriesResponseMsg(resp)

# =============================================================================
# InstallSnapshot
# =============================================================================

proc sendInstallSnapshot*(rt: RaftTransport, targetServerId: int32,
                          term: uint64, leaderId: int32,
                          lastIncludedIndex: uint64, lastIncludedTerm: uint64,
                          offset: uint64, done: bool,
                          data: string): Option[InstallSnapshotResponseMsg] =
  ## Send an InstallSnapshot RPC to a target server
  let targetNodeId = toNodeID(targetServerId)

  var msg: InstallSnapshotMsg
  msg.header = newMessageHeader(uint16(rmtInstallSnapshot), 0'u64, rt.nodeId,
      targetNodeId, term)
  msg.leaderId = toNodeID(leaderId)
  msg.lastIncludedIndex = lastIncludedIndex
  msg.lastIncludedTerm = lastIncludedTerm
  msg.offset = offset
  msg.done = done
  msg.data = data

  let payload = encodeInstallSnapshotMsg(msg)

  let responseOpt = rt.connManager.sendRaftMessageWithResponse(
    targetNodeId, payload, rt.connManager.config.tcpReadTimeoutMs)

  if responseOpt.isSome:
    result = some(decodeInstallSnapshotResponseMsg(responseOpt.get()))

proc handleInstallSnapshot*(rt: RaftTransport,
    data: string): string {.gcsafe.} =
  ## Handle incoming InstallSnapshot message
  let msg = decodeInstallSnapshotMsg(data)
  let handlerOpt = rt.getHandler(uint16(rmtInstallSnapshot))

  if handlerOpt.isSome:
    return handlerOpt.get()(data)

  # Default response
  var resp: InstallSnapshotResponseMsg
  resp.header = newMessageHeader(uint16(rmtInstallSnapshotResponse), msg.header.messageId,
                                  msg.header.targetNodeId,
                                      msg.header.sourceNodeId, 0'u64)
  resp.term = 0'u64
  resp.offset = msg.offset + uint64(msg.data.len)
  result = encodeInstallSnapshotResponseMsg(resp)

# =============================================================================
# TimeoutNow (for leadership transfer)
# =============================================================================

proc sendTimeoutNow*(rt: RaftTransport, targetServerId: int32,
                     term: uint64): bool =
  ## Send a TimeoutNow RPC to a target server (for leadership transfer)
  let targetNodeId = toNodeID(targetServerId)

  var msg: TimeoutNowMsg
  msg.header = newMessageHeader(uint16(rmtTimeoutNow), 0'u64, rt.nodeId,
      targetNodeId, term)

  let payload = encodeTimeoutNowMsg(msg)
  result = rt.connManager.sendRaftMessage(targetNodeId, payload)

proc handleTimeoutNow*(rt: RaftTransport, data: string): string {.gcsafe.} =
  ## Handle incoming TimeoutNow message
  discard decodeTimeoutNowMsg(data)
  let handlerOpt = rt.getHandler(uint16(rmtTimeoutNow))

  if handlerOpt.isSome:
    return handlerOpt.get()(data)

  # No response needed
  result = ""

# =============================================================================
# ReadIndex (for linearizable reads)
# =============================================================================

proc sendReadIndex*(rt: RaftTransport, targetServerId: int32,
                    term: uint64, readRequestId: uint64): Option[
                        ReadIndexResponseMsg] =
  ## Send a ReadIndex RPC to a target server (for linearizable reads)
  let targetNodeId = toNodeID(targetServerId)

  var msg: ReadIndexMsg
  msg.header = newMessageHeader(uint16(rmtReadIndex), 0'u64, rt.nodeId,
      targetNodeId, term)
  msg.readRequestId = readRequestId

  let payload = encodeReadIndexMsg(msg)

  let responseOpt = rt.connManager.sendRaftMessageWithResponse(
    targetNodeId, payload, rt.connManager.config.tcpReadTimeoutMs)

  if responseOpt.isSome:
    result = some(decodeReadIndexResponseMsg(responseOpt.get()))

proc handleReadIndex*(rt: RaftTransport, data: string): string {.gcsafe.} =
  ## Handle incoming ReadIndex message
  let msg = decodeReadIndexMsg(data)
  let handlerOpt = rt.getHandler(uint16(rmtReadIndex))

  if handlerOpt.isSome:
    return handlerOpt.get()(data)

  # Default response
  var resp: ReadIndexResponseMsg
  resp.header = newMessageHeader(uint16(rmtReadIndexResponse), msg.header.messageId,
                                  msg.header.targetNodeId,
                                      msg.header.sourceNodeId, 0'u64)
  resp.readRequestId = msg.readRequestId
  resp.index = 0'u64
  result = encodeReadIndexResponseMsg(resp)

# =============================================================================
# Broadcast Operations
# =============================================================================

proc broadcastRequestVote*(rt: RaftTransport, term: uint64,
                           candidateId: int32, lastLogIndex: uint64,
                           lastLogTerm: uint64): int =
  ## Broadcast RequestVote to all nodes in the cluster
  let nodes = rt.connManager.getRemoteNodes()
  for nodeInfo in nodes:
    let targetServerId = toServerId(nodeInfo.nodeId)
    if targetServerId > 0:
      discard rt.sendRequestVote(targetServerId, term, candidateId,
                                 lastLogIndex, lastLogTerm)
      inc result

proc broadcastAppendEntries*(rt: RaftTransport, term: uint64,
                             leaderId: int32, prevLogIndex: uint64,
                             prevLogTerm: uint64, commitIndex: uint64,
                             entries: seq[raft_types.LogEntry]): int =
  ## Broadcast AppendEntries to all nodes in the cluster
  let nodes = rt.connManager.getRemoteNodes()
  for nodeInfo in nodes:
    let targetServerId = toServerId(nodeInfo.nodeId)
    if targetServerId > 0:
      discard rt.sendAppendEntries(targetServerId, term, leaderId,
                                   prevLogIndex, prevLogTerm, commitIndex, entries)
      inc result

# =============================================================================
# Setup
# =============================================================================

proc setupHandlers*(rt: RaftTransport) =
  ## Set up default message handlers with the connection manager

  # Register RequestVote handler
  proc handleRV(data: string): string {.gcsafe.} =
    rt.handleRequestVote(data)
  rt.connManager.registerRaftHandler(uint16(rmtRequestVote), handleRV)

  # Register AppendEntries handler
  proc handleAE(data: string): string {.gcsafe.} =
    rt.handleAppendEntries(data)
  rt.connManager.registerRaftHandler(uint16(rmtAppendEntries), handleAE)

  # Register InstallSnapshot handler
  proc handleIS(data: string): string {.gcsafe.} =
    rt.handleInstallSnapshot(data)
  rt.connManager.registerRaftHandler(uint16(rmtInstallSnapshot), handleIS)

  # Register TimeoutNow handler
  proc handleTN(data: string): string {.gcsafe.} =
    rt.handleTimeoutNow(data)
  rt.connManager.registerRaftHandler(uint16(rmtTimeoutNow), handleTN)

  # Register ReadIndex handler
  proc handleRI(data: string): string {.gcsafe.} =
    rt.handleReadIndex(data)
  rt.connManager.registerRaftHandler(uint16(rmtReadIndex), handleRI)
