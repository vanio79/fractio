# Raft RPC Handler

import std/json
import std/streams
import std/times
import std/tables

import fractio/utils/logging
import fractio/distributed/raft/types

proc handleAppendEntries*(node: RaftNode, rpc: RaftRPC): RaftRPC =
  ## Handle AppendEntries RPC
  var fields = initTable[string, string]()
  fields["term"] = $rpc.term
  fields["leaderId"] = $rpc.leaderId
  debug("Handling AppendEntries RPC", fields)

  return RaftRPC(
    rpcType: RPC_APPEND_ENTRIES,
    term: node.nodeState.currentTerm,
    leaderId: node.serverId,
    success: true
  )

proc handleRequestVote*(node: RaftNode, rpc: RaftRPC): RaftRPC =
  ## Handle RequestVote RPC
  var fields = initTable[string, string]()
  fields["term"] = $rpc.term
  fields["candidateId"] = $rpc.leaderId
  debug("Handling RequestVote RPC", fields)

  return RaftRPC(
    rpcType: RPC_REQUEST_VOTE,
    term: node.nodeState.currentTerm,
    leaderId: node.serverId,
    success: true
  )

proc handleClientRequest*(node: RaftNode, rpc: RaftRPC): RaftRPC =
  ## Handle client request RPC
  var fields = initTable[string, string]()
  fields["term"] = $rpc.term
  fields["data"] = rpc.data
  debug("Handling client request", fields)

  return RaftRPC(
    rpcType: RPC_CLIENT_REQUEST,
    term: node.nodeState.currentTerm,
    leaderId: node.serverId,
    success: true
  )

proc sendRPC*(node: RaftNode, rpc: RaftRPC, endpoint: string) =
  ## Send an RPC to another node
  discard endpoint
  var fields = initTable[string, string]()
  fields["rpcType"] = $rpc.rpcType
  debug("Sending RPC", fields)

proc processRPC*(node: RaftNode, rpc: RaftRPC): RaftRPC =
  ## Process an incoming RPC
  case rpc.rpcType
  of RPC_APPEND_ENTRIES:
    return handleAppendEntries(node, rpc)
  of RPC_REQUEST_VOTE:
    return handleRequestVote(node, rpc)
  of RPC_CLIENT_REQUEST:
    return handleClientRequest(node, rpc)

proc heartbeat*(node: RaftNode) =
  ## Send periodic heartbeat to followers
  var fields = initTable[string, string]()
  fields["term"] = $node.nodeState.currentTerm
  fields["leaderId"] = $node.serverId
  debug("Sending heartbeat", fields)

proc requestVote*(node: RaftNode) =
  ## Request votes from other nodes during election
  var fields = initTable[string, string]()
  fields["term"] = $node.nodeState.currentTerm
  fields["candidateId"] = $node.serverId
  debug("Requesting votes", fields)

proc replicateLog*(node: RaftNode) =
  ## Replicate log entries to followers
  var fields = initTable[string, string]()
  fields["term"] = $node.nodeState.currentTerm
  fields["leaderId"] = $node.serverId
  debug("Replicating log", fields)

proc processClientRequest*(node: RaftNode, data: string): int64 =
  ## Process a client request
  var fields = initTable[string, string]()
  fields["data"] = data
  debug("Processing client request", fields)
  result = 0
