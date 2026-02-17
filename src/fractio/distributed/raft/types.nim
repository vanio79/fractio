## Raft consensus types for Fractio
## This module defines the core types used by the Raft consensus algorithm,
## including commands, log entries, and snapshots.

import ../../core/types # for base types
import ../../core/errors
import std/tables

type
  NodeId* = uint64
    ## Unique identifier for a Raft node in the cluster

  RaftCommandKind* = enum
    ## Command types understood by the Raft state machine
    rckNoop,         ## No-op (used for heartbeats)
    rckAddNode,      ## Add a new node to the cluster configuration
    rckRemoveNode,   ## Remove a node from the cluster
    rckReconfigure,  ## Change cluster configuration (joint consensus)
    rckClientCommand ## Direct client command (e.g., SQL statement)

  RaftCommand* = ref object
    ## A command to be replicated via Raft and applied to the state machine
    kind*: RaftCommandKind
      ## The kind of command
    data*: seq[byte]
      ## Opaque serialized command data. Interpretation depends on `kind`.

  RaftEntry* = ref object
    ## An entry in the Raft log
    term*: uint64
      ## Term when this entry was created (by leader)
    index*: uint64
      ## Log position (1-indexed). Monotonically increasing.
    command*: RaftCommand
      ## The command to apply
    checksum*: uint32
      ## CRC32 checksum of `command.data` for integrity verification

  Snapshot* = ref object
    ## A point-in-time snapshot of the state machine
    lastIndex*: uint64
      ## Log index of the last entry applied to create this snapshot
    lastTerm*: uint64
      ## Term of the last applied entry
    data*: seq[byte]
      ## Serialized state machine state (opaque)

  # RPC message types
  RaftMessageKind* = enum
    rmRequestVote, rmRequestVoteReply, rmAppendEntries,
    rmAppendEntriesReply, rmInstallSnapshot

  RequestVoteArgs* = object
    term*: uint64
    candidateId*: NodeId
    lastLogIndex*: uint64
    lastLogTerm*: uint64

  RequestVoteReply* = object
    term*: uint64
    voteGranted*: bool

  AppendEntriesArgs* = object
    term*: uint64
    leaderId*: NodeId
    prevLogIndex*: uint64
    prevLogTerm*: uint64
    entries*: seq[RaftEntry]
    leaderCommit*: uint64

  AppendEntriesReply* = object
    term*: uint64
    success*: bool
    conflictTerm*: uint64  ## Optional: used for optimization
    conflictIndex*: uint64 ## Optional: used for optimization

  InstallSnapshotArgs* = object
    term*: uint64
    leaderId*: NodeId
    lastIncludedIndex*: uint64
    lastIncludedTerm*: uint64
    data*: seq[byte]

  RaftMessage* = ref object
    case kind*: RaftMessageKind
    of rmRequestVote:
      requestVote*: RequestVoteArgs
    of rmRequestVoteReply:
      requestVoteReply*: RequestVoteReply
    of rmAppendEntries:
      appendEntries*: AppendEntriesArgs
    of rmAppendEntriesReply:
      appendEntriesReply*: AppendEntriesReply
    of rmInstallSnapshot:
      installSnapshot*: InstallSnapshotArgs

  # Transport sends messages to peers.
  RaftTransport* {.inheritable.} = ref object
    send*: proc(dest: NodeId, msg: RaftMessage) {.raises: [FractioError].}
