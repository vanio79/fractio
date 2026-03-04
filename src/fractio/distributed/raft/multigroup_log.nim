# Multi-Group Raft Log Storage
#
# This module implements per-range Raft log storage using WiscKey backend.

import std/atomics
import std/locks
import std/options
import std/json
import std/strutils
import std/sequtils
import std/tables

import fractio/distributed/range/types
import fractio/distributed/raft/multigroup_types
import fractio/storage/wisckey_backend
import fractio/utils/logging

# ============================================================================
# Raft Log
# ============================================================================

type
  RaftLog* = ref object
    ## Raft log for a single group, stored in WiscKey
    rangeId*: RangeID
    store*: WiscKeyBackend
    firstIndex*: Atomic[uint64]
    lastIndex*: Atomic[uint64]
    lock*: Lock

# ============================================================================
# Log Entry Encoding
# ============================================================================

proc encodeEntry*(entry: LogEntry): string =
  ## Encode a log entry to string for storage
  let json = %*{
    "term": entry.term,
    "index": entry.index,
    "commandKind": ord(entry.command.kind)
  }

  # Add command-specific data
  case entry.command.kind
  of ckNoop:
    discard
  of ckWrite:
    var puts = newJArray()
    for (k, v) in entry.command.writeBatch.puts:
      puts.add(%*{"key": k, "value": v})
    json["puts"] = puts

    var deletes = newJArray()
    for k in entry.command.writeBatch.deletes:
      deletes.add(%*{"key": k})
    json["deletes"] = deletes
  of ckSplit:
    json["splitKey"] = %entry.command.splitKey
    json["newRangeId"] = %entry.command.newRangeId.uint64
  of ckMerge:
    json["otherRangeId"] = %entry.command.otherRangeId.uint64
  of ckChangeReplicas:
    json["changeType"] = %ord(entry.command.changeType)
    json["replica"] = %*{
      "nodeId": entry.command.replica.nodeId.uint32,
      "replicaId": entry.command.replica.replicaId.uint32,
      "replicaType": ord(entry.command.replica.replicaType)
    }
  of ckTransferLease:
    json["targetNode"] = %entry.command.targetNode.uint32
  of ckAcquireLease:
    json["leaseStart"] = %entry.command.leaseStart
    json["leaseExpiration"] = %entry.command.leaseExpiration

  result = $json

proc decodeEntry*(data: string): LogEntry =
  ## Decode a log entry from string
  let json = parseJson(data)

  new(result)
  result.term = uint64(json["term"].getInt())
  result.index = uint64(json["index"].getInt())

  let cmdKind = CommandKind(json["commandKind"].getInt())

  case cmdKind
  of ckNoop:
    result.command = RaftCommand(kind: ckNoop)
  of ckWrite:
    let batch = newWriteBatch()
    for p in json["puts"]:
      var key: seq[byte]
      for b in p["key"]:
        key.add(byte(b.getInt()))
      var value: seq[byte]
      for b in p["value"]:
        value.add(byte(b.getInt()))
      batch.put(key, value)
    for d in json["deletes"]:
      var key: seq[byte]
      for b in d["key"]:
        key.add(byte(b.getInt()))
      batch.delete(key)
    result.command = RaftCommand(kind: ckWrite, writeBatch: batch)
  of ckSplit:
    var splitKey: seq[byte]
    for b in json["splitKey"]:
      splitKey.add(byte(b.getInt()))
    result.command = RaftCommand(
      kind: ckSplit,
      splitKey: splitKey,
      newRangeId: RangeID(uint64(json["newRangeId"].getInt()))
    )
  of ckMerge:
    result.command = RaftCommand(
      kind: ckMerge,
      otherRangeId: RangeID(uint64(json["otherRangeId"].getInt()))
    )
  of ckChangeReplicas:
    let repJson = json["replica"]
    result.command = RaftCommand(
      kind: ckChangeReplicas,
      changeType: ReplicaChangeType(json["changeType"].getInt()),
      replica: ReplicaDescriptor(
        nodeId: NodeID(repJson["nodeId"].getInt()),
        replicaId: ReplicaID(repJson["replicaId"].getInt()),
        replicaType: ReplicaType(repJson["replicaType"].getInt())
      )
    )
  of ckTransferLease:
    result.command = RaftCommand(
      kind: ckTransferLease,
      targetNode: NodeID(json["targetNode"].getInt())
    )
  of ckAcquireLease:
    result.command = RaftCommand(
      kind: ckAcquireLease,
      leaseStart: json["leaseStart"].getInt(),
      leaseExpiration: json["leaseExpiration"].getInt()
    )

# ============================================================================
# Raft Log Operations
# ============================================================================

proc newRaftLog*(rangeId: RangeID, store: WiscKeyBackend): RaftLog =
  ## Create a new Raft log for a range
  new(result)
  result.rangeId = rangeId
  result.store = store
  result.firstIndex.store(0)
  result.lastIndex.store(0)
  initLock(result.lock)

proc close*(log: RaftLog) =
  ## Clean up log resources
  deinitLock(log.lock)

proc putEntry*(log: RaftLog, entry: LogEntry) =
  ## Store a log entry
  let key = encodeLogKey(log.rangeId, entry.index)
  let value = encodeEntry(entry)

  if not log.store.put(key, value):
    raise newException(MultiRaftError, "Failed to write log entry")

  # Update last index atomically
  var current = log.lastIndex.load
  while entry.index > current:
    if log.lastIndex.compareExchange(current, entry.index):
      break

proc getEntry*(log: RaftLog, index: uint64): Option[LogEntry] =
  ## Retrieve a log entry by index
  let key = encodeLogKey(log.rangeId, index)
  let value = log.store.get(key)

  if value.isSome:
    result = some(decodeEntry(value.get))

proc getEntries*(log: RaftLog, startIdx, endIdx: uint64): seq[LogEntry] =
  ## Get a range of log entries [startIdx, endIdx]
  for i in startIdx..endIdx:
    let entry = log.getEntry(i)
    if entry.isSome:
      result.add(entry.get)

proc getLastEntry*(log: RaftLog): Option[LogEntry] =
  ## Get the last log entry
  let lastIdx = log.lastIndex.load
  if lastIdx > 0:
    result = log.getEntry(lastIdx)

proc getFirstEntry*(log: RaftLog): Option[LogEntry] =
  ## Get the first log entry
  let firstIdx = log.firstIndex.load
  if firstIdx > 0:
    result = log.getEntry(firstIdx)

proc containsIndex*(log: RaftLog, index: uint64): bool =
  ## Check if a log index exists
  let key = encodeLogKey(log.rangeId, index)
  log.store.exists(key)

proc truncate*(log: RaftLog, fromIndex: uint64) =
  ## Truncate log entries from index onwards
  ## Used when follower has conflicting entries
  var idx = fromIndex
  while true:
    let key = encodeLogKey(log.rangeId, idx)
    if not log.store.exists(key):
      break
    discard log.store.delete(key)
    inc idx

  # Update last index
  var current = log.lastIndex.load()
  while fromIndex - 1 < current:
    if log.lastIndex.compareExchange(current, fromIndex - 1):
      break

proc compact*(log: RaftLog, toIndex: uint64) =
  ## Compact log entries up to index (exclusive)
  ## Called after snapshot
  var idx = log.firstIndex.load()
  while idx < toIndex:
    let key = encodeLogKey(log.rangeId, idx)
    discard log.store.delete(key)
    inc idx
  log.firstIndex.store(toIndex)

# ============================================================================
# Log Initialization and Recovery
# ============================================================================

proc recoverLog*(log: RaftLog) =
  ## Recover log state from storage
  ## Scan all entries to find first and last index
  var firstIdx: uint64 = 0
  var lastIdx: uint64 = 0

  let iter = log.store.newIterator()
  if iter != nil:
    var currentIter = WiscKeyIterator(iter)
    let prefix = "/raft/" & $log.rangeId.uint64 & "/log/"

    # Seek to first entry with our prefix
    if seekToFirstWiscKey(currentIter):
      while validWiscKey(currentIter):
        let key = keyWiscKey(currentIter)
        if key.startsWith(prefix):
          let idx = parseLogIndex(key)
          if firstIdx == 0 or idx < firstIdx:
            firstIdx = idx
          if idx > lastIdx:
            lastIdx = idx
        discard nextWiscKey(currentIter)
    destroyIter(iter)

  log.firstIndex.store(firstIdx)
  log.lastIndex.store(lastIdx)

  var fields = initTable[string, string]()
  fields["rangeId"] = $log.rangeId
  fields["firstIndex"] = $firstIdx
  fields["lastIndex"] = $lastIdx
  debug("Recovered Raft log", fields)

# ============================================================================
# Persistent State Storage
# ============================================================================

proc saveState*(log: RaftLog, state: RaftPersistentState) =
  ## Save persistent state (term, vote, commit)
  let key = encodeStateKey(log.rangeId)
  let json = %*{
    "currentTerm": state.currentTerm,
    "votedFor": state.votedFor.uint32,
    "commitIndex": state.commitIndex,
    "lastApplied": state.lastApplied
  }

  if not log.store.put(key, $json):
    raise newException(MultiRaftError, "Failed to save Raft state")

proc loadState*(log: RaftLog): Option[RaftPersistentState] =
  ## Load persistent state
  let key = encodeStateKey(log.rangeId)
  let value = log.store.get(key)

  if value.isSome:
    let json = parseJson(value.get)
    result = some(RaftPersistentState(
      currentTerm: uint64(json["currentTerm"].getInt()),
      votedFor: ReplicaID(json["votedFor"].getInt()),
      commitIndex: uint64(json["commitIndex"].getInt()),
      lastApplied: uint64(json["lastApplied"].getInt())
    ))

# ============================================================================
# Snapshot Storage
# ============================================================================

proc saveSnapshot*(log: RaftLog, snapshot: Snapshot) =
  ## Save a snapshot
  let key = encodeSnapshotKey(log.rangeId)
  let json = %*{
    "rangeId": snapshot.rangeId.uint64,
    "lastIncludedIndex": snapshot.raftSnap.lastIncludedIndex,
    "lastIncludedTerm": snapshot.raftSnap.lastIncludedTerm,
    "configuration": snapshot.raftSnap.configuration.mapIt(it.toJson()),
    "stateMachineSnap": snapshot.stateMachineSnap
  }

  if not log.store.put(key, $json):
    raise newException(MultiRaftError, "Failed to save snapshot")

proc loadSnapshot*(log: RaftLog): Option[Snapshot] =
  ## Load the current snapshot
  let key = encodeSnapshotKey(log.rangeId)
  let value = log.store.get(key)

  if value.isSome:
    let json = parseJson(value.get)
    var config: seq[ReplicaDescriptor]
    for repJson in json["configuration"]:
      config.add(parseReplicaDescriptor(repJson))

    var stateMachineSnap: seq[byte]
    for b in json["stateMachineSnap"]:
      stateMachineSnap.add(byte(b.getInt()))

    result = some(Snapshot(
      rangeId: RangeID(uint64(json["rangeId"].getInt())),
      raftSnap: RaftSnapshotMeta(
        lastIncludedIndex: uint64(json["lastIncludedIndex"].getInt()),
        lastIncludedTerm: uint64(json["lastIncludedTerm"].getInt()),
        configuration: config
      ),
      stateMachineSnap: stateMachineSnap
    ))
