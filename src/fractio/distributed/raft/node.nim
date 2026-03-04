# Raft Node Implementation

import std/json
import std/strutils
import std/streams
import std/sets
import std/sequtils
import std/tables
import std/times
import std/options
import std/os
import std/locks

import fractio/utils/logging
import fractio/distributed/raft/types
import fractio/storage/backend
import fractio/storage/wisckey_backend

type
  WiscKeyLogStore* = ref object of RaftLogStore
    ## WiscKey-based log store for Raft
    backend*: WiscKeyBackend
    path*: string # Store path for lock file cleanup
    startIndex*: int64
    nextIndex*: int64
    lock*: Lock   # Lock for thread-safe index assignment

  RaftRPC* = object
    ## Raft RPC message
    rpcType*: RPCType
    term*: int64
    leaderId*: int32
    prevLogIndex*: int64
    prevLogTerm*: int64
    entries*: seq[LogEntry]
    leaderCommit*: int64
    success*: bool
    data*: string

  RPCType* = enum
    RPC_APPEND_ENTRIES
    RPC_REQUEST_VOTE
    RPC_CLIENT_REQUEST

  RaftNodeImpl* = ref object of RaftNode
    ## Internal Raft node implementation
    lastHeartbeat*: int64
    lastVoteTerm*: int64
    voteCount*: int
    logEntries*: seq[LogEntry]
    pendingEntries*: seq[LogEntry]
    wsLogStore*: WiscKeyLogStore # Concrete log store type

  RaftError* = object of CatchableError
    ## Raft-specific errors

# Forward declarations
proc handleRPCAsFollower*(node: RaftNodeImpl, rpc: RaftRPC): RaftRPC
proc handleRPCAsCandidate*(node: RaftNodeImpl, rpc: RaftRPC): RaftRPC
proc handleRPCAsLeader*(node: RaftNodeImpl, rpc: RaftRPC): RaftRPC

proc newWiscKeyLogStore*(path: string): WiscKeyLogStore =
  ## Create a new WiscKey-based log store
  new(result)
  result.path = path
  result.backend = newWiscKeyBackend(StorageConfig(
    path: path,
    createIfMissing: true,
    syncWrites: true
  ))

  if not result.backend.open(StorageConfig(
    path: path,
    createIfMissing: true,
    syncWrites: true
  )):
    raise newException(RaftError, "Failed to open WiscKey backend")

  # Initialize start index (this would need to be persisted)
  result.startIndex = 1
  result.nextIndex = 1
  initLock(result.lock)

proc loadWiscKeyLogStore*(path: string): WiscKeyLogStore =
  ## Load an existing WiscKey-based log store and recover the next index
  new(result)
  result.path = path

  # On Linux, LevelDB uses flock which is process-wide. If the database
  # was closed properly, the lock file should be removable.
  # Try removing the lock file before opening.
  let lockFile = path & "/LOCK"
  if fileExists(lockFile):
    try:
      removeFile(lockFile)
    except:
      discard

  result.backend = newWiscKeyBackend(StorageConfig(
    path: path,
    createIfMissing: false,
    syncWrites: true
  ))

  if not result.backend.open(StorageConfig(
    path: path,
    createIfMissing: false,
    syncWrites: true
  )):
    raise newException(RaftError, "Failed to open WiscKey backend")

  # Recover next index by finding the last entry
  result.startIndex = 1
  result.nextIndex = 1

  # Scan to find the highest index
  var highestIdx: int64 = 0
  let iter = result.backend.newIterator()
  if iter != nil:
    var currentIter = WiscKeyIterator(iter)
    if seekToFirstWiscKey(currentIter):
      while validWiscKey(currentIter):
        let key = keyWiscKey(currentIter)
        try:
          let idx = parseInt(key)
          if idx > highestIdx:
            highestIdx = idx
        except:
          discard
        discard nextWiscKey(currentIter)
    destroyIter(iter)

  if highestIdx > 0:
    result.nextIndex = highestIdx + 1

  initLock(result.lock)

proc init*(node: RaftNodeImpl, config: RaftConfig,
    stateMachine: StateMachine): bool =
  ## Initialize the Raft node
  # If already initialized with the same config, return success (idempotent)
  if node.initialized and node.wsLogStore != nil:
    return true

  node.serverId = config.serverId
  node.endpoint = config.endpoint
  node.config = config
  node.nodeState = RaftNodeState(
    role: SR_FOLLOWER,
    currentTerm: 0,
    votedFor: -1,
    leaderId: -1,
    commitIndex: 0,
    lastApplied: 0
  )

  # Create or load log store depending on whether path exists
  if dirExists(config.logStoragePath):
    node.wsLogStore = loadWiscKeyLogStore(config.logStoragePath)
  else:
    # Create parent directory if needed
    let parentDir = parentDir(config.logStoragePath)
    if parentDir.len > 0 and not dirExists(parentDir):
      createDir(parentDir)
    node.wsLogStore = newWiscKeyLogStore(config.logStoragePath)
  node.logStore = node.wsLogStore # Also set base type
  node.stateMachine = stateMachine
  node.initialized = true
  node.isLeader = false
  node.leaderId = -1

  var fields = initTable[string, string]()
  fields["serverId"] = $config.serverId
  fields["endpoint"] = config.endpoint
  debug("Raft node initialized", fields)
  return true

proc shutdown*(node: RaftNodeImpl) =
  ## Shutdown the Raft node
  if node.wsLogStore != nil:
    node.wsLogStore.close()
  node.initialized = false
  var fields = initTable[string, string]()
  fields["serverId"] = $node.serverId
  debug("Raft node shutdown", fields)

proc appendEntry*(store: WiscKeyLogStore, entry: LogEntry): int64 =
  ## Append a log entry to the store (thread-safe)
  if store.backend == nil:
    raise newException(RaftError, "Log store not initialized")

  # Thread-safe index assignment
  withLock store.lock:
    result = store.nextIndex
    inc store.nextIndex

  # Serialize entry
  let data = entry.data
  let serialized = """{"term": """ & $entry.term &
    ", \"type\": \"" & $entry.entryType &
    "\", \"data\": \"" & data.replace("\"", "\\\"") & "\"}"

  # Write to WiscKey - use index as key
  let key = $result
  if not store.backend.put(key, serialized):
    raise newException(RaftError, "Failed to write log entry")

proc getEntry*(store: WiscKeyLogStore, index: int64): Option[LogEntry] =
  ## Get a log entry by index
  if store.backend == nil:
    raise newException(RaftError, "Log store not initialized")

  let key = $index
  if not store.backend.exists(key):
    return none(LogEntry)

  let value = store.backend.get(key)
  if value.isNone:
    return none(LogEntry)

  # Deserialize entry
  try:
    let jsonNode = parseJson(value.get)
    result = some(LogEntry(
      term: jsonNode["term"].getInt(),
      entryType: parseEnum[LogEntryType](jsonNode["type"].getStr()),
      data: jsonNode["data"].getStr()
    ))
  except JsonParsingError:
    var fields = initTable[string, string]()
    fields["index"] = $index
    warn("Failed to parse log entry", fields)
    return none(LogEntry)

proc getEntries*(store: WiscKeyLogStore, start: int64, endIndex: int64): seq[LogEntry] =
  ## Get a range of log entries
  if store.backend == nil:
    raise newException(RaftError, "Log store not initialized")

  for i in start..endIndex:
    let entry = store.getEntry(i)
    if entry.isSome:
      result.add(entry.get)

proc getLastEntry*(store: WiscKeyLogStore): Option[LogEntry] =
  ## Get the last log entry
  if store.nextIndex == 1:
    return none(LogEntry)

  return store.getEntry(store.nextIndex - 1)

method close*(store: WiscKeyLogStore) =
  ## Close the log store and release the lock file
  if store.backend != nil:
    store.backend.close()
    store.backend = nil
  # Remove the lock file to allow reopening
  let lockFile = store.path & "/LOCK"
  if fileExists(lockFile):
    try:
      removeFile(lockFile)
    except:
      discard
  deinitLock(store.lock)

proc handleRPC*(node: RaftNodeImpl, rpc: RaftRPC): RaftRPC =
  ## Handle an incoming RPC
  var fields = initTable[string, string]()
  fields["rpcType"] = $rpc.rpcType
  fields["term"] = $rpc.term
  fields["serverId"] = $node.serverId
  debug("Handling RPC", fields)

  case node.nodeState.role
  of SR_FOLLOWER:
    return handleRPCAsFollower(node, rpc)
  of SR_CANDIDATE:
    return handleRPCAsCandidate(node, rpc)
  of SR_LEADER:
    return handleRPCAsLeader(node, rpc)

proc handleRPCAsFollower*(node: RaftNodeImpl, rpc: RaftRPC): RaftRPC =
  ## Handle RPC when in follower role
  case rpc.rpcType
  of RPC_APPEND_ENTRIES:
    # Handle heartbeat or append entries
    if rpc.term >= node.nodeState.currentTerm:
      node.nodeState.currentTerm = rpc.term
      node.nodeState.leaderId = rpc.leaderId
      node.lastHeartbeat = getTime().toUnix

      # Apply entries if any
      if rpc.entries.len > 0:
        for entry in rpc.entries:
          let idx = node.wsLogStore.appendEntry(entry)
          # Update commit index if needed
          if rpc.leaderCommit > node.nodeState.commitIndex:
            node.nodeState.commitIndex = min(rpc.leaderCommit, idx)

      return RaftRPC(
        rpcType: RPC_APPEND_ENTRIES,
        term: node.nodeState.currentTerm,
        leaderId: node.serverId,
        success: true
      )
  of RPC_REQUEST_VOTE:
    # Handle vote request
    if rpc.term > node.nodeState.currentTerm or
        (rpc.term == node.nodeState.currentTerm and node.nodeState.votedFor == -1):
      node.nodeState.currentTerm = rpc.term
      node.nodeState.votedFor = rpc.leaderId
      node.lastHeartbeat = getTime().toUnix

      return RaftRPC(
        rpcType: RPC_REQUEST_VOTE,
        term: node.nodeState.currentTerm,
        leaderId: node.serverId,
        success: true
      )
  of RPC_CLIENT_REQUEST:
    # Forward to leader
    return RaftRPC(
      rpcType: RPC_CLIENT_REQUEST,
      term: node.nodeState.currentTerm,
      leaderId: node.nodeState.leaderId,
      success: false
    )

proc handleRPCAsLeader*(node: RaftNodeImpl, rpc: RaftRPC): RaftRPC =
  ## Handle RPC when in leader role
  if rpc.term > node.nodeState.currentTerm:
    # Step down if we see a higher term
    node.nodeState.role = SR_FOLLOWER
    node.nodeState.currentTerm = rpc.term
    node.nodeState.votedFor = -1
    node.isLeader = false
    return handleRPCAsFollower(node, rpc)

  # Handle specific RPC types
  case rpc.rpcType
  of RPC_APPEND_ENTRIES:
    # This should be a heartbeat from another leader (we should have stepped down)
    return handleRPCAsFollower(node, rpc)
  of RPC_REQUEST_VOTE:
    # Another candidate is requesting votes - deny
    return RaftRPC(
      rpcType: RPC_REQUEST_VOTE,
      term: node.nodeState.currentTerm,
      leaderId: node.serverId,
      success: false
    )
  of RPC_CLIENT_REQUEST:
    # Handle client request - we're the leader
    if node.stateMachine != nil:
      let idx = node.wsLogStore.appendEntry(LogEntry(
        term: node.nodeState.currentTerm,
        entryType: LET_NORMAL,
        data: rpc.data
      ))

      # Update commit index if we have a majority (simplified)
      node.nodeState.commitIndex = idx

      return RaftRPC(
        rpcType: RPC_CLIENT_REQUEST,
        term: node.nodeState.currentTerm,
        leaderId: node.serverId,
        success: true,
        data: "Request processed at index " & $idx
      )
    else:
      return RaftRPC(
        rpcType: RPC_CLIENT_REQUEST,
        term: node.nodeState.currentTerm,
        leaderId: node.serverId,
        success: false,
        data: "No state machine configured"
      )

proc handleRPCAsCandidate*(node: RaftNodeImpl, rpc: RaftRPC): RaftRPC =
  ## Handle RPC when in candidate role
  case rpc.rpcType
  of RPC_APPEND_ENTRIES:
    # If we get an append entries from a valid leader, step down
    if rpc.term >= node.nodeState.currentTerm:
      node.nodeState.role = SR_FOLLOWER
      node.nodeState.currentTerm = rpc.term
      node.nodeState.votedFor = -1
      node.isLeader = false
      return handleRPCAsFollower(node, rpc)
  of RPC_REQUEST_VOTE:
    # Deny votes to other candidates
    return RaftRPC(
      rpcType: RPC_REQUEST_VOTE,
      term: node.nodeState.currentTerm,
      leaderId: node.serverId,
      success: false
    )
  of RPC_CLIENT_REQUEST:
    # Forward to leader if we know who it is
    if node.nodeState.leaderId != -1:
      return RaftRPC(
        rpcType: RPC_CLIENT_REQUEST,
        term: node.nodeState.currentTerm,
        leaderId: node.nodeState.leaderId,
        success: false
      )

proc becomeLeader*(node: RaftNodeImpl) =
  ## Transition to leader state
  if node.nodeState.role != SR_CANDIDATE:
    return

  node.nodeState.role = SR_LEADER
  node.isLeader = true
  node.nodeState.leaderId = node.serverId
  var fields = initTable[string, string]()
  fields["serverId"] = $node.serverId
  fields["term"] = $node.nodeState.currentTerm
  debug("Became leader", fields)

proc becomeCandidate*(node: RaftNodeImpl) =
  ## Transition to candidate state
  node.nodeState.role = SR_CANDIDATE
  node.nodeState.currentTerm += 1
  node.nodeState.votedFor = node.serverId
  node.lastVoteTerm = node.nodeState.currentTerm
  var fields = initTable[string, string]()
  fields["serverId"] = $node.serverId
  fields["term"] = $node.nodeState.currentTerm
  debug("Became candidate", fields)

proc startElection*(node: RaftNodeImpl) =
  ## Start a new election
  if node.nodeState.role == SR_LEADER:
    return

  becomeCandidate(node)

  # In a real implementation, we would send RequestVote RPCs to all other nodes
  var fields = initTable[string, string]()
  fields["serverId"] = $node.serverId
  fields["term"] = $node.nodeState.currentTerm
  debug("Started election", fields)

proc stepDown*(node: RaftNodeImpl, term: int64) =
  ## Step down to follower if we see a higher term
  if term > node.nodeState.currentTerm:
    node.nodeState.role = SR_FOLLOWER
    node.nodeState.currentTerm = term
    node.nodeState.votedFor = -1
    node.isLeader = false
    var fields = initTable[string, string]()
    fields["serverId"] = $node.serverId
    fields["term"] = $term
    debug("Stepped down to follower", fields)

proc appendEntries*(node: RaftNodeImpl, entries: seq[LogEntry]): int64 =
  ## Append entries to log and return the index of the last entry
  for entry in entries:
    result = node.wsLogStore.appendEntry(entry)

  return result

proc commit*(node: RaftNodeImpl, data: string): int64 =
  ## Commit data to the Raft log
  if not node.isLeader:
    raise newException(RaftError, "Only leader can commit")

  let entry = LogEntry(
    term: node.nodeState.currentTerm,
    entryType: LET_NORMAL,
    data: data
  )

  return node.appendEntries(@[entry])
