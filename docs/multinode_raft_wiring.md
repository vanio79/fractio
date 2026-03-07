# Multi-Node Raft Wiring Implementation Plan

**Status:** Ready to implement  
**Depends on:** Phase 1–5 protocol layer (complete)  
**Estimated scope:** ~2500 lines across 6 files, 4 implementation phases

---

## 1. Problem Statement

`MultiRaftCoordinator` currently operates as a single-node stub. In `workerProc`, after
appending a log entry, it immediately advances `commitIndex` to that entry and signals
success — there is no replication to peers, no quorum check, no election. The
`checkElectionTimeout` and `sendHeartbeats` procs exist but are never called by a timer
thread, and they contain no network I/O.

The result: every deployment is a cluster of one. Writes are durable (via LevelDB/WiscKey)
but not replicated. Any node failure loses data with no recovery.

**Goal of this document:** specify exactly what must be added, modified, or wired together
to produce a working multi-node Raft implementation that fits the existing codebase with
minimal structural disruption.

---

## 2. What Already Exists (Do Not Rewrite)

| File | What it provides |
|---|---|
| `multigroup_types.nim` | `RaftGroup`, `RaftState`, `LogEntry`, `WriteBatch`, `RaftCommand`, `RaftPersistentState`, `Snapshot`, `Proposal`, `ProposalResultChannel` — all correct, all used |
| `multigroup_log.nim` | `RaftLog`: `putEntry`, `getEntry`, `getEntries`, `truncate`, `compact`, `saveState`, `loadState`, `saveSnapshot`, `loadSnapshot`, `recoverLog` — correct, fully implemented |
| `multigroup_coordinator.nim` | `MultiRaftCoordinator` struct, `createGroup`, `getGroup`, `proposeAndWait` (correct ORC-safe impl), worker thread skeleton — keep all, extend |
| `range/types.nim` | `NodeID`, `RangeID`, `ReplicaID`, `ReplicaDescriptor`, `RangeDescriptor`, `quorumSize`, all key-encoding helpers — complete |
| `protocol/raft_store.nim` | `RaftKVStoreExt`, `proposeWrite`, full KV + intent + COORD API — no changes needed |
| `protocol/raft_txn.nim` | `RaftTxnCoordinator`, 2PC, recovery — no changes needed |
| `protocol/server.nim` | `ProtocolServer`, `handleBuiltinKV` with Raft path — no changes needed |
| `protocol/router.nim` | `RouterTable`, `LeaderChangeCallback`, `notLeaderRedirect` — no changes needed |
| `protocol/client.nim` | `ProtocolClient`, full KV/txn/admin API — needs one addition (§6.4) |

---

## 3. New Files to Create

```
src/fractio/distributed/raft/
  transport.nim          # RaftTransport interface + TCP implementation
  peer_manager.nim       # Per-group peer state (nextIndex, matchIndex, in-flight RPCs)
  election_timer.nim     # Per-group randomised election timer thread
  
src/fractio/
  server_main.nim        # Top-level binary entry point (reads config, starts cluster)

tests/protocol/
  test_multinode_raft.nim  # 3-node cluster tests (ports 20200–20299)
```

---

## 4. Wire Protocol for Inter-Node RPCs

All Raft RPCs between nodes use the existing `TCPTransport` framing from
`distributed/network/tcp_transport.nim` **but as a separate listener** from the client port.
Each node listens on two ports:

| Port offset | Purpose |
|---|---|
| `basePort` | Client-facing (`ProtocolServer`) |
| `basePort + 1000` | Raft peer-to-peer RPCs (`RaftTransport`) |

### 4.1 Message type codes (Raft peer channel)

These are **not** exposed to clients. They live in the 0x0600 range (Replication) reserved
in `protocol_design.md`:

```nim
const
  RaftMsgRequestVote*     = 0x0601'u16
  RaftMsgRequestVoteResp* = 0x0602'u16
  RaftMsgAppendEntries*   = 0x0603'u16
  RaftMsgAppendEntriesResp* = 0x0604'u16
  RaftMsgInstallSnapshot* = 0x0605'u16
  RaftMsgInstallSnapshotResp* = 0x0606'u16
```

### 4.2 Message encoding

All messages use the existing `codec.nim` helpers (big-endian, length-prefixed). No new
serialisation library.

#### RequestVote (0x0601)
```
groupId       uint64   # RangeID of the Raft group this election is for
term          uint64   # candidate's current term
candidateId   uint32   # NodeID of candidate
lastLogIndex  uint64
lastLogTerm   uint64
```

#### RequestVoteResp (0x0602)
```
groupId       uint64
term          uint64   # respondent's current term (may be higher than candidate's)
voteGranted   uint8    # 1 = granted, 0 = denied
```

#### AppendEntries (0x0603)
```
groupId       uint64
term          uint64   # leader's term
leaderId      uint32   # NodeID of leader (for NOT_LEADER hints)
prevLogIndex  uint64
prevLogTerm   uint64
leaderCommit  uint64
entryCount    uint32   # number of LogEntry records following
entries       [entryCount × encoded LogEntry]
```

Each `LogEntry` is encoded as:
```
term          uint64
index         uint64
commandKind   uint8
commandData   bytes (length-prefixed uint32)
```

`commandData` reuses the JSON encoding already in `multigroup_log.nim::encodeEntry` for
the command payload — keep the same format to avoid touching the storage layer.

#### AppendEntriesResp (0x0604)
```
groupId       uint64
term          uint64
success       uint8    # 1 = ok, 0 = consistency check failed
matchIndex    uint64   # highest index follower has accepted (on success)
conflictIndex uint64   # first index of conflicting term (on failure, for fast backup)
conflictTerm  uint64   # term of entry at conflictIndex (0 if none)
```

#### InstallSnapshot (0x0605)
```
groupId             uint64
term                uint64
leaderId            uint32
lastIncludedIndex   uint64
lastIncludedTerm    uint64
configEntryCount    uint32
configEntries       [configEntryCount × ReplicaDescriptor (nodeId uint32, replicaId uint32, replicaType uint8)]
dataLen             uint32
data                bytes   # full KVStateMachine snapshot
```

#### InstallSnapshotResp (0x0606)
```
groupId       uint64
term          uint64
success       uint8
```

---

## 5. Implementation Phases

### Phase A — Transport layer (`transport.nim`)

**Scope:** ~400 lines. No changes to any existing file.

```nim
# src/fractio/distributed/raft/transport.nim

type
  RaftPeer* = object
    nodeId*: NodeID
    host*:   string
    port*:   int          # Raft peer port (basePort + 1000)

  SendResult* = enum
    srOK, srTimeout, srConnRefused, srUnknown

  RaftTransport* = ref object
    ## Listens for incoming Raft RPCs and sends outgoing ones.
    ## One instance per node, shared across all Raft groups.
    localNodeId*: NodeID
    listenPort*:  int
    peers*:       Table[NodeID, RaftPeer]
    peersLock*:   Lock
    ## Incoming message dispatch: groupId → handler proc
    handlers*:    Table[uint16, proc(data: string): string {.gcsafe, raises: [].}]
    handlersLock*: Lock
    serverSock*:  Socket
    running*:     Atomic[bool]
    acceptThread*: Thread[RaftTransport]
    connPool*:    Table[NodeID, Socket]   # outbound persistent connections
    connLock*:    Lock
```

**Key procs to implement:**

```nim
proc newRaftTransport*(localNodeId: NodeID, listenPort: int): RaftTransport

proc addPeer*(t: RaftTransport, peer: RaftPeer)
proc removePeer*(t: RaftTransport, nodeId: NodeID)

proc registerHandler*(t: RaftTransport, msgType: uint16,
    handler: proc(data: string): string {.gcsafe, raises: [].})

proc start*(t: RaftTransport)
  ## Binds listenPort, spawns acceptThread.

proc stop*(t: RaftTransport)

proc send*(t: RaftTransport, nodeId: NodeID, msgType: uint16,
    payload: string): SendResult {.gcsafe, raises: [].}
  ## Sends msgType+payload to nodeId over persistent outbound TCP connection.
  ## Establishes connection on first use; reconnects on failure.
  ## Returns srTimeout after 2 s; srConnRefused if peer is unreachable.
  ## Fire-and-forget — does NOT wait for a reply.

proc sendAndRecv*(t: RaftTransport, nodeId: NodeID, msgType: uint16,
    payload: string, timeoutMs: int = 2000): Option[string] {.gcsafe, raises: [].}
  ## Sends msgType+payload and waits up to timeoutMs for a response frame.
  ## Returns none on timeout or connection failure.
```

**Frame format on the Raft peer channel:**

```
msgType   uint16 BE
length    uint32 BE
payload   [length bytes]
```

This is deliberately simpler than the client protocol frames (no requestId, no CRC, no
flags). Raft RPCs are idempotent; the caller handles retries.

**Accept loop:** for each accepted connection, spawn a short-lived thread (or reuse a pool
of 4 reader threads) that reads frames in a loop, dispatches to the registered handler for
`msgType`, writes the returned string as the response frame, and loops.

**Connection pool:** outbound connections per `NodeID` are cached. On `send`/`sendAndRecv`,
acquire the connection under `connLock`, write the frame, read the response (for
`sendAndRecv`), release. On any socket error, close and remove from pool so the next call
reconnects.

---

### Phase B — Election and heartbeat timer (`election_timer.nim`)

**Scope:** ~250 lines. Replaces the empty `checkElectionTimeout` / `sendHeartbeats` stubs
in `multigroup_coordinator.nim`.

```nim
# src/fractio/distributed/raft/election_timer.nim

type
  TimerContext* = object
    coordinator*: MultiRaftCoordinator
    transport*:   RaftTransport

proc timerProc*(ctx: TimerContext) {.thread.}
  ## Single thread shared across all groups on this node.
  ## Runs every 10 ms, checks each group independently.
```

**`timerProc` loop (every 10 ms):**

```
for each (rangeId, group) in coordinator.groups:  # copy under groupsLock, then iterate
    state = group.state.load()
    
    if state == rsLeader:
        if time_since_last_heartbeat >= heartbeatIntervalNs:
            sendHeartbeatsForGroup(coordinator, transport, group)
    
    else:  # Follower or Candidate
        elapsed = group.timeSinceHeartbeat()
        if elapsed >= randomisedElectionTimeout(group):
            startElectionForGroup(coordinator, transport, group)
```

**Randomised election timeout:**

```nim
proc randomisedElectionTimeout(group: RaftGroup): int64 =
  ## Returns electionTimeoutNs + rand(0..electionTimeoutNs) for this group.
  ## Seed from group.rangeId xor group.replicaId to get deterministic spread
  ## per replica without a shared RNG (avoids lock).
  let seed = group.rangeId.uint64 xor group.replicaId.uint64 xor
              uint64(getTime().toUnixFloat() * 1e9)
  let jitter = int64(seed mod uint64(coordinator.electionTimeoutNs))
  coordinator.electionTimeoutNs + jitter
```

**`startElectionForGroup`:**

1. Call `group.becomeCandidate()` — increments term, votes for self, clears `votesGranted`.
2. Persist new term + vote via `log.saveState(...)`.
3. Build `RequestVote` payload.
4. For each peer replica in `group.descriptor.replicas` where `nodeId != localNodeId`:
   - `transport.sendAndRecv(peer.nodeId, RaftMsgRequestVote, payload, 500)` in a separate
     short-lived thread (spawn N threads, collect results with a `Channel`).
5. Count granted votes. If `group.hasQuorum(granted + 1)`:
   - `group.becomeLeader()`.
   - Persist state.
   - Immediately send no-op `AppendEntries` to all peers (establishes leadership, commits
     any uncommitted entries from previous term).
6. If a peer responds with `term > group.currentTerm`: `group.becomeFollower(term)`,
   persist, stop election.

**`sendHeartbeatsForGroup`:**

For each peer: send `AppendEntries` with no entries (`entryCount = 0`) carrying
`leaderCommit = group.commitIndex.load()`. This serves as the heartbeat. If the peer
responds with `success = 0` and a conflict index, schedule a full `AppendEntries` retry
(see Phase C).

---

### Phase C — Log replication in `workerProc`

**Scope:** ~200 lines replacing 15 lines in `multigroup_coordinator.nim`.

This is the most important change. Replace the single-node quorum shortcut:

**Current (remove):**
```nim
# Update commit index (simplified — single-node quorum)
group.commitIndex.store(index)
sendResult(proposal.resultPtr, RaftResult(success: true, index: index))
```

**Replacement:**

```nim
# 1. Append to local log (already done above — keep that part)
log.putEntry(entry)

# 2. Persist term + vote before responding to any peer
log.saveState(RaftPersistentState(
    currentTerm: group.currentTerm.load(),
    votedFor:    group.votedFor.load(),
    commitIndex: group.commitIndex.load(),
    lastApplied: group.lastApplied.load(),
))

# 3. Replicate to all peers concurrently, then wait for quorum
let voters = group.descriptor.getVoters()
let quorumNeeded = group.quorum()   # majority of voters

if voters.len == 1:
    # Single-voter group (test / single-node deployment) — commit immediately
    group.commitIndex.store(index)
    applyToStateMachine(c, group.rangeId, index)
    sendResult(proposal.resultPtr, RaftResult(success: true, index: index))
else:
    replicateAndWait(c, group, log, entry, index, quorumNeeded, proposal.resultPtr)
```

**`replicateAndWait` (new proc):**

```nim
proc replicateAndWait(c: MultiRaftCoordinator, group: RaftGroup, log: RaftLog,
    entry: LogEntry, index: uint64, quorumNeeded: int,
    resultPtr: ptr ProposalResultChannel) =
  ## Send AppendEntries to all peer replicas, wait for quorum acknowledgements,
  ## then advance commitIndex and apply the entry to the state machine.
  let localNodeId = c.nodeId
  let term = group.currentTerm.load()
  let commitIndex = group.commitIndex.load()
  let prevIndex = if index > 1: index - 1 else: 0
  let prevTerm = if prevIndex > 0:
    let prev = log.getEntry(prevIndex)
    if prev.isSome: prev.get.term else: 0'u64
  else: 0'u64

  let payload = encodeAppendEntries(
    rangeId      = group.rangeId,
    term         = term,
    leaderId     = localNodeId,
    prevLogIndex = prevIndex,
    prevLogTerm  = prevTerm,
    leaderCommit = commitIndex,
    entries      = @[entry],
  )

  # Fan-out: one short-lived thread per peer
  var ackCh: Channel[bool]
  ackCh.open(group.descriptor.replicas.len)

  for rep in group.descriptor.replicas:
    if rep.nodeId == localNodeId: continue
    let peerNodeId = rep.nodeId
    let peerPayload = payload  # copy
    spawn proc() {.gcsafe.} =
      let resp = c.transport.sendAndRecv(peerNodeId, RaftMsgAppendEntries,
                                          peerPayload, 2000)
      if resp.isSome:
        let (success, matchIdx, conflictIdx, conflictTerm) =
            decodeAppendEntriesResp(resp.get)
        if success:
          withLock group.lock:
            group.matchIndex[rep.replicaId] = matchIdx
            group.nextIndex[rep.replicaId]  = matchIdx + 1
          ackCh.send(true)
          return
        else:
          # Log inconsistency: back up nextIndex and retry (async, best-effort)
          scheduleRetry(c, group, rep, conflictIdx, conflictTerm)
      ackCh.send(false)

  # Count acks (local replica always counts as 1)
  var acks = 1
  let peersToWait = group.descriptor.replicas.len - 1
  let deadline = getTime().toUnix * 1000 + c.config.proposeTimeoutMs
  for _ in 0..<peersToWait:
    let (ok, ack) = ackCh.tryRecv()
    if ok and ack: inc acks
    if getTime().toUnix * 1000 >= deadline: break
    if acks >= quorumNeeded: break
    sleep(1)
  ackCh.close()

  if acks >= quorumNeeded:
    # Quorum reached — advance commitIndex to max index with quorum support
    let newCommit = computeNewCommitIndex(group)
    if newCommit > group.commitIndex.load():
      group.commitIndex.store(newCommit)
      applyToStateMachine(c, group.rangeId, newCommit)
      log.saveState(RaftPersistentState(
          currentTerm: term,
          votedFor:    group.votedFor.load(),
          commitIndex: newCommit,
          lastApplied: newCommit,
      ))
    sendResult(resultPtr, RaftResult(success: true, index: index))
  else:
    sendResult(resultPtr, RaftResult(success: false,
        error: "Failed to reach quorum"))
```

**`computeNewCommitIndex`:** The new commit index is the highest index `N` such that:
- `N > currentCommitIndex`
- A majority of `matchIndex[replicaId] >= N`
- `log.getEntry(N).term == group.currentTerm.load()`  (Raft safety: only commit from
  current term)

**`applyToStateMachine`:** Iterate from `lastApplied + 1` to `newCommit`, call
`applyEntry(coordinator, rangeId, entry)` for each. This is the bridge to
`RaftKVStoreExt`'s local `KVStateMachine` — currently `proposeWrite` applies the batch
itself after `proposeAndWait` returns. With multi-node replication the application must
happen here instead. See §7.1 for the required change to `raft_store.nim`.

---

### Phase D — Incoming RPC handlers (wired into `MultiRaftCoordinator`)

**Scope:** ~350 lines added to `multigroup_coordinator.nim` (or a new
`multigroup_rpc.nim`).

Add a `transport: RaftTransport` field to `MultiRaftCoordinator` and register four
handlers in `start()`:

#### D.1 `handleRequestVote`

```nim
proc handleRequestVote(c: MultiRaftCoordinator, data: string): string =
  let (groupId, term, candidateId, lastLogIndex, lastLogTerm) =
      decodeRequestVote(data)
  let rangeId = RangeID(groupId)
  let groupOpt = c.getGroup(rangeId)
  if groupOpt.isNone:
    return encodeRequestVoteResp(groupId, 0, false)
  let group = groupOpt.get
  let log = ...  # getLog(rangeId) under groupsLock

  withLock group.lock:
    let myTerm = group.currentTerm.load()

    # Step down if we see a higher term
    if term > myTerm:
      group.state.store(rsFollower)
      group.currentTerm.store(term)
      group.votedFor.store(ReplicaID(0))

    # Grant vote iff:
    #   1. term >= myTerm
    #   2. haven't voted this term, or already voted for this candidate
    #   3. candidate's log is at least as up-to-date as ours
    let currentTerm = group.currentTerm.load()
    let votedFor = group.votedFor.load()
    let myLastIndex = log.lastIndex.load()
    let myLastTerm = if myLastIndex > 0:
        let e = log.getEntry(myLastIndex); if e.isSome: e.get.term else: 0'u64
      else: 0'u64

    let logOK = (lastLogTerm > myLastTerm) or
                (lastLogTerm == myLastTerm and lastLogIndex >= myLastIndex)
    let voteOK = (term >= currentTerm) and
                 (votedFor.uint32 == 0 or votedFor.uint32 == candidateId.uint32) and
                 logOK

    if voteOK:
      group.votedFor.store(ReplicaID(candidateId))
      group.updateHeartbeat()   # reset election timer
      log.saveState(RaftPersistentState(
          currentTerm: currentTerm,
          votedFor:    ReplicaID(candidateId),
          commitIndex: group.commitIndex.load(),
          lastApplied: group.lastApplied.load(),
      ))

    return encodeRequestVoteResp(groupId, currentTerm, voteOK)
```

#### D.2 `handleAppendEntries`

```nim
proc handleAppendEntries(c: MultiRaftCoordinator, data: string): string =
  let (groupId, term, leaderId, prevLogIndex, prevLogTerm,
       leaderCommit, entries) = decodeAppendEntries(data)
  let rangeId = RangeID(groupId)
  let groupOpt = c.getGroup(rangeId)
  if groupOpt.isNone:
    return encodeAppendEntriesResp(groupId, 0, false, 0, 0, 0)
  let group = groupOpt.get

  withLock group.lock:
    let myTerm = group.currentTerm.load()

    # Reject stale leader
    if term < myTerm:
      return encodeAppendEntriesResp(groupId, myTerm, false, 0, 0, 0)

    # Accept valid leader — step down if necessary
    if term > myTerm:
      group.currentTerm.store(term)
      group.votedFor.store(ReplicaID(0))
    group.state.store(rsFollower)
    group.updateHeartbeat()   # suppress election timer

    let log = c.getLog(rangeId)  # helper, under groupsLock

    # Consistency check
    if prevLogIndex > 0:
      let prevEntry = log.getEntry(prevLogIndex)
      if prevEntry.isNone or prevEntry.get.term != prevLogTerm:
        # Find conflicting term's first index for fast backup
        let conflictTerm = if prevEntry.isSome: prevEntry.get.term else: 0'u64
        var conflictIndex = prevLogIndex
        if conflictTerm > 0:
          while conflictIndex > 1:
            let e = log.getEntry(conflictIndex - 1)
            if e.isNone or e.get.term != conflictTerm: break
            dec conflictIndex
        return encodeAppendEntriesResp(groupId, myTerm, false, 0,
                                        conflictIndex, conflictTerm)

    # Append entries (truncating any conflicting tail first)
    for entry in entries:
      let existing = log.getEntry(entry.index)
      if existing.isSome and existing.get.term != entry.term:
        log.truncate(entry.index)
      if existing.isNone:
        log.putEntry(entry)

    # Advance commit index
    let matchIndex = if entries.len > 0: entries[^1].index
                     else: prevLogIndex
    if leaderCommit > group.commitIndex.load():
      let newCommit = min(leaderCommit, matchIndex)
      group.commitIndex.store(newCommit)
      applyUpTo(c, rangeId, group, newCommit)

    log.saveState(RaftPersistentState(
        currentTerm: group.currentTerm.load(),
        votedFor:    group.votedFor.load(),
        commitIndex: group.commitIndex.load(),
        lastApplied: group.lastApplied.load(),
    ))

    return encodeAppendEntriesResp(groupId, myTerm, true, matchIndex, 0, 0)
```

#### D.3 `handleInstallSnapshot`

Called when a follower is too far behind to catch up via log entries.

```nim
proc handleInstallSnapshot(c: MultiRaftCoordinator, data: string): string =
  let (groupId, term, leaderId, lastIncludedIndex, lastIncludedTerm,
       config, snapshotData) = decodeInstallSnapshot(data)
  let rangeId = RangeID(groupId)
  let groupOpt = c.getGroup(rangeId)
  if groupOpt.isNone:
    return encodeInstallSnapshotResp(groupId, 0, false)
  let group = groupOpt.get

  withLock group.lock:
    let myTerm = group.currentTerm.load()
    if term < myTerm:
      return encodeInstallSnapshotResp(groupId, myTerm, false)

    group.state.store(rsFollower)
    group.updateHeartbeat()

    # Apply snapshot to state machine
    applySnapshot(c, rangeId, snapshotData)

    # Compact log and update group state
    let log = c.getLog(rangeId)
    log.compact(lastIncludedIndex + 1)
    group.commitIndex.store(lastIncludedIndex)
    group.lastApplied.store(lastIncludedIndex)
    log.saveState(RaftPersistentState(
        currentTerm: term,
        votedFor:    group.votedFor.load(),
        commitIndex: lastIncludedIndex,
        lastApplied: lastIncludedIndex,
    ))

    return encodeInstallSnapshotResp(groupId, myTerm, true)
```

---

## 6. Changes to Existing Files

### 6.1 `multigroup_coordinator.nim`

**Add fields to `MultiRaftCoordinator`:**
```nim
transport*:          RaftTransport  # nil for single-node
proposeTimeoutMs*:   int            # default 5000; used in replicateAndWait
timerThread*:        Thread[TimerContext]
```

**Add to `CoordinatorConfig`:**
```nim
peers*:            seq[tuple[nodeId: NodeID, host: string, raftPort: int]]
raftListenPort*:   int    # 0 = no transport (single-node mode)
proposeTimeoutMs*: int    # default 5000
```

**Modify `start`:**
```nim
proc start*(c: MultiRaftCoordinator) =
  # ... existing worker thread startup ...

  if c.config.raftListenPort > 0:
    c.transport = newRaftTransport(c.nodeId, c.config.raftListenPort)
    for p in c.config.peers:
      c.transport.addPeer(RaftPeer(nodeId: p.nodeId, host: p.host, port: p.raftPort))
    c.transport.registerHandler(RaftMsgRequestVote,
        proc(d: string): string {.gcsafe, raises:[].} = c.handleRequestVote(d))
    c.transport.registerHandler(RaftMsgAppendEntries,
        proc(d: string): string {.gcsafe, raises:[].} = c.handleAppendEntries(d))
    c.transport.registerHandler(RaftMsgInstallSnapshot,
        proc(d: string): string {.gcsafe, raises:[].} = c.handleInstallSnapshot(d))
    c.transport.start()
    createThread(c.timerThread, timerProc, TimerContext(
        coordinator: c, transport: c.transport))
```

**Modify `stop`:**
```nim
if c.transport != nil:
  c.transport.stop()
  joinThread(c.timerThread)
```

**Modify `workerProc`:** Replace the single-node commit shortcut as described in §5 Phase C.

**Backward compatibility:** When `raftListenPort == 0` (existing tests), `transport` is
`nil` and `workerProc` falls through to the single-node path (`voters.len == 1` check).
All 352 existing tests continue to pass unchanged.

### 6.2 `raft_store.nim` — decouple apply from propose

Currently `proposeWrite` applies the `WriteBatch` to the local `KVStateMachine` after
`proposeAndWait` returns. For multi-node Raft this is correct only for the leader: the
leader applies after quorum. But `applyToStateMachine` in the coordinator must also be
able to apply entries on followers after `AppendEntries` advances `commitIndex`.

**Required change:** Extract the apply logic into a standalone exported proc:

```nim
proc applyBatchToSM*(store: RaftKVStoreExt, rangeId: RangeID,
    batch: WriteBatch) {.gcsafe, raises: [].} =
  ## Apply a WriteBatch directly to the KVStateMachine for rangeId.
  ## Called by the coordinator's applyToStateMachine / applyUpTo after
  ## commitIndex advances.  Thread-safe under smMu.
  let sm = store.getOrCreateSM(rangeId)
  acquire(store.smMu)
  defer: release(store.smMu)
  for (k, v) in batch.puts:
    sm.kvStore[fromBytes(k)] = fromBytes(v)
  for k in batch.deletes:
    sm.kvStore.del(fromBytes(k))
```

`proposeWrite` stays identical — for single-node or leader it still applies after
`proposeAndWait`. On followers, the coordinator calls `applyBatchToSM` directly from
`applyUpTo`.

The coordinator needs a reference to the `RaftKVStoreExt`. Add:
```nim
kvStore*: RaftKVStoreExt   # nil until set by raft_store bootstrapSingleShardExt
```
Set this field in `bootstrapSingleShardExt` / `newRaftKVStoreExt`.

### 6.3 `multigroup_coordinator.nim` — `applyUpTo` proc

```nim
proc applyUpTo(c: MultiRaftCoordinator, rangeId: RangeID,
    group: RaftGroup, upToIndex: uint64) =
  ## Apply all committed but unapplied log entries to the state machine.
  let startIdx = group.lastApplied.load() + 1
  if startIdx > upToIndex: return
  let log = c.getLog(rangeId)
  for idx in startIdx..upToIndex:
    let entryOpt = log.getEntry(idx)
    if entryOpt.isNone: break
    let entry = entryOpt.get
    case entry.command.kind
    of ckWrite:
      if c.kvStore != nil:
        c.kvStore.applyBatchToSM(rangeId, entry.command.writeBatch)
    of ckNoop: discard
    else: discard  # ckSplit, ckMerge etc. handled in a later phase
    group.lastApplied.store(idx)
```

### 6.4 `protocol/client.nim` — NOT_LEADER retry

Add a `leaderHints` table and auto-retry on `ErrNotLeader`:

```nim
type
  ProtocolClient* = ref object
    # ... existing fields ...
    leaderHints*: Table[string, string]  # key-range or "" -> "host:port"
    hintsLock*:   Lock
    autoRetry*:   bool  # default true
```

```nim
proc handleNotLeader(client: ProtocolClient, errorPayload: string): PResult =
  ## Parse the leader hint from the error response, reconnect, retry.
  ## errorPayload format: "host:port\x00<original-error-message>"
  let hint = errorPayload.split('\x00')[0]
  if hint.len == 0: return pErr(...)
  let parts = hint.split(':')
  if parts.len != 2: return pErr(...)
  let newCfg = ClientConfig(host: parts[0], port: parseInt(parts[1]),
                             timeoutMs: client.config.timeoutMs,
                             clientId: client.config.clientId)
  client.disconnect()
  client.config = newCfg
  client.connect()
```

The `send` proc checks if the response frame has `FlagError` set and the error code is
`ErrNotLeader`, calls `handleNotLeader`, and retries the original request once.

### 6.5 `server.nim` — include leader hint in NOT_LEADER error payload

Change the four `sendError(... ErrNotLeader ...)` calls to include the current known leader
address from `server.raftStore.coordinator`'s routing info in the error message body:

```nim
let hint = server.routerTable.getLeaderAddr(shardId)  # new helper on RouterTable
sendError(conn, requestId, ErrNotLeader, ErrCatKV, hint & "\x00not the leader")
```

---

## 7. Snapshot Support

Snapshots are needed when a follower's log has been compacted past the point needed to
catch it up with `AppendEntries`.

### 7.1 Taking a snapshot (leader)

```nim
proc takeSnapshot*(c: MultiRaftCoordinator, rangeId: RangeID): bool =
  ## Capture the current KVStateMachine state as a snapshot and compact the log.
  if c.kvStore == nil: return false
  let group = c.getGroup(rangeId)
  if group.isNone or not group.get.isLeader(): return false
  let g = group.get
  let commitIndex = g.commitIndex.load()
  let commitTerm = block:
    let e = c.getLog(rangeId).getEntry(commitIndex)
    if e.isNone: return false
    e.get.term

  # Serialise KVStateMachine state
  let sm = c.kvStore.getOrCreateSM(rangeId)
  acquire(c.kvStore.smMu)
  var data: seq[byte]
  for k, v in sm.kvStore:
    # Format: uint32 klen + k bytes + uint32 vlen + v bytes
    encodeKV(data, k, v)
  release(c.kvStore.smMu)

  let snap = Snapshot(
    rangeId: rangeId,
    raftSnap: RaftSnapshotMeta(
      lastIncludedIndex: commitIndex,
      lastIncludedTerm:  commitTerm,
      configuration:     g.descriptor.replicas,
    ),
    stateMachineSnap: data,
  )
  c.getLog(rangeId).saveSnapshot(snap)
  c.getLog(rangeId).compact(commitIndex + 1)
  true
```

### 7.2 Applying a snapshot (follower)

```nim
proc applySnapshot(c: MultiRaftCoordinator, rangeId: RangeID,
    data: seq[byte]) =
  if c.kvStore == nil: return
  let sm = c.kvStore.getOrCreateSM(rangeId)
  acquire(c.kvStore.smMu)
  sm.kvStore.clear()
  var pos = 0
  while pos < data.len:
    let (k, v, newPos) = decodeKV(data, pos)
    sm.kvStore[k] = v
    pos = newPos
  release(c.kvStore.smMu)
```

### 7.3 Snapshot trigger policy

In `timerProc`, after applying entries: if `log.lastIndex - log.firstIndex > LOG_COMPACTION_THRESHOLD` (default 10 000 entries), call `takeSnapshot`. This keeps log size bounded without a separate compaction thread.

---

## 8. Server Binary (`server_main.nim`)

```nim
# src/fractio/server_main.nim
#
# Usage:
#   fractio-server --node-id=1 --host=0.0.0.0 --port=9000 \
#                  --raft-port=10000 --data-dir=/var/lib/fractio/node1 \
#                  --peers=2:10.0.0.2:10000,3:10.0.0.3:10000
#
# --peers format: <nodeId>:<host>:<raftPort>[,...]
```

```nim
import std/[os, parseopt, strutils, tables]
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/range/types as rangeTypes
import fractio/protocol/server
import fractio/protocol/raft_store

proc parseArgs(): tuple[nodeId: NodeID, host: string, port: int,
                         raftPort: int, dataDir: string,
                         peers: seq[tuple[nodeId: NodeID, host: string, raftPort: int]]]

proc main() =
  let args = parseArgs()

  let coordCfg = CoordinatorConfig(
    nodeId:            args.nodeId,
    numWorkers:        4,
    electionTimeoutNs: DEFAULT_ELECTION_TIMEOUT_NS,
    heartbeatIntervalNs: DEFAULT_HEARTBEAT_INTERVAL_NS,
    storagePath:       args.dataDir,
    raftListenPort:    args.raftPort,
    peers:             args.peers,
    proposeTimeoutMs:  5000,
  )

  let coord = newMultiRaftCoordinator(coordCfg)

  # Bootstrap or recover the initial range (range 1 = full keyspace)
  let rid = RangeID(1)
  var desc = newRangeDescriptor(rid, @[], @[])
  for p in args.peers:
    discard desc.addReplica(p.nodeId)
  discard desc.addReplica(args.nodeId)
  let localRep = desc.getReplica(args.nodeId).get
  discard coord.createGroup(desc, localRep.replicaId)
  coord.start()

  let raftSt = newRaftKVStoreExt(coord, proposeTimeoutMs = 5000)
  raftSt.bootstrapSingleShardExt(rid)

  var srvCfg = defaultServerConfig()
  srvCfg.host = args.host
  srvCfg.port = args.port

  let srv = newProtocolServer(srvCfg)
  srv.raftStore = raftSt
  srv.start()

  echo "fractio-server running on ", args.host, ":", args.port
  echo "raft peer port: ", args.raftPort
  echo "node id: ", args.nodeId

  # Block until signal
  while true: sleep(1000)

main()
```

Add to `fractio.nimble`:
```nim
bin = @["server_main"]
```

---

## 9. Test Plan (`test_multinode_raft.nim`)

Ports: **20200–20299**  
Storage: `/tmp/fractio_mn_<port>/`  
Node count: 3 (replication factor 3, quorum 2)

### Suite 1 — Leader election
```
test "three nodes elect exactly one leader"
test "leader re-elected after isolated restart"
test "split vote resolved within 3 election timeouts"
```

### Suite 2 — Log replication
```
test "write on leader is readable from all nodes after quorum"
test "follower catches up after reconnect"
test "write rejected on follower with ErrNotLeader + leader hint"
```

### Suite 3 — Fault tolerance
```
test "cluster remains available with one node down (quorum = 2)"
test "write blocked when two nodes down (below quorum)"
test "previously committed entries survive leader restart"
```

### Suite 4 — Snapshot and compaction
```
test "snapshot taken after threshold entries"
test "lagging follower receives snapshot instead of full log"
test "state machine correct after snapshot apply"
```

### Suite 5 — Client retry
```
test "client auto-retries on ErrNotLeader, reaches new leader"
test "client retry succeeds after leader election"
```

**Test helper `makeCluster(n, basePort)`:**
- Creates `n` `MultiRaftCoordinator` instances in-process (separate goroutines/threads).
- Uses `RaftTransport` with loopback addresses (`127.0.0.1`).
- Each has its own `/tmp/` LevelDB directory.
- Returns a `Cluster` handle with `stop()`, `killNode(i)`, `restartNode(i)` methods.

---

## 10. Implementation Order

| Step | File(s) | Gate |
|---|---|---|
| 1 | `transport.nim` — frame encode/decode, TCP accept loop, `send`, `sendAndRecv` | Compile + unit test send/recv loopback |
| 2 | `election_timer.nim` — timer thread, `startElectionForGroup`, vote fan-out | Compile |
| 3 | RPC message encode/decode procs (can be in `transport.nim`) | Compile |
| 4 | `handleRequestVote`, `handleAppendEntries` in coordinator | Compile |
| 5 | `replicateAndWait` in `workerProc` — replaces single-node shortcut | All 352 existing tests still pass |
| 6 | `applyBatchToSM` in `raft_store.nim`, `applyUpTo` in coordinator | All 352 existing tests still pass |
| 7 | `handleInstallSnapshot`, `takeSnapshot`, `applySnapshot` | Compile |
| 8 | `server_main.nim` | Builds binary |
| 9 | NOT_LEADER retry in `client.nim`, leader hint in `server.nim` | Compile |
| 10 | `test_multinode_raft.nim` — all suites green | Full test pass |

Steps 1–3 can be done with zero risk to existing tests (new files only). Step 5 is the
only step that modifies a tested code path; the `voters.len == 1` guard preserves
single-node behaviour.

---

## 11. Key Invariants to Maintain

1. **Nim 2.2.8 ORC constraint:** Never send a GC-managed `ref` across threads inside a
   closure. The `ProposalResultChannel` raw-pointer pattern (already established) must be
   used for any new cross-thread completion channel. Fan-out threads in `replicateAndWait`
   must communicate results via `Channel[bool]` (built-in, value type) not via closures
   capturing refs.

2. **Lock ordering:** Always acquire locks in this order to prevent deadlock:
   `groupsLock` → `group.lock` → `smMu`. Never invert.

3. **Persistence before response:** A leader must call `log.saveState` before signalling
   success to the proposal caller, and before sending `AppendEntries` to peers. Followers
   must call `log.saveState` before sending `AppendEntriesResp(success=true)`.

4. **Only commit from current term:** `computeNewCommitIndex` must check
   `entry.term == group.currentTerm.load()`. An entry from a previous term is only safe to
   commit once an entry from the current term has been committed (Raft §5.4.2).

5. **Single-node backward compatibility:** `raftListenPort == 0` must produce identical
   behaviour to the current implementation. All 352 tests must pass with no code changes to
   test files.
