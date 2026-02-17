# Raft Consensus and Multi-Group Replication Design

## Overview

Fractio will implement a distributed consensus system based on the Raft algorithm, extended to support multi-group replication for sharded data. This design enables high availability, fault tolerance, and linearizable consistency across the cluster.

**Key Goals:**
- Single-group Raft for basic consensus
- Multi-group architecture for sharded data (each shard = independent Raft group)
- Joint consensus for dynamic membership changes
- Automatic leader election and failover
- Linearizable reads and writes
- Support for cluster reconfiguration (add/remove nodes, move shards)

---

## 1. Single-Group Raft

### 1.1 Core Concepts

A Raft group is a replicated state machine consisting of:
- **Leader**: Handles all client requests, replicates log to followers
- **Followers**: Passive nodes that receive log entries from leader
- **Candidate**: Intermediate state during election

**Terms:**
- Arbitrary increasing integer (4 or 8 bytes)
- Each term has at most one leader
- Terms used to detect stale leadership

**Log:**
- Sequence of commands (e.g., `Insert`, `Update`, `Delete`, `BeginTxn`, `Commit`)
- Each entry: `(term, command, index)`
- Log is strictly append-only
- Entries are considered committed when stored on majority of nodes

### 1.2 RPCs

```nim
type
  RequestVoteArgs* = object
    term*: uint64
    candidateId*: uint16
    lastLogIndex*: uint64
    lastLogTerm*: uint64

  RequestVoteReply* = object
    term*: uint64
    voteGranted*: bool

  AppendEntriesArgs* = object
    term*: uint64
    leaderId*: uint16
    prevLogIndex*: uint64
    prevLogTerm*: uint64
    entries*: seq[RaftEntry]
    leaderCommit*: uint64

  AppendEntriesReply* = object
    term*: uint64
    success*: bool
    conflictIndex*: uint64  # hint for leader to retry
```

### 1.3 State Machine

Each Raft node maintains:
```nim
type
  RaftState* = enum
    rsFollower, rsCandidate, rsLeader

  RaftNode* = ref object
    nodeId*: uint16
    groupId*: ShardId  # which shard this node participates in
    state*: RaftState
    currentTerm*: Atomic[uint64]
    votedFor*: uint16  # candidate this node voted for in current term
    log*: RingBuffer[RaftEntry]  # persistent storage
    commitIndex*: Atomic[uint64]  # highest known committed index
    lastApplied*: Atomic[uint64]  # highest applied to state machine
    leaderId*: Atomic[uint16]     # current leader (0 if none)
    # Volatile runtime state
    nextIndex*: Table[uint16, uint64]  # for each follower
    matchIndex*: Table[uint16, uint64] # for each follower
    electionTimer*: MonotonicTimer
    heartbeatTimer*: MonotonicTimer
    lock*: Lock  # protects volatile state
    stateMachine*: ShardState  # apply committed commands here
```

### 1.4 Rules

**Leader Responsibilities:**
- Accept client commands, append to log
- Replicate log to all followers via `AppendEntries`
- Forward client requests if not leader (`NotLeader` error)
- Send empty `AppendEntries` (heartbeats) to maintain authority
- Advance `commitIndex` when majority of followers have entry

**Follower Responsibilities:**
- Respond to `AppendEntries` from leader or candidate
- If no RPC in `electionTimeout`, become candidate
- On `RequestVote`, grant if candidate's log is at least as up-to-date

**Election:**
1. Follower times out → becomes candidate, increment term, vote for self
2. Send `RequestVote` to all peers
3. If majority votes → becomes leader
4. If any RPC with higher term → revert to follower
5. If split vote, randomize timeout and retry

**Log Replication:**
1. Leader appends entry to its log
2. Leader sends `AppendEntries` to all followers
3. Follower appends if log consistency check passes (`prevLogIndex` and `prevLogTerm`)
4. Leader tracks `nextIndex` (next to send) and `matchIndex` (highest matched)
5. When entry stored on majority, leader commits and applies to state machine
6. Leader notifies followers of new commits via `leaderCommit` field

**Safety:**
- Leader can only commit entries from its term
- Leader never overwrites or deletes its log
- Log matching property: if two logs have same index/term, they're identical up to that point
- Log completeness: if log entry is committed in one term, it will appear in all future leaders' logs

---

## 2. Multi-Group Raft (Shard-Based Replication)

### 2.1 Motivation

Single Raft group replicates the entire dataset. For large clusters and high throughput, we partition data into **shards**, each with its own Raft group. This provides:
- **Horizontal scalability**: Each shard handles subset of data
- **Parallelism**: Multiple shards process requests concurrently
- **Fault isolation**: Failure of one shard doesn't affect others
- **Load balancing**: Requests route to appropriate shard's leader

### 2.2 Architecture

**Shard = Raft Group:**
- Each shard (key range) is independently replicated
- Shard has a group ID (`GroupId` = hash of range start)
- Each shard has a fixed `replicationFactor` (e.g., 3)
- Nodes can participate in multiple shards (multiple Raft groups)

**Group Membership:**
- Nodes assigned to shards via configuration table
- Each node maintains Raft state per shard (multi-raft)
- Client requests include shard key; router determines target group

**Group Assignment Table:**
```nim
type
  ShardRange* = object
    startKey*: Bytes  # start of hash range (inclusive)
    endKey*: Bytes    # end of hash range (exclusive)
    groupId*: GroupId

  GroupConfig* = object
    groupId*: GroupId
    nodes*: seq[uint16]  # node IDs in this group
    leaderId*: uint16    # current leader (cached)
    epoch*: uint64       # configuration epoch (for joint consensus)
```

### 2.3 Cross-Group Operations

**Single-Shard Transaction:**
1. Router hashes shard key → determines target group
2. Request sent to group's leader
3. Raft replicates and applies on that group only
4. Response returned to client

**Cross-Shard Transaction:**
- Two-phase commit (2PC) across multiple Raft groups
- Coordinator node (chosen from participant) runs 2PC
- Phase 1: Prepare → all participants write `Prepare` log entry and vote
- Phase 2: Commit → coordinator writes `Commit` entry to all logs
- If any participant fails or times out, transaction aborts

**2PC in Raft Context:**
- Each participant uses its own Raft group to replicate prepare/commit
- No blocking: participants vote based on conflict detection (MVCC)
- Recovery: coordinator crash → participants time out and clean up

---

## 3. Joint Consensus for Reconfiguration

### 3.1 Problem

Adding/removing nodes from a Raft group requires care to avoid split-brain and data loss. Raft's joint consensus allows safe configuration changes.

### 3.2 Configuration Change Process

**Two-Phase Approach (Unchanged from Raft paper):**

1. **Start joint consensus:**
   - Create new configuration `C_new` (e.g., with added node)
   - Create joint configuration `C_old,new` (both old and new active)
   - Leader appends `ConfigChange` entry with `C_old,new`
   - Configuration change only takes effect when that entry is committed

2. **Commit joint config:**
   - Once `C_old,new` is committed, cluster operates with both configs
   - New node starts receiving logs
   - Leaders can be from either `C_old` or `C_new`

3. **Commit new config only:**
   - Leader appends another entry with `C_new` only
   - Once committed, old configuration is inactive, nodes removed stop serving

4. **Cleanup:**
   - Removed nodes realize they're no longer in config and step down

**Why Two Phases?**
- Ensures overlapping membership during transition
- Prevents two disjoint majorities (old without new, new without old)
- Guarantees safety even if leader changes during transition

### 3.3 Implementation

```nim
type
  ConfigChangeType* = enum
    cctAddNode, cctRemoveNode, cctMoveShard

  ConfigChangeCommand* = object
    changeType*: ConfigChangeType
    groupId*: GroupId
    nodeId*: uint16           # node to add/remove
    targetGroup*: GroupId?    # for move operations
```

**State Machine Handling:**
- `C_old,new` represented as union of both sets
- Majority computed on union for joint config
- After `C_new` committed, only new set counts

---

## 4. Implementation Plan

### Phase 1: Core Raft Singleshard (Week 1-2)

**Step 1.1: Persistent Log Storage**
- File: `src/fractio/distributed/raft/log.nim`
- Use `RingBuffer` on disk (append-only)
- Store: `term`, `command`, `index`, `checksum`
- Ensure fsync on commit (configurable for performance)
- Compaction/snapshot support

**Step 1.2: Raft Node State**
- File: `src/fractio/distributed/raft/raftnode.nim`
- Implement state machine (follower/candidate/leader)
- Election timer, heartbeat timer (MonotonicTimeProvider)
- Thread-safe: all RPC handlers run in own thread, use mutex for volatile state
- Exported methods:
  - `start()`, `close()`
  - `propose(command: RaftCommand): Future[RaftResult]`
  - `getLeader(): uint16`
  - `getState(): RaftState`

**Step 1.3: RPC Transport**
- File: `src/fractio/distributed/raft/raft_rpc.nim`
- Use existing UDPTransport (from timesync) or add TCP for reliability
- Protocol: binary encoding (use PacketCodec style)
- RPCs: `RequestVote`, `AppendEntries`
- Handle retries, timeouts, duplicate detection
- Serialize/deserialize with bounds checking

**Step 1.4: Leader Election**
- Implement timer-based election
- Randomize election timeout (150-300ms)
- `becomeCandidate()`, `becomeLeader()`, `becomeFollower(term)`
- Vote rules: once per term, log_up_to_date check

**Step 1.5: Log Replication**
- Leader: `appendEntry(command)`, send `AppendEntries` to all followers
- Followers: process `AppendEntries`, append if consistency check passes
- Leader: track `nextIndex` (next to send), `matchIndex` (replicated up to)
- Optimistic fast-path: batch multiple entries in one RPC
- Handle conflicts: follower rejects, leader decrements `nextIndex` and retries

**Step 1.6: Commit and Application**
- Leader computes commit when `matchIndex[majority]` ≥ index
- Update `commitIndex`, apply to state machine sequentially
- Followers: when `AppendEntries.leaderCommit` > `commitIndex`, advance
- State machine interface: `apply(command: RaftCommand) -> Result`
- Command types: `Noop`, `SetValue`, `DeleteRange`, `ConfigChange`

**Step 1.7: Snapshots (Compaction)**
- To avoid unbounded log growth
- Snapshot: current state machine dump (serialized)
- Leader triggers snapshot when log too large (e.g., 10K entries)
- After snapshot, discard old log entries
- Followers install snapshot via `InstallSnapshot` RPC

**Step 1.8: Testing**
- Unit tests for single node state transitions
- Integration tests with 3-node cluster (leader election, log replication)
- Fault injection: node crashes, network partitions, slow networks
- 100% coverage target

---

### Phase 2: Multi-Group Raft (Week 3-4)

**Step 2.1: Multi-Raft Scheduler**
- File: `src/fractio/distributed/raft/multiraft.nim`
- `MultiRaftCoordinator`: manages many Raft groups
- Maps `GroupId` → `RaftNode` (or `seq[RaftNode]` for multi-role nodes)
- Node may host multiple groups (each with own thread/context)
- Startup: load group configs, initialize RaftNode per group

**Step 2.2: Group Configuration Table**
- File: `src/fractio/distributed/sharding/groupconfig.nim`
- Persistent store of `ShardRange` → `GroupId` mapping
- Each `GroupId` has `GroupConfig` (nodes, epoch)
- Stored in system table, replicated via Raft itself (bootstrapping)
- Client requests: hash key → find `ShardRange` → lookup `GroupConfig` → route to leader

**Step 2.3: Cross-Group Transaction (2PC)**
- File: `src/fractio/distributed/transaction/coordinator.nim`
- `CrossShardCoordinator`: manages 2PC protocol
- Steps:
  1. Begin transaction on all participants (assign unique txn ID)
  2. Phase 1: send `Prepare` to all participants
     - Each participant appends `Prepare` to its local Raft log
     - If prepared successfully (no conflicts), vote "yes"
  3. If all yes: Phase 2: send `Commit` to all
     - Each appends `Commit` to log
  4. If any no or timeout: send `Abort` to all
- Recovery: participants time out waiting for decision → consult each other or coordinator
- Timeouts must be long enough for slowest Raft group (e.g., 5s)

**Step 2.4: Joint Consensus for Group Reconfiguration**
- File: `raft/configchange.nim`
- Implement `ConfigChangeCommand` handling in state machine
- Transition states: `Normal` → `Joint` → `Normal` (new config)
- Update `GroupConfig` atomically when joint config committed
- Handle node addition: new node starts as follower, catches up via log replication
- Handle node removal: removed node receives `RemoveNode` and steps down
- Handle leader change during joint consensus (requires protocol correctness)

**Step 2.5: Leader Rebalancing**
- Scheduler monitors uneven distribution of leaders
- If a node is leader for too many groups, transfer leadership voluntarily
- Use `TransferLeadership` command to target follower
- Raft has existing `LeadershipTransfer` mechanism (not in basic spec but common extension)

**Step 2.6: Snapshot per Group**
- Each group manages its own snapshots independently
- Snapshot ID includes `groupId` and `index`
- Followers can request snapshot if log diverged too much
- Snapshot installation includes last included index/term for log continuity

**Step 2.7: Client API**
- File: `src/fractio/distributed/client/fractioclient.nim`
- `FractioClient` routes requests to appropriate Raft group
- Connection pooling per group leader
- Linearizable reads: send via leader, or use `ReadIndex` optimization (query leader's commit index)
- Retry logic: if `NotLeader` error, discover new leader via group config and retry

---

### Phase 3: Advanced Features (Week 5-6)

**Step 3.1: Read-Only Optimization (ReadIndex)**
- For linearizable reads without logging
- Leader: when read request arrives, advance `commitIndex` to latest, store `commitIndex` in a "read-only lease" (term)
- Client request includes read index; leader verifies it's still leader
- Avoids write-ahead log overhead for read-only transactions

**Step 3.2: Batching and Pipelining**
- Leader can pipeline `AppendEntries` without waiting for responses
- Allow multiple in-flight RPCs (limited by window)
- Batch multiple client commands into single log entry if from same leader

**Step 3.3: Metrics and Monitoring**
- Per-group metrics: leader index, commit lag, uncommitted log size
- Node-level metrics: number of groups hosted, election count, RPC latency
- Export via Prometheus or logging

**Step 3.4: Graceful Degradation**
- If a shard loses quorum (e.g., 1 of 3 nodes down), mark as degraded
- Router can reject writes, but may allow reads from replicas (stale)
- Automatic recovery when quorum restored

**Step 3.5: Cluster Bootstrap**
- First node starts with single-node group (itself)
- Add more nodes via `ConfigChange` (joint consensus)
- Form initial group assignments via manual config or autosharding

---

## 5. Data Structures

### 5.1 Log Entry

```nim
type
  RaftCommandKind* = enum
    rckNoop,            # heartbeat
    rckSetValue,        # key-value set
    rckDeleteRange,     # delete key range
    rckBeginTxn,        # begin transaction (MVCC)
    rckCommit,          # commit transaction
    rckAbort,           # abort transaction
    rckConfigChange,    # add/remove node, move shard

  RaftCommand* = object
    case kind*: RaftCommandKind
    of rckSetValue:
      key*: Bytes
      value*: Bytes
      timestamp*: Timestamp  # MVCC version
    of rckDeleteRange:
      startKey*: Bytes
      endKey*: Bytes
    of rckBeginTxn:
      txnId*: uint64
      isolation*: IsolationLevel
    of rckCommit:
      txnId*: uint64
    of rckAbort:
      txnId*: uint64
    of rckConfigChange:
      configCmd*: ConfigChangeCommand
    else:
      discard

  RaftEntry* = object
    term*: uint64
    index*: uint64
    command*: RaftCommand
    checksum*: uint32  # CRC32 for integrity
```

### 5.2 Snapshot

```nim
type
  SnapshotMeta* = object
    lastIncludedIndex*: uint64
    lastIncludedTerm*: uint64
    groupId*: GroupId
    nodeId*: uint16
    timestamp*: Timestamp

  Snapshot* = object
    meta*: SnapshotMeta
    data*: seq[uint8]  # serialized state machine
```

---

## 6. Error Handling and Timeouts

- **Election timeout**: 150-300ms (randomized)
- **RPC timeout**: 100ms (configurable)
- **Commit timeout**: 1s (for client proposals)
- **2PC timeout**: 5s (for cross-shard transactions)
- Errors:
  - `RaftError`: generic base
  - `NotLeaderError`: indicates current node isn't leader (includes correct leader ID)
  - `StaleStateError`: term too low, retry
  - `ConfigChangeError`: invalid config transition
  - `TimeoutError`: RPC or election timeout

---

## 7. Testing Strategy

### 7.1 Unit Tests
- Single node: state transitions, timer expiry, log append
- Vote RPC: grant/deny conditions, term updates
- AppendEntries: success, failure (prevLog mismatch), conflict resolution
- ConfigChange: handling at various stages

### 7.2 Integration Tests (3-5 nodes)
- Leader election: one node elected, others followers
- Log replication: append 100 entries, verify all nodes have same sequence
- Fault injection: kill leader, verify new election
- Network partition: split cluster, ensure no split-brain
- Joint consensus: add node, verify new node catches up
- Remove node: verify removed node stops serving

### 7.3 Multi-Group Tests
- 2 shards, 2 replicas each, cross-shard transaction (2PC)
- Reconfigure group while processing requests
- Leader rebalance across nodes

### 7.4 Stress Tests
- 10 groups, 3 nodes each, 10K commands per group
- Measure throughput, latency, consistency

---

## 8. Files to Create

```
src/fractio/distributed/raft/
├── raftnode.nim         # Main Raft node implementation
├── log.nim              # Persistent log storage
├── state.nim            # State machine interface
├── raft_rpc.nim         # RPC protocol (UDP/QUIC/grpc?)
├── snapshot.nim         # Snapshot creation and installation
├── configchange.nim     # Joint consensus implementation
├── errors.nim           # Raft-specific errors
└── types.nim            # Common types

src/fractio/distributed/sharding/
├── groupconfig.nim      # Group configuration table
├── router.nim           # Request routing by shard key
└── rebalancer.nim       # Leader rebalancing logic

src/fractio/distributed/transaction/
├── coordinator.nim      # 2PC coordinator
├── participant.nim      # 2PC participant (wraps RaftNode)
└── recovery.nim         # Transaction recovery after crashes

src/fractio/distributed/raftgroups/
├── multiraft.nim        # Multi-Raft orchestration
├── membership.nim       # Group membership lifecycle
└── metrics.nim          # Monitoring metrics

tests/
├── unit/distributed/raft/
│   ├── test_raftnode.nim
│   ├── test_log.nim
│   ├── test_configchange.nim
│   └── test_raft_rpc.nim
├── integration/
│   ├── test_raft_cluster.nim
│   ├── test_multiraft_2pc.nim
│   └── test_joint_consensus.nim
```

---

## 9. API Surface (Public)

```nim
# RaftNode (per group)
proc newRaftNode*(nodeId: uint16, groupId: GroupId,
                  peers: seq[PeerConfig],
                  storage: RaftStorage,
                  logger: Logger): RaftNode
proc start*(node: RaftNode) {.async.}
proc close*(node: RaftNode) {.async.}
proc propose*(node: RaftNode, cmd: RaftCommand): Future[RaftResult] {.async.}
proc getLeader*(node: RaftNode): uint16
proc getState*(node: RaftNode): RaftState
proc isLeader*(node: RaftNode): bool
proc transferLeadership*(node: RaftNode, toNodeId: uint16) {.async.}

# MultiRaftCoordinator
proc newMultiRaft*(config: ClusterConfig,
                   storageFactory: proc(groupId: GroupId): RaftStorage,
                   logger: Logger): MultiRaftCoordinator
proc start*(m: MultiRaftCoordinator) {.async.}
proc stop*(m: MultiRaftCoordinator) {.async.}
proc getGroupLeader*(m: MultiRaftCoordinator, groupId: GroupId): Future[uint16] {.async.}
proc submit*(m: MultiRaftCoordinator, groupId: GroupId, cmd: RaftCommand): Future[RaftResult] {.async.}
proc rebalance*(m: MultiRaftCoordinator) {.async.}

# Client
proc newFractioClient*(nodes: seq[NodeAddr]): FractioClient
proc execute*(c: FractioClient, sql: string, params: seq[Value]): Future[QueryResult] {.async.}
proc executeTx*(c: FractioClient, ops: seq[TxOp]): Future[TxResult] {.async.}  # cross-shard
```

---

## 10. Open Questions

1. **Transport Layer**: Reuse UDPTransport from timesync or create new TCP-based transport? Current UDPTransport is request-response; Raft needs persistent connections and streaming (or connection per RPC with short timeout). Likely need a new `TcpTransport`.

2. **Storage Backend**: MVCCStorage writes will go through Raft log first. How to write-ahead? Options:
   - Append to Raft log first → replicate → apply writes to MVCCStorage
   - MVCCStorage provides `applySnapshot(snapshot)` and `applyCommand(cmd)` APIs

3. **Snapshot Frequency**: When to trigger snapshot? Based on log size (e.g., 10K entries) or time interval. Need to balance performance.

4. **Node Identity**: `uint16` node ID space (0-65535). Need allocation strategy (static config, or discovery service).

5. **Joint Consensus Edge Cases**: What if leader steps down during joint consensus? Does `C_old,new` remain valid if leader changes? Raft paper says yes, but implementation must track correctly.

6. **Client Routing**: How does client discover group leaders? Options:
   - Query any node → node forwards or redirects (via `NotLeader` error)
   - Maintain cluster-wide routing table (replicated via groups)
   - Use consistent hashing ring (each node knows which groups it leads)

7. **Cross-Group 2PC Timeout**: Need long timeout for worst-case Raft commit (election + commit). If participant fails after prepare but before commit, transaction may be in-doubt until recovery.

---

## 11. Milestones

**M1 (Single-node Raft)**: Can append and apply command on single node
**M2 (Two-node quorum)**: Leader election, replication with 2 of 2 quorum
**M3 (Three-node cluster)**: Fault tolerance (1 failure), leader failover
**M4 (Configuration change)**: Add 4th node via joint consensus
**M5 (Multi-group)**: Two shards, two Raft groups running concurrently
**M6 (Cross-shard transaction)**: 2PC working across groups
**M7 (Production-ready)**: Snapshots, metrics, read-optimizations, thorough testing

---

## 12. Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Single slow follower blocks commit | High throughput | Allow slow followers, leader commits based on majority |
| Large transaction log consumes disk | Medium | Regular snapshots, log compaction |
| Cross-group 2PC blocks on participant crash | Medium | Timeout + recovery protocol, idempotent operations |
| Network partition causes two leaders | Low | Raft guarantees safety during partitions (only one majority possible) |
| Joint consensus bug leads to lost config | High | Thorough testing, dry-run mode, manual review of transition logic |

---

## 13. References

- *In Search of an Understandable Consensus Algorithm* (Raft paper, 2014)
- *Raft Extended Reconfiguration* (joint consensus)
- *Paxos Made Simple* (background)
- CockroachDB architecture (inspiration, but implementation will be original)

---

## Appendix: Example Joint Consensus Sequence

Assume 3-node cluster `{A, B, C}`; we add `D`.

1. Leader `A` receives `ConfigChange(Add D)`.
2. Leader creates joint config `C_old,new = {A,B,C,D}`
3. Leader appends `ConfigChange(C_old,new)` to its log, replicates to followers
4. Once entry committed on majority of `{A,B,C,D}` (need 3 acks), joint config active
5. New node `D` starts receiving log entries (may need snapshot)
6. After some time, leader decides to switch to new config only
7. Leader appends `ConfigChange(C_new = {A,B,D})`
8. Once committed on majority of `{A,B,D}`, new config active
9. Node `C` realizes it's removed (receives RPC with term+config index) and steps down

Note: In实践活动, we might remove one node at a time if reducing cluster, or add nodes before removing (if scaling).
