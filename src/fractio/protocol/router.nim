# Shard routing table for the Fractio protocol layer.
#
# Maps keys to shard ranges and their current Raft leaders.
# Thread-safe via a single Mutex; all public procs acquire the lock.
#
# In Phase 2 the router operates in "mock" mode: a single shard covers the
# entire keyspace and is always routed to the local node.  Phase 5 replaces
# this with real Raft group handles and consistent-hash ring placement.

import std/[tables, locks, strformat, algorithm]
import ./types

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

type
  ShardRange* = object
    ## Contiguous slice of the key space owned by one Raft group.
    startKey*: string ## inclusive; empty string = beginning of keyspace
    endKey*: string   ## exclusive; empty string = end of keyspace
    shardId*: uint32
    raftGroupId*: uint32

  LeaderInfo* = object
    ## Current Raft leader for a shard.
    nodeId*: uint32
    nodeAddr*: string  ## "host:port" of the leader
    lastSeenMs*: int64 ## monotonic ms; updated on each successful contact

  RouterTable* = ref object
    ## Thread-safe mapping: shardId → (ShardRange, LeaderInfo).
    shards*: seq[ShardRange]            ## sorted by startKey ascending
    leaders*: Table[uint32, LeaderInfo] ## shardId → leader
    localNodeId*: uint32
    mu*: Lock

# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

proc newRouterTable*(localNodeId: uint32 = 1): RouterTable =
  result = RouterTable(
    shards: @[],
    leaders: initTable[uint32, LeaderInfo](),
    localNodeId: localNodeId,
  )
  initLock(result.mu)

# ---------------------------------------------------------------------------
# Bootstrap: single-shard covering the whole keyspace
# ---------------------------------------------------------------------------

proc bootstrapSingleShard*(rt: RouterTable, shardId: uint32 = 1,
    raftGroupId: uint32 = 1, leaderAddr: string = "") =
  ## Populate the routing table with one shard spanning the entire keyspace
  ## and pointing to the local node as leader.  Used in single-node and test
  ## configurations.
  acquire(rt.mu)
  defer: release(rt.mu)
  rt.shards = @[ShardRange(
    startKey: "",
    endKey: "",
    shardId: shardId,
    raftGroupId: raftGroupId,
  )]
  rt.leaders[shardId] = LeaderInfo(
    nodeId: rt.localNodeId,
    nodeAddr: leaderAddr,
    lastSeenMs: 0,
  )

# ---------------------------------------------------------------------------
# Find shard for key (internal, must hold lock)
# ---------------------------------------------------------------------------

proc findShardForKey(rt: RouterTable, key: string): int =
  ## Binary-search `rt.shards` for the shard that owns `key`.
  ## Returns the index or -1 if the table is empty.
  ## Shards are sorted by startKey ascending; a shard owns [startKey, endKey).
  if rt.shards.len == 0: return -1
  # Linear scan is fine for small shard counts (Phase 2 has 1).
  # Replace with bisect for large tables if needed.
  var best = -1
  for i, s in rt.shards:
    let afterStart = s.startKey.len == 0 or key >= s.startKey
    let beforeEnd = s.endKey.len == 0 or key < s.endKey
    if afterStart and beforeEnd:
      best = i
      break
  best

# ---------------------------------------------------------------------------
# Public: routeKey
# ---------------------------------------------------------------------------

proc routeKey*(rt: RouterTable,
    key: string): Result[LeaderInfo, ProtocolError] =
  ## Return the LeaderInfo for the shard that owns `key`.
  ## Returns peNotLeader when no leader is currently known for that shard.
  acquire(rt.mu)
  defer: release(rt.mu)

  let idx = findShardForKey(rt, key)
  if idx < 0:
    return peErr(newProtocolError(peNotLeader,
      &"no shard covers key '{key}': routing table is empty"))

  let shard = rt.shards[idx]
  let leader = rt.leaders.getOrDefault(shard.shardId,
    LeaderInfo(nodeId: 0, nodeAddr: "", lastSeenMs: 0))

  if leader.nodeId == 0:
    return peErr(newProtocolError(peNotLeader,
      &"leader unknown for shard {shard.shardId} (election in progress?)"))

  peOk(leader)

# ---------------------------------------------------------------------------
# Public: routeKeys (batch — returns all distinct leader infos)
# ---------------------------------------------------------------------------

proc routeKeys*(rt: RouterTable,
    keys: seq[string]): Result[seq[(string, LeaderInfo)], ProtocolError] =
  ## Return a seq of (key, LeaderInfo) pairs.
  ## Fails fast on the first key that cannot be routed.
  acquire(rt.mu)
  defer: release(rt.mu)

  var pairs = newSeq[(string, LeaderInfo)](keys.len)
  for i, key in keys:
    let idx = findShardForKey(rt, key)
    if idx < 0:
      return peErr(newProtocolError(peNotLeader,
        &"no shard covers key '{key}'"))

    let shard = rt.shards[idx]
    let leader = rt.leaders.getOrDefault(shard.shardId,
      LeaderInfo(nodeId: 0, nodeAddr: "", lastSeenMs: 0))

    if leader.nodeId == 0:
      return peErr(newProtocolError(peNotLeader,
        &"leader unknown for shard {shard.shardId}"))

    pairs[i] = (key, leader)

  peOk(pairs)

# ---------------------------------------------------------------------------
# Public: updateRoute
# ---------------------------------------------------------------------------

proc updateRoute*(rt: RouterTable, shard: ShardRange,
    leader: LeaderInfo) {.gcsafe, raises: [].} =
  ## Insert or replace a shard range entry and its leader.
  ## Called when a NOT_LEADER redirect or a gossip message updates the table.
  acquire(rt.mu)
  defer: release(rt.mu)

  # Replace existing shard with same ID, or append.
  var found = false
  for i in 0 ..< rt.shards.len:
    if rt.shards[i].shardId == shard.shardId:
      rt.shards[i] = shard
      found = true
      break
  if not found:
    rt.shards.add(shard)
    # Keep sorted by startKey for binary search readiness.
    rt.shards.sort(proc(a, b: ShardRange): int =
      cmp(a.startKey, b.startKey))

  rt.leaders[shard.shardId] = leader

# ---------------------------------------------------------------------------
# Public: updateLeader
# ---------------------------------------------------------------------------

proc updateLeader*(rt: RouterTable, shardId: uint32,
    leader: LeaderInfo) {.gcsafe, raises: [].} =
  ## Update only the leader for an existing shard (e.g. after election).
  acquire(rt.mu)
  defer: release(rt.mu)
  rt.leaders[shardId] = leader

# ---------------------------------------------------------------------------
# Public: shardCount / isLocal
# ---------------------------------------------------------------------------

proc shardCount*(rt: RouterTable): int {.gcsafe, raises: [].} =
  acquire(rt.mu)
  defer: release(rt.mu)
  rt.shards.len

proc isLocalLeader*(rt: RouterTable, key: string): bool {.gcsafe, raises: [].} =
  ## Returns true when this node is the current leader for `key`'s shard.
  acquire(rt.mu)
  defer: release(rt.mu)
  let idx = findShardForKey(rt, key)
  if idx < 0: return false
  let shard = rt.shards[idx]
  let leader = rt.leaders.getOrDefault(shard.shardId,
    LeaderInfo(nodeId: 0, nodeAddr: "", lastSeenMs: 0))
  leader.nodeId == rt.localNodeId
