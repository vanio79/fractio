# Shard routing table for the Fractio protocol layer.
#
# Maps keys to shard ranges and their current Raft leaders.
# Thread-safe via a single Mutex; all public procs acquire the lock.
#
# In Phase 2 the router operates in "mock" mode: a single shard covers the
# entire keyspace and is always routed to the local node.
#
# Phase 5 additions:
#   - onLeaderChange callback: invoked whenever a shard leader changes so the
#     RaftKVStoreExt can adapt (e.g. redirect proposals to the new leader).
#   - notLeaderRedirect: record NOT_LEADER hints from Raft and update the table.
#   - syncFromRaftGroup: populate routing entries from a MultiRaftCoordinator's
#     live group map (called at startup or after config changes).
#   - staleness TTL: LeaderInfo.lastSeenMs checked against a configurable TTL;
#     stale entries trigger re-routing.

import std/[tables, locks, strformat, algorithm, times]
import ./types
import ../distributed/sharedtimer/timeprovider

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

  LeaderChangeCallback* = proc(shardId: uint32,
      leader: LeaderInfo) {.gcsafe, raises: [].}
    ## Called whenever the leader for a shard is updated in the routing table.

  RouterTable* = ref object
    ## Thread-safe mapping: shardId → (ShardRange, LeaderInfo).
    shards*: seq[ShardRange]              ## sorted by startKey ascending
    leaders*: Table[uint32, LeaderInfo]   ## shardId → leader
    localNodeId*: uint32
    mu*: Lock
    timeProvider*: TimeProvider
    ## Phase 5: optional callbacks for leader-change notifications
    onLeaderChange*: LeaderChangeCallback ## nil when not configured
    leaderTtlMs*: int64 ## entries older than this are treated as stale (0 = no TTL)

# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

proc newRouterTable*(localNodeId: uint32 = 1,
    leaderTtlMs: int64 = 0,
    timeProvider: TimeProvider = nil): RouterTable =
  result = RouterTable(
    shards: @[],
    leaders: initTable[uint32, LeaderInfo](),
    localNodeId: localNodeId,
    leaderTtlMs: leaderTtlMs,
    onLeaderChange: nil,
    timeProvider: timeProvider,
  )
  initLock(result.mu)

proc rtNowMs(rt: RouterTable): int64 {.inline, raises: [].} =
  if rt.timeProvider != nil:
    try:
      return rt.timeProvider.now() div 1_000_000
    except Exception:
      discard
  (getTime().toUnixFloat() * 1000).int64

proc setLeaderChangeCallback*(rt: RouterTable,
    cb: LeaderChangeCallback) {.gcsafe, raises: [].} =
  ## Register a callback invoked whenever a shard leader changes.
  acquire(rt.mu)
  rt.onLeaderChange = cb
  release(rt.mu)

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
  ## Returns peNotLeader when no leader is currently known or the entry is stale.
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

  # Phase 5: check staleness TTL
  if rt.leaderTtlMs > 0 and leader.lastSeenMs > 0:
    let nowMs = rtNowMs(rt)
    if (nowMs - leader.lastSeenMs) > rt.leaderTtlMs:
      return peErr(newProtocolError(peNotLeader,
        &"leader entry for shard {shard.shardId} is stale (ttl={rt.leaderTtlMs}ms)"))

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
  ## Fires the onLeaderChange callback if registered.
  acquire(rt.mu)
  rt.leaders[shardId] = leader
  let cb = rt.onLeaderChange
  release(rt.mu)
  if cb != nil:
    cb(shardId, leader)

proc notLeaderRedirect*(rt: RouterTable, shardId: uint32,
    newLeader: LeaderInfo) {.gcsafe, raises: [].} =
  ## Called when a NOT_LEADER response carries a leader hint.
  ## Updates the routing table and fires onLeaderChange.
  rt.updateLeader(shardId, newLeader)

proc touchLeader*(rt: RouterTable, shardId: uint32) {.gcsafe, raises: [].} =
  ## Refresh the lastSeenMs timestamp for a shard's leader (call after a
  ## successful operation to keep the TTL alive).
  acquire(rt.mu)
  defer: release(rt.mu)
  var entry = rt.leaders.getOrDefault(shardId,
      LeaderInfo(nodeId: 0, nodeAddr: "", lastSeenMs: 0))
  if entry.nodeId != 0:
    entry.lastSeenMs = rtNowMs(rt)
    rt.leaders[shardId] = entry

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
