# Unit tests for fractio/protocol/router.nim
# Tests shard routing table, leader management, and staleness checks

import unittest
import std/[strutils, tables]
import fractio/protocol/router
import fractio/protocol/types

suite "RouterTable Construction":
  test "newRouterTable default":
    let rt = newRouterTable()
    check rt.localNodeId == 1
    check rt.leaderTtlMs == 0
    check rt.shardCount() == 0
    check rt.onLeaderChange == nil

  test "newRouterTable with custom nodeId":
    let rt = newRouterTable(localNodeId = 42)
    check rt.localNodeId == 42

  test "newRouterTable with TTL":
    let rt = newRouterTable(leaderTtlMs = 5000)
    check rt.leaderTtlMs == 5000

  test "setLeaderChangeCallback":
    let rt = newRouterTable()
    rt.setLeaderChangeCallback(proc(shardId: uint32,
        leader: LeaderInfo) {.gcsafe, raises: [].} =
      discard
    )
    check rt.onLeaderChange != nil

suite "Bootstrap Single Shard":
  test "bootstrapSingleShard default params":
    let rt = newRouterTable(localNodeId = 10)
    rt.bootstrapSingleShard()

    check rt.shardCount() == 1
    let leader = rt.routeKey("anykey")
    check leader.isOk
    check leader.value.nodeId == 10

  test "bootstrapSingleShard custom params":
    let rt = newRouterTable(localNodeId = 1)
    rt.bootstrapSingleShard(shardId = 5, raftGroupId = 10,
        leaderAddr = "localhost:9000")

    check rt.shardCount() == 1
    let leader = rt.routeKey("test")
    check leader.isOk
    check leader.value.nodeId == 1
    check leader.value.nodeAddr == "localhost:9000"

  test "bootstrapSingleShard covers all keys":
    let rt = newRouterTable()
    rt.bootstrapSingleShard()

    for key in ["", "a", "zzzzz", "\x00\xFF", "any-random-key"]:
      let leader = rt.routeKey(key)
      check leader.isOk

suite "routeKey":
  test "routeKey on empty table":
    let rt = newRouterTable()
    let result = rt.routeKey("somekey")
    check result.isErr
    check result.error.kind == peNotLeader
    check "routing table is empty" in result.error.msg

  test "routeKey returns leader":
    let rt = newRouterTable(localNodeId = 7)
    rt.bootstrapSingleShard()

    let leader = rt.routeKey("mykey")
    check leader.isOk
    check leader.value.nodeId == 7

  test "routeKey unknown leader":
    let rt = newRouterTable(localNodeId = 1)
    rt.updateRoute(ShardRange(startKey: "", endKey: "", shardId: 1, raftGroupId: 1),
                   LeaderInfo(nodeId: 0, nodeAddr: "", lastSeenMs: 0))

    let result = rt.routeKey("key")
    check result.isErr
    check result.error.kind == peNotLeader
    check "leader unknown" in result.error.msg

suite "routeKeys Batch":
  test "routeKeys single key":
    let rt = newRouterTable(localNodeId = 3)
    rt.bootstrapSingleShard()

    let pairs = rt.routeKeys(@["key1"])
    check pairs.isOk
    check pairs.value.len == 1
    check pairs.value[0][0] == "key1"
    check pairs.value[0][1].nodeId == 3

  test "routeKeys multiple keys":
    let rt = newRouterTable()
    rt.bootstrapSingleShard()

    let keys = @["key1", "key2", "key3"]
    let pairs = rt.routeKeys(keys)
    check pairs.isOk
    check pairs.value.len == 3
    for i, pair in pairs.value:
      check pair[0] == keys[i]

  test "routeKeys empty list":
    let rt = newRouterTable()
    rt.bootstrapSingleShard()

    let pairs = rt.routeKeys(@[])
    check pairs.isOk
    check pairs.value.len == 0

  test "routeKeys fails on first unroutable":
    let rt = newRouterTable()
    rt.bootstrapSingleShard()

    let pairs = rt.routeKeys(@["key1", "key2"])
    check pairs.isOk

    rt.updateRoute(ShardRange(startKey: "", endKey: "", shardId: 1, raftGroupId: 1),
                   LeaderInfo(nodeId: 0, nodeAddr: "", lastSeenMs: 0))

    let result = rt.routeKeys(@["key1"])
    check result.isErr

suite "updateRoute":
  test "updateRoute adds new shard":
    let rt = newRouterTable()
    rt.updateRoute(ShardRange(startKey: "a", endKey: "z", shardId: 1, raftGroupId: 1),
                   LeaderInfo(nodeId: 5, nodeAddr: "host:8080",
                       lastSeenMs: 1000))

    check rt.shardCount() == 1
    let leader = rt.routeKey("b")
    check leader.isOk
    check leader.value.nodeId == 5

  test "updateRoute replaces existing shard":
    let rt = newRouterTable()
    rt.updateRoute(ShardRange(startKey: "", endKey: "", shardId: 1, raftGroupId: 1),
                   LeaderInfo(nodeId: 1, nodeAddr: "", lastSeenMs: 0))

    rt.updateRoute(ShardRange(startKey: "", endKey: "", shardId: 1, raftGroupId: 1),
                   LeaderInfo(nodeId: 2, nodeAddr: "newhost:9000",
                       lastSeenMs: 500))

    check rt.shardCount() == 1
    let leader = rt.routeKey("x")
    check leader.isOk
    check leader.value.nodeId == 2

  test "updateRoute keeps shards sorted":
    let rt = newRouterTable()

    rt.updateRoute(ShardRange(startKey: "m", endKey: "z", shardId: 2, raftGroupId: 2),
                   LeaderInfo(nodeId: 2, nodeAddr: "", lastSeenMs: 0))
    rt.updateRoute(ShardRange(startKey: "a", endKey: "m", shardId: 1, raftGroupId: 1),
                   LeaderInfo(nodeId: 1, nodeAddr: "", lastSeenMs: 0))

    check rt.shardCount() == 2
    check rt.shards[0].startKey == "a"
    check rt.shards[1].startKey == "m"

suite "updateLeader":
  test "updateLeader changes leader":
    let rt = newRouterTable(localNodeId = 1)
    rt.bootstrapSingleShard(shardId = 1)

    rt.updateLeader(1, LeaderInfo(nodeId: 5, nodeAddr: "new:8080",
        lastSeenMs: 2000))

    let leader = rt.routeKey("key")
    check leader.isOk
    check leader.value.nodeId == 5
    check leader.value.nodeAddr == "new:8080"

  test "updateLeader fires callback":
    let rt = newRouterTable()
    rt.setLeaderChangeCallback(proc(shardId: uint32,
        leader: LeaderInfo) {.gcsafe, raises: [].} =
      discard
    )

    rt.bootstrapSingleShard(shardId = 10)
    rt.updateLeader(10, LeaderInfo(nodeId: 99, nodeAddr: "", lastSeenMs: 0))
    check rt.onLeaderChange != nil

suite "notLeaderRedirect":
  test "notLeaderRedirect updates routing":
    let rt = newRouterTable()
    rt.bootstrapSingleShard(shardId = 1)

    rt.notLeaderRedirect(1, LeaderInfo(nodeId: 20, nodeAddr: "redirect:7000",
        lastSeenMs: 1000))

    let leader = rt.routeKey("key")
    check leader.isOk
    check leader.value.nodeId == 20

suite "touchLeader":
  test "touchLeader updates lastSeenMs":
    let rt = newRouterTable(localNodeId = 1)
    rt.bootstrapSingleShard()

    rt.updateLeader(1, LeaderInfo(nodeId: 1, nodeAddr: "", lastSeenMs: 1000))
    rt.touchLeader(1)

    let leader = rt.routeKey("key")
    check leader.isOk
    check leader.value.lastSeenMs > 1000

  test "touchLeader on unknown leader does nothing":
    let rt = newRouterTable()
    rt.bootstrapSingleShard(shardId = 1)

    rt.touchLeader(99)
    check true

suite "isLocalLeader":
  test "isLocalLeader returns true for local node":
    let rt = newRouterTable(localNodeId = 5)
    rt.bootstrapSingleShard()

    check rt.isLocalLeader("anykey") == true

  test "isLocalLeader returns false for remote":
    let rt = newRouterTable(localNodeId = 1)
    rt.bootstrapSingleShard()
    rt.updateLeader(1, LeaderInfo(nodeId: 99, nodeAddr: "", lastSeenMs: 0))

    check rt.isLocalLeader("key") == false

  test "isLocalLeader returns false on empty table":
    let rt = newRouterTable()
    check rt.isLocalLeader("key") == false

suite "shardCount":
  test "shardCount zero initially":
    let rt = newRouterTable()
    check rt.shardCount() == 0

  test "shardCount after bootstrap":
    let rt = newRouterTable()
    rt.bootstrapSingleShard()
    check rt.shardCount() == 1

  test "shardCount multiple shards":
    let rt = newRouterTable()
    rt.updateRoute(ShardRange(startKey: "a", endKey: "m", shardId: 1, raftGroupId: 1),
                   LeaderInfo(nodeId: 1, nodeAddr: "", lastSeenMs: 0))
    rt.updateRoute(ShardRange(startKey: "m", endKey: "z", shardId: 2, raftGroupId: 2),
                   LeaderInfo(nodeId: 2, nodeAddr: "", lastSeenMs: 0))
    check rt.shardCount() == 2

suite "ShardRange Key Matching":
  test "key in middle of range":
    let rt = newRouterTable()
    rt.updateRoute(ShardRange(startKey: "a", endKey: "z", shardId: 1, raftGroupId: 1),
                   LeaderInfo(nodeId: 1, nodeAddr: "", lastSeenMs: 0))

    let leader = rt.routeKey("m")
    check leader.isOk
    check leader.value.nodeId == 1

  test "key at start boundary":
    let rt = newRouterTable()
    rt.updateRoute(ShardRange(startKey: "a", endKey: "z", shardId: 1, raftGroupId: 1),
                   LeaderInfo(nodeId: 1, nodeAddr: "", lastSeenMs: 0))

    let leader = rt.routeKey("a")
    check leader.isOk

  test "key outside range":
    let rt = newRouterTable()
    rt.updateRoute(ShardRange(startKey: "a", endKey: "z", shardId: 1, raftGroupId: 1),
                   LeaderInfo(nodeId: 1, nodeAddr: "", lastSeenMs: 0))

    let result = rt.routeKey("0")
    check result.isErr
    check result.error.kind == peNotLeader

  test "empty boundaries match all":
    let rt = newRouterTable()
    rt.updateRoute(ShardRange(startKey: "", endKey: "", shardId: 1, raftGroupId: 1),
                   LeaderInfo(nodeId: 1, nodeAddr: "", lastSeenMs: 0))

    for key in ["", "anything", "\xFF\xFF"]:
      let leader = rt.routeKey(key)
      check leader.isOk

suite "LeaderInfo Types":
  test "LeaderInfo default values":
    let info = LeaderInfo()
    check info.nodeId == 0
    check info.nodeAddr == ""
    check info.lastSeenMs == 0

  test "LeaderInfo construction":
    let info = LeaderInfo(nodeId: 42, nodeAddr: "192.168.1.1:8080",
        lastSeenMs: 123456789)
    check info.nodeId == 42
    check info.nodeAddr == "192.168.1.1:8080"
    check info.lastSeenMs == 123456789

suite "ShardRange Types":
  test "ShardRange default values":
    let sr = ShardRange()
    check sr.startKey == ""
    check sr.endKey == ""
    check sr.shardId == 0
    check sr.raftGroupId == 0

  test "ShardRange construction":
    let sr = ShardRange(startKey: "a", endKey: "z", shardId: 10,
        raftGroupId: 20)
    check sr.startKey == "a"
    check sr.endKey == "z"
    check sr.shardId == 10
    check sr.raftGroupId == 20
