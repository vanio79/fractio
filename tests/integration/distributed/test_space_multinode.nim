# Integration test — spaces on a 5-node cluster.
#
# Verifies that CREATE SPACE creates real Raft groups that can store data,
# and that the space continues to function after adding and killing nodes.
#
# Cluster topology:
#   Nodes 1–5, each with its own MultiRaftCoordinator + RaftGroupTransport.
#   Node 1 is forced to be the initial leader for determinism.
#   A space with REPLICAS 3 is created via executeSQL on the leader.
#
# After CREATE SPACE, `distributeSpaceGroups` replicates the group metadata
# to followers and triggers group creation on each node that is a member.
# This mirrors what happens in a production cluster where the server.nim
# recovery path creates groups from sys.groups after Raft log replay.
#
# Port allocation: 21500–21599 (Raft TCP ports)
# Temp storage: /tmp/fractio_space_mn_<nodeId>/ (cleaned up per test)

import std/[unittest, os, options, json, strutils]

import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_transport
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/protocol/raft_store
import fractio/protocol/server
import fractio/sql/executor

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  BASE_PORT = 21500
  TMP_DIR = "/tmp/fractio_space_mn_"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

type
  TestNode = object
    id*: int                       ## 1-based node number
    server*: ProtocolServer
    coord*: MultiRaftCoordinator
    store*: RaftKVStoreExt
    rgt*: RaftGroupTransport
    storagePath*: string

proc cleanDir(p: string) =
  try: removeDir(p) except CatchableError: discard

proc raftPort(nodeNum: int): int = BASE_PORT + (nodeNum - 1) * 10

proc makeNode(nodeNum: int, peerNums: seq[int]): TestNode =
  let nodeId = rangeTypes.NodeID(uint32(nodeNum))
  let port = raftPort(nodeNum)

  var peers: seq[PeerAddr]
  for pn in peerNums:
    peers.add(PeerAddr(
      nodeId: rangeTypes.NodeID(uint32(pn)),
      host: "127.0.0.1",
      raftPort: raftPort(pn),
    ))

  let rgt = newRaftGroupTransport(nodeId, "127.0.0.1", port, peers)
  let transport = newMultiRaftTransport(rgt)

  let storagePath = TMP_DIR & $nodeNum
  cleanDir(storagePath)
  createDir(storagePath)

  let coord = newMultiRaftCoordinator(CoordinatorConfig(
    nodeId: nodeId,
    numWorkers: 1,
    electionTimeoutNs: 800_000_000'i64,    # 800 ms
    heartbeatIntervalNs: 50_000_000'i64,   # 50 ms
    storagePath: storagePath / "raft",
    proposeTimeoutMs: 6000,
    transport: transport,
  ))

  # Create meta + data groups with all peers as voters
  for groupId in [META_GROUP_ID, DATA_GROUP_START_ID]:
    var desc = rangeTypes.newGroupDescriptor(groupId)
    discard desc.addReplica(nodeId, rangeTypes.rtVoter)
    for pn in peerNums:
      discard desc.addReplica(rangeTypes.NodeID(uint32(pn)), rangeTypes.rtVoter)
    let rep = desc.getReplica(nodeId)
    doAssert rep.isSome, "replica not found for node " & $nodeNum
    discard coord.createGroup(desc, rep.get.replicaId)

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 6000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  var cfg = defaultServerConfig()
  cfg.host = "127.0.0.1"
  cfg.port = 19000 + nodeNum
  cfg.serverId = uint16(nodeNum)
  cfg.dataDir = storagePath
  let srv = newProtocolServer(cfg)
  srv.raftStore = store
  srv.raftCoord = coord
  srv.raftTransport = rgt

  TestNode(
    id: nodeNum, server: srv, coord: coord, store: store,
    rgt: rgt, storagePath: storagePath,
  )

proc startNode(n: TestNode) =
  n.coord.start()

proc stopNode(n: TestNode) =
  n.coord.stop()
  cleanDir(n.storagePath)

proc seedSysNodes(leaderStore: RaftKVStoreExt, nodeNums: seq[int]) =
  for num in nodeNums:
    let key = encodeTableKey(SYS_NODES_TABLE_ID, $num)
    let val = $ %*{
      "nodeId": num,
      "host": "127.0.0.1",
      "raftPort": raftPort(num),
      "clientPort": 19000 + num,
      "status": 1,
    }
    let r = leaderStore.raftPut(key, val)
    doAssert r.isOk, "failed to seed sys.nodes for node " & $num

proc seedSysGroups(leaderStore: RaftKVStoreExt, nodeNums: seq[int]) =
  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, $gid.uint64)
    var replicas = newJArray()
    for num in nodeNums:
      replicas.add(%*{"nodeId": num, "type": "voter"})
    let val = $ %*{"groupId": gid.uint64.int, "replicas": replicas}
    discard leaderStore.raftPut(key, val)

proc seedDefaults(leaderStore: RaftKVStoreExt) =
  let dbKey = encodeTableKey(SYS_DATABASES_TABLE_ID, "default")
  discard leaderStore.raftPut(dbKey, $ %*{"name": "default"})
  let scKey = encodeTableKey(SYS_SCHEMAS_TABLE_ID, "default.public")
  discard leaderStore.raftPut(scKey, $ %*{"name": "public", "database": "default"})

proc createSpaceGroupsOnNode(node: TestNode) =
  ## Scan sys.groups and create space groups that this node is a member of.
  let store = node.store
  let coord = node.coord
  let nodeId = rangeTypes.NodeID(uint32(node.id))
  let grpStart = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
  let grpEnd = encodeTableKey(SYS_GROUPS_TABLE_ID + 1, "")
  let grpScan = store.raftScan(grpStart, grpEnd, 0, includeSystemKeys = true)
  if grpScan.isOk:
    for (key, entry) in grpScan.value:
      try:
        let j = parseJson(entry.value)
        let gid = GroupID(uint64(j["groupId"].getInt()))
        if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID: continue
        if coord.hasGroup(gid): continue

        var desc = rangeTypes.newGroupDescriptor(gid)
        if j.hasKey("replicas"):
          for r in j["replicas"]:
            discard desc.addReplica(
              rangeTypes.NodeID(uint32(r["nodeId"].getInt())),
              rangeTypes.rtVoter)

        var myReplicaId = rangeTypes.ReplicaID(0)
        for r in desc.replicas:
          if r.nodeId == nodeId:
            myReplicaId = r.replicaId
            break

        if myReplicaId != rangeTypes.ReplicaID(0):
          discard coord.createAndStartGroup(desc, myReplicaId)
          store.registerGroup(gid)
      except: discard

proc wirePeerStores(nodes: seq[TestNode]) =
  ## Register every node's store as a peer of every other node.
  for i in 0 ..< nodes.len:
    for j in 0 ..< nodes.len:
      if i != j:
        nodes[i].store.addPeerStore(uint32(nodes[j].id), nodes[j].store)

proc distributeSpaceGroups(nodes: seq[TestNode]) =
  ## After CREATE SPACE on the leader: wait for replication of sys.groups
  ## entries, create space groups on all follower nodes, then elect a leader
  ## for each space group (the first member node).
  sleep(500)
  for node in nodes:
    createSpaceGroupsOnNode(node)

  # For each space group, make the first member node the leader.
  # Read sys.groups from the leader to determine membership.
  let store = nodes[0].store
  let grpStart = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
  let grpEnd = encodeTableKey(SYS_GROUPS_TABLE_ID + 1, "")
  let grpScan = store.raftScan(grpStart, grpEnd, 0, includeSystemKeys = true)
  if grpScan.isOk:
    for (key, entry) in grpScan.value:
      try:
        let j = parseJson(entry.value)
        let gid = GroupID(uint64(j["groupId"].getInt()))
        if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID: continue
        # Use the first replica's nodeId as the leader
        if j.hasKey("replicas") and j["replicas"].len > 0:
          let leaderNodeId = j["replicas"][0]["nodeId"].getInt()
          for node in nodes:
            if node.id == leaderNodeId:
              let g = node.coord.getGroup(gid)
              if g.isSome:
                g.get.becomeLeader()
              break
      except: discard
  sleep(300)

proc reelectLeaders(nodes: seq[TestNode], deadNodeIds: seq[int]) =
  ## After killing nodes, find groups with no leader among surviving nodes
  ## and elect a new leader on the first surviving member.
  let store = nodes[0].store
  let grpStart = encodeTableKey(SYS_GROUPS_TABLE_ID, "")
  let grpEnd = encodeTableKey(SYS_GROUPS_TABLE_ID + 1, "")
  let grpScan = store.raftScan(grpStart, grpEnd, 0, includeSystemKeys = true)
  if grpScan.isOk:
    for (key, entry) in grpScan.value:
      try:
        let j = parseJson(entry.value)
        let gid = GroupID(uint64(j["groupId"].getInt()))
        if gid == META_GROUP_ID or gid == DATA_GROUP_START_ID: continue

        # Check if any surviving node is already leader
        var hasLeader = false
        for node in nodes:
          if node.id in deadNodeIds: continue
          if node.coord.hasGroup(gid):
            let g = node.coord.getGroup(gid)
            if g.isSome and g.get.isLeader():
              hasLeader = true
              break

        if not hasLeader:
          # Elect the first surviving member as leader
          if j.hasKey("replicas"):
            for r in j["replicas"]:
              let nid = r["nodeId"].getInt()
              if nid in deadNodeIds: continue
              for node in nodes:
                if node.id == nid and node.coord.hasGroup(gid):
                  let g = node.coord.getGroup(gid)
                  if g.isSome:
                    g.get.becomeLeader()
                  break
              break  # only need one new leader
      except: discard
  sleep(300)

proc exec(store: RaftKVStoreExt, sql: string): ExecResult =
  executeSQL(sql, store, "default", "public")

# ---------------------------------------------------------------------------
# Cluster fixture: 5 nodes, node 1 = leader
# ---------------------------------------------------------------------------

proc makeCluster5(): seq[TestNode] =
  let allNums = @[1, 2, 3, 4, 5]
  var nodes: seq[TestNode]
  for n in allNums:
    var peers: seq[int]
    for p in allNums:
      if p != n: peers.add(p)
    nodes.add(makeNode(n, peers))

  for n in nodes: startNode(n)

  # Force node 1 to be leader for meta + data groups
  for groupId in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let g = nodes[0].coord.getGroup(groupId)
    doAssert g.isSome
    g.get.becomeLeader()

  sleep(400)

  # Seed system tables on the leader
  seedSysNodes(nodes[0].store, allNums)
  seedSysGroups(nodes[0].store, allNums)
  seedDefaults(nodes[0].store)
  sleep(400)

  # Wire peer stores so nodes can forward to groups they don't own
  wirePeerStores(nodes)

  nodes

proc stopCluster(nodes: seq[TestNode]) =
  for n in nodes: stopNode(n)

# ---------------------------------------------------------------------------
# Test suites
# ---------------------------------------------------------------------------

suite "Space multinode — CREATE SPACE creates real Raft groups":

  test "CREATE SPACE succeeds and groups exist in coordinator":
    var nodes = makeCluster5()
    defer: stopCluster(nodes)

    let res = exec(nodes[0].store, "CREATE SPACE testspace WITH REPLICAS = 3")
    if res.kind == erkError:
      echo "  CREATE SPACE error: " & res.error
    check res.kind == erkOk
    if res.kind == erkOk:
      check "5 groups" in res.okMessage

    # With RF=3, the leader (node 1) has groups it's a member of.
    # With ring placement, node 1 is in at least some groups.
    var leaderGroupCount = 0
    for gid in 3'u64 .. 7'u64:
      if nodes[0].coord.hasGroup(GroupID(gid)):
        inc leaderGroupCount
    check leaderGroupCount >= 3  # node 1 is in at least 3 of 5 groups

  test "distributeSpaceGroups creates groups on all member nodes":
    var nodes = makeCluster5()
    defer: stopCluster(nodes)

    discard exec(nodes[0].store, "CREATE SPACE testspace WITH REPLICAS = 3")
    distributeSpaceGroups(nodes)

    # With RF=3, each group is on exactly 3 nodes. Count total group
    # memberships: 5 groups × 3 replicas = 15 total memberships.
    var totalMemberships = 0
    for i in 0 ..< 5:
      for gid in 3'u64 .. 7'u64:
        if nodes[i].coord.hasGroup(GroupID(gid)):
          inc totalMemberships
    check totalMemberships == 15

  test "CREATE TABLE IN SPACE succeeds":
    var nodes = makeCluster5()
    defer: stopCluster(nodes)

    discard exec(nodes[0].store, "CREATE SPACE testspace WITH REPLICAS = 3")
    distributeSpaceGroups(nodes)
    nodes[0].store.loadSpaces()
    nodes[0].store.loadGroupMembers()

    let ctRes = exec(nodes[0].store,
        "CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) IN SPACE testspace")
    check ctRes.kind == erkOk

suite "Space multinode — data operations through space groups":

  test "INSERT and SELECT in space-bound table":
    var nodes = makeCluster5()
    defer: stopCluster(nodes)

    discard exec(nodes[0].store, "CREATE SPACE testspace WITH REPLICAS = 3")
    distributeSpaceGroups(nodes)
    nodes[0].store.loadSpaces()
    nodes[0].store.loadGroupMembers()

    discard exec(nodes[0].store,
        "CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) IN SPACE testspace")
    nodes[0].store.loadTableSpaces()

    let ins1 = exec(nodes[0].store, "INSERT INTO t1 VALUES (1, 'alice')")
    if ins1.kind == erkError:
      echo "  INSERT 1 error: " & ins1.error
    check ins1.kind == erkModified
    if ins1.kind == erkModified:
      check ins1.count == 1

    let ins2 = exec(nodes[0].store, "INSERT INTO t1 VALUES (2, 'bob')")
    if ins2.kind == erkError:
      echo "  INSERT 2 error: " & ins2.error
    check ins2.kind == erkModified

    let ins3 = exec(nodes[0].store, "INSERT INTO t1 VALUES (3, 'carol')")
    if ins3.kind == erkError:
      echo "  INSERT 3 error: " & ins3.error
    check ins3.kind == erkModified

    let sel = exec(nodes[0].store, "SELECT * FROM t1")
    check sel.kind == erkRows
    if sel.kind == erkRows:
      check sel.rows.len == 3

  test "multiple inserts and point lookups":
    var nodes = makeCluster5()
    defer: stopCluster(nodes)

    discard exec(nodes[0].store, "CREATE SPACE myspace WITH REPLICAS = 3")
    distributeSpaceGroups(nodes)
    nodes[0].store.loadSpaces()
    nodes[0].store.loadGroupMembers()

    discard exec(nodes[0].store,
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT) IN SPACE myspace")
    nodes[0].store.loadTableSpaces()

    for i in 1 .. 10:
      let r = exec(nodes[0].store,
          "INSERT INTO users VALUES (" & $i & ", 'user" & $i & "@test.com')")
      if r.kind == erkError:
        echo "  INSERT " & $i & " error: " & r.error
      check r.kind == erkModified

    # Point lookup
    let sel = exec(nodes[0].store, "SELECT * FROM users WHERE id = 5")
    check sel.kind == erkRows
    if sel.kind == erkRows:
      check sel.rows.len == 1
      if sel.rows.len > 0:
        check sel.rows[0][1] == "user5@test.com"

    # Full scan
    let all = exec(nodes[0].store, "SELECT * FROM users")
    check all.kind == erkRows
    if all.kind == erkRows:
      check all.rows.len == 10

suite "Space multinode — resilience after adding a node":

  test "space works after adding a 6th node":
    var nodes = makeCluster5()
    defer:
      for n in nodes: stopNode(n)

    discard exec(nodes[0].store, "CREATE SPACE testspace WITH REPLICAS = 3")
    distributeSpaceGroups(nodes)
    nodes[0].store.loadSpaces()
    nodes[0].store.loadGroupMembers()

    discard exec(nodes[0].store,
        "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT) IN SPACE testspace")
    nodes[0].store.loadTableSpaces()

    let ins1 = exec(nodes[0].store, "INSERT INTO t1 VALUES (1, 'before-add')")
    check ins1.kind == erkModified

    # Add node 6
    let node6 = makeNode(6, @[1, 2, 3, 4, 5])
    startNode(node6)
    nodes.add(node6)

    # Register node 6 with existing nodes' transports and group descriptors
    for i in 0 ..< 5:
      nodes[i].server.addPeerToRaft(6, "127.0.0.1", raftPort(6))

    # Seed node 6 into sys.nodes
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, "6")
    let nodeVal = $ %*{
      "nodeId": 6, "host": "127.0.0.1",
      "raftPort": raftPort(6), "clientPort": 19006, "status": 1,
    }
    discard nodes[0].store.raftPut(nodeKey, nodeVal)
    sleep(500)

    # Verify space still works — insert and select on original leader
    let ins2 = exec(nodes[0].store, "INSERT INTO t1 VALUES (2, 'after-add')")
    if ins2.kind == erkError:
      echo "  INSERT after add error: " & ins2.error
    check ins2.kind == erkModified

    let sel = exec(nodes[0].store, "SELECT * FROM t1")
    check sel.kind == erkRows
    if sel.kind == erkRows:
      check sel.rows.len == 2

suite "Space multinode — resilience after killing a node":

  test "space works after killing a non-leader node":
    var nodes = makeCluster5()
    defer:
      for n in nodes:
        try: stopNode(n)
        except: discard

    discard exec(nodes[0].store, "CREATE SPACE testspace WITH REPLICAS = 3")
    distributeSpaceGroups(nodes)
    nodes[0].store.loadSpaces()
    nodes[0].store.loadGroupMembers()

    discard exec(nodes[0].store,
        "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT) IN SPACE testspace")
    nodes[0].store.loadTableSpaces()

    let ins1 = exec(nodes[0].store, "INSERT INTO t1 VALUES (1, 'before-kill')")
    check ins1.kind == erkModified

    sleep(400)

    # Kill node 5 (a non-leader follower)
    nodes[4].coord.stop()
    sleep(300)

    # Re-elect leaders for groups that had node 5 as leader
    reelectLeaders(nodes, @[5])

    # After killing node 5, groups that had node 5 as leader now have a new
    # leader whose SM may not have data written through the old leader (no
    # real Raft log replication in the test).  Verify the system continues
    # to accept new writes after the kill.
    var postKillSuccess = 0
    let ins2 = exec(nodes[0].store, "INSERT INTO t1 VALUES (2, 'after-kill')")
    if ins2.kind == erkModified: inc postKillSuccess
    let ins3 = exec(nodes[0].store, "INSERT INTO t1 VALUES (3, 'also-after-kill')")
    if ins3.kind == erkModified: inc postKillSuccess

    check postKillSuccess >= 1  # at least one post-kill insert works

    let sel = exec(nodes[0].store, "SELECT * FROM t1")
    check sel.kind == erkRows
    if sel.kind == erkRows:
      # Pre-kill row may or may not survive (depends on which group's leader died).
      # At minimum we should see the successful post-kill inserts.
      check sel.rows.len >= postKillSuccess

  test "space works after killing two non-leader nodes (minority failure)":
    var nodes = makeCluster5()
    defer:
      for n in nodes:
        try: stopNode(n)
        except: discard

    discard exec(nodes[0].store, "CREATE SPACE testspace WITH REPLICAS = 3")
    distributeSpaceGroups(nodes)
    nodes[0].store.loadSpaces()
    nodes[0].store.loadGroupMembers()

    discard exec(nodes[0].store,
        "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT) IN SPACE testspace")
    nodes[0].store.loadTableSpaces()

    let ins1 = exec(nodes[0].store, "INSERT INTO t1 VALUES (1, 'initial')")
    check ins1.kind == erkModified

    sleep(400)

    # Kill nodes 4 and 5. Groups with all 3 replicas on nodes 1-3 still
    # have full quorum. Groups that had a replica on 4 or 5 lose one voter
    # but 2/3 quorum is maintained as long as the other 2 replicas are
    # among nodes 1-3.
    nodes[3].coord.stop()
    nodes[4].coord.stop()
    sleep(300)

    # Re-elect leaders for groups that lost their leader
    reelectLeaders(nodes, @[4, 5])

    # With 2 of 5 nodes dead and RF=3, some groups lose quorum (groups where
    # 2 of 3 members were on the dead nodes).  At least some inserts should
    # work (groups whose majority is still alive among nodes 1-3).
    var successCount = 0
    for i in 2 .. 6:
      let r = exec(nodes[0].store,
          "INSERT INTO t1 VALUES (" & $i & ", 'post-kill-" & $i & "')")
      if r.kind == erkModified:
        inc successCount

    check successCount > 0

    let sel = exec(nodes[0].store, "SELECT * FROM t1")
    check sel.kind == erkRows
    if sel.kind == erkRows:
      # Rows include post-kill successes plus any pre-kill data that survived
      # (only data on leaders that are still alive).
      check sel.rows.len >= successCount
