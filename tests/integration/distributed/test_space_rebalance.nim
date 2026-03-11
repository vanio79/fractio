# Integration tests for space rebalancing on node join.
#
# Tests the full rebalance lifecycle with a real multi-node cluster:
#   1. Start with 2 nodes, CREATE SPACE WITH REPLICAS = 2
#   2. Add a 3rd node, trigger rebalanceSpaces
#   3. Verify dual-read works during migration
#   4. Run migration, verify cutover
#
# Cluster topology: in-process coordinators with RaftGroupTransport
# Port allocation: 22500–22599 (Raft TCP ports)
# Temp storage: /tmp/fractio_rebal_<nodeId>/ (cleaned up per test)

import std/[unittest, os, options, json, strutils, tables, hashes, algorithm, times, locks]
import fractio/protocol/raft_store
import fractio/distributed/raft/multigroup_coordinator
import fractio/distributed/raft/multigroup_transport
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/sql/executor

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  BASE_PORT = 22500
  TMP_DIR = "/tmp/fractio_rebal_"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

type
  TestNode = object
    id*: int
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
    electionTimeoutNs: 800_000_000'i64,
    heartbeatIntervalNs: 50_000_000'i64,
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
    doAssert rep.isSome
    discard coord.createGroup(desc, rep.get.replicaId)

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 6000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  TestNode(
    id: nodeNum, coord: coord, store: store,
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
      "nodeId": num, "host": "127.0.0.1",
      "raftPort": raftPort(num), "clientPort": 19000 + num, "status": 1,
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
  discard leaderStore.raftPut(
    encodeTableKey(SYS_DATABASES_TABLE_ID, "default"),
    $ %*{"name": "default"})
  discard leaderStore.raftPut(
    encodeTableKey(SYS_SCHEMAS_TABLE_ID, "default.public"),
    $ %*{"name": "public", "database": "default"})
  # Seed default space (replicas=0 = ALL, single group = meta group)
  discard leaderStore.raftPut(
    encodeSpaceKey(1),
    $ %*{"spaceId": 1, "name": "default", "replicas": 0,
         "groupCount": 1, "groupIds": [1]})

proc wirePeerStores(nodes: seq[TestNode]) =
  for i in 0 ..< nodes.len:
    for j in 0 ..< nodes.len:
      if i != j:
        nodes[i].store.addPeerStore(uint32(nodes[j].id), nodes[j].store)

proc waitForAutoDistribution(nodes: seq[TestNode], expectedGroupIds: seq[uint64],
    replicaCount: int, maxWaitMs: int = 3000) =
  let expectedTotal = expectedGroupIds.len * replicaCount
  let stepMs = 50
  var waited = 0
  while waited < maxWaitMs:
    var totalMemberships = 0
    for node in nodes:
      for gid in expectedGroupIds:
        if node.coord.hasGroup(GroupID(gid)):
          inc totalMemberships
    if totalMemberships >= expectedTotal:
      break
    sleep(stepMs)
    waited += stepMs

proc electSpaceLeaders(nodes: seq[TestNode]) =
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

proc exec(store: RaftKVStoreExt, sql: string): ExecResult =
  executeSQL(sql, store, "default", "public")

# ---------------------------------------------------------------------------
# Cluster fixtures
# ---------------------------------------------------------------------------

proc makeCluster2(): seq[TestNode] =
  ## 2-node cluster, node 1 = leader.
  let allNums = @[1, 2]
  var nodes: seq[TestNode]
  for n in allNums:
    var peers: seq[int]
    for p in allNums:
      if p != n: peers.add(p)
    nodes.add(makeNode(n, peers))
  for n in nodes: startNode(n)

  # Force node 1 to be leader
  for groupId in [META_GROUP_ID, DATA_GROUP_START_ID]:
    let g = nodes[0].coord.getGroup(groupId)
    doAssert g.isSome
    g.get.becomeLeader()
  sleep(400)

  seedSysNodes(nodes[0].store, allNums)
  seedSysGroups(nodes[0].store, allNums)
  seedDefaults(nodes[0].store)
  sleep(400)

  wirePeerStores(nodes)

  # Load space caches
  for n in nodes:
    n.store.loadSpaces()
    n.store.loadTableSpaces()
    n.store.loadGroupMembers()

  nodes

proc addNodeToCluster(nodes: var seq[TestNode], newNodeNum: int) =
  ## Add a new node to the cluster: create it, wire peer stores,
  ## register in sys.nodes.
  var allNums: seq[int]
  for n in nodes: allNums.add(n.id)
  allNums.add(newNodeNum)

  # Create the new node with all existing nodes as peers
  var peerNums: seq[int]
  for n in nodes: peerNums.add(n.id)
  let newNode = makeNode(newNodeNum, peerNums)
  startNode(newNode)

  # Add new node as peer to existing nodes' transports
  for n in nodes:
    n.rgt.addPeer(rangeTypes.NodeID(uint32(newNodeNum)),
        "127.0.0.1", raftPort(newNodeNum))

  # Add new node as peer replica to existing Raft groups
  for n in nodes:
    withLock n.coord.groupsLock:
      for groupId, group in n.coord.groups:
        discard group.descriptor.addReplica(
          rangeTypes.NodeID(uint32(newNodeNum)), rangeTypes.rtVoter)

  nodes.add(newNode)

  # Re-wire all peer stores
  wirePeerStores(nodes)

  # Register in sys.nodes via leader
  let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, $newNodeNum)
  let nodeVal = $ %*{
    "nodeId": newNodeNum, "host": "127.0.0.1",
    "raftPort": raftPort(newNodeNum), "clientPort": 19000 + newNodeNum,
    "status": 1,
  }
  discard nodes[0].store.raftPut(nodeKey, nodeVal)
  sleep(200)

proc stopCluster(nodes: seq[TestNode]) =
  for n in nodes: stopNode(n)

proc findSpaceId(leaderStore: RaftKVStoreExt, spaceName: string): int =
  ## Look up a space's ID by name from sys.spaces.
  let spacesStart = encodeTableKey(SYS_SPACES_TABLE_ID, "")
  let spacesEnd = encodeTableKey(SYS_SPACES_TABLE_ID + 1, "")
  let sr = leaderStore.raftScan(spacesStart, spacesEnd, 0, includeSystemKeys = true)
  if sr.isOk:
    for (k, entry) in sr.value:
      try:
        let j = parseJson(entry.value)
        if j["name"].getStr() == spaceName:
          return j["spaceId"].getInt()
      except: discard
  doAssert false, "space '" & spaceName & "' not found"

proc findSpaceGroupIds(leaderStore: RaftKVStoreExt, spaceId: int): seq[uint64] =
  ## Look up group IDs for a space.
  leaderStore.loadSpaces()
  acquire(leaderStore.spacesMu)
  result = leaderStore.spaces[spaceId].groupIds
  release(leaderStore.spacesMu)

proc createSpace(leaderStore: RaftKVStoreExt, spaceName: string,
    replicas: int): int =
  ## Create a space and table. Returns spaceId.
  ## Caller must distribute groups + elect leaders before inserting data.
  let csRes = exec(leaderStore,
    "CREATE SPACE " & spaceName & " WITH REPLICAS = " & $replicas)
  doAssert csRes.kind == erkOk, "CREATE SPACE failed: " &
    (if csRes.kind == erkError: csRes.error else: "unknown")

  let ctRes = exec(leaderStore,
    "CREATE TABLE " & spaceName & "_t (id INT PRIMARY KEY, val TEXT) IN SPACE " & spaceName)
  doAssert ctRes.kind == erkOk, "CREATE TABLE failed: " &
    (if ctRes.kind == erkError: ctRes.error else: "unknown")

  findSpaceId(leaderStore, spaceName)

proc insertRows(leaderStore: RaftKVStoreExt, spaceName: string, rowCount: int) =
  ## Insert rows into <spaceName>_t.
  for i in 1 .. rowCount:
    let insRes = exec(leaderStore,
      "INSERT INTO " & spaceName & "_t (id, val) VALUES (" & $i & ", 'v" & $i & "')")
    doAssert insRes.kind == erkModified,
      "INSERT failed row " & $i & ": " & (if insRes.kind == erkError: insRes.error else: "?")

proc setupSpaceWithData(nodes: seq[TestNode], spaceName: string,
    replicas: int, rowCount: int): int =
  ## Full setup: create space, distribute groups, elect leaders, insert data.
  let leaderStore = nodes[0].store
  let spaceId = createSpace(leaderStore, spaceName, replicas)
  let gids = findSpaceGroupIds(leaderStore, spaceId)

  waitForAutoDistribution(nodes, gids, replicas)
  electSpaceLeaders(nodes)
  for n in nodes:
    n.store.loadSpaces()
    n.store.loadTableSpaces()
    n.store.loadGroupMembers()

  insertRows(leaderStore, spaceName, rowCount)
  spaceId

# ---------------------------------------------------------------------------
# Suite: rebalanceSpaces detects mismatch and creates new groups
# ---------------------------------------------------------------------------

suite "Space rebalance integration — rebalanceSpaces":
  test "creates new groups when a 3rd node joins a 2-node cluster":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderStore = nodes[0].store
    let spaceId = setupSpaceWithData(nodes, "orders", 2, 10)

    # Verify: 2 groups, not rebalancing
    acquire(leaderStore.spacesMu)
    let sp1 = leaderStore.spaces[spaceId]
    release(leaderStore.spacesMu)
    check sp1.groupIds.len == 2
    check sp1.rebalancing == false

    # Add a 3rd node
    addNodeToCluster(nodes, 3)

    # Trigger rebalance
    leaderStore.rebalanceSpaces()

    # Verify: now rebalancing, 3 new groups, old groups preserved
    acquire(leaderStore.spacesMu)
    let sp2 = leaderStore.spaces[spaceId]
    release(leaderStore.spacesMu)
    check sp2.rebalancing == true
    check sp2.groupIds.len == 3  # 3 nodes → 3 new groups
    check sp2.oldGroupIds.len == 2  # original 2 groups

  test "is idempotent — does not re-trigger while rebalancing":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderStore = nodes[0].store
    let spaceId = setupSpaceWithData(nodes, "products", 2, 5)

    addNodeToCluster(nodes, 3)
    leaderStore.rebalanceSpaces()

    acquire(leaderStore.spacesMu)
    let firstNewGroups = leaderStore.spaces[spaceId].groupIds
    release(leaderStore.spacesMu)

    # Call again — should not create more groups
    leaderStore.rebalanceSpaces()

    acquire(leaderStore.spacesMu)
    let secondNewGroups = leaderStore.spaces[spaceId].groupIds
    release(leaderStore.spacesMu)
    check secondNewGroups == firstNewGroups

  test "skips space where group count already matches node count":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderStore = nodes[0].store
    let spaceId = setupSpaceWithData(nodes, "stable", 2, 3)

    # Don't add any nodes — group count (2) == node count (2)
    leaderStore.rebalanceSpaces()

    acquire(leaderStore.spacesMu)
    let sp = leaderStore.spaces[spaceId]
    release(leaderStore.spacesMu)
    check sp.rebalancing == false

# ---------------------------------------------------------------------------
# Suite: reads work during rebalance (dual-read mode)
# ---------------------------------------------------------------------------

suite "Space rebalance integration — reads during migration":
  test "SELECT returns all rows during rebalance":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderStore = nodes[0].store
    let spaceId = setupSpaceWithData(nodes, "items", 2, 20)

    # Verify all rows readable before rebalance
    let sel1 = exec(leaderStore, "SELECT * FROM items_t")
    check sel1.kind == erkRows
    check sel1.rows.len == 20

    # Add 3rd node and trigger rebalance
    addNodeToCluster(nodes, 3)
    leaderStore.rebalanceSpaces()

    # Wait for new groups and elect leaders
    acquire(leaderStore.spacesMu)
    let newGids = leaderStore.spaces[spaceId].groupIds
    release(leaderStore.spacesMu)
    waitForAutoDistribution(nodes, newGids, 2, 3000)
    electSpaceLeaders(nodes)
    for n in nodes:
      n.store.loadSpaces()
      n.store.loadGroupMembers()

    # All 20 rows still readable (dual-read fallback to old groups)
    let sel2 = exec(leaderStore, "SELECT * FROM items_t")
    check sel2.kind == erkRows
    check sel2.rows.len == 20

  test "point get works for pre-existing keys during rebalance":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderStore = nodes[0].store
    let spaceId = setupSpaceWithData(nodes, "users", 2, 10)

    addNodeToCluster(nodes, 3)
    leaderStore.rebalanceSpaces()
    acquire(leaderStore.spacesMu)
    let newGids = leaderStore.spaces[spaceId].groupIds
    release(leaderStore.spacesMu)
    waitForAutoDistribution(nodes, newGids, 2, 3000)
    electSpaceLeaders(nodes)
    for n in nodes:
      n.store.loadSpaces()
      n.store.loadGroupMembers()

    for i in 1 .. 10:
      let sel = exec(leaderStore, "SELECT * FROM users_t WHERE id = " & $i)
      check sel.kind == erkRows
      check sel.rows.len == 1
      check sel.rows[0][0] == $i

# ---------------------------------------------------------------------------
# Suite: full migration lifecycle
# ---------------------------------------------------------------------------

proc triggerRebalanceAndSetup(nodes: var seq[TestNode], leaderStore: RaftKVStoreExt,
    spaceId: int) =
  ## Add 3rd node, trigger rebalance, wait for new groups, elect leaders.
  addNodeToCluster(nodes, 3)
  leaderStore.rebalanceSpaces()

  acquire(leaderStore.spacesMu)
  let newGids = leaderStore.spaces[spaceId].groupIds
  release(leaderStore.spacesMu)
  waitForAutoDistribution(nodes, newGids, 2, 5000)
  electSpaceLeaders(nodes)
  for n in nodes:
    n.store.loadSpaces()
    n.store.loadTableSpaces()
    n.store.loadGroupMembers()

suite "Space rebalance integration — full migration":
  test "runRebalanceMigration completes and clears rebalance state":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderStore = nodes[0].store
    let spaceId = setupSpaceWithData(nodes, "migrate", 2, 30)
    triggerRebalanceAndSetup(nodes, leaderStore, spaceId)

    # Verify rebalancing is active
    acquire(leaderStore.spacesMu)
    var sp = leaderStore.spaces[spaceId]
    release(leaderStore.spacesMu)
    check sp.rebalancing == true

    # Run migration
    leaderStore.runRebalanceMigration(spaceId)
    leaderStore.loadSpaces()

    # Verify rebalance is complete
    acquire(leaderStore.spacesMu)
    sp = leaderStore.spaces[spaceId]
    release(leaderStore.spacesMu)
    check sp.rebalancing == false
    check sp.oldGroupIds.len == 0
    check sp.rebalanceWorker == 0

    # All data still accessible
    let sel = exec(leaderStore, "SELECT * FROM migrate_t")
    check sel.kind == erkRows
    check sel.rows.len == 30

  test "data is fully accessible after migration (point gets)":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderStore = nodes[0].store
    let spaceId = setupSpaceWithData(nodes, "fullmig", 2, 20)
    triggerRebalanceAndSetup(nodes, leaderStore, spaceId)

    leaderStore.runRebalanceMigration(spaceId)
    leaderStore.loadSpaces()

    for i in 1 .. 20:
      let sel = exec(leaderStore, "SELECT * FROM fullmig_t WHERE id = " & $i)
      check sel.kind == erkRows
      check sel.rows.len == 1
      check sel.rows[0][1] == "v" & $i

  test "old groups are removed from sys.groups after migration":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderStore = nodes[0].store
    let spaceId = setupSpaceWithData(nodes, "cleanup", 2, 5)

    addNodeToCluster(nodes, 3)
    leaderStore.rebalanceSpaces()

    acquire(leaderStore.spacesMu)
    let oldGids = leaderStore.spaces[spaceId].oldGroupIds
    let newGids = leaderStore.spaces[spaceId].groupIds
    release(leaderStore.spacesMu)
    check oldGids.len > 0

    waitForAutoDistribution(nodes, newGids, 2, 5000)
    electSpaceLeaders(nodes)
    for n in nodes:
      n.store.loadSpaces()
      n.store.loadTableSpaces()
      n.store.loadGroupMembers()

    leaderStore.runRebalanceMigration(spaceId)

    # Old groups should be removed from sys.groups
    for oldGid in oldGids:
      let gkey = encodeTableKey(SYS_GROUPS_TABLE_ID, $oldGid)
      let gr = leaderStore.raftGet(gkey)
      check gr.isOk
      check gr.value.isNone

# ---------------------------------------------------------------------------
# Suite: crash safety — rebalance state persists
# ---------------------------------------------------------------------------

suite "Space rebalance integration — crash safety":
  test "rebalance state persists through loadSpaces reload":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderStore = nodes[0].store
    let spaceId = setupSpaceWithData(nodes, "crash", 2, 10)

    addNodeToCluster(nodes, 3)
    leaderStore.rebalanceSpaces()

    # Reload caches (simulates restart reading persisted state)
    leaderStore.loadSpaces()

    acquire(leaderStore.spacesMu)
    let sp = leaderStore.spaces[spaceId]
    release(leaderStore.spacesMu)
    check sp.rebalancing == true
    check sp.oldGroupIds.len > 0
    check sp.groupIds.len == 3

  test "runRebalanceMigration is idempotent (re-run after completion is no-op)":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderStore = nodes[0].store
    let spaceId = setupSpaceWithData(nodes, "idem", 2, 10)
    triggerRebalanceAndSetup(nodes, leaderStore, spaceId)

    leaderStore.runRebalanceMigration(spaceId)
    leaderStore.loadSpaces()

    # All data accessible
    let sel1 = exec(leaderStore, "SELECT * FROM idem_t")
    check sel1.kind == erkRows
    check sel1.rows.len == 10

    # Re-run — should be a no-op (not rebalancing anymore)
    leaderStore.runRebalanceMigration(spaceId)

    let sel2 = exec(leaderStore, "SELECT * FROM idem_t")
    check sel2.kind == erkRows
    check sel2.rows.len == 10
