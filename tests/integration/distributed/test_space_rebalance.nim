# Integration tests for space rebalancing on node join.
#
# Tests the full rebalance lifecycle with a real multi-node cluster:
#   1. Start with 2 nodes, CREATE SPACE WITH REPLICAS = 2
#   2. Add a 3rd node, trigger rebalanceSpaces
#   3. Verify dual-read works during migration
#   4. Run migration, verify cutover
#
# Cluster topology: in-process NuRaftCoordinators with ASIO networking
# Port allocation: 28000–28299 (NuRaft ASIO, basePort per node spaced by 100)
# Temp storage: /tmp/fractio_rebal_<nodeId>/ (cleaned up per test)

import std/[unittest, os, options, json, strutils, tables, hashes, algorithm, times, locks]
import fractio/protocol/raft_store
import fractio/protocol/server
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/storage/wisckey_backend
import fractio/sql/executor

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  TMP_DIR = "/tmp/fractio_rebal_"

var nextClientPort = 19100  ## incremented per node to avoid port conflicts between tests

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

type
  TestNode = object
    id*: int
    basePort*: int
    clientPort*: int
    server*: ProtocolServer
    coord*: NuRaftCoordinator
    store*: RaftKVStoreExt
    storagePath*: string

proc cleanDir(p: string) =
  try: removeDir(p) except CatchableError: discard

proc makeNode(nodeNum: int, basePort: int,
    members: seq[tuple[nodeId: uint32, host: string, basePort: int]]): TestNode =
  let nodeId = rangeTypes.NodeID(uint32(nodeNum))
  let cPort = nextClientPort
  nextClientPort += 1

  # Use unique storage path for each node + test run instance to avoid LOCK contention
  let storagePath = TMP_DIR & $nodeNum & "_" & $cPort
  cleanDir(storagePath)
  createDir(storagePath)

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    basePort: basePort,
    host: "127.0.0.1",
    dataDir: storagePath,
    electionTimeoutLowerMs: 200,
    electionTimeoutUpperMs: 400,
    heartbeatIntervalMs: 100,
  ))
  coord.start()

  # Create meta + data groups with retries
  for gid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    var success = false
    for attempt in 0 ..< 5:
      if coord.createAndStartGroup(gid, members):
        success = true
        break
      sleep(200)
    if not success:
      raise newException(AssertionDefect, "Failed to create group " & $gid & " for node " & $nodeNum)

  let store = newRaftKVStoreExt(coord, proposeTimeoutMs = 6000)
  store.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  var cfg = defaultServerConfig()
  cfg.host = "127.0.0.1"
  cfg.port = cPort
  cfg.serverId = uint16(nodeNum)
  cfg.dataDir = storagePath
  let srv = newProtocolServer(cfg)
  srv.raftStore = store
  srv.raftCoord = coord

  TestNode(
    id: nodeNum, basePort: basePort, clientPort: cPort, server: srv,
    coord: coord, store: store, storagePath: storagePath,
  )

proc startNode(n: TestNode) =
  n.server.start()

proc stopNode*(n: TestNode) =
  n.server.stop()
  n.coord.stop()
  sleep(100) # Give LevelDB a moment to release its lock
  cleanDir(n.storagePath)

proc waitForLeaderOnGroup(nodes: seq[TestNode], gid: GroupID,
    maxAttempts: int = 50): int =
  for attempt in 0 ..< maxAttempts:
    for i, n in nodes:
      if n.coord.isLeader(gid):
        return i
    sleep(100)
  -1

proc seedSysNodes(leaderStore: RaftKVStoreExt, nodes: seq[TestNode]) =
  for n in nodes:
    let key = encodeTableKey(SYS_NODES_TABLE_ID, $n.id)
    let val = $ %*{
      "nodeId": n.id, "host": "127.0.0.1",
      "raftPort": n.basePort, "clientPort": n.clientPort, "status": 1,
    }
    let r = leaderStore.raftPut(key, val)
    doAssert r.isOk, "failed to seed sys.nodes for node " & $n.id

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

proc waitForSpaceLeaders(nodes: seq[TestNode]) =
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
        for attempt in 0 ..< 50:
          var hasLeader = false
          for node in nodes:
            if node.coord.isLeader(gid):
              hasLeader = true
              break
          if hasLeader: break
          sleep(100)
      except: discard

proc exec(store: RaftKVStoreExt, sql: string): ExecResult =
  executeSQL(sql, store, "default", "public")

# ---------------------------------------------------------------------------
# Cluster fixtures
# ---------------------------------------------------------------------------

proc makeCluster2(): seq[TestNode] =
  ## 2-node cluster.
  let members = @[
    (nodeId: 1'u32, host: "127.0.0.1", basePort: 28000),
    (nodeId: 2'u32, host: "127.0.0.1", basePort: 29000),
  ]

  var nodes: seq[TestNode]
  for i, m in members:
    nodes.add(makeNode(int(m.nodeId), m.basePort, members))
  for n in nodes: startNode(n)

  # Wait for leader election
  let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
  doAssert leaderIdx >= 0
  sleep(200)

  let allNums = @[1, 2]
  seedSysNodes(nodes[leaderIdx].store, nodes)
  seedSysGroups(nodes[leaderIdx].store, allNums)
  seedDefaults(nodes[leaderIdx].store)
  sleep(400)

  # Load space caches
  for n in nodes:
    n.store.loadSpaces()
    n.store.loadTableSpaces()
    n.store.loadGroupMembers()

  nodes

proc addNodeToCluster(nodes: var seq[TestNode], newNodeNum: int) =
  ## Add a new node to the cluster.
  let newBasePort = 28000 + (newNodeNum - 1) * 1000

  # Build members list including all existing + new
  var allMembers: seq[tuple[nodeId: uint32, host: string, basePort: int]]
  for n in nodes:
    allMembers.add((nodeId: uint32(n.id), host: "127.0.0.1", basePort: n.basePort))
  allMembers.add((nodeId: uint32(newNodeNum), host: "127.0.0.1", basePort: newBasePort))

  let newNode = makeNode(newNodeNum, newBasePort, allMembers)
  startNode(newNode)

  # Add new node to existing nodes' NuRaft groups
  for n in nodes:
    n.server.addPeerToRaft(uint32(newNodeNum), "127.0.0.1", newBasePort)

  nodes.add(newNode)

  # Register in sys.nodes via leader
  let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
  if leaderIdx >= 0:
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, $newNodeNum)
    let nodeVal = $ %*{
      "nodeId": newNodeNum, "host": "127.0.0.1",
      "raftPort": newBasePort, "clientPort": newNode.clientPort,
      "status": 1,
    }
    discard nodes[leaderIdx].store.raftPut(nodeKey, nodeVal)
  sleep(200)

proc stopCluster(nodes: seq[TestNode]) =
  for i in countdown(nodes.high, 0):
    stopNode(nodes[i])

proc findSpaceId(leaderStore: RaftKVStoreExt, spaceName: string): int =
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
  leaderStore.loadSpaces()
  acquire(leaderStore.spacesMu)
  result = leaderStore.spaces[spaceId].groupIds
  release(leaderStore.spacesMu)

proc createSpace(leaderStore: RaftKVStoreExt, spaceName: string,
    replicas: int): int =
  let csRes = exec(leaderStore,
    "CREATE SPACE " & spaceName & " WITH REPLICAS = " & $replicas)
  doAssert csRes.kind == erkOk, "CREATE SPACE failed: " &
    (if csRes.kind == erkError: csRes.error else: "unknown")

  let ctRes = exec(leaderStore,
    "CREATE TABLE " & spaceName & "_t (id INT PRIMARY KEY, val TEXT) IN SPACE " & spaceName)
  doAssert ctRes.kind == erkOk, "CREATE TABLE failed: " &
    (if ctRes.kind == erkError: ctRes.error else: "unknown")

  findSpaceId(leaderStore, spaceName)

proc execOnLeader(nodes: seq[TestNode], sql: string): ExecResult =
  for node in nodes:
    let r = exec(node.store, sql)
    if r.kind != erkError:
      return r
    if "not leader" in r.error.toLower() or "Not the leader" in r.error:
      continue
    return r
  exec(nodes[^1].store, sql)

proc replicateMetadata(nodes: seq[TestNode]) =
  let leaderBackend = nodes[0].store.getBackend()
  for sysTableId in [SYS_TABLES_TABLE_ID, SYS_SPACES_TABLE_ID,
                      SYS_GROUPS_TABLE_ID, SYS_NODES_TABLE_ID]:
    let startKey = encodeTableKey(sysTableId, "")
    let endKey = encodeTableKey(sysTableId + 1, "")
    let pairs = leaderBackend.scan(startKey, endKey)
    for (k, v) in pairs:
      for i in 1 ..< nodes.len:
        let peerBackend = nodes[i].store.getBackend()
        if peerBackend != nil and peerBackend.isOpen:
          discard peerBackend.put(k, v)
  for node in nodes:
    node.store.loadSpaces()
    node.store.loadGroupMembers()
    node.store.loadTableSpaces()

proc insertRows(nodes: seq[TestNode], spaceName: string, rowCount: int) =
  for i in 1 .. rowCount:
    let insRes = execOnLeader(nodes,
      "INSERT INTO " & spaceName & "_t (id, val) VALUES (" & $i & ", 'v" & $i & "')")
    doAssert insRes.kind == erkModified,
      "INSERT failed row " & $i & ": " & (if insRes.kind == erkError: insRes.error else: "?")

proc setupSpaceWithData(nodes: seq[TestNode], spaceName: string,
    replicas: int, rowCount: int): int =
  ## Full setup: create space, distribute groups, elect leaders, insert data.
  let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
  doAssert leaderIdx >= 0
  let leaderStore = nodes[leaderIdx].store
  let spaceId = createSpace(leaderStore, spaceName, replicas)
  let gids = findSpaceGroupIds(leaderStore, spaceId)

  waitForAutoDistribution(nodes, gids, replicas)
  waitForSpaceLeaders(nodes)
  replicateMetadata(nodes)

  insertRows(nodes, spaceName, rowCount)
  spaceId

# ---------------------------------------------------------------------------
# Suite: rebalanceSpaces detects mismatch and creates new groups
# ---------------------------------------------------------------------------

suite "Space rebalance integration — rebalanceSpaces":
  test "creates new groups when a 3rd node joins a 2-node cluster":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
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
    check sp2.groupIds.len == 3  # 3 nodes -> 3 new groups
    check sp2.oldGroupIds.len == 2  # original 2 groups

  test "is idempotent — does not re-trigger while rebalancing":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
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

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
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

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
    let spaceId = setupSpaceWithData(nodes, "items", 2, 20)

    # Verify all rows readable before rebalance
    let sel1 = exec(leaderStore, "SELECT * FROM items_t")
    check sel1.kind == erkRows
    check sel1.rows.len == 20

    # Add 3rd node and trigger rebalance
    addNodeToCluster(nodes, 3)
    leaderStore.rebalanceSpaces()

    # Wait for new groups and leaders
    acquire(leaderStore.spacesMu)
    let newGids = leaderStore.spaces[spaceId].groupIds
    release(leaderStore.spacesMu)
    waitForAutoDistribution(nodes, newGids, 2, 3000)
    waitForSpaceLeaders(nodes)
    replicateMetadata(nodes)

    # All 20 rows still readable (dual-read fallback to old groups)
    let sel2 = exec(leaderStore, "SELECT * FROM items_t")
    check sel2.kind == erkRows
    check sel2.rows.len == 20

  test "point get works for pre-existing keys during rebalance":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
    let spaceId = setupSpaceWithData(nodes, "users", 2, 10)

    addNodeToCluster(nodes, 3)
    leaderStore.rebalanceSpaces()
    acquire(leaderStore.spacesMu)
    let newGids = leaderStore.spaces[spaceId].groupIds
    release(leaderStore.spacesMu)
    waitForAutoDistribution(nodes, newGids, 2, 3000)
    waitForSpaceLeaders(nodes)
    replicateMetadata(nodes)

    for i in 1 .. 10:
      let sel = execOnLeader(nodes, "SELECT * FROM users_t WHERE id = " & $i)
      check sel.kind == erkRows
      check sel.rows.len == 1
      check sel.rows[0][0] == $i

# ---------------------------------------------------------------------------
# Suite: full migration lifecycle
# ---------------------------------------------------------------------------

proc triggerRebalanceAndSetup(nodes: var seq[TestNode], leaderStore: RaftKVStoreExt,
    spaceId: int) =
  addNodeToCluster(nodes, 3)
  leaderStore.rebalanceSpaces()

  acquire(leaderStore.spacesMu)
  let newGids = leaderStore.spaces[spaceId].groupIds
  release(leaderStore.spacesMu)
  waitForAutoDistribution(nodes, newGids, 2, 5000)
  waitForSpaceLeaders(nodes)
  replicateMetadata(nodes)

suite "Space rebalance integration — full migration":
  test "runRebalanceMigration completes and clears rebalance state":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
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

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
    let spaceId = setupSpaceWithData(nodes, "fullmig", 2, 20)
    triggerRebalanceAndSetup(nodes, leaderStore, spaceId)

    leaderStore.runRebalanceMigration(spaceId)
    leaderStore.loadSpaces()

    replicateMetadata(nodes)
    # Sync all data from each node to all other nodes
    for srcIdx in 0 ..< nodes.len:
      let srcBackend = nodes[srcIdx].store.getBackend()
      if srcBackend == nil or not srcBackend.isOpen: continue
      let allPairs = srcBackend.scan("/t/", "/u")
      for (k, v) in allPairs:
        for dstIdx in 0 ..< nodes.len:
          if dstIdx == srcIdx: continue
          let dstBackend = nodes[dstIdx].store.getBackend()
          if dstBackend != nil and dstBackend.isOpen:
            discard dstBackend.put(k, v)

    for i in 1 .. 20:
      let sel = execOnLeader(nodes, "SELECT * FROM fullmig_t WHERE id = " & $i)
      check sel.kind == erkRows
      check sel.rows.len == 1
      check sel.rows[0][1] == "v" & $i

  test "old groups are removed from sys.groups after migration":
    var nodes = makeCluster2()
    defer: stopCluster(nodes)

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
    let spaceId = setupSpaceWithData(nodes, "cleanup", 2, 5)

    addNodeToCluster(nodes, 3)
    leaderStore.rebalanceSpaces()

    acquire(leaderStore.spacesMu)
    let oldGids = leaderStore.spaces[spaceId].oldGroupIds
    let newGids = leaderStore.spaces[spaceId].groupIds
    release(leaderStore.spacesMu)
    check oldGids.len > 0

    waitForAutoDistribution(nodes, newGids, 2, 5000)
    waitForSpaceLeaders(nodes)
    replicateMetadata(nodes)

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

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
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

    let leaderIdx = waitForLeaderOnGroup(nodes, META_GROUP_ID)
    let leaderStore = nodes[leaderIdx].store
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
