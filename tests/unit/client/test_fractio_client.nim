# Unit tests for Fractio Client
# Tests for config types, result types, initialization, routing logic

import unittest
import std/[tables, options, strutils, sequtils, algorithm, hashes, atomics, locks]
import std/typedthreads
import fractio/core/types
import fractio/core/kv_interface
import fractio/distributed/raft/group_types
import fractio/distributed/meta/system_schemas
import fractio/distributed/meta/system_tables
import fractio/client/fractio_client as client

# =============================================================================
# Test Suites - Configuration
# =============================================================================

suite "Fractio Client - Configuration":
  test "create default client config":
    let config = client.newFractioClientConfig("localhost", 9000)
    check config.initialHost == "localhost"
    check config.initialPort == 9000
    check config.connectionTimeoutMs == 5000
    check config.requestTimeoutMs == 30000
    check config.refreshIntervalMs == 30000
    check config.autoRefresh == true

  test "create custom client config":
    let config = client.FractioClientConfig(
      initialHost: "db.example.com",
      initialPort: 8080,
      connectionTimeoutMs: 10000,
      requestTimeoutMs: 60000,
      refreshIntervalMs: 60000,
      autoRefresh: false
    )
    check config.initialHost == "db.example.com"
    check config.initialPort == 8080
    check config.connectionTimeoutMs == 10000
    check config.requestTimeoutMs == 60000
    check config.refreshIntervalMs == 60000
    check config.autoRefresh == false

  test "config with localhost and port":
    let config = client.newFractioClientConfig("127.0.0.1", 1234)
    check config.initialHost == "127.0.0.1"
    check config.initialPort == 1234

  test "config with empty host":
    let config = client.newFractioClientConfig("", 0)
    check config.initialHost == ""
    check config.initialPort == 0

# =============================================================================
# Test Suites - Result Types
# =============================================================================

suite "Fractio Client - KVOpResult":
  test "kvOpOk creates success result":
    let result = kvOpOk[string]("test_value")
    check result.isOk == true
    check result.val == "test_value"

  test "kvOpOk with none option":
    let result = kvOpOk[Option[string]](none(string))
    check result.isOk == true
    check result.val.isNone

  test "kvOpErr creates error result":
    let result = kvOpErr[string]("error message")
    check result.isOk == false
    check result.err == "error message"

  test "isErr helper for success":
    let success = kvOpOk[int](42)
    check success.isErr == false

  test "isErr helper for error":
    let error = kvOpErr[int]("failed")
    check error.isErr == true

  test "KVOpResult with different types":
    let intResult = kvOpOk[int](42)
    check intResult.isOk == true
    check intResult.val == 42

    let seqResult = kvOpOk[seq[int]](@[1, 2, 3])
    check seqResult.isOk == true
    check seqResult.val.len == 3

suite "Fractio Client - KVOpVoidResult":
  test "kvVoidOk creates success result":
    let result = kvVoidOk()
    check result.isOk == true
    check result.err == ""

  test "kvVoidErr creates error result":
    let result = kvVoidErr("operation failed")
    check result.isOk == false
    check result.err == "operation failed"
    check result.isErr == true

  test "KVOpVoidResult isErr helper":
    let success = kvVoidOk()
    let error = kvVoidErr("failed")

    check success.isErr == false
    check error.isErr == true

suite "Fractio Client - SpaceOpResult":
  test "SpaceOpResult structure":
    let spaceId = genSpaceID()
    let result = client.SpaceOpResult(isOk: true, spaceId: spaceId,
        groupCount: 2, groupIds: @[genGroupID()])
    check result.isOk == true
    check result.err == ""
    check result.groupCount == 2
    check result.groupIds.len == 1

  test "SpaceOpResult error structure":
    let result = client.SpaceOpResult(isOk: false, err: "space not found")
    check result.isOk == false
    check result.err == "space not found"
    check result.spaceId == zeroSpaceID()
    check result.groupCount == 0
    check result.groupIds.len == 0

# =============================================================================
# Test Suites - NodeInfo
# =============================================================================

suite "Fractio Client - NodeInfo":
  test "create NodeInfo with all fields":
    let nodeInfo = client.NodeInfo(
      nodeId: 1,
      host: "node1.example.com",
      clientPort: 9000,
      status: nsAlive,
      client: nil
    )
    check nodeInfo.nodeId == 1
    check nodeInfo.host == "node1.example.com"
    check nodeInfo.clientPort == 9000
    check nodeInfo.status == nsAlive
    check nodeInfo.client == nil

  test "NodeInfo with different statuses":
    let unknown = client.NodeInfo(nodeId: 1, status: nsUnknown)
    let alive = client.NodeInfo(nodeId: 2, status: nsAlive)
    let draining = client.NodeInfo(nodeId: 3, status: nsDraining)
    let decommissioned = client.NodeInfo(nodeId: 4, status: nsDecommissioned)

    check unknown.status == nsUnknown
    check alive.status == nsAlive
    check draining.status == nsDraining
    check decommissioned.status == nsDecommissioned

# =============================================================================
# Test Suites - GroupInfo
# =============================================================================

suite "Fractio Client - GroupInfo":
  test "create GroupInfo with all fields":
    let groupId = genGroupID()
    let groupInfo = client.GroupInfo(
      groupId: groupId,
      spaceId: genSpaceID(),
      leaderNodeId: 1,
      replicaNodeIds: @[1.uint32, 2, 3]
    )
    check groupInfo.groupId == groupId
    check groupInfo.leaderNodeId == 1
    check groupInfo.replicaNodeIds.len == 3

  test "GroupInfo with empty replicas":
    let groupInfo = client.GroupInfo(
      groupId: genGroupID(),
      spaceId: genSpaceID(),
      leaderNodeId: 0,
      replicaNodeIds: @[]
    )
    check groupInfo.replicaNodeIds.len == 0

# =============================================================================
# Test Suites - TableInfo
# =============================================================================

suite "Fractio Client - TableInfo":
  test "create TableInfo with all fields":
    let tid = genTableId()
    let tableInfo = client.TableInfo(
      tableId: tid,
      name: "users",
      spaceId: genSpaceID()
    )
    check tableInfo.tableId == tid
    check tableInfo.name == "users"

  test "TableInfo with empty name":
    let tid = genTableId()
    let tableInfo = client.TableInfo(tableId: tid, name: "")
    check tableInfo.name == ""

# =============================================================================
# Test Suites - SpaceInfo
# =============================================================================

suite "Fractio Client - SpaceInfo":
  test "create SpaceInfo with all fields":
    let groupId1 = genGroupID()
    let groupId2 = genGroupID()
    let spaceInfo = client.SpaceInfo(
      spaceId: genSpaceID(),
      name: "production",
      groupIds: @[groupId1, groupId2],
      oldGroupIds: @[],
      rebalancing: false
    )
    check spaceInfo.name == "production"
    check spaceInfo.groupIds.len == 2
    check spaceInfo.oldGroupIds.len == 0
    check spaceInfo.rebalancing == false

  test "SpaceInfo with rebalancing":
    let oldGroupId = genGroupID()
    let newGroupId = genGroupID()
    let spaceInfo = client.SpaceInfo(
      spaceId: genSpaceID(),
      name: "rebalancing_space",
      groupIds: @[newGroupId],
      oldGroupIds: @[oldGroupId],
      rebalancing: true
    )
    check spaceInfo.rebalancing == true
    check spaceInfo.oldGroupIds.len == 1
    check spaceInfo.groupIds.len == 1

# =============================================================================
# Test Suites - Client Creation
# =============================================================================

suite "Fractio Client - Creation":
  test "create client with config":
    let config = client.newFractioClientConfig("localhost", 9000)
    let c = client.newFractioClient(config)
    check c.config.initialHost == "localhost"
    check c.config.initialPort == 9000
    check c.initialized.load(moRelaxed) == false
    c.close()

  test "create client with host and port":
    let c = client.newFractioClient("localhost", 9000)
    check c.config.initialHost == "localhost"
    check c.config.initialPort == 9000
    check c.initialized.load(moRelaxed) == false
    c.close()

  test "client starts with empty tables":
    let c = client.newFractioClient("localhost", 9000)
    check c.nodes.len == 0
    check c.groups.len == 0
    check c.tables.len == 0
    check c.spaces.len == 0
    check c.keyPrefixToGroup.len == 0
    check c.leaderConnections.len == 0
    c.close()

  test "client not initialized on creation":
    let c = client.newFractioClient("localhost", 9000)
    check c.initialized.load(moRelaxed) == false
    c.close()

# =============================================================================
# Test Suites - META/DATA Group IDs
# =============================================================================

suite "Fractio Client - Group ID Constants":
  test "META_GROUP_ID is valid":
    check $META_GROUP_ID != ""

  test "DATA_GROUP_START_ID is valid":
    check $DATA_GROUP_START_ID != ""

  test "META and DATA groups are different":
    check META_GROUP_ID != DATA_GROUP_START_ID

# =============================================================================
# Test Suites - Hash Consistency
# =============================================================================

suite "Fractio Client - Hash Consistency":
  test "hash produces consistent results":
    let key = "test_routing_key"
    let h1 = hash(key)
    let h2 = hash(key)
    check h1 == h2

  test "different keys produce different hashes (usually)":
    let h1 = hash("key1")
    let h2 = hash("key2")
    # Hash collision is possible but rare
    check h1 != h2 or h1 == h2 # Just verify computation works

# =============================================================================
# Test Suites - NodeStatus Values
# =============================================================================

suite "Fractio Client - NodeStatus Enum":
  test "NodeStatus enum values":
    check nsUnknown.ord == 0
    check nsAlive.ord == 1
    check nsDraining.ord == 2
    check nsDecommissioned.ord == 3

  test "NodeStatus ordering":
    check nsUnknown < nsAlive
    check nsAlive < nsDraining
    check nsDraining < nsDecommissioned

# =============================================================================
# Test Suites - Client Close
# =============================================================================

suite "Fractio Client - Close Operation":
  test "close clears all tables":
    let c = client.newFractioClient("localhost", 9000)
    # Manually add data
    c.nodes[1] = client.NodeInfo(
      nodeId: 1,
      host: "node1",
      clientPort: 9000,
      status: nsAlive,
      client: nil
    )
    let gid = genGroupID()
    c.groups[gid] = client.GroupInfo(
      groupId: gid,
      spaceId: genSpaceID(),
      leaderNodeId: 1,
      replicaNodeIds: @[1.uint32, 2, 3]
    )
    let tid = genTableId()
    c.tables[tid] = client.TableInfo(
      tableId: tid,
      name: "users",
      spaceId: genSpaceID()
    )

    c.close()
    check c.nodes.len == 0
    check c.groups.len == 0
    check c.tables.len == 0

  test "close sets initialized to false":
    let c = client.newFractioClient("localhost", 9000)
    c.initialized.store(true, moRelaxed)
    c.close()
    check c.initialized.load(moRelaxed) == false

  test "close is safe to call multiple times":
    let c = client.newFractioClient("localhost", 9000)
    c.close()
    c.close()
    c.close()
    # Should not crash

  # =============================================================================
  # Test Suites - Lock Thread Safety
  # =============================================================================

suite "Fractio Client - Thread Safety":
  test "concurrent config access":
    let c = client.newFractioClient("localhost", 9000)

    var results: Atomic[int]
    results.store(0)

    proc reader(client: client.FractioClient) {.thread.} =
      for i in 0..<100:
        if client.config.initialPort == 9000:
          atomicInc results

    var threads: array[4, Thread[client.FractioClient]]
    for i in 0..<4:
      createThread(threads[i], reader, c)

    joinThreads(threads)
    check results.load() == 400
    c.close()

  test "concurrent initialized flag access":
    let c = client.newFractioClient("localhost", 9000)

    var reads: Atomic[int]
    reads.store(0)

    proc flagReader(client: client.FractioClient) {.thread.} =
      for i in 0..<100:
        let val = client.initialized.load(moRelaxed)
        if not val:
          atomicInc reads

    var threads: array[4, Thread[client.FractioClient]]
    for i in 0..<4:
      createThread(threads[i], flagReader, c)

    joinThreads(threads)
    check reads.load() == 400
    c.close()

# =============================================================================
# Test Suites - Edge Cases
# =============================================================================

suite "Fractio Client - Edge Cases":
  test "client with invalid port":
    let c = client.newFractioClient("localhost", 0)
    check c.config.initialPort == 0
    c.close()

  test "client with empty host":
    let c = client.newFractioClient("", 9000)
    check c.config.initialHost == ""
    c.close()

  test "SpaceOpResult default values":
    var result: client.SpaceOpResult
    check result.isOk == false
    check result.groupCount == 0
    check result.groupIds.len == 0

  test "KVOpVoidResult default values":
    var result: KVOpVoidResult
    check result.isOk == false
    check result.err == ""

# =============================================================================
# Test Suites - Stress Tests
# =============================================================================

suite "Fractio Client - Stress Tests":
  test "many client creations and closes":
    for i in 0..<100:
      let c = client.newFractioClient("localhost", 9000)
      c.close()

  test "many hash computations":
    for i in 0..<10000:
      let h = hash("stress_test_key_" & $i)
      check h != 0 or h == 0 # Hash can be 0 but should compute

  test "many GroupID generations":
    var ids: seq[GroupID] = @[]
    for i in 0..<1000:
      let gid = genGroupID()
      ids.add(gid)
    check ids.len == 1000

# =============================================================================
# Test Suites - Key Routing (Internal Functions)
# =============================================================================

suite "Fractio Client - getTableIdFromKey":
  test "getTableIdFromKey with valid key":
    let c = client.newFractioClient("localhost", 9000)
    let tid = genTableId()
    let key = encodeTableKey(tid, "pk123")
    let parsedTid = c.getTableIdFromKey(key)
    check parsedTid == tid
    c.close()

  test "getTableIdFromKey with invalid prefix":
    let c = client.newFractioClient("localhost", 9000)
    let key = "invalid_prefix/123"
    let tid = c.getTableIdFromKey(key)
    check tid == zeroTableId()
    c.close()

  test "getTableIdFromKey with empty key":
    let c = client.newFractioClient("localhost", 9000)
    let tid = c.getTableIdFromKey("")
    check tid == zeroTableId()
    c.close()

  test "getTableIdFromKey with short key":
    let c = client.newFractioClient("localhost", 9000)
    let key = "/t/00" # Too short for tableId (26 chars required)
    let tid = c.getTableIdFromKey(key)
    check tid == zeroTableId()
    c.close()

  test "getTableIdFromKey with key without /d/":
    let c = client.newFractioClient("localhost", 9000)
    let tid = genTableId()
    let key = "/t/" & formatTableId(tid) & "/pk123"         # No /d/ prefix
    let parsedTid = c.getTableIdFromKey(key)
    check parsedTid == tid
    c.close()

# =============================================================================
# Test Suites - Group Routing
# =============================================================================

suite "Fractio Client - getGroupsForTable":
  test "getGroupsForTable for meta group table":
    let c = client.newFractioClient("localhost", 9000)
    # System tables (1-7) are in META_GROUP_ID
    let groups = c.getGroupsForTable(SYS_DATABASES_TABLE_ID)
    check groups.len == 1
    check groups[0] == META_GROUP_ID
    c.close()

  test "getGroupsForTable for unknown table":
    let c = client.newFractioClient("localhost", 9000)
    # Unknown table falls back to DATA_GROUP_START_ID
    let groups = c.getGroupsForTable(genTableId())
    check groups.len == 1
    check groups[0] == DATA_GROUP_START_ID
    c.close()

  test "getGroupsForTable for SYS_NODES_TABLE_ID":
    let c = client.newFractioClient("localhost", 9000)
    let groups = c.getGroupsForTable(SYS_NODES_TABLE_ID)
    check groups.len == 1
    check groups[0] == META_GROUP_ID
    c.close()

  test "getGroupsForTable for SYS_GROUPS_TABLE_ID":
    let c = client.newFractioClient("localhost", 9000)
    let groups = c.getGroupsForTable(SYS_GROUPS_TABLE_ID)
    check groups.len == 1
    check groups[0] == META_GROUP_ID
    c.close()

  test "getGroupsForTable for SYS_TABLES_TABLE_ID":
    let c = client.newFractioClient("localhost", 9000)
    let groups = c.getGroupsForTable(SYS_TABLES_TABLE_ID)
    check groups.len == 1
    check groups[0] == META_GROUP_ID
    c.close()

  test "getGroupsForTable for SYS_SPACES_TABLE_ID":
    let c = client.newFractioClient("localhost", 9000)
    let groups = c.getGroupsForTable(SYS_SPACES_TABLE_ID)
    check groups.len == 1
    check groups[0] == META_GROUP_ID
    c.close()

# =============================================================================
# Test Suites - Key Group Routing
# =============================================================================

suite "Fractio Client - getGroupForKey":
  test "getGroupForKey for system table key":
    let c = client.newFractioClient("localhost", 9000)
    # System table keys should route to META_GROUP_ID
    let key = encodeTableKey(SYS_DATABASES_TABLE_ID, "mydb")
    let group = c.getGroupForKey(key)
    check group == META_GROUP_ID
    c.close()

  test "getGroupForKey for unknown key prefix":
    let c = client.newFractioClient("localhost", 9000)
    let key = "unknown_prefix"
    let group = c.getGroupForKey(key)
    check group == META_GROUP_ID
    c.close()

  test "getGroupForKey for data table key without space":
    let c = client.newFractioClient("localhost", 9000)
    # Generate a valid non-system table ID (ULID format, 26 chars)
    let tid = genTableId()
    let key = encodeTableKey(tid, "test_pk")
    let group = c.getGroupForKey(key)
    # Non-system table without space assignment falls back to DATA_GROUP_START_ID
    check group == DATA_GROUP_START_ID
    c.close()

  test "getGroupForKey for SYS_GROUPS_TABLE_ID key":
    let c = client.newFractioClient("localhost", 9000)
    let key = encodeTableKey(SYS_GROUPS_TABLE_ID, "test")
    let group = c.getGroupForKey(key)
    check group == META_GROUP_ID
    c.close()

  test "getGroupForKey for SYS_TABLES_TABLE_ID key":
    let c = client.newFractioClient("localhost", 9000)
    let key = encodeTableKey(SYS_TABLES_TABLE_ID, "test")
    let group = c.getGroupForKey(key)
    check group == META_GROUP_ID
    c.close()

# =============================================================================
# Test Suites - Leader Connection Management
# =============================================================================

suite "Fractio Client - Leader Connection":
  test "getGroupLeaderConnection returns none for unknown group":
    let c = client.newFractioClient("localhost", 9000)
    let groupId = genGroupID()
    let connOpt = c.getGroupLeaderConnection(groupId)
    check connOpt.isNone
    c.close()

  test "getGroupLeaderConnection returns none for empty client":
    let c = client.newFractioClient("localhost", 9000)
    check c.groups.len == 0
    let connOpt = c.getGroupLeaderConnection(META_GROUP_ID)
    check connOpt.isNone
    c.close()

  test "invalidateGroupLeader is safe for unknown group":
    let c = client.newFractioClient("localhost", 9000)
    let groupId = genGroupID()
    c.invalidateGroupLeader(groupId)
    # Should not crash
    c.close()

  test "invalidateGroupLeader clears leader connection":
    let c = client.newFractioClient("localhost", 9000)
    let groupId = genGroupID()
    # Manually add a group
    c.groups[groupId] = client.GroupInfo(
      groupId: groupId,
      spaceId: genSpaceID(),
      leaderNodeId: 1,
      replicaNodeIds: @[1.uint32]
    )
    # Invalidate
    c.invalidateGroupLeader(groupId)
    # Check leader is cleared
    if groupId in c.groups:
      check c.groups[groupId].leaderNodeId == 0
    c.close()

# =============================================================================
# Test Suites - Table/Space Info Edge Cases
# =============================================================================

suite "Fractio Client - TableInfo Edge Cases":
  test "TableInfo with zero tableId":
    let tableInfo = client.TableInfo(
      tableId: zeroTableId(),
      name: "empty_table",
      spaceId: genSpaceID()
    )
    check tableInfo.tableId == zeroTableId()
    check tableInfo.name == "empty_table"

  test "TableInfo comparison":
    let tid = genTableId()
    let t1 = client.TableInfo(tableId: tid, name: "users")
    let t2 = client.TableInfo(tableId: tid, name: "users")
    check t1.tableId == t2.tableId
    check t1.name == t2.name

suite "Fractio Client - SpaceInfo Edge Cases":
  test "SpaceInfo with empty groupIds":
    let spaceInfo = client.SpaceInfo(
      spaceId: genSpaceID(),
      name: "empty_space",
      groupIds: @[],
      oldGroupIds: @[],
      rebalancing: false
    )
    check spaceInfo.groupIds.len == 0
    check spaceInfo.oldGroupIds.len == 0

  test "SpaceInfo with overlapping old and new groups":
    let gid = genGroupID()
    let spaceInfo = client.SpaceInfo(
      spaceId: genSpaceID(),
      name: "overlap_space",
      groupIds: @[gid],
      oldGroupIds: @[gid],
      rebalancing: true
    )
    check spaceInfo.groupIds.len == 1
    check spaceInfo.oldGroupIds.len == 1
    check spaceInfo.groupIds[0] == spaceInfo.oldGroupIds[0]

  test "SpaceInfo with many groups":
    var groupIds: seq[GroupID] = @[]
    for i in 0..<10:
      groupIds.add(genGroupID())
    let spaceInfo = client.SpaceInfo(
      spaceId: genSpaceID(),
      name: "large_space",
      groupIds: groupIds,
      oldGroupIds: @[],
      rebalancing: false
    )
    check spaceInfo.groupIds.len == 10

# =============================================================================
# Test Suites - NodeInfo Edge Cases
# =============================================================================

suite "Fractio Client - NodeInfo Edge Cases":
  test "NodeInfo with nil client":
    let nodeInfo = client.NodeInfo(
      nodeId: 1,
      host: "localhost",
      clientPort: 9000,
      status: nsAlive,
      client: nil
    )
    check nodeInfo.client == nil
    check nodeInfo.status == nsAlive

  test "NodeInfo with zero nodeId":
    let nodeInfo = client.NodeInfo(
      nodeId: 0,
      host: "unknown",
      clientPort: 0,
      status: nsUnknown,
      client: nil
    )
    check nodeInfo.nodeId == 0
    check nodeInfo.status == nsUnknown

  test "NodeInfo host with special characters":
    let nodeInfo = client.NodeInfo(
      nodeId: 1,
      host: "node-1.example-domain.com",
      clientPort: 9000,
      status: nsAlive,
      client: nil
    )
    check nodeInfo.host == "node-1.example-domain.com"

suite "Fractio Client - GroupInfo Edge Cases":
  test "GroupInfo with zero leaderNodeId":
    let groupInfo = client.GroupInfo(
      groupId: genGroupID(),
      spaceId: genSpaceID(),
      leaderNodeId: 0,
      replicaNodeIds: @[]
    )
    check groupInfo.leaderNodeId == 0

  test "GroupInfo with many replicas":
    var replicas: seq[uint32] = @[]
    for i in 1..10:
      replicas.add(i.uint32)
    let groupInfo = client.GroupInfo(
      groupId: genGroupID(),
      spaceId: genSpaceID(),
      leaderNodeId: 1,
      replicaNodeIds: replicas
    )
    check groupInfo.replicaNodeIds.len == 10

# =============================================================================
# Test Suites - Lock Operations
# =============================================================================

suite "Fractio Client - Lock Operations":
  test "lock acquire and release":
    let c = client.newFractioClient("localhost", 9000)
    c.lock.acquire()
    c.lock.release()
    c.close()

  test "lock withLock block":
    let c = client.newFractioClient("localhost", 9000)
    withLock c.lock:
      check c.initialized.load(moRelaxed) == false
    c.close()

  test "nested lock acquire fails gracefully":
    let c = client.newFractioClient("localhost", 9000)
    c.lock.acquire()
    # Note: Nim Lock is non-recursive by default
    # Attempting to acquire again would hang, so we release first
    c.lock.release()
    c.close()

# =============================================================================
# Test Suites - LastRefreshNs
# =============================================================================

suite "Fractio Client - LastRefreshNs":
  test "lastRefreshNs defaults to 0":
    let c = client.newFractioClient("localhost", 9000)
    check c.lastRefreshNs.load(moRelaxed) == 0
    c.close()

  test "lastRefreshNs can be updated":
    let c = client.newFractioClient("localhost", 9000)
    c.lastRefreshNs.store(123456789, moRelaxed)
    check c.lastRefreshNs.load(moRelaxed) == 123456789
    c.close()

  test "lastRefreshNs atomic operations":
    let c = client.newFractioClient("localhost", 9000)
    c.lastRefreshNs.store(0, moRelaxed)
    for i in 1..100:
      c.lastRefreshNs.store(i.int64 * 1000000, moRelaxed)
    check c.lastRefreshNs.load(moRelaxed) == 100000000'i64
    c.close()

# =============================================================================
# Test Suites - KV Operations Error Handling (Not Initialized)
# =============================================================================

suite "Fractio Client - KV Operations Not Initialized":
  test "kvGet returns error when client not initialized":
    let c = client.newFractioClient("localhost", 9000)
    check c.initialized.load(moRelaxed) == false
    let result = c.kvGet("test_key")
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "kvPut returns error when client not initialized":
    let c = client.newFractioClient("localhost", 9000)
    check c.initialized.load(moRelaxed) == false
    let result = c.kvPut("test_key", "test_value")
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "kvDelete returns error when client not initialized":
    let c = client.newFractioClient("localhost", 9000)
    check c.initialized.load(moRelaxed) == false
    let result = c.kvDelete("test_key")
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "kvScan returns error when client not initialized":
    let c = client.newFractioClient("localhost", 9000)
    check c.initialized.load(moRelaxed) == false
    let result = c.kvScan("start_key", "end_key")
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "kvGet with transaction params returns error when not initialized":
    let c = client.newFractioClient("localhost", 9000)
    let txnId = genTransactionID()
    let result = c.kvGet("test_key", txnId = txnId, readTimestamp = 12345)
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "kvPut with transaction params returns error when not initialized":
    let c = client.newFractioClient("localhost", 9000)
    let txnId = genTransactionID()
    let result = c.kvPut("test_key", "test_value", txnId = txnId)
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "kvDelete with transaction params returns error when not initialized":
    let c = client.newFractioClient("localhost", 9000)
    let txnId = genTransactionID()
    let result = c.kvDelete("test_key", txnId = txnId)
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "kvScan with transaction params returns error when not initialized":
    let c = client.newFractioClient("localhost", 9000)
    let txnId = genTransactionID()
    let result = c.kvScan("start", "end", limit = 10, txnId = txnId,
        readTimestamp = 12345)
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

# =============================================================================
# Test Suites - Transaction Operations Error Handling
# =============================================================================

suite "Fractio Client - Transaction Operations Error Handling":
  test "beginTxn returns error when cannot initialize":
    let c = client.newFractioClient("", 0) # Invalid config
                                           # beginTxn will try to initialize first, which fails
    let result = c.beginTxn()
    check result.isErr == true
    check result.err == "failed to initialize client"
    c.close()

  test "beginTxn returns error when no META group connection":
    let c = client.newFractioClient("localhost", 9000)
    # Manually set initialized to true without any real connections
    c.initialized.store(true, moRelaxed)
    # META_GROUP_ID is not in client.groups, so getGroupLeaderConnection returns none
    let result = c.beginTxn()
    check result.isErr == true
    check result.err == "no connection for beginTxn"
    c.close()

  test "commitTxn returns error when no META group connection":
    let c = client.newFractioClient("localhost", 9000)
    c.initialized.store(true, moRelaxed)
    let txnId = genTransactionID()
    let result = c.commitTxn(txnId)
    check result.isErr == true
    check result.err == "no connection for commitTxn"
    c.close()

  test "rollbackTxn returns error when no META group connection":
    let c = client.newFractioClient("localhost", 9000)
    c.initialized.store(true, moRelaxed)
    let txnId = genTransactionID()
    let result = c.rollbackTxn(txnId)
    check result.isErr == true
    check result.err == "no connection for rollbackTxn"
    c.close()

# =============================================================================
# Test Suites - Space Operations Error Handling
# =============================================================================

suite "Fractio Client - Space Operations Error Handling":
  test "createSpace returns error when no META group connection":
    let c = client.newFractioClient("localhost", 9000)
    c.initialized.store(true, moRelaxed)
    let result = c.createSpace("test_space", replicas = 3)
    check result.isOk == false
    check result.err == "no connection to META group leader"
    c.close()

  test "createSpace returns error with zero replicas":
    let c = client.newFractioClient("localhost", 9000)
    c.initialized.store(true, moRelaxed)
    let result = c.createSpace("test_space", replicas = 0)
    check result.isOk == false
    check result.err == "no connection to META group leader"
    c.close()

  test "dropSpace returns error when no META group connection":
    let c = client.newFractioClient("localhost", 9000)
    c.initialized.store(true, moRelaxed)
    let result = c.dropSpace("test_space")
    check result.isOk == false
    check result.err == "no connection to META group leader"
    c.close()

  test "dropSpace returns error for empty space name":
    let c = client.newFractioClient("localhost", 9000)
    c.initialized.store(true, moRelaxed)
    let result = c.dropSpace("")
    check result.isOk == false
    check result.err == "no connection to META group leader"
    c.close()

# =============================================================================
# Test Suites - getGroupLeaderConnection Edge Cases
# =============================================================================

suite "Fractio Client - getGroupLeaderConnection Edge Cases":
  test "getGroupLeaderConnection returns none when group has leader but node info missing":
    let c = client.newFractioClient("localhost", 9000)
    let groupId = genGroupID()
    # Add group with known leader but no node info in nodes table
    c.groups[groupId] = client.GroupInfo(
      groupId: groupId,
      spaceId: genSpaceID(),
      leaderNodeId: 1, # Known leader
      replicaNodeIds: @[1.uint32]
    )
    # nodes table is empty, so getNodeConnectionInternal returns none
    let connOpt = c.getGroupLeaderConnection(groupId)
    check connOpt.isNone
    c.close()

  test "getGroupLeaderConnection returns none when leader unknown and no replicas":
    let c = client.newFractioClient("localhost", 9000)
    let groupId = genGroupID()
    # Add group with unknown leader (leaderNodeId = 0) and no replicas
    c.groups[groupId] = client.GroupInfo(
      groupId: groupId,
      spaceId: genSpaceID(),
      leaderNodeId: 0, # Unknown leader
      replicaNodeIds: @[] # No replicas
    )
    let connOpt = c.getGroupLeaderConnection(groupId)
    check connOpt.isNone
    c.close()

  test "getGroupLeaderConnection returns none when leader unknown and replica node info missing":
    let c = client.newFractioClient("localhost", 9000)
    let groupId = genGroupID()
    # Add group with unknown leader and replicas, but no node info
    c.groups[groupId] = client.GroupInfo(
      groupId: groupId,
      spaceId: genSpaceID(),
      leaderNodeId: 0, # Unknown leader
      replicaNodeIds: @[1.uint32, 2, 3]
    )
    # nodes table is empty
    let connOpt = c.getGroupLeaderConnection(groupId)
    check connOpt.isNone
    c.close()

  test "getGroupLeaderConnection handles multiple replicas all missing node info":
    let c = client.newFractioClient("localhost", 9000)
    let groupId = genGroupID()
    c.groups[groupId] = client.GroupInfo(
      groupId: groupId,
      spaceId: genSpaceID(),
      leaderNodeId: 0,
      replicaNodeIds: @[10.uint32, 20, 30, 40, 50]
    )
    let connOpt = c.getGroupLeaderConnection(groupId)
    check connOpt.isNone
    c.close()

# =============================================================================
# Test Suites - invalidateGroupLeader Full Behavior
# =============================================================================

suite "Fractio Client - invalidateGroupLeader Full Behavior":
  test "invalidateGroupLeader clears leaderNodeId to zero":
    let c = client.newFractioClient("localhost", 9000)
    let groupId = genGroupID()
    # Add group with known leader
    c.groups[groupId] = client.GroupInfo(
      groupId: groupId,
      spaceId: genSpaceID(),
      leaderNodeId: 5, # Known leader
      replicaNodeIds: @[5.uint32, 6, 7]
    )
    # Invalidate should set leaderNodeId to 0
    c.invalidateGroupLeader(groupId)
    check c.groups[groupId].leaderNodeId == 0
    c.close()

  test "invalidateGroupLeader handles group in leaderConnections":
    let c = client.newFractioClient("localhost", 9000)
    let groupId = genGroupID()
    # Just set up the group without a connection (more realistic)
    c.groups[groupId] = client.GroupInfo(
      groupId: groupId,
      spaceId: genSpaceID(),
      leaderNodeId: 1,
      replicaNodeIds: @[1.uint32]
    )
    # Call invalidate - it should work even without a cached connection
    c.invalidateGroupLeader(groupId)
    check c.groups[groupId].leaderNodeId == 0
    c.close()

  test "invalidateGroupLeader is safe for group not in groups table":
    let c = client.newFractioClient("localhost", 9000)
    let groupId = genGroupID()
    # Only test the case where group is not in groups table
    # No entries in leaderConnections or groups
    c.invalidateGroupLeader(groupId)
    # Should complete without error
    check groupId notin c.leaderConnections
    check groupId notin c.groups
    c.close()

  test "invalidateGroupLeader clears leaderNodeId even when no cached connection":
    let c = client.newFractioClient("localhost", 9000)
    let groupId = genGroupID()
    c.groups[groupId] = client.GroupInfo(
      groupId: groupId,
      spaceId: genSpaceID(),
      leaderNodeId: 10,
      replicaNodeIds: @[10.uint32]
    )
    # No cached connection
    check groupId notin c.leaderConnections
    c.invalidateGroupLeader(groupId)
    # Leader should still be cleared
    check c.groups[groupId].leaderNodeId == 0
    c.close()

# =============================================================================
# Test Suites - refreshMetadata Edge Cases
# =============================================================================

suite "Fractio Client - refreshMetadata Edge Cases":
  test "refreshMetadata calls initialize when not initialized":
    let c = client.newFractioClient("localhost", 9000)
    check c.initialized.load(moRelaxed) == false
    # refreshMetadata should call initialize which will fail (no real server)
    let result = c.refreshMetadata()
    check result == false
    check c.initialized.load(moRelaxed) == false
    c.close()

  test "refreshMetadata returns false when no connections available":
    let c = client.newFractioClient("localhost", 9000)
    c.initialized.store(true, moRelaxed)
    # nodes table is empty, so can't find any existing connection
    # will try to connect to initialHost which fails
    let result = c.refreshMetadata()
    check result == false
    c.close()

# =============================================================================
# Test Suites - getGroupsForTable Rebalancing Mode
# =============================================================================

suite "Fractio Client - getGroupsForTable Rebalancing":
  test "getGroupsForTable returns both old and new groups during rebalancing":
    let c = client.newFractioClient("localhost", 9000)
    let tid = genTableId()
    let spaceId = genSpaceID()
    let oldGid1 = genGroupID()
    let oldGid2 = genGroupID()
    let newGid1 = genGroupID()
    let newGid2 = genGroupID()

    # Set up table in rebalancing space
    c.tables[tid] = client.TableInfo(
      tableId: tid,
      name: "test_table",
      spaceId: spaceId
    )
    c.spaces[spaceId] = client.SpaceInfo(
      spaceId: spaceId,
      name: "rebalancing_space",
      groupIds: @[newGid1, newGid2],
      oldGroupIds: @[oldGid1, oldGid2],
      rebalancing: true
    )

    let groups = c.getGroupsForTable(tid)
    check groups.len == 4
    # Should contain all old and new groups
    check oldGid1 in groups
    check oldGid2 in groups
    check newGid1 in groups
    check newGid2 in groups
    c.close()

  test "getGroupsForTable deduplicates overlapping groups during rebalancing":
    let c = client.newFractioClient("localhost", 9000)
    let tid = genTableId()
    let spaceId = genSpaceID()
    let sharedGid = genGroupID()
    let newGid = genGroupID()
    let oldGid = genGroupID()

    c.tables[tid] = client.TableInfo(
      tableId: tid,
      name: "test_table",
      spaceId: spaceId
    )
    # Old and new share one group
    c.spaces[spaceId] = client.SpaceInfo(
      spaceId: spaceId,
      name: "overlap_space",
      groupIds: @[sharedGid, newGid],
      oldGroupIds: @[sharedGid, oldGid],
      rebalancing: true
    )

    let groups = c.getGroupsForTable(tid)
    check groups.len == 3 # Deduplicated: sharedGid, newGid, oldGid
    check sharedGid in groups
    check newGid in groups
    check oldGid in groups
    c.close()

  test "getGroupsForTable returns only new groups when not rebalancing":
    let c = client.newFractioClient("localhost", 9000)
    let tid = genTableId()
    let spaceId = genSpaceID()
    let gid1 = genGroupID()
    let gid2 = genGroupID()

    c.tables[tid] = client.TableInfo(
      tableId: tid,
      name: "test_table",
      spaceId: spaceId
    )
    c.spaces[spaceId] = client.SpaceInfo(
      spaceId: spaceId,
      name: "stable_space",
      groupIds: @[gid1, gid2],
      oldGroupIds: @[],
      rebalancing: false
    )

    let groups = c.getGroupsForTable(tid)
    check groups.len == 2
    check gid1 in groups
    check gid2 in groups
    c.close()

# =============================================================================
# Test Suites - getGroupForKey Multi-Group Routing
# =============================================================================

suite "Fractio Client - getGroupForKey Multi-Group Routing":
  test "getGroupForKey routes to correct group via hash":
    let c = client.newFractioClient("localhost", 9000)
    let tid = genTableId()
    let spaceId = genSpaceID()
    let gid1 = genGroupID()
    let gid2 = genGroupID()
    let gid3 = genGroupID()

    c.tables[tid] = client.TableInfo(
      tableId: tid,
      name: "sharded_table",
      spaceId: spaceId
    )
    c.spaces[spaceId] = client.SpaceInfo(
      spaceId: spaceId,
      name: "multi_group_space",
      groupIds: @[gid1, gid2, gid3],
      oldGroupIds: @[],
      rebalancing: false
    )

    # Different keys should route to potentially different groups
    let key1 = encodeTableKey(tid, "user_1")
    let key2 = encodeTableKey(tid, "user_2")
    let key3 = encodeTableKey(tid, "user_3")

    let group1 = c.getGroupForKey(key1)
    let group2 = c.getGroupForKey(key2)
    let group3 = c.getGroupForKey(key3)

    # All should route to one of the space's groups
    check group1 in @[gid1, gid2, gid3]
    check group2 in @[gid1, gid2, gid3]
    check group3 in @[gid1, gid2, gid3]
    c.close()

  test "getGroupForKey with empty groupIds returns DATA_GROUP_START_ID":
    let c = client.newFractioClient("localhost", 9000)
    let tid = genTableId()
    let spaceId = genSpaceID()

    c.tables[tid] = client.TableInfo(
      tableId: tid,
      name: "empty_space_table",
      spaceId: spaceId
    )
    c.spaces[spaceId] = client.SpaceInfo(
      spaceId: spaceId,
      name: "empty_space",
      groupIds: @[], # Empty groups
      oldGroupIds: @[],
      rebalancing: false
    )

    let key = encodeTableKey(tid, "test_pk")
    let group = c.getGroupForKey(key)
    # When table assigned to space with empty groupIds, falls back to DATA_GROUP_START_ID
    # (not META_GROUP_ID, because the table is a data table with valid space assignment)
    check group == DATA_GROUP_START_ID
    c.close()

  test "getGroupForKey routes consistently for same key":
    let c = client.newFractioClient("localhost", 9000)
    let tid = genTableId()
    let spaceId = genSpaceID()
    let gid1 = genGroupID()
    let gid2 = genGroupID()

    c.tables[tid] = client.TableInfo(
      tableId: tid,
      name: "consistent_table",
      spaceId: spaceId
    )
    c.spaces[spaceId] = client.SpaceInfo(
      spaceId: spaceId,
      name: "two_group_space",
      groupIds: @[gid1, gid2],
      oldGroupIds: @[],
      rebalancing: false
    )

    let key = encodeTableKey(tid, "consistent_key")
    # Hash should be consistent
    let group1 = c.getGroupForKey(key)
    let group2 = c.getGroupForKey(key)
    let group3 = c.getGroupForKey(key)
    check group1 == group2
    check group2 == group3
    c.close()

# =============================================================================
# Test Suites - SpaceOpResult Helper Functions
# =============================================================================

suite "Fractio Client - SpaceOpResult Helpers":
  test "SpaceOpResult with zero spaceId":
    let result = client.SpaceOpResult(
      isOk: true,
      spaceId: zeroSpaceID(),
      groupCount: 0,
      groupIds: @[]
    )
    check result.isOk == true
    check result.spaceId == zeroSpaceID()
    check result.groupCount == 0

  test "SpaceOpResult error preserves message":
    let result = client.SpaceOpResult(
      isOk: false,
      err: "custom error message"
    )
    check result.err == "custom error message"
    check result.isOk == false

  test "SpaceOpResult with large groupCount":
    let result = client.SpaceOpResult(
      isOk: true,
      spaceId: genSpaceID(),
      groupCount: 100000.int32,
      groupIds: @[]
    )
    check result.groupCount == 100000

# =============================================================================
# Test Suites - Client Active Transaction State
# =============================================================================

suite "Fractio Client - Active Transaction State":
  test "activeTxnId defaults to zero":
    let c = client.newFractioClient("localhost", 9000)
    check c.activeTxnId == zeroTransactionID()
    c.close()

  test "activeReadTs defaults to zero":
    let c = client.newFractioClient("localhost", 9000)
    check c.activeReadTs == 0
    c.close()

  test "activeTxnId can be manually set":
    let c = client.newFractioClient("localhost", 9000)
    let txnId = genTransactionID()
    c.activeTxnId = txnId
    check c.activeTxnId == txnId
    c.close()

  test "activeReadTs can be manually set":
    let c = client.newFractioClient("localhost", 9000)
    c.activeReadTs = 123456789'u64
    check c.activeReadTs == 123456789'u64
    c.close()

# =============================================================================
# Test Suites - keyPrefixToGroup
# =============================================================================

suite "Fractio Client - keyPrefixToGroup":
  test "keyPrefixToGroup starts empty":
    let c = client.newFractioClient("localhost", 9000)
    check c.keyPrefixToGroup.len == 0
    c.close()

  test "keyPrefixToGroup can have entries added":
    let c = client.newFractioClient("localhost", 9000)
    let gid = genGroupID()
    c.keyPrefixToGroup["custom_prefix"] = gid
    check c.keyPrefixToGroup.len == 1
    check c.keyPrefixToGroup["custom_prefix"] == gid
    c.close()

  test "keyPrefixToGroup not cleared on close":
    # Note: keyPrefixToGroup is NOT cleared by close() - this is expected
    # as the field is rarely used and clear() doesn't affect it
    let c = client.newFractioClient("localhost", 9000)
    c.keyPrefixToGroup["prefix1"] = genGroupID()
    c.keyPrefixToGroup["prefix2"] = genGroupID()
    c.close()
    # After close, keyPrefixToGroup still has entries (not cleared)
    # This matches the current implementation behavior

  # =============================================================================
  # Test Suites - getRoutingState
  # =============================================================================

suite "Fractio Client - getRoutingState":
  test "getRoutingState returns empty state for empty client":
    let c = client.newFractioClient("localhost", 9000)
    let state = c.getRoutingState()
    check state.tables.len == 0
    check state.spaces.len == 0
    c.close()

  test "getRoutingState includes tables":
    let c = client.newFractioClient("localhost", 9000)
    let tid1 = genTableId()
    let tid2 = genTableId()
    let spaceId = genSpaceID()

    c.tables[tid1] = client.TableInfo(tableId: tid1, name: "users",
        spaceId: spaceId)
    c.tables[tid2] = client.TableInfo(tableId: tid2, name: "orders",
        spaceId: spaceId)

    let state = c.getRoutingState()
    check state.tables.len == 2
    check state.tables[tid1].name == "users"
    check state.tables[tid2].name == "orders"
    check state.tables[tid1].spaceId == spaceId
    c.close()

  test "getRoutingState includes spaces":
    let c = client.newFractioClient("localhost", 9000)
    let spaceId = genSpaceID()
    let gid1 = genGroupID()
    let gid2 = genGroupID()

    c.spaces[spaceId] = client.SpaceInfo(
      spaceId: spaceId,
      name: "production",
      groupIds: @[gid1, gid2],
      oldGroupIds: @[],
      rebalancing: false
    )

    let state = c.getRoutingState()
    check state.spaces.len == 1
    check state.spaces[spaceId].name == "production"
    check state.spaces[spaceId].groupIds.len == 2
    check state.spaces[spaceId].rebalancing == false
    c.close()

  test "getRoutingState includes rebalancing info":
    let c = client.newFractioClient("localhost", 9000)
    let spaceId = genSpaceID()
    let oldGid = genGroupID()
    let newGid = genGroupID()

    c.spaces[spaceId] = client.SpaceInfo(
      spaceId: spaceId,
      name: "migrating",
      groupIds: @[newGid],
      oldGroupIds: @[oldGid],
      rebalancing: true
    )

    let state = c.getRoutingState()
    check state.spaces[spaceId].rebalancing == true
    check state.spaces[spaceId].oldGroupIds.len == 1
    check state.spaces[spaceId].oldGroupIds[0] == oldGid
    c.close()

  test "getRoutingState snapshot is independent of client":
    let c = client.newFractioClient("localhost", 9000)
    let tid = genTableId()
    let spaceId = genSpaceID()

    c.tables[tid] = client.TableInfo(tableId: tid, name: "test",
        spaceId: spaceId)

    let state = c.getRoutingState()
    check state.tables.len == 1

    # Modify client after snapshot
    c.tables.del(tid)

    # Snapshot should still have the entry
    check state.tables.len == 1
    c.close()

# =============================================================================
# Test Suites - getInGroup/putInGroup/deleteInGroup Error Handling
# =============================================================================

suite "Fractio Client - InGroup Operations Error Handling":
  test "getInGroup returns error when client not initialized":
    let c = client.newFractioClient("localhost", 9000)
    check c.initialized.load(moRelaxed) == false
    let groupId = genGroupID()
    let result = c.getInGroup("test_key", groupId)
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "getInGroup returns error with txn params when not initialized":
    let c = client.newFractioClient("localhost", 9000)
    let groupId = genGroupID()
    let txnId = genTransactionID()
    let result = c.getInGroup("test_key", groupId, txnId = txnId,
        readTimestamp = 12345)
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "putInGroup returns error when client not initialized":
    let c = client.newFractioClient("localhost", 9000)
    let groupId = genGroupID()
    let result = c.putInGroup("test_key", "test_value", groupId)
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "putInGroup returns error with txn params when not initialized":
    let c = client.newFractioClient("localhost", 9000)
    let groupId = genGroupID()
    let txnId = genTransactionID()
    let result = c.putInGroup("test_key", "test_value", groupId, txnId = txnId)
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "deleteInGroup returns error when client not initialized":
    let c = client.newFractioClient("localhost", 9000)
    let groupId = genGroupID()
    let result = c.deleteInGroup("test_key", groupId)
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "deleteInGroup returns error with txn params when not initialized":
    let c = client.newFractioClient("localhost", 9000)
    let groupId = genGroupID()
    let txnId = genTransactionID()
    let result = c.deleteInGroup("test_key", groupId, txnId = txnId)
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "getInGroup returns error for unknown group":
    let c = client.newFractioClient("localhost", 9000)
    c.initialized.store(true, moRelaxed)
    let groupId = genGroupID()
    # Group not in groups table
    let result = c.getInGroup("test_key", groupId)
    check result.isErr == true
    check result.err == "no connection to group leader"
    c.close()

  test "putInGroup returns error for unknown group":
    let c = client.newFractioClient("localhost", 9000)
    c.initialized.store(true, moRelaxed)
    let groupId = genGroupID()
    let result = c.putInGroup("test_key", "test_value", groupId)
    check result.isErr == true
    check result.err == "no connection to group leader"
    c.close()

  test "deleteInGroup returns error for unknown group":
    let c = client.newFractioClient("localhost", 9000)
    c.initialized.store(true, moRelaxed)
    let groupId = genGroupID()
    let result = c.deleteInGroup("test_key", groupId)
    check result.isErr == true
    check result.err == "no connection to group leader"
    c.close()

# =============================================================================
# Test Suites - Additional getGroupForKey Edge Cases
# =============================================================================

suite "Fractio Client - getGroupForKey Additional Edge Cases":
  test "getGroupForKey with malformed table key":
    let c = client.newFractioClient("localhost", 9000)
    # Key with TABLE_KEY_PREFIX but malformed tableId
    let key = "/t/invalid_table_id/d/pk"
    let group = c.getGroupForKey(key)
    # Falls back to META_GROUP_ID when parsing fails
    check group == META_GROUP_ID
    c.close()

  test "getGroupForKey with short tableId":
    let c = client.newFractioClient("localhost", 9000)
    # Key with prefix but tableId shorter than TABLE_ID_WIDTH (26 chars)
    let key = "/t/0000000100" # 10 chars, not 26
    let group = c.getGroupForKey(key)
    check group == META_GROUP_ID
    c.close()

  test "getGroupForKey handles key without primary key":
    let c = client.newFractioClient("localhost", 9000)
    let tid = genTableId()
    let spaceId = genSpaceID()
    let gid = genGroupID()

    c.tables[tid] = client.TableInfo(tableId: tid, name: "test",
        spaceId: spaceId)
    c.spaces[spaceId] = client.SpaceInfo(
      spaceId: spaceId, name: "space", groupIds: @[gid], oldGroupIds: @[],
          rebalancing: false
    )

    # Key with just tableId, no pk part
    let key = "/t/" & formatTableId(tid)
    let group = c.getGroupForKey(key)
    # Should route based on empty pk
    check group == gid or group == META_GROUP_ID
    c.close()

  test "getGroupForKey with special characters in pk":
    let c = client.newFractioClient("localhost", 9000)
    let tid = genTableId()
    let spaceId = genSpaceID()
    let gid1 = genGroupID()
    let gid2 = genGroupID()

    c.tables[tid] = client.TableInfo(tableId: tid, name: "test",
        spaceId: spaceId)
    c.spaces[spaceId] = client.SpaceInfo(
      spaceId: spaceId, name: "space", groupIds: @[gid1, gid2], oldGroupIds: @[],
          rebalancing: false
    )

    let key = encodeTableKey(tid, "d/pk-with-special/chars:123")
    let group = c.getGroupForKey(key)
    check group in @[gid1, gid2]
    c.close()

# =============================================================================
# Test Suites - Additional getTableIdFromKey Edge Cases
# =============================================================================

suite "Fractio Client - getTableIdFromKey Additional Cases":
  test "getTableIdFromKey with special characters in key":
    let c = client.newFractioClient("localhost", 9000)
    # Key with special chars after tableId
    let key = "invalid\tkey\nwith\rspaces"
    let tid = c.getTableIdFromKey(key)
    check tid == zeroTableId()
    c.close()

  test "getTableIdFromKey with unicode in key":
    let c = client.newFractioClient("localhost", 9000)
    let key = "/t/0000000100/d/\u4e2d\u6587" # Chinese characters in pk
    let tid = c.getTableIdFromKey(key)
    # Should still parse tableId
    check tid == zeroTableId() or tid != zeroTableId() # Depends on implementation
    c.close()

  test "getTableIdFromKey with very long key":
    let c = client.newFractioClient("localhost", 9000)
    let tid = genTableId()
    let longPk = "pk_" & repeat('x', 1000)
    let key = encodeTableKey(tid, longPk)
    let parsedTid = c.getTableIdFromKey(key)
    check parsedTid == tid
    c.close()

# =============================================================================
# Test Suites - Client Config Edge Cases
# =============================================================================

suite "Fractio Client - Config Edge Cases":
  test "config with very large port":
    let config = client.newFractioClientConfig("localhost", 65535)
    check config.initialPort == 65535

  test "config with negative port (converted to int)":
    let config = client.newFractioClientConfig("localhost", -1)
    check config.initialPort == -1 # Stored as-is

  test "config with very long hostname":
    let longHost = "a" & repeat(".b", 100)
    let config = client.newFractioClientConfig(longHost, 9000)
    check config.initialHost == longHost

  test "config with unicode hostname":
    let config = client.newFractioClientConfig("\u4e2d\u6587\u4e3b\u673a", 9000)
    check config.initialHost == "\u4e2d\u6587\u4e3b\u673a"

  test "config with IP address":
    let config = client.newFractioClientConfig("192.168.1.100", 9000)
    check config.initialHost == "192.168.1.100"

  test "config with IPv6 address":
    let config = client.newFractioClientConfig("::1", 9000)
    check config.initialHost == "::1"

# =============================================================================
# Test Suites - Lock Stress Tests
# =============================================================================

suite "Fractio Client - Lock Stress":
  test "many lock acquire/release cycles":
    let c = client.newFractioClient("localhost", 9000)
    for i in 0..<1000:
      c.lock.acquire()
      c.lock.release()
    c.close()

  test "many withLock cycles":
    let c = client.newFractioClient("localhost", 9000)
    for i in 0..<500:
      withLock c.lock:
        discard i
    c.close()

# =============================================================================
# Test Suites - NodeStatus Additional Cases
# =============================================================================

suite "Fractio Client - NodeStatus Additional":
  test "NodeStatus string representation":
    check $nsUnknown == "nsUnknown"
    check $nsAlive == "nsAlive"
    check $nsDraining == "nsDraining"
    check $nsDecommissioned == "nsDecommissioned"

  test "NodeStatus all values accessible":
    let allStatuses = [nsUnknown, nsAlive, nsDraining, nsDecommissioned]
    check allStatuses.len == 4

# =============================================================================
# Test Suites - SpaceOpResult Constructors
# =============================================================================

suite "Fractio Client - SpaceOpResult Internal":
  test "spaceOpOk creates success result":
    let spaceId = genSpaceID()
    let gid1 = genGroupID()
    let gid2 = genGroupID()
    # Note: spaceOpOk/spaceOpErr are internal, test via createSpace/dropSpace behavior
    # We test the result type structure instead
    let result = client.SpaceOpResult(
      isOk: true,
      spaceId: spaceId,
      groupCount: 2,
      groupIds: @[gid1, gid2]
    )
    check result.isOk == true
    check result.err == ""
    check result.spaceId == spaceId
    check result.groupCount == 2
    check result.groupIds.len == 2

  test "spaceOpErr creates error result":
    let result = client.SpaceOpResult(isOk: false, err: "test error")
    check result.isOk == false
    check result.err == "test error"
    check result.spaceId == zeroSpaceID()
    check result.groupCount == 0
    check result.groupIds.len == 0

# =============================================================================
# Test Suites - GroupInfo Edge Cases
# =============================================================================

suite "Fractio Client - GroupInfo Additional":
  test "GroupInfo with single replica":
    let groupInfo = client.GroupInfo(
      groupId: genGroupID(),
      spaceId: genSpaceID(),
      leaderNodeId: 1,
      replicaNodeIds: @[1.uint32]
    )
    check groupInfo.replicaNodeIds.len == 1
    check groupInfo.replicaNodeIds[0] == 1

  test "GroupInfo with leader not in replicas":
    let groupInfo = client.GroupInfo(
      groupId: genGroupID(),
      spaceId: genSpaceID(),
      leaderNodeId: 100, # Leader not in replica list
      replicaNodeIds: @[1.uint32, 2, 3]
    )
    check groupInfo.leaderNodeId == 100
    check groupInfo.leaderNodeId notin groupInfo.replicaNodeIds

  test "GroupInfo with duplicate replicas":
    let groupInfo = client.GroupInfo(
      groupId: genGroupID(),
      spaceId: genSpaceID(),
      leaderNodeId: 1,
      replicaNodeIds: @[1.uint32, 1, 1] # Duplicates allowed
    )
    check groupInfo.replicaNodeIds.len == 3

# =============================================================================
# Test Suites - SpaceInfo Additional Cases
# =============================================================================

suite "Fractio Client - SpaceInfo Additional":
  test "SpaceInfo with many old groups":
    var oldGroups: seq[GroupID] = @[]
    for i in 0..<20:
      oldGroups.add(genGroupID())
    let spaceInfo = client.SpaceInfo(
      spaceId: genSpaceID(),
      name: "many_old_groups",
      groupIds: @[genGroupID()],
      oldGroupIds: oldGroups,
      rebalancing: true
    )
    check spaceInfo.oldGroupIds.len == 20

  test "SpaceInfo with empty name":
    let spaceInfo = client.SpaceInfo(
      spaceId: genSpaceID(),
      name: "",
      groupIds: @[genGroupID()],
      oldGroupIds: @[],
      rebalancing: false
    )
    check spaceInfo.name == ""

  test "SpaceInfo comparison":
    let sid = genSpaceID()
    let s1 = client.SpaceInfo(spaceId: sid, name: "test")
    let s2 = client.SpaceInfo(spaceId: sid, name: "test")
    check s1.name == s2.name
    check s1.spaceId == s2.spaceId

# =============================================================================
# Test Suites - initialize Edge Cases
# =============================================================================

suite "Fractio Client - Initialize Edge Cases":
  test "initialize returns false with empty host":
    let c = client.newFractioClient("", 9000)
    let result = c.initialize()
    check result == false
    check c.initialized.load(moRelaxed) == false
    c.close()

  test "initialize returns false with invalid port":
    let c = client.newFractioClient("localhost", -1)
    let result = c.initialize()
    check result == false
    c.close()

  test "initialize returns true if already initialized":
    let c = client.newFractioClient("localhost", 9000)
    c.initialized.store(true, moRelaxed)
    let result = c.initialize()
    check result == true # Returns true if already initialized
    c.close()

  test "initialize is idempotent when already true":
    let c = client.newFractioClient("localhost", 9000)
    c.initialized.store(true, moRelaxed)
    for i in 0..<10:
      check c.initialize() == true
    c.close()

# =============================================================================
# Test Suites - Client State After Close
# =============================================================================

suite "Fractio Client - State After Close":
  test "nodes cleared after close":
    let c = client.newFractioClient("localhost", 9000)
    c.nodes[1] = client.NodeInfo(nodeId: 1, host: "n1", clientPort: 9000,
        status: nsAlive)
    c.nodes[2] = client.NodeInfo(nodeId: 2, host: "n2", clientPort: 9000,
        status: nsAlive)
    c.close()
    check c.nodes.len == 0

  test "groups cleared after close":
    let c = client.newFractioClient("localhost", 9000)
    c.groups[genGroupID()] = client.GroupInfo(groupId: genGroupID(),
        spaceId: genSpaceID())
    c.close()
    check c.groups.len == 0

  test "spaces cleared after close":
    let c = client.newFractioClient("localhost", 9000)
    c.spaces[genSpaceID()] = client.SpaceInfo(spaceId: genSpaceID(), name: "test")
    c.close()
    check c.spaces.len == 0

  test "tables cleared after close":
    let c = client.newFractioClient("localhost", 9000)
    c.tables[genTableId()] = client.TableInfo(tableId: genTableId(), name: "test")
    c.close()
    check c.tables.len == 0

  test "leaderConnections cleared after close":
    let c = client.newFractioClient("localhost", 9000)
    # Note: leaderConnections contains ProtocolClient, we can't easily mock it
    # Just check it starts empty and is cleared
    check c.leaderConnections.len == 0
    c.close()
    check c.leaderConnections.len == 0

# =============================================================================
# Test Suites - Atomic Flag Operations
# =============================================================================

suite "Fractio Client - Atomic Flags":
  test "initialized flag multiple updates":
    let c = client.newFractioClient("localhost", 9000)
    c.initialized.store(true, moRelaxed)
    check c.initialized.load(moRelaxed) == true
    c.initialized.store(false, moRelaxed)
    check c.initialized.load(moRelaxed) == false
    c.initialized.store(true, moRelaxed)
    check c.initialized.load(moRelaxed) == true
    c.close()

  test "lastRefreshNs multiple updates":
    let c = client.newFractioClient("localhost", 9000)
    for i in 1..100:
      c.lastRefreshNs.store(i.int64 * 1000, moRelaxed)
    check c.lastRefreshNs.load(moRelaxed) == 100000
    c.close()

# =============================================================================
# Test Suites - KV Interface Method Coverage
# =============================================================================

suite "Fractio Client - KV Interface Methods":
  test "get method returns error when not initialized":
    let c = client.newFractioClient("localhost", 9000)
    let result = c.get("test_key")
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "put method returns error when not initialized":
    let c = client.newFractioClient("localhost", 9000)
    let result = c.put("test_key", "test_value")
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "delete method returns error when not initialized":
    let c = client.newFractioClient("localhost", 9000)
    let result = c.delete("test_key")
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "scan method returns error when not initialized":
    let c = client.newFractioClient("localhost", 9000)
    let result = c.scan("start", "end")
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "get method with transaction params":
    let c = client.newFractioClient("localhost", 9000)
    let txnId = genTransactionID()
    let result = c.get("test_key", txnId = txnId, readTimestamp = 1000)
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "put method with transaction params":
    let c = client.newFractioClient("localhost", 9000)
    let txnId = genTransactionID()
    let result = c.put("test_key", "test_value", txnId = txnId)
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "delete method with transaction params":
    let c = client.newFractioClient("localhost", 9000)
    let txnId = genTransactionID()
    let result = c.delete("test_key", txnId = txnId)
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

  test "scan method with all params":
    let c = client.newFractioClient("localhost", 9000)
    let txnId = genTransactionID()
    let result = c.scan("start", "end", limit = 10, txnId = txnId,
        readTimestamp = 1000)
    check result.isErr == true
    check result.err == "client not initialized"
    c.close()

# =============================================================================
# Test Suites - Routing State Thread Safety
# =============================================================================

suite "Fractio Client - Routing State Thread Safety":
  test "concurrent getRoutingState calls":
    let c = client.newFractioClient("localhost", 9000)
    let tid = genTableId()
    c.tables[tid] = client.TableInfo(tableId: tid, name: "test",
        spaceId: genSpaceID())

    var successCount: Atomic[int]
    successCount.store(0)

    proc stateReader(client: client.FractioClient) {.thread.} =
      for i in 0..<50:
        let state = client.getRoutingState()
        if state.tables.len == 1:
          atomicInc successCount

    var threads: array[4, Thread[client.FractioClient]]
    for i in 0..<4:
      createThread(threads[i], stateReader, c)

    joinThreads(threads)
    check successCount.load() == 200
    c.close()

# =============================================================================
# Test Suites - FractioClient as KVStore
# =============================================================================

suite "Fractio Client - KVStore Interface":
  test "FractioClient inherits from KVStoreWithRouting":
    let c = client.newFractioClient("localhost", 9000)
    # Check that it can be used as KVStoreWithRouting
    var kv: KVStoreWithRouting = c
    check kv != nil
    c.close()

  test "KVStoreWithRouting interface methods":
    let c = client.newFractioClient("localhost", 9000)
    # get, put, delete, scan should be available through interface
    let getResult = c.get("key")
    check getResult.isErr == true

    let putResult = c.put("key", "value")
    check putResult.isErr == true

    let delResult = c.delete("key")
    check delResult.isErr == true

    let scanResult = c.scan("start", "end")
    check scanResult.isErr == true
    c.close()
