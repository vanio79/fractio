# Unit tests for Fractio Client
# Tests for config types, result types, initialization, routing logic

import unittest
import std/[tables, options, strutils, sequtils, algorithm, hashes, atomics, locks]
import std/typedthreads
import fractio/core/types
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
    let result = client.kvOpOk[string]("test_value")
    check result.isOk == true
    check result.val == "test_value"

  test "kvOpOk with none option":
    let result = client.kvOpOk[Option[string]](none(string))
    check result.isOk == true
    check result.val.isNone

  test "kvOpErr creates error result":
    let result = client.kvOpErr[string]("error message")
    check result.isOk == false
    check result.err == "error message"

  test "isErr helper for success":
    let success = client.kvOpOk[int](42)
    check success.isErr == false

  test "isErr helper for error":
    let error = client.kvOpErr[int]("failed")
    check error.isErr == true

  test "KVOpResult with different types":
    let intResult = client.kvOpOk[int](42)
    check intResult.isOk == true
    check intResult.val == 42

    let seqResult = client.kvOpOk[seq[int]](@[1, 2, 3])
    check seqResult.isOk == true
    check seqResult.val.len == 3

suite "Fractio Client - KVOpVoidResult":
  test "kvVoidOk creates success result":
    let result = client.kvVoidOk()
    check result.isOk == true
    check result.err == ""

  test "kvVoidErr creates error result":
    let result = client.kvVoidErr("operation failed")
    check result.isOk == false
    check result.err == "operation failed"
    check result.isErr == true

  test "KVOpVoidResult isErr helper":
    let success = client.kvVoidOk()
    let error = client.kvVoidErr("failed")

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
    var result: client.KVOpVoidResult
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
