import std/[unittest, tables as stdtables]
import fractio/core/types
import fractio/client/routing
import fractio/distributed/raft/group_types
import fractio/distributed/meta/system_tables

suite "RoutingState - Initialization":
  test "create empty routing state":
    let state = initRoutingState()
    check stdtables.len(state.tables) == 0
    check stdtables.len(state.spaces) == 0

  test "add table to routing state":
    var state = initRoutingState()
    let tableId = genTableIdLocal()
    let spaceId = SpaceID(genULIDLocal())
    state.addTable(tableId, "my_table", spaceId)

    check stdtables.len(state.tables) == 1
    check state.tables[tableId].name == "my_table"
    check state.tables[tableId].spaceId == spaceId

  test "add space to routing state":
    var state = initRoutingState()
    let spaceId = SpaceID(genULIDLocal())
    let group1 = GroupID(genULIDLocal())
    let group2 = GroupID(genULIDLocal())

    state.addSpace(spaceId, "my_space", @[group1, group2])

    check stdtables.len(state.spaces) == 1
    check state.spaces[spaceId].name == "my_space"
    check state.spaces[spaceId].groupIds.len == 2
    check state.spaces[spaceId].rebalancing == false

  test "add space with rebalancing":
    var state = initRoutingState()
    let spaceId = SpaceID(genULIDLocal())
    let group1 = GroupID(genULIDLocal())
    let group2 = GroupID(genULIDLocal())
    let oldGroup = GroupID(genULIDLocal())

    state.addSpace(spaceId, "rebalancing_space", @[group1, group2],
                   oldGroupIds = @[oldGroup], rebalancing = true)

    check state.spaces[spaceId].rebalancing == true
    check state.spaces[spaceId].oldGroupIds.len == 1

suite "isValidSpaceId":
  test "zero spaceId is invalid":
    check not isValidSpaceId(SpaceID(ZeroULID()))

  test "non-zero spaceId is valid":
    let spaceId = SpaceID(genULIDLocal())
    check isValidSpaceId(spaceId)

suite "routeToGroup - Hash Routing":
  test "empty groupIds returns META_GROUP_ID":
    let result = routeToGroup("pk1", @[])
    check result == META_GROUP_ID

  test "single groupId returns that group":
    let group1 = GroupID(genULIDLocal())
    let result = routeToGroup("pk1", @[group1])
    check result == group1

  test "multiple groups routes deterministically":
    let group1 = GroupID(genULIDLocal())
    let group2 = GroupID(genULIDLocal())
    let group3 = GroupID(genULIDLocal())

    # Same key should route to same group every time
    let result1 = routeToGroup("pk1", @[group1, group2, group3])
    let result2 = routeToGroup("pk1", @[group1, group2, group3])
    check result1 == result2

    # Different keys may route to different groups
    discard routeToGroup("pkA", @[group1, group2, group3])
    discard routeToGroup("pkB", @[group1, group2, group3])
    # Note: This may or may not be different depending on hash

suite "getGroupForKey - System Tables":
  test "meta group table IDs route to META_GROUP_ID":
    var state = initRoutingState()

    # System tables (1-7) are in meta group
    for tableId in [SYS_DATABASES_TABLE_ID, SYS_SCHEMAS_TABLE_ID,
                    SYS_TABLES_TABLE_ID, SYS_SPACES_TABLE_ID,
                    SYS_GROUPS_TABLE_ID, SYS_NODES_TABLE_ID]:
      let key = encodeTableKey(tableId, "test")
      let result = getGroupForKey(state, key)
      check result == META_GROUP_ID

suite "getGroupForKey - Data Tables":
  test "table without space assignment falls back to DATA_GROUP_START_ID":
    var state = initRoutingState()
    # Add a table with zero spaceId
    let tableId = genTableIdLocal()
    state.addTable(tableId, "isolated_table", SpaceID(ZeroULID()))

    let key = encodeTableKey(tableId, "d/1")
    let result = getGroupForKey(state, key)
    check result == DATA_GROUP_START_ID

  test "table with single-group space routes to that group":
    var state = initRoutingState()
    let tableId = genTableIdLocal()
    let spaceId = SpaceID(genULIDLocal())
    let groupId = GroupID(genULIDLocal())

    state.addTable(tableId, "single_group_table", spaceId)
    state.addSpace(spaceId, "single_group_space", @[groupId])

    let key = encodeTableKey(tableId, "d/test_pk")
    let result = getGroupForKey(state, key)
    check result == groupId

  test "table with multi-group space uses hash routing":
    var state = initRoutingState()
    let tableId = genTableIdLocal()
    let spaceId = SpaceID(genULIDLocal())
    let group1 = GroupID(genULIDLocal())
    let group2 = GroupID(genULIDLocal())

    state.addTable(tableId, "multi_group_table", spaceId)
    state.addSpace(spaceId, "multi_group_space", @[group1, group2])

    # Different PKs should route to potentially different groups
    let key1 = encodeTableKey(tableId, "d/pk1")
    let key2 = encodeTableKey(tableId, "d/pk2")

    let result1 = getGroupForKey(state, key1)
    let result2 = getGroupForKey(state, key2)

    # Both should route to one of the two groups
    check result1 in @[group1, group2]
    check result2 in @[group1, group2]

  test "key with d/ prefix correctly extracts PK":
    var state = initRoutingState()
    let tableId = genTableIdLocal()
    let spaceId = SpaceID(genULIDLocal())
    let groupId = GroupID(genULIDLocal())

    state.addTable(tableId, "test_table", spaceId)
    state.addSpace(spaceId, "test_space", @[groupId])

    let key = encodeTableKey(tableId, "d/my_pk")
    let result = getGroupForKey(state, key)
    check result == groupId

suite "getGroupsForTable":
  test "unknown table falls back to DATA_GROUP_START_ID":
    let state = initRoutingState()
    let unknownTableId = genTableIdLocal()
    let groups = getGroupsForTable(state, unknownTableId)
    check groups == @[DATA_GROUP_START_ID]

  test "meta group tables return META_GROUP_ID":
    let state = initRoutingState()
    for tableId in [SYS_DATABASES_TABLE_ID, SYS_SCHEMAS_TABLE_ID,
                    SYS_TABLES_TABLE_ID]:
      let groups = getGroupsForTable(state, tableId)
      check groups == @[META_GROUP_ID]

  test "table with space returns space groups":
    var state = initRoutingState()
    let tableId = genTableIdLocal()
    let spaceId = SpaceID(genULIDLocal())
    let group1 = GroupID(genULIDLocal())
    let group2 = GroupID(genULIDLocal())

    state.addTable(tableId, "test_table", spaceId)
    state.addSpace(spaceId, "test_space", @[group1, group2])

    let groups = getGroupsForTable(state, tableId)
    check groups.len == 2
    check group1 in groups
    check group2 in groups

  test "rebalancing space returns old groups for scans (dual-read for point gets)":
    var state = initRoutingState()
    let tableId = genTableIdLocal()
    let spaceId = SpaceID(genULIDLocal())
    let newGroup1 = GroupID(genULIDLocal())
    let newGroup2 = GroupID(genULIDLocal())
    let oldGroup = GroupID(genULIDLocal())

    state.addTable(tableId, "rebalancing_table", spaceId)
    state.addSpace(spaceId, "rebalancing_space", @[newGroup1, newGroup2],
                   oldGroupIds = @[oldGroup], rebalancing = true)

    let groups = getGroupsForTable(state, tableId)
    # During rebalancing, getGroupsForTable returns only old groups for full scans.
    # Old groups contain all existing data; new groups are empty until migration.
    # Point gets use dual-read mode via keyRoutesToGroupIdDuringRebalance.
    check groups.len == 1
    check oldGroup in groups

suite "getTableIdFromKey":
  test "extract tableId from valid key":
    let tableId = genTableIdLocal()
    let key = encodeTableKey(tableId, "test")
    let extractedId = getTableIdFromKey(key)
    check extractedId == tableId

  test "invalid key returns zeroTableId":
    let extractedId = getTableIdFromKey("invalid_key")
    check isZero(extractedId)

  test "non-table key returns zeroTableId":
    let extractedId = getTableIdFromKey("/other/100")
    check isZero(extractedId)

suite "parseTableKey":
  test "parse valid data row key":
    let tableId = genTableIdLocal()
    # encodeTableKey adds "/" separator, so pass "d/my_pk" not "/d/my_pk"
    let key = encodeTableKey(tableId, "d/my_pk")
    let (extractedId, pk) = parseTableKey(key)
    check extractedId == tableId
    check pk == "my_pk"

  test "parse key without d/ prefix":
    let tableId = genTableIdLocal()
    let key = encodeTableKey(tableId, "my_pk")
    let (extractedId, pk) = parseTableKey(key)
    check extractedId == tableId
    check pk == "my_pk"

  test "parse invalid key":
    let (extractedId, pk) = parseTableKey("invalid")
    check isZero(extractedId)
    check pk == ""

suite "Routing Validation Helpers":
  test "isRoutingKey":
    check isRoutingKey("/t/0000000100")
    check not isRoutingKey("/other/key")
    check not isRoutingKey("plain_string")

  test "needsRouting":
    var state = initRoutingState()
    # Meta group keys don't need routing
    check not needsRouting(state, encodeTableKey(SYS_DATABASES_TABLE_ID, "test"))

  test "isMultiGroup":
    var state = initRoutingState()
    let tableId1 = genTableIdLocal()
    let spaceId = SpaceID(genULIDLocal())

    # Single group
    state.addTable(tableId1, "single_table", spaceId)
    state.addSpace(spaceId, "single_space", @[GroupID(genULIDLocal())])
    check not isMultiGroup(state, tableId1)

    # Multi group
    let tableId2 = genTableIdLocal()
    let spaceId2 = SpaceID(genULIDLocal())
    state.addTable(tableId2, "multi_table", spaceId2)
    state.addSpace(spaceId2, "multi_space",
                   @[GroupID(genULIDLocal()), GroupID(genULIDLocal())])
    check isMultiGroup(state, tableId2)

  test "isRebalancing":
    var state = initRoutingState()
    let tableId1 = genTableIdLocal()
    let spaceId = SpaceID(genULIDLocal())

    state.addTable(tableId1, "stable_table", spaceId)
    state.addSpace(spaceId, "stable_space", @[GroupID(genULIDLocal())])
    check not isRebalancing(state, tableId1)

    let tableId2 = genTableIdLocal()
    let spaceId2 = SpaceID(genULIDLocal())
    state.addTable(tableId2, "rebalancing_table", spaceId2)
    state.addSpace(spaceId2, "rebalancing_space", @[GroupID(genULIDLocal())],
                   rebalancing = true)
    check isRebalancing(state, tableId2)
