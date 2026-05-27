# Unit tests for system_tables module
#
# Tests:
# - Table key encoding/decoding with ULID-based TableId
# - Key classification (isSystemKey, isMetaGroupKey, isUserTableKey)
# - User table data row and index key encoding
# - Well-known system table ULIDs
# - SYSTEM_TABLES_REGISTRY completeness and consistency
# - Lookup procs (getSystemTableInfoByName, getSystemTableInfoById)

import std/[unittest, algorithm, strutils, options]

import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas
import fractio/distributed/raft/group_types
import fractio/core/types

suite "Table Key Encoding":
  test "encodeTableKey basic":
    let tableId = SYS_DATABASES_TABLE_ID
    let key = encodeTableKey(tableId, "mydb")
    check key.startsWith("/t/")
    check key.endsWith("/mydb")
    # System table IDs are well-known ULIDs with zero bytes except last one
    let (decodedId, pk) = decodeTableKey(key)
    check decodedId == tableId
    check pk == "mydb"

  test "encodeTableKey user table":
    let tableId = genTableIdLocal()
    let key = encodeTableKey(tableId, "users/alice")
    check key.startsWith("/t/")
    check "/users/alice" in key
    let (decodedId, pk) = decodeTableKey(key)
    check decodedId == tableId
    check pk == "users/alice"

  test "encodeTableKey empty primary key":
    let tableId = SYS_DATABASES_TABLE_ID
    let key = encodeTableKey(tableId, "")
    check key.startsWith("/t/")
    check key.endsWith("/")

  test "formatTableId ULID length":
    # ULIDs are always 26 characters
    let tid = genTableIdLocal()
    let formatted = formatTableId(tid)
    check formatted.len == 26

suite "Table Key Decoding":
  test "decode roundtrip":
    let tableId = genTableIdLocal()
    let original = encodeTableKey(tableId, "hello/world")
    let (decodedId, primaryKey) = decodeTableKey(original)
    check decodedId == tableId
    check primaryKey == "hello/world"

  test "decode system table key":
    let key = encodeTableKey(SYS_NODES_TABLE_ID, "node1")
    let (tableId, primaryKey) = decodeTableKey(key)
    check tableId == SYS_NODES_TABLE_ID
    check primaryKey == "node1"

  test "decode rejects non-table key":
    expect ValueError:
      discard decodeTableKey("/sys/meta1/foo")

  test "decode rejects too-short key":
    expect ValueError:
      discard decodeTableKey("/t/123")

suite "Lexicographic Ordering":
  test "system tables sort before user tables":
    # System table ULIDs have zero timestamp + zero randomness
    # User table ULIDs have non-zero timestamp (current time)
    let sysKey = encodeTableKey(SYS_DATABASES_TABLE_ID, "default")
    let userKey = encodeTableKey(genTableIdLocal(), "data")
    check sysKey < userKey

  test "system tables sort in table number order":
    # System table numbers are in the last byte of the ULID
    let keys = [
      encodeTableKey(SYS_NODES_TABLE_ID, "a"),
      encodeTableKey(SYS_DATABASES_TABLE_ID, "a"),
      encodeTableKey(SYS_TABLES_TABLE_ID, "a"),
      encodeTableKey(SYS_SCHEMAS_TABLE_ID, "a"),
    ]
    var sorted = @keys
    sorted.sort()
    # All system table ULIDs have same prefix (zeros), differ only in last byte
    # They should sort by table number: 1 (databases), 2 (schemas), 3 (tables), 5 (nodes)
    check sorted[0] == keys[1] # databases (1)
    check sorted[1] == keys[3] # schemas (2)
    check sorted[2] == keys[2] # tables (3)
    check sorted[3] == keys[0] # nodes (5)

  test "same table sorts by primary key":
    let tableId = SYS_DATABASES_TABLE_ID
    let k1 = encodeTableKey(tableId, "alpha")
    let k2 = encodeTableKey(tableId, "beta")
    let k3 = encodeTableKey(tableId, "gamma")
    check k1 < k2
    check k2 < k3

  test "meta range end key sorts correctly":
    # META_GROUP_END_KEY is "/t/00000000000000000000000008/"
    # All meta range table keys (1-7) must sort before this
    check encodeTableKey(SYS_DATABASES_TABLE_ID, "\xFF\xFF\xFF\xFF") < META_GROUP_END_KEY
    check encodeTableKey(SYS_SPACES_TABLE_ID, "\xFF\xFF\xFF\xFF") < META_GROUP_END_KEY

suite "Key Classification":
  test "isTableKey":
    check isTableKey(encodeTableKey(SYS_DATABASES_TABLE_ID, "foo"))
    check isTableKey(encodeTableKey(genTableIdLocal(), "bar"))
    check not isTableKey("/sys/meta1/x")
    check not isTableKey("/range/1/data/x")
    check not isTableKey("plain_key")

  test "isSystemKey":
    check isSystemKey(encodeTableKey(SYS_DATABASES_TABLE_ID, "default"))
    check isSystemKey(encodeTableKey(SYS_NODES_TABLE_ID, "node1"))
    check not isSystemKey(encodeTableKey(genTableIdLocal(), "user_data"))
    check not isSystemKey("/sys/meta1/foo")
    check not isSystemKey("plain_key")

  test "isMetaGroupKey for table keys":
    check isMetaGroupKey(encodeTableKey(SYS_DATABASES_TABLE_ID, "db"))
    check isMetaGroupKey(encodeTableKey(SYS_SETTINGS_TABLE_ID, "setting"))
    check isMetaGroupKey(encodeTableKey(SYS_SPACES_TABLE_ID, "space"))
    check not isMetaGroupKey(encodeTableKey(SYS_NODE_METRICS_ID, "x"))
    check not isMetaGroupKey(encodeTableKey(genTableIdLocal(), "user"))

  test "isMetaGroupKey for meta keys":
    # Note: isMetaGroupKey only works for table keys (/t/...)
    # It returns false for non-table keys like /sys/meta1/...
    check not isMetaGroupKey("/sys/meta1/something")
    check not isMetaGroupKey("/sys/meta2/something")
    check not isMetaGroupKey("/sys/liveness/node1")
    # Meta group keys are table keys with system table IDs 1-7
    check isMetaGroupKey(encodeTableKey(SYS_DATABASES_TABLE_ID, "test"))

  test "isUserTableKey":
    check isUserTableKey(encodeTableKey(genTableIdLocal(), "row1"))
    check not isUserTableKey(encodeTableKey(SYS_DATABASES_TABLE_ID, "system"))
    check not isUserTableKey("/sys/meta1/x")

  test "tableIdFromKey":
    let tid = genTableIdLocal()
    check tableIdFromKey(encodeTableKey(tid, "test")) == tid
    check tableIdFromKey(encodeTableKey(SYS_DATABASES_TABLE_ID, "db")) == SYS_DATABASES_TABLE_ID

suite "User Table Key Helpers":
  test "encodeDataRowKey with groupId":
    let tableId = genTableIdLocal()
    let groupId = genGroupIDLocal()
    let key = encodeDataRowKey(tableId, groupId, "alice")
    check key.startsWith("/t/")
    check "/d/" in key
    check $groupId in key
    let (decodedId, decodedGroupId, decodedPk) = decodeDataRowKey(key)
    check decodedId == tableId
    check decodedGroupId == groupId
    check decodedPk == "alice"

  test "encodeDataRowScanBound (scan-bound format without groupId)":
    let tableId = genTableIdLocal()
    let key = encodeDataRowScanBound(tableId, "alice")
    check key.startsWith("/t/")
    check "/d/alice" in key
    let (decodedId, pk) = decodeTableKey(key)
    check decodedId == tableId
    check pk == "d/alice"
    # decodeDataRowKey raises on scan-bound keys (no groupId)
    doAssertRaises(ValueError):
      discard decodeDataRowKey(key)

  test "encodeIndexKey with groupId":
    let tableId = genTableIdLocal()
    let groupId = genGroupIDLocal()
    let indexId = genTableIdLocal()
    let key = encodeIndexKey(tableId, groupId, indexId, "alice@example.com", "alice")
    check key.startsWith("/t/")
    check "/i/" in key
    check $groupId in key
    check "alice@example.com" in key

  test "data rows sort before index entries":
    let tableId = genTableIdLocal()
    let groupId = genGroupIDLocal()
    let dataKey = encodeDataRowKey(tableId, groupId, "alice")
    let indexKey = encodeIndexKey(tableId, groupId, genTableIdLocal(),
        "alice@example.com", "alice")
    check dataKey < indexKey # "d/" < "i/"

suite "GroupDescriptor.isMetaGroup":
  test "Range 1 is meta range":
    let desc = newGroupDescriptor(META_GROUP_ID)
    check desc.isMetaGroup

  test "Range 2 is not meta range":
    let desc = newGroupDescriptor(DATA_GROUP_START_ID)
    check not desc.isMetaGroup

  test "other groups are not meta range":
    let gid = genGroupIDLocal()
    let desc = newGroupDescriptor(gid)
    check not desc.isMetaGroup

suite "Constants":
  test "META_GROUP_ID has well-known ULID":
    # META_GROUP_ID has last byte = 1
    let ulid = groupIDToULID(META_GROUP_ID)
    check ulid.data[15] == 1'u8

  test "DATA_GROUP_START_ID has well-known ULID":
    # DATA_GROUP_START_ID has last byte = 2
    let ulid = groupIDToULID(DATA_GROUP_START_ID)
    check ulid.data[15] == 2'u8

  test "system table IDs are well-known ULIDs":
    # System table ULIDs have zero bytes 0-14, table number in byte 15
    var ulid = ULID(SYS_DATABASES_TABLE_ID)
    check ulid.data[15] == SYS_DATABASES_TABLE_NUM

    ulid = ULID(SYS_NODES_TABLE_ID)
    check ulid.data[15] == SYS_NODES_TABLE_NUM

    ulid = ULID(SYS_SPACES_TABLE_ID)
    check ulid.data[15] == SYS_SPACES_TABLE_NUM

suite "System Table Registry":
  test "registry has 10 entries":
    check SYSTEM_TABLES_REGISTRY.len == 10

  test "all registry entries have well-known ULIDs":
    for info in SYSTEM_TABLES_REGISTRY:
      let ulid = ULID(info.tableId)
      # Bytes 0-14 should be zero
      for i in 0..<15:
        check ulid.data[i] == 0'u8
      # Byte 15 should match tableNum
      check ulid.data[15] == info.tableNum

  test "tableNum matches well-known constant":
    # Verify each registry entry's tableNum matches its well-known constant
    check SYSTEM_TABLES_REGISTRY[0].tableNum == SYS_DATABASES_TABLE_NUM
    check SYSTEM_TABLES_REGISTRY[1].tableNum == SYS_SCHEMAS_TABLE_NUM
    check SYSTEM_TABLES_REGISTRY[2].tableNum == SYS_TABLES_TABLE_NUM
    check SYSTEM_TABLES_REGISTRY[3].tableNum == SYS_GROUPS_TABLE_NUM
    check SYSTEM_TABLES_REGISTRY[4].tableNum == SYS_NODES_TABLE_NUM
    check SYSTEM_TABLES_REGISTRY[5].tableNum == SYS_SETTINGS_TABLE_NUM
    check SYSTEM_TABLES_REGISTRY[6].tableNum == SYS_SPACES_TABLE_NUM
    check SYSTEM_TABLES_REGISTRY[7].tableNum == SYS_NODE_METRICS_NUM
    check SYSTEM_TABLES_REGISTRY[8].tableNum == SYS_GROUP_METRICS_NUM
    check SYSTEM_TABLES_REGISTRY[9].tableNum == SYS_EVENTS_TABLE_NUM

  test "tableId matches well-known ID constant":
    check SYSTEM_TABLES_REGISTRY[0].tableId == SYS_DATABASES_TABLE_ID
    check SYSTEM_TABLES_REGISTRY[1].tableId == SYS_SCHEMAS_TABLE_ID
    check SYSTEM_TABLES_REGISTRY[2].tableId == SYS_TABLES_TABLE_ID
    check SYSTEM_TABLES_REGISTRY[3].tableId == SYS_GROUPS_TABLE_ID
    check SYSTEM_TABLES_REGISTRY[4].tableId == SYS_NODES_TABLE_ID
    check SYSTEM_TABLES_REGISTRY[5].tableId == SYS_SETTINGS_TABLE_ID
    check SYSTEM_TABLES_REGISTRY[6].tableId == SYS_SPACES_TABLE_ID
    check SYSTEM_TABLES_REGISTRY[7].tableId == SYS_NODE_METRICS_ID
    check SYSTEM_TABLES_REGISTRY[8].tableId == SYS_GROUP_METRICS_ID
    check SYSTEM_TABLES_REGISTRY[9].tableId == SYS_EVENTS_TABLE_ID

  test "all entries have sys schema and database":
    for info in SYSTEM_TABLES_REGISTRY:
      check info.schema == "sys"
      check info.database == "sys"

  test "all entries have non-empty name and description":
    for info in SYSTEM_TABLES_REGISTRY:
      check info.name.len > 0
      check info.description.len > 0

  test "all entries have at least one column":
    for info in SYSTEM_TABLES_REGISTRY:
      check info.columns.len > 0

  test "all entries have at least one primary key column":
    for info in SYSTEM_TABLES_REGISTRY:
      check info.primaryKey.len > 0

  test "all entries have non-empty pkSpec":
    for info in SYSTEM_TABLES_REGISTRY:
      check info.pkSpec.columns.len > 0

  test "first column of each entry has _key as primary key":
    for info in SYSTEM_TABLES_REGISTRY:
      check info.primaryKey[0] == "_key"
      # The first column should be _key and marked as primary key
      check info.columns[0].name == "_key"
      check info.columns[0].primaryKey == true

  test "primary key column names match pkSpec columns":
    for info in SYSTEM_TABLES_REGISTRY:
      check info.primaryKey.len == info.pkSpec.columns.len
      for i, pkName in info.primaryKey:
        check info.pkSpec.columns[i].name == pkName

  test "meta group tables (1-7) have tableNum <= MAX_META_GROUP_TABLE_NUM":
    for info in SYSTEM_TABLES_REGISTRY:
      if info.tableNum <= MAX_META_GROUP_TABLE_NUM:
        check isMetaGroupTableId(info.tableId)

  test "metrics tables (10+) are not meta group":
    for info in SYSTEM_TABLES_REGISTRY:
      if info.tableNum > MAX_META_GROUP_TABLE_NUM:
        check not isMetaGroupTableId(info.tableId)

suite "System Table Lookup":
  test "getSystemTableInfoByName finds databases":
    let opt = getSystemTableInfoByName("databases")
    check opt.isSome
    check opt.get.name == "databases"
    check opt.get.tableNum == SYS_DATABASES_TABLE_NUM

  test "getSystemTableInfoByName finds nodes":
    let opt = getSystemTableInfoByName("nodes")
    check opt.isSome
    check opt.get.name == "nodes"
    check opt.get.tableNum == SYS_NODES_TABLE_NUM

  test "getSystemTableInfoByName is case insensitive":
    let opt1 = getSystemTableInfoByName("Databases")
    let opt2 = getSystemTableInfoByName("DATABASES")
    let opt3 = getSystemTableInfoByName("databases")
    check opt1.isSome
    check opt2.isSome
    check opt3.isSome
    check opt1.get.name == "databases"
    check opt2.get.name == "databases"
    check opt3.get.name == "databases"

  test "getSystemTableInfoByName returns none for unknown table":
    let opt = getSystemTableInfoByName("nonexistent_table")
    check opt.isNone

  test "getSystemTableInfoById finds tables":
    let opt = getSystemTableInfoById(SYS_DATABASES_TABLE_ID)
    check opt.isSome
    check opt.get.name == "databases"
    check opt.get.tableNum == SYS_DATABASES_TABLE_NUM

  test "getSystemTableInfoById returns none for unknown ID":
    let unknownId = genTableIdLocal() # random ULID, not a system table
    let opt = getSystemTableInfoById(unknownId)
    check opt.isNone

  test "lookup by name matches lookup by ID for all entries":
    for info in SYSTEM_TABLES_REGISTRY:
      let byName = getSystemTableInfoByName(info.name)
      check byName.isSome
      check byName.get.tableId == info.tableId

      let byId = getSystemTableInfoById(info.tableId)
      check byId.isSome
      check byId.get.name == info.name

suite "SysColDef Types":
  test "SysColDef fields are accessible":
    let col = SysColDef(name: "testCol", dataType: dtInt, maxLen: 0,
        primaryKey: false, notNull: true)
    check col.name == "testCol"
    check col.dataType == dtInt
    check col.maxLen == 0
    check col.primaryKey == false
    check col.notNull == true

  test "SysPrimaryKeySpec columns are accessible":
    let spec = SysPrimaryKeySpec(
      columns: @[(name: "id", dataType: cdtInt, maxLen: 0)]
    )
    check spec.columns.len == 1
    check spec.columns[0].name == "id"
    check spec.columns[0].dataType == cdtInt
    check spec.columns[0].maxLen == 0
