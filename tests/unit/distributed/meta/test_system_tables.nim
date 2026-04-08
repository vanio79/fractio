# Unit tests for system_tables module
#
# Tests:
# - Table key encoding/decoding with ULID-based TableId
# - Key classification (isSystemKey, isMetaGroupKey, isUserTableKey)
# - User table data row and index key encoding
# - Well-known system table ULIDs

import std/[unittest, algorithm, strutils]

import fractio/distributed/meta/system_tables
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
    let tableId = genTableId()
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
    let tid = genTableId()
    let formatted = formatTableId(tid)
    check formatted.len == 26

suite "Table Key Decoding":
  test "decode roundtrip":
    let tableId = genTableId()
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
    let userKey = encodeTableKey(genTableId(), "data")
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
    check isTableKey(encodeTableKey(genTableId(), "bar"))
    check not isTableKey("/sys/meta1/x")
    check not isTableKey("/range/1/data/x")
    check not isTableKey("plain_key")

  test "isSystemKey":
    check isSystemKey(encodeTableKey(SYS_DATABASES_TABLE_ID, "default"))
    check isSystemKey(encodeTableKey(SYS_NODES_TABLE_ID, "node1"))
    check not isSystemKey(encodeTableKey(genTableId(), "user_data"))
    check not isSystemKey("/sys/meta1/foo")
    check not isSystemKey("plain_key")

  test "isMetaGroupKey for table keys":
    check isMetaGroupKey(encodeTableKey(SYS_DATABASES_TABLE_ID, "db"))
    check isMetaGroupKey(encodeTableKey(SYS_SETTINGS_TABLE_ID, "setting"))
    check isMetaGroupKey(encodeTableKey(SYS_SPACES_TABLE_ID, "space"))
    check not isMetaGroupKey(encodeTableKey(SYS_NODE_METRICS_ID, "x"))
    check not isMetaGroupKey(encodeTableKey(genTableId(), "user"))

  test "isMetaGroupKey for meta keys":
    # Note: isMetaGroupKey only works for table keys (/t/...)
    # It returns false for non-table keys like /sys/meta1/...
    check not isMetaGroupKey("/sys/meta1/something")
    check not isMetaGroupKey("/sys/meta2/something")
    check not isMetaGroupKey("/sys/liveness/node1")
    # Meta group keys are table keys with system table IDs 1-7
    check isMetaGroupKey(encodeTableKey(SYS_DATABASES_TABLE_ID, "test"))

  test "isUserTableKey":
    check isUserTableKey(encodeTableKey(genTableId(), "row1"))
    check not isUserTableKey(encodeTableKey(SYS_DATABASES_TABLE_ID, "system"))
    check not isUserTableKey("/sys/meta1/x")

  test "tableIdFromKey":
    let tid = genTableId()
    check tableIdFromKey(encodeTableKey(tid, "test")) == tid
    check tableIdFromKey(encodeTableKey(SYS_DATABASES_TABLE_ID, "db")) == SYS_DATABASES_TABLE_ID

suite "User Table Key Helpers":
  test "encodeDataRowKey":
    let tableId = genTableId()
    let key = encodeDataRowKey(tableId, "alice")
    check key.startsWith("/t/")
    check "/d/alice" in key
    let (decodedId, pk) = decodeTableKey(key)
    check decodedId == tableId
    check pk == "d/alice"

  test "encodeIndexKey":
    let tableId = genTableId()
    let indexId = genTableId()
    let key = encodeIndexKey(tableId, indexId, "alice@example.com", "alice")
    check key.startsWith("/t/")
    check "/i/" in key
    check "alice@example.com" in key

  test "data rows sort before index entries":
    let tableId = genTableId()
    let dataKey = encodeDataRowKey(tableId, "alice")
    let indexKey = encodeIndexKey(tableId, genTableId(), "alice@example.com", "alice")
    check dataKey < indexKey # "d/" < "i/"

suite "GroupDescriptor.isMetaGroup":
  test "Range 1 is meta range":
    let desc = newGroupDescriptor(META_GROUP_ID)
    check desc.isMetaGroup

  test "Range 2 is not meta range":
    let desc = newGroupDescriptor(DATA_GROUP_START_ID)
    check not desc.isMetaGroup

  test "other groups are not meta range":
    let gid = genGroupID()
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
