# Unit tests for system_tables module
#
# Tests:
# - Table key encoding/decoding
# - Lexicographic ordering of encoded keys
# - Key classification (isSystemKey, isMetaGroupKey, isUserTableKey)
# - User table data row and index key encoding
# - findRangeId routing with meta range awareness

import std/[unittest, algorithm]

import fractio/distributed/meta/system_tables
import fractio/distributed/raft/group_types

suite "Table Key Encoding":
  test "encodeTableKey basic":
    let key = encodeTableKey(1, "mydb")
    check key == "/t/0000000001/mydb"

  test "encodeTableKey user table":
    let key = encodeTableKey(100, "users/alice")
    check key == "/t/0000000100/users/alice"

  test "encodeTableKey large tableId":
    let key = encodeTableKey(4294967295'u32, "x")
    check key == "/t/4294967295/x"

  test "encodeTableKey empty primary key":
    let key = encodeTableKey(1, "")
    check key == "/t/0000000001/"

  test "formatTableId zero-padding":
    check formatTableId(1) == "0000000001"
    check formatTableId(10) == "0000000010"
    check formatTableId(100) == "0000000100"
    check formatTableId(999999999) == "0999999999"

suite "Table Key Decoding":
  test "decode roundtrip":
    let original = encodeTableKey(42, "hello/world")
    let (tableId, primaryKey) = decodeTableKey(original)
    check tableId == 42
    check primaryKey == "hello/world"

  test "decode system table key":
    let key = "/t/0000000005/node1"
    let (tableId, primaryKey) = decodeTableKey(key)
    check tableId == SYS_NODES_TABLE_ID
    check primaryKey == "node1"

  test "decode user table key":
    let key = "/t/0000000100/d/row1"
    let (tableId, primaryKey) = decodeTableKey(key)
    check tableId == FIRST_USER_TABLE_ID
    check primaryKey == "d/row1"

  test "decode rejects non-table key":
    expect ValueError:
      discard decodeTableKey("/sys/meta1/foo")

  test "decode rejects too-short key":
    expect ValueError:
      discard decodeTableKey("/t/123")

suite "Lexicographic Ordering":
  test "system tables sort before user tables":
    let sysKey = encodeTableKey(SYS_DATABASES_TABLE_ID, "default")
    let userKey = encodeTableKey(FIRST_USER_TABLE_ID, "data")
    check sysKey < userKey

  test "system tables sort in tableId order":
    let keys = [
      encodeTableKey(SYS_NODES_TABLE_ID, "a"),
      encodeTableKey(SYS_DATABASES_TABLE_ID, "a"),
      encodeTableKey(SYS_TABLES_TABLE_ID, "a"),
      encodeTableKey(SYS_SCHEMAS_TABLE_ID, "a"),
    ]
    var sorted = @keys
    sorted.sort()
    check sorted[0] == keys[1]  # databases (1)
    check sorted[1] == keys[3]  # schemas (2)
    check sorted[2] == keys[2]  # tables (3)
    check sorted[3] == keys[0]  # nodes (5)

  test "same table sorts by primary key":
    let k1 = encodeTableKey(1, "alpha")
    let k2 = encodeTableKey(1, "beta")
    let k3 = encodeTableKey(1, "gamma")
    check k1 < k2
    check k2 < k3

  test "meta range end key sorts correctly":
    # All meta range table keys (1-7) must sort before META_GROUP_END_KEY
    for id in 1'u32 .. MAX_META_GROUP_TABLE_ID:
      let key = encodeTableKey(id, "\xFF\xFF\xFF\xFF") # worst case primary key
      check key < META_GROUP_END_KEY

    # Table 8 onwards should sort >= META_GROUP_END_KEY
    let key8 = encodeTableKey(MAX_META_GROUP_TABLE_ID + 1, "")
    check key8 >= META_GROUP_END_KEY

suite "Key Classification":
  test "isTableKey":
    check isTableKey("/t/0000000001/foo")
    check isTableKey("/t/0000000100/bar")
    check not isTableKey("/sys/meta1/x")
    check not isTableKey("/range/1/data/x")
    check not isTableKey("plain_key")

  test "isSystemKey":
    check isSystemKey(encodeTableKey(1, "default"))
    check isSystemKey(encodeTableKey(99, "last_system"))
    check not isSystemKey(encodeTableKey(100, "user_data"))
    check not isSystemKey("/sys/meta1/foo")
    check not isSystemKey("plain_key")

  test "isMetaGroupKey for table keys":
    check isMetaGroupKey(encodeTableKey(1, "db"))
    check isMetaGroupKey(encodeTableKey(6, "setting"))
    check isMetaGroupKey(encodeTableKey(7, "space"))  # SYS_SPACES_TABLE_ID
    check not isMetaGroupKey(encodeTableKey(8, "x"))
    check not isMetaGroupKey(encodeTableKey(10, "metric"))
    check not isMetaGroupKey(encodeTableKey(100, "user"))

  test "isMetaGroupKey for meta keys":
    check isMetaGroupKey("/sys/meta1/something")
    check isMetaGroupKey("/sys/meta2/something")
    check not isMetaGroupKey("/sys/liveness/node1")

  test "isUserTableKey":
    check isUserTableKey(encodeTableKey(100, "row1"))
    check isUserTableKey(encodeTableKey(500, "row2"))
    check not isUserTableKey(encodeTableKey(1, "system"))
    check not isUserTableKey(encodeTableKey(99, "system"))
    check not isUserTableKey("/sys/meta1/x")

  test "tableIdFromKey":
    check tableIdFromKey(encodeTableKey(42, "test")) == 42'u32
    check tableIdFromKey(encodeTableKey(1, "db")) == 1'u32

suite "User Table Key Helpers":
  test "encodeDataRowKey":
    let key = encodeDataRowKey(100, "alice")
    check key == "/t/0000000100/d/alice"
    let (tableId, pk) = decodeTableKey(key)
    check tableId == 100
    check pk == "d/alice"

  test "encodeIndexKey":
    let key = encodeIndexKey(100, 1, "alice@example.com", "alice")
    check key == "/t/0000000100/i/0000000001/alice@example.com/alice"

  test "data rows sort before index entries":
    let dataKey = encodeDataRowKey(100, "alice")
    let indexKey = encodeIndexKey(100, 1, "alice@example.com", "alice")
    check dataKey < indexKey  # "d/" < "i/"

suite "GroupDescriptor.isMetaGroup":
  test "Range 1 is meta range":
    let desc = newGroupDescriptor(GroupID(1))
    check desc.isMetaGroup

  test "Range 2 is not meta range":
    let desc = newGroupDescriptor(GroupID(2))
    check not desc.isMetaGroup

  test "Range 100 is not meta range":
    let desc = newGroupDescriptor(GroupID(100))
    check not desc.isMetaGroup

suite "Constants":
  test "META_GROUP_ID is 1":
    check META_GROUP_ID == GroupID(1)

  test "DATA_GROUP_START_ID is 2":
    check DATA_GROUP_START_ID == GroupID(2)

  test "system table IDs":
    check SYS_DATABASES_TABLE_ID == 1'u32
    check SYS_SCHEMAS_TABLE_ID == 2'u32
    check SYS_TABLES_TABLE_ID == 3'u32
    check SYS_GROUPS_TABLE_ID == 4'u32
    check SYS_NODES_TABLE_ID == 5'u32
    check SYS_SETTINGS_TABLE_ID == 6'u32
    check SYS_NODE_METRICS_ID == 10'u32
    check SYS_GROUP_METRICS_ID == 11'u32
    check SYS_EVENTS_TABLE_ID == 12'u32
    check FIRST_USER_TABLE_ID == 100'u32
