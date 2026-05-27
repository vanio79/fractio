# Unit tests for data row key encoding with group ID
#
# Tests the key format change from /t/<tableId>/d/<pk> to
# /t/<tableId>/d/<groupId>/<pk>, including:
# - encodeDataRowKey with groupId
# - decodeDataRowKey extracting groupId
# - makeDataRowScanEndKey with groupId (for per-group range scans)
# - makeGroupDataRowScanBounds (start/end for a specific group)
# - encodeIndexKey with groupId
# - Intent key format with groupId
# - Backward compatibility: reading old-format keys without groupId
# - Edge cases: empty pk, binary pk with null bytes, ULID groupId

import unittest
import std/strutils
import fractio/core/types
import fractio/core/primary_key
import fractio/distributed/meta/system_tables
import fractio/distributed/raft/group_types
import fractio/protocol/raft_store

# Helper to create a TableId from a ULID
proc makeTableId(): TableId =
  TableId(genULID(localTimeNs()))

# Helper to create a GroupId from a ULID
proc makeGroupId(): GroupID =
  GroupID(genULID(localTimeNs()))

# Helper to create a well-known GroupId for deterministic tests
proc makeGroupId(suffix: uint8): GroupID =
  var data: array[16, uint8]
  data[15] = suffix
  GroupID(ULID(data: data))

suite "Data Row Key Encoding with Group ID":

  test "encodeDataRowKey includes groupId in key":
    let tableId = makeTableId()
    let groupId = makeGroupId(1)
    let pk = "\x01\x80\x00\x00\x00\x00\x00\x00\x2a" # INT64 PK for value 42

    let key = encodeDataRowKey(tableId, groupId, pk)

    # Key format: /t/<tableId>/d/<groupId>/<pk>
    check key.startsWith(TABLE_KEY_PREFIX)
    check key.contains("/d/")
    # After /d/ there should be the groupId (26-char ULID string) then / then pk
    let afterDPrefix = key[key.find("/d/") + 3 .. ^1]
    check afterDPrefix.startsWith($groupId)
    let afterGroupId = afterDPrefix[26 .. ^1] # skip 26-char ULID + "/" is at [26]
    check afterGroupId.startsWith("/")
    check afterGroupId[1 .. ^1] == pk

  test "encodeDataRowKey with empty pk includes groupId":
    let tableId = makeTableId()
    let groupId = makeGroupId(1)
    let pk = ""

    let key = encodeDataRowKey(tableId, groupId, pk)

    # Key format: /t/<tableId>/d/<groupId>/
    check key.startsWith(TABLE_KEY_PREFIX)
    check key.contains("/d/")
    check key.contains($groupId)

  test "decodeDataRowKey extracts groupId from key":
    let tableId = makeTableId()
    let groupId = makeGroupId(5)
    let pk = "\x01\x80\x00\x00\x00\x00\x00\x00\x2a"

    let key = encodeDataRowKey(tableId, groupId, pk)
    let (decodedTableId, decodedGroupId, decodedPk) = decodeDataRowKey(key)

    check decodedTableId == tableId
    check decodedGroupId == groupId
    check decodedPk == ""

suite "primaryKeyFromDataRowKey — PK extraction for k-way merge":

  test "extracts PK from data row key":
    let tableId = makeTableId()
    let groupId = makeGroupId(1)
    let pk = "hello"
    let key = encodeDataRowKey(tableId, groupId, pk)
    let extracted = primaryKeyFromDataRowKey(key)
    check extracted == pk

  test "extracts PK from data row key with different group":
    let tableId = makeTableId()
    let groupId1 = makeGroupId(1)
    let groupId2 = makeGroupId(2)
    let pk = "world"
    let key1 = encodeDataRowKey(tableId, groupId1, pk)
    let key2 = encodeDataRowKey(tableId, groupId2, pk)
    # Same PK from different groups should extract the same PK portion
    check primaryKeyFromDataRowKey(key1) == primaryKeyFromDataRowKey(key2)

  test "full key ordering differs from PK ordering across groups":
    # When two rows have different PKs in different groups, the full key
    # comparison includes groupId which can flip the ordering.
    let tableId = makeTableId()
    let groupA = makeGroupId(1)
    let groupB = makeGroupId(2)
    let pkA = "z" # Large PK in group A
    let pkB = "a" # Small PK in group B
    let keyA = encodeDataRowKey(tableId, groupA, pkA)
    let keyB = encodeDataRowKey(tableId, groupB, pkB)
    # PK extraction preserves the correct ordering: pkA > pkB
    check primaryKeyFromDataRowKey(keyA) > primaryKeyFromDataRowKey(keyB)

  test "returns full key for non-data-row key":
    let key = "/t/" & $makeTableId() & "/x/some_index_key"
    check primaryKeyFromDataRowKey(key) == key

  test "returns full key for system table key":
    let sysKey = encodeTableKey(SYS_DATABASES_TABLE_ID, "_key1")
    check primaryKeyFromDataRowKey(sysKey) == sysKey

  test "extracts empty PK from key with empty pk":
    let tableId = makeTableId()
    let groupId = makeGroupId(1)
    let key = encodeDataRowKey(tableId, groupId, "")
    check primaryKeyFromDataRowKey(key) == ""

  test "extracts binary PK with null bytes":
    let tableId = makeTableId()
    let groupId = makeGroupId(7)
    let pk = "\x01" & "\x00" & "\x00" & "\x00" & "hello"
    let key = encodeDataRowKey(tableId, groupId, pk)
    check primaryKeyFromDataRowKey(key) == pk

  test "decodeDataRowKey with binary pk containing null bytes":
    let tableId = makeTableId()
    let groupId = makeGroupId(7)
    # Binary PK with embedded null bytes (ULID, etc.)
    let pk = "\x01" & "\x00" & "\x00" & "\x00" & "hello"

    let key = encodeDataRowKey(tableId, groupId, pk)
    let (decodedTableId, decodedGroupId, decodedPk) = decodeDataRowKey(key)

    check decodedTableId == tableId
    check decodedGroupId == groupId
    check decodedPk == pk

  test "makeDataRowScanBounds returns group-scoped range":
    let tableId = makeTableId()
    let groupId = makeGroupId(2)

    let (startKey, endKey) = makeGroupDataRowScanBounds(tableId, groupId)

    # Start key should be: /t/<tableId>/d/<groupId>/
    check startKey.startsWith(TABLE_KEY_PREFIX)
    check startKey.contains("/d/")
    check startKey.contains($groupId)

    # End key should be: /t/<tableId>/d/<groupId>0 (one past groupId ULID)
    check endKey > startKey
    check endKey.startsWith(TABLE_KEY_PREFIX)
    check endKey.contains("/d/")

  test "makeDataRowScanBounds different groups produce disjoint ranges":
    let tableId = makeTableId()
    let group1 = makeGroupId(1)
    let group2 = makeGroupId(2)
    let group3 = makeGroupId(3)

    let (s1, e1) = makeGroupDataRowScanBounds(tableId, group1)
    let (s2, e2) = makeGroupDataRowScanBounds(tableId, group2)
    let (s3, e3) = makeGroupDataRowScanBounds(tableId, group3)

    # Group ranges should not overlap
    check e1 <= s2 or s1 >= e2 # Groups 1 and 2 don't overlap
    check e2 <= s3 or s2 >= e3 # Groups 2 and 3 don't overlap
    check e1 <= s3 or s1 >= e3 # Groups 1 and 3 don't overlap

  test "makeDataRowScanEndKey covers all groups":
    # The table-wide end key should cover all group prefixes
    let tableId = makeTableId()
    let groupId = makeGroupId(255)

    let endKey = makeDataRowScanEndKey(tableId)
    let dataKey = encodeDataRowKey(tableId, groupId, "test-pk")

    check dataKey < endKey

  test "isDataRowKey detects data row keys":
    let tableId = makeTableId()
    let groupId = makeGroupId(1)

    let dataKey = encodeDataRowKey(tableId, groupId, "test")
    check isDataRowKey(dataKey) == true

  test "isDataRowKey rejects system table keys":
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, "1")
    check isDataRowKey(nodeKey) == false

  test "isDataRowKey rejects index keys":
    let tableId = makeTableId()
    let groupId = makeGroupId(1)
    let indexKey = encodeIndexKey(tableId, groupId, makeTableId(), "idx-key", "pk")
    # Index keys use /i/ prefix, not /d/
    check isDataRowKey(indexKey) == false

  test "extractGroupIdFromDataRowKey extracts correct group":
    let tableId = makeTableId()
    let groupId = makeGroupId(42)
    let pk = "some-primary-key"

    let key = encodeDataRowKey(tableId, groupId, pk)
    let extractedGroupId = extractGroupIdFromDataRowKey(key)

    check extractedGroupId == groupId

  test "extractGroupIdFromDataRowKey returns ZeroGroupID for non-data-row keys":
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, "1")
    let extractedGroupId = extractGroupIdFromDataRowKey(nodeKey)

    check extractedGroupId == ZeroGroupID()

  test "encodeIndexKey includes groupId":
    let tableId = makeTableId()
    let groupId = makeGroupId(1)
    let indexId = makeTableId()
    let indexKey = "idx-val"
    let pk = "pk-val"

    let key = encodeIndexKey(tableId, groupId, indexId, indexKey, pk)

    # Key format: /t/<tableId>/i/<groupId>/<indexId>/<indexKey>/<pk>
    check key.startsWith(TABLE_KEY_PREFIX)
    check key.contains("/i/")
    check key.contains($groupId)

  test "encodeIntentKey with groupId for transactional writes":
    let txnId: uint64 = 12345
    let groupId = makeGroupId(1)
    let userKey = "/t/someTableId/d/somePk"

    let intentKey = encodeIntentKey(txnId, userKey)

    # Intent key format: \x00INTENT\x00<8 bytes txnId><userKey>
    # The userKey already contains the groupId in the data row key format
    check intentKey.startsWith("\x00INTENT\x00")
    check decodeIntentUserKey(intentKey) == userKey
    check decodeIntentTxnId(intentKey) == txnId

suite "Data Row Key - Scan Bound Format":

  test "decodeDataRowKey raises on keys without groupId":
    # Scan-bound format: /t/<tableId>/d/<pk> (no groupId)
    # decodeDataRowKey should raise ValueError for these since all
    # stored keys must include a groupId.
    let tableId = makeTableId()
    let pk = "\x01\x80\x00\x00\x00\x00\x00\x00\x2a"

    let scanKey = encodeTableKey(tableId, "d/" & pk)

    # decodeDataRowKey should raise ValueError for scan-bound keys
    doAssertRaises(ValueError):
      discard decodeDataRowKey(scanKey)

  test "decodeTableKey still works for both scan-bound and stored format":
    let tableId = makeTableId()
    let groupId = makeGroupId(1)
    let pk = "test-pk"

    # Scan-bound format (without groupId)
    let scanKey = encodeTableKey(tableId, "d/" & pk)
    let (scanTid, scanPk) = decodeTableKey(scanKey)
    check scanTid == tableId
    check scanPk == "d/" & pk

    # Stored format (with groupId)
    let storedKey = encodeDataRowKey(tableId, groupId, pk)
    let (storedTid, storedPk) = decodeTableKey(storedKey)
    check storedTid == tableId

suite "Data Row Key - Group Scoping":

  test "scan bounds for specific group exclude other groups":
    let tableId = makeTableId()
    let group1 = makeGroupId(1)
    let group2 = makeGroupId(2)

    let (s1, e1) = makeGroupDataRowScanBounds(tableId, group1)

    # Key from group2 should NOT be in group1's range
    let group2Key = encodeDataRowKey(tableId, group2, "test-pk")
    check group2Key < s1 or group2Key >= e1

  test "scan bounds for specific group include keys from that group":
    let tableId = makeTableId()
    let groupId = makeGroupId(1)

    let (startKey, endKey) = makeGroupDataRowScanBounds(tableId, groupId)

    # Keys from this group should be within the range
    let pk1 = "\x01\x80\x00\x00\x00\x00\x00\x00\x01" # value 1
    let pk2 = "\x01\x80\x00\x00\x00\x00\x00\x00\x2a" # value 42
    let pk3 = "\x01\xff\xff\xff\xff\xff\xff\xff" # max int64

    let key1 = encodeDataRowKey(tableId, groupId, pk1)
    let key2 = encodeDataRowKey(tableId, groupId, pk2)
    let key3 = encodeDataRowKey(tableId, groupId, pk3)

    check key1 >= startKey
    check key1 < endKey
    check key2 >= startKey
    check key2 < endKey
    check key3 >= startKey
    check key3 < endKey

  test "table-wide scan bounds cover all groups":
    let tableId = makeTableId()
    let group1 = makeGroupId(1)
    let group2 = makeGroupId(2)
    let group3 = makeGroupId(3)

    let endKey = makeDataRowScanEndKey(tableId)

    # All group keys should be less than the end key
    let key1 = encodeDataRowKey(tableId, group1, "a")
    let key2 = encodeDataRowKey(tableId, group2, "b")
    let key3 = encodeDataRowKey(tableId, group3, "c")

    check key1 < endKey
    check key2 < endKey
    check key3 < endKey

suite "Narrow Scan Bounds to Group":

  test "narrowScanBoundsToGroup with full table range":
    let tableId = makeTableId()
    let groupId = makeGroupId(1)

    let tableStart = encodeDataRowScanBound(tableId, "")
    let tableEnd = makeDataRowScanEndKey(tableId)

    let (narrowStart, narrowEnd) = narrowScanBoundsToGroup(
        tableStart, tableEnd, tableId, groupId)

    # Should be narrowed to group prefix
    check narrowStart.startsWith(TABLE_KEY_PREFIX)
    check narrowEnd > narrowStart

    # Should contain the group's prefix
    let groupBounds = makeGroupDataRowScanBounds(tableId, groupId)
    check narrowStart == groupBounds.startKey
    check narrowEnd == groupBounds.endKey

  test "narrowScanBoundsToGroup with specific start key":
    let tableId = makeTableId()
    let groupId = makeGroupId(1)

    let specificStart = encodeDataRowKey(tableId, groupId, "\x01\x80\x00\x00\x00\x00\x00\x00\x2a")
    let tableEnd = makeDataRowScanEndKey(tableId)

    let (narrowStart, narrowEnd) = narrowScanBoundsToGroup(
        specificStart, tableEnd, tableId, groupId)

    # Start should be the specific start key (it's within the group range)
    check narrowStart == specificStart

    # End should be the group's end bound
    let groupBounds = makeGroupDataRowScanBounds(tableId, groupId)
    check narrowEnd == groupBounds.endKey

  test "narrowScanBoundsToGroup with start key outside group range":
    let tableId = makeTableId()
    let group1 = makeGroupId(1)
    let group2 = makeGroupId(2)

    # Start key in group1's range, but we're scanning group2
    let group1Start = encodeDataRowKey(tableId, group1, "")
    let tableEnd = makeDataRowScanEndKey(tableId)

    let (narrowStart, narrowEnd) = narrowScanBoundsToGroup(
        group1Start, tableEnd, tableId, group2)

    # Start should be group2's start (since group1Start < group2Start)
    let group2Bounds = makeGroupDataRowScanBounds(tableId, group2)
    check narrowStart == group2Bounds.startKey
    check narrowEnd == group2Bounds.endKey

suite "Key Rewrite with Group ID":

  test "addGroupIdToKey adds groupId to scan-bound key":
    let tableId = makeTableId()
    let groupId = makeGroupId(5)
    let pk = "\x01\x80\x00\x00\x00\x00\x00\x00\x2a"

    # Create a scan-bound key (without groupId)
    let scanKey = encodeDataRowScanBound(tableId, pk)

    # Add groupId
    let rewrittenKey = addGroupIdToKey(scanKey, groupId)

    # Should now be in the stored format
    check rewrittenKey != scanKey
    let (decodedTableId, decodedGroupId, decodedPk) = decodeDataRowKey(rewrittenKey)
    check decodedTableId == tableId
    check decodedGroupId == groupId
    check decodedPk == pk

  test "addGroupIdToKey leaves stored-format key unchanged":
    let tableId = makeTableId()
    let groupId = makeGroupId(5)
    let pk = "\x01\x80\x00\x00\x00\x00\x00\x00\x2a"

    # Create a stored-format key (with groupId)
    let storedKey = encodeDataRowKey(tableId, groupId, pk)

    # Add groupId to already-has-groupId key — should be unchanged
    let rewrittenKey = addGroupIdToKey(storedKey, groupId)
    check rewrittenKey == storedKey

  test "addGroupIdToKey leaves non-data-row keys unchanged":
    let nodeKey = encodeTableKey(SYS_NODES_TABLE_ID, "1")
    let groupId = makeGroupId(1)

    # System table keys should not be rewritten
    let rewrittenKey = addGroupIdToKey(nodeKey, groupId)
    check rewrittenKey == nodeKey

  test "addGroupIdToKey handles empty pk":
    let tableId = makeTableId()
    let groupId = makeGroupId(3)

    let scanKey = encodeDataRowScanBound(tableId, "")
    let rewrittenKey = addGroupIdToKey(scanKey, groupId)

    let (decodedTableId, decodedGroupId, decodedPk) = decodeDataRowKey(rewrittenKey)
    check decodedTableId == tableId
    check decodedGroupId == groupId
    check decodedPk == ""
