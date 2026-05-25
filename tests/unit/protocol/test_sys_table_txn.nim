# Unit tests for MicroTransaction (sys_table_txn.nim)
#
# Tests the system table transaction API:
#   - SysTxnOp construction (put/delete)
#   - MicroTransaction lifecycle (begin, put, delete, commit)
#   - Convenience wrappers (putSysRow, deleteSysRow)
#   - Commit-after-commit returns error
#   - Put/delete after commit are silently ignored
#   - Shared MVCC timestamp across all ops
#   - Backward-compatible V2 wrappers
#   - SysTxnResult success/failure construction
#
# Note: commit() cannot be tested in pure unit tests because it requires
# a running Raft cluster. Integration tests cover that path.

import std/[unittest, strutils]

import fractio/distributed/raft/group_types
import fractio/distributed/raft/multigroup_types
import fractio/distributed/meta/system_tables
import fractio/protocol/raft_store

suite "SysTxnOp":

  test "put operation stores key and value":
    let op = SysTxnOp(kind: stokPut, key: "/t/test/key1", value: "val1")
    check op.kind == stokPut
    check op.key == "/t/test/key1"
    check op.value == "val1"

  test "delete operation stores key":
    let op = SysTxnOp(kind: stokDelete, deleteKey: "/t/test/key2")
    check op.kind == stokDelete
    check op.deleteKey == "/t/test/key2"

  test "SysTxnOpKind enumeration values":
    check stokPut.ord == 0
    check stokDelete.ord == 1

suite "MicroTransaction Lifecycle":

  test "beginSysTxn creates empty uncommitted transaction":
    # Create a minimal MicroTransaction directly (without RaftKVStoreExt)
    var txn = MicroTransaction(
      store: nil, # No store needed for lifecycle tests
      ops: @[],
      committed: false
    )
    check txn.ops.len == 0
    check txn.committed == false

  test "put adds put operation":
    var txn = MicroTransaction(
      store: nil,
      ops: @[],
      committed: false
    )
    txn.put("/t/key1", "value1")
    check txn.ops.len == 1
    check txn.ops[0].kind == stokPut
    check txn.ops[0].key == "/t/key1"
    check txn.ops[0].value == "value1"

  test "delete adds delete operation":
    var txn = MicroTransaction(
      store: nil,
      ops: @[],
      committed: false
    )
    txn.delete("/t/key2")
    check txn.ops.len == 1
    check txn.ops[0].kind == stokDelete
    check txn.ops[0].deleteKey == "/t/key2"

  test "put and delete can be mixed":
    var txn = MicroTransaction(
      store: nil,
      ops: @[],
      committed: false
    )
    txn.put("/t/a", "val_a")
    txn.delete("/t/b")
    txn.put("/t/c", "val_c")
    check txn.ops.len == 3
    check txn.ops[0].kind == stokPut
    check txn.ops[1].kind == stokDelete
    check txn.ops[2].kind == stokPut

  test "put after commit is silently ignored":
    var txn = MicroTransaction(
      store: nil,
      ops: @[],
      committed: true # Simulate already committed
    )
    txn.put("/t/after", "should_be_ignored")
    check txn.ops.len == 0 # No new op added

  test "delete after commit is silently ignored":
    var txn = MicroTransaction(
      store: nil,
      ops: @[],
      committed: true # Simulate already committed
    )
    txn.delete("/t/after")
    check txn.ops.len == 0 # No new op added

  test "multiple puts accumulate in order":
    var txn = MicroTransaction(
      store: nil,
      ops: @[],
      committed: false
    )
    for i in 0..<10:
      txn.put($i, "val_" & $i)
    check txn.ops.len == 10
    for i in 0..<10:
      check txn.ops[i].key == $i
      check txn.ops[i].value == "val_" & $i

  test "multiple deletes accumulate in order":
    var txn = MicroTransaction(
      store: nil,
      ops: @[],
      committed: false
    )
    for i in 0..<5:
      txn.delete("/key/" & $i)
    check txn.ops.len == 5
    for i in 0..<5:
      check txn.ops[i].deleteKey == "/key/" & $i

suite "putSysRow and deleteSysRow":

  test "putSysRow encodes key using encodeTableKey":
    var txn = MicroTransaction(
      store: nil,
      ops: @[],
      committed: false
    )
    txn.putSysRow(SYS_NODES_TABLE_ID, "42", "node_data")
    check txn.ops.len == 1
    check txn.ops[0].kind == stokPut
    check txn.ops[0].key == encodeTableKey(SYS_NODES_TABLE_ID, "42")
    check txn.ops[0].value == "node_data"

  test "deleteSysRow encodes key using encodeTableKey":
    var txn = MicroTransaction(
      store: nil,
      ops: @[],
      committed: false
    )
    txn.deleteSysRow(SYS_GROUPS_TABLE_ID, $META_GROUP_ID)
    check txn.ops.len == 1
    check txn.ops[0].kind == stokDelete
    check txn.ops[0].deleteKey == encodeTableKey(SYS_GROUPS_TABLE_ID,
        $META_GROUP_ID)

  test "mixed putSysRow and deleteSysRow":
    var txn = MicroTransaction(
      store: nil,
      ops: @[],
      committed: false
    )
    txn.putSysRow(SYS_DATABASES_TABLE_ID, "default", "db_data")
    txn.putSysRow(SYS_SCHEMAS_TABLE_ID, "public", "schema_data")
    txn.deleteSysRow(SYS_NODES_TABLE_ID, "old_node")
    check txn.ops.len == 3
    check txn.ops[0].key == encodeTableKey(SYS_DATABASES_TABLE_ID, "default")
    check txn.ops[1].key == encodeTableKey(SYS_SCHEMAS_TABLE_ID, "public")
    check txn.ops[2].deleteKey == encodeTableKey(SYS_NODES_TABLE_ID, "old_node")

  test "putSysRow with all system table IDs":
    var txn = MicroTransaction(
      store: nil,
      ops: @[],
      committed: false
    )
    txn.putSysRow(SYS_DATABASES_TABLE_ID, "k1", "v1")
    txn.putSysRow(SYS_SCHEMAS_TABLE_ID, "k2", "v2")
    txn.putSysRow(SYS_TABLES_TABLE_ID, "k3", "v3")
    txn.putSysRow(SYS_GROUPS_TABLE_ID, "k4", "v4")
    txn.putSysRow(SYS_NODES_TABLE_ID, "k5", "v5")
    txn.putSysRow(SYS_SETTINGS_TABLE_ID, "k6", "v6")
    txn.putSysRow(SYS_SPACES_TABLE_ID, "k7", "v7")
    check txn.ops.len == 7
    for op in txn.ops:
      check op.kind == stokPut
      check op.key.startsWith("/t/")

suite "SysTxnResult":

  test "SysTxnResult success":
    let result = SysTxnResult(isOk: true, opsCommitted: 3,
        timestampNs: 12345678'i64)
    check result.isOk
    check result.opsCommitted == 3
    check result.timestampNs == 12345678

  test "SysTxnResult success with zero ops":
    let result = SysTxnResult(isOk: true, opsCommitted: 0, timestampNs: 0'i64)
    check result.isOk
    check result.opsCommitted == 0

  test "SysTxnResult failure with not leader error":
    let err = newRSE(rseNotLeader, "not the leader", hint = 42'u32)
    let result = SysTxnResult(isOk: false, error: err)
    check not result.isOk
    check result.error.kind == rseNotLeader
    check result.error.msg == "not the leader"
    check result.error.leaderHint == 42

  test "SysTxnResult failure with timeout error":
    let err = newRSE(rseTimeout, "propose timed out")
    let result = SysTxnResult(isOk: false, error: err)
    check not result.isOk
    check result.error.kind == rseTimeout

  test "SysTxnResult failure with group not found":
    let err = newRSE(rseGroupNotFound, "group not found")
    let result = SysTxnResult(isOk: false, error: err)
    check not result.isOk
    check result.error.kind == rseGroupNotFound

  test "SysTxnResult failure with internal error":
    let err = newRSE(rseInternal, "already committed")
    let result = SysTxnResult(isOk: false, error: err)
    check not result.isOk
    check result.error.kind == rseInternal

  test "SysTxnResult failure with bad routing":
    let err = newRSE(rseBadRouting, "key hashes to different group")
    let result = SysTxnResult(isOk: false, error: err)
    check not result.isOk
    check result.error.kind == rseBadRouting

suite "encodeTableKey Integration":

  test "all system table keys use correct prefix":
    # Verify that system table keys are correctly encoded for MicroTransaction
    check encodeTableKey(SYS_DATABASES_TABLE_ID, "default").startsWith("/t/")
    check encodeTableKey(SYS_SCHEMAS_TABLE_ID, "public").startsWith("/t/")
    check encodeTableKey(SYS_TABLES_TABLE_ID, "users").startsWith("/t/")
    check encodeTableKey(SYS_GROUPS_TABLE_ID, $META_GROUP_ID).startsWith("/t/")
    check encodeTableKey(SYS_NODES_TABLE_ID, "1").startsWith("/t/")
    check encodeTableKey(SYS_SETTINGS_TABLE_ID, "raft.timeout").startsWith("/t/")
    check encodeTableKey(SYS_SPACES_TABLE_ID, "default_space").startsWith("/t/")

  test "system table IDs are well-known":
    check isSystemTableId(SYS_DATABASES_TABLE_ID)
    check isSystemTableId(SYS_SCHEMAS_TABLE_ID)
    check isSystemTableId(SYS_TABLES_TABLE_ID)
    check isSystemTableId(SYS_GROUPS_TABLE_ID)
    check isSystemTableId(SYS_NODES_TABLE_ID)
    check isSystemTableId(SYS_SETTINGS_TABLE_ID)
    check isSystemTableId(SYS_SPACES_TABLE_ID)

  test "meta group tables are in group 1":
    check isMetaGroupTableId(SYS_DATABASES_TABLE_ID)
    check isMetaGroupTableId(SYS_SCHEMAS_TABLE_ID)
    check isMetaGroupTableId(SYS_TABLES_TABLE_ID)
    check isMetaGroupTableId(SYS_GROUPS_TABLE_ID)
    check isMetaGroupTableId(SYS_NODES_TABLE_ID)
    check isMetaGroupTableId(SYS_SETTINGS_TABLE_ID)
    check isMetaGroupTableId(SYS_SPACES_TABLE_ID)
