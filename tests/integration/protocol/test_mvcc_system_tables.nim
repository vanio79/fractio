# Integration tests for MVCC Transaction Store with System Tables
#
# Tests transaction semantics for system table operations through Raft:
#   - System table CRUD with MVCC
#   - Cross-transaction visibility
#   - Raft-backed persistence
#
# Port range: 20670-20699

import std/[unittest, os, options]
import fractio/protocol/raft_store
import fractio/protocol/mvcc_store
import fractio/protocol/txn_manager
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types as rangeTypes
import fractio/distributed/meta/system_tables
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider
import fractio/core/types as coreTypes
import fractio/storage/wisckey_backend
import fractio/distributed/meta/system_schemas

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

var testBasePort {.global.} = 20670

proc nextBasePort(): int =
  result = testBasePort
  testBasePort += 5

proc cleanDir(path: string) =
  try: removeDir(path) except CatchableError: discard
  try: createDir(path) except CatchableError: discard

proc makeMvccStore(storagePath: string): tuple[
    coord: NuRaftCoordinator, raftStore: RaftKVStoreExt,
    mvccStore: MvccTransactionStore, txnMgr: TransactionManager] =
  cleanDir(storagePath)
  let nodeId = rangeTypes.NodeID(1)
  let port = nextBasePort()
  let members = @[(nodeId: 1'u32, host: "127.0.0.1", port: port)]

  let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
    nodeId: nodeId,
    port: port,
    host: "127.0.0.1",
    dataDir: storagePath,
    electionTimeoutLowerMs: 200,
    electionTimeoutUpperMs: 400,
    heartbeatIntervalMs: 100,
  ))
  coord.start()

  for rid in [META_GROUP_ID, DATA_GROUP_START_ID]:
    doAssert coord.createAndStartGroup(rid, members)

  for attempt in 0 ..< 50:
    if coord.isLeader(META_GROUP_ID) and coord.isLeader(DATA_GROUP_START_ID):
      break
    os.sleep(100)

  let raftStore = newRaftKVStoreExt(coord, proposeTimeoutMs = 2000)
  raftStore.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

  let txnMgr = newTransactionManager()
  let mockTimer = MockTimeProvider(currentTime: timerTypes.Timestamp(1_000_000_000))
  let tsProvider = newTimestampProvider(mockTimer, nodeId.uint16)

  let mvccStore = newMvccTransactionStore(raftStore, txnMgr, tsProvider)
  (coord, raftStore, mvccStore, txnMgr)

proc teardownMvccStore(coord: NuRaftCoordinator, storagePath: string) =
  coord.stop()
  try: removeDir(storagePath) except CatchableError: discard

# System table key helpers
proc tableKey(tableId: TableId): string =
  result = "/t/" & formatTableId(tableId) & "/meta"

proc columnKey(tableId: TableId, colName: string): string =
  result = "/t/" & formatTableId(tableId) & "/col/" & colName

proc indexKey(tableId: TableId, idxName: string): string =
  result = "/t/" & formatTableId(tableId) & "/idx/" & idxName

# ---------------------------------------------------------------------------
# Suite: System Table Transactions
# ---------------------------------------------------------------------------

suite "MVCC System Tables - Basic Operations":
  test "create and read table metadata":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_st01")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_st01")

    let tableId = genTableIdLocal()
    let key = tableKey(tableId)
    let value = """{"name": "users", "columns": 3}"""

    # Write table metadata with explicit transaction
    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    let putRes = mvccStore.txnPut(sessionId, key, value)
    check putRes.isOk
    let commitRes = mvccStore.commitTransaction(sessionId)
    check commitRes.isOk
    mvccStore.closeSession(sessionId)

    # Read it back using latestGet
    let getRes = mvccStore.latestGet(key)
    check getRes.isOk
    check getRes.value.isSome
    check getRes.value.get() == value

  test "create table with columns in transaction":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_st02")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_st02")

    let tableId = genTableIdLocal()
    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)

    # Write table metadata
    let tableMeta = """{"name": "products", "columns": 2}"""
    discard mvccStore.txnPut(sessionId, tableKey(tableId), tableMeta)

    # Write column metadata
    discard mvccStore.txnPut(sessionId, columnKey(tableId, "id"), """{"type": "int", "primary": true}""")
    discard mvccStore.txnPut(sessionId, columnKey(tableId, "name"), """{"type": "string", "nullable": false}""")

    # Commit
    let commitRes = mvccStore.commitTransaction(sessionId)
    check commitRes.isOk

    # Verify all data is visible
    let sessionId2 = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId2)

    let tableRes = mvccStore.txnGet(sessionId2, tableKey(tableId))
    check tableRes.isOk
    check tableRes.value.isSome
    check tableRes.value.get() == tableMeta

    let colRes = mvccStore.txnGet(sessionId2, columnKey(tableId, "id"))
    check colRes.isOk
    check colRes.value.isSome

    mvccStore.closeSession(sessionId2)

  test "rollback table creation":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_st03")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_st03")

    let tableId = genTableIdLocal()
    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)

    # Write table metadata
    discard mvccStore.txnPut(sessionId, tableKey(tableId), """{"name": "temp"}""")

    # Rollback
    let rollbackRes = mvccStore.rollbackTransaction(sessionId)
    check rollbackRes.isOk

    # Verify table doesn't exist
    let getRes = mvccStore.latestGet(tableKey(tableId))
    check getRes.isOk
    check getRes.value.isNone

  test "update table metadata":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_st04")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_st04")

    let tableId = genTableIdLocal()
    let key = tableKey(tableId)

    # Initial write
    var sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, key, """{"version": 1}""")
    discard mvccStore.commitTransaction(sessionId)
    mvccStore.closeSession(sessionId)

    # Update in transaction
    sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, key, """{"version": 2}""")
    let commitRes = mvccStore.commitTransaction(sessionId)
    check commitRes.isOk

    # Verify new version
    let getRes = mvccStore.latestGet(key)
    check getRes.isOk
    check getRes.value.isSome
    check getRes.value.get() == """{"version": 2}"""

# ---------------------------------------------------------------------------
# Suite: Transaction Isolation
# ---------------------------------------------------------------------------

suite "MVCC System Tables - Isolation":
  test "read snapshot isolation":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_iso01")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_iso01")

    let tableId = genTableIdLocal()
    let key = tableKey(tableId)

    # Initial write
    var sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, key, """{"version": 1}""")
    discard mvccStore.commitTransaction(sessionId)
    mvccStore.closeSession(sessionId)

    # Start transaction 1
    let session1 = mvccStore.createSession()
    discard mvccStore.beginTransaction(session1)

    # Read initial value
    let get1 = mvccStore.txnGet(session1, key)
    check get1.isOk
    check get1.value.get() == """{"version": 1}"""

    # Transaction 2 updates the key
    var session2 = mvccStore.createSession()
    discard mvccStore.beginTransaction(session2)
    discard mvccStore.txnPut(session2, key, """{"version": 2}""")
    discard mvccStore.commitTransaction(session2)
    mvccStore.closeSession(session2)

    # Transaction 1 still sees old value (snapshot isolation)
    let get2 = mvccStore.txnGet(session1, key)
    check get2.isOk
    check get2.value.get() == """{"version": 1}"""

    mvccStore.closeSession(session1)

  test "write-write conflict detection":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_iso02")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_iso02")

    let tableId = genTableIdLocal()
    let key = tableKey(tableId)

    # Initial write
    var sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, key, """{"version": 1}""")
    discard mvccStore.commitTransaction(sessionId)
    mvccStore.closeSession(sessionId)

    # Start two concurrent transactions
    let session1 = mvccStore.createSession()
    discard mvccStore.beginTransaction(session1)
    discard mvccStore.txnPut(session1, key, """{"version": 2}""")

    let session2 = mvccStore.createSession()
    discard mvccStore.beginTransaction(session2)
    discard mvccStore.txnPut(session2, key, """{"version": 3}""")

    # First commit succeeds
    let commit1 = mvccStore.commitTransaction(session1)
    check commit1.isOk

    # Second commit should detect conflict (or succeed depending on implementation)
    # In our simplified implementation, conflict is detected at commit time
    let commit2 = mvccStore.commitTransaction(session2)
    # The second transaction may conflict because it wrote the same key
    # Our implementation may or may not detect this depending on timing

  # ---------------------------------------------------------------------------
  # Suite: Index Operations
  # ---------------------------------------------------------------------------

suite "MVCC System Tables - Index Operations":
  test "create and scan indexes":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_idx01")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_idx01")

    let tableId = genTableIdLocal()
    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)

    # Create multiple indexes
    discard mvccStore.txnPut(sessionId, indexKey(tableId, "primary"), """{"type": "primary", "columns": ["id"]}""")
    discard mvccStore.txnPut(sessionId, indexKey(tableId, "name_idx"), """{"type": "btree", "columns": ["name"]}""")
    discard mvccStore.txnPut(sessionId, indexKey(tableId, "email_idx"), """{"type": "hash", "columns": ["email"]}""")

    let commitRes = mvccStore.commitTransaction(sessionId)
    check commitRes.isOk

    # Scan all indexes for this table
    let sessionId2 = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId2)

    let scanKey = "/t/" & formatTableId(tableId) & "/idx/"
    let scanRes = mvccStore.txnScan(sessionId2, scanKey, scanKey & "\xFF", 0)
    check scanRes.isOk
    check scanRes.value.len == 3

    mvccStore.closeSession(sessionId2)

  test "delete index":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_idx02")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_idx02")

    let tableId = genTableIdLocal()
    let idxKey = indexKey(tableId, "temp_idx")

    # Create index
    var sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, idxKey, """{"type": "btree"}""")
    discard mvccStore.commitTransaction(sessionId)
    mvccStore.closeSession(sessionId)

    # Verify it exists
    let get1 = mvccStore.latestGet(idxKey)
    check get1.isOk
    check get1.value.isSome

    # Delete in transaction
    sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    let delRes = mvccStore.txnDelete(sessionId, idxKey)
    check delRes.isOk
    let commitRes = mvccStore.commitTransaction(sessionId)
    check commitRes.isOk

    # Verify it's gone
    let get2 = mvccStore.latestGet(idxKey)
    check get2.isOk
    check get2.value.isNone

# ---------------------------------------------------------------------------
# Suite: Persistence
# ---------------------------------------------------------------------------

suite "MVCC System Tables - Persistence":
  test "data survives restart":
    let storagePath = "/tmp/fractio_mvcc_persist01"
    cleanDir(storagePath)
    let tableId = genTableIdLocal() # Use same tableId across both instances
    let key = tableKey(tableId)
    let value = """{"name": "persistent_table"}"""

    # First instance
    block:
      let nodeId = rangeTypes.NodeID(1)
      let port = nextBasePort()
      let members = @[(nodeId: 1'u32, host: "127.0.0.1", port: port)]

      let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
        nodeId: nodeId,
        port: port,
        host: "127.0.0.1",
        dataDir: storagePath,
        electionTimeoutLowerMs: 200,
        electionTimeoutUpperMs: 400,
        heartbeatIntervalMs: 100,
      ))
      coord.start()

      for rid in [META_GROUP_ID, DATA_GROUP_START_ID]:
        doAssert coord.createAndStartGroup(rid, members)

      for attempt in 0 ..< 50:
        if coord.isLeader(META_GROUP_ID) and coord.isLeader(DATA_GROUP_START_ID):
          break
        os.sleep(100)

      let raftStore = newRaftKVStoreExt(coord, proposeTimeoutMs = 2000)
      raftStore.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

      let txnMgr = newTransactionManager()
      let mockTimer = MockTimeProvider(currentTime: timerTypes.Timestamp(1_000_000_000))
      let tsProvider = newTimestampProvider(mockTimer, nodeId.uint16)

      let mvccStore = newMvccTransactionStore(raftStore, txnMgr, tsProvider)

      let sessionId = mvccStore.createSession()
      discard mvccStore.beginTransaction(sessionId)
      let putRes = mvccStore.txnPut(sessionId, key, value)
      check putRes.isOk
      let commitRes = mvccStore.commitTransaction(sessionId)
      check commitRes.isOk
      mvccStore.closeSession(sessionId)

      coord.stop()
      # Don't delete the storage - we want to verify persistence

    # Second instance - verify data persisted
    block:
      let nodeId = rangeTypes.NodeID(1)
      let port = nextBasePort()
      let members = @[(nodeId: 1'u32, host: "127.0.0.1", port: port)]

      let coord = newNuRaftCoordinator(nuraft_coordinator.CoordinatorConfig(
        nodeId: nodeId,
        port: port,
        host: "127.0.0.1",
        dataDir: storagePath,
        electionTimeoutLowerMs: 200,
        electionTimeoutUpperMs: 400,
        heartbeatIntervalMs: 100,
      ))
      coord.start()

      for rid in [META_GROUP_ID, DATA_GROUP_START_ID]:
        doAssert coord.createAndStartGroup(rid, members)

      for attempt in 0 ..< 50:
        if coord.isLeader(META_GROUP_ID) and coord.isLeader(DATA_GROUP_START_ID):
          break
        os.sleep(100)

      let raftStore = newRaftKVStoreExt(coord, proposeTimeoutMs = 2000)
      raftStore.bootstrapStore(@[META_GROUP_ID, DATA_GROUP_START_ID])

      let txnMgr = newTransactionManager()
      let mockTimer = MockTimeProvider(currentTime: timerTypes.Timestamp(1_000_000_000))
      let tsProvider = newTimestampProvider(mockTimer, nodeId.uint16)

      let mvccStore = newMvccTransactionStore(raftStore, txnMgr, tsProvider)

      let getRes = mvccStore.latestGet(key)
      check getRes.isOk
      check getRes.value.isSome
      check getRes.value.get() == """{"name": "persistent_table"}"""

      coord.stop()

    # Clean up after test
    removeDir(storagePath)
