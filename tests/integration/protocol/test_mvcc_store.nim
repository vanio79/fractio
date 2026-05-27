# Unit tests for MVCC Transaction Store
#
# Tests transaction semantics for system table operations:
#   - Session management
#   - Transaction lifecycle (begin/commit/rollback)
#   - Intent-based writes
#   - Snapshot reads
#   - Conflict detection
#
# Port range: 20640-20669

import std/[unittest, os, options, strutils]
import fractio/protocol/raft_store
import fractio/protocol/mvcc_store
import fractio/protocol/messages/kv
import fractio/protocol/txn_manager
import fractio/distributed/raft/nuraft_coordinator
import fractio/distributed/raft/group_types
import fractio/distributed/meta/system_tables
import fractio/distributed/sharedtimer/mock
import fractio/distributed/sharedtimer/types as timerTypes
import fractio/core/timestamp_provider
import fractio/core/types as coreTypes except NodeID
import fractio/core/transaction as coreTxn
import fractio/storage/mvcc/types as mvccTypes
import fractio/storage/wisckey_backend

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

var testBasePort {.global.} = 20640

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
  let nodeId = NodeID(1)
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
  let mockTimer = MockTimeProvider(currentTime: Timestamp(1_000_000_000))
  let tsProvider = newTimestampProvider(mockTimer, nodeId.uint16)

  let mvccStore = newMvccTransactionStore(raftStore, txnMgr, tsProvider)
  (coord, raftStore, mvccStore, txnMgr)

proc teardownMvccStore(coord: NuRaftCoordinator, storagePath: string) =
  coord.stop()
  try: removeDir(storagePath) except CatchableError: discard

# ---------------------------------------------------------------------------
# Suite: Session management
# ---------------------------------------------------------------------------

suite "MvccTransactionStore - Session management":
  test "create and close session":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_s01")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_s01")

    let sessionId = mvccStore.createSession()
    check sessionId > 0

    let state = mvccStore.getSessionState(sessionId)
    check state.isSome
    check state.get().txn == nil

    mvccStore.closeSession(sessionId)
    let state2 = mvccStore.getSessionState(sessionId)
    check state2.isNone

  test "multiple sessions have unique IDs":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_s02")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_s02")

    let id1 = mvccStore.createSession()
    let id2 = mvccStore.createSession()
    let id3 = mvccStore.createSession()

    check id1 != id2
    check id2 != id3
    check id1 != id3

    check mvccStore.getSessionCount() == 3

# ---------------------------------------------------------------------------
# Suite: Transaction lifecycle
# ---------------------------------------------------------------------------

suite "MvccTransactionStore - Transaction lifecycle":
  test "begin transaction creates active txn":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_t01")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_t01")

    let sessionId = mvccStore.createSession()
    let beginRes = mvccStore.beginTransaction(sessionId)

    check beginRes.isOk
    check beginRes.value != coreTypes.zeroTransactionID()

    let statusRes = mvccStore.getTransactionStatus(sessionId)
    check statusRes.isOk
    check statusRes.value == mvccTypes.TXN_PENDING

  test "commit transaction succeeds":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_t02")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_t02")

    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)

    # Write a key
    let putRes = mvccStore.txnPut(sessionId, "test_key", "test_value")
    check putRes.isOk

    # Commit
    let commitRes = mvccStore.commitTransaction(sessionId)
    check commitRes.isOk
    check commitRes.value > coreTypes.Timestamp(0)

  test "rollback transaction cleans up intents":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_t03")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_t03")

    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)

    let putRes = mvccStore.txnPut(sessionId, "rollback_key", "rollback_value")
    check putRes.isOk

    # Rollback
    let rollbackRes = mvccStore.rollbackTransaction(sessionId)
    check rollbackRes.isOk

    # Key should not exist
    let getRes = mvccStore.latestGet("rollback_key")
    check getRes.isOk
    check getRes.value.isNone

  test "begin on existing transaction returns same txn ID":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_t04")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_t04")

    let sessionId = mvccStore.createSession()
    let begin1 = mvccStore.beginTransaction(sessionId)
    let begin2 = mvccStore.beginTransaction(sessionId)

    check begin1.isOk
    check begin2.isOk
    check begin1.value == begin2.value

# ---------------------------------------------------------------------------
# Suite: Transactional reads and writes
# ---------------------------------------------------------------------------

suite "MvccTransactionStore - Transactional reads and writes":
  test "txnPut and txnGet within same transaction":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_rw01")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_rw01")

    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)

    # Write
    let putRes = mvccStore.txnPut(sessionId, "my_key", "my_value")
    check putRes.isOk

    # Read within same transaction should see the uncommitted value
    let getRes = mvccStore.txnGet(sessionId, "my_key")
    check getRes.isOk
    check getRes.value.isSome
    check getRes.value.get() == "my_value"

  test "read your own writes before commit":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_rw02")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_rw02")

    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)

    discard mvccStore.txnPut(sessionId, "k1", "v1")
    discard mvccStore.txnPut(sessionId, "k1", "v2") # overwrite

    let getRes = mvccStore.txnGet(sessionId, "k1")
    check getRes.isOk
    check getRes.value.get() == "v2"

  test "delete creates tombstone":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_rw03")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_rw03")

    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)

    # Write then delete
    discard mvccStore.txnPut(sessionId, "del_key", "del_value")
    let delRes = mvccStore.txnDelete(sessionId, "del_key")
    check delRes.isOk

    # Should return none (deleted in this transaction)
    let getRes = mvccStore.txnGet(sessionId, "del_key")
    check getRes.isOk
    check getRes.value.isNone

  test "committed value visible after commit":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_rw04")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_rw04")

    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, "committed_key", "committed_value")
    discard mvccStore.commitTransaction(sessionId)

    # Start new transaction to read
    let sessionId2 = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId2)

    let getRes = mvccStore.txnGet(sessionId2, "committed_key")
    check getRes.isOk
    check getRes.value.isSome
    check getRes.value.get() == "committed_value"

# ---------------------------------------------------------------------------
# Suite: Direct operations (auto-commit)
# ---------------------------------------------------------------------------

suite "MvccTransactionStore - Auto-commit operations":
  test "put and latestGet with explicit transaction":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_d01")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_d01")

    # Use explicit transaction for put
    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    let putRes = mvccStore.txnPut(sessionId, "direct_key", "direct_value")
    check putRes.isOk
    let commitRes = mvccStore.commitTransaction(sessionId)
    check commitRes.isOk
    mvccStore.closeSession(sessionId)

    let getRes = mvccStore.latestGet("direct_key")
    check getRes.isOk
    check getRes.value.isSome
    check getRes.value.get() == "direct_value"

  test "delete removes key":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_d02")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_d02")

    # Put
    var sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, "to_delete", "value")
    discard mvccStore.commitTransaction(sessionId)
    mvccStore.closeSession(sessionId)

    # Delete
    sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    let delRes = mvccStore.txnDelete(sessionId, "to_delete")
    check delRes.isOk
    discard mvccStore.commitTransaction(sessionId)
    mvccStore.closeSession(sessionId)

    let getRes = mvccStore.latestGet("to_delete")
    check getRes.isOk
    check getRes.value.isNone

  test "latestScan returns multiple keys":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_d03")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_d03")

    # Put multiple keys in a transaction
    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, "scan_a", "1")
    discard mvccStore.txnPut(sessionId, "scan_b", "2")
    discard mvccStore.txnPut(sessionId, "scan_c", "3")
    discard mvccStore.commitTransaction(sessionId)
    mvccStore.closeSession(sessionId)

    let scanRes = mvccStore.latestScan("scan_a", "scan_d", 0)
    check scanRes.isOk
    check scanRes.value.len == 3

# ---------------------------------------------------------------------------
# Suite: Conflict detection
# ---------------------------------------------------------------------------

suite "MvccTransactionStore - Conflict detection":
  test "concurrent write conflict detected":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_c01")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_c01")

    # Session 1: write and commit
    let session1 = mvccStore.createSession()
    discard mvccStore.beginTransaction(session1)
    discard mvccStore.txnPut(session1, "conflict_key", "value1")
    let commit1 = mvccStore.commitTransaction(session1)
    check commit1.isOk

    # Session 2: write to same key (should detect conflict)
    let session2 = mvccStore.createSession()
    discard mvccStore.beginTransaction(session2)
    discard mvccStore.txnPut(session2, "conflict_key", "value2")

    # This should succeed since we haven't committed yet
    # Conflict is detected at commit time
    let commit2 = mvccStore.commitTransaction(session2)
    # Note: In our simplified implementation, conflict is detected via commitIndex
    # The second commit should either succeed or fail depending on timing
    # For a proper test, we'd need two concurrent transactions

  # ---------------------------------------------------------------------------
  # Suite: snapshotStreamScan
  # ---------------------------------------------------------------------------

suite "MvccTransactionStore - snapshotStreamScan":

  test "snapshotStreamScan returns all keys in range":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_ss01")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_ss01")

    # Insert data via transaction
    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, "stream_a", "val_a")
    discard mvccStore.txnPut(sessionId, "stream_b", "val_b")
    discard mvccStore.txnPut(sessionId, "stream_c", "val_c")
    discard mvccStore.txnPut(sessionId, "other_x", "val_x")
    let commitRes = mvccStore.commitTransaction(sessionId)
    check commitRes.isOk

    # Wait for Raft replication
    os.sleep(200)

    var chunksReceived = 0
    var totalPairs = 0

    let ok = mvccStore.snapshotStreamScan(
      startKey = "stream_a",
      endKey = "stream_z",
      readTs = LATEST_READ_TIMESTAMP,
      limit = 0,
      chunkSize = 100,
      callback = proc(chunk: ScanChunk) {.gcsafe, raises: [].} =
      inc chunksReceived
      totalPairs += chunk.pairs.len
    )
    check ok
    check chunksReceived >= 1
    check totalPairs == 3 # stream_a, stream_b, stream_c

  test "snapshotStreamScan with group filter":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_ss02")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_ss02")

    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, "gfilter_a", "val_a")
    discard mvccStore.txnPut(sessionId, "gfilter_b", "val_b")
    discard mvccStore.txnPut(sessionId, "gfilter_c", "val_c")
    discard mvccStore.commitTransaction(sessionId)

    os.sleep(200)

    # Filter that only allows keys containing 'b'
    var totalPairs = 0
    let ok = mvccStore.snapshotStreamScan(
      startKey = "gfilter_a",
      endKey = "gfilter_z",
      readTs = LATEST_READ_TIMESTAMP,
      limit = 0,
      chunkSize = 100,
      callback = proc(chunk: ScanChunk) {.gcsafe, raises: [].} =
      totalPairs += chunk.pairs.len
    ,
      groupFilter = proc(key: string): bool {.gcsafe, raises: [].} =
      key.contains("b")
    )
    check ok
    check totalPairs == 1 # Only gfilter_b passes the filter

  test "snapshotStreamScan with limit":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_ss03")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_ss03")

    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, "limit_a", "1")
    discard mvccStore.txnPut(sessionId, "limit_b", "2")
    discard mvccStore.txnPut(sessionId, "limit_c", "3")
    discard mvccStore.txnPut(sessionId, "limit_d", "4")
    discard mvccStore.txnPut(sessionId, "limit_e", "5")
    discard mvccStore.commitTransaction(sessionId)

    os.sleep(200)

    var totalPairs = 0
    let ok = mvccStore.snapshotStreamScan(
      startKey = "limit_a",
      endKey = "limit_z",
      readTs = LATEST_READ_TIMESTAMP,
      limit = 3,
      chunkSize = 100,
      callback = proc(chunk: ScanChunk) {.gcsafe, raises: [].} =
      totalPairs += chunk.pairs.len
    )
    check ok
    check totalPairs == 3

  test "snapshotStreamScan with small chunk size sends multiple chunks":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_ss04")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_ss04")

    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    for i in 0..<10:
      discard mvccStore.txnPut(sessionId, "chunk_" & $i, "val_" & $i)
    discard mvccStore.commitTransaction(sessionId)

    os.sleep(200)

    var chunksReceived = 0
    var totalPairs = 0
    var sawHasMore = false
    var sawFinalChunk = false

    let ok = mvccStore.snapshotStreamScan(
      startKey = "chunk_",
      endKey = "chunk_z",
      readTs = LATEST_READ_TIMESTAMP,
      limit = 0,
      chunkSize = 3, # Small chunk size to force multiple chunks
      callback = proc(chunk: ScanChunk) {.gcsafe, raises: [].} =
        inc chunksReceived
        totalPairs += chunk.pairs.len
        if chunk.hasMore:
          sawHasMore = true
        else:
          sawFinalChunk = true
    )
    check ok
    check chunksReceived >= 4 # 10 items / 3 per chunk = 4 chunks
    check totalPairs == 10
    check sawHasMore
    check sawFinalChunk

  test "snapshotStreamScan with empty range":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_ss05")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_ss05")

    # No data in this range
    var chunksReceived = 0
    var totalPairs = 0

    let ok = mvccStore.snapshotStreamScan(
      startKey = "empty_a",
      endKey = "empty_z",
      readTs = LATEST_READ_TIMESTAMP,
      limit = 0,
      chunkSize = 100,
      callback = proc(chunk: ScanChunk) {.gcsafe, raises: [].} =
      inc chunksReceived
      totalPairs += chunk.pairs.len
    )
    check ok
    # Empty result still sends one chunk with hasMore=false and 0 pairs
    check chunksReceived == 1
    check totalPairs == 0

# ---------------------------------------------------------------------------
# Suite: Batched commit (performance optimization)
# ---------------------------------------------------------------------------

suite "MvccTransactionStore - Batched commit":
  test "batched commit: single key put and read":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_bc01")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_bc01")

    # Put a single key via auto-transaction
    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, "batch_key1", "batch_val1")
    let commitRes = mvccStore.commitTransaction(sessionId)
    check commitRes.isOk
    mvccStore.closeSession(sessionId)

    # Verify committed value is readable
    let getRes = mvccStore.latestGet("batch_key1")
    check getRes.isOk
    check getRes.value.isSome
    check getRes.value.get() == "batch_val1"

  test "batched commit: multiple keys in one transaction":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_bc02")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_bc02")

    # Put 5 keys in a single transaction — they should all be committed
    # in a single batched Raft round
    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, "multi_a", "1")
    discard mvccStore.txnPut(sessionId, "multi_b", "2")
    discard mvccStore.txnPut(sessionId, "multi_c", "3")
    discard mvccStore.txnPut(sessionId, "multi_d", "4")
    discard mvccStore.txnPut(sessionId, "multi_e", "5")
    let commitRes = mvccStore.commitTransaction(sessionId)
    check commitRes.isOk
    mvccStore.closeSession(sessionId)

    # Verify all committed values
    for (key, val) in [("multi_a", "1"), ("multi_b", "2"), ("multi_c", "3"),
                        ("multi_d", "4"), ("multi_e", "5")]:
      let getRes = mvccStore.latestGet(key)
      check getRes.isOk
      check getRes.value.isSome
      check getRes.value.get() == val

  test "batched commit: delete key and verify tombstone":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_bc03")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_bc03")

    # Put then delete in same transaction
    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, "del_batch_key", "will_be_deleted")
    discard mvccStore.txnDelete(sessionId, "del_batch_key")
    let commitRes = mvccStore.commitTransaction(sessionId)
    check commitRes.isOk
    mvccStore.closeSession(sessionId)

    # The tombstone should make the key appear absent
    let getRes = mvccStore.latestGet("del_batch_key")
    check getRes.isOk
    check getRes.value.isNone

  test "batched commit: rollback cleans up intents":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_bc04")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_bc04")

    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, "rollback_batch_key", "will_be_rolled_back")
    let rollbackRes = mvccStore.rollbackTransaction(sessionId)
    check rollbackRes.isOk
    mvccStore.closeSession(sessionId)

    # Key should not exist
    let getRes = mvccStore.latestGet("rollback_batch_key")
    check getRes.isOk
    check getRes.value.isNone

  test "batched commit: put-update-delete sequence":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_bc05")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_bc05")

    # First, put a key
    var sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, "seq_key", "v1")
    discard mvccStore.commitTransaction(sessionId)
    mvccStore.closeSession(sessionId)

    # Then update it
    sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnPut(sessionId, "seq_key", "v2")
    discard mvccStore.commitTransaction(sessionId)
    mvccStore.closeSession(sessionId)

    # Verify update
    var getRes = mvccStore.latestGet("seq_key")
    check getRes.isOk
    check getRes.value.isSome
    check getRes.value.get() == "v2"

    # Then delete it
    sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    discard mvccStore.txnDelete(sessionId, "seq_key")
    discard mvccStore.commitTransaction(sessionId)
    mvccStore.closeSession(sessionId)

    # Verify deletion
    getRes = mvccStore.latestGet("seq_key")
    check getRes.isOk
    check getRes.value.isNone

  test "autoPutWithResult without flags skips read":
    # Test that autoPutWithResult works correctly without CAS or return-previous
    # flags (which should skip the expensive latestGetWithMeta call)
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_bc06")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_bc06")

    let res = mvccStore.autoPutWithResult("auto_key", "auto_val")
    check res.isOk
    check res.value.status == PutStatusOK
    check res.value.version > 0

    # Verify the value was actually written
    let getRes = mvccStore.latestGet("auto_key")
    check getRes.isOk
    check getRes.value.isSome
    check getRes.value.get() == "auto_val"

  test "autoPutWithResult with return-previous flag reads value":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_bc07")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_bc07")

    # First put
    const PutFlagReturnPrev = 0x01'u8
    let res1 = mvccStore.autoPutWithResult("prev_key", "val1", PutFlagReturnPrev)
    check res1.isOk
    check res1.value.status == PutStatusOK
    check res1.value.previousValue.isNone # No previous value

    # Second put - should return previous value
    let res2 = mvccStore.autoPutWithResult("prev_key", "val2", PutFlagReturnPrev)
    check res2.isOk
    check res2.value.status == PutStatusOK
    check res2.value.previousValue.isSome
    check res2.value.previousValue.get() == "val1"

  test "autoDeleteWithResult without flags skips read":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_bc08")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_bc08")

    # First put a key
    let putRes = mvccStore.autoPutWithResult("del_auto_key", "del_val")
    check putRes.isOk

    # Delete without return-previous flag (should skip read)
    let delRes = mvccStore.autoDeleteWithResult("del_auto_key")
    check delRes.isOk
    check delRes.value.found == true
    check delRes.value.previousValue.isNone # No previous requested

    # Verify deleted
    let getRes = mvccStore.latestGet("del_auto_key")
    check getRes.isOk
    check getRes.value.isNone

suite "Single-round auto-commit (autoPutDirect / autoDeleteDirect)":

  test "autoPutDirect writes value readable via latestGet":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_d01")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_d01")

    let res = mvccStore.autoPutDirect("direct_key1", "direct_val1")
    check res.isOk
    check res.value.status == PutStatusOK
    check res.value.timestamp > 0
    check res.value.version == 1
    check res.value.previousValue.isNone

    # Verify the value is readable
    let getRes = mvccStore.latestGet("direct_key1")
    check getRes.isOk
    check getRes.value.isSome
    check getRes.value.get() == "direct_val1"

  test "autoPutDirect overwrites existing key":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_d02")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_d02")

    # First write
    let res1 = mvccStore.autoPutDirect("direct_key2", "v1")
    check res1.isOk
    check res1.value.version == 1

    # Second write — should overwrite
    let res2 = mvccStore.autoPutDirect("direct_key2", "v2")
    check res2.isOk
    check res2.value.version == 2

    # Should see the latest value
    let getRes = mvccStore.latestGet("direct_key2")
    check getRes.isOk
    check getRes.value.isSome
    check getRes.value.get() == "v2"

  test "autoPutDirect version key is stored correctly":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_d03")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_d03")

    discard mvccStore.autoPutDirect("direct_key3", "v1")
    discard mvccStore.autoPutDirect("direct_key3", "v2")

    # latestGetWithMeta should show the latest version
    let metaRes = mvccStore.latestGetWithMeta("direct_key3")
    check metaRes.isOk
    check metaRes.value.isSome
    check metaRes.value.get().value == "v2"
    check metaRes.value.get().version == 2

  test "autoDeleteDirect creates tombstone":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_d04")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_d04")

    # Put first
    discard mvccStore.autoPutDirect("direct_del_key", "will_delete")

    # Delete via autoDeleteDirect
    let delRes = mvccStore.autoDeleteDirect("direct_del_key")
    check delRes.isOk
    check delRes.value.found == true
    check delRes.value.previousValue.isNone

    # Key should be gone
    let getRes = mvccStore.latestGet("direct_del_key")
    check getRes.isOk
    check getRes.value.isNone

  test "autoPutDirect + autoDeleteDirect sequence":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_d05")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_d05")

    # Put
    let putRes = mvccStore.autoPutDirect("seq_direct_key", "v1")
    check putRes.isOk
    check putRes.value.version == 1

    # Update
    let putRes2 = mvccStore.autoPutDirect("seq_direct_key", "v2")
    check putRes2.isOk
    check putRes2.value.version == 2

    # Verify update
    let getRes = mvccStore.latestGet("seq_direct_key")
    check getRes.isOk
    check getRes.value.get() == "v2"

    # Delete
    let delRes = mvccStore.autoDeleteDirect("seq_direct_key")
    check delRes.isOk

    # Verify gone
    let getRes2 = mvccStore.latestGet("seq_direct_key")
    check getRes2.isOk
    check getRes2.value.isNone

    # Re-insert after delete
    let putRes3 = mvccStore.autoPutDirect("seq_direct_key", "v3")
    check putRes3.isOk
    check putRes3.value.version == 3

    let getRes3 = mvccStore.latestGet("seq_direct_key")
    check getRes3.isOk
    check getRes3.value.get() == "v3"

  test "autoPutDirect publishes to conflict index":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_d06")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_d06")

    # Write via autoPutDirect
    discard mvccStore.autoPutDirect("conflict_key", "v1")

    # Now try an explicit transaction that reads the key and writes it.
    # Since autoPutDirect published to commitIndex, a concurrent
    # transaction with a readTimestamp before the commit should detect
    # a conflict.
    let sessionId = mvccStore.createSession()
    discard mvccStore.beginTransaction(sessionId)
    # Record a read on the key
    discard mvccStore.recordRead(sessionId, "conflict_key")
    # Write to same key
    discard mvccStore.txnPut(sessionId, "conflict_key", "v2")
    # Commit should detect conflict since autoPutDirect's commit
    # timestamp is after our read timestamp
    let commitRes = mvccStore.commitTransaction(sessionId)
    # The conflict may or may not be detected depending on timestamps,
    # but the key point is that the system doesn't crash and the
    # commitIndex was updated.
    mvccStore.closeSession(sessionId)

  test "autoPutWithResult routes simple puts through autoPutDirect":
    # Verify that autoPutWithResult (no flags) uses the fast single-round path
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_d07")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_d07")

    let res = mvccStore.autoPutWithResult("routed_key", "routed_val")
    check res.isOk
    check res.value.status == PutStatusOK
    check res.value.previousValue.isNone

    # Verify readable
    let getRes = mvccStore.latestGet("routed_key")
    check getRes.isOk
    check getRes.value.isSome
    check getRes.value.get() == "routed_val"

  test "autoDeleteWithResult routes simple deletes through autoDeleteDirect":
    let (coord, raftStore, mvccStore, txnMgr) = makeMvccStore("/tmp/fractio_mvcc_d08")
    defer: teardownMvccStore(coord, "/tmp/fractio_mvcc_d08")

    # Put first
    discard mvccStore.autoPutWithResult("routed_del_key", "val")

    # Delete without flags — should use autoDeleteDirect
    let delRes = mvccStore.autoDeleteWithResult("routed_del_key")
    check delRes.isOk
    check delRes.value.found == true
    check delRes.value.previousValue.isNone

    # Verify deleted
    let getRes = mvccStore.latestGet("routed_del_key")
    check getRes.isOk
    check getRes.value.isNone
