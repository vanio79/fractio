# Micro-Transaction API for System Table Updates
#
# Provides atomic, multi-key mutations to system tables (sys.databases,
# sys.schemas, sys.tables, sys.groups, sys.nodes, sys.spaces, etc.)
# via a single Raft proposal.  A MicroTransaction bundles any number of
# puts and deletes into one WriteBatch that is committed to Raft as a
# single log entry, guaranteeing atomicity.
#
# Design:
#   - All mutations share the same MVCC timestamp, ensuring a consistent
#     snapshot for readers.
#   - All puts go to the META_GROUP_ID (group 1) because system tables
#     are replicated on every node.
#   - Cache updates for spaces/tables/groups are triggered automatically
#     by the applyBatchToSM callback when the Raft entry commits.
#   - On failure, none of the mutations are applied (unlike the previous
#     sysTablePutBatch which could partially commit).
#
# Usage:
#   let txn = store.beginSysTxn()
#   txn.put(encodeTableKey(SYS_NODES_TABLE_ID, "1"), encode(nodeRec))
#   txn.put(encodeTableKey(SYS_GROUPS_TABLE_ID, $groupId), encode(groupRec))
#   txn.delete(encodeTableKey(SYS_SPACES_TABLE_ID, oldSpaceKey))
#   let result = txn.commit()
#   if not result.isOk:
#     # Handle error — none of the writes were applied
#
# Thread safety:
#   MicroTransaction is NOT thread-safe. Each transaction should be used
#   by a single thread and committed promptly. After commit(), the object
#   should not be reused.

import std/strformat

import fractio/core/types
import fractio/core/errors
import fractio/distributed/raft/group_types
import fractio/distributed/raft/multigroup_types
import fractio/distributed/meta/system_tables
import fractio/storage/mvcc/types as mvccTypes
import fractio/protocol/raft_store
import ../utils/logging

# ---------------------------------------------------------------------------
# MicroTransaction Type
# ---------------------------------------------------------------------------

type
  SysTxnOpKind* = enum
    stokPut    ## Insert or update a system table row
    stokDelete ## Delete a system table row

  SysTxnOp* = object
    ## A single operation within a MicroTransaction.
    case kind*: SysTxnOpKind
    of stokPut:
      key*: string       ## Full key (e.g., /t/<tableId>/<primaryKey>)
      value*: string     ## Raw value (will be MVCC-encoded on commit)
    of stokDelete:
      deleteKey*: string ## Full key to delete

  SysTxnResult* = object
    ## Result of a MicroTransaction commit.
    case isOk*: bool
    of true:
      opsCommitted*: int  ## Number of operations committed
      timestampNs*: int64 ## Shared MVCC timestamp of the commit
    of false:
      error*: RaftStoreError

  MicroTransaction* = ref object
    ## A transactional bundle of system table mutations.
    ## All operations are committed atomically via a single Raft proposal.
    store*: RaftKVStoreExt ## The RaftKVStoreExt that owns this transaction
    ops*: seq[SysTxnOp] ## Pending operations
    committed*: bool ## True after commit() has been called

# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

proc beginSysTxn*(store: RaftKVStoreExt): MicroTransaction {.gcsafe, raises: [].} =
  ## Begin a new MicroTransaction for system table updates.
  ## The transaction accumulates puts and deletes, then commits them
  ## as a single Raft proposal for atomicity.
  new(result)
  result.store = store
  result.ops = @[]
  result.committed = false

# ---------------------------------------------------------------------------
# Mutation Operations
# ---------------------------------------------------------------------------

proc put*(txn: MicroTransaction, key, value: string) {.gcsafe, raises: [].} =
  ## Add a put operation to the transaction.
  ## The value will be MVCC-encoded on commit with the transaction's
  ## shared timestamp.
  if txn.committed:
    return
  txn.ops.add(SysTxnOp(kind: stokPut, key: key, value: value))

proc delete*(txn: MicroTransaction, key: string) {.gcsafe, raises: [].} =
  ## Add a delete operation to the transaction.
  ## The key will be deleted from the backend on commit.
  if txn.committed:
    return
  txn.ops.add(SysTxnOp(kind: stokDelete, deleteKey: key))

# ---------------------------------------------------------------------------
# Convenience: System Table Key Helpers
# ---------------------------------------------------------------------------

proc putSysRow*(txn: MicroTransaction, tableId: TableId, primaryKey: string,
    value: string) {.gcsafe, raises: [].} =
  ## Add a put for a system table row using the standard key encoding.
  ## Equivalent to: txn.put(encodeTableKey(tableId, primaryKey), value)
  txn.put(encodeTableKey(tableId, primaryKey), value)

proc deleteSysRow*(txn: MicroTransaction, tableId: TableId,
    primaryKey: string) {.gcsafe, raises: [].} =
  ## Add a delete for a system table row using the standard key encoding.
  ## Equivalent to: txn.delete(encodeTableKey(tableId, primaryKey))
  txn.delete(encodeTableKey(tableId, primaryKey))

# ---------------------------------------------------------------------------
# Commit
# ---------------------------------------------------------------------------

proc commit*(txn: MicroTransaction): SysTxnResult {.gcsafe, raises: [].} =
  ## Commit all pending operations as a single Raft proposal.
  ##
  ## All puts are MVCC-encoded with the same timestamp for consistency.
  ## All operations are bundled into one WriteBatch, so they are either
  ## all committed or all rejected.
  ##
  ## Returns SysTxnResult with:
  ##   - On success: opsCommitted count and shared timestamp
  ##   - On failure: RaftStoreError (e.g., not leader, group not found)
  if txn.committed:
    return SysTxnResult(isOk: false,
      error: newRSE(rseInternal, "MicroTransaction already committed"))

  if txn.ops.len == 0:
    txn.committed = true
    return SysTxnResult(isOk: true, opsCommitted: 0,
      timestampNs: txn.store.nowNs())

  # Get a single shared timestamp for all operations in this transaction
  let ts = txn.store.nowNs()

  # Build a single WriteBatch containing all puts and deletes
  let batch = newWriteBatch()
  for op in txn.ops:
    case op.kind
    of stokPut:
      # MVCC-encode all puts with the same timestamp
      let encoded = mvccTypes.encodeMVCCValue(op.value, ts, false)
      batch.put(toBytes(op.key), toBytes(encoded))
    of stokDelete:
      batch.delete(toBytes(op.deleteKey))

  # Propose the entire batch as a single Raft entry via the meta group
  let vr = txn.store.proposeSysBatch(batch)

  txn.committed = true

  if not vr.isOk:
    return SysTxnResult(isOk: false, error: vr.error)

  return SysTxnResult(isOk: true, opsCommitted: txn.ops.len,
    timestampNs: ts)

# ---------------------------------------------------------------------------
# Convenience: One-shot Operations
# ---------------------------------------------------------------------------

proc sysTxnPut*(store: RaftKVStoreExt, key, value: string): SysTxnResult {.
    gcsafe, raises: [].} =
  ## Single-key put via MicroTransaction.
  ## Equivalent to beginSysTxn + put + commit, but more efficient
  ## than the old sysTablePut because it goes through the transaction path.
  let txn = store.beginSysTxn()
  txn.put(key, value)
  txn.commit()

proc sysTxnPutBatch*(store: RaftKVStoreExt,
    writes: openArray[tuple[key: string, value: string]]): SysTxnResult {.
    gcsafe, raises: [].} =
  ## Multi-key put via MicroTransaction.
  ## All writes share the same timestamp and are committed atomically.
  let txn = store.beginSysTxn()
  for (key, value) in writes:
    txn.put(key, value)
  txn.commit()

proc sysTxnDelete*(store: RaftKVStoreExt, key: string): SysTxnResult {.
    gcsafe, raises: [].} =
  ## Single-key delete via MicroTransaction.
  let txn = store.beginSysTxn()
  txn.delete(key)
  txn.commit()

proc sysTxnDeleteBatch*(store: RaftKVStoreExt,
    keys: openArray[string]): SysTxnResult {.gcsafe, raises: [].} =
  ## Multi-key delete via MicroTransaction.
  let txn = store.beginSysTxn()
  for key in keys:
    txn.delete(key)
  txn.commit()

proc sysTxnPutAndDelete*(store: RaftKVStoreExt,
    puts: openArray[tuple[key: string, value: string]],
    deletes: openArray[string]): SysTxnResult {.gcsafe, raises: [].} =
  ## Mixed put+delete via MicroTransaction.
  ## All operations are committed atomically.
  let txn = store.beginSysTxn()
  for (key, value) in puts:
    txn.put(key, value)
  for key in deletes:
    txn.delete(key)
  txn.commit()

# ---------------------------------------------------------------------------
# Backward-Compatible Wrappers
# ---------------------------------------------------------------------------
# These procs replace the old sysTablePut/sysTableDelete family.
# They delegate to MicroTransaction internally, so callers that use the
# old API automatically get atomic semantics.

proc sysTablePutV2*(store: RaftKVStoreExt, key, value: string): bool {.
    gcsafe, raises: [].} =
  ## Write to a sys table with MVCC encoding via MicroTransaction.
  ## Returns true on success, false on failure.
  let result = store.sysTxnPut(key, value)
  result.isOk

proc sysTablePutBatchV2*(store: RaftKVStoreExt,
    writes: openArray[tuple[key: string, value: string]]): bool {.
    gcsafe, raises: [].} =
  ## Write multiple sys table entries atomically with MVCC encoding.
  ## All entries get the same timestamp for atomicity.
  ## Returns true on success, false on failure.
  let result = store.sysTxnPutBatch(writes)
  result.isOk

proc sysTableDeleteV2*(store: RaftKVStoreExt, key: string): bool {.
    gcsafe, raises: [].} =
  ## Delete from a sys table via MicroTransaction.
  ## Returns true on success, false on failure.
  let result = store.sysTxnDelete(key)
  result.isOk

proc sysTableDeleteBatchV2*(store: RaftKVStoreExt,
    keys: openArray[string]): bool {.gcsafe, raises: [].} =
  ## Delete multiple sys table entries atomically via MicroTransaction.
  ## Returns true on success, false on failure.
  let result = store.sysTxnDeleteBatch(keys)
  result.isOk

proc sysTablePutAndDeleteBatchV2*(store: RaftKVStoreExt,
    puts: openArray[tuple[key: string, value: string]],
    deletes: openArray[string]): bool {.gcsafe, raises: [].} =
  ## Write and delete sys table entries atomically via MicroTransaction.
  ## Returns true on success, false on failure.
  let result = store.sysTxnPutAndDelete(puts, deletes)
  result.isOk
