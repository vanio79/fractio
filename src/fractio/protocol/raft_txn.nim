# Raft-backed 2PC transaction coordinator — Phase 5.
#
# This module implements the **cross-shard two-phase commit (2PC) protocol**
# for transactions that touch keys in more than one shard range.
#
# Single-shard transactions are committed directly through the shard's
# RaftKVStoreExt (via raftResolveIntent) — no coordinator needed.
#
# Multi-shard transactions follow this protocol:
#
#   Phase 1 — Write coordinator record (durability checkpoint):
#     coordinator writes a COORD record to its own Raft log BEFORE Phase 2.
#     This ensures recovery: on restart, any COORD record with state=PREPARED
#     means Phase 2 must be retried.
#
#   Phase 2a — Prepare:
#     For each involved shard, the coordinator resolves all write-intents
#     for the transaction tentatively (marks them as PREPARED in the SM).
#     In the current single-node implementation "prepare" just validates that
#     the intent still exists and there are no conflicting committed writes.
#
#   Phase 2b — Commit or Abort:
#     On all-OK: commit each intent (raftResolveIntent commit=true) and delete
#     the COORD record.
#     On any failure: abort each shard (raftResolveIntent commit=false) and
#     delete the COORD record.
#
# Thread safety:
#   All exported procs are {.gcsafe, raises: [].}.
#
# Limitations (Phase 5 scope):
#   - No network RPC to remote shard leaders yet; all shards are local.
#   - Recovery scanning happens at startup (see recoverPendingCoords).
#   - Distributed prepare vote ("PREPARED / ABORT") is simulated locally.

import std/[tables, sets, locks, times, atomics, strformat, strutils, options]
import ./raft_store
import ./txn_manager
import ./messages/txn as txnMsgs
import fractio/distributed/raft/state_machine

# ---------------------------------------------------------------------------
# 2PC Coordinator record wire format (stored as plain string in COORD key)
# ---------------------------------------------------------------------------
# Format: "STATE:txnId:commitTs:shard1,shard2,...:key1,key2,..."
# STATE = "PREPARED" or "COMMITTING" or "ABORTING"

const
  CoordStatePrepared* = "PREPARED"
  CoordStateCommitting* = "COMMITTING"
  CoordStateAborting* = "ABORTING"

proc encodeCoordRecord*(txnId: uint64, state: string, commitTs: uint64,
    shardKeys: seq[string]): string {.inline.} =
  var keys = ""
  for i, k in shardKeys:
    if i > 0: keys.add(",")
    keys.add(k)
  &"{state}:{txnId}:{commitTs}:{keys}"

proc decodeCoordRecord*(data: string): tuple[state: string, txnId: uint64,
    commitTs: uint64, keys: seq[string]] {.raises: [].} =
  try:
    let parts = data.split(':')
    if parts.len < 4:
      return ("", 0'u64, 0'u64, @[])
    let keyPart = parts[3]
    var keys: seq[string] = @[]
    if keyPart.len > 0:
      keys = keyPart.split(',')
    return (parts[0], parseBiggestUInt(parts[1]), parseBiggestUInt(parts[2]), keys)
  except CatchableError:
    return ("", 0'u64, 0'u64, @[])

# ---------------------------------------------------------------------------
# RaftTxnCoordinator
# ---------------------------------------------------------------------------

type
  RaftTxnCoordinator* = ref object
    store*: RaftKVStoreExt
    txnMgr*: TransactionManager
    mu*: Lock

proc newRaftTxnCoordinator*(store: RaftKVStoreExt,
    mgr: TransactionManager): RaftTxnCoordinator =
  result = RaftTxnCoordinator(store: store, txnMgr: mgr)
  initLock(result.mu)

# ---------------------------------------------------------------------------
# Single-shard commit: resolve all write-set intents directly
# ---------------------------------------------------------------------------

proc commitSingleShard*(coord: RaftTxnCoordinator, txnId: uint64,
    writeSet: HashSet[string],
    commitTs: uint64): bool {.gcsafe, raises: [].} =
  ## Commit a single-shard transaction by resolving all write-intents.
  ## Returns true on success.
  ##
  ## For each key in the write set we read the intent value (stored under
  ## the intent key by raftPutIntent during the Put handler) and move it to
  ## the committed key via raftResolveIntent.
  for key in writeSet:
    # Read the current intent value from the state machine.
    # Call getOrCreateSM BEFORE acquiring smMu — getOrCreateSM takes smMu
    # internally; acquiring smMu first would deadlock (Lock is non-reentrant).
    var intentValue = ""
    let intentKey = encodeIntentKey(txnId, key)
    let ridOpt = coord.store.resolveRangeId(key)
    if ridOpt.isSome:
      let sm = coord.store.getOrCreateSM(ridOpt.get()) # acquires+releases smMu
      acquire(coord.store.smMu)
      intentValue = sm.kvStore.getOrDefault(intentKey)
      release(coord.store.smMu)

    let vr = coord.store.raftResolveIntent(txnId, key, true, intentValue)
    if not vr.isOk:
      return false
  true

proc rollbackSingleShard*(coord: RaftTxnCoordinator, txnId: uint64,
    writeSet: HashSet[string]): bool {.gcsafe, raises: [].} =
  ## Rollback a single-shard transaction by deleting all write-intents.
  for key in writeSet:
    let vr = coord.store.raftDeleteIntent(txnId, key)
    if not vr.isOk:
      discard # Best-effort: continue even if one intent delete fails
  true

# ---------------------------------------------------------------------------
# Cross-shard 2PC commit
# ---------------------------------------------------------------------------

proc coordinateCrossShardCommit*(coord: RaftTxnCoordinator, txnId: uint64,
    writeSet: HashSet[string],
    commitTs: uint64): CommitTxnResponse {.gcsafe, raises: [].} =
  ## Execute 2PC for a transaction whose write-set spans multiple shards.
  ##
  ## Step 1: Write COORD record (durability before Phase 2 begins).
  ## Step 2: Validate all write-intents (Phase 1 - Prepare).
  ## Step 3: Commit or abort based on prepare outcome.

  # --- Collect all keys as a seq for the record ---
  var keySeq: seq[string] = @[]
  for k in writeSet: keySeq.add(k)

  # --- Step 1: Write COORD record (PREPARED state) ---
  let coordData = encodeCoordRecord(txnId, CoordStatePrepared, commitTs, keySeq)
  let crW = coord.store.raftWriteCoordRecord(txnId, coordData)
  if not crW.isOk:
    return CommitTxnResponse(status: TxnCommitConflict, commitTimestamp: 0)

  # --- Step 2: Prepare each shard (validate intents) ---
  # IMPORTANT: call getOrCreateSM before acquiring smMu — getOrCreateSM takes
  # smMu internally; holding smMu first would cause a deadlock.
  var prepareOK = true
  for key in writeSet:
    # Validate the intent still exists (not expired / already resolved)
    let intentKey = encodeIntentKey(txnId, key)
    let ridOpt = coord.store.resolveRangeId(key)
    var intentExists = false
    if ridOpt.isSome:
      let sm = coord.store.getOrCreateSM(ridOpt.get()) # acquires+releases smMu
      acquire(coord.store.smMu)
      intentExists = sm.kvStore.hasKey(intentKey)
      release(coord.store.smMu)

    if not intentExists:
      # Intent missing — another transaction may have aborted ours
      prepareOK = false
      break

  # --- Step 3a: Commit ---
  if prepareOK:
    # Update COORD record to COMMITTING before Phase 2
    let commitData = encodeCoordRecord(txnId, CoordStateCommitting, commitTs,
        keySeq)
    discard coord.store.raftWriteCoordRecord(txnId, commitData)

    # Pipelined commit: all shard proposals dispatched simultaneously so their
    # fsyncs overlap instead of serialising.  For a 2-shard transaction this
    # halves the wall-clock commit latency vs. the old sequential loop.
    var writeSeq: seq[string] = @[]
    for k in writeSet: writeSeq.add(k)
    let vr = coord.store.raftCommitTxnPipelined(txnId, writeSeq)
    let allCommitted = vr.isOk

    # Clean up COORD record on success
    if allCommitted:
      discard coord.store.raftDeleteCoordRecord(txnId)
      return CommitTxnResponse(status: TxnCommitOK, commitTimestamp: commitTs)

  # --- Step 3b: Abort ---
  let abortData = encodeCoordRecord(txnId, CoordStateAborting, 0'u64, keySeq)
  discard coord.store.raftWriteCoordRecord(txnId, abortData)

  for key in writeSet:
    discard coord.store.raftDeleteIntent(txnId, key)

  discard coord.store.raftDeleteCoordRecord(txnId)
  CommitTxnResponse(status: TxnCommitConflict, commitTimestamp: 0)

# ---------------------------------------------------------------------------
# Recovery: re-drive any COORD records found on startup
# ---------------------------------------------------------------------------

proc recoverPendingCoords*(coord: RaftTxnCoordinator) {.gcsafe, raises: [].} =
  ## Scan the Raft state machine for any outstanding COORD records and
  ## re-drive them to completion.  Call once at server startup.
  ##
  ## IMPORTANT: getOrCreateSM acquires smMu internally.  We must NOT hold smMu
  ## when calling it — collect state machines first, then scan under smMu.
  var pending: seq[(uint64, string)] = @[]

  # Scan all state machines for pending COORD records
  acquire(coord.store.smMu)
  for rid, sm in coord.store.stateMachines:
    for k, v in sm.kvStore:
      if isCoordKey(k):
        let txnId = block:
          var r = 0'u64
          let off = COORD_PREFIX.len
          for i in 0 ..< 8:
            r = (r shl 8) or uint64(uint8(k[off + i]))
          r
        pending.add((txnId, v))
  release(coord.store.smMu)

  for (txnId, data) in pending:
    let (state, _, commitTs, keys) = decodeCoordRecord(data)
    var writeSet = initHashSet[string]()
    for k in keys: writeSet.incl(k)

    case state
    of CoordStateCommitting:
      # Restart commit — intents may already be resolved, that's fine.
      # Use pipelined commit so recovery of multi-shard txns is also fast.
      var writeSeqR: seq[string] = @[]
      for k in writeSet: writeSeqR.add(k)
      discard coord.store.raftCommitTxnPipelined(txnId, writeSeqR)
      discard coord.store.raftDeleteCoordRecord(txnId)

    of CoordStatePrepared, CoordStateAborting:
      # Abort — safe to delete all intents
      for key in writeSet:
        discard coord.store.raftDeleteIntent(txnId, key)
      discard coord.store.raftDeleteCoordRecord(txnId)
    else:
      # Unknown state — abort as safe default
      for key in writeSet:
        discard coord.store.raftDeleteIntent(txnId, key)
      discard coord.store.raftDeleteCoordRecord(txnId)
