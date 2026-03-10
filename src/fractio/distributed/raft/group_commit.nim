# Group Commit Batcher for Raft write path
#
# Motivation
# ----------
# Without group commit, every `proposeAndWait` call appends one log entry and
# triggers one `fdatasync()` via the WiscKey backend (syncWrites=true).  This
# limits single-node throughput to ~24 writes/sec — the cost of one fsync per
# commit.
#
# With group commit, many concurrent callers deposit their proposals into a
# shared pending queue.  A single flush goroutine wakes up every `maxDelayNs`
# nanoseconds (or when `maxBatchSize` proposals have accumulated), merges ALL
# pending WriteBatches into ONE combined WriteBatch, appends ONE log entry,
# calls `fdatasync()` exactly once, then signals every waiting caller.
#
# Expected improvement: from ~24 ops/sec to 500–5000 ops/sec depending on
# concurrency, matching or exceeding MySQL/PostgreSQL with group commit enabled.
#
# Design
# ------
#   GroupCommitBatcher
#     pendingCh: Channel[GroupCommitItem]   — callers enqueue here
#     flushThread: Thread[ptr GroupCommitBatcher]
#     running: Atomic[bool]
#     maxBatchSize: int   (default 256)
#     maxDelayNs: int64   (default 2_000_000 = 2 ms)
#
# The flush loop:
#   1. Block-recv first item from pendingCh (to avoid busy-spin when idle).
#   2. tryRecv more items until maxBatchSize or pendingCh is empty or
#      time-since-first-item >= maxDelayNs.
#   3. Merge all WriteBatches into one combined batch.
#   4. Append ONE log entry + apply via the coordinator callbacks.
#   5. Send RaftResult to each item's resultPtr.
#
# Thread safety
# -------------
# `pendingCh` is a buffered Nim Channel — thread-safe by design.
# The flush thread is the sole writer of log entries for a given GroupID
# (within the single-node path).  GroupCommitBatcher is per-GroupID when
# multiple shards exist; the coordinator holds one batcher per group.
#
# Nim 2.2.8 notes
# ---------------
# - `ptr GroupCommitBatcher` is passed to the flush thread to avoid ORC
#   cross-thread ref-counting cycles (same pattern as ProposalResultChannel).
# - The flush thread proc is {.thread.} and captures nothing from the outer
#   scope.

import std/atomics
import std/times
import std/typedthreads

import fractio/distributed/raft/group_types
import fractio/distributed/raft/multigroup_types

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  GC_DEFAULT_MAX_BATCH_SIZE* = 256
  GC_DEFAULT_MAX_DELAY_NS* = 2_000_000'i64 ## 2 ms
  GC_PENDING_CHANNEL_CAP* = 65536

# ---------------------------------------------------------------------------
# Per-proposal item enqueued by callers
# ---------------------------------------------------------------------------

type
  GroupCommitItem* = object
    ## One proposal deposited by a calling thread.
    groupId*: GroupID
    command*: RaftCommand
    resultPtr*: ptr ProposalResultChannel ## raw ptr — caller owns lifetime

# ---------------------------------------------------------------------------
# Coordinator callback vtable (set by coordinator after construction)
# ---------------------------------------------------------------------------
#
# The batcher must append a log entry and apply it without importing
# multigroup_coordinator (circular import).  Instead the coordinator
# injects function pointers after creating the batcher.

type
  GCAppendAndApplyFn* = proc(
    groupId: GroupID,
    command: RaftCommand,
    resultPtr: ptr ProposalResultChannel,
  ) {.gcsafe, raises: [].}
  ## Called by the flush thread for each coalesced batch.
  ## The coordinator implements this: append ONE entry, commit, apply,
  ## then send result to resultPtr.

  GCFlushBatchFn* = proc(
    groupId: GroupID,
    batch: WriteBatch,
    items: seq[ptr ProposalResultChannel],
  ) {.gcsafe, raises: [].}
  ## Alternative: coordinator merges and applies one batch, signals all waiters.

# ---------------------------------------------------------------------------
# GroupCommitBatcher
# ---------------------------------------------------------------------------

type
  GroupCommitBatcher* = object
    ## Batches concurrent write proposals into a single log entry + fsync.
    pendingCh*: Channel[GroupCommitItem]
    running*: Atomic[bool]
    maxBatchSize*: int
    maxDelayNs*: int64
    ## Injected by the coordinator after construction.
    flushFn*: GCFlushBatchFn
    ## Background flush thread.
    flushThread*: Thread[ptr GroupCommitBatcher]

# ---------------------------------------------------------------------------
# Construction / Destruction
# ---------------------------------------------------------------------------

proc initGroupCommitBatcher*(b: ptr GroupCommitBatcher,
    maxBatchSize: int = GC_DEFAULT_MAX_BATCH_SIZE,
    maxDelayNs: int64 = GC_DEFAULT_MAX_DELAY_NS) =
  ## Initialise all fields in-place.  `b` must point to a zero-initialised
  ## GroupCommitBatcher (e.g. allocated with `allocShared0`).
  b[].maxBatchSize = maxBatchSize
  b[].maxDelayNs = maxDelayNs
  b[].running.store(false)
  b[].pendingCh.open(GC_PENDING_CHANNEL_CAP)

proc deinitGroupCommitBatcher*(b: ptr GroupCommitBatcher) =
  ## Close channel.  Must be called after stop().
  b[].pendingCh.close()

# ---------------------------------------------------------------------------
# Flush thread
# ---------------------------------------------------------------------------

proc flushProc(bPtr: ptr GroupCommitBatcher) {.thread.} =
  ## Background flush loop.
  ## Drains pendingCh, merges WriteBatches per GroupID, calls flushFn once
  ## per GroupID per batch window.
  let b = bPtr

  while b[].running.load():
    # Block-recv first item so we don't busy-spin when idle.
    # stopBatcher() sends a sentinel (groupId == 0) to unblock this recv().
    let first = b[].pendingCh.recv()
    if first.groupId.uint64 == 0:
      break # Shutdown sentinel

    # Collect items for this batch window.
    # We coalesce ALL pending items (regardless of GroupID) into one pass.
    var items: seq[GroupCommitItem] = @[first]
    let windowStart = block:
      let t = getTime()
      t.toUnix * 1_000_000_000 + t.nanosecond.int64

    while items.len < b[].maxBatchSize:
      let elapsed = block:
        let t = getTime()
        (t.toUnix * 1_000_000_000 + t.nanosecond.int64) - windowStart
      if elapsed >= b[].maxDelayNs:
        break
      let (ok, item) = b[].pendingCh.tryRecv()
      if not ok: break
      items.add(item)

    # Drain any remaining items that arrived during the window
    # (up to maxBatchSize total) without waiting further.
    while items.len < b[].maxBatchSize:
      let (ok, item) = b[].pendingCh.tryRecv()
      if not ok: break
      items.add(item)

    # Group by GroupID and merge WriteBatches.
    # For simplicity (single-shard common case) we use a small seq scan.
    # With many shards this would be a Table; single shard is the benchmark case.
    type BatchGroup = object
      groupId: GroupID
      combined: WriteBatch
      resultPtrs: seq[ptr ProposalResultChannel]

    var groups: seq[BatchGroup] = @[]

    for item in items:
      # Find existing group for this groupId
      var found = false
      for i in 0 ..< groups.len:
        if groups[i].groupId == item.groupId:
          # Merge into existing combined batch
          if item.command.kind == ckWrite:
            let wb = item.command.writeBatch
            if wb != nil:
              for (k, v) in wb.puts:
                groups[i].combined.puts.add((k, v))
              for k in wb.deletes:
                groups[i].combined.deletes.add(k)
          groups[i].resultPtrs.add(item.resultPtr)
          found = true
          break
      if not found:
        # New batch group
        let combined = newWriteBatch()
        if item.command.kind == ckWrite:
          let wb = item.command.writeBatch
          if wb != nil:
            for (k, v) in wb.puts:
              combined.puts.add((k, v))
            for k in wb.deletes:
              combined.deletes.add(k)
        groups.add(BatchGroup(
          groupId: item.groupId,
          combined: combined,
          resultPtrs: @[item.resultPtr],
        ))

    # Flush each batch group via the injected callback.
    if b[].flushFn != nil:
      for grp in groups:
        {.cast(gcsafe).}:
          b[].flushFn(grp.groupId, grp.combined, grp.resultPtrs)
    else:
      # flushFn not wired — signal all callers with error
      for grp in groups:
        for rptr in grp.resultPtrs:
          if rptr != nil:
            rptr[].ch.send(RaftResult(
              success: false,
              error: "GroupCommitBatcher: flushFn not configured"))

proc startBatcher*(b: ptr GroupCommitBatcher) =
  ## Spawn the flush background thread.
  ## compareExchange guarantees only one caller starts the thread even under
  ## concurrent invocations (start-once semantics).
  var expected = false
  if not b[].running.compareExchange(expected, true):
    return # already running
  createThread(b[].flushThread, flushProc, b)

proc stopBatcher*(b: ptr GroupCommitBatcher) =
  ## Signal stop and join the flush thread.
  ## compareExchange guarantees only one caller joins the thread even under
  ## concurrent invocations (stop-once semantics).
  var expected = true
  if not b[].running.compareExchange(expected, false):
    return # already stopped or never started
  # Send shutdown sentinel (groupId == 0) to unblock the blocking recv()
  # in flushProc. resultPtr is nil since no one waits for this result.
  b[].pendingCh.send(GroupCommitItem(
    groupId: GroupID(0),
    command: RaftCommand(kind: ckNoop),
    resultPtr: nil,
  ))
  joinThread(b[].flushThread)

# ---------------------------------------------------------------------------
# Caller-side enqueue
# ---------------------------------------------------------------------------

proc enqueue*(b: ptr GroupCommitBatcher, groupId: GroupID,
    command: RaftCommand,
    resultPtr: ptr ProposalResultChannel) {.gcsafe, raises: [].} =
  ## Deposit a proposal into the batcher.  Returns immediately; the caller
  ## blocks on resultPtr[].ch.recv() to wait for the commit result.
  b[].pendingCh.send(GroupCommitItem(
    groupId: groupId,
    command: command,
    resultPtr: resultPtr,
  ))
