# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Worker Pool
##
## Manages background worker threads for flushing and compaction.
## Also handles journal maintenance through JournalManager.

import fractio/storage/[error, snapshot_tracker, stats, supervisor,
                        write_buffer_manager, flush/manager, journal/manager]
import fractio/storage/flush/worker as flush_worker
import fractio/storage/compaction/worker as compaction_worker
import fractio/storage/logging
import fractio/storage/keyspace as ks
import fractio/storage/lsm_tree/lsm_tree
import std/[atomics, locks, typedthreads, times, tables, os, options]

# L0 compaction trigger threshold - when L0 table count exceeds this, trigger compaction
const L0_COMPACTION_TRIGGER* = 4

type
  WorkerMessageKind* = enum
    wmFlush
    wmCompact
    wmClose
    wmRotateMemtable

  WorkerMessage* = object
    ## Message sent to worker threads
    case kind*: WorkerMessageKind
    of wmCompact:
      keyspaceId*: uint64 # Use ID instead of ref to avoid cycles
    of wmRotateMemtable:
      keyspaceId2*: uint64
      memtableId*: uint64
    else:
      discard

  WorkerThreadArgs* = object
    ## Arguments passed to worker thread
    poolPtr*: pointer
    workerId*: int
    poolSize*: int

  WorkerPool* = ref object
    threadHandles*: seq[Thread[WorkerThreadArgs]]
    running*: Atomic[bool]
    queue*: seq[WorkerMessage]
    queueLock*: Lock
    flushManager*: FlushManager
    writeBufferSize*: WriteBufferManager
    snapshotTracker*: SnapshotTracker
    stats*: ptr Stats
    poolSize*: int
    keyspaces*: ptr Table[string, ks.Keyspace] # Pointer to keyspaces table
    keyspacesLock*: ptr Lock
    # JournalManager for maintenance - stored directly to avoid Supervisor reference cycle
    journalManager*: JournalManager
    journalManagerLock*: ptr Lock

# Debug representation
proc `$`*(msg: WorkerMessage): string =
  case msg.kind
  of wmFlush:
    "WorkerMessage:Flush"
  of wmCompact:
    "WorkerMessage:Compact(" & $msg.keyspaceId & ")"
  of wmClose:
    "WorkerMessage:Close"
  of wmRotateMemtable:
    "WorkerMessage:Rotate"

# Find keyspace by ID
proc findKeyspace(pool: WorkerPool, id: uint64): ks.Keyspace =
  if pool.keyspaces == nil or pool.keyspacesLock == nil:
    return nil
  pool.keyspacesLock[].acquire()
  defer: pool.keyspacesLock[].release()
  for name, ks in pool.keyspaces[].pairs:
    if ks.inner.id == id:
      return ks
  return nil

# Run journal maintenance - cleans up sealed journals that have been flushed
proc runJournalMaintenance(pool: WorkerPool) {.gcsafe.} =
  if pool.journalManager == nil or pool.journalManagerLock == nil:
    return

  pool.journalManagerLock[].acquire()
  try:
    let jm = pool.journalManager

    # Try to evict journals in a loop
    while jm.sealedJournalCount() > 0:
      # Get watermarks for oldest journal
      let watermarks = jm.getOldestJournalWatermarks()
      if watermarks.len == 0:
        break

      # Check if all keyspaces have been flushed enough
      var canEvict = true

      for watermark in watermarks:
        # Find the keyspace by ID
        let keyspace = pool.findKeyspace(watermark.keyspaceId)
        if keyspace != nil and keyspace.inner != nil:
          # Check if keyspace is deleted
          if not keyspace.inner.isDeleted.load(moAcquire):
            # Check if persisted seqno is high enough
            let persistedSeqno = keyspace.inner.tree.getHighestPersistedSeqno()
            if persistedSeqno.isNone:
              canEvict = false
              break
            if persistedSeqno.get() < watermark.lsn:
              canEvict = false
              break
        # If keyspace is nil (deleted), we can evict

      if not canEvict:
        break

      # Evict the oldest journal
      let evictResult = jm.evictOldestJournal()
      if evictResult.isErr:
        logError("Journal maintenance failed: " & $evictResult.error)
        break
      else:
        logInfo("Evicted sealed journal")
  finally:
    pool.journalManagerLock[].release()

# Worker thread proc
proc workerProc(args: WorkerThreadArgs) {.thread.} =
  let pool = cast[WorkerPool](args.poolPtr)
  let workerId = args.workerId

  logInfo("Worker #" & $workerId & " started")

  while pool.running.load(moRelaxed):
    # Get message from queue
    pool.queueLock.acquire()
    var msg: WorkerMessage
    if pool.queue.len > 0:
      msg = pool.queue[0]
      pool.queue.delete(0)
      pool.queueLock.release()
    else:
      pool.queueLock.release()
      # Sleep briefly if queue is empty
      sleep(10)
      continue

    case msg.kind
    of wmClose:
      break

    of wmFlush:
      # Dequeue flush task from flush manager
      if pool.flushManager != nil:
        let task = pool.flushManager.dequeue()
        if task != nil and task.keyspacePtr != nil:
          let keyspace = cast[ks.Keyspace](task.keyspacePtr)
          if keyspace != nil:
            var statsVal = pool.stats[]
            let flushResult = flush_worker.run(keyspace, pool.writeBufferSize,
                                                pool.snapshotTracker, statsVal)
            pool.stats[] = statsVal
            if flushResult.isOk:
              # Run journal maintenance after flush
              pool.runJournalMaintenance()

              # After successful flush, check L0 threshold and request compaction if needed
              let l0Count = keyspace.l0TableCount()
              if l0Count >= L0_COMPACTION_TRIGGER:
                logInfo("L0 table count (" & $l0Count & ") >= threshold (" &
                        $L0_COMPACTION_TRIGGER & "), triggering compaction")
                pool.queueLock.acquire()
                pool.queue.add(WorkerMessage(kind: wmCompact,
                    keyspaceId: keyspace.inner.id))
                pool.queueLock.release()

    of wmCompact:
      let keyspace = pool.findKeyspace(msg.keyspaceId)
      if keyspace != nil:
        var statsVal = pool.stats[]
        discard compaction_worker.run(keyspace, pool.snapshotTracker, statsVal)
        pool.stats[] = statsVal

    of wmRotateMemtable:
      let keyspace = pool.findKeyspace(msg.keyspaceId2)
      if keyspace != nil:
        # Rotate the memtable
        let rotateResult = ks.rotateMemtable(keyspace)
        if rotateResult.isOk and rotateResult.value:
          # Memtable was rotated, enqueue a flush task
          if pool.flushManager != nil:
            let task = Task(keyspacePtr: cast[pointer](keyspace))
            pool.flushManager.enqueue(task)
            logInfo("Enqueued flush task for keyspace " & keyspace.inner.name)

  logInfo("Worker #" & $workerId & " stopped")

# Create a new worker pool
proc newWorkerPool*(poolSize: int = 4): WorkerPool =
  result = WorkerPool(
    threadHandles: newSeq[Thread[WorkerThreadArgs]](),
    queue: @[],
    poolSize: poolSize,
    keyspaces: nil,
    keyspacesLock: nil
  )
  result.running.store(false, moRelaxed)
  initLock(result.queueLock)

# Request flush
proc requestFlush*(pool: WorkerPool) =
  pool.queueLock.acquire()
  pool.queue.add(WorkerMessage(kind: wmFlush))
  pool.queueLock.release()

# Request flush for a specific keyspace
proc requestFlush*(pool: WorkerPool, keyspace: ks.Keyspace) =
  # Enqueue a flush task directly to the flush manager
  if pool.flushManager != nil:
    let task = Task(keyspacePtr: cast[pointer](keyspace))
    pool.flushManager.enqueue(task)

# Request compaction by keyspace
proc requestCompaction*(pool: WorkerPool, keyspace: ks.Keyspace) =
  pool.queueLock.acquire()
  pool.queue.add(WorkerMessage(kind: wmCompact, keyspaceId: keyspace.inner.id))
  pool.queueLock.release()

# Request memtable rotation for a keyspace
proc requestMemtableRotation*(pool: WorkerPool, keyspace: ks.Keyspace) =
  pool.queueLock.acquire()
  pool.queue.add(WorkerMessage(kind: wmRotateMemtable,
                               keyspaceId2: keyspace.inner.id,
                               memtableId: 0))
  pool.queueLock.release()

# Start the worker pool
proc start*(pool: WorkerPool, flushManager: FlushManager,
            writeBufferSize: WriteBufferManager,
            snapshotTracker: SnapshotTracker, stats: ptr Stats,
            keyspaces: ptr Table[string, ks.Keyspace],
            keyspacesLock: ptr Lock,
            journalManager: JournalManager,
            journalManagerLock: ptr Lock) =
  pool.flushManager = flushManager
  pool.writeBufferSize = writeBufferSize
  pool.snapshotTracker = snapshotTracker
  pool.stats = stats
  pool.keyspaces = keyspaces
  pool.keyspacesLock = keyspacesLock
  pool.journalManager = journalManager
  pool.journalManagerLock = journalManagerLock

  pool.running.store(true, moRelease)

  # Start worker threads
  pool.threadHandles.setLen(pool.poolSize)
  for i in 0 ..< pool.poolSize:
    var args = WorkerThreadArgs(
      poolPtr: cast[pointer](pool),
      workerId: i,
      poolSize: pool.poolSize
    )
    createThread(pool.threadHandles[i], workerProc, args)

  logInfo("Worker pool started with " & $pool.poolSize & " threads")

# Stop the worker pool
proc stop*(pool: WorkerPool) =
  if not pool.running.load(moRelaxed):
    return # Already stopped

  pool.running.store(false, moRelease)

  # Wait for all threads to finish
  for i in 0 ..< pool.threadHandles.len:
    if pool.threadHandles[i].running:
      joinThread(pool.threadHandles[i])

  # Clear the queue to break cycles
  pool.queueLock.acquire()
  pool.queue.setLen(0)
  pool.queueLock.release()

  # Clear pointers to break reference cycles
  pool.flushManager = nil
  pool.writeBufferSize = nil
  pool.snapshotTracker = nil
  pool.stats = nil
  pool.keyspaces = nil
  pool.keyspacesLock = nil
  pool.journalManager = nil
  pool.journalManagerLock = nil

  logInfo("Worker pool stopped")

# Check if pool is running
proc isRunning*(pool: WorkerPool): bool =
  pool.running.load(moRelaxed)

# Wait for queue to be empty
proc waitForEmpty*(pool: WorkerPool, timeoutMs: int = 5000) =
  var waited = 0
  while waited < timeoutMs:
    pool.queueLock.acquire()
    let empty = pool.queue.len == 0
    pool.queueLock.release()
    if empty:
      return
    sleep(10)
    waited += 10
