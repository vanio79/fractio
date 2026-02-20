# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Flush Manager
##
## Manages a queue of pending flush tasks for background flushing.

import std/[locks, deques, os]

type
  Task* = ref object
    ## A flush task for a keyspace
    ## keyspace is stored as a pointer to avoid circular dependencies
    keyspacePtr*: pointer

  FlushManager* = ref object
    ## Thread-safe queue of flush tasks
    lock*: Lock
    tasks*: Deque[Task]
    count*: int

# Create a new flush manager
proc newFlushManager*(): FlushManager =
  result = FlushManager(
    tasks: initDeque[Task](),
    count: 0
  )
  initLock(result.lock)

# Get number of pending tasks
proc len*(manager: FlushManager): int =
  manager.lock.acquire()
  defer: manager.lock.release()
  manager.tasks.len

# Clear all pending tasks
proc clear*(manager: FlushManager) =
  manager.lock.acquire()
  defer: manager.lock.release()
  manager.tasks.clear()
  manager.count = 0

# Wait for queue to be empty
proc waitForEmpty*(manager: FlushManager, timeoutMs: int = 5000) =
  var waited = 0
  while waited < timeoutMs:
    manager.lock.acquire()
    let empty = manager.tasks.len == 0
    manager.lock.release()
    if empty:
      return
    sleep(10)
    waited += 10

# Enqueue a flush task
proc enqueue*(manager: FlushManager, task: Task) =
  manager.lock.acquire()
  defer: manager.lock.release()
  manager.tasks.addLast(task)
  manager.count += 1

# Dequeue a flush task (non-blocking)
proc dequeue*(manager: FlushManager): Task =
  manager.lock.acquire()
  defer: manager.lock.release()
  if manager.tasks.len > 0:
    result = manager.tasks.popFirst()
    manager.count -= 1
  else:
    result = nil
