# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/flush/task
import std/[deques, locks, os, options]

# Forward declarations
type
  TaskRef* = ref Task

# Flush manager
type
  FlushManager* = ref object
    queue*: Deque[TaskRef]
    lock*: Lock

# Constructor
proc newFlushManager*(): FlushManager =
  var lock: Lock
  initLock(lock)

  FlushManager(
    queue: initDeque[TaskRef](),
    lock: lock
  )

# Get queue length
proc len*(manager: FlushManager): int =
  withLock(manager.lock):
    return manager.queue.len

# Clear queue
proc clear*(manager: FlushManager) =
  withLock(manager.lock):
    manager.queue.clear()

# Wait for empty queue
proc waitForEmpty*(manager: FlushManager) =
  while true:
    withLock(manager.lock):
      if manager.queue.len == 0:
        break
    sleep(10) # Sleep 10ms

# Enqueue task
proc enqueue*(manager: FlushManager, task: TaskRef) =
  withLock(manager.lock):
    manager.queue.addLast(task)

# Dequeue task
proc dequeue*(manager: FlushManager): Option[TaskRef] =
  withLock(manager.lock):
    if manager.queue.len > 0:
      return some(manager.queue.popFirst())
    else:
      return none(TaskRef)
