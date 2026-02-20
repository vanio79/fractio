# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Flush Task
##
## Represents a task to flush a keyspace's memtable to disk.

# Forward declaration - actual Keyspace type is in keyspace.nim
type
  Keyspace* = ref object

# Flush task
type
  Task* = object
    keyspace*: Keyspace

# Create a new flush task
proc newTask*(keyspace: Keyspace): Task =
  Task(keyspace: keyspace)

# Debug representation
proc `$`*(task: Task): string =
  "FlushTask(keyspace=" & (if task.keyspace != nil: "set" else: "nil") & ")"
