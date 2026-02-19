# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Flush Task
##
## Represents a task to flush a keyspace's memtable to disk.

# Forward declaration - simplified Keyspace for task definition
# The real Keyspace type is in keyspace.nim
type
  Keyspace* = object
    name*: string

# Flush task
type
  Task* = object
    keyspace*: Keyspace

# Debug representation
proc `$`*(task: Task): string =
  "FlushTask(" & task.keyspace.name & ")"
