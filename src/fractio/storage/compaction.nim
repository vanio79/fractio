# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

# Export compaction modules
import fractio/storage/compaction/worker
export worker

# Re-export compaction strategies
# In a full implementation, these would come from the LSM tree module
type
  Fifo* = object
  Leveled* = object
  Levelled* = object
