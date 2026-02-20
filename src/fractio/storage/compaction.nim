# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Compaction Module
##
## Provides background compaction of SSTables.

import fractio/storage/compaction/worker
export worker

# Compaction strategies
type
  Fifo* = object
    ## FIFO compaction strategy

  Leveled* = object
    ## Leveled compaction strategy

  Levelled* = object
    ## Levelled compaction strategy (alias for Leveled)
