# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree v2 - Main Entry Point
##
## A K.I.S.S. implementation of log-structured merge trees (LSM-trees/LSMTs).
##
## NOTE: This crate only provides a primitive LSM-tree, not a full storage engine.
## It does not ship with a write-ahead log, so writes are not persisted
## until manually flushing the memtable.
##
## Keys are limited to 65536 bytes, values are limited to 2^32 bytes.
##
## # Usage
##
## .. code-block:: nim
##
##   let config = newDefaultConfig("/path/to/data")
##   let tree = createNewTree(config).get()
##
##   tree.insert(newSlice("key"), newSlice("value"), 1)
##   let value = tree.get(newSlice("key"))
##
##   tree.remove(newSlice("key"), 2)

# Core types - MUST come first as other modules depend on it
import lsm_tree_v2/types

# Error handling
import lsm_tree_v2/error

# Hash utilities
import lsm_tree_v2/hash

# Binary encoding/decoding
import lsm_tree_v2/coding

# Merge iterator
import lsm_tree_v2/merge

# Memtable (in-memory buffer)
import lsm_tree_v2/memtable

# SSTable (on-disk storage)
import lsm_tree_v2/table

# Configuration
import lsm_tree_v2/config

# Main Tree implementation
import lsm_tree_v2/lsm_tree

# Additional modules
import lsm_tree_v2/seqno
import lsm_tree_v2/cache
import lsm_tree_v2/checksum
import lsm_tree_v2/compaction
import lsm_tree_v2/version
import lsm_tree_v2/vlog
import lsm_tree_v2/blob_tree

# Re-export for convenience
export types
export error
export hash
export coding
export merge
export memtable
export table
export config
export lsm_tree

when isMainModule:
  echo "LSM Tree v2 - Ported from Rust lsm-tree crate"
  echo "Version: 2.0"
  echo ""
  echo "This is a K.I.S.S. implementation of log-structured merge trees."
  echo "Keys limited to 65536 bytes, values limited to 2^32 bytes."
