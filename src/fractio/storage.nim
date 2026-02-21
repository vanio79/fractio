# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

# Main storage module that exports all storage functionality

# Note: This is a placeholder main module. Actual imports would depend on the 
# specific structure and compilation setup of the Fractio project.

when defined(nimHasUsed):
  {.used.}

# Core types and errors
import ./error
import ./types
import ./file
import ./path
import ./version

# Memory management
import ./snapshot
import ./snapshot_nonce
import ./snapshot_tracker
import ./write_buffer_manager

# Journal system
import ./journal
import ./journal/entry
import ./journal/error
import ./journal/manager
import ./journal/reader
import ./journal/batch_reader
import ./journal/writer
import ./journal/recovery

# Background processing
import ./flush
import ./flush/manager
import ./flush/task
import ./flush/worker
import ./compaction
import ./compaction/worker
import ./worker_pool
import ./supervisor
import ./stats

# Database components
import ./db
import ./db_config
import ./builder
import ./keyspace
import ./keyspace/name
import ./keyspace/options
import ./keyspace/write_delay
import ./keyspace/config
import ./keyspace/config/compression
import ./batch
import ./batch/item
import ./ingestion
import ./iter
import ./guard
import ./readable
import ./meta_keyspace
import ./recovery
import ./locked_file
import ./poison_dart

# Blob storage (KV separation)
# import ./blob  # Import directly: fractio/storage/blob

# Transactions
# import ./tx  # Import directly: fractio/storage/tx

# Export commonly used types
export error
export types
export snapshot
export journal
export db
export keyspace
export batch
