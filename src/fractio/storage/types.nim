# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import std/[tables, hashes]
import options

# Basic types used throughout the storage system

type
  SeqNo* = uint64 # Sequence number for ordering operations
  UserKey* = string # User-provided key type
  UserValue* = string # User-provided value type
  
  # Key-value pair
  KvPair* = tuple[key: UserKey, value: UserValue]

  # Disk format version - enum for valid versions
  FormatVersion* = enum
    fvV1 # Version for 1.x.x releases
    fvV2 # Version for 2.x.x releases
    fvV3 # Version for 3.x.x releases

         # Compression types
  CompressionType* = enum
    ctNone   # No compression
    ctLz4    # LZ4 compression
    ctSnappy # Snappy compression

             # Error during journal recovery
  JournalRecoveryError* = object

    # Value type in the storage system
  ValueType* = enum
    vtValue         # Regular value
    vtTombstone     # Tombstone (deleted key)
    vtWeakTombstone # Weak tombstone (for merge operations)
    vtIndirection   # Blob value indirection (for KV separation)

# Save to memory

# Save to memory
