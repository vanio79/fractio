# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import std/[tables, hashes]

# Basic types used throughout the storage system

type
  SeqNo* = uint64 # Sequence number for ordering operations
  UserKey* = string # User-provided key type
  UserValue* = string # User-provided value type
  
  # Key-value pair
  KvPair* = tuple[key: UserKey, value: UserValue]

  # Compression types
  CompressionType* = enum
    ctNone   # No compression
    ctLz4    # LZ4 compression
    ctSnappy # Snappy compression

             # Value type in the storage system
  ValueType* = enum
    vtValue         # Regular value
    vtTombstone     # Tombstone (deleted key)
    vtWeakTombstone # Weak tombstone (for merge operations)

  # Configuration for key-value separation
  KvSeparationOptions* = object
    sizeThreshold*: int # Size threshold for separating key-value pairs

# Save to memory
