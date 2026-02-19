# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/types
import fractio/storage/path as storage_path
import std/[os, cpuinfo, options]

# Forward declarations for shared components
type
  Cache* = object
  DescriptorTable* = object

# Global database configuration
type
  Config* = object
    # Base path of database
    path*: string

    # When true, the path will be deleted upon drop
    cleanPathOnDrop*: bool

    cache*: Cache

    # Descriptor table that will be shared between keyspaces
    descriptorTable*: Option[DescriptorTable]

    # Max size of all journals in bytes
    maxJournalingSizeInBytes*: uint64

    # Max size of all active memtables
    maxWriteBufferSizeInBytes*: Option[uint64]

    manualJournalPersist*: bool

    # Amount of concurrent worker threads
    workerThreads*: int

    journalCompressionType*: CompressionType
    journalCompressionThreshold*: int

const DEFAULT_CPU_CORES*: int = 4

proc getOpenFileLimit*(): int =
  # Simplified implementation for different platforms
  when defined(windows):
    return 400
  elif defined(macos):
    return 150
  else:
    return 900

# Creates a new configuration
proc newConfig*(path: string): Config =
  # Get number of CPU cores
  let cpuCores = countProcessors()
  let workerThreads = min(cpuCores, DEFAULT_CPU_CORES)

  # Get absolute path using our custom function
  let absolutePath = storage_path.absolutePath(path)

  Config(
    path: absolutePath,
    cleanPathOnDrop: false,
    descriptorTable: some(DescriptorTable()), # Placeholder
    maxWriteBufferSizeInBytes: none(uint64),
    maxJournalingSizeInBytes: 512 * 1024 * 1024, # 512 MiB
    workerThreads: workerThreads,
    manualJournalPersist: false,
    journalCompressionType: ctNone, # Default to no compression
    journalCompressionThreshold: 4096,
    cache: Cache() # Placeholder
  )
