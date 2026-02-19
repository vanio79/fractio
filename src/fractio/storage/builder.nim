# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[error, types]
import std/[os, atomics]

# Forward declarations
type
  Config* = object
    path*: string
    journalCompressionType*: CompressionType
    manualJournalPersist*: bool
    workerThreads*: int
    descriptorTable*: object # Placeholder
    cache*: object           # Placeholder
    maxJournalingSizeInBytes*: uint64
    maxWriteBufferSizeInBytes*: Option[uint64]
    cleanPathOnDrop*: bool

  Openable* = concept T
    proc open*(config: Config): StorageResult[T]

# Database builder
type
  Builder*[O] = object
    inner*: Config
    phantom*: typeDesc[O] # Placeholder for PhantomData

# Constructor
proc newBuilder*[O](path: string): Builder[O] =
  Builder[O](
    inner: Config(
      path: path,
      journalCompressionType: ctNone,
      manualJournalPersist: false,
      workerThreads: min(4, countProcessors()), # Default to min(4, CPU cores)
    maxJournalingSizeInBytes: 512 * 1024 * 1024, # 512 MiB
    maxWriteBufferSizeInBytes: none(uint64),
    cleanPathOnDrop: false
  ),
    phantom: typeDesc[O]
  )

# Convert to config
proc intoConfig*[O](builder: Builder[O]): Config =
  builder.inner

# Open the database
proc open*[O](builder: Builder[O]): StorageResult[O] =
  # In a full implementation, this would open the database
  # For now, we'll return an error since O is unknown
  return asErr(StorageError(kind: seStorage, storageError: "Not implemented"))

# Set journal compression
proc journalCompression*[O](builder: Builder[O],
    comp: CompressionType): Builder[O] =
  var result = builder
  result.inner.journalCompressionType = comp
  return result

# Set manual journal persist
proc manualJournalPersist*[O](builder: Builder[O], flag: bool): Builder[O] =
  var result = builder
  result.inner.manualJournalPersist = flag
  return result

# Set worker threads
proc workerThreads*[O](builder: Builder[O], n: int): Builder[O] =
  # In production, we would assert n > 0
  # For now, we'll just pass it through
  return builder.workerThreadsUnchecked(n)

# Set worker threads unchecked
proc workerThreadsUnchecked*[O](builder: Builder[O], n: int): Builder[O] =
  var result = builder
  result.inner.workerThreads = n
  return result

# Set max cached files
proc maxCachedFiles*[O](builder: Builder[O], n: Option[int]): Builder[O] =
  var result = builder
  # In a full implementation, this would set the descriptor table
  # For now, we'll just store the value
  return result

# Set cache size
proc cacheSize*[O](builder: Builder[O], sizeBytes: uint64): Builder[O] =
  var result = builder
  # In a full implementation, this would set the cache
  # For now, we'll just store the value
  return result

# Set max journaling size
proc maxJournalingSize*[O](builder: Builder[O], bytes: uint64): Builder[O] =
  assert(bytes >= 64 * 1024 * 1024, "Journaling size must be at least 64 MiB")

  var result = builder
  result.inner.maxJournalingSizeInBytes = bytes
  return result

# Set max write buffer size
proc maxWriteBufferSize*[O](builder: Builder[O], bytes: Option[
    uint64]): Builder[O] =
  if bytes.isSome():
    let value = bytes.get()
    assert(value >= 1024 * 1024, "Write buffer size must be at least 1 MiB")

  var result = builder
  result.inner.maxWriteBufferSizeInBytes = bytes
  return result

# Set temporary flag
proc temporary*[O](builder: Builder[O], flag: bool): Builder[O] =
  var result = builder
  result.inner.cleanPathOnDrop = flag
  return result
