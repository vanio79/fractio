# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Blob Storage Types
##
## Types for KV separation - storing large values in separate blob files.

import std/[atomics, locks, options, tables, times]

# Blob file header magic
const BLOB_MAGIC* = "FJBLOB01"

# Blob entry header size
const BLOB_ENTRY_HEADER_SIZE* = 16 # key_len(4) + value_len(4) + seqno(8)

# Default blob file target size (64 MiB)
const DEFAULT_BLOB_FILE_TARGET_SIZE* = 64 * 1024 * 1024'u64

# Default separation threshold (4 KiB - values larger than this go to blob)
const DEFAULT_SEPARATION_THRESHOLD* = 4 * 1024'u32

# Blob file ID type
type
  BlobFileId* = uint64

# Blob handle - reference to a value stored in a blob file
type
  BlobHandle* = object
    ## A reference to a blob value stored in a separate file.
    fileId*: BlobFileId     ## Which blob file
    offset*: uint64         ## Offset within the file
    size*: uint32           ## Size of the value
    compressedSize*: uint32 ## Compressed size (0 if not compressed)

# Blob file metadata
type
  BlobFile* = ref object
    ## Metadata for a blob file.
    id*: BlobFileId
    path*: string
    size*: uint64       ## Current file size
    itemCount*: uint32  ## Number of items in file
    created*: Time      ## Creation time
    staleBytes*: uint64 ## Bytes from deleted/overwritten items
    lock*: Lock

# Blob writer - writes values to blob files
type
  BlobWriter* = ref object
    ## Writer for creating blob files.
    path*: string
    fileId*: BlobFileId
    itemCount*: uint32
    currentSize*: uint64
    targetSize*: uint64
    compression*: bool

# Blob GC stats
type
  BlobGCStats* = object
    ## Statistics for blob garbage collection.
    totalFiles*: int
    totalBytes*: uint64
    staleBytes*: uint64
    liveBytes*: uint64
    fragmentationRatio*: float64

# Blob storage manager
type
  BlobManager* = ref object
    ## Manages blob files for a keyspace.
    path*: string
    files*: Table[BlobFileId, BlobFile]
    currentFileId*: Atomic[uint64]
    separationThreshold*: uint32
    targetFileSize*: uint64
    ageCutoff*: float64          ## Age cutoff for GC (0.0-1.0)
    stalenessThreshold*: float64 ## Stale bytes ratio threshold for GC
    lock*: Lock

# Default blob GC stats
proc defaultBlobGCStats*(): BlobGCStats =
  BlobGCStats(
    totalFiles: 0,
    totalBytes: 0,
    staleBytes: 0,
    liveBytes: 0,
    fragmentationRatio: 0.0
  )

# Create a new blob file
proc newBlobFile*(id: BlobFileId, path: string): BlobFile =
  result = BlobFile(
    id: id,
    path: path,
    size: 0,
    itemCount: 0,
    created: getTime(),
    staleBytes: 0
  )
  initLock(result.lock)

# Create a new blob manager
proc newBlobManager*(path: string, separationThreshold: uint32 = DEFAULT_SEPARATION_THRESHOLD,
                     targetFileSize: uint64 = DEFAULT_BLOB_FILE_TARGET_SIZE): BlobManager =
  result = BlobManager(
    path: path,
    files: initTable[BlobFileId, BlobFile](),
    separationThreshold: separationThreshold,
    targetFileSize: targetFileSize,
    ageCutoff: 0.5, # 50% of items must be old
    stalenessThreshold: 0.3 # 30% stale bytes threshold
  )
  result.currentFileId.store(1'u64, moRelaxed)
  initLock(result.lock)

# Get next blob file ID
proc nextFileId*(manager: BlobManager): BlobFileId =
  manager.currentFileId.fetchAdd(1, moRelaxed)

# Check if value should be separated
proc shouldSeparate*(manager: BlobManager, value: string): bool =
  ## Returns true if the value should be stored in blob storage.
  value.len.uint32 >= manager.separationThreshold

# Get total blob disk space
proc diskSpace*(manager: BlobManager): uint64 =
  ## Returns total disk space used by blob files.
  manager.lock.acquire()
  defer: manager.lock.release()

  result = 0
  for file in manager.files.values:
    result += file.size

# Get blob file count
proc fileCount*(manager: BlobManager): int =
  ## Returns the number of blob files.
  manager.lock.acquire()
  defer: manager.lock.release()
  manager.files.len

# Get fragmented (stale) bytes
proc fragmentedBytes*(manager: BlobManager): uint64 =
  ## Returns the total stale bytes in blob files.
  manager.lock.acquire()
  defer: manager.lock.release()

  result = 0
  for file in manager.files.values:
    result += file.staleBytes
