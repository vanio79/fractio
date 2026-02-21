# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Blob Integration Helper
##
## Provides integration between SSTable writer and blob storage.
## When writing values that exceed the separation threshold, they are
## automatically stored in blob files and a BlobHandle is stored in the SSTable.

import fractio/storage/blob/types as blob_types
import fractio/storage/blob/writer as blob_writer_module
import fractio/storage/lsm_tree/types as lsm_types
import fractio/storage/keyspace/options
import fractio/storage/error
import std/[options, streams, os]

type
  BlobSeparationContext* = object
    ## Context for blob separation during SSTable write.
    ##
    ## Manages blob file creation and tracking during flush.

    # Blob manager for creating blob files
    manager*: blob_types.BlobManager

    # Current blob writer (if any)
    writer*: blob_types.BlobWriter

    # Current blob file stream
    stream*: FileStream

    # Configuration
    options*: KvSeparationOptions

    # Whether separation is enabled
    enabled*: bool

# Create a new blob separation context
proc newBlobSeparationContext*(basePath: string,
                               options: Option[
                                   KvSeparationOptions]): BlobSeparationContext =
  ## Creates a new blob separation context.
  ## If options is None, separation is disabled.

  result = BlobSeparationContext(
    enabled: options.isSome
  )

  if options.isSome:
    result.options = options.get()
    result.manager = blob_types.newBlobManager(
      basePath / "blobs",
      separationThreshold = options.get().separationThreshold,
      targetFileSize = options.get().fileTargetSize
    )

# Check if a value should be separated
proc shouldSeparate*(ctx: BlobSeparationContext, value: string): bool =
  ## Returns true if the value should be stored in a blob file.
  if not ctx.enabled:
    return false

  return value.len.uint32 >= ctx.options.separationThreshold

# Separate a value to blob storage
proc separateValue*(ctx: var BlobSeparationContext,
                    key: string,
                    value: string,
                    seqno: uint64): StorageResult[blob_types.BlobHandle] =
  ## Stores a value in blob storage and returns a BlobHandle.
  ## The BlobHandle should be serialized and stored in the SSTable as vtIndirection.

  # Ensure we have a writer
  if ctx.writer == nil or ctx.writer.path == "":
    # Create new blob file
    let fileId = ctx.manager.nextFileId()
    let blobPath = blob_writer_module.blobFilePath(ctx.manager.path, fileId)
    createDir(parentDir(blobPath))

    ctx.writer = blob_writer_module.newBlobWriter(ctx.manager.path, fileId,
                               targetSize = ctx.options.fileTargetSize)
    ctx.stream = newFileStream(blobPath, fmWrite)

    # Write header
    let headerResult = ctx.writer.writeHeader(ctx.stream)
    if headerResult.isErr:
      return err[blob_types.BlobHandle, StorageError](headerResult.err[])

  # Check if we should rotate to a new blob file
  if ctx.writer.shouldRotate():
    # Finalize current file
    let finalizeResult = ctx.writer.finalize(ctx.stream)
    if finalizeResult.isErr:
      return err[blob_types.BlobHandle, StorageError](finalizeResult.err[])
    ctx.stream.close()

    # Start new file
    let fileId = ctx.manager.nextFileId()
    let blobPath = blob_writer_module.blobFilePath(ctx.manager.path, fileId)

    ctx.writer = blob_writer_module.newBlobWriter(ctx.manager.path, fileId,
                               targetSize = ctx.options.fileTargetSize)
    ctx.stream = newFileStream(blobPath, fmWrite)

    let headerResult = ctx.writer.writeHeader(ctx.stream)
    if headerResult.isErr:
      return err[blob_types.BlobHandle, StorageError](headerResult.err[])

  # Write the entry - check if compression is enabled using the storage/types CompressionType
  let compress = ctx.options.compression.int != 0 # ctNone is 0 in storage/types
  let entryResult = ctx.writer.writeEntry(ctx.stream, key, value, seqno, compress)
  if entryResult.isErr:
    return err[blob_types.BlobHandle, StorageError](entryResult.err[])

  return entryResult

# Finalize blob separation context
proc finalize*(ctx: var BlobSeparationContext): StorageResult[void] =
  ## Finalizes any open blob files.

  if ctx.stream != nil:
    let finalizeResult = ctx.writer.finalize(ctx.stream)
    if finalizeResult.isErr:
      return finalizeResult
    ctx.stream.close()
    ctx.stream = nil

  return okVoid

# Process memtable entry for potential blob separation
proc processEntry*(ctx: var BlobSeparationContext,
                   entry: lsm_types.MemtableEntry): StorageResult[tuple[
                     key: string,
                     value: string,
                     valueType: lsm_types.ValueType,
                     seqno: uint64]] =
  ## Process a memtable entry, potentially separating large values to blob storage.
  ##
  ## Returns:
  ## - For regular values under threshold: (key, value, vtValue, seqno)
  ## - For large values: (key, serializedHandle, vtIndirection, seqno)
  ## - For tombstones: (key, "", vtTombstone/vtWeakTombstone, seqno)

  # Tombstones are never separated
  if entry.valueType == lsm_types.vtTombstone or entry.valueType ==
      lsm_types.vtWeakTombstone:
    return ok[tuple[key: string, value: string, valueType: lsm_types.ValueType,
        seqno: uint64], StorageError](
      (entry.key, "", entry.valueType, entry.seqno)
    )

  # Check if we should separate this value
  if ctx.shouldSeparate(entry.value):
    # Separate to blob file
    let separateResult = ctx.separateValue(entry.key, entry.value, entry.seqno)
    if separateResult.isErr:
      return err[tuple[key: string, value: string,
          valueType: lsm_types.ValueType, seqno: uint64], StorageError](
          separateResult.err[])

    let handle = separateResult.value
    let serialized = blob_writer_module.serializeHandle(handle)

    return ok[tuple[key: string, value: string, valueType: lsm_types.ValueType,
        seqno: uint64], StorageError](
      (entry.key, serialized, lsm_types.vtIndirection, entry.seqno)
    )

  # Regular value - return as-is
  return ok[tuple[key: string, value: string, valueType: lsm_types.ValueType,
      seqno: uint64], StorageError](
    (entry.key, entry.value, entry.valueType, entry.seqno)
  )
