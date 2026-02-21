# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Blob Storage Module
##
## Provides KV separation - storing large values in separate blob files.
##
## When a value exceeds the separation threshold, it is stored in a blob file
## instead of the SSTable. The SSTable stores a BlobHandle that references
## the value's location in the blob file.
##
## Benefits:
## - Reduced SSTable size for better cache utilization
## - Faster compaction (don't have to rewrite large values)
## - Separate GC for blob files based on staleness
##
## Usage:
## ```nim
## # In keyspace options
## let opts = defaultCreateOptions().withKvSeparation(some(KvSeparationOptions(
##   separationThreshold: 4 * 1024,  # 4 KiB
##   fileTargetSize: 64 * 1024 * 1024,  # 64 MiB
##   compression: ctNone,
##   ageCutoff: 0.5,
##   stalenessThreshold: 0.3
## )))
##
## let ksResult = db.keyspace("large_values", opts)
## ```

import ./types
import ./writer
import ./reader
import ./gc
import fractio/storage/keyspace/options

# Re-export KvSeparationOptions from keyspace options
export options.KvSeparationOptions

# Re-export main types
export types.BlobFileId, types.BlobHandle, types.BlobFile, types.BlobWriter
export types.BlobManager, types.BlobGCStats, types.newBlobFile,
    types.newBlobManager
export types.shouldSeparate, types.diskSpace, types.fileCount,
    types.fragmentedBytes
export types.BLOB_MAGIC, types.BLOB_ENTRY_HEADER_SIZE
export types.DEFAULT_BLOB_FILE_TARGET_SIZE, types.DEFAULT_SEPARATION_THRESHOLD

# Re-export writer functions
export writer.newBlobWriter, writer.writeHeader, writer.writeEntry
export writer.finalize, writer.shouldRotate, writer.blobFilePath
export writer.serializeHandle, writer.deserializeHandle

# Re-export reader functions
export reader.newBlobReaderCache, reader.getStream, reader.readValue
export reader.closeAll, reader.readHeader, reader.scanBlobFile,
    reader.BlobScanEntry

# Re-export GC functions
export gc.BlobGCResult, gc.BlobGCMetrics, gc.LiveBlobRefs
export gc.defaultBlobGCResult, gc.defaultBlobGCMetrics
export gc.shouldGCFile, gc.newLiveBlobRefs, gc.addRef
export gc.rewriteBlobFile, gc.gcBlobFile, gc.runBlobGC, gc.getGCStats
