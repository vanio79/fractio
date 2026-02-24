# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Blob Tree
##
## Key-value separation for large values.

import std/[os, streams, tables]
import types
import error

# Blob indirection handle
type
  BlobHandle* = ref object
    blobFileId*: int64
    offset*: uint64
    size*: uint32

# Blob tree (simplified)
type
  BlobTree* = ref object
    path*: string
    blobFiles*: Table[int64, pointer] # blob_id -> BlobFile

proc newBlobTree*(path: string): BlobTree =
  BlobTree(
    path: path,
    blobFiles: initTable[int64, pointer]()
  )

# GC entry
type
  FragmentationEntry* = ref object
    blobFileId*: int64
    offset*: uint64
    size*: uint32

type
  FragmentationMap* = ref object
    entries*: seq[FragmentationEntry]

# Blob ingestion
type
  BlobIngest* = ref object
    ## Handles blob ingestion
    discard

proc newBlobIngest*(): BlobIngest =
  BlobIngest()

# Handle operations
proc decodeBlobHandle*(stream: Stream): LsmResult[BlobHandle] =
  try:
    var blobFileId = int64(0)
    for i in 0 ..< 8:
      blobFileId = blobFileId or (int64(ord(stream.readChar())) shl (i * 8))

    var offset = uint64(0)
    for i in 0 ..< 8:
      offset = offset or (uint64(ord(stream.readChar())) shl (i * 8))

    var size = uint32(0)
    for i in 0 ..< 4:
      size = size or (uint32(ord(stream.readChar())) shl (i * 8))

    ok(BlobHandle(blobFileId: blobFileId, offset: offset, size: size))
  except:
    err[BlobHandle](newIoError(getCurrentExceptionMsg()))

proc encodeBlobHandle*(stream: Stream, h: BlobHandle) =
  ## Encode blob handle to stream
  for i in 0 ..< 8:
    stream.write(((h.blobFileId shr (i * 8)) and 0xFF).chr)
  for i in 0 ..< 8:
    stream.write(((h.offset shr (i * 8)) and 0xFF).chr)
  for i in 0 ..< 4:
    stream.write(((h.size shr (i * 8)) and 0xFF).chr)
