# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Value Log
##
## Value log for key-value separation.

import std/[streams, tables]
import types
import error

# Value handle
type
  ValueHandle* = ref object
    ## Handle to a value in the vlog
    blobFileId*: int64
    offset*: uint64
    size*: uint32
    compression*: CompressionType

# Blob file
type
  BlobFile* = ref object
    ## A blob file containing large values
    id*: int64
    path*: string
    size*: uint64
    numValues*: uint32

# VLog accessor
type
  VLogAccessor* = ref object
    ## Accessor for vlog values
    blobFiles*: Table[int64, BlobFile]

proc newVLogAccessor*(): VLogAccessor =
  VLogAccessor(blobFiles: initTable[int64, BlobFile]())

# VLog writer
type
  VLogWriter* = ref object
    ## Writer for vlog
    path*: string
    currentFileId*: int64
    currentOffset*: uint64

proc newVLogWriter*(path: string): VLogWriter =
  VLogWriter(
    path: path,
    currentFileId: 0,
    currentOffset: 0
  )

# Encode/decode value handle
proc encodeValueHandle*(stream: Stream, vh: ValueHandle) =
  for i in 0 ..< 8:
    stream.write(((vh.blobFileId shr (i * 8)) and 0xFF).chr)
  for i in 0 ..< 8:
    stream.write(((vh.offset shr (i * 8)) and 0xFF).chr)
  for i in 0 ..< 4:
    stream.write(((vh.size shr (i * 8)) and 0xFF).chr)
  stream.write(chr(vh.compression.ord))

proc decodeValueHandle*(stream: Stream): LsmResult[ValueHandle] =
  try:
    var blobFileId = 0.int64
    for i in 0 ..< 8:
      blobFileId = blobFileId or (int64(ord(stream.readChar())) shl (i * 8))

    var offset = 0.uint64
    for i in 0 ..< 8:
      offset = offset or (uint64(ord(stream.readChar())) shl (i * 8))

    var size = 0.uint32
    for i in 0 ..< 4:
      size = size or (uint32(ord(stream.readChar())) shl (i * 8))

    var compression = CompressionType(ord(stream.readChar()))

    ok(ValueHandle(blobFileId: blobFileId, offset: offset, size: size,
        compression: compression))
  except:
    err[ValueHandle](newIoError(getCurrentExceptionMsg()))
