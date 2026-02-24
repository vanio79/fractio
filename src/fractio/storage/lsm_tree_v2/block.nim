# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Block
##
## SSTable block structures and operations.

import std/[streams]
import types
import error

# Block type
type
  BlockKind* = enum
    bkData
    bkIndex
    bkFilter
    bkMeta

# Block header
type
  BlockHeader* = ref object
    blockType*: BlockKind
    compression*: CompressionType
    uncompressedLength*: uint32
    compressedLength*: uint32

proc serializedLen*(h: BlockHeader): int = 12

# Block decoder
type
  BlockDecoder* = ref object
    data*: string
    header*: BlockHeader

proc newBlockDecoder*(data: string): BlockDecoder =
  BlockDecoder(data: data, header: nil)

# Block encoder
type
  BlockEncoder* = ref object
    buffer*: string
    blockType*: BlockKind
    compression*: CompressionType

proc newBlockEncoder*(blockType: BlockKind): BlockEncoder =
  BlockEncoder(buffer: "", blockType: blockType, compression: ctNone)

# Block trailer
type
  BlockTrailer* = ref object
    compression*: CompressionType
    checksum*: uint32

# Block offset
type
  BlockOffset* = uint64

# Binary index
type
  BinaryIndex* = ref object
    entries*: seq[tuple[key: Slice, offset: uint32]]

# Hash index
type
  HashIndex* = ref object
    ## Hash-based index for blocks
    discard
