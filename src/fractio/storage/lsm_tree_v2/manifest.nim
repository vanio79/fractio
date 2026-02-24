# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Manifest
##
## Version manifest for tree metadata.

import std/[os, streams]
import format_version
import config
import error
import checksum

type
  Manifest* = ref object
    version*: FormatVersion
    treeType*: TreeType
    levelCount*: uint8

proc decodeFrom*(path: string): LsmResult[Manifest] =
  # Simplified - would use sfa archive in production
  try:
    let file = open(path, fmRead)
    defer: file.close()

    # Read version
    var versionByte = file.readChar().uint8
    let version = fromUint8(versionByte)

    # Read tree type
    var treeTypeByte = file.readChar().uint8
    let treeType = case treeTypeByte
    of 0: ttStandard
    else: ttBlob

    # Read level count
    var levelCount = uint8(ord(file.readChar()))

    ok(Manifest(
      version: version,
      treeType: treeType,
      levelCount: levelCount
    ))
  except:
    err[Manifest](newIoError(getCurrentExceptionMsg()))
