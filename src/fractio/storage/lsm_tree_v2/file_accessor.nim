# Copyright (c) 2025-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## File Accessor
##
## Allows accessing a file (either cached or pinned).

import descriptor_table

type
  FileAccessorKind* = enum
    faPinned ## Pinned file descriptor
    faCached ## Cached file descriptor

  FileAccessor* = ref object
    case kind*: FileAccessorKind
    of faPinned:
      file*: File
    of faCached:
      descriptorTable*: DescriptorTable

proc newPinnedFileAccessor*(file: File): FileAccessor =
  FileAccessor(kind: faPinned, file: file)

proc newCachedFileAccessor*(descriptorTable: DescriptorTable): FileAccessor =
  FileAccessor(kind: faCached, descriptorTable: descriptorTable)

proc asDescriptorTable*(fa: FileAccessor): Option[DescriptorTable] =
  case fa.kind
  of faCached: some(fa.descriptorTable)
  of faPinned: none(DescriptorTable)
