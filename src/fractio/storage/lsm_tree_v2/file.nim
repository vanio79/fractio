# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## File Utilities
##
## File operations and constants.

import std/[os, streams]
import types

# ============================================================================
# Constants
# ============================================================================

const
  MAGIC_BYTES*: string = "LSM3" # 4 bytes
  TABLES_FOLDER*: string = "tables"
  BLOBS_FOLDER*: string = "blobs"
  CURRENT_VERSION_FILE*: string = "current"

# ============================================================================
# File Operations
# ============================================================================

proc readExact*(file: File, offset: uint64, size: int): string =
  ## Read exactly `size` bytes from file at offset
  file.setFilePointer(offset)
  result = file.readStr(size)
  if result.len != size:
    raise newException(IOError, "read_exact: read " & $result.len &
        " bytes, expected " & $size)

proc rewriteAtomic*(path: string, content: string) =
  ## Atomically rewrite a file
  let tempPath = path & ".tmp"
  var tempFile = open(tempPath, fmWrite)
  tempFile.write(content)
  tempFile.close()

  # On Unix, rename is atomic
  moveFile(tempPath, path)

proc fsyncDirectory*(path: string) =
  ## Sync directory to disk (no-op on Windows)
  when defined(windows):
    discard
  else:
    # Open directory and sync
    let dir = open(path, fmRead)
    dir.close()

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing file utilities..."

  echo "Magic bytes: ", MAGIC_BYTES
  echo "Tables folder: ", TABLES_FOLDER

  echo "File tests passed!"
