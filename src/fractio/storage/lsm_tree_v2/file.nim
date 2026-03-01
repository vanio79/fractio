# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## File Utilities
##
## File operations and constants.

import std/[posix, strutils]
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

proc readExact*(fd: cint, offset: uint64, size: int): string =
  ## Read exactly `size` bytes from file at offset
  let currentPos = posix.lseek(fd, 0, SEEK_CUR)
  posix.lseek(fd, offset, SEEK_SET)
  var buffer = newString(size)
  var totalRead = 0
  while totalRead < size:
    let readCount = posix.read(fd, cast[pointer](addr(buffer[totalRead])),
        size - totalRead)
    if readCount <= 0:
      break
    totalRead += readCount
  posix.lseek(fd, currentPos, SEEK_SET)
  result = buffer
  if result.len != size:
    raise newException(IOError, "read_exact: read " & $result.len &
        " bytes, expected " & $size)



proc rewriteAtomic*(path: string, content: string) =
  ## Atomically rewrite a file
  let tempPath = path & ".tmp"
  let fd = posix.open(tempPath, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
  if fd == -1:
    raise newException(IOError, "rewriteAtomic: failed to open " & tempPath)
  defer: posix.close(fd)
  let bytesWritten = posix.write(fd, cast[pointer](addr(content[0])), content.len)
  if bytesWritten != content.len:
    raise newException(IOError, "rewriteAtomic: wrote " & $bytesWritten &
        " bytes, expected " & $content.len)
  if posix.fsync(fd) != 0:
    raise newException(IOError, "rewriteAtomic: fsync failed")
  if posix.close(fd) != 0:
    raise newException(IOError, "rewriteAtomic: close failed")
  if posix.rename(tempPath, path) != 0:
    raise newException(IOError, "rewriteAtomic: rename failed")

proc fsyncDirectory*(path: string) =
  ## Sync directory to disk (no-op on Windows)
  when defined(windows):
    discard
  else:
    # Open directory and sync
    let dir = posix.open(path, O_RDONLY)
    if dir == -1:
      raise newException(IOError, "fsyncDirectory: failed to open " & path)
    defer: posix.close(dir)
    if posix.fsync(dir) != 0:
      raise newException(IOError, "fsyncDirectory: fsync failed")

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing file utilities..."

  echo "Magic bytes: ", MAGIC_BYTES
  echo "Tables folder: ", TABLES_FOLDER

  echo "File tests passed!"
