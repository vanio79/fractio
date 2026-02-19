# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import std/[os, posix]
import fractio/storage/error

const MAGIC_BYTES*: array[4, byte] = [byte('F'), byte('J'), byte('L'), 3.byte]

const KEYSPACES_FOLDER* = "keyspaces"

const LOCK_FILE* = "lock"
const VERSION_MARKER* = "version"

const LSM_CURRENT_VERSION_MARKER* = "current"

when not defined(windows):
  proc fsyncDirectory*(path: string): StorageResult[void] =
    var fd = posix.open(path, posix.O_RDONLY)
    if fd == -1:
      return asErr(StorageError(kind: seIo,
          ioError: "Failed to open directory: " & path))

    defer:
      discard posix.close(fd)

    var statBuf: posix.Stat
    if posix.fstat(fd, statBuf) != 0:
      return asErr(StorageError(kind: seIo,
          ioError: "Failed to stat directory: " & path))

    if not posix.S_ISDIR(statBuf.st_mode):
      return asErr(StorageError(kind: seIo,
          ioError: "Path is not a directory: " & path))

    if posix.fsync(fd) == -1:
      return asErr(StorageError(kind: seIo,
          ioError: "Failed to fsync directory: " & path))

    return okVoid()

# On Windows, fsync directory is a no-op
when defined(windows):
  proc fsyncDirectory*(path: string): StorageResult[void] =
    return okVoid()


