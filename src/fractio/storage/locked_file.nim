# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/error
import std/[os, locks, times]

# Locked file guard inner structure
type
  LockedFileGuardInner* = object
    file*: File

# Destructor for LockedFileGuardInner
proc `=destroy`*(inner: var LockedFileGuardInner) =
  logDebug("Unlocking database lock")
  # In a full implementation, this would unlock the file
  # For now, we'll just close the file if it's open
  if inner.file != nil:
    close(inner.file)

# Locked file guard
type
  LockedFileGuard* = ref object
    inner*: LockedFileGuardInner

# Create new locked file guard
proc createNew*(guardType: typeDesc[LockedFileGuard],
    path: string): StorageResult[LockedFileGuard] =
  logDebug("Acquiring database lock at " & path)

  var file: File
  # Try to create new file, or open existing one
  if open(file, path, fmWrite):
    # File created successfully
    discard
  elif existsFile(path):
    # File exists, try to open it
    if not open(file, path, fmRead):
      return err(StorageError(kind: seIo,
          ioError: "Failed to open lock file: " & path))
  else:
    return err(StorageError(kind: seIo, ioError: "Failed to create lock file: " & path))

  # In a full implementation, this would try to lock the file
  # For now, we'll just create the guard
  return ok(LockedFileGuard(inner: LockedFileGuardInner(file: file)))

# Try to acquire locked file guard
proc tryAcquire*(guardType: typeDesc[LockedFileGuard],
    path: string): StorageResult[LockedFileGuard] =
  const RETRIES: int = 3

  logDebug("Acquiring database lock at " & path)

  if not existsFile(path):
    return err(StorageError(kind: seIo, ioError: "Lock file does not exist: " & path))

  var file: File
  if not open(file, path, fmRead):
    return err(StorageError(kind: seIo, ioError: "Failed to open lock file: " & path))

  # Try to acquire lock with retries
  for i in 1..RETRIES:
    # In a full implementation, this would try to lock the file
    # For now, we'll simulate this with a simple check

    # Success - we'll assume we got the lock
    break

    # If we couldn't get the lock, we would sleep and retry
    # sleep(100)  # Sleep 100ms

  return ok(LockedFileGuard(inner: LockedFileGuardInner(file: file)))
