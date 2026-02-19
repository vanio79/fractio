# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Database Recovery Implementation
##
## Recovers database state from journals after a crash.
## This module provides journal recovery only to avoid circular dependencies.

import fractio/storage/[error, types, file, logging]
import fractio/storage/journal/writer
import fractio/storage/journal/reader
import std/[os, tables, atomics, options, strutils, algorithm]

# Recovery result
type
  RecoveryResult* = object
    active*: Writer
    sealed*: seq[string]

# Recover journals from directory
proc recoverJournals*(path: string, compressionType: CompressionType,
                      compressionThreshold: int): StorageResult[
                          RecoveryResult] =
  ## Recovers journals from the given directory.
  ## Returns the active journal writer and a list of sealed journal paths.

  if not dirExists(path):
    # Create the journals directory
    try:
      createDir(path)
    except OSError:
      return err[RecoveryResult, StorageError](StorageError(kind: seIo,
          ioError: "Failed to create journals directory"))

    # Create a new empty journal
    let journalPath = path / "0.jnl"
    let journalResult = createNew(journalPath)
    if journalResult.isErr:
      return err[RecoveryResult, StorageError](journalResult.error)

    return ok[RecoveryResult, StorageError](RecoveryResult(
      active: journalResult.value,
      sealed: @[]
    ))

  # Look for journal files
  var journalFiles: seq[string] = @[]
  for kind, filePath in walkDir(path):
    if kind == pcFile and filePath.endsWith(".jnl"):
      journalFiles.add(filePath)

  if journalFiles.len == 0:
    # No journals found, create a new one
    let journalPath = path / "0.jnl"
    let journalResult = createNew(journalPath)
    if journalResult.isErr:
      return err[RecoveryResult, StorageError](journalResult.error)

    return ok[RecoveryResult, StorageError](RecoveryResult(
      active: journalResult.value,
      sealed: @[]
    ))

  # Find the active journal (highest numbered one)
  journalFiles.sort()
  let activeJournalPath = journalFiles[^1]
  let sealedJournals = journalFiles[0..^2]

  # Truncate the active journal to remove incomplete writes
  let readerResult = newJournalReader(activeJournalPath)
  if readerResult.isOk:
    discard maybeTruncateFileToLastValidPos(readerResult.value)
    # Note: JournalReader is a ref object, will be GC'd

  # Open the active journal for writing
  let journalResult = fromFile(activeJournalPath)
  if journalResult.isErr:
    return err[RecoveryResult, StorageError](journalResult.error)

  logInfo("Recovered " & $sealedJournals.len &
      " sealed journals, active journal at " & activeJournalPath)

  return ok[RecoveryResult, StorageError](RecoveryResult(
    active: journalResult.value,
    sealed: sealedJournals
  ))
