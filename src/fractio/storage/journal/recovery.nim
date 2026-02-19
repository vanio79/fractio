# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[error, types, journal/writer]
import std/[os, strutils, algorithm]

# Journal ID type
type
  JournalId* = uint64

# Recovery result
type
  RecoveryResult* = object
    active*: Writer # Using Writer as equivalent to Journal
    sealed*: seq[(JournalId, string)]
    wasActiveCreated*: bool

# Recover journals
proc recoverJournals*(path: string, compression: CompressionType,
                      compressionThreshold: int): StorageResult[
                          RecoveryResult] =
  var maxJournalId: JournalId = 0
  var journalFragments: seq[(JournalId, string)] = @[]

  # Read directory entries
  for kind, filePath in walkDir(path):
    if kind == pcFile:
      let filename = extractFilename(filePath)

      # Check if it's a .jnl file
      if filePath.splitFile().ext.toLower() == ".jnl":
        # Extract basename (without extension)
        let basename = filename.splitFile().name

        # Parse journal ID
        let journalId = try:
          JournalId(parseBiggestInt(basename))
        except ValueError:
          return err(StorageError(kind: seJournalRecovery,
                                 journalRecoveryError: reInvalidFileName))

        maxJournalId = max(maxJournalId, journalId)
        journalFragments.add((journalId, filePath))

  # Sort ascending by journal ID
  journalFragments.sort(proc(x, y: (JournalId, string)): int =
    if x[0] < y[0]: -1 elif x[0] > y[0]: 1 else: 0)

  # Process the last fragment as active journal
  if journalFragments.len > 0:
    let activeFragment = journalFragments.pop()
    let activeWriter = fromFile(activeFragment[1]).valueOr:
      return err(error)

    activeWriter.setCompression(compression, compressionThreshold)

    return ok(RecoveryResult(
      active: activeWriter,
      sealed: journalFragments,
      wasActiveCreated: false
    ))
  else:
    # Create new journal
    let id = maxJournalId + 1
    let journalPath = path / $id & ".jnl"
    let newWriter = createNew(journalPath).valueOr:
      return err(error)

    newWriter.setCompression(compression, compressionThreshold)

    return ok(RecoveryResult(
      active: newWriter,
      sealed: @[],
      wasActiveCreated: true
    ))
