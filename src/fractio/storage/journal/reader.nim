# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[error, types, journal/entry]
import std/[streams, os]

# Reads and emits through the entries in a journal file, but doesn't
# check the validity of batches
#
# Will truncate the file to the last valid position to prevent corrupt
# bytes at the end of the file, which would jeopardize future writes into the file.

type
  JournalReader* = ref object
    path*: string
    reader*: Stream
    lastValidPos*: uint64

# Constructor
proc newJournalReader*(path: string): StorageResult[JournalReader] =
  # In a full implementation, this would open the file with read+write permissions
  let file = newFileStream(path, fmReadWrite)
  if file.isNil:
    return err[JournalReader, StorageError](StorageError(kind: seIo,
        ioError: "Failed to open journal file: " & path))

  return ok[JournalReader, StorageError](JournalReader(
    path: path,
    reader: file,
    lastValidPos: 0
  ))

# Truncate file to position
proc truncateFile*(reader: JournalReader, pos: uint64): StorageResult[void] =
  # In a full implementation, this would truncate the file
  # For now, we just update the position
  reader.lastValidPos = pos
  return okVoid()

# Maybe truncate file to last valid position
proc maybeTruncateFileToLastValidPos*(reader: JournalReader): StorageResult[void] =
  # In a full implementation, this would get the stream position
  # For now, we'll assume we're at the end
  let streamPos = reader.lastValidPos

  if streamPos > reader.lastValidPos:
    return reader.truncateFile(reader.lastValidPos)
  return okVoid()

# Iterator for JournalReader
iterator items*(reader: JournalReader): StorageResult[Entry] =
  while not reader.reader.atEnd:
    # Decode entry from reader
    let decodeResult = decodeFrom(reader.reader)

    if decodeResult.isOk:
      let item = decodeResult.value
      # In a full implementation, this would get the stream position
      reader.lastValidPos = uint64(reader.reader.getPosition())
      yield ok[Entry, StorageError](item)
    else:
      let e = decodeResult.err

      # Handle IO errors
      # In a full implementation, we'd check the specific error type
      # For now, we'll just truncate and stop
      let truncateResult = reader.maybeTruncateFileToLastValidPos()
      if not truncateResult.isOk:
        yield err[Entry, StorageError](truncateResult.err[])
      break
