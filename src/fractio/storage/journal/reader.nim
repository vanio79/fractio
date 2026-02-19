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
    reader*: FileStream
    lastValidPos*: uint64

# Constructor
proc newJournalReader*(path: string): StorageResult[JournalReader] =
  # Open file with read+write permissions to allow truncation
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
  # Close the stream first
  reader.reader.close()

  # Truncate the file using OS-level operation
  try:
    # Open source file for reading
    let srcFile = open(reader.path, fmRead)

    # Read the valid portion (up to pos bytes)
    var buffer = newSeq[byte](int(pos))
    if pos > 0:
      let bytesRead = srcFile.readBuffer(addr buffer[0], int(pos))
      buffer.setLen(bytesRead)
    srcFile.close()

    # Write to temp file
    let tempPath = reader.path & ".tmp"
    let tmpFile = open(tempPath, fmWrite)
    if buffer.len > 0:
      discard tmpFile.writeBuffer(addr buffer[0], buffer.len)
    tmpFile.close()

    # Replace original with temp
    os.moveFile(tempPath, reader.path)

    # Reopen the stream
    let newFile = newFileStream(reader.path, fmReadWrite)
    if newFile.isNil:
      return err[void, StorageError](StorageError(kind: seIo,
          ioError: "Failed to reopen journal file after truncation: " & reader.path))

    reader.reader = newFile
    reader.lastValidPos = pos

    return okVoid
  except OSError:
    return err[void, StorageError](StorageError(kind: seIo,
        ioError: "Failed to truncate journal file: " & reader.path))

# Maybe truncate file to last valid position
proc maybeTruncateFileToLastValidPos*(reader: JournalReader): StorageResult[void] =
  # Get the current stream position (where we tried to read and failed)
  let currentStreamPos = uint64(reader.reader.getPosition())

  # If we're past the last valid position, truncate
  if currentStreamPos > reader.lastValidPos:
    return reader.truncateFile(reader.lastValidPos)

  return okVoid

# Iterator for JournalReader
iterator items*(reader: JournalReader): StorageResult[Entry] =
  while not reader.reader.atEnd:
    # Remember position before attempting to decode
    let posBeforeDecode = uint64(reader.reader.getPosition())

    # Decode entry from reader
    let decodeResult = decodeFrom(reader.reader)

    if decodeResult.isOk:
      let item = decodeResult.value
      # Update last valid position to current position after successful decode
      reader.lastValidPos = uint64(reader.reader.getPosition())
      yield ok[Entry, StorageError](item)
    else:
      # Decode failed - the file is corrupted at this point
      # Seek back to before the failed decode attempt
      reader.reader.setPosition(int(posBeforeDecode))

      # Truncate if we're past the last valid position
      let truncateResult = reader.maybeTruncateFileToLastValidPos()
      if not truncateResult.isOk:
        yield err[Entry, StorageError](truncateResult.err[])
      else:
        # Return the decode error
        yield decodeResult
      break
