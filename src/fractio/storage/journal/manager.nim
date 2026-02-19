# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[error, types, journal/writer]
import std/[os, locks]

# Forward declarations
type
  Keyspace* = object
    name*: string
    isDeleted*: bool # Placeholder for atomic boolean

# Stores the highest seqno of a keyspace found in a journal
type
  EvictionWatermark* = object
    keyspace*: Keyspace
    lsn*: SeqNo

# Debug representation
proc `$`*(watermark: EvictionWatermark): string =
  watermark.keyspace.name & ":" & $watermark.lsn

# Journal manager item
type
  Item* = object
    path*: string
    sizeInBytes*: uint64
    watermarks*: seq[EvictionWatermark]

# Debug representation
proc `$`*(item: Item): string =
  "JournalManagerItem " & item.path & " => " & $item.watermarks

# The JournalManager keeps track of sealed journals that are being flushed
type
  JournalManager* = ref object
    items*: seq[Item]
    diskSpaceInBytes*: uint64

# Constructor
proc newJournalManager*(): JournalManager =
  JournalManager(
    items: newSeq[Item](0),
    diskSpaceInBytes: 0
  )

# Clear all items
proc clear*(manager: JournalManager) =
  manager.items.setLen(0)

# Enqueue an item
proc enqueue*(manager: JournalManager, item: Item) =
  manager.diskSpaceInBytes = manager.diskSpaceInBytes + item.sizeInBytes
  manager.items.add(item)

# Returns the amount of journals
proc journalCount*(manager: JournalManager): int =
  # NOTE: + 1 = active journal
  manager.sealedJournalCount() + 1

# Returns the amount of sealed journals
proc sealedJournalCount*(manager: JournalManager): int =
  manager.items.len

# Returns the amount of bytes used on disk by journals
proc diskSpaceUsed*(manager: JournalManager): uint64 =
  manager.diskSpaceInBytes

# Gets keyspaces to be flushed so that the oldest journal can be safely evicted
proc getKeyspacesToFlushForOldestJournalEviction*(manager: JournalManager): seq[Keyspace] =
  var items: seq[Keyspace] = @[]

  if manager.items.len > 0:
    let firstItem = manager.items[0]
    for watermark in firstItem.watermarks:
      # In a full implementation, this would check the keyspace tree's highest persisted seqno
      # For now, we'll just add the keyspace
      items.add(watermark.keyspace)

  return items

# Performs maintenance, maybe deleting some old journals
proc maintenance*(manager: JournalManager): StorageResult[void] =
  logDebug("Running journal maintenance")

  while manager.items.len > 0:
    let item = manager.items[0]

    # Check if all keyspaces have been flushed enough to evict the journal
    var canEvict = true
    for watermark in item.watermarks:
      # Only check keyspace seqno if not deleted
      if not watermark.keyspace.isDeleted: # Placeholder for atomic load
        # In a full implementation, this would check the keyspace tree's highest persisted seqno
        # For now, we'll assume it can be evicted
        discard

    if not canEvict:
      break

    # Remove the journal file
    try:
      removeFile(item.path)
    except OSError:
      logError("Failed to clean up stale journal file at: " & item.path)
      return err(StorageError(kind: seIo,
          ioError: "Failed to remove journal file"))

    manager.diskSpaceInBytes = manager.diskSpaceInBytes - item.sizeInBytes
    manager.items.delete(0)

  return ok()

# Rotate journal
proc rotateJournal*(manager: JournalManager, journalWriter: Writer,
                    watermarks: seq[EvictionWatermark]): StorageResult[void] =
  let journalSize = journalWriter.len().valueOr:
    return err(error)

  let rotateResult = journalWriter.rotate()
  if rotateResult.isErr():
    return err(rotateResult.error())

  let (sealedPath, _) = rotateResult.get()

  manager.enqueue(Item(
    path: sealedPath,
    watermarks: watermarks,
    sizeInBytes: journalSize
  ))

  return ok()
