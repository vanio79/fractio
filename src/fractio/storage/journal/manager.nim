# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Journal Manager
##
## Tracks sealed journals and handles garbage collection when journals
## are no longer needed (all keyspaces have flushed past the journal's watermark).
##
## Design note: Uses keyspace IDs (not references) to avoid GC reference cycles.
## The caller provides accessors to check keyspace state.

import fractio/storage/[error, types, journal/writer]
import std/[os, options]

# Keyspace ID type
type
  KeyspaceId* = uint64

# Stores the highest seqno of a keyspace found in a journal
type
  EvictionWatermark* = object
    keyspaceId*: KeyspaceId
    lsn*: uint64

# Debug representation
proc `$`*(watermark: EvictionWatermark): string =
  "ks:" & $watermark.keyspaceId & "@" & $watermark.lsn

# Journal manager item
type
  JournalItem* = object
    path*: string
    sizeInBytes*: uint64
    watermarks*: seq[EvictionWatermark]

# Debug representation
proc `$`*(item: JournalItem): string =
  "JournalItem " & item.path & " (" & $item.sizeInBytes & " bytes, " &
    $item.watermarks.len & " watermarks)"

# The JournalManager keeps track of sealed journals that are being flushed
type
  JournalManager* = ref object
    items*: seq[JournalItem]
    diskSpaceInBytes*: uint64

# Constructor
proc newJournalManager*(): JournalManager =
  JournalManager(
    items: newSeq[JournalItem](0),
    diskSpaceInBytes: 0
  )

# Clear all items
proc clear*(manager: JournalManager) =
  manager.items.setLen(0)
  manager.diskSpaceInBytes = 0

# Enqueue an item
proc enqueue*(manager: JournalManager, item: JournalItem) =
  manager.diskSpaceInBytes = manager.diskSpaceInBytes + item.sizeInBytes
  manager.items.add(item)

# Returns the amount of sealed journals
proc sealedJournalCount*(manager: JournalManager): int =
  manager.items.len

# Returns the amount of journals (including active)
proc journalCount*(manager: JournalManager): int =
  # NOTE: + 1 = active journal
  manager.sealedJournalCount() + 1

# Returns the amount of bytes used on disk by journals
proc diskSpaceUsed*(manager: JournalManager): uint64 =
  manager.diskSpaceInBytes

# Get keyspace IDs that need flushing before oldest journal can be evicted
proc getKeyspaceIdsNeedingFlush*(manager: JournalManager): seq[KeyspaceId] =
  result = @[]
  if manager.items.len == 0:
    return

  let firstItem = manager.items[0]
  for watermark in firstItem.watermarks:
    result.add(watermark.keyspaceId)

# Get all watermarks for oldest journal
proc getOldestJournalWatermarks*(manager: JournalManager): seq[
    EvictionWatermark] =
  if manager.items.len == 0:
    return @[]
  return manager.items[0].watermarks

# Evict oldest journal (caller must ensure it's safe)
proc evictOldestJournal*(manager: JournalManager): StorageResult[void] =
  if manager.items.len == 0:
    return okVoid

  let item = manager.items[0]

  # Remove the journal file
  try:
    removeFile(item.path)
  except OSError:
    return err[void, StorageError](StorageError(kind: seIo,
        ioError: "Failed to remove journal file: " & item.path))

  manager.diskSpaceInBytes = manager.diskSpaceInBytes - item.sizeInBytes
  manager.items.delete(0)

  return okVoid

# Rotate journal with watermarks
proc rotateJournal*(manager: JournalManager, journalWriter: Writer,
                    watermarks: seq[EvictionWatermark]): StorageResult[void] =
  let journalSizeResult = journalWriter.len()
  if not journalSizeResult.isOk:
    return err[void, StorageError](journalSizeResult.err[])

  let journalSize = journalSizeResult.value

  let rotateResult = journalWriter.rotate()
  if not rotateResult.isOk:
    return err[void, StorageError](rotateResult.err[])

  let sealedPath = rotateResult.value[0]

  manager.enqueue(JournalItem(
    path: sealedPath,
    watermarks: watermarks,
    sizeInBytes: journalSize
  ))

  return okVoid
