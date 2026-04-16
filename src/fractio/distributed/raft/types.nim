# Raft Types - Helper procs for Raft types
#
# This file contains ONLY executable code (procs).
# Type definitions are in types_base.nim which is excluded from coverage.

import fractio/utils/binary
import ./types_base
export types_base

# =============================================================================
# LogEntry Binary Serialization
# =============================================================================

proc encodeLogEntry*(entry: LogEntry): string =
  ## Encode a LogEntry to binary format.
  ##
  ## Binary format (little-endian):
  ## - Magic: 2 bytes (0x52 0x45 = "RE")
  ## - Version: 1 byte (0x01)
  ## - Term: 8 bytes (int64)
  ## - EntryType: 1 byte (uint8 ordinal)
  ## - Data: length-prefixed (u32 len + bytes)
  ##
  ## Total minimum: 16 bytes (empty data)
  var w = initBinaryWriter()
  w.writeBytes(LOG_ENTRY_MAGIC)
  w.writeU8(LOG_ENTRY_VERSION)
  w.writeI64(entry.term)
  w.writeU8(uint8(ord(entry.entryType)))
  w.writeString(entry.data)
  w.finish()

proc decodeLogEntry*(data: string): LogEntry =
  ## Decode binary data to a LogEntry.
  ## Raises ValueError if data is invalid or not binary format.
  var r = initBinaryReader(data)

  # Verify magic header
  if r.remaining < 3:
    raise newException(ValueError, "LogEntry: data too small for header")
  let magic0 = r.readU8()
  let magic1 = r.readU8()
  if magic0 != LOG_ENTRY_MAGIC[0] or magic1 != LOG_ENTRY_MAGIC[1]:
    raise newException(ValueError, "LogEntry: invalid magic header")

  # Verify version
  let version = r.readU8()
  if version != LOG_ENTRY_VERSION:
    raise newException(ValueError, "LogEntry: unsupported version " & $version)

  # Read fields
  result.term = r.readI64()
  result.entryType = LogEntryType(int(r.readU8()))
  result.data = r.readString()

# =============================================================================
# RaftLogStore Methods
# =============================================================================

method close*(store: RaftLogStore) {.base.} =
  ## Close the log store (base implementation does nothing)
  discard
