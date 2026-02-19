# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

# Recovery mode to use
# Based on `RocksDB`'s WAL Recovery Modes
type
  RecoveryMode* = enum
    rmTolerateCorruptTail # The last batch in the journal may be corrupt on crash, and will be discarded without error

# Default recovery mode
proc defaultRecoveryMode*(): RecoveryMode =
  rmTolerateCorruptTail

# Errors that can occur during journal recovery
type
  RecoveryError* = enum
    reInsufficientLength # Batch had less items than expected, so it's incomplete
    reTooManyItems       # Too many items in batch
    reChecksumMismatch   # The checksum value does not match the expected value
    reInvalidFileName    # An unparseable journal file name was encountered

# String representation
proc `$`*(error: RecoveryError): string =
  "RecoveryError(" & repr(error) & ")"
