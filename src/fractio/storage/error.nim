# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/core/errors

# Forward declarations for types that will be defined in other modules
type
  JournalRecoveryError* = object
  FormatVersion* = uint32
  CompressionType* = enum
    ctNone
    ctLz4
    ctSnappy

# Storage errors that may occur in the storage engine
type
  StorageErrorKind* = enum
    seStorage         # Error inside LSM-tree
    seIo              # I/O error
    seJournalRecovery # Error during journal recovery
    seInvalidVersion  # Invalid or unparsable data format version
    seDecompress      # Decompression failed
    seInvalidTrailer  # Invalid journal trailer detected
    seInvalidTag      # Invalid tag detected during decoding
    sePoisoned        # A previous flush/commit operation failed
    seKeyspaceDeleted # Keyspace is deleted
    seLocked          # Database is locked
    seUnrecoverable   # Database is unrecoverable

  StorageError* = object
    case kind*: StorageErrorKind
    of seStorage:
      storageError: string # Will be replaced with actual LSM tree error type
    of seIo:
      ioError: string      # Will be replaced with actual IO error type
    of seJournalRecovery:
      journalRecoveryError: JournalRecoveryError
    of seInvalidVersion:
      invalidVersion: Option[FormatVersion]
    of seDecompress:
      decompressType: CompressionType
    of seInvalidTrailer:
      discard
    of seInvalidTag:
      tagName: string
      tagValue: uint8
    of sePoisoned:
      discard
    of seKeyspaceDeleted:
      discard
    of seLocked:
      discard
    of seUnrecoverable:
      discard

# Convert to FractioError for compatibility with the rest of the system
proc toFractioError*(err: StorageError): FractioError =
  case err.kind
  of seStorage:
    storageError("Storage error: " & err.storageError)
  of seIo:
    ioError("IO error: " & err.ioError)
  of seJournalRecovery:
    storageError("Journal recovery error")
  of seInvalidVersion:
    storageError("Invalid version")
  of seDecompress:
    storageError("Decompression failed for: " & $err.decompressType)
  of seInvalidTrailer:
    storageError("Invalid journal trailer")
  of seInvalidTag:
    storageError("Invalid tag: " & err.tagName & " with value: " & $err.tagValue)
  of sePoisoned:
    storageError("Database poisoned - previous flush/commit failed")
  of seKeyspaceDeleted:
    storageError("Keyspace deleted")
  of seLocked:
    storageError("Database locked")
  of seUnrecoverable:
    storageError("Database unrecoverable")

# Result helper type
type
  StorageResult*[T] = Result[T, StorageError]


