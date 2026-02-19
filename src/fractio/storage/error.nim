# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/core/errors
import options
import fractio/storage/types
import fractio/storage/journal/error # For RecoveryError

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
      storageError*: string # Will be replaced with actual LSM tree error type
    of seIo:
      ioError*: string      # Will be replaced with actual IO error type
    of seJournalRecovery:
      journalRecoveryError*: RecoveryError
    of seInvalidVersion:
      invalidVersion*: Option[FormatVersion]
    of seDecompress:
      decompressType*: CompressionType
    of seInvalidTrailer:
      discard
    of seInvalidTag:
      tagName*: string
      tagValue*: uint8
    of sePoisoned:
      discard
    of seKeyspaceDeleted:
      discard
    of seLocked:
      discard
    of seUnrecoverable:
      discard

# Simple Result type for storage operations
type
  Result*[T, E] = object
    isOk*: bool
    err*: ref E # Use ref to avoid case object initialization issues
    when T isnot void:
      value*: T

# Helper constructors for Result
proc ok*[T, E](val: T): Result[T, E] =
  when T is void:
    {.error: "Use okVoid() for void result types".}
  else:
    result.isOk = true
    result.value = val

# Helper to create error results
proc err*[T, E](e: E): Result[T, E] =
  when T is void:
    result.isOk = false
    new(result.err)
    result.err[] = e
  else:
    result.isOk = false
    new(result.err)
    result.err[] = e
    result.value = default(T)

# Storage result alias - must be defined before procs using it
type
  StorageResult*[T] = Result[T, StorageError]

# Convenience alias
template asErr*[T, E](e: E): Result[T, E] =
  err[T, E](e)

# Convenience overload for StorageResult[void]
proc asErr*(e: StorageError): StorageResult[void] =
  result.isOk = false
  new(result.err)
  result.err[] = e

# Helper methods for Result
proc isOk*[T, E](r: Result[T, E]): bool =
  r.isOk

proc isErr*[T, E](r: Result[T, E]): bool =
  not r.isOk

proc get*[T, E](r: Result[T, E]): T =
  when T is void:
    if not r.isOk:
      raise newException(ValueError, "Called get on an Err value")
  else:
    if r.isOk:
      r.value
    else:
      raise newException(ValueError, "Called get on an Err value")

proc error*[T, E](r: Result[T, E]): E =
  if not r.isOk and r.err != nil:
    r.err[]
  else:
    raise newException(ValueError, "Called error on an Ok value")

# Helper to create void ok result
template okVoid*: StorageResult[void] =
  StorageResult[void](isOk: true)

# Convert to FractioError for compatibility with the rest of the system
proc toFractioError*(err: StorageError): FractioError =
  case err.kind
  of seStorage:
    storageError("Storage error: " & err.storageError, "storage")
  of seIo:
    storageError("IO error: " & err.ioError, "io")
  of seJournalRecovery:
    storageError("Journal recovery error", "journal")
  of seInvalidVersion:
    storageError("Invalid version", "version")
  of seDecompress:
    storageError("Decompression failed for: " & $err.decompressType, "compression")
  of seInvalidTrailer:
    storageError("Invalid journal trailer", "journal")
  of seInvalidTag:
    storageError("Invalid tag: " & err.tagName & " with value: " &
        $err.tagValue, "journal")
  of sePoisoned:
    storageError("Database poisoned - previous flush/commit failed", "database")
  of seKeyspaceDeleted:
    storageError("Keyspace deleted", "keyspace")
  of seLocked:
    storageError("Database locked", "database")
  of seUnrecoverable:
    storageError("Database unrecoverable", "database")
