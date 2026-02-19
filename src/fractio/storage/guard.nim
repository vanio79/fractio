# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[error, types]

# Forward declaration for LSM tree guard
type
  LsmIterGuardImpl* = object

type
  Guard* = object
    inner*: LsmIterGuardImpl

# Accesses the key-value pair if the predicate returns true
proc intoInnerIf*(guard: Guard, pred: proc(key: UserKey): bool): StorageResult[(
    UserKey, Option[UserValue])] =
  # This will be implemented when we have the actual LSM tree implementation
  # For now, we return a placeholder
  return err(StorageError(kind: seStorage, storageError: "Not implemented"))

# Returns the key-value tuple
proc intoInner*(guard: Guard): StorageResult[(UserKey, UserValue)] =
  # This will be implemented when we have the actual LSM tree implementation
  # For now, we return a placeholder
  return err(StorageError(kind: seStorage, storageError: "Not implemented"))

# Returns the key
proc key*(guard: Guard): StorageResult[UserKey] =
  # This will be implemented when we have the actual LSM tree implementation
  # For now, we return a placeholder
  return err(StorageError(kind: seStorage, storageError: "Not implemented"))

# Returns the value size
proc size*(guard: Guard): StorageResult[uint32] =
  # This will be implemented when we have the actual LSM tree implementation
  # For now, we return a placeholder
  return err(StorageError(kind: seStorage, storageError: "Not implemented"))

# Returns the value
proc value*(guard: Guard): StorageResult[UserValue] =
  # This will be implemented when we have the actual LSM tree implementation
  # For now, we return a placeholder
  return err(StorageError(kind: seStorage, storageError: "Not implemented"))

# Returns the key-value tuple
proc intoInner*(guard: Guard): StorageResult[tuple[key: UserKey,
    value: UserValue]] =
  # This will be implemented when we have the actual LSM tree implementation
  return StorageError(kind: seStorage, storageError: "Not implemented").toFractioError()

# Returns the key
proc key*(guard: Guard): StorageResult[UserKey] =
  # This will be implemented when we have the actual LSM tree implementation
  return StorageError(kind: seStorage, storageError: "Not implemented").toFractioError()

# Returns the value size
proc size*(guard: Guard): StorageResult[uint32] =
  # This will be implemented when we have the actual LSM tree implementation
  return StorageError(kind: seStorage, storageError: "Not implemented").toFractioError()

# Returns the value
proc value*(guard: Guard): StorageResult[UserValue] =
  # This will be implemented when we have the actual LSM tree implementation
  return StorageError(kind: seStorage, storageError: "Not implemented").toFractioError()
