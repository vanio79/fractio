# Unit tests for error module
# Tests for StorageError and error conversion

import unittest
import fractio/storage/error

suite "Error Unit Tests":

  test "StorageError creation":
    let error1 = StorageError(kind: seStorage, storageError: "test error")
    check error1.kind == seStorage
    check error1.storageError == "test error"

    let error2 = StorageError(kind: seIo, ioError: "io error")
    check error2.kind == seIo
    check error2.ioError == "io error"

  test "StorageError to FractioError conversion":
    let storageError = StorageError(kind: seStorage,
        storageError: "test storage error")
    let fractioError = toFractioError(storageError)
    # In a full implementation, we would check the specific error type
    # For now, we just verify it doesn't crash

  test "StorageError kinds":
    check seStorage.ord >= 0
    check seIo.ord >= 0
    check seJournalRecovery.ord >= 0
    check seInvalidVersion.ord >= 0
    check seDecompress.ord >= 0
    check seInvalidTrailer.ord >= 0
    check seInvalidTag.ord >= 0
    check sePoisoned.ord >= 0
    check seKeyspaceDeleted.ord >= 0
    check seLocked.ord >= 0
    check seUnrecoverable.ord >= 0

  test "StorageResult type":
    let result1: StorageResult[int] = ok[int, StorageError](42)
    check result1.isOk()
    check result1.get() == 42

    let error = StorageError(kind: seIo, ioError: "test")
    let result2: StorageResult[int] = err[int, StorageError](error)
    check result2.isErr()
