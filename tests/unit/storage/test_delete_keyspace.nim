# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for delete_keyspace functionality

import unittest
import fractio/storage/db
import fractio/storage/db_config
import fractio/storage/error
import std/[os, tempfiles]

suite "Delete Keyspace Tests":
  setup:
    let tempDir = createTempDir("delete_ks_test_", "")
    let dbPath = tempDir / "test_db"

  teardown:
    try:
      removeDir(tempDir)
    except OSError:
      discard

  test "Delete existing keyspace":
    # Create database config
    let config = db_config.newConfig(dbPath)

    # Create database
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    # Create keyspace
    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk

    # Verify keyspace exists
    check db.keyspaceExists("test_ks")

    # Delete keyspace
    let deleteResult = db.deleteKeyspace("test_ks")
    check deleteResult.isOk

    # Verify keyspace no longer exists
    check not db.keyspaceExists("test_ks")

    # Verify keyspace count decreased
    check db.keyspaceCount() == 0

    db.close()

  test "Delete non-existent keyspace fails":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    let deleteResult = db.deleteKeyspace("nonexistent")
    check deleteResult.isErr

    db.close()

  test "Delete keyspace removes directory":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    # Create keyspace
    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk

    # Get keyspace path
    let ksId = ksResult.value.inner.id
    let ksPath = dbPath / "keyspaces" / $ksId
    check dirExists(ksPath)

    # Delete keyspace
    let deleteResult = db.deleteKeyspace("test_ks")
    check deleteResult.isOk

    # Verify directory is removed
    check not dirExists(ksPath)

    db.close()

  test "Recreating deleted keyspace works":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    # Create and delete keyspace
    discard db.keyspace("test_ks")
    let deleteResult = db.deleteKeyspace("test_ks")
    check deleteResult.isOk

    # Try to get the deleted keyspace - should create new one
    let ksResult = db.keyspace("test_ks")
    check ksResult.isOk

    db.close()

  test "Delete one of multiple keyspaces":
    let config = db_config.newConfig(dbPath)
    let dbResult = open(config)
    check dbResult.isOk
    let db = dbResult.value

    # Create multiple keyspaces
    discard db.keyspace("ks1")
    discard db.keyspace("ks2")
    discard db.keyspace("ks3")

    check db.keyspaceCount() == 3

    # Delete one
    let deleteResult = db.deleteKeyspace("ks2")
    check deleteResult.isOk

    check db.keyspaceCount() == 2
    check db.keyspaceExists("ks1")
    check not db.keyspaceExists("ks2")
    check db.keyspaceExists("ks3")

    db.close()
