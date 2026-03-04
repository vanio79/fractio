# Unit tests for file module
# Tests for file constants and utilities

import unittest
import fractio/storage/file

suite "File Unit Tests":

  test "Magic bytes":
    check MAGIC_BYTES.len == 4
    check MAGIC_BYTES[0] == byte('F')
    check MAGIC_BYTES[1] == byte('J')
    check MAGIC_BYTES[2] == byte('L')
    check MAGIC_BYTES[3] == 3.byte

  test "File constants":
    check KEYSPACES_FOLDER == "keyspaces"
    check LOCK_FILE == "lock"
    check VERSION_MARKER == "version"
    check LSM_CURRENT_VERSION_MARKER == "current"

  test "Magic bytes as sequence":
    let magicSeq = @MAGIC_BYTES
    check magicSeq.len == 4
    check magicSeq[0] == byte('F')
    check magicSeq[1] == byte('J')
    check magicSeq[2] == byte('L')
    check magicSeq[3] == 3.byte
