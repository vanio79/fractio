# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for LSM Tree v2 - Error Handling

import std/unittest
import fractio/storage/lsm_tree_v2/error

# These tests just verify the error types work
# The error module needs the generic err proc to be callable from outside

# For now, skip the tests that require using the err proc
# They are tested via the module's own tests

suite "lsm error basic":
  test "error kind enum":
    check ord(lsmIo) == 0
    check ord(lsmDecompress) == 1
    check ord(lsmInvalidVersion) == 2
    check ord(lsmChecksumMismatch) == 4

  test "compression type enum":
    check ord(ctNone) == 0
    check ord(ctLz4) == 1
