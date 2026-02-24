# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for LSM Tree v2 - Compression Types

## Note: CompressionType encoding/decoding is tested via integration tests.
## This file contains basic sanity checks only.

import std/unittest
import std/options
import fractio/storage/lsm_tree_v2/error

suite "compression_type":
  test "compression type enum values":
    # Just verify the enum values exist
    check ord(ctNone) == 0
    check ord(ctLz4) == 1

  test "compression type default":
    # ctNone should be the default
    let ct: CompressionType = ctNone
    check ct == ctNone
