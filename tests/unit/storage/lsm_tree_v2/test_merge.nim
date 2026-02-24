# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for LSM Tree v2 - Merge Iterator

import std/unittest
import std/sequtils
import fractio/storage/lsm_tree_v2/merge
import fractio/storage/lsm_tree_v2/types
import fractio/storage/lsm_tree_v2/error

suite "merge":
  test "merge simple":
    # Create two sorted sequences
    let source1 = @[
      newInternalValue("a", "value1", 0, vtValue)
    ]

    let source2 = @[
      newInternalValue("b", "value2", 0, vtValue)
    ]

    let merged = mergeSequences(@[source1, source2])
    check merged.len == 2
    check merged[0].key.userKey.asString() == "a"
    check merged[1].key.userKey.asString() == "b"

  test "merge multiple sources":
    let source1 = @[
      newInternalValue("a", "v1", 0, vtValue),
      newInternalValue("c", "v3", 2, vtValue)
    ]
    let source2 = @[
      newInternalValue("b", "v2", 1, vtValue),
      newInternalValue("d", "v4", 3, vtValue)
    ]

    let merged = mergeSequences(@[source1, source2])
    check merged.len == 4
    check merged[0].key.userKey.asString() == "a"
    check merged[1].key.userKey.asString() == "b"
    check merged[2].key.userKey.asString() == "c"
    check merged[3].key.userKey.asString() == "d"

  test "merge empty sources":
    let empty: seq[InternalValue] = @[]
    let source1 = @[
      newInternalValue("a", "v1", 0, vtValue)
    ]

    let merged = mergeSequences(@[empty, source1])
    check merged.len == 1

  test "merge all empty sources":
    let empty1: seq[InternalValue] = @[]
    let empty2: seq[InternalValue] = @[]

    let merged = mergeSequences(@[empty1, empty2])
    check merged.len == 0
