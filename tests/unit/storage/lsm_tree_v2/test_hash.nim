# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for LSM Tree v2 - Hash Functions

import std/unittest
import fractio/storage/lsm_tree_v2/hash
import fractio/storage/lsm_tree_v2/types

suite "hash":
  test "hash64 empty":
    let h = hash64("")
    # FNV offset basis
    check h == 14695981039346656037'u64

  test "hash64 different inputs produce different outputs":
    let h1 = hash64("a")
    let h2 = hash64("b")
    let h3 = hash64("c")
    check h1 != h2
    check h2 != h3
    check h1 != h3

  test "hash64 hello world":
    let h = hash64("hello world")
    check h != 0'u64

  test "hash64 seq of uint8":
    let data = @[0'u8, 0'u8, 0'u8]
    let h = hash64(data)
    check h != 0'u64

  test "hash64 consistency":
    let h1 = hash64("hello")
    let h2 = hash64("hello")
    check h1 == h2

  test "hash128 empty":
    let h = hash128("")
    check h.lo != 0 or h.hi != 0

  test "hash128 different data produces different hashes":
    let h1 = hash128("test1")
    let h2 = hash128("test2")
    check h1 != h2

  test "hash128 consistency":
    let h1 = hash128("hello")
    let h2 = hash128("hello")
    check h1 == h2
