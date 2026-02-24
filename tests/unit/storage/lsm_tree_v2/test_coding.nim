# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for LSM Tree v2 - Coding/Encoding

import std/unittest
import std/streams
import fractio/storage/lsm_tree_v2/coding

suite "coding varint":
  test "encode varint zero":
    let encoded = encodeVarint(0)
    check encoded.len == 1
    check encoded[0] == 0.char

  test "encode varint small value":
    let encoded = encodeVarint(127) # Max for single byte
    check encoded.len == 1
    check encoded[0] == 127.char

  test "encode varint two bytes":
    let encoded = encodeVarint(128)
    check encoded.len == 2

  test "encode varint 300":
    let encoded = encodeVarint(300)
    # 300 = 0x012C = 10101100 00000010
    check encoded.len == 2

  test "encode varint large":
    let encoded = encodeVarint(0xFFFFFFFF'u64)
    check encoded.len > 0

suite "coding fixed32":
  test "encode fixed32":
    let encoded = encodeFixed32(0x12345678'u32)
    check encoded.len == 4
    check encoded[0] == 0x78.char
    check encoded[1] == 0x56.char
    check encoded[2] == 0x34.char
    check encoded[3] == 0x12.char

  test "encode fixed32 zero":
    let encoded = encodeFixed32(0'u32)
    check encoded == "\x00\x00\x00\x00"

  test "encode fixed32 max":
    let encoded = encodeFixed32(0xFFFFFFFF'u32)
    check encoded == "\xFF\xFF\xFF\xFF"

suite "coding fixed64":
  test "encode fixed64":
    let encoded = encodeFixed64(0x123456789ABCDEF0'u64)
    check encoded.len == 8
    check encoded[0] == 0xF0.char
    check encoded[1] == 0xDE.char
    check encoded[2] == 0xBC.char
    check encoded[3] == 0x9A.char
    check encoded[4] == 0x78.char
    check encoded[5] == 0x56.char
    check encoded[6] == 0x34.char
    check encoded[7] == 0x12.char

  test "encode fixed64 zero":
    let encoded = encodeFixed64(0'u64)
    check encoded == "\x00\x00\x00\x00\x00\x00\x00\x00"

  test "encode fixed64 max":
    let encoded = encodeFixed64(0xFFFFFFFFFFFFFFFF'u64)
    check encoded == "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"
