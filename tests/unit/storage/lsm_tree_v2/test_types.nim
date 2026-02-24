# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for LSM Tree v2 - Core Types

import std/unittest
import std/options
import fractio/storage/lsm_tree_v2/types

suite "value_type":
  test "value type toByte":
    check vtValue.toByte() == 0'u8
    check vtTombstone.toByte() == 1'u8
    check vtWeakTombstone.toByte() == 2'u8
    check vtIndirection.toByte() == 4'u8

  test "value type fromByte":
    check fromByte(0'u8) == some(vtValue)
    check fromByte(1'u8) == some(vtTombstone)
    check fromByte(2'u8) == some(vtWeakTombstone)
    check fromByte(4'u8) == some(vtIndirection)
    check fromByte(99'u8).isNone()

  test "isTombstone":
    check vtTombstone.isTombstone() == true
    check vtWeakTombstone.isTombstone() == true
    check vtValue.isTombstone() == false
    check vtIndirection.isTombstone() == false

  test "isIndirection":
    check vtIndirection.isIndirection() == true
    check vtValue.isIndirection() == false

suite "slice":
  test "newSlice from string":
    let s = newSlice("hello")
    check s.len == 5
    check s.data == "hello"

  test "newSlice from seq uint8":
    let data = @[1'u8, 2'u8, 3'u8]
    let s = newSlice(data)
    check s.len == 3
    check s.data == "\x01\x02\x03"

  test "newSlice from openArray":
    let data: seq[uint8] = @[1'u8, 2'u8, 3'u8]
    let s = newSlice(data)
    check s.len == 3

  test "emptySlice":
    let s = emptySlice()
    check s.isEmpty()
    check s.len == 0

  test "slice equality":
    let s1 = newSlice("hello")
    let s2 = newSlice("hello")
    let s3 = newSlice("world")
    check s1 == s2
    check s1 != s3

  test "slice comparison":
    let a = newSlice("a")
    let b = newSlice("b")
    check a < b
    check a <= b
    check b > a
    check b >= a
    check a < a == false
    check a <= a

  test "slice hash":
    let s1 = newSlice("hello")
    let s2 = newSlice("hello")
    let s3 = newSlice("world")
    check hash(s1) == hash(s2)
    check hash(s1) != hash(s3)

  test "slice toSeq":
    let s = newSlice("abc")
    let seq = s.toSeq()
    check seq.len == 3
    check seq[0] == 97'u8 # 'a'

  test "slice asString":
    let s = newSlice("hello")
    check s.asString() == "hello"

  test "slice index access":
    let s = newSlice("hello")
    check s[0] == 'h'
    check s[1] == 'e'

  test "slice slice access":
    let s = newSlice("hello")
    check s[0..2] == "hel"

suite "internal_key":
  test "newInternalKey from slice":
    let key = newInternalKey(newSlice("key"), 100, vtValue)
    check key.userKey == newSlice("key")
    check key.seqno == 100
    check key.valueType == vtValue

  test "newInternalKey from string":
    let key = newInternalKey("key", 100, vtValue)
    check key.userKey == newSlice("key")
    check key.seqno == 100

  test "internalKey isTombstone":
    let key1 = newInternalKey("key", 100, vtValue)
    let key2 = newInternalKey("key", 100, vtTombstone)
    check key1.isTombstone() == false
    check key2.isTombstone() == true

  test "internalKey equality":
    let key1 = newInternalKey("key", 100, vtValue)
    let key2 = newInternalKey("key", 100, vtValue)
    let key3 = newInternalKey("key", 101, vtValue)
    check key1 == key2
    check key1 != key3

  test "internalKey comparison by key then seqno desc":
    # Same key, higher seqno comes first
    let key1 = newInternalKey("key", 100, vtValue)
    let key2 = newInternalKey("key", 50, vtValue)
    check key1 < key2 # 100 > 50, so higher seqno is "smaller" (comes first)

    # Different key, normal ordering
    let key3 = newInternalKey("a", 100, vtValue)
    let key4 = newInternalKey("b", 50, vtValue)
    check key3 < key4

suite "internal_value":
  test "newInternalValue":
    let iv = newInternalValue(newSlice("key"), newSlice("value"), 1, vtValue)
    check iv.key.userKey == newSlice("key")
    check iv.value == newSlice("value")
    check iv.key.seqno == 1
    check iv.key.valueType == vtValue

  test "newInternalValue from strings":
    let iv = newInternalValue("key", "value", 1, vtValue)
    check iv.key.userKey == newSlice("key")
    check iv.value == newSlice("value")

  test "newTombstone":
    let tomb = newTombstone("key", 10)
    check tomb.key.isTombstone() == true
    check tomb.key.seqno == 10

  test "newWeakTombstone":
    let tomb = newWeakTombstone("key", 10)
    check tomb.key.isTombstone() == true
    check tomb.key.valueType == vtWeakTombstone

  test "internalValue isTombstone":
    let val = newInternalValue("key", "value", 1, vtValue)
    let tomb = newTombstone("key", 10)
    check val.isTombstone() == false
    check tomb.isTombstone() == true

suite "seqno":
  test "max valid seqno":
    check MAX_VALID_SEQNO == 0x7FFF_FFFF_FFFF_FFFF'u64
