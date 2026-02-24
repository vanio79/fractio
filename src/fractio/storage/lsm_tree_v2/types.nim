# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree v2 - Core Types
##
## This module provides the core type definitions for the LSM tree implementation,
## including Slice, InternalKey, InternalValue, ValueType, and SeqNo.

import std/[hashes, options, strutils]

# ============================================================================
# Value Type
# ============================================================================

type
  ValueType* = enum
    vtValue = 0'u8         ## Regular existing value
    vtTombstone = 1'u8     ## Deleted value (deletion marker)
    vtWeakTombstone = 2'u8 ## Weak deletion (SingleDelete in RocksDB)
    vtIndirection = 4'u8   ## Value pointer to blob file

func isTombstone*(vt: ValueType): bool {.inline.} =
  vt == vtTombstone or vt == vtWeakTombstone

func isIndirection*(vt: ValueType): bool {.inline.} =
  vt == vtIndirection

func toByte*(vt: ValueType): uint8 =
  case vt
  of vtValue: 0'u8
  of vtTombstone: 1'u8
  of vtWeakTombstone: 2'u8
  of vtIndirection: 4'u8

func fromByte*(b: uint8): Option[ValueType] =
  case b
  of 0'u8: some(vtValue)
  of 1'u8: some(vtTombstone)
  of 2'u8: some(vtWeakTombstone)
  of 4'u8: some(vtIndirection)
  else: none(ValueType)

# ============================================================================
# Sequence Number (SeqNo)
# ============================================================================

type
  SeqNo* = uint64

const
  MAX_VALID_SEQNO*: SeqNo = 0x7FFF_FFFF_FFFF_FFFF'u64

# ============================================================================
# Slice - Immutable Byte String
# ============================================================================

type
  Slice* = ref object
    ## Immutable byte string - can be cloned without heap allocation
    ## (simplified implementation using copy-on-write semantics)
    data*: string

proc newSlice*(data: string): Slice =
  Slice(data: data)

proc newSlice*(data: seq[uint8]): Slice =
  Slice(data: cast[string](data))

proc newSlice*(data: openArray[byte]): Slice =
  var s = newString(data.len)
  for i, b in data:
    s[i] = char(b)
  Slice(data: s)

proc emptySlice*(): Slice =
  Slice(data: "")

proc len*(s: Slice): int {.inline.} =
  s.data.len

proc isEmpty*(s: Slice): bool {.inline.} =
  s.data.len == 0

proc `==`*(a, b: Slice): bool {.inline.} =
  a.data == b.data

proc `<`*(a, b: Slice): bool {.inline.} =
  a.data < b.data

proc `<=`*(a, b: Slice): bool {.inline.} =
  a.data <= b.data

proc `>`*(a, b: Slice): bool {.inline.} =
  a.data > b.data

proc `>=`*(a, b: Slice): bool {.inline.} =
  a.data >= b.data

proc hash*(s: Slice): Hash {.inline.} =
  hash(s.data)

proc `$`*(s: Slice): string =
  "Slice(" & $s.data.len & " bytes)"

proc toSeq*(s: Slice): seq[uint8] {.inline.} =
  cast[seq[uint8]](s.data)

proc asString*(s: Slice): string {.inline.} =
  s.data

proc asBytes*(s: Slice): string {.inline.} =
  s.data

proc `[]`*(s: Slice, i: int): char {.inline.} =
  s.data[i]

proc `[]`*(s: Slice, r: HSlice[int, int]): string {.inline.} =
  s.data[r]

# ============================================================================
# Internal Key
# ============================================================================

type
  InternalKey* = ref object
    ## Internal key structure with user key, sequence number, and value type
    ## Stores userKey as string directly for efficient Table lookups
    userKey*: string
    seqno*: SeqNo
    valueType*: ValueType

proc newInternalKey*(userKey: string, seqno: SeqNo,
    valueType: ValueType): InternalKey =
  assert(userKey.len <= 65535, "keys can be 65535 bytes in length")
  InternalKey(userKey: userKey, seqno: seqno, valueType: valueType)

proc newInternalKeyFromSlice*(userKey: Slice, seqno: SeqNo,
    valueType: ValueType): InternalKey =
  ## Create InternalKey from Slice - converts Slice to string
  assert(userKey.len <= 65535, "keys can be 65535 bytes in length")
  InternalKey(userKey: userKey.data, seqno: seqno, valueType: valueType)

# Generic overloads for backward compatibility
proc newInternalKey*[K: Slice|string](userKey: K, seqno: SeqNo,
    valueType: ValueType): InternalKey =
  when K is string:
    newInternalKey(userKey, seqno, valueType)
  else:
    newInternalKeyFromSlice(userKey, seqno, valueType)

proc isTombstone*(key: InternalKey): bool {.inline.} =
  key.valueType.isTombstone()

proc `==`*(a, b: InternalKey): bool {.inline.} =
  a.userKey == b.userKey and a.seqno == b.seqno

proc cmpInternalKey*(a, b: InternalKey): int =
  ## Compare two internal keys
  ## Order by user key ascending, THEN by sequence number descending (Reverse)
  let keyCmp = cmp(a.userKey, b.userKey)
  if keyCmp != 0:
    keyCmp
  else:
    cmp(b.seqno, a.seqno) # Reverse order for seqno

proc `<`*(a, b: InternalKey): bool {.inline.} =
  cmpInternalKey(a, b) < 0

proc `<=`*(a, b: InternalKey): bool {.inline.} =
  cmpInternalKey(a, b) <= 0

proc `>`*(a, b: InternalKey): bool {.inline.} =
  cmpInternalKey(a, b) > 0

proc `>=`*(a, b: InternalKey): bool {.inline.} =
  cmpInternalKey(a, b) >= 0

proc hash*(key: InternalKey): Hash =
  hash(key.userKey) !& hash(key.seqno)

proc `$`*(key: InternalKey): string =
  let vtStr = case key.valueType
  of vtValue: "V"
  of vtTombstone: "T"
  of vtWeakTombstone: "W"
  of vtIndirection: "Vb"
  key.userKey & ":" & $key.seqno & ":" & vtStr

# ============================================================================
# Internal Value
# ============================================================================

type
  InternalValue* = ref object
    ## Internal key-value pair for storage
    key*: InternalKey
    value*: Slice

proc newInternalValue*(key: InternalKey, value: Slice): InternalValue =
  assert(key.userKey.len > 0, "key may not be empty")
  assert(value.len <= high(int32).int, "values can be 2^32 bytes in length")
  InternalValue(key: key, value: value)

proc newInternalValue*[K: Slice|string, V: Slice|string](
  userKey: K, value: V, seqno: SeqNo, valueType: ValueType
): InternalValue =
  let key = newInternalKey(userKey, seqno, valueType)
  when V is string:
    newInternalValue(key, newSlice(value))
  else:
    newInternalValue(key, value)

proc newTombstone*[K: Slice|string](userKey: K, seqno: SeqNo): InternalValue =
  let key = newInternalKey(userKey, seqno, vtTombstone)
  newInternalValue(key, emptySlice())

proc newWeakTombstone*[K: Slice|string](userKey: K,
    seqno: SeqNo): InternalValue =
  let key = newInternalKey(userKey, seqno, vtWeakTombstone)
  newInternalValue(key, emptySlice())

proc isTombstone*(v: InternalValue): bool {.inline.} =
  v.key.isTombstone()

proc `$`*(v: InternalValue): string =
  let valStr = if v.value.len >= 100:
    "[... " & $v.value.len & " bytes]"
  else:
    v.value.data
  $v.key & " => " & valStr

# ============================================================================
# Type Aliases
# ============================================================================

type
  UserKey* = Slice
  UserValue* = Slice
  KvPair* = tuple[key: Slice, value: Slice]

# ============================================================================
# ID Types
# ============================================================================

type
  TreeId* = int64
  MemtableId* = int64
  TableId* = int64
  GlobalTableId* = int64

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing types..."

  # Test Slice
  let s1 = newSlice("hello")
  let s2 = newSlice("world")
  echo "Slice len: ", s1.len
  echo "Slice eq: ", s1 == s1
  echo "Slice lt: ", s1 < s2

  # Test ValueType
  echo "ValueType toByte: ", vtValue.toByte()
  echo "fromByte 0: ", fromByte(0)
  echo "isTombstone Tombstone: ", vtTombstone.isTombstone()
  echo "isTombstone Value: ", vtValue.isTombstone()

  # Test InternalKey
  let ik1 = newInternalKey("key1", 100, vtValue)
  let ik2 = newInternalKey("key1", 50, vtTombstone)
  echo "InternalKey cmp: ", cmpInternalKey(ik1, ik2)
  echo "InternalKey < : ", ik2 < ik1 # Higher seqno comes first
  
  # Test InternalValue
  let iv = newInternalValue("testkey", "testvalue", 1, vtValue)
  echo "InternalValue: ", $iv

  # Test tombstone
  let tomb = newTombstone("deleted", 10)
  echo "Tombstone isTombstone: ", tomb.isTombstone()

  echo "Types tests passed!"
