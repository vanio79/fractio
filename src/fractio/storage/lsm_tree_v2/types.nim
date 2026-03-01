# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License

## LSM Tree v2 - Core Types
##
## Simple string-based keys for clarity and maintainability

import std/hashes

# ============================================================================
# Domain Types
# ============================================================================

type
  TreeId* = distinct uint64
  TableId* = distinct uint64
  GlobalTableId* = distinct uint64
  MemtableId* = distinct int64
  SeqNo* = distinct int64

  ValueType* = enum
    vtValue = 0'u8
    vtTombstone = 1'u8
    vtWeakTombstone = 2'u8
    vtIndirection = 4'u8

func isTombstone*(vt: ValueType): bool {.inline.} =
  vt == vtTombstone or vt == vtWeakTombstone

func toByte*(vt: ValueType): uint8 =
  case vt
  of vtValue: 0'u8
  of vtTombstone: 1'u8
  of vtWeakTombstone: 2'u8
  of vtIndirection: 4'u8

# ============================================================================
# InternalKey - Simple string-based key
# ============================================================================

type
  InternalKey* = object
    ## Key stored in skip list nodes - owns its string data
    userKey*: string
    seqno*: SeqNo
    valueType*: ValueType

proc newInternalKey*(userKey: string, seqno: SeqNo,
    valueType: ValueType): InternalKey {.inline.} =
  result = InternalKey(
    userKey: userKey,
    seqno: seqno,
    valueType: valueType
  )

proc isTombstone*(key: InternalKey): bool {.inline.} =
  key.valueType.isTombstone()

# Hash support for InternalKey
proc hash*(k: InternalKey): Hash =
  let h1 = hash(k.userKey)
  let h2 = int(hash(k.seqno))
  let h3 = int(hash(k.valueType))
  result = Hash(h1 xor h2 xor h3)

# Compare two InternalKeys for ordering
proc `==`*(a, b: InternalKey): bool =
  a.userKey == b.userKey and int(a.seqno) == int(b.seqno) and a.valueType == b.valueType

proc `<`*(a, b: InternalKey): bool =
  if a.userKey != b.userKey: return a.userKey < b.userKey
  if int(a.seqno) != int(b.seqno): return int(a.seqno) > int(b.seqno) # Descending seqno
  return a.valueType < b.valueType

# ============================================================================
# Comparison: InternalKey for heap ordering
# ============================================================================

proc cmpInternalKey*(a, b: InternalKey): int =
  let cmpKey = cmp(a.userKey, b.userKey)
  if cmpKey != 0: return cmpKey
  let cmpSeq = cmp(int(b.seqno), int(a.seqno)) # Descending seqno
  if cmpSeq != 0: return cmpSeq
  return cmp(int(a.valueType), int(b.valueType))

# String comparison helper
proc cmp*(a, b: string): int =
  if a < b: return -1
  if a > b: return 1
  return 0

# ============================================================================
# SeqNo comparisons
# ============================================================================

proc `==`*(a, b: SeqNo): bool = int(a) == int(b)
proc `<`*(a, b: SeqNo): bool = int(a) < int(b)
proc `<=`*(a, b: SeqNo): bool = int(a) <= int(b)
proc `>`*(a, b: SeqNo): bool = int(a) > int(b)
proc `>=`*(a, b: SeqNo): bool = int(a) >= int(b)
proc `$`*(s: SeqNo): string = $int(s)

# Define `$` for TableId
proc `$`*(t: TableId): string = $uint64(t)

const
  MAX_VALID_SEQNO* = SeqNo(0x7FFFFFFFFFFFFFFF'i64)

type
  FileFlags* = enum
    frRead = 0
    frWrite = 1
    frAppend = 1024
    frCreate = 64
