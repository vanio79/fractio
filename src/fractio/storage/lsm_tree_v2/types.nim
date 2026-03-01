# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License

## LSM Tree v2 - Core Types
##
## OPTIMIZED: Hot path comparisons use templates for true inlining

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

# OPTIMIZED: Template for true inlining
template isTombstone*(vt: ValueType): bool =
  vt == vtTombstone or vt == vtWeakTombstone

func toByte*(vt: ValueType): uint8 =
  case vt
  of vtValue: 0'u8
  of vtTombstone: 1'u8
  of vtWeakTombstone: 2'u8
  of vtIndirection: 4'u8

# ============================================================================
# SeqNo comparisons - OPTIMIZED: Templates for true inlining
# ============================================================================

template `==`*(a, b: SeqNo): bool = int(a) == int(b)
template `<`*(a, b: SeqNo): bool = int(a) < int(b)
template `<=`*(a, b: SeqNo): bool = int(a) <= int(b)
template `>`*(a, b: SeqNo): bool = int(a) > int(b)
template `>=`*(a, b: SeqNo): bool = int(a) >= int(b)
proc `$`*(s: SeqNo): string = $int(s)

# ============================================================================
# InternalKey - Simple string-based key
# ============================================================================

type
  InternalKey* = object
    ## Key stored in skip list nodes - owns its string data
    userKey*: string
    seqno*: SeqNo
    valueType*: ValueType

# OPTIMIZED: Template for true inlining
template newInternalKey*(userKey: string, seqno: SeqNo,
    valueType: ValueType): InternalKey =
  InternalKey(userKey: userKey, seqno: seqno, valueType: valueType)

# OPTIMIZED: Template for true inlining
template isTombstone*(key: InternalKey): bool =
  key.valueType == vtTombstone or key.valueType == vtWeakTombstone

# Hash support for InternalKey
proc hash*(k: InternalKey): Hash =
  let h1 = hash(k.userKey)
  let h2 = int(hash(k.seqno))
  let h3 = int(hash(k.valueType))
  result = Hash(h1 xor h2 xor h3)

# ============================================================================
# InternalKey comparisons - OPTIMIZED: Templates for true inlining
# ============================================================================

# OPTIMIZED: Template for true inlining - called on every skip list comparison
template `==`*(a, b: InternalKey): bool =
  a.userKey == b.userKey and int(a.seqno) == int(b.seqno) and a.valueType == b.valueType

# OPTIMIZED: Template for true inlining - called on every skip list traversal
# Ordering: userKey ASC, seqno DESC, valueType ASC
template `<`*(a, b: InternalKey): bool =
  if a.userKey != b.userKey:
    a.userKey < b.userKey
  elif int(a.seqno) != int(b.seqno):
    int(a.seqno) > int(b.seqno) # Descending seqno
  else:
    a.valueType < b.valueType

# ============================================================================
# Comparison: InternalKey for heap ordering
# ============================================================================

template cmpInternalKey*(a, b: InternalKey): int =
  if a.userKey < b.userKey: -1
  elif a.userKey > b.userKey: 1
  elif int(b.seqno) < int(a.seqno): -1
  elif int(b.seqno) > int(a.seqno): 1
  elif int(a.valueType) < int(b.valueType): -1
  elif int(a.valueType) > int(b.valueType): 1
  else: 0

# ============================================================================
# String comparison helper
# ============================================================================

template cmp*(a, b: string): int =
  if a < b: -1
  elif a > b: 1
  else: 0

# ============================================================================
# Other
# ============================================================================

proc `$`*(t: TableId): string = $uint64(t)

const
  MAX_VALID_SEQNO* = SeqNo(0x7FFFFFFFFFFFFFFF'i64)

type
  FileFlags* = enum
    frRead = 0
    frWrite = 1
    frAppend = 1024
    frCreate = 64
