# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License

## LSM Tree v2 - Core Types with Arena-allocated keys
##
## Uses KeySlice for keys - borrowed from a shared arena
## This eliminates per-key GC tracking overhead

import std/hashes
import arena
export arena

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
# InternalKey - Uses KeySlice (borrowed from arena)
# ============================================================================

type
  InternalKey* = object
    ## Key stored in skip list nodes - borrows data from arena
    userKey*: KeySlice
    seqno*: SeqNo
    valueType*: ValueType

proc newInternalKey*(userKey: KeySlice, seqno: SeqNo,
    valueType: ValueType): InternalKey {.inline.} =
  ## Create an InternalKey with a KeySlice (already in arena)
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
# SearchKey - Borrows from caller's string (temporary, for lookup)
# ============================================================================

type
  SearchKey* = object
    ## Temporary search key - borrows from caller's string
    userKey*: KeySlice
    seqno*: SeqNo
    valueType*: ValueType

proc newSearchKey*(userKey: string, seqno: SeqNo,
    valueType: ValueType): SearchKey {.inline.} =
  ## Create a SearchKey that borrows from a string (zero-copy, temporary!)
  ## WARNING: The string must outlive the SearchKey
  result = SearchKey(
    userKey: KeySlice(
      data: if userKey.len > 0: cast[ptr UncheckedArray[byte]](userKey[
          0].unsafeAddr) else: nil,
      len: userKey.len
    ),
    seqno: seqno,
    valueType: valueType
  )

proc isTombstone*(key: SearchKey): bool {.inline.} =
  key.valueType.isTombstone()

# ============================================================================
# Comparison: SearchKey vs InternalKey
# ============================================================================

proc `==`*(a: SearchKey, b: InternalKey): bool {.inline.} =
  a.userKey == b.userKey and int(a.seqno) == int(b.seqno) and a.valueType == b.valueType

proc `<`*(a: SearchKey, b: InternalKey): bool {.inline.} =
  if a.userKey != b.userKey:
    return a.userKey < b.userKey
  if int(a.seqno) != int(b.seqno):
    return int(a.seqno) > int(b.seqno)
  result = a.valueType < b.valueType

proc `<`*(a: InternalKey, b: SearchKey): bool {.inline.} =
  not (b == a) and not (b < a)

# ============================================================================
# Comparison: InternalKey for heap ordering
# ============================================================================

proc cmpInternalKey*(a, b: InternalKey): int =
  let cmpKey = cmp(a.userKey, b.userKey)
  if cmpKey != 0: return cmpKey
  let cmpSeq = cmp(int(b.seqno), int(a.seqno)) # Descending seqno
  if cmpSeq != 0: return cmpSeq
  return cmp(int(a.valueType), int(b.valueType))

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
