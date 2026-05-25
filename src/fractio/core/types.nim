# Core types for Fractio distributed database
# Thread-safe by design - immutable structs and atomic operations

import hashes
import tables
import sets
import times
import random

const
  ULID_SIZE* = 16        ## ULID binary size in bytes
  ULID_STRING_SIZE* = 26 ## ULID string representation size

type
  # ULID - Universally Unique Lexicographically Sortable Identifier
  ULID* = object
    ## 128-bit unique identifier, lexicographically sortable.
    ## Binary representation: 16 bytes (48-bit timestamp + 80-bit randomness)
    ## String representation: 26 characters (Crockford's base32)
    data*: array[ULID_SIZE, uint8]

# ============================================================================
# ULID Operations (must come first as other types depend on these)
# ============================================================================

proc ZeroULID*(): ULID =
  ## Return an all-zero ULID (useful for placeholder/default values)
  for i in 0 ..< ULID_SIZE:
    result.data[i] = 0'u8

proc `==`*(a, b: ULID): bool =
  ## Compare two ULIDs for equality
  for i in 0 ..< ULID_SIZE:
    if a.data[i] != b.data[i]:
      return false
  true

proc `<`*(a, b: ULID): bool =
  ## Compare two ULIDs for ordering (lexicographic by timestamp then randomness)
  for i in 0 ..< ULID_SIZE:
    if a.data[i] < b.data[i]:
      return true
    elif a.data[i] > b.data[i]:
      return false
  false

proc genULID*(tsNs: int64): ULID {.raises: [].} =
  ## Generate a new ULID with an explicit nanosecond timestamp.
  ## The timestamp is converted to milliseconds and encoded in the first 48 bits
  ## (6 bytes, big-endian). The remaining 80 bits (10 bytes) are filled with
  ## cryptographically random data for uniqueness.
  ## Use this in distributed contexts where the timestamp comes from SharedTimer.
  let ms = tsNs div 1_000_000
  # Encode timestamp in first 6 bytes (48-bit big-endian milliseconds)
  for i in 0 ..< 6:
    result.data[i] = uint8((ms shr ((5 - i) * 8)) and 0xFF)
  # Fill remaining 10 bytes with random entropy
  {.cast(raises: []).}:
    try:
      var rng = initRand()
      for i in 6 ..< 15:
        result.data[i] = uint8(rng.next() and 0xFF)
      # Last byte: use lower 8 bits to avoid any correlation
      result.data[15] = uint8(rng.next() and 0xFF)
    except CatchableError:
      # Fallback: counter-based entropy if random fails
      for i in 6 ..< 16:
        result.data[i] = uint8((i * 17 + int(ms and 0xFF)) and 0xFF)

proc localTimeNs*(): int64 {.inline, raises: [].} =
  ## Current local wall-clock time in nanoseconds. Uses getTime() (NOT SharedTimer).
  ## Only use in tests and non-distributed code. Production code must use TimeProvider.now().
  let t = getTime()
  t.toUnix * 1_000_000_000 + t.nanosecond.int64

proc genULIDLocal*(): ULID {.raises: [].} =
  ## Generate a ULID using the LOCAL clock. Only for tests and non-distributed code.
  ## Production code must use genULID(tsNs) with a SharedTimer-sourced timestamp.
  genULID(localTimeNs())

proc ulidFromString*(s: string): ULID =
  ## Parse a 26-character ULID string to binary.
  ## ULID encodes 128 bits into 26 base32 chars (5 bits each = 130 bits).
  ## The first 2 bits are padding zeros, so we decode MSB to LSB order.
  doAssert s.len == ULID_STRING_SIZE, "ULID string must be 26 characters"
  const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

  proc charVal(c: char): int =
    for i, ch in alphabet:
      if ch == c: return i
    return 0

  var bitIdx = 0
  for i in 0 ..< 26:
    let v = charVal(s[i])
    for j in countdown(4, 0):
      # Skip padding bits (first 2 bits of first char, which are bits 4 and 3)
      if not (i == 0 and j >= 3):
        if bitIdx < 128:
          let bit = (v shr j) and 1
          let byteIdx = bitIdx div 8
          let bitInByte = 7 - (bitIdx mod 8)
          result.data[byteIdx] = result.data[byteIdx] or (uint8(
              bit) shl bitInByte)
        inc bitIdx

proc `$`*(u: ULID): string =
  ## Convert ULID to 26-character string representation.
  ## ULID encodes 128 bits into 26 base32 chars (5 bits each = 130 bits).
  ## The first 2 bits are padding zeros, so we encode MSB to LSB order.
  const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
  result = newString(26)
  var bitIdx = 0

  for i in 0 ..< 26:
    var v = 0
    for j in 0 ..< 5:
      v = v shl 1
      # First 2 bits of first char are padding (zeros), skip them
      if not (i == 0 and j < 2):
        if bitIdx < 128:
          let byteIdx = bitIdx div 8
          let bitInByte = 7 - (bitIdx mod 8)
          v = v or int((u.data[byteIdx] shr bitInByte) and 1)
        inc bitIdx
    result[i] = alphabet[v]

proc ulidToBytes*(u: ULID): string =
  ## Convert ULID to 16-byte binary string for storage
  result = newString(ULID_SIZE)
  for i in 0 ..< ULID_SIZE:
    result[i] = char(u.data[i])

proc ulidFromBytes*(data: string): ULID =
  ## Parse 16-byte binary string to ULID
  doAssert data.len == ULID_SIZE, "ULID binary must be 16 bytes"
  for i in 0 ..< ULID_SIZE:
    result.data[i] = uint8(data[i])

proc ulidTimestamp*(u: ULID): int64 =
  ## Extract the 48-bit timestamp (milliseconds since Unix epoch)
  result = 0
  for i in 0 ..< 6:
    result = (result shl 8) or int64(u.data[i])

type
  # Basic data types
  DataType* = enum
    dtInt, dtFloat, dtString, dtBool, dtDate, dtDateTime, dtBytes, dtULID

  Timestamp* = int64
    ## Nanosecond-precision Unix timestamp

  Constraint* = object
    nullable*: bool
    unique*: bool
    primaryKey*: bool
    defaultValue*: ValueRef

  ValueRef* = ref object
    case kind*: DataType
    of dtInt:
      intValue*: int64
    of dtFloat:
      floatValue*: float64
    of dtString:
      strValue*: string
    of dtBool:
      boolValue*: bool
    of dtDate:
      dateValue*: int64
    of dtDateTime:
      datetimeValue*: int64
    of dtBytes:
      bytesValue*: seq[uint8]
    of dtULID:
      ulidValue*: ULID

  ColumnDef* = object
    name*: string
    dataType*: DataType
    constraints*: Constraint

  RowID* = distinct ULID
    ## Row identifier - ULID for globally unique, sortable row IDs

  Row* = ref object
    id*: RowID
    values*: seq[ValueRef]
    createdAt*: int64
    updatedAt*: int64
    version*: int64

  Table* = ref object
    name*: string
    columns*: seq[ColumnDef]
    rows*: seq[Row]
    indexes*: TableIndexes
    mutex*: pointer
    version*: int64

  TableIndexes* = object
    columnIndices*: tables.Table[string, int]

  Schema* = ref object
    tables*: tables.Table[string, Table]
    mutex*: pointer
    version*: int64

  TransactionID* = distinct ULID
    ## Transaction identifier - ULID for globally unique, sortable transaction IDs

  Transaction* = ref object
    id*: TransactionID
    timestamp*: int64
    status*: TransactionStatus
    readSnapshot*: int64
    mutatedTables*: HashSet[string]
    mutex*: pointer

  TransactionStatus* = enum
    tsActive, tsCommitted, tsAborted

  TableId* = distinct ULID
    ## Table identifier - ULID for globally unique table IDs
    ## Enables distributed table creation without ID conflicts

  SpaceID* = distinct ULID
    ## Space identifier - ULID for globally unique space IDs

  NodeID* = distinct string

  NodeInfo* = object
    id*: NodeID
    address*: string
    port*: uint16
    role*: NodeRole
    capacity*: int
    used*: int
    load*: int

  NodeRole* = enum
    nrCoordinator, nrPrimary, nrSecondary, nrClient

# Helper templates for safe type conversions
template int64Value*(v: ValueRef): int64 =
  case v.kind
  of dtInt: v.intValue
  else: 0

template float64Value*(v: ValueRef): float64 =
  case v.kind
  of dtFloat: v.floatValue
  else: 0.0

template stringValue*(v: ValueRef): string =
  case v.kind
  of dtString: v.strValue
  else: ""

template boolValue*(v: ValueRef): bool =
  case v.kind
  of dtBool: v.boolValue
  else: false

# Constructors for ValueRef
proc newValueRef*(i: int64): ValueRef =
  result = ValueRef(kind: dtInt, intValue: i)

proc newValueRef*(f: float64): ValueRef =
  result = ValueRef(kind: dtFloat, floatValue: f)

proc newValueRef*(s: string): ValueRef =
  result = ValueRef(kind: dtString, strValue: s)

proc newValueRef*(b: bool): ValueRef =
  result = ValueRef(kind: dtBool, boolValue: b)

proc newValueRef*(bytes: seq[uint8]): ValueRef =
  result = ValueRef(kind: dtBytes, bytesValue: bytes)

proc newValueRef*(u: ULID): ValueRef =
  result = ValueRef(kind: dtULID, ulidValue: u)

proc newRow*(id: RowID = RowID(ZeroULID()), createdAtMs: int64): Row =
  ## Create a new Row with an explicit creation timestamp in milliseconds.
  Row(id: id, values: @[], createdAt: createdAtMs, updatedAt: createdAtMs, version: 1)

# TransactionID operations (ULID-based)
proc `==`*(a, b: TransactionID): bool = ULID(a) == ULID(b)
proc `!=`*(a, b: TransactionID): bool = not (a == b)
proc `<`*(a, b: TransactionID): bool = ULID(a) < ULID(b)
proc `$`*(id: TransactionID): string = $ULID(id)
proc hash*(id: TransactionID): Hash = hash($ULID(id))

# RowID operations (ULID-based)
proc `==`*(a, b: RowID): bool = ULID(a) == ULID(b)
proc `!=`*(a, b: RowID): bool = not (a == b)
proc `<`*(a, b: RowID): bool = ULID(a) < ULID(b)
proc `$`*(id: RowID): string = $ULID(id)
proc hash*(id: RowID): Hash = hash($ULID(id))

# TableId operations (ULID-based)
proc `==`*(a, b: TableId): bool = ULID(a) == ULID(b)
proc `!=`*(a, b: TableId): bool = not (a == b)
proc `<`*(a, b: TableId): bool = ULID(a) < ULID(b)
proc `$`*(id: TableId): string = $ULID(id)
proc hash*(id: TableId): Hash = hash($ULID(id))

# SpaceID operations (ULID-based)
proc `==`*(a, b: SpaceID): bool = ULID(a) == ULID(b)
proc `!=`*(a, b: SpaceID): bool = not (a == b)
proc `<`*(a, b: SpaceID): bool = ULID(a) < ULID(b)
proc `$`*(id: SpaceID): string = $ULID(id)
proc hash*(id: SpaceID): Hash = hash($ULID(id))

# Transaction ID generation - uses ULID for globally unique IDs
proc genTransactionID*(tsNs: int64): TransactionID =
  ## Generate TransactionID with nanosecond timestamp (from SharedTimer).
  TransactionID(genULID(tsNs))

proc genTransactionIDLocal*(): TransactionID =
  ## Generate TransactionID using LOCAL clock. Only for tests.
  genTransactionID(localTimeNs())

# Row ID generation - uses ULID for globally unique IDs
proc genRowID*(tsNs: int64): RowID =
  ## Generate RowID with nanosecond timestamp (from SharedTimer).
  RowID(genULID(tsNs))

proc genRowIDLocal*(): RowID =
  ## Generate RowID using LOCAL clock. Only for tests.
  genRowID(localTimeNs())

# TableId generation - uses ULID for globally unique IDs
proc genTableId*(tsNs: int64): TableId =
  ## Generate TableId with nanosecond timestamp (from SharedTimer).
  TableId(genULID(tsNs))

proc genTableIdLocal*(): TableId =
  ## Generate TableId using LOCAL clock. Only for tests.
  genTableId(localTimeNs())

# SpaceID generation - uses ULID for globally unique IDs
proc genSpaceID*(tsNs: int64): SpaceID =
  ## Generate SpaceID with nanosecond timestamp (from SharedTimer).
  SpaceID(genULID(tsNs))

proc genSpaceIDLocal*(): SpaceID =
  ## Generate SpaceID using LOCAL clock. Only for tests.
  genSpaceID(localTimeNs())

# Convenience conversions for TransactionID
proc transactionIDFromBytes*(data: string): TransactionID =
  TransactionID(ulidFromBytes(data))

proc transactionIDFromString*(s: string): TransactionID =
  TransactionID(ulidFromString(s))

proc transactionIDToBytes*(id: TransactionID): string =
  ulidToBytes(ULID(id))

# Convenience conversions for TableId
proc tableIdFromBytes*(data: string): TableId =
  TableId(ulidFromBytes(data))

proc tableIdFromString*(s: string): TableId =
  TableId(ulidFromString(s))

proc tableIdToBytes*(id: TableId): string =
  ulidToBytes(ULID(id))

# Convenience conversions for SpaceID
proc spaceIDFromBytes*(data: string): SpaceID =
  SpaceID(ulidFromBytes(data))

proc spaceIDFromString*(s: string): SpaceID =
  SpaceID(ulidFromString(s))

proc spaceIDToBytes*(id: SpaceID): string =
  ulidToBytes(ULID(id))

# Zero/invalid ID constants (procs must be defined first)
# These are template-based to avoid compile-time evaluation issues
template zeroTransactionID*(): TransactionID = TransactionID(ZeroULID())
template zeroRowID*(): RowID = RowID(ZeroULID())
template zeroTableId*(): TableId = TableId(ZeroULID())
template zeroSpaceID*(): SpaceID = SpaceID(ZeroULID())

# isZero checks for comparing against zero IDs
template isZero*(id: TransactionID): bool = id == zeroTransactionID()
template isZero*(id: RowID): bool = id == zeroRowID()
template isZero*(id: TableId): bool = id == zeroTableId()
template isZero*(id: SpaceID): bool = id == zeroSpaceID()

# =============================================================================
# NodeID Operations
# =============================================================================

proc `$`*(id: NodeID): string =
  ## String representation of NodeID
  string(id)

proc `==`*(a, b: NodeID): bool =
  ## Equality comparison
  string(a) == string(b)

proc hash*(id: NodeID): Hash =
  ## Hash for use in tables
  hash(string(id))
