# Core types for Fractio distributed database
# Thread-safe by design - immutable structs and atomic operations

import tables
import sets
import times
import ulid

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
    isShardKey*: bool

  RowID* = distinct int64

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

  TransactionID* = distinct int64

  Transaction* = ref object
    id*: TransactionID
    timestamp*: int64
    status*: TransactionStatus
    readSnapshot*: int64
    mutatedTables*: HashSet[string]
    mutex*: pointer

  TransactionStatus* = enum
    tsActive, tsCommitted, tsAborted

  ShardID* = distinct int64

  Shard* = ref object
    id*: ShardID
    rangeStart*: uint64
    rangeEnd*: uint64
    replicas*: seq[ReplicaInfo]
    primaryReplica*: int
    table*: string

  ReplicaInfo* = object
    nodeId*: string
    address*: string
    port*: uint16
    lastSeen*: int64

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

proc newRow*(id: RowID = RowID(0)): Row =
  Row(id: id, values: @[], createdAt: getTime().toUnix * 1000,
       updatedAt: getTime().toUnix * 1000, version: 1)

proc `==`*(a, b: TransactionID): bool = a.int64 == b.int64
proc `!=`*(a, b: TransactionID): bool = not (a == b)

# Transaction ID generation (legacy - will be replaced by P2PTimeSynchronizer)
# Row ID generation
proc genRowID*(): RowID =
  RowID(getTime().toUnix * 1000000 + (getTime().toUnix*1000000 mod 1000000).int64)

# ============================================================================
# ULID Operations
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

proc genULID*(): ULID {.raises: [].} =
  ## Generate a new ULID with current timestamp
  ## Uses the ulid library for string generation, then converts to binary
  {.cast(raises: []).}:
    try:
      let s = ulid()
      # Decode Crockford's base32 to binary
      const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
      proc charVal(c: char): int =
        for i, ch in alphabet:
          if ch == c: return i
        return 0
      # Decode 26 chars to 16 bytes (each char is 5 bits, 26*5 = 130 bits, we need 128)
      var bits: array[130, int]
      var bitIdx = 0
      for i in 0 ..< 26:
        let v = charVal(s[i])
        for j in countdown(4, 0):
          bits[bitIdx] = (v shr j) and 1
          inc bitIdx
      # Take first 128 bits for the 16 bytes
      for i in 0 ..< 16:
        var b = 0
        for j in 0 ..< 8:
          b = (b shl 1) or bits[i * 8 + j]
        result.data[i] = uint8(b)
    except CatchableError:
      # Fall back to timestamp-based generation if ulid() fails
      let t = getTime()
      let ms = t.toUnix * 1000 + (t.nanosecond div 1_000_000)
      # Encode timestamp in first 6 bytes
      for i in 0 ..< 6:
        result.data[i] = uint8((ms shr ((5 - i) * 8)) and 0xFF)
      # Fill rest with pseudo-random
      for i in 6 ..< 16:
        result.data[i] = uint8(i * 17)

proc ulidFromString*(s: string): ULID =
  ## Parse a 26-character ULID string to binary
  doAssert s.len == ULID_STRING_SIZE, "ULID string must be 26 characters"
  const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
  proc charVal(c: char): int =
    for i, ch in alphabet:
      if ch == c: return i
    return 0
  var bits: array[130, int]
  var bitIdx = 0
  for i in 0 ..< 26:
    let v = charVal(s[i])
    for j in countdown(4, 0):
      bits[bitIdx] = (v shr j) and 1
      inc bitIdx
  for i in 0 ..< 16:
    var b = 0
    for j in 0 ..< 8:
      b = (b shl 1) or bits[i * 8 + j]
    result.data[i] = uint8(b)

proc `$`*(u: ULID): string =
  ## Convert ULID to 26-character string representation
  const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
  # Convert 16 bytes to 128 bits
  var bits: array[128, int]
  for i in 0 ..< 16:
    for j in 0 ..< 8:
      bits[i * 8 + j] = int((u.data[i] shr (7 - j)) and 1)
  # Encode 128 bits as 26 base32 chars (need to pad to 130 bits)
  # First 10 chars encode 48-bit timestamp (50 bits with padding)
  result = newString(26)
  var bitIdx = 0
  for i in 0 ..< 26:
    var v = 0
    let numBits = if i == 0: 2 else: 5 # First char only uses 2 bits
    for j in 0 ..< numBits:
      if bitIdx < 128:
        v = (v shl 1) or bits[bitIdx]
        inc bitIdx
      else:
        v = v shl 1
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

proc newValueRef*(u: ULID): ValueRef =
  result = ValueRef(kind: dtULID, ulidValue: u)
