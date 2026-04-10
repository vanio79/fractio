# Binary Primary Key Encoding for Fractio
#
# Fixed-width binary format for primary keys that preserves correct sorting.
# Each column value starts with a null indicator byte, followed by type-specific bytes.
# All columns use fixed widths so subsequent fields start at predictable offsets.
#
# Encoding format per column:
#   [1 byte]  NULL flag (0x00 = NULL, 0x01 = non-NULL)
#   [N bytes] Value bytes (fixed width per type, zero-padded for NULL)
#
# Type widths:
#   INT64   : 8 bytes (big-endian for correct numeric order)
#   INT32   : 4 bytes (big-endian)
#   FLOAT64 : 8 bytes (sign-bit flipped for correct numeric order)
#   STRING  : maxLen bytes (UTF-8 + 0x00 padding)
#   BOOL    : 1 byte
#   ULID    : 16 bytes
#
# NULL values sort first (flag 0x00 + zero padding).

import std/endians
import fractio/utils/binary
import fractio/distributed/meta/system_schemas

# =============================================================================
# Constants
# =============================================================================

const
  NULL_FLAG* = 0x00'u8     ## NULL indicator - sorts first
  NON_NULL_FLAG* = 0x01'u8 ## Non-NULL indicator

# =============================================================================
# Type Width Calculations
# =============================================================================

proc columnTypeWidth*(dt: ColumnDataType, maxLen: int = 0): int =
  ## Calculate the value byte width for a column type.
  ## For strings, maxLen is the declared VARCHAR(n) maximum length.
  case dt
  of cdtInt: 8
  of cdtFloat: 8
  of cdtString: maxLen
  of cdtBool: 1
  of cdtBytes: maxLen
  of cdtDate: 8
  of cdtDateTime: 8
  of cdtULID: 16

proc columnTotalWidth*(dt: ColumnDataType, maxLen: int = 0): int =
  ## Total bytes per column including NULL flag.
  ## Always (1 + value_width) for consistent positioning.
  1 + columnTypeWidth(dt, maxLen)

# =============================================================================
# Big-Endian Integer Encoding (for correct sorting)
# =============================================================================

proc encodeInt64BE*(value: int64): array[8, uint8] =
  ## Encode int64 as big-endian bytes with sign-bit flip for correct sorting.
  ##
  ## In two's complement, negative numbers have sign bit = 1, making them
  ## look like large unsigned values (0x80... to 0xFF...). This breaks sorting.
  ##
  ## Solution: XOR with 0x8000000000000000 to flip the sign bit:
  ##   -1 → 0x7FFFFFFFFFFFFFFF (was 0xFFFFFFFFFFFFFFFF)
  ##    0 → 0x8000000000000000 (was 0x0000000000000000)
  ##    1 → 0x8000000000000001 (was 0x0000000000000001)
  ##
  ## Result: -1 < 0 < 1 (correct signed order via unsigned byte comparison)
  var bits = cast[uint64](value) xor 0x8000000000000000'u64
  bigEndian64(result[0].addr, bits.addr)

proc decodeInt64BE*(bytes: openArray[uint8]): int64 =
  ## Decode sign-bit flipped big-endian bytes to int64.
  if bytes.len < 8:
    raise newException(ValueError, "Not enough bytes for int64")
  var bits: uint64
  bigEndian64(bits.addr, bytes[0].addr)
  bits = bits xor 0x8000000000000000'u64 # Flip sign bit back
  result = cast[int64](bits)

proc encodeInt32BE*(value: int32): array[4, uint8] =
  ## Encode int32 as big-endian bytes with sign-bit flip for correct sorting.
  var bits = cast[uint32](value) xor 0x80000000'u32
  bigEndian32(result[0].addr, bits.addr)

proc decodeInt32BE*(bytes: openArray[uint8]): int32 =
  ## Decode sign-bit flipped big-endian bytes to int32.
  if bytes.len < 4:
    raise newException(ValueError, "Not enough bytes for int32")
  var bits: uint32
  bigEndian32(bits.addr, bytes[0].addr)
  bits = bits xor 0x80000000'u32 # Flip sign bit back
  result = cast[int32](bits)

# =============================================================================
# Float64 Encoding with Sign-Bit Flip (for correct sorting)
# =============================================================================

proc encodeFloat64Sortable*(value: float64): array[8, uint8] =
  ## Encode float64 with sign-bit flip for correct numeric ordering.
  ##
  ## IEEE 754 floats have sign bit at position 63:
  ##   - Negative: sign=1, looks like large unsigned value
  ##   - Positive: sign=0, looks like small unsigned value
  ##
  ## Solution:
  ##   - For negatives: flip all 64 bits (makes them small unsigned)
  ##   - For positives/zero: flip only sign bit (makes them large unsigned)
  ##
  ## Result: -100 < -1 < 0 < 1 < 100
  var bits = cast[uint64](value)
  if bits >= 0x8000000000000000'u64:
    # Negative (sign bit set): flip all bits
    bits = not bits
  else:
    # Positive or zero: flip only sign bit
    bits = bits xor 0x8000000000000000'u64
  # Encode as big-endian for byte-wise sorting
  bigEndian64(result[0].addr, bits.addr)

proc decodeFloat64Sortable*(bytes: openArray[uint8]): float64 =
  ## Decode sign-flipped float64 bytes back to original value.
  if bytes.len < 8:
    raise newException(ValueError, "Not enough bytes for float64")
  var bits: uint64
  bigEndian64(bits.addr, bytes[0].addr)
  # Reverse the sign-bit flip
  if bits >= 0x8000000000000000'u64:
    # Was positive: flip only sign bit back
    bits = bits xor 0x8000000000000000'u64
  else:
    # Was negative: flip all bits back
    bits = not bits
  result = cast[float64](bits)

# =============================================================================
# String Encoding (Fixed-Width with Padding)
# =============================================================================

proc encodeStringFixed*(value: string, maxLen: int): seq[uint8] =
  ## Encode string as fixed-width bytes with null-byte padding.
  ## Shorter strings are padded with 0x00 (null bytes).
  ## 0x00 sorts before any printable character (0x20-0x7E).
  ##
  ## Example for VARCHAR(32):
  ##   "abc" → [0x61, 0x62, 0x63, 0x00, ..., 0x00] (32 bytes)
  ##   "abcd" → [0x61, 0x62, 0x63, 0x64, 0x00, ..., 0x00] (32 bytes)
  ##
  ## Byte comparison:
  ##   "abc" < "abcd" because position 3: 0x00 < 0x64
  if value.len > maxLen:
    raise newException(ValueError, "String exceeds max length: " & value &
        " > " & $maxLen)
  result = newSeq[uint8](maxLen)
  # Copy string bytes (UTF-8)
  for i in 0..<value.len:
    result[i] = uint8(value[i])
  # Remaining bytes are already 0 (default seq initialization)

proc decodeStringFixed*(bytes: openArray[uint8]): string =
  ## Decode fixed-width string bytes, stripping null padding.
  ## Finds the first null byte and returns content before it.
  var endPos = bytes.len
  for i in 0..<bytes.len:
    if bytes[i] == 0:
      endPos = i
      break
  if endPos == 0:
    result = ""
  else:
    result = newString(endPos)
    for i in 0..<endPos:
      result[i] = char(bytes[i])

# =============================================================================
# Primary Key Column Value Encoding
# =============================================================================

type
  PrimaryKeyColumnValue* = object
    ## Value to encode as part of a primary key column
    isNull*: bool
    case kind*: ColumnDataType
    of cdtInt:
      intVal*: int64
    of cdtFloat:
      floatVal*: float64
    of cdtString:
      strVal*: string
      strMaxLen*: int ## VARCHAR max length for encoding
    of cdtBool:
      boolVal*: bool
    of cdtBytes:
      bytesVal*: seq[uint8]
      bytesMaxLen*: int
    of cdtDate:
      dateVal*: int64
    of cdtDateTime:
      datetimeVal*: int64
    of cdtULID:
      ulidVal*: array[16, uint8]

proc encodePkColumn*(col: PrimaryKeyColumnValue, w: var BinaryWriter) =
  ## Encode a single primary key column value to binary.
  ## Format: [NULL flag (1 byte)] + [value bytes (fixed width)]

  # Write NULL flag
  if col.isNull:
    w.writeU8(NULL_FLAG)
  else:
    w.writeU8(NON_NULL_FLAG)

  # Write value bytes (zero-padded for NULL)
  case col.kind
  of cdtInt:
    let bytes = if col.isNull: [0'u8, 0, 0, 0, 0, 0, 0, 0] else: encodeInt64BE(col.intVal)
    for b in bytes:
      w.writeU8(b)

  of cdtFloat:
    let bytes = if col.isNull: [0'u8, 0, 0, 0, 0, 0, 0,
        0] else: encodeFloat64Sortable(col.floatVal)
    for b in bytes:
      w.writeU8(b)

  of cdtString:
    let bytes = if col.isNull: newSeq[uint8](
        col.strMaxLen) else: encodeStringFixed(col.strVal, col.strMaxLen)
    for b in bytes:
      w.writeU8(b)

  of cdtBool:
    w.writeU8(if col.isNull: 0'u8 else: (if col.boolVal: 1'u8 else: 0'u8))

  of cdtBytes:
    if col.isNull:
      for _ in 0..<col.bytesMaxLen:
        w.writeU8(0)
    else:
      if col.bytesVal.len > col.bytesMaxLen:
        raise newException(ValueError, "Bytes exceed max length")
      for i in 0..<col.bytesMaxLen:
        if i < col.bytesVal.len:
          w.writeU8(col.bytesVal[i])
        else:
          w.writeU8(0)

  of cdtDate:
    let bytes = if col.isNull: [0'u8, 0, 0, 0, 0, 0, 0, 0] else: encodeInt64BE(col.dateVal)
    for b in bytes:
      w.writeU8(b)

  of cdtDateTime:
    let bytes = if col.isNull: [0'u8, 0, 0, 0, 0, 0, 0, 0] else: encodeInt64BE(
        col.datetimeVal)
    for b in bytes:
      w.writeU8(b)

  of cdtULID:
    if col.isNull:
      for _ in 0..<16:
        w.writeU8(0)
    else:
      for b in col.ulidVal:
        w.writeU8(b)

proc decodePkColumn*(r: var BinaryReader, dt: ColumnDataType,
    maxLen: int = 0): PrimaryKeyColumnValue =
  ## Decode a single primary key column value from binary.

  # Read NULL flag
  let nullFlag = r.readU8()
  let isNull = (nullFlag == NULL_FLAG)

  case dt
  of cdtInt:
    var bytes: array[8, uint8]
    for i in 0..<8:
      bytes[i] = r.readU8()
    let intVal = if isNull: 0'i64 else: decodeInt64BE(bytes)
    result = PrimaryKeyColumnValue(isNull: isNull, kind: cdtInt, intVal: intVal)

  of cdtFloat:
    var bytes: array[8, uint8]
    for i in 0..<8:
      bytes[i] = r.readU8()
    let floatVal = if isNull: 0.0 else: decodeFloat64Sortable(bytes)
    result = PrimaryKeyColumnValue(isNull: isNull, kind: cdtFloat,
        floatVal: floatVal)

  of cdtString:
    var bytes = newSeq[uint8](maxLen)
    for i in 0..<maxLen:
      bytes[i] = r.readU8()
    let strVal = if isNull: "" else: decodeStringFixed(bytes)
    result = PrimaryKeyColumnValue(isNull: isNull, kind: cdtString,
        strVal: strVal, strMaxLen: maxLen)

  of cdtBool:
    let b = r.readU8()
    let boolVal = if isNull: false else: (b == 1)
    result = PrimaryKeyColumnValue(isNull: isNull, kind: cdtBool,
        boolVal: boolVal)

  of cdtBytes:
    var bytes = newSeq[uint8](maxLen)
    for i in 0..<maxLen:
      bytes[i] = r.readU8()
    var bytesVal: seq[uint8] = @[]
    if not isNull:
      # Strip trailing zeros
      var endPos = maxLen
      for i in 0..<maxLen:
        if bytes[i] == 0:
          endPos = i
          break
      bytesVal = bytes[0..<endPos]
    result = PrimaryKeyColumnValue(isNull: isNull, kind: cdtBytes,
        bytesVal: bytesVal, bytesMaxLen: maxLen)

  of cdtDate:
    var bytes: array[8, uint8]
    for i in 0..<8:
      bytes[i] = r.readU8()
    let dateVal = if isNull: 0'i64 else: decodeInt64BE(bytes)
    result = PrimaryKeyColumnValue(isNull: isNull, kind: cdtDate,
        dateVal: dateVal)

  of cdtDateTime:
    var bytes: array[8, uint8]
    for i in 0..<8:
      bytes[i] = r.readU8()
    let datetimeVal = if isNull: 0'i64 else: decodeInt64BE(bytes)
    result = PrimaryKeyColumnValue(isNull: isNull, kind: cdtDateTime,
        datetimeVal: datetimeVal)

  of cdtULID:
    var ulidVal: array[16, uint8]
    for i in 0..<16:
      ulidVal[i] = r.readU8()
    result = PrimaryKeyColumnValue(isNull: isNull, kind: cdtULID,
        ulidVal: ulidVal)

# =============================================================================
# Primary Key Encoding (Composite)
# =============================================================================

type
  PrimaryKeySpec* = object
    ## Specification for encoding a primary key
    ## Describes the columns that make up the primary key
    columns*: seq[tuple[name: string, dataType: ColumnDataType, maxLen: int]]

  PrimaryKey* = seq[PrimaryKeyColumnValue]
    ## A primary key value (sequence of column values)

proc encodePrimaryKey*(pk: PrimaryKey, spec: PrimaryKeySpec): string =
  ## Encode a composite primary key value to binary.
  ## Each column is encoded with fixed width, concatenated.
  var w = initBinaryWriter()
  for i, col in pk:
    encodePkColumn(col, w)
  result = w.finish()

proc decodePrimaryKey*(data: string, spec: PrimaryKeySpec): PrimaryKey =
  ## Decode binary data to a primary key value.
  var r = initBinaryReader(data)
  result = newSeq[PrimaryKeyColumnValue](spec.columns.len)
  for i, colSpec in spec.columns:
    result[i] = decodePkColumn(r, colSpec.dataType, colSpec.maxLen)

# =============================================================================
# Primary Key Spec from TableRecord
# =============================================================================

proc primaryKeySpecFromTable*(rec: TableRecord): PrimaryKeySpec =
  ## Build a PrimaryKeySpec from a TableRecord.
  ## Looks up PK columns and their types/max lengths.
  result.columns = @[]
  for pkColName in rec.primaryKey:
    # Find the column definition
    var found = false
    for col in rec.columns:
      if col.name == pkColName:
        result.columns.add((name: col.name, dataType: col.dataType,
            maxLen: int(col.maxLen)))
        found = true
        break
    if not found:
      raise newException(ValueError, "PK column not found in table: " & pkColName)

# =============================================================================
# Value Extraction Helpers
# =============================================================================

proc pkValueFromInt*(value: int64, isNull: bool = false): PrimaryKeyColumnValue =
  ## Create a PK column value from an int64.
  PrimaryKeyColumnValue(isNull: isNull, kind: cdtInt, intVal: value)

proc pkValueFromFloat*(value: float64, isNull: bool = false): PrimaryKeyColumnValue =
  ## Create a PK column value from a float64.
  PrimaryKeyColumnValue(isNull: isNull, kind: cdtFloat, floatVal: value)

proc pkValueFromString*(value: string, maxLen: int,
    isNull: bool = false): PrimaryKeyColumnValue =
  ## Create a PK column value from a string with max length.
  PrimaryKeyColumnValue(isNull: isNull, kind: cdtString, strVal: value,
      strMaxLen: maxLen)

proc pkValueFromBool*(value: bool, isNull: bool = false): PrimaryKeyColumnValue =
  ## Create a PK column value from a bool.
  PrimaryKeyColumnValue(isNull: isNull, kind: cdtBool, boolVal: value)

proc pkValueFromDate*(value: int64, isNull: bool = false): PrimaryKeyColumnValue =
  ## Create a PK column value from a date (int64 nanoseconds).
  PrimaryKeyColumnValue(isNull: isNull, kind: cdtDate, dateVal: value)

proc pkValueFromDateTime*(value: int64, isNull: bool = false): PrimaryKeyColumnValue =
  ## Create a PK column value from a datetime (int64 nanoseconds).
  PrimaryKeyColumnValue(isNull: isNull, kind: cdtDateTime, datetimeVal: value)

proc pkValueFromULID*(value: array[16, uint8],
    isNull: bool = false): PrimaryKeyColumnValue =
  ## Create a PK column value from a ULID.
  PrimaryKeyColumnValue(isNull: isNull, kind: cdtULID, ulidVal: value)

# =============================================================================
# Debug/Display Helpers
# =============================================================================

proc pkColumnToString*(col: PrimaryKeyColumnValue): string =
  ## Convert a PK column value to a readable string (for debugging).
  if col.isNull:
    return "NULL"
  case col.kind
  of cdtInt: $col.intVal
  of cdtFloat: $col.floatVal
  of cdtString: col.strVal
  of cdtBool: $col.boolVal
  of cdtBytes: "bytes:" & $col.bytesVal.len
  of cdtDate: $col.dateVal
  of cdtDateTime: $col.datetimeVal
  of cdtULID:
    var s = ""
    for b in col.ulidVal:
      s.add(b.char)
    s

proc pkToString*(pk: PrimaryKey): string =
  ## Convert a primary key to a readable string (for debugging).
  result = "("
  for i, col in pk:
    if i > 0: result.add(", ")
    result.add(pkColumnToString(col))
  result.add(")")
