# DataRow Binary Format for Fractio SQL
# 
# High-performance binary format for SQL data rows.
# Replaces JSON serialization in user table data.
#
# Format:
# - Magic: 2 bytes ("DR")
# - Version: 1 byte
# - Column count: 4 bytes (uint32)
# - For each column:
#   - Name: length-prefixed string
#   - Type: 1 byte (DataRowValueKind ordinal)
#   - Value: type-specific encoding

import std/options
import fractio/utils/binary

# =============================================================================
# Binary Serialization Constants
# =============================================================================

const
  DATA_ROW_MAGIC* = [0x44'u8, 0x52'u8] # "DR" - DataRow binary marker
  DATA_ROW_VERSION* = 0x01'u8          # Current binary format version

# =============================================================================
# DataRow Types
# =============================================================================

type
  DataRowValueKind* = enum
    ## Kind of value in a DataRow
    drvkNull = 0
    drvkInt = 1
    drvkFloat = 2
    drvkString = 3
    drvkBool = 4

  DataRowValue* = object
    ## A single value in a DataRow
    case kind*: DataRowValueKind
    of drvkNull:
      discard
    of drvkInt:
      intVal*: int64
    of drvkFloat:
      floatVal*: float64
    of drvkString:
      strVal*: string
    of drvkBool:
      boolVal*: bool

  DataRowColumn* = object
    ## A column with name and value
    name*: string
    value*: DataRowValue

  DataRow* = object
    ## A row of data with named columns.
    ## Stores user table data in binary format.
    ## Columns are stored as a sequence for ordered access.
    columns*: seq[DataRowColumn]

# =============================================================================
# DataRowValue Constructors
# =============================================================================

proc newRowValue*(): DataRowValue =
  ## Create a null DataRowValue
  DataRowValue(kind: drvkNull)

proc newRowValue*(i: int64): DataRowValue =
  ## Create an integer DataRowValue
  DataRowValue(kind: drvkInt, intVal: i)

proc newRowValue*(f: float64): DataRowValue =
  ## Create a float DataRowValue
  DataRowValue(kind: drvkFloat, floatVal: f)

proc newRowValue*(s: string): DataRowValue =
  ## Create a string DataRowValue
  DataRowValue(kind: drvkString, strVal: s)

proc newRowValue*(b: bool): DataRowValue =
  ## Create a bool DataRowValue
  DataRowValue(kind: drvkBool, boolVal: b)

# =============================================================================
# DataRow Constructors
# =============================================================================

proc newDataRow*(): DataRow =
  ## Create an empty DataRow
  DataRow(columns: @[])

proc newDataRow*(columns: seq[DataRowColumn]): DataRow =
  ## Create a DataRow with columns
  DataRow(columns: columns)

proc newColumn*(name: string, value: DataRowValue): DataRowColumn =
  ## Create a DataRowColumn
  DataRowColumn(name: name, value: value)

# =============================================================================
# Column Accessors
# =============================================================================

proc hasColumn*(row: DataRow, name: string): bool =
  ## Check if row has a column with given name
  for col in row.columns:
    if col.name == name:
      return true
  false

proc getColumn*(row: DataRow, name: string): Option[DataRowValue] =
  ## Get value for a column by name
  for col in row.columns:
    if col.name == name:
      return some(col.value)
  none(DataRowValue)

proc getColumnIdx*(row: DataRow, name: string): int =
  ## Get column index by name, or -1 if not found
  for i, col in row.columns:
    if col.name == name:
      return i
  -1

proc setColumn*(row: var DataRow, name: string, value: DataRowValue) =
  ## Set a column value, adding if not exists
  let idx = row.getColumnIdx(name)
  if idx >= 0:
    row.columns[idx].value = value
  else:
    row.columns.add(newColumn(name, value))

proc `[]`*(row: DataRow, name: string): DataRowValue =
  ## Get column value by name (returns null if not found)
  let opt = row.getColumn(name)
  if opt.isSome:
    opt.get
  else:
    newRowValue()

proc `[]=`*(row: var DataRow, name: string, value: DataRowValue) =
  ## Set column value by name
  row.setColumn(name, value)

# =============================================================================
# Value Extraction Helpers
# =============================================================================

proc getInt*(v: DataRowValue, default: int64 = 0): int64 =
  ## Get integer value, or default if not int
  if v.kind == drvkInt: v.intVal else: default

proc getFloat*(v: DataRowValue, default: float64 = 0.0): float64 =
  ## Get float value, or default if not float
  if v.kind == drvkFloat: v.floatVal else: default

proc getString*(v: DataRowValue, default: string = ""): string =
  ## Get string value, or default if not string
  if v.kind == drvkString: v.strVal else: default

proc getBool*(v: DataRowValue, default: bool = false): bool =
  ## Get bool value, or default if not bool
  if v.kind == drvkBool: v.boolVal else: default

proc isNull*(v: DataRowValue): bool =
  ## Check if value is null
  v.kind == drvkNull

# =============================================================================
# Binary Encoding
# =============================================================================

proc encodeDataRowValue*(w: var BinaryWriter, v: DataRowValue) =
  ## Encode a DataRowValue to binary
  w.writeU8(uint8(ord(v.kind)))
  case v.kind
  of drvkNull:
    discard
  of drvkInt:
    w.writeI64(v.intVal)
  of drvkFloat:
    w.writeFloat64(v.floatVal)
  of drvkString:
    w.writeString(v.strVal)
  of drvkBool:
    w.writeU8(if v.boolVal: 1'u8 else: 0'u8)

proc decodeDataRowValue*(r: var BinaryReader): DataRowValue =
  ## Decode a DataRowValue from binary
  let kind = DataRowValueKind(int(r.readU8()))
  case kind
  of drvkNull:
    DataRowValue(kind: drvkNull)
  of drvkInt:
    DataRowValue(kind: drvkInt, intVal: r.readI64())
  of drvkFloat:
    DataRowValue(kind: drvkFloat, floatVal: r.readFloat64())
  of drvkString:
    DataRowValue(kind: drvkString, strVal: r.readString())
  of drvkBool:
    DataRowValue(kind: drvkBool, boolVal: r.readU8() != 0)

proc encodeDataRow*(row: DataRow): string =
  ## Encode a DataRow to binary format.
  ##
  ## Binary format (little-endian):
  ## - Magic: 2 bytes (0x44 0x52 = "DR")
  ## - Version: 1 byte (0x01)
  ## - Column count: 4 bytes (uint32)
  ## - For each column:
  ##   - Name: length-prefixed string
  ##   - Value: type byte + type-specific data
  ##
  ## Total minimum: 7 bytes (empty row)
  var w = initBinaryWriter()
  w.writeBytes(DATA_ROW_MAGIC)
  w.writeU8(DATA_ROW_VERSION)
  w.writeU32(uint32(row.columns.len))
  for col in row.columns:
    w.writeString(col.name)
    encodeDataRowValue(w, col.value)
  w.finish()

proc decodeDataRow*(data: string): DataRow =
  ## Decode binary data to a DataRow.
  ## Raises ValueError if data is invalid or not binary format.
  var r = initBinaryReader(data)

  # Verify magic header
  if r.remaining < 3:
    raise newException(ValueError, "DataRow: data too small for header")
  let magic0 = r.readU8()
  let magic1 = r.readU8()
  if magic0 != DATA_ROW_MAGIC[0] or magic1 != DATA_ROW_MAGIC[1]:
    raise newException(ValueError, "DataRow: invalid magic header")

  # Verify version
  let version = r.readU8()
  if version != DATA_ROW_VERSION:
    raise newException(ValueError, "DataRow: unsupported version " & $version)

  # Read columns
  let colCount = int(r.readU32())
  result = newDataRow()
  for i in 0..<colCount:
    let name = r.readString()
    let value = decodeDataRowValue(r)
    result.columns.add(newColumn(name, value))

# =============================================================================
# String Conversion (for display/output)
# =============================================================================

proc toStringValue*(v: DataRowValue): string =
  ## Convert DataRowValue to string for display
  case v.kind
  of drvkNull: "NULL"
  of drvkInt: $v.intVal
  of drvkFloat: $v.floatVal
  of drvkString: v.strVal
  of drvkBool: $v.boolVal

proc toStringRow*(row: DataRow, columns: seq[string]): seq[string] =
  ## Convert DataRow to string values for specified columns
  for colName in columns:
    result.add(row[colName].toStringValue())

# =============================================================================
# Comparison Operators (for expression evaluation)
# =============================================================================

proc `==`*(a, b: DataRowValue): bool =
  ## Compare two DataRowValues for equality
  if a.kind != b.kind:
    return false
  case a.kind
  of drvkNull: true
  of drvkInt: a.intVal == b.intVal
  of drvkFloat: a.floatVal == b.floatVal
  of drvkString: a.strVal == b.strVal
  of drvkBool: a.boolVal == b.boolVal

proc `!=`*(a, b: DataRowValue): bool =
  ## Compare two DataRowValues for inequality
  not (a == b)

proc `<`*(a, b: DataRowValue): bool =
  ## Compare two DataRowValues for less-than
  if a.kind != b.kind:
    return false
  case a.kind
  of drvkNull: false
  of drvkInt: a.intVal < b.intVal
  of drvkFloat: a.floatVal < b.floatVal
  of drvkString: a.strVal < b.strVal
  of drvkBool: a.boolVal < b.boolVal

proc `<=`*(a, b: DataRowValue): bool =
  ## Compare two DataRowValues for less-or-equal
  if a.kind != b.kind:
    return false
  case a.kind
  of drvkNull: true
  of drvkInt: a.intVal <= b.intVal
  of drvkFloat: a.floatVal <= b.floatVal
  of drvkString: a.strVal <= b.strVal
  of drvkBool: a.boolVal <= b.boolVal

proc `>`*(a, b: DataRowValue): bool =
  ## Compare two DataRowValues for greater-than
  b < a

proc `>=`*(a, b: DataRowValue): bool =
  ## Compare two DataRowValues for greater-or-equal
  b <= a

# =============================================================================
# Arithmetic Operators (for expression evaluation)
# =============================================================================

proc `+`*(a, b: DataRowValue): DataRowValue =
  ## Add two DataRowValues (int only)
  if a.kind == drvkInt and b.kind == drvkInt:
    newRowValue(a.intVal + b.intVal)
  else:
    newRowValue()

proc `-`*(a, b: DataRowValue): DataRowValue =
  ## Subtract two DataRowValues (int only)
  if a.kind == drvkInt and b.kind == drvkInt:
    newRowValue(a.intVal - b.intVal)
  else:
    newRowValue()

proc `*`*(a, b: DataRowValue): DataRowValue =
  ## Multiply two DataRowValues (int only)
  if a.kind == drvkInt and b.kind == drvkInt:
    newRowValue(a.intVal * b.intVal)
  else:
    newRowValue()

proc `div`*(a, b: DataRowValue): DataRowValue =
  ## Divide two DataRowValues (int only)
  if a.kind == drvkInt and b.kind == drvkInt and b.intVal != 0:
    newRowValue(a.intVal div b.intVal)
  else:
    newRowValue()

proc `mod`*(a, b: DataRowValue): DataRowValue =
  ## Modulo two DataRowValues (int only)
  if a.kind == drvkInt and b.kind == drvkInt and b.intVal != 0:
    newRowValue(a.intVal mod b.intVal)
  else:
    newRowValue()

proc `-`*(a: DataRowValue): DataRowValue =
  ## Negate a DataRowValue (int only)
  if a.kind == drvkInt:
    newRowValue(-a.intVal)
  else:
    newRowValue()

# =============================================================================
# Logic Operators (for expression evaluation)
# =============================================================================

proc `and`*(a, b: DataRowValue): DataRowValue =
  ## Logical AND of two bool DataRowValues
  if a.kind == drvkBool and b.kind == drvkBool:
    newRowValue(a.boolVal and b.boolVal)
  else:
    newRowValue()

proc `or`*(a, b: DataRowValue): DataRowValue =
  ## Logical OR of two bool DataRowValues
  if a.kind == drvkBool and b.kind == drvkBool:
    newRowValue(a.boolVal or b.boolVal)
  else:
    newRowValue()

proc `not`*(a: DataRowValue): DataRowValue =
  ## Logical NOT of a bool DataRowValue
  if a.kind == drvkBool:
    newRowValue(not a.boolVal)
  else:
    newRowValue()
