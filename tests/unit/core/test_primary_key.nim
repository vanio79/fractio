# Unit Tests for Binary Primary Key Encoding
#
# Tests the fixed-width binary encoding for primary keys.
# Verifies correct sorting behavior for all supported types.

import unittest
import std/[strformat, algorithm, sequtils]

import fractio/utils/binary
import fractio/core/types # for genULID
import fractio/core/primary_key
import fractio/distributed/meta/system_schemas

# =============================================================================
# Integer Encoding Tests
# =============================================================================

suite "Int64 Big-Endian Encoding":

  test "Encode/decode small positive integer":
    let encoded = encodeInt64BE(42)
    let decoded = decodeInt64BE(encoded)
    check decoded == 42

  test "Encode/decode large positive integer":
    let encoded = encodeInt64BE(1000000)
    let decoded = decodeInt64BE(encoded)
    check decoded == 1000000

  test "Encode/decode zero":
    let encoded = encodeInt64BE(0)
    let decoded = decodeInt64BE(encoded)
    check decoded == 0

  test "Encode/decode negative integer":
    let encoded = encodeInt64BE(-42)
    let decoded = decodeInt64BE(encoded)
    check decoded == -42

  test "Encode/decode int64 max":
    let encoded = encodeInt64BE(int64.high)
    let decoded = decodeInt64BE(encoded)
    check decoded == int64.high

  test "Encode/decode int64 min":
    let encoded = encodeInt64BE(int64.low)
    let decoded = decodeInt64BE(encoded)
    check decoded == int64.low

  test "Byte order matches numeric order":
    # Small number should have smaller bytes
    let small = encodeInt64BE(42)
    let large = encodeInt64BE(100)

    # Compare byte-by-byte
    var smallLess = false
    for i in 0..<8:
      if small[i] < large[i]:
        smallLess = true
        break
      elif small[i] > large[i]:
        break
    check smallLess

  test "Sorting order preserved for multiple integers":
    let values = [int64(100), 42, 1000, 0, -1, 50]
    var encoded: seq[array[8, uint8]] = @[]
    for v in values:
      encoded.add(encodeInt64BE(v))

    # Sort encoded values by byte comparison
    encoded.sort(proc(a, b: array[8, uint8]): int =
      for i in 0..<8:
        if a[i] < b[i]: return -1
        elif a[i] > b[i]: return 1
      return 0
    )

    # Decode and verify order
    let decoded = encoded.mapIt(decodeInt64BE(it))
    check decoded == [int64(-1), 0, 42, 50, 100, 1000]

suite "Int32 Big-Endian Encoding":

  test "Encode/decode positive integer":
    let encoded = encodeInt32BE(42)
    let decoded = decodeInt32BE(encoded)
    check decoded == 42

  test "Encode/decode negative integer":
    let encoded = encodeInt32BE(-100)
    let decoded = decodeInt32BE(encoded)
    check decoded == -100

# =============================================================================
# Float64 Encoding Tests
# =============================================================================

suite "Float64 Sortable Encoding":

  test "Encode/decode positive float":
    let encoded = encodeFloat64Sortable(3.14159)
    let decoded = decodeFloat64Sortable(encoded)
    check decoded == 3.14159

  test "Encode/decode negative float":
    let encoded = encodeFloat64Sortable(-2.71828)
    let decoded = decodeFloat64Sortable(encoded)
    check decoded == -2.71828

  test "Encode/decode zero":
    let encoded = encodeFloat64Sortable(0.0)
    let decoded = decodeFloat64Sortable(encoded)
    check decoded == 0.0

  test "Encode/decode very small positive":
    let encoded = encodeFloat64Sortable(0.000001)
    let decoded = decodeFloat64Sortable(encoded)
    check decoded == 0.000001

  test "Encode/decode very small negative":
    let encoded = encodeFloat64Sortable(-0.000001)
    let decoded = decodeFloat64Sortable(encoded)
    check decoded == -0.000001

  test "Negative sorts before positive":
    let neg = encodeFloat64Sortable(-1.0)
    let pos = encodeFloat64Sortable(1.0)

    var negLess = false
    for i in 0..<8:
      if neg[i] < pos[i]:
        negLess = true
        break
      elif neg[i] > pos[i]:
        break
    check negLess

  test "Sorting order preserved for multiple floats":
    let values = [1.0, -100.0, 50.0, -1.0, 0.0, 0.001]
    var encoded: seq[array[8, uint8]] = @[]
    for v in values:
      encoded.add(encodeFloat64Sortable(v))

    # Sort by byte comparison
    encoded.sort(proc(a, b: array[8, uint8]): int =
      for i in 0..<8:
        if a[i] < b[i]: return -1
        elif a[i] > b[i]: return 1
      return 0
    )

    let decoded = encoded.mapIt(decodeFloat64Sortable(it))
    check decoded == [-100.0, -1.0, 0.0, 0.001, 1.0, 50.0]

# =============================================================================
# String Encoding Tests
# =============================================================================

suite "Fixed-Width String Encoding":

  test "Encode string within max length":
    let encoded = encodeStringFixed("hello", 32)
    check encoded.len == 32
    # First 5 bytes should be 'h', 'e', 'l', 'l', 'o'
    check encoded[0] == uint8('h')
    check encoded[4] == uint8('o')
    # Rest should be zeros
    check encoded[5] == 0
    check encoded[31] == 0

  test "Encode empty string":
    let encoded = encodeStringFixed("", 16)
    check encoded.len == 16
    check encoded[0] == 0

  test "Encode string fills max length exactly":
    let encoded = encodeStringFixed("12345", 5)
    check encoded.len == 5
    check encoded[0] == uint8('1')
    check encoded[4] == uint8('5')

  test "String exceeding max length raises error":
    var raised = false
    try:
      discard encodeStringFixed("too long string", 5)
    except ValueError:
      raised = true
    check raised

  test "Decode string with padding":
    let encoded = encodeStringFixed("abc", 16)
    let decoded = decodeStringFixed(encoded)
    check decoded == "abc"

  test "Decode empty string":
    let encoded = encodeStringFixed("", 16)
    let decoded = decodeStringFixed(encoded)
    check decoded == ""

  test "Shorter string sorts before longer prefix":
    # "abc" should sort before "abcd"
    let short = encodeStringFixed("abc", 32)
    let long = encodeStringFixed("abcd", 32)

    var shortLess = false
    for i in 0..<32:
      if short[i] < long[i]:
        shortLess = true
        break
      elif short[i] > long[i]:
        break
    check shortLess

  test "Alphabetic sorting preserved":
    let strings = ["zebra", "apple", "banana", "aardvark"]
    var encoded: seq[seq[uint8]] = @[]
    for s in strings:
      encoded.add(encodeStringFixed(s, 32))

    # Sort by byte comparison
    encoded.sort(proc(a, b: seq[uint8]): int =
      for i in 0..<32:
        if a[i] < b[i]: return -1
        elif a[i] > b[i]: return 1
      return 0
    )

    let decoded = encoded.mapIt(decodeStringFixed(it))
    check decoded == ["aardvark", "apple", "banana", "zebra"]

# =============================================================================
# Primary Key Column Value Tests
# =============================================================================

suite "Primary Key Column Value Encoding":

  test "Encode/decode INT64 column":
    var w = initBinaryWriter()
    let col = pkValueFromInt(42)
    encodePkColumn(col, w)
    let data = w.finish()

    var r = initBinaryReader(data)
    let decoded = decodePkColumn(r, cdtInt)
    check decoded.isNull == false
    check decoded.intVal == 42

  test "Encode/decode NULL INT64 column":
    var w = initBinaryWriter()
    let col = pkValueFromInt(0, isNull = true)
    encodePkColumn(col, w)
    let data = w.finish()

    var r = initBinaryReader(data)
    let decoded = decodePkColumn(r, cdtInt)
    check decoded.isNull == true

  test "Encode/decode STRING column":
    var w = initBinaryWriter()
    let col = pkValueFromString("test", maxLen = 16)
    encodePkColumn(col, w)
    let data = w.finish()

    # Total bytes: 1 (flag) + 16 (value) = 17
    check data.len == 17

    var r = initBinaryReader(data)
    let decoded = decodePkColumn(r, cdtString, maxLen = 16)
    check decoded.isNull == false
    check decoded.strVal == "test"

  test "Encode/decode NULL STRING column":
    var w = initBinaryWriter()
    let col = pkValueFromString("", maxLen = 16, isNull = true)
    encodePkColumn(col, w)
    let data = w.finish()

    var r = initBinaryReader(data)
    let decoded = decodePkColumn(r, cdtString, maxLen = 16)
    check decoded.isNull == true

  test "Encode/decode FLOAT64 column":
    var w = initBinaryWriter()
    let col = pkValueFromFloat(3.14159)
    encodePkColumn(col, w)
    let data = w.finish()

    var r = initBinaryReader(data)
    let decoded = decodePkColumn(r, cdtFloat)
    check decoded.isNull == false
    check decoded.floatVal == 3.14159

  test "Encode/decode BOOL column":
    var w = initBinaryWriter()
    let col = pkValueFromBool(true)
    encodePkColumn(col, w)
    let data = w.finish()

    var r = initBinaryReader(data)
    let decoded = decodePkColumn(r, cdtBool)
    check decoded.isNull == false
    check decoded.boolVal == true

  test "NULL sorts before non-NULL":
    var w1, w2 = initBinaryWriter()
    encodePkColumn(pkValueFromInt(100, isNull = true), w1)
    encodePkColumn(pkValueFromInt(-100), w2)
    let nullData = w1.finish()
    let nonNullData = w2.finish()

    # NULL flag (0x00) < NON_NULL flag (0x01)
    check nullData[0] < nonNullData[0]

# =============================================================================
# Composite Primary Key Tests
# =============================================================================

suite "Composite Primary Key Encoding":

  test "Encode single column PK":
    let spec = PrimaryKeySpec(columns: @[
      ("id", cdtInt, 0)
    ])
    let pk: PrimaryKey = @[pkValueFromInt(42)]
    let encoded = encodePrimaryKey(pk, spec)

    let decoded = decodePrimaryKey(encoded, spec)
    check decoded.len == 1
    check decoded[0].intVal == 42

  test "Encode composite PK (int, string)":
    let spec = PrimaryKeySpec(columns: @[
      ("user_id", cdtInt, 0),
      ("region", cdtString, 16)
    ])
    let pk: PrimaryKey = @[
      pkValueFromInt(42),
      pkValueFromString("us-west", maxLen = 16)
    ]
    let encoded = encodePrimaryKey(pk, spec)

    # Total: 9 (int) + 17 (string) = 26 bytes
    check encoded.len == 26

    let decoded = decodePrimaryKey(encoded, spec)
    check decoded.len == 2
    check decoded[0].intVal == 42
    check decoded[1].strVal == "us-west"

  test "Composite PK sorting - same first column, different second":
    let spec = PrimaryKeySpec(columns: @[
      ("id", cdtInt, 0),
      ("name", cdtString, 32)
    ])

    let pk1: PrimaryKey = @[pkValueFromInt(42), pkValueFromString("alice", maxLen = 32)]
    let pk2: PrimaryKey = @[pkValueFromInt(42), pkValueFromString("bob", maxLen = 32)]

    let enc1 = encodePrimaryKey(pk1, spec)
    let enc2 = encodePrimaryKey(pk2, spec)

    # "alice" < "bob", so enc1 should be byte-wise less
    var enc1Less = false
    for i in 0..<enc1.len:
      if enc1[i] < enc2[i]:
        enc1Less = true
        break
      elif enc1[i] > enc2[i]:
        break
    check enc1Less

  test "Composite PK sorting - different first column":
    let spec = PrimaryKeySpec(columns: @[
      ("id", cdtInt, 0),
      ("name", cdtString, 32)
    ])

    let pk1: PrimaryKey = @[pkValueFromInt(100), pkValueFromString("aaa", maxLen = 32)]
    let pk2: PrimaryKey = @[pkValueFromInt(42), pkValueFromString("zzz", maxLen = 32)]

    let enc1 = encodePrimaryKey(pk1, spec)
    let enc2 = encodePrimaryKey(pk2, spec)

    # 42 < 100, so enc2 should be byte-wise less
    var enc2Less = false
    for i in 0..<enc1.len:
      if enc2[i] < enc1[i]:
        enc2Less = true
        break
      elif enc2[i] > enc1[i]:
        break
    check enc2Less

  test "Composite PK with NULL in second column":
    let spec = PrimaryKeySpec(columns: @[
      ("id", cdtInt, 0),
      ("region", cdtString, 16)
    ])

    let pkWithNull: PrimaryKey = @[
      pkValueFromInt(42),
      pkValueFromString("", maxLen = 16, isNull = true)
    ]
    let pkWithValue: PrimaryKey = @[
      pkValueFromInt(42),
      pkValueFromString("us-west", maxLen = 16)
    ]

    let encNull = encodePrimaryKey(pkWithNull, spec)
    let encValue = encodePrimaryKey(pkWithValue, spec)

    # Same ID, NULL region should sort before "us-west"
    # Position 9 is the NULL flag for the second column
    check uint8(encNull[9]) == NULL_FLAG
    check uint8(encValue[9]) == NON_NULL_FLAG
    check encNull < encValue

# =============================================================================
# Type Width Tests
# =============================================================================

suite "Type Width Calculations":

  test "INT64 width":
    check columnTypeWidth(cdtInt) == 8
    check columnTotalWidth(cdtInt) == 9

  test "FLOAT64 width":
    check columnTypeWidth(cdtFloat) == 8
    check columnTotalWidth(cdtFloat) == 9

  test "STRING width with max length":
    check columnTypeWidth(cdtString, 32) == 32
    check columnTotalWidth(cdtString, 32) == 33

  test "BOOL width":
    check columnTypeWidth(cdtBool) == 1
    check columnTotalWidth(cdtBool) == 2

  test "ULID width":
    check columnTypeWidth(cdtULID) == 16
    check columnTotalWidth(cdtULID) == 17

# =============================================================================
# Value Extraction Helper Tests
# =============================================================================

suite "Value Extraction Helpers":

  test "pkValueFromInt basic":
    let col = pkValueFromInt(42)
    check col.isNull == false
    check col.kind == cdtInt
    check col.intVal == 42

  test "pkValueFromInt NULL":
    let col = pkValueFromInt(0, isNull = true)
    check col.isNull == true
    check col.kind == cdtInt

  test "pkValueFromFloat basic":
    let col = pkValueFromFloat(3.14159)
    check col.isNull == false
    check col.kind == cdtFloat
    check col.floatVal == 3.14159

  test "pkValueFromFloat NULL":
    let col = pkValueFromFloat(0.0, isNull = true)
    check col.isNull == true
    check col.kind == cdtFloat

  test "pkValueFromString basic":
    let col = pkValueFromString("hello", maxLen = 32)
    check col.isNull == false
    check col.kind == cdtString
    check col.strVal == "hello"
    check col.strMaxLen == 32

  test "pkValueFromString NULL":
    let col = pkValueFromString("", maxLen = 16, isNull = true)
    check col.isNull == true
    check col.kind == cdtString

  test "pkValueFromBool true":
    let col = pkValueFromBool(true)
    check col.isNull == false
    check col.kind == cdtBool
    check col.boolVal == true

  test "pkValueFromBool false":
    let col = pkValueFromBool(false)
    check col.isNull == false
    check col.kind == cdtBool
    check col.boolVal == false

  test "pkValueFromBool NULL":
    let col = pkValueFromBool(false, isNull = true)
    check col.isNull == true
    check col.kind == cdtBool

  test "pkValueFromDate basic":
    let col = pkValueFromDate(1710000000000000000'i64)
    check col.isNull == false
    check col.kind == cdtDate
    check col.dateVal == 1710000000000000000'i64

  test "pkValueFromDate NULL":
    let col = pkValueFromDate(0'i64, isNull = true)
    check col.isNull == true
    check col.kind == cdtDate

  test "pkValueFromDateTime basic":
    let col = pkValueFromDateTime(1710000000000000000'i64)
    check col.isNull == false
    check col.kind == cdtDateTime
    check col.datetimeVal == 1710000000000000000'i64

  test "pkValueFromDateTime NULL":
    let col = pkValueFromDateTime(0'i64, isNull = true)
    check col.isNull == true
    check col.kind == cdtDateTime

  test "pkValueFromULID basic":
    var ulid: array[16, uint8]
    for i in 0..<16:
      ulid[i] = uint8(i + 1)
    let col = pkValueFromULID(ulid)
    check col.isNull == false
    check col.kind == cdtULID
    check col.ulidVal == ulid

  test "pkValueFromULID NULL":
    var ulid: array[16, uint8]
    let col = pkValueFromULID(ulid, isNull = true)
    check col.isNull == true
    check col.kind == cdtULID

# =============================================================================
# Additional Type Encoding Tests
# =============================================================================

suite "Date/DateTime Column Encoding":

  test "Encode/decode DATE column":
    var w = initBinaryWriter()
    let col = pkValueFromDate(1710000000000000000'i64)
    encodePkColumn(col, w)
    let data = w.finish()

    var r = initBinaryReader(data)
    let decoded = decodePkColumn(r, cdtDate)
    check decoded.isNull == false
    check decoded.dateVal == 1710000000000000000'i64

  test "Encode/decode NULL DATE column":
    var w = initBinaryWriter()
    let col = pkValueFromDate(0'i64, isNull = true)
    encodePkColumn(col, w)
    let data = w.finish()

    var r = initBinaryReader(data)
    let decoded = decodePkColumn(r, cdtDate)
    check decoded.isNull == true

  test "Encode/decode DATETIME column":
    var w = initBinaryWriter()
    let col = pkValueFromDateTime(1710000000123456789'i64)
    encodePkColumn(col, w)
    let data = w.finish()

    var r = initBinaryReader(data)
    let decoded = decodePkColumn(r, cdtDateTime)
    check decoded.isNull == false
    check decoded.datetimeVal == 1710000000123456789'i64

  test "Encode/decode NULL DATETIME column":
    var w = initBinaryWriter()
    let col = pkValueFromDateTime(0'i64, isNull = true)
    encodePkColumn(col, w)
    let data = w.finish()

    var r = initBinaryReader(data)
    let decoded = decodePkColumn(r, cdtDateTime)
    check decoded.isNull == true

suite "ULID Column Encoding":

  test "Encode/decode ULID column":
    var w = initBinaryWriter()
    var ulid: array[16, uint8]
    for i in 0..<16:
      ulid[i] = uint8(i * 16)
    let col = pkValueFromULID(ulid)
    encodePkColumn(col, w)
    let data = w.finish()

    # Total bytes: 1 (flag) + 16 (value) = 17
    check data.len == 17

    var r = initBinaryReader(data)
    let decoded = decodePkColumn(r, cdtULID)
    check decoded.isNull == false
    check decoded.ulidVal == ulid

  test "Encode/decode NULL ULID column":
    var w = initBinaryWriter()
    var ulid: array[16, uint8]
    let col = pkValueFromULID(ulid, isNull = true)
    encodePkColumn(col, w)
    let data = w.finish()

    var r = initBinaryReader(data)
    let decoded = decodePkColumn(r, cdtULID)
    check decoded.isNull == true

suite "Bytes Column Encoding":

  test "Encode/decode BYTES column":
    var w = initBinaryWriter()
    let col = PrimaryKeyColumnValue(
      isNull: false,
      kind: cdtBytes,
      bytesVal: @[0x01'u8, 0x02, 0x03, 0x04],
      bytesMaxLen: 16
    )
    encodePkColumn(col, w)
    let data = w.finish()

    # Total bytes: 1 (flag) + 16 (value) = 17
    check data.len == 17

    var r = initBinaryReader(data)
    let decoded = decodePkColumn(r, cdtBytes, maxLen = 16)
    check decoded.isNull == false
    check decoded.bytesVal == @[0x01'u8, 0x02, 0x03, 0x04]

  test "Encode/decode NULL BYTES column":
    var w = initBinaryWriter()
    let col = PrimaryKeyColumnValue(
      isNull: true,
      kind: cdtBytes,
      bytesVal: @[],
      bytesMaxLen: 16
    )
    encodePkColumn(col, w)
    let data = w.finish()

    var r = initBinaryReader(data)
    let decoded = decodePkColumn(r, cdtBytes, maxLen = 16)
    check decoded.isNull == true

  test "BYTES exceeding max length raises error":
    var w = initBinaryWriter()
    let col = PrimaryKeyColumnValue(
      isNull: false,
      kind: cdtBytes,
      bytesVal: @[0x01'u8, 0x02, 0x03, 0x04, 0x05], # 5 bytes
      bytesMaxLen: 4 # max is 4
    )
    var raised = false
    try:
      encodePkColumn(col, w)
    except ValueError:
      raised = true
    check raised

# =============================================================================
# Debug/Display Helper Tests
# =============================================================================

suite "Debug Helpers":

  test "pkColumnToString INT":
    let col = pkValueFromInt(42)
    check pkColumnToString(col) == "42"

  test "pkColumnToString INT negative":
    let col = pkValueFromInt(-100)
    check pkColumnToString(col) == "-100"

  test "pkColumnToString FLOAT":
    let col = pkValueFromFloat(3.14)
    check pkColumnToString(col) == "3.14"

  test "pkColumnToString STRING":
    let col = pkValueFromString("hello", maxLen = 32)
    check pkColumnToString(col) == "hello"

  test "pkColumnToString BOOL true":
    let col = pkValueFromBool(true)
    check pkColumnToString(col) == "true"

  test "pkColumnToString BOOL false":
    let col = pkValueFromBool(false)
    check pkColumnToString(col) == "false"

  test "pkColumnToString DATE":
    let col = pkValueFromDate(1710000000000000000'i64)
    check pkColumnToString(col) == "1710000000000000000"

  test "pkColumnToString DATETIME":
    let col = pkValueFromDateTime(1710000000123456789'i64)
    check pkColumnToString(col) == "1710000000123456789"

  test "pkColumnToString NULL":
    let col = pkValueFromInt(0, isNull = true)
    check pkColumnToString(col) == "NULL"

  test "pkColumnToString BYTES":
    let col = PrimaryKeyColumnValue(
      isNull: false,
      kind: cdtBytes,
      bytesVal: @[0x01'u8, 0x02, 0x03],
      bytesMaxLen: 16
    )
    let s = pkColumnToString(col)
    check s.len > 0
    # The output format is "bytes:N" where N is the length
    check s[0..5] == "bytes:"

  test "pkColumnToString ULID":
    var ulid: array[16, uint8]
    for i in 0..<16:
      ulid[i] = 0x41'u8 # 'A'
    let col = pkValueFromULID(ulid)
    let s = pkColumnToString(col)
    check s.len == 16

  test "pkToString single column":
    let pk: PrimaryKey = @[pkValueFromInt(42)]
    check pkToString(pk) == "(42)"

  test "pkToString multiple columns":
    let pk: PrimaryKey = @[
      pkValueFromInt(42),
      pkValueFromString("test", maxLen = 16)
    ]
    check pkToString(pk) == "(42, test)"

  test "pkToString with NULL":
    let pk: PrimaryKey = @[
      pkValueFromInt(42),
      pkValueFromInt(0, isNull = true)
    ]
    check pkToString(pk) == "(42, NULL)"

  test "pkToString empty":
    let pk: PrimaryKey = @[]
    check pkToString(pk) == "()"

# =============================================================================
# Edge Case Tests
# =============================================================================

suite "Edge Cases":

  test "Very long composite key":
    let spec = PrimaryKeySpec(columns: @[
      ("id", cdtInt, 0),
      ("region", cdtString, 32),
      ("subregion", cdtString, 64),
      ("seq", cdtInt, 0)
    ])

    let pk: PrimaryKey = @[
      pkValueFromInt(999999),
      pkValueFromString("north-america-us-west-california", maxLen = 32),
      pkValueFromString("sf-bay-area", maxLen = 64),
      pkValueFromInt(12345)
    ]

    let encoded = encodePrimaryKey(pk, spec)
    let decoded = decodePrimaryKey(encoded, spec)

    check decoded.len == 4
    check decoded[0].intVal == 999999
    check decoded[3].intVal == 12345

  test "Round-trip all types":
    let spec = PrimaryKeySpec(columns: @[
      ("int_col", cdtInt, 0),
      ("float_col", cdtFloat, 0),
      ("str_col", cdtString, 16),
      ("bool_col", cdtBool, 0)
    ])

    let pk: PrimaryKey = @[
      pkValueFromInt(-999),
      pkValueFromFloat(-3.14159),
      pkValueFromString("test", maxLen = 16),
      pkValueFromBool(false)
    ]

    let encoded = encodePrimaryKey(pk, spec)
    let decoded = decodePrimaryKey(encoded, spec)

    check decoded[0].intVal == -999
    check decoded[1].floatVal == -3.14159
    check decoded[2].strVal == "test"
    check decoded[3].boolVal == false

# =============================================================================
# PrimaryKeySpec from TableRecord Tests
# =============================================================================
# ColumnFlags bit positions: cfPrimaryKey=0, cfNotNull=1, cfUnique=2

suite "PrimaryKeySpec from TableRecord":

  test "primaryKeySpecFromTable single PK column":
    let rec = TableRecord(
      tableId: genTableId(),
      name: "users",
      schema: "public",
      database: "testdb",
      spaceId: genSpaceID(),
      columns: @[
        ColumnDefBin(name: "id", dataType: cdtInt, maxLen: 0,
            flags: uint8(1 shl int(cfPrimaryKey) or 1 shl int(cfNotNull))),
        ColumnDefBin(name: "name", dataType: cdtString, maxLen: 64, flags: 0)
      ],
      primaryKey: @["id"]
    )
    let spec = primaryKeySpecFromTable(rec)
    check spec.columns.len == 1
    check spec.columns[0].name == "id"
    check spec.columns[0].dataType == cdtInt
    check spec.columns[0].maxLen == 0

  test "primaryKeySpecFromTable composite PK":
    let rec = TableRecord(
      tableId: genTableId(),
      name: "orders",
      schema: "public",
      database: "testdb",
      spaceId: genSpaceID(),
      columns: @[
        ColumnDefBin(name: "user_id", dataType: cdtInt, maxLen: 0,
            flags: uint8(1 shl int(cfPrimaryKey) or 1 shl int(cfNotNull))),
        ColumnDefBin(name: "order_id", dataType: cdtInt, maxLen: 0,
            flags: uint8(1 shl int(cfPrimaryKey) or 1 shl int(cfNotNull))),
        ColumnDefBin(name: "status", dataType: cdtString, maxLen: 32, flags: 0)
      ],
      primaryKey: @["user_id", "order_id"]
    )
    let spec = primaryKeySpecFromTable(rec)
    check spec.columns.len == 2
    check spec.columns[0].name == "user_id"
    check spec.columns[1].name == "order_id"

  test "primaryKeySpecFromTable with string PK":
    let rec = TableRecord(
      tableId: genTableId(),
      name: "countries",
      schema: "public",
      database: "testdb",
      spaceId: genSpaceID(),
      columns: @[
        ColumnDefBin(name: "code", dataType: cdtString, maxLen: 4,
            flags: uint8(1 shl int(cfPrimaryKey) or 1 shl int(cfNotNull))),
        ColumnDefBin(name: "name", dataType: cdtString, maxLen: 64, flags: 0)
      ],
      primaryKey: @["code"]
    )
    let spec = primaryKeySpecFromTable(rec)
    check spec.columns.len == 1
    check spec.columns[0].dataType == cdtString
    check spec.columns[0].maxLen == 4

  test "primaryKeySpecFromTable missing PK column raises error":
    let rec = TableRecord(
      tableId: genTableId(),
      name: "bad_table",
      schema: "public",
      database: "testdb",
      spaceId: genSpaceID(),
      columns: @[
        ColumnDefBin(name: "id", dataType: cdtInt, maxLen: 0, flags: 0)
      ],
      primaryKey: @["missing_column"]
    )
    var raised = false
    try:
      discard primaryKeySpecFromTable(rec)
    except ValueError:
      raised = true
    check raised
