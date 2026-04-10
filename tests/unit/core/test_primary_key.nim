# Unit Tests for Binary Primary Key Encoding
#
# Tests the fixed-width binary encoding for primary keys.
# Verifies correct sorting behavior for all supported types.

import unittest
import std/[strformat, algorithm, sequtils]

import fractio/utils/binary
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
