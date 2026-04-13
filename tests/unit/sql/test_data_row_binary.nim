# Unit Tests for DataRow Binary Serialization

import unittest
import std/[strutils, sequtils, options]
import fractio/sql/data_row
import fractio/utils/binary

suite "DataRowValue Constructors Tests":

  test "Create null value":
    let v = newRowValue()
    check v.kind == drvkNull
    check v.isNull()

  test "Create integer value":
    let v = newRowValue(42'i64)
    check v.kind == drvkInt
    check v.getInt() == 42
    check not v.isNull()

  test "Create float value":
    let v = newRowValue(3.14)
    check v.kind == drvkFloat
    check v.getFloat() == 3.14

  test "Create string value":
    let v = newRowValue("hello")
    check v.kind == drvkString
    check v.getString() == "hello"

  test "Create bool value":
    let v = newRowValue(true)
    check v.kind == drvkBool
    check v.getBool() == true

suite "DataRow Constructors Tests":

  test "Create empty row":
    let row = newDataRow()
    check row.columns.len == 0

  test "Create row with columns":
    let row = newDataRow(@[
      newColumn("id", newRowValue(1'i64)),
      newColumn("name", newRowValue("test"))
    ])
    check row.columns.len == 2
    check row.hasColumn("id")
    check row.hasColumn("name")

suite "DataRow Column Access Tests":

  test "Get column by name":
    let row = newDataRow(@[
      newColumn("id", newRowValue(123'i64)),
      newColumn("name", newRowValue("Alice"))
    ])
    let idVal = row["id"]
    check idVal.kind == drvkInt
    check idVal.getInt() == 123

    let nameVal = row["name"]
    check nameVal.kind == drvkString
    check nameVal.getString() == "Alice"

  test "Get missing column returns null":
    let row = newDataRow(@[
      newColumn("id", newRowValue(1'i64))
    ])
    let missing = row["nonexistent"]
    check missing.kind == drvkNull

  test "Set column value":
    var row = newDataRow()
    row["id"] = newRowValue(42'i64)
    check row.hasColumn("id")
    check row["id"].getInt() == 42

  test "Update existing column":
    var row = newDataRow(@[
      newColumn("count", newRowValue(0'i64))
    ])
    row["count"] = newRowValue(10'i64)
    check row["count"].getInt() == 10

suite "DataRowValue Comparison Tests":

  test "Integer equality":
    let a = newRowValue(10'i64)
    let b = newRowValue(10'i64)
    let c = newRowValue(20'i64)
    check a == b
    check a != c

  test "String equality":
    let a = newRowValue("hello")
    let b = newRowValue("hello")
    let c = newRowValue("world")
    check a == b
    check a != c

  test "Integer ordering":
    let a = newRowValue(5'i64)
    let b = newRowValue(10'i64)
    check a < b
    check a <= b
    check b > a
    check b >= a

  test "String ordering":
    let a = newRowValue("apple")
    let b = newRowValue("banana")
    check a < b
    check a <= b

  test "Different types not equal":
    let a = newRowValue(10'i64)
    let b = newRowValue("10")
    check a != b

suite "DataRowValue Arithmetic Tests":

  test "Integer addition":
    let a = newRowValue(5'i64)
    let b = newRowValue(3'i64)
    let result = a + b
    check result.kind == drvkInt
    check result.getInt() == 8

  test "Integer subtraction":
    let a = newRowValue(10'i64)
    let b = newRowValue(3'i64)
    let result = a - b
    check result.kind == drvkInt
    check result.getInt() == 7

  test "Integer multiplication":
    let a = newRowValue(6'i64)
    let b = newRowValue(7'i64)
    let result = a * b
    check result.kind == drvkInt
    check result.getInt() == 42

  test "Integer division":
    let a = newRowValue(20'i64)
    let b = newRowValue(4'i64)
    let result = a div b
    check result.kind == drvkInt
    check result.getInt() == 5

  test "Integer modulo":
    let a = newRowValue(17'i64)
    let b = newRowValue(5'i64)
    let result = a mod b
    check result.kind == drvkInt
    check result.getInt() == 2

  test "Integer negation":
    let a = newRowValue(42'i64)
    let result = -a
    check result.kind == drvkInt
    check result.getInt() == -42

  test "Division by zero returns null":
    let a = newRowValue(10'i64)
    let b = newRowValue(0'i64)
    let result = a div b
    check result.kind == drvkNull

suite "DataRowValue Logic Tests":

  test "Boolean AND":
    let a = newRowValue(true)
    let b = newRowValue(false)
    check (a and b).getBool() == false
    check (a and newRowValue(true)).getBool() == true

  test "Boolean OR":
    let a = newRowValue(true)
    let b = newRowValue(false)
    check (a or b).getBool() == true
    check (newRowValue(false) or newRowValue(false)).getBool() == false

  test "Boolean NOT":
    let a = newRowValue(true)
    check (not a).getBool() == false
    check (not newRowValue(false)).getBool() == true

suite "DataRow Binary Encoding Tests":

  test "Encode empty row":
    let row = newDataRow()
    let encoded = encodeDataRow(row)
    check encoded.len >= 7
    check encoded[0] == 'D'
    check encoded[1] == 'R'
    check encoded[2].ord == 1 # version

  test "Encode row with single int column":
    let row = newDataRow(@[
      newColumn("id", newRowValue(42'i64))
    ])
    let encoded = encodeDataRow(row)
    check encoded[0] == 'D'
    check encoded[1] == 'R'

  test "Encode row with multiple columns":
    let row = newDataRow(@[
      newColumn("id", newRowValue(1'i64)),
      newColumn("name", newRowValue("test")),
      newColumn("active", newRowValue(true))
    ])
    let encoded = encodeDataRow(row)
    check encoded[0] == 'D'
    check encoded[1] == 'R'

  test "Round-trip empty row":
    let original = newDataRow()
    let encoded = encodeDataRow(original)
    let restored = decodeDataRow(encoded)
    check restored.columns.len == 0

  test "Round-trip row with integer":
    let original = newDataRow(@[
      newColumn("count", newRowValue(12345'i64))
    ])
    let encoded = encodeDataRow(original)
    let restored = decodeDataRow(encoded)
    check restored.columns.len == 1
    check restored.hasColumn("count")
    check restored["count"].getInt() == 12345

  test "Round-trip row with string":
    let original = newDataRow(@[
      newColumn("message", newRowValue("Hello, World!"))
    ])
    let encoded = encodeDataRow(original)
    let restored = decodeDataRow(encoded)
    check restored["message"].getString() == "Hello, World!"

  test "Round-trip row with bool":
    let original = newDataRow(@[
      newColumn("enabled", newRowValue(true))
    ])
    let encoded = encodeDataRow(original)
    let restored = decodeDataRow(encoded)
    check restored["enabled"].getBool() == true

  test "Round-trip row with null":
    let original = newDataRow(@[
      newColumn("nullable", newRowValue())
    ])
    let encoded = encodeDataRow(original)
    let restored = decodeDataRow(encoded)
    check restored["nullable"].isNull()

  test "Round-trip row with float":
    let original = newDataRow(@[
      newColumn("price", newRowValue(19.99))
    ])
    let encoded = encodeDataRow(original)
    let restored = decodeDataRow(encoded)
    check restored["price"].getFloat() == 19.99

  test "Round-trip complete row":
    let original = newDataRow(@[
      newColumn("id", newRowValue(1'i64)),
      newColumn("name", newRowValue("Alice")),
      newColumn("age", newRowValue(30'i64)),
      newColumn("active", newRowValue(true)),
      newColumn("salary", newRowValue(50000.0)),
      newColumn("notes", newRowValue())
    ])
    let encoded = encodeDataRow(original)
    let restored = decodeDataRow(encoded)

    check restored.columns.len == 6
    check restored["id"].getInt() == 1
    check restored["name"].getString() == "Alice"
    check restored["age"].getInt() == 30
    check restored["active"].getBool() == true
    check restored["salary"].getFloat() == 50000.0
    check restored["notes"].isNull()

  test "Decode invalid magic":
    let invalidData = "INVALID"
    var raised = false
    try:
      discard decodeDataRow(invalidData)
    except ValueError:
      raised = true
    check raised

  test "Encode row with large string":
    let largeString = "x".repeat(10000)
    let original = newDataRow(@[
      newColumn("data", newRowValue(largeString))
    ])
    let encoded = encodeDataRow(original)
    let restored = decodeDataRow(encoded)
    check restored["data"].getString() == largeString

suite "DataRow String Conversion Tests":

  test "ToStringValue for int":
    let v = newRowValue(42'i64)
    check v.toStringValue() == "42"

  test "ToStringValue for string":
    let v = newRowValue("hello")
    check v.toStringValue() == "hello"

  test "ToStringValue for bool":
    check newRowValue(true).toStringValue() == "true"
    check newRowValue(false).toStringValue() == "false"

  test "ToStringValue for null":
    let v = newRowValue()
    check v.toStringValue() == "NULL"

  test "ToStringRow":
    let row = newDataRow(@[
      newColumn("id", newRowValue(1'i64)),
      newColumn("name", newRowValue("test"))
    ])
    let strRow = toStringRow(row, @["id", "name"])
    check strRow == @["1", "test"]

suite "DataRow Error Cases":

  test "Decode data too small":
    var raised = false
    try:
      discard decodeDataRow("DR") # Only 2 bytes, need at least 7
    except ValueError:
      raised = true
    check raised

  test "Decode unsupported version":
    # Valid magic but wrong version
    let data = @[0x44'u8, 0x52'u8, 0x02'u8, 0x00'u8, 0x00'u8, 0x00'u8, 0x00'u8]
    var raised = false
    try:
      discard decodeDataRow(data.mapIt(chr(it)).join)
    except ValueError:
      raised = true
    check raised

  test "Decode completely wrong magic":
    let data = @[0xFF'u8, 0xFF'u8, 0x01'u8, 0x00'u8, 0x00'u8, 0x00'u8, 0x00'u8]
    var raised = false
    try:
      discard decodeDataRow(data.mapIt(chr(it)).join)
    except ValueError:
      raised = true
    check raised

suite "DataRow Column Index Tests":

  test "getColumnIdx finds column":
    let row = newDataRow(@[
      newColumn("id", newRowValue(1'i64)),
      newColumn("name", newRowValue("test"))
    ])
    check row.getColumnIdx("id") == 0
    check row.getColumnIdx("name") == 1

  test "getColumnIdx returns -1 for missing":
    let row = newDataRow(@[
      newColumn("id", newRowValue(1'i64))
    ])
    check row.getColumnIdx("missing") == -1

  test "getColumn returns some":
    let row = newDataRow(@[
      newColumn("count", newRowValue(42'i64))
    ])
    let opt = row.getColumn("count")
    check opt.isSome
    check opt.get.getInt() == 42

  test "getColumn returns none for missing":
    let row = newDataRow()
    let opt = row.getColumn("missing")
    check opt.isNone

suite "DataRowValue Default Value Tests":

  test "getInt with default":
    let nullVal = newRowValue()
    check nullVal.getInt(99) == 99
    let strVal = newRowValue("text")
    check strVal.getInt(99) == 99

  test "getFloat with default":
    let nullVal = newRowValue()
    check nullVal.getFloat(3.14) == 3.14
    let intVal = newRowValue(10'i64)
    check intVal.getFloat(3.14) == 3.14

  test "getString with default":
    let nullVal = newRowValue()
    check nullVal.getString("default") == "default"
    let intVal = newRowValue(10'i64)
    check intVal.getString("default") == "default"

  test "getBool with default":
    let nullVal = newRowValue()
    check nullVal.getBool(true) == true
    let intVal = newRowValue(10'i64)
    check intVal.getBool(true) == true

suite "DataRowValue Mixed Type Comparison Tests":

  test "Different types equality returns false":
    let a = newRowValue(10'i64)
    let b = newRowValue(10.0)
    check (a == b) == false # Different kinds, so false
    check (a == newRowValue("10")) == false

  test "Different types ordering returns false":
    let a = newRowValue(5'i64)
    let b = newRowValue("10")
    check (a < b) == false # Different kinds, false
    check (a <= b) == false # Different kinds, false
    check (a > b) == false # Different kinds, false

  test "Null equality":
    let a = newRowValue()
    let b = newRowValue()
    check a == b

  test "Null ordering":
    let a = newRowValue()
    let b = newRowValue()
    check (a < b) == false # Null < Null is false
    check a <= b # Null <= Null is true

  test "Bool ordering":
    let a = newRowValue(false)
    let b = newRowValue(true)
    check a < b
    check a <= b
    check b > a

suite "DataRowValue Float Ordering Tests":
  test "Float ordering less than":
    let a = newRowValue(1.5)
    let b = newRowValue(3.5)
    check a < b
    check a <= b
    check not (a > b)
    check not (a >= b)

  test "Float ordering greater than":
    let a = newRowValue(5.5)
    let b = newRowValue(2.5)
    check a > b
    check a >= b
    check not (a < b)
    check not (a <= b)

  test "Float ordering equal":
    let a = newRowValue(3.14)
    let b = newRowValue(3.14)
    check not (a < b)
    check a <= b
    check not (a > b)
    check a >= b

  test "Float ordering negative values":
    let a = newRowValue(-5.0)
    let b = newRowValue(-2.0)
    check a < b
    check a <= b

  test "Float ordering negative and positive":
    let a = newRowValue(-1.0)
    let b = newRowValue(1.0)
    check a < b
    check b > a

suite "DataRowValue Mixed Type Arithmetic Tests":

  test "Add non-int returns null":
    let a = newRowValue(5'i64)
    let b = newRowValue(3.0)
    check (a + b).kind == drvkNull
    check (newRowValue("a") + newRowValue("b")).kind == drvkNull

  test "Subtract non-int returns null":
    let a = newRowValue(10'i64)
    let b = newRowValue("3")
    check (a - b).kind == drvkNull

  test "Multiply non-int returns null":
    let a = newRowValue(6'i64)
    let b = newRowValue(true)
    check (a * b).kind == drvkNull

  test "Divide non-int returns null":
    let a = newRowValue(20'i64)
    let b = newRowValue(4.0)
    check (a div b).kind == drvkNull

  test "Modulo non-int returns null":
    let a = newRowValue(17'i64)
    let b = newRowValue(5.0)
    check (a mod b).kind == drvkNull

  test "Modulo by zero returns null":
    let a = newRowValue(10'i64)
    let b = newRowValue(0'i64)
    check (a mod b).kind == drvkNull

  test "Negate non-int returns null":
    let a = newRowValue(3.14)
    check (-a).kind == drvkNull
    check (-newRowValue("text")).kind == drvkNull

suite "DataRowValue Mixed Type Logic Tests":

  test "AND non-bool returns null":
    let a = newRowValue(true)
    let b = newRowValue(1'i64)
    check (a and b).kind == drvkNull
    check (newRowValue(1'i64) and newRowValue(2'i64)).kind == drvkNull

  test "OR non-bool returns null":
    let a = newRowValue(true)
    let b = newRowValue("false")
    check (a or b).kind == drvkNull

  test "NOT non-bool returns null":
    let a = newRowValue(1'i64)
    check (not a).kind == drvkNull
    check (not newRowValue()).kind == drvkNull

suite "DataRowValue Float Tests":

  test "Float round-trip":
    let original = newRowValue(3.14159265358979)
    let row = newDataRow(@[newColumn("pi", original)])
    let encoded = encodeDataRow(row)
    let restored = decodeDataRow(encoded)
    check restored["pi"].getFloat() == 3.14159265358979

  test "Float zero":
    let v = newRowValue(0.0)
    check v.kind == drvkFloat
    check v.getFloat() == 0.0

  test "Negative float":
    let v = newRowValue(-123.45)
    check v.getFloat() == -123.45

  test "Very large float":
    let v = newRowValue(1e308)
    check v.getFloat() == 1e308

  test "Very small float":
    let v = newRowValue(1e-308)
    check v.getFloat() == 1e-308

suite "DataRowValue Special Cases":

  test "Empty string value":
    let v = newRowValue("")
    check v.kind == drvkString
    check v.getString() == ""

  test "Int min value":
    let v = newRowValue(int64.low)
    check v.getInt() == int64.low

  test "Int max value":
    let v = newRowValue(int64.high)
    check v.getInt() == int64.high

  test "Negative int":
    let v = newRowValue(-1'i64)
    check v.getInt() == -1

suite "DataRow Column Manipulation":

  test "SetColumn adds new column":
    var row = newDataRow()
    row.setColumn("count", newRowValue(5'i64))
    check row.columns.len == 1
    check row["count"].getInt() == 5

  test "SetColumn updates existing":
    var row = newDataRow(@[
      newColumn("count", newRowValue(0'i64))
    ])
    row.setColumn("count", newRowValue(100'i64))
    check row.columns.len == 1 # Not added, just updated
    check row["count"].getInt() == 100

  test "Bracket assignment":
    var row = newDataRow()
    row["status"] = newRowValue("active")
    check row.hasColumn("status")
    check row["status"].getString() == "active"

suite "DataRow ToStringRow Edge Cases":

  test "ToStringRow with missing column":
    let row = newDataRow(@[
      newColumn("id", newRowValue(1'i64))
    ])
    let strRow = toStringRow(row, @["id", "missing"])
    check strRow == @["1", "NULL"] # Missing returns null

  test "ToStringRow with null value":
    let row = newDataRow(@[
      newColumn("id", newRowValue(1'i64)),
      newColumn("nullable", newRowValue())
    ])
    let strRow = toStringRow(row, @["id", "nullable"])
    check strRow == @["1", "NULL"]

  test "ToStringRow with float":
    let row = newDataRow(@[
      newColumn("price", newRowValue(19.99))
    ])
    let strRow = toStringRow(row, @["price"])
    check strRow[0].contains("19.99")
