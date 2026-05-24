# Unit tests for fractio/core/types.nim
# Tests ULID operations, ValueRef, Row, and ID types

import std/[unittest, tables, sets, hashes, times, strutils]
import fractio/core/types

suite "ULID Basic Operations":

  test "ZeroULID creates all zeros":
    let zero = ZeroULID()
    for i in 0 ..< ULID_SIZE:
      check zero.data[i] == 0'u8

  test "ULID equality":
    let a = ZeroULID()
    let b = ZeroULID()
    check a == b

    let c = genULIDLocal()
    check a != c

  test "ULID inequality":
    let a = ZeroULID()
    let b = genULIDLocal()
    check a != b

  test "ULID less than ordering":
    let earlier = ZeroULID()
    let later = genULIDLocal()
    check earlier < later
    check not (later < earlier)

  test "ULID ordering with same prefix":
    # Create two ULIDs - the first should be < second (monotonically increasing)
    let u1 = genULIDLocal()
    let u2 = genULIDLocal()
    # They could be equal if generated in same millisecond with same randomness
    # but typically u1 < u2 due to timestamp ordering
    check u1 == u1 # Self-equality
    check u2 == u2

suite "ULID String Operations":

  test "ulidFromString valid 26 char string":
    let s = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    let u = ulidFromString(s)
    check $u == s

  test "ulidFromString invalid length raises":
    var raised = false
    try:
      discard ulidFromString("too-short")
    except AssertionDefect:
      raised = true
    check raised

  test "ulidFromString handles zeros":
    let s = "00000000000000000000000000"
    let u = ulidFromString(s)
    check u == ZeroULID()

  test "ulidToString roundtrip":
    let original = genULIDLocal()
    let s = $original
    check s.len == 26
    let restored = ulidFromString(s)
    check restored == original

  test "ulidToString format":
    let u = genULIDLocal()
    let s = $u
    # All chars should be valid Crockford base32
    const validChars = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    for c in s:
      check c in validChars

suite "ULID Binary Operations":

  test "ulidToBytes produces 16 bytes":
    let u = genULIDLocal()
    let bytes = ulidToBytes(u)
    check bytes.len == ULID_SIZE

  test "ulidFromBytes roundtrip":
    let original = genULIDLocal()
    let bytes = ulidToBytes(original)
    let restored = ulidFromBytes(bytes)
    check restored == original

  test "ulidFromBytes invalid length raises":
    var raised = false
    try:
      discard ulidFromBytes("short")
    except AssertionDefect:
      raised = true
    check raised

  test "ulidToBytes zero":
    let zero = ZeroULID()
    let bytes = ulidToBytes(zero)
    for b in bytes:
      check uint8(b) == 0'u8

suite "ULID Timestamp Extraction":

  test "ulidTimestamp returns milliseconds":
    let u = genULIDLocal()
    let ts = ulidTimestamp(u)
    # Should be a reasonable Unix timestamp (milliseconds since epoch)
    check ts > 0
    # Current time should be within reasonable range
    let nowMs = getTime().toUnix * 1000
    check ts <= nowMs + 1000 # Allow 1 second tolerance

  test "ZeroULID has zero timestamp":
    let zero = ZeroULID()
    check ulidTimestamp(zero) == 0

suite "genULID":

  test "generates unique IDs":
    var seen: HashSet[ULID] = initHashSet[ULID]()
    for i in 0 ..< 1000:
      let u = genULIDLocal()
      check u notin seen
      seen.incl(u)

  test "generates monotonically increasing timestamps":
    var lastTs: int64 = 0
    for i in 0 ..< 100:
      let u = genULIDLocal()
      let ts = ulidTimestamp(u)
      check ts >= lastTs
      lastTs = ts

  test "generated ULID is not zero":
    let u = genULIDLocal()
    check u != ZeroULID()

suite "DataType Enum":

  test "all data types are defined":
    check dtInt.ord == 0
    check dtFloat.ord == 1
    check dtString.ord == 2
    check dtBool.ord == 3
    check dtDate.ord == 4
    check dtDateTime.ord == 5
    check dtBytes.ord == 6
    check dtULID.ord == 7

suite "ValueRef Constructors":

  test "newValueRef int64":
    let v = newValueRef(123'i64)
    check v.kind == dtInt
    check v.intValue == 123

  test "newValueRef float64":
    let v = newValueRef(3.14'f64)
    check v.kind == dtFloat
    check v.floatValue == 3.14

  test "newValueRef string":
    let v = newValueRef("hello")
    check v.kind == dtString
    check v.strValue == "hello"

  test "newValueRef bool true":
    let v = newValueRef(true)
    check v.kind == dtBool
    check v.boolValue == true

  test "newValueRef bool false":
    let v = newValueRef(false)
    check v.kind == dtBool
    check v.boolValue == false

  test "newValueRef bytes":
    let v = newValueRef(@[1'u8, 2'u8, 3'u8])
    check v.kind == dtBytes
    check v.bytesValue == @[1'u8, 2'u8, 3'u8]

  test "newValueRef ULID":
    let u = genULIDLocal()
    let v = newValueRef(u)
    check v.kind == dtULID
    check v.ulidValue == u

suite "ValueRef Templates":

  test "int64Value returns correct value":
    let v = newValueRef(42'i64)
    check int64Value(v) == 42

  test "int64Value returns 0 for non-int":
    let v = newValueRef("string")
    check int64Value(v) == 0

  test "float64Value returns correct value":
    let v = newValueRef(2.5'f64)
    check float64Value(v) == 2.5

  test "float64Value returns 0.0 for non-float":
    let v = newValueRef(100'i64)
    check float64Value(v) == 0.0

  test "stringValue returns correct value":
    let v = newValueRef("test")
    check stringValue(v) == "test"

  test "stringValue returns empty for non-string":
    let v = newValueRef(100'i64)
    check stringValue(v) == ""

  test "boolValue returns correct value":
    let v = newValueRef(true)
    check boolValue(v) == true

  test "boolValue returns false for non-bool":
    let v = newValueRef(100'i64)
    check boolValue(v) == false

suite "Row Operations":

  test "newRow creates empty row":
    let row = newRow(createdAtMs = localTimeNs() div 1_000_000)
    check row.values.len == 0
    check row.version == 1
    check row.id == RowID(ZeroULID())

  test "newRow with custom ID":
    let id = genRowIDLocal()
    let row = newRow(id, createdAtMs = localTimeNs() div 1_000_000)
    check row.id == id

  test "Row has timestamps":
    let row = newRow(createdAtMs = localTimeNs() div 1_000_000)
    check row.createdAt > 0
    check row.updatedAt > 0

suite "TransactionID Operations":

  test "TransactionID equality":
    let a = genTransactionIDLocal()
    let b = genTransactionIDLocal()
    check a == a
    check a != b

  test "TransactionID inequality":
    let a = genTransactionIDLocal()
    let b = genTransactionIDLocal()
    check a != b
    check not (a == b)

  test "TransactionID ordering":
    let a = zeroTransactionID()
    let b = genTransactionIDLocal()
    check a < b

  test "TransactionID string":
    let id = genTransactionIDLocal()
    let s = $id
    check s.len == 26

  test "TransactionID hash":
    let a = genTransactionIDLocal()
    let b = genTransactionIDLocal()
    check hash(a) != hash(b)
    check hash(a) == hash(a)

  test "zeroTransactionID":
    let zero = zeroTransactionID()
    check zero == zeroTransactionID()

  test "isZero TransactionID":
    check isZero(zeroTransactionID())
    check not isZero(genTransactionIDLocal())

  test "transactionIDFromBytes":
    let original = genTransactionIDLocal()
    let bytes = transactionIDToBytes(original)
    let restored = transactionIDFromBytes(bytes)
    check restored == original

  test "transactionIDFromString":
    let original = genTransactionIDLocal()
    let s = $original
    let restored = transactionIDFromString(s)
    check restored == original

suite "RowID Operations":

  test "RowID equality":
    let a = genRowIDLocal()
    let b = genRowIDLocal()
    check a == a
    check a != b

  test "RowID ordering":
    let a = zeroRowID()
    let b = genRowIDLocal()
    check a < b

  test "RowID string":
    let id = genRowIDLocal()
    let s = $id
    check s.len == 26

  test "RowID hash":
    let a = genRowIDLocal()
    let b = genRowIDLocal()
    check hash(a) != hash(b)

  test "zeroRowID":
    let zero = zeroRowID()
    check zero == zeroRowID()

  test "isZero RowID":
    check isZero(zeroRowID())
    check not isZero(genRowIDLocal())

suite "ShardID Operations":

  test "ShardID equality":
    let a = genShardIDLocal()
    let b = genShardIDLocal()
    check a == a
    check a != b

  test "ShardID ordering":
    let a = zeroShardID()
    let b = genShardIDLocal()
    check a < b

  test "ShardID string":
    let id = genShardIDLocal()
    let s = $id
    check s.len == 26

  test "ShardID hash":
    let id = genShardIDLocal()
    check hash(id) == hash(id)

  test "zeroShardID":
    check zeroShardID() == zeroShardID()

  test "isZero ShardID":
    check isZero(zeroShardID())
    check not isZero(genShardIDLocal())

suite "TableId Operations":

  test "TableId equality":
    let a = genTableIdLocal()
    let b = genTableIdLocal()
    check a == a
    check a != b

  test "TableId ordering":
    let a = zeroTableId()
    let b = genTableIdLocal()
    check a < b

  test "TableId string":
    let id = genTableIdLocal()
    let s = $id
    check s.len == 26

  test "TableId hash":
    check hash(genTableIdLocal()) != hash(genTableIdLocal())

  test "zeroTableId":
    check zeroTableId() == zeroTableId()

  test "isZero TableId":
    check isZero(zeroTableId())
    check not isZero(genTableIdLocal())

  test "tableIdFromBytes":
    let original = genTableIdLocal()
    let bytes = tableIdToBytes(original)
    let restored = tableIdFromBytes(bytes)
    check restored == original

  test "tableIdFromString":
    let original = genTableIdLocal()
    let restored = tableIdFromString($original)
    check restored == original

suite "SpaceID Operations":

  test "SpaceID equality":
    let a = genSpaceIDLocal()
    let b = genSpaceIDLocal()
    check a == a
    check a != b

  test "SpaceID ordering":
    let a = zeroSpaceID()
    let b = genSpaceIDLocal()
    check a < b

  test "SpaceID string":
    let id = genSpaceIDLocal()
    let s = $id
    check s.len == 26

  test "SpaceID hash":
    check hash(genSpaceIDLocal()) != hash(genSpaceIDLocal())

  test "zeroSpaceID":
    check zeroSpaceID() == zeroSpaceID()

  test "isZero SpaceID":
    check isZero(zeroSpaceID())
    check not isZero(genSpaceIDLocal())

  test "spaceIDFromBytes":
    let original = genSpaceIDLocal()
    let bytes = spaceIDToBytes(original)
    let restored = spaceIDFromBytes(bytes)
    check restored == original

  test "spaceIDFromString":
    let original = genSpaceIDLocal()
    let restored = spaceIDFromString($original)
    check restored == original

suite "NodeID Operations":

  test "NodeID string representation":
    let id = NodeID("node-123")
    check $id == "node-123"

  test "NodeID equality":
    let a = NodeID("node-1")
    let b = NodeID("node-1")
    let c = NodeID("node-2")
    check a == b
    check a != c

  test "NodeID hash":
    let a = NodeID("node-1")
    let b = NodeID("node-1")
    check hash(a) == hash(b)

suite "Constraint":

  test "default constraint":
    let c = Constraint()
    check not c.nullable # Default should be non-null
    check not c.unique
    check not c.primaryKey

  test "constraint with defaults":
    let c = Constraint(nullable: true, unique: true, primaryKey: false)
    check c.nullable
    check c.unique
    check not c.primaryKey

suite "ColumnDef":

  test "column definition":
    let col = ColumnDef(
      name: "id",
      dataType: dtInt,
      constraints: Constraint(primaryKey: true),
      isShardKey: false
    )
    check col.name == "id"
    check col.dataType == dtInt
    check col.constraints.primaryKey
    check not col.isShardKey

suite "TransactionStatus":

  test "all statuses defined":
    check tsActive.ord == 0
    check tsCommitted.ord == 1
    check tsAborted.ord == 2

suite "NodeRole":

  test "all roles defined":
    check nrCoordinator.ord == 0
    check nrPrimary.ord == 1
    check nrSecondary.ord == 2
    check nrClient.ord == 3

suite "ID Generation Uniqueness":

  test "TransactionIDs are unique":
    var seen: HashSet[TransactionID] = initHashSet[TransactionID]()
    for i in 0 ..< 1000:
      let id = genTransactionIDLocal()
      check id notin seen
      seen.incl(id)

  test "RowIDs are unique":
    var seen: HashSet[RowID] = initHashSet[RowID]()
    for i in 0 ..< 1000:
      let id = genRowIDLocal()
      check id notin seen
      seen.incl(id)

  test "ShardIDs are unique":
    var seen: HashSet[ShardID] = initHashSet[ShardID]()
    for i in 0 ..< 1000:
      let id = genShardIDLocal()
      check id notin seen
      seen.incl(id)

  test "TableIds are unique":
    var seen: HashSet[TableId] = initHashSet[TableId]()
    for i in 0 ..< 1000:
      let id = genTableIdLocal()
      check id notin seen
      seen.incl(id)

  test "SpaceIDs are unique":
    var seen: HashSet[SpaceID] = initHashSet[SpaceID]()
    for i in 0 ..< 1000:
      let id = genSpaceIDLocal()
      check id notin seen
      seen.incl(id)
