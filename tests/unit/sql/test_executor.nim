import std/[unittest, options, json, locks, atomics, strutils, algorithm]
import std/tables as stdtables
import fractio/sql/ast
import fractio/sql/executor
import fractio/sql/expr_eval # for evalExprDataRow, matchesFilterDataRow
import fractio/sql/planner
import fractio/sql/data_row
import fractio/core/types
import fractio/core/primary_key # for PrimaryKeySpec, encodeInt64BE
import fractio/core/kv_interface # for KVOpResult, KVOpVoidResult types
import fractio/distributed/meta/system_schemas
import fractio/distributed/meta/system_tables
import fractio/distributed/raft/group_types
import fractio/client/fractio_client
import fractio/utils/binary

# =============================================================================
# Helper for PK encoding in tests
# =============================================================================

proc bytesToString*(arr: array[8, uint8]): string =
  ## Convert array of bytes to string
  result = newString(8)
  for i in 0..<8:
    result[i] = char(arr[i])

# =============================================================================
# Mock FractioClient for executor unit tests
# =============================================================================

type
  MockTxnState = object
    txnId: TransactionID
    readTimestamp: uint64
    writes: stdtables.Table[string, string] # pending writes in this txn
    deleted: stdtables.Table[string, bool]  # pending deletes in this txn

  MockFractioClient = ref object
    ## In-memory mock client for executor testing
    initialized: Atomic[bool]
    lock: Lock
    data: stdtables.Table[string, string] # committed data
    txnCounter: int64
    txnState: stdtables.Table[TransactionID, MockTxnState]
    nextTxnId: TransactionID
    nextReadTs: uint64
    spaceRecords: seq[string]             # encoded space records for scan

proc newMockFractioClient(): MockFractioClient =
  result = MockFractioClient()
  result.initialized.store(true, moRelaxed)
  initLock(result.lock)
  result.data = stdtables.initTable[string, string]()
  result.txnState = stdtables.initTable[TransactionID, MockTxnState]()
  result.nextTxnId = TransactionID(genULID())
  result.nextReadTs = 1

proc toFractioClient(m: MockFractioClient): FractioClient =
  ## Create a FractioClient that wraps the mock
  ## This is a workaround since FractioClient is a concrete type
  ## We use nil here and override calls via the mock
  nil

# Mock operations that match FractioClient signatures
proc mockBeginTxn(m: MockFractioClient): KVOpResult[tuple[txnId: TransactionID,
    readTimestamp: uint64]] =
  m.lock.acquire()
  try:
    var txn = MockTxnState()
    txn.txnId = m.nextTxnId
    txn.readTimestamp = m.nextReadTs
    txn.writes = stdtables.initTable[string, string]()
    txn.deleted = stdtables.initTable[string, bool]()
    m.txnState[m.nextTxnId] = txn
    m.nextTxnId = TransactionID(genULID())
    inc m.nextReadTs
    kvOpOk((txnId: txn.txnId, readTimestamp: txn.readTimestamp))
  finally:
    m.lock.release()

proc mockCommitTxn(m: MockFractioClient, txnId: TransactionID): KVOpVoidResult =
  m.lock.acquire()
  try:
    if txnId notin m.txnState:
      return kvVoidErr("transaction not found")
    let txn = m.txnState[txnId]
    for k, v in txn.writes:
      m.data[k] = v
    for k, _ in txn.deleted:
      m.data.del(k)
    m.txnState.del(txnId)
    kvVoidOk()
  finally:
    m.lock.release()

proc mockRollbackTxn(m: MockFractioClient,
    txnId: TransactionID): KVOpVoidResult =
  m.lock.acquire()
  try:
    if txnId in m.txnState:
      m.txnState.del(txnId)
    kvVoidOk()
  finally:
    m.lock.release()

proc mockKvGet(m: MockFractioClient, key: string,
    txnId: TransactionID = zeroTransactionID(),
    readTimestamp: uint64 = 0): KVOpResult[Option[string]] =
  m.lock.acquire()
  try:
    if txnId != zeroTransactionID() and txnId in m.txnState:
      let txn = m.txnState[txnId]
      if key in txn.deleted:
        return kvOpOk(none(string))
      if key in txn.writes:
        return kvOpOk(some(txn.writes[key]))
    if key in m.data:
      kvOpOk(some(m.data[key]))
    else:
      kvOpOk(none(string))
  finally:
    m.lock.release()

proc mockKvPut(m: MockFractioClient, key, value: string,
    txnId: TransactionID = zeroTransactionID()): KVOpVoidResult =
  m.lock.acquire()
  try:
    if txnId != zeroTransactionID():
      if txnId notin m.txnState:
        return kvVoidErr("transaction not found")
      m.txnState[txnId].writes[key] = value
    else:
      m.data[key] = value
    kvVoidOk()
  finally:
    m.lock.release()

proc mockKvDelete(m: MockFractioClient, key: string,
    txnId: TransactionID = zeroTransactionID()): KVOpVoidResult =
  m.lock.acquire()
  try:
    if txnId != zeroTransactionID():
      if txnId notin m.txnState:
        return kvVoidErr("transaction not found")
      m.txnState[txnId].deleted[key] = true
    else:
      m.data.del(key)
    kvVoidOk()
  finally:
    m.lock.release()

proc mockKvScan(m: MockFractioClient, startKey, endKey: string,
    limit: uint32 = 0, txnId: TransactionID = zeroTransactionID(),
        readTimestamp: uint64 = 0): KVOpResult[seq[tuple[key, value: string]]] =
  m.lock.acquire()
  try:
    var entries: seq[tuple[key, value: string]] = @[]
    var txnWrites: stdtables.Table[string, string]
    var txnDeleted: stdtables.Table[string, bool]

    if txnId != zeroTransactionID() and txnId in m.txnState:
      txnWrites = m.txnState[txnId].writes
      txnDeleted = m.txnState[txnId].deleted

    for k, v in m.data:
      if k >= startKey and k < endKey:
        if k notin txnDeleted:
          entries.add((key: k, value: v))

    for k, v in txnWrites:
      if k >= startKey and k < endKey and k notin txnDeleted:
        var found = false
        for i, e in entries:
          if e.key == k:
            entries[i] = (key: k, value: v)
            found = true
            break
        if not found:
          entries.add((key: k, value: v))

    proc cmpEntries(a, b: tuple[key, value: string]): int = cmp(a.key, b.key)
    entries.sort(cmpEntries)

    if limit > 0 and entries.len > int(limit):
      entries.setLen(int(limit))

    kvOpOk(entries)
  finally:
    m.lock.release()

proc mockCreateSpace(m: MockFractioClient, name: string,
    replicas: int32): SpaceOpResult =
  m.lock.acquire()
  try:
    let spaceId = SpaceID(genULID())
    let spaceRec = SpaceRecord(
      spaceId: spaceId,
      name: name,
      replicas: replicas,
      groupCount: 1,
      groupIds: @[groupIDFromULID(genULID())]
    )
    let encoded = encode(spaceRec)
    let key = encodeTableKey(SYS_SPACES_TABLE_ID, name)
    m.data[key] = encoded
    m.spaceRecords.add(encoded)
    SpaceOpResult(isOk: true, spaceId: spaceId, groupCount: 1,
        groupIds: spaceRec.groupIds)
  finally:
    m.lock.release()

proc mockDropSpace(m: MockFractioClient, name: string): SpaceOpResult =
  m.lock.acquire()
  try:
    let key = encodeTableKey(SYS_SPACES_TABLE_ID, name)
    m.data.del(key)
    SpaceOpResult(isOk: true)
  finally:
    m.lock.release()

proc close(m: MockFractioClient) =
  m.lock.acquire()
  try:
    m.data.clear()
    m.txnState.clear()
  finally:
    m.lock.release()
  deinitLock(m.lock)

suite "Executor Result Constructors":

  test "okResult":
    let r = okResult("CREATE TABLE")
    check r.kind == erkOk
    check r.okMessage == "CREATE TABLE"

  test "errorResult":
    let r = errorResult("table not found")
    check r.kind == erkError
    check r.error == "table not found"

  test "modifiedResult with count":
    let r = modifiedResult(5)
    check r.kind == erkModified
    check r.count == 5
    check r.message == "5 row(s) affected"

  test "modifiedResult with custom message":
    let r = modifiedResult(3, "INSERT 3")
    check r.kind == erkModified
    check r.count == 3
    check r.message == "INSERT 3"

  test "modifiedResult zero":
    let r = modifiedResult(0)
    check r.count == 0
    check r.message == "0 row(s) affected"

  test "rowsResult empty":
    let r = rowsResult(@["id", "name"], @[])
    check r.kind == erkRows
    check r.columns == @["id", "name"]
    check r.rows.len == 0

  test "rowsResult with data":
    let r = rowsResult(@["id", "name"], @[@["1", "Alice"], @["2", "Bob"]])
    check r.kind == erkRows
    check r.columns.len == 2
    check r.rows.len == 2
    check r.rows[0] == @["1", "Alice"]
    check r.rows[1] == @["2", "Bob"]

suite "Executor ExecResultKind":

  test "all ExecResultKind values":
    check erkRows.ord >= 0
    check erkModified.ord >= 0
    check erkOk.ord >= 0
    check erkError.ord >= 0
    check erkUseDatabase.ord >= 0
    check erkUseSchema.ord >= 0

suite "Executor ExecResult Variants":

  test "erkUseDatabase":
    let r = ExecResult(kind: erkUseDatabase, newDatabase: "mydb")
    check r.kind == erkUseDatabase
    check r.newDatabase == "mydb"

  test "erkUseSchema":
    let r = ExecResult(kind: erkUseSchema, newSchema: "reporting")
    check r.kind == erkUseSchema
    check r.newSchema == "reporting"

suite "Executor evalExprDataRow Literals":

  test "integer literal":
    let e = Expr(kind: exLiteral, litValue: newValueRef(42'i64))
    let row = newDataRow()
    let v = evalExprDataRow(e, row)
    check v.kind == drvkInt
    check v.intVal == 42

  test "float literal":
    let e = Expr(kind: exLiteral, litValue: newValueRef(3.14))
    let row = newDataRow()
    let v = evalExprDataRow(e, row)
    check v.kind == drvkFloat
    check v.floatVal == 3.14

  test "string literal":
    let e = Expr(kind: exLiteral, litValue: newValueRef("hello"))
    let row = newDataRow()
    let v = evalExprDataRow(e, row)
    check v.kind == drvkString
    check v.strVal == "hello"

  test "bool literal":
    let e = Expr(kind: exLiteral, litValue: newValueRef(true))
    let row = newDataRow()
    let v = evalExprDataRow(e, row)
    check v.kind == drvkBool
    check v.boolVal == true

  test "null literal":
    let e = Expr(kind: exLiteral, litValue: nil)
    let row = newDataRow()
    let v = evalExprDataRow(e, row)
    check v.kind == drvkNull

suite "Executor evalExprDataRow Column":

  test "column reference":
    let e = Expr(kind: exColumn, colName: "id")
    let row = newDataRow(@[newColumn("id", newRowValue(123'i64))])
    let v = evalExprDataRow(e, row)
    check v.kind == drvkInt
    check v.intVal == 123

  test "column reference missing":
    let e = Expr(kind: exColumn, colName: "missing")
    let row = newDataRow()
    let v = evalExprDataRow(e, row)
    check v.kind == drvkNull

  test "column reference string":
    let e = Expr(kind: exColumn, colName: "name")
    let row = newDataRow(@[newColumn("name", newRowValue("Alice"))])
    let v = evalExprDataRow(e, row)
    check v.kind == drvkString
    check v.strVal == "Alice"

suite "Executor evalExprDataRow Binary Operators":

  test "equality true":
    let e = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(42'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(42'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.kind == drvkBool
    check v.boolVal == true

  test "equality false":
    let e = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(42'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(43'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "inequality":
    let e = Expr(kind: exBinOp, binOp: boNeq,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(42'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(43'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "less than":
    let e = Expr(kind: exBinOp, binOp: boLt,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(10'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(20'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "less than or equal":
    let e = Expr(kind: exBinOp, binOp: boLte,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(20'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(20'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "greater than":
    let e = Expr(kind: exBinOp, binOp: boGt,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(20'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(10'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "greater than or equal":
    let e = Expr(kind: exBinOp, binOp: boGte,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(20'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(20'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "AND true true":
    let e = Expr(kind: exBinOp, binOp: boAnd,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(true)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(true)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "AND true false":
    let e = Expr(kind: exBinOp, binOp: boAnd,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(true)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(false)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "OR false false":
    let e = Expr(kind: exBinOp, binOp: boOr,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(false)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(false)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "OR true false":
    let e = Expr(kind: exBinOp, binOp: boOr,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(true)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(false)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "addition":
    let e = Expr(kind: exBinOp, binOp: boAdd,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(3'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.kind == drvkInt
    check v.intVal == 8

  test "subtraction":
    let e = Expr(kind: exBinOp, binOp: boSub,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(10'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(3'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.intVal == 7

  test "multiplication":
    let e = Expr(kind: exBinOp, binOp: boMul,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(6'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(7'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.intVal == 42

  test "division":
    let e = Expr(kind: exBinOp, binOp: boDiv,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(20'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(4'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.intVal == 5

  test "modulo":
    let e = Expr(kind: exBinOp, binOp: boMod,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(17'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(5'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.intVal == 2

  test "division by zero returns null":
    let e = Expr(kind: exBinOp, binOp: boDiv,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(10'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(0'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.kind == drvkNull

suite "Executor evalExprDataRow Unary Operators":

  test "NOT true":
    let e = Expr(kind: exUnaryOp, unaryOp: uoNot,
      unaryExpr: Expr(kind: exLiteral, litValue: newValueRef(true)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "NOT false":
    let e = Expr(kind: exUnaryOp, unaryOp: uoNot,
      unaryExpr: Expr(kind: exLiteral, litValue: newValueRef(false)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "negation":
    let e = Expr(kind: exUnaryOp, unaryOp: uoNeg,
      unaryExpr: Expr(kind: exLiteral, litValue: newValueRef(42'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.intVal == -42

suite "Executor evalExprDataRow IS NULL":

  test "IS NULL true":
    let e = Expr(kind: exIsNull,
      isNullExpr: Expr(kind: exLiteral, litValue: nil),
      isNullNot: false)
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "IS NULL false for non-null":
    let e = Expr(kind: exIsNull,
      isNullExpr: Expr(kind: exLiteral, litValue: newValueRef(42'i64)),
      isNullNot: false)
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "IS NOT NULL true for non-null":
    let e = Expr(kind: exIsNull,
      isNullExpr: Expr(kind: exLiteral, litValue: newValueRef(42'i64)),
      isNullNot: true)
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "IS NOT NULL false for null":
    let e = Expr(kind: exIsNull,
      isNullExpr: Expr(kind: exLiteral, litValue: nil),
      isNullNot: true)
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

suite "Executor evalExprDataRow IN":

  test "IN list match":
    let e = Expr(kind: exIn,
      inExpr: Expr(kind: exLiteral, litValue: newValueRef(2'i64)),
      inNot: false,
      inList: @[
        Expr(kind: exLiteral, litValue: newValueRef(1'i64)),
        Expr(kind: exLiteral, litValue: newValueRef(2'i64)),
        Expr(kind: exLiteral, litValue: newValueRef(3'i64))
      ])
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "IN list no match":
    let e = Expr(kind: exIn,
      inExpr: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      inNot: false,
      inList: @[
        Expr(kind: exLiteral, litValue: newValueRef(1'i64)),
        Expr(kind: exLiteral, litValue: newValueRef(2'i64))
      ])
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "NOT IN":
    let e = Expr(kind: exIn,
      inExpr: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      inNot: true,
      inList: @[
        Expr(kind: exLiteral, litValue: newValueRef(1'i64)),
        Expr(kind: exLiteral, litValue: newValueRef(2'i64))
      ])
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

suite "Executor evalExprDataRow BETWEEN":

  test "BETWEEN in range":
    let e = Expr(kind: exBetween,
      betweenExpr: Expr(kind: exLiteral, litValue: newValueRef(30'i64)),
      betweenNot: false,
      betweenLo: Expr(kind: exLiteral, litValue: newValueRef(18'i64)),
      betweenHi: Expr(kind: exLiteral, litValue: newValueRef(65'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "BETWEEN below range":
    let e = Expr(kind: exBetween,
      betweenExpr: Expr(kind: exLiteral, litValue: newValueRef(10'i64)),
      betweenNot: false,
      betweenLo: Expr(kind: exLiteral, litValue: newValueRef(18'i64)),
      betweenHi: Expr(kind: exLiteral, litValue: newValueRef(65'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "BETWEEN above range":
    let e = Expr(kind: exBetween,
      betweenExpr: Expr(kind: exLiteral, litValue: newValueRef(70'i64)),
      betweenNot: false,
      betweenLo: Expr(kind: exLiteral, litValue: newValueRef(18'i64)),
      betweenHi: Expr(kind: exLiteral, litValue: newValueRef(65'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "NOT BETWEEN":
    let e = Expr(kind: exBetween,
      betweenExpr: Expr(kind: exLiteral, litValue: newValueRef(10'i64)),
      betweenNot: true,
      betweenLo: Expr(kind: exLiteral, litValue: newValueRef(18'i64)),
      betweenHi: Expr(kind: exLiteral, litValue: newValueRef(65'i64)))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

suite "Executor evalExprDataRow LIKE":

  test "LIKE exact match":
    let e = Expr(kind: exLike,
      likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello")),
      likeNot: false,
      likePattern: Expr(kind: exLiteral, litValue: newValueRef("hello")))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "LIKE prefix match":
    let e = Expr(kind: exLike,
      likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello world")),
      likeNot: false,
      likePattern: Expr(kind: exLiteral, litValue: newValueRef("hello%")))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "LIKE suffix match":
    let e = Expr(kind: exLike,
      likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello world")),
      likeNot: false,
      likePattern: Expr(kind: exLiteral, litValue: newValueRef("%world")))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "LIKE contains match":
    let e = Expr(kind: exLike,
      likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello world")),
      likeNot: false,
      likePattern: Expr(kind: exLiteral, litValue: newValueRef("%lo wo%")))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

  test "LIKE no match":
    let e = Expr(kind: exLike,
      likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello")),
      likeNot: false,
      likePattern: Expr(kind: exLiteral, litValue: newValueRef("world%")))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == false

  test "NOT LIKE":
    let e = Expr(kind: exLike,
      likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello")),
      likeNot: true,
      likePattern: Expr(kind: exLiteral, litValue: newValueRef("world%")))
    let v = evalExprDataRow(e, newDataRow())
    check v.boolVal == true

suite "Executor matchesFilterDataRow":

  test "empty filter matches all":
    let row = newDataRow()
    check matchesFilterDataRow(none(Expr), row) == true

  test "filter matches":
    let filter = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exColumn, colName: "id"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(1'i64)))
    let row = newDataRow(@[newColumn("id", newRowValue(1'i64))])
    check matchesFilterDataRow(some(filter), row) == true

  test "filter does not match":
    let filter = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exColumn, colName: "id"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(1'i64)))
    let row = newDataRow(@[newColumn("id", newRowValue(2'i64))])
    check matchesFilterDataRow(some(filter), row) == false

  test "complex filter AND":
    let filter = Expr(kind: exBinOp, binOp: boAnd,
      binLeft: Expr(kind: exBinOp, binOp: boGt,
        binLeft: Expr(kind: exColumn, colName: "age"),
        binRight: Expr(kind: exLiteral, litValue: newValueRef(18'i64))),
      binRight: Expr(kind: exBinOp, binOp: boLt,
        binLeft: Expr(kind: exColumn, colName: "age"),
        binRight: Expr(kind: exLiteral, litValue: newValueRef(65'i64))))
    let row1 = newDataRow(@[newColumn("age", newRowValue(30'i64))])
    check matchesFilterDataRow(some(filter), row1) == true
    let row2 = newDataRow(@[newColumn("age", newRowValue(10'i64))])
    check matchesFilterDataRow(some(filter), row2) == false

suite "Executor evalExpr (JSON legacy)":

  test "integer literal to JSON":
    let e = Expr(kind: exLiteral, litValue: newValueRef(42'i64))
    let j = evalExpr(e, newJObject())
    check j.kind == JInt
    check j.getInt == 42

  test "string literal to JSON":
    let e = Expr(kind: exLiteral, litValue: newValueRef("hello"))
    let j = evalExpr(e, newJObject())
    check j.kind == JString
    check j.getStr == "hello"

  test "bool literal to JSON":
    let e = Expr(kind: exLiteral, litValue: newValueRef(true))
    let j = evalExpr(e, newJObject())
    check j.kind == JBool
    check j.getBool == true

  test "null literal to JSON":
    let e = Expr(kind: exLiteral, litValue: nil)
    let j = evalExpr(e, newJObject())
    check j.kind == JNull

  test "column from JSON":
    let e = Expr(kind: exColumn, colName: "name")
    let row = %*{"name": "Alice", "age": 30}
    let j = evalExpr(e, row)
    check j.kind == JString
    check j.getStr == "Alice"

  test "equality JSON":
    let e = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(42'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(42'i64)))
    let j = evalExpr(e, newJObject())
    check j.kind == JBool
    check j.getBool == true

  test "less than JSON":
    let e = Expr(kind: exBinOp, binOp: boLt,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(10'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(20'i64)))
    let j = evalExpr(e, newJObject())
    check j.getBool == true

suite "Executor matchesFilter (JSON legacy)":

  test "empty filter":
    let row = %*{"id": 1}
    check matchesFilter(none(Expr), row) == true

  test "filter matches JSON":
    let filter = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exColumn, colName: "status"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef("active")))
    let row = %*{"status": "active"}
    check matchesFilter(some(filter), row) == true

  test "filter does not match JSON":
    let filter = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exColumn, colName: "status"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef("active")))
    let row = %*{"status": "pending"}
    check matchesFilter(some(filter), row) == false

suite "Executor evalExpr (JSON legacy) - Literals":

  test "evalExpr integer literal JSON":
    let e = Expr(kind: exLiteral, litValue: newValueRef(42'i64))
    let j = evalExpr(e, %*{"id": 1})
    check j.kind == JInt
    check j.getInt == 42

  test "evalExpr float literal JSON":
    let e = Expr(kind: exLiteral, litValue: newValueRef(3.14))
    let j = evalExpr(e, %*{"id": 1})
    check j.kind == JFloat
    check j.getFloat == 3.14

  test "evalExpr string literal JSON":
    let e = Expr(kind: exLiteral, litValue: newValueRef("hello"))
    let j = evalExpr(e, %*{"id": 1})
    check j.kind == JString
    check j.getStr == "hello"

  test "evalExpr bool literal JSON":
    let e = Expr(kind: exLiteral, litValue: newValueRef(true))
    let j = evalExpr(e, %*{"id": 1})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr null literal JSON":
    let e = Expr(kind: exLiteral, litValue: nil)
    let j = evalExpr(e, %*{"id": 1})
    check j.kind == JNull

suite "Executor evalExpr (JSON legacy) - Column":

  test "evalExpr column existing JSON":
    let e = Expr(kind: exColumn, colName: "name")
    let row = %*{"name": "Alice", "id": 1}
    let j = evalExpr(e, row)
    check j.kind == JString
    check j.getStr == "Alice"

  test "evalExpr column missing JSON":
    let e = Expr(kind: exColumn, colName: "missing")
    let row = %*{"id": 1}
    let j = evalExpr(e, row)
    check j.kind == JNull

suite "Executor evalExpr (JSON legacy) - Binary Operators":

  test "evalExpr boEq integers JSON":
    let e = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(5'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr boNeq integers JSON":
    let e = Expr(kind: exBinOp, binOp: boNeq,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(3'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr boLt integers JSON":
    let e = Expr(kind: exBinOp, binOp: boLt,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(3'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(5'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr boLte integers JSON":
    let e = Expr(kind: exBinOp, binOp: boLte,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(5'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr boGt integers JSON":
    let e = Expr(kind: exBinOp, binOp: boGt,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(3'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr boGte integers JSON":
    let e = Expr(kind: exBinOp, binOp: boGte,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(5'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr boAnd JSON":
    let e = Expr(kind: exBinOp, binOp: boAnd,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(true)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(false)))
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == false

  test "evalExpr boOr JSON":
    let e = Expr(kind: exBinOp, binOp: boOr,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(true)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(false)))
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr boAdd JSON":
    let e = Expr(kind: exBinOp, binOp: boAdd,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(3'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JInt
    check j.getInt == 8

  test "evalExpr boSub JSON":
    let e = Expr(kind: exBinOp, binOp: boSub,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(3'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JInt
    check j.getInt == 2

  test "evalExpr boMul JSON":
    let e = Expr(kind: exBinOp, binOp: boMul,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(3'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JInt
    check j.getInt == 15

  test "evalExpr boDiv JSON":
    let e = Expr(kind: exBinOp, binOp: boDiv,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(15'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(3'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JInt
    check j.getInt == 5

  test "evalExpr boDiv by zero JSON":
    let e = Expr(kind: exBinOp, binOp: boDiv,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(0'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JNull

  test "evalExpr boMod JSON":
    let e = Expr(kind: exBinOp, binOp: boMod,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(7'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(3'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JInt
    check j.getInt == 1

  test "evalExpr boMod by zero JSON":
    let e = Expr(kind: exBinOp, binOp: boMod,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(0'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JNull

  test "evalExpr boLt strings JSON":
    let e = Expr(kind: exBinOp, binOp: boLt,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef("abc")),
      binRight: Expr(kind: exLiteral, litValue: newValueRef("def")))
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr boLte strings JSON returns false (not supported)":
    # JSON evalExpr only supports integer comparison for boLte, not strings
    let e = Expr(kind: exBinOp, binOp: boLte,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef("abc")),
      binRight: Expr(kind: exLiteral, litValue: newValueRef("abc")))
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == false

  test "evalExpr boGt strings JSON returns false (not supported)":
    # JSON evalExpr only supports integer comparison for boGt, not strings
    let e = Expr(kind: exBinOp, binOp: boGt,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef("xyz")),
      binRight: Expr(kind: exLiteral, litValue: newValueRef("abc")))
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == false

  test "evalExpr boGte strings JSON returns false (not supported)":
    # JSON evalExpr only supports integer comparison for boGte, not strings
    let e = Expr(kind: exBinOp, binOp: boGte,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef("abc")),
      binRight: Expr(kind: exLiteral, litValue: newValueRef("abc")))
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == false

  test "evalExpr boLte integers less JSON":
    let e = Expr(kind: exBinOp, binOp: boLte,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(3'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(5'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr boGt integers less JSON":
    let e = Expr(kind: exBinOp, binOp: boGt,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(3'i64)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(5'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == false

  test "evalExpr boLt mixed types JSON returns false":
    let e = Expr(kind: exBinOp, binOp: boLt,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef("abc")),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(5'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == false

suite "Executor evalExpr (JSON legacy) - Unary Operators":

  test "evalExpr uoNot JSON":
    let e = Expr(kind: exUnaryOp, unaryOp: uoNot,
      unaryExpr: Expr(kind: exLiteral, litValue: newValueRef(true)))
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == false

  test "evalExpr uoNeg JSON":
    let e = Expr(kind: exUnaryOp, unaryOp: uoNeg,
      unaryExpr: Expr(kind: exLiteral, litValue: newValueRef(5'i64)))
    let j = evalExpr(e, %*{})
    check j.kind == JInt
    check j.getInt == -5

  test "evalExpr uoNeg non-int JSON returns null":
    let e = Expr(kind: exUnaryOp, unaryOp: uoNeg,
      unaryExpr: Expr(kind: exLiteral, litValue: newValueRef("abc")))
    let j = evalExpr(e, %*{})
    check j.kind == JNull

suite "Executor evalExpr (JSON legacy) - IS NULL":

  test "evalExpr IS NULL JSON":
    let e = Expr(kind: exIsNull, isNullExpr: Expr(kind: exColumn, colName: "missing"),
                 isNullNot: false)
    let j = evalExpr(e, %*{"id": 1})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr IS NOT NULL JSON":
    let e = Expr(kind: exIsNull, isNullExpr: Expr(kind: exColumn, colName: "id"),
                 isNullNot: true)
    let j = evalExpr(e, %*{"id": 1})
    check j.kind == JBool
    check j.getBool == true

suite "Executor evalExpr (JSON legacy) - IN":

  test "evalExpr IN JSON":
    let e = Expr(kind: exIn, inExpr: Expr(kind: exLiteral, litValue: newValueRef(2'i64)),
                 inNot: false,
                 inList: @[Expr(kind: exLiteral, litValue: newValueRef(1'i64)),
                          Expr(kind: exLiteral, litValue: newValueRef(2'i64)),
                          Expr(kind: exLiteral, litValue: newValueRef(3'i64))])
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr NOT IN JSON":
    let e = Expr(kind: exIn, inExpr: Expr(kind: exLiteral, litValue: newValueRef(4'i64)),
                 inNot: true,
                 inList: @[Expr(kind: exLiteral, litValue: newValueRef(1'i64)),
                          Expr(kind: exLiteral, litValue: newValueRef(2'i64)),
                          Expr(kind: exLiteral, litValue: newValueRef(3'i64))])
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

suite "Executor evalExpr (JSON legacy) - BETWEEN":

  test "evalExpr BETWEEN JSON":
    let e = Expr(kind: exBetween, betweenExpr: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
                 betweenLo: Expr(kind: exLiteral, litValue: newValueRef(1'i64)),
                 betweenHi: Expr(kind: exLiteral, litValue: newValueRef(
                     10'i64)),
                 betweenNot: false)
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr NOT BETWEEN JSON":
    let e = Expr(kind: exBetween, betweenExpr: Expr(kind: exLiteral, litValue: newValueRef(15'i64)),
                 betweenLo: Expr(kind: exLiteral, litValue: newValueRef(1'i64)),
                 betweenHi: Expr(kind: exLiteral, litValue: newValueRef(
                     10'i64)),
                 betweenNot: true)
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

suite "Executor evalExpr (JSON legacy) - LIKE":

  test "evalExpr LIKE prefix wildcard JSON":
    let e = Expr(kind: exLike, likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello world")),
                 likePattern: Expr(kind: exLiteral, litValue: newValueRef(
                     "%world")),
                 likeNot: false)
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr LIKE suffix wildcard JSON":
    let e = Expr(kind: exLike, likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello world")),
                 likePattern: Expr(kind: exLiteral, litValue: newValueRef(
                     "hello%")),
                 likeNot: false)
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr LIKE both wildcards JSON":
    let e = Expr(kind: exLike, likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello world")),
                 likePattern: Expr(kind: exLiteral, litValue: newValueRef(
                     "%lo%")),
                 likeNot: false)
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr LIKE exact match JSON":
    let e = Expr(kind: exLike, likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello")),
                 likePattern: Expr(kind: exLiteral, litValue: newValueRef(
                     "hello")),
                 likeNot: false)
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr NOT LIKE JSON":
    let e = Expr(kind: exLike, likeExpr: Expr(kind: exLiteral, litValue: newValueRef("hello world")),
                 likePattern: Expr(kind: exLiteral, litValue: newValueRef(
                     "%bye%")),
                 likeNot: true)
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == true

  test "evalExpr LIKE non-string JSON returns false":
    let e = Expr(kind: exLike, likeExpr: Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
                 likePattern: Expr(kind: exLiteral, litValue: newValueRef(
                     "%5%")),
                 likeNot: false)
    let j = evalExpr(e, %*{})
    check j.kind == JBool
    check j.getBool == false

suite "Executor evalExpr (JSON legacy) - Other":

  test "evalExpr exStar JSON returns null":
    let e = Expr(kind: exStar)
    let j = evalExpr(e, %*{})
    check j.kind == JNull

  test "evalExpr exParam JSON returns null":
    let e = Expr(kind: exParam, paramIdx: 1)
    let j = evalExpr(e, %*{})
    check j.kind == JNull

  test "evalExpr exList JSON returns null":
    let e = Expr(kind: exList, listItems: @[])
    let j = evalExpr(e, %*{})
    check j.kind == JNull

suite "Executor evalExprDataRow Other ExprKinds":

  test "exStar returns null":
    let e = Expr(kind: exStar)
    let v = evalExprDataRow(e, newDataRow())
    check v.kind == drvkNull

  test "exParam returns null":
    let e = Expr(kind: exParam, paramIdx: 1)
    let v = evalExprDataRow(e, newDataRow())
    check v.kind == drvkNull

  test "exList returns null":
    let e = Expr(kind: exList, listItems: @[])
    let v = evalExprDataRow(e, newDataRow())
    check v.kind == drvkNull

suite "Executor KVEntry":

  test "KVEntry construction":
    let kv = KVEntry(key: "/t/123/key", value: "data")
    check kv.key == "/t/123/key"
    check kv.value == "data"

suite "Executor execute with nil client":

  test "execute returns error with nil client":
    let plan = newPlan()
    plan.add(PlanOp(kind: poShowDatabases))
    let result = execute(plan, nil, "default")
    check result.kind == erkError
    check result.error == "FractioClient is required for all operations"

  test "execute returns error for empty plan":
    let plan = newPlan()
    let result = execute(plan, nil, "default")
    check result.kind == erkError
    check result.error == "FractioClient is required for all operations"

suite "ExecutorContext Tests":

  test "ExecutorContext type fields":
    # This test verifies the ExecutorContext type exists and has expected fields
    # Note: We cannot create a real context with nil client since it would crash
    # But we can verify the type definition by creating a ref object manually
    let ctx = ExecutorContext(
      database: "mydb",
      schema: "myschema",
      hasActiveTransaction: false,
      txnId: zeroTransactionID(),
      readTimestamp: 0
    )
    check ctx.database == "mydb"
    check ctx.schema == "myschema"
    check ctx.hasActiveTransaction == false
    check ctx.txnId == zeroTransactionID()
    check ctx.readTimestamp == 0

suite "MockFractioClient Transaction Operations":

  test "mockBeginTxn creates transaction":
    let mock = newMockFractioClient()
    let res = mock.mockBeginTxn()
    check res.isOk
    check res.val.txnId != zeroTransactionID()
    check res.val.readTimestamp >= 1
    mock.close()

  test "mockCommitTxn applies writes":
    let mock = newMockFractioClient()
    let txnRes = mock.mockBeginTxn()
    let txnId = txnRes.val.txnId

    # Write within transaction
    let putRes = mock.mockKvPut("test_key", "test_value", txnId = txnId)
    check putRes.isOk

    # Commit
    let commitRes = mock.mockCommitTxn(txnId)
    check commitRes.isOk

    # Verify write is now in committed data
    let getRes = mock.mockKvGet("test_key")
    check getRes.isOk
    check getRes.val.isSome
    check getRes.val.get() == "test_value"
    mock.close()

  test "mockRollbackTxn discards writes":
    let mock = newMockFractioClient()
    let txnRes = mock.mockBeginTxn()
    let txnId = txnRes.val.txnId

    # Write within transaction
    discard mock.mockKvPut("test_key", "test_value", txnId = txnId)

    # Rollback
    let rollbackRes = mock.mockRollbackTxn(txnId)
    check rollbackRes.isOk

    # Verify write was not applied
    let getRes = mock.mockKvGet("test_key")
    check getRes.isOk
    check getRes.val.isNone
    mock.close()

suite "MockFractioClient KV Operations":

  test "mockKvPut and mockKvGet":
    let mock = newMockFractioClient()

    # Put without transaction (auto-commit)
    let putRes = mock.mockKvPut("key1", "value1")
    check putRes.isOk

    # Get the value
    let getRes = mock.mockKvGet("key1")
    check getRes.isOk
    check getRes.val.isSome
    check getRes.val.get() == "value1"

    # Get non-existent key
    let getRes2 = mock.mockKvGet("key2")
    check getRes2.isOk
    check getRes2.val.isNone
    mock.close()

  test "mockKvDelete":
    let mock = newMockFractioClient()

    # Put a value
    discard mock.mockKvPut("key1", "value1")

    # Delete it
    let delRes = mock.mockKvDelete("key1")
    check delRes.isOk

    # Verify deleted
    let getRes = mock.mockKvGet("key1")
    check getRes.isOk
    check getRes.val.isNone
    mock.close()

  test "mockKvScan basic":
    let mock = newMockFractioClient()

    # Put multiple values
    discard mock.mockKvPut("/t/001/a", "val_a")
    discard mock.mockKvPut("/t/001/b", "val_b")
    discard mock.mockKvPut("/t/001/c", "val_c")
    discard mock.mockKvPut("/t/002/x", "val_x") # different prefix
    
    # Scan /t/001 range
    let scanRes = mock.mockKvScan("/t/001/", "/t/001/\xFF")
    check scanRes.isOk
    check scanRes.val.len == 3

    # Verify sorted order
    check scanRes.val[0].key == "/t/001/a"
    check scanRes.val[1].key == "/t/001/b"
    check scanRes.val[2].key == "/t/001/c"
    mock.close()

  test "mockKvScan with limit":
    let mock = newMockFractioClient()

    discard mock.mockKvPut("/t/001/a", "val_a")
    discard mock.mockKvPut("/t/001/b", "val_b")
    discard mock.mockKvPut("/t/001/c", "val_c")

    let scanRes = mock.mockKvScan("/t/001/", "/t/001/\xFF", limit = 2)
    check scanRes.isOk
    check scanRes.val.len == 2
    mock.close()

  test "mockKvGet within transaction sees pending writes":
    let mock = newMockFractioClient()
    let txnRes = mock.mockBeginTxn()
    let txnId = txnRes.val.txnId
    let readTs = txnRes.val.readTimestamp

    # Write within transaction
    discard mock.mockKvPut("key1", "txn_value", txnId = txnId)

    # Get within transaction should see the pending write
    let getRes = mock.mockKvGet("key1", txnId = txnId, readTimestamp = readTs)
    check getRes.isOk
    check getRes.val.isSome
    check getRes.val.get() == "txn_value"

    # Get without transaction should NOT see pending write
    let getRes2 = mock.mockKvGet("key1")
    check getRes2.isOk
    check getRes2.val.isNone
    mock.close()

suite "MockFractioClient Space Operations":

  test "mockCreateSpace creates space record":
    let mock = newMockFractioClient()
    let res = mock.mockCreateSpace("test_space", replicas = 3)
    check res.isOk
    check res.spaceId != zeroSpaceID()
    check res.groupCount == 1
    check res.groupIds.len == 1
    mock.close()

  test "mockDropSpace removes space record":
    let mock = newMockFractioClient()

    # Create a space
    discard mock.mockCreateSpace("test_space", replicas = 0)

    # Verify it exists
    let key = encodeTableKey(SYS_SPACES_TABLE_ID, "test_space")
    let getRes = mock.mockKvGet(key)
    check getRes.isOk
    check getRes.val.isSome

    # Drop the space
    let dropRes = mock.mockDropSpace("test_space")
    check dropRes.isOk

    # Verify it's gone
    let getRes2 = mock.mockKvGet(key)
    check getRes2.isOk
    check getRes2.val.isNone
    mock.close()

suite "ExecutorPlanOp Formatting":

  test "formatPlanOp poShowDatabases":
    let op = PlanOp(kind: poShowDatabases)
    let text = formatPlanOp(op)
    check text == "ShowDatabases"

  test "formatPlanOp poShowSchemas":
    let op = PlanOp(kind: poShowSchemas, ssDatabase: "mydb")
    let text = formatPlanOp(op)
    check text == "ShowSchemas db=mydb"

  test "formatPlanOp poShowTables":
    let op = PlanOp(kind: poShowTables, stDatabase: "mydb", stSchema: "public")
    let text = formatPlanOp(op)
    check text == "ShowTables db=mydb schema=public"

  test "formatPlanOp poShowSpaces":
    let op = PlanOp(kind: poShowSpaces)
    let text = formatPlanOp(op)
    check text == "ShowSpaces"

  test "formatPlanOp poBeginTxn":
    let op = PlanOp(kind: poBeginTxn, btReadOnly: false)
    let text = formatPlanOp(op)
    check text == "BeginTxn readOnly=false"

  test "formatPlanOp poCommitTxn":
    let op = PlanOp(kind: poCommitTxn)
    let text = formatPlanOp(op)
    check text == "CommitTxn"

  test "formatPlanOp poRollbackTxn":
    let op = PlanOp(kind: poRollbackTxn)
    let text = formatPlanOp(op)
    check text == "RollbackTxn"

  test "formatPlanOp poUseDatabase":
    let op = PlanOp(kind: poUseDatabase, udName: "mydb")
    let text = formatPlanOp(op)
    check text == "UseDatabase name=mydb"

  test "formatPlanOp poUseSchema":
    let op = PlanOp(kind: poUseSchema, usName: "myschema")
    let text = formatPlanOp(op)
    check text == "UseSchema name=myschema"

  test "formatPlanOp poCreateDatabase":
    let op = PlanOp(kind: poCreateDatabase, cdbName: "mydb")
    let text = formatPlanOp(op)
    check text == "CreateDatabase name=mydb"

  test "formatPlanOp poDropDatabase":
    let op = PlanOp(kind: poDropDatabase, ddbName: "mydb")
    let text = formatPlanOp(op)
    check text == "DropDatabase name=mydb"

  test "formatPlanOp poCreateSchema":
    let op = PlanOp(kind: poCreateSchema, csDatabase: "mydb",
        csName: "myschema")
    let text = formatPlanOp(op)
    check text == "CreateSchema name=mydb.myschema"

  test "formatPlanOp poDropSchema":
    let op = PlanOp(kind: poDropSchema, dsDatabase: "mydb", dsName: "myschema")
    let text = formatPlanOp(op)
    check text == "DropSchema name=mydb.myschema"

  test "formatPlanOp poCreateTable":
    let op = PlanOp(kind: poCreateTable, ctDatabase: "mydb",
        ctSchema: "myschema", ctName: "users")
    let text = formatPlanOp(op)
    check text == "CreateTable name=mydb.myschema.users"

  test "formatPlanOp poDropTable":
    let op = PlanOp(kind: poDropTable, dtDatabase: "mydb", dtSchema: "myschema",
        dtName: "users")
    let text = formatPlanOp(op)
    check text == "DropTable name=mydb.myschema.users"

  test "formatPlanOp poCreateSpace":
    let op = PlanOp(kind: poCreateSpace, cspName: "production", cspReplicas: 3)
    let text = formatPlanOp(op)
    check text == "CreateSpace name=production replicas=3"

  test "formatPlanOp poDropSpace":
    let op = PlanOp(kind: poDropSpace, dspName: "production")
    let text = formatPlanOp(op)
    check text == "DropSpace name=production"

  test "formatPlanOp poInsert":
    let op = PlanOp(kind: poInsert, insTableName: "users",
        insTableId: genTableId(), insRows: @["row1", "row2"])
    let text = formatPlanOp(op)
    check text.contains("Insert")
    check text.contains("users")

  test "formatPlanOp poPointGet":
    let op = PlanOp(kind: poPointGet, pgTableId: genTableId(), pgKey: "pk123",
        pgColumns: @["id", "name"])
    let text = formatPlanOp(op)
    check text.contains("PointGet")
    check text.contains("pk123")

  test "formatPlanOp poScan with filter":
    let filter = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exColumn, colName: "id"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(1'i64)))
    let op = PlanOp(kind: poScan, scTableId: genTableId(), scColumns: @["id"],
                    scFilter: some(filter), scLimit: 10)
    let text = formatPlanOp(op)
    check text.contains("Scan")
    check text.contains("filter=")
    check text.contains("limit=10")

  test "formatPlanOp poScan without filter":
    let op = PlanOp(kind: poScan, scTableId: genTableId(), scColumns: @["id"],
                    scFilter: none(Expr), scLimit: 0)
    let text = formatPlanOp(op)
    check text.contains("Scan")
    check "filter=" notin text
    check "limit=" notin text

  test "formatPlanOp poUpdate with filter":
    let filter = Expr(kind: exBinOp, binOp: boGt,
      binLeft: Expr(kind: exColumn, colName: "age"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(18'i64)))
    let op = PlanOp(kind: poUpdate, upTableName: "users", upTableId: genTableId(),
                    upFilter: some(filter), upSets: @[(col: "name",
                        val: Expr(kind: exLiteral, litValue: newValueRef("updated")))])
    let text = formatPlanOp(op)
    check text.contains("Update")
    check text.contains("filter=")

  test "formatPlanOp poDelete with filter":
    let filter = Expr(kind: exBinOp, binOp: boLt,
      binLeft: Expr(kind: exColumn, colName: "id"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(5'i64)))
    let op = PlanOp(kind: poDelete, delTableName: "users", delTableId: genTableId(),
                    delFilter: some(filter))
    let text = formatPlanOp(op)
    check text.contains("Delete")
    check text.contains("filter=")

  test "formatPlanOp poExplain":
    let innerPlan = newPlan()
    innerPlan.add(PlanOp(kind: poShowDatabases))
    let op = PlanOp(kind: poExplain, exInnerPlan: innerPlan)
    let text = formatPlanOp(op)
    check text.contains("Explain")

suite "Executor DataRow PK Extraction":

  test "getPkValueFromDataRow string":
    let row = newDataRow(@[
      newColumn("id", newRowValue("user123")),
      newColumn("name", newRowValue("Alice"))
    ])
    let pk = getPkValueFromDataRow(row, "id")
    check pk == "user123"

  test "getPkValueFromDataRow int":
    let row = newDataRow(@[
      newColumn("id", newRowValue(42'i64)),
      newColumn("name", newRowValue("Bob"))
    ])
    let pk = getPkValueFromDataRow(row, "id")
    check pk == "42"

  test "getPkValueFromDataRow missing column":
    let row = newDataRow(@[
      newColumn("name", newRowValue("Alice"))
    ])
    let pk = getPkValueFromDataRow(row, "id")
    check pk == ""

  test "getPkValueFromDataRow null":
    let row = newDataRow(@[
      newColumn("id", newRowValue())
    ])
    let pk = getPkValueFromDataRow(row, "id")
    check pk == "NULL"

  test "getPkValueFromDataRow float":
    let row = newDataRow(@[
      newColumn("price", newRowValue(19.99))
    ])
    let pk = getPkValueFromDataRow(row, "price")
    check pk.contains("19.99")

suite "Executor DataRow Column Extraction":

  test "extractColumnsFromDataRow basic":
    let row = newDataRow(@[
      newColumn("id", newRowValue(1'i64)),
      newColumn("name", newRowValue("test"))
    ])
    let cols = extractColumnsFromDataRow(row, @["id", "name"])
    check cols == @["1", "test"]

  test "extractColumnsFromDataRow missing column":
    let row = newDataRow(@[
      newColumn("id", newRowValue(1'i64))
    ])
    let cols = extractColumnsFromDataRow(row, @["id", "missing"])
    check cols == @["1", "NULL"]

suite "Executor JSON Helpers":

  test "jsonToStringValue string":
    let j = newJString("hello")
    check jsonToStringValue(j) == "hello"

  test "jsonToStringValue int":
    let j = newJInt(42)
    check jsonToStringValue(j) == "42"

  test "jsonToStringValue float":
    let j = newJFloat(3.14)
    check jsonToStringValue(j) == "3.14"

  test "jsonToStringValue bool true":
    let j = newJBool(true)
    check jsonToStringValue(j) == "true"

  test "jsonToStringValue bool false":
    let j = newJBool(false)
    check jsonToStringValue(j) == "false"

  test "jsonToStringValue null":
    let j = newJNull()
    check jsonToStringValue(j) == "NULL"

  test "extractColumns from JSON":
    let j = %*{"id": 1, "name": "Alice"}
    let cols = extractColumns(j, @["id", "name"])
    check cols == @["1", "Alice"]

  test "extractColumns missing key":
    let j = %*{"id": 1}
    let cols = extractColumns(j, @["id", "missing"])
    check cols == @["1", "NULL"]

  test "getPkValue string from JSON":
    let j = %*{"id": "user123", "name": "Alice"}
    check getPkValue(j, "id") == "user123"

  test "getPkValue int from JSON":
    let j = %*{"id": 42, "name": "Bob"}
    check getPkValue(j, "id") == "42"

  test "getPkValue missing from JSON":
    let j = %*{"name": "Alice"}
    check getPkValue(j, "id") == ""

suite "Executor evalExprDataRow Float Comparisons":

  test "float equality":
    let row = newDataRow()
    let expr = Expr(kind: exBinOp, binOp: boEq,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(3.14)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(3.14)))
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkBool
    check result.boolVal == true

  test "float inequality":
    let row = newDataRow()
    let expr = Expr(kind: exBinOp, binOp: boNeq,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(1.0)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(2.0)))
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkBool
    check result.boolVal == true

  test "float less than":
    let row = newDataRow()
    let expr = Expr(kind: exBinOp, binOp: boLt,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(1.5)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(2.5)))
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkBool
    check result.boolVal == true

  test "float greater than":
    let row = newDataRow()
    let expr = Expr(kind: exBinOp, binOp: boGt,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef(2.5)),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(1.5)))
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkBool
    check result.boolVal == true

suite "Executor evalExprDataRow String Comparisons":

  test "string less than":
    let row = newDataRow()
    let expr = Expr(kind: exBinOp, binOp: boLt,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef("apple")),
      binRight: Expr(kind: exLiteral, litValue: newValueRef("banana")))
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkBool
    check result.boolVal == true

  test "string greater than":
    let row = newDataRow()
    let expr = Expr(kind: exBinOp, binOp: boGt,
      binLeft: Expr(kind: exLiteral, litValue: newValueRef("zebra")),
      binRight: Expr(kind: exLiteral, litValue: newValueRef("apple")))
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkBool
    check result.boolVal == true

suite "Executor evalExprDataRow LIKE Edge Cases":

  test "LIKE with no wildcard":
    let row = newDataRow(@[
      newColumn("name", newRowValue("Alice"))
    ])
    let expr = Expr(kind: exLike, likeExpr: Expr(kind: exColumn, colName: "name"),
                   likePattern: Expr(kind: exLiteral, litValue: newValueRef(
                       "Alice")),
                   likeNot: false)
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkBool
    check result.boolVal == true

  test "LIKE no match":
    let row = newDataRow(@[
      newColumn("name", newRowValue("Bob"))
    ])
    let expr = Expr(kind: exLike, likeExpr: Expr(kind: exColumn, colName: "name"),
                   likePattern: Expr(kind: exLiteral, litValue: newValueRef(
                       "Alice")),
                   likeNot: false)
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkBool
    check result.boolVal == false

  test "LIKE with empty pattern":
    let row = newDataRow(@[
      newColumn("name", newRowValue(""))
    ])
    let expr = Expr(kind: exLike, likeExpr: Expr(kind: exColumn, colName: "name"),
                   likePattern: Expr(kind: exLiteral, litValue: newValueRef(
                       "")),
                   likeNot: false)
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkBool
    check result.boolVal == true

  test "LIKE with non-string column returns null":
    let row = newDataRow(@[
      newColumn("age", newRowValue(25'i64))
    ])
    let expr = Expr(kind: exLike, likeExpr: Expr(kind: exColumn, colName: "age"),
                   likePattern: Expr(kind: exLiteral, litValue: newValueRef(
                       "25")),
                   likeNot: false)
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkNull

suite "Executor evalExprDataRow IN Edge Cases":

  test "IN empty list":
    let row = newDataRow(@[
      newColumn("id", newRowValue(5'i64))
    ])
    let expr = Expr(kind: exIn, inExpr: Expr(kind: exColumn, colName: "id"),
                   inList: @[], inNot: false)
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkBool
    check result.boolVal == false

  test "IN with multiple matches":
    let row = newDataRow(@[
      newColumn("id", newRowValue(5'i64))
    ])
    let expr = Expr(kind: exIn, inExpr: Expr(kind: exColumn, colName: "id"),
                   inList: @[Expr(kind: exLiteral, litValue: newValueRef(3'i64)),
                            Expr(kind: exLiteral, litValue: newValueRef(5'i64)),
                            Expr(kind: exLiteral, litValue: newValueRef(
                                7'i64))],
                   inNot: false)
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkBool
    check result.boolVal == true

  test "NOT IN empty list":
    let row = newDataRow(@[
      newColumn("id", newRowValue(5'i64))
    ])
    let expr = Expr(kind: exIn, inExpr: Expr(kind: exColumn, colName: "id"),
                   inList: @[], inNot: true)
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkBool
    check result.boolVal == true # NOT IN empty means everything matches

suite "Executor evalExprDataRow BETWEEN Edge Cases":

  test "BETWEEN equal bounds":
    let row = newDataRow(@[
      newColumn("age", newRowValue(25'i64))
    ])
    let expr = Expr(kind: exBetween, betweenExpr: Expr(kind: exColumn, colName: "age"),
                   betweenLo: Expr(kind: exLiteral, litValue: newValueRef(
                       25'i64)),
                   betweenHi: Expr(kind: exLiteral, litValue: newValueRef(
                       25'i64)),
                   betweenNot: false)
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkBool
    check result.boolVal == true

  test "BETWEEN outside range":
    let row = newDataRow(@[
      newColumn("age", newRowValue(100'i64))
    ])
    let expr = Expr(kind: exBetween, betweenExpr: Expr(kind: exColumn, colName: "age"),
                   betweenLo: Expr(kind: exLiteral, litValue: newValueRef(
                       20'i64)),
                   betweenHi: Expr(kind: exLiteral, litValue: newValueRef(
                       30'i64)),
                   betweenNot: false)
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkBool
    check result.boolVal == false

suite "Executor evalExprDataRow Star and Param":

  test "evalExprDataRow star returns null":
    let row = newDataRow()
    let expr = Expr(kind: exStar)
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkNull

  test "evalExprDataRow param returns null":
    let row = newDataRow()
    let expr = Expr(kind: exParam, paramIdx: 1)
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkNull

  test "evalExprDataRow list returns null":
    let row = newDataRow()
    let expr = Expr(kind: exList, listItems: @[])
    let result = evalExprDataRow(expr, row)
    check result.kind == drvkNull

suite "formatPlan Full Plan":

  test "formatPlan empty":
    let plan = newPlan()
    let text = formatPlan(plan)
    check text == ""

  test "formatPlan with multiple ops":
    let plan = newPlan()
    plan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    plan.add(PlanOp(kind: poShowDatabases))
    plan.add(PlanOp(kind: poCommitTxn))
    let text = formatPlan(plan)
    check text.contains("BeginTxn")
    check text.contains("ShowDatabases")
    check text.contains("CommitTxn")

# =============================================================================
# Executor tests with MockKVStore
# =============================================================================

import fractio/core/mock_kv

suite "ExecutorContext with MockKVStore":

  test "newExecutorContextWithKV creates context":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)
    check ctx.kv != nil
    check ctx.database == "default"
    check ctx.schema == "public"
    check ctx.hasActiveTransaction == false

  test "newExecutorContextWithKV with custom settings":
    let mockKV = newMockKVStore()
    let txnId = genTransactionID()
    let ctx = newExecutorContextWithKV(mockKV, nil, "mydb", "myschema",
                                        txnId, readTimestamp = 100)
    check ctx.database == "mydb"
    check ctx.schema == "myschema"
    check ctx.txnId == txnId
    check ctx.readTimestamp == 100
    check ctx.hasActiveTransaction == true

  test "newExecutorContextWithKV with zero txnId":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV, nil, "default", "public",
                                        zeroTransactionID(), 0)
    check ctx.hasActiveTransaction == false

suite "Executor Transaction Operations with MockKVStore":

  test "executeWithTxn BEGIN creates transaction":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    let plan = newPlan()
    plan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkOk
    check result.okMessage == "BEGIN"
    check ctx.hasActiveTransaction == true
    check ctx.txnId != zeroTransactionID()

  test "executeWithTxn COMMIT commits transaction":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin transaction
    let beginPlan = newPlan()
    beginPlan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan, ctx)

    # Commit
    let plan = newPlan()
    plan.add(PlanOp(kind: poCommitTxn))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkOk
    check result.okMessage == "COMMIT"
    check ctx.hasActiveTransaction == false
    check ctx.txnId == zeroTransactionID()

  test "executeWithTxn ROLLBACK discards transaction":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin transaction
    let beginPlan = newPlan()
    beginPlan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan, ctx)

    # Write some data (will be discarded)
    let tableId = genTableId()
    let writePlan = newPlan()
    writePlan.add(PlanOp(kind: poInsert, insTableId: tableId,
                         insRows: @["test_row"], insPkValues: @["pk1"]))
    discard executeWithTxn(writePlan, ctx)

    # Rollback
    let rollbackPlan = newPlan()
    rollbackPlan.add(PlanOp(kind: poRollbackTxn))

    let result = executeWithTxn(rollbackPlan, ctx)
    check result.kind == erkOk
    check result.okMessage == "ROLLBACK"
    check ctx.hasActiveTransaction == false

suite "Executor DDL Operations with MockKVStore":

  test "executeWithTxn CREATE DATABASE":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    let dbRec = DatabaseRecord(name: "testdb", createdAtNs: nowNs())
    let plan = newPlan()
    plan.add(PlanOp(kind: poCreateDatabase, cdbName: "testdb",
                    cdbValue: encode(dbRec), cdbIfNotExists: false))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkOk
    check "CREATE DATABASE" in result.okMessage

    # Verify database was created
    let key = encodeTableKey(SYS_DATABASES_TABLE_ID, "testdb")
    check mockKV.hasKey(key)

  test "executeWithTxn CREATE DATABASE IF NOT EXISTS":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Create database first
    let dbRec = DatabaseRecord(name: "testdb", createdAtNs: nowNs())
    let plan1 = newPlan()
    plan1.add(PlanOp(kind: poCreateDatabase, cdbName: "testdb",
                     cdbValue: encode(dbRec), cdbIfNotExists: false))
    discard executeWithTxn(plan1, ctx)

    # Try creating again with IF NOT EXISTS
    let plan2 = newPlan()
    plan2.add(PlanOp(kind: poCreateDatabase, cdbName: "testdb",
                     cdbValue: encode(dbRec), cdbIfNotExists: true))

    let result = executeWithTxn(plan2, ctx)
    check result.kind == erkOk
    check "IF NOT EXISTS" in result.okMessage

  test "executeWithTxn CREATE DATABASE duplicate fails":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Create database first
    let dbRec = DatabaseRecord(name: "testdb", createdAtNs: nowNs())
    let plan1 = newPlan()
    plan1.add(PlanOp(kind: poCreateDatabase, cdbName: "testdb",
                     cdbValue: encode(dbRec), cdbIfNotExists: false))
    discard executeWithTxn(plan1, ctx)

    # Try creating again without IF NOT EXISTS
    let plan2 = newPlan()
    plan2.add(PlanOp(kind: poCreateDatabase, cdbName: "testdb",
                     cdbValue: encode(dbRec), cdbIfNotExists: false))

    let result = executeWithTxn(plan2, ctx)
    check result.kind == erkError
    check "already exists" in result.error

  test "executeWithTxn DROP DATABASE":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Create database first
    let dbRec = DatabaseRecord(name: "testdb", createdAtNs: nowNs())
    var createPlan = newPlan()
    createPlan.add(PlanOp(kind: poCreateDatabase, cdbName: "testdb",
                          cdbValue: encode(dbRec)))
    discard executeWithTxn(createPlan, ctx)

    # Drop it
    let plan = newPlan()
    plan.add(PlanOp(kind: poDropDatabase, ddbName: "testdb",
        ddbIfExists: false))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkOk
    check "DROP DATABASE" in result.okMessage

    # Verify database was dropped
    let key = encodeTableKey(SYS_DATABASES_TABLE_ID, "testdb")
    check not mockKV.hasKey(key)

  test "executeWithTxn CREATE SCHEMA":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    let schemaRec = SchemaRecord(name: "myschema", database: "default",
                                 createdAtNs: nowNs())
    let plan = newPlan()
    plan.add(PlanOp(kind: poCreateSchema, csDatabase: "default",
                    csName: "myschema", csValue: encode(schemaRec),
                    csIfNotExists: false))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkOk
    check "CREATE SCHEMA" in result.okMessage

    # Verify schema was created
    let key = encodeTableKey(SYS_SCHEMAS_TABLE_ID, "default.myschema")
    check mockKV.hasKey(key)

  test "executeWithTxn CREATE TABLE":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    let tableRec = TableRecord(
      tableId: genTableId(),
      name: "users",
      database: "default",
      schema: "public",
      spaceId: genSpaceID(),
      columns: @[ColumnDefBin(name: "id", dataType: cdtInt,
                               flags: uint8(cfPrimaryKey.ord))],
      primaryKey: @["id"]
    )
    let plan = newPlan()
    plan.add(PlanOp(kind: poCreateTable, ctDatabase: "default",
                    ctSchema: "public", ctName: "users",
                    ctValue: encode(tableRec), ctIfNotExists: false))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkOk
    check "CREATE TABLE" in result.okMessage

    # Verify table was created
    let key = encodeTableKey(SYS_TABLES_TABLE_ID, "default.public.users")
    check mockKV.hasKey(key)

suite "Executor SHOW Operations with MockKVStore":

  test "executeWithTxn SHOW DATABASES":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Create some databases
    let db1 = DatabaseRecord(name: "db1", createdAtNs: nowNs())
    let db2 = DatabaseRecord(name: "db2", createdAtNs: nowNs())
    var createPlan1 = newPlan()
    createPlan1.add(PlanOp(kind: poCreateDatabase, cdbName: "db1",
        cdbValue: encode(db1)))
    discard executeWithTxn(createPlan1, ctx)
    var createPlan2 = newPlan()
    createPlan2.add(PlanOp(kind: poCreateDatabase, cdbName: "db2",
        cdbValue: encode(db2)))
    discard executeWithTxn(createPlan2, ctx)

    # Show databases
    let plan = newPlan()
    plan.add(PlanOp(kind: poShowDatabases))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkRows
    check result.columns == @["database_name"]
    check result.rows.len >= 2 # At least db1 and db2

  test "executeWithTxn SHOW SCHEMAS":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Create a schema
    let schemaRec = SchemaRecord(name: "myschema", database: "default",
                                 createdAtNs: nowNs())
    var createSchemaPlan = newPlan()
    createSchemaPlan.add(PlanOp(kind: poCreateSchema, csDatabase: "default",
                                csName: "myschema", csValue: encode(schemaRec)))
    discard executeWithTxn(createSchemaPlan, ctx)

    # Show schemas
    let plan = newPlan()
    plan.add(PlanOp(kind: poShowSchemas, ssDatabase: "default"))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkRows
    check result.columns == @["schema_name"]
    check result.rows.len >= 1

  test "executeWithTxn SHOW TABLES":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Create a table
    let tableRec = TableRecord(
      tableId: genTableId(),
      name: "users",
      database: "default",
      schema: "public",
      spaceId: genSpaceID(),
      columns: @[ColumnDefBin(name: "id", dataType: cdtInt,
                               flags: uint8(cfPrimaryKey.ord))],
      primaryKey: @["id"]
    )
    var createTablePlan = newPlan()
    createTablePlan.add(PlanOp(kind: poCreateTable, ctDatabase: "default",
                               ctSchema: "public", ctName: "users",
                               ctValue: encode(tableRec)))
    discard executeWithTxn(createTablePlan, ctx)

    # Show tables
    let plan = newPlan()
    plan.add(PlanOp(kind: poShowTables, stDatabase: "default",
        stSchema: "public"))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkRows
    check result.columns == @["table_name"]
    check result.rows.len >= 1

suite "Executor USE Operations with MockKVStore":

  test "executeWithTxn USE DATABASE valid":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Create database first
    let dbRec = DatabaseRecord(name: "mydb", createdAtNs: nowNs())
    var createDbPlan = newPlan()
    createDbPlan.add(PlanOp(kind: poCreateDatabase, cdbName: "mydb",
                            cdbValue: encode(dbRec)))
    discard executeWithTxn(createDbPlan, ctx)

    # Use database
    let plan = newPlan()
    plan.add(PlanOp(kind: poUseDatabase, udName: "mydb"))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkUseDatabase
    check result.newDatabase == "mydb"

  test "executeWithTxn USE DATABASE invalid":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    let plan = newPlan()
    plan.add(PlanOp(kind: poUseDatabase, udName: "nonexistent"))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkError
    check "does not exist" in result.error

  test "executeWithTxn USE SCHEMA valid":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV, nil, "default")

    # Create schema first
    let schemaRec = SchemaRecord(name: "myschema", database: "default",
                                 createdAtNs: nowNs())
    var createSchemaPlan2 = newPlan()
    createSchemaPlan2.add(PlanOp(kind: poCreateSchema, csDatabase: "default",
                                 csName: "myschema", csValue: encode(schemaRec)))
    discard executeWithTxn(createSchemaPlan2, ctx)

    # Use schema
    let plan = newPlan()
    plan.add(PlanOp(kind: poUseSchema, usName: "myschema"))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkUseSchema
    check result.newSchema == "myschema"

suite "Executor DDL Forbidden in Transaction":

  test "CREATE DATABASE forbidden in transaction":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin transaction
    var beginPlan = newPlan()
    beginPlan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan, ctx)

    # Try CREATE DATABASE
    let dbRec = DatabaseRecord(name: "testdb", createdAtNs: nowNs())
    let plan = newPlan()
    plan.add(PlanOp(kind: poCreateDatabase, cdbName: "testdb",
                    cdbValue: encode(dbRec)))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkError
    check "not allowed inside a transaction" in result.error

  test "CREATE TABLE forbidden in transaction":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin transaction
    var beginPlan2 = newPlan()
    beginPlan2.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan2, ctx)

    # Try CREATE TABLE
    let tableRec = TableRecord(
      tableId: genTableId(),
      name: "users",
      database: "default",
      schema: "public",
      spaceId: genSpaceID(),
      columns: @[ColumnDefBin(name: "id", dataType: cdtInt,
                               flags: uint8(cfPrimaryKey.ord))],
      primaryKey: @["id"]
    )
    let plan = newPlan()
    plan.add(PlanOp(kind: poCreateTable, ctDatabase: "default",
                    ctSchema: "public", ctName: "users",
                    ctValue: encode(tableRec)))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkError
    check "not allowed inside a transaction" in result.error

suite "Executor EXPLAIN with MockKVStore":

  test "executeWithTxn EXPLAIN":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    let innerPlan = newPlan()
    innerPlan.add(PlanOp(kind: poShowDatabases))

    let plan = newPlan()
    plan.add(PlanOp(kind: poExplain, exInnerPlan: innerPlan))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkRows
    check result.columns == @["plan"]
    check result.rows.len >= 1
    check "ShowDatabases" in result.rows[0][0]

suite "Executor DROP Operations with MockKVStore":

  test "executeWithTxn DROP SCHEMA":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Create schema first
    let schemaRec = SchemaRecord(name: "myschema", database: "default",
                                 createdAtNs: nowNs())
    var createSchemaPlan = newPlan()
    createSchemaPlan.add(PlanOp(kind: poCreateSchema, csDatabase: "default",
                                csName: "myschema", csValue: encode(schemaRec)))
    discard executeWithTxn(createSchemaPlan, ctx)

    # Drop the schema
    let plan = newPlan()
    plan.add(PlanOp(kind: poDropSchema, dsDatabase: "default",
                    dsName: "myschema", dsIfExists: false))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkOk
    check "DROP SCHEMA" in result.okMessage

    # Verify schema was deleted
    let key = encodeTableKey(SYS_SCHEMAS_TABLE_ID, "default.myschema")
    check not mockKV.hasKey(key)

  test "executeWithTxn DROP SCHEMA IF EXISTS non-existent":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    let plan = newPlan()
    plan.add(PlanOp(kind: poDropSchema, dsDatabase: "default",
                    dsName: "nonexistent", dsIfExists: true))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkOk
    check "IF EXISTS" in result.okMessage

  test "executeWithTxn DROP SCHEMA non-existent fails":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    let plan = newPlan()
    plan.add(PlanOp(kind: poDropSchema, dsDatabase: "default",
                    dsName: "nonexistent", dsIfExists: false))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkError
    check "does not exist" in result.error

  test "executeWithTxn DROP TABLE":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Create table first
    let tableRec = TableRecord(
      tableId: genTableId(),
      name: "users",
      database: "default",
      schema: "public",
      spaceId: genSpaceID(),
      columns: @[ColumnDefBin(name: "id", dataType: cdtInt,
                               flags: uint8(cfPrimaryKey.ord))],
      primaryKey: @["id"]
    )
    var createTablePlan = newPlan()
    createTablePlan.add(PlanOp(kind: poCreateTable, ctDatabase: "default",
                               ctSchema: "public", ctName: "users",
                               ctValue: encode(tableRec)))
    discard executeWithTxn(createTablePlan, ctx)

    # Drop the table
    let plan = newPlan()
    plan.add(PlanOp(kind: poDropTable, dtDatabase: "default",
                    dtSchema: "public", dtName: "users", dtIfExists: false))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkOk
    check "DROP TABLE" in result.okMessage

    # Verify table was deleted
    let key = encodeTableKey(SYS_TABLES_TABLE_ID, "default.public.users")
    check not mockKV.hasKey(key)

  test "executeWithTxn DROP TABLE IF EXISTS non-existent":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    let plan = newPlan()
    plan.add(PlanOp(kind: poDropTable, dtDatabase: "default",
                    dtSchema: "public", dtName: "nonexistent",
                    dtIfExists: true))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkOk
    check "IF EXISTS" in result.okMessage

  test "executeWithTxn DROP TABLE non-existent fails":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    let plan = newPlan()
    plan.add(PlanOp(kind: poDropTable, dtDatabase: "default",
                    dtSchema: "public", dtName: "nonexistent",
                    dtIfExists: false))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkError
    check "does not exist" in result.error

suite "Executor DML Operations with MockKVStore":

  test "executeWithTxn INSERT row":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin a transaction for DML
    var beginPlan = newPlan()
    beginPlan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan, ctx)

    # Create a data row
    var row = newDataRow()
    row["id"] = DataRowValue(kind: drvkInt, intVal: 123)
    row["name"] = DataRowValue(kind: drvkString, strVal: "Alice")

    let tableId = genTableId()
    let pkSpec = PrimaryKeySpec(columns: @[("id", cdtInt, 0)])
    let pkValue = bytesToString(encodeInt64BE(123'i64))

    let plan = newPlan()
    plan.add(PlanOp(
      kind: poInsert,
      insTableId: tableId,
      insTableName: "users",
      insColumns: @["id", "name"],
      insPkColumn: "id",
      insPkSpec: pkSpec,
      insRows: @[encodeDataRow(row)],
      insPkValues: @[pkValue]
    ))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkModified
    check result.count == 1

    # Commit the transaction to persist the data
    var commitPlan = newPlan()
    commitPlan.add(PlanOp(kind: poCommitTxn))
    discard executeWithTxn(commitPlan, ctx)

    # Verify row was inserted
    let key = encodeDataRowKey(tableId, pkValue)
    check mockKV.hasKey(key)

  test "executeWithTxn INSERT multiple rows":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin a transaction
    var beginPlan = newPlan()
    beginPlan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan, ctx)

    let tableId = genTableId()
    let pkSpec = PrimaryKeySpec(columns: @[("id", cdtInt, 0)])

    var rows: seq[string] = @[]
    var pkValues: seq[string] = @[]
    for i in 1..3:
      var row = newDataRow()
      row["id"] = DataRowValue(kind: drvkInt, intVal: int64(i))
      row["name"] = DataRowValue(kind: drvkString, strVal: "User" & $i)
      rows.add(encodeDataRow(row))
      pkValues.add(bytesToString(encodeInt64BE(int64(i))))

    let plan = newPlan()
    plan.add(PlanOp(
      kind: poInsert,
      insTableId: tableId,
      insTableName: "users",
      insColumns: @["id", "name"],
      insPkColumn: "id",
      insPkSpec: pkSpec,
      insRows: rows,
      insPkValues: pkValues
    ))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkModified
    check result.count == 3

  test "executeWithTxn INSERT without PK fails":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin a transaction
    var beginPlan = newPlan()
    beginPlan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan, ctx)

    var row = newDataRow()
    row["name"] = DataRowValue(kind: drvkString, strVal: "Alice")

    let plan = newPlan()
    plan.add(PlanOp(
      kind: poInsert,
      insTableId: genTableId(),
      insTableName: "users",
      insColumns: @["name"],
      insPkColumn: "id",
      insPkSpec: PrimaryKeySpec(columns: @[("id", cdtInt, 0)]),
      insRows: @[encodeDataRow(row)],
      insPkValues: @[""] # Empty PK value
    ))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkError
    check "primary key" in result.error

  test "executeWithTxn PointGet existing row":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin a transaction
    var beginPlan = newPlan()
    beginPlan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan, ctx)

    # Insert a row first
    let tableId = genTableId()
    var row = newDataRow()
    row["id"] = DataRowValue(kind: drvkInt, intVal: 42)
    row["name"] = DataRowValue(kind: drvkString, strVal: "Bob")
    let pkValue = bytesToString(encodeInt64BE(42'i64))

    var insertPlan = newPlan()
    insertPlan.add(PlanOp(
      kind: poInsert,
      insTableId: tableId,
      insTableName: "users",
      insColumns: @["id", "name"],
      insPkColumn: "id",
      insPkSpec: PrimaryKeySpec(columns: @[("id", cdtInt, 0)]),
      insRows: @[encodeDataRow(row)],
      insPkValues: @[pkValue]
    ))
    discard executeWithTxn(insertPlan, ctx)

    # PointGet the row
    let plan = newPlan()
    plan.add(PlanOp(
      kind: poPointGet,
      pgTableId: tableId,
      pgKey: pkValue,
      pgPkSpec: PrimaryKeySpec(columns: @[("id", cdtInt, 0)]),
      pgColumns: @["id", "name"],
      pgAllColumns: @["id", "name"]
    ))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkRows
    check result.rows.len == 1
    check result.rows[0].len == 2

  test "executeWithTxn PointGet non-existent row":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin a transaction
    var beginPlan = newPlan()
    beginPlan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan, ctx)

    let tableId = genTableId()
    let pkValue = bytesToString(encodeInt64BE(999'i64))

    let plan = newPlan()
    plan.add(PlanOp(
      kind: poPointGet,
      pgTableId: tableId,
      pgKey: pkValue,
      pgPkSpec: PrimaryKeySpec(columns: @[("id", cdtInt, 0)]),
      pgColumns: @["id", "name"],
      pgAllColumns: @["id", "name"]
    ))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkRows
    check result.rows.len == 0

  test "executeWithTxn Scan all rows":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin a transaction
    var beginPlan = newPlan()
    beginPlan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan, ctx)

    # Insert multiple rows
    let tableId = genTableId()
    let pkSpec = PrimaryKeySpec(columns: @[("id", cdtInt, 0)])

    for i in 1..5:
      var row = newDataRow()
      row["id"] = DataRowValue(kind: drvkInt, intVal: int64(i * 10))
      row["value"] = DataRowValue(kind: drvkString, strVal: "val" & $i)
      var insertPlan = newPlan()
      insertPlan.add(PlanOp(
        kind: poInsert,
        insTableId: tableId,
        insTableName: "test",
        insColumns: @["id", "value"],
        insPkColumn: "id",
        insPkSpec: pkSpec,
        insRows: @[encodeDataRow(row)],
        insPkValues: @[bytesToString(encodeInt64BE(int64(i * 10)))]
      ))
      discard executeWithTxn(insertPlan, ctx)

    # Scan all rows
    let startKey = encodeDataRowKey(tableId, "")
    let endKey = makeDataRowScanEndKey(tableId)

    let plan = newPlan()
    plan.add(PlanOp(
      kind: poScan,
      scTableId: tableId,
      scStartKey: startKey,
      scEndKey: endKey,
      scLimit: 0,
      scFilter: none(Expr),
      scColumns: @["id", "value"],
      scAllColumns: @["id", "value"]
    ))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkRows
    check result.rows.len == 5

  test "executeWithTxn Scan with limit":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin a transaction
    var beginPlan = newPlan()
    beginPlan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan, ctx)

    # Insert multiple rows
    let tableId = genTableId()
    let pkSpec = PrimaryKeySpec(columns: @[("id", cdtInt, 0)])

    for i in 1..10:
      var row = newDataRow()
      row["id"] = DataRowValue(kind: drvkInt, intVal: int64(i))
      var insertPlan = newPlan()
      insertPlan.add(PlanOp(
        kind: poInsert,
        insTableId: tableId,
        insTableName: "test",
        insColumns: @["id"],
        insPkColumn: "id",
        insPkSpec: pkSpec,
        insRows: @[encodeDataRow(row)],
        insPkValues: @[bytesToString(encodeInt64BE(int64(i)))]
      ))
      discard executeWithTxn(insertPlan, ctx)

    # Scan with limit
    let startKey = encodeDataRowKey(tableId, "")
    let endKey = makeDataRowScanEndKey(tableId)

    let plan = newPlan()
    plan.add(PlanOp(
      kind: poScan,
      scTableId: tableId,
      scStartKey: startKey,
      scEndKey: endKey,
      scLimit: 3,
      scFilter: none(Expr),
      scColumns: @["id"],
      scAllColumns: @["id"]
    ))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkRows
    check result.rows.len == 3

  test "executeWithTxn UPDATE rows":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin a transaction
    var beginPlan = newPlan()
    beginPlan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan, ctx)

    # Insert rows
    let tableId = genTableId()
    let pkSpec = PrimaryKeySpec(columns: @[("id", cdtInt, 0)])

    for i in 1..3:
      var row = newDataRow()
      row["id"] = DataRowValue(kind: drvkInt, intVal: int64(i))
      row["status"] = DataRowValue(kind: drvkString, strVal: "active")
      var insertPlan = newPlan()
      insertPlan.add(PlanOp(
        kind: poInsert,
        insTableId: tableId,
        insTableName: "test",
        insColumns: @["id", "status"],
        insPkColumn: "id",
        insPkSpec: pkSpec,
        insRows: @[encodeDataRow(row)],
        insPkValues: @[bytesToString(encodeInt64BE(int64(i)))]
      ))
      discard executeWithTxn(insertPlan, ctx)

    # Update all rows (no filter)
    let plan = newPlan()
    plan.add(PlanOp(
      kind: poUpdate,
      upTableId: tableId,
      upTableName: "test",
      upFilter: none(Expr),
      upSets: @[(col: "status", val: Expr(kind: exLiteral,
          litValue: ValueRef(kind: dtString, strValue: "inactive")))],
      upAllColumns: @["id", "status"],
      upPkColumn: "id"
    ))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkModified
    check result.count == 3

  test "executeWithTxn DELETE rows":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin a transaction
    var beginPlan = newPlan()
    beginPlan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan, ctx)

    # Insert rows
    let tableId = genTableId()
    let pkSpec = PrimaryKeySpec(columns: @[("id", cdtInt, 0)])

    for i in 1..5:
      var row = newDataRow()
      row["id"] = DataRowValue(kind: drvkInt, intVal: int64(i))
      var insertPlan = newPlan()
      insertPlan.add(PlanOp(
        kind: poInsert,
        insTableId: tableId,
        insTableName: "test",
        insColumns: @["id"],
        insPkColumn: "id",
        insPkSpec: pkSpec,
        insRows: @[encodeDataRow(row)],
        insPkValues: @[bytesToString(encodeInt64BE(int64(i)))]
      ))
      discard executeWithTxn(insertPlan, ctx)

    # Delete all rows (no filter)
    let plan = newPlan()
    plan.add(PlanOp(
      kind: poDelete,
      delTableId: tableId,
      delTableName: "test",
      delFilter: none(Expr),
      delAllColumns: @["id"],
      delPkColumn: "id"
    ))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkModified
    check result.count == 5

suite "Executor SHOW SPACES with MockKVStore":

  test "executeWithTxn SHOW SPACES":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Create a space record
    let spaceRec = SpaceRecord(
      spaceId: genSpaceID(),
      name: "myspace",
      replicas: 3,
      groupCount: 5,
      groupIds: @[genGroupID(), genGroupID(), genGroupID(), genGroupID(),
          genGroupID()]
    )
    let key = encodeTableKey(SYS_SPACES_TABLE_ID, "myspace")
    discard mockKV.put(key, encode(spaceRec), txnId = zeroTransactionID())

    let plan = newPlan()
    plan.add(PlanOp(kind: poShowSpaces))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkRows
    check result.columns == @["space_id", "name", "replicas", "group_count", "group_ids"]
    check result.rows.len >= 1

suite "Executor DDL Forbidden in Transaction Extended":

  test "DROP SCHEMA forbidden in transaction":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin transaction
    var beginPlan = newPlan()
    beginPlan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan, ctx)

    # Try DROP SCHEMA
    let plan = newPlan()
    plan.add(PlanOp(kind: poDropSchema, dsDatabase: "default",
                    dsName: "test", dsIfExists: false))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkError
    check "not allowed inside a transaction" in result.error

  test "DROP TABLE forbidden in transaction":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin transaction
    var beginPlan = newPlan()
    beginPlan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan, ctx)

    # Try DROP TABLE
    let plan = newPlan()
    plan.add(PlanOp(kind: poDropTable, dtDatabase: "default",
                    dtSchema: "public", dtName: "test", dtIfExists: false))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkError
    check "not allowed inside a transaction" in result.error

  test "CREATE SPACE forbidden in transaction":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin transaction
    var beginPlan = newPlan()
    beginPlan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan, ctx)

    # Try CREATE SPACE
    let plan = newPlan()
    plan.add(PlanOp(kind: poCreateSpace, cspName: "testspace", cspReplicas: 3))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkError
    check "not allowed inside a transaction" in result.error

  test "DROP SPACE forbidden in transaction":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV)

    # Begin transaction
    var beginPlan = newPlan()
    beginPlan.add(PlanOp(kind: poBeginTxn, btReadOnly: false))
    discard executeWithTxn(beginPlan, ctx)

    # Try DROP SPACE
    let plan = newPlan()
    plan.add(PlanOp(kind: poDropSpace, dspName: "testspace"))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkError
    check "not allowed inside a transaction" in result.error

  test "CREATE SPACE without client fails":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV) # No real client

    let plan = newPlan()
    plan.add(PlanOp(kind: poCreateSpace, cspName: "testspace", cspReplicas: 3))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkError
    check "requires a real FractioClient" in result.error

  test "DROP SPACE without client fails":
    let mockKV = newMockKVStore()
    let ctx = newExecutorContextWithKV(mockKV) # No real client

    let plan = newPlan()
    plan.add(PlanOp(kind: poDropSpace, dspName: "testspace"))

    let result = executeWithTxn(plan, ctx)
    check result.kind == erkError
    check "requires a real FractioClient" in result.error
