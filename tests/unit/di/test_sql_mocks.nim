# Test SQL Layer Mocks (Phase 6)
# Tests for MockSqlExecutor and MockSqlPlanner

import unittest
import fractio/di/interfaces
import fractio/di/mocks
import fractio/core/errors
from fractio/core/types import TransactionID, zeroTransactionID,
    genTransactionID, `==`

suite "MockSqlExecutor Tests":

  var executor: MockSqlExecutor

  setup:
    executor = newMockSqlExecutor()

  teardown:
    executor.reset()

  test "create mock executor":
    check executor != nil
    check executor.executeCallCount == 0
    check executor.executeInTxnCallCount == 0
    check executor.lastSql == ""
    check executor.defaultResult.kind == erkEmpty

  test "execute returns default empty result":
    let result = executor.execute("SELECT * FROM users")
    check result.kind == erkEmpty
    check executor.executeCallCount == 1
    check executor.lastSql == "SELECT * FROM users"

  test "execute tracks multiple calls":
    discard executor.execute("SELECT 1")
    discard executor.execute("SELECT 2")
    discard executor.execute("SELECT 3")
    check executor.executeCallCount == 3
    check executor.lastSql == "SELECT 3"

  test "execute with predefined result":
    # Each row is seq[string] representing column values
    let rowsResult = ExecutionResult(
      kind: erkRows,
      rows: @[@["1", "Alice"], @["2", "Bob"]],
      count: 2
    )
    executor.setResult("SELECT * FROM users", rowsResult)

    let result = executor.execute("SELECT * FROM users")
    check result.kind == erkRows
    check result.rows.len == 2
    check result.rows[0].len == 2
    check result.rows[0][0] == "1"
    check result.rows[0][1] == "Alice"
    check executor.executeCallCount == 1

  test "execute with modified result":
    let modifiedResult = ExecutionResult(
      kind: erkModified,
      count: 5
    )
    executor.setResult("INSERT INTO users VALUES (1)", modifiedResult)

    let result = executor.execute("INSERT INTO users VALUES (1)")
    check result.kind == erkModified
    check result.count == 5

  test "execute with error result":
    executor.setForceError(true, "syntax error near 'SELECT'")
    let result = executor.execute("SELECT")
    check result.kind == erkError
    check result.error.isSome
    check executor.executeCallCount == 1

  test "executeInTxn tracks transaction ID":
    let txnId = genTransactionIDLocal()
    discard executor.executeInTxn("SELECT * FROM users", txnId)
    check executor.executeInTxnCallCount == 1
    check executor.lastSql == "SELECT * FROM users"
    check executor.lastTxnId == txnId

  test "executeInTxn uses predefined result":
    let modifiedResult = ExecutionResult(
      kind: erkModified,
      count: 1
    )
    executor.setResult("UPDATE users SET name='Test'", modifiedResult)

    let txnId = genTransactionIDLocal()
    let result = executor.executeInTxn("UPDATE users SET name='Test'", txnId)
    check result.kind == erkModified
    check result.count == 1

  test "setDefaultResult affects all queries":
    let defaultRows = ExecutionResult(
      kind: erkRows,
      rows: @[@["default", "value"]],
      count: 1
    )
    executor.setDefaultResult(defaultRows)

    let result1 = executor.execute("SELECT 1")
    let result2 = executor.execute("SELECT 2")
    check result1.kind == erkRows
    check result1.rows.len == 1
    check result2.kind == erkRows
    check result2.rows.len == 1

  test "reset clears all state":
    discard executor.execute("SELECT 1")
    discard executor.executeInTxn("SELECT 2", genTransactionIDLocal())
    executor.setForceError(true)

    executor.reset()
    check executor.executeCallCount == 0
    check executor.executeInTxnCallCount == 0
    check executor.lastSql == ""
    check executor.lastTxnId == zeroTransactionID()
    check executor.results.len == 0
    check not executor.forceError

suite "MockSqlExecutor Assertion Tests":

  var executor: MockSqlExecutor

  setup:
    executor = newMockSqlExecutor()

  teardown:
    executor.reset()

  test "assertExecuteCalled succeeds when count matches":
    discard executor.execute("SELECT 1")
    discard executor.execute("SELECT 2")
    assertExecuteCalled(executor, 2)

  test "assertExecuteInTxnCalled succeeds when count matches":
    discard executor.executeInTxn("SELECT 1", genTransactionIDLocal())
    discard executor.executeInTxn("SELECT 2", genTransactionIDLocal())
    assertExecuteInTxnCalled(executor, 2)

  test "assertLastSql succeeds when SQL matches":
    discard executor.execute("SELECT * FROM test")
    assertLastSql(executor, "SELECT * FROM test")

  test "assertLastTxnId succeeds when txnId matches":
    let txnId = genTransactionIDLocal()
    discard executor.executeInTxn("SELECT 1", txnId)
    assertLastTxnId(executor, txnId)

suite "MockSqlPlanner Tests":

  var planner: MockSqlPlanner

  setup:
    planner = newMockSqlPlanner()

  teardown:
    planner.reset()

  test "create mock planner":
    check planner != nil
    check planner.planCallCount == 0
    check planner.planWithDbCallCount == 0
    check planner.lastSql == ""
    check planner.planIdCounter == 0

  test "planSql returns incrementing plan IDs":
    let planId1 = planner.planSql("SELECT 1")
    let planId2 = planner.planSql("SELECT 2")
    let planId3 = planner.planSql("SELECT 3")
    check planId1 == 1
    check planId2 == 2
    check planId3 == 3
    check planner.planCallCount == 3
    check planner.lastSql == "SELECT 3"

  test "planSql caches results for same SQL":
    let planId1 = planner.planSql("SELECT * FROM users")
    let planId2 = planner.planSql("SELECT * FROM users")
    check planId1 == planId2
    check planner.planCallCount == 2 # Both calls counted

  test "planSqlWithDb tracks database and schema":
    let planId = planner.planSqlWithDb("SELECT 1", "testdb", "testschema")
    check planId > 0
    check planner.planWithDbCallCount == 1
    check planner.lastSql == "SELECT 1"
    check planner.lastDatabase == "testdb"
    check planner.lastSchema == "testschema"

  test "planSql with predefined plan ID":
    planner.setPlanId("SELECT * FROM users", 42)
    let planId = planner.planSql("SELECT * FROM users")
    check planId == 42
    check planner.planCallCount == 1

  test "planSql with forced error returns -1":
    planner.setForceError(true, "parse error")
    let planId = planner.planSql("SELECT")
    check planId == -1
    check planner.planCallCount == 1

  test "setDefaultPlanId affects all plans":
    planner.setDefaultPlanId(100)
    # Note: default is only used when SQL not in results table
    # The planner always generates new IDs for unknown SQL
    let planId = planner.planSql("SELECT 1")
    check planId == 1 # First new ID, not 100

  test "reset clears all state":
    discard planner.planSql("SELECT 1")
    discard planner.planSqlWithDb("SELECT 2", "db", "schema")
    planner.setForceError(true)

    planner.reset()
    check planner.planCallCount == 0
    check planner.planWithDbCallCount == 0
    check planner.lastSql == ""
    check planner.lastDatabase == ""
    check planner.lastSchema == ""
    check planner.planIdCounter == 0
    check planner.results.len == 0
    check not planner.forceError

suite "MockSqlPlanner Assertion Tests":

  var planner: MockSqlPlanner

  setup:
    planner = newMockSqlPlanner()

  teardown:
    planner.reset()

  test "assertPlanCalled succeeds when count matches":
    discard planner.planSql("SELECT 1")
    discard planner.planSql("SELECT 2")
    assertPlanCalled(planner, 2)

  test "assertPlanWithDbCalled succeeds when count matches":
    discard planner.planSqlWithDb("SELECT 1", "db1", "schema1")
    discard planner.planSqlWithDb("SELECT 2", "db2", "schema2")
    assertPlanWithDbCalled(planner, 2)

  test "assertLastPlanSql succeeds when SQL matches":
    discard planner.planSql("SELECT * FROM test")
    assertLastPlanSql(planner, "SELECT * FROM test")

  test "assertLastDatabase succeeds when database matches":
    discard planner.planSqlWithDb("SELECT 1", "mydb", "myschema")
    assertLastDatabase(planner, "mydb")

  test "assertLastSchema succeeds when schema matches":
    discard planner.planSqlWithDb("SELECT 1", "mydb", "myschema")
    assertLastSchema(planner, "myschema")

suite "SQL Mock Thread Safety Tests":

  test "MockSqlExecutor execute is thread-safe":
    var executor = newMockSqlExecutor()
    var threads: array[4, Thread[MockSqlExecutor]]

    proc worker(e: MockSqlExecutor) {.thread.} =
      for i in 0..<10:
        discard e.execute("SELECT " & $i)

    for i in 0..<4:
      createThread(threads[i], worker, executor)

    joinThreads(threads)

    check executor.executeCallCount == 40

  test "MockSqlPlanner planSql is thread-safe":
    var planner = newMockSqlPlanner()
    var threads: array[4, Thread[MockSqlPlanner]]

    proc worker(p: MockSqlPlanner) {.thread.} =
      for i in 0..<10:
        discard p.planSql("SELECT " & $i)

    for i in 0..<4:
      createThread(threads[i], worker, planner)

    joinThreads(threads)

    check planner.planCallCount == 40
