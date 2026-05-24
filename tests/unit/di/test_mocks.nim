# Unit tests for Fractio DI Mocks

import std/[unittest, options, tables, strformat]
import fractio/di/mocks # Mocks includes ExecutionResult from interfaces
import fractio/core/types

suite "MockTimeProvider":

  test "newMockTimeProvider creates with start time":
    let m = newMockTimeProvider(1000000)
    check m.nowNs() == 1000000
    check m.nowUs() == 1000
    check m.nowMs() == 1
    m.close()

  test "advance increments time":
    let m = newMockTimeProvider(0)
    m.advance(5000)
    check m.nowNs() == 5000
    m.advance(1000)
    check m.nowNs() == 6000
    m.close()

  test "setTime sets absolute time":
    let m = newMockTimeProvider(0)
    m.setTime(999999)
    check m.nowNs() == 999999
    m.close()

  test "callCount tracks invocations":
    let m = newMockTimeProvider(100)
    check m.callCount == 0

    discard m.nowNs()
    check m.callCount == 1

    discard m.nowUs()
    check m.callCount == 2

    discard m.nowMs()
    check m.callCount == 3
    m.close()

  test "reset clears state":
    let m = newMockTimeProvider(1000)
    discard m.nowNs()
    discard m.nowNs()

    m.reset()
    check m.currentTimeNs == 0
    check m.callCount == 0
    m.close()

  test "assertCalled validates call count":
    let m = newMockTimeProvider(0)
    discard m.nowNs()
    discard m.nowNs()
    discard m.nowNs()

    m.assertCalled(3)
    m.close()

  test "assertTimeEquals validates current time":
    let m = newMockTimeProvider(12345)
    m.assertTimeEquals(12345)

    m.advance(100)
    m.assertTimeEquals(12445)
    m.close()

suite "MockLogProvider":

  test "log captures entries":
    let m = newMockLogProvider()
    m.info("test message")

    check m.callCount == 1
    check m.entries.len == 1

    let entry = m.entries[0]
    check entry.level == llInfo
    check entry.message == "test message"
    m.close()

  test "minLevel filters messages":
    let m = newMockLogProvider(llInfo)

    m.debug("debug msg") # Should be filtered
    check m.entries.len == 0

    m.info("info msg")
    check m.entries.len == 1

    m.warn("warn msg")
    check m.entries.len == 2

    m.error("error msg")
    check m.entries.len == 3
    m.close()

  test "setMinLevel changes filter":
    let m = newMockLogProvider(llError)

    m.info("info") # Filtered
    check m.entries.len == 0

    m.setMinLevel(llInfo)
    m.info("info2") # Now allowed
    check m.entries.len == 1
    m.close()

  test "fields are captured":
    let m = newMockLogProvider()
    var fields = {"key1": "value1", "key2": "value2"}.toTable
    m.info("test", fields)

    let entry = m.entries[0]
    check entry.fields["key1"] == "value1"
    check entry.fields["key2"] == "value2"
    m.close()

  test "assertLogged validates entry":
    let m = newMockLogProvider()
    m.error("error occurred")
    m.assertLogged(llError, "error occurred")
    m.close()

  test "assertLoggedCount validates total":
    let m = newMockLogProvider()
    m.debug("1")
    m.info("2")
    m.warn("3")
    m.assertLoggedCount(3)
    m.close()

  test "assertNoErrors validates no errors":
    let m = newMockLogProvider()
    m.info("ok")
    m.warn("warning")
    m.assertNoErrors()

    m.error("oops")
    var passed = false
    try:
      m.assertNoErrors()
    except AssertionDefect:
      passed = true
    check passed
    m.close()

  test "getEntries returns all":
    let m = newMockLogProvider()
    m.debug("d")
    m.info("i")

    let entries = m.getEntries()
    check entries.len == 2
    check entries[0].level == llDebug
    check entries[1].level == llInfo
    m.close()

  test "getErrorEntries filters errors":
    let m = newMockLogProvider()
    m.info("info")
    m.error("e1")
    m.warn("warn")
    m.error("e2")

    let errors = m.getErrorEntries()
    check errors.len == 2
    check errors[0].message == "e1"
    check errors[1].message == "e2"
    m.close()

suite "MockKVStore":

  test "put and get":
    let m = newMockKVStore()

    let putResult = m.put("key1", "value1")
    check putResult == true

    let val = m.get("key1")
    check val.isSome
    check val.get == "value1"

    let missing = m.get("missing")
    check missing.isNone
    m.close()

  test "delete removes key":
    let m = newMockKVStore()
    discard m.put("key1", "value1")

    check m.exists("key1")

    let delResult = m.delete("key1")
    check delResult == true
    check not m.exists("key1")
    m.close()

  test "scan with prefix":
    let m = newMockKVStore()
    discard m.put("prefix/a", "1")
    discard m.put("prefix/b", "2")
    discard m.put("other/c", "3")
    discard m.put("prefix/d", "4")

    let results = m.scan("prefix/", 10)
    check results.len == 3
    m.close()

  test "scan respects limit":
    let m = newMockKVStore()
    discard m.put("key1", "1")
    discard m.put("key2", "2")
    discard m.put("key3", "3")
    discard m.put("key4", "4")

    let results = m.scan("key", 2)
    check results.len == 2
    m.close()

  test "call counts tracked":
    let m = newMockKVStore()

    discard m.get("a")
    check m.getCallCount == 1

    discard m.put("b", "v")
    check m.putCallCount == 1

    discard m.delete("c")
    check m.deleteCallCount == 1

    discard m.scan("", 10)
    check m.scanCallCount == 1

    discard m.exists("d")
    check m.existsCallCount == 1
    m.close()

  test "forceError injects errors":
    let m = newMockKVStore()
    m.setForceError(true, "test error")

    let putResult = m.put("key", "value")
    check putResult == false

    m.setForceError(false)
    let putResult2 = m.put("key", "value")
    check putResult2 == true
    m.close()

  test "reset clears all state":
    let m = newMockKVStore()
    discard m.put("key", "value")
    discard m.get("key")

    m.reset()
    check m.data.len == 0
    check m.getCallCount == 0
    check m.putCallCount == 0
    m.close()

  test "assertion helpers":
    let m = newMockKVStore()
    discard m.put("test", "value")

    m.assertGetCalled(0)
    discard m.get("test")
    m.assertGetCalled(1)

    m.assertKeyExists("test")
    m.assertKeyValue("test", "value")

    discard m.delete("test")
    m.assertKeyNotExists("test")
    m.close()

suite "MockTransactionManager":

  test "begin creates transaction":
    let m = newMockTransactionManager()

    let txnId = m.begin()
    check m.beginCallCount == 1
    check m.getActiveCount() == 1
    check m.getStatus(txnId) == tsActive

  test "commit removes from active":
    let m = newMockTransactionManager()

    let txnId = m.begin()
    check m.getActiveCount() == 1

    let result = m.commit(txnId)
    check result == true
    check m.getActiveCount() == 0
    check m.getStatus(txnId) == tsCommitted

  test "rollback marks aborted":
    let m = newMockTransactionManager()

    let txnId = m.begin()
    let result = m.rollback(txnId)

    check result == true
    check m.getStatus(txnId) == tsAborted
    check m.getActiveCount() == 0

  test "getOldestSnapshot tracks minimum":
    let m = newMockTransactionManager()

    discard m.begin()
    discard m.begin()

    # Both active, oldest should be first
    let oldest = m.getOldestSnapshot()
    check oldest > 0 # Has some timestamp

  test "multiple transactions":
    let m = newMockTransactionManager()

    let txn1 = m.begin()
    let txn2 = m.begin()
    let txn3 = m.begin()

    check m.getActiveCount() == 3

    discard m.commit(txn1)
    check m.getActiveCount() == 2

    discard m.rollback(txn2)
    check m.getActiveCount() == 1

    discard m.commit(txn3)
    check m.getActiveCount() == 0

  test "forceCommitError":
    let m = newMockTransactionManager()
    m.forceCommitError = true

    let txnId = m.begin()
    let result = m.commit(txnId)
    check result == false

  test "assertion helpers":
    let m = newMockTransactionManager()

    let txnId = m.begin()
    m.assertBeginCalled(1)
    m.assertActiveCount(1)

    discard m.commit(txnId)
    m.assertCommitCalled(1)
    m.assertTxnStatus(txnId, tsCommitted)

suite "MockBackend":

  test "put and get":
    let m = newMockBackend()

    discard m.put("key", "value")
    let val = m.get("key")
    check val.isSome
    check val.get == "value"

  test "delete works":
    let m = newMockBackend()
    discard m.put("k", "v")
    check m.get("k").isSome

    discard m.delete("k")
    check m.get("k").isNone

  test "scan with prefix":
    let m = newMockBackend()
    discard m.put("a/1", "1")
    discard m.put("a/2", "2")
    discard m.put("b/1", "3")

    let results = m.scan("a/", 10)
    check results.len == 2

  test "flush and compact":
    let m = newMockBackend()

    discard m.flush()
    check m.flushCallCount == 1

    discard m.compact()
    check m.compactCallCount == 1

  test "close marks closed":
    let m = newMockBackend()
    check not m.closed

    m.close()
    check m.closed

  test "stats returns metrics":
    let m = newMockBackend()
    discard m.put("k1", "v1")
    discard m.get("k1")
    discard m.delete("k1")

    let stats = m.stats()
    check stats["get_count"] == 1
    check stats["put_count"] == 1
    check stats["delete_count"] == 1

suite "MockConnectionHandle":

  test "send and recv":
    let h = newMockConnectionHandle("localhost:8080")

    let sendResult = h.send(@[1'u8, 2'u8, 3'u8])
    check sendResult == true
    check h.sendCallCount == 1

    h.queueResponse(@[4'u8, 5'u8, 6'u8])
    let recvResult = h.recv(1000)
    check recvResult == @[4'u8, 5'u8, 6'u8]
    discard h.close()

  test "close disconnects":
    let h = newMockConnectionHandle()
    check h.isConnected()

    discard h.close()
    check not h.isConnected()
    discard h.close()

  test "send fails when disconnected":
    let h = newMockConnectionHandle()
    discard h.close()

    let result = h.send(@[1'u8])
    check result == false
    discard h.close()

  test "remoteAddress returns address":
    let h = newMockConnectionHandle("192.168.1.1:9000")
    check h.remoteAddress() == "192.168.1.1:9000"
    discard h.close()

  test "getSentData captures sent":
    let h = newMockConnectionHandle()
    discard h.send(@[1'u8, 2'u8])
    discard h.send(@[3'u8, 4'u8])

    let sent = h.getSentData()
    check sent.len == 2
    check sent[0] == @[1'u8, 2'u8]
    check sent[1] == @[3'u8, 4'u8]
    discard h.close()

suite "MockConnectionManager":

  test "acquire creates connection":
    let m = newMockConnectionManager()

    let conn = m.acquire("localhost", 8080'u16)
    check conn.isConnected()
    check m.acquireCallCount == 1

  test "release tracks calls":
    let m = newMockConnectionManager()
    let conn = m.acquire("host", 80'u16)

    m.release(conn)
    check m.releaseCallCount == 1

  test "closeAll disconnects all":
    let m = newMockConnectionManager()
    discard m.acquire("h1", 80'u16)
    discard m.acquire("h2", 80'u16)

    m.closeAll()
    check m.closeAllCallCount == 1
    check m.poolSize() == 0

  test "activeCount counts connected":
    let m = newMockConnectionManager()
    let c1 = m.acquire("h1", 80'u16)
    let c2 = m.acquire("h2", 80'u16)

    discard c1.close()

    check m.activeCount() == 1

suite "MockSqlExecutor":

  test "execute tracks calls":
    let e = newMockSqlExecutor()

    let result = e.execute("SELECT * FROM test")
    check e.executeCallCount == 1
    check e.lastSql == "SELECT * FROM test"
    check result.kind == erkEmpty

  test "setResult returns predefined":
    let e = newMockSqlExecutor()

    e.setResult("INSERT INTO t VALUES (1)", mocks.ExecutionResult(
      kind: erkModified,
      count: 1
    ))

    let result = e.execute("INSERT INTO t VALUES (1)")
    check result.kind == erkModified
    check result.count == 1

  test "executeInTxn tracks txnId":
    let e = newMockSqlExecutor()
    let txnId = genTransactionIDLocal()

    discard e.executeInTxn("UPDATE t SET x = 1", txnId)
    check e.lastTxnId == txnId

  test "setDefaultResult":
    let e = newMockSqlExecutor()

    e.setDefaultResult(mocks.ExecutionResult(
      kind: erkRows,
      rows: @[],
      count: 1
    ))

    let result = e.execute("SELECT *")
    check result.kind == erkRows

  test "reset clears state":
    let e = newMockSqlExecutor()
    e.setResult("test", mocks.ExecutionResult(kind: erkModified, count: 5))
    discard e.execute("test")

    e.reset()
    check e.executeCallCount == 0
    check e.results.len == 0
