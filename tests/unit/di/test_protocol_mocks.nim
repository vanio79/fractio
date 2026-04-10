# Tests for Protocol Layer Mocks (Phase 5 of DI refactoring)
# Tests MockProtocolServer and MockProtocolClient implementations

import std/unittest
import std/options
import fractio/di/mocks
import fractio/core/types

suite "MockProtocolServer Tests":

  var server: MockProtocolServer

  setup:
    server = newMockProtocolServer()

  teardown:
    server.reset()

  test "newMockProtocolServer creates server with default state":
    check not server.isRunning()
    check server.clientCount() == 0
    check server.startCallCount == 0
    check server.stopCallCount == 0

  test "start sets running flag and increments counter":
    server.start()
    server.assertStartCalled(1)
    server.assertRunning()
    check server.isRunning() == true

  test "stop clears running flag and increments counter":
    server.start()
    server.stop()
    server.assertStopCalled(1)
    server.assertNotRunning()
    check server.isRunning() == false

  test "registerHandler increments counter":
    var handlerCalled = false
    proc testHandler(conn: RootRef, requestId: uint32, flags: uint16,
        payload: string) {.gcsafe.} =
      handlerCalled = true

    server.registerHandler(0x0100, testHandler)
    check server.registerHandlerCallCount == 1

  test "addClient and removeClient affect client count":
    let clientId = 1'u32
    let mockConn = newMockConnectionHandle()
    server.addClient(clientId, cast[RootRef](mockConn))
    check server.clientCount() == 1

    server.removeClient(clientId)
    check server.clientCount() == 0

  test "kvGet on empty data returns none":
    let result = server.kvGet("testkey")
    check result.isNone()

  test "kvPut then kvGet returns value":
    server.kvPut("testkey", "testvalue")
    let result = server.kvGet("testkey")
    check result.isSome()
    check result.get() == "testvalue"

  test "kvDelete removes key":
    server.kvPut("key1", "value1")
    check server.kvGet("key1").isSome()

    let deleted = server.kvDelete("key1")
    check deleted == true
    check server.kvGet("key1").isNone()

  test "kvDelete on non-existent key returns false":
    let deleted = server.kvDelete("nonexistent")
    check deleted == false

  test "reset clears all state":
    server.start()
    server.kvPut("key", "value")
    server.addClient(1'u32, cast[RootRef](newMockConnectionHandle()))

    server.reset()
    check not server.isRunning()
    check server.clientCount() == 0
    check server.kvGet("key").isNone()
    check server.startCallCount == 0
    check server.stopCallCount == 0

suite "MockProtocolClient Tests":

  var client: MockProtocolClient

  setup:
    client = newMockProtocolClient()

  teardown:
    client.reset()

  test "newMockProtocolClient creates client with default state":
    check not client.isConnected()
    check client.connectCallCount == 0
    check client.disconnectCallCount == 0
    check client.pingCallCount == 0
    check client.getCallCount == 0
    check client.putCallCount == 0

  test "connect sets connected flag and increments counter":
    check client.connect() == true
    check client.isConnected() == true
    client.assertConnectCalled(1)

  test "disconnect clears connected flag":
    check client.connect() == true
    client.disconnect()
    check not client.isConnected()
    client.assertDisconnectCalled(1)

  test "connect can be forced to fail":
    client.setForceConnectError(true)
    check client.connect() == false
    check not client.isConnected()

  test "ping returns true when connected":
    check client.connect() == true
    check client.ping() == true
    client.assertPingCalled(1)

  test "ping returns false when not connected":
    check client.ping() == false
    client.assertPingCalled(1)

  test "kvGet returns none when not connected":
    check client.kvGet("key").isNone()

  test "kvGet returns none for non-existent key":
    check client.connect() == true
    check client.kvGet("nonexistent").isNone()
    client.assertGetCalled(1)

  test "kvPut and kvGet work when connected":
    check client.connect() == true
    check client.kvPut("key1", "value1") == true

    let getResult = client.kvGet("key1")
    check getResult.isSome()
    check getResult.get() == "value1"
    client.assertKVData("key1", "value1")

  test "kvPut returns false when not connected":
    check client.kvPut("key", "value") == false

  test "kvPut can be forced to fail":
    check client.connect() == true
    client.setForcePutError(true)
    check client.kvPut("key", "value") == false

  test "kvGet can be forced to fail":
    check client.connect() == true
    check client.kvPut("key", "value") == true
    client.setForceGetError(true)
    check client.kvGet("key").isNone()

  test "kvDelete removes key when connected":
    check client.connect() == true
    check client.kvPut("key1", "value1") == true
    check client.kvDelete("key1") == true
    check client.kvGet("key1").isNone()

  test "kvDelete returns false when not connected":
    check client.connect() == true
    check client.kvPut("key", "value") == true
    client.disconnect()
    check client.kvDelete("key") == false

  test "kvScan returns matching keys":
    check client.connect() == true
    check client.kvPut("prefix_key1", "value1") == true
    check client.kvPut("prefix_key2", "value2") == true
    check client.kvPut("other_key", "value3") == true

    let results = client.kvScan("prefix_", 10'u32)
    check results.len == 2

  test "kvScan respects limit":
    check client.connect() == true
    check client.kvPut("key1", "value1") == true
    check client.kvPut("key2", "value2") == true
    check client.kvPut("key3", "value3") == true

    let results = client.kvScan("key", 2'u32)
    check results.len == 2

  test "kvScan returns empty when not connected":
    check client.connect() == true
    check client.kvPut("key", "value") == true
    client.disconnect()
    check client.kvScan("", 10'u32).len == 0

  test "beginTxn returns transaction ID when connected":
    check client.connect() == true
    let txnId = client.beginTxn()
    check not isZero(txnId)
    check client.beginTxnCallCount == 1

  test "beginTxn returns zero when not connected":
    check isZero(client.beginTxn())

  test "commitTxn returns true when connected":
    check client.connect() == true
    let txnId = client.beginTxn()
    check client.commitTxn(txnId) == true
    check client.commitTxnCallCount == 1
    client.assertTxnCount(0)

  test "commitTxn returns false when not connected":
    check client.connect() == true
    let txnId = client.beginTxn()
    client.disconnect()
    check client.commitTxn(txnId) == false

  test "rollbackTxn returns true when connected":
    check client.connect() == true
    let txnId = client.beginTxn()
    check client.rollbackTxn(txnId) == true
    check client.rollbackTxnCallCount == 1
    client.assertTxnCount(0)

  test "rollbackTxn returns false when not connected":
    check client.connect() == true
    let txnId = client.beginTxn()
    client.disconnect()
    check client.rollbackTxn(txnId) == false

  test "multiple transactions tracked correctly":
    check client.connect() == true
    let txn1 = client.beginTxn()
    let txn2 = client.beginTxn()
    let txn3 = client.beginTxn()
    client.assertTxnCount(3)

    check client.commitTxn(txn1) == true
    client.assertTxnCount(2)

    check client.rollbackTxn(txn2) == true
    client.assertTxnCount(1)

    check client.commitTxn(txn3) == true
    client.assertTxnCount(0)

  test "reset clears all state":
    check client.connect() == true
    check client.kvPut("key", "value") == true
    check not isZero(client.beginTxn())

    client.reset()
    check not client.isConnected()
    check client.kvGet("key").isNone()
    client.assertConnectCalled(0)
    client.assertTxnCount(0)

suite "Protocol Mock Integration Tests":

  test "server and client can interact via mock KV":
    var server = newMockProtocolServer()
    var client = newMockProtocolClient()

    # Simulate server-side operation
    server.start()
    server.kvPut("shared_key", "shared_value")

    # Client connects and reads (using mock's own KV - simulates roundtrip)
    check client.connect() == true
    check client.kvPut("shared_key", "shared_value") == true

    let result = client.kvGet("shared_key")
    check result.isSome()
    check result.get() == "shared_value"

    server.reset()
    client.reset()

  test "transaction workflow with mocks":
    var client = newMockProtocolClient()
    check client.connect() == true

    # Begin transaction
    let txnId = client.beginTxn()
    check not isZero(txnId)

    # Perform operations within "transaction"
    check client.kvPut("txn_key1", "txn_value1") == true
    check client.kvPut("txn_key2", "txn_value2") == true

    # Commit
    check client.commitTxn(txnId) == true

    # Verify data persists
    check client.kvGet("txn_key1").isSome()
    check client.kvGet("txn_key2").isSome()

    client.reset()

  test "error injection testing":
    var client = newMockProtocolClient()

    # Test connect failure
    client.setForceConnectError(true)
    check client.connect() == false

    # Reset and test put failure
    client.reset()
    check client.connect() == true
    client.setForcePutError(true)
    check client.kvPut("key", "value") == false

    # Reset and test get failure
    client.reset()
    check client.connect() == true
    check client.kvPut("key", "value") == true
    client.setForceGetError(true)
    check client.kvGet("key").isNone()

    client.reset()

  test "client lifecycle tracking":
    var client = newMockProtocolClient()

    # Multiple connect/disconnect cycles
    for i in 0..2:
      check client.connect() == true
      client.disconnect()

    client.assertConnectCalled(3)
    client.assertDisconnectCalled(3)

    client.reset()

  test "server handler registration":
    var server = newMockProtocolServer()
    var handlerInvoked = 0

    proc countingHandler(conn: RootRef, requestId: uint32, flags: uint16,
        payload: string) {.gcsafe.} =
      inc handlerInvoked

    # Register multiple handlers
    server.registerHandler(0x0100, countingHandler)
    server.registerHandler(0x0101, countingHandler)
    server.registerHandler(0x0200, countingHandler)

    check server.registerHandlerCallCount == 3

    server.reset()
