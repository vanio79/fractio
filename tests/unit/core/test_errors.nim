# Unit tests for fractio/core/errors.nim
# Tests all error types and constructors

import std/[unittest, strutils, tables]
import fractio/core/errors

suite "FractioErrorKind":

  test "all error kinds except fekNone have codes":
    for kind in FractioErrorKind:
      if kind != fekNone:
        let code = ErrorCodes.getOrDefault(kind, -1)
        check code >= 0

  test "error codes are unique":
    var seenCodes: Table[int, FractioErrorKind] = initTable[int,
        FractioErrorKind]()
    for kind in FractioErrorKind:
      if kind == fekNone:
        continue
      let code = ErrorCodes.getOrDefault(kind, 0)
      if code in seenCodes:
        check false # Duplicate code found
      seenCodes[code] = kind

suite "newError":

  test "creates error with all fields":
    let err = newError(fekSyntax, "test error", "test context")
    check err.kind == fekSyntax
    check err.message == "test error"
    check err.code == ErrorCodes[fekSyntax]
    check err.context == "test context"

  test "creates error without context":
    let err = newError(fekStorage, "storage failed")
    check err.kind == fekStorage
    check err.message == "storage failed"
    check err.context == ""

  test "error code matches kind":
    let syntaxErr = newError(fekSyntax, "msg")
    check syntaxErr.code == 1000

    let semanticErr = newError(fekSemantic, "msg")
    check semanticErr.code == 2000

    let constraintErr = newError(fekConstraint, "msg")
    check constraintErr.code == 3000

    let transactionErr = newError(fekTransaction, "msg")
    check transactionErr.code == 4000

    let deadlockErr = newError(fekDeadlock, "msg")
    check deadlockErr.code == 4100

    let shardingErr = newError(fekSharding, "msg")
    check shardingErr.code == 5000

    let replicationErr = newError(fekReplication, "msg")
    check replicationErr.code == 6000

    let networkErr = newError(fekNetwork, "msg")
    check networkErr.code == 7000

    let storageErr = newError(fekStorage, "msg")
    check storageErr.code == 8000

    let configErr = newError(fekConfig, "msg")
    check configErr.code == 9000

    let permissionErr = newError(fekPermission, "msg")
    check permissionErr.code == 10000

    let notImplErr = newError(fekNotImplemented, "msg")
    check notImplErr.code == 11000

suite "isError":

  test "fekNone is not an error":
    let err = newError(fekNone, "")
    check not isError(err)

  test "other kinds are errors":
    for kind in FractioErrorKind:
      if kind != fekNone:
        let err = newError(kind, "test")
        check isError(err)

suite "error string representation":

  test "string format without context":
    let err = newError(fekSyntax, "syntax error")
    let s = $err
    check s.contains("1000")
    check s.contains("syntax error")
    check not s.contains("context")

  test "string format with context":
    let err = newError(fekSyntax, "syntax error", "line 5")
    let s = $err
    check s.contains("1000")
    check s.contains("syntax error")
    check s.contains("context")
    check s.contains("line 5")

  test "string format for all kinds":
    for kind in FractioErrorKind:
      if kind != fekNone:
        let err = newError(kind, "test message", "test context")
        let s = $err
        check s.contains("test message")
        check s.contains($err.code)
        check s.contains("context")

suite "Error Constructors":

  test "syntaxError":
    let err = syntaxError("invalid SQL", "SELECT")
    check err.kind == fekSyntax
    check err.message == "invalid SQL"
    check err.context == "SELECT"

  test "semanticError":
    let err = semanticError("unknown table", "users")
    check err.kind == fekSemantic
    check err.message == "unknown table"
    check err.context == "users"

  test "constraintError":
    let err = constraintError("primary key violation", "id")
    check err.kind == fekConstraint
    check err.message == "primary key violation"
    check err.context == "id"

  test "transactionError":
    let err = transactionError("transaction aborted", "txn123")
    check err.kind == fekTransaction
    check err.message == "transaction aborted"
    check err.context == "txn123"

  test "deadlockError":
    let err = deadlockError()
    check err.kind == fekDeadlock
    check err.message.contains("Deadlock")
    check err.context == ""

  test "shardingError":
    let err = shardingError("shard not found", "shard5")
    check err.kind == fekSharding
    check err.message == "shard not found"
    check err.context == "shard5"

  test "replicationError":
    let err = replicationError("replica offline", "node3")
    check err.kind == fekReplication
    check err.message == "replica offline"
    check err.context == "node3"

  test "networkError":
    let err = networkError("connection timeout", "192.168.1.1")
    check err.kind == fekNetwork
    check err.message == "connection timeout"
    check err.context == "192.168.1.1"

  test "storageError":
    let err = storageError("disk full", "/data")
    check err.kind == fekStorage
    check err.message == "disk full"
    check err.context == "/data"

  test "configError":
    let err = configError("invalid config", "port")
    check err.kind == fekConfig
    check err.message == "invalid config"
    check err.context == "port"

  test "permissionError":
    let err = permissionError("access denied", "admin")
    check err.kind == fekPermission
    check err.message == "access denied"
    check err.context == "admin"

  test "notImplementedError":
    let err = notImplementedError("feature X", "future")
    check err.kind == fekNotImplemented
    check err.message == "feature X"
    check err.context == "future"

suite "Error Exception Behavior":

  test "can raise and catch":
    var caught = false
    try:
      raise newError(fekStorage, "test error")
    except FractioError as e:
      caught = true
      check e.kind == fekStorage
      check e.message == "test error"
    check caught

  test "can catch as Exception":
    var caught = false
    try:
      raise newError(fekNetwork, "network issue")
    except Exception as e:
      caught = true
      check e.msg.contains("network issue")
    check caught

  test "exception msg matches message field":
    let err = newError(fekSyntax, "SQL syntax error", "SELECT")
    try:
      raise err
    except FractioError as e:
      check e.msg == "SQL syntax error"
