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
      check e.msg.contains("SQL syntax error")

suite "Error Code Verification":

  test "fekNone has no code":
    let code = ErrorCodes.getOrDefault(fekNone, -999)
    check code == -999 # Not defined

  test "fekSyntax code is 1000":
    check ErrorCodes[fekSyntax] == 1000

  test "fekSemantic code is 2000":
    check ErrorCodes[fekSemantic] == 2000

  test "fekConstraint code is 3000":
    check ErrorCodes[fekConstraint] == 3000

  test "fekTransaction code is 4000":
    check ErrorCodes[fekTransaction] == 4000

  test "fekDeadlock code is 4100":
    check ErrorCodes[fekDeadlock] == 4100

  test "fekSharding code is 5000":
    check ErrorCodes[fekSharding] == 5000

  test "fekReplication code is 6000":
    check ErrorCodes[fekReplication] == 6000

  test "fekNetwork code is 7000":
    check ErrorCodes[fekNetwork] == 7000

  test "fekStorage code is 8000":
    check ErrorCodes[fekStorage] == 8000

  test "fekConfig code is 9000":
    check ErrorCodes[fekConfig] == 9000

  test "fekPermission code is 10000":
    check ErrorCodes[fekPermission] == 10000

  test "fekNotImplemented code is 11000":
    check ErrorCodes[fekNotImplemented] == 11000

suite "Error isError helper":

  test "isError returns true for non-none kinds":
    check isError(newError(fekSyntax, "test"))
    check isError(newError(fekStorage, "test"))
    check isError(newError(fekNetwork, "test"))

  test "isError returns false for fekNone":
    let err = newError(fekNone, "test")
    check not isError(err)

suite "Error Dollar Operator":

  test "dollar includes FractioError prefix":
    let err = newError(fekSyntax, "test error")
    let str = $err
    check str.contains("FractioError")
    check str.contains("test error")

  test "dollar includes code":
    let err = newError(fekStorage, "disk full", "/data")
    let str = $err
    check str.contains("8000")

  test "dollar includes context when provided":
    let err = newError(fekNetwork, "timeout", "node1:8080")
    let str = $err
    check str.contains("node1:8080")

suite "Error Kind Names":

  test "all error kinds are accessible":
    check fekNone.ord == 0
    check fekSyntax.ord > 0
    check fekSemantic.ord > 0
    check fekConstraint.ord > 0
    check fekTransaction.ord > 0
    check fekDeadlock.ord > 0
    check fekSharding.ord > 0
    check fekReplication.ord > 0
    check fekNetwork.ord > 0
    check fekStorage.ord > 0
    check fekConfig.ord > 0
    check fekPermission.ord > 0
    check fekNotImplemented.ord > 0

suite "Error Constructors with Empty Context":

  test "syntaxError empty context":
    let err = syntaxError("bad SQL", "")
    check err.context == ""

  test "semanticError empty context":
    let err = semanticError("type mismatch", "")
    check err.context == ""

  test "constraintError empty context":
    let err = constraintError("PK violation", "")
    check err.context == ""

  test "transactionError empty context":
    let err = transactionError("abort", "")
    check err.context == ""

  test "deadlockError always succeeds":
    let err = deadlockError()
    check err.kind == fekDeadlock

  test "shardingError empty context":
    let err = shardingError("key not found", "")
    check err.context == ""

  test "replicationError empty context":
    let err = replicationError("leader lost", "")
    check err.context == ""

  test "networkError empty context":
    let err = networkError("connection failed", "")
    check err.context == ""

  test "storageError empty context":
    let err = storageError("write failed", "")
    check err.context == ""

  test "configError empty context":
    let err = configError("bad config", "")
    check err.context == ""

  test "permissionError empty context":
    let err = permissionError("denied", "")
    check err.context == ""

  test "notImplementedError empty context":
    let err = notImplementedError("future", "")
    check err.context == ""

suite "Error Message Handling":

  test "long messages are preserved":
    let longMsg = "This is a very long error message that should still be preserved in full without truncation or modification"
    let err = newError(fekStorage, longMsg)
    check err.message == longMsg

  test "special characters in message":
    let err = newError(fekSyntax, "Invalid chars: \n\t\r\"'")
    check err.message.contains("\n")
    check err.message.contains("\t")

  test "unicode in message":
    let err = newError(fekStorage, "Unicode: \u00e9\u00e8\u00ea")
    check err.message.contains("\u00e9")

suite "Error Context Handling":

  test "long context preserved":
    let longCtx = "Very long context string with details about where error occurred"
    let err = newError(fekNetwork, "timeout", longCtx)
    check err.context == longCtx

  test "special characters in context":
    let err = newError(fekStorage, "error", "path: /data/file\x00.bin")
    check err.context.contains("/data")
