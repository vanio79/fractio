## Raft consensus state machine interface and command definitions.
## This module defines the abstraction for applying committed Raft entries to the state machine.

import ./types
import ../../core/errors

# Minimal Result type for error handling (compatible with std/result)
type
  Result*[T, E] = object
    case isOk*: bool
    of true:
      value*: T
    of false:
      error*: E

proc ok*[T, E](value: T): Result[T, E] =
  Result[T, E](isOk: true, value: value)

proc err*[T, E](error: E): Result[T, E] =
  Result[T, E](isOk: false, error: error)

proc isOk*[T, E](self: Result[T, E]): bool = self.isOk
proc isErr*[T, E](self: Result[T, E]): bool = not self.isOk
proc error*[T, E](self: Result[T, E]): E =
  if self.isOk:
    raise newException(ValueError, "Result has no error")
  result = self.error
proc get*[T, E](self: Result[T, E]): T =
  if not self.isOk:
    raise self.error
  result = self.value

type
  RaftStateMachine* {.gcsafe.} = ref object of RootObj
    ## Abstract base for state machines that apply committed Raft entries.
    ## Concrete implementations must be thread-safe and idempotent.

  ClientCommandKind* = enum
    cckSet,   ## Set a key to a value
    cckDelete ## Delete a key

  ClientCommand* = object
    ## Decoded client command for in-memory state machine.
    op*: ClientCommandKind
    key*: string
    value*: string ## For delete, this is ignored (can be empty)

method applyImpl*(self: RaftStateMachine, entry: RaftEntry): Result[(),
    FractioError] {.base, gcsafe, raises: [].} =
  ## Apply a committed entry to the state machine.
  ##
  ## Returns `Result[(), FractioError]`. Must be thread-safe and idempotent.
  ## Default implementation returns `notImplementedError()`.
  err[(), FractioError](notImplementedError("applyImpl not implemented", "states"))

proc stringToBytes*(s: string): seq[byte] {.raises: [].} =
  ## Convert a string to a seq[byte] (UTF-8 bytes).
  result = newSeq[byte](s.len)
  if s.len > 0:
    copyMem(result[0].addr, s[0].unsafeAddr, s.len)

proc bytesToString*(data: seq[byte]): string {.raises: [].} =
  ## Convert a seq[byte] to a string (assumes valid UTF-8).
  result = newString(data.len)
  if data.len > 0:
    copyMem(result[0].addr, data[0].addr, data.len)

proc serialize*(cmd: ClientCommand): seq[byte] {.raises: [].} =
  ## Serialize a ClientCommand to a byte sequence.
  ## Format: [op:1][keyLen:2][key][valLen:2][value]
  let keyBytes = stringToBytes(cmd.key)
  let valBytes = stringToBytes(cmd.value)
  var size = 1 + 2 + keyBytes.len + 2 + valBytes.len
  result = newSeq[byte](size)
  result[0] = byte(cmd.op.uint8)
  # Write key length as big-endian uint16
  var beKeyLen = keyBytes.len.uint16
  copyMem(result[1].addr, addr beKeyLen, 2)
  # Copy key
  if keyBytes.len > 0:
    copyMem(result[1+2].addr, keyBytes[0].addr, keyBytes.len)
  # Write value length
  var beValLen = valBytes.len.uint16
  let valPos = 1 + 2 + keyBytes.len
  copyMem(result[valPos].addr, addr beValLen, 2)
  # Copy value
  if valBytes.len > 0:
    copyMem(result[valPos+2].addr, valBytes[0].addr, valBytes.len)

proc deserialize*(data: openArray[byte]): Result[ClientCommand,
    FractioError] {.raises: [].} =
  ## Deserialize a byte sequence into a ClientCommand.
  if data.len < 1:
    return err[ClientCommand, FractioError](storageError(
        "ClientCommand data too short (missing op)", "states"))
  # Convert op byte to enum safely
  var op: ClientCommandKind
  try:
    op = ClientCommandKind(data[0])
  except RangeDefect:
    return err[ClientCommand, FractioError](storageError(
        "ClientCommand invalid op code", "states"))
  var pos = 1
  if pos + 2 > data.len:
    return err[ClientCommand, FractioError](storageError(
        "ClientCommand missing keyLen", "states"))
  var keyLen: uint16
  copyMem(addr keyLen, data[pos].addr, 2)
  pos += 2
  if pos + int(keyLen) > data.len:
    return err[ClientCommand, FractioError](storageError(
        "ClientCommand key overflow", "states"))
  let key = bytesToString(data[pos..<pos+int(keyLen)])
  pos += int(keyLen)
  if pos + 2 > data.len:
    return err[ClientCommand, FractioError](storageError(
        "ClientCommand missing valLen", "states"))
  var valLen: uint16
  copyMem(addr valLen, data[pos].addr, 2)
  pos += 2
  if pos + int(valLen) > data.len:
    return err[ClientCommand, FractioError](storageError(
        "ClientCommand value overflow", "states"))
  let value = bytesToString(data[pos..<pos+int(valLen)])
  ok[ClientCommand, FractioError](ClientCommand(op: op, key: key, value: value))
