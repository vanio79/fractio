# MVCC Types - Key encoding, value format, and metadata definitions
# for Multi-Version Concurrency Control storage

import std/[options, hashes]
import ../../core/types
import ../../core/timestamp_provider

# Constants for key encoding
const
  METADATA_SUFFIX* = "\x00"
  INTENT_SUFFIX* = "\x00\x01"
  VERSION_SEPARATOR* = "\x00\x00"

  # Special timestamp values
  MAX_TIMESTAMP*: Timestamp = high(Timestamp)
  MIN_TIMESTAMP*: Timestamp = low(Timestamp)
  INTENT_TOMBSTONE*: Timestamp = -1

  # Special transaction ID
  InvalidTransactionID*: TransactionID = TransactionID(0)

type
  MVCCKey* = object
    ## Decoded MVCC key with user key and timestamp
    userKey*: string
    timestamp*: Timestamp
    isIntent*: bool

  MVCCValue* = object
    ## MVCC value with metadata
    data*: string
    timestamp*: Timestamp
    isDeleted*: bool
    txnId*: TransactionID

  MVCCKeyValue* = tuple[key: MVCCKey, value: MVCCValue]
    ## Key-value pair for MVCC

  MVCCResult* = object
    ## Result of MVCC operations
    case success*: bool
    of true:
      value*: Option[MVCCValue]
    of false:
      error*: MVCCError

  MVCCScanResult* = object
    ## Result of MVCC scan operation
    case success*: bool
    of true:
      kvs*: seq[MVCCKeyValue]
    of false:
      error*: MVCCError

  MVCCError* = object of CatchableError
    ## MVCC-specific errors
    code*: MVCCErrorCode

  MVCCErrorCode* = enum
    mvccKeyNotFound
    mvccIntentNotFound
    mvccIntentConflict
    mvccTransactionAborted
    mvccTransactionNotFound
    mvccWriteTooOld
    mvccReadWithinGap
    mvccInvalidKey
    mvccInvalidTimestamp
    mvccInvalidTransaction
    mvccStorageError
    mvccSerializationError

  MVCCTransactionStatus* = enum
    ## Transaction states for MVCC
    TXN_PENDING
    TXN_PREPARED
    TXN_COMMITTED
    TXN_ABORTED

  KeyVersion* = object
    ## A single version of a key
    key*: string
    value*: MVCCValue
    isLatest*: bool

  KeyVersions* = object
    ## All versions of a key
    userKey*: string
    versions*: seq[KeyVersion]

  Intent* = object
    ## Represents an uncommitted write intent
    key*: string
    txnId*: TransactionID
    timestamp*: Timestamp
    value*: string
    isDeleted*: bool

# Key encoding functions

proc toBigEndian64*(value: int64): array[8, uint8] =
  ## Convert int64 to big-endian bytes
  result[0] = uint8((value shr 56) and 0xFF)
  result[1] = uint8((value shr 48) and 0xFF)
  result[2] = uint8((value shr 40) and 0xFF)
  result[3] = uint8((value shr 32) and 0xFF)
  result[4] = uint8((value shr 24) and 0xFF)
  result[5] = uint8((value shr 16) and 0xFF)
  result[6] = uint8((value shr 8) and 0xFF)
  result[7] = uint8(value and 0xFF)

proc fromBigEndian64*(T: type[int64], bytes: array[8, uint8]): int64 =
  ## Convert big-endian bytes to int64
  result = int64(bytes[0]) shl 56
  result = result or int64(bytes[1]) shl 48
  result = result or int64(bytes[2]) shl 40
  result = result or int64(bytes[3]) shl 32
  result = result or int64(bytes[4]) shl 24
  result = result or int64(bytes[5]) shl 16
  result = result or int64(bytes[6]) shl 8
  result = result or int64(bytes[7])

proc encodeMVCCKey*(userKey: string, timestamp: Timestamp,
    isIntent: bool = false): string =
  ## Encode a user key with timestamp for MVCC storage
  ## Format: <user_key><VERSION_SEPARATOR><timestamp (big-endian)>
  ## For intents: <user_key><INTENT_SUFFIX><timestamp (big-endian)>
  let suffix = if isIntent: INTENT_SUFFIX else: VERSION_SEPARATOR
  var tsBytes = toBigEndian64(timestamp)
  var tsStr = ""
  for i in 0..7:
    tsStr.add(chr(int(tsBytes[i])))
  result = userKey & suffix & tsStr

proc decodeMVCCKey*(encodedKey: string): MVCCKey =
  ## Decode an MVCC key back to components
  if encodedKey.len < 9:
    raise newException(MVCCError, "Invalid MVCC key: too short")

  let userKeyEnd = encodedKey.len - 9

  # Check for intent suffix
  let suffix = encodedKey[userKeyEnd ..< userKeyEnd + 2]
  if suffix == INTENT_SUFFIX:
    result.isIntent = true
  elif suffix == VERSION_SEPARATOR:
    result.isIntent = false
  else:
    raise newException(MVCCError, "Invalid MVCC key: unknown suffix")

  result.userKey = encodedKey[0 ..< userKeyEnd]

  # Extract timestamp
  var tsArr: array[8, uint8]
  for i in 0..7:
    tsArr[i] = uint8(encodedKey[encodedKey.len - 8 + i])
  result.timestamp = fromBigEndian64(Timestamp, tsArr)

proc encodeMVCCValue*(value: string, timestamp: Timestamp,
    isDeleted: bool = false, txnId: TransactionID = InvalidTransactionID): string =
  ## Encode MVCC value with metadata
  ## Format: <timestamp (8 bytes)><txn_id (8 bytes)><is_deleted (1 byte)><value>
  var tsBytes = toBigEndian64(timestamp)
  var txnBytes = toBigEndian64(int64(txnId))
  var delByte = if isDeleted: "1" else: "0"

  # Build result string manually
  result = ""
  for i in 0..7:
    result.add(chr(int(tsBytes[i])))
  for i in 0..7:
    result.add(chr(int(txnBytes[i])))
  result.add(delByte)
  result.add(value)

proc isLikelyMVCCValue*(data: string): bool {.inline.} =
  ## Fast check if data might be MVCC-encoded.
  ## Returns false for data that is definitely NOT MVCC (JSON, too short, wrong flag).
  ## Returns true for data that MIGHT be MVCC - caller should try decodeMVCCValue.
  if data.len < 17: return false
  if data[0] == '{': return false # JSON
  let d = data[16]
  if d != '0' and d != '1': return false
  # Binary data with small uint32: byte 0 non-zero, bytes 1-3 zero
  # MVCC never has this pattern for valid timestamps
  if data[0] != '\0' and data[1] == '\0' and data[2] == '\0' and data[3] == '\0':
    return false
  true

proc decodeMVCCValueFast*(encodedValue: string): MVCCValue {.inline.} =
  ## Fast decode MVCC value - assumes caller already validated with isLikelyMVCCValue.
  ## Skips length and delete flag validation for performance.
  ## Uses direct computation instead of array extraction.
  # Direct big-endian to host conversion
  result.timestamp = Timestamp(
    (uint64(uint8(encodedValue[0])) shl 56) or
    (uint64(uint8(encodedValue[1])) shl 48) or
    (uint64(uint8(encodedValue[2])) shl 40) or
    (uint64(uint8(encodedValue[3])) shl 32) or
    (uint64(uint8(encodedValue[4])) shl 24) or
    (uint64(uint8(encodedValue[5])) shl 16) or
    (uint64(uint8(encodedValue[6])) shl 8) or
    uint64(uint8(encodedValue[7]))
  )
  result.txnId = TransactionID(
    (int64(uint8(encodedValue[8])) shl 56) or
    (int64(uint8(encodedValue[9])) shl 48) or
    (int64(uint8(encodedValue[10])) shl 40) or
    (int64(uint8(encodedValue[11])) shl 32) or
    (int64(uint8(encodedValue[12])) shl 24) or
    (int64(uint8(encodedValue[13])) shl 16) or
    (int64(uint8(encodedValue[14])) shl 8) or
    int64(uint8(encodedValue[15]))
  )
  result.isDeleted = encodedValue[16] == '1'
  result.data = encodedValue[17 ..< encodedValue.len]

proc decodeMVCCValue*(encodedValue: string): MVCCValue =
  ## Decode MVCC value from storage format.
  ## For hot paths, use isLikelyMVCCValue + decodeMVCCValueFast instead.
  if encodedValue.len < 17:
    raise newException(MVCCError, "Invalid MVCC value: too short")

  let delByte = encodedValue[16]

  # Validate delete flag - must be '0' or '1'
  if delByte != '0' and delByte != '1':
    raise newException(MVCCError, "Invalid MVCC value: invalid delete flag")

  # Use fast path for actual decoding
  result.timestamp = Timestamp(
    (uint64(uint8(encodedValue[0])) shl 56) or
    (uint64(uint8(encodedValue[1])) shl 48) or
    (uint64(uint8(encodedValue[2])) shl 40) or
    (uint64(uint8(encodedValue[3])) shl 32) or
    (uint64(uint8(encodedValue[4])) shl 24) or
    (uint64(uint8(encodedValue[5])) shl 16) or
    (uint64(uint8(encodedValue[6])) shl 8) or
    uint64(uint8(encodedValue[7]))
  )
  result.txnId = TransactionID(
    (int64(uint8(encodedValue[8])) shl 56) or
    (int64(uint8(encodedValue[9])) shl 48) or
    (int64(uint8(encodedValue[10])) shl 40) or
    (int64(uint8(encodedValue[11])) shl 32) or
    (int64(uint8(encodedValue[12])) shl 24) or
    (int64(uint8(encodedValue[13])) shl 16) or
    (int64(uint8(encodedValue[14])) shl 8) or
    int64(uint8(encodedValue[15]))
  )
  result.isDeleted = delByte == '1'
  result.data = encodedValue[17 ..< encodedValue.len]

proc encodeIntentKey*(userKey: string, txnId: TransactionID): string =
  ## Encode intent key for transaction resolution
  ## Format: <user_key><INTENT_SUFFIX><txn_id (big-endian)>
  var txnBytes = toBigEndian64(int64(txnId))
  var txnStr = ""
  for i in 0..7:
    txnStr.add(chr(int(txnBytes[i])))
  result = userKey & INTENT_SUFFIX & txnStr

proc decodeIntentKey*(encodedKey: string): tuple[userKey: string,
    txnId: TransactionID] =
  ## Decode intent key
  if encodedKey.len < 10:
    raise newException(MVCCError, "Invalid intent key: too short")

  let userKeyEnd = encodedKey.len - 10

  var txnArr: array[8, uint8]
  for i in 0..7:
    txnArr[i] = uint8(encodedKey[encodedKey.len - 8 + i])

  result.userKey = encodedKey[0 ..< userKeyEnd]
  result.txnId = TransactionID(fromBigEndian64(int64, txnArr))

proc makeMetadataKey*(userKey: string): string =
  ## Create metadata key for a user key
  ## Format: <user_key><METADATA_SUFFIX>
  result = userKey & METADATA_SUFFIX

proc makeVersionKey*(userKey: string, timestamp: Timestamp): string =
  ## Create a version key for a specific timestamp
  result = encodeMVCCKey(userKey, timestamp, false)

proc makeIntentKey*(userKey: string, txnId: TransactionID): string =
  ## Create an intent key
  result = encodeIntentKey(userKey, txnId)

# Utility functions

proc `$`*(key: MVCCKey): string =
  result = "MVCCKey(userKey: " & key.userKey & ", timestamp: " & $key.timestamp &
           ", isIntent: " & $key.isIntent & ")"

proc `$`*(value: MVCCValue): string =
  result = "MVCCValue(data: " & value.data & ", timestamp: " & $value.timestamp &
           ", isDeleted: " & $value.isDeleted & ", txnId: " & $int64(
               value.txnId) & ")"

proc `==`*(a, b: MVCCKey): bool =
  result = a.userKey == b.userKey and a.timestamp == b.timestamp and
      a.isIntent == b.isIntent

proc hash*(key: MVCCKey): Hash =
  result = hash(key.userKey) !& hash(key.timestamp) !& hash(key.isIntent)

# Error constructors

proc mvccError*(code: MVCCErrorCode, message: string,
    context: string = ""): MVCCError =
  result = MVCCError(
    msg: message,
    code: code
  )

proc keyNotFound*(key: string): MVCCError =
  mvccError(mvccKeyNotFound, "Key not found: " & key)

proc intentConflict*(key: string, txnId: TransactionID): MVCCError =
  mvccError(mvccIntentConflict, "Intent conflict for key: " & key & ", txn: " &
      $int64(txnId))

proc writeTooOld*(key: string, existingTs: Timestamp): MVCCError =
  mvccError(mvccWriteTooOld, "Write too old for key: " & key &
      ", existing ts: " & $existingTs)

# Unit tests
when isMainModule:
  import unittest

  suite "MVCC Key Encoding":
    test "encode and decode MVCC key":
      let userKey = "test_key"
      let timestamp: Timestamp = 1234567890

      let encoded = encodeMVCCKey(userKey, timestamp, false)
      let decoded = decodeMVCCKey(encoded)

      check decoded.userKey == userKey
      check decoded.timestamp == timestamp
      check decoded.isIntent == false

    test "encode and decode intent key":
      let userKey = "test_key"
      let txnId = TransactionID(12345)

      let encoded = makeIntentKey(userKey, txnId)
      let decoded = decodeIntentKey(encoded)

      check decoded.userKey == userKey
      check decoded.txnId == txnId

    test "encode and decode MVCC value":
      let value = "test_value"
      let timestamp: Timestamp = 9876543210
      let txnId = TransactionID(999)

      let encoded = encodeMVCCValue(value, timestamp, false, txnId)
      let decoded = decodeMVCCValue(encoded)

      check decoded.data == value
      check decoded.timestamp == timestamp
      check decoded.txnId == txnId
      check decoded.isDeleted == false

    test "metadata key":
      let userKey = "my_key"
      let metaKey = makeMetadataKey(userKey)

      check metaKey == "my_key\x00"
