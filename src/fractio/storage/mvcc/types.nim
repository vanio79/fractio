# MVCC Types - Key encoding, value format, and metadata definitions
# for Multi-Version Concurrency Control storage

import std/[options, hashes, strutils, locks, deques, atomics, typedthreads]
import ../../core/types
import ../../core/timestamp_provider
import ../../storage/backend

# Constants for key encoding
const
  METADATA_SUFFIX* = "\x00"
  INTENT_SUFFIX* = "\x00\x01"
  VERSION_SEPARATOR* = "\x00\x00"

  # Special timestamp values
  MAX_TIMESTAMP*: Timestamp = high(Timestamp)
  MIN_TIMESTAMP*: Timestamp = low(Timestamp)
  INTENT_TOMBSTONE*: Timestamp = -1

  # Special transaction ID (zero ULID)
  InvalidTransactionID* = zeroTransactionID()

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
  ## Handles both version keys and intent keys:
  ## - Version keys: userKey + VERSION_SEPARATOR(2) + timestamp(8) = userKeyLen + 10
  ## - Intent keys: userKey + INTENT_SUFFIX(2) + txnId(16) = userKeyLen + 18

  if encodedKey.len < 10:
    raise newException(MVCCError, "Invalid MVCC key: too short")

  # Check for intent key first (18 bytes suffix: 2 + 16)
  if encodedKey.len >= 18:
    let intentUserKeyEnd = encodedKey.len - 18
    let intentSuffix = encodedKey[intentUserKeyEnd ..< intentUserKeyEnd + 2]
    if intentSuffix == INTENT_SUFFIX:
      result.isIntent = true
      result.userKey = encodedKey[0 ..< intentUserKeyEnd]
      # Intent keys don't have a timestamp in the same position
      # They have a txnId instead, which we don't decode here
      result.timestamp = 0
      return

  # Must be a version key (10 bytes suffix: 2 + 8)
  let userKeyEnd = encodedKey.len - 10
  let suffix = encodedKey[userKeyEnd ..< userKeyEnd + 2]
  if suffix != VERSION_SEPARATOR:
    raise newException(MVCCError, "Invalid MVCC key: unknown suffix")

  result.isIntent = false
  result.userKey = encodedKey[0 ..< userKeyEnd]

  # Extract timestamp
  var tsArr: array[8, uint8]
  for i in 0..7:
    tsArr[i] = uint8(encodedKey[encodedKey.len - 8 + i])
  result.timestamp = fromBigEndian64(Timestamp, tsArr)

const
  MVCC_MAGIC* = "MVCC"
  MVCC_HEADER_SIZE* = 29 # 4 (magic) + 8 (ts) + 16 (txn ULID) + 1 (del)

proc encodeMVCCValue*(value: string, timestamp: Timestamp,
    isDeleted: bool = false, txnId: TransactionID = InvalidTransactionID): string =
  ## Encode MVCC value with metadata
  ## Format: <MAGIC (4 bytes)><timestamp (8 bytes)><txn_id ULID (16 bytes)><is_deleted (1 byte)><value>
  var tsBytes = toBigEndian64(timestamp)
  var txnBytes = ulidToBytes(ULID(txnId))
  var delByte = if isDeleted: "1" else: "0"

  # Build result string manually
  result = MVCC_MAGIC
  for i in 0..7:
    result.add(chr(int(tsBytes[i])))
  result.add(txnBytes)
  result.add(delByte)
  result.add(value)

proc isLikelyMVCCValue*(data: string): bool {.inline.} =
  ## Fast check if data starts with MVCC magic
  data.startsWith(MVCC_MAGIC)

proc decodeMVCCValueFast*(encodedValue: string): MVCCValue {.inline.} =
  ## Fast decode MVCC value - assumes caller already validated with isLikelyMVCCValue.
  if encodedValue.len < MVCC_HEADER_SIZE:
    return MVCCValue(timestamp: 0, txnId: InvalidTransactionID,
        isDeleted: false, data: encodedValue)

  # Direct big-endian to host conversion for timestamp, offset by magic length
  result.timestamp = Timestamp(
    (uint64(uint8(encodedValue[4])) shl 56) or
    (uint64(uint8(encodedValue[5])) shl 48) or
    (uint64(uint8(encodedValue[6])) shl 40) or
    (uint64(uint8(encodedValue[7])) shl 32) or
    (uint64(uint8(encodedValue[8])) shl 24) or
    (uint64(uint8(encodedValue[9])) shl 16) or
    (uint64(uint8(encodedValue[10])) shl 8) or
    uint64(uint8(encodedValue[11]))
  )
  # Extract 16-byte ULID for txnId (bytes 12-27)
  var txnUlidBytes = encodedValue[12..27]
  result.txnId = TransactionID(ulidFromBytes(txnUlidBytes))
  result.isDeleted = encodedValue[28] == '1'
  result.data = encodedValue[MVCC_HEADER_SIZE ..< encodedValue.len]

proc decodeMVCCValue*(encodedValue: string): MVCCValue =
  ## Decode MVCC value from storage format.
  if not isLikelyMVCCValue(encodedValue):
    raise newException(MVCCError, "Invalid MVCC value: missing magic")
  if encodedValue.len < MVCC_HEADER_SIZE:
    raise newException(MVCCError, "Invalid MVCC value: too short")

  # Delete flag is at position 28: 4 (magic) + 8 (timestamp) + 16 (txn ULID)
  let delByte = encodedValue[28]

  # Validate delete flag - must be '0' or '1'
  if delByte != '0' and delByte != '1':
    raise newException(MVCCError, "Invalid MVCC value: invalid delete flag")

  # Use fast path for actual decoding
  result = decodeMVCCValueFast(encodedValue)

proc encodeIntentKey*(userKey: string, txnId: TransactionID): string =
  ## Encode intent key for transaction resolution
  ## Format: <user_key><INTENT_SUFFIX><txn_id ULID (16 bytes)>
  result = userKey & INTENT_SUFFIX & ulidToBytes(ULID(txnId))

proc decodeIntentKey*(encodedKey: string): tuple[userKey: string,
    txnId: TransactionID] =
  ## Decode intent key
  ## Format: <user_key><INTENT_SUFFIX><txn_id ULID (16 bytes)>
  # INTENT_SUFFIX is 2 bytes, ULID is 16 bytes = 18 bytes at end
  if encodedKey.len < 18:
    raise newException(MVCCError, "Invalid intent key: too short")

  let userKeyEnd = encodedKey.len - 18

  # Verify suffix
  let suffix = encodedKey[userKeyEnd ..< userKeyEnd + 2]
  if suffix != INTENT_SUFFIX:
    raise newException(MVCCError, "Invalid intent key: missing INTENT_SUFFIX")

  result.userKey = encodedKey[0 ..< userKeyEnd]
  # Extract 16-byte ULID
  var txnUlidBytes = encodedKey[userKeyEnd + 2 ..< encodedKey.len]
  result.txnId = TransactionID(ulidFromBytes(txnUlidBytes))

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
           ", isDeleted: " & $value.isDeleted & ", txnId: " & $value.txnId & ")"

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
      $txnId)

proc writeTooOld*(key: string, existingTs: Timestamp): MVCCError =
  mvccError(mvccWriteTooOld, "Write too old for key: " & key &
      ", existing ts: " & $existingTs)

# ============================================================================
# MVCC Streaming ResultSet Types
# ============================================================================

type
  MVCCStreamConfig* = object
    ## Configuration for MVCC streaming result sets
    bufferSize*: int ## Number of MVCCKeyValue pairs to buffer (default: 1000)
    prefetchThreshold*: int ## Items remaining before triggering prefetch (default: 100)

  MVCCStreamState* = enum
    mssIdle      ## Stream not started
    mssReading   ## Stream actively reading (prefetch thread running)
    mssExhausted ## Stream has read all data
    mssError     ## Stream encountered error
    mssClosed    ## Stream explicitly closed

  MVCCStreamError* = object of CatchableError
    ## Error during MVCC streaming operation
    code*: MVCCStreamErrorCode

  MVCCStreamErrorCode* = enum
    msecStreamClosed
    msecStreamExhausted
    msecPrefetchError
    msecInvalidState
    msecIntentConflict

  MVCCStreamSharedData* = object
    ## Thread-safe shared data between consumer and prefetch thread
    buffer*: Deque[MVCCKeyValue]
    bufferLock*: Lock
    state*: Atomic[MVCCStreamState]
    errorMsg*: Atomic[string]
    totalRead*: Atomic[int]
    consumerPos*: Atomic[int]

  MVCCStreamResultSet* = ref object
    ## Streaming result set for MVCC range scans.
    ## Uses a background thread to read ahead and buffer results.
    ## Thread-safe: consumers can call next() while prefetch thread fills buffer.
    engine*: pointer ## Cast to MVCCEngine (avoid circular import)
    startKey*: string
    endKey*: string
    timestamp*: Timestamp
    txnId*: TransactionID
    config*: MVCCStreamConfig
    sharedData*: ptr MVCCStreamSharedData
    prefetchThread*: Thread[MVCCStreamResultSet]

const
  DEFAULT_MVCC_STREAM_BUFFER_SIZE* = 1000
  DEFAULT_MVCC_PREFETCH_THRESHOLD* = 100

proc defaultMVCCStreamConfig*(): MVCCStreamConfig =
  result = MVCCStreamConfig(
    bufferSize: DEFAULT_MVCC_STREAM_BUFFER_SIZE,
    prefetchThreshold: DEFAULT_MVCC_PREFETCH_THRESHOLD
  )

proc smallMVCCStreamConfig*(): MVCCStreamConfig =
  result = MVCCStreamConfig(
    bufferSize: 100,
    prefetchThreshold: 20
  )

proc newMVCCStreamError*(code: MVCCStreamErrorCode,
    message: string): MVCCStreamError =
  result = MVCCStreamError(code: code, msg: message)

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

    test "encode and decode intent key with ULID":
      let userKey = "test_key"
      let txnId = genTransactionID()

      let encoded = makeIntentKey(userKey, txnId)
      let decoded = decodeIntentKey(encoded)

      check decoded.userKey == userKey
      check decoded.txnId == txnId

    test "encode and decode MVCC value with ULID":
      let value = "test_value"
      let timestamp: Timestamp = 9876543210
      let txnId = genTransactionID()

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
