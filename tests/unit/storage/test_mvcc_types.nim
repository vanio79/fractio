# Unit tests for MVCC Types
# Comprehensive tests for MVCC key encoding, value encoding, and type operations

import unittest
import std/[options, hashes, strutils, sequtils, algorithm, tables]
import std/[threadpool, typedthreads, atomics, locks]
import fractio/core/types
import fractio/core/timestamp_provider
import fractio/storage/mvcc/types as mvccTypes

suite "MVCC Types - Constants":
  test "METADATA_SUFFIX is defined":
    check mvccTypes.METADATA_SUFFIX.len == 1
    check mvccTypes.METADATA_SUFFIX[0] == '\x00'

  test "INTENT_SUFFIX is defined":
    check mvccTypes.INTENT_SUFFIX.len == 2
    check mvccTypes.INTENT_SUFFIX[0] == '\x00'
    check mvccTypes.INTENT_SUFFIX[1] == '\x01'

  test "VERSION_SEPARATOR is defined":
    check mvccTypes.VERSION_SEPARATOR.len == 2
    check mvccTypes.VERSION_SEPARATOR[0] == '\x00'
    check mvccTypes.VERSION_SEPARATOR[1] == '\x00'

  test "MAX_TIMESTAMP is high int64":
    check mvccTypes.MAX_TIMESTAMP == high(Timestamp)

  test "MIN_TIMESTAMP is low int64":
    check mvccTypes.MIN_TIMESTAMP == low(Timestamp)

  test "INTENT_TOMBSTONE is -1":
    check mvccTypes.INTENT_TOMBSTONE == -1

  test "InvalidTransactionID is zero ULID":
    check isZero(mvccTypes.InvalidTransactionID) == true

  test "MVCC_MAGIC constant":
    check mvccTypes.MVCC_MAGIC == "MVCC"
    check mvccTypes.MVCC_MAGIC.len == 4

  test "MVCC_HEADER_SIZE is correct":
    check mvccTypes.MVCC_HEADER_SIZE == 29
    check mvccTypes.MVCC_HEADER_SIZE == 4 + 8 + 16 + 1

suite "MVCC Types - MVCCKey":
  test "create MVCCKey with all fields":
    let key = MVCCKey(
      userKey: "test_key",
      timestamp: Timestamp(1000),
      isIntent: false
    )
    check key.userKey == "test_key"
    check key.timestamp == Timestamp(1000)
    check key.isIntent == false

  test "create intent MVCCKey":
    let key = MVCCKey(
      userKey: "intent_key",
      timestamp: Timestamp(500),
      isIntent: true
    )
    check key.isIntent == true

  test "MVCCKey equality":
    let key1 = MVCCKey(userKey: "key", timestamp: 100, isIntent: false)
    let key2 = MVCCKey(userKey: "key", timestamp: 100, isIntent: false)
    let key3 = MVCCKey(userKey: "key", timestamp: 100, isIntent: true)
    let key4 = MVCCKey(userKey: "key", timestamp: 200, isIntent: false)
    let key5 = MVCCKey(userKey: "other", timestamp: 100, isIntent: false)

    check key1 == key2
    check key1 != key3
    check key1 != key4
    check key1 != key5

  test "MVCCKey hash":
    let key1 = MVCCKey(userKey: "key", timestamp: 100, isIntent: false)
    let key2 = MVCCKey(userKey: "key", timestamp: 100, isIntent: false)
    let key3 = MVCCKey(userKey: "key", timestamp: 200, isIntent: false)

    check hash(key1) == hash(key2)
    check hash(key1) != hash(key3)

  test "MVCCKey string representation":
    let key = MVCCKey(userKey: "test", timestamp: 12345, isIntent: true)
    let strRep = $key
    check "test" in strRep
    check "12345" in strRep
    check "true" in strRep

  test "MVCCKey with empty userKey":
    let key = MVCCKey(userKey: "", timestamp: 0, isIntent: false)
    check key.userKey == ""

  test "MVCCKey with large timestamp":
    let key = MVCCKey(userKey: "key", timestamp: mvccTypes.MAX_TIMESTAMP,
        isIntent: false)
    check key.timestamp == mvccTypes.MAX_TIMESTAMP

suite "MVCC Types - MVCCValue":
  test "create MVCCValue with all fields":
    let txnId = genTransactionIDLocal()
    let value = MVCCValue(
      data: "test_data",
      timestamp: Timestamp(1500),
      isDeleted: false,
      txnId: txnId
    )
    check value.data == "test_data"
    check value.timestamp == Timestamp(1500)
    check value.isDeleted == false
    check value.txnId == txnId

  test "create deleted MVCCValue":
    let value = MVCCValue(
      data: "",
      timestamp: Timestamp(1000),
      isDeleted: true,
      txnId: InvalidTransactionID
    )
    check value.isDeleted == true
    check value.data == ""

  test "MVCCValue string representation":
    let txnId = genTransactionIDLocal()
    let value = MVCCValue(data: "data", timestamp: 999, isDeleted: true, txnId: txnId)
    let strRep = $value
    check "data" in strRep
    check "999" in strRep
    check "true" in strRep
    check $txnId in strRep

  test "MVCCValue with empty data":
    let value = MVCCValue(data: "", timestamp: 100, isDeleted: false,
        txnId: InvalidTransactionID)
    check value.data == ""

  test "MVCCValue with large data":
    let largeData = "x".repeat(10000)
    let value = MVCCValue(data: largeData, timestamp: 100, isDeleted: false,
        txnId: InvalidTransactionID)
    check value.data.len == 10000

suite "MVCC Types - MVCCKeyValue":
  test "create MVCCKeyValue tuple":
    let key = MVCCKey(userKey: "key", timestamp: 100, isIntent: false)
    let value = MVCCValue(data: "value", timestamp: 100, isDeleted: false,
        txnId: InvalidTransactionID)
    let kv: MVCCKeyValue = (key: key, value: value)
    check kv.key.userKey == "key"
    check kv.value.data == "value"

  test "MVCCKeyValue sequence":
    let kvs: seq[MVCCKeyValue] = @[
      (key: MVCCKey(userKey: "k1", timestamp: 100, isIntent: false),
       value: MVCCValue(data: "v1", timestamp: 100, isDeleted: false,
           txnId: InvalidTransactionID)),
      (key: MVCCKey(userKey: "k2", timestamp: 200, isIntent: false),
       value: MVCCValue(data: "v2", timestamp: 200, isDeleted: false,
           txnId: InvalidTransactionID))
    ]
    check kvs.len == 2
    check kvs[0].key.userKey == "k1"
    check kvs[1].key.userKey == "k2"

suite "MVCC Types - MVCCResult":
  test "create success MVCCResult":
    let value = MVCCValue(data: "test", timestamp: 100, isDeleted: false,
        txnId: InvalidTransactionID)
    let result = MVCCResult(success: true, value: some(value))
    check result.success == true
    check result.value.isSome
    check result.value.get().data == "test"

  test "create success MVCCResult with none":
    let result = MVCCResult(success: true, value: none(MVCCValue))
    check result.success == true
    check result.value.isNone

  test "create error MVCCResult":
    let err = mvccError(mvccKeyNotFound, "Key not found")
    let result = MVCCResult(success: false, error: err)
    check result.success == false
    check result.error.code == mvccKeyNotFound

  test "MVCCResult case discrimination":
    var successResult = MVCCResult(success: true, value: none(MVCCValue))
    check successResult.success == true

    var errorResult = MVCCResult(success: false, error: mvccError(
        mvccStorageError, "Error"))
    check errorResult.success == false

suite "MVCC Types - MVCCScanResult":
  test "create success MVCCScanResult":
    let kvs: seq[MVCCKeyValue] = @[
      (key: MVCCKey(userKey: "k1", timestamp: 100, isIntent: false),
       value: MVCCValue(data: "v1", timestamp: 100, isDeleted: false,
           txnId: InvalidTransactionID))
    ]
    let result = MVCCScanResult(success: true, kvs: kvs)
    check result.success == true
    check result.kvs.len == 1

  test "create empty success MVCCScanResult":
    let result = MVCCScanResult(success: true, kvs: @[])
    check result.success == true
    check result.kvs.len == 0

  test "create error MVCCScanResult":
    let err = mvccError(mvccInvalidKey, "Invalid key")
    let result = MVCCScanResult(success: false, error: err)
    check result.success == false
    check result.error.code == mvccInvalidKey

suite "MVCC Types - MVCCError":
  test "create MVCCError with code":
    let err = mvccError(mvccKeyNotFound, "Key not found: test")
    check err.code == mvccKeyNotFound
    check "Key not found" in err.msg

  test "MVCCError codes enum values":
    check mvccKeyNotFound.ord == 0
    check mvccIntentNotFound.ord == 1
    check mvccIntentConflict.ord == 2
    check mvccTransactionAborted.ord == 3
    check mvccTransactionNotFound.ord == 4
    check mvccWriteTooOld.ord == 5
    check mvccReadWithinGap.ord == 6
    check mvccInvalidKey.ord == 7
    check mvccInvalidTimestamp.ord == 8
    check mvccInvalidTransaction.ord == 9
    check mvccStorageError.ord == 10
    check mvccSerializationError.ord == 11

  test "MVCCError can be created via newException":
    var caught = false
    try:
      raise newException(MVCCError, "Test error with code")
    except MVCCError as e:
      caught = true
      check "Test error" in e.msg
    check caught == true

suite "MVCC Types - MVCCTransactionStatus":
  test "MVCCTransactionStatus enum values":
    check TXN_PENDING.ord == 0
    check TXN_PREPARED.ord == 1
    check TXN_COMMITTED.ord == 2
    check TXN_ABORTED.ord == 3

  test "MVCCTransactionStatus ordering":
    check TXN_PENDING < TXN_PREPARED
    check TXN_PREPARED < TXN_COMMITTED
    check TXN_COMMITTED < TXN_ABORTED

suite "MVCC Types - KeyVersion":
  test "create KeyVersion":
    let value = MVCCValue(data: "test", timestamp: 100, isDeleted: false,
        txnId: InvalidTransactionID)
    let version = KeyVersion(key: "test_key", value: value, isLatest: true)
    check version.key == "test_key"
    check version.value.data == "test"
    check version.isLatest == true

  test "KeyVersion with isLatest false":
    let version = KeyVersion(
      key: "key",
      value: MVCCValue(data: "data", timestamp: 100, isDeleted: false,
          txnId: InvalidTransactionID),
      isLatest: false
    )
    check version.isLatest == false

suite "MVCC Types - KeyVersions":
  test "create KeyVersions":
    let versions: seq[KeyVersion] = @[
      KeyVersion(key: "k", value: MVCCValue(data: "v1", timestamp: 100,
          isDeleted: false, txnId: InvalidTransactionID), isLatest: false),
      KeyVersion(key: "k", value: MVCCValue(data: "v2", timestamp: 200,
          isDeleted: false, txnId: InvalidTransactionID), isLatest: true)
    ]
    let kv = KeyVersions(userKey: "user_key", versions: versions)
    check kv.userKey == "user_key"
    check kv.versions.len == 2

  test "KeyVersions empty":
    let kv = KeyVersions(userKey: "key", versions: @[])
    check kv.versions.len == 0

suite "MVCC Types - Intent":
  test "create Intent":
    let txnId = genTransactionIDLocal()
    let intent = Intent(
      key: "intent_key",
      txnId: txnId,
      timestamp: Timestamp(1000),
      value: "intent_value",
      isDeleted: false
    )
    check intent.key == "intent_key"
    check intent.txnId == txnId
    check intent.timestamp == Timestamp(1000)
    check intent.value == "intent_value"
    check intent.isDeleted == false

  test "Intent with delete flag":
    let intent = Intent(
      key: "key",
      txnId: genTransactionIDLocal(),
      timestamp: 100,
      value: "",
      isDeleted: true
    )
    check intent.isDeleted == true

suite "MVCC Key Encoding - toBigEndian64":
  test "toBigEndian64 with zero":
    let bytes = toBigEndian64(0'i64)
    for b in bytes:
      check b == 0'u8

  test "toBigEndian64 with 1":
    let bytes = toBigEndian64(1'i64)
    check bytes[7] == 1'u8
    for i in 0..6:
      check bytes[i] == 0'u8

  test "toBigEndian64 with 256":
    let bytes = toBigEndian64(256'i64)
    check bytes[6] == 1'u8
    check bytes[7] == 0'u8

  test "toBigEndian64 with max value":
    let bytes = toBigEndian64(high(int64))
    check bytes[0] == 0x7F'u8
    for i in 1..7:
      check bytes[i] == 0xFF'u8

  test "toBigEndian64 with negative value":
    let bytes = toBigEndian64(-1'i64)
    for b in bytes:
      check b == 0xFF'u8

  test "toBigEndian64 preserves byte order":
    let value = 0x0102030405060708'i64
    let bytes = toBigEndian64(value)
    check bytes[0] == 0x01'u8
    check bytes[1] == 0x02'u8
    check bytes[2] == 0x03'u8
    check bytes[3] == 0x04'u8
    check bytes[4] == 0x05'u8
    check bytes[5] == 0x06'u8
    check bytes[6] == 0x07'u8
    check bytes[7] == 0x08'u8

suite "MVCC Key Encoding - fromBigEndian64":
  test "fromBigEndian64 with zero":
    let bytes: array[8, uint8] = [0'u8, 0, 0, 0, 0, 0, 0, 0]
    let value = fromBigEndian64(Timestamp, bytes)
    check value == 0'i64

  test "fromBigEndian64 with 1":
    let bytes: array[8, uint8] = [0'u8, 0, 0, 0, 0, 0, 0, 1]
    let value = fromBigEndian64(Timestamp, bytes)
    check value == 1'i64

  test "fromBigEndian64 roundtrip":
    let original = 12345678901234567'i64
    let bytes = toBigEndian64(original)
    let recovered = fromBigEndian64(Timestamp, bytes)
    check recovered == original

  test "fromBigEndian64 multiple values":
    for val in [0'i64, 1, 255, 256, 65535, 65536, 0x7FFFFFFF, high(int64)]:
      let bytes = toBigEndian64(val)
      let recovered = fromBigEndian64(Timestamp, bytes)
      check recovered == val

suite "MVCC Key Encoding - encodeMVCCKey":
  test "encode version key":
    let encoded = encodeMVCCKey("user_key", Timestamp(1000), false)
    check encoded.startsWith("user_key")
    check encoded.len == "user_key".len + 10

  test "encode intent key":
    let encoded = encodeMVCCKey("user_key", Timestamp(500), true)
    check encoded.startsWith("user_key")
    check encoded.len == "user_key".len + 10

  test "encode key preserves user key":
    let userKey = "test_user_key"
    let encoded = encodeMVCCKey(userKey, Timestamp(12345), false)
    check encoded[0..userKey.len-1] == userKey

  test "encode key with empty user key":
    let encoded = encodeMVCCKey("", Timestamp(100), false)
    check encoded.len == 10

  test "encode key with special characters":
    let userKey = "key\x00\x01with\xFFspecial"
    let encoded = encodeMVCCKey(userKey, Timestamp(1000), false)
    check encoded.startsWith(userKey)

  test "encode key with unicode":
    let userKey = "日本語キー"
    let encoded = encodeMVCCKey(userKey, Timestamp(1000), false)
    check encoded.startsWith(userKey)

  test "encode key with large timestamp":
    let encoded = encodeMVCCKey("key", mvccTypes.MAX_TIMESTAMP, false)
    let decoded = decodeMVCCKey(encoded)
    check decoded.timestamp == mvccTypes.MAX_TIMESTAMP

suite "MVCC Key Encoding - decodeMVCCKey":
  test "decode version key":
    let encoded = encodeMVCCKey("user_key", Timestamp(1000), false)
    let decoded = decodeMVCCKey(encoded)
    check decoded.userKey == "user_key"
    check decoded.timestamp == Timestamp(1000)
    check decoded.isIntent == false

  test "decode intent key":
    let txnId = genTransactionIDLocal()
    let intentKey = makeIntentKey("user_key", txnId)
    let decoded = decodeMVCCKey(intentKey)
    check decoded.userKey == "user_key"
    check decoded.isIntent == true
    check decoded.timestamp == 0

  test "decode fails with too short key":
    var caught = false
    try:
      discard decodeMVCCKey("short")
    except MVCCError as e:
      caught = true
      check e.code == mvccKeyNotFound
    check caught == true

  test "decode fails with invalid suffix":
    var caught = false
    try:
      let invalid = "user_key\x00\x02" & "\x00\x00\x00\x00\x00\x00\x00\x00"
      discard decodeMVCCKey(invalid)
    except MVCCError as e:
      caught = true
      check e.code == mvccKeyNotFound
    check caught == true

  test "decode roundtrip":
    for ts in [0'i64, 100, 1000, 1000000, mvccTypes.MAX_TIMESTAMP]:
      let encoded = encodeMVCCKey("test_key", Timestamp(ts), false)
      let decoded = decodeMVCCKey(encoded)
      check decoded.userKey == "test_key"
      check decoded.timestamp == Timestamp(ts)
      check decoded.isIntent == false

suite "MVCC Value Encoding - encodeMVCCValue":
  test "encode basic value":
    let encoded = encodeMVCCValue("test_value", Timestamp(1000), false, InvalidTransactionID)
    check encoded.startsWith(mvccTypes.MVCC_MAGIC)
    check encoded.len == mvccTypes.MVCC_HEADER_SIZE + "test_value".len

  test "encode deleted value":
    let encoded = encodeMVCCValue("value", Timestamp(500), true, InvalidTransactionID)
    check encoded.startsWith(mvccTypes.MVCC_MAGIC)
    let decoded = decodeMVCCValue(encoded)
    check decoded.isDeleted == true

  test "encode value with transaction ID":
    let txnId = genTransactionIDLocal()
    let encoded = encodeMVCCValue("data", Timestamp(1000), false, txnId)
    let decoded = decodeMVCCValue(encoded)
    check decoded.txnId == txnId

  test "encode empty value":
    let encoded = encodeMVCCValue("", Timestamp(100), false, InvalidTransactionID)
    check encoded.len == mvccTypes.MVCC_HEADER_SIZE

  test "encode large value":
    let largeValue = "x".repeat(100000)
    let encoded = encodeMVCCValue(largeValue, Timestamp(1000), false, InvalidTransactionID)
    check encoded.len == mvccTypes.MVCC_HEADER_SIZE + 100000

  test "encode preserves timestamp":
    let encoded = encodeMVCCValue("val", Timestamp(9876543210), false, InvalidTransactionID)
    let decoded = decodeMVCCValue(encoded)
    check decoded.timestamp == Timestamp(9876543210)

suite "MVCC Value Encoding - decodeMVCCValue":
  test "decode basic value":
    let encoded = encodeMVCCValue("test_data", Timestamp(1500), false, InvalidTransactionID)
    let decoded = decodeMVCCValue(encoded)
    check decoded.data == "test_data"
    check decoded.timestamp == Timestamp(1500)
    check decoded.isDeleted == false
    check decoded.txnId == InvalidTransactionID

  test "decode fails without magic":
    var caught = false
    try:
      discard decodeMVCCValue("no_magic_here")
    except MVCCError:
      caught = true
    check caught == true

  test "decode fails with too short value":
    var caught = false
    try:
      discard decodeMVCCValue("MVCC")
    except MVCCError:
      caught = true
    check caught == true

  test "decode fails with invalid delete flag":
    var caught = false
    try:
      let invalid = "MVCC" & "\x00\x00\x00\x00\x00\x00\x00\x00" &
          "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" & "X"
      discard decodeMVCCValue(invalid)
    except MVCCError:
      caught = true
    check caught == true

  test "decode roundtrip":
    let txnId = genTransactionIDLocal()
    for data in ["", "a", "test", "日本語", "x".repeat(1000)]:
      for ts in [0'i64, 100, 1000, high(int64)]:
        for deleted in [false, true]:
          let encoded = encodeMVCCValue(data, Timestamp(ts), deleted, txnId)
          let decoded = decodeMVCCValue(encoded)
          check decoded.data == data
          check decoded.timestamp == Timestamp(ts)
          check decoded.isDeleted == deleted

suite "MVCC Value Encoding - isLikelyMVCCValue":
  test "isLikelyMVCCValue returns true for valid":
    let encoded = encodeMVCCValue("data", Timestamp(100), false, InvalidTransactionID)
    check isLikelyMVCCValue(encoded) == true

  test "isLikelyMVCCValue returns false for invalid":
    check isLikelyMVCCValue("no_magic") == false
    check isLikelyMVCCValue("") == false
    check isLikelyMVCCValue("MVC") == false

  test "isLikelyMVCCValue returns true for string starting with MVCC":
    check isLikelyMVCCValue("MVCCanything") == true

  test "isLikelyMVCCValue empty string":
    check isLikelyMVCCValue("") == false

suite "MVCC Value Encoding - decodeMVCCValueFast":
  test "decodeMVCCValueFast skips validation":
    let encoded = encodeMVCCValue("fast_data", Timestamp(2000), false, InvalidTransactionID)
    let decoded = decodeMVCCValueFast(encoded)
    check decoded.data == "fast_data"
    check decoded.timestamp == Timestamp(2000)

  test "decodeMVCCValueFast handles short input":
    let decoded = decodeMVCCValueFast("short")
    check decoded.timestamp == 0

  test "decodeMVCCValueFast handles empty input":
    let decoded = decodeMVCCValueFast("")
    check decoded.data == ""

  test "decodeMVCCValueFast extracts all fields":
    let txnId = genTransactionIDLocal()
    let encoded = encodeMVCCValue("data", Timestamp(1234567890), true, txnId)
    let decoded = decodeMVCCValueFast(encoded)
    check decoded.data == "data"
    check decoded.timestamp == Timestamp(1234567890)
    check decoded.isDeleted == true
    check decoded.txnId == txnId

suite "Intent Key Encoding - encodeIntentKey":
  test "encode intent key":
    let txnId = genTransactionIDLocal()
    let encoded = encodeIntentKey("user_key", txnId)
    check encoded.startsWith("user_key")
    check encoded.len == "user_key".len + 18

  test "encode intent key preserves user key":
    let txnId = genTransactionIDLocal()
    let userKey = "test_intent_key"
    let encoded = encodeIntentKey(userKey, txnId)
    check encoded[0..userKey.len-1] == userKey

  test "encode intent key with empty user key":
    let txnId = genTransactionIDLocal()
    let encoded = encodeIntentKey("", txnId)
    check encoded.len == 18

  test "encode intent key with special characters":
    let txnId = genTransactionIDLocal()
    let userKey = "key\x00with\xFFspecial"
    let encoded = encodeIntentKey(userKey, txnId)
    check encoded.startsWith(userKey)

suite "Intent Key Encoding - decodeIntentKey":
  test "decode intent key":
    let txnId = genTransactionIDLocal()
    let encoded = encodeIntentKey("user_key", txnId)
    let decoded = decodeIntentKey(encoded)
    check decoded.userKey == "user_key"
    check decoded.txnId == txnId

  test "decode fails with too short key":
    var caught = false
    try:
      discard decodeIntentKey("short_key")
    except MVCCError:
      caught = true
    check caught == true

  test "decode fails without intent suffix":
    var caught = false
    try:
      let invalid = "user_key\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
      discard decodeIntentKey(invalid)
    except MVCCError:
      caught = true
    check caught == true

  test "decode intent key roundtrip":
    for txnId in [genTransactionIDLocal(), genTransactionIDLocal(), genTransactionIDLocal()]:
      for userKey in ["key", "test_key", "日本語", ""]:
        let encoded = encodeIntentKey(userKey, txnId)
        let decoded = decodeIntentKey(encoded)
        check decoded.userKey == userKey
        check decoded.txnId == txnId

suite "MVCC Helper Functions - makeMetadataKey":
  test "makeMetadataKey adds suffix":
    let metaKey = makeMetadataKey("user_key")
    check metaKey == "user_key\x00"
    check metaKey.len == "user_key".len + 1

  test "makeMetadataKey with empty key":
    let metaKey = makeMetadataKey("")
    check metaKey == "\x00"

  test "makeMetadataKey with special characters":
    let metaKey = makeMetadataKey("key\x01\x02")
    check metaKey.startsWith("key\x01\x02")

suite "MVCC Helper Functions - makeVersionKey":
  test "makeVersionKey creates version key":
    let versionKey = makeVersionKey("user_key", Timestamp(1000))
    check versionKey.startsWith("user_key")

  test "makeVersionKey is same as encodeMVCCKey":
    let key1 = makeVersionKey("test", Timestamp(500))
    let key2 = encodeMVCCKey("test", Timestamp(500), false)
    check key1 == key2

  test "makeVersionKey with zero timestamp":
    let versionKey = makeVersionKey("key", Timestamp(0))
    check versionKey.len == "key".len + 10

suite "MVCC Helper Functions - makeIntentKey":
  test "makeIntentKey creates intent key":
    let txnId = genTransactionIDLocal()
    let intentKey = makeIntentKey("user_key", txnId)
    check intentKey.startsWith("user_key")

  test "makeIntentKey is same as encodeIntentKey":
    let txnId = genTransactionIDLocal()
    let key1 = makeIntentKey("test", txnId)
    let key2 = encodeIntentKey("test", txnId)
    check key1 == key2

suite "MVCC Error Constructors - mvccError":
  test "mvccError creates error with code":
    let err = mvccError(mvccStorageError, "Storage failed", "context")
    check err.code == mvccStorageError
    check "Storage failed" in err.msg

  test "mvccError with empty context":
    let err = mvccError(mvccKeyNotFound, "Not found")
    check err.code == mvccKeyNotFound

suite "MVCC Error Constructors - keyNotFound":
  test "keyNotFound creates correct error":
    let err = keyNotFound("test_key")
    check err.code == mvccKeyNotFound
    check "test_key" in err.msg

  test "keyNotFound with empty key":
    let err = keyNotFound("")
    check err.code == mvccKeyNotFound

suite "MVCC Error Constructors - intentConflict":
  test "intentConflict creates correct error":
    let txnId = genTransactionIDLocal()
    let err = intentConflict("conflict_key", txnId)
    check err.code == mvccIntentConflict
    check "conflict_key" in err.msg
    check $txnId in err.msg

  test "intentConflict with empty key":
    let txnId = genTransactionIDLocal()
    let err = intentConflict("", txnId)
    check err.code == mvccIntentConflict

suite "MVCC Error Constructors - writeTooOld":
  test "writeTooOld creates correct error":
    let err = writeTooOld("old_key", Timestamp(1000))
    check err.code == mvccWriteTooOld
    check "old_key" in err.msg
    check "1000" in err.msg

  test "writeTooOld with max timestamp":
    let err = writeTooOld("key", mvccTypes.MAX_TIMESTAMP)
    check err.code == mvccWriteTooOld

suite "MVCC Types - Edge Cases":
  test "encode key with null bytes in user key":
    let userKey = "\x00\x00\x00"
    let encoded = encodeMVCCKey(userKey, Timestamp(100), false)
    let decoded = decodeMVCCKey(encoded)
    check decoded.userKey == userKey

  test "encode value with binary data":
    let binaryData = "\x00\x01\x02\x03\x04\x05\xFF"
    let encoded = encodeMVCCValue(binaryData, Timestamp(100), false, InvalidTransactionID)
    let decoded = decodeMVCCValue(encoded)
    check decoded.data == binaryData

  test "encode/decode with all boundary timestamps":
    for ts in [mvccTypes.MIN_TIMESTAMP, 0'i64, 1'i64, mvccTypes.MAX_TIMESTAMP]:
      let encoded = encodeMVCCKey("key", Timestamp(ts), false)
      let decoded = decodeMVCCKey(encoded)
      check decoded.timestamp == Timestamp(ts)

  test "large number of key encodings":
    var keys: seq[string] = @[]
    for i in 0..<10000:
      keys.add(encodeMVCCKey("key" & $i, Timestamp(i), false))
    check keys.len == 10000

    var decoded: seq[MVCCKey] = @[]
    for k in keys:
      decoded.add(decodeMVCCKey(k))
    check decoded.len == 10000

    for i, d in decoded:
      check d.userKey == "key" & $i
      check d.timestamp == Timestamp(i)

  test "intent key vs version key differentiation":
    let versionKey = encodeMVCCKey("key", Timestamp(100), false)
    let intentKey = encodeIntentKey("key", genTransactionIDLocal())

    let versionDecoded = decodeMVCCKey(versionKey)
    let intentDecoded = decodeMVCCKey(intentKey)

    check versionDecoded.isIntent == false
    check intentDecoded.isIntent == true

suite "MVCC Types - Thread Safety":
  test "concurrent key encoding":
    var results: Atomic[int]
    results.store(0)

    proc encodeWorker(startIdx: int) {.thread.} =
      var localResults = 0
      for i in startIdx..<startIdx + 1000:
        let encoded = encodeMVCCKey("key" & $i, Timestamp(i), false)
        let decoded = decodeMVCCKey(encoded)
        if decoded.userKey == "key" & $i and decoded.timestamp == Timestamp(i):
          inc localResults
      atomicInc results, localResults

    var threads: array[4, Thread[int]]
    let chunkSize = 1000
    for i in 0..<4:
      createThread(threads[i], encodeWorker, i * chunkSize)

    joinThreads(threads)

    check results.load() == 4000

  test "concurrent value encoding":
    var results: Atomic[int]
    results.store(0)

    proc valueWorker(startIdx: int) {.thread.} =
      var localResults = 0
      for i in startIdx..<startIdx + 1000:
        let encoded = encodeMVCCValue("data" & $i, Timestamp(i), false, InvalidTransactionID)
        if isLikelyMVCCValue(encoded):
          let decoded = decodeMVCCValue(encoded)
          if decoded.data == "data" & $i:
            inc localResults
      atomicInc results, localResults

    var threads: array[4, Thread[int]]
    let chunkSize = 1000
    for i in 0..<4:
      createThread(threads[i], valueWorker, i * chunkSize)

    joinThreads(threads)

    check results.load() == 4000

  test "concurrent decodeMVCCKey":
    var results: Atomic[int]
    results.store(0)

    proc decodeWorker(dummy: int) {.thread.} =
      var localResults = 0
      for i in 0..<1000:
        let encoded = encodeMVCCKey("decodekey" & $i, Timestamp(i), false)
        let decoded = decodeMVCCKey(encoded)
        if decoded.timestamp == Timestamp(i):
          inc localResults
      atomicInc results, localResults

    var threads: array[4, Thread[int]]
    for i in 0..<4:
      createThread(threads[i], decodeWorker, i)

    joinThreads(threads)

    check results.load() == 4000

suite "MVCC Types - Stress Tests":
  test "rapid encode/decode cycle":
    for cycle in 0..<100:
      for i in 0..<100:
        let encoded = encodeMVCCKey("cycle" & $cycle & "_key" & $i, Timestamp(
            i), false)
        let decoded = decodeMVCCKey(encoded)
        check decoded.userKey == "cycle" & $cycle & "_key" & $i

  test "many timestamp values":
    for ts in countup(0'i64, 100000'i64, 100):
      let encoded = encodeMVCCKey("key", Timestamp(ts), false)
      let decoded = decodeMVCCKey(encoded)
      check decoded.timestamp == Timestamp(ts)

  test "value encoding with varying sizes":
    for size in [0, 1, 10, 100, 1000, 10000, 100000]:
      let data = "x".repeat(size)
      let encoded = encodeMVCCValue(data, Timestamp(1000), false, InvalidTransactionID)
      let decoded = decodeMVCCValue(encoded)
      check decoded.data.len == size

  test "many transaction IDs":
    var txnIds: seq[TransactionID] = @[]
    for i in 0..<1000:
      txnIds.add(genTransactionIDLocal())

    for txnId in txnIds:
      let encoded = encodeIntentKey("key", txnId)
      let decoded = decodeIntentKey(encoded)
      check decoded.txnId == txnId

suite "MVCC Types - Hash Consistency":
  test "hash is stable across calls":
    let key = MVCCKey(userKey: "stable_key", timestamp: 12345, isIntent: false)
    let h1 = hash(key)
    let h2 = hash(key)
    let h3 = hash(key)
    check h1 == h2
    check h2 == h3

  test "hash works in hash table":
    var table = initTable[MVCCKey, string]()
    let key1 = MVCCKey(userKey: "k1", timestamp: 100, isIntent: false)
    let key2 = MVCCKey(userKey: "k2", timestamp: 200, isIntent: false)

    table[key1] = "value1"
    table[key2] = "value2"

    check table[key1] == "value1"
    check table[key2] == "value2"
    check table.len == 2

  test "hash different keys produce different table entries":
    var table = initTable[MVCCKey, int]()
    for i in 0..<100:
      let key = MVCCKey(userKey: "key" & $i, timestamp: Timestamp(i),
          isIntent: false)
      table[key] = i

    check table.len == 100

    for i in 0..<100:
      let key = MVCCKey(userKey: "key" & $i, timestamp: Timestamp(i),
          isIntent: false)
      check table[key] == i
