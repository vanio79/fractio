# MVCC Engine - Multi-Version Concurrency Control storage engine
# Wraps StorageBackend to provide MVCC semantics

import std/[options, sets, sequtils, algorithm, strutils]
import ../../core/types as core_types
import ../../core/timestamp_provider
import ../../core/transaction
import ../../storage/backend
import ./types
export types.MAX_TIMESTAMP

type
  MVCCEngine* = ref object
    ## MVCC storage engine wrapping a StorageBackend
    backend*: StorageBackend
    timestampProvider*: TimestampProvider
    gcEnabled*: bool
      ## Whether garbage collection is enabled

  MVCCIterator* = ref object
    ## MVCC iterator for range scans
    engine*: MVCCEngine
    backendIter*: StorageIterator
    startKey*: string
    endKey*: string
    timestamp*: Timestamp
    currentKey*: string
    currentValue*: Option[MVCCValue]
    exhausted*: bool

# Use InvalidTransactionID from types module (already imported)
# Helper to convert MVCCResult
proc ok*(value: Option[MVCCValue]): MVCCResult =
  MVCCResult(success: true, value: value)

proc err*(code: MVCCErrorCode, message: string): MVCCResult =
  MVCCResult(success: false, error: mvccError(code, message))

proc okScan*(kvs: seq[MVCCKeyValue]): MVCCScanResult =
  MVCCScanResult(success: true, kvs: kvs)

proc errScan*(code: MVCCErrorCode, message: string): MVCCScanResult =
  MVCCScanResult(success: false, error: mvccError(code, message))

# MVCCEngine methods

proc newMVCCEngine*(backend: StorageBackend,
    tsProvider: TimestampProvider): MVCCEngine =
  ## Create a new MVCC engine
  new(result)
  result.backend = backend
  result.timestampProvider = tsProvider
  result.gcEnabled = false

proc mvccGet*(engine: MVCCEngine, key: string, timestamp: Timestamp,
    txnId: TransactionID = InvalidTransactionID): MVCCResult =
  ## Get value at a specific timestamp
  ## If txnId is provided, also checks for intents from that transaction

  # First check for intent from our own transaction
  if txnId != InvalidTransactionID:
    let intentKey = makeIntentKey(key, txnId)
    let intentValue = engine.backend.get(intentKey)
    if intentValue.isSome:
      let mvccValue = decodeMVCCValue(intentValue.get())
      return ok(some(mvccValue))

  # Use seekToLast to find newest versions first, then iterate backwards
  let iter = engine.backend.newIterator()
  discard iter.seekToLast()

  # Iterate backwards to find newest version <= timestamp
  while iter.valid():
    let iterKey = iter.key()

    # Stop if we've moved past our key range
    if not iterKey.startsWith(key):
      break

    # Check if this key matches our user key exactly (not a different key)
    let decoded = decodeMVCCKey(iterKey)
    if decoded.userKey != key:
      discard iter.prev()
      continue

    if decoded.isIntent:
      # There's an intent from another transaction - return conflict
      return err(mvccIntentConflict, "Found intent for key: " & key)
    else:
      # Regular version - check if within timestamp
      if decoded.timestamp <= timestamp:
        let value = iter.value()
        let mvccValue = decodeMVCCValue(value)
        if not mvccValue.isDeleted:
          return ok(some(mvccValue))
        else:
          return ok(none(MVCCValue))

    discard iter.prev()

  # No version found
  return ok(none(MVCCValue))

proc mvccPut*(engine: MVCCEngine, txn: MVCCTransaction, key: string,
    value: string): MVCCResult =
  ## Put a value as an intent (provisional write)
  ## The transaction provides the timestamp and txnId

  if txn.status != TXN_PENDING:
    return err(mvccInvalidTransaction, "Transaction not pending")

  # Check if there's already an intent
  let existingIntentKey = makeIntentKey(key, txn.id)
  let existing = engine.backend.get(existingIntentKey)
  if existing.isSome:
    return err(mvccIntentConflict, "Intent already exists for key: " & key)

  # Encode the intent value
  let encodedValue = encodeMVCCValue(value, txn.startTimestamp, false, txn.id)

  # Write the intent
  let intentKey = makeIntentKey(key, txn.id)
  if not engine.backend.put(intentKey, encodedValue):
    return err(mvccStorageError, "Failed to write intent")

  return ok(none(MVCCValue))

proc mvccDelete*(engine: MVCCEngine, txn: MVCCTransaction,
    key: string): MVCCResult =
  ## Delete a key by writing a delete intent

  if txn.status != TXN_PENDING:
    return err(mvccInvalidTransaction, "Transaction not pending")

  # Encode delete intent
  let encodedValue = encodeMVCCValue("", txn.startTimestamp, true, txn.id)

  # Write the delete intent
  let intentKey = makeIntentKey(key, txn.id)
  if not engine.backend.put(intentKey, encodedValue):
    return err(mvccStorageError, "Failed to write delete intent")

  return ok(none(MVCCValue))

proc mvccScan*(engine: MVCCEngine, startKey: string, endKey: string,
    timestamp: Timestamp, txnId: TransactionID = InvalidTransactionID): MVCCScanResult =
  ## Scan MVCC keys within a range at a specific timestamp
  ## Returns newest version of each key <= timestamp
  ## Iterates backwards to find newest versions first

  var results: seq[MVCCKeyValue] = @[]
  var seenKeys: HashSet[string] = initHashSet[string]()

  let iter = engine.backend.newIterator()

  # Start from the end key (or from last key if no endKey)
  # We need to find a starting position that's within or before our range
  if endKey.len > 0:
    # Try to seek to endKey
    if not iter.seek(endKey):
      # No key >= endKey, so start from the last key
      discard iter.seekToLast()
    elif iter.key() >= endKey:
      # We're at or beyond endKey, move back one position
      discard iter.prev()
  else:
    discard iter.seekToLast()

  # Iterate backwards through keys
  while iter.valid():
    let iterKey = iter.key()

    try:
      let decoded = decodeMVCCKey(iterKey)
      let userKey = decoded.userKey

      # Stop if we've passed below the start key range
      if userKey < startKey:
        break

      # Skip if above end key range (for open-ended scans)
      if endKey.len > 0 and userKey >= endKey:
        discard iter.prev()
        continue

      # Skip intents - they need special handling
      if decoded.isIntent:
        discard iter.prev()
        continue

      # Only include versions <= timestamp
      if decoded.timestamp <= timestamp:
        # Skip if we've already seen this key (only keep newest)
        if userKey in seenKeys:
          discard iter.prev()
          continue

        seenKeys.incl(userKey)

        let value = iter.value()
        let mvccValue = decodeMVCCValue(value)

        # Only include non-deleted values
        if not mvccValue.isDeleted:
          results.add((MVCCKey(userKey: userKey,
              timestamp: decoded.timestamp, isIntent: false), mvccValue))
    except MVCCError:
      # Skip invalid keys
      discard

    discard iter.prev()

  # Reverse results to get them in forward key order
  results.reverse()
  return okScan(results)

proc getLatestVersion*(engine: MVCCEngine, key: string): Option[MVCCValue] =
  ## Get the latest non-intent version of a key
  ## Used for conflict detection during commit
  ## Iterates backwards to find newest version first

  let iter = engine.backend.newIterator()
  discard iter.seekToLast()

  while iter.valid():
    let iterKey = iter.key()

    try:
      let decoded = decodeMVCCKey(iterKey)

      # Check if this key matches our user key exactly
      if decoded.userKey == key and not decoded.isIntent:
        # Found a committed version
        let value = iter.value()
        return some(decodeMVCCValue(value))
    except MVCCError:
      discard

    discard iter.prev()

  return none(MVCCValue)

proc resolveIntent*(engine: MVCCEngine, key: string, txnId: TransactionID,
    commit: bool, commitTimestamp: Timestamp = MIN_TIMESTAMP): MVCCResult =
  ## Resolve an intent - either commit it or rollback/abort it

  let intentKey = makeIntentKey(key, txnId)
  let intentValue = engine.backend.get(intentKey)

  if intentValue.isNone:
    return err(mvccIntentNotFound, "Intent not found for key: " & key)

  let decodedIntent = decodeMVCCValue(intentValue.get())

  if commit:
    # Commit: upgrade intent to committed version
    let committedKey = makeVersionKey(key, commitTimestamp)
    let committedValue = encodeMVCCValue(
      decodedIntent.data,
      commitTimestamp,
      decodedIntent.isDeleted,
      txnId
    )

    if not engine.backend.put(committedKey, committedValue):
      return err(mvccStorageError, "Failed to write committed version")

    # Delete the intent
    if not engine.backend.delete(intentKey):
      return err(mvccStorageError, "Failed to delete intent")
  else:
    # Abort: simply remove the intent
    if not engine.backend.delete(intentKey):
      return err(mvccStorageError, "Failed to delete intent")

  return ok(none(MVCCValue))

proc cleanupIntent*(engine: MVCCEngine, key: string,
    txnId: TransactionID): bool =
  ## Cleanup a single intent (for abort/cleanup)
  let intentKey = makeIntentKey(key, txnId)
  result = engine.backend.delete(intentKey)

proc getIntent*(engine: MVCCEngine, key: string,
    txnId: TransactionID): Option[MVCCValue] =
  ## Get intent for a specific transaction
  let intentKey = makeIntentKey(key, txnId)
  let value = engine.backend.get(intentKey)
  if value.isSome:
    return some(decodeMVCCValue(value.get()))
  return none(MVCCValue)

proc hasIntent*(engine: MVCCEngine, key: string): bool =
  ## Check if any intent exists for a key
  let iter = engine.backend.newIterator()
  discard iter.seekToLast()

  while iter.valid():
    let iterKey = iter.key()

    try:
      let decoded = decodeMVCCKey(iterKey)
      if decoded.userKey == key and decoded.isIntent:
        return true
    except MVCCError:
      discard

    discard iter.prev()

  return false

proc getIntentsForKey*(engine: MVCCEngine, key: string): seq[Intent] =
  ## Get all intents for a key
  result = @[]

  let iter = engine.backend.newIterator()
  discard iter.seekToLast()

  while iter.valid():
    let iterKey = iter.key()

    try:
      let decoded = decodeMVCCKey(iterKey)
      if decoded.userKey == key and decoded.isIntent:
        let decodedKey = decodeIntentKey(iterKey)
        let value = iter.value()
        let mvccValue = decodeMVCCValue(value)

        result.add(Intent(
          key: decodedKey.userKey,
          txnId: decodedKey.txnId,
          timestamp: mvccValue.timestamp,
          value: mvccValue.data,
          isDeleted: mvccValue.isDeleted
        ))
    except MVCCError:
      discard

    discard iter.prev()

proc getAllVersions*(engine: MVCCEngine, userKey: string): seq[KeyVersion] =
  ## Get all versions of a key, sorted by timestamp (newest first)
  ## Iterates backwards to get newest versions first
  result = @[]

  let iter = engine.backend.newIterator()
  discard iter.seekToLast()

  while iter.valid():
    let iterKey = iter.key()

    try:
      let decoded = decodeMVCCKey(iterKey)
      if decoded.userKey == userKey and not decoded.isIntent:
        # This is a committed version
        let value = iter.value()
        let mvccValue = decodeMVCCValue(value)

        result.add(KeyVersion(
          key: userKey,
          value: mvccValue,
          isLatest: result.len == 0 # First one is the latest
        ))
    except MVCCError:
      discard

    discard iter.prev()

# Unit tests
when isMainModule:
  import unittest

  # Note: Full tests would require a mock StorageBackend
  # These are basic compile tests

  suite "MVCCEngine Types":
    test "MVCCResult construction":
      let successResult = ok(some(MVCCValue(data: "test", timestamp: 100,
          isDeleted: false, txnId: InvalidTransactionID)))
      check successResult.success == true
      check successResult.value.isSome

      let errorResult = err(mvccKeyNotFound, "test error")
      check errorResult.success == false
      check errorResult.error.code == mvccKeyNotFound
