# MVCC Engine - Multi-Version Concurrency Control storage engine
# Wraps StorageBackend to provide MVCC semantics

import std/[options, sets, algorithm, strutils, locks, deques,
    atomics, typedthreads, os]
import ../../core/types as core_types
import ../../core/timestamp_provider
import ../../core/transaction
import ../../storage/backend
import ../../storage/wisckey_backend
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
  defer: destroyIter(iter)
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
  defer: destroyIter(iter)

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
  defer: destroyIter(iter)
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
  defer: destroyIter(iter)
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
  defer: destroyIter(iter)
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
  defer: destroyIter(iter)
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

# ============================================================================
# MVCC Streaming Scan Implementation
# ============================================================================

proc mvccPrefetchWorker(stream: MVCCStreamResultSet) {.thread, gcsafe, raises: [].} =
  ## Background thread that prefetches MVCC key-value pairs into the buffer.
  ## Runs until stream is closed, exhausted, or encounters an error.
  if stream.sharedData == nil or stream.engine == nil:
    return

  let engine = cast[MVCCEngine](stream.engine)
  let backend = engine.backend
  if backend == nil:
    stream.sharedData.state.store(mssError, moRelaxed)
    stream.sharedData.errorMsg.store("backend is nil", moRelaxed)
    return

  # Create iterator for reading
  var iter: StorageIterator = nil
  {.cast(raises: []).}:
    try:
      iter = backend.newIterator()
    except:
      stream.sharedData.state.store(mssError, moRelaxed)
      stream.sharedData.errorMsg.store("failed to create iterator", moRelaxed)
      return

  if iter == nil:
    stream.sharedData.state.store(mssError, moRelaxed)
    stream.sharedData.errorMsg.store("iterator is nil", moRelaxed)
    return

  # Start from the end key (or from last key if no endKey)
  var positioned = false
  {.cast(raises: []).}:
    try:
      if stream.endKey.len > 0:
        if not iter.seek(stream.endKey):
          discard iter.seekToLast()
        elif iter.key() >= stream.endKey:
          discard iter.prev()
      else:
        discard iter.seekToLast()
      positioned = true
    except:
      stream.sharedData.state.store(mssError, moRelaxed)
      stream.sharedData.errorMsg.store("iterator positioning failed", moRelaxed)
      return

  if not positioned:
    stream.sharedData.state.store(mssError, moRelaxed)
    stream.sharedData.errorMsg.store("failed to position iterator", moRelaxed)
    return

  var seenKeys: HashSet[string] = initHashSet[string]()
  var itemCount = 0

  # Iterate backwards through keys
  while stream.sharedData.state.load(moRelaxed) == mssReading:
    # Check if we should pause (buffer full)
    var shouldPause = false
    acquire(stream.sharedData.bufferLock)
    shouldPause = stream.sharedData.buffer.len >= stream.config.bufferSize
    release(stream.sharedData.bufferLock)

    if shouldPause:
      # Wait for consumer to drain some items
      os.sleep(10)
      continue

    # Read next item
    var valid = false
    {.cast(raises: []).}:
      try:
        valid = iter.valid()
      except:
        discard

    if not valid:
      # Exhausted
      stream.sharedData.state.store(mssExhausted, moRelaxed)
      break

    var iterKey = ""
    var iterValue = ""
    {.cast(raises: []).}:
      try:
        iterKey = iter.key()
        iterValue = iter.value()
      except:
        discard iter.prev()
        continue

    try:
      let decoded = decodeMVCCKey(iterKey)
      let userKey = decoded.userKey

      # Stop if we've passed below the start key range
      if userKey < stream.startKey:
        stream.sharedData.state.store(mssExhausted, moRelaxed)
        break

      # Skip if above end key range (for open-ended scans)
      if stream.endKey.len > 0 and userKey >= stream.endKey:
        {.cast(raises: []).}:
          try:
            discard iter.prev()
          except:
            discard
        continue

      # Skip intents - they need special handling
      if decoded.isIntent:
        {.cast(raises: []).}:
          try:
            discard iter.prev()
          except:
            discard
        continue

      # Only include versions <= timestamp
      if decoded.timestamp <= stream.timestamp:
        # Skip if we've already seen this key (only keep newest)
        if userKey in seenKeys:
          {.cast(raises: []).}:
            try:
              discard iter.prev()
            except:
              discard
          continue

        seenKeys.incl(userKey)

        let mvccValue = decodeMVCCValue(iterValue)

        # Only include non-deleted values
        if not mvccValue.isDeleted:
          let kv = (MVCCKey(userKey: userKey, timestamp: decoded.timestamp,
                           isIntent: false), mvccValue)

          # Add to buffer
          acquire(stream.sharedData.bufferLock)
          stream.sharedData.buffer.addLast(kv)
          discard stream.sharedData.totalRead.fetchAdd(1, moRelaxed)
          release(stream.sharedData.bufferLock)
          itemCount += 1

    except MVCCError:
      # Skip invalid keys
      discard
    except CatchableError:
      discard

    # Move to previous key
    {.cast(raises: []).}:
      try:
        discard iter.prev()
      except:
        discard

  # Clean up iterator
  {.cast(raises: []).}:
    try:
      destroyIter(iter)
    except:
      discard

proc newMVCCStreamResultSet*(engine: MVCCEngine, startKey: string,
    endKey: string, timestamp: Timestamp,
        txnId: TransactionID = InvalidTransactionID,
    config: MVCCStreamConfig = defaultMVCCStreamConfig()): MVCCStreamResultSet =
  ## Create a new MVCC streaming result set.
  ## The stream must be initialized by calling start() before use.
  new(result)
  result.engine = cast[pointer](engine)
  result.startKey = startKey
  result.endKey = endKey
  result.timestamp = timestamp
  result.txnId = txnId
  result.config = config

  # Allocate shared data on heap
  result.sharedData = create(MVCCStreamSharedData)
  if result.sharedData != nil:
    result.sharedData.buffer = initDeque[MVCCKeyValue]()
    initLock(result.sharedData.bufferLock)
    result.sharedData.state.store(mssIdle, moRelaxed)
    result.sharedData.errorMsg.store("", moRelaxed)
    result.sharedData.totalRead.store(0, moRelaxed)
    result.sharedData.consumerPos.store(0, moRelaxed)

proc start*(stream: MVCCStreamResultSet): bool =
  ## Start the prefetch thread for this stream.
  ## Returns true if started successfully.
  if stream.sharedData == nil:
    return false

  let currentState = stream.sharedData.state.load(moRelaxed)
  if currentState != mssIdle:
    return false

  stream.sharedData.state.store(mssReading, moRelaxed)

  {.cast(raises: []).}:
    try:
      createThread(stream.prefetchThread, mvccPrefetchWorker, stream)
      return true
    except:
      stream.sharedData.state.store(mssError, moRelaxed)
      stream.sharedData.errorMsg.store("failed to start prefetch thread", moRelaxed)
      return false

proc next*(stream: MVCCStreamResultSet): Option[MVCCKeyValue] =
  ## Get the next MVCC key-value pair from the stream.
  ## Returns some(kv) if available, none() if exhausted or closed.
  ## Thread-safe: blocks briefly if buffer empty but prefetch still running.
  if stream.sharedData == nil:
    return none(MVCCKeyValue)

  let state = stream.sharedData.state.load(moRelaxed)

  if state == mssClosed:
    return none(MVCCKeyValue)

  if state == mssError:
    return none(MVCCKeyValue)

  # Try to get item from buffer
  acquire(stream.sharedData.bufferLock)
  if stream.sharedData.buffer.len > 0:
    # Buffer has items - get the first one (results are in forward key order after reversal)
    let kv = stream.sharedData.buffer.popFirst()
    discard stream.sharedData.consumerPos.fetchAdd(1, moRelaxed)
    release(stream.sharedData.bufferLock)
    return some(kv)
  release(stream.sharedData.bufferLock)

  # Buffer empty - check if stream is exhausted
  if state == mssExhausted:
    return none(MVCCKeyValue)

  # Stream still reading - wait briefly for more data
  for _ in 0 ..< 50: # Wait up to 500ms
    os.sleep(10)
    acquire(stream.sharedData.bufferLock)
    if stream.sharedData.buffer.len > 0:
      let kv = stream.sharedData.buffer.popFirst()
      discard stream.sharedData.consumerPos.fetchAdd(1, moRelaxed)
      release(stream.sharedData.bufferLock)
      return some(kv)
    release(stream.sharedData.bufferLock)

    let newState = stream.sharedData.state.load(moRelaxed)
    if newState == mssExhausted or newState == mssError or newState == mssClosed:
      return none(MVCCKeyValue)

  # Timeout waiting for data
  return none(MVCCKeyValue)

proc hasNext*(stream: MVCCStreamResultSet): bool =
  ## Check if more data is available without consuming it.
  ## Returns true if buffer has items or prefetch thread is still running.
  if stream.sharedData == nil:
    return false

  let state = stream.sharedData.state.load(moRelaxed)
  if state == mssClosed or state == mssError:
    return false

  acquire(stream.sharedData.bufferLock)
  let bufferLen = stream.sharedData.buffer.len
  release(stream.sharedData.bufferLock)

  if bufferLen > 0:
    return true

  # Buffer empty - check if stream still reading
  return state == mssReading

proc close*(stream: MVCCStreamResultSet) =
  ## Close the stream and stop the prefetch thread.
  ## Must be called to release resources.
  if stream.sharedData == nil:
    return

  let currentState = stream.sharedData.state.load(moRelaxed)
  if currentState == mssClosed:
    return

  # Signal thread to stop
  stream.sharedData.state.store(mssClosed, moRelaxed)

  # Wait for thread to finish
  {.cast(raises: []).}:
    try:
      joinThread(stream.prefetchThread)
    except:
      discard

  # Clean up shared data
  deinitLock(stream.sharedData.bufferLock)

  # Free shared data memory
  if stream.sharedData != nil:
    dealloc(stream.sharedData)
    stream.sharedData = nil

proc getState*(stream: MVCCStreamResultSet): MVCCStreamState =
  ## Get current stream state.
  if stream.sharedData == nil:
    return mssClosed
  stream.sharedData.state.load(moRelaxed)

proc getTotalRead*(stream: MVCCStreamResultSet): int =
  ## Get total number of items read by the stream.
  if stream.sharedData == nil:
    return 0
  stream.sharedData.totalRead.load(moRelaxed)

proc getError*(stream: MVCCStreamResultSet): Option[string] =
  ## Get error message if stream is in error state.
  if stream.sharedData == nil:
    return some("stream not initialized")
  let state = stream.sharedData.state.load(moRelaxed)
  if state != mssError:
    return none(string)
  let msg = stream.sharedData.errorMsg.load(moRelaxed)
  if msg.len > 0:
    return some(msg)
  return some("unknown error")

proc consumeMVCCStream*(stream: MVCCStreamResultSet): seq[MVCCKeyValue] =
  ## Consume all remaining items from the stream and return them as a sequence.
  ## This is a convenience helper for backward compatibility.
  ## Warning: For large result sets, this defeats the purpose of streaming.
  result = @[]
  while stream.hasNext():
    let kvOpt = stream.next()
    if kvOpt.isSome:
      # Add to end - the prefetch worker reads backwards and adds to deque,
      # so we need to reverse at the end to get forward key order
      result.add(kvOpt.get())
  # Reverse to get forward key order
  result.reverse()
  stream.close()

proc mvccStreamScan*(engine: MVCCEngine, startKey: string, endKey: string,
    timestamp: Timestamp, txnId: TransactionID = InvalidTransactionID,
    config: MVCCStreamConfig = defaultMVCCStreamConfig()): MVCCStreamResultSet =
  ## Create a streaming scan for MVCC keys within a range at a specific timestamp.
  ## Returns newest version of each key <= timestamp.
  ## The caller must call start() to begin prefetch, then iterate with next(),
  ## and finally call close() to release resources.
  let stream = newMVCCStreamResultSet(engine, startKey, endKey, timestamp,
      txnId, config)
  discard stream.start()
  stream

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
