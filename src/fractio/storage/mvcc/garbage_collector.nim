# MVCC Garbage Collector - Cleanup of old MVCC versions
# Removes old versions of keys that are no longer needed for transaction visibility

import std/[times, atomics, locks, tables, sets, sequtils, options]
import ../../core/types
import ../../core/timestamp_provider
import ../../core/transaction
import ../../utils/logging
import ../../storage/backend
import ./types
import ./engine

type
  GCPolicy* = object
    ## Policy for garbage collection
    minTimestamp*: Timestamp
      ## Minimum timestamp to keep (all versions before this can be collected)
    maxVersionsPerKey*: int
      ## Maximum number of versions to keep per key
    maxAgeMs*: int64
      ## Maximum age of versions in milliseconds
    collectIntervalMs*: int64
      ## How often to run GC (in milliseconds)

  GCStats* = object
    ## Statistics for garbage collection
    keysScanned*: int
    versionsCollected*: int
    bytesCollected*: int64
    lastRunTime*: Timestamp
    totalRunTimeMs*: int64
    runCount*: int

  GarbageCollector* = ref object
    ## Garbage collector for MVCC versions
    engine*: MVCCEngine
      ## MVCC engine to collect from
    policy*: GCPolicy
      ## GC policy
    running*: Atomic[bool]
      ## Whether GC is running
    stats*: GCStats
      ## GC statistics
    logger*: Logger
      ## Logger for GC operations
    lock*: Lock
      ## Lock for thread-safe statistics updates

  GCResult* = object
    ## Result of a GC operation
    success*: bool
    keysScanned*: int
    versionsCollected*: int
    bytesCollected*: int64
    error*: string

const
  DEFAULT_MAX_VERSIONS_PER_KEY* = 10
  DEFAULT_MAX_AGE_MS* = 300_000         # 5 minutes
  DEFAULT_COLLECT_INTERVAL_MS* = 60_000 # 1 minute

# Helper functions

proc deleteKey*(backend: StorageBackend, key: string): bool =
  ## Helper to call delete method on backend
  result = delete(backend, key)

proc newGCPolicy*(): GCPolicy =
  ## Create default GC policy
  GCPolicy(
    minTimestamp: Timestamp(0),
    maxVersionsPerKey: DEFAULT_MAX_VERSIONS_PER_KEY,
    maxAgeMs: DEFAULT_MAX_AGE_MS,
    collectIntervalMs: DEFAULT_COLLECT_INTERVAL_MS
  )

proc newGCPolicy*(minTimestamp: Timestamp,
    maxVersionsPerKey: int = DEFAULT_MAX_VERSIONS_PER_KEY,
    maxAgeMs: int64 = DEFAULT_MAX_AGE_MS,
    collectIntervalMs: int64 = DEFAULT_COLLECT_INTERVAL_MS): GCPolicy =
  ## Create GC policy with custom settings
  GCPolicy(
    minTimestamp: minTimestamp,
    maxVersionsPerKey: maxVersionsPerKey,
    maxAgeMs: maxAgeMs,
    collectIntervalMs: collectIntervalMs
  )

proc newGarbageCollector*(engine: MVCCEngine,
    policy: GCPolicy = newGCPolicy(),
    logger: Logger = nil): GarbageCollector =
  ## Create a new garbage collector
  new(result)
  result.engine = engine
  result.policy = policy
  result.running.store(false)
  result.stats = GCStats(
    keysScanned: 0,
    versionsCollected: 0,
    bytesCollected: 0,
    lastRunTime: Timestamp(0),
    totalRunTimeMs: 0,
    runCount: 0
  )
  result.logger = if logger != nil: logger else: newLogger("mvcc_gc")
  initLock(result.lock)

proc isRunning*(gc: GarbageCollector): bool =
  ## Check if GC is running
  result = gc.running.load()

proc getStats*(gc: GarbageCollector): GCStats =
  ## Get GC statistics (thread-safe)
  acquire(gc.lock)
  result = gc.stats
  release(gc.lock)

proc updateStats*(gc: GarbageCollector, keysScanned: int,
    versionsCollected: int, bytesCollected: int64, runTimeMs: int64) =
  ## Update GC statistics (thread-safe)
  acquire(gc.lock)
  gc.stats.keysScanned += keysScanned
  gc.stats.versionsCollected += versionsCollected
  gc.stats.bytesCollected += bytesCollected
  gc.stats.lastRunTime = Timestamp(epochTime().int64 * 1_000_000)
  gc.stats.totalRunTimeMs += runTimeMs
  gc.stats.runCount += 1
  release(gc.lock)

proc shouldCollectVersion*(gc: GarbageCollector,
    key: string, version: MVCCValue, currentTime: Timestamp): bool =
  ## Determine if a version should be collected
  ## Returns true if version can be safely removed

  # Check if version is too old
  let ageMs = (currentTime - version.timestamp) div 1_000_000
  if ageMs > gc.policy.maxAgeMs:
    return true

  # Check if version is before minimum timestamp
  if version.timestamp < gc.policy.minTimestamp:
    return true

  return false

proc collectVersionsForKey*(gc: GarbageCollector,
    userKey: string): GCResult =
  ## Collect old versions for a specific key
  ## Returns result of collection

  result = GCResult(
    success: false,
    keysScanned: 0,
    versionsCollected: 0,
    bytesCollected: 0,
    error: ""
  )

  try:
    # Get all versions for this key
    let versions = gc.engine.getAllVersions(userKey)

    if versions.len == 0:
      result.success = true
      return

    result.keysScanned = 1
    let currentTime = Timestamp(epochTime().int64 * 1_000_000)

    # Determine which versions to keep
    var versionsToKeep: seq[KeyVersion] = @[]

    # Always keep the latest version
    if versions.len > 0:
      versionsToKeep.add(versions[0])

    # Check remaining versions
    var collectedCount = 0
    var collectedBytes: int64 = 0

    for i in 1 ..< versions.len:
      let version = versions[i]

      # Check if we should collect this version
      if gc.shouldCollectVersion(userKey, version.value, currentTime):
        # Collect this version
        let mvccKey = encodeMVCCKey(userKey, version.value.timestamp, false)
        let backend = gc.engine.backend
        discard deleteKey(backend, mvccKey)
        collectedCount += 1
        collectedBytes += mvccKey.len + version.value.data.len

        # Also collect intent if exists
        let intentKey = encodeMVCCKey(userKey, version.value.timestamp, true)
        if backend.exists(intentKey):
          discard deleteKey(backend, intentKey)
          collectedBytes += intentKey.len
      else:
        # Keep this version
        versionsToKeep.add(version)

        # Check if we have too many versions
        if versionsToKeep.len >= gc.policy.maxVersionsPerKey:
          # Collect older versions beyond max
          for j in i + 1 ..< versions.len:
            let oldVersion = versions[j]
            let mvccKey = encodeMVCCKey(userKey, oldVersion.value.timestamp, false)
            let backend = gc.engine.backend
            discard deleteKey(backend, mvccKey)
            collectedCount += 1
            collectedBytes += mvccKey.len + oldVersion.value.data.len

            # Collect intent if exists
            let intentKey = encodeMVCCKey(userKey, oldVersion.value.timestamp, true)
            if backend.exists(intentKey):
              discard deleteKey(backend, intentKey)
              collectedBytes += intentKey.len

          break

    result.versionsCollected = collectedCount
    result.bytesCollected = collectedBytes
    result.success = true

    gc.logger.debug("Collected versions for key",
      {"userKey": userKey,
       "versionsCollected": $collectedCount,
       "bytesCollected": $collectedBytes}.toTable)

  except Exception as e:
    result.error = "Failed to collect versions for key " & userKey & ": " & e.msg
    gc.logger.error("Failed to collect versions for key",
      {"userKey": userKey,
       "error": e.msg}.toTable)

proc collectVersions*(gc: GarbageCollector,
    minTimestamp: Timestamp = Timestamp(0)): GCResult =
  ## Collect old versions across all keys
  ## Returns result of collection

  gc.logger.info("Starting garbage collection",
    {"minTimestamp": $minTimestamp}.toTable)

  result = GCResult(
    success: false,
    keysScanned: 0,
    versionsCollected: 0,
    bytesCollected: 0,
    error: ""
  )

  let startTime = epochTime().int64

  try:
    # Update policy min timestamp if provided
    if minTimestamp > gc.policy.minTimestamp:
      gc.policy.minTimestamp = minTimestamp

    # Scan all keys in the database
    # In a real implementation, we would iterate over all keys
    # For now, we'll simulate this

    # Get iterator over all keys
    var scanIter = gc.engine.backend.newIterator()
    discard scanIter.seekToFirst()

    var keysScanned = 0
    var versionsCollected = 0
    var bytesCollected: int64 = 0

    # Track which user keys we've processed
    var processedUserKeys: HashSet[string] = initHashSet[string]()

    while scanIter.valid():
      let encodedKey = scanIter.key()

      # Decode to get user key
      let mvccKey = decodeMVCCKey(encodedKey)
      let userKey = mvccKey.userKey

      # Only process each user key once
      if userKey notin processedUserKeys:
        processedUserKeys.incl(userKey)

        # Collect versions for this key
        let keyResult = gc.collectVersionsForKey(userKey)

        if keyResult.success:
          keysScanned += keyResult.keysScanned
          versionsCollected += keyResult.versionsCollected
          bytesCollected += keyResult.bytesCollected

      discard scanIter.next()

    result.keysScanned = keysScanned
    result.versionsCollected = versionsCollected
    result.bytesCollected = bytesCollected
    result.success = true

    let runTimeMs = epochTime().int64 - startTime
    gc.updateStats(keysScanned, versionsCollected, bytesCollected, runTimeMs)

    gc.logger.info("Garbage collection completed",
      {"keysScanned": $keysScanned,
       "versionsCollected": $versionsCollected,
       "bytesCollected": $bytesCollected,
       "runTimeMs": $runTimeMs}.toTable)

  except Exception as e:
    result.error = "Failed to collect versions: " & e.msg
    gc.logger.error("Garbage collection failed",
      {"error": e.msg}.toTable)

proc collectVersionsForTransaction*(gc: GarbageCollector,
    transactionId: TransactionID): GCResult =
  ## Collect all versions/intents for a specific transaction
  ## Useful after transaction commit/abort

  gc.logger.info("Collecting versions for transaction",
    {"transactionId": $int64(transactionId)}.toTable)

  result = GCResult(
    success: false,
    keysScanned: 0,
    versionsCollected: 0,
    bytesCollected: 0,
    error: ""
  )

  try:
    # Scan all keys to find intents for this transaction
    var scanIter = gc.engine.backend.newIterator()
    discard scanIter.seekToFirst()

    var versionsCollected = 0
    var bytesCollected: int64 = 0

    while scanIter.valid():
      let encodedKey = scanIter.key()

      # Decode to check if it's an intent for this transaction
      let mvccKey = decodeMVCCKey(encodedKey)

      if mvccKey.isIntent:
        # Get the value to check transaction ID
        let backend = gc.engine.backend
        let valueResult = backend.get(encodedKey)
        if valueResult.isSome:
          let mvccValue = decodeMVCCValue(valueResult.get())

          if mvccValue.txnId == transactionId:
            # Collect this intent
            discard deleteKey(backend, encodedKey)
            versionsCollected += 1
            bytesCollected += encodedKey.len + valueResult.get().len

            gc.logger.debug("Collected intent for transaction",
              {"transactionId": $int64(transactionId),
               "key": mvccKey.userKey}.toTable)

      discard scanIter.next()

    result.versionsCollected = versionsCollected
    result.bytesCollected = bytesCollected
    result.success = true

    gc.logger.info("Collected versions for transaction",
      {"transactionId": $int64(transactionId),
       "versionsCollected": $versionsCollected,
       "bytesCollected": $bytesCollected}.toTable)

  except Exception as e:
    result.error = "Failed to collect versions for transaction: " & e.msg
    gc.logger.error("Failed to collect versions for transaction",
      {"transactionId": $int64(transactionId),
       "error": e.msg}.toTable)

proc startGC*(gc: GarbageCollector) =
  ## Start the garbage collector background thread
  ## In a real implementation, this would spawn a background thread

  if gc.running.load():
    gc.logger.warn("Garbage collector already running")
    return

  gc.running.store(true)
  gc.logger.info("Garbage collector started")

proc stopGC*(gc: GarbageCollector) =
  ## Stop the garbage collector background thread
  ## In a real implementation, this would signal the thread to stop

  if not gc.running.load():
    gc.logger.warn("Garbage collector not running")
    return

  gc.running.store(false)
  gc.logger.info("Garbage collector stopped")

proc resetStats*(gc: GarbageCollector) =
  ## Reset GC statistics
  acquire(gc.lock)
  gc.stats = GCStats(
    keysScanned: 0,
    versionsCollected: 0,
    bytesCollected: 0,
    lastRunTime: Timestamp(0),
    totalRunTimeMs: 0,
    runCount: 0
  )
  release(gc.lock)

proc `==`*(a, b: GCStats): bool =
  ## Compare GC statistics
  result = a.keysScanned == b.keysScanned and
           a.versionsCollected == b.versionsCollected and
           a.bytesCollected == b.bytesCollected and
           a.lastRunTime == b.lastRunTime and
           a.totalRunTimeMs == b.totalRunTimeMs and
           a.runCount == b.runCount

# Unit tests
when isMainModule:
  import unittest

  suite "MVCC Garbage Collector":
    test "create default GC policy":
      let policy = newGCPolicy()

      check policy.maxVersionsPerKey == DEFAULT_MAX_VERSIONS_PER_KEY
      check policy.maxAgeMs == DEFAULT_MAX_AGE_MS
      check policy.collectIntervalMs == DEFAULT_COLLECT_INTERVAL_MS

    test "create custom GC policy":
      let policy = newGCPolicy(
        minTimestamp = Timestamp(1000),
        maxVersionsPerKey = 5,
        maxAgeMs = 60000,
        collectIntervalMs = 30000
      )

      check policy.minTimestamp == Timestamp(1000)
      check policy.maxVersionsPerKey == 5
      check policy.maxAgeMs == 60000
      check policy.collectIntervalMs == 30000

    test "GC stats initialization":
      let stats = GCStats(
        keysScanned: 0,
        versionsCollected: 0,
        bytesCollected: 0,
        lastRunTime: Timestamp(0),
        totalRunTimeMs: 0,
        runCount: 0
      )

      check stats.keysScanned == 0
      check stats.versionsCollected == 0
      check stats.bytesCollected == 0
      check stats.runCount == 0

    test "GC result initialization":
      let result = GCResult(
        success: true,
        keysScanned: 10,
        versionsCollected: 5,
        bytesCollected: 1000,
        error: ""
      )

      check result.success == true
      check result.keysScanned == 10
      check result.versionsCollected == 5
      check result.bytesCollected == 1000

    test "GC result with error":
      let errorResult = GCResult(
        success: false,
        keysScanned: 0,
        versionsCollected: 0,
        bytesCollected: 0,
        error: "Collection failed"
      )

      check errorResult.success == false
      check errorResult.error == "Collection failed"

    test "should collect old version":
      let gc = GarbageCollector(
        policy: GCPolicy(
          minTimestamp: Timestamp(0),
          maxVersionsPerKey: 10,
          maxAgeMs: 60000,
          collectIntervalMs: 60000
        ),
        running: Atomic[bool](),
        stats: GCStats(),
        logger: nil
      )

      let version = MVCCValue(
        data: "test",
        timestamp: Timestamp(1000),
        isDeleted: false,
        txnId: TransactionID(1)
      )

      let currentTime = Timestamp(100000) # 100 seconds later

      # Version is 99 seconds old, which is > 60 seconds max age
      check gc.shouldCollectVersion("key1", version, currentTime) == true

    test "should not collect recent version":
      let gc = GarbageCollector(
        policy: GCPolicy(
          minTimestamp: Timestamp(0),
          maxVersionsPerKey: 10,
          maxAgeMs: 60000,
          collectIntervalMs: 60000
        ),
        running: Atomic[bool](),
        stats: GCStats(),
        logger: nil
      )

      let version = MVCCValue(
        data: "test",
        timestamp: Timestamp(1000),
        isDeleted: false,
        txnId: TransactionID(1)
      )

      let currentTime = Timestamp(50000) # 50 seconds later

      # Version is 49 seconds old, which is < 60 seconds max age
      check gc.shouldCollectVersion("key1", version, currentTime) == false

    test "should collect version before min timestamp":
      let gc = GarbageCollector(
        policy: GCPolicy(
          minTimestamp: Timestamp(50000),
          maxVersionsPerKey: 10,
          maxAgeMs: 60000,
          collectIntervalMs: 60000
        ),
        running: Atomic[bool](),
        stats: GCStats(),
        logger: nil
      )

      let version = MVCCValue(
        data: "test",
        timestamp: Timestamp(1000),
        isDeleted: false,
        txnId: TransactionID(1)
      )

      let currentTime = Timestamp(100000)

      # Version timestamp (1000) < min timestamp (50000)
      check gc.shouldCollectVersion("key1", version, currentTime) == true

    test "GC stats comparison":
      let stats1 = GCStats(
        keysScanned: 10,
        versionsCollected: 5,
        bytesCollected: 1000,
        lastRunTime: Timestamp(1000),
        totalRunTimeMs: 100,
        runCount: 1
      )

      let stats2 = GCStats(
        keysScanned: 10,
        versionsCollected: 5,
        bytesCollected: 1000,
        lastRunTime: Timestamp(1000),
        totalRunTimeMs: 100,
        runCount: 1
      )

      let stats3 = GCStats(
        keysScanned: 20,
        versionsCollected: 10,
        bytesCollected: 2000,
        lastRunTime: Timestamp(2000),
        totalRunTimeMs: 200,
        runCount: 2
      )

      check stats1 == stats2
      check (stats1 == stats3) == false
