# Unit tests for MVCC Garbage Collector

import unittest
import std/[times, sets, tables, atomics]
import fractio/core/types
import fractio/storage/mvcc/types
import fractio/storage/mvcc/garbage_collector

# Constants
const
  INVALID_TIMESTAMP* = Timestamp(0)
  MAX_TIMESTAMP* = high(Timestamp)
  DEFAULT_MAX_VERSIONS_PER_KEY* = 10
  DEFAULT_MAX_AGE_MS* = 300_000
  DEFAULT_COLLECT_INTERVAL_MS* = 60_000

suite "MVCC Garbage Collector - GC Policy":
  test "create default GC policy":
    let policy = newGCPolicy()

    check policy.minTimestamp == Timestamp(0)
    check policy.maxVersionsPerKey == DEFAULT_MAX_VERSIONS_PER_KEY
    check policy.maxAgeMs == DEFAULT_MAX_AGE_MS
    check policy.collectIntervalMs == DEFAULT_COLLECT_INTERVAL_MS

  test "create custom GC policy with all parameters":
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

  test "create custom GC policy with partial parameters":
    let policy = newGCPolicy(
      minTimestamp = Timestamp(5000),
      maxVersionsPerKey = 3
    )

    check policy.minTimestamp == Timestamp(5000)
    check policy.maxVersionsPerKey == 3
    check policy.maxAgeMs == DEFAULT_MAX_AGE_MS
    check policy.collectIntervalMs == DEFAULT_COLLECT_INTERVAL_MS

suite "MVCC Garbage Collector - GC Stats":
  test "initialize GC stats":
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
    check stats.lastRunTime == Timestamp(0)
    check stats.totalRunTimeMs == 0
    check stats.runCount == 0

  test "GC stats with values":
    let stats = GCStats(
      keysScanned: 100,
      versionsCollected: 50,
      bytesCollected: 10000,
      lastRunTime: Timestamp(1000000),
      totalRunTimeMs: 5000,
      runCount: 10
    )

    check stats.keysScanned == 100
    check stats.versionsCollected == 50
    check stats.bytesCollected == 10000
    check stats.lastRunTime == Timestamp(1000000)
    check stats.totalRunTimeMs == 5000
    check stats.runCount == 10

  test "GC stats comparison - equal":
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

    check stats1 == stats2

  test "GC stats comparison - not equal":
    let stats1 = GCStats(
      keysScanned: 10,
      versionsCollected: 5,
      bytesCollected: 1000,
      lastRunTime: Timestamp(1000),
      totalRunTimeMs: 100,
      runCount: 1
    )

    let stats2 = GCStats(
      keysScanned: 20,
      versionsCollected: 10,
      bytesCollected: 2000,
      lastRunTime: Timestamp(2000),
      totalRunTimeMs: 200,
      runCount: 2
    )

    check (stats1 == stats2) == false

  test "GC stats comparison - different fields":
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
      versionsCollected: 6, # Different
      bytesCollected: 1000,
      lastRunTime: Timestamp(1000),
      totalRunTimeMs: 100,
      runCount: 1
    )

    check (stats1 == stats2) == false

suite "MVCC Garbage Collector - GC Result":
  test "initialize successful GC result":
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
    check result.error == ""

  test "initialize failed GC result":
    let result = GCResult(
      success: false,
      keysScanned: 0,
      versionsCollected: 0,
      bytesCollected: 0,
      error: "Collection failed"
    )

    check result.success == false
    check result.keysScanned == 0
    check result.versionsCollected == 0
    check result.bytesCollected == 0
    check result.error == "Collection failed"

  test "GC result with partial success":
    let result = GCResult(
      success: true,
      keysScanned: 5,
      versionsCollected: 2,
      bytesCollected: 500,
      error: ""
    )

    check result.success == true
    check result.keysScanned == 5
    check result.versionsCollected == 2
    check result.bytesCollected == 500

suite "MVCC Garbage Collector - Version Collection Logic":
  test "should collect old version based on age":
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

    let currentTime = Timestamp(100_000_000_000) # 100 seconds later in nanoseconds

    # Version is 99.999 seconds old, which is > 60 seconds max age
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

    let currentTime = Timestamp(50_000_000_000) # 50 seconds later in nanoseconds

    # Version is 49.999 seconds old, which is < 60 seconds max age
    check gc.shouldCollectVersion("key1", version, currentTime) == false

  test "should collect version before min timestamp":
    let gc = GarbageCollector(
      policy: GCPolicy(
        minTimestamp: Timestamp(50_000_000_000),
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

    let currentTime = Timestamp(100_000_000_000)

    # Version timestamp (1000) < min timestamp (50_000_000_000)
    check gc.shouldCollectVersion("key1", version, currentTime) == true

  test "should not collect version after min timestamp":
    let gc = GarbageCollector(
      policy: GCPolicy(
        minTimestamp: Timestamp(50_000_000_000),
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
      timestamp: Timestamp(60_000_000_000),
      isDeleted: false,
      txnId: TransactionID(1)
    )

    let currentTime = Timestamp(100_000_000_000)

    # Version timestamp (60_000_000_000) > min timestamp (50_000_000_000)
    # Age is 40 seconds, which is < 60 seconds max age
    check gc.shouldCollectVersion("key1", version, currentTime) == false

  test "should collect deleted version":
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
      data: "",
      timestamp: Timestamp(1000),
      isDeleted: true,
      txnId: TransactionID(1)
    )

    let currentTime = Timestamp(100_000_000_000)

    # Deleted version is old enough to collect
    check gc.shouldCollectVersion("key1", version, currentTime) == true

suite "MVCC Garbage Collector - Constants":
  test "default max versions per key":
    check DEFAULT_MAX_VERSIONS_PER_KEY == 10

  test "default max age":
    check DEFAULT_MAX_AGE_MS == 300_000 # 5 minutes

  test "default collect interval":
    check DEFAULT_COLLECT_INTERVAL_MS == 60_000 # 1 minute

suite "MVCC Garbage Collector - Policy Validation":
  test "policy with zero max versions":
    let policy = GCPolicy(
      minTimestamp: Timestamp(0),
      maxVersionsPerKey: 0,
      maxAgeMs: 60000,
      collectIntervalMs: 60000
    )

    check policy.maxVersionsPerKey == 0

  test "policy with large max versions":
    let policy = GCPolicy(
      minTimestamp: Timestamp(0),
      maxVersionsPerKey: 1000,
      maxAgeMs: 60000,
      collectIntervalMs: 60000
    )

    check policy.maxVersionsPerKey == 1000

  test "policy with zero max age":
    let policy = GCPolicy(
      minTimestamp: Timestamp(0),
      maxVersionsPerKey: 10,
      maxAgeMs: 0,
      collectIntervalMs: 60000
    )

    check policy.maxAgeMs == 0

  test "policy with large max age":
    let policy = GCPolicy(
      minTimestamp: Timestamp(0),
      maxVersionsPerKey: 10,
      maxAgeMs: 1_000_000_000,
      collectIntervalMs: 60000
    )

    check policy.maxAgeMs == 1_000_000_000

  test "policy with zero collect interval":
    let policy = GCPolicy(
      minTimestamp: Timestamp(0),
      maxVersionsPerKey: 10,
      maxAgeMs: 60000,
      collectIntervalMs: 0
    )

    check policy.collectIntervalMs == 0

  test "policy with large collect interval":
    let policy = GCPolicy(
      minTimestamp: Timestamp(0),
      maxVersionsPerKey: 10,
      maxAgeMs: 60000,
      collectIntervalMs: 1_000_000_000
    )

    check policy.collectIntervalMs == 1_000_000_000

suite "MVCC Garbage Collector - Timestamp Calculations":
  test "age calculation for old version":
    let versionTimestamp = Timestamp(1000)
    let currentTime = Timestamp(100_000_000_000) # 99.999 seconds later
    let ageMs = (currentTime - versionTimestamp) div 1_000_000

    check ageMs == 99999

  test "age calculation for recent version":
    let versionTimestamp = Timestamp(1000)
    let currentTime = Timestamp(50_000_000_000) # 49.999 seconds later
    let ageMs = (currentTime - versionTimestamp) div 1_000_000

    check ageMs == 49999

  test "age calculation for very old version":
    let versionTimestamp = Timestamp(1000)
    let currentTime = Timestamp(1_000_000_000_000) # ~1000 seconds later
    let ageMs = (currentTime - versionTimestamp) div 1_000_000

    check ageMs == 999999

  test "age calculation for same timestamp":
    let versionTimestamp = Timestamp(1000)
    let currentTime = Timestamp(1000)
    let ageMs = (currentTime - versionTimestamp) div 1_000_000

    check ageMs == 0

suite "MVCC Garbage Collector - Edge Cases":
  test "version at exact max age boundary":
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

    let currentTime = Timestamp(61_000_000_000) # Exactly 60 seconds later

    # Version is exactly 60 seconds old
    check gc.shouldCollectVersion("key1", version, currentTime) == true

  test "version just before max age boundary":
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

    let currentTime = Timestamp(60_999_000_000) # 60.999 seconds later

    # Version is 60.998 seconds old, which is > 60 seconds
    check gc.shouldCollectVersion("key1", version, currentTime) == true

  test "version at exact min timestamp boundary":
    let gc = GarbageCollector(
      policy: GCPolicy(
        minTimestamp: Timestamp(50_000_000_000),
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
      timestamp: Timestamp(50_000_000_000),
      isDeleted: false,
      txnId: TransactionID(1)
    )

    let currentTime = Timestamp(100_000_000_000)

    # Version timestamp equals min timestamp
    check gc.shouldCollectVersion("key1", version, currentTime) == false

  test "version just before min timestamp boundary":
    let gc = GarbageCollector(
      policy: GCPolicy(
        minTimestamp: Timestamp(50_000_000_000),
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
      timestamp: Timestamp(49999),
      isDeleted: false,
      txnId: TransactionID(1)
    )

    let currentTime = Timestamp(100_000_000_000)

    # Version timestamp < min timestamp
    check gc.shouldCollectVersion("key1", version, currentTime) == true

suite "MVCC Garbage Collector - Multiple Conditions":
  test "version fails both age and min timestamp checks":
    let gc = GarbageCollector(
      policy: GCPolicy(
        minTimestamp: Timestamp(50_000_000_000),
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

    let currentTime = Timestamp(100_000_000_000)

    # Version is both too old and before min timestamp
    check gc.shouldCollectVersion("key1", version, currentTime) == true

  test "version passes both age and min timestamp checks":
    let gc = GarbageCollector(
      policy: GCPolicy(
        minTimestamp: Timestamp(50_000_000_000),
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
      timestamp: Timestamp(60_000_000_000),
      isDeleted: false,
      txnId: TransactionID(1)
    )

    let currentTime = Timestamp(100_000_000_000)

    # Version is recent enough and after min timestamp
    check gc.shouldCollectVersion("key1", version, currentTime) == false

  test "version fails age check but passes min timestamp check":
    let gc = GarbageCollector(
      policy: GCPolicy(
        minTimestamp: Timestamp(50_000_000_000),
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
      timestamp: Timestamp(60_000_000_000),
      isDeleted: false,
      txnId: TransactionID(1)
    )

    let currentTime = Timestamp(200_000_000_000) # 140 seconds later

    # Version is too old but after min timestamp
    check gc.shouldCollectVersion("key1", version, currentTime) == true

  test "version passes age check but fails min timestamp check":
    let gc = GarbageCollector(
      policy: GCPolicy(
        minTimestamp: Timestamp(50_000_000_000),
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

    let currentTime = Timestamp(60000) # 59 seconds later

    # Version is recent enough but before min timestamp
    check gc.shouldCollectVersion("key1", version, currentTime) == true
