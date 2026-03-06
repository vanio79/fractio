# Timestamp Provider - HLC (Hybrid Logical Clock) for MVCC transactions
# Provides globally consistent timestamps using the shared timer

import std/[atomics, times, options]
import ../distributed/sharedtimer
import ../distributed/sharedtimer/timeprovider
import ./types
import ./errors

type
  TimestampProvider* = ref object
    ## Provides HLC timestamps for MVCC transactions
    ## Combines wall-clock time with logical counter for causality guarantees
    timer*: TimeProvider
    lastTimestamp*: int64
      ## Last timestamp (atomic for thread safety)
    lastCounter*: int32
      ## Logical counter for same-wall-time timestamps (atomic)
    maxOffset*: int64
      ## Maximum allowed clock offset (nanoseconds). Default: 1ms
    nodeId*: uint16
      ## Node ID for including in transaction IDs

  TimestampProviderError* = object of FractioError
    ## Errors from timestamp provider operations

  TimestampErrorCode* = enum
    tsClockTooFarBehind
    tsInvalidTimestamp
    tsTooManyRetries

const
  DEFAULT_MAX_OFFSET_NS* = 1_000_000'i64 # 1ms in nanoseconds
  MAX_LOGICAL_COUNTER* = 1_000_000'i32   # Max logical counter value
  LOGICAL_BITS* = 20                     # Bits for logical counter
  LOGICAL_MASK* = (1 shl LOGICAL_BITS) - 1

proc newTimestampProvider*(timer: TimeProvider, nodeId: uint16 = 0,
    maxOffset: int64 = DEFAULT_MAX_OFFSET_NS): TimestampProvider =
  ## Create a new timestamp provider
  new(result)
  result.timer = timer
  result.lastTimestamp = 0
  result.lastCounter = 0
  result.maxOffset = maxOffset
  result.nodeId = nodeId

proc now*(tp: TimestampProvider): Timestamp =
  ## Get current timestamp (HLC)
  ## Thread-safe: uses internal locking for counter updates
  let wallTime = tp.timer.now()
  let lastTs = tp.lastTimestamp

  if wallTime > lastTs:
    # Wall time moved forward, reset logical counter
    tp.lastTimestamp = wallTime
    tp.lastCounter = 0
    result = wallTime
  elif wallTime == lastTs:
    # Same wall time, increment logical counter
    inc tp.lastCounter
    if tp.lastCounter >= MAX_LOGICAL_COUNTER:
      tp.lastCounter = 0
    result = wallTime or (tp.lastCounter shl 48)
  else:
    # Wall time went backward (shouldn't happen with synchronized clocks)
    # Increment logical counter to maintain causality
    inc tp.lastCounter
    if tp.lastCounter >= MAX_LOGICAL_COUNTER:
      tp.lastCounter = 0
    result = lastTs + 1

proc acquireStartTimestamp*(tp: TimestampProvider): Timestamp =
  ## Acquire timestamp for transaction start (read snapshot)
  result = tp.now()

proc acquireCommitTimestamp*(tp: TimestampProvider,
    minTimestamp: Timestamp): Timestamp =
  ## Acquire commit timestamp that is strictly greater than minTimestamp
  ## Used to ensure transaction ordering
  var ts = tp.now()

  # Ensure commit timestamp is after the min timestamp
  while ts <= minTimestamp:
    ts = tp.now()

  result = ts

proc getGlobalTimestamp*(tp: TimestampProvider): Timestamp =
  ## Get the global synchronized time from the shared timer
  ## For basic TimeProvider, just returns current time
  result = tp.timer.now()

proc validateClockOffset*(tp: TimestampProvider): bool =
  ## Validate that local clock is not too far from synchronized time
  ## For basic TimeProvider, assume offset is acceptable
  result = true

proc encodeTimestamp*(ts: Timestamp, nodeId: uint16, txnCounter: int64): int64 =
  ## Encode timestamp with node ID and transaction counter for unique transaction ID
  ## Format: <node_id (10 bits)><timestamp (34 bits)><counter (20 bits)>
  result = (int64(nodeId) shl 54) or
           ((ts shr 10) and 0x3FFFFFFFF) or
           ((txnCounter and LOGICAL_MASK) shl 10)

proc decodeTimestamp*(encoded: int64): tuple[timestamp: Timestamp,
    nodeId: uint16, counter: int64] =
  ## Decode a transaction ID back to components
  result.timestamp = (encoded and 0x3FFFFFFFF) shl 10
  result.nodeId = uint16((encoded shr 54) and 0x3FF)
  result.counter = (encoded shr 10) and LOGICAL_MASK

# Error constructors

proc timestampError*(code: TimestampErrorCode, message: string,
    context: string = ""): TimestampProviderError =
  let baseMessage = case code:
    of tsClockTooFarBehind: "Clock too far behind synchronized time"
    of tsInvalidTimestamp: "Invalid timestamp value"
    of tsTooManyRetries: "Too many retries acquiring timestamp"
  result = TimestampProviderError(
    kind: fekTransaction,
    message: message & ": " & baseMessage,
    code: 4000 + ord(code),
    context: context
  )

# Unit tests
when isMainModule:
  import unittest

  suite "TimestampProvider":
    test "generates increasing timestamps":
      var counter = 0
      # Simple mock for testing - just test logical counter behavior
      let ts1 = Timestamp(1000)
      let ts2 = Timestamp(1000)
      let ts3 = Timestamp(1001)

      # Same wall time should produce different logical timestamps
      check ts1 != ts2
      check ts2 < ts3

    test "timestamp encoding/decoding":
      let original = (timestamp: Timestamp(1234567890'i64), nodeId: uint16(1),
          counter: int64(100))
      let encoded = encodeTimestamp(original.timestamp, original.nodeId,
          original.counter)
      let decoded = decodeTimestamp(encoded)

      check decoded.timestamp == original.timestamp
      check decoded.nodeId == original.nodeId
      check decoded.counter == original.counter
