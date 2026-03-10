# DistSender - Distributed Request Router
#
# This module implements the DistSender which routes requests to the correct
# range leaseholders. It handles:
# - Request routing via range cache
# - Retry logic with exponential backoff
# - Request splitting by range
# - Leader redirect handling

import std/options
import std/tables
import std/locks
import std/atomics

import fractio/distributed/raft/group_types
import fractio/distributed/meta/types
import fractio/distributed/meta/lookup

# ============================================================================
# Constants
# ============================================================================

const
  DEFAULT_MAX_RETRIES* = 5
    ## Default maximum retry attempts

  DEFAULT_RETRY_BASE_NS* = 100_000_000'i64
    ## Default base retry delay: 100ms

  DEFAULT_RETRY_MAX_NS* = 10_000_000_000'i64
    ## Default maximum retry delay: 10s

  DEFAULT_SEND_TIMEOUT_NS* = 30_000_000_000'i64
    ## Default send timeout: 30s

# ============================================================================
# Error Types
# ============================================================================

type
  SendError* = object of CatchableError
    ## Base error for send operations

  NotLeaderError* = object of SendError
    ## Current node is not the leader
    leaderHint*: NodeID
      ## Hint about who the actual leader is
    groupId*: GroupID
      ## The range that has a different leader

  GroupUnavailableError* = object of SendError
    ## Range is unavailable (no replicas reachable)
    groupId*: GroupID

  SendTimeoutError* = object of SendError
    ## Send operation timed out
    groupId*: GroupID

proc newNotLeaderError*(groupId: GroupID,
    leaderHint: NodeID): ref NotLeaderError =
  ## Create a not-leader error
  new(result)
  result.groupId = groupId
  result.leaderHint = leaderHint
  result.msg = "Not leader for range " & $groupId & ", leader is " & $leaderHint

proc newGroupUnavailableError*(groupId: GroupID): ref GroupUnavailableError =
  ## Create a range unavailable error
  new(result)
  result.groupId = groupId
  result.msg = "Range " & $groupId & " is unavailable"

proc newSendTimeoutError*(groupId: GroupID): ref SendTimeoutError =
  ## Create a send timeout error
  new(result)
  result.groupId = groupId
  result.msg = "Send to range " & $groupId & " timed out"

# ============================================================================
# Request/Response Types
# ============================================================================

type
  RequestKind* = enum
    ## Kind of KV request
    rkGet
    rkPut
    rkDelete
    rkScan
    rkEndTxn

  KVRequest* = object
    ## A single KV request
    case kind*: RequestKind
    of rkGet:
      getKey*: seq[byte]
    of rkPut:
      putKey*: seq[byte]
      putValue*: seq[byte]
    of rkDelete:
      deleteKey*: seq[byte]
    of rkScan:
      scanStart*: seq[byte]
      scanEnd*: seq[byte]
      scanLimit*: int64
    of rkEndTxn:
      commit*: bool

  KVResponse* = object
    ## Response to a KV request
    case kind*: RequestKind
    of rkGet:
      getValue*: Option[seq[byte]]
    of rkPut:
      putSuccess*: bool
    of rkDelete:
      deleteSuccess*: bool
    of rkScan:
      scanKeys*: seq[seq[byte]]
      scanValues*: seq[Option[seq[byte]]]
    of rkEndTxn:
      commitSuccess*: bool

  BatchRequest* = object
    ## A batch of KV requests
    requests*: seq[KVRequest]
    timestampNs*: int64
    priority*: int32

  BatchResponse* = object
    ## Response to a batch request
    responses*: seq[KVResponse]
    error*: Option[string]

  GroupRequest* = object
    ## Request to a single range
    groupId*: GroupID
    requests*: seq[KVRequest]
    timestampNs*: int64
    priority*: int32

  GroupResponse* = object
    ## Response from a single range
    groupId*: GroupID
    responses*: seq[KVResponse]
    error*: Option[string]
    leaderHint*: Option[NodeID]

# ============================================================================
# Request Constructors
# ============================================================================

proc newGetRequest*(key: seq[byte]): KVRequest =
  ## Create a get request
  result = KVRequest(kind: rkGet, getKey: key)

proc newPutRequest*(key, value: seq[byte]): KVRequest =
  ## Create a put request
  result = KVRequest(kind: rkPut, putKey: key, putValue: value)

proc newDeleteRequest*(key: seq[byte]): KVRequest =
  ## Create a delete request
  result = KVRequest(kind: rkDelete, deleteKey: key)

proc newScanRequest*(startKey, endKey: seq[byte], limit: int64): KVRequest =
  ## Create a scan request
  result = KVRequest(kind: rkScan, scanStart: startKey, scanEnd: endKey,
      scanLimit: limit)

proc newEndTxnRequest*(commit: bool): KVRequest =
  ## Create an end transaction request
  result = KVRequest(kind: rkEndTxn, commit: commit)

proc newBatchRequest*(requests: seq[KVRequest], timestampNs: int64,
                      priority: int32 = 0): BatchRequest =
  ## Create a batch request
  result = BatchRequest(
    requests: requests,
    timestampNs: timestampNs,
    priority: priority
  )

# ============================================================================
# Response Constructors
# ============================================================================

proc newGetResponse*(value: Option[seq[byte]]): KVResponse =
  ## Create a get response
  result = KVResponse(kind: rkGet, getValue: value)

proc newPutResponse*(success: bool): KVResponse =
  ## Create a put response
  result = KVResponse(kind: rkPut, putSuccess: success)

proc newDeleteResponse*(success: bool): KVResponse =
  ## Create a delete response
  result = KVResponse(kind: rkDelete, deleteSuccess: success)

proc newScanResponse*(keys: seq[seq[byte]],
                      values: seq[Option[seq[byte]]]): KVResponse =
  ## Create a scan response
  result = KVResponse(kind: rkScan, scanKeys: keys, scanValues: values)

proc newEndTxnResponse*(success: bool): KVResponse =
  ## Create an end transaction response
  result = KVResponse(kind: rkEndTxn, commitSuccess: success)

# ============================================================================
# DistSender
# ============================================================================

type
  SendCallback* = proc(req: GroupRequest): GroupResponse {.closure, gcsafe.}
    ## Callback to send a request to a node

  DistSender* = ref object
    ## Distributes requests across ranges
    lookup*: GroupLookup
      ## Range lookup handler

    # Configuration
    maxRetries*: int
    retryBaseNs*: int64
    retryMaxNs*: int64
    sendTimeoutNs*: int64

    # Statistics
    sendsAttempted*: Atomic[int64]
    sendsSucceeded*: Atomic[int64]
    sendsRetried*: Atomic[int64]
    sendsFailed*: Atomic[int64]

    # Send callback
    sendCallback*: SendCallback
    lock*: Lock

proc newDistSender*(lookup: GroupLookup,
                    callback: SendCallback): DistSender =
  ## Create a new DistSender
  new(result)
  result.lookup = lookup
  result.maxRetries = DEFAULT_MAX_RETRIES
  result.retryBaseNs = DEFAULT_RETRY_BASE_NS
  result.retryMaxNs = DEFAULT_RETRY_MAX_NS
  result.sendTimeoutNs = DEFAULT_SEND_TIMEOUT_NS
  result.sendCallback = callback
  result.sendsAttempted.store(0)
  result.sendsSucceeded.store(0)
  result.sendsRetried.store(0)
  result.sendsFailed.store(0)
  initLock(result.lock)

proc destroy*(sender: DistSender) =
  ## Clean up resources
  deinitLock(sender.lock)

# ============================================================================
# Request Splitting
# ============================================================================

proc splitByGroup*(sender: DistSender, batch: BatchRequest,
                   nowNs: int64): seq[GroupRequest] =
  ## Split a batch request by range
  ## Groups requests by their target range

  var rangeGroups = initTable[GroupID, seq[KVRequest]]()
  var rangeOrder: seq[GroupID] = @[]

  for req in batch.requests:
    # Get the key for this request
    var key: seq[byte]
    case req.kind
    of rkGet:
      key = req.getKey
    of rkPut:
      key = req.putKey
    of rkDelete:
      key = req.deleteKey
    of rkScan:
      key = req.scanStart
    of rkEndTxn:
      # EndTxn doesn't have a key - use first range
      continue

    # Look up the range for this key
    let lookupResp = sender.lookup.fullLookup(key, nowNs)
    if not lookupResp.found:
      continue

    let groupId = lookupResp.descriptor.groupId

    if not rangeGroups.contains(groupId):
      rangeGroups[groupId] = @[]
      rangeOrder.add(groupId)

    rangeGroups[groupId].add(req)

  # Build range requests
  for groupId in rangeOrder:
    result.add(GroupRequest(
      groupId: groupId,
      requests: rangeGroups[groupId],
      timestampNs: batch.timestampNs,
      priority: batch.priority
    ))

proc mergeResponses*(sender: DistSender,
                     responses: seq[GroupResponse]): BatchResponse =
  ## Merge range responses into a batch response
  var allResponses: seq[KVResponse] = @[]
  var hadError = false
  var errorMsg = ""

  for resp in responses:
    if resp.error.isSome:
      hadError = true
      errorMsg = resp.error.get
    else:
      allResponses.add(resp.responses)

  if hadError:
    result = BatchResponse(responses: allResponses, error: some(errorMsg))
  else:
    result = BatchResponse(responses: allResponses, error: none(string))

# ============================================================================
# Retry Logic
# ============================================================================

proc calculateBackoff*(sender: DistSender, attempt: int): int64 =
  ## Calculate exponential backoff delay
  ## Uses: base * 2^attempt, capped at max
  let exp = min(attempt, 30) # Prevent overflow
  let delay = sender.retryBaseNs * (1'i64 shl exp)
  result = min(delay, sender.retryMaxNs)

proc shouldRetry*(sender: DistSender, err: ref SendError,
                  attempt: int): bool =
  ## Check if we should retry after an error
  if attempt >= sender.maxRetries:
    return false

  # Retry on NotLeaderError (with redirect)
  if err of NotLeaderError:
    return true

  # Retry on GroupUnavailableError (might come back)
  if err of GroupUnavailableError:
    return attempt < sender.maxRetries div 2 # Fewer retries for unavailable
  
  # Retry on SendTimeoutError
  if err of SendTimeoutError:
    return true

  return false

# ============================================================================
# Send Logic
# ============================================================================

proc sendToGroup*(sender: DistSender, req: GroupRequest,
                  nowNs: int64): GroupResponse =
  ## Send a request to a specific range with retry logic

  var attempt = 0
  var lastError: ref SendError = nil

  discard sender.sendsAttempted.fetchAdd(1)

  while attempt <= sender.maxRetries:
    # Get leaseholder for this range
    let leaseholderOpt = sender.lookup.getLeaseholder(req.groupId, nowNs)
    if leaseholderOpt.isNone:
      lastError = newGroupUnavailableError(req.groupId)
      if not sender.shouldRetry(lastError, attempt):
        discard sender.sendsFailed.fetchAdd(1)
        return GroupResponse(
          groupId: req.groupId,
          responses: @[],
          error: some("Group unavailable: " & $req.groupId)
        )

      # Wait before retry
      # let backoff = sender.calculateBackoff(attempt)
      # In production, would sleep here
      inc attempt
      discard sender.sendsRetried.fetchAdd(1)
      continue

    # let leaseholder = leaseholderOpt.get

    # Send request
    try:
      let resp = sender.sendCallback(req)

      # Check for leader redirect
      if resp.leaderHint.isSome:
        # Update cache with new leader hint
        # In production, would update leaseholder info
        lastError = newNotLeaderError(req.groupId, resp.leaderHint.get)
        if not sender.shouldRetry(lastError, attempt):
          discard sender.sendsFailed.fetchAdd(1)
          return GroupResponse(
            groupId: req.groupId,
            responses: @[],
            error: some("Not leader: " & $resp.leaderHint.get),
            leaderHint: resp.leaderHint
          )

        inc attempt
        discard sender.sendsRetried.fetchAdd(1)
        continue

      # Success
      discard sender.sendsSucceeded.fetchAdd(1)
      return resp

    except SendError as e:
      lastError = e
      if not sender.shouldRetry(lastError, attempt):
        discard sender.sendsFailed.fetchAdd(1)
        return GroupResponse(
          groupId: req.groupId,
          responses: @[],
          error: some(e.msg)
        )

      inc attempt
      discard sender.sendsRetried.fetchAdd(1)

  # All retries exhausted
  discard sender.sendsFailed.fetchAdd(1)
  return GroupResponse(
    groupId: req.groupId,
    responses: @[],
    error: some(if lastError != nil: lastError.msg else: "Unknown error")
  )

proc send*(sender: DistSender, batch: BatchRequest,
           nowNs: int64): BatchResponse =
  ## Send a batch request, splitting by range and handling retries

  # Split batch by range
  let rangeReqs = sender.splitByGroup(batch, nowNs)

  if rangeReqs.len == 0:
    return BatchResponse(responses: @[], error: some("No valid ranges found"))

  # Send to each range
  var rangeResponses: seq[GroupResponse] = @[]
  for req in rangeReqs:
    rangeResponses.add(sender.sendToGroup(req, nowNs))

  # Merge responses
  return sender.mergeResponses(rangeResponses)

# ============================================================================
# Statistics
# ============================================================================

type
  SenderStats* = object
    ## Statistics for the DistSender
    sendsAttempted*: int64
    sendsSucceeded*: int64
    sendsRetried*: int64
    sendsFailed*: int64
    successRate*: float64

proc getStats*(sender: DistSender): SenderStats =
  ## Get sender statistics
  result.sendsAttempted = sender.sendsAttempted.load()
  result.sendsSucceeded = sender.sendsSucceeded.load()
  result.sendsRetried = sender.sendsRetried.load()
  result.sendsFailed = sender.sendsFailed.load()

  let total = result.sendsAttempted
  if total > 0:
    result.successRate = float64(result.sendsSucceeded) / float64(total)
  else:
    result.successRate = 0.0
