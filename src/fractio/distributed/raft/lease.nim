# Lease Management for Multi-Group Raft
#
# This module implements leader lease management using store liveness.
# Leases are acquired through Raft and validated against store liveness support.
#
# Key concept: The leaseholder is always the Raft leader. A lease is valid
# only if the leader's store liveness is supported by a quorum of voters.

import std/atomics
import std/locks
import std/options
import std/times
import std/tables
import std/json

import fractio/distributed/range/types
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/liveness
import fractio/distributed/raft/multigroup_log
import fractio/utils/logging

# ============================================================================
# Constants
# ============================================================================

const
  DEFAULT_LEASE_DURATION_NS* = 3_000_000_000'i64 # 3 seconds
  MIN_LEASE_DURATION_NS* = 1_000_000_000'i64     # 1 second minimum
  MAX_LEASE_DURATION_NS* = 30_000_000_000'i64    # 30 seconds maximum
  LEASE_RENEWAL_MARGIN_NS* = 500_000_000'i64     # Renew 500ms before expiration

# ============================================================================
# Lease State Machine
# ============================================================================

type
  LeaseManager* = ref object
    ## Manages leases for a single Raft group

    rangeId*: RangeID
    nodeId*: RangeNodeID

    # Current lease state
    lease*: Atomic[Lease]
    leaseState*: Atomic[LeaseState]

    # Store liveness reference
    storeLiveness*: StoreLiveness

    # Configuration
    leaseDurationNs*: int64

    # Synchronization
    lock*: Lock

    # Pending lease operations
    pendingTransfer*: Option[RangeNodeID]
    transferComplete*: bool

  LeaseAcquisitionResult* = object
    ## Result of lease acquisition attempt
    success*: bool
    lease*: Option[Lease]
    error*: string

  LeaseValidationResult* = object
    ## Result of lease validation
    valid*: bool
    reason*: string

# ============================================================================
# Lease Manager Operations
# ============================================================================

proc newLeaseManager*(rangeId: RangeID, nodeId: RangeNodeID,
                       storeLiveness: StoreLiveness,
                       leaseDurationNs = DEFAULT_LEASE_DURATION_NS): LeaseManager =
  ## Create a new lease manager
  new(result)
  result.rangeId = rangeId
  result.nodeId = nodeId
  result.storeLiveness = storeLiveness
  result.leaseDurationNs = leaseDurationNs

  # Initialize atomic state
  result.lease.store(Lease())
  result.leaseState.store(lsNone)

  initLock(result.lock)

proc close*(lm: LeaseManager) =
  ## Clean up resources
  deinitLock(lm.lock)

# ============================================================================
# Lease Acquisition
# ============================================================================

proc canAcquireLease*(lm: LeaseManager, voters: seq[ReplicaDescriptor]): bool =
  ## Check if we can acquire a lease
  ## We need:
  ## 1. To be the Raft leader (checked by caller)
  ## 2. Store liveness support from a quorum of voters

  result = lm.storeLiveness.canAcquireLease(voters)

proc proposeLeaseAcquisition*(lm: LeaseManager,
    group: RaftGroup): LeaseAcquisitionResult =
  ## Propose lease acquisition through Raft.
  ## This should be called by the Raft leader.

  if lm.leaseState.load() == lsHeld:
    # Already have a valid lease
    let current = lm.lease.load()
    return LeaseAcquisitionResult(
      success: true,
      lease: some(current),
      error: ""
    )

  if lm.leaseState.load() == lsAcquiring:
    return LeaseAcquisitionResult(
      success: false,
      lease: none(Lease),
      error: "Lease acquisition already in progress"
    )

  # Check store liveness support
  let voters = group.descriptor.getVoters()
  if not lm.canAcquireLease(voters):
    return LeaseAcquisitionResult(
      success: false,
      lease: none(Lease),
      error: "Insufficient store liveness support"
    )

  # Create lease
  let now = getTime().toUnix * 1_000_000_000
  let newLease = Lease(
    leaseholder: lm.nodeId,
    startTs: now,
    expirationTs: now + lm.leaseDurationNs,
    epoch: lm.storeLiveness.getEpoch()
  )

  # Update state
  lm.leaseState.store(lsAcquiring)
  lm.lease.store(newLease)

  # In a real implementation, we would propose this through Raft
  # For now, we just update the state
  lm.leaseState.store(lsHeld)

  var fields = initTable[string, string]()
  fields["rangeId"] = $lm.rangeId
  fields["nodeId"] = $lm.nodeId
  fields["expirationNs"] = $newLease.expirationTs
  info("Acquired lease", fields)

  return LeaseAcquisitionResult(
    success: true,
    lease: some(newLease),
    error: ""
  )

# ============================================================================
# Lease Validation
# ============================================================================

proc validateLease*(lm: LeaseManager, voters: seq[
    ReplicaDescriptor]): LeaseValidationResult =
  ## Validate that the current lease is still valid.
  ## A lease is valid if:
  ## 1. It hasn't expired
  ## 2. The leaseholder's store liveness is still supported by a quorum

  let current = lm.lease.load()
  let state = lm.leaseState.load()

  if state != lsHeld:
    return LeaseValidationResult(
      valid: false,
      reason: "No lease held"
    )

  let now = getTime().toUnix * 1_000_000_000

  # Check expiration
  if now >= current.expirationTs:
    lm.leaseState.store(lsExpired)
    return LeaseValidationResult(
      valid: false,
      reason: "Lease has expired"
    )

  # Check store liveness support
  if not lm.storeLiveness.canAcquireLease(voters):
    lm.leaseState.store(lsExpired)
    return LeaseValidationResult(
      valid: false,
      reason: "Lost store liveness support"
    )

  return LeaseValidationResult(
    valid: true,
    reason: ""
  )

proc isLeaseValid*(lm: LeaseManager, voters: seq[ReplicaDescriptor]): bool =
  ## Quick check if lease is valid
  let result = lm.validateLease(voters)
  return result.valid

proc timeUntilExpiration*(lm: LeaseManager): int64 =
  ## Get time until lease expires (nanoseconds)
  let current = lm.lease.load()
  let now = getTime().toUnix * 1_000_000_000
  result = current.expirationTs - now
  if result < 0:
    result = 0

proc shouldRenewLease*(lm: LeaseManager): bool =
  ## Check if lease should be renewed
  let timeLeft = lm.timeUntilExpiration()
  result = timeLeft < LEASE_RENEWAL_MARGIN_NS and timeLeft > 0

# ============================================================================
# Lease Renewal
# ============================================================================

proc renewLease*(lm: LeaseManager, group: RaftGroup): LeaseAcquisitionResult =
  ## Renew the current lease
  ## This extends the lease expiration

  if lm.leaseState.load() != lsHeld:
    return LeaseAcquisitionResult(
      success: false,
      lease: none(Lease),
      error: "No lease to renew"
    )

  let voters = group.descriptor.getVoters()
  if not lm.canAcquireLease(voters):
    lm.leaseState.store(lsExpired)
    return LeaseAcquisitionResult(
      success: false,
      lease: none(Lease),
      error: "Insufficient store liveness support for renewal"
    )

  # Extend lease
  let now = getTime().toUnix * 1_000_000_000
  var current = lm.lease.load()
  current.startTs = now
  current.expirationTs = now + lm.leaseDurationNs
  current.epoch = lm.storeLiveness.getEpoch()

  lm.lease.store(current)

  var fields = initTable[string, string]()
  fields["rangeId"] = $lm.rangeId
  fields["nodeId"] = $lm.nodeId
  fields["newExpirationNs"] = $current.expirationTs
  debug("Renewed lease", fields)

  return LeaseAcquisitionResult(
    success: true,
    lease: some(current),
    error: ""
  )

# ============================================================================
# Lease Transfer
# ============================================================================

proc proposeLeaseTransfer*(lm: LeaseManager, target: RangeNodeID): bool =
  ## Propose transferring lease to another node.
  ## The transfer happens through Raft.

  if lm.leaseState.load() != lsHeld:
    return false

  if lm.pendingTransfer.isSome():
    return false # Transfer already in progress

  withLock lm.lock:
    lm.pendingTransfer = some(target)
    lm.transferComplete = false
    lm.leaseState.store(lsTransferring)

  var fields = initTable[string, string]()
  fields["rangeId"] = $lm.rangeId
  fields["target"] = $target
  info("Proposed lease transfer", fields)

  return true

proc completeLeaseTransfer*(lm: LeaseManager, target: RangeNodeID): bool =
  ## Complete a lease transfer to the target node.
  ## Called when the transfer command is applied.

  withLock lm.lock:
    if lm.pendingTransfer.isSome() and lm.pendingTransfer.get() == target:
      lm.transferComplete = true
      lm.leaseState.store(lsNone)
      lm.pendingTransfer = none(RangeNodeID)
      return true
  return false

proc cancelLeaseTransfer*(lm: LeaseManager) =
  ## Cancel a pending lease transfer
  withLock lm.lock:
    lm.pendingTransfer = none(RangeNodeID)
    lm.transferComplete = false
    lm.leaseState.store(lsHeld)

proc isTransferPending*(lm: LeaseManager): bool =
  ## Check if a transfer is pending
  lm.pendingTransfer.isSome()

# ============================================================================
# Lease Expiration Handling
# ============================================================================

proc expireLease*(lm: LeaseManager) =
  ## Mark the lease as expired
  lm.leaseState.store(lsExpired)

  var fields = initTable[string, string]()
  fields["rangeId"] = $lm.rangeId
  fields["nodeId"] = $lm.nodeId
  warn("Lease expired", fields)

proc clearExpiredLease*(lm: LeaseManager) =
  ## Clear an expired lease
  if lm.leaseState.load() == lsExpired:
    lm.leaseState.store(lsNone)
    lm.lease.store(Lease())

# ============================================================================
# Lease State Queries
# ============================================================================

proc getLeaseState*(lm: LeaseManager): LeaseState =
  ## Get current lease state
  lm.leaseState.load()

proc getLease*(lm: LeaseManager): Option[Lease] =
  ## Get current lease if held
  if lm.leaseState.load() == lsHeld:
    result = some(lm.lease.load())
  else:
    result = none(Lease)

proc isLeaseholder*(lm: LeaseManager): bool =
  ## Check if we are the leaseholder
  let state = lm.leaseState.load()
  result = state == lsHeld or state == lsTransferring

proc getLeaseholder*(lm: LeaseManager): Option[RangeNodeID] =
  ## Get the current leaseholder
  if lm.leaseState.load() == lsHeld:
    result = some(lm.lease.load().leaseholder)
  else:
    result = none(RangeNodeID)

# ============================================================================
# Lease Serialization
# ============================================================================

proc leaseToJson*(lease: Lease): JsonNode =
  ## Serialize lease to JSON
  %*{
    "leaseholder": lease.leaseholder.uint32,
    "startTs": lease.startTs,
    "expirationTs": lease.expirationTs,
    "epoch": lease.epoch
  }

proc parseLease*(json: JsonNode): Lease =
  ## Parse lease from JSON
  Lease(
    leaseholder: RangeNodeID(json["leaseholder"].getInt()),
    startTs: json["startTs"].getInt(),
    expirationTs: json["expirationTs"].getInt(),
    epoch: uint64(json["epoch"].getInt())
  )
