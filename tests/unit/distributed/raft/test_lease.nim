# Unit tests for Lease Management

import std/unittest
import std/atomics
import std/times
import std/options
import std/json

import fractio/distributed/range/types
import fractio/distributed/raft/multigroup_types
import fractio/distributed/raft/liveness
import fractio/distributed/raft/lease

suite "LeaseManager":
  test "create lease manager":
    let sl = newStoreLiveness(NodeID(1))
    let lm = newLeaseManager(RangeID(1), NodeID(1), sl)

    check lm.rangeId == RangeID(1)
    check lm.nodeId == NodeID(1)
    check lm.getLeaseState() == lsNone
    check not lm.isLeaseholder()

    lm.close()
    sl.close()

  test "lease acquisition":
    let sl = newStoreLiveness(NodeID(1))
    sl.registerStore(NodeID(1))
    sl.registerStore(NodeID(2))
    sl.registerStore(NodeID(3))

    let lm = newLeaseManager(RangeID(1), NodeID(1), sl)

    # Create a mock group with voters
    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    discard desc.addReplica(NodeID(3))
    let group = newRaftGroup(RangeID(1), NodeID(1), ReplicaID(1), desc)

    # Acquire lease
    let result = lm.proposeLeaseAcquisition(group)
    check result.success
    check result.lease.isSome
    check lm.getLeaseState() == lsHeld
    check lm.isLeaseholder()

    group.close()
    lm.close()
    sl.close()

  test "lease validation":
    let sl = newStoreLiveness(NodeID(1))
    sl.registerStore(NodeID(1))
    sl.registerStore(NodeID(2))
    sl.registerStore(NodeID(3))

    let lm = newLeaseManager(RangeID(1), NodeID(1), sl)

    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    discard desc.addReplica(NodeID(3))
    let group = newRaftGroup(RangeID(1), NodeID(1), ReplicaID(1), desc)

    # No lease initially
    var voters = desc.getVoters()
    var validation = lm.validateLease(voters)
    check not validation.valid

    # Acquire lease
    discard lm.proposeLeaseAcquisition(group)

    # Validate lease
    validation = lm.validateLease(voters)
    check validation.valid

    group.close()
    lm.close()
    sl.close()

  test "lease expiration":
    let sl = newStoreLiveness(NodeID(1))
    let lm = newLeaseManager(RangeID(1), NodeID(1), sl,
                              leaseDurationNs = 100_000_000) # 100ms

    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(NodeID(1))
    sl.registerStore(NodeID(1))

    let group = newRaftGroup(RangeID(1), NodeID(1), ReplicaID(1), desc)

    # Acquire lease
    discard lm.proposeLeaseAcquisition(group)
    check lm.getLeaseState() == lsHeld

    # Expire lease
    lm.expireLease()
    check lm.getLeaseState() == lsExpired
    check not lm.isLeaseholder()

    # Clear expired lease
    lm.clearExpiredLease()
    check lm.getLeaseState() == lsNone

    group.close()
    lm.close()
    sl.close()

  test "lease renewal":
    let sl = newStoreLiveness(NodeID(1))
    sl.registerStore(NodeID(1))
    sl.registerStore(NodeID(2))
    sl.registerStore(NodeID(3))

    let lm = newLeaseManager(RangeID(1), NodeID(1), sl)

    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(NodeID(1))
    discard desc.addReplica(NodeID(2))
    discard desc.addReplica(NodeID(3))
    let group = newRaftGroup(RangeID(1), NodeID(1), ReplicaID(1), desc)

    # Acquire lease
    discard lm.proposeLeaseAcquisition(group)
    let originalLease = lm.lease.load()

    # Renew lease
    let result = lm.renewLease(group)
    check result.success

    let renewedLease = lm.lease.load()
    # The renewed lease should have a valid expiration
    check renewedLease.expirationTs > 0
    check renewedLease.startTs >= originalLease.startTs

    group.close()
    lm.close()
    sl.close()

  test "lease transfer":
    let sl = newStoreLiveness(NodeID(1))
    sl.registerStore(NodeID(1))

    let lm = newLeaseManager(RangeID(1), NodeID(1), sl)

    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(NodeID(1))
    let group = newRaftGroup(RangeID(1), NodeID(1), ReplicaID(1), desc)

    # Acquire lease
    discard lm.proposeLeaseAcquisition(group)

    # Propose transfer
    check lm.proposeLeaseTransfer(NodeID(2))
    check lm.isTransferPending()
    check lm.getLeaseState() == lsTransferring

    # Complete transfer
    check lm.completeLeaseTransfer(NodeID(2))
    check not lm.isTransferPending()
    check lm.getLeaseState() == lsNone

    group.close()
    lm.close()
    sl.close()

  test "cancel lease transfer":
    let sl = newStoreLiveness(NodeID(1))
    sl.registerStore(NodeID(1))

    let lm = newLeaseManager(RangeID(1), NodeID(1), sl)

    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(NodeID(1))
    let group = newRaftGroup(RangeID(1), NodeID(1), ReplicaID(1), desc)

    # Acquire lease
    discard lm.proposeLeaseAcquisition(group)

    # Propose transfer
    discard lm.proposeLeaseTransfer(NodeID(2))

    # Cancel transfer
    lm.cancelLeaseTransfer()
    check not lm.isTransferPending()
    check lm.getLeaseState() == lsHeld

    group.close()
    lm.close()
    sl.close()

  test "time until expiration":
    let sl = newStoreLiveness(NodeID(1))
    sl.registerStore(NodeID(1))

    let lm = newLeaseManager(RangeID(1), NodeID(1), sl,
                              leaseDurationNs = 1_000_000_000) # 1 second

    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(NodeID(1))
    let group = newRaftGroup(RangeID(1), NodeID(1), ReplicaID(1), desc)

    # No lease initially
    check lm.timeUntilExpiration() == 0

    # Acquire lease
    discard lm.proposeLeaseAcquisition(group)

    # Should have time until expiration
    let timeLeft = lm.timeUntilExpiration()
    check timeLeft > 0
    check timeLeft <= 1_000_000_000

    group.close()
    lm.close()
    sl.close()

  test "should renew lease":
    let sl = newStoreLiveness(NodeID(1))
    sl.registerStore(NodeID(1))

    let lm = newLeaseManager(RangeID(1), NodeID(1), sl,
                              leaseDurationNs = 600_000_000) # 600ms

    let desc = newRangeDescriptor(RangeID(1), @[byte 0x00], @[byte 0xFF])
    discard desc.addReplica(NodeID(1))
    let group = newRaftGroup(RangeID(1), NodeID(1), ReplicaID(1), desc)

    # Acquire lease
    discard lm.proposeLeaseAcquisition(group)

    # Should not need renewal initially
    check not lm.shouldRenewLease()

    group.close()
    lm.close()
    sl.close()

suite "Lease JSON":
  test "serialize lease":
    var leaseObj = Lease(
      leaseholder: NodeID(1),
      startTs: 1000,
      expirationTs: 2000,
      epoch: 1
    )

    let json = leaseToJson(leaseObj)
    check json["leaseholder"].getInt() == 1
    check json["startTs"].getInt() == 1000
    check json["expirationTs"].getInt() == 2000
    check json["epoch"].getInt() == 1

  test "parse lease":
    let json = %*{
      "leaseholder": 1,
      "startTs": 1000,
      "expirationTs": 2000,
      "epoch": 1
    }

    let leaseObj = parseLease(json)
    check leaseObj.leaseholder == NodeID(1)
    check leaseObj.startTs == 1000
    check leaseObj.expirationTs == 2000
    check leaseObj.epoch == 1

suite "LeaseAcquisitionResult":
  test "success result":
    let lease = Lease(leaseholder: NodeID(1), startTs: 0, expirationTs: 1000)
    let result = LeaseAcquisitionResult(
      success: true,
      lease: some(lease),
      error: ""
    )
    check result.success
    check result.lease.isSome

  test "failure result":
    let result = LeaseAcquisitionResult(
      success: false,
      lease: none(Lease),
      error: "Insufficient support"
    )
    check not result.success
    check result.lease.isNone
    check result.error == "Insufficient support"

suite "LeaseValidationResult":
  test "valid result":
    let result = LeaseValidationResult(valid: true, reason: "")
    check result.valid

  test "invalid result":
    let result = LeaseValidationResult(valid: false, reason: "Lease expired")
    check not result.valid
    check result.reason == "Lease expired"

suite "Constants":
  test "lease durations":
    check DEFAULT_LEASE_DURATION_NS == 3_000_000_000
    check MIN_LEASE_DURATION_NS == 1_000_000_000
    check MAX_LEASE_DURATION_NS == 30_000_000_000
    check LEASE_RENEWAL_MARGIN_NS == 500_000_000
