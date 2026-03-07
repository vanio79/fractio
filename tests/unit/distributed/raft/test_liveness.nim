# Unit tests for Store Liveness

import std/unittest
import std/atomics
import std/times
import std/options
import std/locks

import fractio/distributed/range/types
import fractio/distributed/raft/liveness

suite "LivenessState":
  test "create liveness state":
    let state = LivenessState(
      nodeId: RangeNodeID(1),
      lastHeartbeat: 1000,
      supportedUntil: 2000,
      epoch: 1
    )
    check state.nodeId == RangeNodeID(1)
    check state.epoch == 1

suite "StoreLiveness":
  test "create store liveness":
    let sl = newStoreLiveness(RangeNodeID(1))
    check sl.nodeId == RangeNodeID(1)
    check sl.getEpoch() == 1
    sl.close()

  test "register and check store":
    let sl = newStoreLiveness(RangeNodeID(1))
    sl.registerStore(RangeNodeID(2))

    check sl.isAlive(RangeNodeID(2))
    sl.close()

  test "heartbeat processing":
    let sl = newStoreLiveness(RangeNodeID(1))
    sl.registerStore(RangeNodeID(2))

    let msg = LivenessMessage(
      nodeId: RangeNodeID(2),
      epoch: 1,
      timestamp: getTime().toUnix * 1_000_000_000,
      messageType: lmtHeartbeat
    )

    let response = sl.processHeartbeat(msg)
    check response.nodeId == RangeNodeID(1)
    check response.messageType == lmtHeartbeatResponse
    sl.close()

  test "support management":
    let sl = newStoreLiveness(RangeNodeID(1))

    sl.grantSupport(RangeNodeID(2))
    check sl.isSupporting(RangeNodeID(2))

    sl.withdrawSupport(RangeNodeID(2))
    check not sl.isSupporting(RangeNodeID(2))
    sl.close()

  test "liveness expiration":
    let sl = newStoreLiveness(RangeNodeID(1),
                               heartbeatIntervalNs = 100_000_000,
                               supportExpirationNs = 200_000_000)

    sl.registerStore(RangeNodeID(2))
    check sl.isAlive(RangeNodeID(2))

    # Unregister to simulate expiration
    sl.unregisterStore(RangeNodeID(2))
    check not sl.isAlive(RangeNodeID(2))
    sl.close()

  test "quorum support":
    let sl = newStoreLiveness(RangeNodeID(1))

    # Register 3 nodes
    sl.registerStore(RangeNodeID(1))
    sl.registerStore(RangeNodeID(2))
    sl.registerStore(RangeNodeID(3))

    # All alive, should have quorum
    let nodes = @[RangeNodeID(1), RangeNodeID(2), RangeNodeID(3)]
    check sl.hasQuorumSupport(nodes)

    # Count supported
    check sl.countSupported(nodes) == 3
    sl.close()

  test "can acquire lease":
    let sl = newStoreLiveness(RangeNodeID(1))
    sl.registerStore(RangeNodeID(1))
    sl.registerStore(RangeNodeID(2))
    sl.registerStore(RangeNodeID(3))

    let voters = @[
      ReplicaDescriptor(nodeId: RangeNodeID(1), replicaId: ReplicaID(1),
          replicaType: rtVoter),
      ReplicaDescriptor(nodeId: RangeNodeID(2), replicaId: ReplicaID(2),
          replicaType: rtVoter),
      ReplicaDescriptor(nodeId: RangeNodeID(3), replicaId: ReplicaID(3),
          replicaType: rtVoter)
    ]

    check sl.canAcquireLease(voters)
    sl.close()

  test "epoch increment":
    let sl = newStoreLiveness(RangeNodeID(1))
    let initialEpoch = sl.getEpoch()

    sl.incrementEpoch()
    check sl.getEpoch() == initialEpoch + 1
    sl.close()

  test "heartbeat creation":
    let sl = newStoreLiveness(RangeNodeID(1))
    let msg = sl.createHeartbeat()

    check msg.nodeId == RangeNodeID(1)
    check msg.messageType == lmtHeartbeat
    check msg.timestamp > 0
    sl.close()

  test "should send heartbeat":
    let sl = newStoreLiveness(RangeNodeID(1), heartbeatIntervalNs = 100_000_000)

    # Initially should send
    check sl.shouldSendHeartbeat()

    # After sending, should not send immediately
    discard sl.createHeartbeat()
    check not sl.shouldSendHeartbeat()
    sl.close()

  test "get alive stores":
    let sl = newStoreLiveness(RangeNodeID(1))
    sl.registerStore(RangeNodeID(2))
    sl.registerStore(RangeNodeID(3))

    let alive = sl.getAliveStores()
    check RangeNodeID(2) in alive
    check RangeNodeID(3) in alive
    sl.close()

  test "get stats":
    let sl = newStoreLiveness(RangeNodeID(1))
    sl.registerStore(RangeNodeID(2))
    sl.registerStore(RangeNodeID(3))
    sl.grantSupport(RangeNodeID(2))

    let stats = sl.getStats()
    check stats.total == 2
    check stats.alive == 2
    check stats.supporting == 1
    sl.close()

suite "LivenessMessage":
  test "create message":
    let msg = LivenessMessage(
      nodeId: RangeNodeID(1),
      epoch: 1,
      timestamp: 1000,
      messageType: lmtHeartbeat
    )
    check msg.nodeId == RangeNodeID(1)
    check msg.messageType == lmtHeartbeat

suite "SupportState":
  test "support state values":
    check ssUnsupported.ord < ssSupported.ord
    check ssSupported.ord < ssExpired.ord

suite "StoreLivenessMode":
  test "mode values":
    check slmFollower.ord < slmCandidate.ord
    check slmCandidate.ord < slmLeader.ord
