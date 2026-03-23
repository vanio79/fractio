# Example test demonstrating SharedClusterFixture usage.
#
# This shows how to use the shared fixture to avoid repeated cluster
# creation/teardown between related tests, significantly speeding up
# test suites.
#
# The cluster is created once and shared across all tests in the suite.

import std/unittest
import std/os
import std/options
import ../../test_config
import ../../test_cluster_helper
import fractio/distributed/meta/system_tables
import fractio/distributed/raft/group_types as rangeTypes

# Create a shared fixture with a unique port offset
var fixture = newSharedClusterFixture(defaultTestClusterConfig())

suite "Shared Fixture Example - Related Tests":
  setup:
    # Setup is called before each test, but only creates cluster once
    fixture.setup()

  teardown:
    # Teardown is called after each test
    # The cluster persists for the next test
    fixture.teardown()

  test "first test - cluster is created":
    let cluster = fixture.get()

    # Verify cluster is running
    let leaderIdx = cluster.findLeader(META_GROUP_ID)
    check leaderIdx >= 0

    # Write some data
    check cluster.nodes[0].kvPut("test_key_1", "value_1")
    sleep(TEST_REPLICATION_WAIT_MS)

    echo "  Test 1: Wrote test_key_1"

  test "second test - cluster persists from first test":
    let cluster = fixture.get()

    # Data from first test should still be there
    let val = cluster.nodes[0].kvGet("test_key_1")
    check val.isSome
    check val.get == "value_1"

    echo "  Test 2: Found test_key_1 from previous test"

    # Add more data
    check cluster.nodes[0].kvPut("test_key_2", "value_2")
    sleep(TEST_REPLICATION_WAIT_MS)

  test "third test - can read data from previous tests":
    let cluster = fixture.get()

    # Both keys should exist
    check cluster.nodes[0].kvGet("test_key_1").get == "value_1"
    check cluster.nodes[0].kvGet("test_key_2").get == "value_2"

    echo "  Test 3: Found both keys from previous tests"

# Note: For tests that need isolated state, use fixture.reset() in setup:
suite "Tests with Isolated State":
  setup:
    fixture.setup()
    # Uncomment to reset cluster state between tests:
    # fixture.reset()

  teardown:
    fixture.teardown()

  test "isolated test 1":
    let cluster = fixture.get()
    check cluster.findLeader(META_GROUP_ID) >= 0

  test "isolated test 2":
    let cluster = fixture.get()
    check cluster.findLeader(DATA_GROUP_START_ID) >= 0
