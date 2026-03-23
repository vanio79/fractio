# Test Configuration Module
# 
# Centralized configuration for integration tests to ensure fast, consistent
# timeouts across all test files. These values are optimized for local testing
# and CI environments where network latency is minimal.
#
# Usage:
#   import test_config
#   
#   let coord = newNuRaftCoordinator(CoordinatorConfig(
#     electionTimeoutLowerMs: TEST_ELECTION_TIMEOUT_LOWER_MS,
#     electionTimeoutUpperMs: TEST_ELECTION_TIMEOUT_UPPER_MS,
#     heartbeatIntervalMs: TEST_HEARTBEAT_INTERVAL_MS,
#     ...
#   ))

const
  # ---------------------------------------------------------------------------
  # Raft Election Timeouts (optimized for tests)
  # ---------------------------------------------------------------------------
  # Production defaults (1-2s election, 500ms heartbeat) are too slow for tests.
  # These values are 5x faster while still allowing elections to complete.

  TEST_ELECTION_TIMEOUT_LOWER_MS* = 150 # 150ms (vs 1000ms production)
  TEST_ELECTION_TIMEOUT_UPPER_MS* = 300 # 300ms (vs 2000ms production)
  TEST_HEARTBEAT_INTERVAL_MS* = 50 # 50ms (vs 500ms production)

  # For multi-node clusters where network jitter is more likely,
  # use slightly more conservative values.
  TEST_ELECTION_TIMEOUT_LOWER_MS_MULTINODE * = 200
  TEST_ELECTION_TIMEOUT_UPPER_MS_MULTINODE * = 400
  TEST_HEARTBEAT_INTERVAL_MS_MULTINODE * = 80

  # ---------------------------------------------------------------------------
  # Polling Intervals
  # ---------------------------------------------------------------------------
  # How often to poll for leader election, replication, etc.
  # Smaller values = faster tests, but more CPU usage.

  TEST_POLL_INTERVAL_MS* = 10 # Poll every 10ms (vs 100ms)
  TEST_POLL_INTERVAL_FAST_MS* = 5 # For very tight loops

  # ---------------------------------------------------------------------------
  # Wait Timeouts
  # ---------------------------------------------------------------------------
  # Maximum time to wait for various operations.
  # These are upper bounds - tests should typically complete faster.

  TEST_LEADER_WAIT_TIMEOUT_MS* = 2000 # Max time to wait for leader
  TEST_REPLICATION_WAIT_MS* = 100 # Wait for Raft replication
  TEST_GROUP_INIT_WAIT_MS* = 100 # Wait for group initialization
  TEST_ELECTION_SETTLE_MS* = 50 # Wait for election to settle after probe

  # ---------------------------------------------------------------------------
  # Retry Configuration
  # ---------------------------------------------------------------------------
  # Number of retries and backoff for operations that may fail transiently.

  TEST_MAX_LEADER_POLL_ATTEMPTS* = 200 # 200 * 10ms = 2s max
  TEST_MAX_READY_POLL_ATTEMPTS* = 60 # 60 * (5ms + probe) = ~300ms-600ms
  TEST_MAX_RETRY_ATTEMPTS* = 10 # Generic retry limit
  TEST_RETRY_BACKOFF_MS* = 20 # Base backoff for retries

  # ---------------------------------------------------------------------------
  # Cluster Startup
  # ---------------------------------------------------------------------------
  # Timeouts for cluster initialization.

  TEST_NODE_START_DELAY_MS* = 20 # Delay between node starts
  TEST_CLUSTER_STARTUP_MS* = 100 # Initial wait after all nodes start
  TEST_SHUTDOWN_DELAY_MS* = 10 # Delay for graceful shutdown

  # ---------------------------------------------------------------------------
  # Preferred Leader Rebalancing
  # ---------------------------------------------------------------------------
  # Timeouts for preferred leader election and transfer.

  TEST_LEADER_TRANSFER_WAIT_MS* = 200 # Wait for leadership transfer to complete
  TEST_REBALANCE_SETTLE_MS* = 1000 # Wait for rebalance background task (was 5000ms)
  TEST_LEADER_STABILITY_CHECKS* = 30 # Number of polls to verify leader stability
