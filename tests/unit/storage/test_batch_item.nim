# Unit tests for batch item module
# Tests for Item functionality

import unittest
import std/strutils
import fractio/storage/[batch/item, types, keyspace]

suite "Batch Item Unit Tests":

  test "Item creation":
    # Create a dummy keyspace for testing
    var keyspace: Keyspace # This would be properly initialized in a real test

    # Test basic item creation
    let item = newItem(keyspace, "test_key", "test_value", vtValue)
    check item.key == "test_key"
    check item.value == "test_value"
    check item.valueType == vtValue
    check item.keyspace == keyspace

  test "Item with tombstone":
    var keyspace: Keyspace

    let tombstoneItem = newItem(keyspace, "deleted_key", "", vtTombstone)
    check tombstoneItem.key == "deleted_key"
    check tombstoneItem.value == ""
    check tombstoneItem.valueType == vtTombstone

  test "Item with weak tombstone":
    var keyspace: Keyspace

    let weakTombstoneItem = newItem(keyspace, "weak_deleted_key", "", vtWeakTombstone)
    check weakTombstoneItem.key == "weak_deleted_key"
    check weakTombstoneItem.value == ""
    check weakTombstoneItem.valueType == vtWeakTombstone

  test "Item validation":
    var keyspace: Keyspace

    # Valid item should not raise exception
    expect AssertionError:
      discard newItem(keyspace, "", "value", vtValue) # Empty key should fail
    
    # Valid key length
    let validItem = newItem(keyspace, "a".repeat(100), "value", vtValue)
    check validItem.key.len == 100

    # Valid value length
    let validValueItem = newItem(keyspace, "key", "v".repeat(1000), vtValue)
    check validValueItem.value.len == 1000

  test "Item debug representation":
    # Create a keyspace with proper initialization for testing
    var inner = KeyspaceInner(id: 1, name: "test")
    var keyspace = Keyspace(inner: inner)

    let valueItem = newItem(keyspace, "key", "value", vtValue)
    let valueStr = $valueItem
    check valueStr.contains("1") # keyspace ID
    check valueStr.contains("key")
    check valueStr.contains("V") # Value type indicator

    let tombstoneItem = newItem(keyspace, "key2", "value2", vtTombstone)
    let tombstoneStr = $tombstoneItem
    check tombstoneStr.contains("T") # Tombstone type indicator

    let weakTombstoneItem = newItem(keyspace, "key3", "value3", vtWeakTombstone)
    let weakTombstoneStr = $weakTombstoneItem
    check weakTombstoneStr.contains("W") # Weak tombstone type indicator
