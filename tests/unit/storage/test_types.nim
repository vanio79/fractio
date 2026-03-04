# Unit tests for types module
# Tests for core storage types

import unittest
import fractio/storage/types

suite "Types Unit Tests":

  test "Basic types":
    # Test SeqNo
    var seqNo: SeqNo = 12345
    check seqNo == 12345

    # Test UserKey
    let key: UserKey = "test_key"
    check key == "test_key"

    # Test UserValue
    let value: UserValue = "test_value"
    check value == "test_value"

    # Test KvPair
    let kv: KvPair = (key: "key", value: "value")
    check kv.key == "key"
    check kv.value == "value"

  test "Compression types":
    check ctNone.ord == 0
    check ctLz4.ord == 1
    check ctSnappy.ord == 2

    # Test that all compression types are distinct
    check ctNone != ctLz4
    check ctLz4 != ctSnappy
    check ctNone != ctSnappy

  test "Value types":
    check vtValue.ord == 0
    check vtTombstone.ord == 1
    check vtWeakTombstone.ord == 2

    # Test that all value types are distinct
    check vtValue != vtTombstone
    check vtTombstone != vtWeakTombstone
    check vtValue != vtWeakTombstone
