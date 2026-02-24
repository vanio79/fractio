# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for LSM Tree v2 - Sequence Number Counter

import std/unittest
import std/atomics
import fractio/storage/lsm_tree_v2/seqno

suite "seqno counter":
  test "seqno counter default":
    let counter = newSequenceNumberCounter()
    check counter.get() == 0

  test "seqno counter next":
    let counter = newSequenceNumberCounter()
    let seqno1 = counter.next()
    check seqno1 == 0
    let seqno2 = counter.next()
    check seqno2 == 1
    let seqno3 = counter.next()
    check seqno3 == 2

  test "seqno counter set":
    let counter = newSequenceNumberCounter()
    counter.set(100)
    check counter.get() == 100
    let seqno = counter.next()
    check seqno == 100

  test "seqno counter fetch max":
    let counter = newSequenceNumberCounter()
    counter.set(50)
    counter.fetchMax(100)
    check counter.get() == 100
    counter.fetchMax(75)
    check counter.get() == 100 # Should stay at 100

  test "seqno counter not max seqno":
    let counter = newSequenceNumberCounter()
    counter.set(0x7FFF_FFFF_FFFF_FFFF'u64 - 1)
    let seqno = counter.next()
    check seqno == 0x7FFF_FFFF_FFFF_FFFF'u64 - 1
    # Should not panic

  test "seqno counter at max boundary":
    let counter = newSequenceNumberCounter()
    counter.set(0x7FFF_FFFF_FFFF_FFFF'u64 - 1)
    discard counter.next()
    # The next one would be at the limit but shouldn't panic
    # In actual implementation we should check for overflow
