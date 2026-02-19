# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[guard, snapshot_nonce]

# Forward declarations
type
  LsmIterGuardImpl* = object

# A wrapper around iterators that keep a snapshot alive
# We need to hold the snapshot nonce so the GC watermark does not
# move past this snapshot nonce, removing data that may still be read.
# Additionally, this struct also maps lsm-tree's Guards to "our" Guards.

type
  Iter* = object
    iter*: seq[LsmIterGuardImpl] # Simplified from boxed iterator
    nonce*: SnapshotNonce
    currentIndex*: int
    reverse*: bool

# Constructor
proc newIter*(nonce: SnapshotNonce, iter: seq[LsmIterGuardImpl]): Iter =
  Iter(
    iter: iter,
    nonce: nonce,
    currentIndex: 0,
    reverse: false
  )

# Iterator implementation
iterator items*(iter: var Iter): Guard =
  while iter.currentIndex < iter.iter.len:
    yield Guard(inner: iter.iter[iter.currentIndex])
    iter.currentIndex += 1

# Reverse iterator implementation
iterator itemsReverse*(iter: var Iter): Guard =
  var index = iter.iter.len - 1
  while index >= 0:
    yield Guard(inner: iter.iter[index])
    index -= 1
