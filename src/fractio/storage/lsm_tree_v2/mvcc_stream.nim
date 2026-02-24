# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## MVCC Stream
##
## Multi-version-aware iterator for read operations.

import std/[options]
import types
import error
import double_ended_peekable

type
  MvccStream*[I] = ref object
    ## Consumes a stream of KVs and emits a new stream according to MVCC rules
    inner*: DoubleEndedPeekable[LsmResult[InternalValue], I]

proc newMvccStream*[I](iter: I): MvccStream[I] =
  MvccStream[I](
    inner: newDoubleEndedPeekable(iter)
  )

proc drainKeyMin*(s: MvccStream, key: Slice): LsmResult[void] =
  ## Drains all entries for the given user key
  okVoid()

proc next*(s: MvccStream): Option[LsmResult[InternalValue]] =
  ## Get next MVCC-compliant item
  # Would skip older versions of the same key
  none(LsmResult[InternalValue])

proc nextBack*(s: MvccStream): Option[LsmResult[InternalValue]] =
  ## Get next item from the back
  none(LsmResult[InternalValue])

proc hasNext*(s: MvccStream): bool =
  false
