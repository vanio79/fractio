# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Double-Ended Peekable Iterator
##
## An advanced version of Peekable that works with double-ended iterators.

import std/[options]

type
  MaybePeekedKind* = enum
    mpUnpeeked
    mpPeeked

  MaybePeeked*[T] = object
    case kind*: MaybePeekedKind
    of mpPeeked:
      value*: T
    of mpUnpeeked:
      discard

  DoubleEndedPeekable*[T, I] = ref object
    iter*: I
    front*: MaybePeeked[T]
    back*: MaybePeeked[T]

proc newDoubleEndedPeekable*[T, I](iter: I): DoubleEndedPeekable[T, I] =
  DoubleEndedPeekable[T, I](
    iter: iter,
    front: MaybePeeked[T](kind: mpUnpeeked),
    back: MaybePeeked[T](kind: mpUnpeeked)
  )

proc peek*[T, I](it: DoubleEndedPeekable[T, I]): Option[T] =
  ## Returns a reference to the next value without advancing
  case it.front.kind
  of mpPeeked: some(it.front.value)
  of mpUnpeeked: none(T)

proc next*[T, I](it: DoubleEndedPeekable[T, I]): Option[T] =
  ## Returns the next value
  case it.front.kind
  of mpPeeked:
    let val = it.front.value
    it.front = MaybePeeked[T](kind: mpUnpeeked)
    some(val)
  of mpUnpeeked:
    it.iter.next()

proc nextBack*[T, I](it: DoubleEndedPeekable[T, I]): Option[T] =
  ## Returns the next value from the back
  case it.back.kind
  of mpPeeked:
    let val = it.back.value
    it.back = MaybePeeked[T](kind: mpUnpeeked)
    some(val)
  of mpUnpeeked:
    # Would need double-ended iterator support
    none(T)
