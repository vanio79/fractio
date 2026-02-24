# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Slice Windows
##
## Window iterator extensions for slices.

import std/[sequtils, options]

# Growing windows iterator
type
  GrowingWindows*[T] = ref object
    data*: seq[T]
    size*: int
    index*: int

proc growingWindows*[T](data: seq[T]): GrowingWindows[T] =
  GrowingWindows[T](data: data, size: 1, index: 0)

proc next*[T](w: GrowingWindows[T]): Option[seq[seq[T]]] =
  if w.size > w.data.len:
    return none(seq[seq[T]])

  var result = newSeq[seq[T]]()
  for i in 0 .. w.data.len - w.size + 1:
    result.add(w.data[i ..< i + w.size])

  w.size += 1
  some(result)

# Shrinking windows iterator
type
  ShrinkingWindows*[T] = ref object
    data*: seq[T]
    size*: int

proc shrinkingWindows*[T](data: seq[T]): ShrinkingWindows[T] =
  ShrinkingWindows[T](data: data, size: data.len)

proc next*[T](w: ShrinkingWindows[T]): Option[seq[seq[T]]] =
  if w.size == 0:
    return none(seq[seq[T]])

  var result = newSeq[seq[T]]()
  for i in 0 .. w.data.len - w.size + 1:
    result.add(w.data[i ..< i + w.size])

  w.size -= 1
  some(result)

when isMainModule:
  echo "Testing slice windows..."

  let data = @[1, 2, 3]

  echo "Growing windows:"
  var gw = growingWindows(data)
  while true:
    let result = gw.next()
    if result.isNone:
      break
    echo "  ", result.get

  echo "Shrinking windows:"
  var sw = shrinkingWindows(data)
  while true:
    let result = sw.next()
    if result.isNone:
      break
    echo "  ", result.get

  echo "Slice windows tests passed!"
