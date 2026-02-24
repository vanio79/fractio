# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Tests for LSM Tree v2 - Slice Windows

import std/unittest
import std/sequtils
import std/options
import fractio/storage/lsm_tree_v2/slice_windows

suite "slice_windows":
  test "growing windows":
    let data = @[1, 2, 3]
    var gw = growingWindows(data)

    # First iteration: size 1
    var result = gw.next()
    check result.isSome()
    if result.isSome():
      check result.get == @[@[1], @[2], @[3]]

    # Second iteration: size 2
    result = gw.next()
    check result.isSome()
    if result.isSome():
      check result.get == @[@[1, 2], @[2, 3]]

    # Third iteration: size 3
    result = gw.next()
    check result.isSome()
    if result.isSome():
      check result.get == @[@[1, 2, 3]]

    # No more
    result = gw.next()
    check result.isNone()

  test "growing windows full":
    let data = @[1, 2, 3, 4, 5]
    var gw = growingWindows(data)
    var count = 0

    while true:
      let result = gw.next()
      if result.isNone:
        break
      count += 1

    # 5 + 4 + 3 + 2 + 1 = 15 iterations
    check count == 15

  test "shrinking windows":
    let data = @[1, 2, 3]
    var sw = shrinkingWindows(data)

    # First iteration: size 3
    var result = sw.next()
    check result.isSome()
    if result.isSome():
      check result.get == @[@[1, 2, 3]]

    # Second iteration: size 2
    result = sw.next()
    check result.isSome()
    if result.isSome():
      check result.get == @[@[1, 2], @[2, 3]]

    # Third iteration: size 1
    result = sw.next()
    check result.isSome()
    if result.isSome():
      check result.get == @[@[1], @[2], @[3]]

    # No more
    result = sw.next()
    check result.isNone()

  test "shrinking windows full":
    let data = @[1, 2, 3, 4, 5]
    var sw = shrinkingWindows(data)
    var count = 0

    while true:
      let result = sw.next()
      if result.isNone:
        break
      count += 1

    # 5 + 4 + 3 + 2 + 1 = 15 iterations
    check count == 15

  test "growing windows empty":
    let data: seq[int] = @[]
    var gw = growingWindows(data)
    let result = gw.next()
    check result.isNone()

  test "shrinking windows empty":
    let data: seq[int] = @[]
    var sw = shrinkingWindows(data)
    let result = sw.next()
    check result.isNone()

  test "growing windows single":
    let data = @[1]
    var gw = growingWindows(data)

    let result = gw.next()
    check result.isSome()
    if result.isSome():
      check result.get == @[@[1]]

    let result2 = gw.next()
    check result2.isNone()

  test "shrinking windows single":
    let data = @[1]
    var sw = shrinkingWindows(data)

    let result = sw.next()
    check result.isSome()
    if result.isSome():
      check result.get == @[@[1]]

    let result2 = sw.next()
    check result2.isNone()
