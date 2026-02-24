# Copyright (c) 2025-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Iterator Guard
##
## Guard to access key-value pairs from iterators.

import types
import error

type
  IterGuard* = ref object of RootObj
    ## Guard to access key-value pairs

  StandardIterGuard* = ref object of IterGuard
    key*: Slice
    value*: Slice

  BlobIterGuard* = ref object of IterGuard
    key*: Slice
    value*: Option[Slice] # May not be loaded if key-value separated

proc intoInner*(g: IterGuard): LsmResult[KvPair] =
  if g of StandardIterGuard:
    let sg = StandardIterGuard(g)
    ok((sg.key, sg.value))
  elif g of BlobIterGuard:
    let bg = BlobIterGuard(g)
    ok((bg.key, bg.value.get(Slice)))
  else:
    err[KvPair](newIoError("Unknown guard type"))

proc key*(g: IterGuard): Slice =
  if g of StandardIterGuard:
    StandardIterGuard(g).key
  elif g of BlobIterGuard:
    BlobIterGuard(g).key
  else:
    emptySlice()

proc value*(g: IterGuard): LsmResult[Slice] =
  if g of StandardIterGuard:
    ok(StandardIterGuard(g).value)
  elif g of BlobIterGuard:
    let bg = BlobIterGuard(g)
    if bg.value.isSome:
      ok(bg.value.get)
    else:
      err[Slice](newIoError("Value not loaded"))
  else:
    err[Slice](newIoError("Unknown guard type"))

proc size*(g: IterGuard): uint32 =
  if g of StandardIterGuard:
    uint32(StandardIterGuard(g).value.len)
  elif g of BlobIterGuard:
    0 # Would need to get from metadata
  else:
    0
