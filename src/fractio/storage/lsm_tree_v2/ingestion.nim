# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Ingestion
##
## Unified ingestion builder for any tree type.

import std/[options]
import types
import error
import tree_ext

# Ingestion state
type
  IngestionState* = enum
    isActive
    isFinished

# Any ingestion (polymorphic)
type
  AnyIngestion* = ref object
    ## Unified ingestion over any tree type
    case isStandard*: bool
    of true:
      standardIngestion*: pointer # Would be Ingestion
    of false:
      blobIngestion*: pointer     # Would be BlobIngestion

proc newStandardIngestion*(ingestion: pointer): AnyIngestion =
  AnyIngestion(isStandard: true, standardIngestion: ingestion)

proc newBlobIngestion*(ingestion: pointer): AnyIngestion =
  AnyIngestion(isStandard: false, blobIngestion: ingestion)

# Tree ingestion interface
type
  TreeIngestion* = ref object of RootObj
    ## Base ingestion interface
    state*: IngestionState

proc write*(ing: TreeIngestion, key, value: Slice): LsmResult[void] {.base.} =
  errVoid(newIoError("Not implemented"))

proc writeTombstone*(ing: TreeIngestion, key: Slice): LsmResult[void] {.base.} =
  errVoid(newIoError("Not implemented"))

proc writeWeakTombstone*(ing: TreeIngestion, key: Slice): LsmResult[
    void] {.base.} =
  errVoid(newIoError("Not implemented"))

proc finish*(ing: TreeIngestion): LsmResult[void] {.base.} =
  errVoid(newIoError("Not implemented"))

# Start ingestion on any tree
proc ingestion*(tree: AnyTree): LsmResult[AnyIngestion] =
  if tree.isStandard:
    # Would create standard ingestion
    ok(newStandardIngestion(nil))
  else:
    # Would create blob ingestion
    ok(newBlobIngestion(nil))
