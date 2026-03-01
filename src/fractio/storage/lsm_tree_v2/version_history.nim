# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Version History - Manages historical versions for snapshot isolation
##
## This module implements version history management similar to Rust's SuperVersions.
## It maintains a list of SuperVersion snapshots for MVCC.

import std/[locks, options, atomics]
import types
import memtable
import table # For SsTable

# ============================================================================
# SuperVersion - Point-in-time snapshot
# ============================================================================

type
  SuperVersion* = ref object
    ## A super version is a point-in-time snapshot of memtables and SSTables
    activeMemtable*: Memtable
    sealedMemtables*: seq[Memtable]
    tables*: seq[SsTable] # SSTables organized by level
    seqno*: SeqNo
    refCount*: Atomic[int32]

proc newSuperVersion*(seqno: SeqNo = 0.SeqNo): SuperVersion =
  ## Create a new SuperVersion with given sequence number
  var refCount: Atomic[int32]
  result = SuperVersion(
    activeMemtable: newMemtable(MemtableId(0)),
    sealedMemtables: newSeq[Memtable](),
    tables: newSeq[SsTable](),
    seqno: seqno,
    refCount: refCount
  )
  store(result.refCount, 0, moRelaxed)

proc acquire*(sv: SuperVersion): int32 =
  ## Acquire reference to SuperVersion
  atomicInc(sv.refCount, 1)
  load(sv.refCount, moRelaxed)

proc release*(sv: SuperVersion): int32 =
  ## Release reference to SuperVersion
  atomicDec(sv.refCount, 1)
  load(sv.refCount, moRelaxed)

# ============================================================================
# VersionHistory - Manages historical versions
# ============================================================================

type
  VersionHistory* = ref object
    ## Manages a list of SuperVersions for MVCC
    versions*: seq[SuperVersion]
    lock*: Lock
    latestSeqno*: Atomic[SeqNo]

proc newVersionHistory*(initialVersion: SuperVersion): VersionHistory =
  ## Create new VersionHistory with initial version
  result = VersionHistory(
    versions: newSeq[SuperVersion](),
    lock: Lock(),
    latestSeqno: Atomic[SeqNo]()
  )
  initLock(result.lock)
  result.versions.add(initialVersion)
  store(result.latestSeqno, initialVersion.seqno, moRelaxed)

proc len*(vh: VersionHistory): int {.inline.} =
  ## Number of versions in history
  vh.versions.len

proc freeListLen*(vh: VersionHistory): int {.inline.} =
  ## Number of versions that can be GC'd (all except latest)
  max(0, vh.versions.len - 1)

proc latestVersion*(vh: VersionHistory): SuperVersion =
  ## Get the latest version (most recent snapshot)
  if vh.versions.len > 0:
    vh.versions[^1]
  else:
    newSuperVersion(0.SeqNo)

proc getVersionForSnapshot*(vh: VersionHistory, seqno: SeqNo): SuperVersion =
  ## Get appropriate version for a given snapshot sequence number.
  ##
  ## If seqno is 0, returns the oldest version.
  ## Otherwise, finds the most recent version with seqno < target seqno.
  if seqno == 0.SeqNo:
    if vh.versions.len > 0:
      return vh.versions[0]
    else:
      return newSuperVersion(0.SeqNo)

  # Find version with seqno < target, starting from most recent
  for i in countdown(vh.versions.len - 1, 0):
    let version = vh.versions[i]
    if version.seqno < seqno:
      return version

  # If no version found, return oldest
  if vh.versions.len > 0:
    vh.versions[0]
  else:
    newSuperVersion(0.SeqNo)

proc appendVersion*(vh: VersionHistory, version: SuperVersion) =
  ## Append a new version to history
  vh.versions.add(version)
  store(vh.latestSeqno, version.seqno, moRelaxed)

proc replaceLatestVersion*(vh: VersionHistory, version: SuperVersion) =
  ## Replace the latest version
  if vh.versions.len > 0:
    vh.versions.del(vh.versions.len - 1)
  vh.versions.add(version)
  store(vh.latestSeqno, version.seqno, moRelaxed)

proc upgradeVersionWithSeqno*(vh: VersionHistory, newVersion: SuperVersion) =
  ## Upgrade to a new version with specific sequence number
  vh.appendVersion(newVersion)

proc gcVersions*(vh: VersionHistory, gcWatermark: SeqNo) =
  ## Remove old versions below the GC watermark
  if gcWatermark == 0.SeqNo:
    return

  if vh.freeListLen() < 1:
    return

  # Remove versions from front that are below watermark
  while vh.versions.len > 1:
    let first = vh.versions[0]
    if first.seqno < gcWatermark:
      vh.versions.delete(0)
    else:
      break

# ============================================================================
