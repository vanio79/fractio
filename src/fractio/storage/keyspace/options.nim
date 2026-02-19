# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[types, compaction]
import std/[tables, strutils, options]

# Forward declarations
type
  InternalKeyspaceId* = uint64
  KvPair* = tuple[key: string, value: string]
  MetaKeyspace* = object

# Configuration policies (simplified versions)
type
  BlockSizePolicy* = object
    size*: uint32

  BloomConstructionPolicyKind* = enum
    bcpFalsePositiveRate
    bcpBitsPerKey

  BloomConstructionPolicy* = object
    case kind*: BloomConstructionPolicyKind
    of bcpFalsePositiveRate:
      falsePositiveRate*: float64
    of bcpBitsPerKey:
      bitsPerKey*: float64

  CompressionPolicy* = object
    compressionTypes*: seq[CompressionType]

  FilterPolicyEntryKind* = enum
    fpeBloom

  FilterPolicyEntry* = object
    case kind*: FilterPolicyEntryKind
    of fpeBloom:
      bloomPolicy*: BloomConstructionPolicy

  FilterPolicy* = object
    entries*: seq[FilterPolicyEntry]

  HashRatioPolicy* = object
    ratio*: float64

  PartitioningPolicy* = object
    partitions*: seq[bool]

  PinningPolicy* = object
    pinned*: seq[bool]

  RestartIntervalPolicy* = object
    intervals*: seq[int]

  KvSeparationOptions* = object
    ageCutoff*: float64
    compression*: CompressionType
    fileTargetSize*: uint64
    separationThreshold*: uint32
    stalenessThreshold*: float64

# Options to configure a keyspace
type
  CreateOptions* = object
    # Amount of levels of the LSM tree (depth of tree)
    levelCount*: uint8

    # Maximum size of this keyspace's memtable - can be changed during runtime
    maxMemtableSize*: uint64

    # Data block hash ratio
    dataBlockHashRatioPolicy*: HashRatioPolicy

    # Block size of data blocks
    dataBlockSizePolicy*: BlockSizePolicy

    dataBlockRestartIntervalPolicy*: RestartIntervalPolicy
    indexBlockRestartIntervalPolicy*: RestartIntervalPolicy
    indexBlockPinningPolicy*: PinningPolicy
    filterBlockPinningPolicy*: PinningPolicy
    filterBlockPartitioningPolicy*: PartitioningPolicy
    indexBlockPartitioningPolicy*: PartitioningPolicy

    # If true, the last level will not build filters
    expectPointReadHits*: bool

    # Filter construction policy
    filterPolicy*: FilterPolicy

    # Compression to use for data blocks
    dataBlockCompressionPolicy*: CompressionPolicy

    # Compression to use for index blocks
    indexBlockCompressionPolicy*: CompressionPolicy

    manualJournalPersist*: bool

    # Compaction strategy (placeholder)
    compactionStrategy*: string

    kvSeparationOpts*: Option[KvSeparationOptions]

# Default constructor
proc defaultCreateOptions*(): CreateOptions =
  CreateOptions(
    manualJournalPersist: false,
    maxMemtableSize: 64 * 1024 * 1024, # 64 MiB
    dataBlockHashRatioPolicy: HashRatioPolicy(ratio: 0.0),
    dataBlockSizePolicy: BlockSizePolicy(size: 4 * 1024), # 4 KiB
    dataBlockRestartIntervalPolicy: RestartIntervalPolicy(intervals: @[10, 16]),
    indexBlockRestartIntervalPolicy: RestartIntervalPolicy(intervals: @[1]),
    indexBlockPinningPolicy: PinningPolicy(pinned: @[true, true, false]),
    filterBlockPinningPolicy: PinningPolicy(pinned: @[true, false]),
    indexBlockPartitioningPolicy: PartitioningPolicy(partitions: @[false, false,
        false, true]),
    filterBlockPartitioningPolicy: PartitioningPolicy(partitions: @[false,
        false, false, true]),
    expectPointReadHits: false,
    filterPolicy: FilterPolicy(entries: @[
      FilterPolicyEntry(kind: fpeBloom, bloomPolicy: BloomConstructionPolicy(
          kind: bcpFalsePositiveRate, falsePositiveRate: 0.0001)),
      FilterPolicyEntry(kind: fpeBloom, bloomPolicy: BloomConstructionPolicy(
          kind: bcpBitsPerKey, bitsPerKey: 10.0))
    ]),
    levelCount: 7,
    dataBlockCompressionPolicy: CompressionPolicy(compressionTypes: @[ctNone]),
    indexBlockCompressionPolicy: CompressionPolicy(compressionTypes: @[ctNone]),
    compactionStrategy: "Leveled", # Default to Leveled compaction
    kvSeparationOpts: none(KvSeparationOptions)
  )

# Policy helper macro equivalent
proc encodeConfigKey*(keyspaceId: InternalKeyspaceId, name: string): string =
  # Simplified encoding - in a full implementation this would be more complex
  return "config:" & $keyspaceId & ":" & name

# Methods for CreateOptions
proc fromKvs*(keyspaceId: InternalKeyspaceId,
    metaKeyspace: MetaKeyspace): Option[CreateOptions] =
  # In a full implementation, this would decode options from key-value pairs
  # For now, we'll return the default options
  return some(defaultCreateOptions())

proc encodeKvs*(options: CreateOptions, keyspaceId: InternalKeyspaceId): seq[KvPair] =
  # In a full implementation, this would encode options to key-value pairs
  # For now, we'll return an empty sequence
  return @[]

# Builder methods
proc withKvSeparation*(options: CreateOptions, opts: Option[
    KvSeparationOptions]): CreateOptions =
  var result = options
  result.kvSeparationOpts = opts
  return result

proc dataBlockRestartIntervalPolicy*(options: CreateOptions,
    policy: RestartIntervalPolicy): CreateOptions =
  var result = options
  result.dataBlockRestartIntervalPolicy = policy
  return result

proc filterBlockPinningPolicy*(options: CreateOptions,
    policy: PinningPolicy): CreateOptions =
  var result = options
  result.filterBlockPinningPolicy = policy
  return result

proc indexBlockPinningPolicy*(options: CreateOptions,
    policy: PinningPolicy): CreateOptions =
  var result = options
  result.indexBlockPinningPolicy = policy
  return result

proc filterBlockPartitioningPolicy*(options: CreateOptions,
    policy: PartitioningPolicy): CreateOptions =
  var result = options
  result.filterBlockPartitioningPolicy = policy
  return result

proc indexBlockPartitioningPolicy*(options: CreateOptions,
    policy: PartitioningPolicy): CreateOptions =
  var result = options
  result.indexBlockPartitioningPolicy = policy
  return result

proc dataBlockHashRatioPolicy*(options: CreateOptions,
    policy: HashRatioPolicy): CreateOptions =
  var result = options
  result.dataBlockHashRatioPolicy = policy
  return result

proc filterPolicy*(options: CreateOptions,
    policy: FilterPolicy): CreateOptions =
  var result = options
  result.filterPolicy = policy
  return result

proc expectPointReadHits*(options: CreateOptions, b: bool): CreateOptions =
  var result = options
  result.expectPointReadHits = b
  return result

proc dataBlockCompressionPolicy*(options: CreateOptions,
    policy: CompressionPolicy): CreateOptions =
  var result = options
  result.dataBlockCompressionPolicy = policy
  return result

proc indexBlockCompressionPolicy*(options: CreateOptions,
    policy: CompressionPolicy): CreateOptions =
  var result = options
  result.indexBlockCompressionPolicy = policy
  return result

proc manualJournalPersist*(options: CreateOptions, flag: bool): CreateOptions =
  var result = options
  result.manualJournalPersist = flag
  return result

proc maxMemtableSize*(options: CreateOptions, bytes: uint64): CreateOptions =
  var result = options
  result.maxMemtableSize = bytes
  return result

proc dataBlockSizePolicy*(options: CreateOptions,
    policy: BlockSizePolicy): CreateOptions =
  var result = options
  result.dataBlockSizePolicy = policy
  return result
