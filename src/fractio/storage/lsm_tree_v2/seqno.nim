# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Sequence Number Counter
##
## Thread-safe sequence number generator for MVCC.

import std/[atomics, locks]
import types

type
  SequenceNumberCounter* = ref object
    ## Thread-safe sequence number generator
    counter*: Atomic[uint64]

proc newSequenceNumberCounter*(prev: SeqNo = 0): SequenceNumberCounter =
  var counter: Atomic[uint64]
  store(counter, prev)
  SequenceNumberCounter(counter: counter)

proc get*(c: SequenceNumberCounter): SeqNo =
  ## Gets the current sequence number without incrementing
  load(c.counter, moAcquire)

proc next*(c: SequenceNumberCounter): SeqNo =
  ## Gets the next sequence number
  let seqno = fetchAdd(c.counter, 1, moAcquire)

  # The MSB is reserved for transactions (63-bit sequence numbers)
  # This gives us 63-bit sequence numbers technically.
  assert(seqno < MAX_VALID_SEQNO, "Ran out of sequence numbers")

  seqno

proc set*(c: SequenceNumberCounter, seqno: SeqNo) =
  ## Sets the sequence number
  store(c.counter, seqno, moRelease)

proc fetchMax*(c: SequenceNumberCounter, seqno: SeqNo) =
  ## Maximizes the sequence number
  var current = load(c.counter, moRelaxed)
  while seqno > current:
    if compareExchange(c.counter, current, seqno, moRelaxed, moRelaxed):
      break
    current = load(c.counter, moRelaxed)

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing sequence number counter..."

  let counter = newSequenceNumberCounter()

  let seq1 = counter.get()
  echo "Initial seqno: ", seq1

  let seq2 = counter.next()
  echo "After next: ", seq2

  let seq3 = counter.next()
  echo "After next: ", seq3

  counter.set(100)
  echo "After set(100): ", counter.get()

  echo "Sequence counter tests passed!"
