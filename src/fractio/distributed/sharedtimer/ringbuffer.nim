## Generic circular buffer (ring buffer) implementation.
## Thread-safe for single-producer single-consumer patterns when used with external locking.

type
  RingBuffer*[T] = ref object
    data*: seq[T]
      ## Fixed-capacity storage.
    capacity*: int
      ## Maximum number of elements.
    head*: int
      ## Index of next write position.
    size*: int
      ## Current number of items.

proc newRingBuffer*[T](capacity: int): RingBuffer[T] =
  ## Create a new ring buffer with given capacity.
  result = RingBuffer[T](
    data: newSeq[T](capacity),
    capacity: capacity,
    head: 0,
    size: 0
  )

proc add*[T](self: RingBuffer[T], item: T) =
  ## Add an item to the buffer, overwriting oldest if full.
  if self.data.len == 0:
    self.data = newSeq[T](self.capacity)
  self.data[self.head] = item
  self.head = (self.head + 1) mod self.capacity
  if self.size < self.capacity:
    inc(self.size)

proc items*[T](self: RingBuffer[T]): seq[T] =
  ## Get all items in order (oldest first).
  result = @[]
  if self.size == 0:
    return
  if self.size < self.capacity:
    result = self.data[0..<self.size]
  else:
    result = self.data[self.head..^1] & self.data[0..<self.head]

proc clear*[T](self: RingBuffer[T]) =
  ## Remove all items.
  self.head = 0
  self.size = 0
