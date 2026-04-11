# Unit tests for fractio/distributed/sharedtimer/ringbuffer.nim
# Tests generic circular buffer operations

import std/unittest
import fractio/distributed/sharedtimer/ringbuffer
import fractio/distributed/sharedtimer/types

suite "RingBuffer - Construction":

  test "create empty ring buffer":
    let rb = newRingBuffer[int](10)
    check rb.capacity == 10
    check rb.size == 0
    check rb.head == 0
    check rb.data.len == 10

  test "create with different capacities":
    for cap in [1, 5, 10, 100, 1000]:
      let rb = newRingBuffer[int](cap)
      check rb.capacity == cap
      check rb.size == 0

  test "create with capacity 1":
    let rb = newRingBuffer[int](1)
    check rb.capacity == 1
    check rb.size == 0

  test "create with ClockOffset type":
    let rb = newRingBuffer[ClockOffset](20)
    check rb.capacity == 20
    check rb.size == 0

  test "create with string type":
    let rb = newRingBuffer[string](5)
    check rb.capacity == 5
    check rb.size == 0

suite "RingBuffer - Add Operations":

  test "add single item":
    let rb = newRingBuffer[int](10)
    rb.add(42)
    check rb.size == 1
    let items = rb.items()
    check items.len == 1
    check items[0] == 42

  test "add multiple items":
    let rb = newRingBuffer[int](10)
    for i in 1..5:
      rb.add(i)
    check rb.size == 5
    let items = rb.items()
    check items.len == 5
    check items == @[1, 2, 3, 4, 5]

  test "add fills to capacity":
    let rb = newRingBuffer[int](5)
    for i in 1..5:
      rb.add(i)
    check rb.size == 5
    check rb.head == 0

  test "add overwrites oldest when full":
    let rb = newRingBuffer[int](3)
    rb.add(1)
    rb.add(2)
    rb.add(3)
    rb.add(4)
    check rb.size == 3
    let items = rb.items()
    check items == @[2, 3, 4]

  test "add multiple overwrites":
    let rb = newRingBuffer[int](3)
    for i in 1..10:
      rb.add(i)
    check rb.size == 3
    let items = rb.items()
    check items == @[8, 9, 10]

  test "add with capacity 1 overwrites":
    let rb = newRingBuffer[int](1)
    rb.add(10)
    rb.add(20)
    rb.add(30)
    check rb.size == 1
    let items = rb.items()
    check items == @[30]

  test "head position advances correctly":
    let rb = newRingBuffer[int](5)
    rb.add(1)
    check rb.head == 1
    rb.add(2)
    check rb.head == 2
    rb.add(3)
    check rb.head == 3
    rb.add(4)
    check rb.head == 4
    rb.add(5)
    check rb.head == 0
    rb.add(6)
    check rb.head == 1

suite "RingBuffer - Items Retrieval":

  test "items from empty buffer":
    let rb = newRingBuffer[int](10)
    let items = rb.items()
    check items.len == 0

  test "items from partially filled buffer":
    let rb = newRingBuffer[int](10)
    rb.add(1)
    rb.add(2)
    rb.add(3)
    let items = rb.items()
    check items == @[1, 2, 3]

  test "items from full buffer preserves order":
    let rb = newRingBuffer[int](5)
    for i in 1..5:
      rb.add(i)
    let items = rb.items()
    check items == @[1, 2, 3, 4, 5]

  test "items from wrapped buffer":
    let rb = newRingBuffer[int](5)
    for i in 1..7:
      rb.add(i)
    let items = rb.items()
    check items == @[3, 4, 5, 6, 7]

  test "items oldest first":
    let rb = newRingBuffer[int](10)
    rb.add(100)
    rb.add(200)
    rb.add(300)
    let items = rb.items()
    check items[0] == 100
    check items[1] == 200
    check items[2] == 300

  test "items with ClockOffset":
    let rb = newRingBuffer[ClockOffset](5)
    rb.add(ClockOffset(offset: 10.0, peerId: "p1"))
    rb.add(ClockOffset(offset: 20.0, peerId: "p2"))
    rb.add(ClockOffset(offset: 30.0, peerId: "p3"))
    let items = rb.items()
    check items.len == 3
    check items[0].offset == 10.0
    check items[1].offset == 20.0
    check items[2].offset == 30.0

  test "items returns copy not reference":
    let rb = newRingBuffer[int](5)
    rb.add(1)
    rb.add(2)
    let items1 = rb.items()
    rb.add(3)
    let items2 = rb.items()
    check items1.len == 2
    check items2.len == 3

suite "RingBuffer - Clear":

  test "clear empty buffer":
    let rb = newRingBuffer[int](10)
    rb.clear()
    check rb.size == 0
    check rb.head == 0

  test "clear partially filled buffer":
    let rb = newRingBuffer[int](10)
    rb.add(1)
    rb.add(2)
    rb.add(3)
    rb.clear()
    check rb.size == 0
    check rb.head == 0
    let items = rb.items()
    check items.len == 0

  test "clear full buffer":
    let rb = newRingBuffer[int](5)
    for i in 1..5:
      rb.add(i)
    rb.clear()
    check rb.size == 0
    check rb.head == 0

  test "clear wrapped buffer":
    let rb = newRingBuffer[int](5)
    for i in 1..10:
      rb.add(i)
    rb.clear()
    check rb.size == 0
    check rb.head == 0

  test "clear preserves capacity":
    let rb = newRingBuffer[int](100)
    for i in 1..50:
      rb.add(i)
    rb.clear()
    check rb.capacity == 100

  test "can add after clear":
    let rb = newRingBuffer[int](5)
    for i in 1..5:
      rb.add(i)
    rb.clear()
    rb.add(100)
    check rb.size == 1
    let items = rb.items()
    check items == @[100]

suite "RingBuffer - Edge Cases":

  test "capacity 0 creates empty data":
    let rb = newRingBuffer[int](0)
    check rb.capacity == 0
    check rb.data.len == 0

  test "capacity 0 add raises index error":
    let rb = newRingBuffer[int](0)
    var raised = false
    try:
      rb.add(1)
    except IndexDefect:
      raised = true
    check raised

  test "single item buffer":
    let rb = newRingBuffer[int](1)
    rb.add(1)
    check rb.size == 1
    rb.add(2)
    check rb.size == 1
    let items = rb.items()
    check items == @[2]

  test "empty items from empty buffer":
    let rb = newRingBuffer[string](5)
    let items = rb.items()
    check items.len == 0

  test "string items":
    let rb = newRingBuffer[string](3)
    rb.add("a")
    rb.add("b")
    rb.add("c")
    rb.add("d")
    let items = rb.items()
    check items == @["b", "c", "d"]

  test "large number of items":
    let rb = newRingBuffer[int](10)
    for i in 1..1000:
      rb.add(i)
    check rb.size == 10
    let items = rb.items()
    check items == @[991, 992, 993, 994, 995, 996, 997, 998, 999, 1000]

  test "wrap around twice":
    let rb = newRingBuffer[int](3)
    for i in 1..9:
      rb.add(i)
    check rb.size == 3
    let items = rb.items()
    check items == @[7, 8, 9]

suite "RingBuffer - Order Preservation":

  test "fifo order maintained":
    let rb = newRingBuffer[int](10)
    rb.add(1)
    rb.add(2)
    rb.add(3)
    rb.add(4)
    rb.add(5)
    let items = rb.items()
    check items[0] == 1
    check items[^1] == 5

  test "fifo order after wrap":
    let rb = newRingBuffer[int](4)
    rb.add(10)
    rb.add(20)
    rb.add(30)
    rb.add(40)
    rb.add(50)
    rb.add(60)
    let items = rb.items()
    check items[0] == 30
    check items[^1] == 60

suite "RingBuffer - Type Safety":

  test "int64 type":
    let rb = newRingBuffer[int64](5)
    rb.add(100'i64)
    rb.add(200'i64)
    let items = rb.items()
    check items[0] == 100'i64

  test "float type":
    let rb = newRingBuffer[float64](5)
    rb.add(1.5)
    rb.add(2.5)
    let items = rb.items()
    check items[0] == 1.5
    check items[1] == 2.5

  test "ClockOffset with all fields":
    let rb = newRingBuffer[ClockOffset](3)
    let o1 = ClockOffset(offset: 100.0, delay: 50.0, peerId: "peer1",
        confidence: 0.9, lastUpdate: 1000)
    let o2 = ClockOffset(offset: 200.0, delay: 100.0, peerId: "peer2",
        confidence: 0.8, lastUpdate: 2000)
    rb.add(o1)
    rb.add(o2)
    let items = rb.items()
    check items[0].peerId == "peer1"
    check items[1].peerId == "peer2"
