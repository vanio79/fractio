# Unit tests for write_buffer_manager module
# Tests for WriteBufferManager functionality

import unittest
import fractio/storage/write_buffer_manager

suite "WriteBufferManager Unit Tests":

  test "WriteBufferManager creation":
    let manager = newWriteBufferManager()
    check manager.get() == 0

  test "WriteBufferManager allocate":
    let manager = newWriteBufferManager()

    let after1 = manager.allocate(100)
    check after1 == 100
    check manager.get() == 100

    let after2 = manager.allocate(50)
    check after2 == 150
    check manager.get() == 150

  test "WriteBufferManager free":
    let manager = newWriteBufferManager()

    # Allocate some bytes
    discard manager.allocate(200)
    check manager.get() == 200

    # Free some bytes
    let afterFree1 = manager.free(50)
    check afterFree1 == 150
    check manager.get() == 150

    # Free more bytes
    let afterFree2 = manager.free(100)
    check afterFree2 == 50
    check manager.get() == 50

    # Free more than available (should saturate at 0)
    let afterFree3 = manager.free(100)
    check afterFree3 == 0
    check manager.get() == 0

  test "WriteBufferManager combined operations":
    let manager = newWriteBufferManager()

    # Start with 0
    check manager.get() == 0

    # Allocate and free in sequence
    discard manager.allocate(1000)
    check manager.get() == 1000

    discard manager.free(300)
    check manager.get() == 700

    discard manager.allocate(200)
    check manager.get() == 900

    discard manager.free(900)
    check manager.get() == 0

    # Try to free more than available
    discard manager.free(100)
    check manager.get() == 0 # Should stay at 0
