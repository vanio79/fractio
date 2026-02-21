# Unit tests for journal manager module
# Tests for JournalManager functionality

import unittest
import fractio/storage/journal/manager

suite "Journal Manager Unit Tests":

  test "JournalManager creation":
    let manager = newJournalManager()
    check manager.items.len == 0
    check manager.diskSpaceInBytes == 0
    check manager.sealedJournalCount() == 0
    check manager.journalCount() == 1 # Active journal

  test "JournalManager enqueue":
    let manager = newJournalManager()

    let item = JournalItem(
      path: "/test/journal.jnl",
      sizeInBytes: 1024,
      watermarks: @[] # Empty watermarks for now
    )

    manager.enqueue(item)
    check manager.items.len == 1
    check manager.diskSpaceInBytes == 1024
    check manager.sealedJournalCount() == 1
    check manager.journalCount() == 2 # Active + 1 sealed

  test "JournalManager clear":
    let manager = newJournalManager()

    let item1 = JournalItem(
      path: "/test/journal1.jnl",
      sizeInBytes: 1024,
      watermarks: @[]
    )

    let item2 = JournalItem(
      path: "/test/journal2.jnl",
      sizeInBytes: 2048,
      watermarks: @[]
    )

    manager.enqueue(item1)
    manager.enqueue(item2)
    check manager.items.len == 2
    check manager.diskSpaceInBytes == 3072

    manager.clear()
    check manager.items.len == 0
    check manager.diskSpaceInBytes == 0

  test "JournalManager disk space":
    let manager = newJournalManager()
    check manager.diskSpaceUsed() == 0

    let item = JournalItem(
      path: "/test/journal.jnl",
      sizeInBytes: 4096,
      watermarks: @[]
    )

    manager.enqueue(item)
    check manager.diskSpaceUsed() == 4096

  test "JournalManager item access":
    let manager = newJournalManager()

    let item = JournalItem(
      path: "/test/test.jnl",
      sizeInBytes: 1000,
      watermarks: @[]
    )

    manager.enqueue(item)
    check manager.items.len == 1
    check manager.items[0].path == "/test/test.jnl"
    check manager.items[0].sizeInBytes == 1000
