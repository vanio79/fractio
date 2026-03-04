# Unit tests for flush manager module
# Tests for FlushManager functionality

import unittest
import fractio/storage/flush/manager

suite "Flush Manager Unit Tests":

  test "FlushManager creation":
    let manager = newFlushManager()
    check manager.len() == 0

  test "FlushManager enqueue and dequeue":
    let manager = newFlushManager()
    check manager.len() == 0

    # Create a dummy task with nil pointer
    let task = Task(keyspacePtr: nil)

    # Enqueue task
    manager.enqueue(task)
    check manager.len() == 1

    # Dequeue task
    let dequeued = manager.dequeue()
    check dequeued != nil
    check manager.len() == 0

    # Dequeue from empty queue
    let emptyDequeue = manager.dequeue()
    check emptyDequeue == nil

  test "FlushManager multiple operations":
    let manager = newFlushManager()

    # Create dummy tasks
    let task1 = Task(keyspacePtr: nil)
    let task2 = Task(keyspacePtr: nil)

    # Enqueue multiple tasks
    manager.enqueue(task1)
    manager.enqueue(task2)
    check manager.len() == 2

    # Dequeue first task
    let first = manager.dequeue()
    check first != nil
    check manager.len() == 1

    # Dequeue second task
    let second = manager.dequeue()
    check second != nil
    check manager.len() == 0

    # Queue is now empty
    let third = manager.dequeue()
    check third == nil

  test "FlushManager clear":
    let manager = newFlushManager()

    # Add some tasks
    let task = Task(keyspacePtr: nil)

    manager.enqueue(task)
    manager.enqueue(task)
    check manager.len() == 2

    # Clear the queue
    manager.clear()
    check manager.len() == 0

    # Dequeue from cleared queue
    let result = manager.dequeue()
    check result == nil

  test "FlushManager waitForEmpty":
    let manager = newFlushManager()

    # Add a task
    let task = Task(keyspacePtr: nil)
    manager.enqueue(task)
    check manager.len() == 1

    # Dequeue it
    discard manager.dequeue()

    # Wait should return immediately since queue is empty
    manager.waitForEmpty(timeoutMs = 100)
    check manager.len() == 0
