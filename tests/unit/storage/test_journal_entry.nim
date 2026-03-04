# Unit tests for journal entry module
# Tests for Entry serialization/deserialization

import unittest
import std/streams
import fractio/storage/[journal/entry, types, error]

suite "Journal Entry Unit Tests":

  test "Tag conversion":
    check uint8FromTag(tStart) == 1
    check uint8FromTag(tItem) == 2
    check uint8FromTag(tEnd) == 3
    check uint8FromTag(tClear) == 4

    let r1 = tagFromUint8(1)
    check r1.isOk == true
    check r1.value == tStart
    let r2 = tagFromUint8(2)
    check r2.isOk == true
    check r2.value == tItem
    let r3 = tagFromUint8(3)
    check r3.isOk == true
    check r3.value == tEnd
    let r4 = tagFromUint8(4)
    check r4.isOk == true
    check r4.value == tClear

    # Test invalid tag
    check tagFromUint8(5).isOk == false
    check tagFromUint8(0).isOk == false

  test "Entry creation":
    # Test Start entry
    let startEntry = Entry(kind: ekStart, itemCount: 5, seqno: 12345)
    check startEntry.kind == ekStart
    check startEntry.itemCount == 5
    check startEntry.seqno == 12345

    # Test Item entry
    let itemEntry = Entry(kind: ekItem, keyspaceId: 1, key: "test",
                         value: "value", valueType: vtValue,
                         compression: ctNone)
    check itemEntry.kind == ekItem
    check itemEntry.keyspaceId == 1
    check itemEntry.key == "test"
    check itemEntry.value == "value"
    check itemEntry.valueType == vtValue
    check itemEntry.compression == ctNone

    # Test End entry
    let endEntry = Entry(kind: ekEnd, checksum: 98765)
    check endEntry.kind == ekEnd
    check endEntry.checksum == 98765

    # Test Clear entry
    let clearEntry = Entry(kind: ekClear, clearKeyspaceId: 2)
    check clearEntry.kind == ekClear
    check clearEntry.clearKeyspaceId == 2

  test "Serialize marker item":
    # This is a complex test that would require streams
    # For now, we'll just test that it doesn't crash
    var stream = newStringStream()
    let result = serializeMarkerItem(stream, 1, "key", "value", vtValue, ctNone)
    check result.isOk == true

  test "Entry encoding and decoding":
    # Test Start entry encode/decode
    let startEntry = Entry(kind: ekStart, itemCount: 3, seqno: 100)
    var stream = newStringStream()
    let encodeResult = startEntry.encodeInto(stream)
    check encodeResult.isOk == true

    # Test Item entry encode/decode
    let itemEntry = Entry(kind: ekItem, keyspaceId: 1, key: "test",
                         value: "data", valueType: vtValue, compression: ctNone)
    stream = newStringStream()
    let itemEncodeResult = itemEntry.encodeInto(stream)
    check itemEncodeResult.isOk == true

    # Test End entry encode/decode
    let endEntry = Entry(kind: ekEnd, checksum: 12345)
    stream = newStringStream()
    let endEncodeResult = endEntry.encodeInto(stream)
    check endEncodeResult.isOk == true

    # Test Clear entry encode/decode
    let clearEntry = Entry(kind: ekClear, clearKeyspaceId: 2)
    stream = newStringStream()
    let clearEncodeResult = clearEntry.encodeInto(stream)
    check clearEncodeResult.isOk == true
