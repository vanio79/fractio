# Unit tests for external_merge_sort.nim
#
# Tests the external merge sort implementation for ORDER BY support.
# Tests in-memory sorting, chunk-based sorting, and k-way merge.

import std/[unittest, options, os, sequtils, strutils]
import fractio/sql/ast
import fractio/sql/data_row
import fractio/core/types as coreTypes # for ValueRef, newValueRef
import fractio/utils/external_merge_sort

suite "Sort Key Comparison":
  test "compare int values ascending":
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "id"),
        descending: false)]
    let a = @[DataRowValue(kind: drvkInt, intVal: 1)]
    let b = @[DataRowValue(kind: drvkInt, intVal: 5)]
    check compareSortKeys(a, b, specs) == -1

  test "compare int values descending":
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "id"),
        descending: true)]
    let a = @[DataRowValue(kind: drvkInt, intVal: 1)]
    let b = @[DataRowValue(kind: drvkInt, intVal: 5)]
    check compareSortKeys(a, b, specs) == 1 # Descending: larger values come first

  test "compare string values ascending":
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "name"),
        descending: false)]
    let a = @[DataRowValue(kind: drvkString, strVal: "alice")]
    let b = @[DataRowValue(kind: drvkString, strVal: "bob")]
    check compareSortKeys(a, b, specs) == -1

  test "compare equal values":
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "id"),
        descending: false)]
    let a = @[DataRowValue(kind: drvkInt, intVal: 10)]
    let b = @[DataRowValue(kind: drvkInt, intVal: 10)]
    check compareSortKeys(a, b, specs) == 0

  test "compare null values (nulls sort last in ascending)":
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "val"),
        descending: false)]
    let a = @[DataRowValue(kind: drvkNull)]
    let b = @[DataRowValue(kind: drvkInt, intVal: 5)]
    check compareSortKeys(a, b, specs) == 1 # Null > non-null in ascending

  test "compare null values (nulls sort last in descending)":
    # In DESC order: NULLs still sort last (special case - no DESC flip for NULLs)
    # compare(null, 5, DESC) should return 1 (null > non-null, appears last)
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "val"),
        descending: true)]
    let a = @[DataRowValue(kind: drvkNull)]
    let b = @[DataRowValue(kind: drvkInt, intVal: 5)]
    check compareSortKeys(a, b, specs) == 1 # Null > non-null (null sorts last)

  test "compare multiple sort keys":
    let specs = @[
      SortSpec(expr: Expr(kind: exColumn, colName: "age"), descending: false),
      SortSpec(expr: Expr(kind: exColumn, colName: "name"), descending: false)
    ]
    let a = @[DataRowValue(kind: drvkInt, intVal: 25), DataRowValue(
        kind: drvkString, strVal: "alice")]
    let b = @[DataRowValue(kind: drvkInt, intVal: 25), DataRowValue(
        kind: drvkString, strVal: "bob")]
    check compareSortKeys(a, b, specs) == -1 # Same age, alice < bob

suite "In-Memory Sort":
  test "sort empty rows":
    let rows: seq[seq[string]] = @[]
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "id"),
        descending: false)]
    let columns = @["id", "name"]
    let sorted = sortRowsInMemory(rows, specs, columns)
    check sorted.len == 0

  test "sort single row":
    let rows = @[@["10", "alice"]]
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "id"),
        descending: false)]
    let columns = @["id", "name"]
    let sorted = sortRowsInMemory(rows, specs, columns)
    check sorted.len == 1
    check sorted[0] == @["10", "alice"]

  test "sort int column ascending":
    let rows = @[
      @["5", "bob"],
      @["10", "alice"],
      @["3", "charlie"]
    ]
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "id"),
        descending: false)]
    let columns = @["id", "name"]
    let sorted = sortRowsInMemory(rows, specs, columns)
    check sorted.len == 3
    check sorted[0] == @["3", "charlie"]
    check sorted[1] == @["5", "bob"]
    check sorted[2] == @["10", "alice"]

  test "sort int column descending":
    let rows = @[
      @["5", "bob"],
      @["10", "alice"],
      @["3", "charlie"]
    ]
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "id"),
        descending: true)]
    let columns = @["id", "name"]
    let sorted = sortRowsInMemory(rows, specs, columns)
    check sorted.len == 3
    check sorted[0] == @["10", "alice"]
    check sorted[1] == @["5", "bob"]
    check sorted[2] == @["3", "charlie"]

  test "sort string column ascending":
    let rows = @[
      @["1", "bob"],
      @["2", "alice"],
      @["3", "charlie"]
    ]
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "name"),
        descending: false)]
    let columns = @["id", "name"]
    let sorted = sortRowsInMemory(rows, specs, columns)
    check sorted.len == 3
    check sorted[0] == @["2", "alice"]
    check sorted[1] == @["1", "bob"]
    check sorted[2] == @["3", "charlie"]

  test "sort by multiple columns":
    let rows = @[
      @["25", "bob"],
      @["25", "alice"],
      @["30", "charlie"],
      @["20", "dave"]
    ]
    let specs = @[
      SortSpec(expr: Expr(kind: exColumn, colName: "age"), descending: false),
      SortSpec(expr: Expr(kind: exColumn, colName: "name"), descending: false)
    ]
    let columns = @["age", "name"]
    let sorted = sortRowsInMemory(rows, specs, columns)
    check sorted.len == 4
    check sorted[0] == @["20", "dave"]
    check sorted[1] == @["25", "alice"]
    check sorted[2] == @["25", "bob"]
    check sorted[3] == @["30", "charlie"]

  test "sort by expression (column + constant)":
    # ORDER BY id + 10 DESC - tests expression evaluation in sort
    let rows = @[
      @["1", "alice"],
      @["5", "bob"],
      @["3", "charlie"]
    ]
    # Expression: id + 10 (descending)
    let addExpr = Expr(
      kind: exBinOp,
      binOp: boAdd,
      binLeft: Expr(kind: exColumn, colName: "id"),
      binRight: Expr(kind: exLiteral, litValue: newValueRef(10'i64))
    )
    let specs = @[SortSpec(expr: addExpr, descending: true)]
    let columns = @["id", "name"]
    let sorted = sortRowsInMemory(rows, specs, columns)
    check sorted.len == 3
    # id + 10 values: 11, 15, 13 -> DESC: 15, 13, 11
    check sorted[0] == @["5", "bob"] # 5+10=15, DESC first
    check sorted[1] == @["3", "charlie"] # 3+10=13
    check sorted[2] == @["1", "alice"] # 1+10=11

  test "sort handles NULL values":
    let rows = @[
      @["10", "alice"],
      @["NULL", "bob"],
      @["5", "charlie"]
    ]
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "id"),
        descending: false)]
    let columns = @["id", "name"]
    let sorted = sortRowsInMemory(rows, specs, columns)
    check sorted.len == 3
    check sorted[0] == @["5", "charlie"]
    check sorted[1] == @["10", "alice"]
    check sorted[2] == @["NULL", "bob"] # NULL sorts last

suite "OrderItem to SortSpec Conversion":
  test "convert single OrderItem":
    let orderItems = @[OrderItem(expr: Expr(kind: exColumn, colName: "id"), desc: false)]
    let columns = @["id", "name"]
    let specs = orderItemsToSortSpecs(orderItems, columns)
    check specs.len == 1
    check specs[0].expr.colName == "id"
    check specs[0].descending == false
    check specs[0].columnIndex == 0

  test "convert descending OrderItem":
    let orderItems = @[OrderItem(expr: Expr(kind: exColumn, colName: "name"), desc: true)]
    let columns = @["id", "name"]
    let specs = orderItemsToSortSpecs(orderItems, columns)
    check specs.len == 1
    check specs[0].descending == true
    check specs[0].columnIndex == 1

  test "convert multiple OrderItems":
    let orderItems = @[
      OrderItem(expr: Expr(kind: exColumn, colName: "age"), desc: false),
      OrderItem(expr: Expr(kind: exColumn, colName: "name"), desc: true)
    ]
    let columns = @["id", "name", "age"]
    let specs = orderItemsToSortSpecs(orderItems, columns)
    check specs.len == 2
    check specs[0].expr.colName == "age"
    check specs[0].descending == false
    check specs[1].expr.colName == "name"
    check specs[1].descending == true

suite "Row Serialization":
  test "encode and decode sorted row":
    let row = SortedRow(
      row: @["10", "alice"],
      sortKeys: @[DataRowValue(kind: drvkInt, intVal: 10)]
    )
    let encoded = encodeSortedRow(row)
    check encoded.len > 0

    let decoded = decodeSortedRow(encoded)
    check decoded.row == @["10", "alice"]
    check decoded.sortKeys.len == 1
    check decoded.sortKeys[0].kind == drvkInt
    check decoded.sortKeys[0].intVal == 10

  test "encode and decode with multiple sort keys":
    let row = SortedRow(
      row: @["25", "alice", "engineer"],
      sortKeys: @[
        DataRowValue(kind: drvkInt, intVal: 25),
        DataRowValue(kind: drvkString, strVal: "alice")
      ]
    )
    let encoded = encodeSortedRow(row)
    let decoded = decodeSortedRow(encoded)
    check decoded.row == @["25", "alice", "engineer"]
    check decoded.sortKeys.len == 2
    check decoded.sortKeys[0].intVal == 25
    check decoded.sortKeys[1].strVal == "alice"

  test "encode and decode with null sort key":
    let row = SortedRow(
      row: @["NULL", "bob"],
      sortKeys: @[DataRowValue(kind: drvkNull)]
    )
    let encoded = encodeSortedRow(row)
    let decoded = decodeSortedRow(encoded)
    check decoded.row == @["NULL", "bob"]
    check decoded.sortKeys.len == 1
    check decoded.sortKeys[0].kind == drvkNull

suite "External Merge Sort (Chunk-based)":
  setup:
    # Create temp directory for tests
    let testTempDir = "/tmp/fractio-sort-test"
    if dirExists(testTempDir):
      try:
        removeDir(testTempDir)
      except OSError:
        discard
    createDir(testTempDir)

  teardown:
    # Clean up temp directory
    if dirExists(testTempDir):
      try:
        removeDir(testTempDir)
      except OSError:
        discard

  test "external sort small dataset":
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "id"),
        descending: false)]
    let columns = @["id", "name"]
    let config = newSortConfig(chunkSize = 2, tempDir = testTempDir)
    let sorter = newExternalMergeSorter(specs, columns, config)

    # Add rows
    let rows = @[
      @["5", "bob"],
      @["10", "alice"],
      @["3", "charlie"],
      @["7", "dave"]
    ]
    sorter.addRowsToChunk(rows[0..1])
    sorter.addRowsToChunk(rows[2..3])

    # Initialize merge
    sorter.initMergePhase()

    # Read sorted rows
    var resultRows: seq[seq[string]] = @[]
    while sorter.hasNextRow():
      let rowOpt = sorter.nextRow()
      if rowOpt.isSome:
        resultRows.add(rowOpt.get())

    sorter.closeSorter()

    check resultRows.len == 4
    check resultRows[0] == @["3", "charlie"]
    check resultRows[1] == @["5", "bob"]
    check resultRows[2] == @["7", "dave"]
    check resultRows[3] == @["10", "alice"]

  test "external sort with single chunk":
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "id"),
        descending: false)]
    let columns = @["id", "name"]
    let config = newSortConfig(chunkSize = 100, tempDir = testTempDir)
    let sorter = newExternalMergeSorter(specs, columns, config)

    let rows = @[
      @["5", "bob"],
      @["10", "alice"],
      @["3", "charlie"]
    ]
    sorter.addRowsToChunk(rows)
    sorter.initMergePhase()

    var resultRows: seq[seq[string]] = @[]
    while sorter.hasNextRow():
      let rowOpt = sorter.nextRow()
      if rowOpt.isSome:
        resultRows.add(rowOpt.get())

    sorter.closeSorter()

    check resultRows.len == 3
    check resultRows[0] == @["3", "charlie"]
    check resultRows[1] == @["5", "bob"]
    check resultRows[2] == @["10", "alice"]

  test "external sort descending":
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "id"),
        descending: true)]
    let columns = @["id", "name"]
    let config = newSortConfig(chunkSize = 2, tempDir = testTempDir)
    let sorter = newExternalMergeSorter(specs, columns, config)

    let rows = @[
      @["5", "bob"],
      @["10", "alice"],
      @["3", "charlie"]
    ]
    sorter.addRowsToChunk(rows[0..1])
    sorter.addRowsToChunk(rows[2..2])

    sorter.initMergePhase()

    var resultRows: seq[seq[string]] = @[]
    while sorter.hasNextRow():
      let rowOpt = sorter.nextRow()
      if rowOpt.isSome:
        resultRows.add(rowOpt.get())

    sorter.closeSorter()

    check resultRows.len == 3
    check resultRows[0] == @["10", "alice"]
    check resultRows[1] == @["5", "bob"]
    check resultRows[2] == @["3", "charlie"]

  test "external sort creates multiple chunk files":
    # This test explicitly verifies that temp files are created
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "id"),
        descending: false)]
    let columns = @["id", "name"]
    let config = newSortConfig(chunkSize = 2, tempDir = testTempDir)
    let sorter = newExternalMergeSorter(specs, columns, config)

    # Add 6 rows across 3 chunks (chunkSize=2)
    let rows = @[
      @["5", "bob"],
      @["10", "alice"],
      @["3", "charlie"],
      @["7", "dave"],
      @["1", "eve"],
      @["9", "frank"]
    ]
    sorter.addRowsToChunk(rows[0..1]) # chunk 0
    sorter.addRowsToChunk(rows[2..3]) # chunk 1
    sorter.addRowsToChunk(rows[4..5]) # chunk 2

    # Check that chunk files were created
    let chunkFiles = walkFiles(testTempDir / "*_chunk_*.dat").toSeq
    check chunkFiles.len == 3 # Should have 3 chunk files

    # Verify each file exists and has content
    for f in chunkFiles:
      check fileExists(f)
      check getFileSize(f) > 0

    # Initialize merge and read all rows
    sorter.initMergePhase()
    var resultRows: seq[seq[string]] = @[]
    while sorter.hasNextRow():
      let rowOpt = sorter.nextRow()
      if rowOpt.isSome:
        resultRows.add(rowOpt.get())

    sorter.closeSorter()

    # Verify sorted output
    check resultRows.len == 6
    check resultRows[0] == @["1", "eve"]
    check resultRows[1] == @["3", "charlie"]
    check resultRows[2] == @["5", "bob"]
    check resultRows[3] == @["7", "dave"]
    check resultRows[4] == @["9", "frank"]
    check resultRows[5] == @["10", "alice"]

suite "Streaming Sort Iterator":
  setup:
    let testTempDir = "/tmp/fractio-sort-test-stream"
    if dirExists(testTempDir):
      try:
        removeDir(testTempDir)
      except OSError:
        discard
    createDir(testTempDir)

  teardown:
    if dirExists(testTempDir):
      try:
        removeDir(testTempDir)
      except OSError:
        discard

  test "streaming iterator basic":
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "id"),
        descending: false)]
    let columns = @["id", "name"]
    let iter = newStreamingSortIterator(specs, columns, chunkSize = 10)

    let rows = @[
      @["5", "bob"],
      @["10", "alice"],
      @["3", "charlie"]
    ]
    iter.addRowsToIterator(rows)
    iter.finalizeIterator()

    var resultRows: seq[seq[string]] = @[]
    while iter.hasNextSortedRow():
      let rowOpt = iter.nextSortedRow()
      if rowOpt.isSome:
        resultRows.add(rowOpt.get())

    iter.closeSortIterator()

    check resultRows.len == 3
    check resultRows[0] == @["3", "charlie"]
    check resultRows[1] == @["5", "bob"]
    check resultRows[2] == @["10", "alice"]

  test "streaming iterator with limit":
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "id"),
        descending: false)]
    let columns = @["id", "name"]
    let iter = newStreamingSortIterator(specs, columns, chunkSize = 10)
    iter.limit = 2

    let rows = @[
      @["5", "bob"],
      @["10", "alice"],
      @["3", "charlie"],
      @["7", "dave"]
    ]
    iter.addRowsToIterator(rows)
    iter.finalizeIterator()

    var resultRows: seq[seq[string]] = @[]
    while iter.hasNextSortedRow():
      let rowOpt = iter.nextSortedRow()
      if rowOpt.isSome:
        resultRows.add(rowOpt.get())

    iter.closeSortIterator()

    check resultRows.len == 2
    check resultRows[0] == @["3", "charlie"]
    check resultRows[1] == @["5", "bob"]

suite "Format Sort Specs":
  test "format single ascending spec":
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "id"),
        descending: false)]
    let formatted = formatSortSpecs(specs)
    check formatted == "id ASC"

  test "format single descending spec":
    let specs = @[SortSpec(expr: Expr(kind: exColumn, colName: "name"),
        descending: true)]
    let formatted = formatSortSpecs(specs)
    check formatted == "name DESC"

  test "format multiple specs":
    let specs = @[
      SortSpec(expr: Expr(kind: exColumn, colName: "age"), descending: false),
      SortSpec(expr: Expr(kind: exColumn, colName: "name"), descending: true)
    ]
    let formatted = formatSortSpecs(specs)
    check formatted == "age ASC, name DESC"

suite "Streaming Reverse (PK DESC Optimization)":
  setup:
    let testTempDir = "/tmp/fractio-reverse-test"
    if dirExists(testTempDir):
      try:
        removeDir(testTempDir)
      except OSError:
        discard
    createDir(testTempDir)

  teardown:
    if dirExists(testTempDir):
      try:
        removeDir(testTempDir)
      except OSError:
        discard

  test "reverse small dataset in memory":
    let rows = @[
      @["1", "Alice"],
      @["2", "Bob"],
      @["3", "Carol"]
    ]
    let columns = @["id", "name"]
    let reversed = reverseRowsWithTempFiles(rows, columns, columns, testTempDir,
        chunkSize = 10)
    check reversed.len == 3
    check reversed[0] == @["3", "Carol"]
    check reversed[1] == @["2", "Bob"]
    check reversed[2] == @["1", "Alice"]

  test "reverse large dataset with temp files":
    # Create 25 rows, chunkSize = 5, so 5 chunks
    var rows: seq[seq[string]] = @[]
    for i in 1..25:
      rows.add(@[$i, "name" & $i])
    let columns = @["id", "name"]
    let reversed = reverseRowsWithTempFiles(rows, columns, columns, testTempDir, chunkSize = 5)
    check reversed.len == 25
    # Should be reversed: 25, 24, 23, ... 1
    check reversed[0][0] == "25"
    check reversed[24][0] == "1"

  test "reverse empty dataset":
    let rows: seq[seq[string]] = @[]
    let columns = @["id"]
    let reversed = reverseRowsWithTempFiles(rows, columns, columns, testTempDir)
    check reversed.len == 0

  test "reverse single row":
    let rows = @[@["42", "test"]]
    let columns = @["id", "name"]
    let reversed = reverseRowsWithTempFiles(rows, columns, columns, testTempDir)
    check reversed.len == 1
    check reversed[0] == @["42", "test"]

  test "StreamingReverseIterator basic usage":
    let columns = @["id", "name"]
    let iter = newStreamingReverseIterator(columns, columns, testTempDir, chunkSize = 3)
    # Add rows in chunks
    iter.addChunkToReverse(@[@["1", "a"], @["2", "b"], @["3", "c"]])
    iter.addChunkToReverse(@[@["4", "d"], @["5", "e"], @["6", "f"]])
    iter.addChunkToReverse(@[@["7", "g"], @["8", "h"]])
    # Initialize and read in reverse
    iter.initReversePhase()
    var resultRows: seq[seq[string]] = @[]
    while iter.hasNextRow():
      let rowOpt = iter.nextRow()
      if rowOpt.isSome:
        resultRows.add(rowOpt.get())
    iter.closeIterator()
    check resultRows.len == 8
    # Should be reversed: 8, 7, 6, 5, 4, 3, 2, 1
    check resultRows[0][0] == "8"
    check resultRows[1][0] == "7"
    check resultRows[2][0] == "6"
    check resultRows[3][0] == "5"
    check resultRows[7][0] == "1"

  test "StreamingReverseIterator with limit simulation":
    let columns = @["id"]
    let iter = newStreamingReverseIterator(columns, columns, testTempDir, chunkSize = 2)
    # Add 6 rows
    iter.addChunkToReverse(@[@["1"], @["2"]])
    iter.addChunkToReverse(@[@["3"], @["4"]])
    iter.addChunkToReverse(@[@["5"], @["6"]])
    iter.initReversePhase()
    var resultRows: seq[seq[string]] = @[]
    var count = 0
    while iter.hasNextRow() and count < 3: # Simulate LIMIT 3
      let rowOpt = iter.nextRow()
      if rowOpt.isSome:
        resultRows.add(rowOpt.get())
        inc count
    iter.closeIterator()
    check resultRows.len == 3
    check resultRows[0][0] == "6"
    check resultRows[1][0] == "5"
    check resultRows[2][0] == "4"
