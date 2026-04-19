# External Merge Sort for Large ORDER BY Result Sets
#
# Implements a memory-efficient sorting algorithm for large datasets.
# Uses temporary files to store sorted chunks, then merges them during streaming.
#
# Design:
# - Input rows are read from a streaming source
# - Rows are sorted in chunks (memory-limited)
# - Each sorted chunk is written to a temporary file
# - During merge phase, chunks are read back and merged using a priority queue
# - Supports multiple sort keys with ASC/DESC ordering
#
# Memory bounds:
# - Chunk size is configurable (default: 10,000 rows)
# - Each chunk is sorted in-memory then flushed to disk
# - Merge phase reads from multiple chunks using a priority queue

import std/[os, sequtils, options, strutils, algorithm, streams, times, tables, random]
import ../sql/ast
import ../sql/data_row
import ../sql/expr_eval
import ../utils/binary

# =============================================================================
# Comparison helper
# =============================================================================
# Provide <=> operator for comparison (returns -1, 0, or 1)
proc `<=>`(a, b: int64): int =
  if a < b: -1
  elif a > b: 1
  else: 0

proc `<=>`(a, b: string): int =
  if a < b: -1
  elif a > b: 1
  else: 0

# =============================================================================
# Constants
# =============================================================================

const
  DEFAULT_CHUNK_SIZE* = 10000             ## Rows per sorted chunk
  DEFAULT_MAX_OPEN_FILES* = 32            ## Maximum open chunk files during merge
  DEFAULT_TEMP_DIR* = "/tmp/fractio-sort" ## Temporary directory for chunk files

# =============================================================================
# Types
# =============================================================================

type
  SortSpec* = object
    ## Sort specification for ORDER BY
    expr*: Expr       ## Expression to evaluate for sorting
    descending*: bool ## true = DESC, false = ASC (default)
    columnIndex*: int ## Column index in the output row (for string comparison)

  SortConfig* = object
    ## Configuration for external merge sort
    chunkSize*: int      ## Rows per chunk
    maxOpenFiles*: int   ## Max open chunk files during merge
    tempDir*: string     ## Directory for temporary files
    memoryBudget*: int64 ## Memory budget in bytes (0 = unlimited)

  SortedRow* = object
    ## A row with its sort key values pre-computed
    row*: seq[string]            ## The actual row data
    sortKeys*: seq[DataRowValue] ## Pre-computed sort key values

  ChunkFile* = object
    ## A sorted chunk stored in a temporary file
    path*: string  ## File path
    rowCount*: int ## Number of rows in the chunk
    index*: int    ## Chunk index for identification

  ChunkReader* = ref object
    ## Reader for a chunk file during merge
    stream*: FileStream    ## Open file stream
    chunkIdx*: int         ## Chunk index
    currentRow*: SortedRow ## Current row (lowest in this chunk)
    exhausted*: bool       ## Whether chunk is exhausted

  ExternalMergeSorter* = ref object
    ## External merge sort implementation
    config*: SortConfig
    sortSpecs*: seq[SortSpec]
    allColumns*: seq[string]   ## All table columns for expression evaluation
    chunks*: seq[ChunkFile]    ## Sorted chunk files
    readers*: seq[ChunkReader] ## Readers for merge phase
    totalRows*: int            ## Total rows sorted
    initialized*: bool         ## Whether merge phase has started
    tempPrefix*: string        ## Prefix for temp file names

# =============================================================================
# Error Types
# =============================================================================

type
  ExternalSortError* = object of CatchableError
    ## Error during external sort operation
    code*: ExternalSortErrorCode

  ExternalSortErrorCode* = enum
    eseTempFileError ## Failed to create/write temp file
    eseMemoryLimit   ## Memory limit exceeded
    eseReadError     ## Failed to read from chunk file
    eseInvalidRow    ## Invalid row format
    eseMergeError    ## Error during merge phase

proc externalSortError(code: ExternalSortErrorCode,
    msg: string): ref ExternalSortError =
  result = newException(ExternalSortError, msg)
  result.code = code

# =============================================================================
# Configuration Helpers
# =============================================================================

proc defaultSortConfig*(): SortConfig =
  ## Create default sort configuration
  SortConfig(
    chunkSize: DEFAULT_CHUNK_SIZE,
    maxOpenFiles: DEFAULT_MAX_OPEN_FILES,
    tempDir: DEFAULT_TEMP_DIR,
    memoryBudget: 0 # unlimited
  )

proc newSortConfig*(chunkSize: int, tempDir: string = DEFAULT_TEMP_DIR): SortConfig =
  ## Create a custom sort configuration
  SortConfig(
    chunkSize: chunkSize,
    maxOpenFiles: DEFAULT_MAX_OPEN_FILES,
    tempDir: tempDir,
    memoryBudget: 0
  )

# =============================================================================
# Sort Key Comparison
# =============================================================================

proc compareSortKeys*(a, b: seq[DataRowValue], specs: seq[SortSpec]): int =
  ## Compare two rows by their sort keys.
  ## Returns -1 if a < b, 0 if equal, 1 if a > b.
  ## Handles ASC/DESC per sort specification.
  ## NULL values always sort last in both ASC and DESC order.
  for i, spec in specs:
    if i >= a.len or i >= b.len:
      return 0 # Safety check

    let keyA = a[i]
    let keyB = b[i]

    # Compare based on type
    var cmpResult: int
    if keyA.kind != keyB.kind:
      # Type mismatch: nulls sort last in both ASC and DESC
      # This is a special case - don't apply DESC flip to NULL comparison
      if keyA.kind == drvkNull:
        # a is NULL, b is non-null: a > b (NULL sorts last)
        return 1
      elif keyB.kind == drvkNull:
        # a is non-null, b is NULL: a < b (NULL sorts last)
        return -1
      else:
        # Non-null type mismatch - compare by kind ordinal
        cmpResult = int(ord(keyA.kind)) - int(ord(keyB.kind))
    else:
      # Same type - compare values
      case keyA.kind
      of drvkNull:
        cmpResult = 0
      of drvkInt:
        cmpResult = keyA.intVal <=> keyB.intVal
      of drvkFloat:
        if keyA.floatVal < keyB.floatVal:
          cmpResult = -1
        elif keyA.floatVal > keyB.floatVal:
          cmpResult = 1
        else:
          cmpResult = 0
      of drvkString:
        cmpResult = keyA.strVal <=> keyB.strVal
      of drvkBool:
        cmpResult = int(keyA.boolVal) - int(keyB.boolVal)

    # Apply DESC ordering (flip comparison)
    if spec.descending:
      cmpResult = -cmpResult

    # If not equal, return the comparison result
    if cmpResult != 0:
      return cmpResult

  # All keys equal
  return 0

proc compareSortedRows(a, b: SortedRow, specs: seq[SortSpec]): int =
  ## Compare two SortedRows using their pre-computed sort keys.
  compareSortKeys(a.sortKeys, b.sortKeys, specs)

# =============================================================================
# Row Serialization for Chunk Files
# =============================================================================

proc encodeSortedRow*(row: SortedRow): string =
  ## Encode a sorted row to a compact binary format for chunk file storage.
  ## Format:
  ## - Row column count: 4 bytes (uint32)
  ## - For each column: length-prefixed string (4 bytes + data)
  ## - Sort key count: 4 bytes (uint32)
  ## - For each sort key: type byte + type-specific data
  var w = initBinaryWriter()

  # Write row columns
  w.writeU32(uint32(row.row.len))
  for col in row.row:
    w.writeString(col)

  # Write sort keys
  w.writeU32(uint32(row.sortKeys.len))
  for key in row.sortKeys:
    w.writeU8(uint8(ord(key.kind)))
    case key.kind
    of drvkNull:
      discard
    of drvkInt:
      w.writeI64(key.intVal)
    of drvkFloat:
      w.writeFloat64(key.floatVal)
    of drvkString:
      w.writeString(key.strVal)
    of drvkBool:
      w.writeU8(if key.boolVal: 1'u8 else: 0'u8)

  w.finish()

proc decodeSortedRow*(data: string): SortedRow =
  ## Decode a sorted row from chunk file data.
  ## Raises ValueError if data is invalid.
  var r = initBinaryReader(data)

  # Read row columns
  let rowLen = int(r.readU32())
  result.row = newSeq[string](rowLen)
  for i in 0..<rowLen:
    result.row[i] = r.readString()

  # Read sort keys
  let keyLen = int(r.readU32())
  result.sortKeys = newSeq[DataRowValue](keyLen)
  for i in 0..<keyLen:
    let kind = DataRowValueKind(r.readU8())

    case kind
    of drvkNull:
      result.sortKeys[i] = DataRowValue(kind: drvkNull)
    of drvkInt:
      result.sortKeys[i] = DataRowValue(kind: drvkInt, intVal: r.readI64())
    of drvkFloat:
      result.sortKeys[i] = DataRowValue(kind: drvkFloat,
          floatVal: r.readFloat64())
    of drvkString:
      result.sortKeys[i] = DataRowValue(kind: drvkString, strVal: r.readString())
    of drvkBool:
      result.sortKeys[i] = DataRowValue(kind: drvkBool, boolVal: r.readU8() == 1)

# =============================================================================
# Chunk File Management
# =============================================================================

proc createChunkFile(sorter: ExternalMergeSorter, chunkIdx: int,
    rows: seq[SortedRow]): ChunkFile =
  ## Write a sorted chunk to a temporary file.
  let filename = sorter.tempPrefix & "_chunk_" & $chunkIdx & ".dat"
  let path = sorter.config.tempDir / filename

  # Write all rows to file using BinaryWriter
  var w = initBinaryWriter()
  w.writeU32(uint32(rows.len)) # Row count header
  for row in rows:
    # Write row data length prefix then data
    let rowData = encodeSortedRow(row)
    w.writeU32(uint32(rowData.len))
    w.writeBytes(rowData)

  # Create directory if needed
  if not dirExists(sorter.config.tempDir):
    createDir(sorter.config.tempDir)

  # Write file
  writeFile(path, w.finish())

  ChunkFile(path: path, rowCount: rows.len, index: chunkIdx)

proc readNextRowFromReader(reader: ChunkReader): Option[SortedRow] =
  ## Read the next row from a chunk reader.
  ## Returns none if chunk is exhausted.
  if reader.exhausted or reader.stream == nil or reader.stream.atEnd():
    reader.exhausted = true
    return none(SortedRow)

  try:
    # Read row data length (little-endian)
    var lenBytes: array[4, byte]
    if reader.stream.readData(addr lenBytes[0], 4) != 4:
      reader.exhausted = true
      return none(SortedRow)

    let rowLen = int(fromBytesU32(lenBytes))
    if rowLen == 0 or rowLen > 10_000_000: # Sanity check (10MB max row)
      reader.exhausted = true
      return none(SortedRow)

    # Read row data
    var rowData = newString(rowLen)
    if reader.stream.readData(addr rowData[0], rowLen) != rowLen:
      reader.exhausted = true
      return none(SortedRow)

    return some(decodeSortedRow(rowData))
  except IOError, ValueError:
    reader.exhausted = true
    return none(SortedRow)

proc cleanupChunkFiles(sorter: ExternalMergeSorter) =
  ## Delete all temporary chunk files and close readers.
  for reader in sorter.readers:
    if reader != nil and reader.stream != nil:
      try:
        reader.stream.close()
      except IOError:
        discard
  sorter.readers = @[]

  for chunk in sorter.chunks:
    try:
      if fileExists(chunk.path):
        removeFile(chunk.path)
    except OSError:
      discard # Ignore cleanup errors
  sorter.chunks = @[]

# =============================================================================
# External Merge Sort Implementation
# =============================================================================

proc newExternalMergeSorter*(specs: seq[SortSpec], allColumns: seq[string],
                              config: SortConfig = defaultSortConfig()): ExternalMergeSorter =
  ## Create a new external merge sorter.
  new(result)
  result.config = config
  result.sortSpecs = specs
  result.allColumns = allColumns
  result.chunks = @[]
  result.readers = @[]
  result.totalRows = 0
  result.initialized = false
  # Generate unique temp file prefix using timestamp and random suffix
  let ts = getTime().toUnix()
  result.tempPrefix = "sort_" & $ts & "_" & $rand(100000)

proc computeSortKeys(row: seq[string], specs: seq[SortSpec],
    allColumns: seq[string]): seq[DataRowValue] =
  ## Compute sort key values for a row.
  ## Converts string row to DataRow for expression evaluation.
  # Build DataRow from string row
  var dataRow = newDataRow()
  for i, col in allColumns:
    if i < row.len:
      # Parse string value - try int, then float, then string
      let valStr = row[i]
      if valStr == "NULL":
        dataRow[col] = newRowValue()
      elif valStr.len > 0 and valStr.allCharsInSet(Digits) or
           (valStr.len > 1 and valStr.startsWith("-") and
            valStr[1..^1].allCharsInSet(Digits)):
        dataRow[col] = newRowValue(parseBiggestInt(valStr))
      elif valStr.contains('.') and
           valStr.replace("-", "").replace(".", "").allCharsInSet(Digits):
        dataRow[col] = newRowValue(parseFloat(valStr))
      elif valStr == "true":
        dataRow[col] = newRowValue(true)
      elif valStr == "false":
        dataRow[col] = newRowValue(false)
      else:
        dataRow[col] = newRowValue(valStr)

  # Evaluate each sort expression
  for spec in specs:
    result.add(evalExprDataRow(spec.expr, dataRow))

proc sortChunk(rows: seq[SortedRow], specs: seq[SortSpec]): seq[SortedRow] =
  ## Sort a chunk of rows in-memory.
  ## Uses Nim's built-in sort with custom comparator.
  if rows.len <= 1:
    return rows

  # Sort rows by comparing their pre-computed sort keys
  result = rows
  result.sort(proc(a, b: SortedRow): int =
    compareSortedRows(a, b, specs))

proc addRowsToChunk*(sorter: ExternalMergeSorter, rows: seq[seq[string]]) =
  ## Add rows to a chunk. Rows are sorted and written to a temporary file.
  if rows.len == 0:
    return

  # Convert rows to SortedRow with computed sort keys
  var sortedRows: seq[SortedRow] = @[]
  for row in rows:
    let sortKeys = computeSortKeys(row, sorter.sortSpecs, sorter.allColumns)
    sortedRows.add(SortedRow(row: row, sortKeys: sortKeys))

  # Sort the chunk
  let sortedChunk = sortChunk(sortedRows, sorter.sortSpecs)

  # Write to file
  let chunkIdx = sorter.chunks.len
  sorter.chunks.add(createChunkFile(sorter, chunkIdx, sortedChunk))
  sorter.totalRows += sortedChunk.len

# =============================================================================
# Merge Phase (k-way merge using linear scan)
# =============================================================================

proc findSmallestReader(sorter: ExternalMergeSorter): int =
  ## Find the reader with the smallest current row.
  ## Returns -1 if all readers exhausted.
  var minIdx = -1
  for i, reader in sorter.readers:
    if not reader.exhausted and reader.currentRow.sortKeys.len > 0:
      if minIdx == -1:
        minIdx = i
      elif compareSortedRows(reader.currentRow,
          sorter.readers[minIdx].currentRow, sorter.sortSpecs) < 0:
        minIdx = i
  return minIdx

proc initMergePhase*(sorter: ExternalMergeSorter) =
  ## Initialize the merge phase by opening all chunk files and
  ## populating each reader with its first row.
  if sorter.initialized:
    return

  sorter.initialized = true

  if sorter.chunks.len == 0:
    return # No data to sort

  # Create readers for each chunk
  sorter.readers = newSeq[ChunkReader](sorter.chunks.len)
  for i, chunk in sorter.chunks:
    # Open chunk file
    let stream = newFileStream(chunk.path, fmRead)
    if stream == nil:
      raise externalSortError(eseTempFileError, "Failed to open chunk: " & chunk.path)

    sorter.readers[i] = ChunkReader(
      stream: stream,
      chunkIdx: i,
      exhausted: false
    )

    # Read row count header (skip it) - little-endian
    var headerBytes: array[4, byte]
    if stream.readData(addr headerBytes[0], 4) != 4:
      sorter.readers[i].exhausted = true
      continue

    # Read first row
    let firstRowOpt = readNextRowFromReader(sorter.readers[i])
    if firstRowOpt.isSome:
      sorter.readers[i].currentRow = firstRowOpt.get()
    else:
      sorter.readers[i].exhausted = true

proc hasNextRow*(sorter: ExternalMergeSorter): bool =
  ## Check if there are more sorted rows to return.
  if not sorter.initialized:
    sorter.initMergePhase()

  # Check if any reader is not exhausted
  for reader in sorter.readers:
    if not reader.exhausted:
      return true
  return false

proc nextRow*(sorter: ExternalMergeSorter): Option[seq[string]] =
  ## Get the next sorted row from the merge.
  ## Returns the row data without sort keys.
  if not sorter.initialized:
    sorter.initMergePhase()

  # Find the reader with the smallest row
  let minIdx = findSmallestReader(sorter)
  if minIdx == -1:
    return none(seq[string])

  # Get the row from that reader
  let resultRow = sorter.readers[minIdx].currentRow.row

  # Read next row from that reader
  let nextRowOpt = readNextRowFromReader(sorter.readers[minIdx])
  if nextRowOpt.isSome:
    sorter.readers[minIdx].currentRow = nextRowOpt.get()
  else:
    sorter.readers[minIdx].exhausted = true

  return some(resultRow)

proc closeSorter*(sorter: ExternalMergeSorter) =
  ## Close the sorter and clean up all temporary files.
  sorter.cleanupChunkFiles()
  sorter.initialized = false

# =============================================================================
# High-Level Streaming Sort API
# =============================================================================

type
  StreamingSortIterator* = ref object
    ## Iterator that sorts rows from a source using external merge sort.
    ## Supports large datasets by using temporary files.
    sorter*: ExternalMergeSorter
    pendingRows*: seq[seq[string]] ## Rows waiting to be chunked
    sourceExhausted*: bool ## All source rows collected
    finalized*: bool ## Sorter initialized for merge
    limit*: uint32 ## Maximum rows to return (0 = unlimited)
    rowsReturned*: int ## Count of rows returned

proc newStreamingSortIterator*(specs: seq[SortSpec], allColumns: seq[string],
                                chunkSize: int = DEFAULT_CHUNK_SIZE): StreamingSortIterator =
  ## Create a new streaming sort iterator.
  new(result)
  let config = newSortConfig(chunkSize)
  result.sorter = newExternalMergeSorter(specs, allColumns, config)
  result.pendingRows = @[]
  result.sourceExhausted = false
  result.finalized = false
  result.limit = 0
  result.rowsReturned = 0

proc addRowsToIterator*(iter: StreamingSortIterator, rows: seq[seq[string]]) =
  ## Add rows to the sort iterator.
  ## Rows are buffered until chunk size is reached, then sorted and written.
  if rows.len == 0:
    return

  iter.pendingRows.add(rows)

  # Write chunks as we accumulate enough rows
  while iter.pendingRows.len >= iter.sorter.config.chunkSize:
    # Take one chunk worth of rows
    let chunkRows = iter.pendingRows[0..<iter.sorter.config.chunkSize]
    iter.pendingRows = iter.pendingRows[iter.sorter.config.chunkSize..^1]

    # Sort and write chunk
    addRowsToChunk(iter.sorter, chunkRows)

proc finalizeIterator*(iter: StreamingSortIterator) =
  ## Flush any remaining rows and start merge phase.
  if iter.finalized:
    return

  # Write remaining rows as final chunk
  if iter.pendingRows.len > 0:
    addRowsToChunk(iter.sorter, iter.pendingRows)
    iter.pendingRows = @[]

  iter.sourceExhausted = true
  iter.finalized = true
  iter.sorter.initMergePhase()

proc hasNextSortedRow*(iter: StreamingSortIterator): bool =
  ## Check if there are more sorted rows available.
  if not iter.finalized:
    return false # Not finalized yet

  # Check limit
  if iter.limit > 0 and iter.rowsReturned >= int(iter.limit):
    return false

  iter.sorter.hasNextRow()

proc nextSortedRow*(iter: StreamingSortIterator): Option[seq[string]] =
  ## Get the next sorted row.
  if not iter.finalized:
    return none(seq[string])

  # Check limit
  if iter.limit > 0 and iter.rowsReturned >= int(iter.limit):
    return none(seq[string])

  let rowOpt = iter.sorter.nextRow()
  if rowOpt.isSome:
    inc iter.rowsReturned
  return rowOpt

proc closeSortIterator*(iter: StreamingSortIterator) =
  ## Close the iterator and clean up resources.
  iter.sorter.closeSorter()
  iter.pendingRows = @[]
  iter.finalized = false

# =============================================================================
# In-Memory Sort (for small result sets)
# =============================================================================

proc sortRowsInMemory*(rows: seq[seq[string]], specs: seq[SortSpec],
                       allColumns: seq[string]): seq[seq[string]] =
  ## Sort rows in-memory. Use for small result sets that fit in memory.
  ## Returns sorted rows.
  if rows.len <= 1 or specs.len == 0:
    return rows

  # Convert to SortedRow with pre-computed sort keys
  var sortedRows: seq[SortedRow] = @[]
  for row in rows:
    let sortKeys = computeSortKeys(row, specs, allColumns)
    sortedRows.add(SortedRow(row: row, sortKeys: sortKeys))

  # Sort using custom comparator
  sortedRows.sort(proc(a, b: SortedRow): int =
    compareSortedRows(a, b, specs))

  # Extract just the row data
  result = sortedRows.mapIt(it.row)

# =============================================================================
# SortSpec Helpers
# =============================================================================

proc orderItemsToSortSpecs*(orderItems: seq[OrderItem],
                            columns: seq[string]): seq[SortSpec] =
  ## Convert AST OrderItem to SortSpec.
  ## Maps column expressions to column indices.
  for item in orderItems:
    var spec = SortSpec(
      expr: item.expr,
      descending: item.desc
    )

    # Determine column index if expression is a simple column reference
    if item.expr.kind == exColumn:
      let colName = item.expr.colName
      for i, col in columns:
        if col == colName:
          spec.columnIndex = i
          break

    result.add(spec)

proc formatSortExpr(e: Expr): string =
  ## Format an expression for sort spec display (simplified version).
  case e.kind
  of exLiteral:
    if e.litValue == nil: return "NULL"
    case e.litValue.kind
    of dtInt: return $e.litValue.intValue
    of dtFloat: return $e.litValue.floatValue
    of dtString: return "'" & e.litValue.strValue & "'"
    of dtBool: return $e.litValue.boolValue
    else: return "?"
  of exColumn:
    if e.colTable.len > 0: return e.colTable & "." & e.colName
    return e.colName
  of exStar: return "*"
  else: return "?"

proc formatSortSpecs*(specs: seq[SortSpec]): string =
  ## Format sort specs for display (EXPLAIN output).
  for i, spec in specs:
    if i > 0:
      result.add(", ")
    result.add(formatSortExpr(spec.expr))
    if spec.descending:
      result.add(" DESC")
    else:
      result.add(" ASC")

# =============================================================================
# Streaming Reverse (for PK DESC optimization)
# =============================================================================

# Helper functions for row serialization (defined before use)
proc encodeRowStrings*(row: seq[string]): string =
  ## Encode a row of strings to binary format.
  var w = initBinaryWriter()
  w.writeU32(uint32(row.len))
  for col in row:
    w.writeString(col)
  w.finish()

proc decodeRowStrings*(data: string): seq[string] =
  ## Decode a row of strings from binary format.
  var r = initBinaryReader(data)
  let colCount = int(r.readU32())
  result = newSeq[string](colCount)
  for i in 0..<colCount:
    result[i] = r.readString()

type
  StreamingReverseIterator* = ref object
    ## Iterator that reverses rows from a streaming source using temp files.
    ## Memory-limited: buffers rows in chunks, writes to temp files,
    ## then reads chunks in reverse order.
    chunks*: seq[ChunkFile] ## Temp files containing buffered chunks
    currentChunkIdx*: int ## Current chunk being read (in reverse order)
    currentChunkRows*: seq[seq[string]] ## Rows from current chunk (reversed)
    currentRowIdx*: int ## Current row index within reversed chunk
    columns*: seq[string] ## Column names for output
    allColumns*: seq[string] ## All fetched columns
    tempDir*: string ## Temp directory for chunk files
    tempPrefix*: string ## Prefix for temp file names
    chunkSize*: int ## Rows per chunk
    exhausted*: bool ## Iterator exhausted flag
    initialized*: bool ## Reverse phase initialized flag

proc newStreamingReverseIterator*(columns, allColumns: seq[string],
                                  tempDir: string = DEFAULT_TEMP_DIR,
                                  chunkSize: int = DEFAULT_CHUNK_SIZE): StreamingReverseIterator =
  ## Create a new streaming reverse iterator.
  ## Used for PK DESC optimization where data needs to be reversed.
  let ts = getTime().toUnix()
  result = StreamingReverseIterator(
    columns: columns,
    allColumns: allColumns,
    tempDir: tempDir,
    tempPrefix: "reverse_" & $ts & "_" & $rand(100000),
    chunkSize: chunkSize,
    currentChunkIdx: -1,
    currentRowIdx: -1,
    exhausted: false,
    initialized: false
  )
  # Ensure temp directory exists
  if not dirExists(tempDir):
    createDir(tempDir)

proc addChunkToReverse*(iter: StreamingReverseIterator, rows: seq[seq[string]]) =
  ## Add rows to a chunk for later reversal.
  ## Rows are stored in order; will be read back in reverse.
  if rows.len == 0:
    return

  let chunkIdx = iter.chunks.len
  let filename = iter.tempPrefix & "_chunk_" & $chunkIdx & ".dat"
  let path = iter.tempDir / filename

  # Write rows to file (unsorted, just stored)
  var w = initBinaryWriter()
  w.writeU32(uint32(rows.len))
  for row in rows:
    let rowData = encodeRowStrings(row)
    w.writeU32(uint32(rowData.len))
    w.writeBytes(rowData)

  writeFile(path, w.finish())
  iter.chunks.add(ChunkFile(path: path, rowCount: rows.len, index: chunkIdx))

proc initReversePhase*(iter: StreamingReverseIterator) =
  ## Initialize the reverse phase: start reading chunks in reverse order.
  if iter.initialized or iter.chunks.len == 0:
    iter.initialized = true
    iter.exhausted = iter.chunks.len == 0
    return

  iter.initialized = true
  # Start from the last chunk
  iter.currentChunkIdx = iter.chunks.len - 1
  iter.currentRowIdx = -1 # Will be set after loading chunk

proc loadNextChunkInReverse(iter: StreamingReverseIterator): bool =
  ## Load the next chunk in reverse order and reverse its rows.
  ## Returns false if no more chunks.
  if iter.currentChunkIdx < 0:
    iter.exhausted = true
    return false

  let chunk = iter.chunks[iter.currentChunkIdx]
  let stream = newFileStream(chunk.path, fmRead)
  if stream == nil:
    iter.exhausted = true
    return false

  # Read row count
  var lenBytes: array[4, byte]
  if stream.readData(addr lenBytes[0], 4) != 4:
    stream.close()
    iter.exhausted = true
    return false
  let rowCount = int(fromBytesU32(lenBytes))

  # Read all rows
  var rows: seq[seq[string]] = @[]
  for i in 0..<rowCount:
    var rowLenBytes: array[4, byte]
    if stream.readData(addr rowLenBytes[0], 4) != 4:
      break
    let rowLen = int(fromBytesU32(rowLenBytes))
    if rowLen == 0 or rowLen > 10_000_000:
      break
    var rowData = newString(rowLen)
    if stream.readData(addr rowData[0], rowLen) != rowLen:
      break
    rows.add(decodeRowStrings(rowData))

  stream.close()

  # Reverse rows within this chunk
  rows.reverse()
  iter.currentChunkRows = rows
  iter.currentRowIdx = 0 # Start from first row of reversed chunk

  # Move to next chunk (in reverse order = decrement index)
  dec iter.currentChunkIdx

  return true

proc hasNextRow*(iter: StreamingReverseIterator): bool =
  ## Check if there's another row available.
  if iter.exhausted:
    return false

  if not iter.initialized:
    iter.initReversePhase()

  # Check if we have rows in current chunk
  if iter.currentRowIdx >= 0 and iter.currentRowIdx < iter.currentChunkRows.len:
    return true

  # Need to load next chunk
  return iter.loadNextChunkInReverse()

proc nextRow*(iter: StreamingReverseIterator): Option[seq[string]] =
  ## Get the next row in reverse order.
  if iter.exhausted:
    return none(seq[string])

  if not iter.initialized:
    iter.initReversePhase()

  # Check if we need to load a new chunk
  if iter.currentRowIdx < 0 or iter.currentRowIdx >= iter.currentChunkRows.len:
    if not iter.loadNextChunkInReverse():
      return none(seq[string])

  # Return current row and advance
  if iter.currentRowIdx >= 0 and iter.currentRowIdx < iter.currentChunkRows.len:
    let row = iter.currentChunkRows[iter.currentRowIdx]
    inc iter.currentRowIdx
    return some(row)

  return none(seq[string])

proc consumeAllRows*(iter: StreamingReverseIterator): seq[seq[string]] =
  ## Consume all remaining rows in reverse order.
  var rows: seq[seq[string]] = @[]
  while iter.hasNextRow():
    let rowOpt = iter.nextRow()
    if rowOpt.isSome:
      rows.add(rowOpt.get())
  rows

proc closeIterator*(iter: StreamingReverseIterator) =
  ## Close the iterator and clean up temp files.
  for chunk in iter.chunks:
    try:
      if fileExists(chunk.path):
        removeFile(chunk.path)
    except OSError:
      discard
  iter.chunks = @[]
  iter.exhausted = true

proc reverseRowsWithTempFiles*(rows: seq[seq[string]],
                               columns, allColumns: seq[string],
                               tempDir: string = DEFAULT_TEMP_DIR,
                               chunkSize: int = DEFAULT_CHUNK_SIZE): seq[seq[string]] =
  ## Reverse rows using temp files for memory-limited operation.
  ## Used for PK DESC optimization where data is already sorted by PK ASC
  ## but needs to be returned in DESC order.
  if rows.len <= 1:
    return rows

  # If rows fit in one chunk, just reverse in memory
  if rows.len <= chunkSize:
    result = rows
    result.reverse()
    return

  # Use streaming reverse for large datasets
  let iter = newStreamingReverseIterator(columns, allColumns, tempDir, chunkSize)

  # Add rows in chunks
  var currentChunk: seq[seq[string]] = @[]
  for row in rows:
    currentChunk.add(row)
    if currentChunk.len >= chunkSize:
      iter.addChunkToReverse(currentChunk)
      currentChunk = @[]

  # Add remaining rows
  if currentChunk.len > 0:
    iter.addChunkToReverse(currentChunk)

  # Initialize and consume
  iter.initReversePhase()
  result = iter.consumeAllRows()
  iter.closeIterator()
