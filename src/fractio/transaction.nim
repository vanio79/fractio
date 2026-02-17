# Transaction Manager
# Provides ACID transactions with snapshot isolation
# Coordinates with MVCC storage engine

import ../core/types
import ../core/errors
import ../storage/mvcc

type
  TransactionManager* = ref object
    storage*: MVCCStorage
    activeTransactions*: Table[TransactionID, Transaction]
    mutex*: pointer
    nextTxId*: TransactionID
    committedTxCount*: Atomic[int]
    abortedTxCount*: Atomic[int]

  TransactionContext* = object
    tx*: Transaction
    storage*: MVCCStorage
    writeSet*: WriteSet

proc newTransactionManager*(storage: MVCCStorage): TransactionManager =
  result = TransactionManager(
    storage: storage,
    activeTransactions: initTable[TransactionID, Transaction](),
    mutex: createMutex(),
    nextTxId: TransactionID(1),
    committedTxCount: atomicInit(0),
    abortedTxCount: atomicInit(0)
  )

# Begin a new transaction
proc beginTransaction*(tm: TransactionManager): Transaction =
  withLock(tm.mutex):
    let txId = tm.nextTxId
    tm.nextTxId = TransactionID(txId.int64 + 1)

    let ts = tm.storage.currentTimestamp()
    let tx = Transaction(
      id: txId,
      timestamp: ts,
      status: tsActive,
      readSnapshot: ts,
      mutatedTables: initHashSet[string](),
      mutex: createMutex()
    )

    tm.activeTransactions[txId] = tx
    return tx

# Commit a transaction
proc commit*(tm: TransactionManager, tx: Transaction): FractioError =
  if tx.status != tsActive:
    return transactionError("Cannot commit non-active transaction", "commit")

  # Validate write-write conflicts
  # (Omitted for brevity - would check other active transactions)

  # Commit to storage
  let err = tm.storage.commit(tx.timestamp)
  if isError(err):
    return err

  # Update transaction status
  tx.status = tsCommitted
  atomicInc(tm.committedTxCount)

  # Remove from active transactions (after a delay to allow MVCC reads)
  # In production, would have a grace period
  withLock(tm.mutex):
    tm.activeTransactions.excl(tx.id)

  return noError()

# Abort/rollback a transaction
proc rollback*(tm: TransactionManager, tx: Transaction): FractioError =
  if tx.status != tsActive:
    return transactionError("Cannot rollback non-active transaction", "rollback")

  tx.status = tsAborted
  atomicInc(tm.abortedTxCount)

  withLock(tm.mutex):
    tm.activeTransactions.excl(tx.id)

  return noError()

# Read operation (consistent snapshot)
proc read*(tm: TransactionManager, tx: Transaction, tableName: string): seq[Row] =
  let snapshot = tm.storage.getTableSnapshot(tableName, tx.readSnapshot)
  if snapshot == nil:
    return @[]
  return snapshot.rows

# Read with condition
proc readWhere*(tm: TransactionManager, tx: Transaction, tableName: string,
               condition: ConditionRef = nil): seq[Row] =
  var rows = tm.read(tx, tableName)

  if condition == nil:
    return rows

  # Filter rows based on condition
  result = @[]
  for row in rows:
    if evalCondition(condition, row):
      result.add(row)

# Insert operation
proc insert*(tm: TransactionManager, tx: Transaction, tableName: string,
            row: Row): FractioError =
  if tx.status != tsActive:
    return transactionError("Transaction not active", "insert")

  # Track mutated table
  tx.mutatedTables.incl(tableName)

  # Insert into storage
  let err = tm.storage.insertRow(tableName, row, tx.timestamp)
  return err

# Update operation
proc update*(tm: TransactionManager, tx: Transaction, tableName: string,
            rowIds: HashSet[RowID], newValues: seq[ValueRef]): FractioError =
  if tx.status != tsActive:
    return transactionError("Transaction not active", "update")

  tx.mutatedTables.incl(tableName)

  # Build update map
  var updates = initTable[RowID, Row]()
  let tableVer = tm.storage.tables.getOrDefault(tableName)
  if tableVer == nil:
    return semanticError("Table not found: " & tableName, "update")

  # Get current rows
  let snapshot = tm.storage.getTableSnapshot(tableName, tx.readSnapshot)
  if snapshot == nil:
    return semanticError("Table not found in snapshot", "update")

  for row in snapshot.rows:
    if rowIds.contains(row.id):
      var newRow = Row(id: row.id, values: newValues,
                      createdAt: row.createdAt, version: row.version + 1,
                      updatedAt: getTime().toUnix * 1000)
      updates[row.id] = newRow

  let err = tm.storage.updateRows(tableName, updates, tx.timestamp)
  return err

# Delete operation
proc delete*(tm: TransactionManager, tx: Transaction, tableName: string,
            rowIds: HashSet[RowID]): FractioError =
  if tx.status != tsActive:
    return transactionError("Transaction not active", "delete")

  tx.mutatedTables.incl(tableName)

  let err = tm.storage.deleteRows(tableName, rowIds, tx.timestamp)
  return err

# Evaluate a condition against a row
proc evalCondition*(condition: ConditionRef, row: Row): bool =
  if condition == nil:
    return true

  # Evaluate left operand
  let leftVal = case condition.left.kind
    of opkColumn:
      # Get column value from row (simplified - would need column index)
      row.values[0] # Placeholder
    of opkValue:
      condition.left.value
    of opkSubquery:
      # Subquery evaluation (not implemented)
      nil

  # Evaluate right operand
  let rightVal = case condition.right.kind
    of opkColumn:
      row.values[0]
    of opkValue:
      condition.right.value
    of opkSubquery:
      nil

  # Compare
  case condition.op
  of copEqual:
    result = leftVal.kind == rightVal.kind and
             leftVal.intValue == rightVal.intValue # Simplified
  of copGreater:
    result = leftVal.intValue > rightVal.intValue
  of copLess:
    result = leftVal.intValue < rightVal.intValue
  else:
    result = false

# Helper to create empty constraint error
proc noError*(): FractioError = FractioError(kind: fekNone, message: "",
    code: 0, context: "")
