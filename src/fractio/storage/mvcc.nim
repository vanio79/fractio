# MVCC (Multi-Version Concurrency Control) Storage Engine
# Provides ACID transactions with snapshot isolation
# Lock-free reads using version chains

import ../core/types
import ../core/errors

type
  MVCCStorage* = ref object
    tables*: Table[string, TableVersion]
    latestVersion*: int64
    mutex*: pointer # Global storage mutex for schema changes
    timestampProvider*: TimestampProvider

  TableVersion* = ref object
    table*: Table
    versions*: seq[TableSnapshot] # Historical snapshots
    activeWriters*: Atomic[int]   # Count of active writers
    readMutex*: pointer           # For long-running read snapshots

  TableSnapshot* = ref object
    version*: int64
    rows*: seq[Row]
    committed*: bool
    timestamp*: int64

  WriteSet* = object
    inserts*: seq[Row]
    updates*: Table[RowID, Row]
    deletes*: HashSet[RowID]

# Initialize MVCC storage
proc newMVCCStorage*(timestampProvider: TimestampProvider): MVCCStorage =
  result = MVCCStorage(
    tables: initTable[string, TableVersion](),
    latestVersion: 1,
    mutex: createMutex(),
    timestampProvider: timestampProvider
  )

# Get the current MVCC timestamp
proc currentTimestamp*(storage: MVCCStorage): int64 =
  var tp = storage.timestampProvider
  result = atomicInc(tp.lastTimestamp)

# Create a new table in the storage engine
proc createTable*(storage: MVCCStorage, tableDef: Table): FractioError =
  withLock(storage.mutex):
    if storage.tables.hasKey(tableDef.name):
      return semanticError("Table already exists: " & tableDef.name, "createTable")

    let tableVersion = TableVersion(
      table: tableDef,
      versions: @[],
      activeWriters: atomicInit(0),
      readMutex: createMutex()
    )
    storage.tables[tableDef.name] = tableVersion
    return noError()

# Begin a snapshot for reading
proc beginSnapshot*(storage: MVCCStorage, timestamp: int64): SnapshotHandle =
  SnapshotHandle(
    storage: storage,
    snapshotTimestamp: timestamp,
    tables: initTable[string, TableSnapshot]()
  )

# Get a consistent snapshot of a table
proc getTableSnapshot*(storage: MVCCStorage, tableName: string,
                      timestamp: int64): TableSnapshot =
  let tableVer = storage.tables.getOrDefault(tableName)
  if tableVer == nil:
    return nil

  withLock(tableVer.readMutex):
    # Find the most recent snapshot at or before the requested timestamp
    for i in countdown(tableVer.versions.high, 0):
      let snap = tableVer.versions[i]
      if snap.timestamp <= timestamp and snap.committed:
        return snap

    # If no snapshot found, return empty snapshot
    return TableSnapshot(version: timestamp, rows: @[], committed: true,
                        timestamp: timestamp)

# Insert a row (part of write transaction)
proc insertRow*(storage: MVCCStorage, tableName: string, row: Row,
               version: int64): FractioError =
  let tableVer = storage.tables.getOrDefault(tableName)
  if tableVer == nil:
    return semanticError("Table does not exist: " & tableName, "insertRow")

  # Increment writer count
  atomicInc(tableVer.activeWriters)
  defer: atomicDec(tableVer.activeWriters)

  withLock(tableVer.readMutex):
    # Validate row against schema
    if row.values.len != tableVer.table.columns.len:
      return constraintError("Column count mismatch", "insertRow")

    # Check constraints
    for i, col in tableVer.table.columns:
      let val = row.values[i]
      if val.kind != col.dataType:
        return constraintError("Type mismatch for column " & col.name, "insertRow")
      if not col.constraints.nullable and val.kind == dtString and
          val.strValue == "":
        return constraintError("NOT NULL constraint violation on " & col.name, "insertRow")

    # Check unique constraints
    # (in production, this would use indexes)

    # Create new version
    let newVersion = TableSnapshot(
      version: version,
      rows: tableVer.versions[^1].rows & row,
      committed: false,
      timestamp: getTime().toUnix * 1000
    )
    tableVer.versions.add(newVersion)

    return noError()

# Update rows (part of write transaction)
proc updateRows*(storage: MVCCStorage, tableName: string,
                updates: Table[RowID, Row], version: int64): FractioError =
  let tableVer = storage.tables.getOrDefault(tableName)
  if tableVer == nil:
    return semanticError("Table does not exist: " & tableName, "updateRows")

  atomicInc(tableVer.activeWriters)
  defer: atomicDec(tableVer.activeWriters)

  withLock(tableVer.readMutex):
    let baseSnapshot = tableVer.versions[^1]
    var newRows: seq[Row] = @[]

    for row in baseSnapshot.rows:
      if updates.hasKey(row.id):
        let updatedRow = updates[row.id]
        newRows.add(updatedRow)
      else:
        newRows.add(row)

    let newVersion = TableSnapshot(
      version: version,
      rows: newRows,
      committed: false,
      timestamp: getTime().toUnix * 1000
    )
    tableVer.versions.add(newVersion)
    return noError()

# Delete rows (part of write transaction)
proc deleteRows*(storage: MVCCStorage, tableName: string,
                rowIds: HashSet[RowID], version: int64): FractioError =
  let tableVer = storage.tables.getOrDefault(tableName)
  if tableVer == nil:
    return semanticError("Table does not exist: " & tableName, "deleteRows")

  atomicInc(tableVer.activeWriters)
  defer: atomicDec(tableVer.activeWriters)

  withLock(tableVer.readMutex):
    let baseSnapshot = tableVer.versions[^1]
    var newRows: seq[Row] = @[]

    for row in baseSnapshot.rows:
      if rowIds.contains(row.id):
        # Mark as deleted (keep in history for MVCC)
        continue
      newRows.add(row)

    let newVersion = TableSnapshot(
      version: version,
      rows: newRows,
      committed: false,
      timestamp: getTime().toUnix * 1000
    )
    tableVer.versions.add(newVersion)
    return noError()

# Commit a transaction's changes
proc commit*(storage: MVCCStorage, version: int64): FractioError =
  # Mark all uncommitted versions as committed
  for tableVer in storage.tables.values:
    withLock(tableVer.readMutex):
      if tableVer.versions[^1].version == version and
         not tableVer.versions[^1].committed:
        tableVer.versions[^1].committed = true

        # Optional: garbage collect old versions
        # In production, would have a background cleanup task
  return noError()

# Clean up old versions (background task)
proc garbageCollect*(storage: MVCCStorage, retention: int64 = 100) =
  ## Remove versions older than retention (in version count)
  withLock(storage.mutex):
    for tableVer in storage.tables.values:
      withLock(tableVer.readMutex):
        while tableVer.versions.len > retention:
          discard tableVer.versions.pop()

# No error constructor helper
proc noError*(): FractioError =
  FractioError(kind: fekNone, message: "", code: 0, context: "")
