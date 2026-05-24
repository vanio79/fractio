# Unit tests for fractio/protocol/txn_manager.nim
# Tests TransactionManager, TxnRecord, begin/commit/rollback/status

import std/[unittest, tables, sets, options]
import fractio/protocol/txn_manager
import fractio/protocol/messages/txn
import fractio/protocol/types
import fractio/core/types

suite "TransactionManager Constructor":

  test "newTransactionManager":
    let mgr = newTransactionManager()
    check mgr != nil
    check mgr.txns.len == 0
    check mgr.commitIndex.len == 0

suite "beginTransaction":

  test "basic beginTransaction":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    check rec.id != TransactionID(ZeroULID())
    check rec.state == TxnStatusActive
    check rec.readTimestamp > 0'u64
    check rec.commitTimestamp == 0'u64
    check rec.writeSet.len == 0
    check rec.readSet.len == 0
    check mgr.txns.len == 1

  test "beginTransaction with flags":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction(TxnFlagReadOnly)
    check rec.flags == TxnFlagReadOnly
    check rec.state == TxnStatusActive

  test "beginTransaction with timeout":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction(0'u8, 5000'u32)
    check rec.timeoutMs == 5000'u32

  test "beginTransaction with forcedId":
    let mgr = newTransactionManager()
    let forcedId = genTransactionIDLocal()
    let rec = mgr.beginTransaction(0'u8, 0'u32, some(forcedId))
    check rec.id == forcedId

  test "multiple beginTransaction unique IDs":
    let mgr = newTransactionManager()
    let rec1 = mgr.beginTransaction()
    let rec2 = mgr.beginTransaction()
    check rec1.id != rec2.id
    check mgr.txns.len == 2

  test "beginTransaction monotonic timestamps":
    let mgr = newTransactionManager()
    let rec1 = mgr.beginTransaction()
    let rec2 = mgr.beginTransaction()
    check rec2.readTimestamp > rec1.readTimestamp

suite "recordRead":

  test "recordRead success":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    let result = mgr.recordRead(rec.id, "key1")
    check result.isOk

  test "recordRead multiple keys":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    check mgr.recordRead(rec.id, "key1").isOk
    check mgr.recordRead(rec.id, "key2").isOk
    check mgr.recordRead(rec.id, "key3").isOk

  test "recordRead non-existent txn":
    let mgr = newTransactionManager()
    let fakeId = genTransactionIDLocal()
    let result = mgr.recordRead(fakeId, "key1")
    check result.isErr

  test "recordRead committed txn":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction(TxnFlagReadOnly)
    discard mgr.commitTransaction(rec.id)
    let result = mgr.recordRead(rec.id, "key1")
    check result.isErr

suite "recordWrite":

  test "recordWrite success":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    let result = mgr.recordWrite(rec.id, "key1")
    check result.isOk

  test "recordWrite multiple keys":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    check mgr.recordWrite(rec.id, "key1").isOk
    check mgr.recordWrite(rec.id, "key2").isOk

  test "recordWrite non-existent txn":
    let mgr = newTransactionManager()
    let fakeId = genTransactionIDLocal()
    let result = mgr.recordWrite(fakeId, "key1")
    check result.isErr

  test "recordWrite aborted txn":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    discard mgr.rollbackTransaction(rec.id)
    let result = mgr.recordWrite(rec.id, "key1")
    check result.isErr

suite "commitTransaction":

  test "commitTransaction read-only txn":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction(TxnFlagReadOnly)
    let resp = mgr.commitTransaction(rec.id)
    check resp.status == TxnCommitOK
    check resp.commitTimestamp > 0'u64

  test "commitTransaction write txn no conflict":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    check mgr.recordWrite(rec.id, "key1").isOk
    let resp = mgr.commitTransaction(rec.id)
    check resp.status == TxnCommitOK
    check resp.commitTimestamp > rec.readTimestamp

  test "commitTransaction multiple writes":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    check mgr.recordWrite(rec.id, "key1").isOk
    check mgr.recordWrite(rec.id, "key2").isOk
    check mgr.recordWrite(rec.id, "key3").isOk
    let resp = mgr.commitTransaction(rec.id)
    check resp.status == TxnCommitOK

  test "commitTransaction no conflict when txn starts after commit":
    let mgr = newTransactionManager()
    let txn1 = mgr.beginTransaction()
    check mgr.recordWrite(txn1.id, "key1").isOk
    let resp1 = mgr.commitTransaction(txn1.id)
    check resp1.status == TxnCommitOK

    let txn2 = mgr.beginTransaction()
    check mgr.recordWrite(txn2.id, "key1").isOk
    let resp2 = mgr.commitTransaction(txn2.id)
    check resp2.status == TxnCommitOK

  test "commitTransaction non-existent txn":
    let mgr = newTransactionManager()
    let fakeId = genTransactionIDLocal()
    let resp = mgr.commitTransaction(fakeId)
    check resp.status == TxnCommitNotFound

  test "commitTransaction already committed":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction(TxnFlagReadOnly)
    let resp1 = mgr.commitTransaction(rec.id)
    check resp1.status == TxnCommitOK
    let resp2 = mgr.commitTransaction(rec.id)
    check resp2.status == TxnCommitOK
    check resp2.commitTimestamp == resp1.commitTimestamp

  test "commitTransaction aborted txn":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    discard mgr.rollbackTransaction(rec.id)
    let resp = mgr.commitTransaction(rec.id)
    check resp.status == TxnCommitConflict

suite "rollbackTransaction":

  test "rollbackTransaction active txn":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    let resp = mgr.rollbackTransaction(rec.id)
    check resp.status == TxnRollbackOK

  test "rollbackTransaction already aborted":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    discard mgr.rollbackTransaction(rec.id)
    let resp = mgr.rollbackTransaction(rec.id)
    check resp.status == TxnRollbackOK

  test "rollbackTransaction non-existent txn":
    let mgr = newTransactionManager()
    let fakeId = genTransactionIDLocal()
    let resp = mgr.rollbackTransaction(fakeId)
    check resp.status == TxnRollbackNotFound

  test "rollbackTransaction committed txn":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction(TxnFlagReadOnly)
    discard mgr.commitTransaction(rec.id)
    let resp = mgr.rollbackTransaction(rec.id)
    check resp.status == TxnRollbackNotFound

suite "getTransactionStatus":

  test "getTransactionStatus active":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    let resp = mgr.getTransactionStatus(rec.id)
    check resp.status == TxnStatusActive
    check resp.commitTimestamp == 0'u64

  test "getTransactionStatus committed":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction(TxnFlagReadOnly)
    discard mgr.commitTransaction(rec.id)
    let resp = mgr.getTransactionStatus(rec.id)
    check resp.status == TxnStatusCommitted
    check resp.commitTimestamp > 0'u64

  test "getTransactionStatus aborted":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    discard mgr.rollbackTransaction(rec.id)
    let resp = mgr.getTransactionStatus(rec.id)
    check resp.status == TxnStatusAborted

  test "getTransactionStatus non-existent":
    let mgr = newTransactionManager()
    let fakeId = genTransactionIDLocal()
    let resp = mgr.getTransactionStatus(fakeId)
    check resp.status == TxnStatusNotFound

suite "getWriteSet":

  test "getWriteSet active txn":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    check mgr.recordWrite(rec.id, "key1").isOk
    check mgr.recordWrite(rec.id, "key2").isOk
    let ws = mgr.getWriteSet(rec.id)
    check ws.len == 2
    check "key1" in ws
    check "key2" in ws

  test "getWriteSet empty":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    let ws = mgr.getWriteSet(rec.id)
    check ws.len == 0

  test "getWriteSet non-existent txn":
    let mgr = newTransactionManager()
    let fakeId = genTransactionIDLocal()
    let ws = mgr.getWriteSet(fakeId)
    check ws.len == 0

suite "activeTxnCount and totalTxnCount":

  test "activeTxnCount empty":
    let mgr = newTransactionManager()
    check mgr.activeTxnCount() == 0

  test "activeTxnCount with active txns":
    let mgr = newTransactionManager()
    discard mgr.beginTransaction()
    discard mgr.beginTransaction()
    discard mgr.beginTransaction()
    check mgr.activeTxnCount() == 3

  test "activeTxnCount after commits":
    let mgr = newTransactionManager()
    let rec1 = mgr.beginTransaction(TxnFlagReadOnly)
    let rec2 = mgr.beginTransaction(TxnFlagReadOnly)
    discard mgr.commitTransaction(rec1.id)
    check mgr.activeTxnCount() == 1

  test "activeTxnCount after rollback":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    discard mgr.rollbackTransaction(rec.id)
    check mgr.activeTxnCount() == 0

  test "totalTxnCount":
    let mgr = newTransactionManager()
    discard mgr.beginTransaction()
    discard mgr.beginTransaction()
    check mgr.totalTxnCount() == 2

suite "expireTimedOutTxns":

  test "expireTimedOutTxns no expired":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction(0'u8, 30_000'u32)
    mgr.expireTimedOutTxns()
    let status = mgr.getTransactionStatus(rec.id)
    check status.status == TxnStatusActive

suite "Transaction Constants":

  test "DEFAULT_TXN_TIMEOUT_MS value":
    check DEFAULT_TXN_TIMEOUT_MS == 30_000'u32

suite "TxnRecord Fields":

  test "TxnRecord timestamps":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    check rec.readTimestamp > 0'u64
    check rec.commitTimestamp == 0'u64
    check rec.createdAtMs > 0'i64

suite "Conflict Detection Edge Cases":

  test "sequential writes to different keys no conflict":
    let mgr = newTransactionManager()
    let txn1 = mgr.beginTransaction()
    check mgr.recordWrite(txn1.id, "keyA").isOk
    let resp1 = mgr.commitTransaction(txn1.id)
    check resp1.status == TxnCommitOK

    let txn2 = mgr.beginTransaction()
    check mgr.recordWrite(txn2.id, "keyB").isOk
    let resp2 = mgr.commitTransaction(txn2.id)
    check resp2.status == TxnCommitOK

  test "write after committed read-only":
    let mgr = newTransactionManager()
    let txn1 = mgr.beginTransaction(TxnFlagReadOnly)
    let resp1 = mgr.commitTransaction(txn1.id)
    check resp1.status == TxnCommitOK

    let txn2 = mgr.beginTransaction()
    check mgr.recordWrite(txn2.id, "key1").isOk
    let resp2 = mgr.commitTransaction(txn2.id)
    check resp2.status == TxnCommitOK

suite "Idempotent Operations":

  test "commit idempotent after commit":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction(TxnFlagReadOnly)
    let ts1 = mgr.commitTransaction(rec.id).commitTimestamp
    let ts2 = mgr.commitTransaction(rec.id).commitTimestamp
    check ts1 == ts2

  test "rollback idempotent after abort":
    let mgr = newTransactionManager()
    let rec = mgr.beginTransaction()
    discard mgr.rollbackTransaction(rec.id)
    let resp1 = mgr.rollbackTransaction(rec.id)
    check resp1.status == TxnRollbackOK
    let resp2 = mgr.rollbackTransaction(rec.id)
    check resp2.status == TxnRollbackOK
