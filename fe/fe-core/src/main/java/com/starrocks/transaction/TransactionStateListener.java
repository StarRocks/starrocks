// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.transaction;

import java.util.List;

// TransactionStateListener will be created by the FE master process before it commit a transaction.
// Used to check if a transaction can be committed and save some information in the TransactionState.
public interface TransactionStateListener {
    // This method is called by the FE master before changing the in-memory TransactionState to COMMITTED.
    void preCommit(TransactionState txnState, List<TabletCommitInfo> finishedTablets,
            List<TabletFailInfo> failedTablets) throws TransactionException;

    // This method is called by the FE master after changing the in-memory TransactionState to COMMITTED and before writing
    // the edit log.
    void preWriteCommitLog(TransactionState txnState);

    // This method is called by the FE master after changed the TransactionState to COMMITTED and persisted the edit log.
    void postWriteCommitLog(TransactionState txnState);

    // This method is called by the FE master after changed the TransactionState to ABORTED and *AFTER* released the writer
    // lock of the DatabaseTransactionMgr.
    // It's *unsafe* to access mutable fields of txnState inside this function.
    void postAbort(TransactionState txnState, List<TabletFailInfo> failedTablets);
}
