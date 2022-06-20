// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.transaction;

import java.util.List;

public abstract class StateMachine {
    // This method is called by the FE master before changing the in-memory TransactionState to COMMITTED.
    public abstract void preCommit(TransactionState txnState, List<TabletCommitInfo> finishedTablets) throws TransactionException;

    // This method is called by the FE master after changing the in-memory TransactionState to COMMITTED.
    public abstract void postCommit(TransactionState txnState);

    // This method is called by the FE master after the edit log is persisted and before the writer lock
    // of DatabaseTransactionMgr is released.
    public abstract void postEditLog(TransactionState txnState);

    public abstract void applyCommit(TransactionState txnState, TableCommitInfo commitInfo);

    public abstract void applyPublishVersion(TransactionState txnState, TableCommitInfo commitInfo);
}
