// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.transaction;

import java.util.List;

public abstract class StateMachine {
    // This method is called by the FE master before changing the in-memory TransactionState to COMMITTED.
    public abstract void preCommit(TransactionState txnState, List<TabletCommitInfo> finishedTablets) throws TransactionException;

    // This method is called by the FE master after changing the in-memory TransactionState to COMMITTED and before writing
    // the edit log.
    public abstract void preWriteCommitLog(TransactionState txnState);

    // This method is called by the FE master after changed the TransactionState to COMMITTED and persisted the edit log.
    public abstract void postWriteCommitLog(TransactionState txnState);

    // This method is called by all the FE nodes after sent/received the edit log to change TransactionState to COMMITTED.
    public abstract void applyCommitLog(TransactionState txnState, TableCommitInfo commitInfo);

    // This method is called by all the FE nodes after sent/received the edit log to change the TransactionState to VISIBLE.
    public abstract void applyVisibleLog(TransactionState txnState, TableCommitInfo commitInfo);
}
