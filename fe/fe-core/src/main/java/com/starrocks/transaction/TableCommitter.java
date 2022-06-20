// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.transaction;

import java.util.List;

public abstract class TableCommitter {
    // This method is called before changing the transaction to COMMITTED
    public abstract void preCommit(TransactionState txnState, List<TabletCommitInfo> finishedTablets) throws TransactionException;

    // This method is called after the transaction state changed to COMMITTED but before
    // writing edit log.
    public abstract void postCommit(TransactionState txnState);

    // This method is called after edit log is persisted.
    public abstract void postEditLog(TransactionState txnState);
}
