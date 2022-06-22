// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.transaction;

import java.util.List;

public interface TransactionStateListener {
    // This method is called by the FE master before changing the in-memory TransactionState to COMMITTED.
    void preCommit(TransactionState txnState, List<TabletCommitInfo> finishedTablets) throws TransactionException;

    // This method is called by the FE master after changing the in-memory TransactionState to COMMITTED and before writing
    // the edit log.
    void preWriteCommitLog(TransactionState txnState);

    // This method is called by the FE master after changed the TransactionState to COMMITTED and persisted the edit log.
    void postWriteCommitLog(TransactionState txnState);
}
