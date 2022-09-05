// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.transaction;

import com.starrocks.catalog.Database;

// Used to apply changes saved in the transaction edit logs to a table.
public interface TransactionLogApplier {

    // This method is called by all the FE nodes after sent/received the edit log to change TransactionState to COMMITTED.
    void applyCommitLog(TransactionState txnState, TableCommitInfo commitInfo);

    // This method is called by all the FE nodes after sent/received the edit log to change the TransactionState to VISIBLE.
    void applyVisibleLog(TransactionState txnState, TableCommitInfo commitInfo, Database db);
}
