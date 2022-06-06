// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.lock;

import com.starrocks.transaction.TransactionState;

// record the context info about this lock
// used to get infos about locks
public class LockContext {
    // should be unique
    // example: txnId for load
    private final long relatedId;
    private final LockMode lockMode;
    // timestamp to start to acquire lock
    private final long lockTimestamp;
    // why this lock should be acquired
    private final TransactionState.LoadJobSourceType loadJobSourceType;

    public LockContext(long relatedId, LockMode lockMode, long lockTimestamp,
                       TransactionState.LoadJobSourceType loadJobSourceType) {
        this.relatedId = relatedId;
        this.lockMode = lockMode;
        this.lockTimestamp = lockTimestamp;
        this.loadJobSourceType = loadJobSourceType;
    }

    public long getRelatedId() {
        return relatedId;
    }

    public LockMode getLockMode() {
        return lockMode;
    }

    public long getLockTimestamp() {
        return lockTimestamp;
    }

    public TransactionState.LoadJobSourceType getLoadJobSourceType() {
        return loadJobSourceType;
    }

    public boolean isExclusive() {
        return getLockMode() == LockMode.EXCLUSIVE;
    }
}
