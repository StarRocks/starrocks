// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.lock;

public class Lock {
    private LockTarget lockTarget;
    private LockMode lockMode;

    public Lock(LockTarget lockTarget, LockMode lockMode) {
        this.lockTarget = lockTarget;
        this.lockMode = lockMode;
    }

    public LockTarget getLockTarget() {
        return lockTarget;
    }

    public LockMode getLockMode() {
        return lockMode;
    }
}
