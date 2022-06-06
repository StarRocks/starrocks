// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.lock;

public class Lock {
    private Long[] pathIds;
    private LockMode lockMode;
    private long lockContextId;

    public Lock(Long[] pathIds, LockMode lockMode, long lockContextId) {
        this.pathIds = pathIds;
        this.lockMode = lockMode;
        this.lockContextId = lockContextId;
    }

    public LockMode getLockMode() {
        return lockMode;
    }

    public Long[] getPathIds() {
        return pathIds;
    }

    public long getLockContextId() {
        return lockContextId;
    }
}
