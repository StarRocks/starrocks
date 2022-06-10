// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.lock;

import java.util.concurrent.locks.Lock;

public abstract class LevelLock implements Lock {
    private Long[] pathIds;

    LevelLock(Long[] pathIds) {
        this.pathIds = pathIds;
    }

    public Long[] getPathIds() {
        return pathIds;
    }

    public boolean isExclusive() {
        return false;
    }
}
