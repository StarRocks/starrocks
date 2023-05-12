// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;

public final class ColumnDict {
    private final ImmutableMap<ByteBuffer, Integer> dict;
    private final long collectedVersionTime;
    private long versionTime;

    public ColumnDict(ImmutableMap<ByteBuffer, Integer> dict, long versionTime) {
        Preconditions.checkState(dict.size() > 0 && dict.size() <= 256);
        this.dict = dict;
        this.collectedVersionTime = versionTime;
        this.versionTime = versionTime;
    }

    public ImmutableMap<ByteBuffer, Integer> getDict() {
        return dict;
    }

    public long getVersionTime() {
        return versionTime;
    }

    public long getCollectedVersionTime() {
        return collectedVersionTime;
    }

    void updateVersionTime(long versionTime) {
        this.versionTime = versionTime;
    }
}
