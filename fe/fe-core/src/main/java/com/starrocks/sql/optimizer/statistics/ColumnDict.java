// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

public final class ColumnDict {
    private final ImmutableMap<String, Integer> dict;
    private final long versionTime;

    public ColumnDict(ImmutableMap<String, Integer> dict, long versionTime) {
        Preconditions.checkState(dict.size() > 0 && dict.size() <= 256);
        this.dict = dict;
        this.versionTime = versionTime;
    }

    public ImmutableMap<String, Integer> getDict() {
        return dict;
    }

    public long getVersionTime() {
        return versionTime;
    }
}